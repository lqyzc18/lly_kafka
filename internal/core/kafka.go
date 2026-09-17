package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var errClientClosed = errors.New("kafka client 已关闭")

// readOpTimeout 读类操作的默认超时。
// 没有超时保护时，broker 无响应会让界面长期停留在「加载中」且无法重试。
const readOpTimeout = 30 * time.Second

// opCtx 为读操作补充默认超时；调用方已设置 deadline 时保持原样。
// 返回的 context 派生自传入 ctx，因此仍然会在断开连接时被取消。
func opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, readOpTimeout)
}

// KafkaClient 对 franz-go 的封装，提供集群管理能力。
// kgo.Client 天然支持并发和 context，无需懒加载 Consumer/Producer。
type KafkaClient struct {
	mu     sync.Mutex
	closed bool
	cancel context.CancelFunc
	ctx    context.Context

	conn   *Connection
	client *kgo.Client
	admin  *kadm.Client

	// 连接握手时已经拿到元数据，这里缓存下来避免界面重复请求。
	clusterID   string
	brokerCount int

	topicMu    sync.RWMutex
	topicNames []string
	fetches    sync.WaitGroup
}

// NewKafkaClient 根据连接配置创建 Kafka 客户端。
// franz-go 自动协商协议版本，无需手动指定 Kafka version。
func NewKafkaClient(ctx context.Context, conn *Connection) (*KafkaClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	brokers := parseBrokers(conn.Brokers)
	if len(brokers) == 0 {
		return nil, errors.New("broker 地址不能为空")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(conn.ClientID),
		kgo.DialTimeout(8 * time.Second),
		kgo.RequestTimeoutOverhead(5 * time.Second),
		kgo.RecordPartitioner(newOptionalPartitioner()),
	}

	if err := applySecurity(&opts, conn); err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	// 验证连通性：BrokerMetadata 不加载 topic，轻量。
	metaCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	meta, err := kadm.NewClient(client).BrokerMetadata(metaCtx)
	cancel()
	if err != nil {
		client.Close()
		return nil, err
	}

	cctx, cancelCtx := context.WithCancel(ctx)
	cp := *conn
	kc := &KafkaClient{
		ctx:         cctx,
		cancel:      cancelCtx,
		conn:        &cp,
		client:      client,
		admin:       kadm.NewClient(client),
		clusterID:   meta.Cluster,
		brokerCount: len(meta.Brokers),
	}
	g.Log().Infof(ctx, "已连接 Kafka: %s (%s) security=%s, brokers=%d, cluster=%s",
		conn.Name, conn.Brokers, conn.Security, len(meta.Brokers), meta.Cluster)
	return kc, nil
}

// TestConnection 用独立客户端探测连通性，不占用主窗口当前连接。
// 返回集群 ID 与 Broker 数量，均为连接握手阶段已获取的信息，不额外发起请求。
func TestConnection(ctx context.Context, conn *Connection) (clusterID string, brokers int, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	kc, err := NewKafkaClient(ctx, conn)
	if err != nil {
		return "", 0, err
	}
	defer kc.Close()
	return kc.ClusterID(), kc.BrokerCount(), nil
}

// Context 返回随 Close 取消的上下文，用于丢弃断开后的异步回调。
func (kc *KafkaClient) Context() context.Context {
	if kc == nil || kc.ctx == nil {
		return context.Background()
	}
	return kc.ctx
}

// Close 取消进行中的操作并关闭底层客户端。可重复调用。
func (kc *KafkaClient) Close() {
	if kc == nil {
		return
	}
	kc.mu.Lock()
	if kc.closed {
		kc.mu.Unlock()
		return
	}
	kc.closed = true
	cancel := kc.cancel
	client := kc.client
	kc.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	kc.fetches.Wait()
	if client != nil {
		client.Close()
	}
}

// invalidateTopics 清除 Topic 名称缓存，下次 ListTopicNames 会强制刷新。
func (kc *KafkaClient) invalidateTopics() {
	kc.topicMu.Lock()
	kc.topicNames = nil
	kc.topicMu.Unlock()
}

// alive 检查客户端是否已关闭，或 context 是否已取消。
func (kc *KafkaClient) alive(ctx context.Context) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	kc.mu.Lock()
	defer kc.mu.Unlock()
	if kc.closed {
		return errClientClosed
	}
	return nil
}

// beginFetch 标记一次拉取开始，Close 会等待所有拉取完成。
func (kc *KafkaClient) beginFetch(ctx context.Context) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	kc.mu.Lock()
	defer kc.mu.Unlock()
	if kc.closed {
		return errClientClosed
	}
	kc.fetches.Add(1)
	return nil
}

// Connection 返回关联的连接配置副本。
func (kc *KafkaClient) Connection() *Connection {
	if kc.conn == nil {
		return nil
	}
	cp := *kc.conn
	return &cp
}

// connName 返回当前连接名称，用于日志和审计。
func (kc *KafkaClient) connName() string {
	if kc.conn == nil {
		return ""
	}
	return kc.conn.Name
}

// ClusterID 返回集群 ID（连接握手时获取并缓存，不额外发起请求）。
// franz-go 自动协商协议版本，因此这里不再提供「Kafka 版本」，
// 改为展示集群真实标识，避免向用户呈现无意义的占位值。
func (kc *KafkaClient) ClusterID() string {
	if kc == nil {
		return ""
	}
	return kc.clusterID
}

// BrokerCount 返回连接时的 Broker 数量（缓存值，不额外发起请求）。
func (kc *KafkaClient) BrokerCount() int {
	if kc == nil {
		return 0
	}
	return kc.brokerCount
}

// parseBrokers 解析逗号分隔的 broker 地址字符串。
func parseBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			out = append(out, b)
		}
	}
	return out
}

// ctxErr 返回 context 的错误，nil context 返回 nil。
func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
