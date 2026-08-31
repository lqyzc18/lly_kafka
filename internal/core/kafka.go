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
		// 手动分区时直接按 record.Partition 发送。
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
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
		ctx:    cctx,
		cancel: cancelCtx,
		conn:   &cp,
		client: client,
		admin:  kadm.NewClient(client),
	}
	g.Log().Infof(ctx, "已连接 Kafka: %s (%s) security=%s, brokers=%d, cluster=%s",
		conn.Name, conn.Brokers, conn.Security, len(meta.Brokers), meta.Cluster)
	return kc, nil
}

// TestConnection 用独立客户端探测连通性，不占用主窗口当前连接。
func TestConnection(ctx context.Context, conn *Connection) (version string, brokers int, err error) {
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
	return kc.KafkaVersion(ctx), kc.BrokerCount(ctx), nil
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

func (kc *KafkaClient) invalidateTopics() {
	kc.topicMu.Lock()
	kc.topicNames = nil
	kc.topicMu.Unlock()
}

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

// KafkaVersion 返回协商后的 Kafka 版本字符串（实际为 API 版本上限）。
func (kc *KafkaClient) KafkaVersion(ctx context.Context) string {
	if kc.client == nil {
		return ""
	}
	// franz-go 没有显式 "Kafka version"，返回 broker 支持的最大 API 版本。
	// 这里简化处理：返回一个描述性字符串。
	if ctx == nil {
		ctx = kc.Context()
	}
	meta, err := kc.admin.BrokerMetadata(ctx)
	if err != nil || len(meta.Brokers) == 0 {
		return "unknown"
	}
	return "auto-negotiated"
}

// BrokerCount 返回集群 Broker 数量。
func (kc *KafkaClient) BrokerCount(ctx context.Context) int {
	if kc.client == nil {
		return 0
	}
	if ctx == nil {
		ctx = kc.Context()
	}
	meta, err := kc.admin.BrokerMetadata(ctx)
	if err != nil {
		return 0
	}
	return len(meta.Brokers)
}

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

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
