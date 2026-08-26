package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gogf/gf/v2/frame/g"
)

var errClientClosed = errors.New("kafka client 已关闭")

// KafkaClient 对 sarama 的封装，提供集群管理能力。
// Admin 在连接时创建；Consumer / Producer 按需懒加载。
type KafkaClient struct {
	mu     sync.Mutex
	closed bool
	cancel context.CancelFunc
	ctx    context.Context

	conn     *Connection
	client   sarama.Client
	admin    sarama.ClusterAdmin
	consumer sarama.Consumer
	producer sarama.SyncProducer

	topicMu    sync.RWMutex
	topicNames []string
	fetches    sync.WaitGroup
}

// NewKafkaClient 根据连接配置创建 Kafka 客户端。
// 若 Kafka 版本不识别，回退到 2.6.0 以保证最大兼容性。
func NewKafkaClient(ctx context.Context, conn *Connection) (*KafkaClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	brokers := parseBrokers(conn.Brokers)
	if len(brokers) == 0 {
		return nil, errors.New("broker 地址不能为空")
	}

	cfg := sarama.NewConfig()
	cfg.ClientID = conn.ClientID
	if cfg.ClientID == "" {
		cfg.ClientID = "lly-kafka"
	}
	ver, err := sarama.ParseKafkaVersion(conn.Version)
	if err != nil || conn.Version == "" {
		g.Log().Warningf(ctx, "Kafka 版本 %q 无法解析，回退到 v2.6.0", conn.Version)
		ver = sarama.V2_6_0_0
	}
	cfg.Version = ver
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.MaxProcessingTime = 5 * time.Second
	cfg.Producer.Return.Successes = true
	cfg.Producer.Timeout = 10 * time.Second
	cfg.Producer.Partitioner = func(topic string) sarama.Partitioner {
		return &flexiblePartitioner{hash: sarama.NewHashPartitioner(topic)}
	}
	cfg.Net.DialTimeout = 8 * time.Second
	cfg.Net.ReadTimeout = 15 * time.Second
	cfg.Metadata.Full = false
	cfg.Metadata.Timeout = 10 * time.Second

	if err := applySecurity(cfg, conn); err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, err
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	cctx, cancel := context.WithCancel(ctx)
	cp := *conn
	kc := &KafkaClient{
		ctx:    cctx,
		cancel: cancel,
		conn:   &cp,
		client: client,
		admin:  admin,
	}
	g.Log().Infof(ctx, "已连接 Kafka: %s (%s) security=%s, 协议版本 %s", conn.Name, conn.Brokers, conn.Security, ver)
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
	return kc.KafkaVersion(), kc.BrokerCount(), nil
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
	producer := kc.producer
	consumer := kc.consumer
	admin := kc.admin
	client := kc.client
	kc.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	// Sarama requires partition consumers to finish before their shared
	// Consumer/Admin client is closed.
	kc.fetches.Wait()
	if producer != nil {
		_ = producer.Close()
	}
	if consumer != nil {
		_ = consumer.Close()
	}
	if admin != nil {
		_ = admin.Close()
	}
	if client != nil {
		_ = client.Close()
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

// KafkaVersion 返回协商后的 Kafka 版本字符串。
func (kc *KafkaClient) KafkaVersion() string {
	if kc.client == nil {
		return ""
	}
	return kc.client.Config().Version.String()
}

// BrokerCount 返回集群 Broker 数量。
func (kc *KafkaClient) BrokerCount() int {
	if kc.client == nil {
		return 0
	}
	return len(kc.client.Brokers())
}

func (kc *KafkaClient) getConsumer() (sarama.Consumer, error) {
	kc.mu.Lock()
	if kc.closed {
		kc.mu.Unlock()
		return nil, errClientClosed
	}
	if kc.consumer != nil {
		c := kc.consumer
		kc.mu.Unlock()
		return c, nil
	}
	client := kc.client
	kc.mu.Unlock()

	c, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	kc.mu.Lock()
	defer kc.mu.Unlock()
	if kc.closed {
		_ = c.Close()
		return nil, errClientClosed
	}
	if kc.consumer != nil {
		_ = c.Close()
		return kc.consumer, nil
	}
	kc.consumer = c
	return c, nil
}

func (kc *KafkaClient) getProducer() (sarama.SyncProducer, error) {
	kc.mu.Lock()
	if kc.closed {
		kc.mu.Unlock()
		return nil, errClientClosed
	}
	if kc.producer != nil {
		p := kc.producer
		kc.mu.Unlock()
		return p, nil
	}
	client := kc.client
	kc.mu.Unlock()

	p, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	kc.mu.Lock()
	defer kc.mu.Unlock()
	if kc.closed {
		_ = p.Close()
		return nil, errClientClosed
	}
	if kc.producer != nil {
		_ = p.Close()
		return kc.producer, nil
	}
	kc.producer = p
	return p, nil
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
