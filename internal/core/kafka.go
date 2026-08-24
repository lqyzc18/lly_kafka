package core

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/gogf/gf/v2/frame/g"
)

// KafkaClient 对 sarama 的封装，提供集群管理能力。
// 包含 Admin（topic/groups 管理）、Consumer（消息拉取）、SyncProducer（消息发送）。
type KafkaClient struct {
	conn     *Connection
	client   sarama.Client
	admin    sarama.ClusterAdmin
	consumer sarama.Consumer
	producer sarama.SyncProducer
}

// NewKafkaClient 根据连接配置创建 Kafka 客户端。
// 若 Kafka 版本不识别，回退到 2.6.0 以保证最大兼容性。
func NewKafkaClient(ctx context.Context, conn *Connection) (*KafkaClient, error) {
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
	cfg.Net.DialTimeout = 5 * time.Second
	cfg.Net.ReadTimeout = 15 * time.Second
	cfg.Metadata.Full = true

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, err
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		_ = admin.Close()
		_ = client.Close()
		return nil, err
	}
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = consumer.Close()
		_ = admin.Close()
		_ = client.Close()
		return nil, err
	}

	kc := &KafkaClient{
		conn:     conn,
		client:   client,
		admin:    admin,
		consumer: consumer,
		producer: producer,
	}
	g.Log().Infof(ctx, "已连接 Kafka: %s (%s), 协议版本 %s", conn.Name, conn.Brokers, ver)
	return kc, nil
}

// Close 关闭所有客户端资源。
func (kc *KafkaClient) Close() {
	if kc.producer != nil {
		_ = kc.producer.Close()
	}
	if kc.consumer != nil {
		_ = kc.consumer.Close()
	}
	if kc.admin != nil {
		_ = kc.admin.Close()
	}
	if kc.client != nil {
		_ = kc.client.Close()
	}
}

// Connection 返回关联的连接配置。
func (kc *KafkaClient) Connection() *Connection { return kc.conn }

// KafkaVersion 返回协商后的 Kafka 版本字符串。
func (kc *KafkaClient) KafkaVersion() string { return kc.client.Config().Version.String() }

// BrokerCount 返回集群 Broker 数量。
func (kc *KafkaClient) BrokerCount() int { return len(kc.client.Brokers()) }

// ClusterID 获取 Kafka 集群 ID（依赖 kafka 2.0+）。
func (kc *KafkaClient) ClusterID(ctx context.Context) (string, error) {
	brokers := kc.client.Brokers()
	if len(brokers) == 0 {
		return "", errors.New("no brokers")
	}
	b := brokers[0]
	if err := b.Open(kc.client.Config()); err != nil {
		return "", err
	}
	defer b.Close()
	req := &sarama.MetadataRequest{Topics: nil, AllowAutoTopicCreation: false}
	resp, err := b.GetMetadata(req)
	if err != nil {
		return "", err
	}
	if resp.ClusterID != nil {
		g.Log().Debug(ctx, "ClusterID: ", *resp.ClusterID)
		return *resp.ClusterID, nil
	}
	return "", nil
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