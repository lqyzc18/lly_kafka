package core

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/gogf/gf/v2/frame/g"
)

// HeaderInfo 消息 Header
type HeaderInfo struct {
	Key   string
	Value string
}

// MessageInfo 一条 Kafka 消息的展示信息。
type MessageInfo struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []HeaderInfo
	Timestamp time.Time
	Size      int
}

// NewestOffset 返回分区的最新 offset（HighWaterMark）。
func (kc *KafkaClient) NewestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	return kc.client.GetOffset(topic, partition, sarama.OffsetNewest)
}

// OldestOffset 返回分区的最旧 offset。
func (kc *KafkaClient) OldestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	return kc.client.GetOffset(topic, partition, sarama.OffsetOldest)
}

// FetchMessages 拉取指定分区 [start, end) 范围内的消息。
//   - start: 起始 offset（包含）
//   - end:   结束 offset（包含）；<=0 时表示读到最新
//   - max:   最大条数（<=0 取默认 500）
//
// 若 start 超出当前最新 offset 则直接返回空切片（避免长时间阻塞）。
func (kc *KafkaClient) FetchMessages(ctx context.Context, topic string, partition int32, start, end int64, max int) ([]MessageInfo, error) {
	if max <= 0 {
		max = 500
	}
	hwm, err := kc.NewestOffset(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	if end <= start || end > hwm {
		end = hwm
	}
	if start < 0 {
		start = 0
	}
	if start >= hwm {
		return []MessageInfo{}, nil
	}

	pc, err := kc.consumer.ConsumePartition(topic, partition, start)
	if err != nil {
		return nil, err
	}
	defer pc.Close()

	msgs := make([]MessageInfo, 0, max)
	deadline := time.After(5 * time.Second)
	expect := end - start
	if expect > int64(max) {
		expect = int64(max)
	}
	for int64(len(msgs)) < expect {
		select {
		case m, ok := <-pc.Messages():
			if !ok {
				return msgs, nil
			}
			msgs = append(msgs, toMessageInfo(m))
		case <-deadline:
			g.Log().Warningf(ctx, "拉取消息超时，已返回 %d 条", len(msgs))
			return msgs, nil
		}
	}
	return msgs, nil
}

// ProduceMessage 同步发送一条消息。partition < 0 时由 broker 根据 Key 选择。
func (kc *KafkaClient) ProduceMessage(ctx context.Context, topic string, partition int32, key, value []byte, headers map[string]string) (int32, int64, error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	if partition >= 0 {
		msg.Partition = partition
	}
	for k, v := range headers {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}
	pid, offset, err := kc.producer.SendMessage(msg)
	if err == nil {
		g.Log().Infof(ctx, "发送消息成功 topic=%s partition=%d offset=%d", topic, pid, offset)
	}
	return pid, offset, err
}

func toMessageInfo(m *sarama.ConsumerMessage) MessageInfo {
	info := MessageInfo{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Timestamp: m.Timestamp,
		Size:      len(m.Key) + len(m.Value),
	}
	for _, h := range m.Headers {
		info.Headers = append(info.Headers, HeaderInfo{Key: string(h.Key), Value: string(h.Value)})
	}
	return info
}