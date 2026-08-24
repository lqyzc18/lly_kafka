package core

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gogf/gf/v2/frame/g"
)

const defaultFetchTimeout = 8 * time.Second

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
	_ = ctx
	return kc.client.GetOffset(topic, partition, sarama.OffsetNewest)
}

// OldestOffset 返回分区的最旧 offset。
func (kc *KafkaClient) OldestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	_ = ctx
	return kc.client.GetOffset(topic, partition, sarama.OffsetOldest)
}

func (kc *KafkaClient) oldestAndNewest(ctx context.Context, topic string, partition int32) (oldest, hwm int64, err error) {
	var oerr, nerr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		oldest, oerr = kc.client.GetOffset(topic, partition, sarama.OffsetOldest)
	}()
	go func() {
		defer wg.Done()
		hwm, nerr = kc.client.GetOffset(topic, partition, sarama.OffsetNewest)
	}()
	wg.Wait()
	if err := ctxErr(ctx); err != nil {
		return 0, 0, err
	}
	if oerr != nil {
		return 0, 0, oerr
	}
	if nerr != nil {
		return 0, 0, nerr
	}
	return oldest, hwm, nil
}

// FetchLatest 拉取指定分区最新 n 条消息（不超过 max，且不早于 OldestOffset）。
func (kc *KafkaClient) FetchLatest(ctx context.Context, topic string, partition int32, n, max int) ([]MessageInfo, error) {
	if ctx == nil {
		ctx = kc.Context()
	}
	oldest, hwm, err := kc.oldestAndNewest(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	start := latestStart(oldest, hwm, int64(n))
	return kc.fetchRange(ctx, topic, partition, oldest, hwm, start, 0, max)
}

// FetchMessages 拉取指定分区的消息。
//   - start: 起始 offset（包含）
//   - end:   结束 offset（包含）；<=0 时表示读到最新
//   - max:   最大条数（<=0 取默认 500）
func (kc *KafkaClient) FetchMessages(ctx context.Context, topic string, partition int32, start, end int64, max int) ([]MessageInfo, error) {
	if ctx == nil {
		ctx = kc.Context()
	}
	oldest, hwm, err := kc.oldestAndNewest(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	return kc.fetchRange(ctx, topic, partition, oldest, hwm, start, end, max)
}

const AllPartitions int32 = -1

// FetchLatest 在 partition == AllPartitions 时拉取该 Topic 全部分区的最新 n 条（合计不超过 max）。
func (kc *KafkaClient) FetchLatestAll(ctx context.Context, topic string, n, max int) ([]MessageInfo, error) {
	if ctx == nil {
		ctx = kc.Context()
	}
	count, err := kc.PartitionCount(ctx, topic)
	if err != nil {
		return nil, err
	}
	if count <= 0 {
		return []MessageInfo{}, nil
	}
	if max <= 0 {
		max = defaultFetchMax
	}
	if n <= 0 {
		n = 50
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	type partResult struct {
		msgs []MessageInfo
		err  error
	}
	results := make([]partResult, count)
	runBounded(count, 8, func(i int) {
		if ctxErr(ctx) != nil {
			return
		}
		msgs, e := kc.FetchLatest(ctx, topic, int32(i), n, n)
		results[i] = partResult{msgs: msgs, err: e}
	})
	var all []MessageInfo
	var firstErr error
	for _, r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
			continue
		}
		all = append(all, r.msgs...)
	}
	if len(all) == 0 && firstErr != nil {
		return nil, firstErr
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].Timestamp.Equal(all[j].Timestamp) {
			return all[i].Timestamp.After(all[j].Timestamp)
		}
		if all[i].Partition != all[j].Partition {
			return all[i].Partition < all[j].Partition
		}
		return all[i].Offset > all[j].Offset
	})
	if len(all) > max {
		all = all[:max]
	}
	return all, nil
}

// FetchMessagesAll 对全部分区应用同一 offset 范围，合计不超过 max。
func (kc *KafkaClient) FetchMessagesAll(ctx context.Context, topic string, start, end int64, max int) ([]MessageInfo, error) {
	if ctx == nil {
		ctx = kc.Context()
	}
	count, err := kc.PartitionCount(ctx, topic)
	if err != nil {
		return nil, err
	}
	if max <= 0 {
		max = defaultFetchMax
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	per := max
	if count > 0 {
		per = max / count
		if per < 1 {
			per = 1
		}
	}
	type partResult struct {
		msgs []MessageInfo
		err  error
	}
	results := make([]partResult, count)
	runBounded(count, 8, func(i int) {
		if ctxErr(ctx) != nil {
			return
		}
		msgs, e := kc.FetchMessages(ctx, topic, int32(i), start, end, per)
		results[i] = partResult{msgs: msgs, err: e}
	})
	var all []MessageInfo
	var firstErr error
	for _, r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
			continue
		}
		all = append(all, r.msgs...)
	}
	if len(all) == 0 && firstErr != nil {
		return nil, firstErr
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Partition != all[j].Partition {
			return all[i].Partition < all[j].Partition
		}
		return all[i].Offset < all[j].Offset
	})
	if len(all) > max {
		all = all[:max]
	}
	return all, nil
}

func (kc *KafkaClient) fetchRange(ctx context.Context, topic string, partition int32, oldest, hwm, start, end int64, max int) ([]MessageInfo, error) {
	from, toEx := normalizeFetchRange(oldest, hwm, start, end, max)
	if from >= toEx {
		return []MessageInfo{}, nil
	}

	consumer, err := kc.getConsumer()
	if err != nil {
		return nil, err
	}
	pc, err := consumer.ConsumePartition(topic, partition, from)
	if err != nil {
		return nil, err
	}
	defer pc.Close()

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultFetchTimeout)
		defer cancel()
	}

	expect := toEx - from
	msgs := make([]MessageInfo, 0, expect)
	for int64(len(msgs)) < expect {
		select {
		case m, ok := <-pc.Messages():
			if !ok {
				return msgs, nil
			}
			msgs = append(msgs, toMessageInfo(m))
		case e, ok := <-pc.Errors():
			if !ok || e == nil {
				continue
			}
			g.Log().Warningf(ctx, "拉取消息出错: %v，已返回 %d 条", e.Err, len(msgs))
			if len(msgs) == 0 {
				return nil, e.Err
			}
			return msgs, nil
		case <-ctx.Done():
			g.Log().Warningf(ctx, "拉取消息超时/取消，已返回 %d 条", len(msgs))
			return msgs, nil
		}
	}
	return msgs, nil
}

// ProduceMessage 同步发送一条消息。partition < 0 时按 Key 哈希（无 Key 则随机）。
func (kc *KafkaClient) ProduceMessage(ctx context.Context, topic string, partition int32, key, value []byte, headers map[string]string) (int32, int64, error) {
	producer, err := kc.getProducer()
	if err != nil {
		return 0, 0, err
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}
	if len(key) > 0 {
		msg.Key = sarama.ByteEncoder(key)
	}
	if partition >= 0 {
		msg.Partition = partition
		msg.Metadata = manualPartitionMeta
	}
	for k, v := range headers {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}
	pid, offset, err := producer.SendMessage(msg)
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
