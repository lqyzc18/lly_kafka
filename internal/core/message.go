package core

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	if err := kc.alive(ctx); err != nil {
		return 0, err
	}
	return kc.client.GetOffset(topic, partition, sarama.OffsetNewest)
}

// OldestOffset 返回分区的最旧 offset。
func (kc *KafkaClient) OldestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	if err := kc.alive(ctx); err != nil {
		return 0, err
	}
	return kc.client.GetOffset(topic, partition, sarama.OffsetOldest)
}

// GetOffsetByTime 根据时间戳定位分区的起始 Offset。
// 如果指定时间早于最旧数据，返回 OldestOffset；若晚于最新数据，返回 NewestOffset。
func (kc *KafkaClient) GetOffsetByTime(ctx context.Context, topic string, partition int32, t time.Time) (int64, error) {
	if err := kc.alive(ctx); err != nil {
		return 0, err
	}
	oldest, hwm, err := kc.oldestAndNewest(ctx, topic, partition)
	if err != nil {
		return 0, err
	}
	if t.IsZero() {
		return oldest, nil
	}
	off, err := kc.client.GetOffset(topic, partition, t.UnixMilli())
	if err != nil {
		return 0, err
	}
	if off < 0 || off > hwm {
		return hwm, nil
	}
	if off < oldest {
		return oldest, nil
	}
	return off, nil
}

func (kc *KafkaClient) oldestAndNewest(ctx context.Context, topic string, partition int32) (oldest, hwm int64, err error) {
	if err := kc.alive(ctx); err != nil {
		return 0, 0, err
	}
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

// FetchByTime 根据起始时间拉取指定分区的消息。
func (kc *KafkaClient) FetchByTime(ctx context.Context, topic string, partition int32, startTime time.Time, max int) ([]MessageInfo, error) {
	if ctx == nil {
		ctx = kc.Context()
	}
	startOffset, err := kc.GetOffsetByTime(ctx, topic, partition, startTime)
	if err != nil {
		return nil, err
	}
	return kc.FetchMessages(ctx, topic, partition, startOffset, 0, max)
}

// FetchByTimeAll 根据起始时间拉取该 Topic 所有分区的消息（合计不超过 max）。
func (kc *KafkaClient) FetchByTimeAll(ctx context.Context, topic string, startTime time.Time, max int) ([]MessageInfo, error) {
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
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	per := max / count
	if per < 1 {
		per = 1
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
		msgs, e := kc.FetchByTime(ctx, topic, int32(i), startTime, per)
		results[i] = partResult{msgs: msgs, err: e}
	})

	var all []MessageInfo
	var errs []error
	for _, r := range results {
		all = append(all, r.msgs...)
		if r.err != nil {
			errs = append(errs, r.err)
		}
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
	return all, errors.Join(errs...)
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

// FetchLatestAll 在 partition == AllPartitions 时拉取该 Topic 全部分区的最新 n 条（合计不超过 max）。
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
	var errs []error
	for _, r := range results {
		all = append(all, r.msgs...)
		if r.err != nil {
			errs = append(errs, r.err)
		}
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
	return all, errors.Join(errs...)
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
	var errs []error
	for _, r := range results {
		all = append(all, r.msgs...)
		if r.err != nil {
			errs = append(errs, r.err)
		}
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
	return all, errors.Join(errs...)
}

// StartLiveTail 启动实时消息监听，从最新消息（HWM）开始持续消费新到达的消息并发送到 msgCh。
func (kc *KafkaClient) StartLiveTail(ctx context.Context, topic string, partition int32, msgCh chan<- MessageInfo, errCh chan<- error) error {
	if err := kc.alive(ctx); err != nil {
		return err
	}
	consumer, err := kc.getConsumer()
	if err != nil {
		return err
	}

	var partitions []int32
	if partition == AllPartitions {
		count, err := kc.PartitionCount(ctx, topic)
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			partitions = append(partitions, int32(i))
		}
	} else {
		partitions = []int32{partition}
	}

	if len(partitions) == 0 {
		return errors.New("无可用分区")
	}

	for _, p := range partitions {
		p := p
		if err := kc.beginFetch(ctx); err != nil {
			return err
		}
		go func() {
			defer kc.fetches.Done()
			pc, err := consumer.ConsumePartition(topic, p, sarama.OffsetNewest)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("分区 %d 监听启动失败: %w", p, err):
				case <-ctx.Done():
				}
				return
			}
			defer pc.Close()

			for {
				select {
				case m, ok := <-pc.Messages():
					if !ok {
						return
					}
					select {
					case msgCh <- toMessageInfo(m):
					case <-ctx.Done():
						return
					}
				case e, ok := <-pc.Errors():
					if !ok || e == nil {
						continue
					}
					select {
					case errCh <- e.Err:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return nil
}

func (kc *KafkaClient) fetchRange(ctx context.Context, topic string, partition int32, oldest, hwm, start, end int64, max int) ([]MessageInfo, error) {
	from, toEx := normalizeFetchRange(oldest, hwm, start, end, max)
	if from >= toEx {
		return []MessageInfo{}, nil
	}
	if err := kc.beginFetch(ctx); err != nil {
		return nil, err
	}
	defer kc.fetches.Done()

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
				return msgs, fmt.Errorf("消息流提前结束: %w", io.ErrUnexpectedEOF)
			}
			msgs = append(msgs, toMessageInfo(m))
		case e, ok := <-pc.Errors():
			if !ok || e == nil {
				continue
			}
			g.Log().Warningf(ctx, "拉取消息出错: %v，已返回 %d 条", e.Err, len(msgs))
			return msgs, e.Err
		case <-ctx.Done():
			g.Log().Warningf(ctx, "拉取消息超时/取消，已返回 %d 条", len(msgs))
			return msgs, fmt.Errorf("拉取未完成: %w", ctx.Err())
		}
	}
	return msgs, nil
}

// ProduceMessage 同步发送一条消息。partition < 0 时按 Key 哈希（无 Key 则随机）。
func (kc *KafkaClient) ProduceMessage(ctx context.Context, topic string, partition int32, key, value []byte, headers map[string]string) (int32, int64, error) {
	if err := kc.alive(ctx); err != nil {
		return 0, 0, err
	}
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
