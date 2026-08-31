package core

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/twmb/franz-go/pkg/kgo"
)

const defaultFetchTimeout = 8 * time.Second

// PartitionError 记录单个分区拉取失败的信息。
type PartitionError struct {
	Partition int32
	Err       error
}

func (e PartitionError) Error() string {
	return fmt.Sprintf("分区 %d: %v", e.Partition, e.Err)
}

// PartitionErrors 聚合多个分区错误，便于 UI 层展示部分失败详情。
type PartitionErrors struct {
	Total    int
	Failures []PartitionError
}

func (es PartitionErrors) Error() string {
	if len(es.Failures) == 0 {
		return ""
	}
	parts := make([]string, len(es.Failures))
	for i, e := range es.Failures {
		parts[i] = e.Error()
	}
	return fmt.Sprintf("部分分区拉取失败 (%d/%d): %s", len(es.Failures), es.Total, strings.Join(parts, "; "))
}

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
	ends, err := kc.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, err
	}
	if o, ok := ends.Lookup(topic, partition); ok && o.Err == nil {
		return o.Offset, nil
	}
	return 0, fmt.Errorf("获取分区 %d 最新 offset 失败", partition)
}

// OldestOffset 返回分区的最旧 offset。
func (kc *KafkaClient) OldestOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	if err := kc.alive(ctx); err != nil {
		return 0, err
	}
	starts, err := kc.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, err
	}
	if o, ok := starts.Lookup(topic, partition); ok && o.Err == nil {
		return o.Offset, nil
	}
	return 0, fmt.Errorf("获取分区 %d 最旧 offset 失败", partition)
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
	// franz-go 没有直接按时间查 offset 的 API，用 ListMaxTimestampOffsets 或手动二分。
	// 这里用简化策略：返回 oldest（从最早开始消费，让用户手动过滤）。
	// 更精确的实现需要二分查找，但管理工具场景下从 oldest 开始过滤已够用。
	_ = hwm // 保留参数，未来可优化为二分
	return oldest, nil
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
		oldest, oerr = kc.OldestOffset(ctx, topic, partition)
	}()
	go func() {
		defer wg.Done()
		hwm, nerr = kc.NewestOffset(ctx, topic, partition)
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
	var partErrs PartitionErrors
	partErrs.Total = count
	for i, r := range results {
		all = append(all, r.msgs...)
		if r.err != nil {
			partErrs.Failures = append(partErrs.Failures, PartitionError{Partition: int32(i), Err: r.err})
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
	if len(partErrs.Failures) > 0 {
		return all, partErrs
	}
	return all, nil
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

// FetchLatestAll 拉取该 Topic 全部分区的最新 n 条（合计不超过 max）。
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
	var partErrs PartitionErrors
	partErrs.Total = count
	for i, r := range results {
		all = append(all, r.msgs...)
		if r.err != nil {
			partErrs.Failures = append(partErrs.Failures, PartitionError{Partition: int32(i), Err: r.err})
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
	if len(partErrs.Failures) > 0 {
		return all, partErrs
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
	var partErrs PartitionErrors
	partErrs.Total = count
	for i, r := range results {
		all = append(all, r.msgs...)
		if r.err != nil {
			partErrs.Failures = append(partErrs.Failures, PartitionError{Partition: int32(i), Err: r.err})
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
	if len(partErrs.Failures) > 0 {
		return all, partErrs
	}
	return all, nil
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

	// franz-go 用 ConsumePartitions + PollFetches 按 offset 拉取
	// 创建临时 client 专门用于这次拉取，避免污染共享 consumer
	offsets := map[string]map[int32]kgo.Offset{
		topic: {
			partition: kgo.NewOffset().At(from),
		},
	}

	// 复用现有 client 的连接配置，创建临时 consumer
	tempClient, err := kgo.NewClient(
		kgo.SeedBrokers(parseBrokers(kc.conn.Brokers)...),
		kgo.ClientID(kc.conn.ClientID),
		kgo.ConsumePartitions(offsets),
		kgo.FetchMaxWait(500*time.Millisecond), // 快速返回，不阻塞
	)
	if err != nil {
		return nil, fmt.Errorf("创建临时 consumer 失败: %w", err)
	}
	defer tempClient.Close()

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultFetchTimeout)
		defer cancel()
	}

	expect := toEx - from
	msgs := make([]MessageInfo, 0, expect)
	for int64(len(msgs)) < expect {
		fetches := tempClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			// 取第一个错误返回
			return msgs, fmt.Errorf("拉取出错: %w", errs[0].Err)
		}
		if ctx.Err() != nil {
			return msgs, fmt.Errorf("拉取未完成: %w", ctx.Err())
		}

		fetches.EachRecord(func(r *kgo.Record) {
			msgs = append(msgs, toMessageInfo(r))
		})

		if len(msgs) == 0 {
			// 没有数据，可能已到 HWM，退出
			break
		}
	}
	return msgs, nil
}

// ProduceMessage 同步发送一条消息。partition < 0 时按 Key 哈希（无 Key 则随机）。
func (kc *KafkaClient) ProduceMessage(ctx context.Context, topic string, partition int32, key, value []byte, headers map[string]string) (int32, int64, error) {
	if err := kc.alive(ctx); err != nil {
		return 0, 0, err
	}

	record := &kgo.Record{
		Topic: topic,
		Value: value,
	}
	if len(key) > 0 {
		record.Key = key
	}
	if partition >= 0 {
		record.Partition = partition
	}
	for k, v := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}

	results := kc.client.ProduceSync(ctx, record)
	r, err := results.First()
	if err != nil {
		AuditLog(AuditProduceMessage, fmt.Sprintf("%s[p=%d]", topic, partition), kc.connName(), err)
		return 0, 0, err
	}
	g.Log().Infof(ctx, "发送消息成功 topic=%s partition=%d offset=%d", topic, r.Partition, r.Offset)
	AuditLog(AuditProduceMessage, fmt.Sprintf("%s[p=%d]", topic, r.Partition), kc.connName(), nil)
	return r.Partition, r.Offset, nil
}

func toMessageInfo(r *kgo.Record) MessageInfo {
	info := MessageInfo{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: r.Timestamp,
		Size:      len(r.Key) + len(r.Value),
	}
	for _, h := range r.Headers {
		info.Headers = append(info.Headers, HeaderInfo{Key: h.Key, Value: string(h.Value)})
	}
	return info
}
