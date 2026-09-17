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

const (
	// defaultFetchTimeout 单分区拉取未显式设置 deadline 时的兜底超时。
	defaultFetchTimeout = 8 * time.Second
	// allPartitionTimeout 全分区拉取的兜底超时（分区多、数据量大时需要更久）。
	allPartitionTimeout = 30 * time.Second
	// tempFetchMaxWait 临时 consumer 单次 PollFetches 的最长等待，避免长时间阻塞。
	tempFetchMaxWait = 500 * time.Millisecond
)

// PartitionError 记录单个分区拉取失败的信息。
type PartitionError struct {
	Partition int32
	Err       error
}

func (e PartitionError) Error() string {
	return fmt.Sprintf("分区 %d: %v", e.Partition, e.Err)
}

// Unwrap 暴露底层错误，使 errors.Is/As 能识别超时、取消等语义。
func (e PartitionError) Unwrap() error {
	return e.Err
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

// Unwrap 展开全部子错误，让 errors.Is(err, context.Canceled) 等判断可穿透聚合错误。
func (es PartitionErrors) Unwrap() []error {
	out := make([]error, 0, len(es.Failures))
	for _, e := range es.Failures {
		if e.Err != nil {
			out = append(out, e.Err)
		}
	}
	return out
}

// merge 追加另一组分区失败明细，用于合并批量边界与批量拉取的错误。
func (es *PartitionErrors) merge(other PartitionErrors) {
	if len(other.Failures) == 0 {
		return
	}
	if es.Total < other.Total {
		es.Total = other.Total
	}
	es.Failures = append(es.Failures, other.Failures...)
	sort.Slice(es.Failures, func(i, j int) bool {
		return es.Failures[i].Partition < es.Failures[j].Partition
	})
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

// partitionRange 单个分区待拉取的半开区间 [From, ToEx)。
type partitionRange struct {
	Partition int32
	From      int64
	ToEx      int64
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
//
// 直接使用 Kafka 原生的 ListOffsets(timestamp) 能力，一次 RPC 完成定位；
// 若指定时间早于最旧数据，broker 返回 log start offset；若晚于最新数据，
// 返回当前 log end offset（此时区间为空）。
func (kc *KafkaClient) GetOffsetByTime(ctx context.Context, topic string, partition int32, t time.Time) (int64, error) {
	if err := kc.alive(ctx); err != nil {
		return 0, err
	}
	if t.IsZero() {
		return kc.OldestOffset(ctx, topic, partition)
	}
	listed, err := kc.admin.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
	if err != nil {
		return 0, err
	}
	lo, ok := listed.Lookup(topic, partition)
	if !ok {
		return 0, fmt.Errorf("按时间定位分区 %d offset 失败", partition)
	}
	if lo.Err != nil {
		return 0, lo.Err
	}
	if lo.Offset < 0 {
		// broker 未返回可用 offset，回落到该分区最新位置。
		return kc.NewestOffset(ctx, topic, partition)
	}
	return lo.Offset, nil
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

// partitionBounds 一次性获取 Topic 各分区的 [oldest, hwm) 边界。
// 单个分区失败只记录到 PartitionErrors，不影响其余分区。
func (kc *KafkaClient) partitionBounds(ctx context.Context, topic string) (map[int32][2]int64, PartitionErrors, error) {
	if err := kc.alive(ctx); err != nil {
		return nil, PartitionErrors{}, err
	}
	starts, err := kc.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, PartitionErrors{}, err
	}
	ends, err := kc.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, PartitionErrors{}, err
	}

	ids := make(map[int32]struct{}, len(starts[topic])+len(ends[topic]))
	for pid := range starts[topic] {
		ids[pid] = struct{}{}
	}
	for pid := range ends[topic] {
		ids[pid] = struct{}{}
	}

	bounds := make(map[int32][2]int64, len(ids))
	partErrs := PartitionErrors{Total: len(ids)}
	for pid := range ids {
		so, sok := starts.Lookup(topic, pid)
		if !sok || so.Err != nil {
			partErrs.Failures = append(partErrs.Failures, PartitionError{Partition: pid, Err: offsetLookupErr(so.Err, "起始")})
			continue
		}
		eo, eok := ends.Lookup(topic, pid)
		if !eok || eo.Err != nil {
			partErrs.Failures = append(partErrs.Failures, PartitionError{Partition: pid, Err: offsetLookupErr(eo.Err, "最新")})
			continue
		}
		bounds[pid] = [2]int64{so.Offset, eo.Offset}
	}
	partErrs.sortFailures()
	return bounds, partErrs, nil
}

func (es *PartitionErrors) sortFailures() {
	sort.Slice(es.Failures, func(i, j int) bool {
		return es.Failures[i].Partition < es.Failures[j].Partition
	})
}

func offsetLookupErr(err error, kind string) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("获取%s offset 失败", kind)
}

// latestRanges 为每个分区计算「最新 n 条」的规范化区间。
func latestRanges(bounds map[int32][2]int64, n, max int) []partitionRange {
	ranges := make([]partitionRange, 0, len(bounds))
	for pid, b := range bounds {
		from, toEx := normalizeFetchRange(b[0], b[1], latestStart(b[0], b[1], int64(n)), 0, max)
		ranges = append(ranges, partitionRange{Partition: pid, From: from, ToEx: toEx})
	}
	sortRanges(ranges)
	return ranges
}

// offsetRanges 为每个分区应用同一 offset 范围。
func offsetRanges(bounds map[int32][2]int64, start, end int64, max int) []partitionRange {
	ranges := make([]partitionRange, 0, len(bounds))
	for pid, b := range bounds {
		from, toEx := normalizeFetchRange(b[0], b[1], start, end, max)
		ranges = append(ranges, partitionRange{Partition: pid, From: from, ToEx: toEx})
	}
	sortRanges(ranges)
	return ranges
}

// timeRanges 为每个分区应用按时间定位得到的起始 offset。
// starts 缺失仅用于未指定时间的场景；负值表示时间晚于最新消息。
func timeRanges(bounds map[int32][2]int64, starts map[int32]int64, max int) []partitionRange {
	ranges := make([]partitionRange, 0, len(bounds))
	for pid, b := range bounds {
		start := b[0]
		if s, ok := starts[pid]; ok {
			if s < 0 {
				start = b[1]
			} else {
				start = s
			}
		}
		from, toEx := normalizeFetchRange(b[0], b[1], start, 0, max)
		ranges = append(ranges, partitionRange{Partition: pid, From: from, ToEx: toEx})
	}
	sortRanges(ranges)
	return ranges
}

func sortRanges(ranges []partitionRange) {
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Partition < ranges[j].Partition })
}

// perPartitionLimit 计算全分区拉取时每个分区的条数上限（至少 1 条）。
func perPartitionLimit(max, partitions int) int {
	if partitions <= 0 {
		return max
	}
	per := max / partitions
	if per < 1 {
		per = 1
	}
	return per
}

// FetchLatest 拉取指定分区最新 n 条消息（不超过 max，且不早于 OldestOffset）。
func (kc *KafkaClient) FetchLatest(ctx context.Context, topic string, partition int32, n, max int) ([]MessageInfo, error) {
	if ctx == nil {
		ctx = kc.Context()
	}
	n = latestCount(n, max)
	oldest, hwm, err := kc.oldestAndNewest(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	from, toEx := normalizeFetchRange(oldest, hwm, latestStart(oldest, hwm, int64(n)), 0, max)
	msgs, partErrs, err := kc.fetchMany(ctx, topic, []partitionRange{{Partition: partition, From: from, ToEx: toEx}})
	if err != nil {
		return msgs, err
	}
	return msgs, firstPartitionErr(partErrs)
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
// 时间定位与消息拉取各只发起一次批量请求。
func (kc *KafkaClient) FetchByTimeAll(ctx context.Context, topic string, startTime time.Time, max int) ([]MessageInfo, error) {
	if ctx == nil {
		ctx = kc.Context()
	}
	if max <= 0 {
		max = defaultFetchMax
	}
	ctx, cancel := context.WithTimeout(ctx, allPartitionTimeout)
	defer cancel()

	bounds, partErrs, err := kc.partitionBounds(ctx, topic)
	if err != nil {
		return nil, err
	}
	if len(bounds) == 0 {
		return []MessageInfo{}, emptyPartitionErrors(partErrs)
	}

	starts := make(map[int32]int64, len(bounds))
	if !startTime.IsZero() {
		listed, err := kc.admin.ListOffsetsAfterMilli(ctx, startTime.UnixMilli(), topic)
		if err != nil {
			return nil, err
		}
		for pid := range bounds {
			lo, ok := listed.Lookup(topic, pid)
			if !ok || lo.Err != nil {
				partErrs.Failures = append(partErrs.Failures, PartitionError{Partition: pid, Err: offsetLookupErr(lo.Err, "按时间定位")})
				delete(bounds, pid)
				continue
			}
			starts[pid] = lo.Offset
		}
		partErrs.sortFailures()
	}
	if len(bounds) == 0 {
		return []MessageInfo{}, emptyPartitionErrors(partErrs)
	}

	per := perPartitionLimit(max, len(bounds))
	all, fetchErrs, err := kc.fetchMany(ctx, topic, timeRanges(bounds, starts, per))
	if err != nil {
		return all, err
	}
	partErrs.merge(fetchErrs)
	return finishAllPartitions(all, partErrs, max, sortByRecency)
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
	from, toEx := normalizeFetchRange(oldest, hwm, start, end, max)
	msgs, partErrs, err := kc.fetchMany(ctx, topic, []partitionRange{{Partition: partition, From: from, ToEx: toEx}})
	if err != nil {
		return msgs, err
	}
	return msgs, firstPartitionErr(partErrs)
}

// FetchLatestAll 拉取该 Topic 全部分区各自的最新 n 条，再按时间倒序取前 max 条。
func (kc *KafkaClient) FetchLatestAll(ctx context.Context, topic string, n, max int) ([]MessageInfo, error) {
	if ctx == nil {
		ctx = kc.Context()
	}
	if max <= 0 {
		max = defaultFetchMax
	}
	n = latestCount(n, max)
	ctx, cancel := context.WithTimeout(ctx, allPartitionTimeout)
	defer cancel()

	bounds, partErrs, err := kc.partitionBounds(ctx, topic)
	if err != nil {
		return nil, err
	}
	if len(bounds) == 0 {
		return []MessageInfo{}, emptyPartitionErrors(partErrs)
	}

	all, fetchErrs, err := kc.fetchMany(ctx, topic, latestRanges(bounds, n, n))
	if err != nil {
		return all, err
	}
	partErrs.merge(fetchErrs)
	return finishAllPartitions(all, partErrs, max, sortByRecency)
}

// FetchMessagesAll 对全部分区应用同一 offset 范围，合计不超过 max。
func (kc *KafkaClient) FetchMessagesAll(ctx context.Context, topic string, start, end int64, max int) ([]MessageInfo, error) {
	if ctx == nil {
		ctx = kc.Context()
	}
	if max <= 0 {
		max = defaultFetchMax
	}
	ctx, cancel := context.WithTimeout(ctx, allPartitionTimeout)
	defer cancel()

	bounds, partErrs, err := kc.partitionBounds(ctx, topic)
	if err != nil {
		return nil, err
	}
	if len(bounds) == 0 {
		return []MessageInfo{}, emptyPartitionErrors(partErrs)
	}

	per := perPartitionLimit(max, len(bounds))
	all, fetchErrs, err := kc.fetchMany(ctx, topic, offsetRanges(bounds, start, end, per))
	if err != nil {
		return all, err
	}
	partErrs.merge(fetchErrs)
	return finishAllPartitions(all, partErrs, max, sortByOffset)
}

// finishAllPartitions 排序、截断并返回分区失败明细。
func finishAllPartitions(all []MessageInfo, partErrs PartitionErrors, max int, less func(a, b MessageInfo) bool) ([]MessageInfo, error) {
	sort.Slice(all, func(i, j int) bool { return less(all[i], all[j]) })
	if max > 0 && len(all) > max {
		all = all[:max]
	}
	if len(partErrs.Failures) > 0 {
		return all, partErrs
	}
	return all, nil
}

// emptyPartitionErrors 在无任何分区数据时，仍返回已有的分区失败明细。
func emptyPartitionErrors(partErrs PartitionErrors) error {
	if len(partErrs.Failures) > 0 {
		return partErrs
	}
	return nil
}

// firstPartitionErr 将单分区拉取的分区错误还原为普通错误。
func firstPartitionErr(partErrs PartitionErrors) error {
	if len(partErrs.Failures) == 0 {
		return nil
	}
	return partErrs.Failures[0].Err
}

// sortByRecency 按时间倒序；同一时间按分区升序、offset 降序。
func sortByRecency(a, b MessageInfo) bool {
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.After(b.Timestamp)
	}
	if a.Partition != b.Partition {
		return a.Partition < b.Partition
	}
	return a.Offset > b.Offset
}

// sortByOffset 按分区升序、分区内 offset 升序。
func sortByOffset(a, b MessageInfo) bool {
	if a.Partition != b.Partition {
		return a.Partition < b.Partition
	}
	return a.Offset < b.Offset
}

// appendRangeRecords uses record offsets rather than record counts: compacted
// partitions can have holes within the requested interval.
func appendRangeRecords(dst []MessageInfo, records []*kgo.Record, from, toEx int64) ([]MessageInfo, bool) {
	if len(records) == 0 {
		return dst, true
	}
	for _, r := range records {
		if r.Offset >= toEx {
			return dst, true
		}
		if r.Offset >= from {
			dst = append(dst, toMessageInfo(r))
		}
	}
	return dst, records[len(records)-1].Offset >= toEx-1
}

// fetchMany 用单个 consumer 同时拉取多个分区的目标区间。
//
// 相比「每个分区各建一个 consumer」，这里只做一次连接与元数据握手。
// 返回值：
//   - err 为全局错误（客户端已关闭、临时 consumer 创建失败、context 取消/超时）
//   - PartitionErrors 记录 broker 针对具体分区返回的错误
func (kc *KafkaClient) fetchMany(ctx context.Context, topic string, ranges []partitionRange) ([]MessageInfo, PartitionErrors, error) {
	offsets := make(map[int32]kgo.Offset, len(ranges))
	bounds := make(map[int32][2]int64, len(ranges))
	var expect int64
	for _, r := range ranges {
		if r.From >= r.ToEx {
			continue
		}
		offsets[r.Partition] = kgo.NewOffset().At(r.From)
		bounds[r.Partition] = [2]int64{r.From, r.ToEx}
		expect += r.ToEx - r.From
	}
	if len(offsets) == 0 {
		return []MessageInfo{}, PartitionErrors{}, nil
	}

	if err := kc.beginFetch(ctx); err != nil {
		return nil, PartitionErrors{}, err
	}
	defer kc.fetches.Done()

	opts := []kgo.Opt{
		kgo.SeedBrokers(parseBrokers(kc.conn.Brokers)...),
		kgo.ClientID(kc.conn.ClientID),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}),
		kgo.FetchMaxWait(tempFetchMaxWait),
	}
	if err := applySecurity(&opts, kc.conn); err != nil {
		return nil, PartitionErrors{}, err
	}
	tempClient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, PartitionErrors{}, fmt.Errorf("创建临时 consumer 失败: %w", err)
	}
	defer tempClient.Close()

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultFetchTimeout)
		defer cancel()
	}

	var partErrs PartitionErrors
	msgs := make([]MessageInfo, 0, expect)
	for len(bounds) > 0 {
		fetches := tempClient.PollFetches(ctx)
		// 取消或超时优先按全局错误上报，让 UI 显示「已取消 / 超时」而非分区失败。
		if err := ctx.Err(); err != nil {
			return msgs, partErrs, fmt.Errorf("拉取未完成: %w", err)
		}
		completed := make([]int32, 0)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if p.Topic != topic || p.Err != nil {
				return
			}
			b, tracked := bounds[p.Partition]
			if !tracked {
				return
			}
			var done bool
			msgs, done = appendRangeRecords(msgs, p.Records, b[0], b[1])
			if done {
				delete(bounds, p.Partition)
				completed = append(completed, p.Partition)
			}
		})
		if len(completed) > 0 {
			tempClient.PauseFetchPartitions(map[string][]int32{topic: completed})
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				partErrs.Failures = append(partErrs.Failures, PartitionError{
					Partition: fe.Partition,
					Err:       fmt.Errorf("拉取出错: %w", fe.Err),
				})
			}
			partErrs.Total = len(offsets)
			partErrs.sortFailures()
			return msgs, partErrs, nil
		}
	}
	return msgs, partErrs, nil
}

// ProduceMessage 同步发送一条消息。partition < 0 时按 Key 哈希（无 Key 则随机）。
func (kc *KafkaClient) ProduceMessage(ctx context.Context, topic string, partition int32, key, value []byte, headers map[string]string) (int32, int64, error) {
	if err := kc.alive(ctx); err != nil {
		return 0, 0, err
	}

	record := &kgo.Record{
		Topic:     topic,
		Value:     value,
		Partition: partition,
	}
	if len(key) > 0 {
		record.Key = key
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

// toMessageInfo 将 franz-go Record 转为内部 MessageInfo 结构。
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
