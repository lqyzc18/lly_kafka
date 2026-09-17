package core

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// 断言区间切片与期望一致。
func assertRanges(t *testing.T, got, want []partitionRange) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("区间数量 = %d, 期望 %d (%#v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 个区间 = %#v, 期望 %#v", i, got[i], want[i])
		}
	}
}

func TestLatestRanges(t *testing.T) {
	bounds := map[int32][2]int64{
		0: {0, 100},
		1: {80, 100},
		2: {50, 50}, // 空分区
	}
	assertRanges(t, latestRanges(bounds, 10, 10), []partitionRange{
		{Partition: 0, From: 90, ToEx: 100},
		{Partition: 1, From: 90, ToEx: 100},
		{Partition: 2, From: 50, ToEx: 50},
	})

	// 分区数据少于请求条数时，应夹到 oldest。
	assertRanges(t, latestRanges(map[int32][2]int64{0: {7, 10}}, 50, 50), []partitionRange{
		{Partition: 0, From: 7, ToEx: 10},
	})
}

func TestOffsetRanges(t *testing.T) {
	bounds := map[int32][2]int64{
		0: {0, 100},
		1: {10, 30}, // 起始 offset 高于请求上限，区间为空
		2: {0, 5},
	}
	assertRanges(t, offsetRanges(bounds, 0, 2, 500), []partitionRange{
		{Partition: 0, From: 0, ToEx: 3},
		{Partition: 1, From: 10, ToEx: 10},
		{Partition: 2, From: 0, ToEx: 3},
	})

	// 每个分区最多取 per 条。
	assertRanges(t, offsetRanges(bounds, 0, 0, 2), []partitionRange{
		{Partition: 0, From: 0, ToEx: 2},
		{Partition: 1, From: 10, ToEx: 12},
		{Partition: 2, From: 0, ToEx: 2},
	})
}

func TestTimeRanges(t *testing.T) {
	bounds := map[int32][2]int64{
		0: {100, 200},
		1: {5, 50},
		2: {0, 10},
	}
	starts := map[int32]int64{
		0: 150,
		1: -1, // 晚于最新消息，区间为空
		// 2 缺失仅用于未指定起始时间
	}
	assertRanges(t, timeRanges(bounds, starts, 100), []partitionRange{
		{Partition: 0, From: 150, ToEx: 200},
		{Partition: 1, From: 50, ToEx: 50},
		{Partition: 2, From: 0, ToEx: 10},
	})

	// 定位到分区末尾时区间为空。
	assertRanges(t, timeRanges(map[int32][2]int64{0: {0, 100}}, map[int32]int64{0: 100}, 50), []partitionRange{
		{Partition: 0, From: 100, ToEx: 100},
	})
}

func TestPerPartitionLimit(t *testing.T) {
	cases := []struct {
		max, partitions, want int
	}{
		{500, 4, 125},
		{10, 4, 2},
		{3, 5, 1},
		{5, 5, 1},
		{500, 0, 500},
	}
	for _, c := range cases {
		if got := perPartitionLimit(c.max, c.partitions); got != c.want {
			t.Fatalf("perPartitionLimit(%d,%d)=%d, 期望 %d", c.max, c.partitions, got, c.want)
		}
	}
}

// 聚合错误必须能让 errors.Is 识别取消/超时，否则「全部分区」被取消时会误报为拉取失败。
func TestPartitionErrorsUnwrap(t *testing.T) {
	es := PartitionErrors{Total: 2, Failures: []PartitionError{
		{Partition: 0, Err: errors.New("boom")},
		{Partition: 1, Err: context.Canceled},
	}}
	if !errors.Is(es, context.Canceled) {
		t.Fatal("errors.Is 应能穿透 PartitionErrors 识别 context.Canceled")
	}
	if errors.Is(es, context.DeadlineExceeded) {
		t.Fatal("不应误判为超时")
	}

	msg := es.Error()
	for _, want := range []string{"部分分区拉取失败 (2/2)", "分区 0: boom", "分区 1: context canceled"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("Error() = %q, 应包含 %q", msg, want)
		}
	}

	if (PartitionErrors{}).Unwrap() == nil {
		t.Fatal("空聚合错误的 Unwrap 应返回空切片而非 nil")
	}
}

func TestPartitionErrorsMerge(t *testing.T) {
	a := PartitionErrors{Total: 2, Failures: []PartitionError{{Partition: 3, Err: errors.New("x")}}}
	b := PartitionErrors{Total: 4, Failures: []PartitionError{
		{Partition: 1, Err: errors.New("y")},
		{Partition: 2, Err: errors.New("z")},
	}}
	a.merge(b)
	if a.Total != 4 {
		t.Fatalf("Total = %d, 期望 4", a.Total)
	}
	got := []int32{a.Failures[0].Partition, a.Failures[1].Partition, a.Failures[2].Partition}
	want := []int32{1, 2, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("失败分区顺序 = %v, 期望 %v", got, want)
		}
	}

	// 合并空集合不应改变状态。
	a.merge(PartitionErrors{})
	if a.Total != 4 || len(a.Failures) != 3 {
		t.Fatalf("合并空集合后状态被改变: %#v", a)
	}
}

func TestMessageSorters(t *testing.T) {
	base := time.Date(2026, 9, 16, 10, 0, 0, 0, time.Local)
	msgs := []MessageInfo{
		{Partition: 2, Offset: 5, Timestamp: base.Add(3 * time.Second)},
		{Partition: 0, Offset: 9, Timestamp: base.Add(3 * time.Second)},
		{Partition: 1, Offset: 1, Timestamp: base.Add(time.Second)},
		{Partition: 1, Offset: 7, Timestamp: base.Add(3 * time.Second)},
	}

	byRecency := append([]MessageInfo(nil), msgs...)
	sort.Slice(byRecency, func(i, j int) bool { return sortByRecency(byRecency[i], byRecency[j]) })
	wantRecency := []struct {
		partition int32
		offset    int64
	}{{0, 9}, {1, 7}, {2, 5}, {1, 1}}
	for i, w := range wantRecency {
		if byRecency[i].Partition != w.partition || byRecency[i].Offset != w.offset {
			t.Fatalf("按时间排序第 %d 项 = p%d/off%d, 期望 p%d/off%d",
				i, byRecency[i].Partition, byRecency[i].Offset, w.partition, w.offset)
		}
	}

	byOffset := append([]MessageInfo(nil), msgs...)
	sort.Slice(byOffset, func(i, j int) bool { return sortByOffset(byOffset[i], byOffset[j]) })
	wantOffset := []struct {
		partition int32
		offset    int64
	}{{0, 9}, {1, 1}, {1, 7}, {2, 5}}
	for i, w := range wantOffset {
		if byOffset[i].Partition != w.partition || byOffset[i].Offset != w.offset {
			t.Fatalf("按 offset 排序第 %d 项 = p%d/off%d, 期望 p%d/off%d",
				i, byOffset[i].Partition, byOffset[i].Offset, w.partition, w.offset)
		}
	}
}

func TestFinishAllPartitions(t *testing.T) {
	all := []MessageInfo{{Partition: 0}, {Partition: 1}, {Partition: 2}}
	out, err := finishAllPartitions(append([]MessageInfo(nil), all...), PartitionErrors{}, 2, sortByOffset)
	if err != nil {
		t.Fatalf("无失败分区时不应返回错误: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("截断后条数 = %d, 期望 2", len(out))
	}

	partErrs := PartitionErrors{Total: 3, Failures: []PartitionError{{Partition: 2, Err: errors.New("boom")}}}
	out, err = finishAllPartitions(append([]MessageInfo(nil), all...), partErrs, 500, sortByOffset)
	if err == nil {
		t.Fatal("存在失败分区时应返回错误")
	}
	if len(out) != 3 {
		t.Fatalf("未超限时不应截断, 实际 = %d", len(out))
	}
	var got PartitionErrors
	if !errors.As(err, &got) {
		t.Fatalf("错误类型应为 PartitionErrors, 实际 %T", err)
	}
	if len(got.Failures) != 1 || got.Failures[0].Partition != 2 {
		t.Fatalf("失败明细 = %#v", got.Failures)
	}
}

// 分区全部失败时应返回可被 UI 识别的明细，而不是静默返回空结果。
func TestEmptyPartitionErrors(t *testing.T) {
	if err := emptyPartitionErrors(PartitionErrors{}); err != nil {
		t.Fatalf("无失败分区时应返回 nil, 实际 %v", err)
	}
	partErrs := PartitionErrors{Total: 1, Failures: []PartitionError{{Partition: 0, Err: errors.New("no leader")}}}
	err := emptyPartitionErrors(partErrs)
	if err == nil {
		t.Fatal("有失败分区时应返回错误")
	}
	var got PartitionErrors
	if !errors.As(err, &got) || len(got.Failures) != 1 {
		t.Fatalf("应返回 PartitionErrors, 实际 %#v", err)
	}
}

func TestFirstPartitionErr(t *testing.T) {
	if err := firstPartitionErr(PartitionErrors{}); err != nil {
		t.Fatalf("无失败分区时应返回 nil, 实际 %v", err)
	}
	sentinel := errors.New("boom")
	err := firstPartitionErr(PartitionErrors{Failures: []PartitionError{{Partition: 0, Err: sentinel}}})
	if !errors.Is(err, sentinel) {
		t.Fatalf("应还原原始错误, 实际 %v", err)
	}
}

func TestAppendRangeRecordsHonorsOffsets(t *testing.T) {
	records := []*kgo.Record{
		{Topic: "t", Partition: 0, Offset: 2},
		{Topic: "t", Partition: 0, Offset: 6},
		{Topic: "t", Partition: 0, Offset: 12},
	}
	got, done := appendRangeRecords(nil, records, 4, 10)
	if !done || len(got) != 1 || got[0].Offset != 6 {
		t.Fatalf("压缩后的区间 [4,10) 应只保留 offset 6, got %#v done=%v", got, done)
	}
	got, done = appendRangeRecords(nil, records[:2], 4, 10)
	if done || len(got) != 1 {
		t.Fatalf("未到区间末尾时仍需继续拉取, got %#v done=%v", got, done)
	}
	if _, done = appendRangeRecords(got, nil, 4, 10); !done {
		t.Fatal("空分区响应应结束该分区")
	}
}
