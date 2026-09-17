package core

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestOptionalPartitioner(t *testing.T) {
	p := newOptionalPartitioner().ForTopic("test")
	manual := &kgo.Record{Partition: 2, Key: []byte("key")}
	if !p.RequiresConsistency(manual) || p.Partition(manual, 4) != 2 {
		t.Fatal("指定分区应优先于 Key 分配")
	}

	auto := &kgo.Record{Partition: -1, Key: []byte("key")}
	first := p.Partition(auto, 4)
	if first < 0 || first >= 4 || !p.RequiresConsistency(auto) {
		t.Fatalf("自动分区按 Key 选择的分区无效: %d", first)
	}
	if next := p.Partition(auto, 4); next != first {
		t.Fatalf("同一 Key 的分区不稳定: %d, %d", first, next)
	}

	noKey := &kgo.Record{Partition: -1}
	if got := p.Partition(noKey, 4); got < 0 || got >= 4 {
		t.Fatalf("无 Key 的自动分区无效: %d", got)
	}
	if batch, ok := p.(kgo.TopicPartitionerOnNewBatch); ok {
		batch.OnNewBatch()
	} else {
		t.Fatal("自动分区器应接收批次更新")
	}
}
