package core

import (
	"testing"

	"github.com/IBM/sarama"
)

type stubHash struct{ n int32 }

func (s stubHash) Partition(*sarama.ProducerMessage, int32) (int32, error) {
	return s.n, nil
}

func (s stubHash) RequiresConsistency() bool { return true }

func TestFlexiblePartitionerManual(t *testing.T) {
	p := &flexiblePartitioner{hash: stubHash{n: 7}}
	got, err := p.Partition(&sarama.ProducerMessage{Partition: 3, Metadata: manualPartitionMeta}, 8)
	if err != nil || got != 3 {
		t.Fatalf("manual got %d err=%v", got, err)
	}
}

func TestFlexiblePartitionerHashFallback(t *testing.T) {
	p := &flexiblePartitioner{hash: stubHash{n: 7}}
	got, err := p.Partition(&sarama.ProducerMessage{}, 8)
	if err != nil || got != 7 {
		t.Fatalf("hash fallback got %d err=%v", got, err)
	}
}

func TestFlexiblePartitionerOutOfRange(t *testing.T) {
	p := &flexiblePartitioner{hash: stubHash{n: 0}}
	_, err := p.Partition(&sarama.ProducerMessage{Partition: 9, Metadata: manualPartitionMeta}, 8)
	if err == nil {
		t.Fatal("expected out of range error")
	}
}
