package core

import "github.com/twmb/franz-go/pkg/kgo"

// optionalPartitioner honors an explicit partition while retaining Kafka's
// keyed/sticky partitioning for records marked with Partition < 0.
type optionalPartitioner struct {
	auto kgo.Partitioner
}

func newOptionalPartitioner() kgo.Partitioner {
	return optionalPartitioner{auto: kgo.StickyKeyPartitioner(nil)}
}

func (p optionalPartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	return optionalTopicPartitioner{auto: p.auto.ForTopic(topic)}
}

type optionalTopicPartitioner struct {
	auto kgo.TopicPartitioner
}

func (p optionalTopicPartitioner) RequiresConsistency(r *kgo.Record) bool {
	return r.Partition >= 0 || p.auto.RequiresConsistency(r)
}

func (p optionalTopicPartitioner) Partition(r *kgo.Record, n int) int {
	if r.Partition >= 0 {
		return int(r.Partition)
	}
	return p.auto.Partition(r, n)
}

func (p optionalTopicPartitioner) OnNewBatch() {
	if batch, ok := p.auto.(kgo.TopicPartitionerOnNewBatch); ok {
		batch.OnNewBatch()
	}
}
