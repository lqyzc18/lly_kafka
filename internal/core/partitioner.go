package core

import (
	"fmt"

	"github.com/IBM/sarama"
)

// manualPartitionMeta 标记 ProducerMessage 使用用户指定的分区，
// 否则走 HashPartitioner（无 Key 时随机）。
const manualPartitionMeta = "manual"

// flexiblePartitioner 在「指定分区」和「按 Key 哈希」之间切换。
// sarama 默认 HashPartitioner 会忽略 ProducerMessage.Partition。
type flexiblePartitioner struct {
	hash sarama.Partitioner
}

func (p *flexiblePartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if msg.Metadata == manualPartitionMeta {
		if msg.Partition < 0 || msg.Partition >= numPartitions {
			return 0, fmt.Errorf("分区 %d 超出范围 [0, %d)", msg.Partition, numPartitions)
		}
		return msg.Partition, nil
	}
	return p.hash.Partition(msg, numPartitions)
}

func (p *flexiblePartitioner) RequiresConsistency() bool {
	return p.hash.RequiresConsistency()
}
