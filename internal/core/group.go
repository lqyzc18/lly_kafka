package core

import (
	"context"
	"sort"

	"github.com/IBM/sarama"
)

// GroupInfo 消费组概要信息
type GroupInfo struct {
	Name         string
	State        string
	ProtocolType string
	Members      int
}

// GroupTopicOffset 消费组在某个 Topic 分区上的消费进度与 lag。
//   - GroupOffset: 消费组已提交偏移
//   - LogEndOffset: 该分区最新 offset（HighWaterMark）
//   - Lag: 滞后量 = LogEndOffset - GroupOffset（<0 表示无数据）
type GroupTopicOffset struct {
	Topic        string
	Partition    int32
	GroupOffset  int64
	LogEndOffset int64
	Lag          int64
}

// ListGroups 返回全部消费组信息。
func (kc *KafkaClient) ListGroups(ctx context.Context) ([]GroupInfo, error) {
	summaries, err := kc.admin.ListConsumerGroups()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(summaries))
	for name := range summaries {
		names = append(names, name)
	}
	if len(names) == 0 {
		return []GroupInfo{}, nil
	}
	descs, err := kc.admin.DescribeConsumerGroups(names)
	if err != nil {
		return nil, err
	}
	out := make([]GroupInfo, 0, len(descs))
	for _, d := range descs {
		out = append(out, GroupInfo{
			Name:         d.GroupId,
			State:        d.State,
			ProtocolType: d.ProtocolType,
			Members:      len(d.Members),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// GroupOffsets 返回消费组在所有 Topic/分区上的消费进度与 lag。
// 内部使用 admin.ListConsumerGroupOffsets 的「全部 topic」模式（topics=nil）。
func (kc *KafkaClient) GroupOffsets(ctx context.Context, group string) ([]GroupTopicOffset, error) {
	resp, err := kc.admin.ListConsumerGroupOffsets(group, nil)
	if err != nil {
		return nil, err
	}
	var out []GroupTopicOffset
	for topic, parts := range resp.Blocks {
		for pid, block := range parts {
			if block == nil {
				continue
			}
			item := GroupTopicOffset{
				Topic:       topic,
				Partition:   pid,
				GroupOffset: block.Offset,
			}
			if end, eerr := kc.client.GetOffset(topic, pid, sarama.OffsetNewest); eerr == nil {
				item.LogEndOffset = end
				if end >= block.Offset {
					item.Lag = end - block.Offset
				}
			} else {
				item.LogEndOffset = -1
				item.Lag = -1
			}
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out, nil
}