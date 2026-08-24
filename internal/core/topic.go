package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/IBM/sarama"
	"github.com/gogf/gf/v2/frame/g"
)

// PartitionInfo 分区详细信息
type PartitionInfo struct {
	ID           int32
	Leader       int32
	Replicas     []int32
	ISR          []int32
	Offline      []int32
	StartOffset  int64
	EndOffset    int64
	MessageCount int64
}

// TopicInfo Topic 详情
type TopicInfo struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	Configs           map[string]string
	Partitions        []PartitionInfo
}

// ListTopicNames 返回全部用户 Topic 名称（过滤内部 topic）。
func (kc *KafkaClient) ListTopicNames(ctx context.Context) ([]string, error) {
	details, err := kc.admin.ListTopics()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(details))
	for name := range details {
		if strings.HasPrefix(name, "__") {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// TopicDetail 返回 Topic 的详细信息（含各分区的 Leader/ISR/Offset 等）。
func (kc *KafkaClient) TopicDetail(ctx context.Context, name string) (*TopicInfo, error) {
	metas, err := kc.admin.DescribeTopics([]string{name})
	if err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, fmt.Errorf("topic %s 未找到", name)
	}
	meta := metas[0]
	if meta.Err != sarama.ErrNoError {
		return nil, meta.Err
	}

	info := &TopicInfo{Name: meta.Name}
	if len(meta.Partitions) > 0 {
		info.ReplicationFactor = int16(len(meta.Partitions[0].Replicas))
	}
	info.NumPartitions = int32(len(meta.Partitions))

	for _, p := range meta.Partitions {
		pi := PartitionInfo{
			ID:       p.ID,
			Leader:   p.Leader,
			Replicas: p.Replicas,
			ISR:      p.Isr,
			Offline:  p.OfflineReplicas,
		}
		if start, err := kc.client.GetOffset(name, p.ID, sarama.OffsetOldest); err == nil {
			pi.StartOffset = start
		}
		if end, err := kc.client.GetOffset(name, p.ID, sarama.OffsetNewest); err == nil {
			pi.EndOffset = end
		}
		if pi.StartOffset >= 0 && pi.EndOffset >= pi.StartOffset {
			pi.MessageCount = pi.EndOffset - pi.StartOffset
		}
		info.Partitions = append(info.Partitions, pi)
	}
	sort.Slice(info.Partitions, func(i, j int) bool { return info.Partitions[i].ID < info.Partitions[j].ID })

	if cfg, err := kc.admin.DescribeConfig(sarama.ConfigResource{Type: sarama.TopicResource, Name: name}); err == nil {
		m := make(map[string]string, len(cfg))
		for _, e := range cfg {
			if !e.Sensitive {
				m[e.Name] = e.Value
			} else {
				m[e.Name] = "(sensitive)"
			}
		}
		info.Configs = m
	}
	return info, nil
}

// CreateTopic 创建一个 Topic。
func (kc *KafkaClient) CreateTopic(ctx context.Context, name string, partitions int32, replication int16, configs map[string]string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("Topic 名称不能为空")
	}
	detail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replication,
	}
	if len(configs) > 0 {
		detail.ConfigEntries = make(map[string]*string, len(configs))
		for k, v := range configs {
			vv := v
			detail.ConfigEntries[k] = &vv
		}
	}
	if err := kc.admin.CreateTopic(name, detail, false); err != nil {
		return err
	}
	g.Log().Infof(ctx, "创建 Topic: %s (partitions=%d, replication=%d)", name, partitions, replication)
	return nil
}

// DeleteTopic 删除一个 Topic。
func (kc *KafkaClient) DeleteTopic(ctx context.Context, name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("Topic 名称不能为空")
	}
	if err := kc.admin.DeleteTopic(name); err != nil {
		return err
	}
	g.Log().Infof(ctx, "删除 Topic: %s", name)
	return nil
}

// PartitionCount 快速获取 Topic 的分区数。
func (kc *KafkaClient) PartitionCount(ctx context.Context, name string) (int, error) {
	metas, err := kc.admin.DescribeTopics([]string{name})
	if err != nil {
		return 0, err
	}
	if len(metas) == 0 || metas[0].Err != sarama.ErrNoError {
		return 0, fmt.Errorf("topic %s 未找到", name)
	}
	return len(metas[0].Partitions), nil
}