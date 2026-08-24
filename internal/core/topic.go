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

// ListTopicNames 返回用户 Topic 名称（带缓存）。
func (kc *KafkaClient) ListTopicNames(ctx context.Context) ([]string, error) {
	kc.topicMu.RLock()
	if len(kc.topicNames) > 0 {
		out := append([]string(nil), kc.topicNames...)
		kc.topicMu.RUnlock()
		return out, nil
	}
	kc.topicMu.RUnlock()
	return kc.RefreshTopicNames(ctx)
}

// RefreshTopicNames 强制刷新 Topic 列表并更新缓存。
func (kc *KafkaClient) RefreshTopicNames(ctx context.Context) ([]string, error) {
	if err := kc.alive(ctx); err != nil {
		return nil, err
	}
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
	kc.topicMu.Lock()
	kc.topicNames = append([]string(nil), out...)
	kc.topicMu.Unlock()
	return out, nil
}

// TopicDetail 返回 Topic 的详细信息（含各分区的 Leader/ISR/Offset 等）。
func (kc *KafkaClient) TopicDetail(ctx context.Context, name string) (*TopicInfo, error) {
	if err := kc.alive(ctx); err != nil {
		return nil, err
	}
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

	info.Partitions = make([]PartitionInfo, len(meta.Partitions))
	runBounded(len(meta.Partitions), defaultConcurrency, func(i int) {
		if ctxErr(ctx) != nil {
			return
		}
		p := meta.Partitions[i]
		pi := PartitionInfo{
			ID:           p.ID,
			Leader:       p.Leader,
			Replicas:     p.Replicas,
			ISR:          p.Isr,
			Offline:      p.OfflineReplicas,
			StartOffset:  -1,
			EndOffset:    -1,
			MessageCount: 0,
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
		info.Partitions[i] = pi
	})
	if err := ctxErr(ctx); err != nil {
		return nil, err
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
	if err := kc.alive(ctx); err != nil {
		return err
	}
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
	kc.invalidateTopics()
	g.Log().Infof(ctx, "创建 Topic: %s (partitions=%d, replication=%d)", name, partitions, replication)
	return nil
}

// DeleteTopic 删除一个 Topic。
func (kc *KafkaClient) DeleteTopic(ctx context.Context, name string) error {
	if err := kc.alive(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(name) == "" {
		return errors.New("Topic 名称不能为空")
	}
	if err := kc.admin.DeleteTopic(name); err != nil {
		return err
	}
	kc.invalidateTopics()
	g.Log().Infof(ctx, "删除 Topic: %s", name)
	return nil
}

// ExpandPartitions 将 Topic 分区数扩容到 count（只能增加）。
func (kc *KafkaClient) ExpandPartitions(ctx context.Context, name string, count int32) error {
	if err := kc.alive(ctx); err != nil {
		return err
	}
	if count <= 0 {
		return errors.New("分区数必须大于 0")
	}
	if err := kc.admin.CreatePartitions(name, count, nil, false); err != nil {
		return err
	}
	kc.invalidateTopics()
	g.Log().Infof(ctx, "扩容 Topic %s 分区数到 %d", name, count)
	return nil
}

// AlterTopicConfig 增量修改 Topic 配置。
func (kc *KafkaClient) AlterTopicConfig(ctx context.Context, name string, configs map[string]string) error {
	if err := kc.alive(ctx); err != nil {
		return err
	}
	if len(configs) == 0 {
		return errors.New("配置不能为空")
	}
	entries := make(map[string]*string, len(configs))
	for k, v := range configs {
		vv := v
		entries[k] = &vv
	}
	if err := kc.admin.AlterConfig(sarama.TopicResource, name, entries, false); err != nil {
		return err
	}
	g.Log().Infof(ctx, "修改 Topic 配置: %s", name)
	return nil
}

// UnderReplicated 表示 ISR 少于副本或存在离线副本。
func (p PartitionInfo) UnderReplicated() bool {
	return len(p.ISR) < len(p.Replicas) || len(p.Offline) > 0
}

// PartitionCount 快速获取 Topic 的分区数。
func (kc *KafkaClient) PartitionCount(ctx context.Context, name string) (int, error) {
	if err := kc.alive(ctx); err != nil {
		return 0, err
	}
	metas, err := kc.admin.DescribeTopics([]string{name})
	if err != nil {
		return 0, err
	}
	if len(metas) == 0 || metas[0].Err != sarama.ErrNoError {
		return 0, fmt.Errorf("topic %s 未找到", name)
	}
	return len(metas[0].Partitions), nil
}
