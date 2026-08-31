package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/twmb/franz-go/pkg/kadm"
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
	details, err := kc.admin.ListTopics(ctx)
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
	meta, err := kc.admin.Metadata(ctx, name)
	if err != nil {
		return nil, err
	}
	td, ok := meta.Topics[name]
	if !ok {
		return nil, fmt.Errorf("topic %s 未找到", name)
	}
	if td.Err != nil {
		return nil, td.Err
	}

	info := &TopicInfo{Name: name}
	info.NumPartitions = int32(len(td.Partitions))
	info.Partitions = make([]PartitionInfo, 0, len(td.Partitions))

	// 批量拉取所有分区的 start/end offset
	starts, err := kc.admin.ListStartOffsets(ctx, name)
	if err != nil {
		return nil, err
	}
	ends, err := kc.admin.ListEndOffsets(ctx, name)
	if err != nil {
		return nil, err
	}

	for pid, pd := range td.Partitions {
		pi := PartitionInfo{
			ID:           pid,
			Leader:       pd.Leader,
			Replicas:     append([]int32(nil), pd.Replicas...),
			ISR:          append([]int32(nil), pd.ISR...),
			Offline:      append([]int32(nil), pd.OfflineReplicas...),
			StartOffset:  -1,
			EndOffset:    -1,
			MessageCount: 0,
		}
		if so, ok := starts.Lookup(name, pid); ok && so.Err == nil {
			pi.StartOffset = so.Offset
		}
		if eo, ok := ends.Lookup(name, pid); ok && eo.Err == nil {
			pi.EndOffset = eo.Offset
		}
		if pi.StartOffset >= 0 && pi.EndOffset >= pi.StartOffset {
			pi.MessageCount = pi.EndOffset - pi.StartOffset
		}
		info.Partitions = append(info.Partitions, pi)
	}

	// 第一个分区的副本数作为 topic 的 replication factor
	if len(info.Partitions) > 0 {
		info.ReplicationFactor = int16(len(info.Partitions[0].Replicas))
	}

	// 排序
	sort.Slice(info.Partitions, func(i, j int) bool { return info.Partitions[i].ID < info.Partitions[j].ID })

	// 拉取配置
	if cfgs, err := kc.admin.DescribeTopicConfigs(ctx, name); err == nil {
		if len(cfgs) > 0 {
			m := make(map[string]string)
			for _, c := range cfgs[0].Configs {
				if !c.Sensitive {
					m[c.Key] = c.MaybeValue()
				} else {
					m[c.Key] = "(sensitive)"
				}
			}
			info.Configs = m
		}
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

	var cfgMap map[string]*string
	if len(configs) > 0 {
		cfgMap = make(map[string]*string, len(configs))
		for k, v := range configs {
			vv := v
			cfgMap[k] = &vv
		}
	}

	resp, err := kc.admin.CreateTopic(ctx, partitions, replication, cfgMap, name)
	if err != nil {
		AuditLog(AuditCreateTopic, name, kc.connName(), err)
		return err
	}
	if resp.Err != nil {
		AuditLog(AuditCreateTopic, name, kc.connName(), resp.Err)
		return resp.Err
	}
	kc.invalidateTopics()
	g.Log().Infof(ctx, "创建 Topic: %s (partitions=%d, replication=%d)", name, partitions, replication)
	AuditLog(AuditCreateTopic, name, kc.connName(), nil)
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
	resp, err := kc.admin.DeleteTopic(ctx, name)
	if err != nil {
		AuditLog(AuditDeleteTopic, name, kc.connName(), err)
		return err
	}
	if resp.Err != nil {
		AuditLog(AuditDeleteTopic, name, kc.connName(), resp.Err)
		return resp.Err
	}
	kc.invalidateTopics()
	g.Log().Infof(ctx, "删除 Topic: %s", name)
	AuditLog(AuditDeleteTopic, name, kc.connName(), nil)
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
	// franz-go 用 UpdatePartitions 设置最终分区数
	resp, err := kc.admin.UpdatePartitions(ctx, int(count), name)
	if err != nil {
		AuditLog(AuditExpandPartitions, fmt.Sprintf("%s -> %d partitions", name, count), kc.connName(), err)
		return err
	}
	if err := resp.Error(); err != nil {
		AuditLog(AuditExpandPartitions, fmt.Sprintf("%s -> %d partitions", name, count), kc.connName(), err)
		return err
	}
	kc.invalidateTopics()
	g.Log().Infof(ctx, "扩容 Topic %s 分区数到 %d", name, count)
	AuditLog(AuditExpandPartitions, fmt.Sprintf("%s -> %d partitions", name, count), kc.connName(), nil)
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

	alters := make([]kadm.AlterConfig, 0, len(configs))
	for k, v := range configs {
		vv := v
		alters = append(alters, kadm.AlterConfig{
			Op:    kadm.SetConfig,
			Name:  k,
			Value: &vv,
		})
	}

	resps, err := kc.admin.AlterTopicConfigs(ctx, alters, name)
	if err != nil {
		AuditLog(AuditAlterTopicConfig, name, kc.connName(), err)
		return err
	}
	for _, r := range resps {
		if r.Err != nil {
			AuditLog(AuditAlterTopicConfig, name, kc.connName(), r.Err)
			return r.Err
		}
	}
	g.Log().Infof(ctx, "修改 Topic 配置: %s", name)
	AuditLog(AuditAlterTopicConfig, name, kc.connName(), nil)
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
	meta, err := kc.admin.Metadata(ctx, name)
	if err != nil {
		return 0, err
	}
	td, ok := meta.Topics[name]
	if !ok {
		return 0, fmt.Errorf("topic %s 未找到", name)
	}
	if td.Err != nil {
		return 0, td.Err
	}
	return len(td.Partitions), nil
}
