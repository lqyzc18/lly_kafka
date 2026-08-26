package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/IBM/sarama"
)

// GroupInfo 消费组概要信息
type GroupInfo struct {
	Name         string
	State        string
	ProtocolType string
	Members      int
}

// MemberInfo 消费组成员信息
type MemberInfo struct {
	MemberID    string
	ClientID    string
	ClientHost  string
	Assignments map[string][]int32 // topic -> 分区列表
}

// GroupDetail 消费组详情（含成员列表）
type GroupDetail struct {
	GroupInfo
	Protocol string
	Members  []MemberInfo
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
	if err := kc.alive(ctx); err != nil {
		return nil, err
	}
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
	const batch = 32
	out := make([]GroupInfo, 0, len(names))
	for i := 0; i < len(names); i += batch {
		if err := ctxErr(ctx); err != nil {
			return nil, err
		}
		end := i + batch
		if end > len(names) {
			end = len(names)
		}
		descs, err := kc.admin.DescribeConsumerGroups(names[i:end])
		if err != nil {
			return nil, err
		}
		for _, d := range descs {
			out = append(out, GroupInfo{
				Name:         d.GroupId,
				State:        d.State,
				ProtocolType: d.ProtocolType,
				Members:      len(d.Members),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// DescribeGroup 返回单个消费组的详情（含成员列表和分区分配）。
func (kc *KafkaClient) DescribeGroup(ctx context.Context, group string) (*GroupDetail, error) {
	if err := kc.alive(ctx); err != nil {
		return nil, err
	}
	descs, err := kc.admin.DescribeConsumerGroups([]string{group})
	if err != nil {
		return nil, err
	}
	if len(descs) == 0 {
		return nil, fmt.Errorf("消费组 %s 未找到", group)
	}
	d := descs[0]
	if d.Err != sarama.ErrNoError {
		return nil, fmt.Errorf("消费组 %s 查询失败: %v", group, d.Err)
	}
	detail := &GroupDetail{
		GroupInfo: GroupInfo{
			Name:         d.GroupId,
			State:        d.State,
			ProtocolType: d.ProtocolType,
			Members:      len(d.Members),
		},
		Protocol: d.Protocol,
		Members:  make([]MemberInfo, 0, len(d.Members)),
	}
	for memberID, m := range d.Members {
		mi := MemberInfo{
			MemberID:    memberID,
			ClientID:    m.ClientId,
			ClientHost:  m.ClientHost,
			Assignments: make(map[string][]int32),
		}
		if assignment, aerr := m.GetMemberAssignment(); aerr == nil && assignment != nil {
			for topic, parts := range assignment.Topics {
				mi.Assignments[topic] = append([]int32(nil), parts...)
			}
		}
		detail.Members = append(detail.Members, mi)
	}
	sort.Slice(detail.Members, func(i, j int) bool {
		return detail.Members[i].ClientID < detail.Members[j].ClientID
	})
	return detail, nil
}

// GroupOffsets 返回消费组在所有 Topic/分区上的消费进度与 lag。
// 内部使用 admin.ListConsumerGroupOffsets 的「全部 topic」模式（topics=nil）。
func (kc *KafkaClient) GroupOffsets(ctx context.Context, group string) ([]GroupTopicOffset, error) {
	if err := kc.alive(ctx); err != nil {
		return nil, err
	}
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
			out = append(out, GroupTopicOffset{
				Topic:        topic,
				Partition:    pid,
				GroupOffset:  block.Offset,
				LogEndOffset: -1,
				Lag:          -1,
			})
		}
	}
	runBounded(len(out), defaultConcurrency, func(i int) {
		if ctxErr(ctx) != nil {
			return
		}
		item := &out[i]
		end, eerr := kc.client.GetOffset(item.Topic, item.Partition, sarama.OffsetNewest)
		if eerr != nil {
			return
		}
		item.LogEndOffset = end
		if end >= item.GroupOffset {
			item.Lag = end - item.GroupOffset
		}
	})
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out, nil
}

const (
	OffsetResetLatest   = "latest"
	OffsetResetEarliest = "earliest"
)

// GroupLagSummary 汇总 lag。
func GroupLagSummary(rows []GroupTopicOffset) (totalLag int64, lagPartitions int) {
	for _, r := range rows {
		if r.Lag > 0 {
			totalLag += r.Lag
			lagPartitions++
		}
	}
	return totalLag, lagPartitions
}

// ResetGroupOffsets 将消费组所有已提交分区重置到最新或最早。
func (kc *KafkaClient) ResetGroupOffsets(ctx context.Context, group, to string) error {
	if err := kc.alive(ctx); err != nil {
		return err
	}
	group = strings.TrimSpace(group)
	if group == "" {
		return errors.New("消费组不能为空")
	}
	rows, err := kc.GroupOffsets(ctx, group)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.New("该消费组没有已提交的分区")
	}
	offsets := make(map[string]map[int32]sarama.OffsetAndMetadata)
	for _, r := range rows {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		off := r.LogEndOffset
		if to == OffsetResetEarliest {
			oldest, oerr := kc.client.GetOffset(r.Topic, r.Partition, sarama.OffsetOldest)
			if oerr != nil {
				return oerr
			}
			off = oldest
		} else if off < 0 {
			hwm, herr := kc.client.GetOffset(r.Topic, r.Partition, sarama.OffsetNewest)
			if herr != nil {
				return herr
			}
			off = hwm
		}
		if offsets[r.Topic] == nil {
			offsets[r.Topic] = make(map[int32]sarama.OffsetAndMetadata)
		}
		offsets[r.Topic][r.Partition] = sarama.OffsetAndMetadata{
			Offset:      off,
			LeaderEpoch: -1,
		}
	}
	resp, err := kc.admin.AlterConsumerGroupOffsets(group, offsets, nil)
	if err != nil {
		AuditLog(AuditResetGroupOffset, fmt.Sprintf("%s -> %s", group, to), kc.connName(), err)
		return err
	}
	if resp == nil {
		AuditLog(AuditResetGroupOffset, fmt.Sprintf("%s -> %s", group, to), kc.connName(), nil)
		return nil
	}
	for topic, parts := range resp.Errors {
		for pid, e := range parts {
			if e != sarama.ErrNoError {
				resetErr := fmt.Errorf("重置 %s[%d] 失败: %v", topic, pid, e)
				AuditLog(AuditResetGroupOffset, fmt.Sprintf("%s -> %s", group, to), kc.connName(), resetErr)
				return resetErr
			}
		}
	}
	AuditLog(AuditResetGroupOffset, fmt.Sprintf("%s -> %s", group, to), kc.connName(), nil)
	return nil
}
