package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
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
	listed, err := kc.admin.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	names := listed.Groups()
	if len(names) == 0 {
		return []GroupInfo{}, nil
	}
	// 批量 DescribeGroups 获取成员数
	described, err := kc.admin.DescribeGroups(ctx, names...)
	if err != nil {
		return nil, err
	}
	out := make([]GroupInfo, 0, len(described))
	for _, d := range described {
		out = append(out, GroupInfo{
			Name:         d.Group,
			State:        d.State,
			ProtocolType: d.ProtocolType,
			Members:      len(d.Members),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// DescribeGroup 返回单个消费组的详情（含成员列表和分区分配）。
func (kc *KafkaClient) DescribeGroup(ctx context.Context, group string) (*GroupDetail, error) {
	if err := kc.alive(ctx); err != nil {
		return nil, err
	}
	described, err := kc.admin.DescribeGroups(ctx, group)
	if err != nil {
		return nil, err
	}
	d, ok := described[group]
	if !ok {
		return nil, fmt.Errorf("消费组 %s 未找到", group)
	}
	if d.Err != nil {
		return nil, fmt.Errorf("消费组 %s 查询失败: %v", group, d.Err)
	}
	detail := &GroupDetail{
		GroupInfo: GroupInfo{
			Name:         d.Group,
			State:        d.State,
			ProtocolType: d.ProtocolType,
			Members:      len(d.Members),
		},
		Protocol: d.Protocol,
		Members:  make([]MemberInfo, 0, len(d.Members)),
	}
	for _, m := range d.Members {
		mi := MemberInfo{
			MemberID:    m.MemberID,
			ClientID:    m.ClientID,
			ClientHost:  m.ClientHost,
			Assignments: make(map[string][]int32),
		}
		// franz-go 的 AssignedPartitions 返回 TopicsSet，按 topic 分组
		if assigned, ok := m.Assigned.AsConsumer(); ok {
			for _, t := range assigned.Topics {
				mi.Assignments[t.Topic] = append(mi.Assignments[t.Topic], t.Partitions...)
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
func (kc *KafkaClient) GroupOffsets(ctx context.Context, group string) ([]GroupTopicOffset, error) {
	if err := kc.alive(ctx); err != nil {
		return nil, err
	}
	// 使用 kadm 的 Lag 功能，自动计算 lag
	lagResp, err := kc.admin.Lag(ctx, group)
	if err != nil {
		return nil, err
	}
	lg, ok := lagResp[group]
	if !ok {
		return []GroupTopicOffset{}, nil
	}

	var out []GroupTopicOffset
	for topic, parts := range lg.Lag {
		for partition, pl := range parts {
			row := GroupTopicOffset{
				Topic:       topic,
				Partition:   partition,
				GroupOffset: pl.Commit.At,
			}
			if pl.Err != nil {
				// Commit 或 End 拉取失败，标记为未提交
				row.GroupOffset = -1
			}
			row.LogEndOffset = pl.End.Offset
			row.Lag = computeGroupLag(row.GroupOffset, row.LogEndOffset)
			out = append(out, row)
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

const (
	OffsetResetLatest   = "latest"
	OffsetResetEarliest = "earliest"
)

// computeGroupLag 计算滞后量。GroupOffset < 0 表示从未提交，Lag 保持 -1。
// logEnd < groupOffset 表示分区已回滚（如日志压缩），也返回 -1。
func computeGroupLag(groupOffset, logEnd int64) int64 {
	if groupOffset < 0 || logEnd < 0 {
		return -1
	}
	if logEnd < groupOffset {
		return -1
	}
	return logEnd - groupOffset
}

// GroupLagSummary 汇总 lag（忽略未提交 / 未知分区）。
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

	// 先拉取当前已提交的 offset
	rows, err := kc.GroupOffsets(ctx, group)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.New("该消费组没有已提交的分区")
	}

	offsets := make(kadm.Offsets)
	for _, r := range rows {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		off := r.LogEndOffset
		if to == OffsetResetEarliest {
			oldest, oerr := kc.OldestOffset(ctx, r.Topic, r.Partition)
			if oerr != nil {
				return oerr
			}
			off = oldest
		} else if off < 0 {
			hwm, herr := kc.NewestOffset(ctx, r.Topic, r.Partition)
			if herr != nil {
				return herr
			}
			off = hwm
		}
		offsets.Add(kadm.Offset{
			Topic:       r.Topic,
			Partition:   r.Partition,
			At:          off,
			LeaderEpoch: -1,
		})
	}

	resps, err := kc.admin.CommitOffsets(ctx, group, offsets)
	if err != nil {
		AuditLog(AuditResetGroupOffset, fmt.Sprintf("%s -> %s", group, to), kc.connName(), err)
		return err
	}
	if err := resps.Error(); err != nil {
		AuditLog(AuditResetGroupOffset, fmt.Sprintf("%s -> %s", group, to), kc.connName(), err)
		return err
	}
	AuditLog(AuditResetGroupOffset, fmt.Sprintf("%s -> %s", group, to), kc.connName(), nil)
	return nil
}
