package core

import (
	"context"
	"sort"
)

// BrokerInfo 集群 Broker 信息
type BrokerInfo struct {
	ID         int32
	Host       string
	Port       int32
	Rack       string
	Controller bool
}

// ClusterOverview 集群概览：Broker 列表、控制器与集群标识。
type ClusterOverview struct {
	ClusterID    string
	ControllerID int32
	Brokers      []BrokerInfo
}

// Controller 返回当前控制器 Broker；集群尚未选出控制器时返回 false。
func (o *ClusterOverview) Controller() (BrokerInfo, bool) {
	for _, b := range o.Brokers {
		if b.Controller {
			return b, true
		}
	}
	return BrokerInfo{}, false
}

// LoadClusterOverview 获取集群概览，仅发起一次元数据请求。
func (kc *KafkaClient) LoadClusterOverview(ctx context.Context) (*ClusterOverview, error) {
	if err := kc.alive(ctx); err != nil {
		return nil, err
	}
	ctx, cancel := opCtx(ctx)
	defer cancel()
	meta, err := kc.admin.BrokerMetadata(ctx)
	if err != nil {
		return nil, err
	}
	ov := &ClusterOverview{
		ClusterID:    meta.Cluster,
		ControllerID: meta.Controller,
		Brokers:      make([]BrokerInfo, 0, len(meta.Brokers)),
	}
	for _, b := range meta.Brokers {
		rack := ""
		if b.Rack != nil {
			rack = *b.Rack
		}
		ov.Brokers = append(ov.Brokers, BrokerInfo{
			ID:         b.NodeID,
			Host:       b.Host,
			Port:       int32(b.Port),
			Rack:       rack,
			Controller: b.NodeID == meta.Controller,
		})
	}
	sort.Slice(ov.Brokers, func(i, j int) bool { return ov.Brokers[i].ID < ov.Brokers[j].ID })
	return ov, nil
}
