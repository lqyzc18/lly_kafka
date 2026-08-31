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

// ListBrokers 返回集群内全部 Broker，包含控制器标识。
func (kc *KafkaClient) ListBrokers(ctx context.Context) ([]BrokerInfo, error) {
	if err := kc.alive(ctx); err != nil {
		return nil, err
	}
	meta, err := kc.admin.BrokerMetadata(ctx)
	if err != nil {
		return nil, err
	}
	controllerID := meta.Controller
	out := make([]BrokerInfo, 0, len(meta.Brokers))
	for _, b := range meta.Brokers {
		rack := ""
		if b.Rack != nil {
			rack = *b.Rack
		}
		out = append(out, BrokerInfo{
			ID:         b.NodeID,
			Host:       b.Host,
			Port:       int32(b.Port),
			Rack:       rack,
			Controller: b.NodeID == controllerID,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
