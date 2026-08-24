package core

import (
	"context"
	"net"
	"sort"

	"github.com/gogf/gf/v2/util/gconv"
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
	controllerID := int32(-1)
	if c, err := kc.admin.Controller(); err == nil && c != nil {
		controllerID = c.ID()
	}
	brokers := kc.client.Brokers()
	out := make([]BrokerInfo, 0, len(brokers))
	for _, b := range brokers {
		host, portStr := b.Addr(), ""
		if h, p, err := net.SplitHostPort(b.Addr()); err == nil {
			host, portStr = h, p
		}
		out = append(out, BrokerInfo{
			ID:         b.ID(),
			Host:       host,
			Port:       gconv.Int32(portStr),
			Rack:       b.Rack(),
			Controller: b.ID() == controllerID,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
