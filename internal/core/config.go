package core

import (
	"context"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/util/gconv"
)

// AppConfig 应用配置（来自 config.yaml，通过 gf gcfg + gconv 读取）
type AppConfig struct {
	Name                string
	Version             string
	DefaultBrokers      string
	FetchMaxMessages    int
	DefaultMessageCount int
	MessagePreviewLen   int
	ConnectionsFile     string
}

// LoadAppConfig 使用 GoFrame 配置组件读取应用配置。
// 通过 gconv 自动将配置值转换为目标类型，简化类型断言。
func LoadAppConfig(ctx context.Context) (*AppConfig, error) {
	cfg := g.Cfg()
	return &AppConfig{
		Name:                gconv.String(cfg.MustGet(ctx, "app.name", "LlyKafka")),
		Version:             gconv.String(cfg.MustGet(ctx, "app.version", "0.1.0")),
		DefaultBrokers:      gconv.String(cfg.MustGet(ctx, "app.default_brokers", "127.0.0.1:9092")),
		FetchMaxMessages:    gconv.Int(cfg.MustGet(ctx, "app.fetch_max_messages", 500)),
		DefaultMessageCount: gconv.Int(cfg.MustGet(ctx, "app.default_message_count", 50)),
		MessagePreviewLen:   gconv.Int(cfg.MustGet(ctx, "app.message_preview_len", 200)),
		ConnectionsFile:     AbsPath(gconv.String(cfg.MustGet(ctx, "store.connections_file", "data/connections.json"))),
	}, nil
}
