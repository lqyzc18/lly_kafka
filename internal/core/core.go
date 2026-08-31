// Package core 是 LlyKafka 的业务核心层，基于 GoFrame(gf) 提供：
//   - 配置加载（gcfg）
//   - 日志（glog）
//   - 连接配置存储（gjson + gfile）
//   - Kafka 操作（封装 franz-go）
package core

import (
	"path/filepath"
	"strings"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gcfg"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/os/glog"
)

// Ctx 是全局上下文，便于 gf 各组件使用（gf 推荐 gctx.New()）。
var Ctx = gctx.New()

// Init 初始化 GoFrame 底层框架：加载配置文件、初始化日志。
func Init(configFile string) error {
	configFile = ResolveConfigFile(configFile)
	configDir = filepath.Dir(configFile)
	adapter, err := gcfg.NewAdapterFile(configFile)
	if err != nil {
		return err
	}
	g.Cfg().SetAdapter(adapter)
	applyLoggerConfig()
	g.Log().Info(Ctx, "GoFrame 初始化完成，配置文件: ", configFile)
	return nil
}

func applyLoggerConfig() {
	path := strings.TrimSpace(g.Cfg().MustGet(Ctx, "logger.path", "logs").String())
	if path == "" {
		path = "logs"
	}
	g.Log().SetPath(AbsPath(path))

	level := strings.TrimSpace(g.Cfg().MustGet(Ctx, "logger.level", "info").String())
	if level == "" {
		level = "info"
	}
	if err := g.Log().SetLevelStr(level); err != nil {
		g.Log().SetLevel(glog.LEVEL_INFO)
	}

	if !g.Cfg().MustGet(Ctx, "logger.stdout", true).Bool() {
		g.Log().SetStdoutPrint(false)
	}
}
