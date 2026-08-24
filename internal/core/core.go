// Package core 是 LlyKafka 的业务核心层，基于 GoFrame(gf) 提供：
//   - 配置加载（gcfg）
//   - 日志（glog）
//   - 连接配置存储（gjson + gfile）
//   - Kafka 操作（封装 sarama）
package core

import (
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gcfg"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/os/glog"
)

// Ctx 是全局上下文，便于 gf 各组件使用（gf 推荐 gctx.New()）。
var Ctx = gctx.New()

// Init 初始化 GoFrame 底层框架：加载配置文件、初始化日志。
// configFile 为 YAML 配置文件路径，默认 config.yaml。
func Init(configFile string) error {
	if configFile == "" {
		configFile = "config.yaml"
	}
	// 显式设置 gcfg 适配器加载指定的配置文件
	adapter, err := gcfg.NewAdapterFile(configFile)
	if err != nil {
		return err
	}
	g.Cfg().SetAdapter(adapter)
	// 设置日志路径（同时 glog 会从配置 logger 段自动读取 level/path/stdout）
	g.Log().SetPath("logs")
	g.Log().SetLevel(glog.LEVEL_INFO)
	g.Log().Info(Ctx, "GoFrame 初始化完成，配置文件: ", configFile)
	return nil
}