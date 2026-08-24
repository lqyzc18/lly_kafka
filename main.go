// LlyKafka 是一个简易的 Kafka 管理工具，使用 fyne 做桌面 UI、gogf/gf 做底层框架。
// 灵感来自 Kafka-King 项目，功能包括连接管理、Broker 概览、Topic 增删查、
// 消息浏览与发送、消费组 lag 监控。
package main

import (
	"github.com/gogf/gf/v2/os/gcmd"

	"lly_kafka/internal/core"
	"lly_kafka/internal/ui"
)

func main() {
	// 通过 gf gcmd 读取 -config 参数（gf 提供的命令行解析能力）
	configFile := gcmd.GetOpt("config", "config.yaml").String()
	if err := core.Init(configFile); err != nil {
		panic(err)
	}
	ui.Run()
}