// LlyKafka 是一个简易的 Kafka 管理工具，使用 fyne 做桌面 UI、gogf/gf 做底层框架。
package main

import (
	"fmt"
	"os"

	"github.com/gogf/gf/v2/os/gcmd"

	"lly_kafka/internal/core"
	"lly_kafka/internal/ui"
)

func main() {
	configFile := gcmd.GetOpt("config", "").String()
	if err := core.Init(configFile); err != nil {
		fmt.Fprintf(os.Stderr, "初始化失败: %v\n", err)
		os.Exit(1)
	}
	ui.Run()
}
