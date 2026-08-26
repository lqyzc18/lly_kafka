package core

import (
	"fmt"
	"sync"

	"github.com/gogf/gf/v2/os/glog"
)

// 审计日志动作常量
const (
	AuditCreateTopic      = "CREATE_TOPIC"
	AuditDeleteTopic      = "DELETE_TOPIC"
	AuditExpandPartitions = "EXPAND_PARTITIONS"
	AuditAlterTopicConfig = "ALTER_TOPIC_CONFIG"
	AuditProduceMessage   = "PRODUCE_MESSAGE"
	AuditResetGroupOffset = "RESET_GROUP_OFFSET"
)

var (
	auditLogger *glog.Logger
	auditOnce   sync.Once
)

func initAuditLogger() {
	auditLogger = glog.New()
	auditLogger.SetPath(AbsPath("logs"))
	auditLogger.SetFile("audit-{Ymd}.log")
	auditLogger.SetStdoutPrint(false)
	auditLogger.SetLevel(glog.LEVEL_INFO)
}

// AuditLog 记录一条审计日志。
// action: 操作类型（使用 Audit* 常量）
// target: 操作目标（如 topic 名称、消费组名称）
// connName: 连接名称
// err: 操作结果，nil 表示成功
func AuditLog(action, target, connName string, err error) {
	auditOnce.Do(initAuditLogger)
	if auditLogger == nil {
		return
	}
	status := "SUCCESS"
	detail := ""
	if err != nil {
		status = "FAILED"
		detail = fmt.Sprintf(" error=%v", err)
	}
	auditLogger.Infof(Ctx, "[AUDIT] action=%s target=%s conn=%s status=%s%s",
		action, target, connName, status, detail)
}
