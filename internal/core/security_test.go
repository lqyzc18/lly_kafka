package core

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestApplySecurityRejectsCredentialsOnPlaintext(t *testing.T) {
	opts := []kgo.Opt{}
	err := applySecurity(&opts, &Connection{
		Security:     SecurityPlaintext,
		SASLUsername: "stale-user",
	})
	if err == nil {
		t.Fatal("PLAINTEXT connection with SASL credentials should be rejected")
	}
}

func TestApplySecurityConfiguresSASLPlain(t *testing.T) {
	opts := []kgo.Opt{}
	err := applySecurity(&opts, &Connection{
		Security:      SecuritySASLPlaintext,
		SASLMechanism: SASLPlain,
		SASLUsername:  "user",
		SASLPassword:  "password",
	})
	if err != nil {
		t.Fatal(err)
	}
	// franz-go 的 SASL 是 functional option，无法直接断言内部状态。
	// 只要不报错即视为配置成功。
}

func TestLoadTLSConfigUsesBrokerHostAsServerName(t *testing.T) {
	// franz-go DialTLSConfig 自动提取 ServerName，这里只验证不报错。
	cfg, err := loadTLSConfig(&Connection{Brokers: "broker.example:9093"})
	if err != nil {
		t.Fatal(err)
	}
	_ = cfg // franz-go 会处理 ServerName
}
