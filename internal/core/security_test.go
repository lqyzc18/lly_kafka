package core

import (
	"testing"

	"github.com/IBM/sarama"
)

func TestApplySecurityRejectsCredentialsOnPlaintext(t *testing.T) {
	cfg := sarama.NewConfig()
	err := applySecurity(cfg, &Connection{
		Security:     SecurityPlaintext,
		SASLUsername: "stale-user",
	})
	if err == nil {
		t.Fatal("PLAINTEXT connection with SASL credentials should be rejected")
	}
}

func TestApplySecurityConfiguresSASLPlain(t *testing.T) {
	cfg := sarama.NewConfig()
	err := applySecurity(cfg, &Connection{
		Security:      SecuritySASLPlaintext,
		SASLMechanism: SASLPlain,
		SASLUsername:  "user",
		SASLPassword:  "password",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Net.SASL.Enable || cfg.Net.SASL.Mechanism != sarama.SASLTypePlaintext {
		t.Fatalf("unexpected SASL config: %+v", cfg.Net.SASL)
	}
}

func TestLoadTLSConfigUsesBrokerHostAsServerName(t *testing.T) {
	cfg, err := loadTLSConfig(&Connection{Brokers: "broker.example:9093"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "broker.example" {
		t.Fatalf("ServerName = %q, want broker.example", cfg.ServerName)
	}
}
