package core

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

const (
	SecurityPlaintext     = "PLAINTEXT"
	SecuritySASLPlaintext = "SASL_PLAINTEXT"
	SecuritySSL           = "SSL"
	SecuritySASLSSL       = "SASL_SSL"

	SASLPlain       = "PLAIN"
	SASLScramSHA256 = "SCRAM-SHA-256"
	SASLScramSHA512 = "SCRAM-SHA-512"
)

func applySecurity(cfg *sarama.Config, conn *Connection) error {
	sec := strings.ToUpper(strings.TrimSpace(conn.Security))
	if sec == "" {
		if strings.TrimSpace(conn.SASLUsername) != "" {
			sec = SecuritySASLPlaintext
		} else {
			sec = SecurityPlaintext
		}
	}

	switch sec {
	case SecurityPlaintext, SecuritySASLPlaintext, SecuritySSL, SecuritySASLSSL:
	default:
		return fmt.Errorf("不支持的安全协议: %s", conn.Security)
	}
	useTLS := sec == SecuritySSL || sec == SecuritySASLSSL
	useSASL := sec == SecuritySASLPlaintext || sec == SecuritySASLSSL

	if !useSASL && (strings.TrimSpace(conn.SASLUsername) != "" || conn.SASLPassword != "") {
		return fmt.Errorf("%s 连接不能设置 SASL 用户名或密码", sec)
	}

	if useTLS {
		tlsCfg, err := loadTLSConfig(conn)
		if err != nil {
			return err
		}
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tlsCfg
	}

	if useSASL {
		if err := applySASL(cfg, conn); err != nil {
			return err
		}
	}
	return nil
}

func applySASL(cfg *sarama.Config, conn *Connection) error {
	user := strings.TrimSpace(conn.SASLUsername)
	if user == "" {
		return errors.New("已启用 SASL，但用户名为空")
	}
	mech := strings.ToUpper(strings.TrimSpace(conn.SASLMechanism))
	if mech == "" {
		mech = SASLPlain
	}
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.User = user
	cfg.Net.SASL.Password = conn.SASLPassword
	cfg.Net.SASL.Handshake = true

	switch mech {
	case SASLPlain, "SASL/PLAIN":
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case SASLScramSHA256:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &xdgSCRAMClient{hashGen: scramSHA256}
		}
	case SASLScramSHA512:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &xdgSCRAMClient{hashGen: scramSHA512}
		}
	default:
		return fmt.Errorf("不支持的 SASL 机制: %s", conn.SASLMechanism)
	}
	return nil
}

func loadTLSConfig(conn *Connection) (*tls.Config, error) {
	t := &tls.Config{
		InsecureSkipVerify: conn.TLSInsecure,
		MinVersion:         tls.VersionTLS12,
	}
	if ca := strings.TrimSpace(conn.TLSCAFile); ca != "" {
		pem, err := os.ReadFile(AbsPath(ca))
		if err != nil {
			return nil, fmt.Errorf("读取 CA 证书失败: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA 证书内容无效")
		}
		t.RootCAs = pool
	}
	certFile := strings.TrimSpace(conn.TLSCertFile)
	keyFile := strings.TrimSpace(conn.TLSKeyFile)
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, errors.New("TLS 客户端证书和私钥必须同时提供")
		}
		cert, err := tls.LoadX509KeyPair(AbsPath(certFile), AbsPath(keyFile))
		if err != nil {
			return nil, fmt.Errorf("加载客户端证书失败: %w", err)
		}
		t.Certificates = []tls.Certificate{cert}
	}
	if brokers := parseBrokers(conn.Brokers); len(brokers) > 0 {
		host, _, err := net.SplitHostPort(brokers[0])
		if err == nil {
			t.ServerName = host
		}
	}
	return t, nil
}
