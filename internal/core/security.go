package core

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
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

func applySecurity(opts *[]kgo.Opt, conn *Connection) error {
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
		*opts = append(*opts, kgo.DialTLSConfig(tlsCfg))
	}

	if useSASL {
		mech, err := applySASL(conn)
		if err != nil {
			return err
		}
		*opts = append(*opts, kgo.SASL(mech))
	}
	return nil
}

func applySASL(conn *Connection) (sasl.Mechanism, error) {
	user := strings.TrimSpace(conn.SASLUsername)
	if user == "" {
		return nil, errors.New("已启用 SASL，但用户名为空")
	}
	mech := strings.ToUpper(strings.TrimSpace(conn.SASLMechanism))
	if mech == "" {
		mech = SASLPlain
	}

	switch mech {
	case SASLPlain, "SASL/PLAIN":
		return plain.Auth{User: user, Pass: conn.SASLPassword}.AsMechanism(), nil
	case SASLScramSHA256:
		return scram.Auth{User: user, Pass: conn.SASLPassword}.AsSha256Mechanism(), nil
	case SASLScramSHA512:
		return scram.Auth{User: user, Pass: conn.SASLPassword}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("不支持的 SASL 机制: %s", conn.SASLMechanism)
	}
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
	// ServerName 由 franz-go DialTLSConfig 自动从 broker 地址提取。
	return t, nil
}
