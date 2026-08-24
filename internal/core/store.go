package core

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/text/gstr"
)

// Connection 一个 Kafka 集群连接配置
type Connection struct {
	Name     string `json:"name"`
	Brokers  string `json:"brokers"`
	Version  string `json:"version"`
	ClientID string `json:"clientId"`
	Remark   string `json:"remark"`

	Security      string `json:"security,omitempty"`
	SASLMechanism string `json:"saslMechanism,omitempty"`
	SASLUsername  string `json:"saslUsername,omitempty"`
	SASLPassword  string `json:"saslPassword,omitempty"`
	TLSInsecure   bool   `json:"tlsInsecure,omitempty"`
	TLSCAFile     string `json:"tlsCaFile,omitempty"`
	TLSCertFile   string `json:"tlsCertFile,omitempty"`
	TLSKeyFile    string `json:"tlsKeyFile,omitempty"`
}

func cloneConn(c *Connection) *Connection {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}

func cloneConns(in []*Connection) []*Connection {
	out := make([]*Connection, len(in))
	for i, c := range in {
		out[i] = cloneConn(c)
	}
	return out
}

// ConnectionStore 连接配置的本地持久化存储。
// 使用 gf 的 gjson 进行解析/编码，gfile 进行文件读写，
// 自动管理目录创建。
type ConnectionStore struct {
	mu    sync.RWMutex
	file  string
	conns []*Connection
}

// NewConnectionStore 加载连接存储（文件不存在时返回空存储）。
func NewConnectionStore(file string) (*ConnectionStore, error) {
	s := &ConnectionStore{file: file, conns: []*Connection{}}
	if _, err := os.Stat(file); err == nil {
		j, err := gjson.Load(file)
		if err != nil {
			return nil, err
		}
		if err := j.Scan(&s.conns); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// List 返回全部连接配置（副本）。
func (s *ConnectionStore) List() []*Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneConns(s.conns)
}

// Names 返回全部连接名称。
func (s *ConnectionStore) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.conns))
	for _, c := range s.conns {
		out = append(out, c.Name)
	}
	return out
}

// Get 按名称获取连接（副本）。
func (s *ConnectionStore) Get(name string) *Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.conns {
		if c.Name == name {
			return cloneConn(c)
		}
	}
	return nil
}

// Upsert 新增或更新连接并持久化（按 Name 匹配）。
func (s *ConnectionStore) Upsert(c *Connection) error {
	if c == nil {
		return errors.New("连接配置不能为空")
	}
	cp := cloneConn(c)
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, old := range s.conns {
		if old.Name == cp.Name {
			s.conns[i] = cp
			return s.saveLocked()
		}
	}
	s.conns = append(s.conns, cp)
	return s.saveLocked()
}

// Remove 删除连接并持久化。
func (s *ConnectionStore) Remove(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Connection, 0, len(s.conns))
	for _, c := range s.conns {
		if c.Name != name {
			out = append(out, c)
		}
	}
	s.conns = out
	return s.saveLocked()
}

// saveLocked 写出 JSON 文件（调用方需持有 mu 写锁）。
func (s *ConnectionStore) saveLocked() error {
	data := gjson.MustEncode(s.conns)
	dir := filepath.Dir(s.file)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// Best effort: POSIX honors these permissions; Windows keeps its ACLs.
	_ = os.Chmod(dir, 0o700)

	tmp, err := os.CreateTemp(dir, ".connections-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.WriteString(gstr.TrimRight(string(data), "\n")); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.file); err != nil {
		return err
	}
	return os.Chmod(s.file, 0o600)
}
