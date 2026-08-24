package core

import (
	"os"
	"path/filepath"
	"strings"
)

var configDir string

// ResolveConfigFile 解析配置文件路径。
// 显式传入则相对当前工作目录；默认优先 cwd/config.yaml，否则可执行文件目录。
func ResolveConfigFile(explicit string) string {
	if explicit != "" && explicit != "config.yaml" {
		if filepath.IsAbs(explicit) {
			return explicit
		}
		abs, err := filepath.Abs(explicit)
		if err == nil {
			return abs
		}
		return explicit
	}
	if st, err := os.Stat("config.yaml"); err == nil && !st.IsDir() {
		abs, err := filepath.Abs("config.yaml")
		if err == nil {
			return abs
		}
	}
	return filepath.Join(exeDir(), "config.yaml")
}

// AbsPath 将相对路径解析为配置文件所在目录下的绝对路径。
func AbsPath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return p
	}
	if filepath.IsAbs(p) {
		return p
	}
	base := configDir
	if base == "" {
		base = exeDir()
	}
	return filepath.Join(base, p)
}

func exeDir() string {
	exe, err := os.Executable()
	if err != nil {
		wd, _ := os.Getwd()
		return wd
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	dir := filepath.Dir(exe)
	lower := strings.ToLower(exe)
	tmp := strings.ToLower(os.TempDir())
	if strings.Contains(lower, "go-build") || (tmp != "" && strings.Contains(lower, tmp)) {
		wd, _ := os.Getwd()
		if wd != "" {
			return wd
		}
	}
	return dir
}
