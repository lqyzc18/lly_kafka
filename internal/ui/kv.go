package ui

import (
	"strings"
)

// parseKV 解析配置或 Header 键值对。
// 支持以下格式：
//  1. 多行模式（包含换行符）：每行一个 key=val 或 key: val（value 内可包含逗号）
//  2. 单行逗号分隔模式：key1=val1,key2=val2 或 key1: val1, key2: val2
//
// 注释行（# 或 // 开头）和空行被忽略。value 为空或 key 为空的行被跳过。
func parseKV(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	parsePair := func(line string) (string, string, bool) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			return "", "", false
		}
		eqIdx := strings.Index(line, "=")
		colonIdx := strings.Index(line, ":")

		splitIdx := -1
		sepLen := 1
		if eqIdx > 0 && colonIdx > 0 {
			if eqIdx < colonIdx {
				splitIdx = eqIdx
			} else {
				splitIdx = colonIdx
			}
		} else if eqIdx > 0 {
			splitIdx = eqIdx
		} else if colonIdx > 0 {
			splitIdx = colonIdx
		}

		if splitIdx <= 0 {
			return "", "", false
		}
		k := strings.TrimSpace(line[:splitIdx])
		v := strings.TrimSpace(line[splitIdx+sepLen:])
		if k == "" {
			return "", "", false
		}
		return k, v, true
	}

	out := make(map[string]string)
	if strings.Contains(raw, "\n") {
		lines := strings.Split(raw, "\n")
		for _, line := range lines {
			line = strings.TrimRight(line, "\r")
			if k, v, ok := parsePair(line); ok {
				out[k] = v
			}
		}
	} else {
		for _, part := range strings.Split(raw, ",") {
			if k, v, ok := parsePair(part); ok {
				out[k] = v
			}
		}
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

// contains 检查字符串切片是否包含指定值。
func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
