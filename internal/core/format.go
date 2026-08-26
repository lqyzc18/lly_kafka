package core

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"unicode/utf8"
)

// FormatPayload 自动把消息内容格式化为可读文本：合法 JSON 则缩进，否则可打印文本，再否则 hex dump。
func FormatPayload(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if json.Valid(b) {
		var buf bytes.Buffer
		if err := json.Indent(&buf, b, "", "  "); err == nil {
			return buf.String()
		}
	}
	if isPrintable(b) {
		return string(b)
	}
	return hex.Dump(b)
}

// FormatJSON 尝试缩进 JSON 格式，若非合法 JSON 则返回原始文本和 false。
func FormatJSON(b []byte) (string, bool) {
	if len(b) == 0 {
		return "", false
	}
	if !json.Valid(b) {
		return string(b), false
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, b, "", "  "); err != nil {
		return string(b), false
	}
	return buf.String(), true
}

// FormatRaw 返回原始 UTF-8 字符串（控制字符原样或转义）。
func FormatRaw(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return string(b)
}

// FormatHex 返回 Hex Dump 格式。
func FormatHex(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.Dump(b)
}

// FormatBase64 返回 Base64 编码字符串。
func FormatBase64(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}

func isPrintable(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for i := 0; i < len(b); {
		r, size := utf8.DecodeRune(b[i:])
		i += size
		if r == utf8.RuneError {
			return false
		}
		if r == 0 {
			return false
		}
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			return false
		}
	}
	return true
}
