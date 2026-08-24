package core

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"unicode/utf8"
)

// FormatPayload 把消息内容格式化为可读文本：合法 JSON 则缩进，否则可打印文本，再否则 hex dump。
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
