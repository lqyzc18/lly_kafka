package core

import "strings"

// FilterMessages 按 Key / Value 子串过滤（大小写不敏感）。
func FilterMessages(msgs []MessageInfo, keyQ, valQ string) []MessageInfo {
	keyQ = strings.TrimSpace(keyQ)
	valQ = strings.TrimSpace(valQ)
	if keyQ == "" && valQ == "" {
		out := make([]MessageInfo, len(msgs))
		copy(out, msgs)
		return out
	}
	keyQ = strings.ToLower(keyQ)
	valQ = strings.ToLower(valQ)
	out := make([]MessageInfo, 0, len(msgs))
	for _, m := range msgs {
		if keyQ != "" && !strings.Contains(strings.ToLower(string(m.Key)), keyQ) {
			continue
		}
		if valQ != "" && !strings.Contains(strings.ToLower(string(m.Value)), valQ) {
			continue
		}
		out = append(out, m)
	}
	return out
}
