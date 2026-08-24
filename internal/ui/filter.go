package ui

import (
	"strings"

	"github.com/gogf/gf/v2/text/gstr"
)

// filterTopics 按大小写不敏感子串过滤，始终返回新切片，避免与源切片共享底层数组。
func filterTopics(topics []string, query string) []string {
	q := strings.TrimSpace(query)
	if q == "" {
		out := make([]string, len(topics))
		copy(out, topics)
		return out
	}
	out := make([]string, 0, len(topics))
	for _, t := range topics {
		if gstr.ContainsI(t, q) {
			out = append(out, t)
		}
	}
	return out
}
