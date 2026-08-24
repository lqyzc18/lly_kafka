package ui

import (
	"strings"
)

func parseKV(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	out := map[string]string{}
	for _, kv := range strings.Split(raw, ",") {
		idx := strings.Index(kv, "=")
		if idx <= 0 {
			continue
		}
		k := strings.TrimSpace(kv[:idx])
		if k == "" {
			continue
		}
		out[k] = strings.TrimSpace(kv[idx+1:])
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
