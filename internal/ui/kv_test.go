package ui

import "testing"

func TestParseKV(t *testing.T) {
	// 单行逗号分隔测试
	got := parseKV(" a=1, b = 2,bad,=x,c=")
	if got["a"] != "1" || got["b"] != "2" || got["c"] != "" {
		t.Fatalf("comma format failed: %v", got)
	}
	if parseKV("  ") != nil {
		t.Fatal("empty should be nil")
	}

	// 冒号分隔测试
	colonGot := parseKV("Content-Type: application/json, X-Trace-Id: abc-123")
	if colonGot["Content-Type"] != "application/json" || colonGot["X-Trace-Id"] != "abc-123" {
		t.Fatalf("colon format failed: %v", colonGot)
	}

	// 多行测试（允许 value 中包含逗号，如 URL 或 JSON）
	multilineRaw := `
# 这是一个注释
traceId: 1001,1002,1003
webhook_url = https://example.com/api?a=1,2,3
retention.ms: 86400000
// 另一行注释
emptyVal=
`
	multiGot := parseKV(multilineRaw)
	if multiGot["traceId"] != "1001,1002,1003" {
		t.Fatalf("expected value with commas preserved, got: %q", multiGot["traceId"])
	}
	if multiGot["webhook_url"] != "https://example.com/api?a=1,2,3" {
		t.Fatalf("expected url with commas, got: %q", multiGot["webhook_url"])
	}
	if multiGot["retention.ms"] != "86400000" {
		t.Fatalf("expected retention.ms, got: %q", multiGot["retention.ms"])
	}
	if val, ok := multiGot["emptyVal"]; !ok || val != "" {
		t.Fatalf("expected empty value for emptyVal, got %q", val)
	}
}
