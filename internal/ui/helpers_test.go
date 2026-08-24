package ui

import "testing"

func TestPreviewBytesNilAndEmpty(t *testing.T) {
	if previewBytes(nil, 10) != "null" {
		t.Fatalf("nil => null, got %q", previewBytes(nil, 10))
	}
	if previewBytes([]byte{}, 10) != "" {
		t.Fatalf("empty => empty, got %q", previewBytes([]byte{}, 10))
	}
}

func TestPreviewBytesRuneCountAndEscape(t *testing.T) {
	got := previewBytes([]byte("你好世界"), 2)
	if got != "你好…" {
		t.Fatalf("got %q", got)
	}
	got = previewBytes([]byte("a\nb\t"), 10)
	if got != `a\nb\t` {
		t.Fatalf("got %q", got)
	}
}
