package ui

import "testing"

func TestParseKV(t *testing.T) {
	got := parseKV(" a=1, b = 2,bad,=x,c=")
	if got["a"] != "1" || got["b"] != "2" || got["c"] != "" {
		t.Fatalf("%v", got)
	}
	if parseKV("  ") != nil {
		t.Fatal("empty should be nil")
	}
}
