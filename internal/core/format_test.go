package core

import (
	"strings"
	"testing"
)

func TestFormatPayloadJSON(t *testing.T) {
	got := FormatPayload([]byte(`{"a":1}`))
	if !strings.Contains(got, `"a"`) || !strings.Contains(got, "\n") {
		t.Fatalf("expected indented json, got %q", got)
	}
}

func TestFormatPayloadHex(t *testing.T) {
	got := FormatPayload([]byte{0x00, 0x01, 0xff})
	if !strings.Contains(got, "00000000") {
		t.Fatalf("expected hex dump, got %q", got)
	}
}

func TestFilterMessages(t *testing.T) {
	msgs := []MessageInfo{
		{Key: []byte("user-1"), Value: []byte("hello")},
		{Key: []byte("order"), Value: []byte("world")},
	}
	got := FilterMessages(msgs, "USER", "")
	if len(got) != 1 || string(got[0].Key) != "user-1" {
		t.Fatalf("key filter: %+v", got)
	}
	got = FilterMessages(msgs, "", "WOR")
	if len(got) != 1 || string(got[0].Value) != "world" {
		t.Fatalf("value filter: %+v", got)
	}
	orig := FilterMessages(msgs, "", "")
	orig[0].Key = []byte("mut")
	if string(msgs[0].Key) != "user-1" {
		t.Fatal("empty filter must copy")
	}
}

func TestGroupLagSummary(t *testing.T) {
	total, n := GroupLagSummary([]GroupTopicOffset{
		{Lag: 10}, {Lag: 0}, {Lag: 5}, {Lag: -1},
	})
	if total != 15 || n != 2 {
		t.Fatalf("got total=%d n=%d", total, n)
	}
}
