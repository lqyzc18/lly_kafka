package ui

import (
	"context"
	"testing"

	"lly_kafka/internal/core"
)

func TestParsePartitionInput(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  int32
	}{
		{"", -1},
		{" -1 ", -1},
		{"0", 0},
		{"42", 42},
	} {
		got, err := parsePartitionInput(tc.input)
		if err != nil || got != tc.want {
			t.Fatalf("parsePartitionInput(%q) = %d, %v; want %d", tc.input, got, err, tc.want)
		}
	}
	for _, input := range []string{"oops", "-2", "2147483648", "1.5"} {
		if _, err := parsePartitionInput(input); err == nil {
			t.Fatalf("parsePartitionInput(%q) 应拒绝无效分区", input)
		}
	}
}

func TestInvalidateFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &messagesPanel{
		allRows:     []core.MessageInfo{{Topic: "old"}},
		rows:        []core.MessageInfo{{Topic: "old"}},
		fetchCancel: cancel,
		fetchSeq:    3,
	}
	p.invalidateFetch()
	if ctx.Err() != context.Canceled || p.fetchSeq != 4 || p.fetchCancel != nil {
		t.Fatalf("旧拉取未失效: ctx=%v seq=%d cancel=%v", ctx.Err(), p.fetchSeq, p.fetchCancel)
	}
	if len(p.allRows) != 0 || len(p.rows) != 0 {
		t.Fatalf("切换选择后仍展示旧消息: %#v", p.rows)
	}
}
