package ui

import (
	"reflect"
	"testing"
)

func TestFilterTopicsCopyAndMatch(t *testing.T) {
	src := []string{"orders", "payments", "user-events"}
	got := filterTopics(src, "")
	if !reflect.DeepEqual(got, src) {
		t.Fatalf("empty query: got %v", got)
	}
	got[0] = "mutated"
	if src[0] != "orders" {
		t.Fatal("filterTopics must not alias source slice")
	}

	got = filterTopics(src, "USER")
	if !reflect.DeepEqual(got, []string{"user-events"}) {
		t.Fatalf("case-insensitive got %v", got)
	}

	src2 := []string{"a", "ab", "c"}
	got = filterTopics(src2, "a")
	if !reflect.DeepEqual(got, []string{"a", "ab"}) {
		t.Fatalf("substring got %v", got)
	}
	if src2[0] != "a" || src2[1] != "ab" || src2[2] != "c" {
		t.Fatalf("source corrupted: %v", src2)
	}
}
