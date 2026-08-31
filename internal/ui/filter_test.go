package ui

import (
	"reflect"
	"testing"

	"fyne.io/fyne/v2/test"
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

func TestFilterSelectUniqueMatchAndKeepSelection(t *testing.T) {
	a := test.NewApp()
	defer a.Quit()

	var got []string
	f := newFilterSelect("筛选 Topic", func(s string) { got = append(got, s) })
	f.SetOptions([]string{"orders", "payments", "user-events"})
	f.SetSelected("orders")
	if f.Selected() != "orders" {
		t.Fatalf("selected: %s", f.Selected())
	}
	if len(got) != 1 || got[0] != "orders" {
		t.Fatalf("onchange: %v", got)
	}

	f.filter.SetText("USER")
	if f.Selected() != "orders" {
		t.Fatalf("typing should not auto-select, got %s", f.Selected())
	}
	if !reflect.DeepEqual(f.sel.Options, []string{"user-events"}) {
		t.Fatalf("filtered options: %v", f.sel.Options)
	}

	f.pickUniqueOrExact()
	if f.Selected() != "user-events" {
		t.Fatalf("enter unique match: %s", f.Selected())
	}
	if len(got) != 2 || got[1] != "user-events" {
		t.Fatalf("unique onchange: %v", got)
	}

	f.filter.SetText("zzz")
	if f.Selected() != "user-events" {
		t.Fatalf("no match should keep selected, got %s", f.Selected())
	}
	if len(f.sel.Options) != 0 {
		t.Fatalf("no match options: %v", f.sel.Options)
	}

	f.filter.SetText("")
	if !reflect.DeepEqual(f.sel.Options, []string{"orders", "payments", "user-events"}) {
		t.Fatalf("cleared filter options: %v", f.sel.Options)
	}
	if f.Selected() != "user-events" {
		t.Fatalf("cleared filter selected: %s", f.Selected())
	}
}
