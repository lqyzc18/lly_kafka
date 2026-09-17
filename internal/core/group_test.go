package core

import "testing"

func TestComputeGroupLag(t *testing.T) {
	cases := []struct {
		off, end, want int64
	}{
		{0, 10, 10},
		{10, 10, 0},
		{5, 12, 7},
		{-1, 100, -1},
		{10, -1, -1},
		{20, 10, -1},
	}
	for _, c := range cases {
		if g := computeGroupLag(c.off, c.end); g != c.want {
			t.Fatalf("computeGroupLag(%d,%d)=%d want %d", c.off, c.end, g, c.want)
		}
	}
}

func TestGroupLagSummarySkipsUncommitted(t *testing.T) {
	rows := []GroupTopicOffset{
		{Lag: 5},
		{Lag: -1, GroupOffset: -1},
		{Lag: 0},
		{Lag: 3},
	}
	total, n := GroupLagSummary(rows)
	if total != 8 || n != 2 {
		t.Fatalf("got total=%d n=%d", total, n)
	}
}

func TestCommittedGroupRows(t *testing.T) {
	rows := []GroupTopicOffset{
		{Topic: "a", Partition: 0, GroupOffset: -1},
		{Topic: "a", Partition: 1, GroupOffset: 0},
		{Topic: "b", Partition: 0, GroupOffset: 42},
	}
	got := committedGroupRows(rows)
	if len(got) != 2 || got[0].Partition != 1 || got[1].Topic != "b" {
		t.Fatalf("仅已提交分区可重置, got %#v", got)
	}
}
