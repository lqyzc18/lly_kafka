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
