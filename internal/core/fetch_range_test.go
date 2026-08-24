package core

import "testing"

func TestParseBrokers(t *testing.T) {
	got := parseBrokers(" 127.0.0.1:9092,  10.0.0.2:9092,, ")
	if len(got) != 2 || got[0] != "127.0.0.1:9092" || got[1] != "10.0.0.2:9092" {
		t.Fatalf("parseBrokers = %#v", got)
	}
	if len(parseBrokers("   ")) != 0 {
		t.Fatalf("empty brokers should be empty slice")
	}
}

func TestLatestStart(t *testing.T) {
	cases := []struct {
		oldest, hwm, n, want int64
	}{
		{0, 100, 50, 50},
		{80, 100, 50, 80},
		{0, 10, 50, 0},
		{5, 5, 10, 5},
	}
	for _, c := range cases {
		if g := latestStart(c.oldest, c.hwm, c.n); g != c.want {
			t.Fatalf("latestStart(%d,%d,%d)=%d want %d", c.oldest, c.hwm, c.n, g, c.want)
		}
	}
}

func TestNormalizeFetchRange(t *testing.T) {
	type tc struct {
		oldest, hwm, start, end int64
		max                     int
		from, to                int64
	}
	cases := []tc{
		{0, 100, 50, 0, 500, 50, 100},
		{80, 100, 0, 0, 500, 80, 100},
		{0, 1000, 0, 0, 50, 0, 50},
		{0, 100, 10, 19, 500, 10, 20},
		{0, 100, 100, 0, 50, 100, 100},
		{0, 100, 90, 200, 20, 90, 100},
	}
	for i, c := range cases {
		from, to := normalizeFetchRange(c.oldest, c.hwm, c.start, c.end, c.max)
		if from != c.from || to != c.to {
			t.Fatalf("case %d got [%d,%d) want [%d,%d)", i, from, to, c.from, c.to)
		}
	}
}
