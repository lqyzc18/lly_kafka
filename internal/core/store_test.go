package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConnectionStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "connections.json")
	s, err := NewConnectionStore(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(&Connection{Name: "dev", Brokers: "127.0.0.1:9092", Version: "3.6.0"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(&Connection{Name: "prod", Brokers: "10.0.0.1:9092"}); err != nil {
		t.Fatal(err)
	}

	got := s.Get("dev")
	if got == nil || got.Brokers != "127.0.0.1:9092" {
		t.Fatalf("Get dev = %#v", got)
	}
	got.Brokers = "mutated"
	if s.Get("dev").Brokers == "mutated" {
		t.Fatal("Get should return a copy")
	}

	s2, err := NewConnectionStore(file)
	if err != nil {
		t.Fatal(err)
	}
	if len(s2.Names()) != 2 {
		t.Fatalf("reload names = %v", s2.Names())
	}
	if err := s2.Remove("dev"); err != nil {
		t.Fatal(err)
	}
	if s2.Get("dev") != nil {
		t.Fatal("dev should be removed")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatal(err)
	}
}
