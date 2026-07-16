package reconcile

import (
	"log/slog"
	"testing"
	"time"
)

func TestSnapshotReturnsEmptyArraysInsteadOfNil(t *testing.T) {
	engine := NewEngine(nil, nil, 50*time.Second, slog.Default())
	snapshot := engine.Snapshot()
	if snapshot.Bindings == nil {
		t.Fatal("bindings must be an empty slice")
	}
	if snapshot.Unmatched == nil {
		t.Fatal("unmatched monitors must be an empty slice")
	}
	if snapshot.Conflicts == nil {
		t.Fatal("conflicts must be an empty slice")
	}
}
