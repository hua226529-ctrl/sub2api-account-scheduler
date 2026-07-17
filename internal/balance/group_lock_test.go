package balance

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGroupKeyLocksSerializeSameKeyAndAllowDifferentKeys(t *testing.T) {
	locks := newGroupKeyLocks()
	releaseFirst, err := locks.acquire(context.Background(), 7, "key-a")
	if err != nil {
		t.Fatal(err)
	}
	sameAcquired := make(chan func(), 1)
	go func() {
		release, acquireErr := locks.acquire(context.Background(), 7, "key-a")
		if acquireErr == nil {
			sameAcquired <- release
		}
	}()

	differentAcquired := make(chan func(), 1)
	go func() {
		release, acquireErr := locks.acquire(context.Background(), 7, "key-b")
		if acquireErr == nil {
			differentAcquired <- release
		}
	}()

	select {
	case release := <-differentAcquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("different group key was blocked by the active resource lock")
	}
	select {
	case release := <-sameAcquired:
		release()
		t.Fatal("same group key acquired before the first holder released")
	default:
	}
	releaseFirst()
	select {
	case release := <-sameAcquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("same group key did not acquire after release")
	}
}

func TestGroupTransitionExecutorOwnsTheOnlyProductionTransportCall(t *testing.T) {
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry, "_test.go") || filepath.Base(entry) == "fetcher.go" {
			continue
		}
		content, readErr := os.ReadFile(entry)
		if readErr != nil {
			t.Fatal(readErr)
		}
		calls += strings.Count(string(content), ".fetcher.SwitchGroup(")
	}
	if calls != 1 {
		t.Fatalf("production group transport call count = %d, want exactly one executor call", calls)
	}
	content, err := os.ReadFile("failover.go")
	if err != nil {
		t.Fatal(err)
	}
	transitionBody := string(content)
	start := strings.Index(transitionBody, "func (m *Manager) TransitionGroupTier")
	if start < 0 {
		t.Fatal("group transition executor function not found")
	}
	end := strings.Index(transitionBody[start:], "func (m *Manager) executeGroupTransport")
	if end < 0 {
		t.Fatal("group transition executor functions not found")
	}
	if strings.Contains(transitionBody[start:start+end], "runMu") {
		t.Fatal("group transition executor still holds the global balance runMu")
	}
}
