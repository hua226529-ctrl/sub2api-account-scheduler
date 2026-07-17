package reconcile

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type expiryRepoFake struct {
	mu       sync.Mutex
	next     *time.Time
	expired  int
	accounts []int64
}

func (f *expiryRepoFake) NextActiveAccountOverrideExpiry(context.Context) (*time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.next == nil {
		return nil, nil
	}
	value := *f.next
	return &value, nil
}

func (f *expiryRepoFake) ExpireActiveAccountOverrides(context.Context, time.Time) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expired++
	ids := append([]int64(nil), f.accounts...)
	f.next = nil
	return ids, nil
}

type expiryRequesterFake struct{ called chan []int64 }

func (f *expiryRequesterFake) RequestAccounts(ids ...int64) { f.called <- append([]int64(nil), ids...) }

type timedExpiryPass struct{ started chan time.Time }

func (p *timedExpiryPass) ReconcileFull(context.Context) error {
	p.started <- time.Now()
	return nil
}

func (p *timedExpiryPass) ReconcileAccounts(context.Context, []int64) error {
	p.started <- time.Now()
	return nil
}

type timedExpiryRequester struct {
	coordinator *Coordinator
	requested   chan time.Time
	source      chan string
}

func (r *timedExpiryRequester) RequestAccounts(ids ...int64) {
	r.requested <- time.Now()
	r.coordinator.RequestAccounts(ids...)
}

func (r *timedExpiryRequester) RequestAccountsFrom(source string, ids ...int64) {
	r.source <- source
	r.requested <- time.Now()
	r.coordinator.RequestAccountsFrom(source, ids...)
}

type controlledExpiryTimer struct{ fired chan time.Time }

func (t *controlledExpiryTimer) C() <-chan time.Time { return t.fired }
func (t *controlledExpiryTimer) Stop() bool          { return true }

func TestOverrideExpiryWorkerExpiresDueRowsAndRequestsAfterCommit(t *testing.T) {
	now := time.Date(2026, 7, 17, 1, 0, 0, 0, time.UTC)
	due := now.Add(-time.Second)
	repository := &expiryRepoFake{next: &due, accounts: []int64{2, 2, 5}}
	requester := &expiryRequesterFake{called: make(chan []int64, 1)}
	worker := NewOverrideExpiryWorker(repository, requester, slog.Default(), WithOverrideExpiryClock(func() time.Time { return now }))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)
	select {
	case ids := <-requester.called:
		if len(ids) != 2 || ids[0] != 2 || ids[1] != 5 {
			t.Fatalf("unexpected expired account ids: %#v", ids)
		}
	case <-time.After(time.Second):
		t.Fatal("due override was not processed")
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.expired != 1 {
		t.Fatalf("expiry was not committed exactly once: %d", repository.expired)
	}
}

func TestOverrideExpiryWorkerWithoutRowsWaitsForWakeAndStops(t *testing.T) {
	repository := &expiryRepoFake{}
	requester := &expiryRequesterFake{called: make(chan []int64, 1)}
	worker := NewOverrideExpiryWorker(repository, requester, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { worker.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expiry worker did not stop")
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.expired != 0 {
		t.Fatalf("worker expired rows without a due override: %d", repository.expired)
	}
}

func TestOverrideExpiryWorkerEarlierDeadlineWakesAndResetsTimer(t *testing.T) {
	now := time.Date(2026, 7, 17, 1, 0, 0, 0, time.UTC)
	later := now.Add(10 * time.Minute)
	repository := &expiryRepoFake{next: &later}
	requester := &expiryRequesterFake{called: make(chan []int64, 1)}
	durations := make(chan time.Duration, 2)
	worker := NewOverrideExpiryWorker(repository, requester, slog.Default(),
		WithOverrideExpiryClock(func() time.Time { return now }),
		withOverrideExpiryTimerFactory(func(duration time.Duration) expiryTimer {
			durations <- duration
			return &controlledExpiryTimer{fired: make(chan time.Time)}
		}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { worker.Run(ctx); close(done) }()
	if duration := <-durations; duration != 10*time.Minute {
		t.Fatalf("initial timer = %v, want 10m", duration)
	}
	earlier := now.Add(time.Minute)
	repository.mu.Lock()
	repository.next = &earlier
	repository.mu.Unlock()
	worker.Wake()
	if duration := <-durations; duration != time.Minute {
		t.Fatalf("reset timer = %v, want 1m", duration)
	}
	repository.mu.Lock()
	if repository.expired != 0 {
		repository.mu.Unlock()
		t.Fatal("wake expired an override before the reset timer fired")
	}
	repository.mu.Unlock()
	cancel()
	<-done
}

func TestOverrideExpiryCommitStartsTargetedReconcileWithinLatencyTarget(t *testing.T) {
	now := time.Now().UTC()
	due := now.Add(-time.Second)
	repository := &expiryRepoFake{next: &due, accounts: []int64{9}}
	pass := &timedExpiryPass{started: make(chan time.Time, 1)}
	coordinator := NewCoordinator(pass, slog.Default(), WithReconcileInterval(time.Hour))
	requester := &timedExpiryRequester{coordinator: coordinator, requested: make(chan time.Time, 1), source: make(chan string, 1)}
	worker := NewOverrideExpiryWorker(repository, requester, slog.Default(), WithOverrideExpiryClock(func() time.Time { return now }))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go coordinator.Run(ctx)
	go worker.Run(ctx)
	requested := <-requester.requested
	if source := <-requester.source; source != "override_expiry" {
		t.Fatalf("override expiry trigger source=%q", source)
	}
	select {
	case started := <-pass.started:
		latency := started.Sub(requested)
		t.Logf("override expiry commit to reconcile start: %v", latency)
		if latency >= time.Second {
			t.Fatalf("override expiry reconcile start exceeded target: %v", latency)
		}
	case <-time.After(time.Second):
		t.Fatal("expired override did not start targeted reconcile within one second")
	}
}
