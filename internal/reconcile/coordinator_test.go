package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"
)

type coordinatorPass struct {
	mu       sync.Mutex
	full     int
	accounts [][]int64
	started  chan struct{}
	release  chan struct{}
}

func (p *coordinatorPass) ReconcileFull(context.Context) error {
	p.mu.Lock()
	p.full++
	p.mu.Unlock()
	if p.started != nil {
		select {
		case p.started <- struct{}{}:
		default:
		}
	}
	if p.release != nil {
		<-p.release
	}
	return nil
}

func (p *coordinatorPass) ReconcileAccounts(_ context.Context, ids []int64) error {
	p.mu.Lock()
	copyIDs := append([]int64(nil), ids...)
	p.accounts = append(p.accounts, copyIDs)
	p.mu.Unlock()
	if p.started != nil {
		select {
		case p.started <- struct{}{}:
		default:
		}
	}
	if p.release != nil {
		<-p.release
	}
	return nil
}

func TestCoordinatorMergesTargetedRequestsAndDoesNotBlockRequesters(t *testing.T) {
	pass := &coordinatorPass{started: make(chan struct{}, 2), release: make(chan struct{})}
	coordinator := NewCoordinator(pass, nil, WithReconcileDebounce(0), WithReconcileInterval(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go coordinator.Run(ctx)

	coordinator.RequestAccounts(3, 1, 3)
	select {
	case <-pass.started:
	case <-time.After(time.Second):
		t.Fatal("targeted pass did not start")
	}
	start := time.Now()
	coordinator.RequestAccounts(2, 1)
	coordinator.RequestFull()
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("request methods blocked while pass was running")
	}
	pass.release <- struct{}{}
	select {
	case <-pass.started:
	case <-time.After(time.Second):
		t.Fatal("pending full pass did not run")
	}
	pass.release <- struct{}{}

	pass.mu.Lock()
	defer pass.mu.Unlock()
	if len(pass.accounts) != 1 || len(pass.accounts[0]) != 2 || pass.accounts[0][0] != 1 || pass.accounts[0][1] != 3 {
		t.Fatalf("unexpected targeted merge: %#v", pass.accounts)
	}
	if pass.full != 1 {
		t.Fatalf("full request did not override pending targeted requests: %d", pass.full)
	}
}

func TestCoordinatorPassRequestsAreNotLostAndFullIsSingleEntry(t *testing.T) {
	pass := &coordinatorPass{started: make(chan struct{}, 3), release: make(chan struct{}, 3)}
	coordinator := NewCoordinator(pass, nil, WithReconcileDebounce(0), WithReconcileInterval(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go coordinator.Run(ctx)

	coordinator.RequestAccounts(7)
	<-pass.started
	coordinator.RequestAccounts(8)
	coordinator.RequestAccounts(9)
	pass.release <- struct{}{}
	<-pass.started
	pass.release <- struct{}{}

	pass.mu.Lock()
	defer pass.mu.Unlock()
	if len(pass.accounts) != 2 || len(pass.accounts[1]) != 2 || pass.accounts[1][0] != 8 || pass.accounts[1][1] != 9 {
		t.Fatalf("requests arriving during pass were lost: %#v", pass.accounts)
	}
}

func TestCoordinatorCancellationExitsWithoutBusyLoop(t *testing.T) {
	pass := &coordinatorPass{}
	coordinator := NewCoordinator(pass, nil, WithReconcileDebounce(0), WithReconcileInterval(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { coordinator.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not exit on context cancellation")
	}
	pass.mu.Lock()
	defer pass.mu.Unlock()
	if pass.full != 0 || len(pass.accounts) != 0 {
		t.Fatalf("coordinator ran without a request: full=%d accounts=%#v", pass.full, pass.accounts)
	}
}

func TestCoordinatorPeriodicFullUsesTheSamePassLoop(t *testing.T) {
	pass := &coordinatorPass{started: make(chan struct{}, 1)}
	coordinator := NewCoordinator(pass, nil, WithReconcileDebounce(0), WithReconcileInterval(time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { coordinator.Run(ctx); close(done) }()
	select {
	case <-pass.started:
		cancel()
	case <-time.After(time.Second):
		cancel()
		t.Fatal("periodic full reconcile did not enter the coordinator pass loop")
	}
	<-done
	pass.mu.Lock()
	defer pass.mu.Unlock()
	if pass.full != 1 || len(pass.accounts) != 0 {
		t.Fatalf("periodic fallback used an unexpected pass path: full=%d targeted=%#v", pass.full, pass.accounts)
	}
}

func TestCoordinatorRetainsStableTriggerSourcesWhileCoalescing(t *testing.T) {
	coordinator := NewCoordinator(&coordinatorPass{}, nil, WithReconcileDebounce(0), WithReconcileInterval(time.Hour))
	coordinator.RequestAccountsFrom("telemetry", 2)
	coordinator.RequestAccountsFrom("balance_lock", 2, 3)
	coordinator.RequestFullFrom("policy_activation")
	full, ids, sources, _, _, _ := coordinator.takePending()
	if !full || len(ids) != 0 {
		t.Fatalf("full request did not cover targeted requests: full=%v ids=%#v", full, ids)
	}
	want := []string{"balance_lock", "policy_activation", "telemetry"}
	if len(sources) != len(want) {
		t.Fatalf("trigger sources=%#v, want %#v", sources, want)
	}
	for index := range want {
		if sources[index] != want[index] {
			t.Fatalf("trigger sources=%#v, want %#v", sources, want)
		}
	}
}
