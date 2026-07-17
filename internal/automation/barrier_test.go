package automation

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

func TestFreezeWaitsForInflightMutationAndBlocksNewMutation(t *testing.T) {
	barrier := NewBarrier()
	releaseFirst, err := barrier.EnterMutation(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	freezeAcquired := make(chan struct{})
	freezeRelease := make(chan struct{})
	go func() {
		release, freezeErr := barrier.EnterFreeze(context.Background())
		if freezeErr != nil {
			return
		}
		close(freezeAcquired)
		<-freezeRelease
		release()
	}()
	waitForBarrierState(t, barrier, func() bool { return barrier.freezeWaiters == 1 })
	select {
	case <-freezeAcquired:
		t.Fatal("freeze crossed an in-flight mutation")
	default:
	}
	releaseFirst()
	select {
	case <-freezeAcquired:
	case <-time.After(time.Second):
		t.Fatal("freeze did not acquire after the mutation completed")
	}

	blockedCtx, cancelBlocked := context.WithCancel(context.Background())
	cancelBlocked()
	if _, err := barrier.EnterMutation(blockedCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("mutation crossed active freeze: %v", err)
	}
	close(freezeRelease)
	releaseSecond, err := barrier.EnterMutation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	releaseSecond()
}

func TestBarrierWaitsHonorContextCancellation(t *testing.T) {
	barrier := NewBarrier()
	releaseMutation, err := barrier.EnterMutation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	freezeCtx, cancelFreeze := context.WithCancel(context.Background())
	freezeResult := make(chan error, 1)
	go func() {
		_, freezeErr := barrier.EnterFreeze(freezeCtx)
		freezeResult <- freezeErr
	}()
	waitForBarrierState(t, barrier, func() bool { return barrier.freezeWaiters == 1 })
	cancelFreeze()
	if err := <-freezeResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("freeze wait returned %v", err)
	}
	releaseMutation()

	releaseFreeze, err := barrier.EnterFreeze(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mutationCtx, cancelMutation := context.WithCancel(context.Background())
	cancelMutation()
	if _, err := barrier.EnterMutation(mutationCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("mutation wait returned %v", err)
	}
	releaseFreeze()
}

func waitForBarrierState(t *testing.T, barrier *Barrier, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		barrier.mu.Lock()
		ready := condition()
		barrier.mu.Unlock()
		if ready {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("barrier did not reach expected state")
}
