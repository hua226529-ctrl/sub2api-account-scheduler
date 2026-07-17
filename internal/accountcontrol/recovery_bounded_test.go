package accountcontrol_test

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func TestRecoveryUsesBoundedWorkersAndBatch(t *testing.T) {
	fixture := newServiceFixture(t, 101)
	for accountID := int64(1); accountID <= 101; accountID++ {
		intent := schedulableIntent(t, "bounded-recovery-"+strconv.FormatInt(accountID, 10), accountID, false,
			controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy, *fixture.now, nil)
		mutation := mutationForRecovery(t, intent, accountcontrol.StatusPrepared)
		if _, _, err := fixture.db.Store.PrepareAccountMutation(context.Background(), mutation, nil); err != nil {
			t.Fatal(err)
		}
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var active atomic.Int64
	var once sync.Once
	fixture.api.SetBeforeCall(func(call testsupport.Call) {
		if call.Name != testsupport.CallListAccounts {
			return
		}
		if active.Add(1) == 4 {
			once.Do(func() { close(entered) })
		}
		<-release
		active.Add(-1)
	})
	baseline := runtime.NumGoroutine()
	done := make(chan error, 1)
	go func() { done <- fixture.service.ReconcilePendingAccountMutations(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("recovery did not start four bounded workers")
	}
	if delta := runtime.NumGoroutine() - baseline; delta > 12 {
		close(release)
		<-done
		t.Fatalf("recovery created unbounded waiter goroutines: delta=%d", delta)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if maximum := fixture.api.Stats().MaxConcurrent; maximum > 4 {
		t.Fatalf("recovery concurrency=%d, want <=4", maximum)
	}
	pending, err := fixture.db.Store.ListPendingAccountMutations(context.Background(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("first recovery batch left %d records, want 1", len(pending))
	}
	if err := fixture.service.ReconcilePendingAccountMutations(context.Background()); err != nil {
		t.Fatal(err)
	}
	pending, err = fixture.db.Store.ListPendingAccountMutations(context.Background(), 1000)
	if err != nil || len(pending) != 0 {
		t.Fatalf("second recovery batch pending=%d err=%v", len(pending), err)
	}
}

func TestRecoveryWorkersExitOnContextCancellation(t *testing.T) {
	fixture := newServiceFixture(t, 20)
	for accountID := int64(1); accountID <= 20; accountID++ {
		intent := schedulableIntent(t, "cancel-recovery-"+strconv.FormatInt(accountID, 10), accountID, false,
			controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy, *fixture.now, nil)
		mutation := mutationForRecovery(t, intent, accountcontrol.StatusPrepared)
		if _, _, err := fixture.db.Store.PrepareAccountMutation(context.Background(), mutation, nil); err != nil {
			t.Fatal(err)
		}
	}
	entered := make(chan struct{})
	var once sync.Once
	fixture.api.SetBeforeCall(func(call testsupport.Call) {
		if call.Name == testsupport.CallListAccounts {
			once.Do(func() { close(entered) })
		}
	})
	fixture.api.SetCallDelay(testsupport.CallListAccounts, time.Hour)
	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fixture.service.ReconcilePendingAccountMutations(ctx) }()
	<-entered
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled recovery returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("recovery workers did not exit after context cancellation")
	}
	if calls := fixture.api.Stats().ByName[testsupport.CallListAccounts]; calls > 4 {
		t.Fatalf("canceled recovery accepted %d jobs, want <=4", calls)
	}
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > baseline+4 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if delta := runtime.NumGoroutine() - baseline; delta > 4 {
		t.Fatalf("recovery goroutines did not return to baseline: delta=%d", delta)
	}
}
