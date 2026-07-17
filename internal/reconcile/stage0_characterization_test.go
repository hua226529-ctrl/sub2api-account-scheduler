package reconcile

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func stage0Engine(t *testing.T) (*Engine, *testsupport.TempDatabase, *testsupport.FakeSub2API) {
	t.Helper()
	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: 1, Monitors: 1})
	api := testsupport.NewFakeSub2API(fixture)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewEngine(api, database.Store, 50*time.Second, logger), database, api
}

func TestCharacterizationWritesFrozenCollectsButSkipsDeterministicMutation(t *testing.T) {
	engine, _, api := stage0Engine(t)
	ctx := context.Background()
	if err := engine.UpdateFreezeState(ctx, model.FreezeState{Mode: model.AgentFreezeModeWritesFrozen, Reason: "incident"}, "web"); err != nil {
		t.Fatal(err)
	}
	api.ResetStats()
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	stats := api.Stats()
	if stats.ByName[testsupport.CallListMonitors] != 1 || stats.ByName[testsupport.CallListAccounts] != 1 {
		t.Fatalf("frozen reconcile reads = %+v, want monitor and account collection", stats.ByName)
	}
	if stats.ByName[testsupport.CallSetSchedulable] != 0 || stats.ByName[testsupport.CallUpdateLoadFactor] != 0 {
		t.Fatalf("frozen deterministic reconcile wrote upstream: %+v", stats.ByName)
	}
	if engine.Snapshot().LastSyncAt == nil {
		t.Fatal("frozen reconcile did not publish a fresh snapshot")
	}
}

func TestAccountCommandsCannotBypassWritesFrozen(t *testing.T) {
	engine, _, api := stage0Engine(t)
	ctx := context.Background()
	if err := engine.UpdateFreezeState(ctx, model.FreezeState{Mode: model.AgentFreezeModeWritesFrozen, Reason: "incident"}, "web"); err != nil {
		t.Fatal(err)
	}
	api.ResetStats()
	if err := engine.ManualPause(ctx, 1, "web"); err == nil {
		t.Fatal("manual pause bypassed writes freeze")
	}
	if err := engine.ManualResume(ctx, 1, "web"); err == nil {
		t.Fatal("manual resume bypassed writes freeze")
	}
	stats := api.Stats()
	if stats.ByName[testsupport.CallSetSchedulable] != 0 {
		t.Fatalf("manual writes under writes_frozen = %d, want 0", stats.ByName[testsupport.CallSetSchedulable])
	}
}

func TestCurrentBehaviorManualPauseCanOverlapReconcileNetworkRead(t *testing.T) {
	engine, _, api := stage0Engine(t)
	ctx := context.Background()
	entered := make(chan struct{})
	writeEntered := make(chan struct{})
	release := make(chan struct{})
	api.SetBeforeCall(func(call testsupport.Call) {
		if call.Name == testsupport.CallListMonitors {
			close(entered)
			<-release
		}
		if call.Name == testsupport.CallSetSchedulable {
			close(writeEntered)
		}
	})
	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- engine.Reconcile(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reconcile did not reach the fake network read")
	}
	pauseDone := make(chan error, 1)
	go func() { pauseDone <- engine.ManualPause(ctx, 1, "web") }()
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		close(release)
		<-reconcileDone
		t.Fatal("manual pause was serialized behind reconcile; current behavior changed")
	}
	close(release)
	if err := <-pauseDone; err != nil {
		t.Fatal(err)
	}
	if err := <-reconcileDone; err != nil {
		t.Fatal(err)
	}
	if stats := api.Stats(); stats.MaxConcurrent < 2 {
		t.Fatalf("maximum concurrent fake calls = %d, want overlap", stats.MaxConcurrent)
	}
}

func TestCurrentBehaviorManualResumeCanOverlapReconcileNetworkRead(t *testing.T) {
	engine, _, api := stage0Engine(t)
	ctx := context.Background()
	if err := engine.ManualPause(ctx, 1, "web"); err != nil {
		t.Fatal(err)
	}
	api.ResetStats()
	entered := make(chan struct{})
	writeEntered := make(chan struct{})
	release := make(chan struct{})
	api.SetBeforeCall(func(call testsupport.Call) {
		if call.Name == testsupport.CallListMonitors {
			close(entered)
			<-release
		}
		if call.Name == testsupport.CallSetSchedulable {
			close(writeEntered)
		}
	})
	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- engine.Reconcile(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reconcile did not reach the fake network read")
	}
	resumeDone := make(chan error, 1)
	go func() { resumeDone <- engine.ManualResume(ctx, 1, "web") }()
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		close(release)
		<-reconcileDone
		t.Fatal("manual resume was serialized behind reconcile; current behavior changed")
	}
	close(release)
	if err := <-resumeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-reconcileDone; err != nil {
		t.Fatal(err)
	}
	if stats := api.Stats(); stats.MaxConcurrent < 2 {
		t.Fatalf("maximum concurrent fake calls = %d, want overlap", stats.MaxConcurrent)
	}
}

func TestAmbiguousAppliedManualPauseIsConfirmedAndPersistsAcrossRestart(t *testing.T) {
	engine, database, api := stage0Engine(t)
	ctx := context.Background()
	api.SetFailure(testsupport.CallSetSchedulable, testsupport.Failure{
		AtCall: 1, Err: io.EOF, ApplyBeforeError: true,
	})
	if err := engine.ManualPause(ctx, 1, "web"); err != nil {
		t.Fatalf("readback-confirmed pause returned error: %v", err)
	}
	if err := database.Reopen(); err != nil {
		t.Fatal(err)
	}
	control, err := database.Store.GetControl(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !control.OwnsPause || !control.ManualLocked {
		t.Fatalf("confirmed pause was not persisted: %+v", control)
	}
	accounts, err := api.ListAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if accounts[0].Schedulable {
		t.Fatal("fake upstream did not retain the applied write")
	}
}
