package accountcontrol_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

type lockObservedContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func newLockObservedContext() *lockObservedContext {
	return &lockObservedContext{Context: context.Background(), observed: make(chan struct{})}
}

func (c *lockObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestWaitingSubmissionIsReArbitratedAfterAccountLock(t *testing.T) {
	tests := []struct {
		name          string
		lowAuthority  controlplane.Authority
		lowProducer   controlplane.Producer
		highAuthority controlplane.Authority
		highProducer  controlplane.Producer
	}{
		{"policy superseded by manual hold", controlplane.AuthorityActivePolicy, controlplane.ProducerPolicyScheduler,
			controlplane.AuthorityManualHold, controlplane.ProducerAdminUI},
		{"agent superseded by administrator", controlplane.AuthorityAutonomousAgent, controlplane.ProducerAgentOperator,
			controlplane.AuthorityAdministratorCommand, controlplane.ProducerAdminUI},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newServiceFixture(t, 1)
			releaseBlocker, blockerDone := blockAccountWithLoadMutation(t, fixture)

			lowExpiry := fixture.now.Add(time.Hour)
			low := schedulableIntent(t, "queued-low", 1, false, test.lowProducer, test.lowAuthority, fixture.now.Add(time.Second), &lowExpiry)
			if test.lowAuthority == controlplane.AuthorityActivePolicy {
				low.ExpiresAt = nil
			}
			highExpiry := fixture.now.Add(time.Hour)
			var highExpires *time.Time
			if test.highAuthority != controlplane.AuthorityManualHold {
				highExpires = &highExpiry
			}
			high := schedulableIntent(t, "queued-high", 1, true, test.highProducer, test.highAuthority, fixture.now.Add(2*time.Second), highExpires)

			lowCtx, highCtx := newLockObservedContext(), newLockObservedContext()
			lowResult := make(chan submitOutcome, 1)
			highResult := make(chan submitOutcome, 1)
			go submitAsync(lowCtx, fixture.service, accountcontrol.Submission{CommandID: "queued-low", Intent: low,
				PersistOverride: test.lowAuthority != controlplane.AuthorityActivePolicy, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}, lowResult)
			<-lowCtx.observed
			go submitAsync(highCtx, fixture.service, accountcontrol.Submission{CommandID: "queued-high", Intent: high,
				PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}, highResult)
			<-highCtx.observed

			releaseBlocker()
			if err := <-blockerDone; err != nil {
				t.Fatal(err)
			}
			lowOutcome, highOutcome := <-lowResult, <-highResult
			if lowOutcome.err != nil || lowOutcome.result.Status != accountcontrol.StatusSuperseded {
				t.Fatalf("low outcome=%+v err=%v", lowOutcome.result, lowOutcome.err)
			}
			if highOutcome.err != nil || (highOutcome.result.Status != accountcontrol.StatusAppliedNoop && highOutcome.result.Status != accountcontrol.StatusApplied) {
				t.Fatalf("high outcome=%+v err=%v", highOutcome.result, highOutcome.err)
			}
			if writes := fixture.api.Stats().ByName[testsupport.CallSetSchedulable]; writes != 0 {
				t.Fatalf("superseded submission wrote upstream %d times", writes)
			}
		})
	}
}

func TestOlderPolicyWaitingForLockIsSupersededByNewPolicyVersion(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	releaseBlocker, blockerDone := blockAccountWithLoadMutation(t, fixture)
	oldPolicy := schedulableIntent(t, "policy-old", 1, false, controlplane.ProducerPolicyScheduler,
		controlplane.AuthorityActivePolicy, *fixture.now, nil)
	oldPolicy.PolicyVersion = "policy-v1"
	newPolicy := schedulableIntent(t, "policy-new", 1, true, controlplane.ProducerPolicyScheduler,
		controlplane.AuthorityActivePolicy, fixture.now.Add(time.Second), nil)
	newPolicy.PolicyVersion = "policy-v2"
	oldCtx, newCtx := newLockObservedContext(), newLockObservedContext()
	oldResult, newResult := make(chan submitOutcome, 1), make(chan submitOutcome, 1)
	go submitAsync(oldCtx, fixture.service, accountcontrol.Submission{CommandID: "policy-old", Intent: oldPolicy,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}, oldResult)
	<-oldCtx.observed
	go submitAsync(newCtx, fixture.service, accountcontrol.Submission{CommandID: "policy-new", Intent: newPolicy,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}, newResult)
	<-newCtx.observed
	releaseBlocker()
	if err := <-blockerDone; err != nil {
		t.Fatal(err)
	}
	oldOutcome, newOutcome := <-oldResult, <-newResult
	if oldOutcome.err != nil || oldOutcome.result.Status != accountcontrol.StatusSuperseded {
		t.Fatalf("old policy outcome=%+v err=%v", oldOutcome.result, oldOutcome.err)
	}
	if newOutcome.err != nil || newOutcome.result.Status != accountcontrol.StatusAppliedNoop {
		t.Fatalf("new policy outcome=%+v err=%v", newOutcome.result, newOutcome.err)
	}
	if writes := fixture.api.Stats().ByName[testsupport.CallSetSchedulable]; writes != 0 {
		t.Fatalf("stale policy wrote upstream %d times", writes)
	}
}

func TestNewerPolicyHoldingLockStillSupersedesQueuedOldPolicy(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	releaseBlocker, blockerDone := blockAccountWithLoadMutation(t, fixture)
	oldPolicy := schedulableIntent(t, "policy-old-second", 1, true, controlplane.ProducerPolicyScheduler,
		controlplane.AuthorityActivePolicy, *fixture.now, nil)
	oldPolicy.PolicyVersion = "policy-v1"
	newPolicy := schedulableIntent(t, "policy-new-first", 1, false, controlplane.ProducerPolicyScheduler,
		controlplane.AuthorityActivePolicy, fixture.now.Add(time.Second), nil)
	newPolicy.PolicyVersion = "policy-v2"
	newCtx, oldCtx := newLockObservedContext(), newLockObservedContext()
	newResult, oldResult := make(chan submitOutcome, 1), make(chan submitOutcome, 1)
	go submitAsync(newCtx, fixture.service, accountcontrol.Submission{CommandID: "policy-new-first", Intent: newPolicy,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}, newResult)
	<-newCtx.observed
	go submitAsync(oldCtx, fixture.service, accountcontrol.Submission{CommandID: "policy-old-second", Intent: oldPolicy,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}, oldResult)
	<-oldCtx.observed
	releaseBlocker()
	if err := <-blockerDone; err != nil {
		t.Fatal(err)
	}
	newOutcome, oldOutcome := <-newResult, <-oldResult
	if newOutcome.err != nil || newOutcome.result.Status != accountcontrol.StatusApplied {
		t.Fatalf("new policy outcome=%+v err=%v", newOutcome.result, newOutcome.err)
	}
	if oldOutcome.err != nil || oldOutcome.result.Status != accountcontrol.StatusSuperseded {
		t.Fatalf("old policy outcome=%+v err=%v", oldOutcome.result, oldOutcome.err)
	}
	if writes := fixture.api.Stats().ByName[testsupport.CallSetSchedulable]; writes != 1 {
		t.Fatalf("policy lock order wrote upstream %d times", writes)
	}
}

func TestSubmissionExpiringWhileWaitingForLockDoesNotWrite(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	var clock atomic.Int64
	clock.Store(fixture.now.UnixNano())
	fixture.service = accountcontrol.New(fixture.db.Store, fixture.api, accountcontrol.WithClock(func() time.Time {
		return time.Unix(0, clock.Load()).UTC()
	}))
	releaseBlocker, blockerDone := blockAccountWithLoadMutation(t, fixture)
	expires := fixture.now.Add(time.Minute)
	intent := schedulableIntent(t, "expires-in-lock-queue", 1, false, controlplane.ProducerAgentOperator,
		controlplane.AuthorityAutonomousAgent, *fixture.now, &expires)
	observed := newLockObservedContext()
	result := make(chan submitOutcome, 1)
	go submitAsync(observed, fixture.service, accountcontrol.Submission{CommandID: "expires-in-lock-queue", Intent: intent,
		PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}, result)
	<-observed.observed
	clock.Store(expires.Add(time.Second).UnixNano())
	releaseBlocker()
	if err := <-blockerDone; err != nil {
		t.Fatal(err)
	}
	outcome := <-result
	if outcome.err != nil || outcome.result.Status != accountcontrol.StatusExpired || outcome.result.VerifiedAfter != nil {
		t.Fatalf("expired outcome=%+v err=%v", outcome.result, outcome.err)
	}
	if writes := fixture.api.Stats().ByName[testsupport.CallSetSchedulable]; writes != 0 {
		t.Fatalf("expired submission wrote upstream %d times", writes)
	}
	override, err := fixture.db.Store.FindActiveAccountOverride(context.Background(), 1,
		controlplane.OperationSetAccountSchedulable, controlplane.AuthorityAutonomousAgent, expires.Add(time.Second))
	if err != nil || override != nil {
		t.Fatalf("expired pending override became active: override=%+v err=%v", override, err)
	}
}

func TestConcurrentIdempotencyAndBlockedReplay(t *testing.T) {
	t.Run("same semantics execute once", func(t *testing.T) {
		fixture := newServiceFixture(t, 1)
		intent := schedulableIntent(t, "concurrent-same", 1, false, controlplane.ProducerAdminUI,
			controlplane.AuthorityManualHold, *fixture.now, nil)
		submission := accountcontrol.Submission{CommandID: "concurrent-same", Intent: intent, PersistOverride: true,
			Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}
		start := make(chan struct{})
		results := make(chan submitOutcome, 2)
		for range 2 {
			go func() {
				<-start
				submitAsync(context.Background(), fixture.service, submission, results)
			}()
		}
		close(start)
		first, second := <-results, <-results
		if first.err != nil || second.err != nil || first.result.MutationID != second.result.MutationID {
			t.Fatalf("first=%+v/%v second=%+v/%v", first.result, first.err, second.result, second.err)
		}
		if !first.result.IdempotentReplay && !second.result.IdempotentReplay {
			t.Fatal("neither concurrent result was identified as replay")
		}
		if writes := fixture.api.Stats().ByName[testsupport.CallSetSchedulable]; writes != 1 {
			t.Fatalf("same request wrote %d times", writes)
		}
	})

	t.Run("different semantics conflict", func(t *testing.T) {
		fixture := newServiceFixture(t, 1)
		pause := schedulableIntent(t, "concurrent-conflict", 1, false, controlplane.ProducerAdminUI,
			controlplane.AuthorityManualHold, *fixture.now, nil)
		resume := pause
		resume.DesiredState = schedulableIntent(t, "conflict-resume", 1, true, controlplane.ProducerAdminUI,
			controlplane.AuthorityManualHold, *fixture.now, nil).DesiredState
		start := make(chan struct{})
		results := make(chan submitOutcome, 2)
		for _, intent := range []controlplane.Intent{pause, resume} {
			intent := intent
			go func() {
				<-start
				submitAsync(context.Background(), fixture.service, accountcontrol.Submission{CommandID: "concurrent-conflict", Intent: intent,
					PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}, results)
			}()
		}
		close(start)
		outcomes := []submitOutcome{<-results, <-results}
		conflicts := 0
		for _, outcome := range outcomes {
			var conflict *accountcontrol.IdempotencyConflictError
			if errors.As(outcome.err, &conflict) {
				conflicts++
			}
		}
		if conflicts != 1 {
			t.Fatalf("outcomes=%+v, want exactly one conflict", outcomes)
		}
		if writes := fixture.api.Stats().ByName[testsupport.CallSetSchedulable]; writes > 1 {
			t.Fatalf("conflicting request wrote %d times", writes)
		}
	})

	t.Run("blocked replay remains blocked", func(t *testing.T) {
		fixture := newServiceFixture(t, 1)
		state := model.AgentFreezeState{ScopeType: "global", Mode: model.AgentFreezeModeWritesFrozen, Actor: "test", Reason: "incident"}
		if err := fixture.db.Store.SetAgentFreezeState(context.Background(), &state); err != nil {
			t.Fatal(err)
		}
		intent := schedulableIntent(t, "blocked-replay", 1, false, controlplane.ProducerAdminUI,
			controlplane.AuthorityManualHold, *fixture.now, nil)
		submission := accountcontrol.Submission{CommandID: "blocked-replay", Intent: intent, PersistOverride: true,
			Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}
		first, firstErr := fixture.service.Submit(context.Background(), submission)
		state.Mode = model.AgentFreezeModeActive
		if err := fixture.db.Store.SetAgentFreezeState(context.Background(), &state); err != nil {
			t.Fatal(err)
		}
		second, secondErr := fixture.service.Submit(context.Background(), submission)
		var firstBlocked, secondBlocked *accountcontrol.BlockedError
		if !errors.As(firstErr, &firstBlocked) || !errors.As(secondErr, &secondBlocked) || first.MutationID != second.MutationID || !second.IdempotentReplay {
			t.Fatalf("first=%+v/%v second=%+v/%v", first, firstErr, second, secondErr)
		}
		if writes := fixture.api.Stats().ByName[testsupport.CallSetSchedulable]; writes != 0 {
			t.Fatalf("blocked replay wrote %d times", writes)
		}
	})

	t.Run("restart replays original result", func(t *testing.T) {
		fixture := newServiceFixture(t, 1)
		intent := schedulableIntent(t, "restart-replay", 1, false, controlplane.ProducerAdminUI,
			controlplane.AuthorityManualHold, *fixture.now, nil)
		submission := accountcontrol.Submission{CommandID: "restart-replay", Intent: intent, PersistOverride: true,
			Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}
		first, err := fixture.service.Submit(context.Background(), submission)
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.db.Reopen(); err != nil {
			t.Fatal(err)
		}
		fixture.service = accountcontrol.New(fixture.db.Store, fixture.api, accountcontrol.WithClock(func() time.Time { return *fixture.now }))
		second, err := fixture.service.Submit(context.Background(), submission)
		if err != nil || !second.IdempotentReplay || second.MutationID != first.MutationID {
			t.Fatalf("first=%+v second=%+v err=%v", first, second, err)
		}
		if writes := fixture.api.Stats().ByName[testsupport.CallSetSchedulable]; writes != 1 {
			t.Fatalf("restart replay wrote %d times", writes)
		}
	})

	t.Run("one administrator grant cannot target two accounts", func(t *testing.T) {
		fixture := newServiceFixture(t, 2)
		start := make(chan struct{})
		results := make(chan submitOutcome, 2)
		expires := fixture.now.Add(time.Hour)
		submissions := make([]accountcontrol.Submission, 0, 2)
		for accountID := int64(1); accountID <= 2; accountID++ {
			intent := schedulableIntent(t, "grant-account-"+strconv.FormatInt(accountID, 10), accountID, false,
				controlplane.ProducerAgentOperator, controlplane.AuthorityAdministratorCommand, *fixture.now, &expires)
			submissions = append(submissions, accountcontrol.Submission{CommandID: "grant-consumption-1", Intent: intent,
				RequestIdempotencyKey: "agent-command:grant-consumption-1", PersistOverride: true,
				Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		}
		for _, submission := range submissions {
			submission := submission
			go func() {
				<-start
				submitAsync(context.Background(), fixture.service, submission, results)
			}()
		}
		close(start)
		outcomes := []submitOutcome{<-results, <-results}
		conflicts := 0
		for _, outcome := range outcomes {
			var conflict *accountcontrol.IdempotencyConflictError
			if errors.As(outcome.err, &conflict) {
				conflicts++
			}
		}
		if conflicts != 1 {
			t.Fatalf("grant outcomes=%+v, want one conflict", outcomes)
		}
		if writes := fixture.api.Stats().ByName[testsupport.CallSetSchedulable]; writes != 1 {
			t.Fatalf("grant reuse wrote %d times", writes)
		}
	})
}

func TestNetworkWaitDoesNotHoldSQLiteTransaction(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	fixture.api.SetBeforeCall(func(call testsupport.Call) {
		if call.Name == testsupport.CallSetSchedulable {
			once.Do(func() { close(entered) })
			<-release
		}
	})
	intent := schedulableIntent(t, "db-not-held", 1, false, controlplane.ProducerAdminUI,
		controlplane.AuthorityManualHold, *fixture.now, nil)
	done := make(chan error, 1)
	go func() {
		_, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "db-not-held", Intent: intent,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		done <- err
	}()
	<-entered
	readCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	readDone := make(chan error, 1)
	go func() {
		_, err := fixture.db.Store.GetSettings(readCtx)
		readDone <- err
	}()
	if err := <-readDone; err != nil {
		close(release)
		<-done
		t.Fatalf("SQLite read blocked behind upstream request: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type submitOutcome struct {
	result accountcontrol.Result
	err    error
}

func submitAsync(ctx context.Context, service *accountcontrol.Service, submission accountcontrol.Submission, output chan<- submitOutcome) {
	result, err := service.Submit(ctx, submission)
	output <- submitOutcome{result: result, err: err}
}

func blockAccountWithLoadMutation(t *testing.T, fixture serviceFixture) (func(), <-chan error) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	fixture.api.SetBeforeCall(func(call testsupport.Call) {
		if call.Name == testsupport.CallUpdateLoadFactor {
			once.Do(func() { close(entered) })
			<-release
		}
	})
	expires := fixture.now.Add(time.Hour)
	intent := loadIntent(t, "account-lock-blocker", 1, 50, *fixture.now, expires)
	done := make(chan error, 1)
	go func() {
		_, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "account-lock-blocker", Intent: intent,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocker did not reach upstream load write")
	}
	return func() { close(release) }, done
}
