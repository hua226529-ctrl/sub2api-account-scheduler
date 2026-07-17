package accountcontrol_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

type repositoryWithLocks struct {
	accountcontrol.Repository
	balance map[int64]*model.BalanceLock
	cost    map[int64]*model.CostLock
}

func (r repositoryWithLocks) GetActiveBalanceLock(_ context.Context, accountID int64) (*model.BalanceLock, error) {
	return r.balance[accountID], nil
}

func (r repositoryWithLocks) GetActiveCostLock(_ context.Context, accountID int64) (*model.CostLock, error) {
	return r.cost[accountID], nil
}

type serviceFixture struct {
	service *accountcontrol.Service
	db      *testsupport.TempDatabase
	api     *testsupport.FakeSub2API
	now     *time.Time
}

func newServiceFixture(t *testing.T, accounts int) serviceFixture {
	t.Helper()
	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	fixed := time.Date(2026, 7, 17, 1, 2, 3, 0, time.UTC)
	fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: accounts, Monitors: accounts, Now: fixed})
	api := testsupport.NewFakeSub2API(fixture)
	var sequence atomic.Int64
	service := accountcontrol.New(database.Store, api, accountcontrol.WithClock(func() time.Time { return fixed }),
		accountcontrol.WithIDGenerator(func() (string, error) { return fmt.Sprintf("cmd-test-%d", sequence.Add(1)), nil }))
	return serviceFixture{service: service, db: database, api: api, now: &fixed}
}

func TestIdempotencyReplayAndConflict(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	intent := schedulableIntent(t, "manual-1", 1, false, controlplane.ProducerAdminUI, controlplane.AuthorityManualHold, *fixture.now, nil)
	submission := accountcontrol.Submission{CommandID: "manual-1", Intent: intent, PersistOverride: true,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}
	first, err := fixture.service.Submit(context.Background(), submission)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.service.Submit(context.Background(), submission)
	if err != nil {
		t.Fatal(err)
	}
	if first.MutationID != second.MutationID || !second.IdempotentReplay {
		t.Fatalf("idempotent replay mismatch: first=%+v second=%+v", first, second)
	}
	if writes := fixture.api.Stats().ByName[testsupport.CallSetSchedulable]; writes != 1 {
		t.Fatalf("idempotent request wrote %d times", writes)
	}
	conflict := intent
	conflict.DesiredState = schedulableIntent(t, "manual-1", 1, true, controlplane.ProducerAdminUI,
		controlplane.AuthorityManualHold, *fixture.now, nil).DesiredState
	_, err = fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "manual-1", Intent: conflict, PersistOverride: true,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
	var idempotencyConflict *accountcontrol.IdempotencyConflictError
	if !errors.As(err, &idempotencyConflict) {
		t.Fatalf("different semantics did not conflict: %v", err)
	}
}

func TestOverrideAuthorityAndExpiration(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	expires := fixture.now.Add(30 * time.Minute)
	admin := schedulableIntent(t, "admin-pause", 1, false, controlplane.ProducerAdminUI,
		controlplane.AuthorityAdministratorCommand, *fixture.now, &expires)
	if _, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "admin-pause", Intent: admin,
		PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}); err != nil {
		t.Fatal(err)
	}
	policyTime := fixture.now.Add(time.Minute)
	policy := schedulableIntent(t, "policy-resume-1", 1, true, controlplane.ProducerPolicyScheduler,
		controlplane.AuthorityActivePolicy, policyTime, nil)
	result, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "policy-resume-1", Intent: policy,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != accountcontrol.StatusSuperseded || result.WinningAuthority != controlplane.AuthorityAdministratorCommand || result.VerifiedAfter != nil {
		t.Fatalf("administrator override did not beat policy: %+v", result)
	}
	*fixture.now = expires.Add(time.Second)
	policy = schedulableIntent(t, "policy-resume-2", 1, true, controlplane.ProducerPolicyScheduler,
		controlplane.AuthorityActivePolicy, *fixture.now, nil)
	result, err = fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "policy-resume-2", Intent: policy,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
	if err != nil {
		t.Fatal(err)
	}
	if result.WinningAuthority != controlplane.AuthorityActivePolicy || result.VerifiedAfter == nil || !result.VerifiedAfter.Schedulable {
		t.Fatalf("expired override did not return control to policy: %+v", result)
	}
}

func TestLegacyOwnershipProjectionDoesNotCreateArbitrationCandidate(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	legacy := model.AccountControl{AccountID: 1, OwnsPause: true, Owner: "agent", ManualLocked: true,
		ManualOverrideUntil: timePointer(fixture.now.Add(time.Hour)), UpdatedAt: *fixture.now}
	if err := fixture.db.Store.UpsertControl(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}
	policy := schedulableIntent(t, "projection-is-not-override", 1, false, controlplane.ProducerPolicyScheduler,
		controlplane.AuthorityActivePolicy, *fixture.now, nil)
	result, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "projection-is-not-override",
		Intent: policy, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
	if err != nil || result.WinningAuthority != controlplane.AuthorityActivePolicy || result.Status != accountcontrol.StatusApplied {
		t.Fatalf("legacy projection affected arbitration: result=%+v err=%v", result, err)
	}
	overrides, err := fixture.db.Store.ListActiveAccountOverrides(context.Background(), 1,
		controlplane.OperationSetAccountSchedulable, *fixture.now)
	if err != nil || len(overrides) != 0 {
		t.Fatalf("runtime rebuilt an override from account_controls: overrides=%+v err=%v", overrides, err)
	}
}

func TestSafetyFreezeAndCredentialMatrix(t *testing.T) {
	t.Run("writes frozen blocks manual hold", func(t *testing.T) {
		fixture := newServiceFixture(t, 1)
		state := model.AgentFreezeState{ScopeType: "global", Mode: model.AgentFreezeModeWritesFrozen, Actor: "test", Reason: "incident"}
		if err := fixture.db.Store.SetAgentFreezeState(context.Background(), &state); err != nil {
			t.Fatal(err)
		}
		intent := schedulableIntent(t, "manual-frozen", 1, false, controlplane.ProducerAdminUI, controlplane.AuthorityManualHold, *fixture.now, nil)
		result, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "manual-frozen", Intent: intent,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		var blocked *accountcontrol.BlockedError
		if !errors.As(err, &blocked) || result.BlockedReason != accountcontrol.BlockWritesFrozen {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		if fixture.api.Stats().ByName[testsupport.CallSetSchedulable] != 0 {
			t.Fatal("frozen command reached write transport")
		}
	})

	t.Run("credential invalid allows pause but blocks resume and increase", func(t *testing.T) {
		database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
		now := time.Date(2026, 7, 17, 1, 2, 3, 0, time.UTC)
		load := 50
		api := testsupport.NewFakeSub2API(testsupport.Fixture{Accounts: []model.Account{{ID: 1, Status: "error", ErrorMessage: "credential rejected",
			Schedulable: true, LoadFactor: &load}}, History: map[int64][]model.MonitorHistoryRecord{}})
		var sequence atomic.Int64
		service := accountcontrol.New(database.Store, api, accountcontrol.WithClock(func() time.Time { return now }),
			accountcontrol.WithIDGenerator(func() (string, error) { return fmt.Sprintf("cmd-cred-%d", sequence.Add(1)), nil }))
		pause := schedulableIntent(t, "pause-invalid", 1, false, controlplane.ProducerAdminUI, controlplane.AuthorityManualHold, now, nil)
		if _, err := service.Submit(context.Background(), accountcontrol.Submission{CommandID: "pause-invalid", Intent: pause,
			Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}); err != nil {
			t.Fatal(err)
		}
		expires := now.Add(time.Hour)
		resume := schedulableIntent(t, "resume-invalid", 1, true, controlplane.ProducerAdminUI,
			controlplane.AuthorityAdministratorCommand, now.Add(time.Second), &expires)
		result, err := service.Submit(context.Background(), accountcontrol.Submission{CommandID: "resume-invalid", Intent: resume,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		if result.BlockedReason != accountcontrol.BlockCredentialInvalid || err == nil {
			t.Fatalf("resume result=%+v err=%v", result, err)
		}
		decrease := loadIntent(t, "decrease-invalid", 1, 25, now.Add(2*time.Second), expires)
		if _, err := service.Submit(context.Background(), accountcontrol.Submission{CommandID: "decrease-invalid", Intent: decrease,
			Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}); err != nil {
			t.Fatal(err)
		}
		increase := loadIntent(t, "increase-invalid", 1, 75, now.Add(3*time.Second), expires)
		result, err = service.Submit(context.Background(), accountcontrol.Submission{CommandID: "increase-invalid", Intent: increase,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		if result.BlockedReason != accountcontrol.BlockCredentialInvalid || err == nil {
			t.Fatalf("increase result=%+v err=%v", result, err)
		}
	})
}

func TestDifferentAccountsProceedWhileOneWriteIsBlocked(t *testing.T) {
	fixture := newServiceFixture(t, 2)
	enteredA := make(chan struct{})
	releaseA := make(chan struct{})
	var once sync.Once
	fixture.api.SetBeforeCall(func(call testsupport.Call) {
		if call.Name == testsupport.CallSetSchedulable && call.Resource == "1" {
			once.Do(func() { close(enteredA) })
			<-releaseA
		}
	})
	doneA := make(chan error, 1)
	go func() {
		intent := schedulableIntent(t, "parallel-a", 1, false, controlplane.ProducerAdminUI, controlplane.AuthorityManualHold, *fixture.now, nil)
		_, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "parallel-a", Intent: intent,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		doneA <- err
	}()
	select {
	case <-enteredA:
	case <-time.After(time.Second):
		t.Fatal("account A did not enter write")
	}
	doneB := make(chan error, 1)
	go func() {
		intent := schedulableIntent(t, "parallel-b", 2, false, controlplane.ProducerAdminUI, controlplane.AuthorityManualHold, *fixture.now, nil)
		_, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "parallel-b", Intent: intent,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		doneB <- err
	}()
	select {
	case err := <-doneB:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("account B was blocked by account A")
	}
	close(releaseA)
	if err := <-doneA; err != nil {
		t.Fatal(err)
	}
}

func TestSameAccountOperationsAreSerialized(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{}, 1)
	var calls atomic.Int32
	fixture.api.SetBeforeCall(func(call testsupport.Call) {
		if call.Name != testsupport.CallSetSchedulable {
			return
		}
		if calls.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
			return
		}
		secondEntered <- struct{}{}
	})
	done := make(chan error, 2)
	firstExpires := fixture.now.Add(time.Hour)
	go func() {
		intent := schedulableIntent(t, "serial-1", 1, false, controlplane.ProducerAdminUI, controlplane.AuthorityAdministratorCommand, *fixture.now, &firstExpires)
		_, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "serial-1", Intent: intent,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		done <- err
	}()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}
	created := fixture.now.Add(time.Second)
	expires := created.Add(time.Hour)
	secondStarted := make(chan struct{})
	go func() {
		close(secondStarted)
		intent := schedulableIntent(t, "serial-2", 1, true, controlplane.ProducerAdminUI, controlplane.AuthorityAdministratorCommand, created, &expires)
		_, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "serial-2", Intent: intent,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		done <- err
	}()
	<-secondStarted
	close(releaseFirst)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("second write did not run after first")
	}
	if got := fixture.api.Stats().MaxConcurrent; got != 1 {
		t.Fatalf("same-account writes overlapped: max concurrent calls = %d", got)
	}
}

func TestAmbiguousWriteUsesReadbackAndUncertainIsRecoverable(t *testing.T) {
	t.Run("write applied before timeout", func(t *testing.T) {
		fixture := newServiceFixture(t, 1)
		fixture.api.SetFailure(testsupport.CallSetSchedulable, testsupport.Failure{AtCall: 1, Err: io.EOF, ApplyBeforeError: true})
		intent := schedulableIntent(t, "timeout-applied", 1, false, controlplane.ProducerAdminUI, controlplane.AuthorityManualHold, *fixture.now, nil)
		result, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "timeout-applied", Intent: intent,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		if err != nil || result.Status != accountcontrol.StatusApplied {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})

	t.Run("readback unavailable remains uncertain", func(t *testing.T) {
		fixture := newServiceFixture(t, 1)
		fixture.api.SetFailure(testsupport.CallSetSchedulable, testsupport.Failure{AtCall: 1, Err: io.EOF})
		fixture.api.SetFailure(testsupport.CallListAccounts, testsupport.Failure{AtCall: 2, Err: io.EOF})
		intent := schedulableIntent(t, "timeout-unknown", 1, false, controlplane.ProducerAdminUI, controlplane.AuthorityManualHold, *fixture.now, nil)
		result, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "timeout-unknown", Intent: intent,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		if err == nil || !result.Uncertain || result.Status != accountcontrol.StatusUncertain {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		fixture.api.SetFailure(testsupport.CallListAccounts, testsupport.Failure{})
		fixture.api.SetFailure(testsupport.CallSetSchedulable, testsupport.Failure{})
		if err := fixture.service.ReconcilePendingAccountMutations(context.Background()); err != nil {
			t.Fatal(err)
		}
		items, err := fixture.db.Store.ListAccountMutations(context.Background(), 1)
		if err != nil || len(items) != 1 || items[0].Status != accountcontrol.StatusApplied {
			t.Fatalf("items=%+v err=%v", items, err)
		}
	})
}

func TestHardBlockedSubmittedOverrideIsNotActivated(t *testing.T) {
	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	now := time.Date(2026, 7, 17, 2, 0, 0, 0, time.UTC)
	api := testsupport.NewFakeSub2API(testsupport.Fixture{Accounts: []model.Account{{ID: 1, Status: "error",
		ErrorMessage: "credential invalid", Schedulable: false}}, History: map[int64][]model.MonitorHistoryRecord{}})
	service := accountcontrol.New(database.Store, api, accountcontrol.WithClock(func() time.Time { return now }))
	expires := now.Add(accountcontrol.DefaultAdministratorTTL)
	resume := schedulableIntent(t, "blocked-resume", 1, true, controlplane.ProducerAdminUI,
		controlplane.AuthorityAdministratorCommand, now, &expires)
	result, err := service.Submit(context.Background(), accountcontrol.Submission{CommandID: "blocked-resume", Intent: resume,
		PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
	var blocked *accountcontrol.BlockedError
	if !errors.As(err, &blocked) || result.BlockedReason != accountcontrol.BlockCredentialInvalid {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	override, err := service.FindActiveOverride(context.Background(), 1, controlplane.OperationSetAccountSchedulable,
		controlplane.AuthorityAdministratorCommand)
	if err != nil {
		t.Fatal(err)
	}
	if override != nil {
		t.Fatalf("hard-blocked resume left a hidden active override: %+v", override)
	}
	if api.Stats().ByName[testsupport.CallSetSchedulable] != 0 {
		t.Fatal("hard-blocked resume reached the write transport")
	}
}

func TestAgentFreezeStaleAndCooldownMatrix(t *testing.T) {
	t.Run("agent freeze only blocks AgentOperator", func(t *testing.T) {
		fixture := newServiceFixture(t, 1)
		state := model.AgentFreezeState{ScopeType: "global", Mode: model.AgentFreezeModeAgentPaused, Actor: "test"}
		if err := fixture.db.Store.SetAgentFreezeState(context.Background(), &state); err != nil {
			t.Fatal(err)
		}
		expires := fixture.now.Add(time.Hour)
		agent := schedulableIntent(t, "agent-frozen", 1, false, controlplane.ProducerAgentOperator,
			controlplane.AuthorityAutonomousAgent, *fixture.now, &expires)
		result, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "agent-frozen", Intent: agent,
			PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
		if result.BlockedReason != accountcontrol.BlockAgentWritesFrozen || err == nil {
			t.Fatalf("agent result=%+v err=%v", result, err)
		}
		admin := schedulableIntent(t, "admin-not-frozen", 1, false, controlplane.ProducerAdminUI,
			controlplane.AuthorityAdministratorCommand, fixture.now.Add(time.Second), &expires)
		if _, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "admin-not-frozen", Intent: admin,
			Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}); err != nil {
			t.Fatalf("direct administrator was incorrectly blocked: %v", err)
		}
	})

	for _, test := range []struct {
		name      string
		authority controlplane.Authority
		producer  controlplane.Producer
		fresh     bool
		cooldown  bool
		blocked   bool
	}{
		{"stale policy", controlplane.AuthorityActivePolicy, controlplane.ProducerPolicyScheduler, false, false, true},
		{"stale autonomous", controlplane.AuthorityAutonomousAgent, controlplane.ProducerAgentOperator, false, false, true},
		{"stale administrator", controlplane.AuthorityAdministratorCommand, controlplane.ProducerAdminUI, false, false, false},
		{"cooldown policy", controlplane.AuthorityActivePolicy, controlplane.ProducerPolicyScheduler, true, true, true},
		{"cooldown administrator", controlplane.AuthorityAdministratorCommand, controlplane.ProducerAdminUI, true, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newServiceFixture(t, 1)
			var expires *time.Time
			if test.authority != controlplane.AuthorityActivePolicy {
				value := fixture.now.Add(time.Hour)
				expires = &value
			}
			intent := schedulableIntent(t, "matrix-"+strings.ReplaceAll(test.name, " ", "-"), 1, false,
				test.producer, test.authority, *fixture.now, expires)
			result, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "matrix-" + test.name,
				Intent: intent, Safety: accountcontrol.SafetyContext{TelemetryFresh: test.fresh, CooldownActive: test.cooldown}})
			if test.blocked && err == nil {
				t.Fatalf("expected blocked result: %+v", result)
			}
			if !test.blocked && err != nil {
				t.Fatalf("unexpected block: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestBalanceAndCostLocksBlockResumeButAllowPause(t *testing.T) {
	for _, lockType := range []string{"balance", "cost"} {
		t.Run(lockType, func(t *testing.T) {
			fixture := newServiceFixture(t, 1)
			repository := repositoryWithLocks{Repository: fixture.db.Store, balance: map[int64]*model.BalanceLock{}, cost: map[int64]*model.CostLock{}}
			if lockType == "balance" {
				repository.balance[1] = &model.BalanceLock{AccountID: 1, SourceID: 10}
			} else {
				repository.cost[1] = &model.CostLock{AccountID: 1, SourceID: 11, Pool: "primary"}
			}
			service := accountcontrol.New(repository, fixture.api, accountcontrol.WithClock(func() time.Time { return *fixture.now }))
			expires := fixture.now.Add(time.Hour)
			pause := schedulableIntent(t, lockType+"-pause", 1, false, controlplane.ProducerAdminUI,
				controlplane.AuthorityAdministratorCommand, *fixture.now, &expires)
			if _, err := service.Submit(context.Background(), accountcontrol.Submission{CommandID: lockType + "-pause", Intent: pause,
				Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}); err != nil {
				t.Fatalf("safe pause was blocked: %v", err)
			}
			resume := schedulableIntent(t, lockType+"-resume", 1, true, controlplane.ProducerAdminUI,
				controlplane.AuthorityAdministratorCommand, fixture.now.Add(time.Second), &expires)
			result, err := service.Submit(context.Background(), accountcontrol.Submission{CommandID: lockType + "-resume", Intent: resume,
				Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
			expected := accountcontrol.BlockBalanceLocked
			if lockType == "cost" {
				expected = accountcontrol.BlockCostLocked
			}
			if err == nil || result.BlockedReason != expected {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestHealthLockBlocksResumeWithoutPersistingHiddenOverride(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	control, err := fixture.db.Store.GetControl(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	control.HealthLocked = true
	if err := fixture.db.Store.UpsertControl(context.Background(), control); err != nil {
		t.Fatal(err)
	}
	expires := fixture.now.Add(time.Hour)
	intent := schedulableIntent(t, "health-resume", 1, true, controlplane.ProducerAdminUI,
		controlplane.AuthorityAdministratorCommand, *fixture.now, &expires)
	result, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "health-resume", Intent: intent,
		PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
	if err == nil || result.BlockedReason != accountcontrol.BlockHealthLocked {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	overrides, err := fixture.db.Store.ListActiveAccountOverrides(context.Background(), 1, controlplane.OperationSetAccountSchedulable, *fixture.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(overrides) != 0 {
		t.Fatalf("blocked resume left a hidden override: %+v", overrides)
	}
	if fixture.api.Stats().ByName[testsupport.CallSetSchedulable] != 0 {
		t.Fatal("blocked resume reached the upstream")
	}
}

func TestUpstreamRateLimitWindowBlocksIncreaseButAllowsSafeReduction(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	until := fixture.now.Add(time.Hour)
	fixture.api.SetAccountRateLimit(1, &until)
	expires := fixture.now.Add(time.Hour)
	resume := schedulableIntent(t, "rate-limited-resume", 1, true, controlplane.ProducerAdminUI,
		controlplane.AuthorityAdministratorCommand, *fixture.now, &expires)
	result, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "rate-limited-resume", Intent: resume,
		PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true}})
	if err == nil || result.BlockedReason != accountcontrol.BlockRateLimited {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	decrease := loadIntent(t, "rate-limited-decrease", 1, 25, fixture.now.Add(time.Second), expires)
	if _, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "rate-limited-decrease", Intent: decrease,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}); err != nil {
		t.Fatalf("safe decrease was blocked: %v", err)
	}
}

func TestWriteVerificationMismatchRemainsUncertainAndReplayIsNotSuccess(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	fixture.api.SetStaleReads(true)
	intent := schedulableIntent(t, "stale-readback", 1, false, controlplane.ProducerAdminUI,
		controlplane.AuthorityManualHold, *fixture.now, nil)
	submission := accountcontrol.Submission{CommandID: "stale-readback", Intent: intent, PersistOverride: true,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}}
	result, err := fixture.service.Submit(context.Background(), submission)
	if err == nil || result.Status != accountcontrol.StatusUncertain || !result.Uncertain {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	replay, err := fixture.service.Submit(context.Background(), submission)
	var stateErr *accountcontrol.MutationStateError
	if !errors.As(err, &stateErr) || !replay.IdempotentReplay || replay.Status != accountcontrol.StatusUncertain {
		t.Fatalf("uncertain replay was reported as success: result=%+v err=%v", replay, err)
	}
	if fixture.api.Stats().ByName[testsupport.CallSetSchedulable] != 1 {
		t.Fatal("uncertain replay repeated the upstream write")
	}
}

func TestLocalCommitFailureRecoversWithoutRollback(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	raw, err := sql.Open("sqlite", fixture.db.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TRIGGER fail_manual_pause_event BEFORE INSERT ON events
		WHEN NEW.type='manual_pause' BEGIN SELECT RAISE(FAIL,'forced event commit failure'); END`); err != nil {
		t.Fatal(err)
	}
	intent := schedulableIntent(t, "commit-failure", 1, false, controlplane.ProducerAdminUI,
		controlplane.AuthorityManualHold, *fixture.now, nil)
	result, err := fixture.service.Submit(context.Background(), accountcontrol.Submission{CommandID: "commit-failure", Intent: intent,
		PersistOverride: true, Safety: accountcontrol.SafetyContext{TelemetryFresh: true},
		Event: model.Event{Type: "manual_pause", Severity: "warning", Message: "test"}})
	if err == nil || !result.Uncertain || result.Status != accountcontrol.StatusVerifying {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if fixture.api.Stats().ByName[testsupport.CallSetSchedulable] != 1 {
		t.Fatal("unexpected rollback write after local commit failure")
	}
	if _, err := raw.Exec(`DROP TRIGGER fail_manual_pause_event`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.Reopen(); err != nil {
		t.Fatal(err)
	}
	service := accountcontrol.New(fixture.db.Store, fixture.api, accountcontrol.WithClock(func() time.Time { return *fixture.now }))
	if err := service.ReconcilePendingAccountMutations(context.Background()); err != nil {
		t.Fatal(err)
	}
	items, err := fixture.db.Store.ListAccountMutations(context.Background(), 1)
	if err != nil || len(items) != 1 || items[0].Status != accountcontrol.StatusApplied || items[0].CompletedAt == nil {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if fixture.api.Stats().ByName[testsupport.CallSetSchedulable] != 1 {
		t.Fatal("startup recovery repeated an already applied write")
	}
}

func TestRecoveryLifecycleStatesAndDivergence(t *testing.T) {
	for _, status := range []accountcontrol.MutationStatus{accountcontrol.StatusPrepared, accountcontrol.StatusExecuting, accountcontrol.StatusVerifying} {
		t.Run(string(status), func(t *testing.T) {
			fixture := newServiceFixture(t, 1)
			intent := schedulableIntent(t, "recover-"+string(status), 1, false, controlplane.ProducerAdminUI,
				controlplane.AuthorityAdministratorCommand, *fixture.now, timePointer(fixture.now.Add(time.Hour)))
			mutation := mutationForRecovery(t, intent, status)
			if status != accountcontrol.StatusPrepared {
				load := 100
				mutation.Before = &accountcontrol.AccountState{Schedulable: true, LoadFactor: &load}
				mutation.AttemptCount = 1
			}
			if status == accountcontrol.StatusVerifying {
				if _, err := fixture.api.SetSchedulable(context.Background(), 1, false); err != nil {
					t.Fatal(err)
				}
				fixture.api.ResetStats()
			}
			if _, _, err := fixture.db.Store.PrepareAccountMutation(context.Background(), mutation, nil); err != nil {
				t.Fatal(err)
			}
			if err := fixture.service.ReconcilePendingAccountMutations(context.Background()); err != nil {
				t.Fatal(err)
			}
			items, err := fixture.db.Store.ListAccountMutations(context.Background(), 1)
			if err != nil || len(items) != 1 || items[0].Status != accountcontrol.StatusApplied || items[0].CompletedAt == nil {
				t.Fatalf("items=%+v err=%v", items, err)
			}
			expectedWrites := 1
			if status == accountcontrol.StatusVerifying {
				expectedWrites = 0
			}
			if fixture.api.Stats().ByName[testsupport.CallSetSchedulable] != expectedWrites {
				t.Fatalf("status %s wrote an unexpected number of times", status)
			}
		})
	}

	t.Run("diverged uncertain is not replayed", func(t *testing.T) {
		fixture := newServiceFixture(t, 1)
		fifty := 50
		if _, err := fixture.api.UpdateLoadFactor(context.Background(), 1, &fifty); err != nil {
			t.Fatal(err)
		}
		fixture.api.ResetStats()
		expires := fixture.now.Add(time.Hour)
		intent := loadIntent(t, "recover-diverged", 1, 25, *fixture.now, expires)
		mutation := mutationForRecovery(t, intent, accountcontrol.StatusUncertain)
		hundred := 100
		mutation.Before = &accountcontrol.AccountState{Schedulable: true, LoadFactor: &hundred}
		mutation.AttemptCount = 1
		if _, _, err := fixture.db.Store.PrepareAccountMutation(context.Background(), mutation, nil); err != nil {
			t.Fatal(err)
		}
		if err := fixture.service.ReconcilePendingAccountMutations(context.Background()); err != nil {
			t.Fatal(err)
		}
		items, _ := fixture.db.Store.ListAccountMutations(context.Background(), 1)
		if len(items) != 1 || items[0].Status != accountcontrol.StatusUncertain ||
			fixture.api.Stats().ByName[testsupport.CallUpdateLoadFactor] != 0 {
			t.Fatalf("diverged mutation was replayed: %+v", items)
		}
	})
}

func TestSameAccountRecoveryOperationsAreSerialized(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	expires := fixture.now.Add(time.Hour)
	sched := schedulableIntent(t, "recover-serial-sched", 1, false, controlplane.ProducerAdminUI,
		controlplane.AuthorityAdministratorCommand, *fixture.now, &expires)
	load := loadIntent(t, "recover-serial-load", 1, 25, fixture.now.Add(time.Second), expires)
	for _, mutation := range []accountcontrol.Mutation{
		mutationForRecovery(t, sched, accountcontrol.StatusExecuting),
		mutationForRecovery(t, load, accountcontrol.StatusExecuting),
	} {
		load := 100
		mutation.Before = &accountcontrol.AccountState{Schedulable: true, LoadFactor: &load}
		if _, _, err := fixture.db.Store.PrepareAccountMutation(context.Background(), mutation, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.service.ReconcilePendingAccountMutations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := fixture.api.Stats().MaxConcurrent; got != 1 {
		t.Fatalf("same-account recovery calls overlapped: max concurrent=%d", got)
	}
}

func TestRecoveryWaitersForOneAccountDoNotConsumeOtherAccountCapacity(t *testing.T) {
	fixture := newServiceFixture(t, 2)
	expires := fixture.now.Add(time.Hour)
	beforeLoad := 100
	for i := range 5 {
		intent := schedulableIntent(t, fmt.Sprintf("recover-account-one-%d", i), 1, false, controlplane.ProducerAdminUI,
			controlplane.AuthorityAdministratorCommand, fixture.now.Add(time.Duration(i)*time.Second), &expires)
		mutation := mutationForRecovery(t, intent, accountcontrol.StatusExecuting)
		mutation.Before = &accountcontrol.AccountState{Schedulable: true, LoadFactor: &beforeLoad}
		if _, _, err := fixture.db.Store.PrepareAccountMutation(context.Background(), mutation, nil); err != nil {
			t.Fatal(err)
		}
	}
	enteredAccountOne := make(chan struct{})
	releaseAccountOne := make(chan struct{})
	accountTwoWritten := make(chan struct{})
	var onceOne, onceTwo sync.Once
	fixture.api.SetBeforeCall(func(call testsupport.Call) {
		if call.Name != testsupport.CallSetSchedulable {
			return
		}
		switch call.Resource {
		case "1":
			onceOne.Do(func() { close(enteredAccountOne) })
			<-releaseAccountOne
		case "2":
			onceTwo.Do(func() { close(accountTwoWritten) })
		}
	})
	doneOne := make(chan error, 1)
	go func() { doneOne <- fixture.service.ReconcilePendingAccountMutations(context.Background()) }()
	select {
	case <-enteredAccountOne:
	case <-time.After(time.Second):
		t.Fatal("account one recovery did not reach the blocked write")
	}
	other := schedulableIntent(t, "recover-account-two", 2, false, controlplane.ProducerAdminUI,
		controlplane.AuthorityAdministratorCommand, *fixture.now, &expires)
	otherMutation := mutationForRecovery(t, other, accountcontrol.StatusExecuting)
	otherMutation.Before = &accountcontrol.AccountState{Schedulable: true, LoadFactor: &beforeLoad}
	if _, _, err := fixture.db.Store.PrepareAccountMutation(context.Background(), otherMutation, nil); err != nil {
		t.Fatal(err)
	}
	doneTwo := make(chan error, 1)
	go func() { doneTwo <- fixture.service.ReconcilePendingAccountMutations(context.Background()) }()
	select {
	case <-accountTwoWritten:
	case <-time.After(time.Second):
		close(releaseAccountOne)
		<-doneOne
		<-doneTwo
		t.Fatal("same-account recovery waiters blocked recovery for account two")
	}
	close(releaseAccountOne)
	if err := <-doneOne; err != nil {
		t.Fatal(err)
	}
	if err := <-doneTwo; err != nil {
		t.Fatal(err)
	}
	stats := fixture.api.Stats()
	if stats.MaxConcurrent < 2 || stats.MaxConcurrent > 4 {
		t.Fatalf("unexpected bounded recovery concurrency: %d", stats.MaxConcurrent)
	}
}

func TestRecoveryDoesNotReplayExpiredPendingOverride(t *testing.T) {
	fixture := newServiceFixture(t, 1)
	expires := fixture.now.Add(time.Minute)
	intent := schedulableIntent(t, "recover-expired", 1, false, controlplane.ProducerAdminUI,
		controlplane.AuthorityAdministratorCommand, *fixture.now, &expires)
	mutation := mutationForRecovery(t, intent, accountcontrol.StatusPrepared)
	beforeLoad := 100
	mutation.Before = &accountcontrol.AccountState{Schedulable: true, LoadFactor: &beforeLoad}
	if _, _, err := fixture.db.Store.PrepareAccountMutation(context.Background(), mutation, nil); err != nil {
		t.Fatal(err)
	}
	*fixture.now = expires.Add(time.Second)
	if err := fixture.service.ReconcilePendingAccountMutations(context.Background()); err != nil {
		t.Fatal(err)
	}
	items, err := fixture.db.Store.ListAccountMutations(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Status != accountcontrol.StatusExpired || items[0].CompletedAt == nil {
		t.Fatalf("expired mutation was not finalized: %+v", items)
	}
	if fixture.api.Stats().ByName[testsupport.CallSetSchedulable] != 0 {
		t.Fatal("expired pending override was written during recovery")
	}
}

func mutationForRecovery(t *testing.T, intent controlplane.Intent, status accountcontrol.MutationStatus) accountcontrol.Mutation {
	t.Helper()
	accountID, _ := intent.Resource.AccountID()
	signature, err := controlplane.SemanticSignature(intent)
	if err != nil {
		t.Fatal(err)
	}
	mutation := accountcontrol.Mutation{ID: "mutation-" + intent.ID, CommandID: intent.IdempotencyKey,
		IntentID: intent.ID, IdempotencyKey: intent.IdempotencyKey, SemanticSignature: signature, AccountID: accountID,
		Operation: intent.Operation, Producer: intent.Producer, Authority: intent.Authority, Actor: intent.Actor, Reason: intent.Reason,
		PolicyVersion: intent.PolicyVersion, SnapshotVersion: intent.SnapshotVersion, ExpiresAt: intent.ExpiresAt,
		WinningIntentID: intent.ID, WinningIdempotencyKey: intent.IdempotencyKey, WinningProducer: intent.Producer,
		WinningAuthority: intent.Authority, WinningActor: intent.Actor, WinningReason: intent.Reason,
		WinningEvidenceRefs: append([]string(nil), intent.EvidenceRefs...), WinningPolicyVersion: intent.PolicyVersion,
		WinningSnapshotVersion: intent.SnapshotVersion, WinningCreatedAt: intent.CreatedAt, WinningExpiresAt: intent.ExpiresAt,
		Status: status, TelemetryFresh: true, CreatedAt: intent.CreatedAt, UpdatedAt: intent.CreatedAt}
	if desired, ok := intent.DesiredState.Schedulable(); ok {
		mutation.RequestedSchedulable = &desired
		mutation.WinningSchedulable = &desired
	} else if desired, configured, ok := intent.DesiredState.LoadFactor(); ok {
		mutation.RequestedLoadSet = configured
		mutation.WinningLoadSet = configured
		if configured {
			mutation.RequestedLoadFactor = &desired
			mutation.WinningLoadFactor = &desired
		}
	}
	return mutation
}

func timePointer(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}

func schedulableIntent(t *testing.T, source string, accountID int64, desired bool, producer controlplane.Producer,
	authority controlplane.Authority, created time.Time, expires *time.Time) controlplane.Intent {
	t.Helper()
	metadata := controlplane.IntentMetadata{ID: "intent-" + source, IdempotencyKey: "idem-" + source,
		Producer: producer, Authority: authority, Actor: "test", Reason: source, CreatedAt: created, ExpiresAt: expires}
	if authority == controlplane.AuthorityActivePolicy {
		metadata.PolicyVersion, metadata.SnapshotVersion = "policy-1", "snapshot-"+source
	}
	if authority == controlplane.AuthorityAutonomousAgent {
		metadata.SnapshotVersion, metadata.EvidenceRefs = "snapshot-"+source, []string{"evidence-" + source}
	}
	intent, err := controlplane.NewAccountSchedulableIntent(metadata, accountID, desired)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func loadIntent(t *testing.T, source string, accountID int64, desired int, created time.Time, expires time.Time) controlplane.Intent {
	t.Helper()
	intent, err := controlplane.NewAccountLoadFactorIntent(controlplane.IntentMetadata{ID: "intent-" + source, IdempotencyKey: "idem-" + source,
		Producer: controlplane.ProducerAdminUI, Authority: controlplane.AuthorityAdministratorCommand, Actor: "test", Reason: source,
		CreatedAt: created, ExpiresAt: &expires}, accountID, &desired)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}
