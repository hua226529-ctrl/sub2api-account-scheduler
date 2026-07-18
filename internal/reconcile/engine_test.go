package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

type fakeAPI struct {
	mu           sync.Mutex
	listStarted  chan time.Time
	monitors     []model.Monitor
	accounts     []model.Account
	actions      []bool
	actionIDs    []int64
	loadActions  []*int
	setError     error
	scheduleErr  error
	loadErr      error
	scheduleErrs []error
	loadErrs     []error
}

func (f *fakeAPI) ListMonitors(context.Context) ([]model.Monitor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listStarted != nil {
		select {
		case f.listStarted <- time.Now():
		default:
		}
	}
	return append([]model.Monitor(nil), f.monitors...), nil
}

func (f *fakeAPI) ListAccounts(context.Context) ([]model.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.Account(nil), f.accounts...), nil
}

func (f *fakeAPI) SetSchedulable(_ context.Context, id int64, value bool) (model.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actions = append(f.actions, value)
	f.actionIDs = append(f.actionIDs, id)
	if len(f.scheduleErrs) > 0 {
		err := f.scheduleErrs[0]
		f.scheduleErrs = f.scheduleErrs[1:]
		if err != nil {
			return model.Account{}, err
		}
	}
	if f.scheduleErr != nil {
		return model.Account{}, f.scheduleErr
	}
	if f.setError != nil {
		return model.Account{}, f.setError
	}
	for i := range f.accounts {
		if f.accounts[i].ID == id {
			f.accounts[i].Schedulable = value
			return f.accounts[i], nil
		}
	}
	return model.Account{}, nil
}

func (f *fakeAPI) UpdateLoadFactor(_ context.Context, id int64, value *int) (model.Account, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadActions = append(f.loadActions, cloneIntPointer(value))
	if len(f.loadErrs) > 0 {
		err := f.loadErrs[0]
		f.loadErrs = f.loadErrs[1:]
		if err != nil {
			return model.Account{}, err
		}
	}
	if f.loadErr != nil {
		return model.Account{}, f.loadErr
	}
	if f.setError != nil {
		return model.Account{}, f.setError
	}
	for i := range f.accounts {
		if f.accounts[i].ID == id {
			f.accounts[i].LoadFactor = cloneIntPointer(value)
			return f.accounts[i], nil
		}
	}
	return model.Account{}, nil
}

func (f *fakeAPI) setMonitorResult(status string, checkedAt time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.monitors[0].PrimaryStatus = status
	f.monitors[0].LastCheckedAt = &checkedAt
}

func (f *fakeAPI) setMonitorHealth(status string, latencyMS int64, checkedAt time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.monitors[0].PrimaryStatus = status
	f.monitors[0].PrimaryLatencyMS = latencyMS
	f.monitors[0].LastCheckedAt = &checkedAt
}

func newEngineTest(t *testing.T, dryRun bool) (*Engine, *store.Store, *fakeAPI) {
	t.Helper()
	engine, database, api, _ := newEngineTestWithPath(t, dryRun)
	return engine, database, api
}

func newEngineTestWithPath(t *testing.T, dryRun bool) (*Engine, *store.Store, *fakeAPI, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "scheduler.db")
	database, err := store.Open(path, model.Settings{
		DryRun: dryRun, FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
		HealthMode: model.HealthModeLegacy,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	now := time.Now().UTC()
	api := &fakeAPI{
		monitors: []model.Monitor{{ID: 2, Name: "monitor", Provider: "openai", Endpoint: "https://upstream.example", PrimaryModel: "gpt", Enabled: true, IntervalSeconds: 60, LastCheckedAt: &now, PrimaryStatus: model.StatusOperational}},
		accounts: []model.Account{{ID: 225, Name: "account", Platform: "openai", Type: "apikey", Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": "https://upstream.example/v1"}}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewEngine(api, database, 50*time.Second, logger), database, api, path
}

func newTargetedEngineTest(t *testing.T) (*Engine, *store.Store, *fakeAPI) {
	t.Helper()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 1, RecoveryThreshold: 1, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
		HealthMode: model.HealthModeLegacy,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	now := time.Now().UTC()
	api := &fakeAPI{
		monitors: []model.Monitor{
			{ID: 1, Name: "one", Provider: "openai", Endpoint: "https://one.example", PrimaryModel: "gpt", Enabled: true, LastCheckedAt: &now, PrimaryStatus: model.StatusFailed},
			{ID: 2, Name: "two", Provider: "openai", Endpoint: "https://two.example", PrimaryModel: "gpt", Enabled: true, LastCheckedAt: &now, PrimaryStatus: model.StatusFailed},
		},
		accounts: []model.Account{
			{ID: 101, Name: "one", Platform: "openai", Type: "apikey", Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": "https://one.example/v1"}},
			{ID: 102, Name: "two", Platform: "openai", Type: "apikey", Status: "active", Schedulable: true, Credentials: map[string]any{"base_url": "https://two.example/v1"}},
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewEngine(api, database, 50*time.Second, logger), database, api
}

func TestReconcileAccountsOnlyAllowsTargetAccountsToMutate(t *testing.T) {
	engine, _, api := newTargetedEngineTest(t)
	if err := engine.ReconcileAccounts(context.Background(), []int64{101}); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.actionIDs) != 1 || api.actionIDs[0] != 101 {
		t.Fatalf("targeted reconcile wrote non-target accounts: %#v", api.actionIDs)
	}
}

func TestReconcileAccountsContinuesAfterOneTargetFails(t *testing.T) {
	engine, _, api := newTargetedEngineTest(t)
	api.scheduleErrs = []error{errors.New("first target rejected"), nil}
	err := engine.ReconcileAccounts(context.Background(), []int64{101, 102})
	if err == nil {
		t.Fatal("target failure was not reported")
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.actionIDs) != 2 || api.actionIDs[0] != 101 || api.actionIDs[1] != 102 {
		t.Fatalf("second target did not continue after the first failed: %#v", api.actionIDs)
	}
}

func TestPolicyCommitQueuesTargetedReconcileAfterPersistence(t *testing.T) {
	engine, database, _ := newEngineTest(t, false)
	policy := model.Policy{AccountID: 225, Enabled: true}
	if err := engine.UpdatePolicy(context.Background(), policy, "web"); err != nil {
		t.Fatal(err)
	}
	persisted, err := database.ListPolicies(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := persisted[225]; !ok {
		t.Fatal("policy trigger was queued before the policy commit became durable")
	}
	full, ids, sources, _, _, _ := engine.coordinator.takePending()
	if full || len(ids) != 1 || ids[0] != 225 {
		t.Fatalf("account policy did not queue one targeted pass: full=%v ids=%#v", full, ids)
	}
	if len(sources) != 1 || sources[0] != "policy_update" {
		t.Fatalf("policy trigger source was not retained: %#v", sources)
	}
}

func TestPolicyCommitStartsCoordinatorReconcileWithinLatencyTarget(t *testing.T) {
	engine, _, api := newEngineTest(t, false)
	api.listStarted = make(chan time.Time, 1)
	engine.coordinator.interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go engine.coordinator.Run(ctx)
	if err := engine.UpdatePolicy(ctx, model.Policy{AccountID: 225, Enabled: true}, "web"); err != nil {
		t.Fatal(err)
	}
	committed := time.Now()
	select {
	case started := <-api.listStarted:
		latency := started.Sub(committed)
		t.Logf("policy commit to reconcile start: %v", latency)
		if latency >= 500*time.Millisecond {
			t.Fatalf("policy reconcile start exceeded target: %v", latency)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("policy commit did not start reconcile within 500ms")
	}
}

func failEventCommit(t *testing.T, path, eventType string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	statement := fmt.Sprintf(`CREATE TRIGGER fail_%s BEFORE INSERT ON events
		WHEN NEW.type = %q BEGIN SELECT RAISE(ABORT, 'forced event commit failure'); END`, eventType, eventType)
	if _, err := database.Exec(statement); err != nil {
		t.Fatalf("install commit failure trigger: %v", err)
	}
}

func applyChecks(t *testing.T, engine *Engine, api *fakeAPI, status string, base *time.Time, count int) {
	t.Helper()
	for range count {
		*base = base.Add(time.Second)
		api.setMonitorResult(status, *base)
		if err := engine.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func applyV3Checks(t *testing.T, engine *Engine, database *store.Store, api *fakeAPI, status string, latencyMS int64, errorClass string, base *time.Time, count int) {
	t.Helper()
	for range count {
		*base = base.Add(time.Second)
		api.setMonitorHealth(status, latencyMS, *base)
		inserted, err := database.InsertMonitorHistory(context.Background(), model.MonitorHistoryRecord{
			SourceID: base.UnixNano(), MonitorID: 2, Model: "gpt", Status: status,
			LatencyMS: latencyMS, ErrorClass: errorClass, CheckedAt: *base, IngestedAt: *base,
		})
		if err != nil || !inserted {
			t.Fatalf("insert V3 monitor evidence: inserted=%v err=%v", inserted, err)
		}
		if err := engine.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCharacterizationAgentCannotTakeOwnershipOfManualPause(t *testing.T) {
	engine, database, api := newEngineTest(t, false)
	ctx := context.Background()
	if err := engine.ManualPause(ctx, 225, "web"); err != nil {
		t.Fatal(err)
	}
	if err := engine.AgentPause(ctx, 225, "agent:1", "模型建议暂停"); err == nil {
		t.Fatal("agent pause should reject a manually paused account")
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.Owner != "operator" || !control.ManualLocked {
		t.Fatalf("manual ownership was overwritten: %+v", control)
	}
	if len(api.actions) != 1 || api.actions[0] {
		t.Fatalf("unexpected external writes: %v", api.actions)
	}
}

func TestCharacterizationAgentResumeRequiresAgentOwnershipAndNoLocks(t *testing.T) {
	engine, database, api := newEngineTest(t, false)
	ctx := autonomousAgentTestContext("agent-pause", time.Now().UTC())
	if err := engine.AgentPause(ctx, 225, "agent:2", "风险证据充分"); err != nil {
		t.Fatal(err)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	control.HealthLocked = true
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := engine.AgentResume(autonomousAgentTestContext("agent-resume-blocked", time.Now().UTC()), 225, "agent:3", "尝试恢复"); err == nil {
		t.Fatal("agent resume should reject an active health lock")
	}
	if len(api.actions) != 1 || api.actions[0] {
		t.Fatalf("locked resume reached Sub2API: %v", api.actions)
	}
	control.HealthLocked = false
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := engine.AgentResume(autonomousAgentTestContext("agent-resume", time.Now().UTC().Add(time.Second)), 225, "agent:4", "控制锁均已解除"); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 2 || !api.actions[1] {
		t.Fatalf("expected confirmed resume: %v", api.actions)
	}
}

func TestCharacterizationReconcileCountsOnlyDistinctChecksAndPausesAfterThreeFailures(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	base := time.Now().UTC()
	api.setMonitorResult(model.StatusFailed, base)
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	state, _ := database.GetMonitorState(ctx, 2)
	if state.UnhealthyStreak != 1 {
		t.Fatalf("duplicate check counted twice: %+v", state)
	}
	for i := 1; i < 3; i++ {
		api.setMonitorResult(model.StatusError, base.Add(time.Duration(i)*time.Minute))
		if err := engine.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(api.actions) != 1 || api.actions[0] {
		t.Fatalf("expected one pause action, got %v", api.actions)
	}
	control, _ := database.GetControl(ctx, 225)
	if !control.OwnsPause || control.Owner != "automatic" {
		t.Fatalf("pause ownership not persisted: %+v", control)
	}
}

func TestCharacterizationReconcileRecoversOnlyOwnedPauseAfterThreeHealthyChecks(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	base := time.Now().UTC()
	for i := 0; i < 3; i++ {
		api.setMonitorResult(model.StatusFailed, base.Add(time.Duration(i)*time.Minute))
		if err := engine.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := 3; i < 6; i++ {
		api.setMonitorResult(model.StatusOperational, base.Add(time.Duration(i)*time.Minute))
		if err := engine.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(api.actions) != 2 || api.actions[0] || !api.actions[1] {
		t.Fatalf("expected pause then resume, got %v", api.actions)
	}
	control, _ := database.GetControl(ctx, 225)
	if control.OwnsPause {
		t.Fatalf("pause ownership should be cleared: %+v", control)
	}
}

func TestCharacterizationDegradedAndStaleMonitorNeverPause(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	api.setMonitorResult(model.StatusDegraded, time.Now().UTC())
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	api.setMonitorResult(model.StatusFailed, time.Now().UTC().Add(-10*time.Minute))
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 0 {
		t.Fatalf("degraded or stale monitor acted on account: %v", api.actions)
	}
	state, _ := database.GetMonitorState(ctx, 2)
	if state.Phase != model.PhaseFrozen {
		t.Fatalf("stale monitor should freeze: %+v", state)
	}
}

func TestCharacterizationBalanceAndHealthLocksMustBothClearBeforeResume(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	source, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{Name: "balance", Provider: "newapi", BaseURL: "https://upstream.example", NormalizedURL: "https://upstream.example", CredentialNonce: []byte{1}, CredentialCiphertext: []byte{1}, PauseBelow: 5, ResumeAt: 10, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SyncBalanceLocks(ctx, source.ID, []int64{225}, true); err != nil {
		t.Fatal(err)
	}
	checkedAt := time.Now().UTC()
	api.setMonitorResult(model.StatusOperational, checkedAt)
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 1 || api.actions[0] {
		t.Fatalf("balance lock did not pause account: %v", api.actions)
	}
	applyChecks(t, engine, api, model.StatusFailed, &checkedAt, 3)
	if err := database.SyncBalanceLocks(ctx, source.ID, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 1 {
		t.Fatalf("account resumed while health lock remained: %v", api.actions)
	}
	applyChecks(t, engine, api, model.StatusOperational, &checkedAt, 3)
	if len(api.actions) != 2 || !api.actions[1] {
		t.Fatalf("account did not resume after both locks cleared: %v", api.actions)
	}
	events, _ := database.ListEvents(ctx, 20)
	if !containsEvent(events, "balance_pause") || !containsEvent(events, "automatic_resume") {
		t.Fatalf("balance control actions were not audited: %+v", events)
	}
}

func TestManualResumeDoesNotBypassHealthLock(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	base := time.Now().UTC()
	for i := 0; i < 3; i++ {
		api.setMonitorResult(model.StatusFailed, base.Add(time.Duration(i)*time.Minute))
		if err := engine.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := engine.ManualResume(ctx, 225, "web"); err == nil {
		t.Fatal("manual resume bypassed an active health lock")
	}
	control, _ := database.GetControl(ctx, 225)
	if control.ManualOverrideUntil != nil || !control.HealthLocked || !control.OwnsPause {
		t.Fatalf("blocked manual resume changed protected control state: %+v", control)
	}
	if len(api.actions) != 1 || api.actions[0] {
		t.Fatalf("blocked manual resume reached upstream: %v", api.actions)
	}
}

func TestOwnedPauseDoesNotRecoverInvalidAccount(t *testing.T) {
	ctx := context.Background()
	engine, _, api := newEngineTest(t, false)
	base := time.Now().UTC()
	for i := 0; i < 3; i++ {
		api.setMonitorResult(model.StatusFailed, base.Add(time.Duration(i)*time.Minute))
		if err := engine.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	api.mu.Lock()
	api.accounts[0].Status = "error"
	api.accounts[0].ErrorMessage = "credential rejected"
	api.mu.Unlock()
	for i := 3; i < 6; i++ {
		api.setMonitorResult(model.StatusOperational, base.Add(time.Duration(i)*time.Minute))
		if err := engine.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(api.actions) != 1 || api.actions[0] {
		t.Fatalf("invalid account must stay paused, actions=%v", api.actions)
	}
}

func TestThirdAutomaticPauseLocksRecoveryUntilTenthDistinctSuccess(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	checkedAt := time.Now().UTC()

	for cycle := 0; cycle < 2; cycle++ {
		applyChecks(t, engine, api, model.StatusFailed, &checkedAt, 3)
		applyChecks(t, engine, api, model.StatusOperational, &checkedAt, 3)
	}
	applyChecks(t, engine, api, model.StatusError, &checkedAt, 3)

	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if !control.FlapActive || control.FlapRecoveryRequired != 10 {
		t.Fatalf("third pause did not activate 10-check protection: %+v", control)
	}
	if len(api.actions) != 5 {
		t.Fatalf("expected pause/resume/pause/resume/pause, got %v", api.actions)
	}

	applyChecks(t, engine, api, model.StatusOperational, &checkedAt, 9)
	if len(api.actions) != 5 {
		t.Fatalf("account recovered before tenth success: %v", api.actions)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 5 {
		t.Fatalf("duplicate monitor result counted as success: %v", api.actions)
	}
	applyChecks(t, engine, api, model.StatusOperational, &checkedAt, 1)
	if len(api.actions) != 6 || !api.actions[5] {
		t.Fatalf("tenth success did not recover account: %v", api.actions)
	}
	control, _ = database.GetControl(ctx, 225)
	if control.FlapActive || control.OwnsPause {
		t.Fatalf("successful protected recovery did not clear latch: %+v", control)
	}
}

func TestDegradedResetsProtectedRecoveryProgress(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	checkedAt := time.Now().UTC()
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.mu.Unlock()
	falseValue := false
	now := time.Now().UTC()
	control := model.AccountControl{
		AccountID: 225, MonitorID: int64Ptr(2), OwnsPause: true, Owner: "automatic",
		ExpectedSchedulable: &falseValue, LastObserved: &falseValue,
		HealthLocked: true, FlapActive: true, FlapTriggeredAt: &now, FlapRecoveryRequired: 10,
	}
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	applyChecks(t, engine, api, model.StatusOperational, &checkedAt, 6)
	applyChecks(t, engine, api, model.StatusDegraded, &checkedAt, 1)
	applyChecks(t, engine, api, model.StatusOperational, &checkedAt, 9)
	if len(api.actions) != 0 {
		t.Fatalf("degraded result did not reset recovery progress: %v", api.actions)
	}
	applyChecks(t, engine, api, model.StatusOperational, &checkedAt, 1)
	if len(api.actions) != 1 || !api.actions[0] {
		t.Fatalf("ten fresh operational checks should recover: %v", api.actions)
	}
}

func TestFailedPauseWriteDoesNotCountTowardFlapping(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	api.setError = errors.New("upstream write failed")
	checkedAt := time.Now().UTC()
	for i := 0; i < 3; i++ {
		checkedAt = checkedAt.Add(time.Second)
		api.setMonitorResult(model.StatusFailed, checkedAt)
		err := engine.Reconcile(ctx)
		if i == 2 && err == nil {
			t.Fatal("expected pause write failure")
		}
	}
	count, err := database.CountAutomaticPauses(ctx, 225, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed write was counted as automatic pause: %d", count)
	}
}

func TestManualResumeTemporarilyOverridesButDoesNotDeleteFlapProtection(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.mu.Unlock()
	falseValue := false
	control := model.AccountControl{AccountID: 225, OwnsPause: true, Owner: "automatic", ExpectedSchedulable: &falseValue, FlapActive: true, FlapRecoveryRequired: 10}
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := engine.ManualResume(ctx, 225, "web"); err != nil {
		t.Fatal(err)
	}
	control, _ = database.GetControl(ctx, 225)
	if control.OwnsPause || !control.FlapActive || control.FlapRecoveryRequired != 10 || control.ManualOverrideUntil == nil {
		t.Fatalf("manual resume did not preserve the business cooldown while applying a temporary override: %+v", control)
	}
	events, _ := database.ListEvents(ctx, 20)
	if !containsEvent(events, "manual_resume") {
		t.Fatal("manual temporary override was not audited")
	}
}

func TestEffectiveFlapPolicyUsesAccountOverridesAndStrongerRecovery(t *testing.T) {
	disabled := false
	window, pauses, recovery := 90, 4, 8
	settings := model.Settings{RecoveryThreshold: 12, FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10}
	policy := model.Policy{FlapEnabled: &disabled, FlapWindowMinutes: &window, FlapPauseThreshold: &pauses, FlapRecoveryThreshold: &recovery}
	flap := resolveFlapPolicy(settings, policy)
	if flap.Enabled || flap.WindowMinutes != 90 || flap.PauseThreshold != 4 || flap.RecoveryThreshold != 8 {
		t.Fatalf("account flap overrides not applied: %+v", flap)
	}
	control := model.AccountControl{FlapActive: true, FlapRecoveryRequired: flap.RecoveryThreshold}
	if threshold := effectiveRecoveryThreshold(settings.RecoveryThreshold, control); threshold != 12 {
		t.Fatalf("flap protection weakened stronger regular policy: %d", threshold)
	}
}

func TestCharacterizationAdaptiveHealthReducesLoadAndRespectsManualLoadOverride(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.HealthMode = model.HealthModeAdaptive
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	original := 100
	api.mu.Lock()
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &original
	api.mu.Unlock()

	checkedAt := time.Now().UTC()
	api.setMonitorHealth(model.StatusDegraded, 20_000, checkedAt)
	if _, err := database.InsertMonitorHistory(ctx, model.MonitorHistoryRecord{SourceID: 1, MonitorID: 2, Model: "gpt", Status: model.StatusDegraded, LatencyMS: 20_000, CheckedAt: checkedAt, IngestedAt: checkedAt}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 1 || api.loadActions[0] == nil || *api.loadActions[0] != 80 {
		t.Fatalf("单次性能下降只能温和降至 80%%，实际为 %v", api.loadActions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if !control.OwnsLoadFactor || control.OriginalLoadFactor == nil || *control.OriginalLoadFactor != 100 {
		t.Fatalf("原始负载和调度归属未保存: %+v", control)
	}

	manual := 75
	if _, err := engine.ForceSetLoadFactorCommand(ctx, 225, &manual, "web", "manual load override",
		"manual-load-override", accountcontrol.DefaultAdministratorTTL); err != nil {
		t.Fatal(err)
	}
	checkedAt = checkedAt.Add(time.Minute)
	api.setMonitorHealth(model.StatusDegraded, 20_000, checkedAt)
	if _, err := database.InsertMonitorHistory(ctx, model.MonitorHistoryRecord{SourceID: 2, MonitorID: 2, Model: "gpt", Status: model.StatusDegraded, LatencyMS: 20_000, CheckedAt: checkedAt, IngestedAt: checkedAt}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	control, err = database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.OwnsLoadFactor || control.LoadOverrideUntil == nil || len(api.loadActions) != 2 || api.loadActions[1] == nil || *api.loadActions[1] != manual {
		t.Fatalf("人工修改负载后应进入保护期且不再覆盖: control=%+v actions=%v", control, api.loadActions)
	}
}

func TestAdaptiveHealthRecoversThroughTwentyFiveAndFiftyPercentStages(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.HealthMode = model.HealthModeAdaptive
	settings.HealthMinSamples = 3
	settings.HealthRecoveryWindow = 4
	settings.HealthRecoverySuccesses = 3
	settings.HealthTrialMinutes = 5
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	currentLoad, originalLoad := 50, 100
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &currentLoad
	api.monitors[0].PrimaryLatencyMS = 1_000
	api.mu.Unlock()
	falseValue := false
	control := model.AccountControl{
		AccountID: 225, MonitorID: int64Ptr(2), OwnsPause: true, Owner: "automatic",
		ExpectedSchedulable: &falseValue, LastObserved: &falseValue, HealthLocked: true,
		OwnsLoadFactor: true, OriginalLoadFactor: &originalLoad, ExpectedLoadFactor: &currentLoad,
		LoadStage: model.HealthStageQuarantined,
	}
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	holdUntil := now.Add(-time.Minute)
	if err := database.UpsertMonitorHealthState(ctx, model.MonitorHealthState{
		MonitorID: 2, Stage: model.HealthStageQuarantined, HoldUntil: &holdUntil,
		LastTransitionAt: now.Add(-10 * time.Minute), UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	checkedAt := now
	applyV3Checks(t, engine, database, api, model.StatusOperational, 1_000, "", &checkedAt, 4)
	if len(api.actions) != 1 || !api.actions[0] {
		t.Fatalf("满足恢复条件后应恢复账号调度: %v", api.actions)
	}
	if len(api.loadActions) != 1 || api.loadActions[0] == nil || *api.loadActions[0] != 25 {
		t.Fatalf("首次恢复应进入 25%% 负载: %v", api.loadActions)
	}

	control, err = database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	stageStarted := time.Now().UTC().Add(-6 * time.Minute)
	control.RecoveryStartedAt = &stageStarted
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	applyV3Checks(t, engine, database, api, model.StatusOperational, 1_000, "", &checkedAt, 1)
	if len(api.loadActions) != 2 || api.loadActions[1] == nil || *api.loadActions[1] != 50 {
		t.Fatalf("试运行稳定后应提升到 50%% 负载: %v", api.loadActions)
	}

	control, err = database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	stageStarted = time.Now().UTC().Add(-11 * time.Minute)
	control.RecoveryStartedAt = &stageStarted
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	applyV3Checks(t, engine, database, api, model.StatusOperational, 1_000, "", &checkedAt, 1)
	if len(api.loadActions) != 3 || api.loadActions[2] == nil || *api.loadActions[2] != 80 {
		t.Fatalf("第二阶段稳定后应提升到 80%% 负载: %v", api.loadActions)
	}
	control, err = database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	stageStarted = time.Now().UTC().Add(-16 * time.Minute)
	control.RecoveryStartedAt = &stageStarted
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	applyV3Checks(t, engine, database, api, model.StatusOperational, 1_000, "", &checkedAt, 1)
	if len(api.loadActions) != 4 || api.loadActions[3] == nil || *api.loadActions[3] != 100 {
		t.Fatalf("最终应还原原始负载: %v", api.loadActions)
	}
	control, err = database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.OwnsLoadFactor || control.OriginalLoadFactor != nil || control.LoadStage != model.HealthStageHealthy {
		t.Fatalf("完成恢复后应清除负载归属: %+v", control)
	}
}

func TestV3SemanticMismatchReducesLoadButNeverPauses(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	original := 100
	api.mu.Lock()
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &original
	api.mu.Unlock()
	now := time.Now().UTC()
	for i := 0; i < 10; i++ {
		checkedAt := now.Add(time.Duration(i-9) * time.Minute)
		if _, err := database.InsertMonitorHistory(ctx, model.MonitorHistoryRecord{
			SourceID: int64(i + 1), MonitorID: 2, Model: "gpt", Status: model.StatusFailed,
			LatencyMS: 1_000, ErrorClass: model.ErrorClassSemantic, CheckedAt: checkedAt, IngestedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	api.setMonitorHealth(model.StatusFailed, 1_000, now)
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 0 {
		t.Fatalf("语义校验不一致不能暂停账号: %v", api.actions)
	}
	if len(api.loadActions) != 1 || api.loadActions[0] == nil || *api.loadActions[0] != 80 {
		t.Fatalf("语义校验不一致应只进入 80%% 观察负载: %v", api.loadActions)
	}
	snapshot := engine.Snapshot()
	if len(snapshot.Bindings) != 1 || snapshot.Bindings[0].Decision == nil || snapshot.Bindings[0].Decision.HardSuccessRate10 != 100 {
		t.Fatalf("语义校验应计为硬可用成功: %+v", snapshot.Bindings)
	}
}

func TestV3ThreeInfrastructureFailuresPauseOnceAndSnapshotOnce(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for i := 0; i < 10; i++ {
		checkedAt := now.Add(time.Duration(i-9) * time.Minute)
		status, class := model.StatusOperational, ""
		if i >= 7 {
			status, class = model.StatusError, model.ErrorClassInfrastructure
		}
		if _, err := database.InsertMonitorHistory(ctx, model.MonitorHistoryRecord{
			SourceID: int64(i + 1), MonitorID: 2, Model: "gpt", Status: status,
			LatencyMS: 1_000, ErrorClass: class, CheckedAt: checkedAt, IngestedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	api.setMonitorHealth(model.StatusError, 0, now)
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 1 || api.actions[0] {
		t.Fatalf("连续三次基础设施故障应且只应暂停一次: %v", api.actions)
	}
	decisions, err := database.ListDecisionSnapshots(ctx, 225, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(decisions) != 1 || decisions[0].Action != "pause" {
		t.Fatalf("相同检测结果只应产生一条暂停决策快照: %+v", decisions)
	}
}

func TestV3HealthyRealTrafficSuppressesMonitorOnlyPause(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	original := 100
	api.mu.Lock()
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &original
	api.mu.Unlock()
	now := time.Now().UTC()
	for i := 0; i < 20; i++ {
		_, err := database.InsertTrafficSuccesses(ctx, []model.TrafficSuccess{
			{EventKey: fmt.Sprintf("success-%d", i), AccountID: 225, Model: "gpt", DurationMS: 1_000, CreatedAt: now.Add(-time.Duration(i) * time.Second)},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		checkedAt := now.Add(time.Duration(i-9) * time.Minute)
		status, class := model.StatusOperational, ""
		if i >= 7 {
			status, class = model.StatusError, model.ErrorClassInfrastructure
		}
		if _, err := database.InsertMonitorHistory(ctx, model.MonitorHistoryRecord{SourceID: int64(i + 1), MonitorID: 2, Model: "gpt", Status: status, LatencyMS: 1_000, ErrorClass: class, CheckedAt: checkedAt, IngestedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	api.setMonitorHealth(model.StatusError, 0, now)
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 0 {
		t.Fatalf("真实流量健康时不得仅因监控暂停: %v", api.actions)
	}
	snapshot := engine.Snapshot()
	decision := snapshot.Bindings[0].Decision
	if decision == nil || !decision.Disagreement || decision.SuggestedLoadPercent != 25 || decision.TrafficSampleCount != 20 {
		t.Fatalf("真实流量纠偏证据不完整: %+v", decision)
	}
}

func TestAdaptiveV3ShortLagHealthyDecisionClearsHealthLockAndResumes(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	settings.HealthRecoverySuccesses = 8
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	detailCheckedAt := now.Add(-80 * time.Second)
	monitorCheckedAt := now.Add(-20 * time.Second)
	insertV3History(t, database, detailCheckedAt, []string{
		model.StatusOperational, model.StatusOperational, model.StatusOperational,
		model.StatusOperational, model.StatusOperational, model.StatusOperational,
		model.StatusOperational, model.StatusOperational,
	})
	previousCheckedAt := monitorCheckedAt.Add(-time.Minute)
	if err := database.UpsertMonitorState(ctx, model.MonitorState{
		MonitorID: 2, LastCheckedAt: &previousCheckedAt, LastStatus: model.StatusOperational,
		HealthyStreak: 7, Phase: model.PhaseHealthy, UpdatedAt: previousCheckedAt,
	}); err != nil {
		t.Fatal(err)
	}
	api.setMonitorHealth(model.StatusOperational, 1_000, monitorCheckedAt)
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.accounts[0].Concurrency = 100
	loadFactor := 100
	api.accounts[0].LoadFactor = &loadFactor
	api.mu.Unlock()
	monitorID := int64(2)
	expected := false
	if err := database.UpsertControl(ctx, model.AccountControl{
		AccountID: 225, MonitorID: &monitorID, OwnsPause: true, Owner: "automatic",
		ExpectedSchedulable: &expected, LastObserved: &expected, HealthLocked: true,
		LoadStage: model.HealthStageQuarantined, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 1 || !api.actions[0] {
		t.Fatalf("短暂滞后的健康决策应恢复账号: %v", api.actions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.HealthLocked || control.OwnsPause || control.LoadStage != model.HealthStageRecovering25 {
		t.Fatalf("恢复后的控制状态不正确: %+v", control)
	}
}

func TestAdaptiveV3LaggingPauseDecisionNeverPauses(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	detailCheckedAt := now.Add(-80 * time.Second)
	monitorCheckedAt := now.Add(-20 * time.Second)
	insertV3History(t, database, detailCheckedAt, []string{
		model.StatusOperational, model.StatusOperational, model.StatusOperational,
		model.StatusError, model.StatusError, model.StatusError,
	})
	api.setMonitorHealth(model.StatusOperational, 1_000, monitorCheckedAt)

	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 0 {
		t.Fatalf("落后的暂停决策不得写入账号: %v", api.actions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.HealthLocked {
		t.Fatalf("落后的暂停决策不应建立健康锁: %+v", control)
	}
}

func TestAdaptiveV3TooOldHealthyDecisionDoesNotRecover(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	detailCheckedAt := now.Add(-4 * time.Minute)
	monitorCheckedAt := now.Add(-20 * time.Second)
	insertV3History(t, database, detailCheckedAt, []string{
		model.StatusOperational, model.StatusOperational, model.StatusOperational,
		model.StatusOperational, model.StatusOperational,
	})
	previousCheckedAt := monitorCheckedAt.Add(-time.Minute)
	if err := database.UpsertMonitorState(ctx, model.MonitorState{
		MonitorID: 2, LastCheckedAt: &previousCheckedAt, LastStatus: model.StatusOperational,
		HealthyStreak: 2, Phase: model.PhaseHealthy, UpdatedAt: previousCheckedAt,
	}); err != nil {
		t.Fatal(err)
	}
	api.setMonitorHealth(model.StatusOperational, 1_000, monitorCheckedAt)
	setHealthLockedAccount(t, database, api, now)

	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 0 {
		t.Fatalf("超过新鲜度窗口的健康决策不得恢复账号: %v", api.actions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if !control.HealthLocked || !control.OwnsPause {
		t.Fatalf("过期健康证据不应清除控制锁: %+v", control)
	}
}

func TestAdaptiveV3HealthyDecisionDoesNotRecoverWhenLatestSummaryFailed(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	detailCheckedAt := now.Add(-80 * time.Second)
	monitorCheckedAt := now.Add(-20 * time.Second)
	insertV3History(t, database, detailCheckedAt, []string{
		model.StatusOperational, model.StatusOperational, model.StatusOperational,
		model.StatusOperational, model.StatusOperational,
	})
	api.setMonitorHealth(model.StatusError, 0, monitorCheckedAt)
	setHealthLockedAccount(t, database, api, now)

	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 0 {
		t.Fatalf("最新监控失败时不得使用旧健康决策恢复: %v", api.actions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if !control.HealthLocked || !control.OwnsPause {
		t.Fatalf("最新监控失败时健康锁必须保留: %+v", control)
	}
}

func TestAdaptiveV3RecoveryUsesStrictestThreshold(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	settings.RecoveryThreshold = 3
	settings.HealthRecoverySuccesses = 5
	settings.FlapRecoveryThreshold = 7
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	setHealthLockedAccount(t, database, api, now)
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	control.FlapActive = true
	control.FlapRecoveryRequired = 7
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}

	checkedAt := now
	applyV3Checks(t, engine, database, api, model.StatusOperational, 1_000, "", &checkedAt, 6)
	if len(api.actions) != 0 {
		t.Fatalf("未达到抖动恢复门槛前不得恢复账号: %v", api.actions)
	}
	snapshot := engine.Snapshot()
	if len(snapshot.Bindings) != 1 || snapshot.Bindings[0].BaseRecoveryThreshold != 5 || snapshot.Bindings[0].RecoveryThreshold != 7 {
		t.Fatalf("V3 应采用旧门槛、健康门槛和抖动门槛中的最大值: %+v", snapshot.Bindings)
	}
	applyV3Checks(t, engine, database, api, model.StatusOperational, 1_000, "", &checkedAt, 1)
	if len(api.actions) != 1 || !api.actions[0] {
		t.Fatalf("达到最严格恢复门槛后应恢复账号: %v", api.actions)
	}
}

func TestAdaptiveV3ShortLagHealthyDecisionAdvancesRecoveryToFull(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	settings.HealthRecoverySuccesses = 8
	settings.HealthTrialMinutes = 1
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	detailCheckedAt := now.Add(-80 * time.Second)
	monitorCheckedAt := now.Add(-20 * time.Second)
	insertV3History(t, database, detailCheckedAt, []string{
		model.StatusOperational, model.StatusOperational, model.StatusOperational,
		model.StatusOperational, model.StatusOperational, model.StatusOperational,
		model.StatusOperational, model.StatusOperational,
	})
	previousCheckedAt := monitorCheckedAt.Add(-time.Minute)
	if err := database.UpsertMonitorState(ctx, model.MonitorState{
		MonitorID: 2, LastCheckedAt: &previousCheckedAt, LastStatus: model.StatusOperational,
		HealthyStreak: 7, Phase: model.PhaseHealthy, UpdatedAt: previousCheckedAt,
	}); err != nil {
		t.Fatal(err)
	}
	api.setMonitorHealth(model.StatusOperational, 1_000, monitorCheckedAt)
	currentLoad, originalLoad := 25, 100
	api.mu.Lock()
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &currentLoad
	api.mu.Unlock()
	stageStarted := now.Add(-2 * time.Minute)
	if err := database.UpsertControl(ctx, model.AccountControl{
		AccountID: 225, OwnsLoadFactor: true, OriginalLoadFactor: &originalLoad,
		ExpectedLoadFactor: &currentLoad, LoadStage: model.HealthStageRecovering25,
		RecoveryStep: 1, RecoveryStartedAt: &stageStarted, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 1 || api.loadActions[0] == nil || *api.loadActions[0] != 50 {
		t.Fatalf("短滞后健康证据应将试运行从 25%% 提升到 50%%: %v", api.loadActions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	stageStarted = now.Add(-3 * time.Minute)
	control.RecoveryStartedAt = &stageStarted
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 2 || api.loadActions[1] == nil || *api.loadActions[1] != 80 {
		t.Fatalf("短滞后健康证据应将试运行从 50%% 提升到 80%%: %v", api.loadActions)
	}
	control, err = database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	stageStarted = now.Add(-4 * time.Minute)
	control.RecoveryStartedAt = &stageStarted
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 3 || api.loadActions[2] == nil || *api.loadActions[2] != 100 {
		t.Fatalf("短滞后健康证据应完成 80%% 到 100%% 的恢复: %v", api.loadActions)
	}
	control, err = database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.OwnsLoadFactor || control.LoadStage != model.HealthStageHealthy {
		t.Fatalf("完成恢复后应释放负载归属: %+v", control)
	}
}

func TestAdaptiveV3LaggingDecisionNeverRegressesRecoveryLoad(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	settings.HealthRecoverySuccesses = 8
	settings.HealthTrialMinutes = 1
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	detailCheckedAt := now.Add(-80 * time.Second)
	monitorCheckedAt := now.Add(-20 * time.Second)
	insertV3History(t, database, detailCheckedAt, []string{
		model.StatusOperational, model.StatusError, model.StatusError,
		model.StatusOperational, model.StatusOperational, model.StatusOperational,
		model.StatusOperational, model.StatusOperational, model.StatusOperational,
		model.StatusOperational, model.StatusOperational,
	})
	previousCheckedAt := monitorCheckedAt.Add(-time.Minute)
	if err := database.UpsertMonitorState(ctx, model.MonitorState{
		MonitorID: 2, LastCheckedAt: &previousCheckedAt, LastStatus: model.StatusOperational,
		HealthyStreak: 7, Phase: model.PhaseHealthy, UpdatedAt: previousCheckedAt,
	}); err != nil {
		t.Fatal(err)
	}
	api.setMonitorHealth(model.StatusOperational, 1_000, monitorCheckedAt)
	currentLoad, originalLoad := 50, 100
	api.mu.Lock()
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &currentLoad
	api.mu.Unlock()
	stageStarted := now.Add(-3 * time.Minute)
	if err := database.UpsertControl(ctx, model.AccountControl{
		AccountID: 225, OwnsLoadFactor: true, OriginalLoadFactor: &originalLoad,
		ExpectedLoadFactor: &currentLoad, LoadStage: model.HealthStageRecovering50,
		RecoveryStep: 2, RecoveryStartedAt: &stageStarted, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 0 {
		t.Fatalf("落后的降载建议不得回退正在恢复的账号: %v", api.loadActions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.LoadStage != model.HealthStageRecovering50 || control.RecoveryStep != 2 {
		t.Fatalf("落后的降载建议不应改变恢复阶段: %+v", control)
	}
}

func TestAdaptiveLoadDoesNotStartForExternallyPausedAccount(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	statuses := make([]string, 10)
	for i := range statuses {
		statuses[i] = model.StatusDegraded
	}
	insertV3History(t, database, now, statuses)
	api.setMonitorHealth(model.StatusDegraded, 1_000, now)
	loadFactor := 100
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &loadFactor
	api.mu.Unlock()

	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 0 {
		t.Fatalf("人工暂停账号不得新建降载控制: %v", api.loadActions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.OwnsLoadFactor || control.LoadStage != "" {
		t.Fatalf("人工暂停账号不应取得负载归属: %+v", control)
	}
	snapshot := engine.Snapshot()
	if len(snapshot.Bindings) != 1 || snapshot.Bindings[0].Decision == nil || snapshot.Bindings[0].Decision.SuggestedLoadPercent >= 100 {
		t.Fatalf("测试证据本应产生降载建议: %+v", snapshot.Bindings)
	}
}

func TestAdaptiveLoadFreezesExistingOwnershipForExternallyPausedAccount(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeAdaptive
	settings.HealthRecoverySuccesses = 3
	settings.HealthTrialMinutes = 1
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	insertV3History(t, database, now, []string{
		model.StatusOperational, model.StatusOperational, model.StatusOperational,
	})
	api.setMonitorHealth(model.StatusOperational, 1_000, now)
	currentLoad, originalLoad := 25, 100
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &currentLoad
	api.mu.Unlock()
	stageStarted := now.Add(-2 * time.Minute)
	if err := database.UpsertControl(ctx, model.AccountControl{
		AccountID: 225, OwnsPause: false, OwnsLoadFactor: true,
		OriginalLoadFactor: &originalLoad, ExpectedLoadFactor: &currentLoad,
		LoadStage: model.HealthStageRecovering25, RecoveryStep: 1,
		RecoveryStartedAt: &stageStarted, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 0 || len(api.loadActions) != 0 {
		t.Fatalf("人工暂停账号不得触发调度或负载写入: schedulable=%v load=%v", api.actions, api.loadActions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if !control.OwnsLoadFactor || control.LoadStage != model.HealthStageRecovering25 || control.RecoveryStep != 1 {
		t.Fatalf("人工暂停期间应冻结并保留已有负载归属: %+v", control)
	}

	api.mu.Lock()
	api.accounts[0].Schedulable = true
	api.mu.Unlock()
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 1 || api.loadActions[0] == nil || *api.loadActions[0] != 50 {
		t.Fatalf("人工重新开启后应继续原有恢复阶段: %v", api.loadActions)
	}
}

func TestAdaptiveLoadDoesNotWriteWhenJournalIsUnavailable(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.HealthMode = model.HealthModeAdaptive

	load := 100
	api.mu.Lock()
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &load
	account := api.accounts[0]
	api.mu.Unlock()
	binding := model.ResolvedBinding{
		Account: account,
		Monitor: &model.Monitor{ID: 2, Enabled: true},
		State:   "bound",
	}
	control := model.AccountControl{AccountID: account.ID, LoadStage: model.HealthStageLimited25}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	err = engine.reconcileAdaptiveLoad(ctx, &binding, &control, settings, time.Now().UTC())
	if err == nil {
		t.Fatal("控制状态写库失败时应返回错误")
	}
	if len(api.loadActions) != 0 {
		t.Fatalf("journal 不可用时不得先写上游，也不得执行反向 rollback: %v", api.loadActions)
	}
	api.mu.Lock()
	actual := cloneIntPointer(api.accounts[0].LoadFactor)
	api.mu.Unlock()
	if actual == nil || *actual != 100 {
		t.Fatalf("journal 失败后上游状态被改变: %v", actual)
	}
}

func TestAdaptiveLoadAlreadyAppliedDoesNotGrowMutationJournalAcrossFiftyPasses(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.HealthMode = model.HealthModeAdaptive
	settings.HealthTrialPercent = 8
	load := 8
	api.mu.Lock()
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &load
	account := api.accounts[0]
	api.loadActions = nil
	api.mu.Unlock()
	original, expected := 100, 8
	control := model.AccountControl{AccountID: account.ID, OwnsLoadFactor: true, OriginalLoadFactor: &original,
		ExpectedLoadFactor: &expected, LoadStage: model.HealthStageLimited25, UpdatedAt: time.Now().UTC()}
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	binding := model.ResolvedBinding{Account: account, Monitor: &model.Monitor{ID: 2, Enabled: true}, State: "bound", Control: control}
	before, err := database.ListAccountMutations(ctx, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 50; index++ {
		if err := engine.reconcileAdaptiveLoad(ctx, &binding, &control, settings, time.Now().UTC().Add(time.Duration(index)*time.Second)); err != nil {
			t.Fatalf("pass %d: %v", index+1, err)
		}
	}
	after, err := database.ListAccountMutations(ctx, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("automatic no-op grew mutation journal across 50 passes: %d -> %d", len(before), len(after))
	}
	if len(api.loadActions) != 0 {
		t.Fatalf("automatic no-op reached upstream writes: %v", api.loadActions)
	}
}

func TestAdaptiveLoadAlreadyAppliedRepairsProjectionWithoutMutation(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.HealthMode = model.HealthModeAdaptive
	settings.HealthTrialPercent = 8
	load := 8
	api.mu.Lock()
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &load
	account := api.accounts[0]
	api.loadActions = nil
	api.mu.Unlock()
	original, staleExpected := 100, 25
	control := model.AccountControl{AccountID: account.ID, OwnsLoadFactor: true, OriginalLoadFactor: &original,
		ExpectedLoadFactor: &staleExpected, LoadStage: model.HealthStageLimited25, UpdatedAt: time.Now().UTC()}
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	binding := model.ResolvedBinding{Account: account, Monitor: &model.Monitor{ID: 2, Enabled: true}, State: "bound", Control: control}
	before, _ := database.ListAccountMutations(ctx, account.ID)
	if err := engine.reconcileAdaptiveLoad(ctx, &binding, &control, settings, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	after, _ := database.ListAccountMutations(ctx, account.ID)
	persisted, err := database.GetControl(ctx, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) || len(api.loadActions) != 0 || persisted.ExpectedLoadFactor == nil ||
		*persisted.ExpectedLoadFactor != load || !persisted.OwnsLoadFactor || persisted.OriginalLoadFactor == nil ||
		*persisted.OriginalLoadFactor != original {
		t.Fatalf("projection repair was not local-only: before=%d after=%d writes=%v control=%+v",
			len(before), len(after), api.loadActions, persisted)
	}
}

func TestObserveModeNeverWritesAccountState(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, _ := database.GetSettings(ctx)
	settings.HealthMode = model.HealthModeObserve
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	applyChecks(t, engine, api, model.StatusFailed, &base, 3)
	if len(api.actions) != 0 || len(api.loadActions) != 0 {
		t.Fatalf("只观察模式不得写账号状态: schedulable=%v load=%v", api.actions, api.loadActions)
	}
}

func TestWritesFreezeBlocksAllAccountWritesButKeepsCollection(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	if err := engine.UpdateFreezeState(ctx, model.FreezeState{AllAutomation: true, Reason: "incident"}, "web"); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	applyChecks(t, engine, api, model.StatusFailed, &base, 3)
	if len(api.actions) != 0 {
		t.Fatalf("all-automation freeze must block automatic pause: %v", api.actions)
	}
	snapshot := engine.Snapshot()
	if !snapshot.Freeze.AllAutomation || snapshot.LastSyncAt == nil || len(snapshot.Bindings) != 1 {
		t.Fatalf("freeze must not stop collection or snapshot refresh: %+v", snapshot)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if !control.HealthLocked {
		t.Fatalf("health state should advance while external write is frozen: %+v", control)
	}
	mutations, err := database.ListAccountMutations(ctx, 225)
	if err != nil || len(mutations) == 0 || mutations[len(mutations)-1].Status != accountcontrol.StatusBlocked ||
		mutations[len(mutations)-1].LastErrorCode != string(accountcontrol.BlockWritesFrozen) {
		t.Fatalf("blocked policy action was not journaled: mutations=%+v err=%v", mutations, err)
	}

	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.mu.Unlock()
	if err := engine.ForceResume(ctx, 225, "web", "operator emergency recovery"); err == nil {
		t.Fatal("force resume bypassed the global writes freeze")
	}
	if len(api.actions) != 0 {
		t.Fatalf("global writes freeze allowed an account write: %v", api.actions)
	}
}

func TestAgentFreezeBlocksAgentWriterButNotOperator(t *testing.T) {
	ctx := context.Background()
	engine, _, api := newEngineTest(t, false)
	if err := engine.UpdateFreezeState(ctx, model.FreezeState{Agent: true, Reason: "review"}, "web"); err != nil {
		t.Fatal(err)
	}
	if err := engine.AgentPause(ctx, 225, "agent:9", "model decision"); err == nil {
		t.Fatal("agent freeze should reject agent-owned writes")
	}
	if len(api.actions) != 0 {
		t.Fatalf("rejected agent action reached Sub2API: %v", api.actions)
	}
	if err := engine.ManualPause(ctx, 225, "web"); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 1 || api.actions[0] {
		t.Fatalf("operator action should remain available: %v", api.actions)
	}
}

func TestLoadPinPreventsHealthOverwriteAndRetainsOriginalBaseline(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.HealthMode = model.HealthModeAdaptive
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	original := 100
	api.mu.Lock()
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &original
	api.mu.Unlock()
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engine.PinLoad(ctx, 225, 40, time.Now().UTC().Add(time.Hour), "web", "night capacity cap"); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 1 {
		t.Fatalf("load pin did not produce exactly one write: %v", api.loadActions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	control.LoadStage = model.HealthStageLimited25
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	binding, ok := findBinding(engine.Snapshot().Bindings, 225)
	if !ok {
		t.Fatal("reconciled account is missing from the policy snapshot")
	}
	binding.Control = control
	if err := engine.reconcileAdaptiveLoad(ctx, &binding, &control, settings, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 1 || api.loadActions[0] == nil || *api.loadActions[0] != 40 {
		t.Fatalf("health control overwrote an active load pin: %v", api.loadActions)
	}

	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ClearLoadPinCommand(ctx, 225, "web", "capacity restored", "clear-load-pin"); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 2 || api.loadActions[1] == nil || *api.loadActions[1] != 100 {
		t.Fatalf("clearing the pin did not return to the policy baseline: %v", api.loadActions)
	}
	loaded, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.LoadPinValue != nil || loaded.LoadPinUntil != nil || loaded.OriginalLoadFactor == nil || *loaded.OriginalLoadFactor != 100 {
		t.Fatalf("cleared pin projection did not retain the policy baseline: %+v", loaded)
	}
}

func TestDirectAdministratorLoadAdjustmentCreatesTemporaryOverride(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	original := 90
	api.mu.Lock()
	api.accounts[0].LoadFactor = &original
	api.mu.Unlock()

	desired := 70
	started := time.Now().UTC()
	if _, err := engine.ForceSetLoadFactorCommand(ctx, 225, &desired, "web", "direct administrator command",
		"admin-load-override", accountcontrol.DefaultAdministratorTTL); err != nil {
		t.Fatal(err)
	}
	if len(api.loadActions) != 1 || api.loadActions[0] == nil || *api.loadActions[0] != desired {
		t.Fatalf("administrator load adjustment did not reach Sub2API: %v", api.loadActions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.LoadPinValue != nil || control.LoadPinUntil != nil || control.OwnsLoadFactor ||
		control.OriginalLoadFactor == nil || *control.OriginalLoadFactor != original || control.ExpectedLoadFactor == nil ||
		*control.ExpectedLoadFactor != desired || control.LoadOverrideUntil == nil {
		t.Fatalf("administrator adjustment did not establish a clean manual override: %+v", control)
	}
	if control.LoadOverrideUntil.Before(started.Add(29*time.Minute)) || control.LoadOverrideUntil.After(time.Now().UTC().Add(31*time.Minute)) {
		t.Fatalf("administrator protection window does not use product default: %v", control.LoadOverrideUntil)
	}
}

func insertV3History(t *testing.T, database *store.Store, latest time.Time, statuses []string) {
	t.Helper()
	ctx := context.Background()
	for i, status := range statuses {
		checkedAt := latest.Add(time.Duration(i-len(statuses)+1) * time.Minute)
		errorClass := ""
		if status == model.StatusFailed || status == model.StatusError {
			errorClass = model.ErrorClassInfrastructure
		}
		inserted, err := database.InsertMonitorHistory(ctx, model.MonitorHistoryRecord{
			SourceID: checkedAt.UnixNano(), MonitorID: 2, Model: "gpt", Status: status,
			LatencyMS: 1_000, ErrorClass: errorClass, CheckedAt: checkedAt, IngestedAt: latest,
		})
		if err != nil || !inserted {
			t.Fatalf("insert V3 monitor evidence: inserted=%v err=%v", inserted, err)
		}
	}
}

func setHealthLockedAccount(t *testing.T, database *store.Store, api *fakeAPI, now time.Time) {
	t.Helper()
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.mu.Unlock()
	monitorID := int64(2)
	expected := false
	if err := database.UpsertControl(context.Background(), model.AccountControl{
		AccountID: 225, MonitorID: &monitorID, OwnsPause: true, Owner: "automatic",
		ExpectedSchedulable: &expected, LastObserved: &expected, HealthLocked: true,
		LoadStage: model.HealthStageQuarantined, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
}

func containsEvent(events []model.Event, eventType string) bool {
	for _, event := range events {
		if event.Type == eventType {
			return true
		}
	}
	return false
}

func autonomousAgentTestContext(commandID string, created time.Time) context.Context {
	expires := created.Add(accountcontrol.DefaultAutonomousTTL)
	return accountcontrol.WithCommandContext(context.Background(), accountcontrol.CommandContext{
		CommandID: commandID, CreatedAt: created, ExpiresAt: &expires, SnapshotVersion: "test-snapshot-" + commandID,
		EvidenceRefs: []string{"test-evidence-" + commandID},
	})
}

func int64Ptr(value int64) *int64 { return &value }
