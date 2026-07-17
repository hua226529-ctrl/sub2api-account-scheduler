package failover

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type fakeStore struct {
	mu               sync.Mutex
	policies         []model.GroupFailoverPolicy
	windows          map[string]model.AgentWindowStats
	dispatch         model.Settings
	states           []model.GroupFailoverState
	events           []model.Event
	count30          int
	count6h          int
	windowErrAccount int64
	windowCalls      []int64
	evidence         []model.GroupValidationEvidence
}

func (f *fakeStore) ListGroupFailoverPolicies(context.Context, int64) ([]model.GroupFailoverPolicy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.GroupFailoverPolicy(nil), f.policies...), nil
}
func (f *fakeStore) SaveGroupFailoverState(_ context.Context, state model.GroupFailoverState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveGroupFailoverState(state)
	return nil
}
func (f *fakeStore) CompareAndSaveGroupFailoverState(_ context.Context, expected, state model.GroupFailoverState) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for index := range f.policies {
		current := f.policies[index].State
		if current.SourceID != expected.SourceID || current.KeyID != expected.KeyID {
			continue
		}
		if current.ValidationTransitionID != expected.ValidationTransitionID || current.ValidationStatus != expected.ValidationStatus ||
			current.ValidationTargetTier != expected.ValidationTargetTier || current.ValidationTargetGroupID != expected.ValidationTargetGroupID {
			return false, nil
		}
		f.saveGroupFailoverState(state)
		return true, nil
	}
	return false, nil
}
func (f *fakeStore) saveGroupFailoverState(state model.GroupFailoverState) {
	f.states = append(f.states, state)
	for index := range f.policies {
		if f.policies[index].SourceID == state.SourceID && f.policies[index].KeyID == state.KeyID {
			f.policies[index].State = state
		}
	}
}
func (f *fakeStore) CountCompletedGroupTierTransitions(_ context.Context, _ int64, _ string, since time.Time) (int, error) {
	if time.Since(since) <= time.Hour {
		return f.count30, nil
	}
	return f.count6h, nil
}

func (f *fakeStore) GetAgentWindowStats(_ context.Context, accountID int64, _ time.Time, _ time.Time, label string) (model.AgentWindowStats, error) {
	f.windowCalls = append(f.windowCalls, accountID)
	if accountID == f.windowErrAccount {
		return model.AgentWindowStats{}, errors.New("injected window failure")
	}
	return f.windows[windowKey(label, accountID)], nil
}
func (f *fakeStore) GetSettings(context.Context) (model.Settings, error) {
	return f.dispatch, nil
}
func (f *fakeStore) AddEvent(_ context.Context, event model.Event) error {
	f.events = append(f.events, event)
	return nil
}
func (f *fakeStore) ListGroupValidationEvidence(_ context.Context, _ []int64, _ []int64, monitorAfter, trafficAfter int64) ([]model.GroupValidationEvidence, error) {
	items := make([]model.GroupValidationEvidence, 0, len(f.evidence))
	for _, item := range f.evidence {
		if (item.Source == "monitor" && item.ID > monitorAfter) || (item.Source == "traffic" && item.ID > trafficAfter) {
			items = append(items, item)
		}
	}
	return items, nil
}

type fakeSnapshots struct{ snapshot model.Snapshot }

func (f *fakeSnapshots) Snapshot() model.Snapshot { return f.snapshot }

type fakeUpstreams struct {
	sources               []model.UpstreamSource
	transitions           []model.GroupTierTransitionRequest
	transitionErrBySource map[int64]error
}

func (f *fakeUpstreams) List(context.Context) ([]model.UpstreamSource, error) {
	return append([]model.UpstreamSource(nil), f.sources...), nil
}
func (f *fakeUpstreams) TransitionGroupTier(_ context.Context, request model.GroupTierTransitionRequest) (model.GroupTierTransition, error) {
	f.transitions = append(f.transitions, request)
	if err := f.transitionErrBySource[request.SourceID]; err != nil {
		return model.GroupTierTransition{}, err
	}
	return model.GroupTierTransition{SourceID: request.SourceID, KeyID: request.KeyID, ToTier: request.TargetTier, Status: model.GroupTransitionApplied}, nil
}

type fakeTelemetry struct{ at time.Time }

func (f fakeTelemetry) Status() (*time.Time, string) { value := f.at; return &value, "" }

func TestCharacterizationControllerFallsBackToBackupOnlyAfterGlobalOutage(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 3)
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatal("first confirmed outage cycle must give the emergency agent a chance")
	}
	now = now.Add(50 * time.Second)
	snapshots.snapshot.LastSyncAt = timePtr(now)
	snapshots.snapshot.Bindings[0].Monitor.LastCheckedAt = timePtr(now)
	snapshots.snapshot.Bindings[0].MonitorState.LastCheckedAt = timePtr(now)
	controller.telemetry = fakeTelemetry{at: now}
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatal("deterministic fallback ran before the emergency agent grace period ended")
	}
	now = now.Add(50 * time.Second)
	snapshots.snapshot.LastSyncAt = timePtr(now)
	snapshots.snapshot.Bindings[0].Monitor.LastCheckedAt = timePtr(now)
	snapshots.snapshot.Bindings[0].MonitorState.LastCheckedAt = timePtr(now)
	controller.telemetry = fakeTelemetry{at: now}
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 1 || upstreams.transitions[0].TargetTier != model.GroupTierBackup {
		t.Fatalf("transitions = %+v", upstreams.transitions)
	}
	if upstreams.transitions[0].DryRun {
		t.Fatal("control mode transition unexpectedly became a dry run")
	}
}

func TestCharacterizationControllerRejectsStaleSnapshotBeforeAnyTransition(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 3)
	stale := now.Add(-10 * time.Minute)
	snapshots.snapshot.LastSyncAt = &stale
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	if err := controller.RunOnce(context.Background()); err == nil {
		t.Fatal("stale account snapshot unexpectedly allowed failover evaluation")
	}
	if len(upstreams.transitions) != 0 {
		t.Fatalf("stale snapshot caused transitions: %+v", upstreams.transitions)
	}
}

func TestFailoverObserveModeMakesTransitionDryRun(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 3)
	store.dispatch.FailoverMode = model.FailoverModeObserve
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	controller.outageSince["pool-a"] = now.Add(-5 * time.Minute)
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 1 || !upstreams.transitions[0].DryRun {
		t.Fatalf("observe mode failover transition = %+v, want one dry run", upstreams.transitions)
	}
}

func TestPoolAssessmentFailureDoesNotStopLaterPools(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 3)
	secondPolicy := store.policies[0]
	secondPolicy.SourceID, secondPolicy.KeyID, secondPolicy.Pool = 2, "22", "pool-b"
	secondPolicy.AccountIDs = []int64{202}
	secondPolicy.State.SourceID, secondPolicy.State.KeyID = 2, "22"
	store.policies = append(store.policies, secondPolicy)
	secondSource := upstreams.sources[0]
	secondSource.ID, secondSource.NormalizedURL, secondSource.RoutingPool = 2, "https://upstream-b.example", "pool-b"
	secondSource.KeyRates = []model.KeyRate{{ExternalID: "22", GroupID: "main", Status: "active"}}
	upstreams.sources = append(upstreams.sources, secondSource)
	secondBinding := snapshots.snapshot.Bindings[0]
	secondBinding.Account.ID = 202
	secondBinding.NormalizedEndpoint = secondSource.NormalizedURL
	secondBinding.Monitor = &model.Monitor{ID: 8, Enabled: true, PrimaryStatus: model.StatusFailed, LastCheckedAt: &now}
	secondBinding.MonitorState.MonitorID = 8
	snapshots.snapshot.Bindings = append(snapshots.snapshot.Bindings, secondBinding)
	store.windowErrAccount = 101
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	if err := controller.RunOnce(context.Background()); err == nil {
		t.Fatal("first pool assessment failure unexpectedly allowed the round to succeed")
	}
	foundLaterPool := false
	for _, accountID := range store.windowCalls {
		foundLaterPool = foundLaterPool || accountID == 202
	}
	if !foundLaterPool {
		t.Fatalf("later pool was not evaluated after the first pool error: calls=%v", store.windowCalls)
	}
}

func TestFailoverMutationBudgetDefersLaterPoolWithoutSkippingAssessment(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 3)
	addSecondFailoverPool(store, snapshots, upstreams, now)
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	controller.outageSince["pool-a"] = now.Add(-5 * time.Minute)
	controller.outageSince["pool-b"] = now.Add(-5 * time.Minute)
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 2 || upstreams.transitions[0].DryRun || !upstreams.transitions[1].DryRun {
		t.Fatalf("mutation budget did not execute one and defer one: %+v", upstreams.transitions)
	}
	foundDeferred := false
	for _, event := range store.events {
		foundDeferred = foundDeferred || event.Type == "group_failover_mutation_deferred"
	}
	if !foundDeferred {
		t.Fatal("deferred mutation reason was not recorded")
	}
}

func TestFailoverTransitionFailureDoesNotStopLaterPool(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 3)
	addSecondFailoverPool(store, snapshots, upstreams, now)
	upstreams.transitionErrBySource = map[int64]error{1: errors.New("injected transition failure")}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	controller.outageSince["pool-a"] = now.Add(-5 * time.Minute)
	controller.outageSince["pool-b"] = now.Add(-5 * time.Minute)
	if err := controller.RunOnce(context.Background()); err == nil {
		t.Fatal("aggregated cycle error was not returned")
	}
	if len(upstreams.transitions) != 2 || upstreams.transitions[1].SourceID != 2 || upstreams.transitions[1].DryRun {
		t.Fatalf("later pool was not executed after the first transition failure: %+v", upstreams.transitions)
	}
}

func TestControllerHotLoadsConfiguredMonitorFailureThreshold(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 3)
	store.dispatch.FailoverMonitorFailures = 4
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatal("configured four-result threshold was ignored")
	}

	store.dispatch.FailoverMonitorFailures = 3
	now = now.Add(50 * time.Second)
	updateFixtureTimes(snapshots, now)
	controller.telemetry = fakeTelemetry{at: now}
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(100 * time.Second)
	updateFixtureTimes(snapshots, now)
	controller.telemetry = fakeTelemetry{at: now}
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 1 || upstreams.transitions[0].TargetTier != model.GroupTierBackup {
		t.Fatalf("hot-loaded threshold did not trigger backup: %+v", upstreams.transitions)
	}
}

func TestControllerDoesNotSwitchWhenAnyChannelIsAvailable(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	store.windows[windowKey("5m", 101)] = model.AgentWindowStats{EligibleCount: 20, SuccessCount: 20, SuccessRate: 100, ErrorCategoryCounts: map[string]int{}}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatalf("healthy pool switched groups: %+v", upstreams.transitions)
	}
}

func TestControllerDoesNotSwitchWhenFailedMonitorAccountIsStillSchedulable(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 5)
	snapshots.snapshot.Bindings[0].Account.Schedulable = true
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(50 * time.Second)
	snapshots.snapshot.LastSyncAt = timePtr(now)
	snapshots.snapshot.Bindings[0].Monitor.LastCheckedAt = timePtr(now)
	snapshots.snapshot.Bindings[0].MonitorState.LastCheckedAt = timePtr(now)
	controller.telemetry = fakeTelemetry{at: now}
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(50 * time.Second)
	snapshots.snapshot.LastSyncAt = timePtr(now)
	snapshots.snapshot.Bindings[0].Monitor.LastCheckedAt = timePtr(now)
	snapshots.snapshot.Bindings[0].MonitorState.LastCheckedAt = timePtr(now)
	controller.telemetry = fakeTelemetry{at: now}
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatalf("schedulable channel caused a group switch: %+v", upstreams.transitions)
	}
}

func TestControllerDoesNotTreatUnknownTrafficErrorsAsHardFailures(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 5)
	store.windows[windowKey("5m", 101)] = model.AgentWindowStats{
		SampleCount: 10, EligibleCount: 10, ErrorCount: 10,
		ErrorCategoryCounts: map[string]int{model.ErrorClassUnknown: 10},
	}
	store.windows[windowKey("hard_tail", 101)] = store.windows[windowKey("5m", 101)]
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatalf("unknown traffic errors caused a group switch: %+v", upstreams.transitions)
	}
}

func TestCharacterizationControllerRequiresPersistedDistinctMonitorStreak(t *testing.T) {
	now := time.Date(2026, 7, 15, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 5)
	old := now.Add(-50 * time.Second)
	snapshots.snapshot.Bindings[0].MonitorState.LastCheckedAt = &old
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatalf("stale/repeated monitor streak caused a group switch: %+v", upstreams.transitions)
	}
}

func TestControllerAwaitingEvidenceDoesNotEscalateOrSwitchAnotherKey(t *testing.T) {
	now := time.Date(2026, 7, 15, 3, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 5)
	started := now.Add(-3 * time.Minute)
	store.policies[0].State = model.GroupFailoverState{
		SourceID: 1, KeyID: "11", CurrentTier: model.GroupTierBackup, ObservedGroupID: "backup",
		LastTransitionAt: &started, VerificationStartedAt: &started, ValidationStatus: model.GroupValidationAwaitingEvidence,
	}
	upstreams.sources[0].KeyRates[0].GroupID = "backup"

	secondPolicy := store.policies[0]
	secondPolicy.SourceID = 2
	secondPolicy.KeyID = "22"
	secondPolicy.KeyName = "secondary"
	secondPolicy.AccountIDs = []int64{202}
	secondPolicy.State = model.GroupFailoverState{SourceID: 2, KeyID: "22", CurrentTier: model.GroupTierMain, ObservedGroupID: "main"}
	store.policies = append(store.policies, secondPolicy)
	secondSource := upstreams.sources[0]
	secondSource.ID = 2
	secondSource.Name = "secondary-upstream"
	secondSource.NormalizedURL = "https://secondary.example"
	secondSource.KeyRates = []model.KeyRate{{ExternalID: "22", GroupID: "main", Status: "active"}}
	upstreams.sources = append(upstreams.sources, secondSource)
	checkedAt := now
	snapshots.snapshot.Bindings = append(snapshots.snapshot.Bindings, model.ResolvedBinding{
		Account:            model.Account{ID: 202, Name: "secondary-channel", Status: "active", Schedulable: false},
		Monitor:            &model.Monitor{ID: 8, Enabled: true, PrimaryStatus: model.StatusFailed, LastCheckedAt: &checkedAt},
		MonitorState:       model.MonitorState{MonitorID: 8, LastCheckedAt: &checkedAt, LastStatus: model.StatusFailed, UnhealthyStreak: 5},
		NormalizedEndpoint: "https://secondary.example",
	})
	store.windows[windowKey("5m", 202)] = model.AgentWindowStats{
		SampleCount: 5, EligibleCount: 5, ErrorCount: 5,
		ErrorCategoryCounts: map[string]int{model.ErrorClassInfrastructure: 5},
	}
	store.windows[windowKey("hard_tail", 202)] = store.windows[windowKey("5m", 202)]
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	controller.outageSince["pool-a"] = now.Add(-controller.interval)
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatalf("backup verification switched another token or escalated early: %+v", upstreams.transitions)
	}

}

func TestControllerConfirmedBackupFailureAdvancesToFixedEmergency(t *testing.T) {
	now := time.Date(2026, 7, 15, 4, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 5)
	started := now.Add(-time.Minute)
	store.policies[0].State = model.GroupFailoverState{
		SourceID: 1, KeyID: "11", CurrentTier: model.GroupTierBackup, ObservedGroupID: "backup",
		LastTransitionAt: &started, VerificationStartedAt: &started, ValidationStatus: model.GroupValidationConfirmedFailed,
	}
	upstreams.sources[0].KeyRates[0].GroupID = "backup"
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 1 || upstreams.transitions[0].TargetTier != model.GroupTierEmergency {
		t.Fatalf("confirmed backup failure did not advance to fixed emergency: %+v", upstreams.transitions)
	}
}

func TestControllerDoesNotAutoReturnToUnobservableMain(t *testing.T) {
	now := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	healthySince := now.Add(-31 * time.Minute)
	lastTransition := now.Add(-40 * time.Minute)
	store.policies[0].State = model.GroupFailoverState{
		SourceID: 1, KeyID: "11", CurrentTier: model.GroupTierBackup, ObservedGroupID: "backup",
		HealthySince: &healthySince, LastTransitionAt: &lastTransition, ValidationStatus: model.GroupValidationConfirmedHealthy,
	}
	snapshots.snapshot.Bindings[0].MonitorState.HealthyStreak = 12
	store.windows[windowKey("5m", 101)] = model.AgentWindowStats{EligibleCount: 5, SuccessCount: 5, SuccessRate: 100, ErrorCategoryCounts: map[string]int{}}
	store.windows[windowKey("30m", 101)] = model.AgentWindowStats{EligibleCount: 25, SuccessCount: 25, SuccessRate: 100, ErrorCategoryCounts: map[string]int{}}
	upstreams.sources[0].KeyRates[0].GroupID = "backup"
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatalf("unobservable main group caused automatic return: %+v", upstreams.transitions)
	}
}

func failoverFixture(now time.Time, status string, unhealthyStreak int) (*fakeStore, *fakeSnapshots, *fakeUpstreams) {
	balance := 50.0
	policy := model.GroupFailoverPolicy{
		SourceID: 1, KeyID: "11", KeyName: "production", Enabled: true, MainGroupID: "main", BackupGroupID: "backup",
		EmergencyGroupID: "emergency", AccountIDs: []int64{101}, Pool: "pool-a", Version: 1, ConfirmedVersion: 1, Confirmed: true,
		State: model.GroupFailoverState{SourceID: 1, KeyID: "11", CurrentTier: model.GroupTierMain, ObservedGroupID: "main"},
	}
	store := &fakeStore{
		policies: []model.GroupFailoverPolicy{policy},
		dispatch: testFailoverSettings(),
		windows: map[string]model.AgentWindowStats{
			windowKey("5m", 101):        {SampleCount: 10, EligibleCount: 10, SuccessCount: 1, ErrorCount: 9, SuccessRate: 10, ErrorCategoryCounts: map[string]int{model.ErrorClassInfrastructure: 9}},
			windowKey("hard_tail", 101): {SampleCount: 10, EligibleCount: 10, SuccessCount: 1, ErrorCount: 9, SuccessRate: 10, ErrorCategoryCounts: map[string]int{model.ErrorClassInfrastructure: 9}},
		},
	}
	checkedAt := now
	binding := model.ResolvedBinding{
		Account:      model.Account{ID: 101, Name: "channel", Status: "active", Schedulable: status == model.StatusOperational},
		Monitor:      &model.Monitor{ID: 7, Enabled: true, PrimaryStatus: status, LastCheckedAt: &checkedAt},
		MonitorState: model.MonitorState{MonitorID: 7, LastCheckedAt: &checkedAt, LastStatus: status, UnhealthyStreak: unhealthyStreak, HealthyStreak: 12},
		Decision:     &model.HealthDecision{HardFailureStreak: unhealthyStreak}, NormalizedEndpoint: "https://upstream.example",
	}
	snapshots := &fakeSnapshots{snapshot: model.Snapshot{Bindings: []model.ResolvedBinding{binding}, LastSyncAt: &now}}
	upstreams := &fakeUpstreams{sources: []model.UpstreamSource{{
		ID: 1, Name: "upstream", NormalizedURL: "https://upstream.example", CredentialMode: "password", Enabled: true,
		Balance: &balance, PauseBelow: 5, LastSuccessAt: &now, RoutingPool: "pool-a",
		KeyRates: []model.KeyRate{{ExternalID: "11", GroupID: "main", Status: "active"}},
		Groups:   []model.UpstreamGroup{{ExternalID: "main", RateMultiplier: .5}, {ExternalID: "backup", RateMultiplier: .8}, {ExternalID: "emergency", RateMultiplier: 1.2}},
	}}}
	return store, snapshots, upstreams
}

func addSecondFailoverPool(store *fakeStore, snapshots *fakeSnapshots, upstreams *fakeUpstreams, now time.Time) {
	secondPolicy := store.policies[0]
	secondPolicy.SourceID, secondPolicy.KeyID, secondPolicy.Pool = 2, "22", "pool-b"
	secondPolicy.AccountIDs = []int64{202}
	secondPolicy.State.SourceID, secondPolicy.State.KeyID = 2, "22"
	store.policies = append(store.policies, secondPolicy)
	store.windows[windowKey("5m", 202)] = store.windows[windowKey("5m", 101)]
	store.windows[windowKey("hard_tail", 202)] = store.windows[windowKey("hard_tail", 101)]
	secondSource := upstreams.sources[0]
	secondSource.ID, secondSource.NormalizedURL, secondSource.RoutingPool = 2, "https://upstream-b.example", "pool-b"
	secondSource.KeyRates = []model.KeyRate{{ExternalID: "22", GroupID: "main", Status: "active"}}
	upstreams.sources = append(upstreams.sources, secondSource)
	secondBinding := snapshots.snapshot.Bindings[0]
	secondBinding.Account.ID = 202
	secondBinding.NormalizedEndpoint = secondSource.NormalizedURL
	secondBinding.Monitor = &model.Monitor{ID: 8, Enabled: true, PrimaryStatus: model.StatusFailed, LastCheckedAt: &now}
	secondBinding.MonitorState.MonitorID = 8
	snapshots.snapshot.Bindings = append(snapshots.snapshot.Bindings, secondBinding)
}

func testFailoverSettings() model.Settings {
	return model.Settings{
		FailoverMode: model.FailoverModeControl, FailoverMutationBudget: 1,
		FailoverAccountFreshMinutes: 3, FailoverTelemetryFreshMinutes: 6, FailoverGroupFreshMinutes: 30,
		FailoverAgentGraceSeconds: 90, FailoverMonitorFailures: 3, FailoverNoTrafficFailures: 5,
		FailoverTrafficWindowMinutes: 5, FailoverTrafficMinSamples: 10, FailoverTrafficSuccessBelow: 20,
		FailoverConsecutiveHardErrors: 5, FailoverBackupVerifyMinutes: 2, FailoverPostSwitchMonitors: 2,
		FailoverPostSwitchRequests: 5, FailoverMainVerifyMinutes: 5, FailoverSwitchCooldownMinutes: 15,
		FailoverManualProtectionMinutes: 30, FailoverShortLimitWindowMinutes: 30, FailoverShortLimitCount: 2,
		FailoverLongLimitWindowMinutes: 360, FailoverLongLimitCount: 3, FailoverRecoveryWindowMinutes: 30,
		FailoverRecoveryStableMinutes: 30, FailoverRecoveryMonitorSuccesses: 10, FailoverRecoveryMinSamples: 20,
		FailoverRecoverySuccessAt: 98, FailoverReturnRetryMinutes: 120,
	}
}

func windowKey(label string, accountID int64) string {
	return label + ":" + strconv.FormatInt(accountID, 10)
}

func updateFixtureTimes(snapshots *fakeSnapshots, now time.Time) {
	snapshots.snapshot.LastSyncAt = timePtr(now)
	for index := range snapshots.snapshot.Bindings {
		binding := &snapshots.snapshot.Bindings[index]
		if binding.Monitor != nil {
			binding.Monitor.LastCheckedAt = timePtr(now)
			binding.MonitorState.LastCheckedAt = timePtr(now)
		}
	}
}
