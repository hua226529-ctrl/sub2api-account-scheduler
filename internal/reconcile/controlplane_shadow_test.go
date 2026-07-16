package reconcile

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplanebridge"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplaneshadow"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func TestNewEngineDefaultsToNoopShadow(t *testing.T) {
	engine := NewEngine(nil, nil, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if engine.shadow.Enabled() {
		t.Fatal("default engine shadow is enabled")
	}
}

func TestManualPathsObserveOnceWithoutChangingBehavior(t *testing.T) {
	_, database, api := newEngineTest(t, false)
	capture := controlplaneshadow.NewCaptureObserver()
	engine := NewEngine(api, database, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(capture))
	ctx := context.Background()

	if err := engine.ManualPause(ctx, 225, "web"); err != nil {
		t.Fatal(err)
	}
	pause := onlyShadowObservation(t, capture.Observations())
	if pause.Path != controlplaneshadow.PathManualPause || pause.ConversionStatus != controlplanebridge.ConversionIncomplete || pause.GapCode != controlplanebridge.GapMissingIdempotencySource {
		t.Fatalf("manual pause observation = %+v", pause)
	}
	if len(api.actions) != 1 || api.actions[0] {
		t.Fatalf("manual pause writes = %v", api.actions)
	}

	capture.Reset()
	if err := engine.ManualResume(ctx, 225, "web"); err != nil {
		t.Fatal(err)
	}
	resume := onlyShadowObservation(t, capture.Observations())
	if resume.Path != controlplaneshadow.PathManualResume || resume.ConversionStatus != controlplanebridge.ConversionIncomplete ||
		resume.GapCode != controlplanebridge.GapAmbiguousManualResume || resume.IntentID != "" || resume.Match {
		t.Fatalf("manual resume observation = %+v", resume)
	}
	if len(api.actions) != 2 || !api.actions[1] {
		t.Fatalf("manual resume writes = %v", api.actions)
	}
}

func TestAdministratorAndAgentAccountPathsReportRealContext(t *testing.T) {
	_, database, api := newEngineTest(t, false)
	capture := controlplaneshadow.NewCaptureObserver()
	engine := NewEngine(api, database, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(capture))
	ctx := context.Background()
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	capture.Reset()

	if err := engine.AgentPause(ctx, 225, "agent:legacy", "legacy pause"); err != nil {
		t.Fatal(err)
	}
	assertShadowGap(t, capture, controlplaneshadow.PathAgentPause, controlplanebridge.GapLegacyPermanentAgentControl)
	capture.Reset()
	if err := engine.AgentResume(ctx, 225, "agent:legacy", "legacy resume"); err != nil {
		t.Fatal(err)
	}
	assertShadowGap(t, capture, controlplaneshadow.PathAgentResume, controlplanebridge.GapLegacyPermanentAgentControl)
	capture.Reset()

	load := 30
	if err := engine.AgentSetLoadFactor(ctx, 225, &load, "agent:legacy", "legacy load"); err != nil {
		t.Fatal(err)
	}
	assertShadowGap(t, capture, controlplaneshadow.PathAgentSetLoad, controlplanebridge.GapLegacyPermanentAgentControl)
	capture.Reset()

	if err := engine.ForceSetLoadFactor(ctx, 225, &load, "administrator:agent", "exact load"); err != nil {
		t.Fatal(err)
	}
	assertShadowGap(t, capture, controlplaneshadow.PathForceSetLoad, controlplanebridge.GapMissingTTL)
	capture.Reset()

	until := time.Now().UTC().Add(time.Hour)
	if err := engine.PinLoad(ctx, 225, 40, until, "web", "capacity pin"); err != nil {
		t.Fatal(err)
	}
	assertShadowGap(t, capture, controlplaneshadow.PathPinLoad, controlplanebridge.GapMissingIdempotencySource)
}

func TestCompleteAdministratorAndAutonomousContextsMapWithoutChangingWrites(t *testing.T) {
	_, database, api := newEngineTest(t, false)
	capture := controlplaneshadow.NewCaptureObserver()
	engine := NewEngine(api, database, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(capture))
	if err := engine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	capture.Reset()

	createdAt := time.Now().UTC()
	expiresAt := createdAt.Add(30 * time.Minute)
	agentCtx := controlplaneshadow.WithActionContext(context.Background(), controlplaneshadow.ActionContext{
		StableSourceNamespace: controlplanebridge.SourceAgentAction,
		StableSourceID:        "goal:7/step:8/action:pause",
		SnapshotVersion:       "packet:9",
		EvidenceRefs:          []string{"packet:9", "monitor:2"},
		CreatedAt:             createdAt,
		ExpiresAt:             &expiresAt,
	})
	if err := engine.AgentPause(agentCtx, 225, "agent:v2", "evidence-backed pause"); err != nil {
		t.Fatal(err)
	}
	agent := onlyShadowObservation(t, capture.Observations())
	if !agent.Match || agent.Authority != controlplane.AuthorityAutonomousAgent || agent.Producer != controlplane.ProducerAgentOperator {
		t.Fatalf("autonomous mapped observation = %+v", agent)
	}

	capture.Reset()
	grantID := "grant-consumption:shadow-test"
	adminCtx := controlplaneshadow.WithActionContext(context.Background(), controlplaneshadow.ActionContext{
		StableSourceNamespace: controlplanebridge.SourceAdministratorGrantConsumption,
		StableSourceID:        grantID,
		CreatedAt:             createdAt,
		ExpiresAt:             &expiresAt,
		AdministratorAuthorization: controlplanebridge.AdministratorAuthorization{
			IdentityVerified: true, ExactGrant: true, GrantConsumed: true, GrantConsumptionID: grantID,
		},
	})
	if err := engine.ForceResume(adminCtx, 225, "administrator:agent", "exact recovery"); err != nil {
		t.Fatal(err)
	}
	admin := onlyShadowObservation(t, capture.Observations())
	if !admin.Match || admin.Authority != controlplane.AuthorityAdministratorCommand || admin.Producer != controlplane.ProducerAgentOperator {
		t.Fatalf("administrator mapped observation = %+v", admin)
	}
}

func TestAdministratorShadowReportsAuthoritySourceAndTTLGaps(t *testing.T) {
	createdAt := time.Now().UTC()
	expiresAt := createdAt.Add(time.Hour)
	grantID := "grant-consumption:gaps"
	tests := []struct {
		name    string
		context controlplaneshadow.ActionContext
		gap     controlplanebridge.GapCode
	}{
		{name: "missing ttl", context: controlplaneshadow.ActionContext{
			StableSourceNamespace: controlplanebridge.SourceAdministratorGrantConsumption, StableSourceID: grantID, CreatedAt: createdAt,
			AdministratorAuthorization: controlplanebridge.AdministratorAuthorization{IdentityVerified: true, ExactGrant: true, GrantConsumed: true, GrantConsumptionID: grantID},
		}, gap: controlplanebridge.GapMissingTTL},
		{name: "missing authority", context: controlplaneshadow.ActionContext{
			StableSourceNamespace: controlplanebridge.SourceAdministratorGrantConsumption, StableSourceID: grantID, CreatedAt: createdAt, ExpiresAt: &expiresAt,
		}, gap: controlplanebridge.GapMissingAuthorityContext},
		{name: "missing source", context: controlplaneshadow.ActionContext{
			StableSourceNamespace: controlplanebridge.SourceAdministratorGrantConsumption, CreatedAt: createdAt, ExpiresAt: &expiresAt,
			AdministratorAuthorization: controlplanebridge.AdministratorAuthorization{IdentityVerified: true, ExactGrant: true, GrantConsumed: true, GrantConsumptionID: grantID},
		}, gap: controlplanebridge.GapMissingIdempotencySource},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, database, api := newEngineTest(t, false)
			capture := controlplaneshadow.NewCaptureObserver()
			engine := NewEngine(api, database, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(capture))
			ctx := controlplaneshadow.WithActionContext(context.Background(), test.context)
			if err := engine.ForceResume(ctx, 225, "administrator:agent", "exact recovery"); err != nil {
				t.Fatal(err)
			}
			assertShadowGap(t, capture, controlplaneshadow.PathForceResume, test.gap)
		})
	}
}

func TestAutonomousShadowReportsMissingEvidenceWithoutElevatingAuthority(t *testing.T) {
	_, database, api := newEngineTest(t, false)
	capture := controlplaneshadow.NewCaptureObserver()
	engine := NewEngine(api, database, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(capture))
	if err := engine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	capture.Reset()
	createdAt := time.Now().UTC()
	expiresAt := createdAt.Add(time.Hour)
	ctx := controlplaneshadow.WithActionContext(context.Background(), controlplaneshadow.ActionContext{
		StableSourceNamespace: controlplanebridge.SourceAgentAction,
		StableSourceID:        "goal:7/step:8/action:resume",
		SnapshotVersion:       "packet:9",
		CreatedAt:             createdAt,
		ExpiresAt:             &expiresAt,
	})
	if err := engine.AgentPause(ctx, 225, "administrator:agent", "evidence omitted"); err != nil {
		t.Fatal(err)
	}
	observation := onlyShadowObservation(t, capture.Observations())
	if observation.Producer != controlplane.ProducerAgentOperator || observation.Authority != "" ||
		observation.GapCode != controlplanebridge.GapMissingEvidence || observation.Match {
		t.Fatalf("missing-evidence observation = %+v", observation)
	}
}

func TestPolicyShadowUsesOnlyAvailableVersionsAndCanMapControlledContext(t *testing.T) {
	_, database, api := newEngineTest(t, false)
	capture := controlplaneshadow.NewCaptureObserver()
	engine := NewEngine(api, database, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(capture))
	now := time.Date(2026, 7, 17, 4, 0, 0, 0, time.UTC)
	binding := model.ResolvedBinding{Account: api.accounts[0], Policy: model.Policy{AccountID: 225}}

	engine.observePolicySchedulable(context.Background(), controlplaneshadow.PathReconcilePolicyPause, &binding, false, "health pause", now)
	assertShadowGap(t, capture, controlplaneshadow.PathReconcilePolicyPause, controlplanebridge.GapMissingPolicyVersion)
	capture.Reset()

	policyVersionID := int64(17)
	binding.Policy.ScorePolicyVersionID = &policyVersionID
	engine.observePolicySchedulable(context.Background(), controlplaneshadow.PathReconcilePolicyPause, &binding, false, "health pause", now)
	assertShadowGap(t, capture, controlplaneshadow.PathReconcilePolicyPause, controlplanebridge.GapMissingSnapshotVersion)
	capture.Reset()

	ctx := controlplaneshadow.WithActionContext(context.Background(), controlplaneshadow.ActionContext{
		StableSourceNamespace: controlplanebridge.SourcePolicyDecision,
		StableSourceID:        "decision:17/action:pause",
		SnapshotVersion:       "decision-snapshot:17",
		CreatedAt:             now,
	})
	engine.observePolicySchedulable(ctx, controlplaneshadow.PathReconcilePolicyPause, &binding, false, "health pause", now)
	mapped := onlyShadowObservation(t, capture.Observations())
	if !mapped.Match || mapped.Authority != controlplane.AuthorityActivePolicy {
		t.Fatalf("controlled policy mapping = %+v", mapped)
	}
	capture.Reset()

	missingSource := controlplaneshadow.WithActionContext(context.Background(), controlplaneshadow.ActionContext{
		SnapshotVersion: "decision-snapshot:17", CreatedAt: now,
	})
	engine.observePolicySchedulable(missingSource, controlplaneshadow.PathReconcilePolicyResume, &binding, true, "health resume", now)
	assertShadowGap(t, capture, controlplaneshadow.PathReconcilePolicyResume, controlplanebridge.GapMissingIdempotencySource)
}

func TestPolicyPauseResumeAndLoadActualWritePointsObserveOnce(t *testing.T) {
	_, database, api := newEngineTest(t, false)
	capture := controlplaneshadow.NewCaptureObserver()
	engine := NewEngine(api, database, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(capture))
	now := time.Date(2026, 7, 17, 5, 0, 0, 0, time.UTC)
	policyVersionID := int64(17)
	monitor := api.monitors[0]
	binding := model.ResolvedBinding{
		Account: api.accounts[0], Monitor: &monitor, State: "bound",
		Policy:      model.Policy{AccountID: 225, ScorePolicyVersionID: &policyVersionID},
		FlapEnabled: true, FlapWindowMinutes: 60, FlapPauseThreshold: 3, BaseRecoveryThreshold: 3,
	}
	ctx := controlplaneshadow.WithActionContext(context.Background(), controlplaneshadow.ActionContext{
		StableSourceNamespace: controlplanebridge.SourcePolicyDecision,
		StableSourceID:        "decision:17/action:pause",
		SnapshotVersion:       "decision-snapshot:17",
		CreatedAt:             now,
	})
	control := model.AccountControl{AccountID: 225, HealthLocked: true}
	if err := engine.applyPause(ctx, &binding, &control, false, "health pause", now); err != nil {
		t.Fatal(err)
	}
	assertShadowMapped(t, capture, controlplaneshadow.PathReconcilePolicyPause)

	capture.Reset()
	ctx = controlplaneshadow.WithActionContext(context.Background(), controlplaneshadow.ActionContext{
		StableSourceNamespace: controlplanebridge.SourcePolicyDecision,
		StableSourceID:        "decision:18/action:resume",
		SnapshotVersion:       "decision-snapshot:18",
		CreatedAt:             now.Add(time.Minute),
	})
	control.HealthLocked = false
	if err := engine.applyResume(ctx, &binding, &control, false, "health resume", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertShadowMapped(t, capture, controlplaneshadow.PathReconcilePolicyResume)

	capture.Reset()
	load := 100
	binding.Account.LoadFactor = &load
	binding.Account.Schedulable = true
	control = model.AccountControl{AccountID: 225, LoadStage: model.HealthStageLimited80}
	ctx = controlplaneshadow.WithActionContext(context.Background(), controlplaneshadow.ActionContext{
		StableSourceNamespace: controlplanebridge.SourcePolicyDecision,
		StableSourceID:        "decision:19/action:load",
		SnapshotVersion:       "decision-snapshot:19",
		CreatedAt:             now.Add(2 * time.Minute),
	})
	settings := model.Settings{HealthMode: model.HealthModeAdaptive, HealthDegradedPercent: 50, HealthTrialPercent: 25, HealthMidPercent: 50}
	if err := engine.reconcileAdaptiveLoadWithFreeze(ctx, &binding, &control, settings, now.Add(2*time.Minute), false); err != nil {
		t.Fatal(err)
	}
	assertShadowMapped(t, capture, controlplaneshadow.PathReconcilePolicyLoad)
}

func TestSingleReconcileShadowSummaryCountsFinalPolicyActionOnce(t *testing.T) {
	_, database, api := newEngineTest(t, false)
	capture := controlplaneshadow.NewCaptureObserver()
	engine := NewEngine(api, database, 50*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(capture))
	checkedAt := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	applyChecks(t, engine, api, model.StatusFailed, &checkedAt, 3)

	summary := capture.Summary(engine.shadow.PanicCount())
	if summary.Total != 1 || summary.PathCounts[controlplaneshadow.PathReconcilePolicyPause] != 1 ||
		summary.Incomplete != 1 || summary.GapCounts[controlplanebridge.GapMissingPolicyVersion] != 1 {
		t.Fatalf("single reconcile shadow summary = %+v; observations=%+v", summary, capture.Observations())
	}
}

func TestRollbackAndCompensationDoNotCreateAdditionalObservation(t *testing.T) {
	engine, _, api, path := newEngineTestWithPath(t, false)
	capture := controlplaneshadow.NewCaptureObserver()
	engine = NewEngine(api, engine.store, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(capture))
	failEventCommit(t, path, "manual_pause")
	if err := engine.ManualPause(context.Background(), 225, "web"); err == nil {
		t.Fatal("manual pause unexpectedly succeeded")
	}
	if len(capture.Observations()) != 1 {
		t.Fatalf("rollback created extra observations: %+v", capture.Observations())
	}
	if !reflect.DeepEqual(api.actions, []bool{false, true}) {
		t.Fatalf("expected original write and compensation: %v", api.actions)
	}
}

func TestObserverPanicDoesNotAffectLegacyWrite(t *testing.T) {
	_, database, api := newEngineTest(t, false)
	engine := NewEngine(api, database, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(reconcilePanicObserver{}))
	if err := engine.ManualPause(context.Background(), 225, "web"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(api.actions, []bool{false}) || engine.shadow.PanicCount() != 1 {
		t.Fatalf("observer panic changed legacy path: writes=%v panics=%d", api.actions, engine.shadow.PanicCount())
	}
}

func TestShadowOffOnRequestSQLAndFinalStateAreEquivalent(t *testing.T) {
	off := runShadowEquivalenceScenario(t, controlplaneshadow.NoopObserver{})
	onCapture := controlplaneshadow.NewCaptureObserver()
	on := runShadowEquivalenceScenario(t, onCapture)
	if off.ReadRequests != on.ReadRequests || off.WriteRequests != on.WriteRequests || off.SQLQueries != on.SQLQueries || off.SQLExecs != on.SQLExecs {
		t.Fatalf("shadow changed I/O counts: off=%+v on=%+v", off, on)
	}
	if off.AccountSchedulable != on.AccountSchedulable || !reflect.DeepEqual(off.Control, on.Control) {
		t.Fatalf("shadow changed final state: off=%+v on=%+v", off, on)
	}
	if len(onCapture.Observations()) != 1 {
		t.Fatalf("enabled shadow observations = %+v", onCapture.Observations())
	}
	t.Logf("shadow off/on counts: reads=%d/%d writes=%d/%d sql_queries=%d/%d sql_execs=%d/%d",
		off.ReadRequests, on.ReadRequests, off.WriteRequests, on.WriteRequests,
		off.SQLQueries, on.SQLQueries, off.SQLExecs, on.SQLExecs)
}

type reconcilePanicObserver struct{}

func (reconcilePanicObserver) Enabled() bool                          { return true }
func (reconcilePanicObserver) Observe(controlplaneshadow.Observation) { panic("observer failure") }

type shadowEquivalenceResult struct {
	ReadRequests       int
	WriteRequests      int
	SQLQueries         int64
	SQLExecs           int64
	AccountSchedulable bool
	Control            comparableControl
}

type comparableControl struct {
	OwnsPause           bool
	Owner               string
	ManualLocked        bool
	BalanceLocked       bool
	CostLocked          bool
	ExpectedSchedulable bool
	ManualOverride      bool
	LastDecision        string
}

func runShadowEquivalenceScenario(t *testing.T, observer controlplaneshadow.Observer) shadowEquivalenceResult {
	t.Helper()
	fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: 1, Monitors: 1, Now: time.Date(2026, 7, 17, 6, 0, 0, 0, time.UTC)})
	fixture.Accounts[0].Schedulable = false
	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	api := testsupport.NewFakeSub2API(fixture)
	engine := NewEngine(api, database.Store, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithControlplaneShadow(observer))
	api.ResetStats()
	database.SQLCounter.Reset()
	if err := engine.ForceResume(context.Background(), 1, "web", "fixed comparison"); err != nil {
		t.Fatal(err)
	}
	stats := api.Stats()
	counts := database.SQLCounter.Snapshot()
	accounts, err := api.ListAccounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	control, err := database.Store.GetControl(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	expected := false
	if control.ExpectedSchedulable != nil {
		expected = *control.ExpectedSchedulable
	}
	return shadowEquivalenceResult{
		ReadRequests:  stats.ByName[testsupport.CallListAccounts] + stats.ByName[testsupport.CallListMonitors],
		WriteRequests: stats.ByName[testsupport.CallSetSchedulable] + stats.ByName[testsupport.CallUpdateLoadFactor],
		SQLQueries:    counts.Queries, SQLExecs: counts.Execs, AccountSchedulable: accounts[0].Schedulable,
		Control: comparableControl{OwnsPause: control.OwnsPause, Owner: control.Owner, ManualLocked: control.ManualLocked,
			BalanceLocked: control.BalanceLocked, CostLocked: control.CostLocked, ExpectedSchedulable: expected,
			ManualOverride: control.ManualOverrideUntil != nil, LastDecision: control.LastDecision},
	}
}

func onlyShadowObservation(t *testing.T, observations []controlplaneshadow.Observation) controlplaneshadow.Observation {
	t.Helper()
	if len(observations) != 1 {
		t.Fatalf("observations = %d, want 1: %+v", len(observations), observations)
	}
	return observations[0]
}

func assertShadowGap(t *testing.T, capture *controlplaneshadow.CaptureObserver, path controlplaneshadow.Path, gap controlplanebridge.GapCode) {
	t.Helper()
	observation := onlyShadowObservation(t, capture.Observations())
	if observation.Path != path || observation.ConversionStatus == controlplanebridge.ConversionMapped || observation.GapCode != gap || observation.Match {
		t.Fatalf("observation = %+v, want path=%s gap=%s", observation, path, gap)
	}
}

func assertShadowMapped(t *testing.T, capture *controlplaneshadow.CaptureObserver, path controlplaneshadow.Path) {
	t.Helper()
	observation := onlyShadowObservation(t, capture.Observations())
	if observation.Path != path || observation.ConversionStatus != controlplanebridge.ConversionMapped || !observation.Match || observation.Authority != controlplane.AuthorityActivePolicy {
		t.Fatalf("mapped observation = %+v", observation)
	}
}
