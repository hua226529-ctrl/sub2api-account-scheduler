package failover

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type blockingEvidenceStore struct {
	*fakeStore
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (s *blockingEvidenceStore) ListGroupValidationEvidence(context.Context, []int64, []int64, int64, int64) ([]model.GroupValidationEvidence, error) {
	s.calls.Add(1)
	s.entered <- struct{}{}
	<-s.release
	return nil, nil
}

func TestFailoverEvidenceProcessingIsSingleFlight(t *testing.T) {
	now := time.Date(2026, 7, 18, 0, 15, 0, 0, time.UTC)
	base, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	base.policies[0].State = awaitingEvidenceState(1, "11", now.Add(-time.Second), now, now.Add(time.Minute))
	store := &blockingEvidenceStore{fakeStore: base, entered: make(chan struct{}, 1), release: make(chan struct{})}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	done := make(chan error, 2)

	go func() { done <- controller.ProcessFailoverEvidence(context.Background()) }()
	<-store.entered
	go func() { done <- controller.ProcessFailoverEvidence(context.Background()) }()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	close(store.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls := store.calls.Load(); calls != 1 {
		t.Fatalf("concurrent evidence processors ran %d database passes", calls)
	}
}

func TestCurrentLevelReasonCodesAndDisabledChannelAdvance(t *testing.T) {
	now := time.Date(2026, 7, 18, 0, 30, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	assessment, err := controller.assessPool(context.Background(), nil, now, store.dispatch)
	if err != nil || assessment.ReasonCode != "group_empty" || assessment.Outage {
		t.Fatalf("empty fixed group assessment = %+v, %v", assessment, err)
	}
	assessment, err = controller.assessPool(context.Background(), snapshots.snapshot.Bindings, now, store.dispatch)
	if err != nil || assessment.ReasonCode != "evidence_insufficient" || assessment.Outage {
		t.Fatalf("insufficient evidence assessment = %+v, %v", assessment, err)
	}

	snapshots.snapshot.Bindings[0].Account.Schedulable = false
	assessment, err = controller.assessPool(context.Background(), snapshots.snapshot.Bindings, now, store.dispatch)
	if err != nil || assessment.ReasonCode != "no_schedulable_channels" || !assessment.Outage {
		t.Fatalf("unschedulable assessment = %+v, %v", assessment, err)
	}
	snapshots.snapshot.Bindings[0].Account.Status = "disabled"
	assessment, err = controller.assessPool(context.Background(), snapshots.snapshot.Bindings, now, store.dispatch)
	if err != nil || assessment.ReasonCode != "all_channels_disabled" || !assessment.Outage {
		t.Fatalf("disabled assessment = %+v, %v", assessment, err)
	}

	controller.outageSince["pool-a"] = now.Add(-2 * time.Minute)
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 1 || upstreams.transitions[0].TargetTier != model.GroupTierBackup {
		t.Fatalf("disabled current level did not advance to fixed backup: %+v", upstreams.transitions)
	}

	stale := now.Add(-time.Hour)
	snapshots.snapshot.LastSyncAt = &stale
	if err := controller.RunOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "data_stale") {
		t.Fatalf("stale snapshot reason = %v", err)
	}
}

func TestCurrentLevelHardEvidenceReasonIsAllChannelsFailed(t *testing.T) {
	now := time.Date(2026, 7, 18, 0, 45, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 5)
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	assessment, err := controller.assessPool(context.Background(), snapshots.snapshot.Bindings, now, store.dispatch)
	if err != nil || assessment.ReasonCode != "all_channels_failed" || !assessment.Outage {
		t.Fatalf("hard failure assessment = %+v, %v", assessment, err)
	}
}

func TestFixedFailoverSkipsDisabledBackupAndUsesEmergency(t *testing.T) {
	now := time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 5)
	store.policies[0].MainEnabled = true
	store.policies[0].BackupEnabled = false
	store.policies[0].EmergencyEnabled = true
	store.policies[0].State.ValidationStatus = model.GroupValidationConfirmedFailed
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 1 || upstreams.transitions[0].TargetTier != model.GroupTierEmergency {
		t.Fatalf("fixed chain did not skip disabled backup: %+v", upstreams.transitions)
	}
}

func TestFixedTransitionDoesNotRequirePreSwitchTargetTelemetry(t *testing.T) {
	now := time.Date(2026, 7, 18, 1, 10, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 5)
	store.policies[0].State.ValidationStatus = model.GroupValidationConfirmedFailed
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 1 || upstreams.transitions[0].TargetTier != model.GroupTierBackup {
		t.Fatalf("missing target telemetry blocked the configured next level: %+v", upstreams.transitions)
	}
}

func TestDisabledBackupCurrentLevelAdvancesToFixedEmergency(t *testing.T) {
	now := time.Date(2026, 7, 18, 1, 20, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	store.policies[0].State.CurrentTier = model.GroupTierBackup
	store.policies[0].State.ObservedGroupID = "backup"
	store.policies[0].State.ValidationStatus = model.GroupValidationConfirmedHealthy
	upstreams.sources[0].KeyRates[0].GroupID = "backup"
	snapshots.snapshot.Bindings[0].Account.Status = "disabled"
	snapshots.snapshot.Bindings[0].Account.Schedulable = false
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }
	controller.outageSince["pool-a"] = now.Add(-2 * time.Minute)

	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 1 || upstreams.transitions[0].TargetTier != model.GroupTierEmergency {
		t.Fatalf("disabled backup level did not advance to fixed emergency: %+v", upstreams.transitions)
	}
}

func TestFixedFailoverEmergencyFailureBecomesExhausted(t *testing.T) {
	now := time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 5)
	store.policies[0].State.CurrentTier = model.GroupTierEmergency
	store.policies[0].State.ObservedGroupID = "emergency"
	store.policies[0].State.ValidationStatus = model.GroupValidationConfirmedFailed
	upstreams.sources[0].KeyRates[0].GroupID = "emergency"
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	state := store.policies[0].State
	if state.ValidationStatus != model.GroupValidationExhausted || !state.Frozen || state.LastError != "fixed_failover_levels_exhausted" {
		t.Fatalf("emergency failure was not exhausted: %+v", state)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatalf("exhausted chain continued switching: %+v", upstreams.transitions)
	}
	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatalf("exhausted chain switched on a later cycle: %+v", upstreams.transitions)
	}
}

func TestPostSwitchEvidenceRequiresWatermarkAndPropagationDelay(t *testing.T) {
	now := time.Date(2026, 7, 18, 2, 0, 10, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	notBefore := now.Add(-5 * time.Second)
	verified := now.Add(-10 * time.Second)
	deadline := now.Add(time.Minute)
	store.policies[0].State = awaitingEvidenceState(1, "11", verified, notBefore, deadline)
	store.policies[0].State.MonitorWatermark = 10
	store.policies[0].State.MonitorEvidenceCursor = 10
	store.evidence = []model.GroupValidationEvidence{
		startedEvidence(10, "monitor", 7, 0, model.StatusOperational, now.Add(-time.Second)),
		startedEvidence(11, "monitor", 7, 0, model.StatusOperational, notBefore.Add(-time.Nanosecond)),
	}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationAwaitingEvidence || state.SuccessfulEvidenceCount != 0 || state.MonitorEvidenceCursor != 11 {
		t.Fatalf("old or in-flight evidence validated target group: %+v", state)
	}
	store.evidence = append(store.evidence, startedEvidence(12, "monitor", 7, 0, model.StatusOperational, now))
	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationConfirmedHealthy || state.SuccessfulEvidenceCount != 1 || state.LastEvidenceID != "monitor:12" {
		t.Fatalf("new post-switch monitor evidence did not validate target: %+v", state)
	}
}

func TestPostSwitchTrafficWithoutRequestStartCannotConfirm(t *testing.T) {
	now := time.Date(2026, 7, 18, 2, 30, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	state := awaitingEvidenceState(1, "11", now.Add(-time.Minute), now.Add(-30*time.Second), now.Add(time.Minute))
	state.TrafficWatermark, state.TrafficEvidenceCursor = 20, 20
	store.policies[0].State = state
	store.evidence = []model.GroupValidationEvidence{{ID: 21, Source: "traffic", AccountID: 101, Status: "success", ObservedAt: now}}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationAwaitingEvidence || state.SuccessfulEvidenceCount != 0 || state.TrafficEvidenceCursor != 21 {
		t.Fatalf("traffic without request start validated target: %+v", state)
	}
}

func TestPostSwitchTrafficWithoutRequestStartCannotConfirmFailure(t *testing.T) {
	now := time.Date(2026, 7, 18, 2, 32, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	state := awaitingEvidenceState(1, "11", now.Add(-time.Minute), now.Add(-30*time.Second), now.Add(time.Minute))
	store.policies[0].State = state
	for id := int64(1); id <= 3; id++ {
		store.evidence = append(store.evidence, model.GroupValidationEvidence{
			ID: id, Source: "traffic", AccountID: 101, Status: "error", ErrorClass: model.ErrorClassInfrastructure,
			ObservedAt: now, TimeBasis: model.EvidenceTimeBasisCompletion,
		})
	}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationAwaitingEvidence || state.FailedEvidenceCount != 0 || state.TrafficEvidenceCursor != 3 {
		t.Fatalf("traffic completion timestamps confirmed failure: %+v", state)
	}
}

func TestPostSwitchTrafficWithRequestStartConfirmsHealthy(t *testing.T) {
	now := time.Date(2026, 7, 18, 2, 35, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	state := awaitingEvidenceState(1, "11", now.Add(-time.Minute), now.Add(-30*time.Second), now.Add(time.Minute))
	state.TrafficWatermark, state.TrafficEvidenceCursor = 20, 20
	store.policies[0].State = state
	store.evidence = []model.GroupValidationEvidence{startedEvidence(21, "traffic", 0, 101, "success", now)}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationConfirmedHealthy || state.LastEvidenceSource != "traffic" {
		t.Fatalf("attributed post-switch traffic did not validate target: %+v", state)
	}
}

func TestRequestStartedBeforeSwitchCannotValidateAfterLateCompletion(t *testing.T) {
	verified := time.Date(2026, 7, 18, 2, 40, 0, 0, time.UTC)
	now := verified.Add(20 * time.Second)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	state := awaitingEvidenceState(1, "11", verified, verified.Add(model.GroupValidationPropagationDelay), now.Add(time.Minute))
	state.TrafficWatermark, state.TrafficEvidenceCursor = 30, 30
	store.policies[0].State = state
	item := startedEvidence(31, "traffic", 0, 101, "success", verified.Add(-time.Second))
	item.ObservedAt = now
	store.evidence = []model.GroupValidationEvidence{item}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationAwaitingEvidence || state.SuccessfulEvidenceCount != 0 {
		t.Fatalf("pre-switch request completed after the old five-second boundary validated target: %+v", state)
	}
}

func TestCompletionOnlyMonitorUsesMaximumRequestTimeoutBoundary(t *testing.T) {
	verified := time.Date(2026, 7, 18, 2, 45, 0, 0, time.UTC)
	safeBoundary := verified.Add(model.GroupValidationPropagationDelay + model.GroupValidationMonitorRequestTimeout)
	now := safeBoundary.Add(time.Second)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	store.policies[0].State = awaitingEvidenceState(1, "11", verified, verified.Add(model.GroupValidationPropagationDelay), now.Add(time.Minute))
	store.evidence = []model.GroupValidationEvidence{{ID: 1, Source: "monitor", MonitorID: 7, Status: model.StatusOperational, ObservedAt: safeBoundary.Add(-time.Nanosecond), TimeBasis: model.EvidenceTimeBasisCompletion}}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationAwaitingEvidence || state.SuccessfulEvidenceCount != 0 {
		t.Fatalf("completion-only monitor validated before the conservative timeout boundary: %+v", state)
	}
	store.evidence = append(store.evidence, model.GroupValidationEvidence{ID: 2, Source: "monitor", MonitorID: 7, Status: model.StatusOperational, ObservedAt: safeBoundary, TimeBasis: model.EvidenceTimeBasisCompletion})
	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationConfirmedHealthy {
		t.Fatalf("completion-only monitor was not accepted at its proven-safe boundary: %+v", state)
	}
}

func TestEvidenceWithMismatchedTransitionOrTargetIsIgnored(t *testing.T) {
	now := time.Date(2026, 7, 18, 2, 50, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	store.policies[0].State = awaitingEvidenceState(1, "11", now.Add(-time.Minute), now.Add(-30*time.Second), now.Add(time.Minute))
	wrongTransition := startedEvidence(1, "monitor", 7, 0, model.StatusOperational, now)
	wrongTransition.TransitionID = 8
	wrongTier := startedEvidence(2, "monitor", 7, 0, model.StatusOperational, now)
	wrongTier.TransitionID, wrongTier.SourceID, wrongTier.KeyID = 7, 1, "11"
	wrongTier.TargetTier, wrongTier.TargetGroupID = model.GroupTierEmergency, "emergency"
	store.evidence = []model.GroupValidationEvidence{wrongTransition, wrongTier}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationAwaitingEvidence || state.SuccessfulEvidenceCount != 0 {
		t.Fatalf("mismatched evidence validated the current transition: %+v", state)
	}
}

func TestPostSwitchFailuresRequireThresholdAndTimeoutDoesNotAdvance(t *testing.T) {
	now := time.Date(2026, 7, 18, 3, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusFailed, 5)
	store.policies[0].State = awaitingEvidenceState(1, "11", now.Add(-time.Minute), now.Add(-30*time.Second), now.Add(time.Minute))
	store.evidence = []model.GroupValidationEvidence{startedEvidence(1, "monitor", 7, 0, model.StatusFailed, now)}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationAwaitingEvidence || state.FailedEvidenceCount != 1 {
		t.Fatalf("single failed observation advanced fixed chain: %+v", state)
	}
	store.evidence = append(store.evidence,
		startedEvidence(2, "monitor", 7, 0, model.StatusFailed, now),
		startedEvidence(3, "monitor", 7, 0, model.StatusFailed, now),
	)
	if err := controller.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationConfirmedFailed || state.FailedEvidenceCount != 3 {
		t.Fatalf("failure threshold was not persisted: %+v", state)
	}

	timeoutStore, timeoutSnapshots, timeoutUpstreams := failoverFixture(now, model.StatusOperational, 0)
	timeoutStore.policies[0].State = awaitingEvidenceState(1, "11", now.Add(-time.Minute), now.Add(-30*time.Second), now.Add(-time.Nanosecond))
	timeoutController := NewController(timeoutStore, timeoutSnapshots, timeoutUpstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	timeoutController.now = func() time.Time { return now }
	if err := timeoutController.ProcessFailoverEvidence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := timeoutStore.policies[0].State; state.ValidationStatus != model.GroupValidationUncertain || state.LastError != "evidence_timeout" {
		t.Fatalf("evidence timeout did not become non-advancing uncertain: %+v", state)
	}
	if len(timeoutUpstreams.transitions) != 0 {
		t.Fatalf("evidence timeout performed a blind transition: %+v", timeoutUpstreams.transitions)
	}
}

func TestPeriodicRunMarksEvidenceTimeoutUncertainWithoutNewTelemetry(t *testing.T) {
	now := time.Date(2026, 7, 18, 3, 30, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	store.policies[0].State = awaitingEvidenceState(1, "11", now.Add(-time.Minute), now.Add(-30*time.Second), now.Add(-time.Nanosecond))
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default())
	controller.now = func() time.Time { return now }

	if err := controller.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationUncertain || state.LastError != "evidence_timeout" {
		t.Fatalf("periodic run left an expired evidence window actionable: %+v", state)
	}
	if len(upstreams.transitions) != 0 {
		t.Fatalf("periodic timeout processing advanced without evidence: %+v", upstreams.transitions)
	}
}

type fakePostSwitchProbe struct {
	results  []PostSwitchProbeResult
	errors   []error
	calls    int
	requests []PostSwitchProbeRequest
}

func (f *fakePostSwitchProbe) Probe(_ context.Context, request PostSwitchProbeRequest) (PostSwitchProbeResult, error) {
	index := f.calls
	f.calls++
	f.requests = append(f.requests, request)
	var result PostSwitchProbeResult
	var err error
	if index < len(f.results) {
		result = f.results[index]
	}
	if index < len(f.errors) {
		err = f.errors[index]
	}
	if result.TransitionID == 0 {
		result.TransitionID = request.TransitionID
		result.SourceID = request.SourceID
		result.KeyID = request.KeyID
		result.TargetTier = request.TargetTier
		result.TargetGroupID = request.TargetGroupID
		result.RequestStartedAt = request.RequestStartedAt
	}
	return result, err
}

func TestActiveProbeOnlyAfterAppliedTransitionAndUsesFailureThreshold(t *testing.T) {
	now := time.Date(2026, 7, 18, 4, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	probe := &fakePostSwitchProbe{results: []PostSwitchProbeResult{
		{Status: model.StatusFailed, EvidenceID: "probe-1", ObservedAt: now, ReasonCode: "failed"},
		{Status: model.StatusFailed, EvidenceID: "probe-2", ObservedAt: now, ReasonCode: "failed"},
		{Status: model.StatusFailed, EvidenceID: "probe-3", ObservedAt: now, ReasonCode: "failed"},
	}}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default(), WithPostSwitchProbe(probe))
	controller.now = func() time.Time { return now }
	if err := controller.RunPostSwitchProbe(context.Background(), 1, "11"); err == nil {
		t.Fatal("probe ran before an applied transition entered awaiting_evidence")
	}
	store.policies[0].State = awaitingEvidenceState(1, "11", now.Add(-time.Second), now, now.Add(time.Minute))
	for attempt := 1; attempt <= 3; attempt++ {
		if err := controller.RunPostSwitchProbe(context.Background(), 1, "11"); err != nil {
			t.Fatal(err)
		}
		state := store.policies[0].State
		if attempt < 3 && state.ValidationStatus != model.GroupValidationAwaitingEvidence {
			t.Fatalf("probe failure %d advanced too early: %+v", attempt, state)
		}
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationConfirmedFailed || state.FailedEvidenceCount != 3 || state.ActiveProbeAttempts != 3 {
		t.Fatalf("active probe threshold state = %+v", state)
	}
	if len(probe.requests) != 3 || !probe.requests[0].RequestStartedAt.After(now.Add(-time.Second)) || probe.requests[0].RequestStartedAt.Before(now) {
		t.Fatalf("probe did not start inside the post-switch evidence window: %+v", probe.requests)
	}
}

func TestActiveProbeSuccessConfirmsHealthy(t *testing.T) {
	now := time.Date(2026, 7, 18, 4, 30, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	store.policies[0].State = awaitingEvidenceState(1, "11", now.Add(-time.Second), now, now.Add(time.Minute))
	probe := &fakePostSwitchProbe{results: []PostSwitchProbeResult{{Status: "success", EvidenceID: "probe-ok", ObservedAt: now}}}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default(), WithPostSwitchProbe(probe))
	controller.now = func() time.Time { return now }

	if err := controller.RunPostSwitchProbe(context.Background(), 1, "11"); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationConfirmedHealthy || state.LastEvidenceSource != "active_probe" {
		t.Fatalf("successful active probe did not validate target: %+v", state)
	}
}

func TestActiveProbeUncertainDoesNotAdvance(t *testing.T) {
	now := time.Date(2026, 7, 18, 5, 0, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	store.policies[0].State = awaitingEvidenceState(1, "11", now.Add(-time.Second), now, now.Add(time.Minute))
	probe := &fakePostSwitchProbe{errors: []error{errors.New("probe timeout")}}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default(), WithPostSwitchProbe(probe))
	controller.now = func() time.Time { return now }

	if err := controller.RunPostSwitchProbe(context.Background(), 1, "11"); err != nil {
		t.Fatal(err)
	}
	if state := store.policies[0].State; state.ValidationStatus != model.GroupValidationUncertain || state.FailedEvidenceCount != 0 {
		t.Fatalf("uncertain active probe was treated as a confirmed failure: %+v", state)
	}
}

type blockingPostSwitchProbe struct {
	entered chan PostSwitchProbeRequest
	release chan struct{}
}

func (p *blockingPostSwitchProbe) Probe(_ context.Context, request PostSwitchProbeRequest) (PostSwitchProbeResult, error) {
	p.entered <- request
	<-p.release
	return PostSwitchProbeResult{
		TransitionID: request.TransitionID, SourceID: request.SourceID, KeyID: request.KeyID,
		TargetTier: request.TargetTier, TargetGroupID: request.TargetGroupID,
		RequestStartedAt: request.RequestStartedAt, Status: "success", EvidenceID: "old-probe", ObservedAt: request.RequestStartedAt,
	}, nil
}

func TestOldProbeResultCannotValidateSupersedingTransition(t *testing.T) {
	now := time.Date(2026, 7, 18, 5, 30, 0, 0, time.UTC)
	store, snapshots, upstreams := failoverFixture(now, model.StatusOperational, 0)
	store.policies[0].State = awaitingEvidenceState(1, "11", now.Add(-time.Minute), now.Add(-time.Second), now.Add(time.Minute))
	probe := &blockingPostSwitchProbe{entered: make(chan PostSwitchProbeRequest, 1), release: make(chan struct{})}
	controller := NewController(store, snapshots, upstreams, fakeTelemetry{at: now}, 50*time.Second, slog.Default(), WithPostSwitchProbe(probe))
	controller.now = func() time.Time { return now }
	done := make(chan error, 1)
	go func() { done <- controller.RunPostSwitchProbe(context.Background(), 1, "11") }()
	request := <-probe.entered
	if request.TransitionID != 7 || request.TargetTier != model.GroupTierBackup || request.TargetGroupID != "backup" {
		t.Fatalf("probe was not bound to the active transition: %+v", request)
	}
	replacement := awaitingEvidenceState(1, "11", now, now.Add(time.Second), now.Add(time.Minute))
	replacement.ValidationTransitionID = 8
	replacement.ValidationTargetTier = model.GroupTierEmergency
	replacement.ValidationTargetGroupID = "emergency"
	if err := store.SaveGroupFailoverState(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	close(probe.release)
	if err := <-done; err == nil {
		t.Fatal("superseded probe result was accepted")
	}
	if state := store.policies[0].State; state.ValidationTransitionID != 8 || state.ValidationStatus != model.GroupValidationAwaitingEvidence || state.SuccessfulEvidenceCount != 0 {
		t.Fatalf("old probe overwrote the superseding transition: %+v", state)
	}
}

func awaitingEvidenceState(sourceID int64, keyID string, verified, notBefore, deadline time.Time) model.GroupFailoverState {
	return model.GroupFailoverState{
		SourceID: sourceID, KeyID: keyID, CurrentTier: model.GroupTierBackup, ObservedGroupID: "backup",
		ValidationStatus: model.GroupValidationAwaitingEvidence, ValidationMode: model.GroupValidationModePassive,
		ValidationTransitionID: 7, ValidationFromTier: model.GroupTierMain, ValidationTargetTier: model.GroupTierBackup,
		ValidationFromGroupID: "main", ValidationTargetGroupID: "backup", SwitchVerifiedAt: &verified,
		ValidationNotBefore: &notBefore, EvidenceDeadline: &deadline,
	}
}

func startedEvidence(id int64, source string, monitorID, accountID int64, status string, startedAt time.Time) model.GroupValidationEvidence {
	started := startedAt.UTC()
	basis := model.EvidenceTimeBasisRequestStart
	if source == "monitor" {
		basis = model.EvidenceTimeBasisMonitorRequestStart
	}
	return model.GroupValidationEvidence{
		ID: id, Source: source, MonitorID: monitorID, AccountID: accountID, Status: status,
		ObservedAt: started, RequestStartedAt: &started, TimeBasis: basis,
	}
}
