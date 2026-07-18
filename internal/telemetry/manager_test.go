package telemetry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/sub2api"
)

type failoverEvidenceProcessorFake struct {
	database  *store.Store
	monitorID int64
	calls     int
	err       error
	committed bool
}

func (f *failoverEvidenceProcessorFake) ProcessFailoverEvidence(ctx context.Context) error {
	f.calls++
	items, err := f.database.ListGroupValidationEvidence(ctx, []int64{f.monitorID}, nil, 0, 0)
	if err != nil {
		return err
	}
	f.committed = len(items) > 0
	return f.err
}

type fakeTelemetryAPI struct {
	monitor  model.Monitor
	history  []model.MonitorHistoryRecord
	success  []model.TrafficSuccess
	failures []model.TrafficError
	queries  []sub2api.TelemetryQuery
}

type reconcileRequestFake struct{ accounts chan []int64 }

func (f *reconcileRequestFake) RequestAccounts(ids ...int64) {
	f.accounts <- append([]int64(nil), ids...)
}
func (f *reconcileRequestFake) RequestFull() {}

type telemetryResolverFake struct{ accountID int64 }

func (f telemetryResolverFake) AccountIDsForMonitors(...int64) []int64 { return []int64{f.accountID} }

type filteringTelemetryResolverFake struct {
	accepted map[int64]bool
}

func (f filteringTelemetryResolverFake) AccountIDsForMonitors(...int64) []int64 { return nil }
func (f filteringTelemetryResolverFake) FilterReconcileAccountIDs(ids ...int64) ([]int64, []int64) {
	accepted, ignored := make([]int64, 0), make([]int64, 0)
	for _, id := range ids {
		if f.accepted[id] {
			accepted = append(accepted, id)
		} else {
			ignored = append(ignored, id)
		}
	}
	return accepted, ignored
}

type telemetryPassFake struct{ started chan time.Time }

func (f *telemetryPassFake) ReconcileFull(context.Context) error {
	f.started <- time.Now()
	return nil
}

func (f *telemetryPassFake) ReconcileAccounts(context.Context, []int64) error {
	f.started <- time.Now()
	return nil
}

type timedTelemetryRequester struct {
	coordinator *reconcile.Coordinator
	requested   chan time.Time
	source      chan string
}

func (r *timedTelemetryRequester) RequestAccounts(ids ...int64) {
	r.requested <- time.Now()
	r.coordinator.RequestAccounts(ids...)
}

func (r *timedTelemetryRequester) RequestFull() { r.coordinator.RequestFull() }

func (r *timedTelemetryRequester) RequestAccountsFrom(source string, ids ...int64) {
	r.source <- source
	r.requested <- time.Now()
	r.coordinator.RequestAccountsFrom(source, ids...)
}

func (r *timedTelemetryRequester) RequestFullFrom(source string) {
	r.coordinator.RequestFullFrom(source)
}

func (f *fakeTelemetryAPI) ListMonitors(context.Context) ([]model.Monitor, error) {
	return []model.Monitor{f.monitor}, nil
}

func (f *fakeTelemetryAPI) ListMonitorHistory(_ context.Context, _ int64, query sub2api.TelemetryQuery) ([]model.MonitorHistoryRecord, error) {
	f.queries = append(f.queries, query)
	return append([]model.MonitorHistoryRecord(nil), f.history...), nil
}

func (f *fakeTelemetryAPI) ListSuccessfulRequests(_ context.Context, query sub2api.TelemetryQuery) ([]model.TrafficSuccess, error) {
	f.queries = append(f.queries, query)
	return append([]model.TrafficSuccess(nil), f.success...), nil
}

func (f *fakeTelemetryAPI) ListRequestErrors(_ context.Context, query sub2api.TelemetryQuery) ([]model.TrafficError, error) {
	f.queries = append(f.queries, query)
	return append([]model.TrafficError(nil), f.failures...), nil
}

func TestManagerImportsOverlappingWindowsIdempotently(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Now().UTC().Add(-time.Minute)
	api := &fakeTelemetryAPI{
		monitor:  model.Monitor{ID: 2, PrimaryModel: "gpt"},
		history:  []model.MonitorHistoryRecord{{SourceID: 1, MonitorID: 2, Model: "gpt", Status: model.StatusOperational, LatencyMS: 1_200, CheckedAt: now}},
		success:  []model.TrafficSuccess{{EventKey: "request-one", AccountID: 225, Model: "gpt", DurationMS: 1_200, CreatedAt: now}},
		failures: []model.TrafficError{{EventKey: "request-two", AccountID: 225, Model: "gpt-mini", ErrorClass: model.ErrorClassModelCapability, ReasonCode: "model_unsupported", CreatedAt: now}},
	}
	manager := NewManager(api, database, 2*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := manager.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	history, err := database.ListMonitorHistory(ctx, 2, "gpt", 10)
	if err != nil || len(history) != 1 {
		t.Fatalf("overlapping monitor reads must be idempotent: count=%d err=%v", len(history), err)
	}
	window, err := database.GetTrafficWindow(ctx, 225, now.Add(-time.Minute), time.Now().UTC().Add(time.Minute))
	if err != nil || window.SuccessCount != 1 || window.CapabilityErrors != 1 {
		t.Fatalf("overlapping traffic reads must be idempotent: %+v err=%v", window, err)
	}
	capabilities, err := database.ListAccountModelCapabilities(ctx, 225)
	if err != nil || len(capabilities) != 2 {
		t.Fatalf("model capabilities were not refreshed: %+v err=%v", capabilities, err)
	}
	if len(api.queries) != 6 || !api.queries[3].Since.After(api.queries[0].Since) {
		t.Fatalf("second poll should use an overlapping cursor instead of refetching a full window: %+v", api.queries)
	}
}

func TestTelemetryRequestsOnlyAccountsWithNewCommittedTraffic(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Now().UTC().Add(-time.Minute)
	api := &fakeTelemetryAPI{
		monitor: model.Monitor{ID: 2, PrimaryModel: "gpt"},
		history: []model.MonitorHistoryRecord{{SourceID: 1, MonitorID: 2, Model: "gpt", Status: model.StatusOperational, CheckedAt: now}},
		success: []model.TrafficSuccess{{EventKey: "new-request", AccountID: 225, Model: "gpt", CreatedAt: now}},
	}
	requester := &reconcileRequestFake{accounts: make(chan []int64, 2)}
	manager := NewManager(api, database, 2*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), WithReconcileRequester(requester))
	if err := manager.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case ids := <-requester.accounts:
		if len(ids) != 1 || ids[0] != 225 {
			t.Fatalf("unexpected targeted telemetry request: %#v", ids)
		}
	default:
		t.Fatal("new committed telemetry did not request account reconcile")
	}
	if err := manager.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case ids := <-requester.accounts:
		t.Fatalf("duplicate telemetry unexpectedly requested accounts: %#v", ids)
	default:
	}
}

func TestTelemetryFiltersUnboundTargetedAccountsAfterCommit(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Now().UTC().Add(-time.Minute)
	api := &fakeTelemetryAPI{monitor: model.Monitor{ID: 2, PrimaryModel: "gpt"},
		success: []model.TrafficSuccess{
			{EventKey: "bound-request", AccountID: 225, Model: "gpt", CreatedAt: now},
			{EventKey: "unbound-oauth-request", AccountID: 295, Model: "gpt", CreatedAt: now},
		}}
	requester := &reconcileRequestFake{accounts: make(chan []int64, 1)}
	resolver := filteringTelemetryResolverFake{accepted: map[int64]bool{225: true}}
	manager := NewManager(api, database, 2*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithReconcileRequester(requester), WithMonitorAccountResolver(resolver))
	if err := manager.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case ids := <-requester.accounts:
		if len(ids) != 1 || ids[0] != 225 {
			t.Fatalf("unbound account reached targeted reconcile: %#v", ids)
		}
	default:
		t.Fatal("valid bound account was not requested")
	}
}

func TestTelemetryProcessesFailoverEvidenceOnlyAfterCommit(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Now().UTC().Add(-time.Second)
	api := &fakeTelemetryAPI{
		monitor: model.Monitor{ID: 2, PrimaryModel: "gpt"},
		history: []model.MonitorHistoryRecord{{MonitorID: 2, Model: "gpt", Status: model.StatusOperational, CheckedAt: now}},
	}
	processor := &failoverEvidenceProcessorFake{database: database, monitorID: 2}
	manager := NewManager(api, database, 2*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	manager.SetFailoverEvidenceProcessor(processor)
	if err := manager.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if processor.calls != 1 || !processor.committed {
		t.Fatalf("failover processor ran before commit or was not called: %+v", processor)
	}
	if err := manager.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if processor.calls != 1 {
		t.Fatalf("duplicate telemetry woke failover evidence processor: calls=%d", processor.calls)
	}
}

func TestTelemetryFailoverEvidenceErrorDoesNotRollbackCommittedMonitor(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Now().UTC().Add(-time.Second)
	api := &fakeTelemetryAPI{
		monitor: model.Monitor{ID: 2, PrimaryModel: "gpt"},
		history: []model.MonitorHistoryRecord{{MonitorID: 2, Model: "gpt", Status: model.StatusOperational, CheckedAt: now}},
	}
	processor := &failoverEvidenceProcessorFake{database: database, monitorID: 2, err: errors.New("injected evidence failure")}
	manager := NewManager(api, database, 2*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	manager.SetFailoverEvidenceProcessor(processor)
	err = manager.RunOnce(ctx)
	var partial *PartialError
	if !errors.As(err, &partial) || len(partial.Issues) != 1 || partial.Issues[0].Code != "failover_evidence_failed" {
		t.Fatalf("evidence error was not isolated as partial success: %v", err)
	}
	history, queryErr := database.ListMonitorHistory(ctx, 2, "gpt", 10)
	if queryErr != nil || len(history) != 1 || !processor.committed {
		t.Fatalf("evidence processor failure rolled back monitor commit: history=%+v err=%v", history, queryErr)
	}
}

type concurrencyTelemetryAPI struct {
	monitors []model.Monitor
	release  chan struct{}
	started  chan struct{}
	active   atomic.Int32
	maximum  atomic.Int32
}

func (f *concurrencyTelemetryAPI) ListMonitors(context.Context) ([]model.Monitor, error) {
	return append([]model.Monitor(nil), f.monitors...), nil
}

func (f *concurrencyTelemetryAPI) ListMonitorHistory(ctx context.Context, monitorID int64, _ sub2api.TelemetryQuery) ([]model.MonitorHistoryRecord, error) {
	active := f.active.Add(1)
	for {
		maximum := f.maximum.Load()
		if active <= maximum || f.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	f.started <- struct{}{}
	select {
	case <-ctx.Done():
		f.active.Add(-1)
		return nil, ctx.Err()
	case <-f.release:
	}
	f.active.Add(-1)
	return []model.MonitorHistoryRecord{{SourceID: 1, MonitorID: monitorID, Model: "gpt", Status: model.StatusOperational, CheckedAt: time.Now().UTC()}}, nil
}

func (f *concurrencyTelemetryAPI) ListSuccessfulRequests(context.Context, sub2api.TelemetryQuery) ([]model.TrafficSuccess, error) {
	return nil, nil
}

func (f *concurrencyTelemetryAPI) ListRequestErrors(context.Context, sub2api.TelemetryQuery) ([]model.TrafficError, error) {
	return nil, nil
}

func TestTelemetryMonitorFetchConcurrencyIsBoundedAtFour(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	monitors := make([]model.Monitor, 10)
	for index := range monitors {
		monitors[index] = model.Monitor{ID: int64(index + 1), PrimaryModel: "gpt"}
	}
	api := &concurrencyTelemetryAPI{monitors: monitors, release: make(chan struct{}), started: make(chan struct{}, len(monitors))}
	manager := NewManager(api, database, 2*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan error, 1)
	go func() { done <- manager.RunOnce(context.Background()) }()
	for range 4 {
		<-api.started
	}
	close(api.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if maximum := api.maximum.Load(); maximum != 4 {
		t.Fatalf("monitor fetch concurrency peak = %d, want 4", maximum)
	}
}

func TestTelemetryCommitStartsTargetedReconcileWithinLatencyTarget(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Now().UTC()
	api := &fakeTelemetryAPI{monitor: model.Monitor{ID: 2, PrimaryModel: "gpt"},
		history: []model.MonitorHistoryRecord{{SourceID: 1, MonitorID: 2, Model: "gpt", Status: model.StatusOperational, CheckedAt: now}}}
	pass := &telemetryPassFake{started: make(chan time.Time, 1)}
	coordinator := reconcile.NewCoordinator(pass, slog.New(slog.NewTextHandler(io.Discard, nil)), reconcile.WithReconcileInterval(time.Hour))
	requester := &timedTelemetryRequester{coordinator: coordinator, requested: make(chan time.Time, 1), source: make(chan string, 1)}
	manager := NewManager(api, database, 2*time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithReconcileRequester(requester), WithMonitorAccountResolver(telemetryResolverFake{accountID: 225}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go coordinator.Run(ctx)
	if err := manager.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	requested := <-requester.requested
	if source := <-requester.source; source != "telemetry" {
		t.Fatalf("telemetry trigger source=%q", source)
	}
	select {
	case started := <-pass.started:
		latency := started.Sub(requested)
		t.Logf("telemetry commit to reconcile start: %v", latency)
		if latency >= time.Second {
			t.Fatalf("telemetry reconcile start exceeded target: %v", latency)
		}
	case <-time.After(time.Second):
		t.Fatal("committed telemetry did not start targeted reconcile within one second")
	}
}
