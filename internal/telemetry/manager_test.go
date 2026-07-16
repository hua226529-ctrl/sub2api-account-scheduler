package telemetry

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/sub2api"
)

type fakeTelemetryAPI struct {
	monitor  model.Monitor
	history  []model.MonitorHistoryRecord
	success  []model.TrafficSuccess
	failures []model.TrafficError
	queries  []sub2api.TelemetryQuery
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
