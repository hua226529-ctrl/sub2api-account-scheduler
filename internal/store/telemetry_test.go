package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestTelemetryMigrationHistoryAndIdempotency(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Date(2026, 7, 14, 6, 0, 0, 0, time.UTC)
	items := []model.MonitorHistoryRecord{
		{SourceID: 1, MonitorID: 8, Model: "gpt-5.5", Status: model.StatusOperational, LatencyMS: 1000, CheckedAt: now},
		{SourceID: 2, MonitorID: 8, Model: "gpt-5.5", Status: model.StatusDegraded, LatencyMS: 8000, ReasonCode: "performance_degraded", CheckedAt: now.Add(-time.Minute)},
		{SourceID: 3, MonitorID: 8, Model: "gpt-5.4", Status: model.StatusOperational, LatencyMS: 900, CheckedAt: now},
	}
	inserted, err := database.InsertMonitorHistoryBatch(ctx, items)
	if err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	inserted, err = database.InsertMonitorHistoryBatch(ctx, items)
	if err != nil || inserted {
		t.Fatalf("duplicate inserted=%v err=%v", inserted, err)
	}
	latest, err := database.ListMonitorHistory(ctx, 8, "gpt-5.5", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 2 || latest[0].SourceID != 1 || latest[1].ReasonCode != "performance_degraded" {
		t.Fatalf("unexpected history: %+v", latest)
	}

	for _, forbidden := range []string{"message", "request_id", "request_body"} {
		var count int
		if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('traffic_events') WHERE name=?`, forbidden).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("sensitive column %q exists", forbidden)
		}
	}
}

func TestTrafficIdempotencyAggregationAndRetention(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Date(2026, 7, 14, 6, 0, 0, 0, time.UTC)
	successes := []model.TrafficSuccess{
		{EventKey: "s1", AccountID: 225, Model: "gpt-5.5", DurationMS: 100, CreatedAt: now.Add(-9 * time.Minute)},
		{EventKey: "s2", AccountID: 225, Model: "gpt-5.5", DurationMS: 200, CreatedAt: now.Add(-8 * time.Minute)},
		{EventKey: "s3", AccountID: 225, Model: "gpt-5.4", DurationMS: 1000, CreatedAt: now.Add(-7 * time.Minute)},
	}
	failures := []model.TrafficError{
		{EventKey: "e1", AccountID: 225, Model: "gpt-5.5", ErrorClass: model.ErrorClassInfrastructure, CreatedAt: now.Add(-6 * time.Minute)},
		{EventKey: "e2", AccountID: 225, Model: "gpt-5.5", ErrorClass: model.ErrorClassCapacity, CreatedAt: now.Add(-5 * time.Minute)},
		{EventKey: "e3", AccountID: 225, Model: "gpt-5.5", ErrorClass: model.ErrorClassSemantic, CreatedAt: now.Add(-4 * time.Minute)},
		{EventKey: "e4", AccountID: 225, Model: "gpt-5.5", ErrorClass: model.ErrorClassClient, CreatedAt: now.Add(-3 * time.Minute)},
		{EventKey: "e5", AccountID: 225, Model: "gpt-5.4", ErrorClass: model.ErrorClassModelCapability, CreatedAt: now.Add(-2 * time.Minute)},
		{EventKey: "e6", AccountID: 225, Model: "gpt-5.5", ErrorClass: model.ErrorClassUnknown, CreatedAt: now.Add(-time.Minute)},
	}
	inserted, err := database.InsertTrafficBatch(ctx, successes, failures)
	if err != nil || inserted != 9 {
		t.Fatalf("inserted=%d err=%v", inserted, err)
	}
	inserted, err = database.InsertTrafficBatch(ctx, successes, failures)
	if err != nil || inserted != 0 {
		t.Fatalf("duplicate inserted=%d err=%v", inserted, err)
	}
	window, err := database.GetTrafficWindow(ctx, 225, now.Add(-10*time.Minute), now)
	if err != nil {
		t.Fatal(err)
	}
	if window.SuccessCount != 3 || window.ErrorCount != 6 || window.EligibleCount != 6 || window.SuccessRate != 50 ||
		window.P90DurationMS != 1000 || window.SemanticErrors != 1 || window.CapabilityErrors != 1 {
		t.Fatalf("unexpected traffic window: %+v", window)
	}
	modelWindow, err := database.GetModelTrafficWindow(ctx, 225, "gpt-5.4", now.Add(-10*time.Minute), now)
	if err != nil {
		t.Fatal(err)
	}
	if modelWindow.SuccessCount != 1 || modelWindow.CapabilityErrors != 1 || modelWindow.EligibleCount != 1 || modelWindow.SuccessRate != 100 {
		t.Fatalf("unexpected model traffic window: %+v", modelWindow)
	}
	if err := database.DeleteTelemetryBefore(ctx, time.Time{}, now.Add(-4*time.Minute), time.Time{}); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM traffic_events`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 4 {
		t.Fatalf("remaining traffic events=%d, want 4", remaining)
	}
}

func TestTrafficBatchSkipsInvalidRowsWithoutRollingBackValidEvidence(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Date(2026, 7, 14, 6, 0, 0, 0, time.UTC)
	successes := []model.TrafficSuccess{
		{EventKey: "valid-success", AccountID: 225, Model: "gpt-5.5", CreatedAt: now},
		{EventKey: "missing-account", Model: "gpt-5.5", CreatedAt: now},
		{EventKey: "missing-time", AccountID: 225, Model: "gpt-5.5"},
		{AccountID: 225, Model: "gpt-5.5", CreatedAt: now},
	}
	failures := []model.TrafficError{
		{EventKey: "valid-error", AccountID: 225, Model: "gpt-5.5", ErrorClass: model.ErrorClassInfrastructure, CreatedAt: now.Add(time.Second)},
		{EventKey: "missing-account-error", Model: "gpt-5.5", CreatedAt: now},
	}
	inserted, err := database.InsertTrafficBatch(ctx, successes, failures)
	if err != nil || inserted != 2 {
		t.Fatalf("inserted=%d err=%v, want two valid rows", inserted, err)
	}

	var count int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM traffic_events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("traffic row count=%d, want 2", count)
	}
}

func TestCapabilityAndDecisionSnapshotAuditTransaction(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Date(2026, 7, 14, 6, 0, 0, 0, time.UTC)
	capability := model.AccountModelCapability{AccountID: 225, Model: "gpt-5.4-mini", Supported: false, FailureCount: 12,
		LastErrorClass: model.ErrorClassModelCapability, LastReasonCode: "model_unsupported", LastObservedAt: now}
	if err := database.UpsertAccountModelCapability(ctx, capability); err != nil {
		t.Fatal(err)
	}
	capability.Supported, capability.SuccessCount, capability.FailureCount = true, 3, 0
	capability.LastObservedAt = now.Add(time.Minute)
	if err := database.UpsertAccountModelCapability(ctx, capability); err != nil {
		t.Fatal(err)
	}
	capabilities, err := database.ListAccountModelCapabilities(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if len(capabilities) != 1 || !capabilities[0].Supported || capabilities[0].SuccessCount != 3 {
		t.Fatalf("unexpected capability: %+v", capabilities)
	}

	monitorID, accountID := int64(8), int64(225)
	snapshot := model.DecisionSnapshot{DecisionID: "decision-8-225-1", MonitorID: &monitorID, AccountID: &accountID, CheckedAt: now,
		AvailabilityState: "healthy", LoadStage: "watch", TargetLoadPercent: 80, Action: "reduce_load", ActionResult: "confirmed",
		ReasonCode: "persistent_latency", Evidence: model.DecisionEvidence{HardSuccessRate10: 100, DegradedCount10: 4, QualityScore: 76}}
	event := &model.Event{Type: "load_adjusted", Severity: "info", MonitorID: &monitorID, AccountID: &accountID, Message: "负载调整", Actor: "system", CreatedAt: now}
	inserted, err := database.CommitDecisionSnapshot(ctx, snapshot, event)
	if err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	inserted, err = database.CommitDecisionSnapshot(ctx, snapshot, event)
	if err != nil || inserted {
		t.Fatalf("duplicate inserted=%v err=%v", inserted, err)
	}
	items, err := database.ListDecisionSnapshots(ctx, 225, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Evidence.QualityScore != 76 || items[0].TargetLoadPercent != 80 {
		t.Fatalf("unexpected snapshot: %+v", items)
	}
	var eventCount int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='load_adjusted'`).Scan(&eventCount); err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Fatalf("event count=%d, want 1", eventCount)
	}
}
