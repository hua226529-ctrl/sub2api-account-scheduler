package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func testSettings() model.Settings {
	return model.Settings{
		DryRun: false, FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	}
}

func TestUpsertPoliciesIsAtomic(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	if _, err := database.db.ExecContext(ctx, `CREATE TRIGGER reject_second_pool_policy
		BEFORE INSERT ON account_policies WHEN NEW.account_id=299
		BEGIN SELECT RAISE(ABORT, 'injected pool projection failure'); END`); err != nil {
		t.Fatal(err)
	}
	policies := []model.Policy{{AccountID: 298, Enabled: true, ScorePolicySource: "pool"},
		{AccountID: 299, Enabled: true, ScorePolicySource: "pool"}}
	if err := database.UpsertPolicies(ctx, policies); err == nil {
		t.Fatal("injected second write did not fail the batch")
	}
	loaded, err := database.ListPolicies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Fatalf("failed pool projection left partial rows: %+v", loaded)
	}

	if _, err := database.db.ExecContext(ctx, `DROP TRIGGER reject_second_pool_policy`); err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertPolicies(ctx, policies); err != nil {
		t.Fatal(err)
	}
	loaded, err = database.ListPolicies(ctx)
	if err != nil || len(loaded) != 2 {
		t.Fatalf("valid pool projection was not committed together: policies=%+v err=%v", loaded, err)
	}
}

func TestRollingAutomaticPauseWindowAndPersistentLatch(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC()
	accountID := int64(225)
	for _, createdAt := range []time.Time{now.Add(-time.Hour - time.Second), now.Add(-time.Hour), now.Add(-30 * time.Minute)} {
		if err := database.AddEvent(ctx, model.Event{Type: "automatic_pause", Severity: "warning", AccountID: &accountID, Message: "test", Actor: "system", CreatedAt: createdAt}); err != nil {
			t.Fatal(err)
		}
	}
	count, err := database.CountAutomaticPauses(ctx, accountID, now.Add(-time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("rolling window boundary count = %d, want 2", count)
	}

	falseValue := false
	control := model.AccountControl{AccountID: accountID, OwnsPause: true, Owner: "automatic", ExpectedSchedulable: &falseValue}
	control, count, activated, err := database.CommitAutomaticPause(ctx, control, model.Event{
		Type: "automatic_pause", Severity: "warning", AccountID: &accountID,
		Message: "third recent pause", Actor: "system", CreatedAt: now,
	}, model.FlapPolicy{Enabled: true, WindowMinutes: 60, PauseThreshold: 3, RecoveryThreshold: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !activated || count != 3 || !control.FlapActive || control.FlapRecoveryRequired != 10 {
		t.Fatalf("third pause did not atomically latch protection: control=%+v count=%d activated=%v", control, count, activated)
	}

	count, err = database.CountAutomaticPauses(ctx, accountID, now.Add(time.Hour), now.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := database.GetControl(ctx, accountID)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || !persisted.FlapActive {
		t.Fatalf("latch should persist after rolling records expire: count=%d control=%+v", count, persisted)
	}
}

func TestOpenMigratesLegacyPolicyAndControlTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := []string{
		`CREATE TABLE account_policies (account_id INTEGER PRIMARY KEY, monitor_id INTEGER, excluded INTEGER NOT NULL DEFAULT 0, enabled INTEGER NOT NULL DEFAULT 1, failure_threshold INTEGER, recovery_threshold INTEGER, updated_at TEXT NOT NULL)`,
		`CREATE TABLE account_controls (account_id INTEGER PRIMARY KEY, monitor_id INTEGER, owns_pause INTEGER NOT NULL DEFAULT 0, owner TEXT NOT NULL DEFAULT '', expected_schedulable INTEGER, manual_override_until TEXT, last_observed_schedulable INTEGER, last_decision TEXT NOT NULL DEFAULT '', last_action_at TEXT, updated_at TEXT NOT NULL)`,
	}
	for _, statement := range legacy {
		if _, err := raw.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	database, err := Open(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	enabled := true
	window, pauses, recovery := 45, 4, 12
	policy := model.Policy{AccountID: 225, Enabled: true, FlapEnabled: &enabled, FlapWindowMinutes: &window, FlapPauseThreshold: &pauses, FlapRecoveryThreshold: &recovery}
	if err := database.UpsertPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	policies, err := database.ListPolicies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	loaded := policies[225]
	if loaded.FlapEnabled == nil || !*loaded.FlapEnabled || loaded.FlapWindowMinutes == nil || *loaded.FlapWindowMinutes != 45 {
		t.Fatalf("legacy policy migration failed: %+v", loaded)
	}
	now := time.Now().UTC()
	originalLoad, expectedLoad := 20, 5
	loadOverrideUntil := now.Add(30 * time.Minute)
	loadPinUntil := now.Add(2 * time.Hour)
	recoveryStartedAt := now.Add(-time.Minute)
	control := model.AccountControl{
		AccountID: 225, FlapActive: true, FlapTriggeredAt: &now, FlapRecoveryRequired: 12,
		OwnsLoadFactor: true, OriginalLoadFactor: &originalLoad, ExpectedLoadFactor: &expectedLoad,
		LoadStage: model.HealthStageRecovering25, LoadOverrideUntil: &loadOverrideUntil,
		LoadPinValue: &expectedLoad, LoadPinUntil: &loadPinUntil, LoadPinOwner: "web", LoadPinReason: "夜间固定",
		RecoveryStep: 1, RecoveryStartedAt: &recoveryStartedAt,
	}
	if err := database.UpsertControl(ctx, control); err != nil {
		t.Fatal(err)
	}
	loadedControl, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if !loadedControl.FlapActive || loadedControl.FlapRecoveryRequired != 12 || loadedControl.FlapTriggeredAt == nil {
		t.Fatalf("legacy control migration failed: %+v", loadedControl)
	}
	if !loadedControl.OwnsLoadFactor || loadedControl.OriginalLoadFactor == nil || *loadedControl.OriginalLoadFactor != 20 ||
		loadedControl.ExpectedLoadFactor == nil || *loadedControl.ExpectedLoadFactor != 5 || loadedControl.RecoveryStep != 1 ||
		loadedControl.LoadOverrideUntil == nil || loadedControl.RecoveryStartedAt == nil || loadedControl.LoadPinValue == nil ||
		*loadedControl.LoadPinValue != 5 || loadedControl.LoadPinUntil == nil || loadedControl.LoadPinOwner != "web" ||
		loadedControl.LoadPinReason != "夜间固定" {
		t.Fatalf("legacy load control migration failed: %+v", loadedControl)
	}
	inserted, err := database.InsertMonitorObservation(ctx, model.MonitorObservation{MonitorID: 2, CheckedAt: now, Status: model.StatusOperational})
	if err != nil || !inserted {
		t.Fatalf("legacy database did not receive observation schema: inserted=%v err=%v", inserted, err)
	}
	if err := database.UpsertMonitorHealthState(ctx, model.MonitorHealthState{MonitorID: 2, Stage: model.HealthStageHealthy}); err != nil {
		t.Fatalf("legacy database did not receive health state schema: %v", err)
	}
}

func TestFreezeStatePersists(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	initial, err := database.GetAgentFreezeState(ctx, "global", "")
	if err != nil {
		t.Fatal(err)
	}
	if initial.Mode != model.AgentFreezeModeActive {
		t.Fatalf("new database should start unfrozen: %+v", initial)
	}
	state := model.AgentFreezeState{ScopeType: "global", Mode: model.AgentFreezeModeWritesFrozen, Reason: "incident", Actor: "web"}
	if err := database.SetAgentFreezeState(ctx, &state); err != nil {
		t.Fatal(err)
	}
	loaded, err := database.GetAgentFreezeState(ctx, "global", "")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Mode != model.AgentFreezeModeWritesFrozen || loaded.Reason != "incident" || loaded.Actor != "web" || loaded.UpdatedAt.IsZero() {
		t.Fatalf("freeze state did not persist: %+v", loaded)
	}
}

func TestMonitorObservationDeduplicationWindowAndCleanup(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	observations := []model.MonitorObservation{
		{MonitorID: 2, CheckedAt: now.Add(-15 * 24 * time.Hour), Status: model.StatusOperational, LatencyMS: 900, Score: 95, Confidence: 1, CreatedAt: now},
		{MonitorID: 2, CheckedAt: now.Add(-45 * time.Minute), Status: model.StatusDegraded, LatencyMS: 2000, Availability7D: 88.5, ExtraOK: 2, ExtraDegraded: 1, Score: 64, Confidence: 0.8, ReasonJSON: `{"reason":"slow"}`, CreatedAt: now},
		{MonitorID: 2, CheckedAt: now.Add(-5 * time.Minute), Status: model.StatusFailed, LatencyMS: 15000, ExtraFailed: 2, DecryptFailed: true, Score: 10, Confidence: 1, CreatedAt: now},
		{MonitorID: 9, CheckedAt: now.Add(-time.Minute), Status: model.StatusOperational, Score: 100, CreatedAt: now},
	}
	for _, observation := range observations {
		inserted, err := database.InsertMonitorObservation(ctx, observation)
		if err != nil || !inserted {
			t.Fatalf("insert observation: inserted=%v err=%v", inserted, err)
		}
	}
	inserted, err := database.InsertMonitorObservation(ctx, observations[2])
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("duplicate checked_at should not be inserted")
	}

	items, err := database.ListMonitorObservations(ctx, 2, now.Add(-time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Status != model.StatusFailed || items[1].Status != model.StatusDegraded {
		t.Fatalf("unexpected observation window: %+v", items)
	}
	if !items[0].DecryptFailed || items[1].ExtraDegraded != 1 || items[1].ReasonJSON == "" {
		t.Fatalf("observation fields not preserved: %+v", items)
	}

	if err := database.DeleteMonitorObservationsBefore(ctx, now.Add(-14*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM monitor_observations`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 3 {
		t.Fatalf("remaining observations = %d, want 3", remaining)
	}
}

func TestOpenMigratesPreHealthControlTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-health.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`CREATE TABLE account_controls (
		account_id INTEGER PRIMARY KEY, monitor_id INTEGER, owns_pause INTEGER NOT NULL DEFAULT 0,
		owner TEXT NOT NULL DEFAULT '', expected_schedulable INTEGER, manual_override_until TEXT,
		last_observed_schedulable INTEGER, last_decision TEXT NOT NULL DEFAULT '', last_action_at TEXT,
		flap_active INTEGER NOT NULL DEFAULT 0, flap_triggered_at TEXT,
		flap_recovery_required INTEGER NOT NULL DEFAULT 0, health_locked INTEGER NOT NULL DEFAULT 0,
		manual_locked INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	database, err := Open(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	original, expected := 12, 6
	control := model.AccountControl{AccountID: 298, OwnsLoadFactor: true, OriginalLoadFactor: &original, ExpectedLoadFactor: &expected, LoadStage: model.HealthStageDegraded}
	if err := database.UpsertControl(context.Background(), control); err != nil {
		t.Fatal(err)
	}
	loaded, err := database.GetControl(context.Background(), 298)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.OwnsLoadFactor || loaded.ExpectedLoadFactor == nil || *loaded.ExpectedLoadFactor != 6 || loaded.LoadStage != model.HealthStageDegraded {
		t.Fatalf("pre-health control migration failed: %+v", loaded)
	}
}

func TestMonitorHealthStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC().Truncate(time.Millisecond)
	holdUntil := now.Add(5 * time.Minute)
	state := model.MonitorHealthState{
		MonitorID: 2, Stage: model.HealthStageQuarantined, Score: 24.5, Confidence: 0.9,
		CurrentLatencyMS: 16000, BaselineLatencyMS: 1250.5, Availability15M: 65,
		Availability1H: 82.5, Availability24H: 96.2, SampleCount: 24, RecoveryHealthyCount: 7,
		LastTwoHealthy: true, RecoveryEligible: false, NextRecoveryCondition: "还需一次正常检测",
		HoldUntil: &holdUntil, LastTransitionAt: now, ReasonJSON: `{"reasons":["延迟严重"]}`, UpdatedAt: now,
	}
	if err := database.UpsertMonitorHealthState(ctx, state); err != nil {
		t.Fatal(err)
	}
	loaded, err := database.GetMonitorHealthState(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Stage != state.Stage || loaded.BaselineLatencyMS != state.BaselineLatencyMS || loaded.SampleCount != 24 ||
		!loaded.LastTwoHealthy || loaded.RecoveryEligible || loaded.HoldUntil == nil || loaded.NextRecoveryCondition == "" {
		t.Fatalf("health state not preserved: %+v", loaded)
	}

	missing, err := database.GetMonitorHealthState(ctx, 999)
	if err != nil {
		t.Fatal(err)
	}
	if missing.Stage != model.HealthStageFrozen {
		t.Fatalf("missing health state = %+v", missing)
	}
}

func TestHealthSettingsDefaultsAndRoundTrip(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	settings, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if settings.HealthMode != "observe" || settings.HealthHealthyScore != 80 || settings.HealthLatencyCriticalMS != 15000 || settings.HealthTrialPercent != 25 || settings.HealthMidPercent != 50 {
		t.Fatalf("unexpected health defaults: %+v", settings)
	}
	if settings.FailoverAccountFreshMinutes != 3 || settings.FailoverTrafficMinSamples != 10 ||
		settings.FailoverSwitchCooldownMinutes != 15 || settings.FailoverRecoverySuccessAt != 98 {
		t.Fatalf("unexpected failover defaults: %+v", settings)
	}
	settings.HealthMode = "adaptive"
	settings.HealthMinSamples = 12
	settings.HealthMidPercent = 65
	settings.HealthLoadOverrideMinutes = 45
	settings.FailoverTrafficMinSamples = 14
	settings.FailoverSwitchCooldownMinutes = 22
	settings.FailoverRecoverySuccessAt = 99
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	loaded, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.HealthMode != "adaptive" || loaded.HealthMinSamples != 12 || loaded.HealthMidPercent != 65 || loaded.HealthLoadOverrideMinutes != 45 ||
		loaded.FailoverTrafficMinSamples != 14 || loaded.FailoverSwitchCooldownMinutes != 22 || loaded.FailoverRecoverySuccessAt != 99 {
		t.Fatalf("health settings not preserved: %+v", loaded)
	}
	loaded.FailoverLongLimitWindowMinutes = loaded.FailoverShortLimitWindowMinutes
	if err := database.UpdateSettings(ctx, loaded); err == nil {
		t.Fatal("invalid failover window ordering was accepted")
	}
}
