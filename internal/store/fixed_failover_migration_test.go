package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestFixedFailoverPolicyAndValidationContextSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixed-failover.db")
	database, err := Open(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	source, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{
		Name: "test", Provider: "newapi", BaseURL: "https://upstream.example", NormalizedURL: "https://upstream.example",
		CredentialNonce: []byte{1}, CredentialCiphertext: []byte{2}, CredentialMode: "password", PauseBelow: 1, ResumeAt: 2, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := database.SaveGroupFailoverPolicy(ctx, model.GroupFailoverPolicy{
		SourceID: source.ID, KeyID: "key-1", Enabled: true, MainEnabled: true, BackupEnabled: false, EmergencyEnabled: true,
		MainGroupID: "main", BackupGroupID: "backup", EmergencyGroupID: "emergency", AccountIDs: []int64{101},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 18, 6, 0, 0, 0, time.UTC)
	notBefore, deadline := now.Add(5*time.Second), now.Add(10*time.Minute)
	state := policy.State
	state.CurrentTier, state.ObservedGroupID = model.GroupTierBackup, "backup"
	state.ValidationStatus, state.ValidationMode = model.GroupValidationAwaitingEvidence, model.GroupValidationModePassive
	state.ValidationTransitionID = 17
	state.ValidationFromTier, state.ValidationTargetTier = model.GroupTierMain, model.GroupTierBackup
	state.ValidationFromGroupID, state.ValidationTargetGroupID = "main", "backup"
	state.SwitchRequestedAt, state.SwitchVerifiedAt = &now, &now
	state.ValidationNotBefore, state.EvidenceDeadline = &notBefore, &deadline
	state.MonitorWatermark, state.TrafficWatermark = 31, 41
	state.MonitorEvidenceCursor, state.TrafficEvidenceCursor = 32, 42
	state.ActiveProbeAttempts, state.SuccessfulEvidenceCount, state.FailedEvidenceCount = 1, 2, 3
	state.LastEvidenceID, state.LastEvidenceSource, state.LastEvidenceReason, state.LastEvidenceAt = "monitor:32", "monitor", "upstream_failed", &now
	transition, replay, err := database.BeginGroupTierTransition(ctx, model.GroupTierTransition{
		IdempotencyKey: "applied-before-restart", SourceID: source.ID, KeyID: policy.KeyID,
		FromTier: model.GroupTierMain, ToTier: model.GroupTierBackup,
		FromGroupID: "main", ToGroupID: "backup", Actor: "test", CreatedAt: now,
	})
	if err != nil || replay {
		t.Fatalf("begin applied transition: replay=%v err=%v", replay, err)
	}
	state.ValidationTransitionID = transition.ID
	if err := database.CompleteGroupTierTransition(ctx, transition.ID, state, now); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		key    string
		status string
		tier   string
	}{
		{key: "uncertain", status: model.GroupValidationUncertain, tier: model.GroupTierBackup},
		{key: "exhausted", status: model.GroupValidationExhausted, tier: model.GroupTierEmergency},
	} {
		other, err := database.SaveGroupFailoverPolicy(ctx, model.GroupFailoverPolicy{
			SourceID: source.ID, KeyID: item.key, Enabled: true,
			MainGroupID: "main", BackupGroupID: "backup", EmergencyGroupID: "emergency",
		})
		if err != nil {
			t.Fatal(err)
		}
		other.State.CurrentTier = item.tier
		other.State.ObservedGroupID = item.tier
		other.State.ValidationStatus = item.status
		if err := database.SaveGroupFailoverState(ctx, other.State); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = Open(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	loaded, err := database.GetGroupFailoverPolicy(ctx, source.ID, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.MainEnabled || loaded.BackupEnabled || !loaded.EmergencyEnabled {
		t.Fatalf("per-level enablement was not preserved: %+v", loaded)
	}
	got := loaded.State
	if got.ValidationStatus != model.GroupValidationAwaitingEvidence || got.ValidationTransitionID != transition.ID || got.MonitorWatermark != 31 || got.TrafficWatermark != 41 || got.MonitorEvidenceCursor != 32 || got.TrafficEvidenceCursor != 42 || got.LastEvidenceID != "monitor:32" || got.FailedEvidenceCount != 3 {
		t.Fatalf("validation context was not preserved: %+v", got)
	}
	storedTransition, err := database.GetGroupTierTransitionByKey(ctx, "applied-before-restart")
	if err != nil || storedTransition.Status != model.GroupTransitionApplied {
		t.Fatalf("applied transition was not preserved: %+v err=%v", storedTransition, err)
	}
	for _, item := range []struct {
		key    string
		status string
	}{
		{key: "uncertain", status: model.GroupValidationUncertain},
		{key: "exhausted", status: model.GroupValidationExhausted},
	} {
		persisted, err := database.GetGroupFailoverPolicy(ctx, source.ID, item.key)
		if err != nil || persisted.State.ValidationStatus != item.status {
			t.Fatalf("%s state was not preserved: %+v err=%v", item.key, persisted.State, err)
		}
	}
}

func TestGroupValidationEvidenceUsesCommittedRowIDWatermarks(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "evidence.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 7, 0, 0, 0, time.UTC)
	if _, err := database.InsertMonitorHistoryBatch(ctx, []model.MonitorHistoryRecord{{MonitorID: 7, Model: "m", Status: model.StatusOperational, CheckedAt: now}}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.InsertTrafficSuccesses(ctx, []model.TrafficSuccess{{EventKey: "before", AccountID: 101, Model: "m", CreatedAt: now}}); err != nil {
		t.Fatal(err)
	}
	watermarks, err := database.GetFailoverEvidenceWatermarks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.InsertMonitorHistoryBatch(ctx, []model.MonitorHistoryRecord{{MonitorID: 7, Model: "m", Status: model.StatusFailed, CheckedAt: now.Add(time.Second)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.InsertTrafficSuccesses(ctx, []model.TrafficSuccess{{EventKey: "after", AccountID: 101, Model: "m", CreatedAt: now.Add(time.Second)}}); err != nil {
		t.Fatal(err)
	}
	items, err := database.ListGroupValidationEvidence(ctx, []int64{7}, []int64{101}, watermarks.Monitor, watermarks.Traffic)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("watermark query returned pre-switch rows: %+v", items)
	}
	for _, item := range items {
		if item.ID <= watermarks.Monitor && item.Source == "monitor" || item.ID <= watermarks.Traffic && item.Source == "traffic" {
			t.Fatalf("watermark boundary included an old row: %+v", item)
		}
	}
}

func TestCompareAndSaveGroupFailoverStateRejectsSupersededTransition(t *testing.T) {
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	source, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{
		Name: "test", Provider: "newapi", BaseURL: "https://upstream.example", NormalizedURL: "https://upstream.example",
		CredentialNonce: []byte{1}, CredentialCiphertext: []byte{2}, CredentialMode: "password", PauseBelow: 1, ResumeAt: 2, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := database.SaveGroupFailoverPolicy(ctx, model.GroupFailoverPolicy{
		SourceID: source.ID, KeyID: "key", Enabled: true, MainGroupID: "main", BackupGroupID: "backup", EmergencyGroupID: "emergency",
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := model.GroupFailoverState{
		SourceID: policy.SourceID, KeyID: policy.KeyID, ValidationTransitionID: 7,
		ValidationStatus:     model.GroupValidationAwaitingEvidence,
		ValidationTargetTier: model.GroupTierBackup, ValidationTargetGroupID: "backup",
	}
	if err := database.SaveGroupFailoverState(ctx, expected); err != nil {
		t.Fatal(err)
	}
	superseding := expected
	superseding.ValidationTransitionID = 8
	superseding.ValidationTargetTier = model.GroupTierEmergency
	superseding.ValidationTargetGroupID = "emergency"
	if err := database.SaveGroupFailoverState(ctx, superseding); err != nil {
		t.Fatal(err)
	}
	next := expected
	next.ValidationStatus = model.GroupValidationConfirmedHealthy
	saved, err := database.CompareAndSaveGroupFailoverState(ctx, expected, next)
	if err != nil {
		t.Fatal(err)
	}
	if saved {
		t.Fatal("evidence for the old transition overwrote its successor")
	}
	state, err := database.GetGroupFailoverState(ctx, policy.SourceID, policy.KeyID)
	if err != nil {
		t.Fatal(err)
	}
	if state.ValidationTransitionID != 8 || state.ValidationTargetTier != model.GroupTierEmergency || state.ValidationStatus != model.GroupValidationAwaitingEvidence {
		t.Fatalf("superseding transition changed: %+v", state)
	}
}

func TestLegacyFailoverValidationMigratesToUncertainIdempotently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-failover.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	statements := []string{
		`CREATE TABLE upstream_group_failover_policies (source_id INTEGER NOT NULL,key_id TEXT NOT NULL,key_name TEXT NOT NULL DEFAULT '',key_hint TEXT NOT NULL DEFAULT '',enabled INTEGER NOT NULL DEFAULT 1,main_group_id TEXT NOT NULL,backup_group_id TEXT NOT NULL,emergency_group_id TEXT NOT NULL,pool TEXT NOT NULL DEFAULT '',version INTEGER NOT NULL DEFAULT 1,confirmed_version INTEGER NOT NULL DEFAULT 0,confirmed_at TEXT,confirmed_by TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(source_id,key_id))`,
		`INSERT INTO upstream_group_failover_policies(source_id,key_id,main_group_id,backup_group_id,emergency_group_id,created_at,updated_at) VALUES(1,'legacy','main','backup','emergency','2026-07-18T00:00:00Z','2026-07-18T00:00:00Z')`,
		`INSERT INTO upstream_group_failover_policies(source_id,key_id,main_group_id,backup_group_id,emergency_group_id,created_at,updated_at) VALUES(1,'legacy-initial','main','backup','emergency','2026-07-18T00:00:00Z','2026-07-18T00:00:00Z')`,
		`CREATE TABLE upstream_group_failover_states (source_id INTEGER NOT NULL,key_id TEXT NOT NULL,current_tier TEXT NOT NULL DEFAULT '',observed_group_id TEXT NOT NULL DEFAULT '',previous_tier TEXT NOT NULL DEFAULT '',previous_stable_tier TEXT NOT NULL DEFAULT '',previous_group_id TEXT NOT NULL DEFAULT '',frozen INTEGER NOT NULL DEFAULT 0,freeze_reason TEXT NOT NULL DEFAULT '',last_error TEXT NOT NULL DEFAULT '',manual_hold_until TEXT,manual_override_until TEXT,cooldown_until TEXT,return_blocked_until TEXT,recovery_since TEXT,last_switch_at TEXT,last_transition_at TEXT,verification_started_at TEXT,healthy_since TEXT,recovery_healthy_count INTEGER NOT NULL DEFAULT 0,last_confirmed_at TEXT,updated_at TEXT NOT NULL,PRIMARY KEY(source_id,key_id))`,
		`INSERT INTO upstream_group_failover_states(source_id,key_id,current_tier,observed_group_id,last_transition_at,updated_at) VALUES(1,'legacy','backup','backup','2026-07-18T00:01:00Z','2026-07-18T00:01:00Z')`,
		`INSERT INTO upstream_group_failover_states(source_id,key_id,current_tier,observed_group_id,updated_at) VALUES(1,'legacy-initial','main','main','2026-07-18T00:01:00Z')`,
		`CREATE TRIGGER fail_core_c_validation_migration BEFORE UPDATE ON upstream_group_failover_states BEGIN SELECT RAISE(FAIL,'forced core C migration failure'); END`,
	}
	for _, statement := range statements {
		if _, err := raw.Exec(statement); err != nil {
			raw.Close()
			t.Fatal(err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if database, err := Open(path, testSettings()); err == nil {
		database.Close()
		t.Fatal("migration trigger did not interrupt the stage C validation migration")
	}
	raw, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var columns int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('upstream_group_failover_states') WHERE name='validation_status'`).Scan(&columns); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if columns != 1 {
		raw.Close()
		t.Fatalf("partial migration did not leave one retry-safe validation column: %d", columns)
	}
	if _, err := raw.Exec(`DROP TRIGGER fail_core_c_validation_migration`); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		database, err := Open(path, testSettings())
		if err != nil {
			t.Fatalf("migration pass %d: %v", pass+1, err)
		}
		policy, err := database.GetGroupFailoverPolicy(context.Background(), 1, "legacy")
		if err != nil {
			database.Close()
			t.Fatal(err)
		}
		if !policy.MainEnabled || !policy.BackupEnabled || !policy.EmergencyEnabled || policy.State.ValidationStatus != model.GroupValidationUncertain {
			database.Close()
			t.Fatalf("legacy state was silently treated as healthy: %+v", policy)
		}
		initial, err := database.GetGroupFailoverPolicy(context.Background(), 1, "legacy-initial")
		if err != nil {
			database.Close()
			t.Fatal(err)
		}
		if initial.State.ValidationStatus != model.GroupValidationUnknown {
			database.Close()
			t.Fatalf("legacy state without post-switch evidence became healthy: %+v", initial.State)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
