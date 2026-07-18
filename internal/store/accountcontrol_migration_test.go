package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
)

func TestAccountControlTablesInitializeAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "account-control.db")
	database, err := Open(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	assertTableExists(t, database.db, "account_overrides")
	assertTableExists(t, database.db, "account_mutations")
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	assertTableExists(t, database.db, "account_overrides")
	assertTableExists(t, database.db, "account_mutations")
}

func TestAccountControlMigrationBackfillsManualAndDisablesLegacyAgent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-account-control.db")
	raw := createLegacyAccountControlDatabase(t, path, false)
	now := time.Now().UTC().Truncate(time.Second)
	manualUntil := now.Add(45 * time.Minute).Format(time.RFC3339Nano)
	if _, err := raw.Exec(`INSERT INTO account_controls(account_id,owns_pause,owner,manual_override_until,manual_locked,
		expected_schedulable,last_observed_schedulable,last_decision,updated_at) VALUES
		(101,1,'operator',NULL,0,0,0,'legacy manual pause',?),
		(102,1,'agent',NULL,0,0,0,'legacy agent pause',?),
		(103,0,'',?,0,1,1,'legacy resume protection',?)`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), manualUntil, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO account_controls(account_id,owns_pause,owner,manual_locked,owns_load_factor,
		expected_load_factor,load_pin_value,load_pin_owner,last_decision,updated_at)
		VALUES(104,0,'',0,1,35,35,'agent:9','agent_load_adjusted',?)`, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO events(type,severity,message,actor,created_at) VALUES('legacy_event','info','preserve me','test',?)`, now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	database, err := Open(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var eventProvenanceColumns int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('events') WHERE name IN ('goal_id','step_id')`).Scan(&eventProvenanceColumns); err != nil || eventProvenanceColumns != 2 {
		t.Fatalf("event provenance migration missing: count=%d err=%v", eventProvenanceColumns, err)
	}
	manual, err := database.FindActiveAccountOverride(ctx, 101, controlplane.OperationSetAccountSchedulable,
		controlplane.AuthorityManualHold, now)
	if err != nil || manual == nil || manual.Schedulable == nil || *manual.Schedulable || manual.ExpiresAt != nil {
		t.Fatalf("manual hold was not preserved: override=%+v err=%v", manual, err)
	}
	resume, err := database.FindActiveAccountOverride(ctx, 103, controlplane.OperationSetAccountSchedulable,
		controlplane.AuthorityAdministratorCommand, now)
	if err != nil || resume == nil || resume.Schedulable == nil || !*resume.Schedulable || resume.ExpiresAt == nil {
		t.Fatalf("legacy temporary resume was not preserved: override=%+v err=%v", resume, err)
	}
	var legacyAgentStatus string
	var legacyAgentExpires sql.NullString
	if err := database.db.QueryRow(`SELECT status,expires_at FROM account_overrides WHERE id='legacy-agent-disabled:102'`).Scan(&legacyAgentStatus, &legacyAgentExpires); err != nil {
		t.Fatal(err)
	}
	if legacyAgentStatus != string(accountcontrol.OverrideLegacyDisabled) || legacyAgentExpires.Valid {
		t.Fatalf("legacy Agent control became active or temporary: status=%s expires=%+v", legacyAgentStatus, legacyAgentExpires)
	}
	var activeAgent int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM account_overrides WHERE account_id=102 AND authority=? AND status=?`,
		controlplane.AuthorityAutonomousAgent.String(), accountcontrol.OverrideActive).Scan(&activeAgent); err != nil {
		t.Fatal(err)
	}
	if activeAgent != 0 {
		t.Fatalf("legacy Agent migration created %d active autonomous overrides", activeAgent)
	}
	var activeAgentLoad int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM account_overrides WHERE account_id=104 AND operation=? AND status=?`,
		controlplane.OperationSetAccountLoadFactor.String(), accountcontrol.OverrideActive).Scan(&activeAgentLoad); err != nil {
		t.Fatal(err)
	}
	if activeAgentLoad != 0 {
		t.Fatalf("legacy Agent load migration created %d active overrides", activeAgentLoad)
	}
	var legacyAgentLoadStatus string
	if err := database.db.QueryRow(`SELECT status FROM account_overrides WHERE id='legacy-agent-load-disabled:104'`).Scan(&legacyAgentLoadStatus); err != nil {
		t.Fatal(err)
	}
	if legacyAgentLoadStatus != string(accountcontrol.OverrideLegacyDisabled) {
		t.Fatalf("legacy Agent load control was not disabled: %s", legacyAgentLoadStatus)
	}
	control102, err := database.GetControl(ctx, 102)
	if err != nil {
		t.Fatal(err)
	}
	control104, err := database.GetControl(ctx, 104)
	if err != nil {
		t.Fatal(err)
	}
	if control102.OwnsPause || control102.Owner != "" || control104.OwnsLoadFactor || control104.LoadPinValue != nil || control104.LoadPinOwner != "" {
		t.Fatalf("legacy Agent ownership projection remained active: pause=%+v load=%+v", control102, control104)
	}
	var auditCount, preservedCount int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM events WHERE type='legacy_agent_control_disabled' AND account_id=102`).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM events WHERE type='legacy_event' AND message='preserve me'`).Scan(&preservedCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 || preservedCount != 1 {
		t.Fatalf("migration audit or legacy event mismatch: audit=%d preserved=%d", auditCount, preservedCount)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = Open(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM events WHERE type='legacy_agent_control_disabled' AND account_id=102`).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	var overrideCount int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM account_overrides`).Scan(&overrideCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 || overrideCount != 4 {
		t.Fatalf("repeated migration was not idempotent: audits=%d overrides=%d", auditCount, overrideCount)
	}
}

func TestAccountControlMigrationRetriesAfterTransactionalFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retry-account-control.db")
	raw := createLegacyAccountControlDatabase(t, path, true)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := raw.Exec(`INSERT INTO account_controls(account_id,owns_pause,owner,expected_schedulable,
		last_observed_schedulable,last_decision,updated_at) VALUES(201,1,'agent',0,0,'legacy agent pause',?)`, now); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if database, err := Open(path, testSettings()); err == nil {
		database.Close()
		t.Fatal("migration trigger did not interrupt the transaction")
	}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var partial int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM account_overrides`).Scan(&partial); err != nil {
		t.Fatal(err)
	}
	if partial != 0 {
		t.Fatalf("failed migration left %d partial overrides", partial)
	}
	if _, err := raw.Exec(`DROP TRIGGER fail_account_control_marker`); err != nil {
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
	var status string
	if err := database.db.QueryRow(`SELECT status FROM account_overrides WHERE id='legacy-agent-disabled:201'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(accountcontrol.OverrideLegacyDisabled) {
		t.Fatalf("retry did not complete disabled legacy conversion: %s", status)
	}
}

func TestPrepareAccountMutationAtomicallyPersistsOverrideAndJournal(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "prepare-atomic.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	created := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	intent, err := controlplane.NewAccountSchedulableIntent(controlplane.IntentMetadata{
		ID: "intent-prepare-atomic", IdempotencyKey: "idem-prepare-atomic", Producer: controlplane.ProducerAdminUI,
		Authority: controlplane.AuthorityManualHold, Actor: "test", Reason: "atomic prepare", CreatedAt: created,
	}, 501, false)
	if err != nil {
		t.Fatal(err)
	}
	override, err := accountcontrol.OverrideFromIntent("override-prepare-atomic", "command-prepare-atomic", intent)
	if err != nil {
		t.Fatal(err)
	}
	override.MutationID = "mutation-prepare-atomic"
	signature, err := controlplane.SemanticSignature(intent)
	if err != nil {
		t.Fatal(err)
	}
	desired := false
	mutation := accountcontrol.Mutation{
		ID: "mutation-prepare-atomic", CommandID: "command-prepare-atomic", IntentID: intent.ID,
		IdempotencyKey: intent.IdempotencyKey, SemanticSignature: signature, AccountID: 501,
		Operation: intent.Operation, RequestedSchedulable: &desired, WinningIntentID: intent.ID,
		WinningIdempotencyKey: intent.IdempotencyKey, WinningProducer: intent.Producer, WinningAuthority: intent.Authority,
		WinningActor: intent.Actor, WinningReason: intent.Reason, WinningCreatedAt: created, WinningSchedulable: &desired,
		Producer: intent.Producer, Authority: intent.Authority, Actor: intent.Actor, Reason: intent.Reason,
		Status: accountcontrol.StatusPrepared, OverrideID: override.ID, TelemetryFresh: true, CreatedAt: created, UpdatedAt: created,
	}
	if _, err := database.db.Exec(`CREATE TRIGGER fail_prepared_journal BEFORE INSERT ON account_mutations
		BEGIN SELECT RAISE(ABORT,'forced journal insert failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.PrepareAccountMutation(ctx, mutation, &override); err == nil {
		t.Fatal("forced journal failure did not fail prepare")
	}
	var overrides, mutations int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM account_overrides WHERE id=?`, override.ID).Scan(&overrides); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM account_mutations WHERE id=?`, mutation.ID).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if overrides != 0 || mutations != 0 {
		t.Fatalf("failed prepare left partial state: overrides=%d mutations=%d", overrides, mutations)
	}
	if _, err := database.db.Exec(`DROP TRIGGER fail_prepared_journal`); err != nil {
		t.Fatal(err)
	}
	if _, replay, err := database.PrepareAccountMutation(ctx, mutation, &override); err != nil || replay {
		t.Fatalf("prepare after rollback: replay=%v err=%v", replay, err)
	}
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM account_overrides WHERE id=?`, override.ID).Scan(&overrides); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM account_mutations WHERE id=?`, mutation.ID).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if overrides != 1 || mutations != 1 {
		t.Fatalf("successful prepare was not atomic: overrides=%d mutations=%d", overrides, mutations)
	}
}

func createLegacyAccountControlDatabase(t *testing.T, path string, failMarker bool) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	statements := []string{
		`CREATE TABLE settings (key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)`,
		`CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT,type TEXT NOT NULL,severity TEXT NOT NULL,
			monitor_id INTEGER,account_id INTEGER,message TEXT NOT NULL,before_state TEXT NOT NULL DEFAULT '',
			after_state TEXT NOT NULL DEFAULT '',details TEXT NOT NULL DEFAULT '',actor TEXT NOT NULL DEFAULT 'system',created_at TEXT NOT NULL)`,
		`CREATE TABLE account_policies (account_id INTEGER PRIMARY KEY,monitor_id INTEGER,excluded INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,failure_threshold INTEGER,recovery_threshold INTEGER,updated_at TEXT NOT NULL)`,
		`CREATE TABLE account_controls (account_id INTEGER PRIMARY KEY,monitor_id INTEGER,owns_pause INTEGER NOT NULL DEFAULT 0,
			owner TEXT NOT NULL DEFAULT '',expected_schedulable INTEGER,manual_override_until TEXT,last_observed_schedulable INTEGER,
			last_decision TEXT NOT NULL DEFAULT '',last_action_at TEXT,manual_locked INTEGER NOT NULL DEFAULT 0,
			owns_load_factor INTEGER NOT NULL DEFAULT 0,expected_load_factor INTEGER,load_override_until TEXT,
			load_pin_value INTEGER,load_pin_until TEXT,load_pin_owner TEXT NOT NULL DEFAULT '',load_pin_reason TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL)`,
	}
	for _, statement := range statements {
		if _, err := raw.Exec(statement); err != nil {
			raw.Close()
			t.Fatal(err)
		}
	}
	if failMarker {
		if _, err := raw.Exec(`CREATE TRIGGER fail_account_control_marker BEFORE INSERT ON settings
			WHEN NEW.key='account_control_core_a_migrated' BEGIN SELECT RAISE(FAIL,'forced migration failure'); END`); err != nil {
			raw.Close()
			t.Fatal(err)
		}
	}
	return raw
}

func assertTableExists(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("table %s does not exist", name)
	}
}
