package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestCoreCMigrationPreservesStageBControlPlaneData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core-b.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	statements := []string{
		`CREATE TABLE agent_settings (id INTEGER PRIMARY KEY CHECK(id=1), enabled INTEGER NOT NULL DEFAULT 0, mode TEXT NOT NULL DEFAULT 'observe', analysis_interval_minutes INTEGER NOT NULL DEFAULT 30, emergency_cooldown_minutes INTEGER NOT NULL DEFAULT 5, context_token_budget INTEGER NOT NULL DEFAULT 16000, max_anomalies INTEGER NOT NULL DEFAULT 20, max_drilldowns INTEGER NOT NULL DEFAULT 8, retention_days INTEGER NOT NULL DEFAULT 90, observation_started_at TEXT, successful_observation_runs INTEGER NOT NULL DEFAULT 0, observation_proposed_actions INTEGER NOT NULL DEFAULT 0, observation_executable_actions INTEGER NOT NULL DEFAULT 0, observation_violations INTEGER NOT NULL DEFAULT 0, observation_structure_errors INTEGER NOT NULL DEFAULT 0, last_scheduled_at TEXT, last_emergency_at TEXT, updated_at TEXT NOT NULL)`,
		`INSERT INTO agent_settings(id,enabled,mode,updated_at) VALUES(1,1,'observe','2026-07-16T00:00:00Z')`,
		`CREATE TABLE score_policy_versions (id INTEGER PRIMARY KEY AUTOINCREMENT, scope_type TEXT NOT NULL, scope_id TEXT NOT NULL DEFAULT '', version INTEGER NOT NULL, status TEXT NOT NULL, config_json TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', agent_run_id INTEGER, created_by TEXT NOT NULL, activated_at TEXT, created_at TEXT NOT NULL, UNIQUE(scope_type,scope_id,version))`,
		`INSERT INTO score_policy_versions(id,scope_type,scope_id,version,status,config_json,reason,created_by,activated_at,created_at) VALUES(1,'global','',1,'active','{"failure_threshold":3}','active before C','administrator','2026-07-16T00:00:00Z','2026-07-16T00:00:00Z'),(2,'global','',2,'draft','{"failure_threshold":4}','draft before C','agent','', '2026-07-16T01:00:00Z')`,
		`CREATE TABLE agent_goals (id INTEGER PRIMARY KEY AUTOINCREMENT, parent_goal_id INTEGER, conversation_id INTEGER, title TEXT NOT NULL, objective TEXT NOT NULL, status TEXT NOT NULL, lane TEXT NOT NULL DEFAULT 'background', priority INTEGER NOT NULL DEFAULT 50, risk_level TEXT NOT NULL DEFAULT 'low', source TEXT NOT NULL DEFAULT 'system', context_json TEXT NOT NULL DEFAULT '{}', plan_hash TEXT NOT NULL DEFAULT '', created_by TEXT NOT NULL DEFAULT 'system', deadline_at TEXT, last_error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT, lease_owner TEXT NOT NULL DEFAULT '', lease_until TEXT, next_runnable_at TEXT)`,
		`INSERT INTO agent_goals(id,title,objective,status,lane,priority,risk_level,source,context_json,created_by,created_at,updated_at) VALUES(7,'stage-b-goal','preserve','planned','background',55,'low','scheduler','{}','system','2026-07-16T02:00:00Z','2026-07-16T02:00:00Z')`,
		`CREATE TABLE agent_checkpoints (id INTEGER PRIMARY KEY AUTOINCREMENT, goal_id INTEGER NOT NULL, step_id INTEGER, kind TEXT NOT NULL, state_json TEXT NOT NULL, state_hash TEXT NOT NULL, created_at TEXT NOT NULL)`,
		`INSERT INTO agent_checkpoints(id,goal_id,kind,state_json,state_hash,created_at) VALUES(8,7,'runtime','{"sequence":2}','stage-b-checkpoint','2026-07-16T02:01:00Z')`,
		`CREATE TABLE agent_scheduled_commands (id INTEGER PRIMARY KEY AUTOINCREMENT, goal_id INTEGER, step_id INTEGER, capability TEXT NOT NULL, arguments_json TEXT NOT NULL DEFAULT '{}', conditions_json TEXT NOT NULL DEFAULT '{}', status TEXT NOT NULL, timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai', execute_at TEXT NOT NULL, expires_at TEXT, idempotency_key TEXT NOT NULL UNIQUE, lease_owner TEXT NOT NULL DEFAULT '', lease_until TEXT, attempt_count INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 3, mutation_attempted_at TEXT, result_json TEXT NOT NULL DEFAULT '{}', last_error TEXT NOT NULL DEFAULT '', created_by TEXT NOT NULL DEFAULT 'system', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT)`,
		`INSERT INTO agent_scheduled_commands(id,goal_id,capability,status,execute_at,idempotency_key,created_by,created_at,updated_at) VALUES(9,7,'pause_account','planned','2026-07-18T02:00:00Z','stage-b-occurrence','administrator','2026-07-16T02:02:00Z','2026-07-16T02:02:00Z')`,
		`CREATE TABLE upstream_group_transitions (id INTEGER PRIMARY KEY AUTOINCREMENT, idempotency_key TEXT NOT NULL UNIQUE, source_id INTEGER NOT NULL, key_id TEXT NOT NULL, from_tier TEXT NOT NULL DEFAULT '', to_tier TEXT NOT NULL, from_group_id TEXT NOT NULL DEFAULT '', to_group_id TEXT NOT NULL, status TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', evidence TEXT NOT NULL DEFAULT '', trigger TEXT NOT NULL DEFAULT '', packet_id INTEGER NOT NULL DEFAULT 0, run_id INTEGER NOT NULL DEFAULT 0, dry_run INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', manual INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, completed_at TEXT)`,
		`INSERT INTO upstream_group_transitions(id,idempotency_key,source_id,key_id,from_tier,to_tier,from_group_id,to_group_id,status,actor,reason,created_at) VALUES(10,'stage-b-transition',3,'key-redacted','main','backup','group-a','group-b','succeeded','administrator','preserve','2026-07-16T03:00:00Z')`,
		`CREATE TABLE agent_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, trigger_reason TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, provider_slot TEXT NOT NULL DEFAULT '', model TEXT NOT NULL DEFAULT '', packet_id INTEGER, conversation_id INTEGER, summary TEXT NOT NULL DEFAULT '', conclusion TEXT NOT NULL DEFAULT '', confidence REAL NOT NULL DEFAULT 0, actions_json TEXT NOT NULL DEFAULT '[]', error TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL, completed_at TEXT)`,
		`INSERT INTO agent_runs(id,kind,status,started_at) VALUES(11,'scheduled','running','2026-07-16T04:00:00Z')`,
	}
	for _, statement := range statements {
		if _, err := raw.Exec(statement); err != nil {
			raw.Close()
			t.Fatalf("prepare stage B schema: %v\n%s", err, statement)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	for pass := 1; pass <= 2; pass++ {
		t.Run("pass_"+string(rune('0'+pass)), func(t *testing.T) {
			database, err := Open(path, testSettings())
			if err != nil {
				t.Fatalf("migration pass %d: %v", pass, err)
			}
			defer database.Close()
			assertCoreCMigrationData(t, database)
		})
	}
}

func assertCoreCMigrationData(t *testing.T, database *Store) {
	t.Helper()
	ctx := context.Background()
	versions, err := database.ListPolicyLifecycle(ctx, 10)
	statuses := make(map[int64]string, len(versions))
	for _, version := range versions {
		statuses[version.ID] = version.Status
	}
	if err != nil || len(versions) != 2 || statuses[1] != model.PolicyStatusActive || statuses[2] != model.PolicyStatusDraft {
		t.Fatalf("policy history was not preserved: %+v err=%v", versions, err)
	}
	goal, err := database.GetAgentGoal(ctx, 7)
	if err != nil || goal.Title != "stage-b-goal" || goal.Lane != model.AgentLaneBackground {
		t.Fatalf("goal was not preserved: %+v err=%v", goal, err)
	}
	checkpoint, err := database.LatestAgentCheckpoint(ctx, 7)
	if err != nil || checkpoint.StateHash != "stage-b-checkpoint" {
		t.Fatalf("checkpoint was not preserved: %+v err=%v", checkpoint, err)
	}
	command, err := database.GetScheduledCommand(ctx, 9)
	if err != nil || command.ID != 9 || command.OccurrenceID != "stage-b-occurrence" || command.IntentType != "scheduled_action" || command.MissedPolicy != "catch_up_once" {
		t.Fatalf("scheduled command was not upgraded: %+v err=%v", command, err)
	}
	var transitionStatus, producer, authority string
	var attempts, uncertain int
	if err := database.db.QueryRowContext(ctx, `SELECT status,producer,authority,attempt_count,uncertain FROM upstream_group_transitions WHERE id=10`).
		Scan(&transitionStatus, &producer, &authority, &attempts, &uncertain); err != nil {
		t.Fatal(err)
	}
	if transitionStatus != "succeeded" || producer != "" || authority != "" || attempts != 0 || uncertain != 0 {
		t.Fatalf("group transition migration changed history: %s %q %q %d %d", transitionStatus, producer, authority, attempts, uncertain)
	}
	var legacyRuns, goals int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_runs WHERE id=11 AND status='running'`).Scan(&legacyRuns); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_goals`).Scan(&goals); err != nil {
		t.Fatal(err)
	}
	if legacyRuns != 1 || goals != 1 {
		t.Fatalf("legacy execution history was reactivated: runs=%d goals=%d", legacyRuns, goals)
	}
}
