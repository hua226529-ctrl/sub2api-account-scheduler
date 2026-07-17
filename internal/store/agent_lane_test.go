package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func laneTestSettings() model.Settings {
	return model.Settings{FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10}
}

func TestAgentGoalLaneMigrationAndClaimOrdering(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), laneTestSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	conversationID := int64(7)
	legacyChat := model.AgentGoal{ConversationID: &conversationID, Title: "legacy chat", Objective: "chat", Status: model.AgentGoalStatusPlanned,
		Priority: 80, RiskLevel: model.AgentRiskHigh, Source: "administrator", Context: []byte(`{}`), CreatedBy: "administrator"}
	if err := database.CreateAgentGoal(ctx, &legacyChat); err != nil {
		t.Fatal(err)
	}
	background := model.AgentGoal{Title: "background", Objective: "background", Status: model.AgentGoalStatusPlanned,
		Priority: 90, RiskLevel: model.AgentRiskMedium, Source: "scheduler", Context: []byte(`{}`), CreatedBy: "scheduler"}
	if err := database.CreateAgentGoal(ctx, &background); err != nil {
		t.Fatal(err)
	}
	if legacyChat.Lane != model.AgentLaneInteractive || background.Lane != model.AgentLaneBackground {
		t.Fatalf("unexpected lane defaults: chat=%q background=%q", legacyChat.Lane, background.Lane)
	}
	claimedInteractive, err := database.ClaimAgentGoal(ctx, model.AgentLaneInteractive, "interactive-worker", time.Now().UTC(), time.Minute)
	if err != nil || claimedInteractive == nil || claimedInteractive.ID != legacyChat.ID {
		t.Fatalf("interactive claim crossed lane or ordering: goal=%+v err=%v", claimedInteractive, err)
	}
	claimedBackground, err := database.ClaimAgentGoal(ctx, model.AgentLaneBackground, "background-worker", time.Now().UTC(), time.Minute)
	if err != nil || claimedBackground == nil || claimedBackground.ID != background.ID {
		t.Fatalf("background claim crossed lane: goal=%+v err=%v", claimedBackground, err)
	}
	second, err := database.ClaimAgentGoal(ctx, model.AgentLaneInteractive, "other-worker", time.Now().UTC(), time.Minute)
	if err != nil || second != nil {
		t.Fatalf("same goal was claimed twice: goal=%+v err=%v", second, err)
	}
}

func TestAgentGoalExpiredLeaseCanBeReclaimed(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), laneTestSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	goal := model.AgentGoal{Title: "recover", Objective: "recover", Status: model.AgentGoalStatusPlanned,
		Lane: model.AgentLaneInteractive, Priority: 50, RiskLevel: model.AgentRiskLow, Source: "administrator", Context: []byte(`{}`), CreatedBy: "administrator"}
	if err := database.CreateAgentGoal(ctx, &goal); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	claimed, err := database.ClaimAgentGoal(ctx, model.AgentLaneInteractive, "old-worker", now, time.Millisecond)
	if err != nil || claimed == nil {
		t.Fatalf("initial claim failed: %+v %v", claimed, err)
	}
	reclaimed, err := database.ClaimAgentGoal(ctx, model.AgentLaneInteractive, "new-worker", now.Add(time.Second), time.Minute)
	if err != nil || reclaimed == nil || reclaimed.ID != goal.ID || reclaimed.LeaseOwner != "new-worker" {
		t.Fatalf("expired lease was not reclaimed: %+v err=%v", reclaimed, err)
	}
}

func TestAgentGoalLaneIndexAndLegacyBackfill(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), laneTestSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	var columns int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('agent_goals') WHERE name IN ('lane','lease_owner','lease_until','next_runnable_at')`).Scan(&columns); err != nil || columns != 4 {
		t.Fatalf("lane migration columns missing: count=%d err=%v", columns, err)
	}
	var indexName sql.NullString
	if err := database.db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='index' AND name='idx_agent_goals_claim'`).Scan(&indexName); err != nil || !indexName.Valid {
		t.Fatalf("lane claim index missing: %v", err)
	}
	rows, err := database.db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT id FROM agent_goals
		WHERE lane=? AND status=? AND (next_runnable_at IS NULL OR next_runnable_at<=?)
		ORDER BY priority DESC,created_at,id LIMIT 1`, model.AgentLaneInteractive, model.AgentGoalStatusPlanned, formatTime(time.Now().UTC()))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	usesClaimIndex := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "idx_agent_goals_claim") {
			usesClaimIndex = true
		}
	}
	if !usesClaimIndex {
		t.Fatal("lane claim query did not use idx_agent_goals_claim")
	}
	conversationID := int64(11)
	if _, err := database.db.ExecContext(ctx, `INSERT INTO agent_goals(conversation_id,title,objective,status,lane,priority,risk_level,source,context_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		conversationID, "old chat", "old chat", model.AgentGoalStatusPlanned, model.AgentLaneBackground, 50, model.AgentRiskHigh, "administrator", "{}", formatTime(time.Now().UTC()), formatTime(time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if err := database.backfillAgentGoalLanes(ctx); err != nil {
		t.Fatal(err)
	}
	var lane string
	if err := database.db.QueryRowContext(ctx, `SELECT lane FROM agent_goals WHERE conversation_id=?`, conversationID).Scan(&lane); err != nil || lane != model.AgentLaneInteractive {
		t.Fatalf("legacy chat was not backfilled interactive: lane=%q err=%v", lane, err)
	}
}

func TestAgentGoalLaneMigrationUpgradesLegacySchemaWithoutLosingRuntimeState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`CREATE TABLE agent_goals (
		id INTEGER PRIMARY KEY AUTOINCREMENT,parent_goal_id INTEGER,conversation_id INTEGER,title TEXT NOT NULL,
		objective TEXT NOT NULL,status TEXT NOT NULL,priority INTEGER NOT NULL DEFAULT 50,risk_level TEXT NOT NULL DEFAULT 'low',
		source TEXT NOT NULL DEFAULT 'system',context_json TEXT NOT NULL DEFAULT '{}',plan_hash TEXT NOT NULL DEFAULT '',
		created_by TEXT NOT NULL DEFAULT 'system',deadline_at TEXT,last_error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,completed_at TEXT)`)
	if err != nil {
		raw.Close()
		t.Fatal(err)
	}
	now := formatTime(time.Now().UTC())
	for _, values := range []struct {
		title, source, status string
		conversation          any
	}{
		{title: "legacy-chat", source: "administrator", status: model.AgentGoalStatusPlanned, conversation: int64(9)},
		{title: "legacy-daily", source: "scheduler", status: model.AgentGoalStatusRunning},
		{title: "legacy-unknown", source: "legacy-import", status: model.AgentGoalStatusPlanned},
	} {
		if _, err := raw.Exec(`INSERT INTO agent_goals(conversation_id,title,objective,status,priority,risk_level,source,context_json,created_by,created_at,updated_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`, values.conversation, values.title, values.title, values.status, 50,
			model.AgentRiskLow, values.source, `{}`, values.source, now, now); err != nil {
			raw.Close()
			t.Fatal(err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := Open(path, laneTestSettings())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	assertLane := func(title, want string) int64 {
		t.Helper()
		var id int64
		var lane string
		if err := database.db.QueryRowContext(ctx, `SELECT id,lane FROM agent_goals WHERE title=?`, title).Scan(&id, &lane); err != nil {
			t.Fatal(err)
		}
		if lane != want {
			t.Fatalf("%s lane=%q, want %q", title, lane, want)
		}
		return id
	}
	chatID := assertLane("legacy-chat", model.AgentLaneInteractive)
	backgroundID := assertLane("legacy-daily", model.AgentLaneBackground)
	assertLane("legacy-unknown", model.AgentLaneBackground)
	leaseUntil := time.Now().UTC().Add(time.Minute)
	if _, err := database.db.ExecContext(ctx, `UPDATE agent_goals SET lease_owner='legacy-worker',lease_until=? WHERE id=?`, formatTime(leaseUntil), backgroundID); err != nil {
		t.Fatal(err)
	}
	step := model.AgentStep{GoalID: chatID, Sequence: 1, Capability: "inspect_snapshot", Arguments: []byte(`{}`),
		Preconditions: []byte(`{}`), Compensation: []byte(`{}`), Status: model.AgentStepStatusPending,
		RiskLevel: model.AgentRiskLow, IdempotencyKey: "legacy-lane-step", BeforeState: []byte(`{}`), AfterState: []byte(`{}`), Result: []byte(`{}`)}
	if err := database.CreateAgentStep(ctx, &step); err != nil {
		t.Fatal(err)
	}
	if err := database.SaveAgentCheckpoint(ctx, &model.AgentCheckpoint{GoalID: chatID, StepID: &step.ID, Kind: "runtime", State: []byte(`{}`), StateHash: "legacy-lane-checkpoint"}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path, laneTestSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var status, owner string
	var lease string
	if err := database.db.QueryRowContext(ctx, `SELECT status,lease_owner,lease_until FROM agent_goals WHERE id=?`, backgroundID).Scan(&status, &owner, &lease); err != nil {
		t.Fatal(err)
	}
	if status != model.AgentGoalStatusRunning || owner != "legacy-worker" || parseTime(lease).IsZero() {
		t.Fatalf("running goal lease changed during repeat migration: status=%q owner=%q lease=%q", status, owner, lease)
	}
	var steps, checkpoints int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_steps WHERE goal_id=?`, chatID).Scan(&steps); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_checkpoints WHERE goal_id=?`, chatID).Scan(&checkpoints); err != nil {
		t.Fatal(err)
	}
	if steps != 1 || checkpoints != 1 {
		t.Fatalf("step/checkpoint lost during repeat migration: steps=%d checkpoints=%d", steps, checkpoints)
	}
}
