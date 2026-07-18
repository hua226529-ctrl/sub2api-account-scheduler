package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestRC2MigratesAndQuarantinesLegacyContractFailureGoal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "scheduler.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`CREATE TABLE agent_goals (
		id INTEGER PRIMARY KEY AUTOINCREMENT,parent_goal_id INTEGER,conversation_id INTEGER,title TEXT NOT NULL,
		objective TEXT NOT NULL,status TEXT NOT NULL,lane TEXT NOT NULL DEFAULT 'background',priority INTEGER NOT NULL DEFAULT 50,
		risk_level TEXT NOT NULL DEFAULT 'low',source TEXT NOT NULL DEFAULT 'system',context_json TEXT NOT NULL DEFAULT '{}',
		plan_hash TEXT NOT NULL DEFAULT '',created_by TEXT NOT NULL DEFAULT 'system',deadline_at TEXT,last_error TEXT NOT NULL DEFAULT '',
		attempt_count INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL,updated_at TEXT NOT NULL,completed_at TEXT,lease_owner TEXT NOT NULL DEFAULT '',lease_until TEXT,
		next_runnable_at TEXT)`)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	if _, err := raw.Exec(`INSERT INTO agent_goals(title,objective,status,lane,priority,risk_level,source,context_json,
		created_by,last_error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?),(?,?,?,?,?,?,?,?,?,?,?,?)`,
		"legacy invalid", "must stop", model.AgentGoalStatusPlanned, model.AgentLaneBackground, 50, model.AgentRiskLow,
		"system", `{}`, "system", "模型最终结构化结论无效: cannot unmarshal string into Go value of type agent.EvidenceRequest", now, now,
		"legacy failed", "must stay failed", model.AgentGoalStatusFailed, model.AgentLaneBackground, 50, model.AgentRiskLow,
		"system", `{}`, "system", "old terminal failure", now, now); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	database, err := Open(path, testSettings())
	if err != nil {
		t.Fatal(err)
	}
	quarantined, err := database.GetAgentGoal(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if quarantined.Status != model.AgentGoalStatusFailed || quarantined.LastErrorClass != "model_contract_invalid" ||
		!quarantined.DeadLettered || quarantined.MaxAttempts != 5 || quarantined.NextRunnableAt != nil {
		t.Fatalf("legacy contract goal was not safely quarantined: %+v", quarantined)
	}
	terminal, err := database.GetAgentGoal(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Status != model.AgentGoalStatusFailed || terminal.LastError != "old terminal failure" {
		t.Fatalf("legacy terminal goal changed: %+v", terminal)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = Open(path, testSettings())
	if err != nil {
		t.Fatalf("repeat migration failed: %v", err)
	}
	defer database.Close()
	second, err := database.GetAgentGoal(ctx, 1)
	if err != nil || second.Status != model.AgentGoalStatusFailed || !second.DeadLettered {
		t.Fatalf("repeat migration changed quarantine: goal=%+v err=%v", second, err)
	}
	var columns int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('agent_goals') WHERE name IN
		('attempt_count','max_attempts','model_attempt_count','contract_failure_count','no_progress_count','last_error_class',
		'last_error_at','completed_with_warnings','dead_lettered_at')`).Scan(&columns); err != nil || columns != 9 {
		t.Fatalf("RC2 goal columns missing: count=%d err=%v", columns, err)
	}
}

func TestRC2ProviderSuccessPreservesRecentFailureObservability(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.UpsertAgentProvider(ctx, model.AgentProvider{Slot: "primary", BaseURL: "http://model.invalid",
		Model: "test", Enabled: true, TimeoutSeconds: 10, MaxOutputTokens: 100}); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Second)
	if err := database.RecordAgentProviderFailure(ctx, "primary", "provider_timeout", "deadline", at); err != nil {
		t.Fatal(err)
	}
	if err := database.RecordAgentProviderFailure(ctx, "primary", "provider_server_error", "HTTP 503", at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := database.RecordAgentProviderSuccess(ctx, "primary", at.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	provider, err := database.GetAgentProvider(ctx, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if provider.LastError != "" || provider.ConsecutiveFailures != 0 || provider.RecentError != "HTTP 503" ||
		provider.LastErrorClass != "provider_server_error" || provider.LastErrorAt == nil || provider.ErrorCount24h != 2 {
		t.Fatalf("provider success erased failure history: %+v", provider)
	}
	if err := database.RecordAgentProviderFailure(ctx, "primary", "provider_timeout", "next window", at.Add(25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	provider, err = database.GetAgentProvider(ctx, "primary")
	if err != nil || provider.ErrorCount24h != 1 || provider.ErrorWindowStartedAt == nil ||
		provider.ErrorWindowStartedAt.Before(at.Add(24*time.Hour)) {
		t.Fatalf("provider 24h error window did not reset: provider=%+v err=%v", provider, err)
	}
}

func TestRC2RuntimeProvenanceRoundTripsWithoutLegacyRunID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	goalID, stepID, packetID := int64(101), int64(202), int64(303)
	policy := model.ScorePolicyVersion{ScopeType: "global", Patch: json.RawMessage(`{"minimum_samples":10}`),
		Config: json.RawMessage(`{"minimum_samples":10}`), Status: model.PolicyStatusSimulated, CreatedBy: "agent:v2",
		SourceGoalID: &goalID, SourceStepID: &stepID, SourcePacketID: &packetID, SourcePacketHash: "packet-hash",
		IdempotencyKey: "rc2-policy-provenance", SemanticHash: "semantic-hash"}
	if err := database.CreatePolicyProposal(ctx, &policy); err != nil {
		t.Fatal(err)
	}
	loadedPolicy, err := database.GetPolicyLifecycle(ctx, policy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loadedPolicy.AgentRunID != nil || loadedPolicy.SourceGoalID == nil || *loadedPolicy.SourceGoalID != goalID ||
		loadedPolicy.SourceStepID == nil || *loadedPolicy.SourceStepID != stepID || loadedPolicy.SourcePacketID == nil ||
		*loadedPolicy.SourcePacketID != packetID || loadedPolicy.SourcePacketHash != "packet-hash" {
		t.Fatalf("V2 policy provenance was not preserved: %+v", loadedPolicy)
	}

	accountID := int64(225)
	outcome := model.DecisionOutcome{GoalID: goalID, StepID: stepID, PacketID: packetID, PacketHash: "packet-hash",
		AccountID: &accountID, EvaluateAt: time.Now().UTC().Add(-time.Minute)}
	if err := database.AddDecisionOutcome(ctx, &outcome); err != nil {
		t.Fatal(err)
	}
	items, err := database.ListPendingDecisionOutcomes(ctx, time.Now().UTC(), 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("list V2 outcome: items=%+v err=%v", items, err)
	}
	if items[0].RunID != 0 || items[0].GoalID != goalID || items[0].StepID != stepID ||
		items[0].PacketID != packetID || items[0].PacketHash != "packet-hash" {
		t.Fatalf("V2 outcome provenance was not preserved: %+v", items[0])
	}

	if err := database.AddEvent(ctx, model.Event{GoalID: goalID, StepID: stepID, Type: "runtime_v2_audit",
		Severity: "info", Message: "provenance", Actor: "agent:v2"}); err != nil {
		t.Fatal(err)
	}
	events, err := database.ListEvents(ctx, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("list V2 audit event: events=%+v err=%v", events, err)
	}
	if events[0].GoalID != goalID || events[0].StepID != stepID {
		t.Fatalf("V2 audit provenance was not preserved: %+v", events[0])
	}
}
