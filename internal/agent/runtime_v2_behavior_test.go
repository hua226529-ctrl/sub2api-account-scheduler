package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

func TestOverviewDerivesRunningStateFromPersistentGoal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	manager := &Manager{store: database}
	goal := model.AgentGoal{Title: "running", Objective: "verify overview", Status: model.AgentGoalStatusPlanned,
		Lane: model.AgentLaneInteractive, Priority: 50, RiskLevel: model.AgentRiskLow, Source: "test",
		Context: json.RawMessage(`{"kind":"chat"}`), CreatedBy: "test"}
	if err := database.CreateAgentGoal(ctx, &goal); err != nil {
		t.Fatal(err)
	}
	claimed, err := database.ClaimAgentGoal(ctx, model.AgentLaneInteractive, "overview-test", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != goal.ID {
		t.Fatalf("goal was not claimed: %+v", claimed)
	}
	overview, err := manager.Overview(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !overview.Running {
		t.Fatal("overview did not expose the persistently leased goal")
	}

	claimed.Status = model.AgentGoalStatusCompleted
	if err := database.UpdateAgentGoal(ctx, *claimed); err != nil {
		t.Fatal(err)
	}
	overview, err = manager.Overview(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if overview.Running {
		t.Fatal("overview remained running after the goal completed")
	}
}

func TestCharacterizationTransitionTokenGroupTierRequiresAgentConfidenceButAllowsAdministrator(t *testing.T) {
	t.Parallel()
	manager := &Manager{}
	arguments := json.RawMessage(`{"source_id":9,"key_id":"token-7","target_tier":"backup","confidence":0.89,"reason":"pool outage"}`)

	_, retryable, err := manager.executeMutationCapability(context.Background(), CapabilityInvocation{
		Name:      "transition_token_group_tier",
		Arguments: arguments,
		Actor:     "agent:v2",
		DryRun:    true,
	})
	if err == nil || !strings.Contains(err.Error(), "0.90") {
		t.Fatalf("low-confidence autonomous transition was not rejected: retryable=%v err=%v", retryable, err)
	}
	if retryable {
		t.Fatal("a deterministic confidence rejection must not be retried as an external failure")
	}

	output, retryable, err := manager.executeMutationCapability(context.Background(), CapabilityInvocation{
		Name:      "transition_token_group_tier",
		Arguments: arguments,
		Actor:     "administrator:agent",
		AdministratorGrant: mintAdministratorGrant(administratorCommandHash("tier-test-scope"),
			administratorCommandHash("把上游9令牌token-7切换到备用分组"), "immediate",
			"transition_token_group_tier", arguments, []string{"source:9", "key:token-7"}, "", nil, nil),
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("administrator command did not bypass the autonomous confidence threshold: %v", err)
	}
	if retryable {
		t.Fatal("successful administrator validation was unexpectedly marked retryable")
	}
	result, ok := output.(map[string]any)
	if !ok || result["would_transition"] == nil {
		t.Fatalf("administrator transition did not reach the execution layer: %#v", output)
	}
}

func TestTransitionTokenGroupTierPolicyRequiresAdministratorConfirmation(t *testing.T) {
	spec, ok := capabilitySpec("transition_token_group_tier")
	if !ok {
		t.Fatal("transition capability missing")
	}
	if spec.ExecutionPolicy.SupportsAutonomous || !spec.ExecutionPolicy.RequiresConfirmation ||
		spec.RiskLevel != model.AgentRiskCritical {
		t.Fatalf("unsafe transition policy: %+v", spec.ExecutionPolicy)
	}

	manager := &Manager{}
	_, err := manager.ExecuteCapability(context.Background(), CapabilityInvocation{
		Name: "transition_token_group_tier", Actor: "agent:v2", DryRun: true,
		Arguments: json.RawMessage(`{"source_id":9,"key_id":"token-7","target_tier":"backup","confidence":0.99,"reason":"confirmed outage evidence"}`),
		CreatedAt: time.Now().UTC(), SnapshotVersion: "packet:1", EvidenceRefs: []string{"packet:1"},
	})
	if err == nil || !strings.Contains(err.Error(), "管理员确认") {
		t.Fatalf("autonomous critical transition was not blocked: %v", err)
	}
}

func TestExecutionPolicyDefaultTTLIsAppliedAtCapabilityBoundary(t *testing.T) {
	spec, ok := capabilitySpec("pause_account")
	if !ok {
		t.Fatal("pause capability missing")
	}
	createdAt := time.Now().UTC().Truncate(time.Second)
	invocation := CapabilityInvocation{Name: "pause_account", CreatedAt: createdAt}
	if err := applyExecutionPolicyDefaults(&invocation, spec); err != nil {
		t.Fatal(err)
	}
	if invocation.ExpiresAt == nil || invocation.ExpiresAt.Sub(createdAt) != time.Duration(spec.ExecutionPolicy.DefaultTTLSeconds)*time.Second {
		t.Fatalf("default TTL was not applied: invocation=%+v policy=%+v", invocation, spec.ExecutionPolicy)
	}
	missingTime := CapabilityInvocation{Name: "pause_account"}
	if err := applyExecutionPolicyDefaults(&missingTime, spec); err == nil {
		t.Fatal("default TTL was invented without an auditable creation time")
	}
	explicit := createdAt.Add(time.Hour)
	invocation = CapabilityInvocation{Name: "pause_account", CreatedAt: createdAt,
		Arguments: json.RawMessage(fmt.Sprintf(`{"account_id":1,"expires_at":%q,"reason":"test"}`, explicit.Format(time.RFC3339)))}
	if err := applyExecutionPolicyDefaults(&invocation, spec); err != nil || invocation.ExpiresAt == nil || !invocation.ExpiresAt.Equal(explicit) {
		t.Fatalf("explicit capability TTL was not preserved: invocation=%+v err=%v", invocation, err)
	}
	tooLong := createdAt.Add(time.Duration(spec.ExecutionPolicy.MaxTTLSeconds+1) * time.Second)
	invocation = CapabilityInvocation{Name: "pause_account", CreatedAt: createdAt,
		Arguments: json.RawMessage(fmt.Sprintf(`{"account_id":1,"expires_at":%q,"reason":"test"}`, tooLong.Format(time.RFC3339)))}
	if err := applyExecutionPolicyDefaults(&invocation, spec); err != nil {
		t.Fatal(err)
	}
	if ttl := invocation.ExpiresAt.Sub(createdAt); ttl <= time.Duration(spec.ExecutionPolicy.MaxTTLSeconds)*time.Second {
		t.Fatalf("test TTL did not exceed policy: %s", ttl)
	}
	_, err := (&Manager{}).ExecuteCapability(context.Background(), CapabilityInvocation{Name: "pause_account",
		Arguments: invocation.Arguments, Actor: "agent:v2", CreatedAt: createdAt,
		SnapshotVersion: "packet:ttl", EvidenceRefs: []string{"packet:ttl"}})
	if err == nil || !strings.Contains(err.Error(), "TTL") {
		t.Fatalf("execution boundary accepted TTL above maximum: %v", err)
	}
}

func TestConfirmedCapabilityStillRequiresEvidenceAndSnapshot(t *testing.T) {
	arguments := json.RawMessage(`{"source_id":9,"key_id":"token-7","target_tier":"backup","confidence":1,"reason":"administrator confirmed"}`)
	arguments, _ = normalizedArguments(arguments)
	grant := mintAdministratorGrant(administratorCommandHash("confirmed-policy-scope"), administratorCommandHash("切换令牌分组"),
		"immediate", "transition_token_group_tier", arguments, []string{"source:9", "key:token-7"}, "", nil, nil)
	manager := &Manager{}
	_, err := manager.ExecuteCapability(context.Background(), CapabilityInvocation{Name: "transition_token_group_tier",
		Arguments: arguments, Actor: "administrator:agent", AdministratorGrant: grant, GoalID: 1, StepID: 2,
		CreatedAt: time.Now().UTC(), SnapshotVersion: "packet:1"})
	if err == nil || !strings.Contains(err.Error(), "EvidenceRefs") {
		t.Fatalf("administrator confirmation bypassed evidence policy: %v", err)
	}
}

func TestCharacterizationScheduledCommandPersistsExactAdministratorGrant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	manager := &Manager{store: database}
	executeAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	arguments, err := json.Marshal(map[string]any{
		"capability": "trigger_reconcile",
		"arguments":  map[string]any{"reason": "scheduled administrator command"},
		"execute_at": executeAt,
		"timezone":   model.AgentDefaultTimezone,
		"reason":     "preserve administrator provenance",
	})
	if err != nil {
		t.Fatal(err)
	}
	commandHash := administratorCommandHash("早上6点执行一次协调")
	const idempotencyKey = "schedule-admin-command-hash-test"
	targetArguments := json.RawMessage(`{"reason":"scheduled administrator command"}`)
	outerGrant := mintAdministratorGrant(administratorCommandHash("scheduled-test-scope"), commandHash,
		"scheduled", "schedule_command", arguments,
		[]string{"global:scheduler"}, "trigger_reconcile", targetArguments, []string{"global:scheduler"})
	output, retryable, err := manager.executeMutationCapability(ctx, CapabilityInvocation{
		Name:               "schedule_command",
		Arguments:          arguments,
		Actor:              "administrator:agent",
		IdempotencyKey:     idempotencyKey,
		AdministratorGrant: outerGrant,
	})
	if err != nil {
		t.Fatal(err)
	}
	if retryable {
		t.Fatal("local scheduled-command persistence was unexpectedly marked retryable")
	}

	created, ok := output.(model.ScheduledCommand)
	if !ok || created.OccurrenceID == "" || created.IdempotencyKey != created.OccurrenceID {
		t.Fatalf("scheduled command did not return a stable occurrence identity: %#v", output)
	}
	command, err := database.GetScheduledCommandByIdempotencyKey(ctx, created.OccurrenceID)
	if err != nil {
		t.Fatal(err)
	}
	var conditions struct {
		AdministratorGrant *AdministratorGrant `json:"administrator_grant"`
	}
	if err := json.Unmarshal(command.Conditions, &conditions); err != nil {
		t.Fatalf("stored scheduled-command conditions are invalid: %v", err)
	}
	if conditions.AdministratorGrant == nil || conditions.AdministratorGrant.CommandHash != commandHash ||
		conditions.AdministratorGrant.Capability != "trigger_reconcile" || conditions.AdministratorGrant.ArgumentsHash != administratorArgumentsHash(targetArguments) {
		t.Fatalf("administrator provenance was not preserved: %+v", conditions)
	}
	if conditions.AdministratorGrant.GrantID == "" || conditions.AdministratorGrant.GrantID == outerGrant.GrantID {
		t.Fatalf("scheduled target reused the outer command grant id: outer=%s target=%+v", outerGrant.GrantID, conditions.AdministratorGrant)
	}
	if !command.ExecuteAt.Equal(executeAt) || command.CreatedBy != "administrator:agent" {
		t.Fatalf("scheduled command metadata changed during persistence: %+v", command)
	}
}

func TestExplicitAdministratorCommandSeparatesQuestionsFromCommands(t *testing.T) {
	t.Parallel()
	tests := []struct {
		message string
		want    bool
	}{
		{message: "为什么现在还不恢复", want: false},
		{message: "分析一下为什么 account-example 被禁用", want: false},
		{message: "把 account-example 设为25并保持到早上6点", want: true},
		{message: "立即恢复备用账号", want: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.message, func(t *testing.T) {
			t.Parallel()
			if got := explicitAdministratorCommand(test.message); got != test.want {
				t.Fatalf("explicitAdministratorCommand(%q)=%v, want %v", test.message, got, test.want)
			}
		})
	}
}

func TestEmergencyGoalDeduplicationUsesEventTypeScopeAndWindow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	manager := &Manager{store: database, interactiveWake: make(chan struct{}, 1), backgroundWake: make(chan struct{}, 1)}
	window := time.Now().UTC().Truncate(emergencyAggregationWindow).Format(time.RFC3339)
	firstContext := runtimeGoalContext{Kind: model.AgentRunEmergency, Trigger: "严重运行事件", EventType: "health_stage_changed",
		ResourceScope: "account:1", AggregationWindow: window, AuditEventRefs: []int64{10}}
	first, err := manager.enqueueAnalysisGoal(ctx, firstContext, emergencyObjective(firstContext.EventType, firstContext.ResourceScope, firstContext.AuditEventRefs), 95)
	if err != nil {
		t.Fatal(err)
	}
	mergedContext := firstContext
	mergedContext.AuditEventRefs = []int64{11}
	second, err := manager.enqueueAnalysisGoal(ctx, mergedContext, emergencyObjective(mergedContext.EventType, mergedContext.ResourceScope, mergedContext.AuditEventRefs), 90)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatalf("same scoped emergency was not merged: first=%d second=%d", first.ID, second.ID)
	}
	second, err = database.GetAgentGoal(ctx, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	var merged runtimeGoalContext
	if err := json.Unmarshal(second.Context, &merged); err != nil || len(merged.AuditEventRefs) != 2 {
		t.Fatalf("merged emergency did not retain audit references: context=%s err=%v", second.Context, err)
	}
	different := firstContext
	different.ResourceScope = "account:2"
	different.AuditEventRefs = []int64{12}
	third, err := manager.enqueueAnalysisGoal(ctx, different, emergencyObjective(different.EventType, different.ResourceScope, different.AuditEventRefs), 95)
	if err != nil {
		t.Fatal(err)
	}
	if third.ID == first.ID {
		t.Fatal("different emergency accounts were incorrectly merged")
	}
	active, err := database.ListAgentGoals(ctx, model.AgentGoalStatusPlanned, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 2 {
		t.Fatalf("expected two scoped emergency goals, got %d: %+v", len(active), active)
	}

	second.Status = model.AgentGoalStatusCompleted
	if err := database.UpdateAgentGoal(ctx, second); err != nil {
		t.Fatal(err)
	}
	newWindow := firstContext
	newWindow.AggregationWindow = time.Now().UTC().Add(emergencyAggregationWindow).Truncate(emergencyAggregationWindow).Format(time.RFC3339)
	fourth, err := manager.enqueueAnalysisGoal(ctx, newWindow, emergencyObjective(newWindow.EventType, newWindow.ResourceScope, []int64{13}), 95)
	if err != nil {
		t.Fatal(err)
	}
	if fourth.ID == first.ID {
		t.Fatal("completed emergency goal incorrectly blocked a new emergency goal")
	}
}

func TestOperationalEventScopeNeverInventsAccountZero(t *testing.T) {
	manager, _ := stage0AgentManager(t)
	if err := manager.engine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	accountID := int64(9)
	monitorID := int64(10_000)
	if got := manager.operationalEventScope(model.Event{AccountID: &accountID}); got != "account:9" {
		t.Fatalf("account scope=%q", got)
	}
	if got := manager.operationalEventScope(model.Event{MonitorID: &monitorID}); got != "account:1" {
		t.Fatalf("unique monitor binding scope=%q", got)
	}
	unbound := int64(99_999)
	if got := manager.operationalEventScope(model.Event{MonitorID: &unbound}); got != "monitor:99999" {
		t.Fatalf("unbound monitor scope=%q", got)
	}
	if got := manager.operationalEventScope(model.Event{}); got != "global" {
		t.Fatalf("global scope=%q", got)
	}
	if objective := emergencyObjective("health_stage_changed", "monitor:99999", []int64{7}); strings.Contains(objective, "账号 0") {
		t.Fatalf("objective invented account zero: %s", objective)
	}
}

func TestCharacterizationRuntimeMessagesAdvancePastStepsCreatedAfterCheckpoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	goal := model.AgentGoal{Title: "resume", Objective: "resume after crash", Status: model.AgentGoalStatusPlanned,
		Priority: 50, RiskLevel: model.AgentRiskLow, Source: "test", Context: json.RawMessage(`{"kind":"chat"}`), CreatedBy: "test"}
	if err := database.CreateAgentGoal(ctx, &goal); err != nil {
		t.Fatal(err)
	}
	checkpointState, _ := json.Marshal(runtimeCheckpoint{Messages: []RuntimeMessage{{Role: "system", Content: "test"}}, NextSequence: 1})
	if err := database.SaveAgentCheckpoint(ctx, &model.AgentCheckpoint{GoalID: goal.ID, Kind: "runtime",
		State: checkpointState, StateHash: "checkpoint-before-step"}); err != nil {
		t.Fatal(err)
	}
	step := model.AgentStep{GoalID: goal.ID, Sequence: 3, Capability: "pause_account",
		Arguments: json.RawMessage(`{"account_id":225}`), Preconditions: json.RawMessage(`{}`), Compensation: json.RawMessage(`{}`),
		Status: model.AgentStepStatusCompleted, RiskLevel: model.AgentRiskMedium, IdempotencyKey: "post-checkpoint-step",
		BeforeState: json.RawMessage(`{"schedulable":true}`), AfterState: json.RawMessage(`{"schedulable":false}`), Result: json.RawMessage(`{}`)}
	if err := database.CreateAgentStep(ctx, &step); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{store: database}
	messages, sequence, _, _, err := manager.runtimeMessages(ctx, goal, runtimeGoalContext{}, model.AnalysisPacket{}, model.AgentSettings{})
	if err != nil {
		t.Fatal(err)
	}
	if sequence != 3 {
		t.Fatalf("runtime reused a sequence behind persisted work: got %d want 3", sequence)
	}
	found := false
	for _, message := range messages {
		content := fmt.Sprint(message.Content)
		if strings.Contains(content, "只读核对已解析步骤") {
			found = true
		}
	}
	if !found {
		t.Fatalf("resolved reconciliation result was not injected into context: %+v", messages)
	}
	if messages[0].Role != "system" || messages[0].Content != runtimeSystemPrompt() {
		t.Fatalf("checkpoint retained a stale system prompt: %+v", messages[0])
	}
	refreshed := false
	for _, message := range messages {
		content := fmt.Sprint(message.Content)
		if message.Role == "user" && strings.Contains(content, "最新数据水位") &&
			strings.Contains(content, "北京时间") && strings.Contains(content, goal.Objective) {
			refreshed = true
			break
		}
	}
	if !refreshed {
		t.Fatalf("checkpoint resume did not inject fresh packet/time/context: %+v", messages)
	}
}

func TestCheckpointRuntimeYieldWaitsGoalWithoutWritingLegacyRun(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	goal := model.AgentGoal{Title: "freeze", Objective: "wait safely", Status: model.AgentGoalStatusRunning,
		Priority: 50, RiskLevel: model.AgentRiskLow, Source: "test", Context: json.RawMessage(`{"kind":"chat"}`), CreatedBy: "test"}
	if err := database.CreateAgentGoal(ctx, &goal); err != nil {
		t.Fatal(err)
	}
	run := model.AgentRun{Kind: model.AgentRunChat, Trigger: "freeze", Status: "running", StartedAt: time.Now().UTC(), ActionsJSON: json.RawMessage("[]")}
	manager := &Manager{store: database, interactiveWake: make(chan struct{}, 1), backgroundWake: make(chan struct{}, 1)}
	cause := errors.New("智能体已被冻结")
	gotErr := manager.checkpointRuntimeYield(ctx, &goal, &run,
		[]RuntimeMessage{{Role: "system", Content: runtimeSystemPrompt()}}, 2, "", 0,
		"waiting", model.AgentGoalStatusWaiting, "goal_paused_by_freeze", cause, false)
	if !errors.Is(gotErr, cause) {
		t.Fatalf("yield returned unexpected error: %v", gotErr)
	}

	storedGoal, err := database.GetAgentGoal(ctx, goal.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedGoal.Status != model.AgentGoalStatusWaiting || storedGoal.LastError != cause.Error() {
		t.Fatalf("goal was not persisted as waiting: %+v", storedGoal)
	}
	runs, err := database.ListAgentRuns(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("runtime v2 wrote the legacy agent_runs table: %+v", runs)
	}
	checkpoint, err := database.LatestAgentCheckpoint(ctx, goal.ID)
	if err != nil || len(checkpoint.State) == 0 {
		t.Fatalf("yield did not save a checkpoint: checkpoint=%+v err=%v", checkpoint, err)
	}
}

func TestRuntimeErrorReleasePreservesExplicitWaitingState(t *testing.T) {
	manager, database := stage0AgentManager(t)
	ctx := context.Background()
	goal := model.AgentGoal{Title: "waiting", Objective: "waiting", Status: model.AgentGoalStatusPlanned,
		Lane: model.AgentLaneInteractive, Priority: 50, RiskLevel: model.AgentRiskLow, Source: "administrator",
		Context: json.RawMessage(`{"kind":"chat"}`), CreatedBy: "administrator"}
	if err := database.Store.CreateAgentGoal(ctx, &goal); err != nil {
		t.Fatal(err)
	}
	worker := manager.workerID + ":" + model.AgentLaneInteractive
	claimed, err := database.Store.ClaimAgentGoal(ctx, model.AgentLaneInteractive, worker, time.Now().UTC(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim waiting goal: goal=%+v err=%v", claimed, err)
	}
	claimed.Status = model.AgentGoalStatusWaiting
	claimed.LastError = "frozen"
	if err := database.Store.UpdateAgentGoal(ctx, *claimed); err != nil {
		t.Fatal(err)
	}
	manager.releaseRuntimeGoalAfterError(ctx, claimed.ID, worker, model.AgentLaneInteractive, errors.New("frozen"))
	stored, err := database.Store.GetAgentGoal(ctx, claimed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.AgentGoalStatusWaiting || stored.LeaseOwner != "" || stored.LeaseUntil != nil || stored.NextRunnableAt != nil {
		t.Fatalf("waiting goal was requeued or retained its lease: %+v", stored)
	}
}

func TestResumeGoalAfterReconciliationDoesNotReopenTerminalGoal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	manager := &Manager{store: database, interactiveWake: make(chan struct{}, 1), backgroundWake: make(chan struct{}, 1)}

	for _, status := range []string{model.AgentGoalStatusCompleted, model.AgentGoalStatusCancelled, model.AgentGoalStatusFailed} {
		goal := model.AgentGoal{Title: status, Objective: "terminal", Status: status, Priority: 50,
			RiskLevel: model.AgentRiskLow, Source: "test", Context: json.RawMessage(`{"kind":"chat"}`), CreatedBy: "test"}
		if err := database.CreateAgentGoal(ctx, &goal); err != nil {
			t.Fatal(err)
		}
		manager.resumeGoalAfterReconciliation(ctx, goal.ID, "late reconciliation")
		stored, err := database.GetAgentGoal(ctx, goal.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Status != status {
			t.Fatalf("terminal goal %s was reopened as %s", status, stored.Status)
		}
	}
}

func TestRuntimeObservationRecordsStructuralAndPrivilegeFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := store.Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	manager := &Manager{store: database}
	settings := model.AgentSettings{OptimizerMode: model.AgentOptimizerObserve}
	manager.recordRuntimeObservation(ctx, settings, 2, 0, 2, 1)
	loaded, err := database.GetAgentSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ObservationProposedActions != 2 || loaded.ObservationExecutableActions != 0 ||
		loaded.ObservationViolations != 2 || loaded.ObservationStructureErrors != 1 {
		t.Fatalf("observation counters were not updated: %+v", loaded)
	}
}
