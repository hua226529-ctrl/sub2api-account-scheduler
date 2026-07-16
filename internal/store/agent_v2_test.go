package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestAgentV2GoalStepEventCheckpointAndMemory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	goal := model.AgentGoal{Title: "夜间调度", Objective: "降低负载并在早上恢复", Status: model.AgentGoalStatusRunning,
		Priority: 80, RiskLevel: model.AgentRiskMedium, Source: "chat", Context: json.RawMessage(`{"packet_id":9}`),
		PlanHash: "plan-9", CreatedBy: "operator", DeadlineAt: &deadline}
	if err := database.CreateAgentGoal(ctx, &goal); err != nil {
		t.Fatal(err)
	}
	step := model.AgentStep{GoalID: goal.ID, Sequence: 1, Capability: "set_load_pin",
		Arguments: json.RawMessage(`{"account_id":225,"value":25}`), Preconditions: json.RawMessage(`{"fresh":true}`),
		Compensation: json.RawMessage(`{"capability":"clear_load_pin"}`), Status: model.AgentStepStatusPending,
		RiskLevel: model.AgentRiskLow, IdempotencyKey: "goal-1-step-1", MaxAttempts: 2}
	if err := database.CreateAgentStep(ctx, &step); err != nil {
		t.Fatal(err)
	}
	loadedGoal, err := database.GetAgentGoal(ctx, goal.ID)
	if err != nil {
		t.Fatal(err)
	}
	steps, err := database.ListAgentSteps(ctx, goal.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loadedGoal.PlanHash != "plan-9" || loadedGoal.DeadlineAt == nil || !loadedGoal.DeadlineAt.Equal(deadline) ||
		len(steps) != 1 || steps[0].Capability != "set_load_pin" || steps[0].MaxAttempts != 2 {
		t.Fatalf("v2 goal or step was not preserved: goal=%+v steps=%+v", loadedGoal, steps)
	}

	event := model.AgentEvent{EventKey: "goal-1-created", GoalID: &goal.ID, StepID: &step.ID, Type: "step_planned",
		Severity: "info", Actor: "agent", Payload: json.RawMessage(`{"sequence":1}`)}
	created, err := database.AppendAgentEvent(ctx, &event)
	if err != nil || !created {
		t.Fatalf("append event: created=%v err=%v", created, err)
	}
	duplicate := model.AgentEvent{EventKey: event.EventKey, Type: "must_not_replace", Payload: json.RawMessage(`{"changed":true}`)}
	created, err = database.AppendAgentEvent(ctx, &duplicate)
	if err != nil || created || duplicate.ID != event.ID || duplicate.Type != "step_planned" {
		t.Fatalf("event deduplication failed: created=%v event=%+v err=%v", created, duplicate, err)
	}
	events, err := database.ListAgentEvents(ctx, goal.ID, 0, 20)
	if err != nil || len(events) != 1 || events[0].EventKey != event.EventKey {
		t.Fatalf("event list failed: events=%+v err=%v", events, err)
	}

	checkpoint := model.AgentCheckpoint{GoalID: goal.ID, StepID: &step.ID, Kind: "plan",
		State: json.RawMessage(`{"next_step":1}`), StateHash: "checkpoint-1"}
	if err := database.SaveAgentCheckpoint(ctx, &checkpoint); err != nil {
		t.Fatal(err)
	}
	latest, err := database.LatestAgentCheckpoint(ctx, goal.ID)
	if err != nil || latest.StateHash != checkpoint.StateHash || latest.StepID == nil || *latest.StepID != step.ID {
		t.Fatalf("checkpoint round trip failed: checkpoint=%+v err=%v", latest, err)
	}

	memory := model.AgentMemory{ScopeType: "account", ScopeID: "225", Kind: model.AgentMemoryDecision,
		Key: "night-load", Summary: "夜间保持低负载", Content: json.RawMessage(`{"load":25}`), Importance: .9, Pinned: true}
	if err := database.UpsertAgentMemory(ctx, &memory); err != nil {
		t.Fatal(err)
	}
	memory.Summary = "夜间保持25负载"
	if err := database.UpsertAgentMemory(ctx, &memory); err != nil {
		t.Fatal(err)
	}
	memories, err := database.ListAgentMemories(ctx, "account", "225", 10)
	if err != nil || len(memories) != 1 || memories[0].Summary != memory.Summary || !memories[0].Pinned {
		t.Fatalf("memory upsert failed: memories=%+v err=%v", memories, err)
	}
}

func TestScheduledCommandLeaseRetryAndCompletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC().Truncate(time.Second)
	command := model.ScheduledCommand{Capability: "conditional_resume", Arguments: json.RawMessage(`{"account_id":298}`),
		Conditions: json.RawMessage(`{"data_fresh":true}`), ExecuteAt: now.Add(-time.Second),
		IdempotencyKey: "resume-298-at-6", MaxAttempts: 2, CreatedBy: "agent"}
	if err := database.CreateScheduledCommand(ctx, &command); err != nil {
		t.Fatal(err)
	}
	duplicate := command
	duplicate.ID = 0
	if err := database.CreateScheduledCommand(ctx, &duplicate); err != nil || duplicate.ID != command.ID {
		t.Fatalf("command idempotency failed: duplicate=%+v err=%v", duplicate, err)
	}
	claimed, err := database.ClaimDueScheduledCommands(ctx, "worker-a", now, time.Minute, 10)
	if err != nil || len(claimed) != 1 || claimed[0].AttemptCount != 1 || claimed[0].LeaseOwner != "worker-a" {
		t.Fatalf("claim failed: commands=%+v err=%v", claimed, err)
	}
	claimedAgain, err := database.ClaimDueScheduledCommands(ctx, "worker-b", now, time.Minute, 10)
	if err != nil || len(claimedAgain) != 0 {
		t.Fatalf("command was claimed twice: commands=%+v err=%v", claimedAgain, err)
	}
	if err := database.RenewScheduledCommandLease(ctx, command.ID, "worker-b", now, time.Minute); err == nil {
		t.Fatal("another worker renewed a lease")
	}
	retryAt := now.Add(2 * time.Minute)
	status, err := database.FailScheduledCommand(ctx, command.ID, "worker-a", "confirmed no write", &retryAt)
	if err != nil || status != model.AgentCommandStatusPending {
		t.Fatalf("release for retry failed: status=%s err=%v", status, err)
	}
	claimed, err = database.ClaimDueScheduledCommands(ctx, "worker-b", retryAt, time.Minute, 10)
	if err != nil || len(claimed) != 1 || claimed[0].AttemptCount != 2 {
		t.Fatalf("retry claim failed: commands=%+v err=%v", claimed, err)
	}
	if err := database.CompleteScheduledCommand(ctx, command.ID, "worker-b", json.RawMessage(`{"confirmed":true}`), retryAt); err != nil {
		t.Fatal(err)
	}
	completed, err := database.GetScheduledCommand(ctx, command.ID)
	if err != nil || completed.Status != model.AgentCommandStatusCompleted || completed.CompletedAt == nil {
		t.Fatalf("command completion failed: command=%+v err=%v", completed, err)
	}
}

func TestAgentV2RecoveryIsConservative(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC().Truncate(time.Second)
	goal := model.AgentGoal{Title: "恢复测试", Objective: "核对重启状态", Status: model.AgentGoalStatusRunning,
		Priority: 50, RiskLevel: model.AgentRiskMedium}
	if err := database.CreateAgentGoal(ctx, &goal); err != nil {
		t.Fatal(err)
	}
	step := model.AgentStep{GoalID: goal.ID, Sequence: 1, Capability: "pause_account", Status: model.AgentStepStatusRunning,
		IdempotencyKey: "recovery-step", MaxAttempts: 1}
	if err := database.CreateAgentStep(ctx, &step); err != nil {
		t.Fatal(err)
	}
	attemptedAt := now.Add(-30 * time.Second)
	if err := database.RecordAgentStepMutationAttempt(ctx, step.ID,
		json.RawMessage(`{"account_id":225,"schedulable":true}`),
		json.RawMessage(`{"attempted_at":"`+attemptedAt.Format(time.RFC3339Nano)+`"}`), attemptedAt); err != nil {
		t.Fatal(err)
	}
	leased := model.ScheduledCommand{GoalID: &goal.ID, StepID: &step.ID, Capability: "pause_account",
		Arguments: json.RawMessage(`{"account_id":225}`), ExecuteAt: now.Add(-time.Minute), IdempotencyKey: "recovery-command"}
	if err := database.CreateScheduledCommand(ctx, &leased); err != nil {
		t.Fatal(err)
	}
	claimed, err := database.ClaimDueScheduledCommands(ctx, "old-worker", now, time.Hour, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("pre-restart claim failed: commands=%+v err=%v", claimed, err)
	}
	if err := database.RecordScheduledCommandAttemptState(ctx, leased.ID, "old-worker",
		json.RawMessage(`{"attempted_at":"`+attemptedAt.Format(time.RFC3339Nano)+`","before_state":{"account_id":225,"schedulable":true}}`),
		attemptedAt); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(-time.Second)
	expired := model.ScheduledCommand{Capability: "trigger_reconcile", ExecuteAt: now.Add(-time.Minute), ExpiresAt: &expires,
		IdempotencyKey: "expired-command"}
	if err := database.CreateScheduledCommand(ctx, &expired); err != nil {
		t.Fatal(err)
	}
	summary, err := database.RecoverAgentV2State(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if summary.ReconcilingCommands != 1 || summary.ReconcilingSteps != 1 || summary.ExpiredCommands != 1 {
		t.Fatalf("unexpected recovery summary: %+v", summary)
	}
	recoveredCommand, _ := database.GetScheduledCommand(ctx, leased.ID)
	recoveredStep, _ := database.GetAgentStep(ctx, step.ID)
	recoveredGoal, _ := database.GetAgentGoal(ctx, goal.ID)
	expiredCommand, _ := database.GetScheduledCommand(ctx, expired.ID)
	if recoveredCommand.Status != model.AgentCommandStatusReconciling || recoveredStep.Status != model.AgentStepStatusReconciling ||
		recoveredGoal.Status != model.AgentGoalStatusPlanned || expiredCommand.Status != model.AgentCommandStatusExpired {
		t.Fatalf("unsafe recovery state: goal=%+v command=%+v step=%+v expired=%+v", recoveredGoal, recoveredCommand, recoveredStep, expiredCommand)
	}
	claimed, err = database.ClaimDueScheduledCommands(ctx, "new-worker", now.Add(time.Hour), time.Minute, 10)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("reconciling command was replayed: commands=%+v err=%v", claimed, err)
	}
	if err := database.ResolveScheduledCommandReconciliation(ctx, leased.ID, model.AgentCommandStatusCompleted,
		json.RawMessage(`{"read_back":"paused"}`), "上游状态确认动作已经成功", nil); err != nil {
		t.Fatal(err)
	}
	recoveredCommand, _ = database.GetScheduledCommand(ctx, leased.ID)
	if recoveredCommand.Status != model.AgentCommandStatusCompleted {
		t.Fatalf("reconciliation result was not persisted: %+v", recoveredCommand)
	}
}

func TestAgentV2RecoveryFailsClosedUnobservableWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC().Truncate(time.Second)
	localGoal := model.AgentGoal{Title: "本地动作恢复", Objective: "重新规划不可核对动作",
		Status: model.AgentGoalStatusWaiting, Priority: 60, RiskLevel: model.AgentRiskMedium}
	if err := database.CreateAgentGoal(ctx, &localGoal); err != nil {
		t.Fatal(err)
	}
	externalGoal := model.AgentGoal{Title: "外部动作恢复", Objective: "只读核对外部动作",
		Status: model.AgentGoalStatusRunning, Priority: 70, RiskLevel: model.AgentRiskHigh}
	if err := database.CreateAgentGoal(ctx, &externalGoal); err != nil {
		t.Fatal(err)
	}

	localCapabilities := []string{"update_dispatch_policy", "activate_policy_version", "schedule_command",
		"cancel_scheduled_command", "refresh_upstream", "trigger_reconcile"}
	activeStatuses := []string{model.AgentStepStatusRunning, model.AgentStepStatusVerifying,
		model.AgentStepStatusCompensating, model.AgentStepStatusReconciling}
	localSteps := make([]model.AgentStep, 0, len(localCapabilities))
	localCommands := make([]model.ScheduledCommand, 0, len(localCapabilities))
	for index, capability := range localCapabilities {
		step := model.AgentStep{GoalID: localGoal.ID, Sequence: index + 1, Capability: capability,
			Arguments: json.RawMessage(`{}`), Status: model.AgentStepStatusRunning,
			RiskLevel: model.AgentRiskHigh, IdempotencyKey: "local-step-" + capability, MaxAttempts: 1}
		if err := database.CreateAgentStep(ctx, &step); err != nil {
			t.Fatal(err)
		}
		attemptedAt := now.Add(-30 * time.Second)
		if err := database.RecordAgentStepMutationAttempt(ctx, step.ID, json.RawMessage(`{"state":"before"}`),
			json.RawMessage(`{"attempted_at":"`+attemptedAt.Format(time.RFC3339Nano)+`"}`), attemptedAt); err != nil {
			t.Fatal(err)
		}
		step, err = database.GetAgentStep(ctx, step.ID)
		if err != nil {
			t.Fatal(err)
		}
		step.Status = activeStatuses[index%len(activeStatuses)]
		if err := database.UpdateAgentStep(ctx, step); err != nil {
			t.Fatal(err)
		}
		localSteps = append(localSteps, step)
		command := model.ScheduledCommand{GoalID: &localGoal.ID, Capability: capability, Arguments: json.RawMessage(`{}`),
			ExecuteAt: now.Add(-time.Minute), IdempotencyKey: "local-command-" + capability, MaxAttempts: 3}
		if err := database.CreateScheduledCommand(ctx, &command); err != nil {
			t.Fatal(err)
		}
		localCommands = append(localCommands, command)
	}
	externalStep := model.AgentStep{GoalID: externalGoal.ID, Sequence: 1, Capability: "pause_account",
		Arguments: json.RawMessage(`{"account_id":225}`), Status: model.AgentStepStatusRunning,
		RiskLevel: model.AgentRiskHigh, IdempotencyKey: "external-step-pause", MaxAttempts: 1}
	if err := database.CreateAgentStep(ctx, &externalStep); err != nil {
		t.Fatal(err)
	}
	externalAttemptedAt := now.Add(-20 * time.Second)
	if err := database.RecordAgentStepMutationAttempt(ctx, externalStep.ID,
		json.RawMessage(`{"account_id":225,"schedulable":true}`),
		json.RawMessage(`{"attempted_at":"`+externalAttemptedAt.Format(time.RFC3339Nano)+`"}`), externalAttemptedAt); err != nil {
		t.Fatal(err)
	}
	externalCommand := model.ScheduledCommand{GoalID: &externalGoal.ID, Capability: "pause_account",
		Arguments: json.RawMessage(`{"account_id":225}`), ExecuteAt: now.Add(-time.Minute),
		IdempotencyKey: "external-command-pause", MaxAttempts: 3}
	if err := database.CreateScheduledCommand(ctx, &externalCommand); err != nil {
		t.Fatal(err)
	}
	claimed, err := database.ClaimDueScheduledCommands(ctx, "old-worker", now, time.Hour, 20)
	if err != nil || len(claimed) != len(localCommands)+1 {
		t.Fatalf("pre-restart claim failed: commands=%d err=%v", len(claimed), err)
	}
	for _, command := range claimed {
		attemptedAt := now.Add(-15 * time.Second)
		if err := database.RecordScheduledCommandAttemptState(ctx, command.ID, "old-worker",
			json.RawMessage(`{"attempted_at":"`+attemptedAt.Format(time.RFC3339Nano)+`","before_state":{"state":"before"}}`),
			attemptedAt); err != nil {
			t.Fatal(err)
		}
	}

	summary, err := database.RecoverAgentV2State(ctx, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if summary.FailedCommands != len(localCommands) || summary.ReconcilingCommands != 1 || summary.ReconcilingSteps != 1 {
		t.Fatalf("unexpected recovery summary: %+v", summary)
	}
	for _, original := range localSteps {
		step, getErr := database.GetAgentStep(ctx, original.ID)
		if getErr != nil || step.Status != model.AgentStepStatusFailed || step.CompletedAt == nil {
			t.Fatalf("unobservable step was not failed closed: step=%+v err=%v", step, getErr)
		}
	}
	for _, original := range localCommands {
		command, getErr := database.GetScheduledCommand(ctx, original.ID)
		if getErr != nil || command.Status != model.AgentCommandStatusFailed || command.CompletedAt == nil {
			t.Fatalf("unobservable command was not failed closed: command=%+v err=%v", command, getErr)
		}
	}
	recoveredLocalGoal, _ := database.GetAgentGoal(ctx, localGoal.ID)
	recoveredExternalGoal, _ := database.GetAgentGoal(ctx, externalGoal.ID)
	recoveredExternalStep, _ := database.GetAgentStep(ctx, externalStep.ID)
	recoveredExternalCommand, _ := database.GetScheduledCommand(ctx, externalCommand.ID)
	if recoveredLocalGoal.Status != model.AgentGoalStatusPlanned || recoveredExternalGoal.Status != model.AgentGoalStatusPlanned ||
		recoveredExternalStep.Status != model.AgentStepStatusReconciling ||
		recoveredExternalCommand.Status != model.AgentCommandStatusReconciling {
		t.Fatalf("restart recovery did not separate replan/readback work: local_goal=%+v external_goal=%+v step=%+v command=%+v",
			recoveredLocalGoal, recoveredExternalGoal, recoveredExternalStep, recoveredExternalCommand)
	}
	claimed, err = database.ClaimDueScheduledCommands(ctx, "new-worker", now.Add(2*time.Hour), time.Minute, 20)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("failed/reconciling commands were replayed: commands=%+v err=%v", claimed, err)
	}
}

func TestCapabilitySupportsRestartReadbackIsFailClosed(t *testing.T) {
	t.Parallel()
	for _, capability := range []string{"pause_account", "resume_account", "set_load_factor", "pin_load_until",
		"clear_load_pin", "clear_flap_protection", "clear_manual_override", "update_binding",
		"update_upstream_control", "transition_token_group_tier"} {
		if !CapabilitySupportsRestartReadback(capability) {
			t.Errorf("observable external capability %q was not quarantined for readback", capability)
		}
	}
	for _, capability := range []string{"update_dispatch_policy", "activate_policy_version", "schedule_command",
		"cancel_scheduled_command", "refresh_upstream", "trigger_reconcile", "future_unknown_mutation"} {
		if CapabilitySupportsRestartReadback(capability) {
			t.Errorf("unobservable capability %q was incorrectly made replayable", capability)
		}
	}
}

func TestAgentV2FreezeAndUnifiedRetention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	global, err := database.GetAgentFreezeState(ctx, "global", "")
	if err != nil || global.Mode != model.AgentFreezeModeActive {
		t.Fatalf("unexpected default freeze state: state=%+v err=%v", global, err)
	}
	global.Mode, global.Reason, global.Actor = model.AgentFreezeModeWritesFrozen, "emergency", "operator"
	if err := database.SetAgentFreezeState(ctx, &global); err != nil {
		t.Fatal(err)
	}
	accountFreeze := model.AgentFreezeState{ScopeType: "account", ScopeID: "225", Mode: model.AgentFreezeModeReadOnly,
		Reason: "investigation", Actor: "operator"}
	if err := database.SetAgentFreezeState(ctx, &accountFreeze); err != nil {
		t.Fatal(err)
	}
	states, err := database.ListAgentFreezeStates(ctx)
	if err != nil || len(states) != 2 || states[0].Mode != model.AgentFreezeModeWritesFrozen {
		t.Fatalf("freeze state round trip failed: states=%+v err=%v", states, err)
	}

	memory := model.AgentMemory{ScopeType: "global", Kind: model.AgentMemorySemantic, Key: "old-pinned",
		Summary: "must expire", Pinned: true, Content: json.RawMessage(`{"old":true}`)}
	if err := database.UpsertAgentMemory(ctx, &memory); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-91 * 24 * time.Hour)
	if _, err := database.db.ExecContext(ctx, `UPDATE agent_memories SET updated_at=? WHERE id=?`, formatTime(old), memory.ID); err != nil {
		t.Fatal(err)
	}
	if err := database.CleanupAgentV2Data(ctx, time.Now().UTC().Add(-90*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	_, err = database.GetAgentMemory(ctx, memory.ScopeType, memory.ScopeID, memory.Kind, memory.Key)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("pinned memory bypassed unified retention: %v", err)
	}
	stillFrozen, err := database.GetAgentFreezeState(ctx, "global", "")
	if err != nil || stillFrozen.Mode != model.AgentFreezeModeWritesFrozen {
		t.Fatalf("cleanup removed live freeze state: state=%+v err=%v", stillFrozen, err)
	}
}
