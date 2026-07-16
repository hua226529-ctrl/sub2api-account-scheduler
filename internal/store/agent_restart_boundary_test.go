package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestRecoverySeparatesStepCreationFromMutationAttempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC().Truncate(time.Second)
	goal := model.AgentGoal{Title: "step boundary", Objective: "recover safely", Status: model.AgentGoalStatusRunning}
	if err := database.CreateAgentGoal(ctx, &goal); err != nil {
		t.Fatal(err)
	}
	beforeGate := model.AgentStep{GoalID: goal.ID, Sequence: 1, Capability: "pause_account",
		Arguments: json.RawMessage(`{"account_id":225}`), Status: model.AgentStepStatusRunning,
		IdempotencyKey: "step-before-gate", MaxAttempts: 1}
	afterGate := model.AgentStep{GoalID: goal.ID, Sequence: 2, Capability: "pause_account",
		Arguments: json.RawMessage(`{"account_id":298}`), Status: model.AgentStepStatusRunning,
		IdempotencyKey: "step-after-gate", MaxAttempts: 1}
	for _, step := range []*model.AgentStep{&beforeGate, &afterGate} {
		if err := database.CreateAgentStep(ctx, step); err != nil {
			t.Fatal(err)
		}
	}
	attemptedAt := now.Add(-time.Second)
	evidence := json.RawMessage(`{"attempted_at":"` + attemptedAt.Format(time.RFC3339Nano) + `"}`)
	if err := database.RecordAgentStepMutationAttempt(ctx, afterGate.ID,
		json.RawMessage(`{"account_id":298,"schedulable":true}`), evidence, attemptedAt); err != nil {
		t.Fatal(err)
	}
	if err := database.RecordAgentStepMutationAttempt(ctx, afterGate.ID,
		json.RawMessage(`{"account_id":298,"schedulable":true}`), evidence, attemptedAt); err == nil {
		t.Fatal("a step crossed the durable mutation gate twice")
	}

	summary, err := database.RecoverAgentV2State(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if summary.ReplannedSteps != 1 || summary.ReconcilingSteps != 1 {
		t.Fatalf("unexpected recovery summary: %+v", summary)
	}
	createdOnly, _ := database.GetAgentStep(ctx, beforeGate.ID)
	attempted, _ := database.GetAgentStep(ctx, afterGate.ID)
	if createdOnly.Status != model.AgentStepStatusFailed || createdOnly.MutationAttemptedAt != nil {
		t.Fatalf("pre-gate step was not safely replanned: %+v", createdOnly)
	}
	if attempted.Status != model.AgentStepStatusReconciling || attempted.MutationAttemptedAt == nil {
		t.Fatalf("post-gate step was not quarantined for readback: %+v", attempted)
	}
}

func TestRecoveryRequeuesPreGateCommandButQuarantinesExpiredAttempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC().Truncate(time.Second)
	expiresSoon := now.Add(time.Minute)
	expiresLater := now.Add(time.Hour)
	preGate := model.ScheduledCommand{Capability: "pause_account", Arguments: json.RawMessage(`{"account_id":225}`),
		ExecuteAt: now.Add(-time.Minute), ExpiresAt: &expiresLater, IdempotencyKey: "command-before-gate", MaxAttempts: 3}
	postGate := model.ScheduledCommand{Capability: "pause_account", Arguments: json.RawMessage(`{"account_id":298}`),
		ExecuteAt: now.Add(-time.Minute), ExpiresAt: &expiresSoon, IdempotencyKey: "command-after-gate", MaxAttempts: 3}
	for _, command := range []*model.ScheduledCommand{&preGate, &postGate} {
		if err := database.CreateScheduledCommand(ctx, command); err != nil {
			t.Fatal(err)
		}
	}
	claimed, err := database.ClaimDueScheduledCommands(ctx, "old-worker", now, 10*time.Minute, 10)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claim setup failed: commands=%+v err=%v", claimed, err)
	}
	attemptedAt := now.Add(time.Second)
	baseline := json.RawMessage(`{"attempted_at":"` + attemptedAt.Format(time.RFC3339Nano) +
		`","before_state":{"account_id":298,"schedulable":true}}`)
	if err := database.RecordScheduledCommandAttemptState(ctx, postGate.ID, "old-worker", baseline, attemptedAt); err != nil {
		t.Fatal(err)
	}
	if err := database.RecordScheduledCommandAttemptState(ctx, postGate.ID, "old-worker", baseline, attemptedAt); err == nil {
		t.Fatal("a scheduled command crossed the durable mutation gate twice")
	}

	restartAt := now.Add(2 * time.Minute)
	summary, err := database.RecoverAgentV2State(ctx, restartAt)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RequeuedCommands != 1 || summary.ReconcilingCommands != 1 || summary.ExpiredCommands != 0 {
		t.Fatalf("expired unknown mutation was incorrectly discarded: %+v", summary)
	}
	requeued, _ := database.GetScheduledCommand(ctx, preGate.ID)
	uncertain, _ := database.GetScheduledCommand(ctx, postGate.ID)
	if requeued.Status != model.AgentCommandStatusPending || requeued.AttemptCount != 0 || requeued.MutationAttemptedAt != nil {
		t.Fatalf("pre-gate command was not safely released: %+v", requeued)
	}
	if uncertain.Status != model.AgentCommandStatusReconciling || uncertain.MutationAttemptedAt == nil {
		t.Fatalf("expired post-gate command was not preserved for readback: %+v", uncertain)
	}
	claimed, err = database.ClaimDueScheduledCommands(ctx, "new-worker", restartAt, time.Minute, 10)
	if err != nil || len(claimed) != 1 || claimed[0].ID != preGate.ID || claimed[0].AttemptCount != 1 {
		t.Fatalf("recovery replayed unknown work or failed to reclaim safe work: commands=%+v err=%v", claimed, err)
	}
}

func TestLeaseExpiryUsesMutationBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC().Truncate(time.Second)
	commands := []model.ScheduledCommand{
		{Capability: "pause_account", Arguments: json.RawMessage(`{"account_id":225}`), ExecuteAt: now.Add(-time.Minute), IdempotencyKey: "lease-pre-gate"},
		{Capability: "pause_account", Arguments: json.RawMessage(`{"account_id":298}`), ExecuteAt: now.Add(-time.Minute), IdempotencyKey: "lease-post-gate"},
	}
	for index := range commands {
		if err := database.CreateScheduledCommand(ctx, &commands[index]); err != nil {
			t.Fatal(err)
		}
	}
	claimed, err := database.ClaimDueScheduledCommands(ctx, "old-worker", now, time.Second, 10)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claim setup failed: commands=%+v err=%v", claimed, err)
	}
	attemptedAt := now.Add(500 * time.Millisecond)
	if err := database.RecordScheduledCommandAttemptState(ctx, commands[1].ID, "old-worker",
		json.RawMessage(`{"attempted_at":"`+attemptedAt.Format(time.RFC3339Nano)+`","before_state":{"account_id":298}}`),
		attemptedAt); err != nil {
		t.Fatal(err)
	}
	claimed, err = database.ClaimDueScheduledCommands(ctx, "new-worker", now.Add(2*time.Second), time.Minute, 10)
	if err != nil || len(claimed) != 1 || claimed[0].ID != commands[0].ID {
		t.Fatalf("lease recovery did not respect mutation boundary: commands=%+v err=%v", claimed, err)
	}
	uncertain, _ := database.GetScheduledCommand(ctx, commands[1].ID)
	if uncertain.Status != model.AgentCommandStatusReconciling {
		t.Fatalf("post-gate expired lease was replayable: %+v", uncertain)
	}
}

func TestLegacyReconciliationWithoutBoundaryFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC().Truncate(time.Second)
	goal := model.AgentGoal{Title: "legacy uncertainty", Objective: "do not replay", Status: model.AgentGoalStatusRunning}
	if err := database.CreateAgentGoal(ctx, &goal); err != nil {
		t.Fatal(err)
	}
	step := model.AgentStep{GoalID: goal.ID, Sequence: 1, Capability: "pause_account",
		Arguments: json.RawMessage(`{"account_id":225}`), Status: model.AgentStepStatusReconciling,
		IdempotencyKey: "legacy-step-no-boundary", MaxAttempts: 1}
	if err := database.CreateAgentStep(ctx, &step); err != nil {
		t.Fatal(err)
	}
	command := model.ScheduledCommand{GoalID: &goal.ID, Capability: "pause_account",
		Arguments: json.RawMessage(`{"account_id":225}`), ExecuteAt: now.Add(-time.Minute),
		IdempotencyKey: "legacy-command-no-boundary", MaxAttempts: 3}
	if err := database.CreateScheduledCommand(ctx, &command); err != nil {
		t.Fatal(err)
	}
	claimed, err := database.ClaimDueScheduledCommands(ctx, "old-worker", now, time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim setup failed: commands=%+v err=%v", claimed, err)
	}
	if err := database.MarkScheduledCommandReconciling(ctx, command.ID, "old-worker", "legacy uncertain result"); err != nil {
		t.Fatal(err)
	}

	summary, err := database.RecoverAgentV2State(ctx, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if summary.FailedCommands != 1 {
		t.Fatalf("legacy unknown work was not failed closed: %+v", summary)
	}
	storedStep, _ := database.GetAgentStep(ctx, step.ID)
	storedCommand, _ := database.GetScheduledCommand(ctx, command.ID)
	storedGoal, _ := database.GetAgentGoal(ctx, goal.ID)
	if storedStep.Status != model.AgentStepStatusFailed || storedCommand.Status != model.AgentCommandStatusFailed ||
		storedGoal.Status != model.AgentGoalStatusFailed {
		t.Fatalf("legacy uncertainty can still loop or replay: step=%+v command=%+v goal=%+v", storedStep, storedCommand, storedGoal)
	}
}
