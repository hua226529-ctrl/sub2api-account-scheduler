package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestScheduledCommandPersistsBaselineBeforeReconciliation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	now := time.Now().UTC().Truncate(time.Second)
	command := model.ScheduledCommand{Capability: "pause_account", Arguments: json.RawMessage(`{"account_id":225}`),
		Conditions: json.RawMessage(`{}`), Status: model.AgentCommandStatusPending, ExecuteAt: now.Add(-time.Second),
		Timezone: model.AgentDefaultTimezone, IdempotencyKey: "reconciliation-baseline", CreatedBy: "agent:v2", MaxAttempts: 3}
	if err := database.CreateScheduledCommand(ctx, &command); err != nil {
		t.Fatal(err)
	}
	claimed, err := database.ClaimDueScheduledCommands(ctx, "worker-a", now, time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim failed: commands=%+v err=%v", claimed, err)
	}
	attemptedText := now.Format(time.RFC3339Nano)
	baseline := json.RawMessage(`{"attempted_at":"` + attemptedText + `","before_state":{"account_id":225,"schedulable":true}}`)
	if err := database.RecordScheduledCommandAttemptState(ctx, command.ID, "worker-a", baseline, now); err != nil {
		t.Fatal(err)
	}
	ambiguous := json.RawMessage(`{"attempted_at":"` + attemptedText + `","before_state":{"account_id":225,"schedulable":true},"execution":{"status":"failed","retryable":true}}`)
	if err := database.MarkScheduledCommandReconcilingWithResult(ctx, command.ID, "worker-a", "timeout", ambiguous); err != nil {
		t.Fatal(err)
	}
	stored, err := database.GetScheduledCommand(ctx, command.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.AgentCommandStatusReconciling || stored.MutationAttemptedAt == nil ||
		string(stored.Result) != string(ambiguous) {
		t.Fatalf("reconciliation evidence was not retained: %+v", stored)
	}
	readback := json.RawMessage(`{"attempted_at":"` + attemptedText + `","before_state":{"account_id":225,"schedulable":true},"assessment":{"verdict":"inconclusive"}}`)
	if err := database.TouchScheduledCommandReconciliation(ctx, command.ID, readback, "waiting for fresh snapshot"); err != nil {
		t.Fatal(err)
	}
	stored, err = database.GetScheduledCommand(ctx, command.ID)
	if err != nil || stored.Status != model.AgentCommandStatusReconciling || string(stored.Result) != string(readback) {
		t.Fatalf("inconclusive readback escaped quarantine: command=%+v err=%v", stored, err)
	}
	retryAt := now.Add(time.Minute)
	if err := database.ResolveScheduledCommandReconciliation(ctx, command.ID, model.AgentCommandStatusPending,
		json.RawMessage(`{"verdict":"not_applied"}`), "safe retry", &retryAt); err != nil {
		t.Fatal(err)
	}
	stored, err = database.GetScheduledCommand(ctx, command.ID)
	if err != nil || stored.Status != model.AgentCommandStatusPending || stored.MutationAttemptedAt != nil ||
		!stored.ExecuteAt.Equal(retryAt) {
		t.Fatalf("confirmed non-effect did not return to pending: command=%+v err=%v", stored, err)
	}
}

func TestListAgentStepsByStatusDoesNotReturnOtherWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	goal := model.AgentGoal{Title: "reconcile", Objective: "test", Status: model.AgentGoalStatusPlanned,
		Priority: 50, RiskLevel: model.AgentRiskLow, Source: "test", Context: json.RawMessage(`{}`), CreatedBy: "test"}
	if err := database.CreateAgentGoal(ctx, &goal); err != nil {
		t.Fatal(err)
	}
	for sequence, status := range []string{model.AgentStepStatusReconciling, model.AgentStepStatusCompleted} {
		step := model.AgentStep{GoalID: goal.ID, Sequence: sequence + 1, Capability: "pause_account",
			Arguments: json.RawMessage(`{"account_id":225}`), Preconditions: json.RawMessage(`{}`), Compensation: json.RawMessage(`{}`),
			Status: status, RiskLevel: model.AgentRiskMedium, IdempotencyKey: "step-" + status,
			BeforeState: json.RawMessage(`{}`), AfterState: json.RawMessage(`{}`), Result: json.RawMessage(`{}`)}
		if err := database.CreateAgentStep(ctx, &step); err != nil {
			t.Fatal(err)
		}
	}
	steps, err := database.ListAgentStepsByStatus(ctx, model.AgentStepStatusReconciling, 10)
	if err != nil || len(steps) != 1 || steps[0].Status != model.AgentStepStatusReconciling {
		t.Fatalf("status query returned unsafe work: steps=%+v err=%v", steps, err)
	}
}

func TestDeferLeasedScheduledCommandDoesNotConsumeAttempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	command := model.ScheduledCommand{Capability: "pause_account", Arguments: json.RawMessage(`{"account_id":225}`),
		Conditions: json.RawMessage(`{}`), Status: model.AgentCommandStatusPending, ExecuteAt: now.Add(-time.Second),
		Timezone: model.AgentDefaultTimezone, IdempotencyKey: "freeze-deferral", CreatedBy: "agent:v2", MaxAttempts: 3}
	if err := database.CreateScheduledCommand(ctx, &command); err != nil {
		t.Fatal(err)
	}
	claimed, err := database.ClaimDueScheduledCommands(ctx, "worker-a", now, time.Minute, 1)
	if err != nil || len(claimed) != 1 || claimed[0].AttemptCount != 1 {
		t.Fatalf("claim failed: commands=%+v err=%v", claimed, err)
	}
	retryAt := now.Add(time.Minute)
	if err := database.DeferLeasedScheduledCommand(ctx, command.ID, "worker-a", "frozen", retryAt); err != nil {
		t.Fatal(err)
	}
	stored, err := database.GetScheduledCommand(ctx, command.ID)
	if err != nil || stored.Status != model.AgentCommandStatusPending || stored.AttemptCount != 0 || !stored.ExecuteAt.Equal(retryAt) {
		t.Fatalf("freeze deferral consumed an execution attempt: command=%+v err=%v", stored, err)
	}
}
