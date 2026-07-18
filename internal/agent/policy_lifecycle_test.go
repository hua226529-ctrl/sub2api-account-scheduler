package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/store"
)

func TestPolicyProposalSimulatesWithoutActivationAndIsIdempotent(t *testing.T) {
	manager, database := stage0AgentManager(t)
	ctx := context.Background()
	if err := manager.engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	accountID := manager.engine.Snapshot().Bindings[0].Account.ID
	insertPolicySimulationSamples(t, database.Store, accountID, 30)
	input := PolicyProposalInput{ScopeType: "account", ScopeID: fmt.Sprint(accountID),
		Patch: []byte(`{"failure_threshold":4}`), Reason: "reduce flapping", Actor: "agent:optimizer",
		GoalID: 9, StepID: 10, PacketID: 11, PacketHash: "packet-hash", IdempotencyKey: "proposal-idempotent-1"}
	proposal, err := manager.ProposeDispatchPolicy(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if proposal.Status != model.PolicyStatusSimulated || proposal.ActivatedAt != nil || proposal.Simulation.SampleCount < 20 ||
		!proposal.Simulation.Passed || proposal.RiskLevel != model.AgentRiskLow || len(proposal.Diff) == 0 {
		t.Fatalf("proposal lifecycle = %+v", proposal)
	}
	if _, err := database.Store.FindActivePolicyVersion(ctx, "account", fmt.Sprint(accountID)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("proposal was activated during creation: %v", err)
	}
	events, err := database.Store.ListEvents(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	var proposalAudit *model.Event
	for index := range events {
		if events[index].Type == "dispatch_policy_proposed" {
			proposalAudit = &events[index]
			break
		}
	}
	if proposalAudit == nil || proposalAudit.GoalID != 9 || proposalAudit.StepID != 10 {
		t.Fatalf("V2 policy audit provenance missing: %+v", proposalAudit)
	}
	replay, err := manager.ProposeDispatchPolicy(ctx, input)
	if err != nil || replay.ID != proposal.ID {
		t.Fatalf("proposal idempotent replay = %+v err=%v", replay, err)
	}
	input.Patch = []byte(`{"failure_threshold":5}`)
	if _, err := manager.ProposeDispatchPolicy(ctx, input); !errors.Is(err, store.ErrPolicyIdempotencyConflict) {
		t.Fatalf("same key with changed patch did not conflict: %v", err)
	}
}

func TestPolicyActivationChecksBaseAndAutoBudget(t *testing.T) {
	manager, database := stage0AgentManager(t)
	ctx := context.Background()
	if err := manager.engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	accountID := manager.engine.Snapshot().Bindings[0].Account.ID
	insertPolicySimulationSamples(t, database.Store, accountID, 30)
	settings, err := database.Store.GetAgentSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.Enabled, settings.Mode = true, model.AgentModeControl
	settings.OptimizerMode, settings.OperatorMode = model.AgentOptimizerAuto, model.AgentOperatorConfirm
	settings.DailyPolicyChangeBudget = 1
	if err := database.Store.UpdateAgentSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	first, err := manager.ProposeDispatchPolicy(ctx, PolicyProposalInput{ScopeType: "account", ScopeID: fmt.Sprint(accountID),
		Patch: []byte(`{"failure_threshold":4}`), Reason: "first", Actor: "agent:optimizer", GoalID: 101, StepID: 201,
		PacketID: 301, PacketHash: "first-packet", IdempotencyKey: "proposal-first"})
	if err != nil {
		t.Fatal(err)
	}
	stale, err := manager.ProposeDispatchPolicy(ctx, PolicyProposalInput{ScopeType: "account", ScopeID: fmt.Sprint(accountID),
		Patch: []byte(`{"failure_threshold":5}`), Reason: "stale", Actor: "agent:optimizer", IdempotencyKey: "proposal-stale"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ActivatePolicyProposal(ctx, first.ID, "", true); err != nil {
		t.Fatal(err)
	}
	activated, err := database.Store.GetPolicyLifecycle(ctx, first.ID)
	if err != nil || activated.Status != model.PolicyStatusActive {
		t.Fatalf("activated proposal = %+v err=%v", activated, err)
	}
	events, err := database.Store.ListEvents(ctx, 30)
	if err != nil {
		t.Fatal(err)
	}
	var activationAudit *model.Event
	for index := range events {
		if events[index].Type == "dispatch_policy_activated" {
			activationAudit = &events[index]
			break
		}
	}
	if activationAudit == nil || activationAudit.GoalID != 101 || activationAudit.StepID != 201 {
		t.Fatalf("V2 policy activation audit provenance missing: %+v", activationAudit)
	}
	if err := manager.ActivatePolicyProposal(ctx, stale.ID, "administrator", false); !errors.Is(err, store.ErrPolicyBaseChanged) {
		t.Fatalf("stale proposal activation = %v", err)
	}
	stale, _ = database.Store.GetPolicyLifecycle(ctx, stale.ID)
	if stale.Status != model.PolicyStatusSuperseded {
		t.Fatalf("stale proposal status = %q", stale.Status)
	}
	second, err := manager.ProposeDispatchPolicy(ctx, PolicyProposalInput{ScopeType: "account", ScopeID: fmt.Sprint(accountID),
		Patch: []byte(`{"failure_threshold":6}`), Reason: "budget", Actor: "agent:optimizer", IdempotencyKey: "proposal-budget"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ActivatePolicyProposal(ctx, second.ID, "", true); err == nil || err.Error() != "策略自动发布已达到每日变更预算" {
		t.Fatalf("daily activation budget was not enforced: %v", err)
	}
	if err := manager.ActivatePolicyProposal(ctx, second.ID, "administrator", false); err != nil {
		t.Fatal(err)
	}
	if err := manager.RollbackPolicy(ctx, second.ID, "administrator", "manual rollback", false); err != nil {
		t.Fatal(err)
	}
	first, _ = database.Store.GetPolicyLifecycle(ctx, first.ID)
	second, _ = database.Store.GetPolicyLifecycle(ctx, second.ID)
	if first.Status != model.PolicyStatusActive || second.Status != model.PolicyStatusRolledBack {
		t.Fatalf("rollback lifecycle first=%s second=%s", first.Status, second.Status)
	}
	rejected, err := manager.ProposeDispatchPolicy(ctx, PolicyProposalInput{ScopeType: "account", ScopeID: fmt.Sprint(accountID),
		Patch: []byte(`{"failure_threshold":7}`), Reason: "reject", Actor: "agent:optimizer", IdempotencyKey: "proposal-reject"})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RejectPolicy(ctx, rejected.ID, "administrator", "not needed"); err != nil {
		t.Fatal(err)
	}
	rejected, _ = database.Store.GetPolicyLifecycle(ctx, rejected.ID)
	if rejected.Status != model.PolicyStatusRejected || rejected.OutcomeSummary != "not needed" {
		t.Fatalf("rejected proposal = %+v", rejected)
	}
}

func TestPolicyActivationCapabilityRejectsAutonomousExecution(t *testing.T) {
	manager, _ := stage0AgentManager(t)
	invocation := CapabilityInvocation{
		Name:      "activate_policy_version",
		Arguments: []byte(`{"policy_id":1,"reason":"model requested activation"}`),
		Actor:     "agent:v2",
		DryRun:    true,
	}
	result, err := manager.ExecuteCapability(context.Background(), invocation)
	if err == nil || (!strings.Contains(err.Error(), "自主执行") && !strings.Contains(err.Error(), "管理员确认")) {
		t.Fatalf("autonomous policy activation result=%+v err=%v", result, err)
	}
}

func insertPolicySimulationSamples(t *testing.T, database interface {
	InsertTrafficSuccesses(context.Context, []model.TrafficSuccess) (int, error)
}, accountID int64, count int) {
	t.Helper()
	now := time.Now().UTC()
	items := make([]model.TrafficSuccess, count)
	for index := range items {
		items[index] = model.TrafficSuccess{EventKey: fmt.Sprintf("policy-sample-%d-%d", accountID, index), AccountID: accountID,
			Model: "fake", DurationMS: 20, CreatedAt: now.Add(-time.Duration(index) * time.Minute)}
	}
	if _, err := database.InsertTrafficSuccesses(context.Background(), items); err != nil {
		t.Fatal(err)
	}
}
