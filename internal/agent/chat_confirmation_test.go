package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type failingRandomReader struct{}

func (failingRandomReader) Read([]byte) (int, error) {
	return 0, errors.New("random source unavailable")
}

func TestChatConfirmationRandomFailureDoesNotPersistGoal(t *testing.T) {
	manager, database := stage0AgentManager(t)
	if err := manager.engine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.randomReader = failingRandomReader{}
	before, err := database.Store.ListAgentGoals(context.Background(), "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ChatAsync(context.Background(), 0, "暂停所有账号。"); err == nil {
		t.Fatal("high-risk chat succeeded after crypto/rand failure")
	}
	after, err := database.Store.ListAgentGoals(context.Background(), "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("random failure persisted a goal: before=%d after=%d", len(before), len(after))
	}
}

func TestHighRiskChatWaitsForExactSingleConfirmation(t *testing.T) {
	manager, _ := stage0AgentManager(t)
	ctx := context.Background()
	if err := manager.engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	receipt, err := manager.ChatAsync(ctx, 0, "暂停所有账号。")
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Status != model.AgentGoalStatusWaiting || receipt.ConfirmationToken == "" || receipt.ConfirmationExpiresAt == nil ||
		!receipt.Intent.RequiresConfirmation || receipt.Intent.Confirmed {
		t.Fatalf("confirmation preview = %+v", receipt)
	}
	if manager.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive) {
		t.Fatal("waiting high-risk goal was claimed before confirmation")
	}
	if _, err := manager.ConfirmChatGoal(ctx, receipt.GoalID, "wrong-token"); err == nil {
		t.Fatal("wrong confirmation token was accepted")
	}
	confirmed, err := manager.ConfirmChatGoal(ctx, receipt.GoalID, receipt.ConfirmationToken)
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.Status != model.AgentGoalStatusPlanned || !confirmed.Intent.Confirmed {
		t.Fatalf("confirmed receipt = %+v", confirmed)
	}
	if _, err := manager.ConfirmChatGoal(ctx, receipt.GoalID, receipt.ConfirmationToken); err == nil {
		t.Fatal("confirmation token was reusable")
	}
}

func TestReadOnlyChatIntentBlocksModelMutation(t *testing.T) {
	intent := ChatIntent{IntentType: ChatIntentAnalysis, ResourceType: ChatResourceSystem, Operation: "analyze",
		ReadOnly: true, RiskLevel: model.AgentRiskReadOnly}
	if err := enforceChatIntentCapability(intent, "pause_account", []byte(`{"account_id":1,"reason":"unsafe"}`), true); err == nil {
		t.Fatal("read-only chat intent allowed a mutation capability")
	}
}
