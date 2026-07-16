package agent

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/balance"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func stage0AgentManager(t *testing.T) (*Manager, *testsupport.TempDatabase) {
	t.Helper()
	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: 1, Monitors: 1})
	api := testsupport.NewFakeSub2API(fixture)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := reconcile.NewEngine(api, database.Store, 50*time.Second, logger)
	balances := balance.NewManager(database.Store, api, engine, nil, nil, time.Hour, logger)
	return NewManager(database.Store, engine, balances, nil, logger), database
}

func TestCharacterizationChatAsyncPersistsPlannedGoalWithoutModelCall(t *testing.T) {
	manager, database := stage0AgentManager(t)
	ctx := context.Background()
	conversationID, goalID, runID, status, err := manager.ChatAsync(ctx, 0, "分析当前账号状态")
	if err != nil {
		t.Fatal(err)
	}
	if conversationID <= 0 || goalID <= 0 || runID != 0 || status != model.AgentGoalStatusPlanned {
		t.Fatalf("ChatAsync result = conversation:%d goal:%d run:%d status:%q", conversationID, goalID, runID, status)
	}
	goal, err := database.Store.GetAgentGoal(ctx, goalID)
	if err != nil {
		t.Fatal(err)
	}
	if goal.Status != model.AgentGoalStatusPlanned || goal.Source != "administrator" || goal.ConversationID == nil || *goal.ConversationID != conversationID {
		t.Fatalf("persisted goal changed: %+v", goal)
	}
	messages, err := database.Store.ListAgentMessages(ctx, conversationID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Role != "user" || messages[0].Content != "分析当前账号状态" {
		t.Fatalf("persisted messages = %+v", messages)
	}
}

func TestCurrentBehaviorInteractiveGoalWaitsForOccupiedRuntimeMutex(t *testing.T) {
	manager, database := stage0AgentManager(t)
	ctx := context.Background()
	_, goalID, _, _, err := manager.ChatAsync(ctx, 0, "分析当前账号状态")
	if err != nil {
		t.Fatal(err)
	}
	manager.runtimeMu.Lock()
	done := make(chan bool, 1)
	go func() { done <- manager.processNextRuntimeGoal(ctx) }()
	select {
	case <-done:
		manager.runtimeMu.Unlock()
		t.Fatal("interactive goal bypassed the occupied runtime mutex")
	case <-time.After(20 * time.Millisecond):
	}
	goal, err := database.Store.GetAgentGoal(ctx, goalID)
	if err != nil {
		manager.runtimeMu.Unlock()
		t.Fatal(err)
	}
	if goal.Status != model.AgentGoalStatusPlanned {
		manager.runtimeMu.Unlock()
		t.Fatalf("goal advanced while runtime mutex was occupied: %+v", goal)
	}
	manager.runtimeMu.Unlock()
	select {
	case processed := <-done:
		if !processed {
			t.Fatal("worker did not select the queued interactive goal")
		}
	case <-time.After(time.Second):
		t.Fatal("interactive goal did not resume after runtime mutex release")
	}
	goal, err = database.Store.GetAgentGoal(ctx, goalID)
	if err != nil {
		t.Fatal(err)
	}
	if goal.Status != model.AgentGoalStatusWaiting {
		t.Fatalf("goal status after missing model provider = %q, want waiting", goal.Status)
	}
}

func TestCharacterizationMissingModelProviderDoesNotStopDeterministicReconcile(t *testing.T) {
	manager, _ := stage0AgentManager(t)
	ctx := context.Background()
	if _, _, _, _, err := manager.ChatAsync(ctx, 0, "分析当前账号状态"); err != nil {
		t.Fatal(err)
	}
	if !manager.processNextRuntimeGoal(ctx) {
		t.Fatal("agent worker did not process the goal without a model provider")
	}
	if err := manager.engine.Reconcile(ctx); err != nil {
		t.Fatalf("deterministic reconcile depended on model availability: %v", err)
	}
	if manager.engine.Snapshot().LastSyncAt == nil {
		t.Fatal("deterministic reconcile did not publish a snapshot")
	}
}
