package agent

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
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
	manager.interactiveWake <- struct{}{}
	started := time.Now()
	receipt, err := manager.ChatAsync(ctx, 0, "分析最近整体调度效果。")
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ConversationID <= 0 || receipt.GoalID <= 0 || receipt.RunID != 0 || receipt.Status != model.AgentGoalStatusPlanned {
		t.Fatalf("ChatAsync result = %+v", receipt)
	}
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("ChatAsync blocked instead of returning after durable enqueue: %v", elapsed)
	} else {
		t.Logf("ChatAsync durable return: %v", elapsed)
	}
	goal, err := database.Store.GetAgentGoal(ctx, receipt.GoalID)
	if err != nil {
		t.Fatal(err)
	}
	if goal.Status != model.AgentGoalStatusPlanned || goal.Lane != model.AgentLaneInteractive || goal.Source != "administrator" || goal.ConversationID == nil || *goal.ConversationID != receipt.ConversationID {
		t.Fatalf("persisted goal changed: %+v", goal)
	}
	messages, err := database.Store.ListAgentMessages(ctx, receipt.ConversationID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Role != "user" || messages[0].Content != "分析最近整体调度效果。" {
		t.Fatalf("persisted messages = %+v", messages)
	}
}

func TestInteractiveGoalDoesNotWaitForBackgroundLane(t *testing.T) {
	manager, database := stage0AgentManager(t)
	ctx := context.Background()
	receipt, err := manager.ChatAsync(ctx, 0, "分析最近整体调度效果。")
	if err != nil {
		t.Fatal(err)
	}
	if !manager.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive) {
		t.Fatal("interactive worker did not select the queued goal")
	}
	goal, err := database.Store.GetAgentGoal(ctx, receipt.GoalID)
	if err != nil {
		t.Fatal(err)
	}
	if goal.Status != model.AgentGoalStatusPlanned {
		t.Fatalf("goal status after missing model provider = %q, want planned retry", goal.Status)
	}
}

func TestInteractiveAndBackgroundUseIndependentSingleModelSlots(t *testing.T) {
	manager := &Manager{interactiveModelSlot: make(chan struct{}, 1), backgroundModelSlot: make(chan struct{}, 1)}
	backgroundRelease, err := manager.acquireModelSlot(context.Background(), model.AgentLaneBackground)
	if err != nil {
		t.Fatal(err)
	}
	interactiveRelease, err := manager.acquireModelSlot(context.Background(), model.AgentLaneInteractive)
	if err != nil {
		t.Fatalf("interactive slot waited for the occupied background slot: %v", err)
	}
	interactiveRelease()

	var acquired atomic.Bool
	done := make(chan struct{})
	go func() {
		release, acquireErr := manager.acquireModelSlot(context.Background(), model.AgentLaneBackground)
		if acquireErr == nil {
			acquired.Store(true)
			release()
		}
		close(done)
	}()
	if len(manager.backgroundModelSlot) != 1 {
		t.Fatalf("background lane did not retain exactly one occupied model slot: %d", len(manager.backgroundModelSlot))
	}
	if acquired.Load() {
		t.Fatal("two background model calls acquired the single lane slot")
	}
	backgroundRelease()
	<-done
	if !acquired.Load() {
		t.Fatal("second background model call did not resume after slot release")
	}
}

func TestAgentLaneWorkerRecoversPanicsAtGoalBoundary(t *testing.T) {
	manager := &Manager{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if manager.processRuntimeLaneSafely(context.Background(), model.AgentLaneInteractive) {
		t.Fatal("panicking lane iteration was reported as successful work")
	}
}

func TestIdleInteractiveGoalClaimWithinLatencyTarget(t *testing.T) {
	manager, _ := stage0AgentManager(t)
	claimed := make(chan struct{}, 1)
	manager.onGoalClaimed = func(lane string, _ int64) {
		if lane == model.AgentLaneInteractive {
			claimed <- struct{}{}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.runtimeWorker(ctx, model.AgentLaneInteractive)
	started := time.Now()
	if _, err := manager.ChatAsync(ctx, 0, "分析最近整体调度效果。"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-claimed:
		latency := time.Since(started)
		t.Logf("idle interactive claim: %v", latency)
		if latency >= 500*time.Millisecond {
			t.Fatalf("idle interactive claim exceeded target: %v", latency)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("idle interactive goal was not claimed within 500ms")
	}
}

func TestBackgroundModelWaitDoesNotDelayInteractiveClaim(t *testing.T) {
	manager, database := stage0AgentManager(t)
	settings, err := database.Store.GetAgentSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	settings.Enabled = true
	settings.OptimizerMode = model.AgentOptimizerPropose
	settings.OperatorMode = model.AgentOperatorConfirm
	if err := database.Store.UpdateAgentSettings(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	backgroundRelease, err := manager.acquireModelSlot(context.Background(), model.AgentLaneBackground)
	if err != nil {
		t.Fatal(err)
	}
	claimed := make(chan string, 2)
	waiting := make(chan string, 2)
	manager.onGoalClaimed = func(lane string, _ int64) { claimed <- lane }
	manager.onModelSlotWait = func(lane string) { waiting <- lane }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.runtimeWorker(ctx, model.AgentLaneBackground)
	go manager.runtimeWorker(ctx, model.AgentLaneInteractive)
	if _, err := manager.EnqueueAnalysisGoal(ctx, model.AgentRunScheduled, "lane barrier", 50); err != nil {
		backgroundRelease()
		t.Fatal(err)
	}
	select {
	case lane := <-claimed:
		if lane != model.AgentLaneBackground {
			backgroundRelease()
			t.Fatalf("unexpected first claimed lane: %q", lane)
		}
	case <-time.After(500 * time.Millisecond):
		backgroundRelease()
		t.Fatal("background goal was not claimed")
	}
	select {
	case lane := <-waiting:
		if lane != model.AgentLaneBackground {
			backgroundRelease()
			t.Fatalf("background worker did not reach its occupied model slot: %q", lane)
		}
	case <-time.After(500 * time.Millisecond):
		backgroundRelease()
		t.Fatal("background worker did not reach its occupied model slot")
	}
	started := time.Now()
	if _, err := manager.ChatAsync(ctx, 0, "分析最近整体调度效果。"); err != nil {
		backgroundRelease()
		t.Fatal(err)
	}
	select {
	case lane := <-claimed:
		if lane != model.AgentLaneInteractive {
			backgroundRelease()
			t.Fatalf("background worker claimed interactive goal: %q", lane)
		}
		if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
			backgroundRelease()
			t.Fatalf("interactive claim waited for background model slot: %v", elapsed)
		} else {
			t.Logf("background-blocked interactive claim: %v", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		backgroundRelease()
		t.Fatal("interactive goal was not claimed while background model slot was blocked")
	}
	backgroundRelease()
}

func TestCharacterizationMissingModelProviderDoesNotStopDeterministicReconcile(t *testing.T) {
	manager, _ := stage0AgentManager(t)
	ctx := context.Background()
	if _, err := manager.ChatAsync(ctx, 0, "分析最近整体调度效果。"); err != nil {
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
