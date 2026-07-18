package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/balance"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/reconcile"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/testsupport"
)

func TestRuntimeV2InvalidFinalDecisionsFailBoundedWithoutWrites(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{name: "string evidence request", content: `{"summary":"需要证据","conclusion":"等待","confidence":0.4,"no_change":true,"actions":[],"advice":[],"data_limitations":[],"evidence_requests":["需要更多审计事件"]}`},
		{name: "nonempty evidence request", content: `{"summary":"需要证据","conclusion":"等待","confidence":0.4,"no_change":true,"actions":[],"advice":[],"data_limitations":[],"evidence_requests":[{"tool":"get_audit_events","limit":10}]}`},
		{name: "no change with action", content: `{"summary":"暂停","conclusion":"准备暂停","confidence":0.99,"no_change":true,"actions":[{"type":"pause_account","account_id":1,"reason":"监控连续失败需要临时保护","prediction":{"success_rate_delta":0,"latency_delta_ms":0,"cost_delta":0}}],"advice":[],"data_limitations":[],"evidence_requests":[]}`},
		{name: "read only with mutating action", content: `{"summary":"暂停","conclusion":"准备暂停","confidence":0.99,"no_change":false,"actions":[{"type":"pause_account","account_id":1,"reason":"监控连续失败需要临时保护","prediction":{"success_rate_delta":0,"latency_delta_ms":0,"cost_delta":0}}],"advice":[],"data_limitations":[],"evidence_requests":[]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := scriptedRuntimeServer(t, &calls, func(int) map[string]any {
				return map[string]any{"role": "assistant", "content": test.content}
			})
			defer server.Close()
			manager, database, api := runtimeContractTestManager(t, server.URL)
			ctx := context.Background()
			beforeMutations, err := database.Store.ListAccountMutations(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			beforeTransitions, err := database.Store.ListGroupTierTransitions(ctx, 0, "", 100)
			if err != nil {
				t.Fatal(err)
			}

			receipt, err := manager.ChatAsync(ctx, 0, "分析最近整体调度效果。")
			if err != nil {
				t.Fatal(err)
			}
			if !manager.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive) {
				t.Fatal("interactive goal was not processed")
			}
			goal, err := database.Store.GetAgentGoal(ctx, receipt.GoalID)
			if err != nil {
				t.Fatal(err)
			}
			if goal.Status != model.AgentGoalStatusFailed || goal.LastErrorClass != string(runtimeErrorModelContractInvalid) ||
				goal.ContractFailureCount != 2 || goal.ModelAttemptCount != 2 || !goal.DeadLettered {
				t.Fatalf("contract failure did not terminate safely: %+v", goal)
			}
			for range 3 {
				if manager.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive) {
					t.Fatal("terminal contract goal was reclaimed")
				}
			}
			if calls.Load() != 2 {
				t.Fatalf("model calls=%d, want exactly 2", calls.Load())
			}
			assertRuntimeProducedNoWrites(t, database, api, beforeMutations, beforeTransitions)
		})
	}
}

func TestRuntimeV2RepeatedInvalidToolArgumentsTerminateNoProgress(t *testing.T) {
	var calls atomic.Int32
	server := scriptedRuntimeServer(t, &calls, func(call int) map[string]any {
		return map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{
			"id": "audit-" + string(rune('0'+call)), "type": "function",
			"function": map[string]any{"name": "get_audit_events", "arguments": `{"reason":""}`},
		}}}
	})
	defer server.Close()
	manager, database, api := runtimeContractTestManager(t, server.URL)
	ctx := context.Background()
	beforeMutations, _ := database.Store.ListAccountMutations(ctx, 1)
	beforeTransitions, _ := database.Store.ListGroupTierTransitions(ctx, 0, "", 100)
	receipt, err := manager.ChatAsync(ctx, 0, "分析最近整体调度效果。")
	if err != nil {
		t.Fatal(err)
	}
	if !manager.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive) {
		t.Fatal("interactive goal was not processed")
	}
	goal, err := database.Store.GetAgentGoal(ctx, receipt.GoalID)
	if err != nil {
		t.Fatal(err)
	}
	if goal.Status != model.AgentGoalStatusFailed || goal.LastErrorClass != string(runtimeErrorModelNoProgress) ||
		goal.NoProgressCount != 2 || goal.ModelAttemptCount != 3 {
		t.Fatalf("invalid tool loop did not terminate as no-progress: %+v", goal)
	}
	if calls.Load() != 3 {
		t.Fatalf("model calls=%d, want 3", calls.Load())
	}
	assertRuntimeProducedNoWrites(t, database, api, beforeMutations, beforeTransitions)
}

func TestRuntimeV2RepeatedReadOnlyMutationTerminatesNoProgress(t *testing.T) {
	var calls atomic.Int32
	server := scriptedRuntimeServer(t, &calls, func(call int) map[string]any {
		return map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{
			"id": "pause-" + string(rune('0'+call)), "type": "function",
			"function": map[string]any{"name": "pause_account", "arguments": `{"account_id":1,"reason":"重复尝试修改只读目标"}`},
		}}}
	})
	defer server.Close()
	manager, database, api := runtimeContractTestManager(t, server.URL)
	ctx := context.Background()
	beforeMutations, _ := database.Store.ListAccountMutations(ctx, 1)
	beforeTransitions, _ := database.Store.ListGroupTierTransitions(ctx, 0, "", 100)
	receipt, err := manager.ChatAsync(ctx, 0, "分析最近整体调度效果。")
	if err != nil || !manager.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive) {
		t.Fatalf("read-only goal did not run: receipt=%+v err=%v", receipt, err)
	}
	goal, err := database.Store.GetAgentGoal(ctx, receipt.GoalID)
	if err != nil {
		t.Fatal(err)
	}
	if goal.Status != model.AgentGoalStatusFailed || goal.LastErrorClass != string(runtimeErrorModelNoProgress) ||
		goal.NoProgressCount != 2 || goal.ModelAttemptCount != 3 || !goal.DeadLettered {
		t.Fatalf("repeated pre-execution rejection did not terminate: %+v", goal)
	}
	if calls.Load() != 3 {
		t.Fatalf("model calls=%d, want 3", calls.Load())
	}
	assertRuntimeProducedNoWrites(t, database, api, beforeMutations, beforeTransitions)
}

func TestRuntimeV2ChangingInvalidToolsCannotBypassGoalModelLimit(t *testing.T) {
	var calls atomic.Int32
	server := scriptedRuntimeServer(t, &calls, func(call int) map[string]any {
		arguments := `{"reason":"invalid-` + fmt.Sprint(call) + `"}`
		return map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{
			"id": "audit-" + fmt.Sprint(call), "type": "function",
			"function": map[string]any{"name": "get_audit_events", "arguments": arguments},
		}}}
	})
	defer server.Close()
	manager, database, api := runtimeContractTestManager(t, server.URL)
	ctx := context.Background()
	beforeMutations, _ := database.Store.ListAccountMutations(ctx, 1)
	beforeTransitions, _ := database.Store.ListGroupTierTransitions(ctx, 0, "", 100)
	receipt, err := manager.ChatAsync(ctx, 0, "分析最近整体调度效果。")
	if err != nil || !manager.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive) {
		t.Fatalf("changing-invalid goal did not run: receipt=%+v err=%v", receipt, err)
	}
	goal, err := database.Store.GetAgentGoal(ctx, receipt.GoalID)
	if err != nil {
		t.Fatal(err)
	}
	if goal.Status != model.AgentGoalStatusFailed || goal.LastErrorClass != string(runtimeErrorModelNoProgress) ||
		goal.ModelAttemptCount != maxModelAttemptsPerGoal || !goal.DeadLettered {
		t.Fatalf("model hard limit did not terminate changing invalid calls: %+v", goal)
	}
	if calls.Load() != maxModelAttemptsPerGoal {
		t.Fatalf("model calls=%d, want hard limit %d", calls.Load(), maxModelAttemptsPerGoal)
	}
	assertRuntimeProducedNoWrites(t, database, api, beforeMutations, beforeTransitions)
}

func TestRuntimeV2TemporaryPauseUsesAccountControlOnceWithV2Provenance(t *testing.T) {
	var calls atomic.Int32
	server := scriptedRuntimeServer(t, &calls, func(call int) map[string]any {
		if call == 1 {
			return map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{
				"id": "pause-1", "type": "function", "function": map[string]any{
					"name": "pause_account", "arguments": `{"account_id":1,"reason":"管理员要求临时保护账号"}`},
			}}}
		}
		return map[string]any{"role": "assistant", "content": `{"summary":"已暂停","conclusion":"临时暂停已经回读确认","confidence":1,"no_change":true,"actions":[],"advice":[],"data_limitations":[],"evidence_requests":[]}`}
	})
	defer server.Close()
	manager, database, api := runtimeContractTestManager(t, server.URL)
	ctx := context.Background()
	before, err := database.Store.ListAccountMutations(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := manager.ChatAsync(ctx, 0, "暂停账号1 15分钟。")
	if err != nil {
		t.Fatal(err)
	}
	if !manager.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive) {
		t.Fatal("interactive pause goal was not processed")
	}
	goal, err := database.Store.GetAgentGoal(ctx, receipt.GoalID)
	if err != nil || goal.Status != model.AgentGoalStatusCompleted {
		t.Fatalf("pause goal did not complete: goal=%+v err=%v", goal, err)
	}
	steps, err := database.Store.ListAgentSteps(ctx, receipt.GoalID)
	if err != nil || len(steps) != 1 || steps[0].Status != model.AgentStepStatusCompleted {
		t.Fatalf("pause step was not completed once: steps=%+v err=%v", steps, err)
	}
	after, err := database.Store.ListAccountMutations(ctx, 1)
	if err != nil || len(after) != len(before)+1 {
		t.Fatalf("pause mutation count=%d before=%d err=%v", len(after), len(before), err)
	}
	mutation := after[0]
	if mutation.RunID != 0 || mutation.GoalID != receipt.GoalID || mutation.StepID != steps[0].ID ||
		mutation.SnapshotVersion == "" || mutation.ExpiresAt == nil {
		t.Fatalf("pause mutation has incomplete V2 provenance or TTL: %+v", mutation)
	}
	events, err := database.Store.ListEvents(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	var mutationAudit *model.Event
	for index := range events {
		if events[index].Type == "agent_pause" {
			mutationAudit = &events[index]
			break
		}
	}
	if mutationAudit == nil || mutationAudit.GoalID != receipt.GoalID || mutationAudit.StepID != steps[0].ID {
		t.Fatalf("V2 account audit provenance missing: %+v", mutationAudit)
	}
	stats := api.Stats()
	if stats.ByName[testsupport.CallSetSchedulable] != 1 {
		t.Fatalf("pause external writes=%d, want exactly one", stats.ByName[testsupport.CallSetSchedulable])
	}
}

func TestProviderTimeoutClassificationAndRetryAreBounded(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err := (completionClient{}).CompleteRuntimeNative(ctx, model.AgentProvider{BaseURL: server.URL, Model: "fake",
		TimeoutSeconds: 10, MaxOutputTokens: 100}, "key", []RuntimeMessage{{Role: "user", Content: "test"}}, CapabilitySpecs())
	cancel()
	close(release)
	server.Close()
	if runtimeErrorClassOf(err) != runtimeErrorProviderTimeout {
		t.Fatalf("provider timeout class=%s err=%v", runtimeErrorClassOf(err), err)
	}

	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	manager := &Manager{store: database.Store, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	goal := model.AgentGoal{Title: "provider retry", Objective: "bounded timeout", Status: model.AgentGoalStatusRunning,
		Lane: model.AgentLaneBackground, Priority: 50, RiskLevel: model.AgentRiskLow, Source: "system", Context: json.RawMessage(`{}`),
		MaxAttempts: 3}
	if err := database.Store.CreateAgentGoal(context.Background(), &goal); err != nil {
		t.Fatal(err)
	}
	var previousDelay time.Duration
	for attempt := 1; attempt <= goal.MaxAttempts; attempt++ {
		before := time.Now().UTC()
		_ = manager.retryProviderGoal(context.Background(), &goal, runtimeErrorProviderTimeout, context.DeadlineExceeded)
		if attempt < goal.MaxAttempts {
			if goal.Status != model.AgentGoalStatusPlanned || goal.NextRunnableAt == nil {
				t.Fatalf("attempt %d was not scheduled: %+v", attempt, goal)
			}
			delay := goal.NextRunnableAt.Sub(before)
			if delay < previousDelay || delay > maxProviderRetryDelay {
				t.Fatalf("attempt %d delay=%s previous=%s", attempt, delay, previousDelay)
			}
			previousDelay = delay
		}
	}
	loaded, err := database.Store.GetAgentGoal(context.Background(), goal.ID)
	if err != nil || loaded.Status != model.AgentGoalStatusFailed || !loaded.DeadLettered ||
		loaded.LastErrorClass != string(runtimeErrorProviderTimeout) || loaded.AttemptCount != loaded.MaxAttempts {
		t.Fatalf("provider retry did not terminate: goal=%+v err=%v", loaded, err)
	}
}

func TestInvalidProviderConfigurationIsTerminalClass(t *testing.T) {
	_, err := (completionClient{}).CompleteRuntimeNative(context.Background(), model.AgentProvider{BaseURL: "not-a-url",
		Model: "fake", TimeoutSeconds: 10, MaxOutputTokens: 100}, "key", nil, CapabilitySpecs())
	if runtimeErrorClassOf(err) != runtimeErrorProviderAuthFailed || runtimeErrorRetryable(runtimeErrorClassOf(err)) {
		t.Fatalf("invalid provider configuration class=%s err=%v", runtimeErrorClassOf(err), err)
	}
	transient := newRuntimeError(runtimeErrorProviderServer, context.DeadlineExceeded)
	auth := newRuntimeError(runtimeErrorProviderAuthFailed, context.Canceled)
	if runtimeErrorClassOf(preferredRuntimeFailure(auth, transient)) != runtimeErrorProviderServer {
		t.Fatal("transient fallback provider failure did not remain retryable")
	}
}

func TestInvalidContractRemainsTerminalAcrossThirtyMinuteSimulation(t *testing.T) {
	var calls atomic.Int32
	server := scriptedRuntimeServer(t, &calls, func(int) map[string]any {
		return map[string]any{"role": "assistant", "content": `{"summary":"需要证据","conclusion":"等待","confidence":0.4,"no_change":true,"actions":[],"advice":[],"data_limitations":[],"evidence_requests":["需要更多审计事件"]}`}
	})
	defer server.Close()
	manager, database, api := runtimeContractTestManager(t, server.URL)
	ctx := context.Background()
	receipt, err := manager.ChatAsync(ctx, 0, "分析最近整体调度效果。")
	if err != nil || !manager.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive) {
		t.Fatalf("initial invalid goal did not run: receipt=%+v err=%v", receipt, err)
	}
	beforeMutations, _ := database.Store.ListAccountMutations(ctx, 1)
	beforeTransitions, _ := database.Store.ListGroupTierTransitions(ctx, 0, "", 100)
	// Simulate one scheduler check every five seconds for thirty minutes. The
	// test advances logical cycles without a wall-clock sleep.
	for range 360 {
		if manager.processNextRuntimeGoalLane(ctx, model.AgentLaneInteractive) {
			t.Fatal("dead-lettered contract goal was reclaimed during simulation")
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("model calls grew after terminal failure: %d", calls.Load())
	}
	assertRuntimeProducedNoWrites(t, database, api, beforeMutations, beforeTransitions)
}

func scriptedRuntimeServer(t *testing.T, calls *atomic.Int32, response func(int) map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request runtimeChatRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		call := int(calls.Add(1))
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": response(call)}}})
	}))
}

func runtimeContractTestManager(t *testing.T, modelURL string) (*Manager, *testsupport.TempDatabase, *testsupport.FakeSub2API) {
	t.Helper()
	database := testsupport.OpenTempDatabase(t, testsupport.DefaultSettings())
	fixture := testsupport.GenerateFixture(testsupport.FixtureConfig{Accounts: 1, Monitors: 1})
	api := testsupport.NewFakeSub2API(fixture)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := reconcile.NewEngine(api, database.Store, 50*time.Second, logger)
	balances := balance.NewManager(database.Store, api, engine, nil, nil, time.Hour, logger)
	box, err := balance.NewSecretBox([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager(database.Store, engine, balances, box, logger)
	settings, err := database.Store.GetAgentSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	settings.Enabled = true
	settings.OptimizerMode = model.AgentOptimizerObserve
	settings.OperatorMode = model.AgentOperatorDirect
	if err := database.Store.UpdateAgentSettings(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	nonce, ciphertext, err := box.Encrypt([]byte("fake-model-key"))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Store.UpsertAgentProvider(context.Background(), model.AgentProvider{
		Slot: "primary", BaseURL: modelURL, Model: "fake", CredentialNonce: nonce, CredentialCiphertext: ciphertext,
		Enabled: true, TimeoutSeconds: 10, MaxOutputTokens: 4096, Temperature: .1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	api.ResetStats()
	return manager, database, api
}

func assertRuntimeProducedNoWrites(t *testing.T, database *testsupport.TempDatabase, api *testsupport.FakeSub2API,
	beforeMutations []accountcontrol.Mutation, beforeTransitions []model.GroupTierTransition) {
	t.Helper()
	afterMutations, err := database.Store.ListAccountMutations(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	afterTransitions, err := database.Store.ListGroupTierTransitions(context.Background(), 0, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterMutations) != len(beforeMutations) || len(afterTransitions) != len(beforeTransitions) {
		t.Fatalf("runtime created mutation/transition: mutations %d->%d transitions %d->%d",
			len(beforeMutations), len(afterMutations), len(beforeTransitions), len(afterTransitions))
	}
	stats := api.Stats()
	for _, name := range []string{testsupport.CallSetSchedulable, testsupport.CallUpdateLoadFactor, testsupport.CallTransitionGroupTier} {
		if stats.ByName[name] != 0 {
			t.Fatalf("runtime performed external write %s %d times", name, stats.ByName[name])
		}
	}
}
