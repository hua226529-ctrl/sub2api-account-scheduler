package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/health"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestCompletionClientParsesStructuredDecision(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/chat/completions" {
			t.Errorf("request path = %q, want /v1/chat/completions", request.URL.Path)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("authorization = %q", got)
		}
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		responseFormat, ok := payload["response_format"].(map[string]any)
		if !ok || responseFormat["type"] != "json_object" {
			t.Errorf("response_format = %#v", payload["response_format"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"分析结果如下：\n` +
			"```json\\n{\\\"summary\\\":\\\"系统稳定\\\",\\\"conclusion\\\":\\\"保持当前策略\\\",\\\"confidence\\\":1.2,\\\"no_change\\\":true,\\\"actions\\\":[],\\\"advice\\\":[\\\"继续观察\\\"],\\\"data_limitations\\\":[]}\\n```" +
			`"}}]}`))
	}))
	defer server.Close()

	decision, err := (completionClient{}).Complete(context.Background(), model.AgentProvider{
		BaseURL: server.URL, Model: "test-model", TimeoutSeconds: 10, MaxOutputTokens: 512, Temperature: .1,
	}, "test-key", "system", "packet")
	if err != nil {
		t.Fatal(err)
	}
	if decision.Summary != "系统稳定" || decision.Conclusion != "保持当前策略" || !decision.NoChange {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if decision.Confidence != 1 {
		t.Fatalf("confidence = %v, want clamped to 1", decision.Confidence)
	}
	if len(decision.Advice) != 1 || decision.Advice[0] != "继续观察" {
		t.Fatalf("advice = %#v", decision.Advice)
	}
}

func TestCompletionClientRetriesWithoutResponseFormatOnBadRequest(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		call := calls.Add(1)
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if call == 1 {
			if _, ok := payload["response_format"]; !ok {
				t.Error("first request should contain response_format")
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"response_format is unsupported"}}`))
			return
		}
		if _, ok := payload["response_format"]; ok {
			t.Errorf("fallback request still contains response_format: %#v", payload["response_format"])
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":[{"type":"text","text":"{\"summary\":\"兼容模式成功\",\"conclusion\":\"不调整\",\"confidence\":0.8,\"no_change\":true,\"actions\":[],\"advice\":[],\"data_limitations\":[]}"}]}}]}`))
	}))
	defer server.Close()

	decision, err := (completionClient{}).Complete(context.Background(), model.AgentProvider{
		BaseURL: server.URL + "/v1", Model: "compatible-model", TimeoutSeconds: 10,
	}, "test-key", "system", "packet")
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("request count = %d, want 2", calls.Load())
	}
	if decision.Summary != "兼容模式成功" || !decision.NoChange {
		t.Fatalf("unexpected fallback decision: %+v", decision)
	}
}

func TestDeriveAvailabilityKeepsDegradedMonitorAvailableWhenTrafficSucceeds(t *testing.T) {
	t.Parallel()

	now := testPacketTime()
	binding := availabilityBinding(now, model.StatusDegraded)
	state := availabilityState(20, 20, 100)

	deriveAvailability(&state, binding, health.DefaultSettings(), now)

	if state.AvailabilityState != "available" {
		t.Fatalf("availability state = %q, want available", state.AvailabilityState)
	}
	if state.AvailabilityScore != 100 || state.Confidence < .8 {
		t.Fatalf("unexpected availability result: %+v", state)
	}
	if state.EvidenceConflict {
		t.Fatalf("degraded monitor with successful traffic should not be a conflict: %+v", state)
	}
	if !containsReason(state.Reasons, "真实请求成功") {
		t.Fatalf("reasons = %#v", state.Reasons)
	}
}

func TestDeriveAvailabilityMarksMonitorTrafficConflicts(t *testing.T) {
	t.Parallel()

	now := testPacketTime()
	tests := []struct {
		name          string
		monitorStatus string
		successes     int
		successRate   float64
		reason        string
	}{
		{name: "monitor failed but traffic succeeds", monitorStatus: model.StatusFailed, successes: 20, successRate: 100, reason: "疑似监控误报"},
		{name: "monitor succeeds but traffic fails", monitorStatus: model.StatusOperational, successes: 2, successRate: 10, reason: "真实流量异常"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding := availabilityBinding(now, tt.monitorStatus)
			state := availabilityState(20, tt.successes, tt.successRate)

			deriveAvailability(&state, binding, health.DefaultSettings(), now)

			if state.AvailabilityState != "degraded" || !state.EvidenceConflict {
				t.Fatalf("conflicting evidence should be degraded, got %+v", state)
			}
			if containsReason(state.Reasons, "同时异常") || !containsReason(state.Reasons, tt.reason) {
				t.Fatalf("unexpected conflict reasons: %#v", state.Reasons)
			}
		})
	}
}

func testPacketTime() (resultTime time.Time) {
	return time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
}

func availabilityBinding(now time.Time, status string) model.ResolvedBinding {
	checkedAt := now.Add(-10 * time.Second)
	return model.ResolvedBinding{
		Monitor: &model.Monitor{Enabled: true, IntervalSeconds: 60, LastCheckedAt: &checkedAt, PrimaryStatus: status},
		Account: model.Account{ID: 225, Name: "test", Status: "active", Schedulable: true, Concurrency: 10},
	}
}

func availabilityState(eligible, successes int, successRate float64) model.AgentAccountState {
	return model.AgentAccountState{
		Schedulable: true, AccountStatus: "active", StabilityScore: 100, CapacityScore: 100,
		Reasons: []string{}, Windows: map[string]model.AgentWindowStats{
			"30m": {Window: "30m", SampleCount: eligible, EligibleCount: eligible, SuccessCount: successes, SuccessRate: successRate},
			"24h": {Window: "24h"},
		},
	}
}

func containsReason(reasons []string, fragment string) bool {
	for _, reason := range reasons {
		if strings.Contains(reason, fragment) {
			return true
		}
	}
	return false
}

func TestDecisionValidationRejectsContradictionsAndUnknownTargets(t *testing.T) {
	manager := &Manager{}
	packet := model.AnalysisPacket{AccountCompactStates: []model.AgentAccountState{{AccountID: 225}}}
	if err := manager.validateDecision(packet, ModelDecision{NoChange: true, Confidence: 1, Actions: []AgentAction{{
		Type: "pause_account", AccountID: 225, Reason: "连续故障证据充分",
	}}}); err == nil {
		t.Fatal("no_change decision with actions should be rejected")
	}
	if err := manager.validateDecision(packet, ModelDecision{Confidence: 1, Actions: []AgentAction{{
		Type: "pause_account", AccountID: 999, Reason: "账号不在数据包中",
	}}}); err == nil {
		t.Fatal("action target outside the immutable packet should be rejected")
	}
}

func TestDecisionValidationAllowsOneConfirmedGroupTierTransition(t *testing.T) {
	now := testPacketTime()
	manager := &Manager{}
	packet := model.AnalysisPacket{
		CutoffAt:      now,
		DataHealth:    model.AgentDataHealth{MonitorFresh: true, TrafficFresh: true},
		PoolSummaries: []model.AgentPoolSummary{{Name: "主池", Accounts: 2, Unavailable: 2}},
		AccountCompactStates: []model.AgentAccountState{
			{AccountID: 1, AvailabilityState: "unavailable", HardFailureStreak: 3,
				Windows: map[string]model.AgentWindowStats{"5m": {EligibleCount: 5, ErrorCategoryCounts: map[string]int{model.ErrorClassInfrastructure: 5}}}},
			{AccountID: 2, AvailabilityState: "unavailable", HardFailureStreak: 3,
				Windows: map[string]model.AgentWindowStats{"5m": {EligibleCount: 5, ErrorCategoryCounts: map[string]int{model.ErrorClassInfrastructure: 5}}}},
		},
		GroupFailoverTokens: []model.AgentGroupFailoverToken{{
			SourceID: 9, Pool: "主池", KeyID: "token-7", Enabled: true, Confirmed: true,
			DataFresh: true, CurrentTier: model.GroupTierMain, AccountIDs: []int64{1, 2},
			Main:      model.AgentGroupTierSummary{Tier: model.GroupTierMain, Name: "主分组", Configured: true, Enabled: true},
			Backup:    model.AgentGroupTierSummary{Tier: model.GroupTierBackup, Name: "备用分组", Configured: true, Enabled: true},
			Emergency: model.AgentGroupTierSummary{Tier: model.GroupTierEmergency, Name: "紧急分组", Configured: true, Enabled: true},
		}},
	}
	action := AgentAction{Type: "transition_token_group_tier", SourceID: 9, KeyID: "token-7",
		TargetTier: model.GroupTierBackup, Reason: "调度池已连续确认完全不可用"}
	if err := manager.validateDecision(packet, ModelDecision{Confidence: .90, Actions: []AgentAction{action}}); err != nil {
		t.Fatalf("confirmed transition rejected: %v", err)
	}
	if err := manager.validateDecision(packet, ModelDecision{Confidence: .89, Actions: []AgentAction{action}}); err == nil {
		t.Fatal("transition below 0.90 confidence should be rejected")
	}
	second := action
	second.SourceID = 10
	second.KeyID = "token-8"
	packet.GroupFailoverTokens = append(packet.GroupFailoverTokens, model.AgentGroupFailoverToken{
		SourceID: 10, Pool: "主池", KeyID: "token-8", Enabled: true, Confirmed: true, DataFresh: true,
		CurrentTier: model.GroupTierMain,
		Main:        model.AgentGroupTierSummary{Tier: model.GroupTierMain, Name: "主二组", Configured: true, Enabled: true},
		Backup:      model.AgentGroupTierSummary{Tier: model.GroupTierBackup, Name: "备用二组", Configured: true, Enabled: true},
		AccountIDs:  []int64{1, 2},
	})
	if err := manager.validateDecision(packet, ModelDecision{Confidence: 1, Actions: []AgentAction{action, second}}); err == nil {
		t.Fatal("two group transitions in one analysis should be rejected")
	}
}

func TestDecisionValidationRejectsUnsafeGroupTierTransition(t *testing.T) {
	manager := &Manager{}
	packet := model.AnalysisPacket{
		CutoffAt:      testPacketTime(),
		DataHealth:    model.AgentDataHealth{MonitorFresh: true, TrafficFresh: true},
		PoolSummaries: []model.AgentPoolSummary{{Name: "主池", Accounts: 1, Unavailable: 1}},
		AccountCompactStates: []model.AgentAccountState{{
			AccountID: 1, AvailabilityState: "unavailable", HardFailureStreak: 3,
			Windows: map[string]model.AgentWindowStats{"5m": {EligibleCount: 10, ErrorCategoryCounts: map[string]int{model.ErrorClassInfrastructure: 10}}},
		}},
		GroupFailoverTokens: []model.AgentGroupFailoverToken{{
			SourceID: 9, Pool: "主池", KeyID: "token-7", Enabled: true, Confirmed: true,
			DataFresh: true, CurrentTier: model.GroupTierMain, AccountIDs: []int64{1},
			Main:      model.AgentGroupTierSummary{Tier: model.GroupTierMain, Name: "主分组", Configured: true, Enabled: true},
			Backup:    model.AgentGroupTierSummary{Tier: model.GroupTierBackup, Name: "备用分组", Configured: true, Enabled: true},
			Emergency: model.AgentGroupTierSummary{Tier: model.GroupTierEmergency, Name: "紧急分组", Configured: true, Enabled: true},
		}},
	}
	action := AgentAction{Type: "transition_token_group_tier", SourceID: 9, KeyID: "token-7",
		TargetTier: model.GroupTierEmergency, Reason: "尝试越过备用层级直接进入紧急分组"}
	if err := manager.validateDecision(packet, ModelDecision{Confidence: 1, Actions: []AgentAction{action}}); err == nil {
		t.Fatal("main to emergency transition should be rejected")
	}
	action.TargetTier = model.GroupTierBackup
	packet.AccountCompactStates[0].Schedulable = true
	if err := manager.validateDecision(packet, ModelDecision{Confidence: 1, Actions: []AgentAction{action}}); err == nil {
		t.Fatal("a still-schedulable channel must block a group transition")
	}
	packet.AccountCompactStates[0].Schedulable = false
	action.TargetTier = model.GroupTierEmergency
	packet.GroupFailoverTokens[0].CurrentTier = model.GroupTierBackup
	packet.GroupFailoverTokens[0].ManualHoldUntil = timePointer(testPacketTime().Add(time.Minute))
	if err := manager.validateDecision(packet, ModelDecision{Confidence: 1, Actions: []AgentAction{action}}); err == nil {
		t.Fatal("transition during manual hold should be rejected")
	}
	packet.GroupFailoverTokens[0].CurrentTier = model.GroupTierMain
	packet.GroupFailoverTokens[0].ManualHoldUntil = nil
	packet.AccountCompactStates[0].ErrorCategoryCounts = map[string]int{model.ErrorClassCredential: 1}
	action.TargetTier = model.GroupTierBackup
	if err := manager.validateDecision(packet, ModelDecision{Confidence: 1, Actions: []AgentAction{action}}); err == nil {
		t.Fatal("credential failures must not trigger a group transition")
	}
}

func TestDecisionValidationRejectsAutonomousReturnToMain(t *testing.T) {
	now := testPacketTime()
	healthySince := now.Add(-31 * time.Minute)
	manager := &Manager{}
	packet := model.AnalysisPacket{
		CutoffAt: now, DataHealth: model.AgentDataHealth{MonitorFresh: true, TrafficFresh: true},
		PoolSummaries: []model.AgentPoolSummary{{Name: "主池", Accounts: 1, Available: 1}},
		AccountCompactStates: []model.AgentAccountState{{AccountID: 1, AvailabilityState: "available",
			Windows: map[string]model.AgentWindowStats{"30m": {EligibleCount: 20, SuccessCount: 20}}}},
		GroupFailoverTokens: []model.AgentGroupFailoverToken{{
			SourceID: 9, Pool: "主池", KeyID: "token-7", Enabled: true, Confirmed: true, DataFresh: true,
			CurrentTier: model.GroupTierBackup,
			Main:        model.AgentGroupTierSummary{Tier: model.GroupTierMain, Name: "主分组", Configured: true, Enabled: true},
			Backup:      model.AgentGroupTierSummary{Tier: model.GroupTierBackup, Name: "备用分组", Configured: true, Enabled: true},
			AccountIDs:  []int64{1}, HealthySince: &healthySince, RecoveryHealthyCount: 10,
		}},
	}
	action := AgentAction{Type: "transition_token_group_tier", SourceID: 9, KeyID: "token-7",
		TargetTier: model.GroupTierMain, Reason: "已满足稳定窗口并尝试恢复主分组"}
	if err := manager.validateDecision(packet, ModelDecision{Confidence: 1, Actions: []AgentAction{action}}); err == nil {
		t.Fatal("autonomous return to an unobservable main group must always be rejected")
	}
}

func TestDecisionValidationSkipsDisabledBackupInFixedChain(t *testing.T) {
	manager := &Manager{}
	packet := model.AnalysisPacket{
		CutoffAt:      testPacketTime(),
		DataHealth:    model.AgentDataHealth{MonitorFresh: true, TrafficFresh: true},
		PoolSummaries: []model.AgentPoolSummary{{Name: "主池", Accounts: 1, Unavailable: 1}},
		AccountCompactStates: []model.AgentAccountState{{
			AccountID: 1, AvailabilityState: "unavailable", HardFailureStreak: 3,
			Windows: map[string]model.AgentWindowStats{"5m": {EligibleCount: 10, ErrorCategoryCounts: map[string]int{model.ErrorClassInfrastructure: 10}}},
		}},
		GroupFailoverTokens: []model.AgentGroupFailoverToken{{
			SourceID: 9, Pool: "主池", KeyID: "token-7", Enabled: true, Confirmed: true,
			DataFresh: true, CurrentTier: model.GroupTierMain, AccountIDs: []int64{1},
			Main:      model.AgentGroupTierSummary{Tier: model.GroupTierMain, Name: "主分组", Configured: true, Enabled: true},
			Backup:    model.AgentGroupTierSummary{Tier: model.GroupTierBackup, Name: "备用分组", Configured: true, Enabled: false},
			Emergency: model.AgentGroupTierSummary{Tier: model.GroupTierEmergency, Name: "紧急分组", Configured: true, Enabled: true},
		}},
	}
	action := AgentAction{Type: "transition_token_group_tier", SourceID: 9, KeyID: "token-7",
		TargetTier: model.GroupTierEmergency, Reason: "备用层禁用，按固定链进入紧急分组"}
	if err := manager.validateDecision(packet, ModelDecision{Confidence: 1, Actions: []AgentAction{action}}); err != nil {
		t.Fatalf("fixed chain should skip a disabled backup: %v", err)
	}
}

func TestGroupFailoverPacketIsTierBasedAndRedacted(t *testing.T) {
	balance := 32.5
	items := buildGroupFailoverTokens([]model.UpstreamSource{{
		ID: 3, Name: "上游甲", Provider: "newapi", RoutingPool: "主池", Balance: &balance,
		LastSuccessAt: timePointer(testPacketTime()),
		Groups: []model.UpstreamGroup{{ExternalID: "secret-main-id", Name: "主分组", RateMultiplier: 1},
			{ExternalID: "secret-backup-id", Name: "备用分组", RateMultiplier: 1.2},
			{ExternalID: "secret-emergency-id", Name: "紧急分组", RateMultiplier: 2}},
		FailoverPolicies: []model.GroupFailoverPolicy{{
			SourceID: 3, KeyID: "token-7", KeyName: "生产令牌", KeyHint: "sk-***7890", Enabled: true,
			MainGroupID: "secret-main-id", BackupGroupID: "secret-backup-id", EmergencyGroupID: "secret-emergency-id",
			MainEnabled: true, BackupEnabled: true, EmergencyEnabled: true,
			Version: 2, ConfirmedVersion: 2, Confirmed: true, AccountIDs: []int64{225},
			State: model.GroupFailoverState{CurrentTier: model.GroupTierMain},
		}},
		MatchedAccounts: []model.AccountRef{{ID: 225, Name: "账号225"}},
	}}, nil)
	if len(items) != 1 || items[0].Backup.Name != "备用分组" || items[0].Backup.RateMultiplier != 1.2 ||
		!items[0].Main.Configured || !items[0].Main.Enabled || !items[0].Backup.Configured || !items[0].Backup.Enabled ||
		!items[0].Emergency.Configured || !items[0].Emergency.Enabled {
		t.Fatalf("unexpected failover packet: %#v", items)
	}
	payload, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "secret-main-id") || strings.Contains(string(payload), "secret-backup-id") ||
		strings.Contains(string(payload), "secret-emergency-id") {
		t.Fatalf("real group identifiers leaked into agent packet: %s", payload)
	}
}

func timePointer(value time.Time) *time.Time { return &value }

func TestModelInputEnforcesContextBudget(t *testing.T) {
	packet := model.AnalysisPacket{SystemSummary: model.AgentSystemSummary{Accounts: 300}, ActivePolicies: json.RawMessage(`{}`), DecisionOutcomes: json.RawMessage(`[]`)}
	for id := int64(1); id <= 300; id++ {
		packet.AccountCompactStates = append(packet.AccountCompactStates, model.AgentAccountState{
			AccountID: id, Name: strings.Repeat("很长的账号名称", 10), AvailabilityState: "available",
			AvailabilityScore: 100, PerformanceScore: 100, StabilityScore: 100, Confidence: .9,
		})
	}
	input, err := modelInput(packet, model.AgentSettings{ContextTokenBudget: 2000})
	if err != nil {
		t.Fatal(err)
	}
	if len([]byte(input))/4 > 2100 {
		t.Fatalf("compressed model input still exceeds budget: %d estimated tokens", len([]byte(input))/4)
	}
	if !strings.Contains(input, "omitted_accounts") {
		t.Fatal("compressed input must disclose omitted account count")
	}
}

func TestCompletionEndpointRejectsInsecureRemoteHTTP(t *testing.T) {
	if _, err := completionEndpoint("http://example.com/v1"); err == nil {
		t.Fatal("remote plaintext HTTP endpoint should be rejected")
	}
	if _, err := completionEndpoint("http://169.254.169.254/v1"); err == nil {
		t.Fatal("link-local metadata endpoint should be rejected")
	}
}
