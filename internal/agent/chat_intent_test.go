package agent

import (
	"context"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type chatIntentResolverStub struct {
	accounts []int64
	token    string
}

func (r chatIntentResolverStub) AccountExists(id int64) bool {
	for _, candidate := range r.accounts {
		if candidate == id {
			return true
		}
	}
	return false
}

func (r chatIntentResolverStub) AccountIDs() []int64 { return append([]int64(nil), r.accounts...) }

func (r chatIntentResolverStub) ResolveToken(_ context.Context, identity string) (int64, string, bool, error) {
	if identity == r.token {
		return 7, "key-9", true, nil
	}
	return 0, "", false, nil
}

func TestChatIntentContractExamples(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	resolver := chatIntentResolverStub{accounts: []int64{123, 124}, token: "X"}
	tests := []struct {
		name       string
		message    string
		intentType string
		operation  string
		readOnly   bool
		confirm    bool
		ttl        time.Duration
	}{
		{"query", "账号 123 现在怎么样？", ChatIntentQuery, "get_status", true, false, 0},
		{"analysis", "分析账号 123 最近为什么抖动，不要执行动作。", ChatIntentAnalysis, "analyze_flapping", true, false, 0},
		{"pause", "暂停账号 123 三十分钟。", ChatIntentDirectAction, "pause", false, false, 30 * time.Minute},
		{"manual hold", "暂停账号 123，直到我手动解除。", ChatIntentDirectAction, "manual_hold", false, true, 0},
		{"resume", "恢复账号 123。", ChatIntentDirectAction, "resume", false, false, 30 * time.Minute},
		{"load", "把账号 123 的负载调到 50% 一小时。", ChatIntentDirectAction, "set_load_factor", false, false, time.Hour},
		{"policy proposal", "以后池 A 连续失败五次再暂停。", ChatIntentPolicyChange, "propose_policy", false, false, 0},
		{"scheduled group", "今晚两点将令牌 X 切换到备用组。", ChatIntentScheduledAction, "transition_group_tier", false, true, 0},
		{"overall analysis", "分析最近整体调度效果。", ChatIntentAnalysis, "analyze", true, false, 0},
		{"delegated", "你自己处理账号 123 的异常。", ChatIntentDirectAction, "analyze_and_act", false, false, 15 * time.Minute},
		{"bulk", "暂停所有账号。", ChatIntentDirectAction, "bulk_pause", false, true, 0},
		{"ambiguous account", "账号 测试账号 现在怎么样？", ChatIntentAmbiguous, "clarify", true, false, 0},
		{"vague action", "处理一下账号 123。", ChatIntentAmbiguous, "clarify", true, false, 0},
		{"explicit read only wins", "暂停账号 123 三十分钟，但不要执行动作。", ChatIntentAnalysis, "analyze", true, false, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			intent := classifyChatIntent(context.Background(), test.message, now, resolver)
			if err := intent.Validate(); err != nil {
				t.Fatalf("intent validation failed: %v (%+v)", err, intent)
			}
			if intent.IntentType != test.intentType || intent.Operation != test.operation || intent.ReadOnly != test.readOnly || intent.RequiresConfirmation != test.confirm {
				t.Fatalf("unexpected intent: %+v", intent)
			}
			if test.ttl > 0 && time.Duration(intent.DurationSeconds)*time.Second != test.ttl {
				t.Fatalf("unexpected ttl: %s", time.Duration(intent.DurationSeconds)*time.Second)
			}
		})
	}
}

func TestChatIntentScheduledActionIsTypedAndDoesNotRunImmediately(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	intent := classifyChatIntent(context.Background(), "今晚两点将令牌 X 切换到备用组。", now,
		chatIntentResolverStub{accounts: []int64{123}, token: "X"})
	if intent.ScheduledAt == nil || intent.Timezone != model.AgentDefaultTimezone || intent.DesiredState.TargetTier != model.GroupTierBackup {
		t.Fatalf("scheduled intent was not typed: %+v", intent)
	}
	if !onlyCapability(intent.AllowedCapabilities, "schedule_command") {
		t.Fatalf("scheduled action could execute immediately: %+v", intent.AllowedCapabilities)
	}
}

func TestChatIntentAutonomousActionHasBoundedTTL(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	intent := classifyChatIntent(context.Background(), "你自己处理账号 123 的异常。", now,
		chatIntentResolverStub{accounts: []int64{123}})
	if !intent.Autonomous || intent.ExpiresAt == nil || intent.ExpiresAt.Sub(now) != 15*time.Minute {
		t.Fatalf("autonomous ttl missing: %+v", intent)
	}
	if err := intent.Validate(); err != nil {
		t.Fatal(err)
	}
}
