package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/health"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestSafetyShortLaggingHealthySummaryAdvancesRecoveryStage(t *testing.T) {
	engine, _, _ := newEngineTest(t, false)
	now := time.Now().UTC()
	stageStarted := now.Add(-6 * time.Minute)
	binding := safetyRegressionBinding(now, model.StatusOperational, model.PhaseHealthy)
	control := model.AccountControl{
		AccountID:          binding.Account.ID,
		OwnsLoadFactor:     true,
		LoadStage:          model.HealthStageRecovering25,
		RecoveryStep:       1,
		RecoveryStartedAt:  &stageStarted,
		ExpectedLoadFactor: intPointerForSafetyRegression(25),
	}

	engine.advanceV3HealthControl(context.Background(), &binding, &control, model.Settings{HealthTrialMinutes: 5}, now)

	if control.LoadStage != model.HealthStageRecovering50 || control.RecoveryStep != 2 {
		t.Fatalf("短时滞后的健康证据应推进到 50%% 恢复阶段: %+v", control)
	}
}

func TestSafetyLatestFailedSummaryDoesNotAdvanceLaggingRecovery(t *testing.T) {
	engine, _, _ := newEngineTest(t, false)
	now := time.Now().UTC()
	stageStarted := now.Add(-20 * time.Minute)
	binding := safetyRegressionBinding(now, model.StatusFailed, model.PhaseUnhealthy)
	control := model.AccountControl{
		AccountID:          binding.Account.ID,
		OwnsLoadFactor:     true,
		LoadStage:          model.HealthStageRecovering25,
		RecoveryStep:       1,
		RecoveryStartedAt:  &stageStarted,
		ExpectedLoadFactor: intPointerForSafetyRegression(25),
	}

	engine.advanceV3HealthControl(context.Background(), &binding, &control, model.Settings{HealthTrialMinutes: 5}, now)

	if control.LoadStage != model.HealthStageRecovering25 || control.RecoveryStep != 1 {
		t.Fatalf("最新摘要失败时不得用旧健康决策推进恢复: %+v", control)
	}
}

func TestSafetyRecoveryThresholdUsesStrongerFlapRequirement(t *testing.T) {
	engine, _, _ := newEngineTest(t, false)
	now := time.Now().UTC()
	binding := safetyRegressionBinding(now, model.StatusOperational, model.PhaseHealthy)
	control := model.AccountControl{
		AccountID:            binding.Account.ID,
		HealthLocked:         true,
		FlapActive:           true,
		FlapRecoveryRequired: 8,
		LoadStage:            model.HealthStageQuarantined,
	}
	binding.RecoveryThreshold = effectiveRecoveryThreshold(3, control)
	if binding.RecoveryThreshold != 8 {
		t.Fatalf("恢复门槛应取 max(3, 8)，实际为 %d", binding.RecoveryThreshold)
	}

	binding.Decision.RecoverySuccessStreak = 7
	engine.advanceV3HealthControl(context.Background(), &binding, &control, model.Settings{HealthTrialMinutes: 5}, now)
	if !control.HealthLocked {
		t.Fatal("连续成功 7 次时不应越过 8 次抖动保护门槛")
	}

	binding.Decision.RecoverySuccessStreak = 8
	engine.advanceV3HealthControl(context.Background(), &binding, &control, model.Settings{HealthTrialMinutes: 5}, now)
	if control.HealthLocked || control.LoadStage != model.HealthStageRecovering25 {
		t.Fatalf("连续成功达到 8 次后应解除健康锁并进入试运行: %+v", control)
	}
}

func TestSafetyManualPausedAccountWithoutOwnershipDoesNotWriteLoad(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.HealthMode = model.HealthModeAdaptive
	if err := database.UpdateSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	originalLoad := 100
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &originalLoad
	api.mu.Unlock()

	checkedAt := time.Now().UTC()
	api.setMonitorHealth(model.StatusDegraded, 20_000, checkedAt)
	if _, err := database.InsertMonitorHistory(ctx, model.MonitorHistoryRecord{
		SourceID: 1, MonitorID: 2, Model: "gpt", Status: model.StatusDegraded,
		LatencyMS: 20_000, CheckedAt: checkedAt, IngestedAt: checkedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}

	if len(api.loadActions) != 0 {
		t.Fatalf("人工暂停且无调度器暂停归属时不得写负载: %v", api.loadActions)
	}
	control, err := database.GetControl(ctx, 225)
	if err != nil {
		t.Fatal(err)
	}
	if control.OwnsLoadFactor {
		t.Fatalf("人工暂停账号不应被调度器取得负载归属: %+v", control)
	}
}

func TestSafetySchedulerOwnedPauseStillResumesNormally(t *testing.T) {
	ctx := context.Background()
	engine, database, api := newEngineTest(t, false)
	settings, err := database.GetSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.HealthMode = model.HealthModeAdaptive
	settings.HealthTrialMinutes = 5

	now := time.Now().UTC()
	load := 100
	api.mu.Lock()
	api.accounts[0].Schedulable = false
	api.accounts[0].Concurrency = 100
	api.accounts[0].LoadFactor = &load
	account := api.accounts[0]
	api.mu.Unlock()

	falseValue := false
	binding := safetyRegressionBinding(now, model.StatusOperational, model.PhaseHealthy)
	binding.Account = account
	binding.Control = model.AccountControl{
		AccountID:           account.ID,
		MonitorID:           int64PointerForSafetyRegression(2),
		OwnsPause:           true,
		Owner:               "automatic",
		ExpectedSchedulable: &falseValue,
		LastObserved:        &falseValue,
		HealthLocked:        true,
		LoadStage:           model.HealthStageQuarantined,
	}
	binding.RecoveryThreshold = 3

	if err := engine.reconcileAccount(ctx, &binding, settings, now); err != nil {
		t.Fatal(err)
	}
	if len(api.actions) != 1 || !api.actions[0] {
		t.Fatalf("调度器拥有暂停归属且健康锁解除后应正常恢复: %v", api.actions)
	}
	control, err := database.GetControl(ctx, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if control.OwnsPause || control.HealthLocked {
		t.Fatalf("恢复后应清除暂停归属和健康锁: %+v", control)
	}
}

func safetyRegressionBinding(now time.Time, summaryStatus, phase string) model.ResolvedBinding {
	monitorCheckedAt := now.Add(-10 * time.Second)
	return model.ResolvedBinding{
		Account: model.Account{
			ID: 225, Name: "account", Platform: "openai", Type: "apikey",
			Status: "active", Schedulable: true, Concurrency: 100,
			Credentials: map[string]any{"base_url": "https://upstream.example/v1"},
		},
		Monitor: &model.Monitor{
			ID: 2, Name: "monitor", Provider: "openai", Endpoint: "https://upstream.example",
			Enabled: true, IntervalSeconds: 60, LastCheckedAt: &monitorCheckedAt, PrimaryStatus: summaryStatus,
		},
		State: "bound",
		MonitorState: model.MonitorState{
			MonitorID: 2, LastCheckedAt: &monitorCheckedAt, LastStatus: summaryStatus,
			HealthyStreak: 8, Phase: phase,
		},
		Decision: &model.HealthDecision{
			RecoverySuccessStreak: 8,
			SuggestedLoadPercent:  100,
			Action:                health.RecommendationFull,
			CheckedAt:             now.Add(-50 * time.Second),
		},
		BaseRecoveryThreshold: 3,
		RecoveryThreshold:     3,
	}
}

func intPointerForSafetyRegression(value int) *int { return &value }

func int64PointerForSafetyRegression(value int64) *int64 { return &value }
