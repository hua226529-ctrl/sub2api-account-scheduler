package health

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestDefaultAndResolvedSettings(t *testing.T) {
	defaults := NormalizeSettings(model.Settings{})
	if defaults.HealthMode != ModeObserve || defaults.HealthHealthyScore != 80 || defaults.HealthWatchScore != 60 || defaults.HealthQuarantineScore != 35 {
		t.Fatalf("unexpected default score settings: %+v", defaults)
	}
	if defaults.HealthMinSamples != 10 || defaults.HealthLatencyWarningMS != 8_000 || defaults.HealthLatencyCriticalMS != 15_000 {
		t.Fatalf("unexpected default sample/latency settings: %+v", defaults)
	}
	if defaults.HealthRecoveryWindow != 10 || defaults.HealthRecoverySuccesses != 8 || defaults.HealthTrialPercent != 25 || defaults.HealthMidPercent != 50 || defaults.HealthDegradedPercent != 50 {
		t.Fatalf("unexpected default recovery/load settings: %+v", defaults)
	}

	healthy, watch, quarantine, minimum := 85, 65, 40, 12
	warning, critical := int64(6_000), int64(12_000)
	resolved := ResolveSettings(defaults, model.Policy{
		HealthHealthyScore:      &healthy,
		HealthWatchScore:        &watch,
		HealthQuarantineScore:   &quarantine,
		HealthMinSamples:        &minimum,
		HealthLatencyWarningMS:  &warning,
		HealthLatencyCriticalMS: &critical,
	})
	if resolved.HealthHealthyScore != 85 || resolved.HealthWatchScore != 65 || resolved.HealthQuarantineScore != 40 || resolved.HealthMinSamples != 12 || resolved.HealthLatencyWarningMS != 6_000 || resolved.HealthLatencyCriticalMS != 12_000 {
		t.Fatalf("policy overrides were not applied: %+v", resolved)
	}
}

func TestMonitorAndAccountDecodeHealthFields(t *testing.T) {
	payload := `{
		"id":2,"enabled":true,"primary_status":"operational","primary_latency_ms":1234,
		"availability_7d":98.75,"api_key_decrypt_failed":true,
		"extra_models":[{"model":"备用模型","status":"degraded","latency_ms":2500}]
	}`
	var monitor model.Monitor
	if err := json.Unmarshal([]byte(payload), &monitor); err != nil {
		t.Fatal(err)
	}
	if monitor.PrimaryLatencyMS != 1234 || monitor.Availability7D == nil || *monitor.Availability7D != 98.75 || !monitor.DecryptFailed || len(monitor.ExtraModels) != 1 || monitor.ExtraModels[0].Status != model.StatusDegraded {
		t.Fatalf("health fields not decoded: %+v", monitor)
	}

	accountPayload := `{
		"id":225,"concurrency":30,"load_factor":15,"priority":7,"credential_status":"valid",
		"rate_limit_reset_at":"2026-07-14T01:02:03Z","overload_until":"2026-07-14T01:03:03Z",
		"temp_unschedulable_until":"2026-07-14T01:04:03Z"
	}`
	var account model.Account
	if err := json.Unmarshal([]byte(accountPayload), &account); err != nil {
		t.Fatal(err)
	}
	if account.Concurrency != 30 || account.LoadFactor == nil || *account.LoadFactor != 15 || account.Priority != 7 || account.CredentialStatus != "valid" || account.RateLimitResetAt == nil || account.OverloadUntil == nil || account.TempUnschedulableUntil == nil {
		t.Fatalf("account scheduling fields not decoded: %+v", account)
	}
}

func TestEvaluateFrozenMonitors(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	checked := now.Add(-time.Minute)
	tests := []struct {
		name    string
		monitor model.Monitor
		reason  string
	}{
		{name: "关闭", monitor: model.Monitor{Enabled: false, LastCheckedAt: &checked, PrimaryStatus: model.StatusOperational}, reason: "监控已关闭"},
		{name: "无数据", monitor: model.Monitor{Enabled: true, PrimaryStatus: model.StatusOperational}, reason: "暂无检测数据"},
		{name: "失联", monitor: model.Monitor{Enabled: true, IntervalSeconds: 60, LastCheckedAt: timePointer(now.Add(-4 * time.Minute)), PrimaryStatus: model.StatusOperational}, reason: "长时间未更新"},
		{name: "未知", monitor: model.Monitor{Enabled: true, LastCheckedAt: &checked, PrimaryStatus: "pending"}, reason: "状态未知"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, observation := Evaluate(test.monitor, nil, model.MonitorHealthState{}, model.Settings{}, now)
			if state.Stage != model.HealthStageFrozen || !observation.CheckedAt.IsZero() || !strings.Contains(state.NextRecoveryCondition, test.reason) {
				t.Fatalf("unexpected frozen result: state=%+v observation=%+v", state, observation)
			}
		})
	}
}

func TestEvaluateStatusScoring(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	history := observations(now, 10, model.StatusOperational, 1_000)
	tests := []struct {
		status    string
		wantScore float64
		wantStage string
	}{
		{status: model.StatusOperational, wantScore: 100, wantStage: model.HealthStageHealthy},
		{status: model.StatusDegraded, wantScore: 75, wantStage: model.HealthStageWatch},
		{status: model.StatusFailed, wantScore: 38, wantStage: model.HealthStageDegraded},
		{status: model.StatusError, wantScore: 28, wantStage: model.HealthStageQuarantined},
	}
	for _, test := range tests {
		t.Run(test.status, func(t *testing.T) {
			monitor := currentMonitor(now, test.status, 1_000)
			state, observation := Evaluate(monitor, history, model.MonitorHealthState{}, model.Settings{}, now)
			if observation.Score != test.wantScore || state.Stage != test.wantStage {
				t.Fatalf("score/stage = %.0f/%s, want %.0f/%s; reasons=%s", observation.Score, state.Stage, test.wantScore, test.wantStage, observation.ReasonJSON)
			}
		})
	}
}

func TestEvaluateConfidenceAndSevereSignals(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	t.Run("样本不足不会只凭低分隔离", func(t *testing.T) {
		monitor := currentMonitor(now, model.StatusError, 20_000)
		state, _ := Evaluate(monitor, nil, model.MonitorHealthState{}, model.Settings{}, now)
		if state.Confidence >= 1 || state.Stage != model.HealthStageDegraded {
			t.Fatalf("unexpected low confidence state: %+v", state)
		}
	})

	t.Run("连续两次失败直接隔离", func(t *testing.T) {
		history := []model.MonitorObservation{{MonitorID: 2, CheckedAt: now.Add(-time.Minute), Status: model.StatusFailed, LatencyMS: 1_000, Score: 40}}
		state, _ := Evaluate(currentMonitor(now, model.StatusError, 1_000), history, model.MonitorHealthState{}, model.Settings{}, now)
		if state.Stage != model.HealthStageQuarantined || state.HoldUntil == nil || state.HoldUntil.Sub(now) != 5*time.Minute {
			t.Fatalf("severe failures should quarantine: %+v", state)
		}
	})

	t.Run("密钥解密失败直接隔离", func(t *testing.T) {
		monitor := currentMonitor(now, model.StatusOperational, 1_000)
		monitor.DecryptFailed = true
		state, observation := Evaluate(monitor, nil, model.MonitorHealthState{}, model.Settings{}, now)
		if state.Stage != model.HealthStageQuarantined || observation.Score != 0 || !strings.Contains(observation.ReasonJSON, "密钥解密失败") {
			t.Fatalf("decrypt failure should quarantine: %+v %+v", state, observation)
		}
	})
}

func TestEvaluateCountsEachCheckedAtOnce(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	checked := now.Add(-time.Minute)
	history := make([]model.MonitorObservation, 10)
	for i := range history {
		history[i] = model.MonitorObservation{MonitorID: 2, CheckedAt: checked, Status: model.StatusOperational, LatencyMS: 1_000, Score: 100}
	}
	state, _ := Evaluate(currentMonitor(now, model.StatusOperational, 1_000), history, model.MonitorHealthState{}, model.Settings{}, now)
	if state.SampleCount != 2 || state.Confidence != 0.2 {
		t.Fatalf("duplicate checked_at values must count once: %+v", state)
	}
}

func TestEvaluateLatencyExtraModelsAndLongAvailability(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	history := observations(now, 10, model.StatusOperational, 1_000)
	tests := []struct {
		name      string
		mutate    func(*model.Monitor)
		wantScore float64
		wantText  string
	}{
		{
			name:      "动态基准三倍",
			mutate:    func(m *model.Monitor) { m.PrimaryLatencyMS = 3_000 },
			wantScore: 70,
			wantText:  "正常基准三倍",
		},
		{
			name: "附加模型异常",
			mutate: func(m *model.Monitor) {
				m.ExtraModels = []model.MonitorModelStatus{{Status: model.StatusDegraded}, {Status: model.StatusFailed}}
			},
			wantScore: 72,
			wantText:  "附加模型存在异常",
		},
		{
			name:      "长期可用率只轻度扣分",
			mutate:    func(m *model.Monitor) { value := 70.0; m.Availability7D = &value },
			wantScore: 90,
			wantText:  "七天可用率低于80%",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			monitor := currentMonitor(now, model.StatusOperational, 1_000)
			test.mutate(&monitor)
			state, observation := Evaluate(monitor, history, model.MonitorHealthState{}, model.Settings{}, now)
			if observation.Score != test.wantScore || !strings.Contains(observation.ReasonJSON, test.wantText) {
				t.Fatalf("score/reasons = %.0f/%s, state=%+v", observation.Score, observation.ReasonJSON, state)
			}
		})
	}
}

func TestCheckRecoverySignals(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	settings := NormalizeSettings(model.Settings{})
	// Fifteen-minute availability remains at 90% while the latest ten results
	// contain the exact minimum of eight successes.
	history := make([]model.MonitorObservation, 0, 10)
	for i := 0; i < 10; i++ {
		history = append(history, model.MonitorObservation{MonitorID: 2, CheckedAt: now.Add(-15*time.Minute + time.Duration(i)*30*time.Second), Status: model.StatusOperational, LatencyMS: 1_000, Score: 100})
	}
	latest := make([]model.MonitorObservation, 0, 10)
	statuses := []string{model.StatusFailed, model.StatusFailed, model.StatusOperational, model.StatusOperational, model.StatusOperational, model.StatusOperational, model.StatusOperational, model.StatusOperational, model.StatusOperational, model.StatusOperational}
	for i, status := range statuses {
		latest = append(latest, model.MonitorObservation{MonitorID: 2, CheckedAt: now.Add(time.Duration(i-9) * time.Minute), Status: status, LatencyMS: 1_000, Score: 100})
	}
	all := append(history, latest...)
	result := CheckRecovery(all, model.MonitorHealthState{}, nil, 1_000, settings, now)
	if !result.Eligible || result.HealthyCount != 8 || !result.LastTwoHealthy || !result.AvailabilityOK || !result.LatencyOK {
		t.Fatalf("expected eligible recovery: %+v", result)
	}

	t.Run("七天可用率低时要求十次全部正常", func(t *testing.T) {
		availability7D := 75.0
		result := CheckRecovery(all, model.MonitorHealthState{}, &availability7D, 1_000, settings, now)
		if result.Eligible || result.RequiredHealthy != 10 || !strings.Contains(result.NextCondition, "正常次数不足") {
			t.Fatalf("unexpected strict recovery result: %+v", result)
		}
	})

	t.Run("隔离保持时间阻止提前恢复", func(t *testing.T) {
		hold := now.Add(time.Minute)
		result := CheckRecovery(all, model.MonitorHealthState{HoldUntil: &hold}, nil, 1_000, settings, now)
		if result.Eligible || result.QuarantineElapsed || !strings.Contains(result.NextCondition, "隔离时间") {
			t.Fatalf("unexpected hold result: %+v", result)
		}
	})

	t.Run("末两次必须连续正常", func(t *testing.T) {
		changed := append([]model.MonitorObservation(nil), all...)
		changed[len(changed)-1].Status = model.StatusFailed
		result := CheckRecovery(changed, model.MonitorHealthState{}, nil, 1_000, settings, now)
		if result.Eligible || result.LastTwoHealthy || !strings.Contains(result.NextCondition, "正常次数不足") && !strings.Contains(result.NextCondition, "连续正常") {
			t.Fatalf("unexpected last-two result: %+v", result)
		}
	})
}

func TestEvaluateStagedRecovery(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	history := observations(now, 20, model.StatusOperational, 1_000)
	settings := NormalizeSettings(model.Settings{})
	previous := model.MonitorHealthState{MonitorID: 2, Stage: model.HealthStageQuarantined, LastTransitionAt: now.Add(-10 * time.Minute), HoldUntil: timePointer(now.Add(-time.Minute))}

	state, current := Evaluate(currentMonitor(now, model.StatusOperational, 1_000), history, previous, settings, now)
	if state.Stage != model.HealthStageRecovering25 || !state.RecoveryEligible {
		t.Fatalf("expected first recovery stage: %+v", state)
	}

	history = append(history, current)
	state.LastTransitionAt = now.Add(-settingsDuration(settings))
	state, _ = Evaluate(currentMonitor(now.Add(time.Minute), model.StatusOperational, 1_000), history, state, settings, now.Add(time.Minute))
	if state.Stage != model.HealthStageRecovering50 {
		t.Fatalf("expected second recovery stage: %+v", state)
	}

	history = append(history, model.MonitorObservation{MonitorID: 2, CheckedAt: now.Add(2 * time.Minute), Status: model.StatusDegraded, LatencyMS: 9_000, Score: 50})
	state.LastTransitionAt = now.Add(-settingsDuration(settings))
	state, _ = Evaluate(currentMonitor(now.Add(3*time.Minute), model.StatusDegraded, 9_000), history, state, settings, now.Add(3*time.Minute))
	if state.Stage != model.HealthStageRecovering25 {
		t.Fatalf("degraded recovery should fall back to 25 percent stage: %+v", state)
	}
}

func TestEvaluateRecoveryWithSmallConfiguredWindow(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	settings := NormalizeSettings(model.Settings{HealthMinSamples: 3, HealthRecoveryWindow: 4, HealthRecoverySuccesses: 3})
	hold := now.Add(-time.Minute)
	previous := model.MonitorHealthState{MonitorID: 2, Stage: model.HealthStageQuarantined, HoldUntil: &hold, LastTransitionAt: now.Add(-10 * time.Minute)}
	history := make([]model.MonitorObservation, 0, 4)
	for i := 0; i < 4; i++ {
		checked := now.Add(time.Duration(i) * time.Second)
		state, observation := Evaluate(currentMonitor(checked, model.StatusOperational, 1_000), history, previous, settings, checked)
		history = append(history, observation)
		previous = state
	}
	if previous.Stage != model.HealthStageRecovering25 || !previous.RecoveryEligible {
		t.Fatalf("small recovery window should enter first trial stage: %+v", previous)
	}
}

func TestRepeatedObservationDoesNotAdvanceTimers(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	checked := now.Add(-time.Minute)
	t.Run("不重复延长隔离", func(t *testing.T) {
		hold := now.Add(2 * time.Minute)
		previous := model.MonitorHealthState{MonitorID: 2, Stage: model.HealthStageQuarantined, LastTransitionAt: now.Add(-3 * time.Minute), HoldUntil: &hold}
		history := []model.MonitorObservation{
			{MonitorID: 2, CheckedAt: checked.Add(-time.Minute), Status: model.StatusFailed, LatencyMS: 1_000, Score: 40},
			{MonitorID: 2, CheckedAt: checked, Status: model.StatusError, LatencyMS: 1_000, Score: 20},
		}
		monitor := currentMonitor(checked, model.StatusError, 1_000)
		state, _ := Evaluate(monitor, history, previous, model.Settings{}, now)
		if state.HoldUntil == nil || !state.HoldUntil.Equal(hold) {
			t.Fatalf("duplicate result extended quarantine: %+v", state)
		}
	})

	t.Run("不重复推进试运行阶段", func(t *testing.T) {
		history := observations(now, 20, model.StatusOperational, 1_000)
		history = append(history, model.MonitorObservation{MonitorID: 2, CheckedAt: checked, Status: model.StatusOperational, LatencyMS: 1_000, Score: 100})
		previous := model.MonitorHealthState{MonitorID: 2, Stage: model.HealthStageRecovering25, LastTransitionAt: now.Add(-10 * time.Minute)}
		state, _ := Evaluate(currentMonitor(checked, model.StatusOperational, 1_000), history, previous, model.Settings{}, now)
		if state.Stage != model.HealthStageRecovering25 {
			t.Fatalf("duplicate result advanced recovery: %+v", state)
		}
	})
}

func TestWindowAvailabilityUsesDegradedAsHalfSuccess(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	items := []model.MonitorObservation{
		{CheckedAt: now.Add(-3 * time.Minute), Status: model.StatusOperational},
		{CheckedAt: now.Add(-2 * time.Minute), Status: model.StatusDegraded},
		{CheckedAt: now.Add(-time.Minute), Status: model.StatusFailed},
		{CheckedAt: now, Status: model.StatusError},
	}
	got, count := availability(items, now.Add(-15*time.Minute))
	if count != 4 || math.Abs(got-37.5) > 0.001 {
		t.Fatalf("availability = %.2f/%d, want 37.5/4", got, count)
	}
}

func currentMonitor(now time.Time, status string, latency int64) model.Monitor {
	return model.Monitor{
		ID:               2,
		Enabled:          true,
		IntervalSeconds:  60,
		LastCheckedAt:    timePointer(now),
		PrimaryStatus:    status,
		PrimaryLatencyMS: latency,
	}
}

func observations(now time.Time, count int, status string, latency int64) []model.MonitorObservation {
	items := make([]model.MonitorObservation, 0, count)
	for i := count; i > 0; i-- {
		items = append(items, model.MonitorObservation{
			MonitorID: 2,
			CheckedAt: now.Add(-time.Duration(i) * time.Minute),
			Status:    status,
			LatencyMS: latency,
			Score:     100,
		})
	}
	return items
}

func timePointer(value time.Time) *time.Time { return &value }

func settingsDuration(settings model.Settings) time.Duration {
	return time.Duration(settings.HealthTrialMinutes)*time.Minute + time.Second
}
