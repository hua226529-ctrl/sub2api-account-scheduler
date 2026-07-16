package health

import (
	"math"
	"testing"
	"time"
)

func TestClassifyCheckV3(t *testing.T) {
	tests := []struct {
		name    string
		check   Check
		class   SignalClass
		hard    bool
		counted bool
		success bool
	}{
		{"测试答案不匹配", Check{Status: "failed", Message: "challenge answer mismatch"}, SignalSemanticMismatch, false, true, true},
		{"四零一凭据故障", Check{HTTPStatus: 401}, SignalCredentialFailure, true, true, false},
		{"四零三凭据故障", Check{HTTPStatus: 403}, SignalCredentialFailure, true, true, false},
		{"限流", Check{HTTPStatus: 429}, SignalCapacityLimited, false, true, false},
		{"五零二", Check{HTTPStatus: 502}, SignalInfrastructureFailure, true, true, false},
		{"五零三", Check{HTTPStatus: 503}, SignalInfrastructureFailure, true, true, false},
		{"五二零", Check{HTTPStatus: 520}, SignalInfrastructureFailure, true, true, false},
		{"五二二", Check{HTTPStatus: 522}, SignalInfrastructureFailure, true, true, false},
		{"连接超时", Check{Status: "error", Message: "upstream request timeout"}, SignalInfrastructureFailure, true, true, false},
		{"上下文过长", Check{Status: "failed", Message: "maximum context window exceeded"}, SignalClientError, false, false, false},
		{"参数错误", Check{HTTPStatus: 400, Message: "invalid parameter: temperature"}, SignalClientError, false, false, false},
		{"模型不存在", Check{HTTPStatus: 404, Message: "model not found"}, SignalModelUnsupported, false, false, false},
		{"没有可用渠道", Check{Status: "failed", Message: "no available channel for model"}, SignalModelUnsupported, false, false, false},
		{"性能慢", Check{Status: "operational", LatencyMS: 8_000}, SignalPerformanceSlow, false, true, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ClassifyCheck(test.check)
			if got.Class != test.class || got.HardFailure != test.hard || got.CountedInAvailability != test.counted || got.AvailabilitySuccess != test.success {
				t.Fatalf("classification = %+v", got)
			}
			if got.ReasonCode == "" || got.Explanation == "" {
				t.Fatalf("classification must be auditable: %+v", got)
			}
		})
	}
}

func TestEvaluateV3SemanticMismatchCorrectedByRealTraffic(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	checks := makeV3Checks(now, 50, "operational", "", 1_000)
	checks = append(checks, makeV3Checks(now.Add(50*time.Minute), 10, "failed", "测试答案不匹配", 1_000)...)

	result := EvaluateV3(V3Input{Checks: checks, BaselineLatencyMS: 1_000, RealTraffic: RealTraffic{SampleCount: 1_000, Successes: 998}})
	if result.Pause || result.SuggestedLoadPercent != 100 || !result.Disagreement {
		t.Fatalf("healthy real traffic must override semantic mismatch: %+v", result)
	}
	if result.HardSuccessRate60 != 100 || result.ErrorCategoryCounts[SignalSemanticMismatch] != 10 {
		t.Fatalf("semantic mismatches are hard-availability successes: %+v", result)
	}
}

func TestEvaluateV3SemanticMismatchNeverPausesAlone(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	result := EvaluateV3(V3Input{Checks: makeV3Checks(now, 10, "failed", "challenge mismatch", 1_000)})
	if result.Pause || result.SuggestedLoadPercent != 80 {
		t.Fatalf("semantic mismatch should only enter observation: %+v", result)
	}
}

func TestEvaluateV3YellowResultsAreSuccessfulAndLatencyIsNotDoubleCounted(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	checks := makeV3Checks(now, 32, "operational", "", 2_000)
	checks = append(checks, makeV3Checks(now.Add(32*time.Minute), 28, "degraded", "", 9_000)...)
	result := EvaluateV3(V3Input{Checks: checks, BaselineLatencyMS: 2_000})

	// P90=9s: absolute penalty 8, relative penalty 14. Only the larger one is
	// applied, and degraded does not become a half-success or another 25 points.
	if result.Pause || result.P90LatencyMS != 9_000 || result.LatencyPenalty != 14 || result.QualityScore != 86 {
		t.Fatalf("unexpected yellow evaluation: %+v", result)
	}
	if result.HardSuccessRate60 != 100 || result.DegradedRate60 < 46 || result.SuggestedLoadPercent != 80 {
		t.Fatalf("yellow must remain available but reduce exposure: %+v", result)
	}
}

func TestEvaluateV3RealInfrastructureFailureAndTrafficCorrection(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	checks := makeV3Checks(now, 7, "operational", "", 2_000)
	checks = append(checks,
		Check{CheckedAt: now.Add(8 * time.Minute), Status: "error", HTTPStatus: 502},
		Check{CheckedAt: now.Add(9 * time.Minute), Status: "error", Message: "connection timeout"},
		Check{CheckedAt: now.Add(10 * time.Minute), Status: "error", HTTPStatus: 522},
	)

	t.Run("示例账号真实故障触发暂停", func(t *testing.T) {
		result := EvaluateV3(V3Input{Checks: checks, RealTraffic: RealTraffic{SampleCount: 40, Successes: 34}})
		if !result.Pause || result.Action != RecommendationPause || result.HardFailureStreak != 3 {
			t.Fatalf("real infrastructure failures should pause: %+v", result)
		}
	})

	t.Run("真实流量健康抑制监控暂停", func(t *testing.T) {
		result := EvaluateV3(V3Input{Checks: checks, RealTraffic: RealTraffic{SampleCount: 40, Successes: 39}})
		if result.Pause || !result.MonitorPauseSuppressed || !result.Disagreement || result.SuggestedLoadPercent != 25 {
			t.Fatalf("healthy traffic should suppress monitor-only pause: %+v", result)
		}
	})
}

func TestEvaluateV3ClientErrorsAndUnsupportedModelsAreExcluded(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	checks := makeV3Checks(now, 5, "operational", "", 1_000)
	checks = append(checks,
		Check{CheckedAt: now.Add(6 * time.Minute), Status: "error", Message: "context window exceeded"},
		Check{CheckedAt: now.Add(7 * time.Minute), Status: "error", Message: "invalid parameter"},
		Check{CheckedAt: now.Add(8 * time.Minute), Status: "error", Message: "model not found"},
		Check{CheckedAt: now.Add(9 * time.Minute), Status: "error", Message: "no available channel"},
	)
	result := EvaluateV3(V3Input{Checks: checks})
	if result.Pause || result.Availability.Considered != 5 || result.Availability.SuccessRate != 100 {
		t.Fatalf("caller/capability errors must be excluded: %+v", result)
	}
	if result.Availability.ClientErrorsExcluded != 2 || result.Availability.UnsupportedModelsExcluded != 2 {
		t.Fatalf("unexpected excluded counts: %+v", result.Availability)
	}
}

func TestEvaluateV3CredentialsAndUnhealthyTrafficPause(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	t.Run("最新凭据故障立即暂停", func(t *testing.T) {
		checks := makeV3Checks(now, 9, "operational", "", 1_000)
		checks = append(checks, Check{CheckedAt: now.Add(10 * time.Minute), HTTPStatus: 401})
		result := EvaluateV3(V3Input{Checks: checks, RealTraffic: RealTraffic{SampleCount: 100, Successes: 100}})
		if !result.Pause {
			t.Fatalf("credential failure cannot be overridden: %+v", result)
		}
	})

	t.Run("真实业务低于八十强化暂停", func(t *testing.T) {
		result := EvaluateV3(V3Input{Checks: makeV3Checks(now, 10, "operational", "", 1_000), RealTraffic: RealTraffic{SampleCount: 20, Successes: 15}})
		if !result.Pause || !hasV3Reason(result, "real_traffic_unhealthy") {
			t.Fatalf("unhealthy real traffic should pause: %+v", result)
		}
	})
}

func TestEvaluateV3DeduplicatesCheckedAt(t *testing.T) {
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	checks := []Check{
		{CheckedAt: now, Status: "error", HTTPStatus: 502},
		{CheckedAt: now, Status: "operational", LatencyMS: 1_000},
	}
	result := EvaluateV3(V3Input{Checks: checks})
	if len(result.Checks) != 1 || result.Availability.Considered != 1 || math.Abs(result.HardSuccessRate10-100) > 0.001 {
		t.Fatalf("same checked_at must count once: %+v", result)
	}
}

func makeV3Checks(start time.Time, count int, status, message string, latency int64) []Check {
	checks := make([]Check, 0, count)
	for index := 0; index < count; index++ {
		checks = append(checks, Check{
			CheckedAt: start.Add(time.Duration(index) * time.Minute),
			Status:    status,
			Message:   message,
			LatencyMS: latency,
		})
	}
	return checks
}

func hasV3Reason(result V3Result, code string) bool {
	for _, reason := range result.Reasons {
		if reason.Code == code {
			return true
		}
	}
	return false
}
