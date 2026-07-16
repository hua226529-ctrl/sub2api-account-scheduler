package health

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SignalClass describes what a monitor result actually proves. It deliberately
// separates availability failures from slow but successful responses.
type SignalClass string

const (
	SignalOperational           SignalClass = "operational"
	SignalPerformanceSlow       SignalClass = "performance_slow"
	SignalCredentialFailure     SignalClass = "credential_failure"
	SignalInfrastructureFailure SignalClass = "infrastructure_failure"
	SignalCapacityLimited       SignalClass = "capacity_limited"
	SignalSemanticMismatch      SignalClass = "semantic_mismatch"
	SignalClientError           SignalClass = "client_error"
	SignalModelUnsupported      SignalClass = "model_unsupported"
	SignalUnknown               SignalClass = "unknown"
)

const (
	RecommendationFull     = "full"
	RecommendationReduce80 = "reduce_80"
	RecommendationReduce50 = "reduce_50"
	RecommendationReduce25 = "reduce_25"
	RecommendationPause    = "pause"
)

// Check is the provider-neutral input accepted by the V3 classifier. Checks
// passed to EvaluateV3 should be ordered from oldest to newest. Repeated,
// non-zero CheckedAt values are counted only once.
type Check struct {
	CheckedAt       time.Time
	Status          string
	HTTPStatus      int
	Message         string
	LatencyMS       int64
	DecryptFailed   bool
	SlowThresholdMS int64
}

// ClassifiedCheck contains the stable reason code used by decisions and audit
// records. CountedInAvailability is false for caller mistakes and unsupported
// models because neither proves that the upstream account is unavailable.
type ClassifiedCheck struct {
	Check
	Class                 SignalClass
	ReasonCode            string
	Explanation           string
	HardFailure           bool
	CountedInAvailability bool
	AvailabilitySuccess   bool
}

type RealTraffic struct {
	SampleCount int
	Successes   int
}

func (r RealTraffic) SuccessRate() float64 {
	if r.SampleCount <= 0 {
		return 0
	}
	successes := r.Successes
	if successes < 0 {
		successes = 0
	}
	if successes > r.SampleCount {
		successes = r.SampleCount
	}
	return float64(successes) / float64(r.SampleCount) * 100
}

type V3Input struct {
	Checks                []Check
	BaselineLatencyMS     float64
	LongTermSuccessRate   *float64
	RealTraffic           RealTraffic
	RealTrafficMinSamples int
	HardFailureStreak     int
	HardFailuresInWindow  int
	TrafficPauseBelow     float64
	TrafficHealthyAt      float64
	LatencyWarningMS      int64
	LatencyCriticalMS     int64
	QualityFullAt         float64
	QualityReduce80At     float64
	QualityReduce50At     float64
	PersistentSlowRate    float64
}

type HardAvailability struct {
	Considered                   int
	Successes                    int
	Failures                     int
	SuccessRate                  float64
	InfrastructureFailures       int
	CapacityFailures             int
	CredentialFailures           int
	SemanticMismatches           int
	ClientErrorsExcluded         int
	UnsupportedModelsExcluded    int
	ConsecutiveInfrastructure    int
	InfrastructureFailuresLast10 int
	CapacityFailuresLast10       int
}

type DecisionReason struct {
	Code        string
	Explanation string
}

type V3Result struct {
	Checks                 []ClassifiedCheck
	Availability           HardAvailability
	QualityScore           float64
	P90LatencyMS           int64
	LatencyPenalty         float64
	RecommendedLoad        int
	Recommendation         string
	Pause                  bool
	MonitorPauseSuppressed bool
	RealTrafficSuccessRate float64
	Reasons                []DecisionReason

	HardSuccessRate10    float64
	HardSuccessRate60    float64
	DegradedRate10       float64
	DegradedRate60       float64
	TrafficSuccessRate   float64
	TrafficSampleCount   int
	HardFailureStreak    int
	HardFailures10       int
	SuggestedLoadPercent int
	Action               string
	Disagreement         bool
	ResponseP90MS        int64
	BaselineLatencyMS    float64
	ReasonCodes          []string
	ErrorCategoryCounts  map[SignalClass]int
	CheckedAt            time.Time
}

// ClassifyCheck maps provider-specific messages into scheduler semantics. The
// matching order is important: specific model/client/semantic errors must be
// recognized before a generic failed/error monitor status.
func ClassifyCheck(check Check) ClassifiedCheck {
	result := ClassifiedCheck{Check: check}
	status := strings.ToLower(strings.TrimSpace(check.Status))
	message := strings.ToLower(strings.TrimSpace(check.Message))
	contains := func(values ...string) bool {
		for _, value := range values {
			if strings.Contains(message, value) {
				return true
			}
		}
		return false
	}
	finish := func(class SignalClass, code, explanation string, hard, counted, success bool) ClassifiedCheck {
		result.Class = class
		result.ReasonCode = code
		result.Explanation = explanation
		result.HardFailure = hard
		result.CountedInAvailability = counted
		result.AvailabilitySuccess = success
		return result
	}

	if check.DecryptFailed {
		return finish(SignalCredentialFailure, "credential_decrypt_failed", "监控密钥无法解密", true, true, false)
	}
	if check.HTTPStatus == 401 || check.HTTPStatus == 403 || contains(
		"unauthorized", "forbidden", "authentication failed", "invalid api key", "invalid token",
		"api key invalid", "credential invalid", "认证失败", "鉴权失败", "密钥无效", "令牌无效",
	) {
		return finish(SignalCredentialFailure, "credential_rejected", "上游拒绝凭据", true, true, false)
	}
	if contains(
		"model not found", "model_not_found", "unknown model", "unsupported model", "model is not supported",
		"no available channel", "no channel available", "没有可用渠道", "无可用渠道", "模型不存在", "不支持该模型",
	) {
		return finish(SignalModelUnsupported, "model_unsupported", "该模型不存在或没有可用渠道", false, false, false)
	}
	if contains(
		"challenge mismatch", "answer mismatch", "response mismatch", "unexpected answer", "content mismatch",
		"测试答案不匹配", "答案不匹配", "响应内容不匹配", "内容校验失败",
	) {
		return finish(SignalSemanticMismatch, "semantic_mismatch", "请求已完成但测试内容校验不一致", false, true, true)
	}
	if contains(
		"context window", "context length", "maximum context", "too many tokens", "invalid parameter",
		"invalid_argument", "invalid request parameter", "参数错误", "参数无效", "上下文过长", "超出上下文",
	) {
		return finish(SignalClientError, "client_request_error", "请求参数或上下文不符合上游要求", false, false, false)
	}
	if check.HTTPStatus == 429 || contains(
		"rate limit", "rate_limit", "too many requests", "overloaded", "over capacity", "capacity exceeded",
		"限流", "请求过多", "容量不足", "上游过载",
	) {
		return finish(SignalCapacityLimited, "capacity_limited", "上游限流或容量暂时不足", false, true, false)
	}
	if check.HTTPStatus == 502 || check.HTTPStatus == 503 || check.HTTPStatus == 520 || check.HTTPStatus == 522 ||
		contains("timeout", "timed out", "deadline exceeded", "connection refused", "connection reset", "connection failed", "dial tcp", "eof", "网关错误", "连接失败", "连接被重置", "超时") ||
		messageHasHTTPStatus(message, 502, 503, 520, 522) {
		return finish(SignalInfrastructureFailure, "infrastructure_failure", "上游网络、网关或服务连接失败", true, true, false)
	}
	if check.HTTPStatus >= 500 {
		return finish(SignalInfrastructureFailure, "upstream_server_error", "上游返回服务器错误", true, true, false)
	}
	if check.HTTPStatus >= 400 || status == "client_error" {
		return finish(SignalClientError, "client_request_error", "请求被上游作为客户端错误拒绝", false, false, false)
	}

	slowThreshold := check.SlowThresholdMS
	if slowThreshold <= 0 {
		slowThreshold = 6_000
	}
	if status == "degraded" || status == "performance_degraded" || status == "slow" || check.LatencyMS >= slowThreshold {
		return finish(SignalPerformanceSlow, "performance_slow", "请求成功但响应速度偏慢", false, true, true)
	}
	if status == "operational" || status == "healthy" || status == "success" || status == "ok" {
		return finish(SignalOperational, "operational", "请求正常完成", false, true, true)
	}
	if status == "failed" || status == "error" || status == "unhealthy" {
		return finish(SignalInfrastructureFailure, "monitor_hard_failure", "监控报告未分类的上游失败", true, true, false)
	}
	return finish(SignalUnknown, "unknown_signal", "无法识别监控结果", false, false, false)
}

// EvaluateV3 evaluates hard availability and performance independently. A slow
// result is a successful availability sample. Its latency is penalized exactly
// once through the P90 calculation.
func EvaluateV3(input V3Input) V3Result {
	policy := normalizeV3Policy(input)
	checks := deduplicateChecks(input.Checks)
	classified := make([]ClassifiedCheck, 0, len(checks))
	result := V3Result{RecommendedLoad: 100, Recommendation: RecommendationFull}
	latencies := make([]int64, 0, len(checks))
	for _, check := range checks {
		item := ClassifyCheck(check)
		classified = append(classified, item)
		if item.CountedInAvailability {
			result.Availability.Considered++
			if item.AvailabilitySuccess {
				result.Availability.Successes++
			} else {
				result.Availability.Failures++
			}
		}
		switch item.Class {
		case SignalInfrastructureFailure:
			result.Availability.InfrastructureFailures++
		case SignalCapacityLimited:
			result.Availability.CapacityFailures++
		case SignalCredentialFailure:
			result.Availability.CredentialFailures++
		case SignalSemanticMismatch:
			result.Availability.SemanticMismatches++
		case SignalClientError:
			result.Availability.ClientErrorsExcluded++
		case SignalModelUnsupported:
			result.Availability.UnsupportedModelsExcluded++
		}
		if item.AvailabilitySuccess && check.LatencyMS > 0 {
			latencies = append(latencies, check.LatencyMS)
		}
	}
	result.Checks = classified
	if result.Availability.Considered > 0 {
		result.Availability.SuccessRate = float64(result.Availability.Successes) / float64(result.Availability.Considered) * 100
	}
	result.Availability.ConsecutiveInfrastructure = trailingClassCount(classified, SignalInfrastructureFailure)
	last10 := classified
	if len(last10) > 10 {
		last10 = last10[len(last10)-10:]
	}
	for _, item := range last10 {
		switch item.Class {
		case SignalInfrastructureFailure:
			result.Availability.InfrastructureFailuresLast10++
		case SignalCapacityLimited:
			result.Availability.CapacityFailuresLast10++
		}
	}

	result.P90LatencyMS = percentile90(latencies)
	absolutePenalty, absoluteReason := absoluteLatencyPenalty(result.P90LatencyMS, policy.latencyWarningMS, policy.latencyCriticalMS)
	relativePenalty, relativeReason := relativeLatencyPenalty(result.P90LatencyMS, input.BaselineLatencyMS)
	result.LatencyPenalty = math.Max(absolutePenalty, relativePenalty)
	result.QualityScore = 100 - result.LatencyPenalty
	if result.LatencyPenalty > 0 {
		reason := absoluteReason
		if relativePenalty > absolutePenalty {
			reason = relativeReason
		}
		result.Reasons = append(result.Reasons, DecisionReason{Code: "latency_quality_penalty", Explanation: reason})
	}
	// A degraded result without a measurable latency still carries a small
	// quality warning. Measured slow results are already covered by P90 above.
	if result.P90LatencyMS == 0 && containsClass(classified, SignalPerformanceSlow) {
		result.QualityScore -= 4
		result.Reasons = append(result.Reasons, DecisionReason{Code: "slow_without_latency", Explanation: "监控报告性能下降，但没有可用响应时间"})
	}
	if input.LongTermSuccessRate != nil {
		penalty := longTermPenalty(*input.LongTermSuccessRate)
		result.QualityScore -= penalty
		if penalty > 0 {
			result.Reasons = append(result.Reasons, DecisionReason{Code: "long_term_quality", Explanation: "长期成功率仍需观察"})
		}
	}
	result.QualityScore = math.Max(0, math.Min(100, result.QualityScore))
	setLoadFromQuality(&result, policy.qualityFullAt, policy.qualityReduce80At, policy.qualityReduce50At)

	if result.Availability.InfrastructureFailuresLast10 >= 1 {
		capLoad(&result, 50, "recent_hard_failure", "最近十次检测中存在一次基础设施故障")
	}
	if result.Availability.InfrastructureFailuresLast10 >= 2 {
		capLoad(&result, 25, "repeated_hard_failure", "最近十次检测中存在多次基础设施故障")
	}
	if result.Availability.CapacityFailuresLast10 >= 1 {
		capLoad(&result, 50, "capacity_pressure", "最近检测出现上游容量不足")
	}
	if result.Availability.CapacityFailuresLast10 >= 2 {
		capLoad(&result, 25, "repeated_capacity_pressure", "最近十次检测多次出现容量不足")
	}
	// A persistent yellow trend can reduce exposure, but it does not deduct
	// quality points a second time and can never produce a pause by itself.
	if classRate(last10, SignalPerformanceSlow) >= policy.persistentSlowRate || classRate(classifiedWindow(classified, 60), SignalPerformanceSlow) >= policy.persistentSlowRate {
		capLoad(&result, 80, "persistent_performance_slow", "近期性能下降结果较多，先降低流量观察")
	}

	minimumTraffic := input.RealTrafficMinSamples
	if minimumTraffic < policy.minimumTrafficSamples {
		minimumTraffic = policy.minimumTrafficSamples
	}
	realRate := input.RealTraffic.SuccessRate()
	result.RealTrafficSuccessRate = realRate
	hasEnoughTraffic := input.RealTraffic.SampleCount >= minimumTraffic
	latestCredentialFailure := len(classified) > 0 && classified[len(classified)-1].Class == SignalCredentialFailure
	monitorWantsPause := result.Availability.ConsecutiveInfrastructure >= policy.hardFailureStreak ||
		result.Availability.InfrastructureFailuresLast10 >= policy.hardFailuresInWindow

	if latestCredentialFailure {
		pause(&result, "credential_failure_pause", "最新检测确认凭据失效，立即暂停账号")
	} else if hasEnoughTraffic && realRate < policy.trafficPauseBelow {
		pause(&result, "real_traffic_unhealthy", "真实业务请求成功率低于百分之八十")
	} else if monitorWantsPause {
		if hasEnoughTraffic && realRate >= policy.trafficHealthyAt {
			result.MonitorPauseSuppressed = true
			capLoad(&result, 25, "real_traffic_healthy_override", "监控达到暂停门槛，但真实业务成功率不低于百分之九十五")
		} else if result.Availability.ConsecutiveInfrastructure >= policy.hardFailureStreak {
			pause(&result, "consecutive_hard_failures", "连续三次基础设施故障")
		} else {
			pause(&result, "hard_failures_in_window", "最近十次检测中至少五次基础设施故障")
		}
	}

	semanticInLast10 := countClass(last10, SignalSemanticMismatch)
	if !result.Pause && semanticInLast10 > 0 {
		if hasEnoughTraffic && realRate >= policy.trafficHealthyAt {
			result.Reasons = append(result.Reasons, DecisionReason{Code: "semantic_mismatch_overridden", Explanation: "内容校验异常，但真实业务流量健康"})
		} else {
			capLoad(&result, 80, "semantic_mismatch_observe", "内容校验异常仅进入观察，不作为暂停依据")
		}
	}
	if len(result.Reasons) == 0 {
		result.Reasons = append(result.Reasons, DecisionReason{Code: "healthy", Explanation: "硬可用性与性能质量均正常"})
	}
	finalizeV3Result(&result, classified, input)
	return result
}

func finalizeV3Result(result *V3Result, classified []ClassifiedCheck, input V3Input) {
	last10 := classified
	if len(last10) > 10 {
		last10 = last10[len(last10)-10:]
	}
	last60 := classified
	if len(last60) > 60 {
		last60 = last60[len(last60)-60:]
	}
	result.HardSuccessRate10 = hardSuccessRate(last10)
	result.HardSuccessRate60 = hardSuccessRate(last60)
	result.DegradedRate10 = classRate(last10, SignalPerformanceSlow)
	result.DegradedRate60 = classRate(last60, SignalPerformanceSlow)
	result.TrafficSuccessRate = result.RealTrafficSuccessRate
	result.TrafficSampleCount = input.RealTraffic.SampleCount
	result.HardFailureStreak = result.Availability.ConsecutiveInfrastructure
	result.HardFailures10 = result.Availability.InfrastructureFailuresLast10
	result.SuggestedLoadPercent = result.RecommendedLoad
	result.Action = result.Recommendation
	result.Disagreement = result.MonitorPauseSuppressed ||
		(result.Availability.SemanticMismatches > 0 && input.RealTraffic.SampleCount >= maxIntV3(20, input.RealTrafficMinSamples) && result.RealTrafficSuccessRate >= 95)
	result.ResponseP90MS = result.P90LatencyMS
	result.BaselineLatencyMS = input.BaselineLatencyMS
	result.ReasonCodes = make([]string, 0, len(result.Reasons))
	for _, reason := range result.Reasons {
		result.ReasonCodes = append(result.ReasonCodes, reason.Code)
	}
	result.ErrorCategoryCounts = make(map[SignalClass]int)
	for _, check := range classified {
		result.ErrorCategoryCounts[check.Class]++
		if !check.CheckedAt.IsZero() && check.CheckedAt.After(result.CheckedAt) {
			result.CheckedAt = check.CheckedAt
		}
	}
}

func hardSuccessRate(checks []ClassifiedCheck) float64 {
	considered := 0
	successes := 0
	for _, check := range checks {
		if !check.CountedInAvailability {
			continue
		}
		considered++
		if check.AvailabilitySuccess {
			successes++
		}
	}
	if considered == 0 {
		return 0
	}
	return float64(successes) / float64(considered) * 100
}

func classRate(checks []ClassifiedCheck, class SignalClass) float64 {
	considered := 0
	matches := 0
	for _, check := range checks {
		if !check.CountedInAvailability {
			continue
		}
		considered++
		if check.Class == class {
			matches++
		}
	}
	if considered == 0 {
		return 0
	}
	return float64(matches) / float64(considered) * 100
}

func maxIntV3(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func messageHasHTTPStatus(message string, statuses ...int) bool {
	for _, status := range statuses {
		code := strconv.Itoa(status)
		if strings.Contains(message, "http "+code) || strings.Contains(message, "status "+code) || strings.Contains(message, "status code "+code) {
			return true
		}
	}
	return false
}

func deduplicateChecks(checks []Check) []Check {
	result := make([]Check, 0, len(checks))
	positions := make(map[int64]int, len(checks))
	for _, check := range checks {
		if check.CheckedAt.IsZero() {
			result = append(result, check)
			continue
		}
		key := check.CheckedAt.UTC().UnixNano()
		if position, ok := positions[key]; ok {
			result[position] = check
			continue
		}
		positions[key] = len(result)
		result = append(result, check)
	}
	return result
}

func percentile90(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]int64(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := int(math.Ceil(float64(len(ordered))*0.9)) - 1
	if index < 0 {
		index = 0
	}
	return ordered[index]
}

type normalizedV3Policy struct {
	minimumTrafficSamples int
	hardFailureStreak     int
	hardFailuresInWindow  int
	trafficPauseBelow     float64
	trafficHealthyAt      float64
	latencyWarningMS      int64
	latencyCriticalMS     int64
	qualityFullAt         float64
	qualityReduce80At     float64
	qualityReduce50At     float64
	persistentSlowRate    float64
}

func normalizeV3Policy(input V3Input) normalizedV3Policy {
	policy := normalizedV3Policy{
		minimumTrafficSamples: 20, hardFailureStreak: 3, hardFailuresInWindow: 5,
		trafficPauseBelow: 80, trafficHealthyAt: 95, latencyWarningMS: 5_000, latencyCriticalMS: 15_000,
		qualityFullAt: 85, qualityReduce80At: 70, qualityReduce50At: 55, persistentSlowRate: 10,
	}
	if input.RealTrafficMinSamples > 0 {
		policy.minimumTrafficSamples = input.RealTrafficMinSamples
	}
	if input.HardFailureStreak > 0 {
		policy.hardFailureStreak = input.HardFailureStreak
	}
	if input.HardFailuresInWindow > 0 {
		policy.hardFailuresInWindow = input.HardFailuresInWindow
	}
	if input.TrafficPauseBelow > 0 && input.TrafficPauseBelow < 100 {
		policy.trafficPauseBelow = input.TrafficPauseBelow
	}
	if input.TrafficHealthyAt > policy.trafficPauseBelow && input.TrafficHealthyAt <= 100 {
		policy.trafficHealthyAt = input.TrafficHealthyAt
	}
	if input.LatencyWarningMS > 0 {
		policy.latencyWarningMS = input.LatencyWarningMS
	}
	if input.LatencyCriticalMS > policy.latencyWarningMS {
		policy.latencyCriticalMS = input.LatencyCriticalMS
	}
	if input.QualityFullAt > 0 && input.QualityFullAt <= 100 {
		policy.qualityFullAt = input.QualityFullAt
	}
	if input.QualityReduce80At > 0 && input.QualityReduce80At < policy.qualityFullAt {
		policy.qualityReduce80At = input.QualityReduce80At
	}
	if input.QualityReduce50At > 0 && input.QualityReduce50At < policy.qualityReduce80At {
		policy.qualityReduce50At = input.QualityReduce50At
	}
	if input.PersistentSlowRate > 0 && input.PersistentSlowRate <= 100 {
		policy.persistentSlowRate = input.PersistentSlowRate
	}
	return policy
}

func absoluteLatencyPenalty(latency, warning, critical int64) (float64, string) {
	if warning <= 0 {
		warning = 5_000
	}
	if critical <= warning {
		critical = 15_000
	}
	switch {
	case latency <= 0 || latency < warning:
		return 0, ""
	case latency < critical:
		return 4, "九成响应时间超过延迟警戒线"
	case latency < critical*2:
		return 8, "九成响应时间超过延迟严重线"
	case latency < critical*4:
		return 16, "九成响应时间长期高于严重线"
	default:
		return 30, "九成响应时间远高于严重线"
	}
}

func relativeLatencyPenalty(latency int64, baseline float64) (float64, string) {
	if latency <= 0 || baseline <= 0 {
		return 0, ""
	}
	ratio := float64(latency) / baseline
	switch {
	case ratio <= 1.5:
		return 0, ""
	case ratio <= 2:
		return 4, "九成响应时间超过自身基线一点五倍"
	case ratio <= 3:
		return 8, "九成响应时间超过自身基线两倍"
	case ratio <= 5:
		return 14, "九成响应时间超过自身基线三倍"
	case ratio <= 8:
		return 22, "九成响应时间超过自身基线五倍"
	default:
		return 30, "九成响应时间超过自身基线八倍"
	}
}

func longTermPenalty(rate float64) float64 {
	switch {
	case rate >= 99:
		return 0
	case rate >= 97:
		return 1
	case rate >= 95:
		return 2
	case rate >= 90:
		return 5
	case rate >= 80:
		return 8
	default:
		return 10
	}
}

func setLoadFromQuality(result *V3Result, fullAt, reduce80At, reduce50At float64) {
	switch {
	case result.QualityScore >= fullAt:
		result.RecommendedLoad, result.Recommendation = 100, RecommendationFull
	case result.QualityScore >= reduce80At:
		result.RecommendedLoad, result.Recommendation = 80, RecommendationReduce80
	case result.QualityScore >= reduce50At:
		result.RecommendedLoad, result.Recommendation = 50, RecommendationReduce50
	default:
		result.RecommendedLoad, result.Recommendation = 25, RecommendationReduce25
	}
}

func capLoad(result *V3Result, maximum int, code, explanation string) {
	if result.Pause {
		return
	}
	if result.RecommendedLoad > maximum {
		result.RecommendedLoad = maximum
		switch maximum {
		case 80:
			result.Recommendation = RecommendationReduce80
		case 50:
			result.Recommendation = RecommendationReduce50
		default:
			result.Recommendation = RecommendationReduce25
		}
	}
	result.Reasons = append(result.Reasons, DecisionReason{Code: code, Explanation: explanation})
}

func pause(result *V3Result, code, explanation string) {
	result.Pause = true
	result.RecommendedLoad = 0
	result.Recommendation = RecommendationPause
	result.Reasons = append(result.Reasons, DecisionReason{Code: code, Explanation: explanation})
}

func trailingClassCount(checks []ClassifiedCheck, class SignalClass) int {
	count := 0
	for index := len(checks) - 1; index >= 0; index-- {
		if checks[index].Class != class {
			break
		}
		count++
	}
	return count
}

func countClass(checks []ClassifiedCheck, class SignalClass) int {
	count := 0
	for _, check := range checks {
		if check.Class == class {
			count++
		}
	}
	return count
}

func classifiedWindow(checks []ClassifiedCheck, size int) []ClassifiedCheck {
	if size <= 0 || len(checks) <= size {
		return checks
	}
	return checks[len(checks)-size:]
}

func containsClass(checks []ClassifiedCheck, class SignalClass) bool {
	return countClass(checks, class) > 0
}
