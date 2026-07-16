package health

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

const (
	ModeLegacy   = model.HealthModeLegacy
	ModeObserve  = model.HealthModeObserve
	ModeAdaptive = model.HealthModeAdaptive
)

// DefaultSettings returns the conservative defaults used by the adaptive
// health engine. Existing non-health settings are intentionally untouched.
func DefaultSettings() model.Settings {
	return model.Settings{
		HealthMode:                ModeObserve,
		HealthHealthyScore:        80,
		HealthWatchScore:          60,
		HealthQuarantineScore:     35,
		HealthMinSamples:          10,
		HealthLatencyWarningMS:    8_000,
		HealthLatencyCriticalMS:   15_000,
		HealthTrafficPauseBelow:   80,
		HealthTrafficHealthyAt:    95,
		HealthHardFailures10:      5,
		HealthPersistentSlowRate:  40,
		HealthQuarantineMinutes:   5,
		HealthRecoveryWindow:      10,
		HealthRecoverySuccesses:   8,
		HealthTrialPercent:        25,
		HealthMidPercent:          50,
		HealthDegradedPercent:     50,
		HealthTrialMinutes:        5,
		HealthLoadOverrideMinutes: 30,
	}
}

// NormalizeSettings fills health fields that are absent in databases created
// by an older scheduler version and repairs unsafe threshold combinations.
func NormalizeSettings(settings model.Settings) model.Settings {
	defaults := DefaultSettings()
	if settings.HealthMode != ModeLegacy && settings.HealthMode != ModeObserve && settings.HealthMode != ModeAdaptive {
		settings.HealthMode = defaults.HealthMode
	}
	if settings.HealthHealthyScore <= 0 || settings.HealthHealthyScore > 100 {
		settings.HealthHealthyScore = defaults.HealthHealthyScore
	}
	if settings.HealthWatchScore <= 0 || settings.HealthWatchScore >= settings.HealthHealthyScore {
		settings.HealthWatchScore = defaults.HealthWatchScore
	}
	if settings.HealthQuarantineScore <= 0 || settings.HealthQuarantineScore >= settings.HealthWatchScore {
		settings.HealthQuarantineScore = defaults.HealthQuarantineScore
	}
	if settings.HealthMinSamples <= 0 {
		settings.HealthMinSamples = defaults.HealthMinSamples
	}
	if settings.HealthLatencyWarningMS <= 0 {
		settings.HealthLatencyWarningMS = defaults.HealthLatencyWarningMS
	}
	if settings.HealthLatencyCriticalMS <= settings.HealthLatencyWarningMS {
		settings.HealthLatencyCriticalMS = defaults.HealthLatencyCriticalMS
		if settings.HealthLatencyCriticalMS <= settings.HealthLatencyWarningMS {
			settings.HealthLatencyCriticalMS = settings.HealthLatencyWarningMS * 2
		}
	}
	if settings.HealthTrafficPauseBelow <= 0 || settings.HealthTrafficPauseBelow >= 100 {
		settings.HealthTrafficPauseBelow = defaults.HealthTrafficPauseBelow
	}
	if settings.HealthTrafficHealthyAt <= settings.HealthTrafficPauseBelow || settings.HealthTrafficHealthyAt > 100 {
		settings.HealthTrafficHealthyAt = defaults.HealthTrafficHealthyAt
	}
	if settings.HealthHardFailures10 <= 0 {
		settings.HealthHardFailures10 = defaults.HealthHardFailures10
	}
	if settings.HealthPersistentSlowRate <= 0 || settings.HealthPersistentSlowRate > 100 {
		settings.HealthPersistentSlowRate = defaults.HealthPersistentSlowRate
	}
	if settings.HealthQuarantineMinutes <= 0 {
		settings.HealthQuarantineMinutes = defaults.HealthQuarantineMinutes
	}
	if settings.HealthRecoveryWindow < 2 {
		settings.HealthRecoveryWindow = defaults.HealthRecoveryWindow
	}
	if settings.HealthRecoverySuccesses <= 0 || settings.HealthRecoverySuccesses > settings.HealthRecoveryWindow {
		settings.HealthRecoverySuccesses = min(defaults.HealthRecoverySuccesses, settings.HealthRecoveryWindow)
	}
	if settings.HealthTrialPercent <= 0 || settings.HealthTrialPercent > 100 {
		settings.HealthTrialPercent = defaults.HealthTrialPercent
	}
	if settings.HealthMidPercent <= 0 || settings.HealthMidPercent > 100 {
		settings.HealthMidPercent = defaults.HealthMidPercent
	}
	if settings.HealthDegradedPercent <= 0 || settings.HealthDegradedPercent > 100 {
		settings.HealthDegradedPercent = defaults.HealthDegradedPercent
	}
	if settings.HealthTrialMinutes <= 0 {
		settings.HealthTrialMinutes = defaults.HealthTrialMinutes
	}
	if settings.HealthLoadOverrideMinutes <= 0 {
		settings.HealthLoadOverrideMinutes = defaults.HealthLoadOverrideMinutes
	}
	return settings
}

// ResolveSettings applies the account policy overrides that are safe to vary
// independently from the global recovery state machine.
func ResolveSettings(settings model.Settings, policy model.Policy) model.Settings {
	settings = NormalizeSettings(settings)
	if policy.HealthHealthyScore != nil {
		settings.HealthHealthyScore = *policy.HealthHealthyScore
	}
	if policy.HealthWatchScore != nil {
		settings.HealthWatchScore = *policy.HealthWatchScore
	}
	if policy.HealthQuarantineScore != nil {
		settings.HealthQuarantineScore = *policy.HealthQuarantineScore
	}
	if policy.HealthMinSamples != nil {
		settings.HealthMinSamples = *policy.HealthMinSamples
	}
	if policy.HealthLatencyWarningMS != nil {
		settings.HealthLatencyWarningMS = *policy.HealthLatencyWarningMS
	}
	if policy.HealthLatencyCriticalMS != nil {
		settings.HealthLatencyCriticalMS = *policy.HealthLatencyCriticalMS
	}
	if policy.HealthTrafficPauseBelow != nil {
		settings.HealthTrafficPauseBelow = *policy.HealthTrafficPauseBelow
	}
	if policy.HealthTrafficHealthyAt != nil {
		settings.HealthTrafficHealthyAt = *policy.HealthTrafficHealthyAt
	}
	if policy.HealthHardFailures10 != nil {
		settings.HealthHardFailures10 = *policy.HealthHardFailures10
	}
	if policy.HealthPersistentSlowRate != nil {
		settings.HealthPersistentSlowRate = *policy.HealthPersistentSlowRate
	}
	return NormalizeSettings(settings)
}

// Evaluate evaluates one fresh monitor result. A zero CheckedAt in the returned
// observation means the monitor is frozen and no observation should be stored.
func Evaluate(monitor model.Monitor, observations []model.MonitorObservation, previous model.MonitorHealthState, settings model.Settings, now time.Time) (model.MonitorHealthState, model.MonitorObservation) {
	settings = NormalizeSettings(settings)
	now = now.UTC()
	state := previous
	state.MonitorID = monitor.ID
	state.UpdatedAt = now

	if reason := frozenReason(monitor, now); reason != "" {
		state.Stage = model.HealthStageFrozen
		state.Score = 0
		state.Confidence = 0
		state.RecoveryEligible = false
		state.NextRecoveryCondition = reason
		state.ReasonJSON = reasonsJSON([]string{reason})
		if previous.Stage != model.HealthStageFrozen {
			state.LastTransitionAt = now
		}
		return state, model.MonitorObservation{}
	}

	current := observationFromMonitor(monitor, now)
	isNewObservation := !containsCheckedAt(observations, current.CheckedAt)
	history := mergeObservations(observations, current, now)
	baseline, baselineSamples := latencyBaseline(observations, now, settings.HealthMinSamples)
	availability15, samples15 := availability(history, now.Add(-15*time.Minute))
	availability1H, samples1H := availability(history, now.Add(-time.Hour))
	availability24H, samples24H := availability(history, now.Add(-24*time.Hour))
	confidence := math.Min(1, float64(samples24H)/float64(settings.HealthMinSamples))

	score, reasons := scoreObservation(current, availability15, samples15, availability1H, samples1H, baseline, baselineSamples, settings)
	current.Score = score
	current.Confidence = confidence
	current.ReasonJSON = reasonsJSON(reasons)
	history = mergeObservations(observations, current, now)

	recovery := CheckRecovery(history, previous, monitor.Availability7D, baseline, settings, now)
	candidate, severe := classifyStage(current, history, confidence, settings)
	stage, transitionAt, holdUntil := transitionStage(previous, candidate, severe, recovery, settings, now, isNewObservation)

	state.Stage = stage
	state.Score = score
	state.Confidence = confidence
	state.CurrentLatencyMS = current.LatencyMS
	state.BaselineLatencyMS = baseline
	state.Availability15M = availability15
	state.Availability1H = availability1H
	state.Availability24H = availability24H
	state.SampleCount = samples24H
	state.RecoveryHealthyCount = recovery.HealthyCount
	state.LastTwoHealthy = recovery.LastTwoHealthy
	state.RecoveryEligible = recovery.Eligible
	state.NextRecoveryCondition = recovery.NextCondition
	state.LastTransitionAt = transitionAt
	state.HoldUntil = holdUntil
	state.ReasonJSON = reasonsJSON(reasons)
	return state, current
}

type RecoveryResult struct {
	Eligible          bool
	HealthyCount      int
	RequiredHealthy   int
	LastTwoHealthy    bool
	AvailabilityOK    bool
	LatencyOK         bool
	EnoughSamples     bool
	QuarantineElapsed bool
	NextCondition     string
}

// CheckRecovery exposes every signal used by the staged recovery decision.
// Account locks are deliberately checked by the reconciliation layer.
func CheckRecovery(observations []model.MonitorObservation, previous model.MonitorHealthState, availability7D *float64, baseline float64, settings model.Settings, now time.Time) RecoveryResult {
	settings = NormalizeSettings(settings)
	unique := mergeObservations(observations, model.MonitorObservation{}, now.UTC())
	ordered := append([]model.MonitorObservation(nil), unique...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].CheckedAt.Before(ordered[j].CheckedAt) })
	if len(ordered) > settings.HealthRecoveryWindow {
		ordered = ordered[len(ordered)-settings.HealthRecoveryWindow:]
	}

	result := RecoveryResult{RequiredHealthy: settings.HealthRecoverySuccesses}
	if availability7D != nil && *availability7D < 80 {
		result.RequiredHealthy = settings.HealthRecoveryWindow
	}
	for _, observation := range ordered {
		if normalizedStatus(observation.Status) == model.StatusOperational {
			result.HealthyCount++
		}
	}
	result.EnoughSamples = len(ordered) >= settings.HealthRecoveryWindow
	result.LastTwoHealthy = len(ordered) >= 2 && normalizedStatus(ordered[len(ordered)-1].Status) == model.StatusOperational && normalizedStatus(ordered[len(ordered)-2].Status) == model.StatusOperational
	availability15, samples15 := availability(unique, now.UTC().Add(-15*time.Minute))
	result.AvailabilityOK = samples15 >= settings.HealthMinSamples && availability15 >= 90
	if len(ordered) > 0 {
		currentLatency := ordered[len(ordered)-1].LatencyMS
		if baseline > 0 {
			result.LatencyOK = currentLatency > 0 && float64(currentLatency) <= baseline*1.5
		} else {
			result.LatencyOK = currentLatency > 0 && currentLatency < settings.HealthLatencyWarningMS
		}
	}
	result.QuarantineElapsed = previous.HoldUntil == nil || !now.UTC().Before(previous.HoldUntil.UTC())
	result.Eligible = result.EnoughSamples && result.HealthyCount >= result.RequiredHealthy && result.LastTwoHealthy && result.AvailabilityOK && result.LatencyOK && result.QuarantineElapsed

	switch {
	case !result.QuarantineElapsed:
		result.NextCondition = "等待最短隔离时间结束"
	case !result.EnoughSamples:
		result.NextCondition = "等待收集最近检测样本"
	case result.HealthyCount < result.RequiredHealthy:
		result.NextCondition = "最近检测中正常次数不足"
	case !result.LastTwoHealthy:
		result.NextCondition = "最后两次检测必须连续正常"
	case !result.AvailabilityOK:
		result.NextCondition = "最近十五分钟可用率需要达到90%"
	case !result.LatencyOK:
		result.NextCondition = "当前响应时间尚未恢复到正常范围"
	default:
		result.NextCondition = "已满足恢复试运行条件"
	}
	return result
}

func observationFromMonitor(monitor model.Monitor, now time.Time) model.MonitorObservation {
	observation := model.MonitorObservation{
		MonitorID:     monitor.ID,
		CheckedAt:     monitor.LastCheckedAt.UTC(),
		Status:        normalizedStatus(monitor.PrimaryStatus),
		LatencyMS:     monitor.PrimaryLatencyMS,
		DecryptFailed: monitor.DecryptFailed,
		CreatedAt:     now,
	}
	if monitor.Availability7D != nil {
		observation.Availability7D = *monitor.Availability7D
	}
	extraModels := make([]model.MonitorModelStatus, 0, len(monitor.ExtraModels)+len(monitor.ExtraModelsStatus))
	extraModels = append(extraModels, monitor.ExtraModels...)
	extraModels = append(extraModels, monitor.ExtraModelsStatus...)
	for _, extra := range extraModels {
		switch normalizedStatus(extra.Status) {
		case model.StatusOperational:
			observation.ExtraOK++
		case model.StatusDegraded:
			observation.ExtraDegraded++
		case model.StatusFailed, model.StatusError:
			observation.ExtraFailed++
		}
	}
	return observation
}

func scoreObservation(current model.MonitorObservation, availability15 float64, samples15 int, availability1H float64, samples1H int, baseline float64, baselineSamples int, settings model.Settings) (float64, []string) {
	score := 100.0
	reasons := make([]string, 0, 8)
	penalize := func(points float64, reason string) {
		if points <= 0 {
			return
		}
		score -= points
		reasons = append(reasons, reason)
	}

	switch normalizedStatus(current.Status) {
	case model.StatusOperational:
	case model.StatusDegraded:
		penalize(25, "本次检测结果为性能下降")
	case model.StatusFailed:
		penalize(55, "本次检测失败")
	case model.StatusError:
		penalize(65, "本次检测发生错误")
	}
	if current.DecryptFailed {
		penalize(100, "监控密钥解密失败，需要立即隔离")
	}
	extraPenalty := math.Min(40, float64(current.ExtraDegraded*8+current.ExtraFailed*20))
	if extraPenalty > 0 {
		penalize(extraPenalty, "附加模型存在异常")
	}
	if samples15 > 0 {
		switch {
		case availability15 < 70:
			penalize(45, "最近十五分钟可用率低于70%")
		case availability15 < 80:
			penalize(30, "最近十五分钟可用率低于80%")
		case availability15 < 90:
			penalize(15, "最近十五分钟可用率低于90%")
		case availability15 < 95:
			penalize(5, "最近十五分钟可用率低于95%")
		}
	}
	if samples1H >= settings.HealthMinSamples {
		switch {
		case availability1H < 70:
			penalize(10, "最近一小时稳定性持续偏低")
		case availability1H < 80:
			penalize(7, "最近一小时稳定性偏低")
		case availability1H < 90:
			penalize(4, "最近一小时稳定性需要观察")
		case availability1H < 95:
			penalize(2, "最近一小时存在轻微波动")
		}
	}

	latencyPenalty := 0.0
	latencyReason := ""
	if current.LatencyMS >= settings.HealthLatencyCriticalMS {
		latencyPenalty, latencyReason = 30, "当前响应时间超过严重阈值"
	} else if current.LatencyMS >= settings.HealthLatencyWarningMS {
		latencyPenalty, latencyReason = 15, "当前响应时间超过警告阈值"
	}
	if baselineSamples >= settings.HealthMinSamples && baseline > 0 && current.LatencyMS > 0 {
		ratio := float64(current.LatencyMS) / baseline
		switch {
		case ratio >= 3:
			if latencyPenalty < 30 {
				latencyPenalty, latencyReason = 30, "当前响应时间超过正常基准三倍"
			}
		case ratio >= 2:
			if latencyPenalty < 15 {
				latencyPenalty, latencyReason = 15, "当前响应时间超过正常基准两倍"
			}
		case ratio >= 1.5:
			if latencyPenalty < 5 {
				latencyPenalty, latencyReason = 5, "当前响应时间超过正常基准一点五倍"
			}
		}
	}
	penalize(latencyPenalty, latencyReason)

	if current.Availability7D > 0 {
		switch {
		case current.Availability7D < 80:
			penalize(10, "七天可用率低于80%")
		case current.Availability7D < 90:
			penalize(6, "七天可用率低于90%")
		case current.Availability7D < 95:
			penalize(3, "七天可用率低于95%")
		}
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "各项健康指标正常")
	}
	return math.Max(0, math.Min(100, score)), reasons
}

func classifyStage(current model.MonitorObservation, observations []model.MonitorObservation, confidence float64, settings model.Settings) (string, bool) {
	if current.DecryptFailed || consecutiveFailures(observations, 2) || lowScores(observations, 5, 3, settings.HealthQuarantineScore) {
		return model.HealthStageQuarantined, true
	}
	switch {
	case current.Score >= float64(settings.HealthHealthyScore):
		return model.HealthStageHealthy, false
	case current.Score >= float64(settings.HealthWatchScore):
		return model.HealthStageWatch, false
	case current.Score >= float64(settings.HealthQuarantineScore):
		return model.HealthStageDegraded, false
	case confidence < 1:
		return model.HealthStageDegraded, false
	default:
		return model.HealthStageQuarantined, false
	}
}

func transitionStage(previous model.MonitorHealthState, candidate string, severe bool, recovery RecoveryResult, settings model.Settings, now time.Time, isNewObservation bool) (string, time.Time, *time.Time) {
	transitionAt := previous.LastTransitionAt
	if transitionAt.IsZero() {
		transitionAt = now
	}
	holdUntil := previous.HoldUntil
	if candidate == model.HealthStageQuarantined {
		if previous.Stage != model.HealthStageQuarantined || severe && isNewObservation {
			transitionAt = now
			hold := now.Add(time.Duration(settings.HealthQuarantineMinutes) * time.Minute)
			holdUntil = &hold
		}
		return candidate, transitionAt, holdUntil
	}

	if previous.Stage == model.HealthStageQuarantined {
		if candidate == model.HealthStageHealthy && recovery.Eligible {
			return model.HealthStageRecovering25, now, nil
		}
		return model.HealthStageQuarantined, transitionAt, holdUntil
	}
	if previous.Stage == model.HealthStageRecovering25 || previous.Stage == model.HealthStageRecovering50 {
		if candidate != model.HealthStageHealthy || !recovery.Eligible {
			if isNewObservation {
				transitionAt = now
			}
			if previous.Stage == model.HealthStageRecovering50 && (candidate == model.HealthStageWatch || candidate == model.HealthStageDegraded) {
				return model.HealthStageRecovering25, transitionAt, nil
			}
			return previous.Stage, transitionAt, nil
		}
		if !isNewObservation || now.Sub(transitionAt) < time.Duration(settings.HealthTrialMinutes)*time.Minute {
			return previous.Stage, transitionAt, nil
		}
		if previous.Stage == model.HealthStageRecovering25 {
			return model.HealthStageRecovering50, now, nil
		}
		return model.HealthStageHealthy, now, nil
	}
	if previous.Stage != candidate {
		transitionAt = now
	}
	return candidate, transitionAt, nil
}

func containsCheckedAt(observations []model.MonitorObservation, checkedAt time.Time) bool {
	for _, observation := range observations {
		if observation.CheckedAt.Equal(checkedAt) {
			return true
		}
	}
	return false
}

func frozenReason(monitor model.Monitor, now time.Time) string {
	if !monitor.Enabled {
		return "监控已关闭，保持账号现状"
	}
	if monitor.LastCheckedAt == nil {
		return "监控暂无检测数据，保持账号现状"
	}
	staleAfter := time.Duration(monitor.IntervalSeconds*3) * time.Second
	if staleAfter < 3*time.Minute {
		staleAfter = 3 * time.Minute
	}
	if now.Sub(monitor.LastCheckedAt.UTC()) > staleAfter {
		return "监控结果长时间未更新，保持账号现状"
	}
	switch normalizedStatus(monitor.PrimaryStatus) {
	case model.StatusOperational, model.StatusDegraded, model.StatusFailed, model.StatusError:
		return ""
	default:
		return "监控状态未知，保持账号现状"
	}
}

func mergeObservations(history []model.MonitorObservation, current model.MonitorObservation, now time.Time) []model.MonitorObservation {
	byCheckedAt := make(map[int64]model.MonitorObservation, len(history)+1)
	cutoff := now.Add(-24 * time.Hour)
	for _, observation := range history {
		if observation.CheckedAt.Before(cutoff) || observation.CheckedAt.After(now.Add(time.Minute)) {
			continue
		}
		byCheckedAt[observation.CheckedAt.UTC().UnixNano()] = observation
	}
	if !current.CheckedAt.IsZero() {
		byCheckedAt[current.CheckedAt.UTC().UnixNano()] = current
	}
	result := make([]model.MonitorObservation, 0, len(byCheckedAt))
	for _, observation := range byCheckedAt {
		result = append(result, observation)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CheckedAt.Before(result[j].CheckedAt) })
	return result
}

func availability(observations []model.MonitorObservation, since time.Time) (float64, int) {
	var total float64
	var count int
	for _, observation := range observations {
		if observation.CheckedAt.Before(since) {
			continue
		}
		switch normalizedStatus(observation.Status) {
		case model.StatusOperational:
			total++
			count++
		case model.StatusDegraded:
			total += 0.5
			count++
		case model.StatusFailed, model.StatusError:
			count++
		}
	}
	if count == 0 {
		return 0, 0
	}
	return total / float64(count) * 100, count
}

func latencyBaseline(observations []model.MonitorObservation, now time.Time, minimum int) (float64, int) {
	latencies := make([]int64, 0, len(observations))
	cutoff := now.Add(-24 * time.Hour)
	for _, observation := range observations {
		if observation.CheckedAt.Before(cutoff) || observation.CheckedAt.After(now) || normalizedStatus(observation.Status) != model.StatusOperational || observation.LatencyMS <= 0 {
			continue
		}
		latencies = append(latencies, observation.LatencyMS)
	}
	if len(latencies) < minimum {
		return 0, len(latencies)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	middle := len(latencies) / 2
	if len(latencies)%2 == 1 {
		return float64(latencies[middle]), len(latencies)
	}
	return float64(latencies[middle-1]+latencies[middle]) / 2, len(latencies)
}

func consecutiveFailures(observations []model.MonitorObservation, required int) bool {
	if len(observations) < required {
		return false
	}
	for _, observation := range observations[len(observations)-required:] {
		status := normalizedStatus(observation.Status)
		if status != model.StatusFailed && status != model.StatusError {
			return false
		}
	}
	return true
}

func lowScores(observations []model.MonitorObservation, window, required, threshold int) bool {
	if len(observations) > window {
		observations = observations[len(observations)-window:]
	}
	count := 0
	for _, observation := range observations {
		if observation.Score < float64(threshold) {
			count++
		}
	}
	return count >= required
}

func normalizedStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
}

func reasonsJSON(reasons []string) string {
	payload, _ := json.Marshal(reasons)
	return string(payload)
}
