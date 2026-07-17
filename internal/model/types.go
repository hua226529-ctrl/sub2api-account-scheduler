package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	StatusOperational = "operational"
	StatusDegraded    = "degraded"
	StatusFailed      = "failed"
	StatusError       = "error"

	PhaseUnknown   = "unknown"
	PhaseHealthy   = "healthy"
	PhaseDegraded  = "degraded"
	PhaseUnhealthy = "unhealthy"
	PhaseFrozen    = "frozen"

	HealthStageHealthy      = "healthy"
	HealthStageWatch        = "watch"
	HealthStageDegraded     = "degraded"
	HealthStageQuarantined  = "quarantined"
	HealthStageRecovering25 = "recovering_25"
	HealthStageRecovering50 = "recovering_50"
	HealthStageRecovering80 = "recovering_80"
	HealthStageLimited25    = "limited_25"
	HealthStageLimited50    = "limited_50"
	HealthStageLimited80    = "limited_80"
	HealthStageFrozen       = "frozen"

	HealthModeLegacy   = "legacy"
	HealthModeObserve  = "observe"
	HealthModeAdaptive = "adaptive"

	ErrorClassCredential      = "credential"
	ErrorClassInfrastructure  = "infrastructure"
	ErrorClassCapacity        = "capacity"
	ErrorClassSemantic        = "semantic"
	ErrorClassClient          = "client"
	ErrorClassModelCapability = "model_capability"
	ErrorClassUnknown         = "unknown"

	GroupTierMain      = "main"
	GroupTierBackup    = "backup"
	GroupTierEmergency = "emergency"

	GroupTransitionPending = "pending"
	GroupTransitionApplied = "applied"
	// GroupTransitionCompleted is retained for read compatibility with
	// journals written before post-switch validation was separated.
	GroupTransitionCompleted = "completed"
	GroupTransitionFailed    = "failed"
	GroupTransitionSimulated = "simulated"

	GroupValidationUnknown          = "unknown"
	GroupValidationStable           = "stable"
	GroupValidationTransitioning    = "transitioning"
	GroupValidationAwaitingEvidence = "awaiting_evidence"
	GroupValidationProbing          = "probing"
	GroupValidationConfirmedHealthy = "confirmed_healthy"
	GroupValidationConfirmedFailed  = "confirmed_failed"
	GroupValidationUncertain        = "uncertain"
	GroupValidationExhausted        = "exhausted"

	GroupValidationModePassive           = "passive"
	GroupValidationModeActive            = "active"
	GroupValidationModeActiveThenPassive = "active_then_passive"

	GroupValidationPropagationDelay = 5 * time.Second
	// GroupValidationMonitorRequestTimeout matches Sub2API's bounded model
	// request timeout. It is used only when a monitor record cannot expose a
	// request start time and completion must be conservatively attributed.
	GroupValidationMonitorRequestTimeout = 45 * time.Second
	GroupValidationEvidenceTimeout       = 10 * time.Minute
	GroupValidationMinimumSuccesses      = 1

	EvidenceTimeBasisMonitorRequestStart = "monitor_request_start"
	EvidenceTimeBasisRequestStart        = "request_start"
	EvidenceTimeBasisCompletion          = "completion"

	SchedulerModeObserve = "observe"
	SchedulerModeControl = "control"

	FailoverModeDisabled = "disabled"
	FailoverModeObserve  = "observe"
	FailoverModeControl  = "control"
)

var HealthStageLabels = map[string]string{
	HealthStageHealthy:      "健康",
	HealthStageWatch:        "观察",
	HealthStageDegraded:     "性能下降",
	HealthStageQuarantined:  "已隔离",
	HealthStageRecovering25: "恢复试运行（25%）",
	HealthStageRecovering50: "恢复试运行（50%）",
	HealthStageRecovering80: "恢复试运行（80%）",
	HealthStageLimited25:    "限制负载（25%）",
	HealthStageLimited50:    "限制负载（50%）",
	HealthStageLimited80:    "限制负载（80%）",
	HealthStageFrozen:       "数据冻结",
}

type MonitorModelStatus struct {
	Model     string `json:"model"`
	Status    string `json:"status"`
	LatencyMS int64  `json:"latency_ms"`
}

type Monitor struct {
	ID                int64                `json:"id"`
	Name              string               `json:"name"`
	Provider          string               `json:"provider"`
	Endpoint          string               `json:"endpoint"`
	PrimaryModel      string               `json:"primary_model"`
	Enabled           bool                 `json:"enabled"`
	IntervalSeconds   int                  `json:"interval_seconds"`
	LastCheckedAt     *time.Time           `json:"last_checked_at"`
	PrimaryStatus     string               `json:"primary_status"`
	PrimaryLatencyMS  int64                `json:"primary_latency_ms"`
	Availability7D    *float64             `json:"availability_7d,omitempty"`
	ExtraModels       []MonitorModelStatus `json:"extra_models"`
	ExtraModelsStatus []MonitorModelStatus `json:"extra_models_status"`
	DecryptFailed     bool                 `json:"api_key_decrypt_failed"`
}

// UnmarshalJSON accepts both Sub2API monitor response shapes. Newer responses
// return extra model health objects, while some deployments return the
// configured extra_models value as a string.
func (m *Monitor) UnmarshalJSON(data []byte) error {
	type MonitorAlias Monitor
	decoded := struct {
		*MonitorAlias
		ExtraModels       json.RawMessage `json:"extra_models"`
		ExtraModelsStatus json.RawMessage `json:"extra_models_status"`
	}{MonitorAlias: (*MonitorAlias)(m)}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.ExtraModels != nil {
		items, err := decodeMonitorModelStatuses(decoded.ExtraModels)
		if err != nil {
			return fmt.Errorf("decode extra_models: %w", err)
		}
		m.ExtraModels = items
	}
	if decoded.ExtraModelsStatus != nil {
		items, err := decodeMonitorModelStatuses(decoded.ExtraModelsStatus)
		if err != nil {
			return fmt.Errorf("decode extra_models_status: %w", err)
		}
		m.ExtraModelsStatus = items
	}
	return nil
}

func decodeMonitorModelStatuses(raw json.RawMessage) ([]MonitorModelStatus, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}

	switch raw[0] {
	case '[':
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return nil, err
		}
		items := make([]MonitorModelStatus, 0, len(values))
		for _, value := range values {
			decoded, err := decodeMonitorModelStatuses(value)
			if err != nil {
				return nil, err
			}
			items = append(items, decoded...)
		}
		return items, nil
	case '{':
		var item MonitorModelStatus
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		return []MonitorModelStatus{item}, nil
	case '"':
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, nil
		}
		if embedded := bytes.TrimSpace([]byte(value)); json.Valid(embedded) && len(embedded) > 0 &&
			(embedded[0] == '[' || embedded[0] == '{' || embedded[0] == '"') {
			return decodeMonitorModelStatuses(embedded)
		}

		models := strings.FieldsFunc(value, func(r rune) bool {
			return r == ',' || r == ';' || r == '\n' || r == '\r'
		})
		items := make([]MonitorModelStatus, 0, len(models))
		for _, modelName := range models {
			if modelName = strings.TrimSpace(modelName); modelName != "" {
				items = append(items, MonitorModelStatus{Model: modelName})
			}
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unsupported JSON value %s", string(raw))
	}
}

type Account struct {
	ID                     int64          `json:"id"`
	Name                   string         `json:"name"`
	Platform               string         `json:"platform"`
	Type                   string         `json:"type"`
	Status                 string         `json:"status"`
	Schedulable            bool           `json:"schedulable"`
	ErrorMessage           string         `json:"error_message"`
	ExpiresAt              *int64         `json:"expires_at"`
	Credentials            map[string]any `json:"credentials"`
	Concurrency            int            `json:"concurrency"`
	LoadFactor             *int           `json:"load_factor"`
	Priority               int            `json:"priority"`
	CredentialStatus       string         `json:"credential_status"`
	RateLimitResetAt       *time.Time     `json:"rate_limit_reset_at"`
	OverloadUntil          *time.Time     `json:"overload_until"`
	TempUnschedulableUntil *time.Time     `json:"temp_unschedulable_until"`
	UpdatedAt              time.Time      `json:"updated_at"`
}

func (a Account) BaseURL() string {
	if value, ok := a.Credentials["base_url"].(string); ok {
		return value
	}
	return ""
}

type Policy struct {
	AccountID                int64  `json:"account_id"`
	ScorePolicySource        string `json:"score_policy_source,omitempty"`
	ScorePolicyVersionID     *int64 `json:"score_policy_version_id,omitempty"`
	MonitorID                *int64 `json:"monitor_id,omitempty"`
	Excluded                 bool   `json:"excluded"`
	Enabled                  bool   `json:"enabled"`
	FailureThreshold         *int   `json:"failure_threshold,omitempty"`
	RecoveryThreshold        *int   `json:"recovery_threshold,omitempty"`
	FlapEnabled              *bool  `json:"flap_enabled,omitempty"`
	FlapWindowMinutes        *int   `json:"flap_window_minutes,omitempty"`
	FlapPauseThreshold       *int   `json:"flap_pause_threshold,omitempty"`
	FlapRecoveryThreshold    *int   `json:"flap_recovery_threshold,omitempty"`
	HealthHealthyScore       *int   `json:"healthy_score_threshold,omitempty"`
	HealthWatchScore         *int   `json:"watch_score_threshold,omitempty"`
	HealthQuarantineScore    *int   `json:"quarantine_score_threshold,omitempty"`
	HealthMinSamples         *int   `json:"minimum_samples,omitempty"`
	HealthLatencyWarningMS   *int64 `json:"latency_warning_ms,omitempty"`
	HealthLatencyCriticalMS  *int64 `json:"latency_critical_ms,omitempty"`
	HealthTrafficPauseBelow  *int   `json:"traffic_pause_below,omitempty"`
	HealthTrafficHealthyAt   *int   `json:"traffic_healthy_at,omitempty"`
	HealthHardFailures10     *int   `json:"hard_failures_10_threshold,omitempty"`
	HealthPersistentSlowRate *int   `json:"persistent_slow_rate,omitempty"`
}

type Settings struct {
	DryRun                           bool   `json:"dry_run"`
	SchedulerMode                    string `json:"scheduler_mode"`
	FailoverMode                     string `json:"failover_mode"`
	FailoverMutationBudget           int    `json:"group_failover_mutation_budget"`
	FailureThreshold                 int    `json:"failure_threshold"`
	RecoveryThreshold                int    `json:"recovery_threshold"`
	ManualHoldMinutes                int    `json:"manual_hold_minutes"`
	FlapWindowMinutes                int    `json:"flap_window_minutes"`
	FlapPauseThreshold               int    `json:"flap_pause_threshold"`
	FlapRecoveryThreshold            int    `json:"flap_recovery_threshold"`
	HealthMode                       string `json:"health_engine_mode"`
	HealthHealthyScore               int    `json:"healthy_score_threshold"`
	HealthWatchScore                 int    `json:"watch_score_threshold"`
	HealthQuarantineScore            int    `json:"quarantine_score_threshold"`
	HealthMinSamples                 int    `json:"minimum_samples"`
	HealthLatencyWarningMS           int64  `json:"latency_warning_ms"`
	HealthLatencyCriticalMS          int64  `json:"latency_critical_ms"`
	HealthTrafficPauseBelow          int    `json:"traffic_pause_below"`
	HealthTrafficHealthyAt           int    `json:"traffic_healthy_at"`
	HealthHardFailures10             int    `json:"hard_failures_10_threshold"`
	HealthPersistentSlowRate         int    `json:"persistent_slow_rate"`
	HealthQuarantineMinutes          int    `json:"quarantine_minutes"`
	HealthRecoveryWindow             int    `json:"recovery_window_size"`
	HealthRecoverySuccesses          int    `json:"recovery_required_successes"`
	HealthTrialPercent               int    `json:"recovery_initial_percent"`
	HealthMidPercent                 int    `json:"recovery_mid_percent"`
	HealthDegradedPercent            int    `json:"degraded_load_percent"`
	HealthTrialMinutes               int    `json:"recovery_stage_minutes"`
	HealthLoadOverrideMinutes        int    `json:"load_manual_hold_minutes"`
	FailoverAccountFreshMinutes      int    `json:"group_failover_account_fresh_minutes"`
	FailoverTelemetryFreshMinutes    int    `json:"group_failover_telemetry_fresh_minutes"`
	FailoverGroupFreshMinutes        int    `json:"group_failover_data_fresh_minutes"`
	FailoverAgentGraceSeconds        int    `json:"group_failover_agent_grace_seconds"`
	FailoverMonitorFailures          int    `json:"group_failover_monitor_failures"`
	FailoverNoTrafficFailures        int    `json:"group_failover_no_traffic_failures"`
	FailoverTrafficWindowMinutes     int    `json:"group_failover_traffic_window_minutes"`
	FailoverTrafficMinSamples        int    `json:"group_failover_traffic_min_samples"`
	FailoverTrafficSuccessBelow      int    `json:"group_failover_traffic_success_below"`
	FailoverConsecutiveHardErrors    int    `json:"group_failover_consecutive_hard_errors"`
	FailoverBackupVerifyMinutes      int    `json:"group_failover_backup_verify_minutes"`
	FailoverPostSwitchMonitors       int    `json:"group_failover_post_switch_monitors"`
	FailoverPostSwitchRequests       int    `json:"group_failover_post_switch_requests"`
	FailoverMainVerifyMinutes        int    `json:"group_failover_main_verify_minutes"`
	FailoverSwitchCooldownMinutes    int    `json:"group_failover_switch_cooldown_minutes"`
	FailoverManualProtectionMinutes  int    `json:"group_failover_manual_protection_minutes"`
	FailoverShortLimitWindowMinutes  int    `json:"group_failover_short_limit_window_minutes"`
	FailoverShortLimitCount          int    `json:"group_failover_short_limit_count"`
	FailoverLongLimitWindowMinutes   int    `json:"group_failover_long_limit_window_minutes"`
	FailoverLongLimitCount           int    `json:"group_failover_long_limit_count"`
	FailoverRecoveryWindowMinutes    int    `json:"group_failover_recovery_window_minutes"`
	FailoverRecoveryStableMinutes    int    `json:"group_failover_recovery_stable_minutes"`
	FailoverRecoveryMonitorSuccesses int    `json:"group_failover_recovery_monitor_successes"`
	FailoverRecoveryMinSamples       int    `json:"group_failover_recovery_min_samples"`
	FailoverRecoverySuccessAt        int    `json:"group_failover_recovery_success_at"`
	FailoverReturnRetryMinutes       int    `json:"group_failover_return_retry_minutes"`
}

type MonitorObservation struct {
	MonitorID      int64     `json:"monitor_id"`
	CheckedAt      time.Time `json:"checked_at"`
	Status         string    `json:"status"`
	LatencyMS      int64     `json:"latency_ms"`
	Availability7D float64   `json:"availability_7d"`
	ExtraOK        int       `json:"extra_ok"`
	ExtraDegraded  int       `json:"extra_degraded"`
	ExtraFailed    int       `json:"extra_failed"`
	DecryptFailed  bool      `json:"decrypt_failed"`
	Score          float64   `json:"score"`
	Confidence     float64   `json:"confidence"`
	ReasonJSON     string    `json:"reason_json"`
	CreatedAt      time.Time `json:"created_at"`
}

// MonitorHistoryRecord is a model-level result returned by Sub2API's monitor
// history endpoint. Diagnostic message bodies are deliberately not retained.
type MonitorHistoryRecord struct {
	SourceID          int64     `json:"source_id,omitempty"`
	MonitorID         int64     `json:"monitor_id"`
	Model             string    `json:"model"`
	Status            string    `json:"status"`
	LatencyMS         int64     `json:"latency_ms"`
	PingLatencyMS     int64     `json:"ping_latency_ms"`
	StatusCode        int       `json:"status_code,omitempty"`
	ErrorClass        string    `json:"error_class,omitempty"`
	ReasonCode        string    `json:"reason_code,omitempty"`
	ReasonFingerprint string    `json:"reason_fingerprint,omitempty"`
	CheckedAt         time.Time `json:"checked_at"`
	IngestedAt        time.Time `json:"ingested_at"`
}

// TrafficSuccess contains only scheduler-relevant metadata. Request bodies,
// credentials and the original request identifier never leave the client.
type TrafficSuccess struct {
	EventKey         string     `json:"-"`
	AccountID        int64      `json:"account_id"`
	Model            string     `json:"model"`
	UpstreamModel    string     `json:"upstream_model,omitempty"`
	DurationMS       int64      `json:"duration_ms"`
	Kind             string     `json:"kind,omitempty"`
	RequestStartedAt *time.Time `json:"request_started_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

// TrafficError is the classified, redacted form of an upstream error. Message
// and request_id from Sub2API are represented only by one-way fingerprints.
type TrafficError struct {
	EventKey          string     `json:"-"`
	AccountID         int64      `json:"account_id"`
	Model             string     `json:"model"`
	RequestedModel    string     `json:"requested_model,omitempty"`
	UpstreamModel     string     `json:"upstream_model,omitempty"`
	Phase             string     `json:"phase,omitempty"`
	Type              string     `json:"type,omitempty"`
	Severity          string     `json:"severity,omitempty"`
	StatusCode        int        `json:"status_code,omitempty"`
	ErrorClass        string     `json:"error_class"`
	ReasonCode        string     `json:"reason_code"`
	ReasonFingerprint string     `json:"reason_fingerprint,omitempty"`
	RequestStartedAt  *time.Time `json:"request_started_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
}

type TrafficWindow struct {
	AccountID            int64     `json:"account_id"`
	Since                time.Time `json:"since"`
	Until                time.Time `json:"until"`
	SuccessCount         int       `json:"success_count"`
	ErrorCount           int       `json:"error_count"`
	CredentialErrors     int       `json:"credential_errors"`
	InfrastructureErrors int       `json:"infrastructure_errors"`
	CapacityErrors       int       `json:"capacity_errors"`
	SemanticErrors       int       `json:"semantic_errors"`
	ClientErrors         int       `json:"client_errors"`
	CapabilityErrors     int       `json:"capability_errors"`
	UnknownErrors        int       `json:"unknown_errors"`
	EligibleCount        int       `json:"eligible_count"`
	SuccessRate          float64   `json:"success_rate"`
	P90DurationMS        int64     `json:"p90_duration_ms"`
}

type AccountModelCapability struct {
	AccountID      int64     `json:"account_id"`
	Model          string    `json:"model"`
	Supported      bool      `json:"supported"`
	SuccessCount   int       `json:"success_count"`
	FailureCount   int       `json:"failure_count"`
	LastErrorClass string    `json:"last_error_class,omitempty"`
	LastReasonCode string    `json:"last_reason_code,omitempty"`
	LastObservedAt time.Time `json:"last_observed_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// DecisionEvidence is intentionally typed so arbitrary upstream error bodies
// cannot be accidentally written into a decision snapshot.
type DecisionEvidence struct {
	HardFailureStreak      int     `json:"hard_failure_streak"`
	HardFailures10         int     `json:"hard_failures_10"`
	HardSuccessRate10      float64 `json:"hard_success_rate_10"`
	HardSuccessRate60      float64 `json:"hard_success_rate_60"`
	DegradedCount10        int     `json:"degraded_count_10"`
	DegradedCount60        int     `json:"degraded_count_60"`
	DegradedStreak         int     `json:"degraded_streak"`
	LatencyP90MS           int64   `json:"latency_p90_ms"`
	BaselineLatencyMS      int64   `json:"baseline_latency_ms"`
	TrafficSuccessCount10M int     `json:"traffic_success_count_10m"`
	TrafficFailureCount10M int     `json:"traffic_failure_count_10m"`
	TrafficSuccessRate10M  float64 `json:"traffic_success_rate_10m"`
	TrafficSuccessRate60M  float64 `json:"traffic_success_rate_60m"`
	QualityScore           float64 `json:"quality_score"`
	Confidence             float64 `json:"confidence"`
}

type DecisionSnapshot struct {
	ID                int64            `json:"id"`
	DecisionID        string           `json:"decision_id"`
	MonitorID         *int64           `json:"monitor_id,omitempty"`
	AccountID         *int64           `json:"account_id,omitempty"`
	CheckedAt         time.Time        `json:"checked_at"`
	AvailabilityState string           `json:"availability_state"`
	LoadStage         string           `json:"load_stage"`
	TargetLoadPercent int              `json:"target_load_percent"`
	Action            string           `json:"action"`
	ActionResult      string           `json:"action_result"`
	ReasonCode        string           `json:"reason_code"`
	Evidence          DecisionEvidence `json:"evidence"`
	CreatedAt         time.Time        `json:"created_at"`
}

// HealthDecision is the current V3 decision projection exposed with a binding.
// DecisionSnapshot remains the immutable audit representation.
type HealthDecision struct {
	QualityScore          float64                  `json:"quality_score"`
	HardSuccessRate10     float64                  `json:"hard_success_rate_10"`
	HardSuccessRate60     float64                  `json:"hard_success_rate_60"`
	DegradedRate10        float64                  `json:"degraded_rate_10"`
	DegradedRate60        float64                  `json:"degraded_rate_60"`
	TrafficSuccessRate    float64                  `json:"traffic_success_rate"`
	TrafficSampleCount    int                      `json:"traffic_sample_count"`
	HardFailureStreak     int                      `json:"hard_failure_streak"`
	HardFailures10        int                      `json:"hard_failures_10"`
	RecoverySuccessStreak int                      `json:"recovery_success_streak"`
	SuggestedLoadPercent  int                      `json:"suggested_load_percent"`
	Action                string                   `json:"action"`
	Disagreement          bool                     `json:"disagreement"`
	ResponseP90MS         int64                    `json:"response_p90_ms"`
	BaselineLatencyMS     float64                  `json:"baseline_latency_ms"`
	ReasonCodes           []string                 `json:"reason_codes"`
	ErrorCategoryCounts   map[string]int           `json:"error_category_counts"`
	ModelCapabilities     []AccountModelCapability `json:"model_capabilities"`
	CheckedAt             time.Time                `json:"checked_at"`
}

type MonitorHealthState struct {
	MonitorID             int64      `json:"monitor_id"`
	Stage                 string     `json:"stage"`
	Score                 float64    `json:"score"`
	Confidence            float64    `json:"confidence"`
	CurrentLatencyMS      int64      `json:"current_latency_ms"`
	BaselineLatencyMS     float64    `json:"baseline_latency_ms"`
	Availability15M       float64    `json:"availability_15m"`
	Availability1H        float64    `json:"availability_1h"`
	Availability24H       float64    `json:"availability_24h"`
	SampleCount           int        `json:"sample_count"`
	RecoveryHealthyCount  int        `json:"recovery_healthy_count"`
	LastTwoHealthy        bool       `json:"last_two_healthy"`
	RecoveryEligible      bool       `json:"recovery_eligible"`
	NextRecoveryCondition string     `json:"next_recovery_condition"`
	HoldUntil             *time.Time `json:"hold_until,omitempty"`
	LastTransitionAt      time.Time  `json:"last_transition_at"`
	ReasonJSON            string     `json:"reason_json"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

type FlapPolicy struct {
	Enabled           bool
	WindowMinutes     int
	PauseThreshold    int
	RecoveryThreshold int
}

type MonitorState struct {
	MonitorID       int64      `json:"monitor_id"`
	LastCheckedAt   *time.Time `json:"last_checked_at,omitempty"`
	LastStatus      string     `json:"last_status"`
	HealthyStreak   int        `json:"healthy_streak"`
	UnhealthyStreak int        `json:"unhealthy_streak"`
	Phase           string     `json:"phase"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type AccountControl struct {
	AccountID             int64      `json:"account_id"`
	MonitorID             *int64     `json:"monitor_id,omitempty"`
	OwnsPause             bool       `json:"owns_pause"`
	Owner                 string     `json:"owner"`
	ExpectedSchedulable   *bool      `json:"expected_schedulable,omitempty"`
	ManualOverrideUntil   *time.Time `json:"manual_override_until,omitempty"`
	LastObserved          *bool      `json:"last_observed_schedulable,omitempty"`
	LastDecision          string     `json:"last_decision"`
	LastActionAt          *time.Time `json:"last_action_at,omitempty"`
	FlapActive            bool       `json:"flap_active"`
	FlapTriggeredAt       *time.Time `json:"flap_triggered_at,omitempty"`
	FlapRecoveryRequired  int        `json:"flap_recovery_required"`
	RecentAutomaticPauses int        `json:"recent_automatic_pauses"`
	HealthLocked          bool       `json:"health_locked"`
	ManualLocked          bool       `json:"manual_locked"`
	BalanceLocked         bool       `json:"balance_locked"`
	BalanceSourceID       *int64     `json:"balance_source_id,omitempty"`
	CostLocked            bool       `json:"cost_locked"`
	CostSourceID          *int64     `json:"cost_source_id,omitempty"`
	CostPool              string     `json:"cost_pool,omitempty"`
	OwnsLoadFactor        bool       `json:"owns_load_factor"`
	OriginalLoadFactor    *int       `json:"original_load_factor,omitempty"`
	ExpectedLoadFactor    *int       `json:"expected_load_factor,omitempty"`
	LoadStage             string     `json:"load_stage"`
	LoadOverrideUntil     *time.Time `json:"load_override_until,omitempty"`
	LoadPinValue          *int       `json:"load_pin_value,omitempty"`
	LoadPinUntil          *time.Time `json:"load_pin_until,omitempty"`
	LoadPinOwner          string     `json:"load_pin_owner,omitempty"`
	LoadPinReason         string     `json:"load_pin_reason,omitempty"`
	RecoveryStep          int        `json:"recovery_step"`
	RecoveryStartedAt     *time.Time `json:"recovery_started_at,omitempty"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

// FreezeState is the effective global freeze view consumed by the existing
// scheduler and agent capability layer. It is derived from AgentFreezeState;
// persistence remains centralized in agent_freeze_states.
type FreezeState struct {
	Agent         bool       `json:"agent"`
	AllAutomation bool       `json:"all_automation"`
	Mode          string     `json:"mode"`
	Reason        string     `json:"reason,omitempty"`
	Actor         string     `json:"actor,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type UpstreamSource struct {
	ID                   int64                 `json:"id"`
	Name                 string                `json:"name"`
	Provider             string                `json:"provider"`
	BaseURL              string                `json:"base_url"`
	NormalizedURL        string                `json:"normalized_url"`
	CredentialNonce      []byte                `json:"-"`
	CredentialCiphertext []byte                `json:"-"`
	CredentialConfigured bool                  `json:"credential_configured"`
	UsernameHint         string                `json:"username_hint"`
	CredentialHint       string                `json:"credential_hint"`
	CredentialMode       string                `json:"credential_mode"`
	MigrationRequired    bool                  `json:"credential_migration_required"`
	PauseBelow           float64               `json:"pause_below"`
	ResumeAt             float64               `json:"resume_at"`
	Enabled              bool                  `json:"enabled"`
	Balance              *float64              `json:"balance,omitempty"`
	Unit                 string                `json:"unit"`
	LowStreak            int                   `json:"low_streak"`
	RecoveryStreak       int                   `json:"recovery_streak"`
	BalanceLocked        bool                  `json:"balance_locked"`
	LastAttemptAt        *time.Time            `json:"last_attempt_at,omitempty"`
	LastSuccessAt        *time.Time            `json:"last_success_at,omitempty"`
	LastError            string                `json:"last_error,omitempty"`
	Stale                bool                  `json:"stale"`
	KeyRates             []KeyRate             `json:"key_rates"`
	Groups               []UpstreamGroup       `json:"groups"`
	SelectedKeyID        string                `json:"selected_key_id"`
	RoutingEnabled       bool                  `json:"routing_enabled"`
	RoutingPool          string                `json:"routing_pool"`
	MatchedAccounts      []AccountRef          `json:"matched_accounts"`
	FailoverPolicies     []GroupFailoverPolicy `json:"failover_policies"`
	CreatedAt            time.Time             `json:"created_at"`
	UpdatedAt            time.Time             `json:"updated_at"`
}

// GroupFailoverPolicy is the confirmed, operator-owned mapping between one
// upstream token and its three permitted recovery groups. Editing any mapping
// increments Version and clears confirmation before it can be automated again.
type GroupFailoverPolicy struct {
	SourceID         int64              `json:"source_id"`
	KeyID            string             `json:"key_id"`
	KeyName          string             `json:"key_name"`
	KeyHint          string             `json:"key_hint"`
	Enabled          bool               `json:"enabled"`
	MainEnabled      bool               `json:"main_enabled"`
	BackupEnabled    bool               `json:"backup_enabled"`
	EmergencyEnabled bool               `json:"emergency_enabled"`
	MainGroupID      string             `json:"main_group_id"`
	BackupGroupID    string             `json:"backup_group_id"`
	EmergencyGroupID string             `json:"emergency_group_id"`
	AccountIDs       []int64            `json:"account_ids"`
	Pool             string             `json:"pool"`
	Version          int64              `json:"version"`
	ConfirmedVersion int64              `json:"confirmed_version"`
	Confirmed        bool               `json:"confirmed"`
	ConfirmedAt      *time.Time         `json:"confirmed_at,omitempty"`
	ConfirmedBy      string             `json:"confirmed_by,omitempty"`
	State            GroupFailoverState `json:"state"`
	CreatedAt        time.Time          `json:"created_at"`
	UpdatedAt        time.Time          `json:"updated_at"`
}

type GroupFailoverState struct {
	SourceID                int64      `json:"source_id"`
	KeyID                   string     `json:"key_id"`
	CurrentTier             string     `json:"current_tier"`
	ObservedGroupID         string     `json:"observed_group_id"`
	PreviousTier            string     `json:"previous_tier"`
	PreviousStableTier      string     `json:"previous_stable_tier"`
	PreviousGroupID         string     `json:"previous_group_id"`
	Frozen                  bool       `json:"frozen"`
	FreezeReason            string     `json:"freeze_reason,omitempty"`
	LastError               string     `json:"last_error,omitempty"`
	ManualHoldUntil         *time.Time `json:"manual_hold_until,omitempty"`
	ManualOverrideUntil     *time.Time `json:"manual_override_until,omitempty"`
	CooldownUntil           *time.Time `json:"cooldown_until,omitempty"`
	ReturnBlockedUntil      *time.Time `json:"return_blocked_until,omitempty"`
	RecoverySince           *time.Time `json:"recovery_since,omitempty"`
	LastSwitchAt            *time.Time `json:"last_switch_at,omitempty"`
	LastTransitionAt        *time.Time `json:"last_transition_at,omitempty"`
	VerificationStartedAt   *time.Time `json:"verification_started_at,omitempty"`
	HealthySince            *time.Time `json:"healthy_since,omitempty"`
	RecoveryHealthyCount    int        `json:"recovery_healthy_count"`
	LastConfirmedAt         *time.Time `json:"last_confirmed_at,omitempty"`
	ValidationStatus        string     `json:"validation_status"`
	ValidationMode          string     `json:"validation_mode"`
	ValidationTransitionID  int64      `json:"validation_transition_id,omitempty"`
	ValidationFromTier      string     `json:"validation_from_tier,omitempty"`
	ValidationTargetTier    string     `json:"validation_target_tier,omitempty"`
	ValidationFromGroupID   string     `json:"validation_from_group_id,omitempty"`
	ValidationTargetGroupID string     `json:"validation_target_group_id,omitempty"`
	SwitchRequestedAt       *time.Time `json:"switch_requested_at,omitempty"`
	SwitchVerifiedAt        *time.Time `json:"switch_verified_at,omitempty"`
	ValidationNotBefore     *time.Time `json:"validation_not_before,omitempty"`
	EvidenceDeadline        *time.Time `json:"evidence_deadline,omitempty"`
	MonitorWatermark        int64      `json:"monitor_watermark"`
	TrafficWatermark        int64      `json:"traffic_watermark"`
	MonitorEvidenceCursor   int64      `json:"monitor_evidence_cursor"`
	TrafficEvidenceCursor   int64      `json:"traffic_evidence_cursor"`
	ActiveProbeAttempts     int        `json:"active_probe_attempts"`
	SuccessfulEvidenceCount int        `json:"successful_evidence_count"`
	FailedEvidenceCount     int        `json:"failed_evidence_count"`
	LastEvidenceID          string     `json:"last_evidence_id,omitempty"`
	LastEvidenceSource      string     `json:"last_evidence_source,omitempty"`
	LastEvidenceReason      string     `json:"last_evidence_reason,omitempty"`
	LastEvidenceAt          *time.Time `json:"last_evidence_at,omitempty"`
	UpdatedAt               time.Time  `json:"updated_at"`
}

type FailoverEvidenceWatermarks struct {
	Monitor int64
	Traffic int64
}

type GroupValidationEvidence struct {
	ID               int64
	Source           string
	MonitorID        int64
	AccountID        int64
	Status           string
	ErrorClass       string
	ReasonCode       string
	ObservedAt       time.Time
	RequestStartedAt *time.Time
	TimeBasis        string
	TransitionID     int64
	SourceID         int64
	KeyID            string
	TargetTier       string
	TargetGroupID    string
}

type GroupTierTransitionRequest struct {
	SourceID         int64      `json:"source_id"`
	KeyID            string     `json:"key_id"`
	TargetTier       string     `json:"target_tier"`
	IdempotencyKey   string     `json:"idempotency_key"`
	Actor            string     `json:"actor"`
	Producer         string     `json:"producer"`
	Authority        string     `json:"authority"`
	Reason           string     `json:"reason"`
	Evidence         string     `json:"evidence,omitempty"`
	SnapshotVersion  string     `json:"snapshot_version,omitempty"`
	Trigger          string     `json:"trigger,omitempty"`
	PacketID         int64      `json:"packet_id,omitempty"`
	RunID            int64      `json:"run_id,omitempty"`
	Manual           bool       `json:"manual"`
	DryRun           bool       `json:"dry_run"`
	ExpectedPool     string     `json:"-"`
	ExpectedFromTier string     `json:"-"`
	EvidenceCutoffAt *time.Time `json:"-"`
	// AutomationLeaseHeld is process-local execution metadata. It prevents a
	// nested read lock when the capability executor already owns the global
	// automation mutation barrier.
	AutomationLeaseHeld bool `json:"-"`
}

type GroupTierTransition struct {
	ID              int64      `json:"id"`
	IdempotencyKey  string     `json:"idempotency_key"`
	SourceID        int64      `json:"source_id"`
	KeyID           string     `json:"key_id"`
	FromTier        string     `json:"from_tier"`
	ToTier          string     `json:"to_tier"`
	FromGroupID     string     `json:"from_group_id"`
	ToGroupID       string     `json:"to_group_id"`
	Status          string     `json:"status"`
	Actor           string     `json:"actor"`
	Producer        string     `json:"producer"`
	Authority       string     `json:"authority"`
	Reason          string     `json:"reason"`
	Evidence        string     `json:"evidence,omitempty"`
	SnapshotVersion string     `json:"snapshot_version,omitempty"`
	Trigger         string     `json:"trigger,omitempty"`
	PacketID        int64      `json:"packet_id,omitempty"`
	RunID           int64      `json:"run_id,omitempty"`
	Error           string     `json:"error,omitempty"`
	AttemptCount    int        `json:"attempt_count"`
	BeforeState     string     `json:"before_state,omitempty"`
	VerifiedAfter   string     `json:"verified_after_state,omitempty"`
	Uncertain       bool       `json:"uncertain"`
	Manual          bool       `json:"manual"`
	DryRun          bool       `json:"dry_run"`
	CreatedAt       time.Time  `json:"created_at"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
}

type KeyRate struct {
	ExternalID     string   `json:"external_id"`
	Name           string   `json:"name"`
	KeyHint        string   `json:"key_hint"`
	GroupID        string   `json:"group_id"`
	GroupName      string   `json:"group_name"`
	RateMultiplier *float64 `json:"rate_multiplier,omitempty"`
	Dynamic        bool     `json:"dynamic"`
	Status         string   `json:"status"`
}

type UpstreamGroup struct {
	ExternalID     string  `json:"external_id"`
	Name           string  `json:"name"`
	RateMultiplier float64 `json:"rate_multiplier"`
}

type AccountRef struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Schedulable bool   `json:"schedulable"`
}

type UpstreamCredentials struct {
	AuthMode  string `json:"auth_mode,omitempty"`
	AccessKey string `json:"access_key,omitempty"`
	UserID    string `json:"user_id,omitempty"`
	Username  string `json:"username,omitempty"`
	Password  string `json:"password,omitempty"`
}

type UpstreamResult struct {
	Balance           float64         `json:"balance"`
	Unit              string          `json:"unit"`
	Username          string          `json:"username"`
	KeyRates          []KeyRate       `json:"key_rates"`
	Groups            []UpstreamGroup `json:"groups"`
	FetchedAt         time.Time       `json:"fetched_at"`
	RotatedAccessKey  string          `json:"-"`
	CredentialWarning string          `json:"credential_warning,omitempty"`
}

type BalanceLock struct {
	SourceID  int64     `json:"source_id"`
	AccountID int64     `json:"account_id"`
	CreatedAt time.Time `json:"created_at"`
}

type CostLock struct {
	SourceID       int64     `json:"source_id"`
	AccountID      int64     `json:"account_id"`
	Pool           string    `json:"pool"`
	RateMultiplier float64   `json:"rate_multiplier"`
	CreatedAt      time.Time `json:"created_at"`
}

type ResolvedBinding struct {
	Monitor               *Monitor           `json:"monitor,omitempty"`
	Account               Account            `json:"account"`
	Policy                Policy             `json:"policy"`
	Source                string             `json:"source"`
	State                 string             `json:"state"`
	Reason                string             `json:"reason"`
	NormalizedEndpoint    string             `json:"normalized_endpoint"`
	MonitorState          MonitorState       `json:"monitor_state"`
	HealthState           MonitorHealthState `json:"health_state"`
	Decision              *HealthDecision    `json:"decision,omitempty"`
	Control               AccountControl     `json:"control"`
	FailureThreshold      int                `json:"failure_threshold"`
	BaseRecoveryThreshold int                `json:"base_recovery_threshold"`
	RecoveryThreshold     int                `json:"recovery_threshold"`
	FlapEnabled           bool               `json:"flap_enabled"`
	FlapWindowMinutes     int                `json:"flap_window_minutes"`
	FlapPauseThreshold    int                `json:"flap_pause_threshold"`
	FlapRecoveryThreshold int                `json:"flap_recovery_threshold"`
}

type Event struct {
	ID          int64     `json:"id"`
	Type        string    `json:"type"`
	Severity    string    `json:"severity"`
	MonitorID   *int64    `json:"monitor_id,omitempty"`
	AccountID   *int64    `json:"account_id,omitempty"`
	Message     string    `json:"message"`
	BeforeState string    `json:"before_state,omitempty"`
	AfterState  string    `json:"after_state,omitempty"`
	Details     string    `json:"details,omitempty"`
	Actor       string    `json:"actor"`
	CreatedAt   time.Time `json:"created_at"`
}

type Snapshot struct {
	Bindings       []ResolvedBinding `json:"bindings"`
	Unmatched      []Monitor         `json:"unmatched_monitors"`
	Conflicts      []string          `json:"conflicts"`
	LastSyncAt     *time.Time        `json:"last_sync_at,omitempty"`
	LastSyncError  string            `json:"last_sync_error,omitempty"`
	Settings       Settings          `json:"settings"`
	Freeze         FreezeState       `json:"freeze"`
	ServiceStarted time.Time         `json:"service_started_at"`
}
