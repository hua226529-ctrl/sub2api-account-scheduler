package model

import (
	"encoding/json"
	"time"
)

const (
	AgentModeObserve = "observe"
	AgentModeControl = "control"

	AgentOptimizerDisabled = "disabled"
	AgentOptimizerObserve  = "observe"
	AgentOptimizerPropose  = "propose"
	AgentOptimizerAuto     = "auto"

	AgentOperatorDisabled = "disabled"
	AgentOperatorConfirm  = "confirm"
	AgentOperatorDirect   = "direct"

	AgentRunScheduled = "scheduled"
	AgentRunEmergency = "emergency"
	AgentRunManual    = "manual"
	AgentRunChat      = "chat"
	AgentRunDaily     = "daily"

	PolicyStatusDraft           = "draft"
	PolicyStatusSimulated       = "simulated"
	PolicyStatusPendingApproval = "pending_approval"
	PolicyStatusActive          = "active"
	PolicyStatusRejected        = "rejected"
	PolicyStatusRolledBack      = "rolled_back"
	PolicyStatusSuperseded      = "superseded"
)

// AgentProvider stores one OpenAI-compatible model endpoint. The API key is
// encrypted at rest and is never included in API responses or model context.
type AgentProvider struct {
	Slot                 string     `json:"slot"`
	BaseURL              string     `json:"base_url"`
	Model                string     `json:"model"`
	CredentialNonce      []byte     `json:"-"`
	CredentialCiphertext []byte     `json:"-"`
	APIKeyConfigured     bool       `json:"api_key_configured"`
	Enabled              bool       `json:"enabled"`
	TimeoutSeconds       int        `json:"timeout_seconds"`
	MaxOutputTokens      int        `json:"max_output_tokens"`
	Temperature          float64    `json:"temperature"`
	LastValidatedAt      *time.Time `json:"last_validated_at,omitempty"`
	LastError            string     `json:"last_error,omitempty"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

type AgentSettings struct {
	Enabled                      bool       `json:"enabled"`
	Mode                         string     `json:"mode"`
	OptimizerMode                string     `json:"optimizer_mode"`
	OperatorMode                 string     `json:"operator_mode"`
	DailyPolicyChangeBudget      int        `json:"daily_policy_change_budget"`
	AnalysisIntervalMinutes      int        `json:"analysis_interval_minutes"`
	EmergencyCooldownMinutes     int        `json:"emergency_cooldown_minutes"`
	ContextTokenBudget           int        `json:"context_token_budget"`
	MaxAnomalies                 int        `json:"max_anomalies"`
	MaxDrilldowns                int        `json:"max_drilldowns"`
	RetentionDays                int        `json:"retention_days"`
	ObservationStartedAt         *time.Time `json:"observation_started_at,omitempty"`
	SuccessfulObservationRuns    int        `json:"successful_observation_runs"`
	ObservationProposedActions   int        `json:"observation_proposed_actions"`
	ObservationExecutableActions int        `json:"observation_executable_actions"`
	ObservationViolations        int        `json:"observation_violations"`
	ObservationStructureErrors   int        `json:"observation_structure_errors"`
	LastScheduledAt              *time.Time `json:"last_scheduled_at,omitempty"`
	LastEmergencyAt              *time.Time `json:"last_emergency_at,omitempty"`
	UpdatedAt                    time.Time  `json:"updated_at"`
}

type AgentWindowStats struct {
	Window              string         `json:"window"`
	SampleCount         int            `json:"sample_count"`
	EligibleCount       int            `json:"eligible_count"`
	SuccessCount        int            `json:"success_count"`
	ErrorCount          int            `json:"error_count"`
	SuccessRate         float64        `json:"success_rate"`
	P50DurationMS       int64          `json:"p50_duration_ms"`
	P90DurationMS       int64          `json:"p90_duration_ms"`
	P99DurationMS       int64          `json:"p99_duration_ms"`
	ErrorCategoryCounts map[string]int `json:"error_category_counts"`
	StateChanges        int            `json:"state_changes"`
	AutomaticPauseCount int            `json:"automatic_pause_count"`
}

type AgentAccountState struct {
	AccountID           int64                       `json:"account_id"`
	Name                string                      `json:"name"`
	Pool                string                      `json:"pool,omitempty"`
	MonitorID           *int64                      `json:"monitor_id,omitempty"`
	MonitorStatus       string                      `json:"monitor_status"`
	MonitorCheckedAt    *time.Time                  `json:"monitor_checked_at,omitempty"`
	Schedulable         bool                        `json:"schedulable"`
	AccountStatus       string                      `json:"account_status"`
	LoadFactor          *int                        `json:"load_factor,omitempty"`
	Concurrency         int                         `json:"concurrency"`
	BalanceLocked       bool                        `json:"balance_locked"`
	CostLocked          bool                        `json:"cost_locked"`
	HealthLocked        bool                        `json:"health_locked"`
	FlapActive          bool                        `json:"flap_active"`
	AvailabilityState   string                      `json:"availability_state"`
	AvailabilityScore   float64                     `json:"availability_score"`
	PerformanceScore    float64                     `json:"performance_score"`
	StabilityScore      float64                     `json:"stability_score"`
	CapacityScore       float64                     `json:"capacity_score"`
	CostScore           float64                     `json:"cost_score"`
	Confidence          float64                     `json:"confidence"`
	EvidenceConflict    bool                        `json:"evidence_conflict"`
	HardFailureStreak   int                         `json:"hard_failure_streak"`
	HardFailures10      int                         `json:"hard_failures_10"`
	TrafficSampleCount  int                         `json:"traffic_sample_count"`
	TrafficSuccessRate  float64                     `json:"traffic_success_rate"`
	ErrorCategoryCounts map[string]int              `json:"error_category_counts"`
	Reasons             []string                    `json:"reasons"`
	Windows             map[string]AgentWindowStats `json:"windows"`
	RiskScore           float64                     `json:"risk_score"`
	Changed             bool                        `json:"changed"`
}

type AgentPoolSummary struct {
	Name              string   `json:"name"`
	Accounts          int      `json:"accounts"`
	Schedulable       int      `json:"schedulable"`
	Available         int      `json:"available"`
	Degraded          int      `json:"degraded"`
	Unavailable       int      `json:"unavailable"`
	InsufficientData  int      `json:"insufficient_data"`
	Capacity          int      `json:"capacity"`
	AverageMultiplier float64  `json:"average_multiplier,omitempty"`
	MinimumBalance    *float64 `json:"minimum_balance,omitempty"`
	StaleSources      int      `json:"stale_sources"`
}

type AgentSystemSummary struct {
	Accounts            int     `json:"accounts"`
	Schedulable         int     `json:"schedulable"`
	Available           int     `json:"available"`
	Degraded            int     `json:"degraded"`
	Unavailable         int     `json:"unavailable"`
	InsufficientData    int     `json:"insufficient_data"`
	AverageAvailability float64 `json:"average_availability"`
	AveragePerformance  float64 `json:"average_performance"`
	AverageConfidence   float64 `json:"average_confidence"`
	CriticalAnomalies   int     `json:"critical_anomalies"`
	DataFresh           bool    `json:"data_fresh"`
}

type AgentDataHealth struct {
	SchedulerLastSyncAt *time.Time `json:"scheduler_last_sync_at,omitempty"`
	SchedulerError      string     `json:"scheduler_error,omitempty"`
	BalanceLastSyncAt   *time.Time `json:"balance_last_sync_at,omitempty"`
	TelemetryLastSyncAt *time.Time `json:"telemetry_last_sync_at,omitempty"`
	TelemetryError      string     `json:"telemetry_error,omitempty"`
	MonitorFresh        bool       `json:"monitor_fresh"`
	TrafficFresh        bool       `json:"traffic_fresh"`
	TrafficSamples30M   int        `json:"traffic_samples_30m"`
	MissingSources      []string   `json:"missing_sources"`
}

// AgentGroupTierSummary deliberately omits the upstream group identifier. The
// model chooses a pre-confirmed tier; the deterministic executor resolves the
// real group identifier at execution time.
type AgentGroupTierSummary struct {
	Tier           string  `json:"tier"`
	Name           string  `json:"name"`
	RateMultiplier float64 `json:"rate_multiplier"`
	Configured     bool    `json:"configured"`
	Enabled        bool    `json:"enabled"`
}

type AgentGroupTransitionResult struct {
	FromTier    string     `json:"from_tier"`
	ToTier      string     `json:"to_tier"`
	Status      string     `json:"status"`
	Manual      bool       `json:"manual"`
	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type AgentGroupFailoverToken struct {
	SourceID              int64                        `json:"source_id"`
	SourceName            string                       `json:"source_name"`
	Provider              string                       `json:"provider"`
	Pool                  string                       `json:"pool"`
	KeyID                 string                       `json:"key_id"`
	KeyName               string                       `json:"key_name"`
	KeyHint               string                       `json:"key_hint"`
	Enabled               bool                         `json:"enabled"`
	Confirmed             bool                         `json:"confirmed"`
	PolicyVersion         int64                        `json:"policy_version"`
	CurrentTier           string                       `json:"current_tier"`
	PreviousTier          string                       `json:"previous_tier,omitempty"`
	PreviousStableTier    string                       `json:"previous_stable_tier,omitempty"`
	Main                  AgentGroupTierSummary        `json:"main"`
	Backup                AgentGroupTierSummary        `json:"backup"`
	Emergency             AgentGroupTierSummary        `json:"emergency"`
	AccountIDs            []int64                      `json:"account_ids"`
	AccountNames          []string                     `json:"account_names"`
	Balance               *float64                     `json:"balance,omitempty"`
	Unit                  string                       `json:"unit,omitempty"`
	DataFresh             bool                         `json:"data_fresh"`
	Frozen                bool                         `json:"frozen"`
	FreezeReason          string                       `json:"freeze_reason,omitempty"`
	ManualHoldUntil       *time.Time                   `json:"manual_hold_until,omitempty"`
	ManualOverrideUntil   *time.Time                   `json:"manual_override_until,omitempty"`
	CooldownUntil         *time.Time                   `json:"cooldown_until,omitempty"`
	ReturnBlockedUntil    *time.Time                   `json:"return_blocked_until,omitempty"`
	LastSwitchAt          *time.Time                   `json:"last_switch_at,omitempty"`
	LastTransitionAt      *time.Time                   `json:"last_transition_at,omitempty"`
	VerificationStartedAt *time.Time                   `json:"verification_started_at,omitempty"`
	HealthySince          *time.Time                   `json:"healthy_since,omitempty"`
	RecoveryHealthyCount  int                          `json:"recovery_healthy_count"`
	LastConfirmedAt       *time.Time                   `json:"last_confirmed_at,omitempty"`
	RecentTransitions     []AgentGroupTransitionResult `json:"recent_transitions"`
}

type AnalysisPacket struct {
	ID                   int64                     `json:"id"`
	Kind                 string                    `json:"kind"`
	CutoffAt             time.Time                 `json:"cutoff_at"`
	PreviousPacketID     *int64                    `json:"previous_packet_id,omitempty"`
	Hash                 string                    `json:"hash"`
	TokenEstimate        int                       `json:"token_estimate"`
	NoMaterialChange     bool                      `json:"no_material_change"`
	DataHealth           AgentDataHealth           `json:"data_health"`
	SystemSummary        AgentSystemSummary        `json:"system_summary"`
	PoolSummaries        []AgentPoolSummary        `json:"pool_summaries"`
	GroupFailoverTokens  []AgentGroupFailoverToken `json:"group_failover_tokens"`
	AccountCompactStates []AgentAccountState       `json:"account_compact_states"`
	Anomalies            []AgentAccountState       `json:"anomalies"`
	Changes              []string                  `json:"changes"`
	ActivePolicies       json.RawMessage           `json:"active_policies"`
	DecisionOutcomes     json.RawMessage           `json:"decision_outcomes"`
	EvidenceCatalog      []string                  `json:"evidence_catalog"`
	CreatedAt            time.Time                 `json:"created_at"`
}

type AvailabilityAssessment struct {
	ID                int64     `json:"id"`
	PacketID          int64     `json:"packet_id"`
	AccountID         int64     `json:"account_id"`
	State             string    `json:"state"`
	AvailabilityScore float64   `json:"availability_score"`
	PerformanceScore  float64   `json:"performance_score"`
	StabilityScore    float64   `json:"stability_score"`
	CapacityScore     float64   `json:"capacity_score"`
	CostScore         float64   `json:"cost_score"`
	Confidence        float64   `json:"confidence"`
	EvidenceConflict  bool      `json:"evidence_conflict"`
	ReasonsJSON       string    `json:"reasons_json"`
	CreatedAt         time.Time `json:"created_at"`
}

type AgentRun struct {
	ID             int64           `json:"id"`
	Kind           string          `json:"kind"`
	Trigger        string          `json:"trigger"`
	Status         string          `json:"status"`
	ProviderSlot   string          `json:"provider_slot,omitempty"`
	Model          string          `json:"model,omitempty"`
	PacketID       *int64          `json:"packet_id,omitempty"`
	ConversationID *int64          `json:"conversation_id,omitempty"`
	Summary        string          `json:"summary,omitempty"`
	Conclusion     string          `json:"conclusion,omitempty"`
	Confidence     float64         `json:"confidence"`
	ActionsJSON    json.RawMessage `json:"actions,omitempty"`
	Error          string          `json:"error,omitempty"`
	StartedAt      time.Time       `json:"started_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

type AgentToolCall struct {
	ID          int64           `json:"id"`
	RunID       int64           `json:"run_id"`
	Tool        string          `json:"tool"`
	Arguments   json.RawMessage `json:"arguments"`
	Status      string          `json:"status"`
	BeforeState string          `json:"before_state,omitempty"`
	AfterState  string          `json:"after_state,omitempty"`
	Result      string          `json:"result,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

type ScorePolicyVersion struct {
	ID                      int64            `json:"id"`
	ScopeType               string           `json:"scope_type"`
	ScopeID                 string           `json:"scope_id"`
	Version                 int              `json:"version"`
	Status                  string           `json:"status"`
	Config                  json.RawMessage  `json:"config"`
	Patch                   json.RawMessage  `json:"patch"`
	Diff                    json.RawMessage  `json:"diff"`
	Simulation              PolicySimulation `json:"simulation"`
	RiskLevel               string           `json:"risk_level"`
	AffectedAccountIDs      []int64          `json:"affected_account_ids"`
	Reason                  string           `json:"reason"`
	AgentRunID              *int64           `json:"agent_run_id,omitempty"`
	SourceGoalID            *int64           `json:"source_goal_id,omitempty"`
	BaseVersionID           *int64           `json:"base_version_id,omitempty"`
	PreviousActiveVersionID *int64           `json:"previous_active_version_id,omitempty"`
	CreatedBy               string           `json:"created_by"`
	ApprovedBy              string           `json:"approved_by,omitempty"`
	IdempotencyKey          string           `json:"idempotency_key,omitempty"`
	SemanticHash            string           `json:"semantic_hash,omitempty"`
	RollbackReason          string           `json:"rollback_reason,omitempty"`
	OutcomeSummary          string           `json:"outcome_summary,omitempty"`
	AutoRollbackCount       int              `json:"auto_rollback_count"`
	ActivatedAt             *time.Time       `json:"activated_at,omitempty"`
	CreatedAt               time.Time        `json:"created_at"`
}

type PolicySimulation struct {
	Window               string   `json:"window"`
	SampleCount          int      `json:"sample_count"`
	CurrentActionCount   int      `json:"current_action_count"`
	ProposedActionCount  int      `json:"proposed_action_count"`
	PauseDelta           int      `json:"pause_delta"`
	ResumeDelta          int      `json:"resume_delta"`
	LoadAdjustmentDelta  int      `json:"load_adjustment_delta"`
	FlapDelta            int      `json:"flap_delta"`
	BaselineSuccessRate  float64  `json:"baseline_success_rate"`
	DataSufficient       bool     `json:"data_sufficient"`
	Passed               bool     `json:"passed"`
	UnsimmulatableFields []string `json:"unsimmulatable_fields,omitempty"`
}

type DecisionOutcome struct {
	ID                        int64      `json:"id"`
	RunID                     int64      `json:"run_id"`
	ToolCallID                *int64     `json:"tool_call_id,omitempty"`
	AccountID                 *int64     `json:"account_id,omitempty"`
	PredictedSuccessRateDelta float64    `json:"predicted_success_rate_delta"`
	PredictedLatencyDeltaMS   int64      `json:"predicted_latency_delta_ms"`
	PredictedCostDelta        float64    `json:"predicted_cost_delta"`
	EvaluateAt                time.Time  `json:"evaluate_at"`
	ActualSuccessRateDelta    *float64   `json:"actual_success_rate_delta,omitempty"`
	ActualLatencyDeltaMS      *int64     `json:"actual_latency_delta_ms,omitempty"`
	ActualCostDelta           *float64   `json:"actual_cost_delta,omitempty"`
	Verdict                   string     `json:"verdict,omitempty"`
	EvaluatedAt               *time.Time `json:"evaluated_at,omitempty"`
	CreatedAt                 time.Time  `json:"created_at"`
}

type AgentDailyReport struct {
	ID          int64           `json:"id"`
	ReportDate  string          `json:"report_date"`
	PacketID    *int64          `json:"packet_id,omitempty"`
	RunID       *int64          `json:"run_id,omitempty"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	Summary     string          `json:"summary"`
	MetricsJSON json.RawMessage `json:"metrics"`
	AdviceJSON  json.RawMessage `json:"advice"`
	Error       string          `json:"error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type AgentConversation struct {
	ID        int64     `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type AgentMessage struct {
	ID             int64     `json:"id"`
	ConversationID int64     `json:"conversation_id"`
	Role           string    `json:"role"`
	Content        string    `json:"content"`
	RunID          *int64    `json:"run_id,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}
