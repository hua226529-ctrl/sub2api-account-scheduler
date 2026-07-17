package model

import (
	"encoding/json"
	"time"
)

const (
	AgentGoalStatusPlanned   = "planned"
	AgentGoalStatusRunning   = "running"
	AgentGoalStatusWaiting   = "waiting"
	AgentGoalStatusCompleted = "completed"
	AgentGoalStatusFailed    = "failed"
	AgentGoalStatusCancelled = "cancelled"

	AgentLaneInteractive = "interactive"
	AgentLaneBackground  = "background"

	AgentStepStatusPending      = "pending"
	AgentStepStatusScheduled    = "scheduled"
	AgentStepStatusRunning      = "running"
	AgentStepStatusVerifying    = "verifying"
	AgentStepStatusCompensating = "compensating"
	AgentStepStatusReconciling  = "reconciling"
	AgentStepStatusCompleted    = "completed"
	AgentStepStatusFailed       = "failed"
	AgentStepStatusCancelled    = "cancelled"
	AgentStepStatusSkipped      = "skipped"

	AgentCommandStatusPending     = "pending"
	AgentCommandStatusLeased      = "leased"
	AgentCommandStatusReconciling = "reconciling"
	AgentCommandStatusCompleted   = "completed"
	AgentCommandStatusFailed      = "failed"
	AgentCommandStatusCancelled   = "cancelled"
	AgentCommandStatusExpired     = "expired"

	AgentRiskReadOnly = "read_only"
	AgentRiskLow      = "low"
	AgentRiskMedium   = "medium"
	AgentRiskHigh     = "high"
	AgentRiskCritical = "critical"

	AgentMemoryEpisodic   = "episodic"
	AgentMemorySemantic   = "semantic"
	AgentMemoryProcedural = "procedural"
	AgentMemoryDecision   = "decision"

	AgentFreezeModeActive       = "active"
	AgentFreezeModeAgentPaused  = "agent_paused"
	AgentFreezeModeReadOnly     = "read_only"
	AgentFreezeModeWritesFrozen = "writes_frozen"

	AgentDefaultTimezone = "Asia/Shanghai"
)

// AgentGoal is a durable objective. It contains no credentials and points to
// immutable evidence through Context rather than embedding raw request data.
type AgentGoal struct {
	ID             int64           `json:"id"`
	ParentGoalID   *int64          `json:"parent_goal_id,omitempty"`
	ConversationID *int64          `json:"conversation_id,omitempty"`
	Title          string          `json:"title"`
	Objective      string          `json:"objective"`
	Status         string          `json:"status"`
	Lane           string          `json:"lane"`
	Priority       int             `json:"priority"`
	RiskLevel      string          `json:"risk_level"`
	Source         string          `json:"source"`
	Context        json.RawMessage `json:"context"`
	PlanHash       string          `json:"plan_hash,omitempty"`
	CreatedBy      string          `json:"created_by"`
	DeadlineAt     *time.Time      `json:"deadline_at,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
	LeaseOwner     string          `json:"lease_owner,omitempty"`
	LeaseUntil     *time.Time      `json:"lease_until,omitempty"`
	NextRunnableAt *time.Time      `json:"next_runnable_at,omitempty"`
}

// AgentStep is one typed capability invocation in a goal plan. IdempotencyKey
// is globally unique so a model retry cannot duplicate an external mutation.
type AgentStep struct {
	ID              int64           `json:"id"`
	GoalID          int64           `json:"goal_id"`
	Sequence        int             `json:"sequence"`
	DependsOnStepID *int64          `json:"depends_on_step_id,omitempty"`
	Capability      string          `json:"capability"`
	Arguments       json.RawMessage `json:"arguments"`
	Preconditions   json.RawMessage `json:"preconditions"`
	Compensation    json.RawMessage `json:"compensation"`
	Status          string          `json:"status"`
	RiskLevel       string          `json:"risk_level"`
	IdempotencyKey  string          `json:"idempotency_key"`
	ScheduledFor    *time.Time      `json:"scheduled_for,omitempty"`
	ExpiresAt       *time.Time      `json:"expires_at,omitempty"`
	LeaseOwner      string          `json:"lease_owner,omitempty"`
	LeaseUntil      *time.Time      `json:"lease_until,omitempty"`
	AttemptCount    int             `json:"attempt_count"`
	MaxAttempts     int             `json:"max_attempts"`
	// MutationAttemptedAt is persisted atomically with BeforeState before the
	// executor is allowed to enter a mutating capability. A nil value proves
	// that an interrupted running step never crossed the external-write gate.
	MutationAttemptedAt *time.Time      `json:"mutation_attempted_at,omitempty"`
	BeforeState         json.RawMessage `json:"before_state"`
	AfterState          json.RawMessage `json:"after_state"`
	Result              json.RawMessage `json:"result"`
	LastError           string          `json:"last_error,omitempty"`
	CreatedAt           time.Time       `json:"created_at"`
	UpdatedAt           time.Time       `json:"updated_at"`
	CompletedAt         *time.Time      `json:"completed_at,omitempty"`
}

// AgentEvent is append-only. EventKey is supplied by the producer and is the
// deduplication boundary for retries.
type AgentEvent struct {
	ID        int64           `json:"id"`
	EventKey  string          `json:"event_key"`
	GoalID    *int64          `json:"goal_id,omitempty"`
	StepID    *int64          `json:"step_id,omitempty"`
	Type      string          `json:"type"`
	Severity  string          `json:"severity"`
	Actor     string          `json:"actor"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

type ActionConfirmation struct {
	ID            int64           `json:"id"`
	GoalID        int64           `json:"goal_id"`
	Administrator string          `json:"administrator"`
	TokenHash     string          `json:"-"`
	PayloadHash   string          `json:"-"`
	Resources     json.RawMessage `json:"resources"`
	Operation     string          `json:"operation"`
	DesiredState  json.RawMessage `json:"desired_state"`
	ProposalID    *int64          `json:"proposal_id,omitempty"`
	Status        string          `json:"status"`
	ExpiresAt     time.Time       `json:"expires_at"`
	ConsumedAt    *time.Time      `json:"consumed_at,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

// AgentCheckpoint stores the last fully persisted state needed to resume a
// plan without reconstructing it from model conversation text.
type AgentCheckpoint struct {
	ID        int64           `json:"id"`
	GoalID    int64           `json:"goal_id"`
	StepID    *int64          `json:"step_id,omitempty"`
	Kind      string          `json:"kind"`
	State     json.RawMessage `json:"state"`
	StateHash string          `json:"state_hash"`
	CreatedAt time.Time       `json:"created_at"`
}

// AgentMemory is compact, pre-summarized memory. Pinned entries are ranked
// first during normal reads but remain subject to the global retention limit.
type AgentMemory struct {
	ID         int64           `json:"id"`
	ScopeType  string          `json:"scope_type"`
	ScopeID    string          `json:"scope_id"`
	Kind       string          `json:"kind"`
	Key        string          `json:"key"`
	Summary    string          `json:"summary"`
	Content    json.RawMessage `json:"content"`
	Importance float64         `json:"importance"`
	Pinned     bool            `json:"pinned"`
	ExpiresAt  *time.Time      `json:"expires_at,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// ScheduledCommand is a durable command envelope. A lease only grants one
// worker temporary ownership; the capability executor must still revalidate
// all Preconditions immediately before an external write.
type ScheduledCommand struct {
	ID             int64           `json:"id"`
	GoalID         *int64          `json:"goal_id,omitempty"`
	StepID         *int64          `json:"step_id,omitempty"`
	Capability     string          `json:"capability"`
	Arguments      json.RawMessage `json:"arguments"`
	Conditions     json.RawMessage `json:"conditions"`
	IntentType     string          `json:"intent_type"`
	ResourceType   string          `json:"resource_type"`
	ResourceIDs    json.RawMessage `json:"resource_ids"`
	Operation      string          `json:"operation"`
	DesiredState   json.RawMessage `json:"desired_state"`
	Status         string          `json:"status"`
	Timezone       string          `json:"timezone"`
	ExecuteAt      time.Time       `json:"execute_at"`
	ExpiresAt      *time.Time      `json:"expires_at,omitempty"`
	IdempotencyKey string          `json:"idempotency_key"`
	OccurrenceID   string          `json:"occurrence_id"`
	MissedPolicy   string          `json:"missed_policy"`
	Authority      string          `json:"authority"`
	LeaseOwner     string          `json:"lease_owner,omitempty"`
	LeaseUntil     *time.Time      `json:"lease_until,omitempty"`
	AttemptCount   int             `json:"attempt_count"`
	MaxAttempts    int             `json:"max_attempts"`
	// MutationAttemptedAt has the same write-gate semantics as AgentStep. A
	// claimed command with no marker can be safely released after a crash.
	MutationAttemptedAt *time.Time      `json:"mutation_attempted_at,omitempty"`
	Result              json.RawMessage `json:"result"`
	LastError           string          `json:"last_error,omitempty"`
	CreatedBy           string          `json:"created_by"`
	CreatedAt           time.Time       `json:"created_at"`
	UpdatedAt           time.Time       `json:"updated_at"`
	CompletedAt         *time.Time      `json:"completed_at,omitempty"`
}

// AgentFreezeState is an operator-owned write barrier. The agent may request
// a stricter mode, but a future policy layer must prevent it from relaxing a
// freeze created by an operator.
type AgentFreezeState struct {
	ScopeType string     `json:"scope_type"`
	ScopeID   string     `json:"scope_id"`
	Mode      string     `json:"mode"`
	Reason    string     `json:"reason"`
	Actor     string     `json:"actor"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

type AgentRecoverySummary struct {
	ReconcilingSteps    int `json:"reconciling_steps"`
	ReconcilingCommands int `json:"reconciling_commands"`
	ReplannedSteps      int `json:"replanned_steps"`
	RequeuedCommands    int `json:"requeued_commands"`
	ExpiredCommands     int `json:"expired_commands"`
	FailedCommands      int `json:"failed_commands"`
}
