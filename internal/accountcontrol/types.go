package accountcontrol

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

const (
	DefaultAdministratorTTL = 30 * time.Minute
	DefaultAutonomousTTL    = 15 * time.Minute
	MaximumAutonomousTTL    = 2 * time.Hour
)

type MutationStatus string

const (
	StatusPrepared    MutationStatus = "prepared"
	StatusValidating  MutationStatus = "validating"
	StatusExecuting   MutationStatus = "executing"
	StatusVerifying   MutationStatus = "verifying"
	StatusApplied     MutationStatus = "applied"
	StatusAppliedNoop MutationStatus = "applied_noop"
	StatusBlocked     MutationStatus = "blocked"
	StatusFailed      MutationStatus = "failed"
	StatusUncertain   MutationStatus = "uncertain"
	StatusSuperseded  MutationStatus = "superseded"
	StatusExpired     MutationStatus = "expired"
)

func (s MutationStatus) Terminal() bool {
	switch s {
	case StatusApplied, StatusAppliedNoop, StatusBlocked, StatusFailed, StatusSuperseded, StatusExpired:
		return true
	default:
		return false
	}
}

type OverrideStatus string

const (
	OverridePending        OverrideStatus = "pending"
	OverrideActive         OverrideStatus = "active"
	OverrideRevoked        OverrideStatus = "revoked"
	OverrideExpired        OverrideStatus = "expired"
	OverrideBlocked        OverrideStatus = "blocked"
	OverrideFailed         OverrideStatus = "failed"
	OverrideSuperseded     OverrideStatus = "superseded"
	OverrideLegacyDisabled OverrideStatus = "legacy_disabled"
)

type OverrideKind string

const (
	OverrideKindTemporary  OverrideKind = "temporary"
	OverrideKindManualHold OverrideKind = "manual_hold"
	OverrideKindLoadPin    OverrideKind = "load_pin"
	OverrideKindLegacy     OverrideKind = "legacy"
)

type BlockReason string

const (
	BlockNone              BlockReason = ""
	BlockWritesFrozen      BlockReason = "writes_frozen"
	BlockAgentWritesFrozen BlockReason = "agent_writes_frozen"
	BlockAccountNotFound   BlockReason = "account_not_found"
	BlockCredentialInvalid BlockReason = "credential_invalid"
	BlockHealthLocked      BlockReason = "health_locked"
	BlockBalanceLocked     BlockReason = "balance_locked"
	BlockCostLocked        BlockReason = "cost_locked"
	BlockRateLimited       BlockReason = "rate_limited"
	BlockStaleTelemetry    BlockReason = "stale_telemetry"
	BlockCooldown          BlockReason = "cooldown"
	BlockInvalidTarget     BlockReason = "invalid_target"
)

type AccountState struct {
	Schedulable bool `json:"schedulable"`
	LoadFactor  *int `json:"load_factor,omitempty"`
}

type Override struct {
	ID                string
	CommandID         string
	IntentID          string
	IdempotencyKey    string
	SemanticSignature string
	AccountID         int64
	Operation         controlplane.Operation
	Kind              OverrideKind
	Schedulable       *bool
	LoadFactor        *int
	LoadFactorSet     bool
	Producer          controlplane.Producer
	Authority         controlplane.Authority
	Actor             string
	Reason            string
	EvidenceRefs      []string
	PolicyVersion     string
	SnapshotVersion   string
	CreatedAt         time.Time
	ExpiresAt         *time.Time
	Status            OverrideStatus
	MutationID        string
	RevokedAt         *time.Time
	RevokedBy         string
	RevokeReason      string
	UpdatedAt         time.Time
}

func OverrideFromIntent(id, commandID string, intent controlplane.Intent) (Override, error) {
	if err := intent.Validate(); err != nil {
		return Override{}, err
	}
	if intent.Authority == controlplane.AuthorityActivePolicy {
		return Override{}, errors.New("active policy is not a persistent override")
	}
	accountID, ok := intent.Resource.AccountID()
	if !ok {
		return Override{}, errors.New("account override requires an account resource")
	}
	signature, err := controlplane.SemanticSignature(intent)
	if err != nil {
		return Override{}, err
	}
	override := Override{
		ID: id, CommandID: commandID, IntentID: intent.ID, IdempotencyKey: intent.IdempotencyKey,
		SemanticSignature: signature, AccountID: accountID, Operation: intent.Operation,
		Producer: intent.Producer, Authority: intent.Authority, Actor: intent.Actor, Reason: intent.Reason,
		EvidenceRefs: append([]string(nil), intent.EvidenceRefs...), PolicyVersion: intent.PolicyVersion,
		SnapshotVersion: intent.SnapshotVersion, CreatedAt: intent.CreatedAt, ExpiresAt: cloneTime(intent.ExpiresAt),
		Status: OverridePending, UpdatedAt: intent.CreatedAt,
	}
	if intent.Authority == controlplane.AuthorityManualHold {
		override.Kind = OverrideKindManualHold
	} else {
		override.Kind = OverrideKindTemporary
	}
	switch intent.Operation {
	case controlplane.OperationSetAccountSchedulable:
		value, valid := intent.DesiredState.Schedulable()
		if !valid {
			return Override{}, errors.New("invalid schedulable override desired state")
		}
		override.Schedulable = &value
	case controlplane.OperationSetAccountLoadFactor:
		value, configured, valid := intent.DesiredState.LoadFactor()
		if !valid {
			return Override{}, errors.New("invalid load-factor override desired state")
		}
		override.LoadFactorSet = configured
		if configured {
			override.LoadFactor = &value
		}
	default:
		return Override{}, errors.New("unsupported account override operation")
	}
	return override, nil
}

func (o Override) Intent() (controlplane.Intent, error) {
	metadata := controlplane.IntentMetadata{
		ID: o.IntentID, IdempotencyKey: o.IdempotencyKey, Producer: o.Producer, Authority: o.Authority,
		Actor: o.Actor, Reason: o.Reason, EvidenceRefs: append([]string(nil), o.EvidenceRefs...),
		PolicyVersion: o.PolicyVersion, SnapshotVersion: o.SnapshotVersion, CreatedAt: o.CreatedAt,
		ExpiresAt: cloneTime(o.ExpiresAt),
	}
	switch o.Operation {
	case controlplane.OperationSetAccountSchedulable:
		if o.Schedulable == nil {
			return controlplane.Intent{}, errors.New("stored schedulable override has no desired value")
		}
		return controlplane.NewAccountSchedulableIntent(metadata, o.AccountID, *o.Schedulable)
	case controlplane.OperationSetAccountLoadFactor:
		var value *int
		if o.LoadFactorSet {
			if o.LoadFactor == nil {
				return controlplane.Intent{}, errors.New("stored load-factor override has no configured value")
			}
			value = cloneInt(o.LoadFactor)
		}
		return controlplane.NewAccountLoadFactorIntent(metadata, o.AccountID, value)
	default:
		return controlplane.Intent{}, errors.New("stored override operation is unsupported")
	}
}

type Mutation struct {
	ID                     string
	CommandID              string
	IntentID               string
	IdempotencyKey         string
	SemanticSignature      string
	AccountID              int64
	Operation              controlplane.Operation
	RequestedSchedulable   *bool
	RequestedLoadFactor    *int
	RequestedLoadSet       bool
	WinningIntentID        string
	WinningIdempotencyKey  string
	WinningProducer        controlplane.Producer
	WinningAuthority       controlplane.Authority
	WinningActor           string
	WinningReason          string
	WinningEvidenceRefs    []string
	WinningPolicyVersion   string
	WinningSnapshotVersion string
	WinningCreatedAt       time.Time
	WinningExpiresAt       *time.Time
	WinningSchedulable     *bool
	WinningLoadFactor      *int
	WinningLoadSet         bool
	WinningOverrideKind    OverrideKind
	Producer               controlplane.Producer
	Authority              controlplane.Authority
	Actor                  string
	ReasonCode             string
	Reason                 string
	PolicyVersion          string
	SnapshotVersion        string
	ExpiresAt              *time.Time
	Status                 MutationStatus
	AttemptCount           int
	Before                 *AccountState
	After                  *AccountState
	LastErrorCode          string
	CreatedAt              time.Time
	UpdatedAt              time.Time
	CompletedAt            *time.Time
	OverrideID             string
	RevokeOverrideID       string
	TelemetryFresh         bool
	CooldownActive         bool
}

type Result struct {
	CommandID        string                 `json:"command_id"`
	MutationID       string                 `json:"mutation_id"`
	IntentID         string                 `json:"intent_id"`
	AccountID        int64                  `json:"account_id"`
	Operation        controlplane.Operation `json:"operation"`
	Requested        AccountState           `json:"requested"`
	RequestedLoadSet bool                   `json:"requested_load_factor_set,omitempty"`
	WinningAuthority controlplane.Authority `json:"winning_authority"`
	Status           MutationStatus         `json:"status"`
	BlockedReason    BlockReason            `json:"blocked_reason,omitempty"`
	Before           *AccountState          `json:"before,omitempty"`
	VerifiedAfter    *AccountState          `json:"verified_after,omitempty"`
	ExpiresAt        *time.Time             `json:"expires_at,omitempty"`
	IdempotentReplay bool                   `json:"idempotent_replay"`
	Uncertain        bool                   `json:"uncertain"`
}

type Submission struct {
	CommandID                string
	Intent                   controlplane.Intent
	RequestIdempotencyKey    string
	RequestSemanticSignature string
	PersistOverride          bool
	OverrideKind             OverrideKind
	RevokeOverrideID         string
	Safety                   SafetyContext
	Event                    model.Event
	FlapPolicy               *model.FlapPolicy
}

type SafetyContext struct {
	TelemetryFresh bool
	CooldownActive bool
}

type Finalization struct {
	Mutation         Mutation
	Control          model.AccountControl
	Event            model.Event
	ActivateOverride bool
	OverrideStatus   OverrideStatus
	RevokeOverrideID string
	RevokeActor      string
	RevokeReason     string
	FlapPolicy       *model.FlapPolicy
}

type Repository interface {
	GetControl(context.Context, int64) (model.AccountControl, error)
	GetActiveBalanceLock(context.Context, int64) (*model.BalanceLock, error)
	GetActiveCostLock(context.Context, int64) (*model.CostLock, error)
	GetAgentFreezeState(context.Context, string, string) (model.AgentFreezeState, error)
	ListActiveAccountOverrides(context.Context, int64, controlplane.Operation, time.Time) ([]Override, error)
	FindActiveAccountOverride(context.Context, int64, controlplane.Operation, controlplane.Authority, time.Time) (*Override, error)
	GetAccountOverrideRevision(context.Context, int64, controlplane.Operation, time.Time) (string, error)
	FindAccountMutationByIdempotency(context.Context, string) (*Mutation, error)
	PrepareAccountMutation(context.Context, Mutation, *Override) (Mutation, bool, error)
	UpdateAccountMutation(context.Context, Mutation) error
	FinalizeAccountMutation(context.Context, Finalization) error
	ListPendingAccountMutations(context.Context, int) ([]Mutation, error)
}

type Transport interface {
	ListAccounts(context.Context) ([]model.Account, error)
	SetSchedulable(context.Context, int64, bool) (model.Account, error)
	UpdateLoadFactor(context.Context, int64, *int) (model.Account, error)
}

type IdempotencyConflictError struct {
	Key string
}

func (e *IdempotencyConflictError) Error() string {
	return fmt.Sprintf("idempotency key %q is already bound to different semantics", e.Key)
}

type BlockedError struct {
	Result Result
}

func (e *BlockedError) Error() string {
	return fmt.Sprintf("account mutation blocked: %s", e.Result.BlockedReason)
}

type MutationStateError struct {
	Result Result
	Cause  error
}

func (e *MutationStateError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("account mutation is %s: %v", e.Result.Status, e.Cause)
	}
	return fmt.Sprintf("account mutation is %s", e.Result.Status)
}

func (e *MutationStateError) Unwrap() error { return e.Cause }

func NewCommandID() (string, error) {
	buffer := make([]byte, 24)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate command id: %w", err)
	}
	return "cmd-" + base64.RawURLEncoding.EncodeToString(buffer), nil
}

func ValidateCommandID(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("command id is required")
	}
	if len(value) > 256 {
		return errors.New("command id is too long")
	}
	return nil
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func cloneControlplaneIntent(intent controlplane.Intent) controlplane.Intent {
	intent.EvidenceRefs = append([]string(nil), intent.EvidenceRefs...)
	intent.ExpiresAt = cloneTime(intent.ExpiresAt)
	return intent
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
