package controlplane

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrInvalidIntent = errors.New("invalid control-plane intent")

type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

func (e *ValidationError) Unwrap() error { return ErrInvalidIntent }

func invalidIntent(field, message string) error {
	return &ValidationError{Field: field, Message: message}
}

type IntentMetadata struct {
	ID              string
	IdempotencyKey  string
	Producer        Producer
	Authority       Authority
	Actor           string
	Reason          string
	EvidenceRefs    []string
	PolicyVersion   string
	SnapshotVersion string
	CreatedAt       time.Time
	ExpiresAt       *time.Time
}

type Intent struct {
	ID              string
	IdempotencyKey  string
	Producer        Producer
	Authority       Authority
	Resource        Resource
	Operation       Operation
	DesiredState    DesiredState
	Actor           string
	Reason          string
	EvidenceRefs    []string
	PolicyVersion   string
	SnapshotVersion string
	CreatedAt       time.Time
	ExpiresAt       *time.Time
}

func NewAccountSchedulableIntent(metadata IntentMetadata, accountID int64, schedulable bool) (Intent, error) {
	resource, err := NewAccountResource(accountID)
	if err != nil {
		return Intent{}, err
	}
	return newIntent(metadata, resource, OperationSetAccountSchedulable, schedulableState(schedulable))
}

func NewAccountLoadFactorIntent(metadata IntentMetadata, accountID int64, loadFactor *int) (Intent, error) {
	resource, err := NewAccountResource(accountID)
	if err != nil {
		return Intent{}, err
	}
	desired, err := loadFactorState(loadFactor)
	if err != nil {
		return Intent{}, err
	}
	return newIntent(metadata, resource, OperationSetAccountLoadFactor, desired)
}

func NewUpstreamKeyGroupTierIntent(metadata IntentMetadata, sourceID int64, keyID, targetTier string) (Intent, error) {
	resource, err := NewUpstreamKeyResource(sourceID, keyID)
	if err != nil {
		return Intent{}, err
	}
	desired, err := groupTierState(targetTier)
	if err != nil {
		return Intent{}, err
	}
	return newIntent(metadata, resource, OperationSetUpstreamKeyGroupTier, desired)
}

func newIntent(metadata IntentMetadata, resource Resource, operation Operation, desired DesiredState) (Intent, error) {
	metadata = normalizeMetadata(metadata)
	intent := Intent{
		ID: metadata.ID, IdempotencyKey: metadata.IdempotencyKey,
		Producer: metadata.Producer, Authority: metadata.Authority,
		Resource: resource, Operation: operation, DesiredState: desired,
		Actor: metadata.Actor, Reason: metadata.Reason,
		EvidenceRefs:  append([]string(nil), metadata.EvidenceRefs...),
		PolicyVersion: metadata.PolicyVersion, SnapshotVersion: metadata.SnapshotVersion,
		CreatedAt: metadata.CreatedAt, ExpiresAt: cloneTimePointer(metadata.ExpiresAt),
	}
	if err := intent.Validate(); err != nil {
		return Intent{}, err
	}
	return intent, nil
}

func normalizeMetadata(metadata IntentMetadata) IntentMetadata {
	metadata.ID = strings.TrimSpace(metadata.ID)
	metadata.IdempotencyKey = strings.TrimSpace(metadata.IdempotencyKey)
	metadata.Actor = strings.TrimSpace(metadata.Actor)
	metadata.Reason = strings.TrimSpace(metadata.Reason)
	metadata.PolicyVersion = strings.TrimSpace(metadata.PolicyVersion)
	metadata.SnapshotVersion = strings.TrimSpace(metadata.SnapshotVersion)
	metadata.EvidenceRefs = append([]string(nil), metadata.EvidenceRefs...)
	for index := range metadata.EvidenceRefs {
		metadata.EvidenceRefs[index] = strings.TrimSpace(metadata.EvidenceRefs[index])
	}
	metadata.ExpiresAt = cloneTimePointer(metadata.ExpiresAt)
	return metadata
}

func (i Intent) Validate() error {
	if strings.TrimSpace(i.ID) == "" {
		return invalidIntent("id", "ID is required")
	}
	if strings.TrimSpace(i.IdempotencyKey) == "" {
		return invalidIntent("idempotency_key", "idempotency key is required")
	}
	if !i.Producer.Valid() {
		return invalidIntent("producer", "unknown producer")
	}
	if !i.Authority.Valid() {
		return invalidIntent("authority", "unknown authority")
	}
	if err := i.Resource.Validate(); err != nil {
		return err
	}
	if !i.Operation.Valid() {
		return invalidIntent("operation", "unknown operation")
	}
	if err := i.DesiredState.validateFor(i.Resource, i.Operation); err != nil {
		return err
	}
	if strings.TrimSpace(i.Actor) == "" {
		return invalidIntent("actor", "actor is required")
	}
	if strings.TrimSpace(i.Reason) == "" {
		return invalidIntent("reason", "reason is required")
	}
	if i.CreatedAt.IsZero() {
		return invalidIntent("created_at", "created time must be explicit")
	}
	if i.ExpiresAt != nil && !i.ExpiresAt.After(i.CreatedAt) {
		return invalidIntent("expires_at", "expiration must be after creation")
	}
	if isAutomationAuthority(i.Authority) && strings.TrimSpace(i.SnapshotVersion) == "" {
		return invalidIntent("snapshot_version", "automation intent requires snapshot version")
	}
	if i.Authority == AuthorityActivePolicy && strings.TrimSpace(i.PolicyVersion) == "" {
		return invalidIntent("policy_version", "active policy intent requires policy version")
	}
	if i.Authority == AuthorityAutonomousAgent || i.Authority == AuthorityEmergencyAutomation {
		if len(i.EvidenceRefs) == 0 {
			return invalidIntent("evidence_refs", "authority requires evidence")
		}
		for _, reference := range i.EvidenceRefs {
			if strings.TrimSpace(reference) == "" {
				return invalidIntent("evidence_refs", "evidence reference cannot be empty")
			}
		}
	}
	switch i.Authority {
	case AuthorityAutonomousAgent:
		if i.ExpiresAt == nil {
			return invalidIntent("expires_at", "autonomous agent override must be temporary")
		}
	case AuthorityOptimization:
		if i.ExpiresAt == nil {
			return invalidIntent("expires_at", "optimization override must be temporary")
		}
	case AuthorityAdministratorCommand:
		if i.ExpiresAt == nil {
			return invalidIntent("expires_at", "administrator command requires TTL; use ManualHold for a permanent hold")
		}
	}
	return nil
}

func (i Intent) ConflictKey() ConflictKey {
	return ConflictKey{Resource: i.Resource, Operation: i.Operation}
}

func (i Intent) Expired(now time.Time) bool {
	return i.ExpiresAt != nil && !now.Before(*i.ExpiresAt)
}

func isAutomationAuthority(authority Authority) bool {
	switch authority {
	case AuthorityEmergencyAutomation, AuthorityAutonomousAgent, AuthorityActivePolicy, AuthorityOptimization:
		return true
	default:
		return false
	}
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneIntent(intent Intent) Intent {
	intent.EvidenceRefs = append([]string(nil), intent.EvidenceRefs...)
	intent.ExpiresAt = cloneTimePointer(intent.ExpiresAt)
	return intent
}
