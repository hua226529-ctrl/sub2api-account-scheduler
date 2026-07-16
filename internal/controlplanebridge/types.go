package controlplanebridge

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
)

type ConversionStatus string

const (
	ConversionMapped      ConversionStatus = "mapped"
	ConversionUnsupported ConversionStatus = "unsupported"
	ConversionIncomplete  ConversionStatus = "incomplete"
	ConversionInvalid     ConversionStatus = "invalid"
)

func (s ConversionStatus) Valid() bool {
	switch s {
	case ConversionMapped, ConversionUnsupported, ConversionIncomplete, ConversionInvalid:
		return true
	default:
		return false
	}
}

func (s ConversionStatus) String() string {
	if s.Valid() {
		return string(s)
	}
	return "unknown_conversion_status(" + strconv.Quote(string(s)) + ")"
}

type GapCode string

const (
	GapNone                        GapCode = ""
	GapMissingTTL                  GapCode = "missing_ttl"
	GapMissingActor                GapCode = "missing_actor"
	GapMissingAuthorityContext     GapCode = "missing_authority_context"
	GapMissingSnapshotVersion      GapCode = "missing_snapshot_version"
	GapMissingPolicyVersion        GapCode = "missing_policy_version"
	GapMissingEvidence             GapCode = "missing_evidence"
	GapAmbiguousManualResume       GapCode = "ambiguous_manual_resume"
	GapLegacyPermanentAgentControl GapCode = "legacy_permanent_agent_control"
	GapUnsupportedOperation        GapCode = "unsupported_operation"
	GapInvalidDesiredState         GapCode = "invalid_desired_state"
	GapMissingIdempotencySource    GapCode = "missing_idempotency_source"
	GapMissingReason               GapCode = "missing_reason"
	GapMissingCreatedAt            GapCode = "missing_created_at"
	GapInvalidExpiration           GapCode = "invalid_expiration"
	GapInvalidResource             GapCode = "invalid_resource"
)

func (g GapCode) Valid() bool {
	switch g {
	case GapNone, GapMissingTTL, GapMissingActor, GapMissingAuthorityContext,
		GapMissingSnapshotVersion, GapMissingPolicyVersion, GapMissingEvidence,
		GapAmbiguousManualResume, GapLegacyPermanentAgentControl,
		GapUnsupportedOperation, GapInvalidDesiredState, GapMissingIdempotencySource,
		GapMissingReason, GapMissingCreatedAt, GapInvalidExpiration, GapInvalidResource:
		return true
	default:
		return false
	}
}

func (g GapCode) String() string {
	if g.Valid() {
		return string(g)
	}
	return "unknown_gap_code(" + strconv.Quote(string(g)) + ")"
}

type ConversionResult struct {
	Intent  *controlplane.Intent
	Status  ConversionStatus
	GapCode GapCode
	Detail  string
}

type StableSourceNamespace string

const (
	SourcePolicyDecision                StableSourceNamespace = "policy_decision"
	SourceAdministratorRequest          StableSourceNamespace = "administrator_request"
	SourceAdministratorGrantConsumption StableSourceNamespace = "administrator_grant_consumption"
	SourceAgentAction                   StableSourceNamespace = "agent_action"
	SourceFailoverTransition            StableSourceNamespace = "failover_transition"
	SourceScheduleOccurrence            StableSourceNamespace = "schedule_occurrence"
	SourceOptimizationAction            StableSourceNamespace = "optimization_action"
)

func (s StableSourceNamespace) Valid() bool {
	switch s {
	case SourcePolicyDecision, SourceAdministratorRequest, SourceAdministratorGrantConsumption,
		SourceAgentAction, SourceFailoverTransition, SourceScheduleOccurrence, SourceOptimizationAction:
		return true
	default:
		return false
	}
}

func (s StableSourceNamespace) String() string {
	if s.Valid() {
		return string(s)
	}
	return "unknown_stable_source_namespace(" + strconv.Quote(string(s)) + ")"
}

type LegacyContext struct {
	StableSourceNamespace StableSourceNamespace
	StableSourceID        string
	Actor                 string
	Reason                string
	EvidenceRefs          []string
	PolicyVersion         string
	SnapshotVersion       string
	CreatedAt             time.Time
	ExpiresAt             *time.Time
}

type AdministratorAuthorization struct {
	IdentityVerified   bool
	ExactGrant         bool
	GrantConsumed      bool
	GrantConsumptionID string
}

type AccountSchedulableInput struct {
	Context     LegacyContext
	AccountID   int64
	Schedulable bool
}

type AccountLoadFactorInput struct {
	Context    LegacyContext
	AccountID  int64
	LoadFactor *int
}

type AccountActionInput struct {
	Context   LegacyContext
	AccountID int64
}

type UpstreamGroupTierInput struct {
	Context    LegacyContext
	SourceID   int64
	KeyID      string
	TargetTier string
}

type LegacyDirectGroupSwitchInput struct {
	Context  LegacyContext
	SourceID int64
	KeyID    string
	GroupID  string
}

type contextRequirements struct {
	ttl      bool
	policy   bool
	snapshot bool
	evidence bool
	agentTTL bool
}

func prepareMetadata(context LegacyContext, producer controlplane.Producer, authority controlplane.Authority, requirements contextRequirements, expectedNamespace StableSourceNamespace, resource controlplane.Resource, operation controlplane.Operation) (controlplane.IntentMetadata, ConversionResult) {
	context = normalizedContext(context)
	if context.Actor == "" {
		return controlplane.IntentMetadata{}, failed(ConversionIncomplete, GapMissingActor, "legacy action has no explicit actor")
	}
	if context.Reason == "" {
		return controlplane.IntentMetadata{}, failed(ConversionIncomplete, GapMissingReason, "legacy action has no explicit reason")
	}
	if context.ExpiresAt == nil && requirements.agentTTL {
		return controlplane.IntentMetadata{}, failed(ConversionIncomplete, GapLegacyPermanentAgentControl, "legacy autonomous control has no finite expiration")
	}
	if context.ExpiresAt == nil && requirements.ttl {
		return controlplane.IntentMetadata{}, failed(ConversionIncomplete, GapMissingTTL, "conversion requires an explicit finite expiration")
	}
	if requirements.policy && context.PolicyVersion == "" {
		return controlplane.IntentMetadata{}, failed(ConversionIncomplete, GapMissingPolicyVersion, "active policy conversion requires the actual policy version")
	}
	if requirements.snapshot && context.SnapshotVersion == "" {
		return controlplane.IntentMetadata{}, failed(ConversionIncomplete, GapMissingSnapshotVersion, "automated conversion requires the actual snapshot version")
	}
	if requirements.evidence && !hasEvidence(context.EvidenceRefs) {
		return controlplane.IntentMetadata{}, failed(ConversionIncomplete, GapMissingEvidence, "automated conversion requires existing evidence references")
	}
	if !context.StableSourceNamespace.Valid() || context.StableSourceNamespace != expectedNamespace || context.StableSourceID == "" {
		return controlplane.IntentMetadata{}, failed(ConversionIncomplete, GapMissingIdempotencySource, "legacy action has no stable business-action identifier")
	}
	if context.CreatedAt.IsZero() {
		return controlplane.IntentMetadata{}, failed(ConversionIncomplete, GapMissingCreatedAt, "legacy action has no explicit creation time")
	}
	if context.ExpiresAt != nil && !context.ExpiresAt.After(context.CreatedAt) {
		return controlplane.IntentMetadata{}, failed(ConversionInvalid, GapInvalidExpiration, "expiration must be after creation")
	}

	return controlplane.IntentMetadata{
		ID: "pending-intent-id",
		IdempotencyKey: deriveDigest("cp-idem-v2-", producer.String(), context.StableSourceNamespace.String(),
			context.StableSourceID, resource.String(), operation.String()),
		Producer:        producer,
		Authority:       authority,
		Actor:           context.Actor,
		Reason:          context.Reason,
		EvidenceRefs:    append([]string(nil), context.EvidenceRefs...),
		PolicyVersion:   context.PolicyVersion,
		SnapshotVersion: context.SnapshotVersion,
		CreatedAt:       context.CreatedAt,
		ExpiresAt:       cloneTime(context.ExpiresAt),
	}, ConversionResult{}
}

func mapped(intent controlplane.Intent, err error) ConversionResult {
	if err != nil {
		return failed(ConversionInvalid, GapInvalidDesiredState, err.Error())
	}
	signature, err := controlplane.SemanticSignature(intent)
	if err != nil {
		return failed(ConversionInvalid, GapInvalidDesiredState, err.Error())
	}
	copy := intent
	copy.ID = deriveDigest("cp-intent-v2-", copy.IdempotencyKey, signature)
	if err := copy.Validate(); err != nil {
		return failed(ConversionInvalid, GapInvalidDesiredState, err.Error())
	}
	return ConversionResult{Intent: &copy, Status: ConversionMapped, GapCode: GapNone}
}

func failed(status ConversionStatus, gap GapCode, detail string) ConversionResult {
	return ConversionResult{Status: status, GapCode: gap, Detail: strings.TrimSpace(detail)}
}

func invalidResource(detail string) ConversionResult {
	return failed(ConversionInvalid, GapInvalidResource, detail)
}

func validAdministratorAuthorization(authorization AdministratorAuthorization) bool {
	return authorization.IdentityVerified && authorization.ExactGrant && authorization.GrantConsumed && strings.TrimSpace(authorization.GrantConsumptionID) != ""
}

func verifyAdministratorAuthorization(context LegacyContext, authorization AdministratorAuthorization) ConversionResult {
	if !validAdministratorAuthorization(authorization) {
		return failed(ConversionIncomplete, GapMissingAuthorityContext, "administrator chat command lacks verified, exact, consumed grant context")
	}
	sourceID := strings.TrimSpace(context.StableSourceID)
	if sourceID != "" && sourceID != strings.TrimSpace(authorization.GrantConsumptionID) {
		return failed(ConversionIncomplete, GapMissingAuthorityContext, "administrator grant consumption does not match the explicit stable source ID")
	}
	return ConversionResult{}
}

func validateAccountID(accountID int64) ConversionResult {
	if accountID <= 0 {
		return invalidResource("account ID must be positive")
	}
	return ConversionResult{}
}

func validateUpstreamResource(sourceID int64, keyID string) ConversionResult {
	if sourceID <= 0 || strings.TrimSpace(keyID) == "" {
		return invalidResource("upstream source ID and key ID are required")
	}
	return ConversionResult{}
}

func invalidResult(result ConversionResult) bool { return result.Status != "" }

func (r ConversionResult) Validate() error {
	if !r.Status.Valid() || !r.GapCode.Valid() {
		return fmt.Errorf("invalid conversion status or gap code")
	}
	if r.Status == ConversionMapped {
		if r.Intent == nil || r.GapCode != GapNone {
			return fmt.Errorf("mapped conversion requires an intent and no semantic gap")
		}
		return r.Intent.Validate()
	}
	if r.Intent != nil || r.GapCode == GapNone {
		return fmt.Errorf("failed conversion must contain no intent and one semantic gap")
	}
	return nil
}
