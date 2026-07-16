package controlplanebridge

import (
	"strings"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
)

func AdaptTemporaryAdministratorGroupTier(input UpstreamGroupTierInput) ConversionResult {
	return adaptGroupTier(input, controlplane.ProducerAdminUI, controlplane.AuthorityAdministratorCommand,
		contextRequirements{ttl: true}, SourceAdministratorRequest)
}

func AdaptAgentAdministratorGroupTier(input UpstreamGroupTierInput, authorization AdministratorAuthorization) ConversionResult {
	if result := verifyAdministratorAuthorization(input.Context, authorization); invalidResult(result) {
		return result
	}
	return adaptGroupTier(input, controlplane.ProducerAgentOperator, controlplane.AuthorityAdministratorCommand,
		contextRequirements{ttl: true}, SourceAdministratorGrantConsumption)
}

func AdaptAutonomousAgentGroupTier(input UpstreamGroupTierInput) ConversionResult {
	return adaptGroupTier(input, controlplane.ProducerAgentOperator, controlplane.AuthorityAutonomousAgent,
		contextRequirements{snapshot: true, evidence: true, agentTTL: true}, SourceAgentAction)
}

func AdaptFailoverTransition(input UpstreamGroupTierInput) ConversionResult {
	return adaptGroupTier(input, controlplane.ProducerFailoverController, controlplane.AuthorityEmergencyAutomation,
		contextRequirements{snapshot: true, evidence: true}, SourceFailoverTransition)
}

func AdaptLegacyDirectGroupSwitch(input LegacyDirectGroupSwitchInput) ConversionResult {
	if result := validateUpstreamResource(input.SourceID, input.KeyID); invalidResult(result) {
		return result
	}
	if strings.TrimSpace(input.GroupID) == "" {
		return failed(ConversionInvalid, GapInvalidDesiredState, "legacy direct group switch has no target group")
	}
	return failed(ConversionUnsupported, GapUnsupportedOperation,
		"legacy SwitchGroup targets an arbitrary group ID, while the control-plane model only accepts confirmed tiers")
}

func adaptGroupTier(input UpstreamGroupTierInput, producer controlplane.Producer, authority controlplane.Authority, requirements contextRequirements, sourceNamespace StableSourceNamespace) ConversionResult {
	if result := validateUpstreamResource(input.SourceID, input.KeyID); invalidResult(result) {
		return result
	}
	tier := strings.ToLower(strings.TrimSpace(input.TargetTier))
	resource, err := controlplane.NewUpstreamKeyResource(input.SourceID, input.KeyID)
	if err != nil {
		return invalidResource(err.Error())
	}
	metadata, result := prepareMetadata(input.Context, producer, authority, requirements, sourceNamespace,
		resource, controlplane.OperationSetUpstreamKeyGroupTier)
	if invalidResult(result) {
		return result
	}
	intent, err := controlplane.NewUpstreamKeyGroupTierIntent(metadata, input.SourceID, input.KeyID, tier)
	return mapped(intent, err)
}
