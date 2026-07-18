package controlplanebridge

import "github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"

func AdaptPolicyAccountSchedulable(input AccountSchedulableInput) ConversionResult {
	return adaptAccountSchedulable(input, controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy,
		contextRequirements{policy: true, snapshot: true}, SourcePolicyDecision)
}

func AdaptPolicyAccountLoadFactor(input AccountLoadFactorInput) ConversionResult {
	return adaptAccountLoadFactor(input, controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy,
		contextRequirements{policy: true, snapshot: true}, SourcePolicyDecision)
}

func AdaptPermanentManualPause(input AccountActionInput) ConversionResult {
	if result := validateAccountID(input.AccountID); invalidResult(result) {
		return result
	}
	if input.Context.ExpiresAt != nil {
		return failed(ConversionInvalid, GapInvalidExpiration, "permanent ManualHold must not be disguised as a temporary command")
	}
	return adaptAccountSchedulable(AccountSchedulableInput{Context: input.Context, AccountID: input.AccountID, Schedulable: false},
		controlplane.ProducerAdminUI, controlplane.AuthorityManualHold, contextRequirements{}, SourceAdministratorRequest)
}

func AdaptTemporaryAdministratorAccountSchedulable(input AccountSchedulableInput) ConversionResult {
	return adaptAccountSchedulable(input, controlplane.ProducerAdminUI, controlplane.AuthorityAdministratorCommand,
		contextRequirements{ttl: true}, SourceAdministratorRequest)
}

func AdaptTemporaryAdministratorLoadFactor(input AccountLoadFactorInput) ConversionResult {
	return adaptAccountLoadFactor(input, controlplane.ProducerAdminUI, controlplane.AuthorityAdministratorCommand,
		contextRequirements{ttl: true}, SourceAdministratorRequest)
}

func AdaptAgentAdministratorAccountSchedulable(input AccountSchedulableInput, authorization AdministratorAuthorization) ConversionResult {
	if result := verifyAdministratorAuthorization(input.Context, authorization); invalidResult(result) {
		return result
	}
	return adaptAccountSchedulable(input, controlplane.ProducerAgentOperator, controlplane.AuthorityAdministratorCommand,
		contextRequirements{ttl: true}, SourceAdministratorGrantConsumption)
}

func AdaptAgentAdministratorLoadFactor(input AccountLoadFactorInput, authorization AdministratorAuthorization) ConversionResult {
	if result := verifyAdministratorAuthorization(input.Context, authorization); invalidResult(result) {
		return result
	}
	return adaptAccountLoadFactor(input, controlplane.ProducerAgentOperator, controlplane.AuthorityAdministratorCommand,
		contextRequirements{ttl: true}, SourceAdministratorGrantConsumption)
}

func AdaptAutonomousAgentAccountSchedulable(input AccountSchedulableInput) ConversionResult {
	return adaptAccountSchedulable(input, controlplane.ProducerAgentOperator, controlplane.AuthorityAutonomousAgent,
		contextRequirements{snapshot: true, evidence: true, agentTTL: true}, SourceAgentAction)
}

func AdaptAutonomousAgentLoadFactor(input AccountLoadFactorInput) ConversionResult {
	return adaptAccountLoadFactor(input, controlplane.ProducerAgentOperator, controlplane.AuthorityAutonomousAgent,
		contextRequirements{snapshot: true, evidence: true, agentTTL: true}, SourceAgentAction)
}

func AdaptCostOptimizationSchedulable(input AccountSchedulableInput) ConversionResult {
	return adaptAccountSchedulable(input, controlplane.ProducerCostOptimizer, controlplane.AuthorityOptimization,
		contextRequirements{ttl: true, snapshot: true}, SourceOptimizationAction)
}

func AdaptCostOptimizationLoadFactor(input AccountLoadFactorInput) ConversionResult {
	return adaptAccountLoadFactor(input, controlplane.ProducerCostOptimizer, controlplane.AuthorityOptimization,
		contextRequirements{ttl: true, snapshot: true}, SourceOptimizationAction)
}

func AdaptManualResume(input AccountActionInput) ConversionResult {
	if result := validateAccountID(input.AccountID); invalidResult(result) {
		return result
	}
	return failed(ConversionIncomplete, GapAmbiguousManualResume,
		"legacy ManualResume both writes schedulable=true and changes ownership/protection state")
}

func AdaptManualHoldRelease(input AccountActionInput) ConversionResult {
	if result := validateAccountID(input.AccountID); invalidResult(result) {
		return result
	}
	return failed(ConversionUnsupported, GapUnsupportedOperation,
		"releasing ManualHold is override revocation, not SetAccountSchedulable(true)")
}

func adaptAccountSchedulable(input AccountSchedulableInput, producer controlplane.Producer, authority controlplane.Authority, requirements contextRequirements, sourceNamespace StableSourceNamespace) ConversionResult {
	if result := validateAccountID(input.AccountID); invalidResult(result) {
		return result
	}
	resource, err := controlplane.NewAccountResource(input.AccountID)
	if err != nil {
		return invalidResource(err.Error())
	}
	metadata, result := prepareMetadata(input.Context, producer, authority, requirements, sourceNamespace,
		resource, controlplane.OperationSetAccountSchedulable)
	if invalidResult(result) {
		return result
	}
	intent, err := controlplane.NewAccountSchedulableIntent(metadata, input.AccountID, input.Schedulable)
	return mapped(intent, err)
}

func adaptAccountLoadFactor(input AccountLoadFactorInput, producer controlplane.Producer, authority controlplane.Authority, requirements contextRequirements, sourceNamespace StableSourceNamespace) ConversionResult {
	if result := validateAccountID(input.AccountID); invalidResult(result) {
		return result
	}
	if input.LoadFactor != nil {
		if *input.LoadFactor < 1 {
			return failed(ConversionInvalid, GapInvalidDesiredState, "load factor must be nil or positive")
		}
	}
	resource, err := controlplane.NewAccountResource(input.AccountID)
	if err != nil {
		return invalidResource(err.Error())
	}
	metadata, result := prepareMetadata(input.Context, producer, authority, requirements, sourceNamespace,
		resource, controlplane.OperationSetAccountLoadFactor)
	if invalidResult(result) {
		return result
	}
	intent, err := controlplane.NewAccountLoadFactorIntent(metadata, input.AccountID, input.LoadFactor)
	return mapped(intent, err)
}
