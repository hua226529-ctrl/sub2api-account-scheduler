package controlplanebridge

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
)

func TestIdentityIsStableForSameBusinessActionAndSemantics(t *testing.T) {
	input := AccountSchedulableInput{Context: policyContext("decision:stable"), AccountID: 123, Schedulable: false}
	first := assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	second := assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	if first.IdempotencyKey != second.IdempotencyKey || first.ID != second.ID {
		t.Fatalf("same input produced unstable identity: first=%s/%s second=%s/%s", first.IdempotencyKey, first.ID, second.IdempotencyKey, second.ID)
	}
	firstSignature := mustSemanticSignature(t, first)
	secondSignature := mustSemanticSignature(t, second)
	if firstSignature != secondSignature {
		t.Fatalf("same input produced different semantic signatures: %s != %s", firstSignature, secondSignature)
	}
}

func TestSameBusinessActionSemanticChangesConflict(t *testing.T) {
	baseInput := AccountSchedulableInput{Context: policyContext("decision:conflict"), AccountID: 123, Schedulable: false}
	base := assertMapped(t, AdaptPolicyAccountSchedulable(baseInput), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)

	tests := []struct {
		name    string
		changed func(t *testing.T) controlplane.Intent
	}{
		{name: "desired state", changed: func(t *testing.T) controlplane.Intent {
			input := baseInput
			input.Schedulable = true
			return assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
		}},
		{name: "authority", changed: func(t *testing.T) controlplane.Intent {
			return assertMapped(t, adaptAccountSchedulable(baseInput, controlplane.ProducerPolicyScheduler, controlplane.AuthorityManualHold,
				contextRequirements{}, SourcePolicyDecision), controlplane.ProducerPolicyScheduler, controlplane.AuthorityManualHold)
		}},
		{name: "actor", changed: func(t *testing.T) controlplane.Intent {
			input := baseInput
			input.Context.Actor = "scheduler:replacement"
			return assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
		}},
		{name: "reason", changed: func(t *testing.T) controlplane.Intent {
			input := baseInput
			input.Context.Reason = "replacement decision reason"
			return assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
		}},
		{name: "created at", changed: func(t *testing.T) controlplane.Intent {
			input := baseInput
			input.Context.CreatedAt = input.Context.CreatedAt.Add(time.Second)
			return assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
		}},
		{name: "expires at", changed: func(t *testing.T) controlplane.Intent {
			input := baseInput
			expiresAt := input.Context.CreatedAt.Add(time.Hour)
			input.Context.ExpiresAt = &expiresAt
			return assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
		}},
		{name: "evidence content", changed: func(t *testing.T) controlplane.Intent {
			input := baseInput
			input.Context.EvidenceRefs = []string{"decision:evidence:replacement"}
			return assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
		}},
		{name: "policy version", changed: func(t *testing.T) controlplane.Intent {
			input := baseInput
			input.Context.PolicyVersion = "policy:v18"
			return assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
		}},
		{name: "snapshot version", changed: func(t *testing.T) controlplane.Intent {
			input := baseInput
			input.Context.SnapshotVersion = "snapshot:v42"
			return assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := test.changed(t)
			assertIdentityConflict(t, base, changed)
		})
	}
}

func TestEvidenceRefsHaveSetSemanticsWithoutMutatingInput(t *testing.T) {
	firstInput := AccountSchedulableInput{Context: policyContext("decision:evidence-set"), AccountID: 123, Schedulable: false}
	firstInput.Context.EvidenceRefs = []string{" evidence:b ", "evidence:a", "evidence:a"}
	before := append([]string(nil), firstInput.Context.EvidenceRefs...)
	secondInput := firstInput
	secondInput.Context.EvidenceRefs = []string{"evidence:a", "evidence:b"}

	first := assertMapped(t, AdaptPolicyAccountSchedulable(firstInput), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	second := assertMapped(t, AdaptPolicyAccountSchedulable(secondInput), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	if first.IdempotencyKey != second.IdempotencyKey || first.ID != second.ID {
		t.Fatalf("evidence order or duplicates changed identity: first=%s/%s second=%s/%s", first.IdempotencyKey, first.ID, second.IdempotencyKey, second.ID)
	}
	if mustSemanticSignature(t, first) != mustSemanticSignature(t, second) {
		t.Fatal("evidence order or duplicates changed semantic signature")
	}
	if !reflect.DeepEqual(firstInput.Context.EvidenceRefs, before) {
		t.Fatalf("adapter modified caller evidence: got %v want %v", firstInput.Context.EvidenceRefs, before)
	}
	if !reflect.DeepEqual(first.EvidenceRefs, []string{"evidence:a", "evidence:b"}) {
		t.Fatalf("intent evidence was not canonicalized: %v", first.EvidenceRefs)
	}
	result := onlyBridgeResult(t, controlplane.Arbitrate(fixedAdapterTime().Add(time.Second), []controlplane.Intent{first, second}))
	if result.Winner == nil || len(result.Ignored) != 1 || result.Ignored[0].ReasonCode != controlplane.ReasonDuplicate {
		t.Fatalf("equivalent evidence sets were not deduplicated: %+v", result)
	}
}

func TestDifferentBusinessActionsWithSamePayloadRemainDistinct(t *testing.T) {
	firstInput := AccountSchedulableInput{Context: policyContext("decision:first"), AccountID: 123, Schedulable: false}
	secondInput := firstInput
	secondInput.Context.StableSourceID = "decision:second"
	first := assertMapped(t, AdaptPolicyAccountSchedulable(firstInput), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	second := assertMapped(t, AdaptPolicyAccountSchedulable(secondInput), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	if first.IdempotencyKey == second.IdempotencyKey || first.ID == second.ID {
		t.Fatal("different stable source IDs produced the same identity")
	}
	result := onlyBridgeResult(t, controlplane.Arbitrate(fixedAdapterTime().Add(time.Second), []controlplane.Intent{first, second}))
	if result.Winner == nil || len(result.Ignored) != 0 || len(result.Superseded) != 1 {
		t.Fatalf("different business actions were treated as duplicate or conflict: %+v", result)
	}
}

func TestStableSourceNamespaceAndProducerPreventCollisions(t *testing.T) {
	policyInput := AccountSchedulableInput{Context: policyContext("raw:123"), AccountID: 123, Schedulable: false}
	policy := assertMapped(t, AdaptPolicyAccountSchedulable(policyInput), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)

	agentInput := policyInput
	agentInput.Context.StableSourceNamespace = SourceAgentAction
	namespaced := assertMapped(t, adaptAccountSchedulable(agentInput, controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy,
		contextRequirements{policy: true, snapshot: true}, SourceAgentAction), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	if policy.IdempotencyKey == namespaced.IdempotencyKey || policy.ID == namespaced.ID {
		t.Fatal("same raw source ID collided across stable source namespaces")
	}

	produced := assertMapped(t, adaptAccountSchedulable(policyInput, controlplane.ProducerAgentOperator, controlplane.AuthorityActivePolicy,
		contextRequirements{policy: true, snapshot: true}, SourcePolicyDecision), controlplane.ProducerAgentOperator, controlplane.AuthorityActivePolicy)
	if policy.IdempotencyKey == produced.IdempotencyKey || policy.ID == produced.ID {
		t.Fatal("same raw source ID collided across producers")
	}
}

func TestMissingOrWrongStableSourceCannotMap(t *testing.T) {
	tests := []LegacyContext{policyContext(""), policyContext("decision:wrong-namespace")}
	tests[1].StableSourceNamespace = SourceAgentAction
	for index, context := range tests {
		result := AdaptPolicyAccountSchedulable(AccountSchedulableInput{Context: context, AccountID: 123, Schedulable: false})
		if result.Status != ConversionIncomplete || result.GapCode != GapMissingIdempotencySource || result.Intent != nil {
			t.Fatalf("case %d = %+v, want missing idempotency source and nil intent", index, result)
		}
	}
}

func TestIdentityDigestsDoNotExposeSensitiveSemanticValues(t *testing.T) {
	const (
		sourceSecret   = "token:source-secret"
		reasonSecret   = "administrator-key:reason-secret"
		evidenceSecret = "model-key:evidence-secret"
		resourceSecret = "upstream-token:resource-secret"
	)
	context := contextWithoutTTL(sourceSecret)
	context.StableSourceNamespace = SourceFailoverTransition
	context.Actor = "system:failover"
	context.Reason = reasonSecret
	context.SnapshotVersion = "snapshot:sensitive"
	context.EvidenceRefs = []string{evidenceSecret}
	intent := assertMapped(t, AdaptFailoverTransition(UpstreamGroupTierInput{
		Context: context, SourceID: 7, KeyID: resourceSecret, TargetTier: "backup",
	}), controlplane.ProducerFailoverController, controlplane.AuthorityEmergencyAutomation)
	signature := mustSemanticSignature(t, intent)
	for _, digest := range []string{intent.IdempotencyKey, intent.ID, signature} {
		for _, secret := range []string{sourceSecret, reasonSecret, evidenceSecret, resourceSecret} {
			if strings.Contains(digest, secret) {
				t.Fatalf("digest %q exposes sensitive value %q", digest, secret)
			}
		}
	}
}

func TestAdministratorGrantSourceMustBeExplicitAndMatchAuthorization(t *testing.T) {
	authorization := validAuthorization()
	missing := contextWithTTL(authorization.GrantConsumptionID)
	assertGap(t, AdaptAgentAdministratorAccountSchedulable(AccountSchedulableInput{
		Context: missing, AccountID: 123, Schedulable: false,
	}, authorization), ConversionIncomplete, GapMissingIdempotencySource)

	mismatch := administratorGrantContext("grant-consumption:other")
	assertGap(t, AdaptAgentAdministratorAccountSchedulable(AccountSchedulableInput{
		Context: mismatch, AccountID: 123, Schedulable: false,
	}, authorization), ConversionIncomplete, GapMissingAuthorityContext)
}

func assertIdentityConflict(t *testing.T, first, second controlplane.Intent) {
	t.Helper()
	if first.IdempotencyKey != second.IdempotencyKey {
		t.Fatalf("semantic change escaped conflict with a new idempotency key: %s != %s", first.IdempotencyKey, second.IdempotencyKey)
	}
	if first.ID == second.ID {
		t.Fatalf("semantic change reused intent ID %s", first.ID)
	}
	if mustSemanticSignature(t, first) == mustSemanticSignature(t, second) {
		t.Fatal("semantic change reused semantic signature")
	}
	result := onlyBridgeResult(t, controlplane.Arbitrate(fixedAdapterTime().Add(2*time.Second), []controlplane.Intent{first, second}))
	if result.Winner != nil || len(result.Ignored) != 2 {
		t.Fatalf("semantic conflict was silently selected: %+v", result)
	}
	for _, ignored := range result.Ignored {
		if ignored.ReasonCode != controlplane.ReasonIdempotencyConflict {
			t.Fatalf("semantic conflict reason = %+v", result.Ignored)
		}
	}
}

func mustSemanticSignature(t *testing.T, intent controlplane.Intent) string {
	t.Helper()
	signature, err := controlplane.SemanticSignature(intent)
	if err != nil {
		t.Fatal(err)
	}
	return signature
}

func onlyBridgeResult(t *testing.T, results []controlplane.ArbitrationResult) controlplane.ArbitrationResult {
	t.Helper()
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1: %+v", len(results), results)
	}
	return results[0]
}
