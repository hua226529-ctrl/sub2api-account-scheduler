package controlplanebridge

import (
	"bytes"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestPolicyAccountDecisionsMapToActivePolicy(t *testing.T) {
	pause := assertMapped(t, AdaptPolicyAccountSchedulable(AccountSchedulableInput{
		Context: policyContext("policy-pause"), AccountID: 123, Schedulable: false,
	}), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	if desired, ok := pause.DesiredState.Schedulable(); !ok || desired {
		t.Fatalf("pause desired state = %v, %v", desired, ok)
	}

	resume := assertMapped(t, AdaptPolicyAccountSchedulable(AccountSchedulableInput{
		Context: policyContext("policy-resume"), AccountID: 123, Schedulable: true,
	}), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	if desired, ok := resume.DesiredState.Schedulable(); !ok || !desired {
		t.Fatalf("resume desired state = %v, %v", desired, ok)
	}

	loadFactor := 25
	load := assertMapped(t, AdaptPolicyAccountLoadFactor(AccountLoadFactorInput{
		Context: policyContext("policy-load"), AccountID: 123, LoadFactor: &loadFactor,
	}), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	if value, configured, ok := load.DesiredState.LoadFactor(); !ok || !configured || value != loadFactor {
		t.Fatalf("load desired state = %d, %v, %v", value, configured, ok)
	}
}

func TestPolicyConversionReportsMissingVersions(t *testing.T) {
	missingPolicy := policyContext("missing-policy")
	missingPolicy.PolicyVersion = ""
	assertGap(t, AdaptPolicyAccountSchedulable(AccountSchedulableInput{Context: missingPolicy, AccountID: 123}), ConversionIncomplete, GapMissingPolicyVersion)

	missingSnapshot := policyContext("missing-snapshot")
	missingSnapshot.SnapshotVersion = ""
	assertGap(t, AdaptPolicyAccountSchedulable(AccountSchedulableInput{Context: missingSnapshot, AccountID: 123}), ConversionIncomplete, GapMissingSnapshotVersion)
}

func TestManualAndTemporaryAdministratorMappings(t *testing.T) {
	manual := contextWithoutTTL("manual-pause")
	hold := assertMapped(t, AdaptPermanentManualPause(AccountActionInput{Context: manual, AccountID: 123}),
		controlplane.ProducerAdminUI, controlplane.AuthorityManualHold)
	if hold.ExpiresAt != nil {
		t.Fatal("permanent ManualHold unexpectedly has an expiration")
	}

	temporary := contextWithTTL("admin-temporary")
	command := assertMapped(t, AdaptTemporaryAdministratorAccountSchedulable(AccountSchedulableInput{
		Context: temporary, AccountID: 123, Schedulable: false,
	}), controlplane.ProducerAdminUI, controlplane.AuthorityAdministratorCommand)
	if command.ExpiresAt == nil {
		t.Fatal("temporary administrator command has no expiration")
	}

	assertGap(t, AdaptTemporaryAdministratorAccountSchedulable(AccountSchedulableInput{
		Context: contextWithoutTTL("admin-no-ttl"), AccountID: 123, Schedulable: false,
	}), ConversionIncomplete, GapMissingTTL)
}

func TestAdministratorChatKeepsAgentProducerAndAdministratorAuthority(t *testing.T) {
	authorization := validAuthorization()
	intent := assertMapped(t, AdaptAgentAdministratorAccountSchedulable(AccountSchedulableInput{
		Context: administratorGrantContext(authorization.GrantConsumptionID), AccountID: 123, Schedulable: false,
	}, authorization), controlplane.ProducerAgentOperator, controlplane.AuthorityAdministratorCommand)
	if intent.Producer != controlplane.ProducerAgentOperator || intent.Authority != controlplane.AuthorityAdministratorCommand {
		t.Fatalf("producer/authority = %s/%s", intent.Producer, intent.Authority)
	}

	authorization.GrantConsumed = false
	assertGap(t, AdaptAgentAdministratorAccountSchedulable(AccountSchedulableInput{
		Context: administratorGrantContext(authorization.GrantConsumptionID), AccountID: 123, Schedulable: false,
	}, authorization), ConversionIncomplete, GapMissingAuthorityContext)
}

func TestAdministratorGrantConsumptionParticipatesInStableIdentity(t *testing.T) {
	firstAuthorization := validAuthorization()
	firstInput := AccountSchedulableInput{Context: administratorGrantContext(firstAuthorization.GrantConsumptionID), AccountID: 123, Schedulable: false}
	first := assertMapped(t, AdaptAgentAdministratorAccountSchedulable(firstInput, firstAuthorization),
		controlplane.ProducerAgentOperator, controlplane.AuthorityAdministratorCommand)
	secondAuthorization := firstAuthorization
	secondAuthorization.GrantConsumptionID = "grant-consumption:92"
	secondInput := firstInput
	secondInput.Context.StableSourceID = secondAuthorization.GrantConsumptionID
	second := assertMapped(t, AdaptAgentAdministratorAccountSchedulable(secondInput, secondAuthorization),
		controlplane.ProducerAgentOperator, controlplane.AuthorityAdministratorCommand)
	if first.IdempotencyKey == second.IdempotencyKey {
		t.Fatal("different administrator grant consumption reused idempotency key")
	}
	if strings.Contains(first.IdempotencyKey, firstAuthorization.GrantConsumptionID) {
		t.Fatal("administrator grant consumption ID leaked into plaintext idempotency key")
	}
}

func TestAutonomousAgentRequiresTTLAndEvidence(t *testing.T) {
	context := contextWithTTL("agent-temporary")
	context.StableSourceNamespace = SourceAgentAction
	context.SnapshotVersion = "packet:44"
	context.EvidenceRefs = []string{"packet:44", "monitor:9"}
	assertMapped(t, AdaptAutonomousAgentAccountSchedulable(AccountSchedulableInput{
		Context: context, AccountID: 123, Schedulable: false,
	}), controlplane.ProducerAgentOperator, controlplane.AuthorityAutonomousAgent)

	permanent := context
	permanent.StableSourceID = "agent-permanent"
	permanent.ExpiresAt = nil
	assertGap(t, AdaptAutonomousAgentAccountSchedulable(AccountSchedulableInput{
		Context: permanent, AccountID: 123, Schedulable: false,
	}), ConversionIncomplete, GapLegacyPermanentAgentControl)

	withoutEvidence := context
	withoutEvidence.StableSourceID = "agent-no-evidence"
	withoutEvidence.EvidenceRefs = nil
	assertGap(t, AdaptAutonomousAgentAccountSchedulable(AccountSchedulableInput{
		Context: withoutEvidence, AccountID: 123, Schedulable: false,
	}), ConversionIncomplete, GapMissingEvidence)
}

func TestManualResumeAndHoldReleaseRemainDistinct(t *testing.T) {
	input := AccountActionInput{Context: contextWithoutTTL("manual-resume"), AccountID: 123}
	assertGap(t, AdaptManualResume(input), ConversionIncomplete, GapAmbiguousManualResume)
	assertGap(t, AdaptManualHoldRelease(input), ConversionUnsupported, GapUnsupportedOperation)
}

func TestFailoverTransitionMapsWithStableResourceAndEvidence(t *testing.T) {
	context := contextWithoutTTL("transition:991")
	context.StableSourceNamespace = SourceFailoverTransition
	context.Actor = "system:failover"
	context.SnapshotVersion = "assessment:2026-07-17T00:00:00Z"
	context.EvidenceRefs = []string{"traffic-window:991", "monitor-streak:8"}
	intent := assertMapped(t, AdaptFailoverTransition(UpstreamGroupTierInput{
		Context: context, SourceID: 7, KeyID: "key-42", TargetTier: model.GroupTierEmergency,
	}), controlplane.ProducerFailoverController, controlplane.AuthorityEmergencyAutomation)
	sourceID, keyID, ok := intent.Resource.UpstreamKey()
	if !ok || sourceID != 7 || keyID != "key-42" {
		t.Fatalf("upstream resource = %d/%q/%v", sourceID, keyID, ok)
	}

	withoutEvidence := context
	withoutEvidence.StableSourceID = "transition:no-evidence"
	withoutEvidence.EvidenceRefs = nil
	assertGap(t, AdaptFailoverTransition(UpstreamGroupTierInput{
		Context: withoutEvidence, SourceID: 7, KeyID: "key-42", TargetTier: model.GroupTierBackup,
	}), ConversionIncomplete, GapMissingEvidence)
}

func TestCostOptimizationRequiresFiniteTTL(t *testing.T) {
	context := contextWithTTL("cost:pool-a:123")
	context.StableSourceNamespace = SourceOptimizationAction
	context.SnapshotVersion = "cost-snapshot:77"
	assertMapped(t, AdaptCostOptimizationSchedulable(AccountSchedulableInput{
		Context: context, AccountID: 123, Schedulable: false,
	}), controlplane.ProducerCostOptimizer, controlplane.AuthorityOptimization)

	context.StableSourceID = "cost:permanent"
	context.ExpiresAt = nil
	assertGap(t, AdaptCostOptimizationSchedulable(AccountSchedulableInput{
		Context: context, AccountID: 123, Schedulable: false,
	}), ConversionIncomplete, GapMissingTTL)
}

func TestStableIdentitySeparatesBusinessActionFromSemanticPayload(t *testing.T) {
	input := AccountSchedulableInput{Context: policyContext("stable-policy"), AccountID: 123, Schedulable: false}
	first := assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	second := assertMapped(t, AdaptPolicyAccountSchedulable(input), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	if first.ID != second.ID || first.IdempotencyKey != second.IdempotencyKey {
		t.Fatal("same conversion input generated different stable identity")
	}

	differentDesired := input
	differentDesired.Schedulable = true
	desired := assertMapped(t, AdaptPolicyAccountSchedulable(differentDesired), controlplane.ProducerPolicyScheduler, controlplane.AuthorityActivePolicy)
	if desired.IdempotencyKey != first.IdempotencyKey || desired.ID == first.ID {
		t.Fatal("payload change did not preserve business-action identity and change content identity")
	}

	temporary := AccountSchedulableInput{Context: contextWithTTL("admin-expiration"), AccountID: 123, Schedulable: false}
	before := assertMapped(t, AdaptTemporaryAdministratorAccountSchedulable(temporary), controlplane.ProducerAdminUI, controlplane.AuthorityAdministratorCommand)
	laterExpiration := temporary
	expiresAt := laterExpiration.Context.ExpiresAt.Add(time.Minute)
	laterExpiration.Context.ExpiresAt = &expiresAt
	after := assertMapped(t, AdaptTemporaryAdministratorAccountSchedulable(laterExpiration), controlplane.ProducerAdminUI, controlplane.AuthorityAdministratorCommand)
	if before.IdempotencyKey != after.IdempotencyKey || before.ID == after.ID {
		t.Fatal("expiration change did not preserve business-action identity and change content identity")
	}
}

func TestStableIdentityDoesNotExposeUpstreamKey(t *testing.T) {
	const sensitiveKey = "real-token-material-must-not-leak"
	context := contextWithoutTTL("transition:secret-test")
	context.StableSourceNamespace = SourceFailoverTransition
	context.Actor = "system:failover"
	context.SnapshotVersion = "assessment:secret-test"
	context.EvidenceRefs = []string{"evidence:secret-test"}
	intent := assertMapped(t, AdaptFailoverTransition(UpstreamGroupTierInput{
		Context: context, SourceID: 7, KeyID: sensitiveKey, TargetTier: model.GroupTierBackup,
	}), controlplane.ProducerFailoverController, controlplane.AuthorityEmergencyAutomation)
	if strings.Contains(intent.ID, sensitiveKey) || strings.Contains(intent.IdempotencyKey, sensitiveKey) {
		t.Fatal("stable identity contains plaintext upstream key material")
	}
}

func TestAdaptersDoNotModifyInput(t *testing.T) {
	context := contextWithTTL("immutable")
	context.StableSourceNamespace = SourceAgentAction
	context.SnapshotVersion = "packet:immutable"
	context.EvidenceRefs = []string{" evidence:1 ", "evidence:2"}
	loadFactor := 25
	input := AccountLoadFactorInput{Context: context, AccountID: 123, LoadFactor: &loadFactor}
	before := AccountLoadFactorInput{Context: context, AccountID: input.AccountID, LoadFactor: &loadFactor}
	before.Context.EvidenceRefs = append([]string(nil), context.EvidenceRefs...)
	before.Context.ExpiresAt = cloneTime(context.ExpiresAt)

	_ = AdaptAutonomousAgentLoadFactor(input)
	if !reflect.DeepEqual(input, before) {
		t.Fatalf("input was modified:\ngot  %+v\nwant %+v", input, before)
	}
}

func TestFailedConversionsNeverReturnPartialIntent(t *testing.T) {
	invalidLoad := 101
	autonomous := contextWithTTL("failure-agent")
	autonomous.StableSourceNamespace = SourceAgentAction
	autonomous.SnapshotVersion = "packet:failure"
	tests := []ConversionResult{
		AdaptPolicyAccountSchedulable(AccountSchedulableInput{Context: contextWithoutTTL("failure-policy"), AccountID: 123}),
		AdaptTemporaryAdministratorAccountSchedulable(AccountSchedulableInput{Context: contextWithoutTTL("failure-admin"), AccountID: 123}),
		AdaptAgentAdministratorAccountSchedulable(AccountSchedulableInput{Context: administratorGrantContext("failure-chat"), AccountID: 123}, AdministratorAuthorization{}),
		AdaptAutonomousAgentAccountSchedulable(AccountSchedulableInput{Context: autonomous, AccountID: 123}),
		AdaptCostOptimizationSchedulable(AccountSchedulableInput{Context: contextWithoutTTL("failure-cost"), AccountID: 123}),
		AdaptPolicyAccountLoadFactor(AccountLoadFactorInput{Context: policyContext("failure-load"), AccountID: 123, LoadFactor: &invalidLoad}),
		AdaptLegacyDirectGroupSwitch(LegacyDirectGroupSwitchInput{SourceID: 7, KeyID: "key-42", GroupID: "arbitrary"}),
		AdaptManualResume(AccountActionInput{AccountID: 123}),
	}
	for index, result := range tests {
		if result.Status == ConversionMapped || result.Intent != nil || result.GapCode == GapNone {
			t.Fatalf("failure %d returned partial intent: %+v", index, result)
		}
		if err := result.Validate(); err != nil {
			t.Fatalf("failure %d is not structurally valid: %v", index, err)
		}
	}
}

func TestMissingStableSourceIsNotInvented(t *testing.T) {
	context := policyContext("missing-source")
	context.StableSourceID = ""
	assertGap(t, AdaptPolicyAccountSchedulable(AccountSchedulableInput{
		Context: context, AccountID: 123, Schedulable: false,
	}), ConversionIncomplete, GapMissingIdempotencySource)
}

func TestGroupAndLoadDesiredStateValidation(t *testing.T) {
	invalidLoad := 0
	assertGap(t, AdaptPolicyAccountLoadFactor(AccountLoadFactorInput{
		Context: policyContext("invalid-load"), AccountID: 123, LoadFactor: &invalidLoad,
	}), ConversionInvalid, GapInvalidDesiredState)

	context := contextWithoutTTL("invalid-tier")
	context.StableSourceNamespace = SourceFailoverTransition
	context.Actor = "system:failover"
	context.SnapshotVersion = "assessment:invalid-tier"
	context.EvidenceRefs = []string{"evidence:invalid-tier"}
	assertGap(t, AdaptFailoverTransition(UpstreamGroupTierInput{
		Context: context, SourceID: 7, KeyID: "key-42", TargetTier: "cheapest",
	}), ConversionInvalid, GapInvalidDesiredState)
}

func TestLegacyDirectGroupSwitchIsUnsupported(t *testing.T) {
	assertGap(t, AdaptLegacyDirectGroupSwitch(LegacyDirectGroupSwitchInput{
		Context: contextWithoutTTL("direct-group"), SourceID: 7, KeyID: "key-42", GroupID: "17",
	}), ConversionUnsupported, GapUnsupportedOperation)
}

func TestAdapterSourceDoesNotReadCurrentTime(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		contents, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(contents, []byte("time.Now(")) {
			t.Fatalf("adapter production source %s reads the system clock", entry.Name())
		}
	}
}

func TestConversionEnumsHandleUnknownValuesSafely(t *testing.T) {
	if got := ConversionStatus("future").String(); !strings.Contains(got, "future") {
		t.Fatalf("status string = %q", got)
	}
	if got := GapCode("future").String(); !strings.Contains(got, "future") {
		t.Fatalf("gap string = %q", got)
	}
}

func assertMapped(t *testing.T, result ConversionResult, producer controlplane.Producer, authority controlplane.Authority) controlplane.Intent {
	t.Helper()
	if err := result.Validate(); err != nil {
		t.Fatalf("mapped result validation failed: %v; result=%+v", err, result)
	}
	if result.Status != ConversionMapped || result.Intent == nil || result.Intent.Producer != producer || result.Intent.Authority != authority {
		t.Fatalf("mapped result = %+v, want %s/%s", result, producer, authority)
	}
	return *result.Intent
}

func assertGap(t *testing.T, result ConversionResult, status ConversionStatus, gap GapCode) {
	t.Helper()
	if result.Status != status || result.GapCode != gap || result.Intent != nil {
		t.Fatalf("result = %+v, want status=%s gap=%s and nil intent", result, status, gap)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("failed result validation failed: %v", err)
	}
}

func policyContext(stableSource string) LegacyContext {
	context := contextWithoutTTL(stableSource)
	context.StableSourceNamespace = SourcePolicyDecision
	context.Actor = "scheduler"
	context.PolicyVersion = "policy:v17"
	context.SnapshotVersion = "snapshot:v41"
	return context
}

func contextWithoutTTL(stableSource string) LegacyContext {
	return LegacyContext{
		StableSourceNamespace: SourceAdministratorRequest,
		StableSourceID:        stableSource,
		Actor:                 "administrator:web",
		Reason:                "fixed test decision",
		CreatedAt:             fixedAdapterTime(),
	}
}

func contextWithTTL(stableSource string) LegacyContext {
	context := contextWithoutTTL(stableSource)
	expiresAt := context.CreatedAt.Add(30 * time.Minute)
	context.ExpiresAt = &expiresAt
	return context
}

func administratorGrantContext(stableSource string) LegacyContext {
	context := contextWithTTL(stableSource)
	context.StableSourceNamespace = SourceAdministratorGrantConsumption
	return context
}

func validAuthorization() AdministratorAuthorization {
	return AdministratorAuthorization{
		IdentityVerified: true, ExactGrant: true, GrantConsumed: true, GrantConsumptionID: "grant-consumption:91",
	}
}

func fixedAdapterTime() time.Time {
	return time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
}

func TestGapCodesAreStableAndUnique(t *testing.T) {
	values := []GapCode{
		GapMissingTTL, GapMissingActor, GapMissingAuthorityContext, GapMissingSnapshotVersion,
		GapMissingPolicyVersion, GapMissingEvidence, GapAmbiguousManualResume,
		GapLegacyPermanentAgentControl, GapUnsupportedOperation, GapInvalidDesiredState,
		GapMissingIdempotencySource, GapMissingReason, GapMissingCreatedAt,
		GapInvalidExpiration, GapInvalidResource,
	}
	sort.Slice(values, func(left, right int) bool { return values[left] < values[right] })
	for index := 1; index < len(values); index++ {
		if values[index] == values[index-1] {
			t.Fatalf("duplicate gap code %q", values[index])
		}
	}
}
