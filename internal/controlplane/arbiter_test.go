package controlplane

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestArbiterAuthorityOrder(t *testing.T) {
	tests := []struct {
		name   string
		higher Authority
		lower  Authority
	}{
		{name: "administrator beats autonomous agent", higher: AuthorityAdministratorCommand, lower: AuthorityAutonomousAgent},
		{name: "manual hold beats administrator", higher: AuthorityManualHold, lower: AuthorityAdministratorCommand},
		{name: "emergency beats autonomous agent", higher: AuthorityEmergencyAutomation, lower: AuthorityAutonomousAgent},
		{name: "autonomous agent beats active policy", higher: AuthorityAutonomousAgent, lower: AuthorityActivePolicy},
		{name: "active policy beats optimization", higher: AuthorityActivePolicy, lower: AuthorityOptimization},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			higher := mustSchedulableIntent(t, test.higher, "higher", 123, false, fixedNow)
			lower := mustSchedulableIntent(t, test.lower, "lower", 123, true, fixedNow.Add(time.Second))
			result := onlyResult(t, Arbitrate(fixedNow.Add(2*time.Second), []Intent{lower, higher}))
			if result.Winner == nil || result.Winner.ID != higher.ID {
				t.Fatalf("winner = %+v, want %s", result.Winner, higher.ID)
			}
			if len(result.Superseded) != 1 || result.Superseded[0].ReasonCode != ReasonLowerAuthority {
				t.Fatalf("superseded = %+v", result.Superseded)
			}
		})
	}
}

func TestAgentProducedAdministratorChatCommandBeatsAutonomousAction(t *testing.T) {
	adminMetadata := metadataFor(AuthorityAdministratorCommand, "chat-command", fixedNow)
	adminMetadata.Producer = ProducerAgentOperator
	admin, err := NewAccountSchedulableIntent(adminMetadata, 123, false)
	if err != nil {
		t.Fatal(err)
	}
	autonomous := mustSchedulableIntent(t, AuthorityAutonomousAgent, "autonomous", 123, true, fixedNow.Add(time.Second))
	result := onlyResult(t, Arbitrate(fixedNow.Add(2*time.Second), []Intent{autonomous, admin}))
	if result.Winner == nil || result.Winner.ID != admin.ID || result.Winner.Producer != ProducerAgentOperator || result.Winner.Authority != AuthorityAdministratorCommand {
		t.Fatalf("winner did not preserve producer/authority separation: %+v", result.Winner)
	}
}

func TestExpiredAdministratorOverrideLetsPolicyWinAgain(t *testing.T) {
	createdAt := fixedNow.Add(-time.Hour)
	metadata := metadataFor(AuthorityAdministratorCommand, "admin-expired", createdAt)
	expiresAt := fixedNow
	metadata.ExpiresAt = &expiresAt
	admin, err := NewAccountSchedulableIntent(metadata, 123, false)
	if err != nil {
		t.Fatal(err)
	}
	policy := mustSchedulableIntent(t, AuthorityActivePolicy, "policy", 123, true, fixedNow.Add(-time.Minute))
	result := onlyResult(t, Arbitrate(fixedNow, []Intent{admin, policy}))
	if result.Winner == nil || result.Winner.ID != policy.ID {
		t.Fatalf("winner = %+v, want active policy", result.Winner)
	}
	if len(result.Ignored) != 1 || result.Ignored[0].ReasonCode != ReasonExpired {
		t.Fatalf("ignored = %+v, want expired override", result.Ignored)
	}
}

func TestArbiterSameAuthorityUsesNewerCreationTime(t *testing.T) {
	older := mustSchedulableIntent(t, AuthorityActivePolicy, "older", 123, false, fixedNow)
	newer := mustSchedulableIntent(t, AuthorityActivePolicy, "newer", 123, true, fixedNow.Add(time.Second))
	result := onlyResult(t, Arbitrate(fixedNow.Add(2*time.Second), []Intent{older, newer}))
	if result.Winner == nil || result.Winner.ID != newer.ID || result.Superseded[0].ReasonCode != ReasonOlderSameAuthority {
		t.Fatalf("result = %+v", result)
	}
}

func TestArbiterSameTimeUsesLexicographicallySmallerID(t *testing.T) {
	alpha := mustSchedulableIntent(t, AuthorityActivePolicy, "alpha", 123, false, fixedNow)
	zulu := mustSchedulableIntent(t, AuthorityActivePolicy, "zulu", 123, true, fixedNow)
	result := onlyResult(t, Arbitrate(fixedNow.Add(time.Second), []Intent{zulu, alpha}))
	if result.Winner == nil || result.Winner.ID != alpha.ID || result.Superseded[0].ReasonCode != ReasonDeterministicTieBreak {
		t.Fatalf("result = %+v", result)
	}
}

func TestArbiterPermutationDoesNotChangeResults(t *testing.T) {
	intents := []Intent{
		mustSchedulableIntent(t, AuthorityActivePolicy, "policy", 123, true, fixedNow),
		mustSchedulableIntent(t, AuthorityAutonomousAgent, "agent", 123, false, fixedNow.Add(time.Second)),
		mustSchedulableIntent(t, AuthorityManualHold, "manual", 123, true, fixedNow.Add(2*time.Second)),
	}
	permutations := permuteIntents(intents)
	want := Arbitrate(fixedNow.Add(3*time.Second), permutations[0])
	for index, permutation := range permutations[1:] {
		if got := Arbitrate(fixedNow.Add(3*time.Second), permutation); !reflect.DeepEqual(got, want) {
			t.Fatalf("permutation %d changed result:\ngot  %+v\nwant %+v", index+1, got, want)
		}
	}
}

func TestArbiterDoesNotModifyInput(t *testing.T) {
	intent := mustSchedulableIntent(t, AuthorityAutonomousAgent, "agent", 123, false, fixedNow)
	before := []Intent{cloneIntent(intent)}
	input := []Intent{intent}
	_ = Arbitrate(fixedNow.Add(time.Second), input)
	if !reflect.DeepEqual(input, before) {
		t.Fatalf("input changed:\ngot  %+v\nwant %+v", input, before)
	}
}

func TestArbiterSeparatesResourcesAndOperations(t *testing.T) {
	load := 25
	intents := []Intent{
		mustSchedulableIntent(t, AuthorityActivePolicy, "account-1-sched", 1, false, fixedNow),
		mustSchedulableIntent(t, AuthorityActivePolicy, "account-2-sched", 2, false, fixedNow),
		mustLoadFactorIntent(t, AuthorityActivePolicy, "account-1-load", 1, &load, fixedNow),
	}
	results := Arbitrate(fixedNow.Add(time.Second), intents)
	if len(results) != 3 {
		t.Fatalf("results = %d, want separate winners for two resources and operations: %+v", len(results), results)
	}
	for _, result := range results {
		if result.Winner == nil {
			t.Fatalf("missing winner: %+v", result)
		}
	}
}

func TestArbiterDeduplicatesSameIdempotentSemantics(t *testing.T) {
	canonical := mustSchedulableIntent(t, AuthorityActivePolicy, "alpha", 123, false, fixedNow)
	duplicate := cloneIntent(canonical)
	duplicate.ID = "zulu"
	result := onlyResult(t, Arbitrate(fixedNow.Add(time.Second), []Intent{duplicate, canonical}))
	if result.Winner == nil || result.Winner.ID != "alpha" {
		t.Fatalf("winner = %+v", result.Winner)
	}
	if len(result.Ignored) != 1 || result.Ignored[0].ReasonCode != ReasonDuplicate || result.Ignored[0].Intent.ID != "zulu" {
		t.Fatalf("ignored duplicate = %+v", result.Ignored)
	}
}

func TestArbiterRejectsConflictingIdempotencyKeyWithoutWinner(t *testing.T) {
	first := mustSchedulableIntent(t, AuthorityActivePolicy, "first", 123, false, fixedNow)
	second := mustSchedulableIntent(t, AuthorityActivePolicy, "second", 123, true, fixedNow)
	second.IdempotencyKey = first.IdempotencyKey
	result := onlyResult(t, Arbitrate(fixedNow.Add(time.Second), []Intent{first, second}))
	if result.Winner != nil || len(result.Ignored) != 2 {
		t.Fatalf("conflicting idempotency key was silently selected: %+v", result)
	}
	for _, ignored := range result.Ignored {
		if ignored.ReasonCode != ReasonIdempotencyConflict {
			t.Fatalf("ignored = %+v", result.Ignored)
		}
	}
}

func TestArbiterMarksCrossResourceIdempotencyConflict(t *testing.T) {
	first := mustSchedulableIntent(t, AuthorityActivePolicy, "first", 1, false, fixedNow)
	second := mustSchedulableIntent(t, AuthorityActivePolicy, "second", 2, false, fixedNow)
	second.IdempotencyKey = first.IdempotencyKey
	results := Arbitrate(fixedNow.Add(time.Second), []Intent{second, first})
	if len(results) != 2 {
		t.Fatalf("results = %+v", results)
	}
	for _, result := range results {
		if result.Winner != nil || len(result.Ignored) != 1 || result.Ignored[0].ReasonCode != ReasonIdempotencyConflict {
			t.Fatalf("cross-resource conflict = %+v", result)
		}
	}
}

func TestArbiterOnlyInvalidOrExpiredReturnsReasonsWithoutWinner(t *testing.T) {
	invalid := mustSchedulableIntent(t, AuthorityActivePolicy, "invalid", 123, false, fixedNow.Add(-time.Hour))
	invalid.Actor = ""
	expiredMetadata := metadataFor(AuthorityAutonomousAgent, "expired", fixedNow.Add(-time.Hour))
	expiresAt := fixedNow
	expiredMetadata.ExpiresAt = &expiresAt
	expired, err := NewAccountSchedulableIntent(expiredMetadata, 123, true)
	if err != nil {
		t.Fatal(err)
	}
	result := onlyResult(t, Arbitrate(fixedNow, []Intent{expired, invalid}))
	if result.Winner != nil || len(result.Ignored) != 2 {
		t.Fatalf("result = %+v", result)
	}
	reasons := []ReasonCode{result.Ignored[0].ReasonCode, result.Ignored[1].ReasonCode}
	sort.Slice(reasons, func(left, right int) bool { return reasons[left] < reasons[right] })
	if !reflect.DeepEqual(reasons, []ReasonCode{ReasonExpired, ReasonInvalid}) {
		t.Fatalf("reasons = %v", reasons)
	}
}

func onlyResult(t *testing.T, results []ArbitrationResult) ArbitrationResult {
	t.Helper()
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1: %+v", len(results), results)
	}
	return results[0]
}

func permuteIntents(input []Intent) [][]Intent {
	working := append([]Intent(nil), input...)
	result := make([][]Intent, 0)
	var generate func(int)
	generate = func(index int) {
		if index == len(working) {
			result = append(result, append([]Intent(nil), working...))
			return
		}
		for candidate := index; candidate < len(working); candidate++ {
			working[index], working[candidate] = working[candidate], working[index]
			generate(index + 1)
			working[index], working[candidate] = working[candidate], working[index]
		}
	}
	generate(0)
	return result
}
