package controlplane

import (
	"errors"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

var fixedNow = time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)

func metadataFor(authority Authority, id string, createdAt time.Time) IntentMetadata {
	metadata := IntentMetadata{
		ID: id, IdempotencyKey: "idem-" + id, Authority: authority,
		Actor: "test-actor", Reason: "fixed test reason", CreatedAt: createdAt,
	}
	switch authority {
	case AuthorityManualHold:
		metadata.Producer = ProducerAdminUI
	case AuthorityAdministratorCommand:
		metadata.Producer = ProducerAdminUI
		expiresAt := createdAt.Add(30 * time.Minute)
		metadata.ExpiresAt = &expiresAt
	case AuthorityEmergencyAutomation:
		metadata.Producer = ProducerFailoverController
		metadata.SnapshotVersion = "snapshot-1"
		metadata.EvidenceRefs = []string{"monitor:7"}
	case AuthorityAutonomousAgent:
		metadata.Producer = ProducerAgentOperator
		metadata.SnapshotVersion = "snapshot-1"
		metadata.EvidenceRefs = []string{"packet:9"}
		expiresAt := createdAt.Add(15 * time.Minute)
		metadata.ExpiresAt = &expiresAt
	case AuthorityActivePolicy:
		metadata.Producer = ProducerPolicyScheduler
		metadata.SnapshotVersion = "snapshot-1"
		metadata.PolicyVersion = "policy-4"
	case AuthorityOptimization:
		metadata.Producer = ProducerCostOptimizer
		metadata.SnapshotVersion = "snapshot-1"
		expiresAt := createdAt.Add(10 * time.Minute)
		metadata.ExpiresAt = &expiresAt
	}
	return metadata
}

func mustSchedulableIntent(t *testing.T, authority Authority, id string, accountID int64, value bool, createdAt time.Time) Intent {
	t.Helper()
	intent, err := NewAccountSchedulableIntent(metadataFor(authority, id, createdAt), accountID, value)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func mustLoadFactorIntent(t *testing.T, authority Authority, id string, accountID int64, value *int, createdAt time.Time) Intent {
	t.Helper()
	intent, err := NewAccountLoadFactorIntent(metadataFor(authority, id, createdAt), accountID, value)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func TestIntentConstructorsProduceTypedDesiredStates(t *testing.T) {
	schedulable := mustSchedulableIntent(t, AuthorityActivePolicy, "sched", 123, false, fixedNow)
	if value, ok := schedulable.DesiredState.Schedulable(); !ok || value {
		t.Fatalf("schedulable state = %v, %v", value, ok)
	}
	load := 25
	loadIntent := mustLoadFactorIntent(t, AuthorityActivePolicy, "load", 123, &load, fixedNow)
	if value, configured, ok := loadIntent.DesiredState.LoadFactor(); !ok || !configured || value != 25 {
		t.Fatalf("load state = %d, %v, %v", value, configured, ok)
	}
	groupIntent, err := NewUpstreamKeyGroupTierIntent(metadataFor(AuthorityEmergencyAutomation, "group", fixedNow), 8, "token-1", model.GroupTierBackup)
	if err != nil {
		t.Fatal(err)
	}
	if tier, ok := groupIntent.DesiredState.GroupTier(); !ok || tier != model.GroupTierBackup {
		t.Fatalf("group tier state = %q, %v", tier, ok)
	}
}

func TestIntentValidationAuthorityRequirements(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*IntentMetadata)
	}{
		{name: "autonomous agent requires expiration", mutate: func(metadata *IntentMetadata) { metadata.ExpiresAt = nil }},
		{name: "optimization requires expiration", mutate: func(metadata *IntentMetadata) { metadata.ExpiresAt = nil }},
		{name: "active policy requires policy version", mutate: func(metadata *IntentMetadata) { metadata.PolicyVersion = "" }},
		{name: "automation requires snapshot version", mutate: func(metadata *IntentMetadata) { metadata.SnapshotVersion = "" }},
	}
	authorities := []Authority{AuthorityAutonomousAgent, AuthorityOptimization, AuthorityActivePolicy, AuthorityEmergencyAutomation}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := metadataFor(authorities[index], "invalid-authority", fixedNow)
			test.mutate(&metadata)
			if _, err := NewAccountSchedulableIntent(metadata, 123, false); !errors.Is(err, ErrInvalidIntent) {
				t.Fatalf("error = %v, want ErrInvalidIntent", err)
			}
		})
	}
}

func TestAutomationAuthoritiesRequireEvidenceWhereSpecified(t *testing.T) {
	for _, authority := range []Authority{AuthorityAutonomousAgent, AuthorityEmergencyAutomation} {
		metadata := metadataFor(authority, authority.String(), fixedNow)
		metadata.EvidenceRefs = nil
		if _, err := NewAccountSchedulableIntent(metadata, 123, false); !errors.Is(err, ErrInvalidIntent) {
			t.Fatalf("authority %s error = %v, want evidence validation", authority, err)
		}
	}
}

func TestManualHoldWithoutExpirationIsValid(t *testing.T) {
	intent := mustSchedulableIntent(t, AuthorityManualHold, "manual", 123, false, fixedNow)
	if intent.ExpiresAt != nil {
		t.Fatalf("manual hold unexpectedly expires at %v", intent.ExpiresAt)
	}
	if err := intent.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestAdministratorCommandRequiresTTLInsteadOfImplicitPermanentHold(t *testing.T) {
	metadata := metadataFor(AuthorityAdministratorCommand, "admin", fixedNow)
	metadata.ExpiresAt = nil
	if _, err := NewAccountSchedulableIntent(metadata, 123, false); !errors.Is(err, ErrInvalidIntent) {
		t.Fatalf("error = %v, want explicit TTL validation", err)
	}
}

func TestIntentRejectsExpirationAtOrBeforeCreation(t *testing.T) {
	for _, expiration := range []time.Time{fixedNow, fixedNow.Add(-time.Second)} {
		metadata := metadataFor(AuthorityAdministratorCommand, "bad-expiration", fixedNow)
		metadata.ExpiresAt = &expiration
		if _, err := NewAccountSchedulableIntent(metadata, 123, false); !errors.Is(err, ErrInvalidIntent) {
			t.Fatalf("expiration %s error = %v", expiration, err)
		}
	}
}

func TestIntentRejectsRequiredEmptyText(t *testing.T) {
	valid := mustSchedulableIntent(t, AuthorityActivePolicy, "valid", 123, false, fixedNow)
	tests := []struct {
		name   string
		mutate func(*Intent)
	}{
		{name: "id", mutate: func(intent *Intent) { intent.ID = "" }},
		{name: "idempotency key", mutate: func(intent *Intent) { intent.IdempotencyKey = "" }},
		{name: "actor", mutate: func(intent *Intent) { intent.Actor = "" }},
		{name: "reason", mutate: func(intent *Intent) { intent.Reason = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			intent := cloneIntent(valid)
			test.mutate(&intent)
			if err := intent.Validate(); !errors.Is(err, ErrInvalidIntent) {
				t.Fatalf("error = %v, want ErrInvalidIntent", err)
			}
		})
	}
}

func TestIntentRejectsMismatchedOperationAndDesiredState(t *testing.T) {
	intent := mustSchedulableIntent(t, AuthorityActivePolicy, "mismatch", 123, false, fixedNow)
	load := 25
	desired, err := loadFactorState(&load)
	if err != nil {
		t.Fatal(err)
	}
	intent.DesiredState = desired
	if err := intent.Validate(); !errors.Is(err, ErrInvalidIntent) {
		t.Fatalf("error = %v, want desired-state mismatch", err)
	}
}

func TestIntentRejectsIllegalLoadFactor(t *testing.T) {
	for _, value := range []int{0, 101} {
		metadata := metadataFor(AuthorityActivePolicy, "bad-load", fixedNow)
		if _, err := NewAccountLoadFactorIntent(metadata, 123, &value); !errors.Is(err, ErrInvalidIntent) {
			t.Fatalf("load factor %d error = %v", value, err)
		}
	}
}

func TestIntentAllowsClearingLoadFactor(t *testing.T) {
	intent := mustLoadFactorIntent(t, AuthorityActivePolicy, "clear-load", 123, nil, fixedNow)
	if value, configured, ok := intent.DesiredState.LoadFactor(); !ok || configured || value != 0 {
		t.Fatalf("clear load state = %d, %v, %v", value, configured, ok)
	}
}

func TestIntentRejectsIllegalGroupTier(t *testing.T) {
	metadata := metadataFor(AuthorityEmergencyAutomation, "bad-tier", fixedNow)
	if _, err := NewUpstreamKeyGroupTierIntent(metadata, 8, "token-1", "other"); !errors.Is(err, ErrInvalidIntent) {
		t.Fatalf("error = %v, want invalid existing tier", err)
	}
}

func TestUnknownEnumStringConversionsDoNotPanic(t *testing.T) {
	if Producer("other").String() == "" || Authority("other").String() == "" || Operation("other").String() == "" {
		t.Fatal("unknown enum conversion returned an empty string")
	}
}
