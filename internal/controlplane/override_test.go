package controlplane

import (
	"errors"
	"testing"
	"time"
)

func TestTemporaryOverrideExpiresDeterministically(t *testing.T) {
	intent := mustSchedulableIntent(t, AuthorityAdministratorCommand, "admin", 123, false, fixedNow)
	lease, err := NewTemporaryOverride(intent)
	if err != nil {
		t.Fatal(err)
	}
	if !lease.Active(fixedNow.Add(time.Minute)) {
		t.Fatal("temporary override was inactive before expiration")
	}
	if lease.Active(*intent.ExpiresAt) {
		t.Fatal("temporary override remained active at expiration")
	}
}

func TestAutonomousAndOptimizationCannotCreatePermanentOverride(t *testing.T) {
	for _, authority := range []Authority{AuthorityAutonomousAgent, AuthorityOptimization} {
		metadata := metadataFor(authority, authority.String(), fixedNow)
		metadata.ExpiresAt = nil
		if _, err := NewAccountSchedulableIntent(metadata, 123, false); !errors.Is(err, ErrInvalidIntent) {
			t.Fatalf("authority %s error = %v", authority, err)
		}
	}
}

func TestManualHoldCanRemainActiveWithoutExpiration(t *testing.T) {
	intent := mustSchedulableIntent(t, AuthorityManualHold, "manual", 123, false, fixedNow)
	lease, err := NewManualHold(intent)
	if err != nil {
		t.Fatal(err)
	}
	if !lease.Active(fixedNow.Add(10 * 365 * 24 * time.Hour)) {
		t.Fatal("unexpired and unrevoked manual hold became inactive")
	}
}

func TestOverrideRevocationIsExplicitAndTimeDependent(t *testing.T) {
	intent := mustSchedulableIntent(t, AuthorityManualHold, "manual", 123, false, fixedNow)
	lease, err := NewManualHold(intent)
	if err != nil {
		t.Fatal(err)
	}
	revokedAt := fixedNow.Add(time.Hour)
	revoked, err := lease.Revoke(revokedAt, "administrator", "incident resolved")
	if err != nil {
		t.Fatal(err)
	}
	if !revoked.Active(revokedAt.Add(-time.Nanosecond)) || revoked.Active(revokedAt) {
		t.Fatal("revocation boundary is not deterministic")
	}
	if lease.Revocation() != nil || revoked.Revocation() == nil {
		t.Fatal("revocation mutated the original lease or was not modeled")
	}
}

func TestActivePolicyIsNotAnOverrideLease(t *testing.T) {
	policy := mustSchedulableIntent(t, AuthorityActivePolicy, "policy", 123, true, fixedNow)
	if _, err := NewTemporaryOverride(policy); !errors.Is(err, ErrInvalidOverride) {
		t.Fatalf("error = %v, want ErrInvalidOverride", err)
	}
	if _, err := NewManualHold(policy); !errors.Is(err, ErrInvalidOverride) {
		t.Fatalf("error = %v, want ErrInvalidOverride", err)
	}
}

func TestOverridePreservesIntentValidationError(t *testing.T) {
	_, err := NewTemporaryOverride(Intent{})
	if !errors.Is(err, ErrInvalidOverride) || !errors.Is(err, ErrInvalidIntent) {
		t.Fatalf("error = %v, want both override and intent validation errors", err)
	}
}

func TestZeroOverrideCannotBeRevoked(t *testing.T) {
	_, err := (OverrideLease{}).Revoke(fixedNow, "administrator", "invalid lease")
	if !errors.Is(err, ErrInvalidOverride) || !errors.Is(err, ErrInvalidIntent) {
		t.Fatalf("error = %v, want both override and intent validation errors", err)
	}
}
