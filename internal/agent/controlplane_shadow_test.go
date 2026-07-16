package agent

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplanebridge"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplaneshadow"
)

func TestControlplaneShadowContextUsesActualInvocationMetadata(t *testing.T) {
	createdAt := time.Date(2026, 7, 17, 7, 0, 0, 0, time.UTC)
	expiresAt := createdAt.Add(30 * time.Minute)
	evidence := []string{"packet:17", "monitor:4"}
	invocation := CapabilityInvocation{
		Name: "pause_account", Arguments: json.RawMessage(`{"reason":"verified decision"}`),
		Actor: "agent:v2", IdempotencyKey: "goal:7/step:8/action:pause",
		CreatedAt: createdAt, ExpiresAt: &expiresAt, SnapshotVersion: "packet:17", EvidenceRefs: evidence,
	}

	ctx := withControlplaneShadowContext(context.Background(), invocation)
	evidence[0] = "mutated"
	expiresAt = expiresAt.Add(time.Hour)
	actual := controlplaneshadow.ActionContextFrom(ctx)
	if actual.StableSourceNamespace != controlplanebridge.SourceAgentAction ||
		actual.StableSourceID != invocation.IdempotencyKey || actual.Reason != "verified decision" ||
		actual.SnapshotVersion != "packet:17" || !actual.CreatedAt.Equal(createdAt) ||
		actual.ExpiresAt == nil || !actual.ExpiresAt.Equal(createdAt.Add(30*time.Minute)) ||
		!reflect.DeepEqual(actual.EvidenceRefs, []string{"packet:17", "monitor:4"}) {
		t.Fatalf("autonomous action context = %+v", actual)
	}
	if actual.AdministratorAuthorization != (controlplanebridge.AdministratorAuthorization{}) {
		t.Fatalf("autonomous invocation gained administrator authority: %+v", actual.AdministratorAuthorization)
	}
}

func TestControlplaneShadowContextRequiresActualGrantForAdministratorAuthority(t *testing.T) {
	createdAt := time.Date(2026, 7, 17, 7, 30, 0, 0, time.UTC)
	expiresAt := createdAt.Add(15 * time.Minute)
	grantID := "grant-consumption:actual-17"
	invocation := CapabilityInvocation{
		Name: "resume_account", Arguments: json.RawMessage(`{"reason":"administrator recovery"}`),
		Actor: "administrator:agent", IdempotencyKey: "agent-looking-source", CreatedAt: createdAt, ExpiresAt: &expiresAt,
		AdministratorGrant: &AdministratorGrant{GrantID: grantID},
	}

	actual := controlplaneshadow.ActionContextFrom(withControlplaneShadowContext(context.Background(), invocation))
	if actual.StableSourceNamespace != controlplanebridge.SourceAdministratorGrantConsumption || actual.StableSourceID != grantID {
		t.Fatalf("administrator source = %s/%q", actual.StableSourceNamespace, actual.StableSourceID)
	}
	authorization := actual.AdministratorAuthorization
	if !authorization.IdentityVerified || !authorization.ExactGrant || !authorization.GrantConsumed || authorization.GrantConsumptionID != grantID {
		t.Fatalf("administrator authorization = %+v", authorization)
	}

	invocation.AdministratorGrant = nil
	withoutGrant := controlplaneshadow.ActionContextFrom(withControlplaneShadowContext(context.Background(), invocation))
	if withoutGrant.StableSourceNamespace != controlplanebridge.SourceAgentAction ||
		withoutGrant.AdministratorAuthorization != (controlplanebridge.AdministratorAuthorization{}) {
		t.Fatalf("actor string elevated authority without a grant: %+v", withoutGrant)
	}
}

func TestControlplaneShadowContextDoesNotInventMissingMetadata(t *testing.T) {
	actual := controlplaneshadow.ActionContextFrom(withControlplaneShadowContext(context.Background(), CapabilityInvocation{
		Name: "pause_account", Arguments: json.RawMessage(`{"account_id":17}`), Actor: "agent:v2",
	}))
	if !actual.CreatedAt.IsZero() || actual.ExpiresAt != nil || actual.StableSourceID != "" ||
		actual.SnapshotVersion != "" || len(actual.EvidenceRefs) != 0 || actual.Reason != "" {
		t.Fatalf("missing runtime metadata was synthesized: %+v", actual)
	}
}
