package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestAgentSettingsRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	defaults, err := database.GetAgentSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Mode != model.AgentModeObserve || defaults.AnalysisIntervalMinutes != 30 ||
		defaults.EmergencyCooldownMinutes != 5 || defaults.ContextTokenBudget != 16000 ||
		defaults.MaxAnomalies != 20 || defaults.MaxDrilldowns != 8 || defaults.RetentionDays != 90 {
		t.Fatalf("unexpected defaults: %+v", defaults)
	}

	now := time.Now().UTC().Truncate(time.Second)
	updated := model.AgentSettings{
		OptimizerMode: model.AgentOptimizerAuto, OperatorMode: model.AgentOperatorConfirm, DailyPolicyChangeBudget: 3,
		AnalysisIntervalMinutes:  45,
		EmergencyCooldownMinutes: 7, ContextTokenBudget: 12000, MaxAnomalies: 12,
		MaxDrilldowns: 4, RetentionDays: 45, ObservationStartedAt: &now,
		SuccessfulObservationRuns: 41, ObservationProposedActions: 20,
		ObservationExecutableActions: 19, ObservationViolations: 1,
		ObservationStructureErrors: 2, LastScheduledAt: &now, LastEmergencyAt: &now,
	}
	if err := database.UpdateAgentSettings(ctx, updated); err != nil {
		t.Fatal(err)
	}
	loaded, err := database.GetAgentSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Enabled || loaded.Mode != model.AgentModeControl || loaded.OptimizerMode != model.AgentOptimizerAuto ||
		loaded.OperatorMode != model.AgentOperatorConfirm || loaded.DailyPolicyChangeBudget != 3 || loaded.AnalysisIntervalMinutes != 45 ||
		loaded.ContextTokenBudget != 12000 || loaded.SuccessfulObservationRuns != 41 ||
		loaded.ObservationProposedActions != 20 || loaded.ObservationExecutableActions != 19 ||
		loaded.ObservationViolations != 1 || loaded.ObservationStructureErrors != 2 ||
		loaded.ObservationStartedAt == nil || !loaded.ObservationStartedAt.Equal(now) ||
		loaded.LastScheduledAt == nil || !loaded.LastScheduledAt.Equal(now) {
		t.Fatalf("settings were not preserved: %+v", loaded)
	}
}

func TestAgentObservationDoesNotImplicitlyEnableControl(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC().Truncate(time.Second)
	started := now.Add(-24*time.Hour - time.Minute)
	settings, err := database.GetAgentSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	settings.OptimizerMode = model.AgentOptimizerObserve
	settings.OperatorMode = model.AgentOperatorDisabled
	settings.ObservationStartedAt = &started
	settings.SuccessfulObservationRuns = 39
	settings.ObservationProposedActions = 20
	settings.ObservationExecutableActions = 19
	if err := database.UpdateAgentSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}

	activatedSettings, activated, err := database.AdvanceAgentSchedule(ctx, model.AgentRunScheduled, now)
	if err != nil {
		t.Fatal(err)
	}
	if activated || activatedSettings.OptimizerMode != model.AgentOptimizerObserve || activatedSettings.SuccessfulObservationRuns != 40 {
		t.Fatalf("observation must not activate control: activated=%v settings=%+v", activated, activatedSettings)
	}

	settings = activatedSettings
	settings.ObservationStartedAt = &started
	settings.SuccessfulObservationRuns = 39
	settings.ObservationViolations = 1
	if err := database.UpdateAgentSettings(ctx, settings); err != nil {
		t.Fatal(err)
	}
	blockedSettings, activated, err := database.AdvanceAgentSchedule(ctx, model.AgentRunScheduled, now)
	if err != nil {
		t.Fatal(err)
	}
	if activated || blockedSettings.OptimizerMode != model.AgentOptimizerObserve {
		t.Fatalf("observation mode changed unexpectedly: activated=%v settings=%+v", activated, blockedSettings)
	}
}

func TestPublishPolicyVersionAtomicallyUpdatesProjectionAndActivation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	oldVersion := testLifecycleProposal("old-pool-policy", "pool", "gpt", json.RawMessage(`{"failure_threshold":3}`), nil)
	if err := database.CreatePolicyProposal(ctx, &oldVersion); err != nil {
		t.Fatal(err)
	}
	oldID := oldVersion.ID
	for _, accountID := range []int64{298, 299} {
		threshold := 3
		if err := database.UpsertPolicy(ctx, model.Policy{AccountID: accountID, Enabled: true, FailureThreshold: &threshold,
			ScorePolicySource: "pool", ScorePolicyVersionID: &oldID}); err != nil {
			t.Fatal(err)
		}
	}
	initialProjection := make([]model.Policy, 0, 2)
	for _, accountID := range []int64{298, 299} {
		threshold := 3
		initialProjection = append(initialProjection, model.Policy{AccountID: accountID, Enabled: true, FailureThreshold: &threshold,
			ScorePolicySource: "pool", ScorePolicyVersionID: &oldID})
	}
	if err := database.PublishPolicyProposal(ctx, oldVersion.ID, "test", nil, initialProjection); err != nil {
		t.Fatal(err)
	}
	newVersion := testLifecycleProposal("new-pool-policy", "pool", "gpt", json.RawMessage(`{"failure_threshold":5}`), &oldID)
	if err := database.CreatePolicyProposal(ctx, &newVersion); err != nil {
		t.Fatal(err)
	}
	newID := newVersion.ID
	threshold := 5
	projection := []model.Policy{
		{AccountID: 298, Enabled: true, FailureThreshold: &threshold, ScorePolicySource: "pool", ScorePolicyVersionID: &newID},
		{AccountID: 299, Enabled: true, FailureThreshold: &threshold, ScorePolicySource: "pool", ScorePolicyVersionID: &newID},
	}
	if _, err := database.db.ExecContext(ctx, `CREATE TRIGGER reject_atomic_policy_publish
		BEFORE INSERT ON account_policies WHEN NEW.account_id=299
		BEGIN SELECT RAISE(ABORT, 'injected publish failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := database.PublishPolicyProposal(ctx, newVersion.ID, "test", nil, projection); err == nil {
		t.Fatal("injected publication failure was ignored")
	}
	policies, err := database.ListPolicies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, accountID := range []int64{298, 299} {
		policy := policies[accountID]
		if policy.ScorePolicyVersionID == nil || *policy.ScorePolicyVersionID != oldVersion.ID || policy.FailureThreshold == nil || *policy.FailureThreshold != 3 {
			t.Fatalf("failed publication changed account %d: %+v", accountID, policy)
		}
	}
	oldLoaded, _ := database.GetPolicyLifecycle(ctx, oldVersion.ID)
	newLoaded, _ := database.GetPolicyLifecycle(ctx, newVersion.ID)
	if oldLoaded.Status != model.PolicyStatusActive || newLoaded.Status != model.PolicyStatusSimulated {
		t.Fatalf("failed projection changed active pointer: old=%+v new=%+v", oldLoaded, newLoaded)
	}

	if _, err := database.db.ExecContext(ctx, `DROP TRIGGER reject_atomic_policy_publish`); err != nil {
		t.Fatal(err)
	}
	if err := database.PublishPolicyProposal(ctx, newVersion.ID, "test", nil, projection); err != nil {
		t.Fatal(err)
	}
	policies, _ = database.ListPolicies(ctx)
	for _, accountID := range []int64{298, 299} {
		policy := policies[accountID]
		if policy.ScorePolicyVersionID == nil || *policy.ScorePolicyVersionID != newVersion.ID || policy.FailureThreshold == nil || *policy.FailureThreshold != 5 {
			t.Fatalf("successful publication did not update account %d: %+v", accountID, policy)
		}
	}
	oldLoaded, _ = database.GetPolicyLifecycle(ctx, oldVersion.ID)
	newLoaded, _ = database.GetPolicyLifecycle(ctx, newVersion.ID)
	if oldLoaded.Status != model.PolicyStatusSuperseded || newLoaded.Status != model.PolicyStatusActive {
		t.Fatalf("successful projection did not move active pointer: old=%+v new=%+v", oldLoaded, newLoaded)
	}
}

func TestAgentProviderRoundTripAndDefaults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	providers, err := database.ListAgentProviders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 2 || providers[0].Slot != "primary" || providers[1].Slot != "fallback" ||
		providers[0].TimeoutSeconds != 90 || providers[0].MaxOutputTokens != 4096 {
		t.Fatalf("unexpected provider defaults: %+v", providers)
	}

	validatedAt := time.Now().UTC().Truncate(time.Second)
	want := model.AgentProvider{
		Slot: "primary", BaseURL: "https://model.example/v1", Model: "reasoner",
		CredentialNonce: []byte{1, 2, 3}, CredentialCiphertext: []byte{4, 5, 6}, Enabled: true,
		TimeoutSeconds: 75, MaxOutputTokens: 8192, Temperature: .2,
		LastValidatedAt: &validatedAt, LastError: "",
	}
	if err := database.UpsertAgentProvider(ctx, want); err != nil {
		t.Fatal(err)
	}
	loaded, err := database.GetAgentProvider(ctx, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Enabled || !loaded.APIKeyConfigured || loaded.BaseURL != want.BaseURL || loaded.Model != want.Model ||
		loaded.TimeoutSeconds != 75 || loaded.MaxOutputTokens != 8192 || loaded.Temperature != .2 ||
		loaded.LastValidatedAt == nil || !loaded.LastValidatedAt.Equal(validatedAt) ||
		string(loaded.CredentialNonce) != string(want.CredentialNonce) ||
		string(loaded.CredentialCiphertext) != string(want.CredentialCiphertext) {
		t.Fatalf("provider was not preserved: %+v", loaded)
	}
}

func TestAnalysisPacketRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	cutoff := time.Now().UTC().Truncate(time.Second)
	packet := model.AnalysisPacket{
		Kind: model.AgentRunScheduled, CutoffAt: cutoff, Hash: "packet-hash", TokenEstimate: 321,
		NoMaterialChange: true,
		SystemSummary:    model.AgentSystemSummary{Accounts: 2, Available: 1, Degraded: 1, AverageAvailability: 97.5},
		PoolSummaries:    []model.AgentPoolSummary{{Name: "pool-a", Accounts: 2, Available: 1, Degraded: 1}},
		AccountCompactStates: []model.AgentAccountState{{
			AccountID: 225, Name: "account-a", AvailabilityState: "available", AvailabilityScore: 99,
			Reasons: []string{"真实流量正常"}, Windows: map[string]model.AgentWindowStats{"30m": {Window: "30m", SampleCount: 10}},
		}},
		Anomalies: []model.AgentAccountState{{AccountID: 298, Name: "account-b", AvailabilityState: "degraded", RiskScore: 40}},
		Changes:   []string{"account-b changed"}, ActivePolicies: json.RawMessage(`{"mode":"adaptive"}`),
		DecisionOutcomes: json.RawMessage(`[]`), EvidenceCatalog: []string{"account:225", "pool:pool-a"},
	}
	if err := database.SaveAnalysisPacket(ctx, &packet); err != nil {
		t.Fatal(err)
	}
	if packet.ID == 0 {
		t.Fatal("saved packet has no ID")
	}

	loaded, err := database.GetAnalysisPacket(ctx, packet.ID)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := database.LatestAnalysisPacket(ctx, model.AgentRunScheduled)
	if err != nil {
		t.Fatal(err)
	}
	items, err := database.ListAnalysisPackets(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ID != packet.ID || latest.ID != packet.ID || len(items) != 1 || items[0].ID != packet.ID ||
		loaded.Hash != "packet-hash" || !loaded.NoMaterialChange || len(loaded.AccountCompactStates) != 1 ||
		loaded.AccountCompactStates[0].Windows["30m"].SampleCount != 10 || len(loaded.EvidenceCatalog) != 2 {
		t.Fatalf("packet was not preserved: loaded=%+v latest=%+v items=%+v", loaded, latest, items)
	}
}

func TestPolicyLifecycleActivationAndRollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	first := testLifecycleProposal("first-account-policy", "account", "225", json.RawMessage(`{"failure_threshold":3}`), nil)
	if err := database.CreatePolicyProposal(ctx, &first); err != nil {
		t.Fatal(err)
	}
	firstThreshold := 3
	firstProjection := []model.Policy{{AccountID: 225, Enabled: true, FailureThreshold: &firstThreshold, ScorePolicySource: "account_version", ScorePolicyVersionID: &first.ID}}
	if err := database.PublishPolicyProposal(ctx, first.ID, "test", nil, firstProjection); err != nil {
		t.Fatal(err)
	}
	second := testLifecycleProposal("second-account-policy", "account", "225", json.RawMessage(`{"failure_threshold":4}`), &first.ID)
	if err := database.CreatePolicyProposal(ctx, &second); err != nil {
		t.Fatal(err)
	}
	secondThreshold := 4
	secondProjection := []model.Policy{{AccountID: 225, Enabled: true, FailureThreshold: &secondThreshold, ScorePolicySource: "account_version", ScorePolicyVersionID: &second.ID}}
	if err := database.PublishPolicyProposal(ctx, second.ID, "test", nil, secondProjection); err != nil {
		t.Fatal(err)
	}
	if first.Version != 1 || second.Version != 2 || second.Status != model.PolicyStatusSimulated {
		t.Fatalf("unexpected versions: first=%+v second=%+v", first, second)
	}

	items, err := database.ListPolicyLifecycle(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	statuses := policyStatuses(items)
	if statuses[first.ID] != "superseded" || statuses[second.ID] != "active" {
		t.Fatalf("unexpected active policy before rollback: %#v", statuses)
	}
	if err := database.RollbackPolicyProposal(ctx, second.ID, first.ID, "test", "rollback test", nil, firstProjection); err != nil {
		t.Fatal(err)
	}
	items, err = database.ListPolicyLifecycle(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	statuses = policyStatuses(items)
	if statuses[first.ID] != model.PolicyStatusActive || statuses[second.ID] != model.PolicyStatusRolledBack {
		t.Fatalf("unexpected active policy after rollback: %#v", statuses)
	}
}

func testLifecycleProposal(key, scopeType, scopeID string, patch json.RawMessage, baseID *int64) model.ScorePolicyVersion {
	return model.ScorePolicyVersion{ScopeType: scopeType, ScopeID: scopeID, Status: model.PolicyStatusSimulated,
		Config: append(json.RawMessage(nil), patch...), Patch: append(json.RawMessage(nil), patch...), Diff: json.RawMessage(`{}`),
		Simulation: model.PolicySimulation{Passed: true, SampleCount: 100}, RiskLevel: model.AgentRiskLow,
		Reason: "test", CreatedBy: "test", BaseVersionID: baseID, IdempotencyKey: key, SemanticHash: key + "-semantic"}
}

func TestAccountScorePolicyRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	warning, critical := int64(9000), int64(18000)
	policy := model.Policy{
		AccountID: 225, Enabled: true, HealthHealthyScore: scoreIntPtr(88), HealthWatchScore: scoreIntPtr(72),
		HealthQuarantineScore: scoreIntPtr(40), HealthMinSamples: scoreIntPtr(25),
		HealthLatencyWarningMS: &warning, HealthLatencyCriticalMS: &critical,
		HealthTrafficPauseBelow: scoreIntPtr(75), HealthTrafficHealthyAt: scoreIntPtr(97),
		HealthHardFailures10: scoreIntPtr(4), HealthPersistentSlowRate: scoreIntPtr(55),
	}
	if err := database.UpsertPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	items, err := database.ListPolicies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := items[225]
	if got.HealthTrafficPauseBelow == nil || *got.HealthTrafficPauseBelow != 75 ||
		got.HealthTrafficHealthyAt == nil || *got.HealthTrafficHealthyAt != 97 ||
		got.HealthHardFailures10 == nil || *got.HealthHardFailures10 != 4 ||
		got.HealthPersistentSlowRate == nil || *got.HealthPersistentSlowRate != 55 ||
		got.HealthLatencyCriticalMS == nil || *got.HealthLatencyCriticalMS != critical {
		t.Fatalf("score policy overrides were not preserved: %+v", got)
	}
}

func scoreIntPtr(value int) *int { return &value }

func policyStatuses(items []model.ScorePolicyVersion) map[int64]string {
	result := make(map[int64]string, len(items))
	for _, item := range items {
		result[item.ID] = item.Status
	}
	return result
}
