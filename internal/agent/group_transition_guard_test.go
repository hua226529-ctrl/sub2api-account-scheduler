package agent

import (
	"context"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type groupGuardWindowStore struct {
	windows map[int64]model.AgentWindowStats
}

func (s groupGuardWindowStore) GetAgentWindowStats(_ context.Context, accountID int64, _, _ time.Time, _ string) (model.AgentWindowStats, error) {
	return s.windows[accountID], nil
}

func TestAgentPoolOutageGuardRequiresHardMonitorAndHardTraffic(t *testing.T) {
	now := time.Now().UTC()
	binding := hardGuardBinding(now, 225, 3)
	store := groupGuardWindowStore{windows: map[int64]model.AgentWindowStats{225: {
		SampleCount: 10, EligibleCount: 10, SuccessCount: 1,
		ErrorCategoryCounts: map[string]int{model.ErrorClassInfrastructure: 9},
	}}}
	if !agentPoolOutageProven(context.Background(), store, []model.ResolvedBinding{binding}, now) {
		t.Fatal("fresh hard monitor failures plus hard traffic outage should authorize the deterministic guard")
	}

	binding.Account.Schedulable = true
	if agentPoolOutageProven(context.Background(), store, []model.ResolvedBinding{binding}, now) {
		t.Fatal("a pool with a schedulable active account must never be considered completely unavailable")
	}
	binding.Account.Schedulable = false
	binding.Monitor.PrimaryStatus = model.StatusDegraded
	if agentPoolOutageProven(context.Background(), store, []model.ResolvedBinding{binding}, now) {
		t.Fatal("yellow performance degradation must not authorize a group transition")
	}
}

func TestAgentPoolOutageGuardAllowsFiveHardMonitorResultsWithoutTraffic(t *testing.T) {
	now := time.Now().UTC()
	binding := hardGuardBinding(now, 299, 5)
	store := groupGuardWindowStore{windows: map[int64]model.AgentWindowStats{299: {
		ErrorCategoryCounts: map[string]int{},
	}}}
	if !agentPoolOutageProven(context.Background(), store, []model.ResolvedBinding{binding}, now) {
		t.Fatal("five hard monitor results with a stopped pool and no request samples should authorize fallback")
	}
}

func TestNextConfiguredAgentTierUsesFixedChainAndSkipsDisabledLevels(t *testing.T) {
	policy := model.GroupFailoverPolicy{
		MainGroupID: "main", BackupGroupID: "backup", EmergencyGroupID: "emergency",
		MainEnabled: true, BackupEnabled: false, EmergencyEnabled: true,
	}
	if got := nextConfiguredAgentTier(policy, model.GroupTierMain); got != model.GroupTierEmergency {
		t.Fatalf("disabled backup was not skipped: got %q", got)
	}
	if got := nextConfiguredAgentTier(policy, model.GroupTierEmergency); got != "" {
		t.Fatalf("fixed chain advanced beyond emergency: got %q", got)
	}
	policy.EmergencyEnabled = false
	if got := nextConfiguredAgentTier(policy, model.GroupTierMain); got != "" {
		t.Fatalf("fixed chain selected a disabled level: got %q", got)
	}
}

func hardGuardBinding(now time.Time, accountID int64, streak int) model.ResolvedBinding {
	checkedAt := now.Add(-time.Second)
	return model.ResolvedBinding{
		Account:      model.Account{ID: accountID, Status: "active", Schedulable: false},
		Monitor:      &model.Monitor{ID: accountID, Enabled: true, PrimaryStatus: model.StatusFailed, LastCheckedAt: &checkedAt},
		MonitorState: model.MonitorState{MonitorID: accountID, LastCheckedAt: &checkedAt, LastStatus: model.StatusFailed, UnhealthyStreak: streak},
	}
}
