package controlplanebridge

import (
	"os"
	"strings"
	"testing"
)

func TestLegacyWritePathMappingDocumentsAllKnownMutationPoints(t *testing.T) {
	contents, err := os.ReadFile("../../docs/architecture/legacy-write-path-mapping.md")
	if err != nil {
		t.Fatal(err)
	}
	required := []string{
		"## StableSourceID 审计",
		"WP-A01", "WP-A02", "WP-A03", "WP-A04", "WP-A05", "WP-A06",
		"WP-A07", "WP-A08", "WP-A09", "WP-A10", "WP-A11",
		"WP-G01", "WP-G02", "WP-G03", "WP-G04", "WP-G05", "WP-G06", "WP-G07",
		"Engine.ManualPause",
		"Engine.AgentPause",
		"Engine.AgentResume",
		"Engine.AgentSetLoadFactor",
		"Engine.ForceSetLoadFactor",
		"Engine.PinLoad",
		"Engine.ManualResume",
		"Engine.ForceResume",
		"Engine.reconcileAdaptiveLoadWithFreeze",
		"Engine.applyPause",
		"Engine.applyResume",
		"balance.Manager.reconcileCostRouting",
		"balance.Manager.SwitchGroup",
		"balance.Manager.TransitionGroupTier",
		"balance.Manager.switchAutomatedGroup",
		"failover.Controller.handleOutage",
		"failover.Controller.handleRecovery",
		"agent.Manager.executeAction",
		"agent.Manager.executeMutationCapability",
		"httpserver.accountAction",
		"httpserver.switchUpstreamKeyGroup",
		"httpserver.switchUpstreamKeyTier",
		"sub2api.Client.SetSchedulable",
		"sub2api.Client.UpdateLoadFactor",
		"balance.Fetcher.SwitchGroup",
	}
	document := string(contents)
	for _, marker := range required {
		if !strings.Contains(document, marker) {
			t.Errorf("legacy mapping is missing %s", marker)
		}
	}
}
