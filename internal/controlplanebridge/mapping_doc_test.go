package controlplanebridge

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyWritePathMappingDocumentsFinalMutationBoundaries(t *testing.T) {
	documentPath := filepath.Join("..", "..", "docs", "architecture", "legacy-write-path-mapping.md")
	contents, err := os.ReadFile(documentPath)
	if err != nil {
		t.Fatalf("read legacy write-path mapping %q (build contexts running Go tests must include docs): %v", documentPath, err)
	}
	required := []string{
		"## StableSourceID 审计",
		"WP-A01", "WP-A02", "WP-A03", "WP-A04", "WP-A05", "WP-A06",
		"WP-A07", "WP-A08", "WP-A09", "WP-A10", "WP-A11",
		"WP-G01", "WP-G02", "WP-G03", "WP-G04", "WP-G05", "WP-G06", "WP-G07",
		"Engine.ManualPause",
		"Engine.ForceSetLoadFactor",
		"Engine.PinLoad",
		"Engine.ManualResume",
		"Engine.ForceResume",
		"Engine.reconcileAdaptiveLoad",
		"Engine.applyPause",
		"Engine.applyResume",
		"balance.Manager.reconcileCostRouting",
		"balance.Manager.TransitionGroupTier",
		"failover.Controller.handleOutage",
		"next configured enabled level",
		"Automatic return and dynamic candidate selection are absent",
		"sub2api.Client.SetSchedulable",
		"sub2api.Client.UpdateLoadFactor",
		"balance.Fetcher.SwitchGroup",
		"PublishPolicyProposal",
		"RollbackPolicyProposal",
		"historical read-only",
	}
	document := string(contents)
	for _, marker := range required {
		if !strings.Contains(document, marker) {
			t.Errorf("legacy mapping is missing %s", marker)
		}
	}
}
