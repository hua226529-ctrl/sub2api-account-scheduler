package store

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPolicyActivationHasNoLegacyOrParallelWriteAPI(t *testing.T) {
	root := filepath.Join("..", "..", "internal")
	forbidden := []string{"CreatePolicyVersion(", "ActivatePolicyVersion(", "PublishPolicyVersion(", "activatePolicyVersionTx("}
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, marker := range forbidden {
			if strings.Contains(string(content), marker) {
				t.Errorf("%s contains legacy policy write API %q", path, marker)
			}
		}
		if filepath.Base(path) != "policy_lifecycle.go" && strings.Contains(string(content), "UPDATE score_policy_versions SET status") {
			t.Errorf("%s updates policy lifecycle status outside the lifecycle store", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
