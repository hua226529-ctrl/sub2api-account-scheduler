package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProductionAgentHasNoLegacyRuntimeOrExecutionTableWrites(t *testing.T) {
	entries, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry, "_test.go") {
			continue
		}
		content, readErr := os.ReadFile(entry)
		if readErr != nil {
			t.Fatal(readErr)
		}
		text := string(content)
		for _, forbidden := range []string{
			"func (m *Manager) Run(",
			"func (m *Manager) Chat(",
			"func (m *Manager) executeActions(",
			"func (m *Manager) executeAction(",
			"CreateAgentRun(",
			"UpdateAgentRun(",
			"AddAgentToolCall(",
			"UpdateAgentToolCall(",
			"runtimeMu",
		} {
			if strings.Contains(text, forbidden) {
				t.Errorf("%s still contains legacy Agent production path %q", entry, forbidden)
			}
		}
	}
	storeSource, err := os.ReadFile(filepath.Join("..", "store", "agent.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"INSERT INTO agent_runs", "UPDATE agent_runs SET", "DELETE FROM agent_runs",
		"INSERT INTO agent_tool_calls", "UPDATE agent_tool_calls SET", "DELETE FROM agent_tool_calls",
	} {
		if strings.Contains(string(storeSource), forbidden) {
			t.Errorf("legacy Agent store is not read-only: found %q", forbidden)
		}
	}
}
