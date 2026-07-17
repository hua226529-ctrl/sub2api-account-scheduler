package agent

import (
	"encoding/json"
	"testing"
	"time"
)

func TestScheduledOccurrenceIdentityIsStableAndPayloadBound(t *testing.T) {
	executeAt := time.Date(2026, 7, 18, 2, 0, 0, 0, time.UTC)
	arguments := json.RawMessage(`{"source_id":7,"key_id":"key-1","target_tier":"backup"}`)
	first := scheduledOccurrenceID("chat-goal-9", "transition_token_group_tier", arguments, executeAt, "Asia/Shanghai")
	second := scheduledOccurrenceID("chat-goal-9", "transition_token_group_tier", arguments, executeAt, "Asia/Shanghai")
	changed := scheduledOccurrenceID("chat-goal-9", "transition_token_group_tier", json.RawMessage(`{"source_id":7,"key_id":"key-1","target_tier":"emergency"}`), executeAt, "Asia/Shanghai")
	if first == "" || first != second || first == changed {
		t.Fatalf("occurrence identity is not stable and payload-bound: first=%q second=%q changed=%q", first, second, changed)
	}
}

func TestScheduledGroupMetadataIsTyped(t *testing.T) {
	resourceType, resourceIDs, operation, desired, err := scheduledCommandMetadata("transition_token_group_tier",
		json.RawMessage(`{"source_id":7,"key_id":"key-1","target_tier":"backup","reason":"maintenance"}`))
	if err != nil {
		t.Fatal(err)
	}
	if resourceType != "upstream_key" || string(resourceIDs) != `[7,"key-1"]` || operation != "transition_token_group_tier" ||
		string(desired) != `{"key_id":"key-1","source_id":7,"target_tier":"backup"}` {
		t.Fatalf("unexpected typed metadata: type=%s resources=%s operation=%s desired=%s", resourceType, resourceIDs, operation, desired)
	}
}
