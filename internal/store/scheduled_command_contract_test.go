package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestScheduledCommandPersistsTypedOccurrenceContract(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Now().UTC().Truncate(time.Second)
	expires := now.Add(10 * time.Minute)
	command := model.ScheduledCommand{
		Capability: "transition_token_group_tier", Arguments: json.RawMessage(`{"source_id":7,"key_id":"key-1","target_tier":"backup"}`),
		Conditions: json.RawMessage(`{"fresh":true}`), IntentType: "scheduled_action", ResourceType: "upstream_key",
		ResourceIDs: json.RawMessage(`[7,"key-1"]`), Operation: "transition_token_group_tier", DesiredState: json.RawMessage(`{"target_tier":"backup"}`),
		Timezone: "Asia/Shanghai", ExecuteAt: now.Add(time.Minute), ExpiresAt: &expires, IdempotencyKey: "occurrence-command-1",
		OccurrenceID: "occurrence-1", MissedPolicy: "skip", Authority: "administrator_command", CreatedBy: "administrator:agent",
	}
	if err := database.CreateScheduledCommand(context.Background(), &command); err != nil {
		t.Fatal(err)
	}
	loaded, err := database.GetScheduledCommand(context.Background(), command.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.OccurrenceID != "occurrence-1" || loaded.MissedPolicy != "skip" || loaded.Authority != "administrator_command" ||
		loaded.IntentType != "scheduled_action" || loaded.ResourceType != "upstream_key" || loaded.Operation != "transition_token_group_tier" {
		t.Fatalf("typed scheduled command metadata was not preserved: %+v", loaded)
	}

	conflict := command
	conflict.ID = 0
	conflict.Operation = "different_operation"
	if err := database.CreateScheduledCommand(context.Background(), &conflict); !errors.Is(err, ErrScheduledCommandIdempotencyConflict) {
		t.Fatalf("same idempotency key with changed command was not rejected: %v", err)
	}
}

func TestScheduledCommandMissedPolicyIsExplicit(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	command := model.ScheduledCommand{Capability: "pause_account", Arguments: json.RawMessage(`{"account_id":1}`),
		ExecuteAt: time.Now().UTC().Add(time.Minute), IdempotencyKey: "skip-without-window", MissedPolicy: "skip"}
	if err := database.CreateScheduledCommand(context.Background(), &command); err == nil {
		t.Fatal("skip missed policy was accepted without an explicit expiry window")
	}
}
