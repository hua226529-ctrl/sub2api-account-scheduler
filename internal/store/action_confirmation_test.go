package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestActionConfirmationHashBindingExpiryAndSingleConsumption(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	now := time.Now().UTC().Truncate(time.Second)
	enqueue := func(tokenHash, payloadHash string, expiresAt time.Time) model.AgentGoal {
		t.Helper()
		goal := model.AgentGoal{Title: "confirmation", Objective: "confirm exact action", Status: model.AgentGoalStatusWaiting,
			Lane: model.AgentLaneInteractive, Priority: 100, RiskLevel: model.AgentRiskCritical, Source: "administrator",
			Context: json.RawMessage(`{"confirmed":false}`), CreatedBy: "administrator"}
		confirmation := model.ActionConfirmation{Administrator: "administrator", TokenHash: tokenHash, PayloadHash: payloadHash,
			Resources: json.RawMessage(`["1","2"]`), Operation: "bulk_pause", DesiredState: json.RawMessage(`{"schedulable":false}`),
			Status: "pending", ExpiresAt: expiresAt}
		if _, err := database.EnqueueChatGoal(ctx, 0, "pause all", &goal, &confirmation); err != nil {
			t.Fatal(err)
		}
		return goal
	}

	goal := enqueue("sha256-token", "payload-v1", now.Add(5*time.Minute))
	var storedHash string
	if err := database.db.QueryRowContext(ctx, `SELECT token_hash FROM action_confirmations WHERE goal_id=?`, goal.ID).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	if storedHash != "sha256-token" || strings.Contains(storedHash, "raw-secret") {
		t.Fatalf("confirmation token storage = %q", storedHash)
	}
	if _, err := database.ConfirmAgentGoal(ctx, goal.ID, "administrator", "sha256-token", "payload-v2",
		json.RawMessage(`{"confirmed":true}`), now); err == nil || !strings.Contains(err.Error(), "payload changed") {
		t.Fatalf("changed payload was not rejected: %v", err)
	}
	confirmed, err := database.ConfirmAgentGoal(ctx, goal.ID, "administrator", "sha256-token", "payload-v1",
		json.RawMessage(`{"confirmed":true}`), now)
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.Status != model.AgentGoalStatusPlanned || string(confirmed.Context) != `{"confirmed":true}` {
		t.Fatalf("confirmed goal = %+v", confirmed)
	}
	if _, err := database.ConfirmAgentGoal(ctx, goal.ID, "administrator", "sha256-token", "payload-v1",
		json.RawMessage(`{"confirmed":true}`), now); err == nil {
		t.Fatal("confirmation token was reusable")
	}

	expired := enqueue("expired-token", "payload-expired", now.Add(-time.Second))
	if _, err := database.ConfirmAgentGoal(ctx, expired.ID, "administrator", "expired-token", "payload-expired",
		json.RawMessage(`{"confirmed":true}`), now); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired token was not rejected: %v", err)
	}
	var expiredStatus string
	if err := database.db.QueryRowContext(ctx, `SELECT status FROM action_confirmations WHERE goal_id=?`, expired.ID).Scan(&expiredStatus); err != nil {
		t.Fatal(err)
	}
	if expiredStatus != "expired" {
		t.Fatalf("expired confirmation status = %q", expiredStatus)
	}
}
