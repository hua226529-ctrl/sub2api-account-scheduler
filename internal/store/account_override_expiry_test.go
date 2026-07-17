package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
)

func TestExpireActiveAccountOverridesIsIdempotentAndIgnoresManualHold(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), laneTestSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 17, 1, 0, 0, 0, time.UTC)
	due := now.Add(-time.Minute)
	future := now.Add(time.Hour)
	items := []accountcontrol.Override{
		expiryTestOverride("due-a", 2, accountcontrol.OverrideKindTemporary, &due, accountcontrol.OverrideActive, now),
		expiryTestOverride("due-b", 2, accountcontrol.OverrideKindTemporary, &due, accountcontrol.OverrideActive, now),
		expiryTestOverride("future", 3, accountcontrol.OverrideKindTemporary, &future, accountcontrol.OverrideActive, now),
		expiryTestOverride("revoked", 4, accountcontrol.OverrideKindTemporary, &due, accountcontrol.OverrideRevoked, now),
		expiryTestOverride("manual-hold", 5, accountcontrol.OverrideKindManualHold, nil, accountcontrol.OverrideActive, now),
	}
	for _, item := range items {
		if err := insertOverride(ctx, database.db, item); err != nil {
			t.Fatal(err)
		}
	}
	next, err := database.NextActiveAccountOverrideExpiry(ctx)
	if err != nil || next == nil || !next.Equal(due) {
		t.Fatalf("nearest expiry = %v, want %v, err=%v", next, due, err)
	}
	accounts, err := database.ExpireActiveAccountOverrides(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1 || accounts[0] != 2 {
		t.Fatalf("expired accounts were not deduplicated: %#v", accounts)
	}
	again, err := database.ExpireActiveAccountOverrides(ctx, now)
	if err != nil || len(again) != 0 {
		t.Fatalf("expiry was not idempotent: accounts=%#v err=%v", again, err)
	}
	var manualStatus string
	if err := database.db.QueryRowContext(ctx, `SELECT status FROM account_overrides WHERE id='manual-hold'`).Scan(&manualStatus); err != nil {
		t.Fatal(err)
	}
	if manualStatus != string(accountcontrol.OverrideActive) {
		t.Fatalf("manual hold without expiry was modified: %q", manualStatus)
	}
}

func expiryTestOverride(id string, accountID int64, kind accountcontrol.OverrideKind, expiresAt *time.Time,
	status accountcontrol.OverrideStatus, now time.Time) accountcontrol.Override {
	schedulable := false
	return accountcontrol.Override{ID: id, CommandID: "cmd-" + id, IntentID: "intent-" + id,
		IdempotencyKey: "idem-" + id, SemanticSignature: "semantic-" + id, AccountID: accountID,
		Operation: controlplane.OperationSetAccountSchedulable, Kind: kind, Schedulable: &schedulable,
		Producer: controlplane.ProducerAdminUI, Authority: controlplane.AuthorityAdministratorCommand,
		Actor: "test", Reason: "expiry test", CreatedAt: now.Add(-time.Hour), ExpiresAt: expiresAt,
		Status: status, UpdatedAt: now.Add(-time.Hour)}
}
