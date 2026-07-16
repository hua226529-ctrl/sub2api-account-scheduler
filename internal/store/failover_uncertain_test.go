package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestUncertainGroupTransitionRetainsPendingIdempotencyReservation(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), model.Settings{
		FailureThreshold: 3, RecoveryThreshold: 3, ManualHoldMinutes: 10,
		FlapWindowMinutes: 60, FlapPauseThreshold: 3, FlapRecoveryThreshold: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	source, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{
		Name: "test", Provider: "newapi", BaseURL: "https://upstream.example", NormalizedURL: "https://upstream.example",
		CredentialNonce: []byte{1}, CredentialCiphertext: []byte{2}, PauseBelow: 1, ResumeAt: 2, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.SaveGroupFailoverPolicy(ctx, model.GroupFailoverPolicy{
		SourceID: source.ID, KeyID: "9", Enabled: true, Pool: "test", MainGroupID: "1", BackupGroupID: "2", EmergencyGroupID: "3",
	}); err != nil {
		t.Fatal(err)
	}
	transition, existed, err := database.BeginGroupTierTransition(ctx, model.GroupTierTransition{
		IdempotencyKey: "uncertain-group-write", SourceID: source.ID, KeyID: "9", FromTier: model.GroupTierMain,
		ToTier: model.GroupTierBackup, FromGroupID: "1", ToGroupID: "2", CreatedAt: time.Now().UTC(),
	})
	if err != nil || existed {
		t.Fatalf("begin transition: existed=%v err=%v", existed, err)
	}
	if err := database.MarkGroupTierTransitionUncertain(ctx, transition.ID, "write result unknown"); err != nil {
		t.Fatal(err)
	}
	stored, err := database.GetGroupTierTransitionByKey(ctx, transition.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.GroupTransitionPending || stored.CompletedAt != nil || stored.Error != "write result unknown" {
		t.Fatalf("uncertain transition lost its readback-only reservation: %+v", stored)
	}
	if err := database.AssertNoPendingGroupTransition(ctx, transition.SourceID, transition.KeyID); err == nil {
		t.Fatal("uncertain transition allowed a second group write")
	}
}
