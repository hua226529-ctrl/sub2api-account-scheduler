package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestConsumeAdministratorGrantIsSingleOwnerAndIdempotentForSameStep(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	grantID := "ag1_" + strings.Repeat("a", 64)
	argumentsHash := strings.Repeat("b", 64)
	first, err := database.ConsumeAdministratorGrant(ctx, grantID, 11, 21, "pause_account", argumentsHash)
	if err != nil || !first {
		t.Fatalf("first grant consumption was not recorded: first=%v err=%v", first, err)
	}
	first, err = database.ConsumeAdministratorGrant(ctx, grantID, 11, 21, "pause_account", argumentsHash)
	if err != nil || first {
		t.Fatalf("same durable step was not treated as an idempotent retry: first=%v err=%v", first, err)
	}

	for name, test := range map[string]struct {
		goalID     int64
		stepID     int64
		capability string
		hash       string
	}{
		"other goal":       {12, 21, "pause_account", argumentsHash},
		"other step":       {11, 22, "pause_account", argumentsHash},
		"other capability": {11, 21, "resume_account", argumentsHash},
		"other arguments":  {11, 21, "pause_account", strings.Repeat("c", 64)},
	} {
		_, err := database.ConsumeAdministratorGrant(ctx, grantID, test.goalID, test.stepID, test.capability, test.hash)
		if !errors.Is(err, ErrAdministratorGrantAlreadyConsumed) {
			t.Fatalf("%s reused a consumed grant: %v", name, err)
		}
	}
}

func TestConsumeAdministratorGrantRejectsIncompleteIdentity(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), testSettings())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if _, err := database.ConsumeAdministratorGrant(context.Background(), "ag1_"+strings.Repeat("a", 64),
		0, 1, "pause_account", strings.Repeat("b", 64)); err == nil {
		t.Fatal("grant without a durable goal was accepted")
	}
}
