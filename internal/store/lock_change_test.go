package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestLockSynchronizationReturnsOnlyAccountsWhoseStateChanged(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "scheduler.db"), laneTestSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	primary, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{Name: "primary", Provider: "newapi",
		BaseURL: "https://primary.example", NormalizedURL: "https://primary.example", CredentialNonce: []byte{1}, CredentialCiphertext: []byte{2},
		CredentialMode: "password", PauseBelow: 1, ResumeAt: 2, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	backup, err := database.CreateUpstreamSource(ctx, model.UpstreamSource{Name: "backup", Provider: "newapi",
		BaseURL: "https://backup.example", NormalizedURL: "https://backup.example", CredentialNonce: []byte{1}, CredentialCiphertext: []byte{2},
		CredentialMode: "password", PauseBelow: 1, ResumeAt: 2, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := database.SyncBalanceLocksChanged(ctx, primary.ID, []int64{2, 1, 2}, true)
	if err != nil || !equalAccountIDs(changed, []int64{1, 2}) {
		t.Fatalf("balance lock creation changes=%#v err=%v", changed, err)
	}
	changed, err = database.SyncBalanceLocksChanged(ctx, primary.ID, []int64{1, 2}, true)
	if err != nil || len(changed) != 0 {
		t.Fatalf("unchanged balance locks triggered accounts=%#v err=%v", changed, err)
	}
	changed, err = database.SyncBalanceLocksChanged(ctx, primary.ID, nil, false)
	if err != nil || !equalAccountIDs(changed, []int64{1, 2}) {
		t.Fatalf("balance lock release changes=%#v err=%v", changed, err)
	}

	locks := []model.CostLock{{SourceID: primary.ID, AccountID: 1, Pool: "primary", RateMultiplier: 1},
		{SourceID: backup.ID, AccountID: 2, Pool: "backup", RateMultiplier: 2}}
	changed, err = database.SyncCostLocksChanged(ctx, locks)
	if err != nil || !equalAccountIDs(changed, []int64{1, 2}) {
		t.Fatalf("cost lock creation changes=%#v err=%v", changed, err)
	}
	changed, err = database.SyncCostLocksChanged(ctx, locks)
	if err != nil || len(changed) != 0 {
		t.Fatalf("unchanged cost locks triggered accounts=%#v err=%v", changed, err)
	}
	locks[0].RateMultiplier = 1.5
	changed, err = database.SyncCostLocksChanged(ctx, locks[:1])
	if err != nil || !equalAccountIDs(changed, []int64{1, 2}) {
		t.Fatalf("cost lock update/release changes=%#v err=%v", changed, err)
	}
}

func equalAccountIDs(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}
