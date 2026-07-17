package accountcontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

const (
	maxRecoveryBatchSize = 100
	maxRecoveryWorkers   = maxConcurrentAccountMutations
)

func (s *Service) ReconcilePendingAccountMutations(ctx context.Context) error {
	items, err := s.repository.ListPendingAccountMutations(ctx, maxRecoveryBatchSize)
	if err != nil || len(items) == 0 {
		return err
	}
	batches := groupRecoveryItems(items)
	workerCount := min(len(batches), maxRecoveryWorkers)
	jobs := make(chan recoveryBatch)
	var failures []error
	var failuresMu sync.Mutex
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for batch := range jobs {
				for _, mutation := range batch.mutations {
					if ctx.Err() != nil {
						break
					}
					if recoverErr := s.recoverOne(ctx, mutation); recoverErr != nil {
						failuresMu.Lock()
						failures = append(failures, fmt.Errorf("recover account %d mutation %s: %w", mutation.AccountID, mutation.ID, recoverErr))
						failuresMu.Unlock()
					}
				}
			}
		}()
	}
	feedErr := feedRecoveryJobs(ctx, jobs, batches)
	close(jobs)
	workers.Wait()
	if feedErr != nil {
		failures = append(failures, feedErr)
	}
	return errors.Join(failures...)
}

type recoveryBatch struct {
	mutations []Mutation
}

func groupRecoveryItems(items []Mutation) []recoveryBatch {
	indexes := make(map[int64]int)
	batches := make([]recoveryBatch, 0, len(items))
	for _, mutation := range items {
		index, found := indexes[mutation.AccountID]
		if !found {
			index = len(batches)
			indexes[mutation.AccountID] = index
			batches = append(batches, recoveryBatch{})
		}
		batches[index].mutations = append(batches[index].mutations, mutation)
	}
	return batches
}

func feedRecoveryJobs(ctx context.Context, jobs chan<- recoveryBatch, batches []recoveryBatch) error {
	for _, batch := range batches {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobs <- batch:
		}
	}
	return nil
}

func (s *Service) recoverOne(ctx context.Context, mutation Mutation) error {
	release, err := s.locks.acquire(ctx, mutation.AccountID)
	if err != nil {
		return err
	}
	defer release()
	releaseSlot, err := s.acquireSlot(ctx)
	if err != nil {
		return err
	}
	defer releaseSlot()
	current, err := s.repository.FindAccountMutationByIdempotency(ctx, mutation.IdempotencyKey)
	if err != nil {
		return err
	}
	if current == nil || current.Status.Terminal() {
		return nil
	}
	mutation = *current
	intent, err := intentFromWinningMutation(mutation)
	if err != nil {
		return err
	}
	now := s.now()
	if (mutation.Status == StatusPrepared || mutation.Status == StatusValidating) && intentExpired(intent, now) {
		return s.expirePendingMutation(ctx, mutation, now)
	}
	account, err := s.readAccount(ctx, mutation.AccountID)
	if err != nil {
		mutation.Status = StatusUncertain
		mutation.LastErrorCode = "recovery_read_failed"
		mutation.UpdatedAt = s.now()
		return s.repository.UpdateAccountMutation(ctx, mutation)
	}
	control, err := s.loadControl(ctx, mutation.AccountID)
	if err != nil {
		return err
	}
	if mutation.Status == StatusPrepared || mutation.Status == StatusValidating {
		reason, guardErr := s.checkSafety(ctx, intent, account, control, SafetyContext{TelemetryFresh: mutation.TelemetryFresh, CooldownActive: mutation.CooldownActive})
		if guardErr != nil {
			return guardErr
		}
		if reason != BlockNone {
			mutation.Status = StatusBlocked
			mutation.LastErrorCode = string(reason)
			now := s.now()
			mutation.UpdatedAt, mutation.CompletedAt = now, &now
			return s.repository.FinalizeAccountMutation(ctx, Finalization{Mutation: mutation,
				Event: defaultEvent(mutation, StatusBlocked, reason, now), OverrideStatus: OverrideBlocked})
		}
	}
	if accountMatchesIntent(account, intent) {
		after := stateFromAccount(account)
		mutation.After = &after
		mutation.Status = StatusApplied
		now := s.now()
		mutation.UpdatedAt, mutation.CompletedAt = now, &now
		submission := Submission{RevokeOverrideID: mutation.RevokeOverrideID, Event: model.Event{Type: mutation.ReasonCode}}
		control = updateControl(control, mutation, account, submission, now)
		event := defaultEvent(mutation, StatusApplied, BlockNone, now)
		return s.repository.FinalizeAccountMutation(ctx, Finalization{Mutation: mutation, Control: control, Event: event,
			ActivateOverride: mutation.OverrideID != "", OverrideStatus: recoveredOverrideStatus(intent, s.now()), RevokeOverrideID: mutation.RevokeOverrideID,
			RevokeActor: mutation.Actor, RevokeReason: "completed during startup recovery"})
	}
	if intentExpired(intent, s.now()) {
		return s.expirePendingMutation(ctx, mutation, s.now())
	}
	if mutation.Before == nil {
		if mutation.Status != StatusPrepared && mutation.Status != StatusValidating {
			mutation.Status = StatusUncertain
			mutation.LastErrorCode = "recovery_missing_before_state"
			mutation.UpdatedAt = s.now()
			return s.repository.UpdateAccountMutation(ctx, mutation)
		}
		before := stateFromAccount(account)
		mutation.Before = &before
	}
	if !stateMatchesAccount(*mutation.Before, account) || mutation.AttemptCount >= 2 {
		mutation.Status = StatusUncertain
		mutation.LastErrorCode = "recovery_state_diverged"
		mutation.UpdatedAt = s.now()
		return s.repository.UpdateAccountMutation(ctx, mutation)
	}
	reason, guardErr := s.checkSafety(ctx, intent, account, control, SafetyContext{TelemetryFresh: mutation.TelemetryFresh, CooldownActive: mutation.CooldownActive})
	if guardErr != nil {
		return guardErr
	}
	if reason != BlockNone {
		mutation.Status = StatusBlocked
		mutation.LastErrorCode = string(reason)
		now := s.now()
		mutation.UpdatedAt, mutation.CompletedAt = now, &now
		return s.repository.FinalizeAccountMutation(ctx, Finalization{Mutation: mutation, Control: control,
			Event: defaultEvent(mutation, StatusBlocked, reason, now), OverrideStatus: OverrideBlocked})
	}
	mutation.Status = StatusExecuting
	mutation.AttemptCount++
	mutation.UpdatedAt = s.now()
	if err := s.repository.UpdateAccountMutation(ctx, mutation); err != nil {
		return err
	}
	if err := s.executor.write(ctx, intent); err != nil {
		mutation.Status = StatusUncertain
		mutation.LastErrorCode = "recovery_retry_uncertain"
		mutation.UpdatedAt = s.now()
		return s.repository.UpdateAccountMutation(ctx, mutation)
	}
	verified, err := s.readAccount(ctx, mutation.AccountID)
	if err != nil || !accountMatchesIntent(verified, intent) {
		mutation.Status = StatusUncertain
		mutation.LastErrorCode = "recovery_retry_unverified"
		mutation.UpdatedAt = s.now()
		return s.repository.UpdateAccountMutation(ctx, mutation)
	}
	after := stateFromAccount(verified)
	mutation.After = &after
	mutation.Status = StatusApplied
	now = s.now()
	mutation.UpdatedAt, mutation.CompletedAt = now, &now
	submission := Submission{RevokeOverrideID: mutation.RevokeOverrideID, Event: model.Event{Type: mutation.ReasonCode}}
	control = updateControl(control, mutation, verified, submission, now)
	return s.repository.FinalizeAccountMutation(ctx, Finalization{Mutation: mutation, Control: control,
		Event: defaultEvent(mutation, StatusApplied, BlockNone, s.now()), ActivateOverride: mutation.OverrideID != "",
		OverrideStatus: recoveredOverrideStatus(intent, s.now()), RevokeOverrideID: mutation.RevokeOverrideID, RevokeActor: mutation.Actor,
		RevokeReason: "completed during controlled recovery retry"})
}

func (s *Service) expirePendingMutation(ctx context.Context, mutation Mutation, now time.Time) error {
	mutation.Status = StatusExpired
	mutation.LastErrorCode = "override_expired"
	mutation.UpdatedAt, mutation.CompletedAt = now, &now
	return s.repository.FinalizeAccountMutation(ctx, Finalization{Mutation: mutation,
		Event: defaultEvent(mutation, StatusExpired, BlockNone, now), OverrideStatus: OverrideExpired})
}

func intentExpired(intent controlplane.Intent, now time.Time) bool {
	return intent.ExpiresAt != nil && !intent.ExpiresAt.After(now)
}

func recoveredOverrideStatus(intent controlplane.Intent, now time.Time) OverrideStatus {
	if intentExpired(intent, now) {
		return OverrideExpired
	}
	return OverrideActive
}

func intentFromWinningMutation(mutation Mutation) (controlplane.Intent, error) {
	metadata := controlplane.IntentMetadata{
		ID: mutation.WinningIntentID, IdempotencyKey: mutation.WinningIdempotencyKey, Producer: mutation.WinningProducer,
		Authority: mutation.WinningAuthority, Actor: mutation.WinningActor, Reason: mutation.WinningReason,
		EvidenceRefs:  append([]string(nil), mutation.WinningEvidenceRefs...),
		PolicyVersion: mutation.WinningPolicyVersion, SnapshotVersion: mutation.WinningSnapshotVersion,
		CreatedAt: mutation.WinningCreatedAt, ExpiresAt: cloneTime(mutation.WinningExpiresAt),
	}
	if mutation.WinningSchedulable != nil {
		return controlplane.NewAccountSchedulableIntent(metadata, mutation.AccountID, *mutation.WinningSchedulable)
	}
	var value *int
	if mutation.WinningLoadSet {
		value = cloneInt(mutation.WinningLoadFactor)
	}
	return controlplane.NewAccountLoadFactorIntent(metadata, mutation.AccountID, value)
}

func stateMatchesAccount(state AccountState, account model.Account) bool {
	return state.Schedulable == account.Schedulable && sameInt(state.LoadFactor, account.LoadFactor)
}
