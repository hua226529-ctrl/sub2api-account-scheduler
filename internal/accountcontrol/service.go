package accountcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

var (
	errAccountNotFound      = errors.New("account not found")
	errUnsupportedOperation = errors.New("unsupported account mutation operation")
)

const maxConcurrentAccountMutations = 4

type Option func(*Service)

func WithClock(clock func() time.Time) Option {
	return func(service *Service) {
		if clock != nil {
			service.clock = clock
		}
	}
}

func WithIDGenerator(generator func() (string, error)) Option {
	return func(service *Service) {
		if generator != nil {
			service.idGenerator = generator
		}
	}
}

type Service struct {
	repository  Repository
	transport   Transport
	executor    executor
	locks       *accountLocks
	candidates  *candidateQueue
	slots       chan struct{}
	clock       func() time.Time
	idGenerator func() (string, error)
}

func New(repository Repository, transport Transport, options ...Option) *Service {
	service := &Service{
		repository: repository, transport: transport, executor: executor{transport: transport},
		locks: newAccountLocks(), candidates: newCandidateQueue(), slots: make(chan struct{}, maxConcurrentAccountMutations),
		clock: func() time.Time { return time.Now().UTC() }, idGenerator: NewCommandID,
	}
	for _, option := range options {
		if option != nil {
			option(service)
		}
	}
	return service
}

func (s *Service) now() time.Time { return s.clock().UTC() }

func (s *Service) FindMutation(ctx context.Context, idempotencyKey string) (*Mutation, error) {
	return s.repository.FindAccountMutationByIdempotency(ctx, strings.TrimSpace(idempotencyKey))
}

func (s *Service) LookupResult(ctx context.Context, idempotencyKey, semanticSignature string) (*Result, error) {
	existing, err := s.repository.FindAccountMutationByIdempotency(ctx, strings.TrimSpace(idempotencyKey))
	if err != nil || existing == nil {
		return nil, err
	}
	if existing.SemanticSignature != strings.TrimSpace(semanticSignature) {
		return nil, &IdempotencyConflictError{Key: idempotencyKey}
	}
	result, replayErr := replayMutation(*existing)
	return &result, replayErr
}

func (s *Service) FindActiveOverride(ctx context.Context, accountID int64, operation controlplane.Operation,
	authority controlplane.Authority) (*Override, error) {
	return s.repository.FindActiveAccountOverride(ctx, accountID, operation, authority, s.now())
}

func (s *Service) ArbitrationRevision(ctx context.Context, accountID int64, operation controlplane.Operation) (string, error) {
	return s.repository.GetAccountOverrideRevision(ctx, accountID, operation, s.now())
}

func (s *Service) Submit(ctx context.Context, submission Submission) (result Result, submitErr error) {
	if s == nil || s.repository == nil || s.transport == nil {
		return Result{}, errors.New("account control service is not configured")
	}
	if err := ValidateCommandID(submission.CommandID); err != nil {
		return Result{}, err
	}
	if err := validateAccountIntent(submission.Intent); err != nil {
		return Result{}, err
	}
	// Caller barrier (when required) -> queue mutex (released here) -> account
	// lock -> semaphore -> short Store transaction. Network calls never overlap a
	// Store transaction, and AccountControl never acquires a caller business lock.
	queued, finishCandidate := s.candidates.add(submission)
	accountID, _ := submission.Intent.Resource.AccountID()
	release, err := s.locks.acquire(ctx, accountID)
	if err != nil {
		finishCandidate(false)
		return Result{}, err
	}
	defer release()
	// An applied candidate remains visible only while commands that overlapped
	// it are still queued. This makes arbitration independent of which waiter
	// acquires the account lock first without creating durable in-memory state.
	defer func() { finishCandidate(retainQueuedCandidate(result.Status)) }()
	releaseSlot, err := s.acquireSlot(ctx)
	if err != nil {
		return Result{}, err
	}
	defer releaseSlot()
	result, submitErr = s.submitLocked(ctx, submission, queued)
	return result, submitErr
}

func retainQueuedCandidate(status MutationStatus) bool {
	switch status {
	case StatusApplied, StatusAppliedNoop, StatusExecuting, StatusVerifying, StatusUncertain:
		return true
	default:
		return false
	}
}

func (s *Service) acquireSlot(ctx context.Context) (func(), error) {
	select {
	case s.slots <- struct{}{}:
		return func() { <-s.slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Service) submitLocked(ctx context.Context, submission Submission, queued queuedCandidate) (Result, error) {
	now := s.now()
	accountID, _ := submission.Intent.Resource.AccountID()
	requestKey := strings.TrimSpace(submission.RequestIdempotencyKey)
	if requestKey == "" {
		requestKey = submission.Intent.IdempotencyKey
	}
	requestSignature := strings.TrimSpace(submission.RequestSemanticSignature)
	if requestSignature == "" {
		var signatureErr error
		requestSignature, signatureErr = controlplane.SemanticSignature(submission.Intent)
		if signatureErr != nil {
			return Result{}, signatureErr
		}
	}
	if existing, lookupErr := s.repository.FindAccountMutationByIdempotency(ctx, requestKey); lookupErr != nil {
		return Result{}, lookupErr
	} else if existing != nil {
		if existing.SemanticSignature != requestSignature {
			return Result{}, &IdempotencyConflictError{Key: requestKey}
		}
		return replayMutation(*existing)
	}
	candidates, overrideKinds, err := s.activeCandidates(ctx, submission, queued, now)
	if err != nil {
		return Result{}, err
	}
	winner, status := submissionDecision(now, candidates, submission.Intent)
	winningIntent := submission.Intent
	if winner != nil {
		winningIntent = *winner
	}

	mutationID, err := s.idGenerator()
	if err != nil {
		return Result{}, fmt.Errorf("generate mutation id: %w", err)
	}
	mutationID = "mutation-" + strings.TrimPrefix(mutationID, "cmd-")
	mutation, err := newMutation(mutationID, submission, winningIntent, overrideKinds[winningIntent.ID], now)
	if err != nil {
		return Result{}, err
	}
	var pendingOverride *Override
	if submission.PersistOverride {
		overrideID, idErr := s.idGenerator()
		if idErr != nil {
			return Result{}, fmt.Errorf("generate override id: %w", idErr)
		}
		override, overrideErr := OverrideFromIntent("override-"+strings.TrimPrefix(overrideID, "cmd-"), submission.CommandID, submission.Intent)
		if overrideErr != nil {
			return Result{}, overrideErr
		}
		override.MutationID = mutation.ID
		override.Kind = overrideKindForSubmission(submission)
		pendingOverride = &override
		mutation.OverrideID = override.ID
	}

	stored, replay, err := s.repository.PrepareAccountMutation(ctx, mutation, pendingOverride)
	if err != nil {
		return Result{}, err
	}
	if replay {
		if stored.SemanticSignature != mutation.SemanticSignature {
			return Result{}, &IdempotencyConflictError{Key: mutation.IdempotencyKey}
		}
		return replayMutation(stored)
	}
	if status == StatusExpired || status == StatusSuperseded {
		return s.finishWithoutWrite(ctx, submission, mutation, status, BlockNone, nil)
	}
	winner = &winningIntent

	mutation.Status = StatusValidating
	mutation.UpdatedAt = now
	if err := s.repository.UpdateAccountMutation(ctx, mutation); err != nil {
		return Result{}, err
	}
	account, err := s.readAccount(ctx, accountID)
	if err != nil {
		if errors.Is(err, errAccountNotFound) {
			return s.finishWithoutWrite(ctx, submission, mutation, StatusBlocked, BlockAccountNotFound, err)
		}
		return s.finishWithoutWrite(ctx, submission, mutation, StatusFailed, BlockNone, err)
	}
	control, err := s.loadControl(ctx, accountID)
	if err != nil {
		return s.finishWithoutWrite(ctx, submission, mutation, StatusFailed, BlockNone, err)
	}
	before := stateFromAccount(account)
	mutation.Before = &before
	mutation.UpdatedAt = s.now()
	if err := s.repository.UpdateAccountMutation(ctx, mutation); err != nil {
		return Result{}, err
	}

	blockReason, err := s.checkSafety(ctx, *winner, account, control, submission.Safety)
	if err != nil {
		return s.finishWithoutWrite(ctx, submission, mutation, StatusFailed, blockReason, err)
	}
	if blockReason != BlockNone {
		return s.finishWithoutWrite(ctx, submission, mutation, StatusBlocked, blockReason, nil)
	}
	setWinningMutation(&mutation, *winner, overrideKinds[winner.ID])
	mutation.UpdatedAt = s.now()
	if err := s.repository.UpdateAccountMutation(ctx, mutation); err != nil {
		return Result{}, err
	}

	if accountMatchesIntent(account, *winner) {
		after := stateFromAccount(account)
		mutation.After = &after
		mutation.Status = StatusAppliedNoop
		return s.finalize(ctx, submission, mutation, pendingOverride, control, account, true)
	}

	mutation.Status = StatusExecuting
	mutation.AttemptCount++
	mutation.UpdatedAt = s.now()
	if err := s.repository.UpdateAccountMutation(ctx, mutation); err != nil {
		return Result{}, err
	}
	writeErr := s.executor.write(ctx, *winner)
	if writeErr != nil {
		return s.resolveWriteError(ctx, submission, mutation, pendingOverride, control, account, *winner, writeErr)
	}
	mutation.Status = StatusVerifying
	mutation.UpdatedAt = s.now()
	if err := s.repository.UpdateAccountMutation(ctx, mutation); err != nil {
		return resultFromMutation(mutation, false), fmt.Errorf("persist verifying account mutation: %w", err)
	}
	afterAccount, err := s.readAccount(ctx, mutation.AccountID)
	if err != nil {
		return s.markUncertain(ctx, mutation, "post_write_read_failed", err)
	}
	if !accountMatchesIntent(afterAccount, *winner) {
		return s.markUncertain(ctx, mutation, "post_write_mismatch", errors.New("verified upstream state does not match desired state"))
	}
	after := stateFromAccount(afterAccount)
	mutation.After = &after
	mutation.Status = StatusApplied
	return s.finalize(ctx, submission, mutation, pendingOverride, control, afterAccount, false)
}

func (s *Service) activeCandidates(ctx context.Context, submission Submission, queued queuedCandidate, now time.Time) ([]controlplane.Intent, map[string]OverrideKind, error) {
	submitted := submission.Intent
	accountID, _ := submitted.Resource.AccountID()
	overrides, err := s.repository.ListActiveAccountOverrides(ctx, accountID, submitted.Operation, now)
	if err != nil {
		return nil, nil, err
	}
	candidates := make([]controlplane.Intent, 0, len(overrides)+1)
	kinds := make(map[string]OverrideKind, len(overrides)+1)
	for _, override := range overrides {
		if override.ID == submission.RevokeOverrideID {
			continue
		}
		intent, conversionErr := override.Intent()
		if conversionErr != nil {
			return nil, nil, fmt.Errorf("load override %s: %w", override.ID, conversionErr)
		}
		candidates = append(candidates, intent)
		kinds[intent.ID] = override.Kind
	}
	if submission.PersistOverride {
		kinds[submitted.ID] = overrideKindForSubmission(submission)
	}
	for _, candidate := range s.candidates.competing(queued) {
		candidates = append(candidates, candidate.intent)
		kinds[candidate.intent.ID] = candidate.kind
	}
	return append(candidates, submitted), kinds, nil
}

func (s *Service) resolveWriteError(ctx context.Context, submission Submission, mutation Mutation, pendingOverride *Override,
	control model.AccountControl, beforeAccount model.Account, winner controlplane.Intent, writeErr error) (Result, error) {
	if !ambiguousWriteError(writeErr) {
		return s.finishWithoutWrite(ctx, submission, mutation, StatusFailed, BlockNone, writeErr)
	}
	readback, readErr := s.readAccount(ctx, mutation.AccountID)
	if readErr != nil {
		return s.markUncertain(ctx, mutation, "write_and_readback_uncertain", writeErr)
	}
	if accountMatchesIntent(readback, winner) {
		after := stateFromAccount(readback)
		mutation.After = &after
		mutation.Status = StatusApplied
		return s.finalize(ctx, submission, mutation, pendingOverride, control, readback, false)
	}
	if sameAccountState(readback, beforeAccount) && mutation.AttemptCount < 2 {
		mutation.Status = StatusExecuting
		mutation.AttemptCount++
		mutation.LastErrorCode = "controlled_retry"
		mutation.UpdatedAt = s.now()
		if err := s.repository.UpdateAccountMutation(ctx, mutation); err != nil {
			return Result{}, err
		}
		if err := s.executor.write(ctx, winner); err != nil {
			readback, readErr := s.readAccount(ctx, mutation.AccountID)
			if readErr == nil && accountMatchesIntent(readback, winner) {
				after := stateFromAccount(readback)
				mutation.After = &after
				mutation.Status = StatusApplied
				return s.finalize(ctx, submission, mutation, pendingOverride, control, readback, false)
			}
			return s.markUncertain(ctx, mutation, "controlled_retry_uncertain", err)
		}
		verified, err := s.readAccount(ctx, mutation.AccountID)
		if err != nil || !accountMatchesIntent(verified, winner) {
			return s.markUncertain(ctx, mutation, "controlled_retry_unverified", err)
		}
		after := stateFromAccount(verified)
		mutation.After = &after
		mutation.Status = StatusApplied
		return s.finalize(ctx, submission, mutation, pendingOverride, control, verified, false)
	}
	return s.markUncertain(ctx, mutation, "write_state_diverged", writeErr)
}

func (s *Service) finishWithoutWrite(ctx context.Context, submission Submission, mutation Mutation,
	status MutationStatus, reason BlockReason, cause error) (Result, error) {
	mutation.Status = status
	mutation.LastErrorCode = string(reason)
	if mutation.LastErrorCode == "" && cause != nil {
		mutation.LastErrorCode = "mutation_failed"
	}
	now := s.now()
	mutation.UpdatedAt, mutation.CompletedAt = now, &now
	overrideStatus := OverrideFailed
	switch status {
	case StatusBlocked:
		overrideStatus = OverrideBlocked
	case StatusExpired:
		overrideStatus = OverrideExpired
	case StatusSuperseded:
		overrideStatus = OverrideSuperseded
	}
	event := submission.Event
	if event.Type == "" {
		event = defaultEvent(mutation, status, reason, now)
	}
	finalErr := s.repository.FinalizeAccountMutation(ctx, Finalization{
		Mutation: mutation, Event: event, OverrideStatus: overrideStatus,
	})
	if finalErr != nil {
		return Result{}, finalErr
	}
	result := resultFromMutation(mutation, false)
	if status == StatusBlocked {
		return result, &BlockedError{Result: result}
	}
	if cause != nil {
		return result, &MutationStateError{Result: result, Cause: cause}
	}
	return result, nil
}

func (s *Service) markUncertain(ctx context.Context, mutation Mutation, code string, cause error) (Result, error) {
	mutation.Status = StatusUncertain
	mutation.LastErrorCode = code
	mutation.UpdatedAt = s.now()
	if err := s.repository.UpdateAccountMutation(ctx, mutation); err != nil {
		return Result{}, err
	}
	result := resultFromMutation(mutation, false)
	if cause == nil {
		cause = errors.New(code)
	}
	return result, &MutationStateError{Result: result, Cause: cause}
}

func (s *Service) finalize(ctx context.Context, submission Submission, mutation Mutation, pendingOverride *Override,
	control model.AccountControl, verified model.Account, noop bool) (Result, error) {
	now := s.now()
	if noop {
		mutation.Status = StatusAppliedNoop
	}
	mutation.UpdatedAt, mutation.CompletedAt = now, &now
	control = updateControl(control, mutation, verified, submission, now)
	event := submission.Event
	if event.Type == "" {
		event = defaultEvent(mutation, mutation.Status, BlockNone, now)
	}
	if mutation.Status == StatusAppliedNoop {
		event.Type = "account_mutation_applied_noop"
		event.Severity = "info"
		event.Message = "账号实际状态已满足仲裁结果，未调用写接口"
	}
	event.AccountID = &mutation.AccountID
	event.Actor = mutation.Actor
	event.CreatedAt = now
	event.BeforeState = formatState(mutation.Before)
	event.AfterState = formatState(mutation.After)
	details, _ := json.Marshal(map[string]any{
		"mutation_id": mutation.ID, "intent_id": mutation.IntentID, "authority": mutation.WinningAuthority,
		"status": mutation.Status,
	})
	event.Details = string(details)
	finalization := Finalization{
		Mutation: mutation, Control: control, Event: event, ActivateOverride: pendingOverride != nil,
		OverrideStatus: OverrideActive, RevokeOverrideID: submission.RevokeOverrideID,
		RevokeActor: mutation.Actor, RevokeReason: "superseded by explicit account control command", FlapPolicy: submission.FlapPolicy,
	}
	if err := s.repository.FinalizeAccountMutation(ctx, finalization); err != nil {
		mutation.Status = StatusVerifying
		mutation.CompletedAt = nil
		mutation.LastErrorCode = "local_commit_failed"
		_ = s.repository.UpdateAccountMutation(ctx, mutation)
		result := resultFromMutation(mutation, false)
		return result, &MutationStateError{Result: result, Cause: fmt.Errorf("account mutation applied upstream but local commit is uncertain: %w", err)}
	}
	return resultFromMutation(mutation, false), nil
}

func validateAccountIntent(intent controlplane.Intent) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	if _, ok := intent.Resource.AccountID(); !ok {
		return errors.New("account mutation requires an account resource")
	}
	if intent.Operation != controlplane.OperationSetAccountSchedulable && intent.Operation != controlplane.OperationSetAccountLoadFactor {
		return errUnsupportedOperation
	}
	return nil
}

func newMutation(id string, submission Submission, winner controlplane.Intent, winnerKind OverrideKind, now time.Time) (Mutation, error) {
	signature, err := controlplane.SemanticSignature(submission.Intent)
	if err != nil {
		return Mutation{}, err
	}
	accountID, _ := submission.Intent.Resource.AccountID()
	idempotencyKey := strings.TrimSpace(submission.RequestIdempotencyKey)
	if idempotencyKey == "" {
		idempotencyKey = submission.Intent.IdempotencyKey
	}
	requestSignature := strings.TrimSpace(submission.RequestSemanticSignature)
	if requestSignature == "" {
		requestSignature = signature
	}
	mutation := Mutation{
		ID: id, CommandID: strings.TrimSpace(submission.CommandID), IntentID: submission.Intent.ID,
		IdempotencyKey: idempotencyKey, SemanticSignature: requestSignature, AccountID: accountID,
		Operation: submission.Intent.Operation, Producer: submission.Intent.Producer, Authority: submission.Intent.Authority,
		Actor: submission.Intent.Actor, ReasonCode: submission.Event.Type, Reason: submission.Intent.Reason, PolicyVersion: submission.Intent.PolicyVersion,
		SnapshotVersion: submission.Intent.SnapshotVersion, ExpiresAt: cloneTime(submission.Intent.ExpiresAt),
		Status: StatusPrepared, CreatedAt: submission.Intent.CreatedAt, UpdatedAt: now, RevokeOverrideID: submission.RevokeOverrideID,
		TelemetryFresh: submission.Safety.TelemetryFresh, CooldownActive: submission.Safety.CooldownActive,
	}
	setRequestedMutation(&mutation, submission.Intent)
	setWinningMutation(&mutation, winner, winnerKind)
	return mutation, nil
}

func setRequestedMutation(mutation *Mutation, intent controlplane.Intent) {
	if value, ok := intent.DesiredState.Schedulable(); ok {
		mutation.RequestedSchedulable = &value
		return
	}
	value, configured, ok := intent.DesiredState.LoadFactor()
	if ok {
		mutation.RequestedLoadSet = configured
		if configured {
			mutation.RequestedLoadFactor = &value
		}
	}
}

func setWinningMutation(mutation *Mutation, intent controlplane.Intent, kind OverrideKind) {
	mutation.WinningIntentID = intent.ID
	mutation.WinningIdempotencyKey = intent.IdempotencyKey
	mutation.WinningProducer = intent.Producer
	mutation.WinningAuthority = intent.Authority
	mutation.WinningActor = intent.Actor
	mutation.WinningReason = intent.Reason
	mutation.WinningEvidenceRefs = append([]string(nil), intent.EvidenceRefs...)
	mutation.WinningPolicyVersion = intent.PolicyVersion
	mutation.WinningSnapshotVersion = intent.SnapshotVersion
	mutation.WinningCreatedAt = intent.CreatedAt
	mutation.WinningExpiresAt = cloneTime(intent.ExpiresAt)
	mutation.WinningOverrideKind = kind
	mutation.WinningSchedulable = nil
	mutation.WinningLoadFactor = nil
	mutation.WinningLoadSet = false
	if value, ok := intent.DesiredState.Schedulable(); ok {
		mutation.WinningSchedulable = &value
		return
	}
	value, configured, ok := intent.DesiredState.LoadFactor()
	if ok {
		mutation.WinningLoadSet = configured
		if configured {
			mutation.WinningLoadFactor = &value
		}
	}
}

func overrideKindForSubmission(submission Submission) OverrideKind {
	if submission.OverrideKind != "" {
		return submission.OverrideKind
	}
	if submission.Intent.Authority == controlplane.AuthorityManualHold {
		return OverrideKindManualHold
	}
	return OverrideKindTemporary
}

func submissionDecision(now time.Time, candidates []controlplane.Intent, submitted controlplane.Intent) (*controlplane.Intent, MutationStatus) {
	results := controlplane.Arbitrate(now, candidates)
	for _, result := range results {
		if result.ConflictKey != submitted.ConflictKey() {
			continue
		}
		if result.Winner != nil {
			winner := cloneControlplaneIntent(*result.Winner)
			if winner.ID == submitted.ID {
				return &winner, StatusPrepared
			}
			return &winner, StatusSuperseded
		}
		for _, ignored := range result.Ignored {
			if ignored.Intent.ID == submitted.ID && ignored.ReasonCode == controlplane.ReasonExpired {
				return nil, StatusExpired
			}
		}
	}
	return nil, StatusSuperseded
}

func (s *Service) readAccount(ctx context.Context, accountID int64) (model.Account, error) {
	accounts, err := s.transport.ListAccounts(ctx)
	if err != nil {
		return model.Account{}, err
	}
	account, ok := findAccount(accounts, accountID)
	if !ok {
		return model.Account{}, errAccountNotFound
	}
	return account, nil
}

func (s *Service) loadControl(ctx context.Context, accountID int64) (model.AccountControl, error) {
	control, err := s.repository.GetControl(ctx, accountID)
	if err != nil {
		return model.AccountControl{}, err
	}
	balanceLock, err := s.repository.GetActiveBalanceLock(ctx, accountID)
	if err != nil {
		return model.AccountControl{}, err
	}
	control.BalanceLocked = balanceLock != nil
	control.BalanceSourceID = nil
	if balanceLock != nil {
		control.BalanceSourceID = &balanceLock.SourceID
	}
	costLock, err := s.repository.GetActiveCostLock(ctx, accountID)
	if err != nil {
		return model.AccountControl{}, err
	}
	control.CostLocked = costLock != nil
	control.CostSourceID = nil
	control.CostPool = ""
	if costLock != nil {
		control.CostSourceID = &costLock.SourceID
		control.CostPool = costLock.Pool
	}
	return control, nil
}
