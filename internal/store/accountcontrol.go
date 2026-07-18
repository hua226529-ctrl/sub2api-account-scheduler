package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

const overrideColumns = `id,command_id,intent_id,idempotency_key,semantic_signature,account_id,operation,override_kind,
	desired_schedulable,desired_load_factor,desired_load_factor_set,producer,authority,actor,reason,evidence_refs,
	policy_version,snapshot_version,created_at,expires_at,status,mutation_id,revoked_at,revoked_by,revoke_reason,updated_at`

const mutationColumns = `id,command_id,intent_id,idempotency_key,semantic_signature,account_id,operation,
	requested_schedulable,requested_load_factor,requested_load_factor_set,winning_intent_id,winning_idempotency_key,
	winning_producer,winning_authority,winning_actor,winning_reason,winning_evidence_refs,winning_policy_version,
	winning_snapshot_version,winning_created_at,winning_expires_at,winning_schedulable,winning_load_factor,
	winning_load_factor_set,winning_override_kind,producer,authority,actor,reason_code,reason,policy_version,snapshot_version,run_id,goal_id,step_id,expires_at,
	status,attempt_count,before_schedulable,before_load_factor,before_load_factor_set,after_schedulable,
	after_load_factor,after_load_factor_set,last_error_code,override_id,revoke_override_id,telemetry_fresh,
	cooldown_active,created_at,updated_at,completed_at`

// NextActiveAccountOverrideExpiry returns the nearest durable temporary
// override deadline. It is intentionally a single read for the process-local
// expiry worker; account arbitration still expires rows transactionally.
func (s *Store) NextActiveAccountOverrideExpiry(ctx context.Context) (*time.Time, error) {
	var value sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT MIN(expires_at) FROM account_overrides
		WHERE status=? AND expires_at IS NOT NULL`, accountcontrol.OverrideActive).Scan(&value)
	if err != nil {
		return nil, err
	}
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil, nil
	}
	parsed := parseTime(value.String)
	return &parsed, nil
}

// ExpireActiveAccountOverrides atomically marks all due active overrides and
// returns the affected accounts only after commit. It never writes upstream.
func (s *Store) ExpireActiveAccountOverrides(ctx context.Context, now time.Time) ([]int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT account_id FROM account_overrides
		WHERE status=? AND expires_at IS NOT NULL AND expires_at<=? ORDER BY account_id`,
		accountcontrol.OverrideActive, formatTime(now.UTC()))
	if err != nil {
		return nil, err
	}
	accountIDs := make([]int64, 0)
	for rows.Next() {
		var accountID int64
		if err := rows.Scan(&accountID); err != nil {
			rows.Close()
			return nil, err
		}
		accountIDs = append(accountIDs, accountID)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE account_overrides SET status=?,updated_at=?
		WHERE status=? AND expires_at IS NOT NULL AND expires_at<=?`, accountcontrol.OverrideExpired,
		formatTime(now.UTC()), accountcontrol.OverrideActive, formatTime(now.UTC())); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return accountIDs, nil
}

func (s *Store) ListActiveAccountOverrides(ctx context.Context, accountID int64, operation controlplane.Operation, now time.Time) ([]accountcontrol.Override, error) {
	if err := s.expireAccountOverrides(ctx, accountID, operation, now); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+overrideColumns+` FROM account_overrides
		WHERE account_id=? AND operation=? AND status=? AND (expires_at IS NULL OR expires_at>?) ORDER BY created_at,id`,
		accountID, operation.String(), accountcontrol.OverrideActive, formatTime(now))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []accountcontrol.Override
	for rows.Next() {
		item, scanErr := scanOverride(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) HasPendingAccountOverrideTransition(ctx context.Context, accountID int64, operation controlplane.Operation) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM account_overrides
		WHERE account_id=? AND operation=? AND status=?`, accountID, operation.String(), accountcontrol.OverridePending).Scan(&count)
	return count > 0, err
}

func (s *Store) expireAccountOverrides(ctx context.Context, accountID int64, operation controlplane.Operation, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE account_overrides SET status=?,updated_at=?
		WHERE account_id=? AND operation=? AND status=? AND expires_at IS NOT NULL AND expires_at<=?`,
		accountcontrol.OverrideExpired, formatTime(now), accountID, operation.String(), accountcontrol.OverrideActive, formatTime(now))
	return err
}

func (s *Store) FindActiveAccountOverride(ctx context.Context, accountID int64, operation controlplane.Operation,
	authority controlplane.Authority, now time.Time) (*accountcontrol.Override, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+overrideColumns+` FROM account_overrides
		WHERE account_id=? AND operation=? AND authority=? AND status=? AND (expires_at IS NULL OR expires_at>?)
		ORDER BY created_at DESC,id DESC LIMIT 1`, accountID, operation.String(), authority.String(), accountcontrol.OverrideActive, formatTime(now))
	item, err := scanOverride(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (s *Store) GetAccountOverrideRevision(ctx context.Context, accountID int64, operation controlplane.Operation, now time.Time) (string, error) {
	if err := s.expireAccountOverrides(ctx, accountID, operation, now); err != nil {
		return "", err
	}
	var id, status string
	err := s.db.QueryRowContext(ctx, `SELECT id,status FROM account_overrides
		WHERE account_id=? AND operation=? AND status IN (?,?,?) ORDER BY updated_at DESC,id DESC LIMIT 1`,
		accountID, operation.String(), accountcontrol.OverrideActive, accountcontrol.OverrideRevoked, accountcontrol.OverrideExpired).Scan(&id, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return "none", nil
	}
	if err != nil {
		return "", err
	}
	return id + "|" + status, nil
}

func (s *Store) FindAccountMutationByIdempotency(ctx context.Context, key string) (*accountcontrol.Mutation, error) {
	item, err := getMutationByIdempotency(ctx, s.db, key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (s *Store) PrepareAccountMutation(ctx context.Context, mutation accountcontrol.Mutation, override *accountcontrol.Override) (accountcontrol.Mutation, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return accountcontrol.Mutation{}, false, err
	}
	defer tx.Rollback()
	stored, err := getMutationByIdempotency(ctx, tx, mutation.IdempotencyKey)
	if err == nil {
		return stored, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return accountcontrol.Mutation{}, false, err
	}
	if override != nil {
		if err := insertOverride(ctx, tx, *override); err != nil {
			return accountcontrol.Mutation{}, false, err
		}
	}
	if err := insertMutation(ctx, tx, mutation); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			stored, readErr := getMutationByIdempotency(ctx, tx, mutation.IdempotencyKey)
			if readErr == nil {
				return stored, true, nil
			}
		}
		return accountcontrol.Mutation{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return accountcontrol.Mutation{}, false, err
	}
	return mutation, false, nil
}

func (s *Store) UpdateAccountMutation(ctx context.Context, mutation accountcontrol.Mutation) error {
	_, err := s.db.ExecContext(ctx, mutationUpdateSQL(), mutationArguments(mutation)...)
	return err
}

func (s *Store) FinalizeAccountMutation(ctx context.Context, final accountcontrol.Finalization) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, mutationUpdateSQL(), mutationArguments(final.Mutation)...); err != nil {
		return err
	}
	if final.Mutation.OverrideID != "" {
		status := final.OverrideStatus
		if status == "" {
			status = accountcontrol.OverrideFailed
		}
		if _, err := tx.ExecContext(ctx, `UPDATE account_overrides SET status=?,mutation_id=?,updated_at=? WHERE id=?`,
			status, final.Mutation.ID, formatTime(final.Mutation.UpdatedAt), final.Mutation.OverrideID); err != nil {
			return err
		}
	}
	if final.RevokeOverrideID != "" {
		at := final.Mutation.UpdatedAt
		if _, err := tx.ExecContext(ctx, `UPDATE account_overrides SET status=?,revoked_at=?,revoked_by=?,revoke_reason=?,updated_at=?
			WHERE id=? AND status=?`, accountcontrol.OverrideRevoked, formatTime(at), final.RevokeActor, final.RevokeReason,
			formatTime(at), final.RevokeOverrideID, accountcontrol.OverrideActive); err != nil {
			return err
		}
	}
	if final.Event.Type != "" {
		if _, err := insertEvent(ctx, tx, final.Event); err != nil {
			return err
		}
	}
	if final.FlapPolicy != nil && final.Event.Type == "automatic_pause" {
		if err := applyFlapPolicyInMutation(ctx, tx, &final.Control, final.Event, *final.FlapPolicy); err != nil {
			return err
		}
	}
	if final.Control.AccountID > 0 {
		if err := upsertControl(ctx, tx, final.Control); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListPendingAccountMutations(ctx context.Context, limit int) ([]accountcontrol.Mutation, error) {
	if limit <= 0 {
		return []accountcontrol.Mutation{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+mutationColumns+` FROM account_mutations
		WHERE status IN (?,?,?,?,?) ORDER BY account_id,created_at,id LIMIT ?`, accountcontrol.StatusPrepared,
		accountcontrol.StatusValidating, accountcontrol.StatusExecuting, accountcontrol.StatusVerifying, accountcontrol.StatusUncertain, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []accountcontrol.Mutation
	for rows.Next() {
		item, scanErr := scanMutation(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ListAccountMutations(ctx context.Context, accountID int64) ([]accountcontrol.Mutation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+mutationColumns+` FROM account_mutations WHERE account_id=? ORDER BY created_at,id`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []accountcontrol.Mutation
	for rows.Next() {
		item, scanErr := scanMutation(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func insertOverride(ctx context.Context, exec sqlExecer, item accountcontrol.Override) error {
	evidence, _ := json.Marshal(item.EvidenceRefs)
	_, err := exec.ExecContext(ctx, `INSERT INTO account_overrides(`+overrideColumns+`) VALUES(`+placeholders(26)+`)`,
		item.ID, item.CommandID, item.IntentID, item.IdempotencyKey, item.SemanticSignature, item.AccountID, item.Operation.String(),
		item.Kind, nullableBool(item.Schedulable), nullableInt(item.LoadFactor), boolInt(item.LoadFactorSet), item.Producer.String(), item.Authority.String(),
		item.Actor, item.Reason, string(evidence), item.PolicyVersion, item.SnapshotVersion, formatTime(item.CreatedAt), nullableTime(item.ExpiresAt),
		item.Status, item.MutationID, nullableTime(item.RevokedAt), item.RevokedBy, item.RevokeReason, formatTime(item.UpdatedAt))
	return err
}

func insertMutation(ctx context.Context, exec sqlExecer, item accountcontrol.Mutation) error {
	_, err := exec.ExecContext(ctx, `INSERT INTO account_mutations(`+mutationColumns+`) VALUES(`+placeholders(52)+`)`, mutationInsertArguments(item)...)
	return err
}

func mutationInsertArguments(item accountcontrol.Mutation) []any {
	update := mutationArguments(item)
	return append([]any{item.ID}, update[:len(update)-1]...)
}

func mutationUpdateSQL() string {
	return `UPDATE account_mutations SET command_id=?,intent_id=?,idempotency_key=?,semantic_signature=?,account_id=?,operation=?,
		requested_schedulable=?,requested_load_factor=?,requested_load_factor_set=?,winning_intent_id=?,winning_idempotency_key=?,
		winning_producer=?,winning_authority=?,winning_actor=?,winning_reason=?,winning_evidence_refs=?,winning_policy_version=?,
		winning_snapshot_version=?,winning_created_at=?,winning_expires_at=?,winning_schedulable=?,winning_load_factor=?,
		winning_load_factor_set=?,winning_override_kind=?,producer=?,authority=?,actor=?,reason_code=?,reason=?,policy_version=?,snapshot_version=?,run_id=?,goal_id=?,step_id=?,expires_at=?,
		status=?,attempt_count=?,before_schedulable=?,before_load_factor=?,before_load_factor_set=?,after_schedulable=?,after_load_factor=?,
		after_load_factor_set=?,last_error_code=?,override_id=?,revoke_override_id=?,telemetry_fresh=?,cooldown_active=?,created_at=?,updated_at=?,completed_at=? WHERE id=?`
}

func mutationArguments(item accountcontrol.Mutation) []any {
	winningEvidence, _ := json.Marshal(item.WinningEvidenceRefs)
	beforeSched, beforeLoad, beforeLoadSet := stateColumns(item.Before)
	afterSched, afterLoad, afterLoadSet := stateColumns(item.After)
	return []any{
		item.CommandID, item.IntentID, item.IdempotencyKey, item.SemanticSignature, item.AccountID, item.Operation.String(),
		nullableBool(item.RequestedSchedulable), nullableInt(item.RequestedLoadFactor), boolInt(item.RequestedLoadSet),
		item.WinningIntentID, item.WinningIdempotencyKey, item.WinningProducer.String(), item.WinningAuthority.String(),
		item.WinningActor, item.WinningReason, string(winningEvidence), item.WinningPolicyVersion, item.WinningSnapshotVersion,
		formatTime(item.WinningCreatedAt), nullableTime(item.WinningExpiresAt), nullableBool(item.WinningSchedulable),
		nullableInt(item.WinningLoadFactor), boolInt(item.WinningLoadSet), item.WinningOverrideKind, item.Producer.String(), item.Authority.String(), item.Actor,
		item.ReasonCode, item.Reason, item.PolicyVersion, item.SnapshotVersion, item.RunID, item.GoalID, item.StepID,
		nullableTime(item.ExpiresAt), item.Status, item.AttemptCount,
		beforeSched, beforeLoad, beforeLoadSet, afterSched, afterLoad, afterLoadSet, item.LastErrorCode, item.OverrideID,
		item.RevokeOverrideID, boolInt(item.TelemetryFresh), boolInt(item.CooldownActive), formatTime(item.CreatedAt),
		formatTime(item.UpdatedAt), nullableTime(item.CompletedAt), item.ID,
	}
}

func getMutationByIdempotency(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, key string) (accountcontrol.Mutation, error) {
	return scanMutation(query.QueryRowContext(ctx, `SELECT `+mutationColumns+` FROM account_mutations WHERE idempotency_key=?`, key))
}

type scanner interface{ Scan(...any) error }

func scanOverride(row scanner) (accountcontrol.Override, error) {
	var item accountcontrol.Override
	var operation, kind, producer, authority, status, created, updated, evidence string
	var desiredSched, desiredLoad sql.NullInt64
	var loadSet int
	var expires, revoked sql.NullString
	err := row.Scan(&item.ID, &item.CommandID, &item.IntentID, &item.IdempotencyKey, &item.SemanticSignature,
		&item.AccountID, &operation, &kind, &desiredSched, &desiredLoad, &loadSet, &producer, &authority, &item.Actor,
		&item.Reason, &evidence, &item.PolicyVersion, &item.SnapshotVersion, &created, &expires, &status,
		&item.MutationID, &revoked, &item.RevokedBy, &item.RevokeReason, &updated)
	if err != nil {
		return item, err
	}
	item.Operation = controlplane.Operation(operation)
	item.Kind = accountcontrol.OverrideKind(kind)
	item.Producer = controlplane.Producer(producer)
	item.Authority = controlplane.Authority(authority)
	item.Status = accountcontrol.OverrideStatus(status)
	item.Schedulable = nullBoolPointer(desiredSched)
	item.LoadFactor = nullIntPointer(desiredLoad)
	item.LoadFactorSet = loadSet == 1
	item.CreatedAt, item.ExpiresAt, item.RevokedAt, item.UpdatedAt = parseTime(created), parseNullTime(expires), parseNullTime(revoked), parseTime(updated)
	_ = json.Unmarshal([]byte(evidence), &item.EvidenceRefs)
	return item, nil
}

func scanMutation(row scanner) (accountcontrol.Mutation, error) {
	var item accountcontrol.Mutation
	var operation, producer, authority, winningProducer, winningAuthority, status string
	var expires, winningExpires, completed sql.NullString
	var requestedSched, requestedLoad, winningSched, winningLoad sql.NullInt64
	var beforeSched, beforeLoad, afterSched, afterLoad sql.NullInt64
	var requestedLoadSet, winningLoadSet, beforeLoadSet, afterLoadSet, telemetryFresh, cooldownActive int
	var winningCreated, created, updated, winningEvidence string
	err := row.Scan(&item.ID, &item.CommandID, &item.IntentID, &item.IdempotencyKey, &item.SemanticSignature, &item.AccountID,
		&operation, &requestedSched, &requestedLoad, &requestedLoadSet, &item.WinningIntentID, &item.WinningIdempotencyKey,
		&winningProducer, &winningAuthority, &item.WinningActor, &item.WinningReason, &winningEvidence,
		&item.WinningPolicyVersion, &item.WinningSnapshotVersion, &winningCreated, &winningExpires, &winningSched, &winningLoad,
		&winningLoadSet, &item.WinningOverrideKind, &producer, &authority, &item.Actor, &item.ReasonCode, &item.Reason, &item.PolicyVersion,
		&item.SnapshotVersion, &item.RunID, &item.GoalID, &item.StepID, &expires, &status, &item.AttemptCount, &beforeSched, &beforeLoad, &beforeLoadSet,
		&afterSched, &afterLoad, &afterLoadSet, &item.LastErrorCode, &item.OverrideID, &item.RevokeOverrideID,
		&telemetryFresh, &cooldownActive, &created, &updated, &completed)
	if err != nil {
		return item, err
	}
	item.Operation = controlplane.Operation(operation)
	item.Producer, item.Authority = controlplane.Producer(producer), controlplane.Authority(authority)
	item.WinningProducer, item.WinningAuthority = controlplane.Producer(winningProducer), controlplane.Authority(winningAuthority)
	item.Status = accountcontrol.MutationStatus(status)
	item.RequestedSchedulable, item.RequestedLoadFactor = nullBoolPointer(requestedSched), nullIntPointer(requestedLoad)
	item.RequestedLoadSet = requestedLoadSet == 1
	item.WinningSchedulable, item.WinningLoadFactor = nullBoolPointer(winningSched), nullIntPointer(winningLoad)
	item.WinningLoadSet = winningLoadSet == 1
	item.WinningCreatedAt, item.WinningExpiresAt = parseTime(winningCreated), parseNullTime(winningExpires)
	item.ExpiresAt, item.CreatedAt, item.UpdatedAt, item.CompletedAt = parseNullTime(expires), parseTime(created), parseTime(updated), parseNullTime(completed)
	item.Before = scannedState(beforeSched, beforeLoad, beforeLoadSet)
	item.After = scannedState(afterSched, afterLoad, afterLoadSet)
	item.TelemetryFresh, item.CooldownActive = telemetryFresh == 1, cooldownActive == 1
	_ = json.Unmarshal([]byte(winningEvidence), &item.WinningEvidenceRefs)
	return item, nil
}

func stateColumns(state *accountcontrol.AccountState) (any, any, int) {
	if state == nil {
		return nil, nil, 0
	}
	return boolInt(state.Schedulable), nullableInt(state.LoadFactor), boolInt(state.LoadFactor != nil)
}

func scannedState(sched, load sql.NullInt64, loadSet int) *accountcontrol.AccountState {
	if !sched.Valid {
		return nil
	}
	state := &accountcontrol.AccountState{Schedulable: sched.Int64 == 1}
	if loadSet == 1 {
		state.LoadFactor = nullIntPointer(load)
	}
	return state
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func placeholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

func applyFlapPolicyInMutation(ctx context.Context, tx *sql.Tx, control *model.AccountControl, event model.Event, flap model.FlapPolicy) error {
	windowStart := event.CreatedAt.Add(-time.Duration(flap.WindowMinutes) * time.Minute)
	var recent int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='automatic_pause' AND account_id=? AND created_at>=? AND created_at<=?`,
		control.AccountID, formatTime(windowStart), formatTime(event.CreatedAt)).Scan(&recent); err != nil {
		return err
	}
	control.RecentAutomaticPauses = recent
	if !flap.Enabled || control.FlapActive || recent < flap.PauseThreshold {
		return nil
	}
	triggered := event.CreatedAt
	control.FlapActive, control.FlapTriggeredAt, control.FlapRecoveryRequired = true, &triggered, flap.RecoveryThreshold
	details, _ := json.Marshal(map[string]any{"window_minutes": flap.WindowMinutes, "recent_pause_count": recent,
		"pause_threshold": flap.PauseThreshold, "recovery_threshold": flap.RecoveryThreshold})
	_, err := insertEvent(ctx, tx, model.Event{Type: "flap_protection_activated", Severity: "warning",
		MonitorID: event.MonitorID, AccountID: event.AccountID, Message: "账号在滚动窗口内反复暂停，已启用抖动保护",
		BeforeState: "normal_recovery", AfterState: "flap_protected", Details: string(details), Actor: "scheduler", CreatedAt: event.CreatedAt})
	return err
}

func (s *Store) migrateLegacyAccountOverrides(ctx context.Context) error {
	const marker = "account_control_core_a_migrated"
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key=?`, marker).Scan(&value)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read account control migration marker: %w", err)
	}

	rows, err := s.db.QueryContext(ctx, `SELECT account_id,owns_pause,owner,manual_override_until,manual_locked,
		owns_load_factor,expected_load_factor,load_override_until,load_pin_value,load_pin_until,load_pin_owner,load_pin_reason,
		last_decision,updated_at FROM account_controls ORDER BY account_id`)
	if err != nil {
		return fmt.Errorf("read legacy account controls: %w", err)
	}
	type legacyControl struct {
		accountID             int64
		ownsPause             int
		owner                 string
		manualUntil           sql.NullString
		manualLocked          int
		ownsLoadFactor        int
		expectedLoad          sql.NullInt64
		loadOverride          sql.NullString
		pinValue              sql.NullInt64
		pinUntil              sql.NullString
		pinOwner, pinReason   string
		lastDecision, updated string
	}
	var legacy []legacyControl
	for rows.Next() {
		var item legacyControl
		if err := rows.Scan(&item.accountID, &item.ownsPause, &item.owner, &item.manualUntil, &item.manualLocked,
			&item.ownsLoadFactor, &item.expectedLoad, &item.loadOverride, &item.pinValue, &item.pinUntil, &item.pinOwner,
			&item.pinReason, &item.lastDecision, &item.updated); err != nil {
			rows.Close()
			return err
		}
		legacy = append(legacy, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	for _, item := range legacy {
		created := parseTime(item.updated)
		if created.IsZero() {
			created = now
		}
		if item.manualLocked == 1 || (item.ownsPause == 1 && item.owner == "operator") {
			if err := insertLegacyOverride(ctx, tx, item.accountID, controlplane.OperationSetAccountSchedulable,
				controlplane.AuthorityManualHold, false, nil, false, created, nil, "legacy-manual-hold", "migration", "legacy manual pause"); err != nil {
				return err
			}
		}
		if until := parseNullTime(item.manualUntil); until != nil && until.After(now) {
			if err := insertLegacyOverride(ctx, tx, item.accountID, controlplane.OperationSetAccountSchedulable,
				controlplane.AuthorityAdministratorCommand, true, nil, false, created, until, "legacy-manual-resume", "migration", "legacy manual protection"); err != nil {
				return err
			}
		}
		legacyAgentPause := item.ownsPause == 1 && strings.EqualFold(strings.TrimSpace(item.owner), "agent")
		legacyAgentLoad := strings.HasPrefix(strings.ToLower(strings.TrimSpace(item.pinOwner)), "agent") ||
			(item.ownsLoadFactor == 1 && strings.HasPrefix(strings.ToLower(strings.TrimSpace(item.lastDecision)), "agent_"))
		if item.pinValue.Valid && !legacyAgentLoad {
			until := parseNullTime(item.pinUntil)
			authority := controlplane.AuthorityManualHold
			if until != nil {
				authority = controlplane.AuthorityAdministratorCommand
			}
			actor := strings.TrimSpace(item.pinOwner)
			if actor == "" {
				actor = "migration"
			}
			value := int(item.pinValue.Int64)
			if err := insertLegacyOverride(ctx, tx, item.accountID, controlplane.OperationSetAccountLoadFactor,
				authority, false, &value, true, created, until, "legacy-load-pin", actor, item.pinReason); err != nil {
				return err
			}
		} else if until := parseNullTime(item.loadOverride); !legacyAgentLoad && until != nil && until.After(now) && item.expectedLoad.Valid {
			value := int(item.expectedLoad.Int64)
			if err := insertLegacyOverride(ctx, tx, item.accountID, controlplane.OperationSetAccountLoadFactor,
				controlplane.AuthorityAdministratorCommand, false, &value, true, created, until, "legacy-load-override", "migration", "legacy load protection"); err != nil {
				return err
			}
		}
		if legacyAgentPause {
			paused := false
			if err := insertLegacyDisabledOverride(ctx, tx, fmt.Sprintf("legacy-agent-disabled:%d", item.accountID), item.accountID,
				controlplane.OperationSetAccountSchedulable, &paused, nil, false, created, now); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE account_controls SET owns_pause=0,owner='',manual_locked=0,manual_override_until=NULL WHERE account_id=?`, item.accountID); err != nil {
				return err
			}
		}
		if legacyAgentLoad {
			var desired *int
			if item.pinValue.Valid {
				value := int(item.pinValue.Int64)
				desired = &value
			} else if item.expectedLoad.Valid {
				value := int(item.expectedLoad.Int64)
				desired = &value
			}
			if err := insertLegacyDisabledOverride(ctx, tx, fmt.Sprintf("legacy-agent-load-disabled:%d", item.accountID), item.accountID,
				controlplane.OperationSetAccountLoadFactor, nil, desired, desired != nil, created, now); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE account_controls SET owns_load_factor=0,load_override_until=NULL,
				load_pin_value=NULL,load_pin_until=NULL,load_pin_owner='',load_pin_reason='' WHERE account_id=?`, item.accountID); err != nil {
				return err
			}
		}
		if legacyAgentPause || legacyAgentLoad {
			accountID := item.accountID
			if _, err := insertEvent(ctx, tx, model.Event{Type: "legacy_agent_control_disabled", Severity: "warning", AccountID: &accountID,
				Message: "旧版 Agent 永久账号控制未迁移为有效 Override，需要管理员检查", Actor: "migration", CreatedAt: now}); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)`, marker, "1", formatTime(now)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func insertLegacyDisabledOverride(ctx context.Context, tx *sql.Tx, id string, accountID int64, operation controlplane.Operation,
	schedulable *bool, load *int, loadSet bool, created, updated time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO account_overrides(id,command_id,intent_id,idempotency_key,semantic_signature,
		account_id,operation,override_kind,desired_schedulable,desired_load_factor,desired_load_factor_set,producer,authority,actor,reason,
		evidence_refs,policy_version,snapshot_version,created_at,status,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, id, id, id, id, accountID, operation.String(), accountcontrol.OverrideKindLegacy, nullableBool(schedulable), nullableInt(load), boolInt(loadSet),
		controlplane.ProducerAgentOperator.String(), controlplane.AuthorityAutonomousAgent.String(), "migration",
		"legacy permanent agent control disabled", "[]", "", "", formatTime(created), accountcontrol.OverrideLegacyDisabled, formatTime(updated))
	return err
}

func insertLegacyOverride(ctx context.Context, tx *sql.Tx, accountID int64, operation controlplane.Operation,
	authority controlplane.Authority, schedulable bool, load *int, loadSet bool, created time.Time, expires *time.Time,
	prefix, actor, reason string) error {
	id := fmt.Sprintf("%s:%d", prefix, accountID)
	producer := controlplane.ProducerAdminUI
	kind := accountcontrol.OverrideKindTemporary
	if authority == controlplane.AuthorityManualHold {
		kind = accountcontrol.OverrideKindManualHold
	}
	if strings.Contains(prefix, "load-pin") {
		kind = accountcontrol.OverrideKindLoadPin
	}
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO account_overrides(`+overrideColumns+`) VALUES(`+placeholders(26)+`)`,
		id, id, id, id, id, accountID, operation.String(), kind, func() any {
			if operation == controlplane.OperationSetAccountSchedulable {
				return boolInt(schedulable)
			}
			return nil
		}(),
		nullableInt(load), boolInt(loadSet), producer.String(), authority.String(), actor, strings.TrimSpace(reason), "[]", "", "",
		formatTime(created), nullableTime(expires), accountcontrol.OverrideActive, "", nil, "", "", formatTime(created))
	return err
}
