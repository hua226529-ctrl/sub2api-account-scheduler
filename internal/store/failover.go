package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

var ErrGroupTransitionIdempotencyConflict = errors.New("group transition idempotency conflict")

const groupTierTransitionColumns = `id,idempotency_key,source_id,key_id,from_tier,to_tier,from_group_id,to_group_id,status,actor,producer,authority,reason,evidence,snapshot_version,trigger,packet_id,run_id,error,attempt_count,before_state,verified_after_state,uncertain,manual,dry_run,created_at,completed_at`

const groupFailoverStateColumns = `source_id,key_id,current_tier,observed_group_id,previous_tier,previous_stable_tier,previous_group_id,frozen,freeze_reason,last_error,manual_hold_until,manual_override_until,cooldown_until,return_blocked_until,recovery_since,last_switch_at,last_transition_at,verification_started_at,healthy_since,recovery_healthy_count,last_confirmed_at,validation_status,validation_mode,validation_transition_id,validation_from_tier,validation_target_tier,validation_from_group_id,validation_target_group_id,switch_requested_at,switch_verified_at,validation_not_before,evidence_deadline,monitor_watermark,traffic_watermark,monitor_evidence_cursor,traffic_evidence_cursor,active_probe_attempts,successful_evidence_count,failed_evidence_count,last_evidence_id,last_evidence_source,last_evidence_reason,last_evidence_at,updated_at`

const upsertGroupFailoverStateSQL = `INSERT INTO upstream_group_failover_states(` + groupFailoverStateColumns + `)
	VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT(source_id,key_id) DO UPDATE SET current_tier=excluded.current_tier,observed_group_id=excluded.observed_group_id,previous_tier=excluded.previous_tier,previous_stable_tier=excluded.previous_stable_tier,previous_group_id=excluded.previous_group_id,frozen=excluded.frozen,freeze_reason=excluded.freeze_reason,last_error=excluded.last_error,manual_hold_until=excluded.manual_hold_until,manual_override_until=excluded.manual_override_until,cooldown_until=excluded.cooldown_until,return_blocked_until=excluded.return_blocked_until,recovery_since=excluded.recovery_since,last_switch_at=excluded.last_switch_at,last_transition_at=excluded.last_transition_at,verification_started_at=excluded.verification_started_at,healthy_since=excluded.healthy_since,recovery_healthy_count=excluded.recovery_healthy_count,last_confirmed_at=excluded.last_confirmed_at,validation_status=excluded.validation_status,validation_mode=excluded.validation_mode,validation_transition_id=excluded.validation_transition_id,validation_from_tier=excluded.validation_from_tier,validation_target_tier=excluded.validation_target_tier,validation_from_group_id=excluded.validation_from_group_id,validation_target_group_id=excluded.validation_target_group_id,switch_requested_at=excluded.switch_requested_at,switch_verified_at=excluded.switch_verified_at,validation_not_before=excluded.validation_not_before,evidence_deadline=excluded.evidence_deadline,monitor_watermark=excluded.monitor_watermark,traffic_watermark=excluded.traffic_watermark,monitor_evidence_cursor=excluded.monitor_evidence_cursor,traffic_evidence_cursor=excluded.traffic_evidence_cursor,active_probe_attempts=excluded.active_probe_attempts,successful_evidence_count=excluded.successful_evidence_count,failed_evidence_count=excluded.failed_evidence_count,last_evidence_id=excluded.last_evidence_id,last_evidence_source=excluded.last_evidence_source,last_evidence_reason=excluded.last_evidence_reason,last_evidence_at=excluded.last_evidence_at,updated_at=excluded.updated_at`

func (s *Store) ListGroupFailoverPolicies(ctx context.Context, sourceID int64) ([]model.GroupFailoverPolicy, error) {
	query := `SELECT source_id,key_id,key_name,key_hint,enabled,main_enabled,backup_enabled,emergency_enabled,main_group_id,backup_group_id,emergency_group_id,pool,version,confirmed_version,confirmed_at,confirmed_by,created_at,updated_at FROM upstream_group_failover_policies`
	args := []any{}
	if sourceID > 0 {
		query += ` WHERE source_id=?`
		args = append(args, sourceID)
	}
	query += ` ORDER BY source_id,key_name,key_id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	items := make([]model.GroupFailoverPolicy, 0)
	for rows.Next() {
		item, scanErr := scanGroupFailoverPolicy(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		items = append(items, item)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range items {
		accounts, err := s.listGroupFailoverAccounts(ctx, items[i].SourceID, items[i].KeyID)
		if err != nil {
			return nil, err
		}
		items[i].AccountIDs = accounts
		state, err := s.GetGroupFailoverState(ctx, items[i].SourceID, items[i].KeyID)
		if err != nil {
			return nil, err
		}
		items[i].State = state
	}
	return items, nil
}

func (s *Store) GetGroupFailoverPolicy(ctx context.Context, sourceID int64, keyID string) (model.GroupFailoverPolicy, error) {
	row := s.db.QueryRowContext(ctx, `SELECT source_id,key_id,key_name,key_hint,enabled,main_enabled,backup_enabled,emergency_enabled,main_group_id,backup_group_id,emergency_group_id,pool,version,confirmed_version,confirmed_at,confirmed_by,created_at,updated_at FROM upstream_group_failover_policies WHERE source_id=? AND key_id=?`, sourceID, strings.TrimSpace(keyID))
	item, err := scanGroupFailoverPolicy(row)
	if errors.Is(err, sql.ErrNoRows) {
		return model.GroupFailoverPolicy{}, errors.New("三级分组策略不存在")
	}
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	item.AccountIDs, err = s.listGroupFailoverAccounts(ctx, sourceID, item.KeyID)
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	item.State, err = s.GetGroupFailoverState(ctx, sourceID, item.KeyID)
	return item, err
}

func scanGroupFailoverPolicy(row rowScanner) (model.GroupFailoverPolicy, error) {
	var item model.GroupFailoverPolicy
	var enabled, mainEnabled, backupEnabled, emergencyEnabled int
	var confirmedAt sql.NullString
	var createdAt, updatedAt string
	err := row.Scan(&item.SourceID, &item.KeyID, &item.KeyName, &item.KeyHint, &enabled, &mainEnabled, &backupEnabled, &emergencyEnabled, &item.MainGroupID, &item.BackupGroupID, &item.EmergencyGroupID, &item.Pool, &item.Version, &item.ConfirmedVersion, &confirmedAt, &item.ConfirmedBy, &createdAt, &updatedAt)
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	item.Enabled = enabled == 1
	item.MainEnabled = mainEnabled == 1
	item.BackupEnabled = backupEnabled == 1
	item.EmergencyEnabled = emergencyEnabled == 1
	item.Confirmed = item.ConfirmedVersion > 0 && item.ConfirmedVersion == item.Version
	item.ConfirmedAt = parseNullTime(confirmedAt)
	item.CreatedAt = parseTime(createdAt)
	item.UpdatedAt = parseTime(updatedAt)
	return item, nil
}

func (s *Store) SaveGroupFailoverPolicy(ctx context.Context, policy model.GroupFailoverPolicy) (model.GroupFailoverPolicy, error) {
	policy.KeyID = strings.TrimSpace(policy.KeyID)
	policy.MainGroupID = strings.TrimSpace(policy.MainGroupID)
	policy.BackupGroupID = strings.TrimSpace(policy.BackupGroupID)
	policy.EmergencyGroupID = strings.TrimSpace(policy.EmergencyGroupID)
	if !policy.MainEnabled && !policy.BackupEnabled && !policy.EmergencyEnabled {
		policy.MainEnabled, policy.BackupEnabled, policy.EmergencyEnabled = true, true, true
	}
	policy.AccountIDs = uniqueSortedInt64(policy.AccountIDs)
	now := time.Now().UTC()

	existing, getErr := s.GetGroupFailoverPolicy(ctx, policy.SourceID, policy.KeyID)
	if getErr != nil && getErr.Error() != "三级分组策略不存在" {
		return model.GroupFailoverPolicy{}, getErr
	}
	exists := getErr == nil
	changed := !exists || !sameGroupFailoverPolicy(existing, policy)
	version := int64(1)
	confirmedVersion := int64(0)
	confirmedBy := ""
	var confirmedAt any
	createdAt := now
	if exists {
		version = existing.Version
		confirmedVersion = existing.ConfirmedVersion
		confirmedBy = existing.ConfirmedBy
		confirmedAt = nullableTime(existing.ConfirmedAt)
		createdAt = existing.CreatedAt
		if changed {
			version++
			confirmedVersion = 0
			confirmedBy = ""
			confirmedAt = nil
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO upstream_group_failover_policies(source_id,key_id,key_name,key_hint,enabled,main_enabled,backup_enabled,emergency_enabled,main_group_id,backup_group_id,emergency_group_id,pool,version,confirmed_version,confirmed_at,confirmed_by,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(source_id,key_id) DO UPDATE SET key_name=excluded.key_name,key_hint=excluded.key_hint,enabled=excluded.enabled,main_enabled=excluded.main_enabled,backup_enabled=excluded.backup_enabled,emergency_enabled=excluded.emergency_enabled,main_group_id=excluded.main_group_id,backup_group_id=excluded.backup_group_id,emergency_group_id=excluded.emergency_group_id,pool=excluded.pool,version=excluded.version,confirmed_version=excluded.confirmed_version,confirmed_at=excluded.confirmed_at,confirmed_by=excluded.confirmed_by,updated_at=excluded.updated_at`,
		policy.SourceID, policy.KeyID, strings.TrimSpace(policy.KeyName), strings.TrimSpace(policy.KeyHint), boolInt(policy.Enabled), boolInt(policy.MainEnabled), boolInt(policy.BackupEnabled), boolInt(policy.EmergencyEnabled), policy.MainGroupID, policy.BackupGroupID, policy.EmergencyGroupID, strings.TrimSpace(policy.Pool), version, confirmedVersion, confirmedAt, confirmedBy, formatTime(createdAt), formatTime(now))
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM upstream_group_failover_accounts WHERE source_id=? AND key_id=?`, policy.SourceID, policy.KeyID); err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	for _, accountID := range policy.AccountIDs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO upstream_group_failover_accounts(source_id,key_id,account_id,created_at) VALUES(?,?,?,?)`, policy.SourceID, policy.KeyID, accountID, formatTime(now)); err != nil {
			return model.GroupFailoverPolicy{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO upstream_group_failover_states(source_id,key_id,updated_at) VALUES(?,?,?) ON CONFLICT(source_id,key_id) DO NOTHING`, policy.SourceID, policy.KeyID, formatTime(now)); err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	return s.GetGroupFailoverPolicy(ctx, policy.SourceID, policy.KeyID)
}

func (s *Store) ConfirmGroupFailoverPolicy(ctx context.Context, sourceID int64, keyID string, version int64, actor string) (model.GroupFailoverPolicy, error) {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `UPDATE upstream_group_failover_policies SET confirmed_version=version,confirmed_at=?,confirmed_by=?,updated_at=? WHERE source_id=? AND key_id=? AND version=?`, formatTime(now), strings.TrimSpace(actor), formatTime(now), sourceID, strings.TrimSpace(keyID), version)
	if err != nil {
		return model.GroupFailoverPolicy{}, err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return model.GroupFailoverPolicy{}, errors.New("策略版本已变化，请重新检查后确认")
	}
	return s.GetGroupFailoverPolicy(ctx, sourceID, keyID)
}

func (s *Store) DeleteGroupFailoverPolicy(ctx context.Context, sourceID int64, keyID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM upstream_group_failover_policies WHERE source_id=? AND key_id=?`, sourceID, strings.TrimSpace(keyID))
	return err
}

func (s *Store) GetGroupFailoverState(ctx context.Context, sourceID int64, keyID string) (model.GroupFailoverState, error) {
	var item model.GroupFailoverState
	var frozen int
	var manualHold, cooldown, returnBlocked, recoverySince, lastSwitch, lastConfirmed sql.NullString
	var updatedAt string
	var manualOverride, lastTransition, verificationStarted, healthySince sql.NullString
	var switchRequested, switchVerified, validationNotBefore, evidenceDeadline, lastEvidenceAt sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT `+groupFailoverStateColumns+` FROM upstream_group_failover_states WHERE source_id=? AND key_id=?`, sourceID, strings.TrimSpace(keyID)).
		Scan(&item.SourceID, &item.KeyID, &item.CurrentTier, &item.ObservedGroupID, &item.PreviousTier, &item.PreviousStableTier, &item.PreviousGroupID, &frozen, &item.FreezeReason, &item.LastError, &manualHold, &manualOverride, &cooldown, &returnBlocked, &recoverySince, &lastSwitch, &lastTransition, &verificationStarted, &healthySince, &item.RecoveryHealthyCount, &lastConfirmed,
			&item.ValidationStatus, &item.ValidationMode, &item.ValidationTransitionID, &item.ValidationFromTier, &item.ValidationTargetTier, &item.ValidationFromGroupID, &item.ValidationTargetGroupID, &switchRequested, &switchVerified, &validationNotBefore, &evidenceDeadline, &item.MonitorWatermark, &item.TrafficWatermark, &item.MonitorEvidenceCursor, &item.TrafficEvidenceCursor, &item.ActiveProbeAttempts, &item.SuccessfulEvidenceCount, &item.FailedEvidenceCount, &item.LastEvidenceID, &item.LastEvidenceSource, &item.LastEvidenceReason, &lastEvidenceAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return model.GroupFailoverState{SourceID: sourceID, KeyID: strings.TrimSpace(keyID), ValidationStatus: model.GroupValidationUnknown, ValidationMode: model.GroupValidationModePassive}, nil
	}
	if err != nil {
		return model.GroupFailoverState{}, err
	}
	item.Frozen = frozen == 1
	item.ManualHoldUntil = parseNullTime(manualHold)
	item.ManualOverrideUntil = parseNullTime(manualOverride)
	item.CooldownUntil = parseNullTime(cooldown)
	item.ReturnBlockedUntil = parseNullTime(returnBlocked)
	item.RecoverySince = parseNullTime(recoverySince)
	item.LastSwitchAt = parseNullTime(lastSwitch)
	item.LastTransitionAt = parseNullTime(lastTransition)
	item.VerificationStartedAt = parseNullTime(verificationStarted)
	item.HealthySince = parseNullTime(healthySince)
	item.LastConfirmedAt = parseNullTime(lastConfirmed)
	item.SwitchRequestedAt = parseNullTime(switchRequested)
	item.SwitchVerifiedAt = parseNullTime(switchVerified)
	item.ValidationNotBefore = parseNullTime(validationNotBefore)
	item.EvidenceDeadline = parseNullTime(evidenceDeadline)
	item.LastEvidenceAt = parseNullTime(lastEvidenceAt)
	item.UpdatedAt = parseTime(updatedAt)
	return item, nil
}

func (s *Store) SaveGroupFailoverState(ctx context.Context, state model.GroupFailoverState) error {
	state.UpdatedAt = time.Now().UTC()
	_, err := s.db.ExecContext(ctx, upsertGroupFailoverStateSQL, groupFailoverStateValues(state)...)
	return err
}

// CompareAndSaveGroupFailoverState prevents evidence collected for an older
// transition from overwriting a superseding transition. The comparison and
// write share the same SQLite transaction.
func (s *Store) CompareAndSaveGroupFailoverState(ctx context.Context, expected, next model.GroupFailoverState) (bool, error) {
	if expected.SourceID != next.SourceID || strings.TrimSpace(expected.KeyID) != strings.TrimSpace(next.KeyID) {
		return false, errors.New("group failover state identity changed")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var transitionID int64
	var status, targetTier, targetGroupID string
	err = tx.QueryRowContext(ctx, `SELECT validation_transition_id,validation_status,validation_target_tier,validation_target_group_id
		FROM upstream_group_failover_states WHERE source_id=? AND key_id=?`, expected.SourceID, strings.TrimSpace(expected.KeyID)).
		Scan(&transitionID, &status, &targetTier, &targetGroupID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if transitionID != expected.ValidationTransitionID || status != expected.ValidationStatus ||
		targetTier != expected.ValidationTargetTier || targetGroupID != expected.ValidationTargetGroupID {
		return false, nil
	}
	next.UpdatedAt = time.Now().UTC()
	if _, err := tx.ExecContext(ctx, upsertGroupFailoverStateSQL, groupFailoverStateValues(next)...); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func groupFailoverStateValues(state model.GroupFailoverState) []any {
	if state.ValidationStatus == "" {
		state.ValidationStatus = model.GroupValidationUnknown
	}
	if state.ValidationMode == "" {
		state.ValidationMode = model.GroupValidationModePassive
	}
	return []any{
		state.SourceID, strings.TrimSpace(state.KeyID), state.CurrentTier, state.ObservedGroupID, state.PreviousTier, state.PreviousStableTier, state.PreviousGroupID, boolInt(state.Frozen), state.FreezeReason, state.LastError,
		nullableTime(state.ManualHoldUntil), nullableTime(state.ManualOverrideUntil), nullableTime(state.CooldownUntil), nullableTime(state.ReturnBlockedUntil), nullableTime(state.RecoverySince), nullableTime(state.LastSwitchAt), nullableTime(state.LastTransitionAt), nullableTime(state.VerificationStartedAt), nullableTime(state.HealthySince), state.RecoveryHealthyCount, nullableTime(state.LastConfirmedAt),
		state.ValidationStatus, state.ValidationMode, state.ValidationTransitionID, state.ValidationFromTier, state.ValidationTargetTier, state.ValidationFromGroupID, state.ValidationTargetGroupID, nullableTime(state.SwitchRequestedAt), nullableTime(state.SwitchVerifiedAt), nullableTime(state.ValidationNotBefore), nullableTime(state.EvidenceDeadline), state.MonitorWatermark, state.TrafficWatermark, state.MonitorEvidenceCursor, state.TrafficEvidenceCursor, state.ActiveProbeAttempts, state.SuccessfulEvidenceCount, state.FailedEvidenceCount, state.LastEvidenceID, state.LastEvidenceSource, state.LastEvidenceReason, nullableTime(state.LastEvidenceAt), formatTime(state.UpdatedAt),
	}
}

// BeginGroupTierTransition reserves an idempotency key before any upstream
// write. A repeated request receives the original transition and never writes
// the upstream a second time.
func (s *Store) BeginGroupTierTransition(ctx context.Context, item model.GroupTierTransition) (model.GroupTierTransition, bool, error) {
	if strings.TrimSpace(item.IdempotencyKey) == "" {
		return model.GroupTierTransition{}, false, errors.New("切换幂等编号不能为空")
	}
	existing, err := s.GetGroupTierTransitionByKey(ctx, item.IdempotencyKey)
	if err == nil {
		if !sameGroupTransitionSemantics(existing, item) {
			return model.GroupTierTransition{}, false, ErrGroupTransitionIdempotencyConflict
		}
		return existing, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.GroupTierTransition{}, false, err
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	item.Status = model.GroupTransitionPending
	item.AttemptCount = 1
	item.BeforeState = item.FromGroupID
	result, err := s.db.ExecContext(ctx, `INSERT INTO upstream_group_transitions(idempotency_key,source_id,key_id,from_tier,to_tier,from_group_id,to_group_id,status,actor,producer,authority,reason,evidence,snapshot_version,trigger,packet_id,run_id,error,attempt_count,before_state,verified_after_state,uncertain,manual,dry_run,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		strings.TrimSpace(item.IdempotencyKey), item.SourceID, strings.TrimSpace(item.KeyID), item.FromTier, item.ToTier, item.FromGroupID, item.ToGroupID, item.Status, item.Actor, item.Producer, item.Authority, item.Reason, item.Evidence, item.SnapshotVersion, item.Trigger, item.PacketID, item.RunID, "", item.AttemptCount, item.BeforeState, "", 0, boolInt(item.Manual), boolInt(item.DryRun), formatTime(item.CreatedAt))
	if err != nil {
		if stringsContains(err.Error(), "UNIQUE") {
			existing, getErr := s.GetGroupTierTransitionByKey(ctx, item.IdempotencyKey)
			if getErr == nil && !sameGroupTransitionSemantics(existing, item) {
				return model.GroupTierTransition{}, false, ErrGroupTransitionIdempotencyConflict
			}
			return existing, true, getErr
		}
		return model.GroupTierTransition{}, false, err
	}
	item.ID, err = result.LastInsertId()
	return item, false, err
}

func (s *Store) CompleteGroupTierTransition(ctx context.Context, transitionID int64, state model.GroupFailoverState, completedAt time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE upstream_group_transitions SET status=?,error='',verified_after_state=?,uncertain=0,completed_at=? WHERE id=? AND status=?`, model.GroupTransitionApplied, state.ObservedGroupID, formatTime(completedAt), transitionID, model.GroupTransitionPending)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("切换流水已经结束，不能重复确认")
	}
	state.UpdatedAt = completedAt
	_, err = tx.ExecContext(ctx, upsertGroupFailoverStateSQL, groupFailoverStateValues(state)...)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) FailGroupTierTransition(ctx context.Context, transitionID int64, message string, completedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE upstream_group_transitions SET status=?,error=?,completed_at=? WHERE id=? AND status=?`, model.GroupTransitionFailed, message, formatTime(completedAt), transitionID, model.GroupTransitionPending)
	return err
}

// MarkGroupTierTransitionUncertain keeps the idempotency reservation pending.
// A later refresh may only read the upstream and complete or fail the row; it
// must never replay the group write.
func (s *Store) MarkGroupTierTransitionUncertain(ctx context.Context, transitionID int64, message string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE upstream_group_transitions SET error=?,uncertain=1 WHERE id=? AND status=?`,
		strings.TrimSpace(message), transitionID, model.GroupTransitionPending)
	return err
}

func (s *Store) SimulateGroupTierTransition(ctx context.Context, transitionID int64, completedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE upstream_group_transitions SET status=?,error='',completed_at=? WHERE id=? AND status=?`, model.GroupTransitionSimulated, formatTime(completedAt), transitionID, model.GroupTransitionPending)
	return err
}

func (s *Store) GetGroupTierTransitionByKey(ctx context.Context, idempotencyKey string) (model.GroupTierTransition, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+groupTierTransitionColumns+` FROM upstream_group_transitions WHERE idempotency_key=?`, strings.TrimSpace(idempotencyKey))
	item, err := scanGroupTierTransition(row)
	if errors.Is(err, sql.ErrNoRows) {
		return model.GroupTierTransition{}, fmt.Errorf("切换流水不存在: %w", sql.ErrNoRows)
	}
	return item, err
}

func (s *Store) ListGroupTierTransitions(ctx context.Context, sourceID int64, keyID string, limit int) ([]model.GroupTierTransition, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT ` + groupTierTransitionColumns + ` FROM upstream_group_transitions WHERE 1=1`
	args := []any{}
	if sourceID > 0 {
		query += ` AND source_id=?`
		args = append(args, sourceID)
	}
	if strings.TrimSpace(keyID) != "" {
		query += ` AND key_id=?`
		args = append(args, strings.TrimSpace(keyID))
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.GroupTierTransition, 0)
	for rows.Next() {
		item, err := scanGroupTierTransition(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ListPendingGroupTierTransitions(ctx context.Context) ([]model.GroupTierTransition, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+groupTierTransitionColumns+` FROM upstream_group_transitions WHERE status=? ORDER BY id`, model.GroupTransitionPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.GroupTierTransition, 0)
	for rows.Next() {
		item, scanErr := scanGroupTierTransition(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) CountCompletedGroupTierTransitions(ctx context.Context, sourceID int64, keyID string, since time.Time) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM upstream_group_transitions WHERE source_id=? AND key_id=? AND status IN (?,?) AND manual=0 AND completed_at>=?`, sourceID, strings.TrimSpace(keyID), model.GroupTransitionApplied, model.GroupTransitionCompleted, formatTime(since)).Scan(&count)
	return count, err
}

func (s *Store) HasAutomaticGroupTransitionInPoolSince(ctx context.Context, pool string, since time.Time) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM upstream_group_transitions AS transition
		JOIN upstream_group_failover_policies AS policy
		  ON policy.source_id=transition.source_id AND policy.key_id=transition.key_id
		WHERE policy.pool=? AND transition.manual=0 AND transition.dry_run=0
		  AND transition.status IN (?,?,?) AND transition.created_at>?`,
		strings.TrimSpace(pool), model.GroupTransitionPending, model.GroupTransitionApplied, model.GroupTransitionCompleted, formatTime(since.UTC())).Scan(&count)
	return count > 0, err
}

func scanGroupTierTransition(row rowScanner) (model.GroupTierTransition, error) {
	var item model.GroupTierTransition
	var manual, dryRun, uncertain int
	var createdAt string
	var completedAt sql.NullString
	err := row.Scan(&item.ID, &item.IdempotencyKey, &item.SourceID, &item.KeyID, &item.FromTier, &item.ToTier, &item.FromGroupID, &item.ToGroupID, &item.Status, &item.Actor, &item.Producer, &item.Authority, &item.Reason, &item.Evidence, &item.SnapshotVersion, &item.Trigger, &item.PacketID, &item.RunID, &item.Error, &item.AttemptCount, &item.BeforeState, &item.VerifiedAfter, &uncertain, &manual, &dryRun, &createdAt, &completedAt)
	if err != nil {
		return model.GroupTierTransition{}, err
	}
	item.Manual = manual == 1
	item.DryRun = dryRun == 1
	item.Uncertain = uncertain == 1
	item.CreatedAt = parseTime(createdAt)
	item.CompletedAt = parseNullTime(completedAt)
	return item, nil
}

func sameGroupTransitionSemantics(left, right model.GroupTierTransition) bool {
	return left.SourceID == right.SourceID && strings.TrimSpace(left.KeyID) == strings.TrimSpace(right.KeyID) &&
		left.FromTier == right.FromTier && left.ToTier == right.ToTier && left.FromGroupID == right.FromGroupID && left.ToGroupID == right.ToGroupID &&
		left.Actor == right.Actor && left.Producer == right.Producer && left.Authority == right.Authority && left.Reason == right.Reason &&
		left.Evidence == right.Evidence && left.SnapshotVersion == right.SnapshotVersion && left.Trigger == right.Trigger &&
		left.PacketID == right.PacketID && left.RunID == right.RunID && left.Manual == right.Manual && left.DryRun == right.DryRun
}

func (s *Store) listGroupFailoverAccounts(ctx context.Context, sourceID int64, keyID string) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT account_id FROM upstream_group_failover_accounts WHERE source_id=? AND key_id=? ORDER BY account_id`, sourceID, keyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]int64, 0)
	for rows.Next() {
		var accountID int64
		if err := rows.Scan(&accountID); err != nil {
			return nil, err
		}
		items = append(items, accountID)
	}
	return items, rows.Err()
}

func sameGroupFailoverPolicy(left, right model.GroupFailoverPolicy) bool {
	return left.Enabled == right.Enabled && left.MainEnabled == right.MainEnabled && left.BackupEnabled == right.BackupEnabled && left.EmergencyEnabled == right.EmergencyEnabled && left.KeyName == strings.TrimSpace(right.KeyName) && left.KeyHint == strings.TrimSpace(right.KeyHint) && left.MainGroupID == right.MainGroupID && left.BackupGroupID == right.BackupGroupID && left.EmergencyGroupID == right.EmergencyGroupID && left.Pool == strings.TrimSpace(right.Pool) && equalInt64s(left.AccountIDs, uniqueSortedInt64(right.AccountIDs))
}

func (s *Store) GetFailoverEvidenceWatermarks(ctx context.Context) (model.FailoverEvidenceWatermarks, error) {
	var watermarks model.FailoverEvidenceWatermarks
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid),0) FROM monitor_history_records`).Scan(&watermarks.Monitor); err != nil {
		return watermarks, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid),0) FROM traffic_events`).Scan(&watermarks.Traffic); err != nil {
		return watermarks, err
	}
	return watermarks, nil
}

func (s *Store) ListGroupValidationEvidence(ctx context.Context, monitorIDs, accountIDs []int64, monitorAfter, trafficAfter int64) ([]model.GroupValidationEvidence, error) {
	items := make([]model.GroupValidationEvidence, 0)
	if len(monitorIDs) > 0 {
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(monitorIDs)), ",")
		args := make([]any, 0, len(monitorIDs)+1)
		args = append(args, monitorAfter)
		for _, id := range monitorIDs {
			args = append(args, id)
		}
		rows, err := s.db.QueryContext(ctx, `SELECT rowid,monitor_id,status,error_class,reason_code,checked_at FROM monitor_history_records WHERE rowid>? AND monitor_id IN (`+placeholders+`) ORDER BY rowid`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var item model.GroupValidationEvidence
			var observedAt string
			item.Source = "monitor"
			if err := rows.Scan(&item.ID, &item.MonitorID, &item.Status, &item.ErrorClass, &item.ReasonCode, &observedAt); err != nil {
				rows.Close()
				return nil, err
			}
			item.ObservedAt = parseTime(observedAt)
			startedAt := item.ObservedAt
			item.RequestStartedAt = &startedAt
			item.TimeBasis = model.EvidenceTimeBasisMonitorRequestStart
			items = append(items, item)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	if len(accountIDs) > 0 {
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(accountIDs)), ",")
		args := make([]any, 0, len(accountIDs)+1)
		args = append(args, trafficAfter)
		for _, id := range accountIDs {
			args = append(args, id)
		}
		rows, err := s.db.QueryContext(ctx, `SELECT rowid,account_id,kind,error_class,reason_code,created_at,request_started_at FROM traffic_events WHERE rowid>? AND account_id IN (`+placeholders+`) ORDER BY rowid`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var item model.GroupValidationEvidence
			var observedAt string
			var requestStartedAt sql.NullString
			item.Source = "traffic"
			if err := rows.Scan(&item.ID, &item.AccountID, &item.Status, &item.ErrorClass, &item.ReasonCode, &observedAt, &requestStartedAt); err != nil {
				rows.Close()
				return nil, err
			}
			item.ObservedAt = parseTime(observedAt)
			item.RequestStartedAt = parseNullTime(requestStartedAt)
			if item.RequestStartedAt != nil {
				item.TimeBasis = model.EvidenceTimeBasisRequestStart
			} else {
				item.TimeBasis = model.EvidenceTimeBasisCompletion
			}
			items = append(items, item)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].ObservedAt.Equal(items[j].ObservedAt) {
			if items[i].Source == items[j].Source {
				return items[i].ID < items[j].ID
			}
			return items[i].Source < items[j].Source
		}
		return items[i].ObservedAt.Before(items[j].ObservedAt)
	})
	return items, nil
}

func uniqueSortedInt64(values []int64) []int64 {
	seen := make(map[int64]bool, len(values))
	result := make([]int64, 0, len(values))
	for _, value := range values {
		if value > 0 && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func equalInt64s(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (s *Store) AssertNoPendingGroupTransition(ctx context.Context, sourceID int64, keyID string) error {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM upstream_group_transitions WHERE source_id=? AND key_id=? AND status=?`, sourceID, keyID, model.GroupTransitionPending).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("该令牌有 %d 个状态未确认的切换请求", count)
	}
	return nil
}
