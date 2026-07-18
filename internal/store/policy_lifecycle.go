package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

var (
	ErrPolicyIdempotencyConflict = errors.New("policy proposal idempotency conflict")
	ErrPolicyBaseChanged         = errors.New("policy proposal base version changed")
)

func (s *Store) CreatePolicyProposal(ctx context.Context, item *model.ScorePolicyVersion) error {
	if item == nil || item.ScopeType == "" || len(item.Patch) == 0 || item.IdempotencyKey == "" || item.SemanticHash == "" {
		return errors.New("invalid policy proposal")
	}
	if existing, err := s.findPolicyByIdempotency(ctx, item.IdempotencyKey); err == nil {
		if existing.SemanticHash != item.SemanticHash {
			return ErrPolicyIdempotencyConflict
		}
		*item = existing
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM score_policy_versions WHERE scope_type=? AND scope_id=?`,
		item.ScopeType, item.ScopeID).Scan(&item.Version); err != nil {
		return err
	}
	item.CreatedAt = time.Now().UTC()
	if item.Status == "" {
		item.Status = model.PolicyStatusPendingApproval
	}
	if len(item.Config) == 0 {
		item.Config = append(json.RawMessage(nil), item.Patch...)
	}
	diff, simulation, affected := normalizedPolicyLifecycleJSON(item)
	result, err := tx.ExecContext(ctx, `INSERT INTO score_policy_versions(scope_type,scope_id,version,status,config_json,reason,
		agent_run_id,created_by,activated_at,created_at,base_version_id,source_goal_id,source_step_id,source_packet_id,
		source_packet_hash,patch_json,diff_json,simulation_json,
		risk_level,affected_accounts_json,approved_by,previous_active_version_id,rollback_reason,outcome_summary,
		idempotency_key,semantic_hash,auto_rollback_count) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		item.ScopeType, item.ScopeID, item.Version, item.Status, string(item.Config), item.Reason, item.AgentRunID,
		item.CreatedBy, formatOptionalTime(item.ActivatedAt), formatTime(item.CreatedAt), item.BaseVersionID, item.SourceGoalID,
		item.SourceStepID, item.SourcePacketID, item.SourcePacketHash,
		string(item.Patch), diff, simulation, item.RiskLevel, affected, item.ApprovedBy, item.PreviousActiveVersionID,
		item.RollbackReason, item.OutcomeSummary, item.IdempotencyKey, item.SemanticHash, item.AutoRollbackCount)
	if err != nil {
		return err
	}
	item.ID, err = result.LastInsertId()
	if err != nil {
		return err
	}
	return tx.Commit()
}

func normalizedPolicyLifecycleJSON(item *model.ScorePolicyVersion) (string, string, string) {
	diff := item.Diff
	if len(diff) == 0 {
		diff = json.RawMessage("{}")
	}
	simulation, _ := json.Marshal(item.Simulation)
	affected, _ := json.Marshal(item.AffectedAccountIDs)
	return string(diff), string(simulation), string(affected)
}

func (s *Store) GetPolicyLifecycle(ctx context.Context, id int64) (model.ScorePolicyVersion, error) {
	return scanPolicyLifecycle(s.db.QueryRowContext(ctx, policyLifecycleSelect+` WHERE id=?`, id))
}

func (s *Store) FindActivePolicyVersion(ctx context.Context, scopeType, scopeID string) (model.ScorePolicyVersion, error) {
	return scanPolicyLifecycle(s.db.QueryRowContext(ctx, policyLifecycleSelect+
		` WHERE scope_type=? AND scope_id=? AND status='active' ORDER BY id DESC LIMIT 1`, scopeType, scopeID))
}

func (s *Store) ListPolicyLifecycle(ctx context.Context, limit int) ([]model.ScorePolicyVersion, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, policyLifecycleSelect+` ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.ScorePolicyVersion, 0)
	for rows.Next() {
		item, err := scanPolicyLifecycle(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) CountPolicyActivationsSince(ctx context.Context, since time.Time) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM score_policy_versions WHERE activated_at>=?`, formatTime(since.UTC())).Scan(&count)
	return count, err
}

func (s *Store) RejectPolicyProposal(ctx context.Context, id int64, actor, reason string) error {
	if id <= 0 || strings.TrimSpace(actor) == "" || strings.TrimSpace(reason) == "" {
		return errors.New("policy rejection requires id, actor, and reason")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE score_policy_versions SET status=?,approved_by=?,outcome_summary=?
		WHERE id=? AND status IN (?,?,?)`, model.PolicyStatusRejected, strings.TrimSpace(actor), strings.TrimSpace(reason), id,
		model.PolicyStatusDraft, model.PolicyStatusSimulated, model.PolicyStatusPendingApproval)
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return errors.New("policy proposal cannot be rejected")
	}
	return nil
}

func (s *Store) findPolicyByIdempotency(ctx context.Context, key string) (model.ScorePolicyVersion, error) {
	return scanPolicyLifecycle(s.db.QueryRowContext(ctx, policyLifecycleSelect+` WHERE idempotency_key=?`, key))
}

const policyLifecycleSelect = `SELECT id,scope_type,scope_id,version,status,config_json,patch_json,diff_json,
	simulation_json,risk_level,affected_accounts_json,reason,agent_run_id,source_goal_id,source_step_id,source_packet_id,
	source_packet_hash,base_version_id,
	previous_active_version_id,created_by,approved_by,idempotency_key,semantic_hash,rollback_reason,outcome_summary,
	auto_rollback_count,activated_at,created_at FROM score_policy_versions`

type policyScanner interface {
	Scan(...any) error
}

func scanPolicyLifecycle(scanner policyScanner) (model.ScorePolicyVersion, error) {
	var item model.ScorePolicyVersion
	var config, patch, diff, simulation, affected, created string
	var runID, goalID, stepID, packetID, baseID, previousID sql.NullInt64
	var activated sql.NullString
	err := scanner.Scan(&item.ID, &item.ScopeType, &item.ScopeID, &item.Version, &item.Status, &config, &patch, &diff,
		&simulation, &item.RiskLevel, &affected, &item.Reason, &runID, &goalID, &stepID, &packetID,
		&item.SourcePacketHash, &baseID, &previousID,
		&item.CreatedBy, &item.ApprovedBy, &item.IdempotencyKey, &item.SemanticHash, &item.RollbackReason,
		&item.OutcomeSummary, &item.AutoRollbackCount, &activated, &created)
	if err != nil {
		return item, err
	}
	item.Config, item.Patch, item.Diff = json.RawMessage(config), json.RawMessage(patch), json.RawMessage(diff)
	_ = json.Unmarshal([]byte(simulation), &item.Simulation)
	_ = json.Unmarshal([]byte(affected), &item.AffectedAccountIDs)
	item.AgentRunID, item.SourceGoalID, item.SourceStepID = nullableInt64(runID), nullableInt64(goalID), nullableInt64(stepID)
	item.SourcePacketID, item.BaseVersionID = nullableInt64(packetID), nullableInt64(baseID)
	item.PreviousActiveVersionID = nullableInt64(previousID)
	item.ActivatedAt, item.CreatedAt = parseNullableTime(activated), parseTime(created)
	return item, nil
}

func (s *Store) PublishPolicyProposal(ctx context.Context, id int64, approvedBy string, settings *model.Settings, policies []model.Policy) error {
	if id <= 0 || (settings == nil) == (len(policies) == 0) {
		return errors.New("policy publication requires exactly one projection")
	}
	values, err := validatePolicyProjection(settings, policies)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var scopeType, scopeID, status string
	var baseID sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT scope_type,scope_id,status,base_version_id FROM score_policy_versions WHERE id=?`, id).
		Scan(&scopeType, &scopeID, &status, &baseID); err != nil {
		return err
	}
	if status != model.PolicyStatusSimulated && status != model.PolicyStatusPendingApproval {
		return fmt.Errorf("policy proposal status %s cannot be activated", status)
	}
	var currentID sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM score_policy_versions WHERE scope_type=? AND scope_id=? AND status='active'
		ORDER BY id DESC LIMIT 1`, scopeType, scopeID).Scan(&currentID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if currentID.Valid != baseID.Valid || (currentID.Valid && currentID.Int64 != baseID.Int64) {
		if _, err := tx.ExecContext(ctx, `UPDATE score_policy_versions SET status=? WHERE id=?`, model.PolicyStatusSuperseded, id); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return ErrPolicyBaseChanged
	}
	if err := writePolicyProjection(ctx, tx, values, settings, policies); err != nil {
		return err
	}
	now := time.Now().UTC()
	if currentID.Valid {
		if _, err := tx.ExecContext(ctx, `UPDATE score_policy_versions SET status=? WHERE id=?`, model.PolicyStatusSuperseded, currentID.Int64); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE score_policy_versions SET status=?,approved_by=?,previous_active_version_id=?,activated_at=? WHERE id=?`,
		model.PolicyStatusActive, strings.TrimSpace(approvedBy), nullableInt64Value(currentID), formatTime(now), id)
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return errors.New("policy proposal was not activated")
	}
	return tx.Commit()
}

func (s *Store) RollbackPolicyProposal(ctx context.Context, activeID, previousID int64, actor, reason string,
	settings *model.Settings, policies []model.Policy) error {
	if activeID <= 0 || previousID <= 0 || activeID == previousID || (settings == nil) == (len(policies) == 0) {
		return errors.New("invalid policy rollback")
	}
	values, err := validatePolicyProjection(settings, policies)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status string
	var recordedPrevious sql.NullInt64
	var rollbackCount int
	if err := tx.QueryRowContext(ctx, `SELECT status,previous_active_version_id,auto_rollback_count
		FROM score_policy_versions WHERE id=?`, activeID).Scan(&status, &recordedPrevious, &rollbackCount); err != nil {
		return err
	}
	if status != model.PolicyStatusActive || !recordedPrevious.Valid || recordedPrevious.Int64 != previousID || rollbackCount > 0 {
		return errors.New("policy rollback fence changed")
	}
	if err := writePolicyProjection(ctx, tx, values, settings, policies); err != nil {
		return err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE score_policy_versions SET status=?,rollback_reason=?,outcome_summary=?,
		auto_rollback_count=auto_rollback_count+1 WHERE id=?`, model.PolicyStatusRolledBack, reason,
		"rolled back by "+strings.TrimSpace(actor), activeID); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE score_policy_versions SET status=?,activated_at=? WHERE id=?`,
		model.PolicyStatusActive, formatTime(now), previousID)
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return errors.New("previous policy version was not restored")
	}
	return tx.Commit()
}

func validatePolicyProjection(settings *model.Settings, policies []model.Policy) (map[string]string, error) {
	if settings != nil {
		return settingsValues(*settings)
	}
	seen := make(map[int64]struct{}, len(policies))
	for _, policy := range policies {
		if err := validatePolicy(policy); err != nil {
			return nil, err
		}
		if _, exists := seen[policy.AccountID]; exists {
			return nil, fmt.Errorf("duplicate account policy %d", policy.AccountID)
		}
		seen[policy.AccountID] = struct{}{}
	}
	return nil, nil
}

func writePolicyProjection(ctx context.Context, tx *sql.Tx, values map[string]string, settings *model.Settings, policies []model.Policy) error {
	if settings != nil {
		return writeSettings(ctx, tx, values, time.Now().UTC())
	}
	now := time.Now().UTC()
	for _, policy := range policies {
		if err := upsertPolicy(ctx, tx, policy, now); err != nil {
			return err
		}
	}
	return nil
}

func nullableInt64Value(value sql.NullInt64) any {
	if value.Valid {
		return value.Int64
	}
	return nil
}
