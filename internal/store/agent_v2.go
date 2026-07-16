package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

var ErrAdministratorGrantAlreadyConsumed = errors.New("administrator grant was already consumed by another step")

func (s *Store) migrateAgentV2(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS agent_goals (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			parent_goal_id INTEGER,
			conversation_id INTEGER,
			title TEXT NOT NULL,
			objective TEXT NOT NULL,
			status TEXT NOT NULL,
			priority INTEGER NOT NULL DEFAULT 50,
			risk_level TEXT NOT NULL DEFAULT 'low',
			source TEXT NOT NULL DEFAULT 'system',
			context_json TEXT NOT NULL DEFAULT '{}',
			plan_hash TEXT NOT NULL DEFAULT '',
			created_by TEXT NOT NULL DEFAULT 'system',
			deadline_at TEXT,
			last_error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			completed_at TEXT,
			FOREIGN KEY(parent_goal_id) REFERENCES agent_goals(id) ON DELETE SET NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_goals_status_priority ON agent_goals(status,priority DESC,created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_goals_updated ON agent_goals(updated_at DESC)`,
		`CREATE TABLE IF NOT EXISTS agent_steps (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			goal_id INTEGER NOT NULL,
			sequence INTEGER NOT NULL,
			depends_on_step_id INTEGER,
			capability TEXT NOT NULL,
			arguments_json TEXT NOT NULL DEFAULT '{}',
			preconditions_json TEXT NOT NULL DEFAULT '{}',
			compensation_json TEXT NOT NULL DEFAULT '{}',
			status TEXT NOT NULL,
			risk_level TEXT NOT NULL DEFAULT 'low',
			idempotency_key TEXT NOT NULL UNIQUE,
			scheduled_for TEXT,
			expires_at TEXT,
			lease_owner TEXT NOT NULL DEFAULT '',
			lease_until TEXT,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 1,
			mutation_attempted_at TEXT,
			before_state TEXT NOT NULL DEFAULT '{}',
			after_state TEXT NOT NULL DEFAULT '{}',
			result_json TEXT NOT NULL DEFAULT '{}',
			last_error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			completed_at TEXT,
			UNIQUE(goal_id,sequence),
			FOREIGN KEY(goal_id) REFERENCES agent_goals(id) ON DELETE CASCADE,
			FOREIGN KEY(depends_on_step_id) REFERENCES agent_steps(id) ON DELETE SET NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_steps_goal_sequence ON agent_steps(goal_id,sequence)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_steps_status_schedule ON agent_steps(status,scheduled_for,lease_until)`,
		`CREATE TABLE IF NOT EXISTS agent_v2_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_key TEXT NOT NULL UNIQUE,
			goal_id INTEGER,
			step_id INTEGER,
			type TEXT NOT NULL,
			severity TEXT NOT NULL DEFAULT 'info',
			actor TEXT NOT NULL DEFAULT 'system',
			payload_json TEXT NOT NULL DEFAULT '{}',
			created_at TEXT NOT NULL,
			FOREIGN KEY(goal_id) REFERENCES agent_goals(id) ON DELETE SET NULL,
			FOREIGN KEY(step_id) REFERENCES agent_steps(id) ON DELETE SET NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_v2_events_goal_created ON agent_v2_events(goal_id,created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_v2_events_created ON agent_v2_events(created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS agent_checkpoints (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			goal_id INTEGER NOT NULL,
			step_id INTEGER,
			kind TEXT NOT NULL,
			state_json TEXT NOT NULL,
			state_hash TEXT NOT NULL,
			created_at TEXT NOT NULL,
			FOREIGN KEY(goal_id) REFERENCES agent_goals(id) ON DELETE CASCADE,
			FOREIGN KEY(step_id) REFERENCES agent_steps(id) ON DELETE SET NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_checkpoints_goal_created ON agent_checkpoints(goal_id,created_at DESC,id DESC)`,
		`CREATE TABLE IF NOT EXISTS agent_memories (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			scope_type TEXT NOT NULL,
			scope_id TEXT NOT NULL DEFAULT '',
			kind TEXT NOT NULL,
			memory_key TEXT NOT NULL,
			summary TEXT NOT NULL DEFAULT '',
			content_json TEXT NOT NULL DEFAULT '{}',
			importance REAL NOT NULL DEFAULT 0,
			pinned INTEGER NOT NULL DEFAULT 0,
			expires_at TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE(scope_type,scope_id,kind,memory_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_memories_scope ON agent_memories(scope_type,scope_id,kind,importance DESC,updated_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_memories_expiry ON agent_memories(pinned,expires_at,updated_at)`,
		`CREATE TABLE IF NOT EXISTS agent_scheduled_commands (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			goal_id INTEGER,
			step_id INTEGER,
			capability TEXT NOT NULL,
			arguments_json TEXT NOT NULL DEFAULT '{}',
			conditions_json TEXT NOT NULL DEFAULT '{}',
			status TEXT NOT NULL,
			timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai',
			execute_at TEXT NOT NULL,
			expires_at TEXT,
			idempotency_key TEXT NOT NULL UNIQUE,
			lease_owner TEXT NOT NULL DEFAULT '',
			lease_until TEXT,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 3,
			mutation_attempted_at TEXT,
			result_json TEXT NOT NULL DEFAULT '{}',
			last_error TEXT NOT NULL DEFAULT '',
			created_by TEXT NOT NULL DEFAULT 'system',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			completed_at TEXT,
			FOREIGN KEY(goal_id) REFERENCES agent_goals(id) ON DELETE SET NULL,
			FOREIGN KEY(step_id) REFERENCES agent_steps(id) ON DELETE SET NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_commands_claim ON agent_scheduled_commands(status,execute_at,lease_until)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_commands_goal ON agent_scheduled_commands(goal_id,created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS agent_freeze_states (
			scope_type TEXT NOT NULL,
			scope_id TEXT NOT NULL DEFAULT '',
			mode TEXT NOT NULL,
			reason TEXT NOT NULL DEFAULT '',
			actor TEXT NOT NULL DEFAULT 'system',
			expires_at TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(scope_type,scope_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_freeze_mode_expiry ON agent_freeze_states(mode,expires_at)`,
		`CREATE TABLE IF NOT EXISTS agent_administrator_grant_consumptions (
			grant_id TEXT PRIMARY KEY,
			goal_id INTEGER NOT NULL,
			step_id INTEGER NOT NULL,
			capability TEXT NOT NULL,
			arguments_hash TEXT NOT NULL,
			consumed_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_admin_grants_consumed_at ON agent_administrator_grant_consumptions(consumed_at)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate agent v2 database: %w", err)
		}
	}
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "agent_steps", name: "mutation_attempted_at"},
		{table: "agent_scheduled_commands", name: "mutation_attempted_at"},
	} {
		if err := s.ensureColumn(ctx, column.table, column.name, "TEXT"); err != nil {
			return err
		}
	}
	if err := s.backfillAgentV2MutationAttempts(ctx); err != nil {
		return err
	}
	now := formatTime(time.Now().UTC())
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO agent_freeze_states(scope_type,scope_id,mode,reason,actor,created_at,updated_at)
		VALUES('global','',?,'','system',?,?)`, model.AgentFreezeModeActive, now, now)
	return err
}

// backfillAgentV2MutationAttempts preserves the write boundary for databases
// created before the explicit column existed. The old runtime stored the same
// timestamp inside the reconciliation JSON before entering a mutation.
func (s *Store) backfillAgentV2MutationAttempts(ctx context.Context) error {
	type legacyAttempt struct {
		id          int64
		attemptedAt time.Time
	}
	for _, source := range []struct {
		table     string
		column    string
		predicate string
	}{
		{table: "agent_steps", column: "preconditions_json",
			predicate: "status IN ('running','verifying','compensating','reconciling')"},
		{table: "agent_scheduled_commands", column: "result_json",
			predicate: "status IN ('leased','reconciling')"},
	} {
		query := fmt.Sprintf("SELECT id,%s FROM %s WHERE mutation_attempted_at IS NULL AND %s",
			source.column, source.table, source.predicate)
		rows, err := s.db.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("read legacy %s mutation attempts: %w", source.table, err)
		}
		attempts := make([]legacyAttempt, 0)
		for rows.Next() {
			var id int64
			var raw string
			if err := rows.Scan(&id, &raw); err != nil {
				rows.Close()
				return err
			}
			var evidence struct {
				AttemptedAt time.Time `json:"attempted_at"`
			}
			if json.Unmarshal([]byte(raw), &evidence) == nil && !evidence.AttemptedAt.IsZero() {
				attempts = append(attempts, legacyAttempt{id: id, attemptedAt: evidence.AttemptedAt.UTC()})
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, attempt := range attempts {
			query := fmt.Sprintf("UPDATE %s SET mutation_attempted_at=? WHERE id=? AND mutation_attempted_at IS NULL", source.table)
			if _, err := s.db.ExecContext(ctx, query, formatTime(attempt.attemptedAt), attempt.id); err != nil {
				return fmt.Errorf("backfill %s mutation attempt: %w", source.table, err)
			}
		}
	}
	return nil
}

// ConsumeAdministratorGrant records the one step which owns an exact
// administrator privilege envelope. A retry of that same durable step is
// accepted, while every cross-step or cross-goal reuse fails closed.
func (s *Store) ConsumeAdministratorGrant(ctx context.Context, grantID string, goalID, stepID int64,
	capability, argumentsHash string) (bool, error) {
	grantID, capability, argumentsHash = strings.TrimSpace(grantID), strings.TrimSpace(capability), strings.TrimSpace(argumentsHash)
	if len(grantID) != len("ag1_")+hex.EncodedLen(32) || !strings.HasPrefix(grantID, "ag1_") {
		return false, errors.New("administrator grant id is invalid")
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(grantID, "ag1_")); err != nil {
		return false, errors.New("administrator grant id is invalid")
	}
	if goalID <= 0 || stepID <= 0 || capability == "" || len(argumentsHash) != hex.EncodedLen(32) {
		return false, errors.New("administrator grant consumption identity is invalid")
	}
	if _, err := hex.DecodeString(argumentsHash); err != nil {
		return false, errors.New("administrator grant arguments hash is invalid")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO agent_administrator_grant_consumptions(
		grant_id,goal_id,step_id,capability,arguments_hash,consumed_at) VALUES(?,?,?,?,?,?)`,
		grantID, goalID, stepID, capability, argumentsHash, formatTime(time.Now().UTC()))
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	var existingGoalID, existingStepID int64
	var existingCapability, existingArgumentsHash string
	if err := tx.QueryRowContext(ctx, `SELECT goal_id,step_id,capability,arguments_hash
		FROM agent_administrator_grant_consumptions WHERE grant_id=?`, grantID).
		Scan(&existingGoalID, &existingStepID, &existingCapability, &existingArgumentsHash); err != nil {
		return false, err
	}
	if existingGoalID != goalID || existingStepID != stepID || existingCapability != capability ||
		existingArgumentsHash != argumentsHash {
		return false, fmt.Errorf("%w: grant=%s", ErrAdministratorGrantAlreadyConsumed, grantID)
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return rows == 1, nil
}

func normalizedJSON(value json.RawMessage) (json.RawMessage, error) {
	if len(value) == 0 {
		return json.RawMessage(`{}`), nil
	}
	if !json.Valid(value) {
		return nil, errors.New("invalid JSON payload")
	}
	return value, nil
}

func validAgentGoalStatus(value string) bool {
	switch value {
	case model.AgentGoalStatusPlanned, model.AgentGoalStatusRunning, model.AgentGoalStatusWaiting,
		model.AgentGoalStatusCompleted, model.AgentGoalStatusFailed, model.AgentGoalStatusCancelled:
		return true
	default:
		return false
	}
}

func validAgentStepStatus(value string) bool {
	switch value {
	case model.AgentStepStatusPending, model.AgentStepStatusScheduled, model.AgentStepStatusRunning,
		model.AgentStepStatusVerifying, model.AgentStepStatusCompensating, model.AgentStepStatusReconciling,
		model.AgentStepStatusCompleted, model.AgentStepStatusFailed, model.AgentStepStatusCancelled,
		model.AgentStepStatusSkipped:
		return true
	default:
		return false
	}
}

func validAgentCommandStatus(value string) bool {
	switch value {
	case model.AgentCommandStatusPending, model.AgentCommandStatusLeased, model.AgentCommandStatusReconciling,
		model.AgentCommandStatusCompleted, model.AgentCommandStatusFailed, model.AgentCommandStatusCancelled,
		model.AgentCommandStatusExpired:
		return true
	default:
		return false
	}
}

func validAgentFreezeMode(value string) bool {
	switch value {
	case model.AgentFreezeModeActive, model.AgentFreezeModeAgentPaused,
		model.AgentFreezeModeReadOnly, model.AgentFreezeModeWritesFrozen:
		return true
	default:
		return false
	}
}

func terminalAgentGoal(status string) bool {
	return status == model.AgentGoalStatusCompleted || status == model.AgentGoalStatusFailed || status == model.AgentGoalStatusCancelled
}

func terminalAgentStep(status string) bool {
	return status == model.AgentStepStatusCompleted || status == model.AgentStepStatusFailed ||
		status == model.AgentStepStatusCancelled || status == model.AgentStepStatusSkipped
}

func terminalAgentCommand(status string) bool {
	return status == model.AgentCommandStatusCompleted || status == model.AgentCommandStatusFailed ||
		status == model.AgentCommandStatusCancelled || status == model.AgentCommandStatusExpired
}

func requiredString(value, name string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func (s *Store) CreateAgentGoal(ctx context.Context, item *model.AgentGoal) error {
	if item == nil {
		return errors.New("agent goal is nil")
	}
	var err error
	if item.Title, err = requiredString(item.Title, "title"); err != nil {
		return err
	}
	if item.Objective, err = requiredString(item.Objective, "objective"); err != nil {
		return err
	}
	if item.Status == "" {
		item.Status = model.AgentGoalStatusPlanned
	}
	if !validAgentGoalStatus(item.Status) {
		return errors.New("invalid agent goal status")
	}
	if item.Priority == 0 {
		item.Priority = 50
	}
	if item.Priority < 1 || item.Priority > 100 {
		return errors.New("agent goal priority must be between 1 and 100")
	}
	if item.RiskLevel == "" {
		item.RiskLevel = model.AgentRiskLow
	}
	if item.Source == "" {
		item.Source = "system"
	}
	if item.CreatedBy == "" {
		item.CreatedBy = "system"
	}
	item.Context, err = normalizedJSON(item.Context)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	if terminalAgentGoal(item.Status) && item.CompletedAt == nil {
		item.CompletedAt = &now
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO agent_goals(parent_goal_id,conversation_id,title,objective,status,priority,
		risk_level,source,context_json,plan_hash,created_by,deadline_at,last_error,created_at,updated_at,completed_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, item.ParentGoalID, item.ConversationID, item.Title, item.Objective,
		item.Status, item.Priority, item.RiskLevel, item.Source, string(item.Context), item.PlanHash, item.CreatedBy,
		formatOptionalTime(item.DeadlineAt), item.LastError, formatTime(item.CreatedAt), formatTime(item.UpdatedAt),
		formatOptionalTime(item.CompletedAt))
	if err != nil {
		return err
	}
	item.ID, err = result.LastInsertId()
	return err
}

func (s *Store) GetAgentGoal(ctx context.Context, id int64) (model.AgentGoal, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id,parent_goal_id,conversation_id,title,objective,status,priority,risk_level,
		source,context_json,plan_hash,created_by,deadline_at,last_error,created_at,updated_at,completed_at
		FROM agent_goals WHERE id=?`, id)
	return scanAgentGoal(row)
}

func (s *Store) UpdateAgentGoal(ctx context.Context, item model.AgentGoal) error {
	if item.ID <= 0 || !validAgentGoalStatus(item.Status) {
		return errors.New("invalid agent goal")
	}
	if item.Priority < 1 || item.Priority > 100 {
		return errors.New("agent goal priority must be between 1 and 100")
	}
	var err error
	if item.Title, err = requiredString(item.Title, "title"); err != nil {
		return err
	}
	if item.Objective, err = requiredString(item.Objective, "objective"); err != nil {
		return err
	}
	item.Context, err = normalizedJSON(item.Context)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	item.UpdatedAt = now
	if terminalAgentGoal(item.Status) && item.CompletedAt == nil {
		item.CompletedAt = &now
	}
	if !terminalAgentGoal(item.Status) {
		item.CompletedAt = nil
	}
	result, err := s.db.ExecContext(ctx, `UPDATE agent_goals SET parent_goal_id=?,conversation_id=?,title=?,objective=?,status=?,
		priority=?,risk_level=?,source=?,context_json=?,plan_hash=?,created_by=?,deadline_at=?,last_error=?,updated_at=?,completed_at=?
		WHERE id=?`, item.ParentGoalID, item.ConversationID, item.Title, item.Objective, item.Status, item.Priority,
		item.RiskLevel, item.Source, string(item.Context), item.PlanHash, item.CreatedBy, formatOptionalTime(item.DeadlineAt),
		item.LastError, formatTime(item.UpdatedAt), formatOptionalTime(item.CompletedAt), item.ID)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("agent goal not found")
	}
	return nil
}

func (s *Store) ListAgentGoals(ctx context.Context, status string, limit int) ([]model.AgentGoal, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	query := `SELECT id,parent_goal_id,conversation_id,title,objective,status,priority,risk_level,source,context_json,
		plan_hash,created_by,deadline_at,last_error,created_at,updated_at,completed_at FROM agent_goals`
	args := make([]any, 0, 2)
	if status = strings.TrimSpace(status); status != "" {
		if !validAgentGoalStatus(status) {
			return nil, errors.New("invalid agent goal status")
		}
		query += ` WHERE status=?`
		args = append(args, status)
	}
	query += ` ORDER BY priority DESC,created_at,id LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AgentGoal, 0)
	for rows.Next() {
		item, err := scanAgentGoal(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

type agentGoalScanner interface {
	Scan(...any) error
}

func scanAgentGoal(scanner agentGoalScanner) (model.AgentGoal, error) {
	var item model.AgentGoal
	var parentID, conversationID sql.NullInt64
	var contextJSON, createdAt, updatedAt string
	var deadlineAt, completedAt sql.NullString
	err := scanner.Scan(&item.ID, &parentID, &conversationID, &item.Title, &item.Objective, &item.Status,
		&item.Priority, &item.RiskLevel, &item.Source, &contextJSON, &item.PlanHash, &item.CreatedBy,
		&deadlineAt, &item.LastError, &createdAt, &updatedAt, &completedAt)
	if err != nil {
		return item, err
	}
	item.ParentGoalID = nullableInt64(parentID)
	item.ConversationID = nullableInt64(conversationID)
	item.Context = json.RawMessage(contextJSON)
	item.DeadlineAt = parseNullableTime(deadlineAt)
	item.CreatedAt, item.UpdatedAt = parseTime(createdAt), parseTime(updatedAt)
	item.CompletedAt = parseNullableTime(completedAt)
	return item, nil
}

func (s *Store) CreateAgentStep(ctx context.Context, item *model.AgentStep) error {
	if item == nil || item.GoalID <= 0 || item.Sequence < 1 {
		return errors.New("invalid agent step")
	}
	var err error
	if item.Capability, err = requiredString(item.Capability, "capability"); err != nil {
		return err
	}
	if item.IdempotencyKey, err = requiredString(item.IdempotencyKey, "idempotency_key"); err != nil {
		return err
	}
	if item.Status == "" {
		item.Status = model.AgentStepStatusPending
	}
	if !validAgentStepStatus(item.Status) {
		return errors.New("invalid agent step status")
	}
	if item.RiskLevel == "" {
		item.RiskLevel = model.AgentRiskLow
	}
	if item.MaxAttempts == 0 {
		item.MaxAttempts = 1
	}
	if item.MaxAttempts < 1 || item.MaxAttempts > 100 {
		return errors.New("agent step max attempts must be between 1 and 100")
	}
	for target, value := range map[string]*json.RawMessage{
		"arguments": &item.Arguments, "preconditions": &item.Preconditions, "compensation": &item.Compensation,
		"before_state": &item.BeforeState, "after_state": &item.AfterState, "result": &item.Result,
	} {
		*value, err = normalizedJSON(*value)
		if err != nil {
			return fmt.Errorf("%s: %w", target, err)
		}
	}
	now := time.Now().UTC()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	if terminalAgentStep(item.Status) && item.CompletedAt == nil {
		item.CompletedAt = &now
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO agent_steps(goal_id,sequence,depends_on_step_id,capability,arguments_json,
		preconditions_json,compensation_json,status,risk_level,idempotency_key,scheduled_for,expires_at,lease_owner,lease_until,
		attempt_count,max_attempts,mutation_attempted_at,before_state,after_state,result_json,last_error,created_at,updated_at,completed_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, item.GoalID, item.Sequence, item.DependsOnStepID,
		item.Capability, string(item.Arguments), string(item.Preconditions), string(item.Compensation), item.Status,
		item.RiskLevel, item.IdempotencyKey, formatOptionalTime(item.ScheduledFor), formatOptionalTime(item.ExpiresAt),
		item.LeaseOwner, formatOptionalTime(item.LeaseUntil), item.AttemptCount, item.MaxAttempts,
		formatOptionalTime(item.MutationAttemptedAt), string(item.BeforeState),
		string(item.AfterState), string(item.Result), item.LastError, formatTime(item.CreatedAt), formatTime(item.UpdatedAt),
		formatOptionalTime(item.CompletedAt))
	if err != nil {
		return err
	}
	item.ID, err = result.LastInsertId()
	return err
}

func (s *Store) GetAgentStep(ctx context.Context, id int64) (model.AgentStep, error) {
	row := s.db.QueryRowContext(ctx, agentStepSelect+` WHERE id=?`, id)
	return scanAgentStep(row)
}

func (s *Store) GetAgentStepByIdempotencyKey(ctx context.Context, key string) (model.AgentStep, error) {
	row := s.db.QueryRowContext(ctx, agentStepSelect+` WHERE idempotency_key=?`, strings.TrimSpace(key))
	return scanAgentStep(row)
}

func (s *Store) UpdateAgentStep(ctx context.Context, item model.AgentStep) error {
	if item.ID <= 0 || !validAgentStepStatus(item.Status) {
		return errors.New("invalid agent step")
	}
	var err error
	for target, value := range map[string]*json.RawMessage{
		"arguments": &item.Arguments, "preconditions": &item.Preconditions, "compensation": &item.Compensation,
		"before_state": &item.BeforeState, "after_state": &item.AfterState, "result": &item.Result,
	} {
		*value, err = normalizedJSON(*value)
		if err != nil {
			return fmt.Errorf("%s: %w", target, err)
		}
	}
	now := time.Now().UTC()
	item.UpdatedAt = now
	if terminalAgentStep(item.Status) && item.CompletedAt == nil {
		item.CompletedAt = &now
	}
	if !terminalAgentStep(item.Status) {
		item.CompletedAt = nil
	}
	result, err := s.db.ExecContext(ctx, `UPDATE agent_steps SET depends_on_step_id=?,capability=?,arguments_json=?,
		preconditions_json=?,compensation_json=?,status=?,risk_level=?,scheduled_for=?,expires_at=?,lease_owner=?,lease_until=?,
		attempt_count=?,max_attempts=?,mutation_attempted_at=?,before_state=?,after_state=?,result_json=?,last_error=?,updated_at=?,completed_at=? WHERE id=?`,
		item.DependsOnStepID, item.Capability, string(item.Arguments), string(item.Preconditions), string(item.Compensation),
		item.Status, item.RiskLevel, formatOptionalTime(item.ScheduledFor), formatOptionalTime(item.ExpiresAt), item.LeaseOwner,
		formatOptionalTime(item.LeaseUntil), item.AttemptCount, item.MaxAttempts, formatOptionalTime(item.MutationAttemptedAt),
		string(item.BeforeState), string(item.AfterState),
		string(item.Result), item.LastError, formatTime(item.UpdatedAt), formatOptionalTime(item.CompletedAt), item.ID)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("agent step not found")
	}
	return nil
}

// RecordAgentStepMutationAttempt atomically persists the read-only baseline
// and opens the external-write gate. ExecuteCapability must never be entered
// for a mutating step unless this update succeeds.
func (s *Store) RecordAgentStepMutationAttempt(ctx context.Context, id int64, beforeState,
	preconditions json.RawMessage, attemptedAt time.Time) error {
	if id <= 0 || attemptedAt.IsZero() {
		return errors.New("invalid agent step mutation attempt")
	}
	var err error
	if beforeState, err = normalizedJSON(beforeState); err != nil {
		return fmt.Errorf("before_state: %w", err)
	}
	if preconditions, err = normalizedJSON(preconditions); err != nil {
		return fmt.Errorf("preconditions: %w", err)
	}
	if !mutationEvidenceTimeMatches(preconditions, attemptedAt) {
		return errors.New("agent step mutation evidence does not match attempted time")
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `UPDATE agent_steps SET before_state=?,preconditions_json=?,
		mutation_attempted_at=?,attempt_count=attempt_count+1,updated_at=?
		WHERE id=? AND status=? AND mutation_attempted_at IS NULL`, string(beforeState), string(preconditions),
		formatTime(attemptedAt.UTC()), formatTime(now), id, model.AgentStepStatusRunning)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("agent step is not eligible to enter a mutation")
	}
	return nil
}

func (s *Store) ListAgentSteps(ctx context.Context, goalID int64) ([]model.AgentStep, error) {
	rows, err := s.db.QueryContext(ctx, agentStepSelect+` WHERE goal_id=? ORDER BY sequence,id`, goalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AgentStep, 0)
	for rows.Next() {
		item, err := scanAgentStep(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ListAgentStepsByStatus is used by the recovery loop to inspect uncertain
// work without having to reopen every historical goal. Reconciling steps are
// never claimed for execution by this query.
func (s *Store) ListAgentStepsByStatus(ctx context.Context, status string, limit int) ([]model.AgentStep, error) {
	status = strings.TrimSpace(status)
	if !validAgentStepStatus(status) {
		return nil, errors.New("invalid agent step status")
	}
	if limit < 1 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, agentStepSelect+` WHERE status=? ORDER BY updated_at,id LIMIT ?`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AgentStep, 0)
	for rows.Next() {
		item, scanErr := scanAgentStep(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const agentStepSelect = `SELECT id,goal_id,sequence,depends_on_step_id,capability,arguments_json,preconditions_json,
	compensation_json,status,risk_level,idempotency_key,scheduled_for,expires_at,lease_owner,lease_until,attempt_count,
	max_attempts,mutation_attempted_at,before_state,after_state,result_json,last_error,created_at,updated_at,completed_at FROM agent_steps`

type agentStepScanner interface {
	Scan(...any) error
}

func scanAgentStep(scanner agentStepScanner) (model.AgentStep, error) {
	var item model.AgentStep
	var dependency sql.NullInt64
	var arguments, preconditions, compensation, beforeState, afterState, result, createdAt, updatedAt string
	var scheduledFor, expiresAt, leaseUntil, mutationAttemptedAt, completedAt sql.NullString
	err := scanner.Scan(&item.ID, &item.GoalID, &item.Sequence, &dependency, &item.Capability, &arguments,
		&preconditions, &compensation, &item.Status, &item.RiskLevel, &item.IdempotencyKey, &scheduledFor,
		&expiresAt, &item.LeaseOwner, &leaseUntil, &item.AttemptCount, &item.MaxAttempts, &mutationAttemptedAt, &beforeState,
		&afterState, &result, &item.LastError, &createdAt, &updatedAt, &completedAt)
	if err != nil {
		return item, err
	}
	item.DependsOnStepID = nullableInt64(dependency)
	item.Arguments, item.Preconditions, item.Compensation = json.RawMessage(arguments), json.RawMessage(preconditions), json.RawMessage(compensation)
	item.BeforeState, item.AfterState, item.Result = json.RawMessage(beforeState), json.RawMessage(afterState), json.RawMessage(result)
	item.ScheduledFor, item.ExpiresAt, item.LeaseUntil = parseNullableTime(scheduledFor), parseNullableTime(expiresAt), parseNullableTime(leaseUntil)
	item.MutationAttemptedAt = parseNullableTime(mutationAttemptedAt)
	item.CreatedAt, item.UpdatedAt, item.CompletedAt = parseTime(createdAt), parseTime(updatedAt), parseNullableTime(completedAt)
	return item, nil
}

func (s *Store) AppendAgentEvent(ctx context.Context, item *model.AgentEvent) (bool, error) {
	if item == nil {
		return false, errors.New("agent event is nil")
	}
	var err error
	if item.EventKey, err = requiredString(item.EventKey, "event_key"); err != nil {
		return false, err
	}
	if item.Type, err = requiredString(item.Type, "type"); err != nil {
		return false, err
	}
	if item.Severity == "" {
		item.Severity = "info"
	}
	if item.Actor == "" {
		item.Actor = "system"
	}
	item.Payload, err = normalizedJSON(item.Payload)
	if err != nil {
		return false, err
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO agent_v2_events(event_key,goal_id,step_id,type,severity,actor,payload_json,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, item.EventKey, item.GoalID, item.StepID, item.Type, item.Severity, item.Actor,
		string(item.Payload), formatTime(item.CreatedAt))
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if count == 1 {
		item.ID, err = result.LastInsertId()
		return true, err
	}
	var createdAt, payload string
	var goalID, stepID sql.NullInt64
	err = s.db.QueryRowContext(ctx, `SELECT id,goal_id,step_id,type,severity,actor,payload_json,created_at
		FROM agent_v2_events WHERE event_key=?`, item.EventKey).Scan(&item.ID, &goalID, &stepID, &item.Type,
		&item.Severity, &item.Actor, &payload, &createdAt)
	item.GoalID, item.StepID = nullableInt64(goalID), nullableInt64(stepID)
	item.Payload, item.CreatedAt = json.RawMessage(payload), parseTime(createdAt)
	return false, err
}

func (s *Store) ListAgentEvents(ctx context.Context, goalID, afterID int64, limit int) ([]model.AgentEvent, error) {
	if limit < 1 || limit > 1000 {
		limit = 200
	}
	query := `SELECT id,event_key,goal_id,step_id,type,severity,actor,payload_json,created_at FROM agent_v2_events WHERE id>?`
	args := []any{afterID}
	if goalID > 0 {
		query += ` AND goal_id=?`
		args = append(args, goalID)
	}
	query += ` ORDER BY id LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AgentEvent, 0)
	for rows.Next() {
		var item model.AgentEvent
		var goal, step sql.NullInt64
		var payload, createdAt string
		if err := rows.Scan(&item.ID, &item.EventKey, &goal, &step, &item.Type, &item.Severity, &item.Actor, &payload, &createdAt); err != nil {
			return nil, err
		}
		item.GoalID, item.StepID = nullableInt64(goal), nullableInt64(step)
		item.Payload, item.CreatedAt = json.RawMessage(payload), parseTime(createdAt)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) SaveAgentCheckpoint(ctx context.Context, item *model.AgentCheckpoint) error {
	if item == nil || item.GoalID <= 0 {
		return errors.New("invalid agent checkpoint")
	}
	var err error
	if item.Kind, err = requiredString(item.Kind, "kind"); err != nil {
		return err
	}
	if item.StateHash, err = requiredString(item.StateHash, "state_hash"); err != nil {
		return err
	}
	item.State, err = normalizedJSON(item.State)
	if err != nil {
		return err
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO agent_checkpoints(goal_id,step_id,kind,state_json,state_hash,created_at)
		VALUES(?,?,?,?,?,?)`, item.GoalID, item.StepID, item.Kind, string(item.State), item.StateHash, formatTime(item.CreatedAt))
	if err != nil {
		return err
	}
	item.ID, err = result.LastInsertId()
	return err
}

func (s *Store) LatestAgentCheckpoint(ctx context.Context, goalID int64) (model.AgentCheckpoint, error) {
	var item model.AgentCheckpoint
	var stepID sql.NullInt64
	var state, createdAt string
	err := s.db.QueryRowContext(ctx, `SELECT id,goal_id,step_id,kind,state_json,state_hash,created_at
		FROM agent_checkpoints WHERE goal_id=? ORDER BY created_at DESC,id DESC LIMIT 1`, goalID).Scan(&item.ID,
		&item.GoalID, &stepID, &item.Kind, &state, &item.StateHash, &createdAt)
	if err != nil {
		return item, err
	}
	item.StepID, item.State, item.CreatedAt = nullableInt64(stepID), json.RawMessage(state), parseTime(createdAt)
	return item, nil
}

func (s *Store) UpsertAgentMemory(ctx context.Context, item *model.AgentMemory) error {
	if item == nil {
		return errors.New("agent memory is nil")
	}
	var err error
	if item.ScopeType, err = requiredString(item.ScopeType, "scope_type"); err != nil {
		return err
	}
	if item.Kind, err = requiredString(item.Kind, "kind"); err != nil {
		return err
	}
	if item.Key, err = requiredString(item.Key, "key"); err != nil {
		return err
	}
	if item.Importance < 0 || item.Importance > 1 {
		return errors.New("agent memory importance must be between 0 and 1")
	}
	item.Content, err = normalizedJSON(item.Content)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	_, err = s.db.ExecContext(ctx, `INSERT INTO agent_memories(scope_type,scope_id,kind,memory_key,summary,content_json,importance,
		pinned,expires_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(scope_type,scope_id,kind,memory_key) DO UPDATE SET summary=excluded.summary,content_json=excluded.content_json,
		importance=excluded.importance,pinned=excluded.pinned,expires_at=excluded.expires_at,updated_at=excluded.updated_at`,
		item.ScopeType, strings.TrimSpace(item.ScopeID), item.Kind, item.Key, item.Summary, string(item.Content), item.Importance,
		boolInt(item.Pinned), formatOptionalTime(item.ExpiresAt), formatTime(item.CreatedAt), formatTime(item.UpdatedAt))
	if err != nil {
		return err
	}
	stored, err := s.GetAgentMemory(ctx, item.ScopeType, item.ScopeID, item.Kind, item.Key)
	if err != nil {
		return err
	}
	*item = stored
	return nil
}

func (s *Store) GetAgentMemory(ctx context.Context, scopeType, scopeID, kind, key string) (model.AgentMemory, error) {
	row := s.db.QueryRowContext(ctx, agentMemorySelect+` WHERE scope_type=? AND scope_id=? AND kind=? AND memory_key=?`,
		strings.TrimSpace(scopeType), strings.TrimSpace(scopeID), strings.TrimSpace(kind), strings.TrimSpace(key))
	return scanAgentMemory(row)
}

func (s *Store) ListAgentMemories(ctx context.Context, scopeType, scopeID string, limit int) ([]model.AgentMemory, error) {
	if limit < 1 || limit > 1000 {
		limit = 200
	}
	query := agentMemorySelect + ` WHERE (expires_at IS NULL OR expires_at>?)`
	args := []any{formatTime(time.Now().UTC())}
	if scopeType = strings.TrimSpace(scopeType); scopeType != "" {
		query += ` AND scope_type=?`
		args = append(args, scopeType)
	}
	if scopeID = strings.TrimSpace(scopeID); scopeID != "" {
		query += ` AND scope_id=?`
		args = append(args, scopeID)
	}
	query += ` ORDER BY pinned DESC,importance DESC,updated_at DESC,id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AgentMemory, 0)
	for rows.Next() {
		item, err := scanAgentMemory(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) DeleteAgentMemory(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM agent_memories WHERE id=?`, id)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("agent memory not found")
	}
	return nil
}

const agentMemorySelect = `SELECT id,scope_type,scope_id,kind,memory_key,summary,content_json,importance,pinned,
	expires_at,created_at,updated_at FROM agent_memories`

type agentMemoryScanner interface {
	Scan(...any) error
}

func scanAgentMemory(scanner agentMemoryScanner) (model.AgentMemory, error) {
	var item model.AgentMemory
	var content, createdAt, updatedAt string
	var pinned int
	var expiresAt sql.NullString
	err := scanner.Scan(&item.ID, &item.ScopeType, &item.ScopeID, &item.Kind, &item.Key, &item.Summary,
		&content, &item.Importance, &pinned, &expiresAt, &createdAt, &updatedAt)
	if err != nil {
		return item, err
	}
	item.Content, item.Pinned = json.RawMessage(content), pinned == 1
	item.ExpiresAt = parseNullableTime(expiresAt)
	item.CreatedAt, item.UpdatedAt = parseTime(createdAt), parseTime(updatedAt)
	return item, nil
}

func (s *Store) CreateScheduledCommand(ctx context.Context, item *model.ScheduledCommand) error {
	if item == nil || item.ExecuteAt.IsZero() {
		return errors.New("invalid scheduled command")
	}
	var err error
	if item.Capability, err = requiredString(item.Capability, "capability"); err != nil {
		return err
	}
	if item.IdempotencyKey, err = requiredString(item.IdempotencyKey, "idempotency_key"); err != nil {
		return err
	}
	if item.Status == "" {
		item.Status = model.AgentCommandStatusPending
	}
	if item.Status != model.AgentCommandStatusPending {
		return errors.New("new scheduled command must be pending")
	}
	if item.Timezone == "" {
		item.Timezone = model.AgentDefaultTimezone
	}
	if _, err := time.LoadLocation(item.Timezone); err != nil {
		return errors.New("invalid scheduled command timezone")
	}
	if item.MaxAttempts == 0 {
		item.MaxAttempts = 3
	}
	if item.MaxAttempts < 1 || item.MaxAttempts > 100 {
		return errors.New("scheduled command max attempts must be between 1 and 100")
	}
	item.Arguments, err = normalizedJSON(item.Arguments)
	if err != nil {
		return fmt.Errorf("arguments: %w", err)
	}
	item.Conditions, err = normalizedJSON(item.Conditions)
	if err != nil {
		return fmt.Errorf("conditions: %w", err)
	}
	item.Result, err = normalizedJSON(item.Result)
	if err != nil {
		return fmt.Errorf("result: %w", err)
	}
	if item.CreatedBy == "" {
		item.CreatedBy = "system"
	}
	now := time.Now().UTC()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	result, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO agent_scheduled_commands(goal_id,step_id,capability,
		arguments_json,conditions_json,status,timezone,execute_at,expires_at,idempotency_key,lease_owner,lease_until,
		attempt_count,max_attempts,mutation_attempted_at,result_json,last_error,created_by,created_at,updated_at,completed_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, item.GoalID, item.StepID, item.Capability, string(item.Arguments),
		string(item.Conditions), item.Status, item.Timezone, formatTime(item.ExecuteAt), formatOptionalTime(item.ExpiresAt),
		item.IdempotencyKey, "", nil, 0, item.MaxAttempts, formatOptionalTime(item.MutationAttemptedAt), string(item.Result), item.LastError, item.CreatedBy,
		formatTime(item.CreatedAt), formatTime(item.UpdatedAt), nil)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 1 {
		item.ID, err = result.LastInsertId()
		return err
	}
	existing, err := s.GetScheduledCommandByIdempotencyKey(ctx, item.IdempotencyKey)
	if err != nil {
		return err
	}
	*item = existing
	return nil
}

func (s *Store) GetScheduledCommand(ctx context.Context, id int64) (model.ScheduledCommand, error) {
	row := s.db.QueryRowContext(ctx, scheduledCommandSelect+` WHERE id=?`, id)
	return scanScheduledCommand(row)
}

func (s *Store) GetScheduledCommandByIdempotencyKey(ctx context.Context, key string) (model.ScheduledCommand, error) {
	row := s.db.QueryRowContext(ctx, scheduledCommandSelect+` WHERE idempotency_key=?`, strings.TrimSpace(key))
	return scanScheduledCommand(row)
}

func (s *Store) ListScheduledCommands(ctx context.Context, status string, goalID int64, limit int) ([]model.ScheduledCommand, error) {
	if limit < 1 || limit > 1000 {
		limit = 200
	}
	query := scheduledCommandSelect + ` WHERE 1=1`
	args := make([]any, 0, 3)
	if status = strings.TrimSpace(status); status != "" {
		if !validAgentCommandStatus(status) {
			return nil, errors.New("invalid scheduled command status")
		}
		query += ` AND status=?`
		args = append(args, status)
	}
	if goalID > 0 {
		query += ` AND goal_id=?`
		args = append(args, goalID)
	}
	query += ` ORDER BY execute_at,id LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.ScheduledCommand, 0)
	for rows.Next() {
		item, err := scanScheduledCommand(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ClaimDueScheduledCommands atomically leases pending commands. An expired
// lease is quarantined only after its mutation gate was durably opened; a
// lease lost before that marker is safe to release and claim again.
func (s *Store) ClaimDueScheduledCommands(ctx context.Context, worker string, now time.Time, leaseDuration time.Duration, limit int) ([]model.ScheduledCommand, error) {
	worker = strings.TrimSpace(worker)
	if worker == "" || leaseDuration <= 0 {
		return nil, errors.New("worker and positive lease duration are required")
	}
	if limit < 1 || limit > 100 {
		limit = 20
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	nowText := formatTime(now)
	if _, err := tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,
		attempt_count=MAX(attempt_count-1,0),last_error='执行租约在外部写入前失联，已安全重新排队',
		completed_at=NULL,updated_at=? WHERE status=? AND lease_until IS NOT NULL AND lease_until<=?
		AND mutation_attempted_at IS NULL`, model.AgentCommandStatusPending, nowText,
		model.AgentCommandStatusLeased, nowText); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,
		last_error=CASE WHEN last_error='' THEN '执行租约已失联，必须回读实际状态' ELSE last_error END,updated_at=?
		WHERE status=? AND lease_until IS NOT NULL AND lease_until<=? AND mutation_attempted_at IS NOT NULL`, model.AgentCommandStatusReconciling, nowText,
		model.AgentCommandStatusLeased, nowText); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,last_error='任务已超过有效期',
		completed_at=?,updated_at=? WHERE status=? AND expires_at IS NOT NULL AND expires_at<=?`,
		model.AgentCommandStatusExpired, nowText, nowText, model.AgentCommandStatusPending, nowText); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,last_error='任务重试次数已耗尽',
		completed_at=?,updated_at=? WHERE status=? AND attempt_count>=max_attempts`, model.AgentCommandStatusFailed,
		nowText, nowText, model.AgentCommandStatusPending); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM agent_scheduled_commands WHERE status=? AND execute_at<=?
		AND attempt_count<max_attempts AND (expires_at IS NULL OR expires_at>?) ORDER BY execute_at,id LIMIT ?`,
		model.AgentCommandStatusPending, nowText, nowText, limit)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	leaseUntil := now.Add(leaseDuration)
	claimed := make([]model.ScheduledCommand, 0, len(ids))
	for _, id := range ids {
		result, err := tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner=?,lease_until=?,
			attempt_count=attempt_count+1,updated_at=? WHERE id=? AND status=?`, model.AgentCommandStatusLeased,
			worker, formatTime(leaseUntil), nowText, id, model.AgentCommandStatusPending)
		if err != nil {
			return nil, err
		}
		count, _ := result.RowsAffected()
		if count != 1 {
			continue
		}
		item, err := scanScheduledCommand(tx.QueryRowContext(ctx, scheduledCommandSelect+` WHERE id=?`, id))
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, item)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *Store) RenewScheduledCommandLease(ctx context.Context, id int64, worker string, now time.Time, leaseDuration time.Duration) error {
	if id <= 0 || strings.TrimSpace(worker) == "" || leaseDuration <= 0 {
		return errors.New("invalid scheduled command lease")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE agent_scheduled_commands SET lease_until=?,updated_at=?
		WHERE id=? AND status=? AND lease_owner=? AND lease_until>?`, formatTime(now.UTC().Add(leaseDuration)),
		formatTime(now.UTC()), id, model.AgentCommandStatusLeased, strings.TrimSpace(worker), formatTime(now.UTC()))
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("scheduled command lease is not owned or has expired")
	}
	return nil
}

// DeferLeasedScheduledCommand releases a command that was claimed immediately
// before an operator freeze became visible. No capability was entered, so the
// claim is not counted as an execution attempt and no reconciliation is needed.
func (s *Store) DeferLeasedScheduledCommand(ctx context.Context, id int64, worker, message string, retryAt time.Time) error {
	if id <= 0 || strings.TrimSpace(worker) == "" || retryAt.IsZero() {
		return errors.New("invalid scheduled command deferral")
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,execute_at=?,lease_owner='',lease_until=NULL,
		attempt_count=MAX(attempt_count-1,0),mutation_attempted_at=NULL,result_json='{}',last_error=?,updated_at=?
		WHERE id=? AND status=? AND lease_owner=?`,
		model.AgentCommandStatusPending, formatTime(retryAt.UTC()), strings.TrimSpace(message), formatTime(now), id,
		model.AgentCommandStatusLeased, strings.TrimSpace(worker))
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("scheduled command lease is not owned")
	}
	return nil
}

func (s *Store) CompleteScheduledCommand(ctx context.Context, id int64, worker string, resultJSON json.RawMessage, completedAt time.Time) error {
	resultJSON, err := normalizedJSON(resultJSON)
	if err != nil {
		return err
	}
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,result_json=?,last_error='',
		lease_owner='',lease_until=NULL,completed_at=?,updated_at=? WHERE id=? AND status=? AND lease_owner=?`,
		model.AgentCommandStatusCompleted, string(resultJSON), formatTime(completedAt.UTC()), formatTime(completedAt.UTC()),
		id, model.AgentCommandStatusLeased, strings.TrimSpace(worker))
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("scheduled command lease is not owned")
	}
	return nil
}

// RecordScheduledCommandAttemptState persists the read-only baseline before a
// leased command enters its mutation. If the process disappears after the
// external write, startup recovery can compare the real state with this
// baseline instead of replaying the command.
func (s *Store) RecordScheduledCommandAttemptState(ctx context.Context, id int64, worker string,
	resultJSON json.RawMessage, attemptedAt time.Time) error {
	if attemptedAt.IsZero() {
		return errors.New("scheduled command mutation attempt time is required")
	}
	resultJSON, err := normalizedJSON(resultJSON)
	if err != nil {
		return err
	}
	if !mutationEvidenceTimeMatches(resultJSON, attemptedAt) {
		return errors.New("scheduled command mutation evidence does not match attempted time")
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `UPDATE agent_scheduled_commands SET result_json=?,mutation_attempted_at=?,updated_at=?
		WHERE id=? AND status=? AND lease_owner=? AND mutation_attempted_at IS NULL`, string(resultJSON),
		formatTime(attemptedAt.UTC()), formatTime(now), id, model.AgentCommandStatusLeased, strings.TrimSpace(worker))
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("scheduled command is not eligible to enter a mutation")
	}
	return nil
}

func mutationEvidenceTimeMatches(raw json.RawMessage, attemptedAt time.Time) bool {
	var evidence struct {
		AttemptedAt time.Time `json:"attempted_at"`
	}
	return json.Unmarshal(raw, &evidence) == nil && !evidence.AttemptedAt.IsZero() &&
		evidence.AttemptedAt.Equal(attemptedAt)
}

// FailScheduledCommand releases a leased command for a deliberate retry or
// marks it failed. retryAt must only be used after the executor has confirmed
// that no external mutation occurred.
func (s *Store) FailScheduledCommand(ctx context.Context, id int64, worker, message string, retryAt *time.Time) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var attempts, maxAttempts int
	if err := tx.QueryRowContext(ctx, `SELECT attempt_count,max_attempts FROM agent_scheduled_commands
		WHERE id=? AND status=? AND lease_owner=?`, id, model.AgentCommandStatusLeased, strings.TrimSpace(worker)).Scan(&attempts, &maxAttempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", errors.New("scheduled command lease is not owned")
		}
		return "", err
	}
	now := time.Now().UTC()
	status := model.AgentCommandStatusFailed
	var executeAt any
	var completedAt any = formatTime(now)
	if retryAt != nil && attempts < maxAttempts {
		status = model.AgentCommandStatusPending
		executeAt = formatTime(retryAt.UTC())
		completedAt = nil
	}
	if executeAt == nil {
		if err := tx.QueryRowContext(ctx, `SELECT execute_at FROM agent_scheduled_commands WHERE id=?`, id).Scan(&executeAt); err != nil {
			return "", err
		}
	}
	mutationAttemptedAt := "mutation_attempted_at"
	if status == model.AgentCommandStatusPending {
		mutationAttemptedAt = "NULL"
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,execute_at=?,lease_owner='',lease_until=NULL,
		last_error=?,completed_at=?,updated_at=?,mutation_attempted_at=`+mutationAttemptedAt+` WHERE id=?`, status,
		executeAt, strings.TrimSpace(message), completedAt, formatTime(now), id); err != nil {
		return "", err
	}
	return status, tx.Commit()
}

func (s *Store) MarkScheduledCommandReconciling(ctx context.Context, id int64, worker, message string) error {
	return s.MarkScheduledCommandReconcilingWithResult(ctx, id, worker, message, nil)
}

// MarkScheduledCommandReconcilingWithResult keeps the execution baseline and
// ambiguous result together with the quarantine transition.
func (s *Store) MarkScheduledCommandReconcilingWithResult(ctx context.Context, id int64, worker, message string, resultJSON json.RawMessage) error {
	var err error
	if len(resultJSON) > 0 {
		resultJSON, err = normalizedJSON(resultJSON)
		if err != nil {
			return err
		}
	}
	now := time.Now().UTC()
	query := `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,last_error=?,updated_at=?`
	args := []any{model.AgentCommandStatusReconciling, strings.TrimSpace(message), formatTime(now)}
	if len(resultJSON) > 0 {
		query += `,result_json=?`
		args = append(args, string(resultJSON))
	}
	query += ` WHERE id=? AND status=? AND lease_owner=?`
	args = append(args, id, model.AgentCommandStatusLeased, strings.TrimSpace(worker))
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("scheduled command lease is not owned")
	}
	return nil
}

// TouchScheduledCommandReconciliation stores the latest readback while
// deliberately leaving the command quarantined. It also provides a natural
// backoff timestamp for the reconciliation worker.
func (s *Store) TouchScheduledCommandReconciliation(ctx context.Context, id int64, resultJSON json.RawMessage, message string) error {
	resultJSON, err := normalizedJSON(resultJSON)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `UPDATE agent_scheduled_commands SET result_json=?,last_error=?,updated_at=?
		WHERE id=? AND status=?`, string(resultJSON), strings.TrimSpace(message), formatTime(now), id,
		model.AgentCommandStatusReconciling)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("scheduled command is not reconciling")
	}
	return nil
}

// ResolveScheduledCommandReconciliation records the result of a mandatory
// read-back. Returning to pending is allowed only when the caller has proved
// that the original external mutation did not occur.
func (s *Store) ResolveScheduledCommandReconciliation(ctx context.Context, id int64, status string, resultJSON json.RawMessage, message string, retryAt *time.Time) error {
	if status != model.AgentCommandStatusPending && status != model.AgentCommandStatusCompleted && status != model.AgentCommandStatusFailed {
		return errors.New("invalid reconciliation result status")
	}
	resultJSON, err := normalizedJSON(resultJSON)
	if err != nil {
		return err
	}
	if status == model.AgentCommandStatusPending && retryAt == nil {
		return errors.New("retry time is required when reconciliation returns to pending")
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var attempts, maxAttempts int
	var executeAt string
	if err := tx.QueryRowContext(ctx, `SELECT attempt_count,max_attempts,execute_at FROM agent_scheduled_commands
		WHERE id=? AND status=?`, id, model.AgentCommandStatusReconciling).Scan(&attempts, &maxAttempts, &executeAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("scheduled command is not reconciling")
		}
		return err
	}
	var completedAt any = formatTime(now)
	if status == model.AgentCommandStatusPending {
		if attempts >= maxAttempts {
			return errors.New("scheduled command retry limit is exhausted")
		}
		executeAt = formatTime(retryAt.UTC())
		completedAt = nil
	}
	mutationAttemptedAt := "mutation_attempted_at"
	if status == model.AgentCommandStatusPending {
		mutationAttemptedAt = "NULL"
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,execute_at=?,result_json=?,last_error=?,
		lease_owner='',lease_until=NULL,completed_at=?,updated_at=?,mutation_attempted_at=`+mutationAttemptedAt+`
		WHERE id=? AND status=?`, status, executeAt, string(resultJSON), strings.TrimSpace(message), completedAt,
		formatTime(now), id, model.AgentCommandStatusReconciling); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CancelScheduledCommand(ctx context.Context, id int64, actor, reason string) error {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,
		last_error=?,completed_at=?,updated_at=? WHERE id=? AND status IN (?,?)`, model.AgentCommandStatusCancelled,
		strings.TrimSpace(actor)+": "+strings.TrimSpace(reason), formatTime(now), formatTime(now), id,
		model.AgentCommandStatusPending, model.AgentCommandStatusReconciling)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count != 1 {
		return errors.New("scheduled command cannot be cancelled in its current state")
	}
	return nil
}

const scheduledCommandSelect = `SELECT id,goal_id,step_id,capability,arguments_json,conditions_json,status,timezone,
	execute_at,expires_at,idempotency_key,lease_owner,lease_until,attempt_count,max_attempts,mutation_attempted_at,result_json,last_error,
	created_by,created_at,updated_at,completed_at FROM agent_scheduled_commands`

type scheduledCommandScanner interface {
	Scan(...any) error
}

func scanScheduledCommand(scanner scheduledCommandScanner) (model.ScheduledCommand, error) {
	var item model.ScheduledCommand
	var goalID, stepID sql.NullInt64
	var arguments, conditions, result, executeAt, createdAt, updatedAt string
	var expiresAt, leaseUntil, mutationAttemptedAt, completedAt sql.NullString
	err := scanner.Scan(&item.ID, &goalID, &stepID, &item.Capability, &arguments, &conditions, &item.Status,
		&item.Timezone, &executeAt, &expiresAt, &item.IdempotencyKey, &item.LeaseOwner, &leaseUntil,
		&item.AttemptCount, &item.MaxAttempts, &mutationAttemptedAt, &result, &item.LastError, &item.CreatedBy, &createdAt, &updatedAt,
		&completedAt)
	if err != nil {
		return item, err
	}
	item.GoalID, item.StepID = nullableInt64(goalID), nullableInt64(stepID)
	item.Arguments, item.Conditions, item.Result = json.RawMessage(arguments), json.RawMessage(conditions), json.RawMessage(result)
	item.ExecuteAt, item.ExpiresAt = parseTime(executeAt), parseNullableTime(expiresAt)
	item.MutationAttemptedAt = parseNullableTime(mutationAttemptedAt)
	item.LeaseUntil, item.CompletedAt = parseNullableTime(leaseUntil), parseNullableTime(completedAt)
	item.CreatedAt, item.UpdatedAt = parseTime(createdAt), parseTime(updatedAt)
	return item, nil
}

func (s *Store) SetAgentFreezeState(ctx context.Context, item *model.AgentFreezeState) error {
	if item == nil {
		return errors.New("agent freeze state is nil")
	}
	var err error
	if item.ScopeType, err = requiredString(item.ScopeType, "scope_type"); err != nil {
		return err
	}
	item.ScopeID = strings.TrimSpace(item.ScopeID)
	if item.ScopeType != "global" && item.ScopeID == "" {
		return errors.New("non-global freeze state requires scope_id")
	}
	if item.ScopeType == "global" {
		item.ScopeID = ""
	}
	if !validAgentFreezeMode(item.Mode) {
		return errors.New("invalid agent freeze mode")
	}
	if item.Actor == "" {
		item.Actor = "system"
	}
	now := time.Now().UTC()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	_, err = s.db.ExecContext(ctx, `INSERT INTO agent_freeze_states(scope_type,scope_id,mode,reason,actor,expires_at,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(scope_type,scope_id) DO UPDATE SET mode=excluded.mode,reason=excluded.reason,
		actor=excluded.actor,expires_at=excluded.expires_at,updated_at=excluded.updated_at`, item.ScopeType, item.ScopeID,
		item.Mode, strings.TrimSpace(item.Reason), item.Actor, formatOptionalTime(item.ExpiresAt), formatTime(item.CreatedAt),
		formatTime(item.UpdatedAt))
	if err != nil {
		return err
	}
	stored, err := s.GetAgentFreezeState(ctx, item.ScopeType, item.ScopeID)
	if err != nil {
		return err
	}
	*item = stored
	return nil
}

func (s *Store) GetAgentFreezeState(ctx context.Context, scopeType, scopeID string) (model.AgentFreezeState, error) {
	var item model.AgentFreezeState
	var expiresAt sql.NullString
	var createdAt, updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT scope_type,scope_id,mode,reason,actor,expires_at,created_at,updated_at
		FROM agent_freeze_states WHERE scope_type=? AND scope_id=?`, strings.TrimSpace(scopeType), strings.TrimSpace(scopeID)).Scan(
		&item.ScopeType, &item.ScopeID, &item.Mode, &item.Reason, &item.Actor, &expiresAt, &createdAt, &updatedAt)
	if err != nil {
		return item, err
	}
	item.ExpiresAt = parseNullableTime(expiresAt)
	item.CreatedAt, item.UpdatedAt = parseTime(createdAt), parseTime(updatedAt)
	return item, nil
}

func (s *Store) ListAgentFreezeStates(ctx context.Context) ([]model.AgentFreezeState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT scope_type,scope_id,mode,reason,actor,expires_at,created_at,updated_at
		FROM agent_freeze_states ORDER BY CASE scope_type WHEN 'global' THEN 0 ELSE 1 END,scope_type,scope_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AgentFreezeState, 0)
	for rows.Next() {
		var item model.AgentFreezeState
		var expiresAt sql.NullString
		var createdAt, updatedAt string
		if err := rows.Scan(&item.ScopeType, &item.ScopeID, &item.Mode, &item.Reason, &item.Actor, &expiresAt,
			&createdAt, &updatedAt); err != nil {
			return nil, err
		}
		item.ExpiresAt = parseNullableTime(expiresAt)
		item.CreatedAt, item.UpdatedAt = parseTime(createdAt), parseTime(updatedAt)
		items = append(items, item)
	}
	return items, rows.Err()
}

// CapabilitySupportsRestartReadback reports whether a mutation has a durable,
// read-only projection that can prove its external effect after a crash. Keep
// this list deliberately narrow: unknown and purely local capabilities must be
// replanned instead of being replayed or quarantined forever.
func CapabilitySupportsRestartReadback(capability string) bool {
	switch strings.TrimSpace(capability) {
	case "pause_account", "resume_account", "set_load_factor", "pin_load_until", "clear_load_pin",
		"clear_flap_protection", "clear_manual_override", "update_binding", "update_upstream_control",
		"transition_token_group_tier":
		return true
	default:
		return false
	}
}

// RecoverAgentV2State is called once by the runtime at startup. External writes
// with an observable projection are quarantined for readback. Purely local or
// otherwise unobservable work is failed closed and its goal is made eligible
// for a fresh model plan; the interrupted invocation itself is never replayed.
func (s *Store) RecoverAgentV2State(ctx context.Context, now time.Time) (model.AgentRecoverySummary, error) {
	var summary model.AgentRecoverySummary
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return summary, err
	}
	defer tx.Rollback()
	nowText := formatTime(now)
	result, err := tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,
		last_error='任务在服务停止期间超过有效期',completed_at=?,updated_at=?
		WHERE status=? AND expires_at IS NOT NULL AND expires_at<=?`, model.AgentCommandStatusExpired,
		nowText, nowText, model.AgentCommandStatusPending, nowText)
	if err != nil {
		return summary, err
	}
	count, _ := result.RowsAffected()
	summary.ExpiredCommands = int(count)
	result, err = tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,
		last_error='任务在重启前已耗尽重试次数',completed_at=?,updated_at=?
		WHERE status=? AND attempt_count>=max_attempts`, model.AgentCommandStatusFailed, nowText, nowText,
		model.AgentCommandStatusPending)
	if err != nil {
		return summary, err
	}
	count, _ = result.RowsAffected()
	summary.FailedCommands = int(count)
	replanGoals := make(map[int64]struct{})
	blockedGoals := make(map[int64]struct{})
	commandRows, err := tx.QueryContext(ctx, `SELECT id,goal_id,status,capability,mutation_attempted_at,expires_at
		FROM agent_scheduled_commands
		WHERE status IN (?,?)`, model.AgentCommandStatusLeased, model.AgentCommandStatusReconciling)
	if err != nil {
		return summary, err
	}
	type recoveryCommand struct {
		id         int64
		goalID     sql.NullInt64
		status     string
		capability string
		attempted  sql.NullString
		expiresAt  sql.NullString
	}
	commands := make([]recoveryCommand, 0)
	for commandRows.Next() {
		var command recoveryCommand
		if err := commandRows.Scan(&command.id, &command.goalID, &command.status, &command.capability,
			&command.attempted, &command.expiresAt); err != nil {
			commandRows.Close()
			return summary, err
		}
		commands = append(commands, command)
	}
	if err := commandRows.Close(); err != nil {
		return summary, err
	}
	for _, command := range commands {
		// A leased command without the durable marker never entered the
		// external mutation. Release that claim rather than quarantining it.
		if command.status == model.AgentCommandStatusLeased && !command.attempted.Valid {
			if command.expiresAt.Valid && !parseTime(command.expiresAt.String).After(now) {
				result, err = tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,
					last_error='任务在服务停止期间超过有效期，且确认尚未进入外部写入',completed_at=?,updated_at=?
					WHERE id=? AND status=? AND mutation_attempted_at IS NULL`, model.AgentCommandStatusExpired,
					nowText, nowText, command.id, model.AgentCommandStatusLeased)
				if err != nil {
					return summary, err
				}
				count, _ = result.RowsAffected()
				summary.ExpiredCommands += int(count)
				continue
			}
			result, err = tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,
				attempt_count=MAX(attempt_count-1,0),last_error='服务重启发生在外部写入前，已安全重新排队',
				completed_at=NULL,updated_at=? WHERE id=? AND status=? AND mutation_attempted_at IS NULL`,
				model.AgentCommandStatusPending, nowText, command.id, model.AgentCommandStatusLeased)
			if err != nil {
				return summary, err
			}
			count, _ = result.RowsAffected()
			summary.RequeuedCommands += int(count)
			continue
		}
		// A legacy reconciling row without a write marker cannot be proved safe
		// to retry and has no trustworthy baseline to compare. Terminate it and
		// require an explicit new administrator action instead of looping.
		if command.status == model.AgentCommandStatusReconciling && !command.attempted.Valid {
			result, err = tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,
				last_error='旧版待核对命令缺少写入边界证据，已失败关闭且不会自动重放',completed_at=?,updated_at=?
				WHERE id=? AND status=? AND mutation_attempted_at IS NULL`, model.AgentCommandStatusFailed,
				nowText, nowText, command.id, model.AgentCommandStatusReconciling)
			if err != nil {
				return summary, err
			}
			count, _ = result.RowsAffected()
			summary.FailedCommands += int(count)
			if count == 1 && command.goalID.Valid {
				blockedGoals[command.goalID.Int64] = struct{}{}
			}
			continue
		}
		if !CapabilitySupportsRestartReadback(command.capability) {
			result, err = tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,
				last_error='服务重启时该能力无法通过只读状态证明执行结果；旧命令已失败关闭，等待目标重新规划',
				completed_at=?,updated_at=? WHERE id=? AND status IN (?,?)`, model.AgentCommandStatusFailed, nowText,
				nowText, command.id, model.AgentCommandStatusLeased, model.AgentCommandStatusReconciling)
			if err != nil {
				return summary, err
			}
			count, _ = result.RowsAffected()
			summary.FailedCommands += int(count)
			if count == 1 && command.goalID.Valid {
				replanGoals[command.goalID.Int64] = struct{}{}
			}
			continue
		}
		if command.status != model.AgentCommandStatusLeased {
			continue
		}
		result, err = tx.ExecContext(ctx, `UPDATE agent_scheduled_commands SET status=?,lease_owner='',lease_until=NULL,
			last_error=CASE WHEN last_error='' THEN '服务重启，必须回读实际状态后再处理' ELSE last_error END,updated_at=?
			WHERE id=? AND status=?`, model.AgentCommandStatusReconciling, nowText, command.id, model.AgentCommandStatusLeased)
		if err != nil {
			return summary, err
		}
		count, _ = result.RowsAffected()
		summary.ReconcilingCommands += int(count)
	}

	stepRows, err := tx.QueryContext(ctx, `SELECT id,goal_id,status,capability,mutation_attempted_at FROM agent_steps
		WHERE status IN (?,?,?,?)`, model.AgentStepStatusRunning, model.AgentStepStatusVerifying,
		model.AgentStepStatusCompensating, model.AgentStepStatusReconciling)
	if err != nil {
		return summary, err
	}
	type recoveryStep struct {
		id         int64
		goalID     int64
		status     string
		capability string
		attempted  sql.NullString
	}
	steps := make([]recoveryStep, 0)
	for stepRows.Next() {
		var step recoveryStep
		if err := stepRows.Scan(&step.id, &step.goalID, &step.status, &step.capability, &step.attempted); err != nil {
			stepRows.Close()
			return summary, err
		}
		steps = append(steps, step)
	}
	if err := stepRows.Close(); err != nil {
		return summary, err
	}
	for _, step := range steps {
		if !step.attempted.Valid && step.status != model.AgentStepStatusReconciling {
			result, err = tx.ExecContext(ctx, `UPDATE agent_steps SET status=?,lease_owner='',lease_until=NULL,
				last_error='服务重启发生在外部写入前；旧步骤已结束，等待智能体基于最新状态重新规划',
				completed_at=?,updated_at=? WHERE id=? AND status IN (?,?,?) AND mutation_attempted_at IS NULL`,
				model.AgentStepStatusFailed, nowText, nowText, step.id, model.AgentStepStatusRunning,
				model.AgentStepStatusVerifying, model.AgentStepStatusCompensating)
			if err != nil {
				return summary, err
			}
			count, _ = result.RowsAffected()
			if count == 1 {
				summary.ReplannedSteps++
				replanGoals[step.goalID] = struct{}{}
			}
			continue
		}
		if !step.attempted.Valid && step.status == model.AgentStepStatusReconciling {
			result, err = tx.ExecContext(ctx, `UPDATE agent_steps SET status=?,lease_owner='',lease_until=NULL,
				last_error='旧版待核对步骤缺少写入边界证据，已失败关闭且不会自动重放',completed_at=?,updated_at=?
				WHERE id=? AND status=? AND mutation_attempted_at IS NULL`, model.AgentStepStatusFailed,
				nowText, nowText, step.id, model.AgentStepStatusReconciling)
			if err != nil {
				return summary, err
			}
			count, _ = result.RowsAffected()
			if count == 1 {
				blockedGoals[step.goalID] = struct{}{}
			}
			continue
		}
		if !CapabilitySupportsRestartReadback(step.capability) {
			result, err = tx.ExecContext(ctx, `UPDATE agent_steps SET status=?,lease_owner='',lease_until=NULL,
				last_error='服务重启时该能力无法通过只读状态证明执行结果；旧步骤已失败关闭，等待智能体重新规划',
				completed_at=?,updated_at=? WHERE id=? AND status IN (?,?,?,?)`, model.AgentStepStatusFailed, nowText,
				nowText, step.id, model.AgentStepStatusRunning, model.AgentStepStatusVerifying,
				model.AgentStepStatusCompensating, model.AgentStepStatusReconciling)
			if err != nil {
				return summary, err
			}
			count, _ = result.RowsAffected()
			if count == 1 {
				replanGoals[step.goalID] = struct{}{}
			}
			continue
		}
		if step.status == model.AgentStepStatusReconciling {
			continue
		}
		result, err = tx.ExecContext(ctx, `UPDATE agent_steps SET status=?,lease_owner='',lease_until=NULL,
			last_error=CASE WHEN last_error='' THEN '服务重启，必须核对步骤实际效果' ELSE last_error END,updated_at=?
			WHERE id=? AND status IN (?,?,?)`, model.AgentStepStatusReconciling, nowText, step.id,
			model.AgentStepStatusRunning, model.AgentStepStatusVerifying, model.AgentStepStatusCompensating)
		if err != nil {
			return summary, err
		}
		count, _ = result.RowsAffected()
		summary.ReconcilingSteps += int(count)
	}
	for goalID := range blockedGoals {
		delete(replanGoals, goalID)
		if _, err := tx.ExecContext(ctx, `UPDATE agent_goals SET status=?,
			last_error='存在缺少写入边界证据的旧动作；已失败关闭，必须由管理员明确创建新目标',updated_at=?,completed_at=?
			WHERE id=? AND status IN (?,?,?)`, model.AgentGoalStatusFailed, nowText, nowText, goalID,
			model.AgentGoalStatusRunning, model.AgentGoalStatusWaiting, model.AgentGoalStatusPlanned); err != nil {
			return summary, err
		}
	}
	for goalID := range replanGoals {
		if _, err := tx.ExecContext(ctx, `UPDATE agent_goals SET status=?,
			last_error='服务重启已失败关闭不可核对的旧动作，等待基于最新状态重新规划',updated_at=?,completed_at=NULL
			WHERE id=? AND status IN (?,?,?)`, model.AgentGoalStatusPlanned, nowText, goalID,
			model.AgentGoalStatusRunning, model.AgentGoalStatusWaiting, model.AgentGoalStatusPlanned); err != nil {
			return summary, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_goals SET status=?,
		last_error=CASE WHEN last_error='' THEN '服务重启，已从最近检查点恢复等待续跑' ELSE last_error END,
		updated_at=?,completed_at=NULL WHERE status=?`, model.AgentGoalStatusPlanned, nowText, model.AgentGoalStatusRunning); err != nil {
		return summary, err
	}
	if err := tx.Commit(); err != nil {
		return summary, err
	}
	return summary, nil
}

// CleanupAgentV2Data applies one retention boundary to every V2 historical
// object. Pinned memories affect ranking only and do not bypass retention.
func (s *Store) CleanupAgentV2Data(ctx context.Context, before time.Time) error {
	if before.IsZero() {
		return errors.New("cleanup boundary is required")
	}
	cutoff := formatTime(before.UTC())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	statements := []struct {
		query string
		args  []any
	}{
		{`DELETE FROM agent_administrator_grant_consumptions WHERE consumed_at<?`, []any{cutoff}},
		{`DELETE FROM agent_v2_events WHERE created_at<?`, []any{cutoff}},
		{`DELETE FROM agent_checkpoints WHERE created_at<?`, []any{cutoff}},
		{`DELETE FROM agent_memories WHERE updated_at<?`, []any{cutoff}},
		{`DELETE FROM agent_scheduled_commands WHERE status IN (?,?,?,?) AND COALESCE(completed_at,updated_at)<?`,
			[]any{model.AgentCommandStatusCompleted, model.AgentCommandStatusFailed, model.AgentCommandStatusCancelled,
				model.AgentCommandStatusExpired, cutoff}},
		{`DELETE FROM agent_steps WHERE status IN (?,?,?,?) AND COALESCE(completed_at,updated_at)<?`,
			[]any{model.AgentStepStatusCompleted, model.AgentStepStatusFailed, model.AgentStepStatusCancelled,
				model.AgentStepStatusSkipped, cutoff}},
		{`DELETE FROM agent_goals WHERE status IN (?,?,?) AND COALESCE(completed_at,updated_at)<?`,
			[]any{model.AgentGoalStatusCompleted, model.AgentGoalStatusFailed, model.AgentGoalStatusCancelled, cutoff}},
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}
