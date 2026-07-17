package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func (s *Store) migrateTelemetry(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS monitor_history_records (
			monitor_id INTEGER NOT NULL,
			model TEXT NOT NULL,
			checked_at TEXT NOT NULL,
			source_id INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL,
			latency_ms INTEGER NOT NULL DEFAULT 0,
			ping_latency_ms INTEGER NOT NULL DEFAULT 0,
			status_code INTEGER NOT NULL DEFAULT 0,
			error_class TEXT NOT NULL DEFAULT '',
			reason_code TEXT NOT NULL DEFAULT '',
			reason_fingerprint TEXT NOT NULL DEFAULT '',
			ingested_at TEXT NOT NULL,
			PRIMARY KEY(monitor_id,model,checked_at)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_history_recent ON monitor_history_records(monitor_id,checked_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_history_model_recent ON monitor_history_records(monitor_id,model,checked_at DESC)`,
		`CREATE TABLE IF NOT EXISTS traffic_events (
			event_key TEXT PRIMARY KEY,
			account_id INTEGER NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			requested_model TEXT NOT NULL DEFAULT '',
			upstream_model TEXT NOT NULL DEFAULT '',
			kind TEXT NOT NULL,
			duration_ms INTEGER NOT NULL DEFAULT 0,
			request_kind TEXT NOT NULL DEFAULT '',
			phase TEXT NOT NULL DEFAULT '',
			error_type TEXT NOT NULL DEFAULT '',
			severity TEXT NOT NULL DEFAULT '',
			status_code INTEGER NOT NULL DEFAULT 0,
			error_class TEXT NOT NULL DEFAULT '',
			reason_code TEXT NOT NULL DEFAULT '',
			reason_fingerprint TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			request_started_at TEXT,
			ingested_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_traffic_events_account_created ON traffic_events(account_id,created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_traffic_events_account_model_created ON traffic_events(account_id,model,created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_traffic_events_created ON traffic_events(created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_traffic_events_class_created ON traffic_events(error_class,created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS account_model_capabilities (
			account_id INTEGER NOT NULL,
			model TEXT NOT NULL,
			supported INTEGER NOT NULL,
			success_count INTEGER NOT NULL DEFAULT 0,
			failure_count INTEGER NOT NULL DEFAULT 0,
			last_error_class TEXT NOT NULL DEFAULT '',
			last_reason_code TEXT NOT NULL DEFAULT '',
			last_observed_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(account_id,model)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_account_model_capabilities_supported ON account_model_capabilities(account_id,supported,model)`,
		`CREATE TABLE IF NOT EXISTS decision_snapshots (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			decision_id TEXT NOT NULL UNIQUE,
			monitor_id INTEGER,
			account_id INTEGER,
			checked_at TEXT NOT NULL,
			availability_state TEXT NOT NULL DEFAULT '',
			load_stage TEXT NOT NULL DEFAULT '',
			target_load_percent INTEGER NOT NULL DEFAULT 0,
			action TEXT NOT NULL DEFAULT '',
			action_result TEXT NOT NULL DEFAULT '',
			reason_code TEXT NOT NULL DEFAULT '',
			evidence_json TEXT NOT NULL DEFAULT '{}',
			created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_decision_snapshots_account_created ON decision_snapshots(account_id,created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_decision_snapshots_monitor_created ON decision_snapshots(monitor_id,created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_decision_snapshots_checked ON decision_snapshots(checked_at DESC)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate telemetry database: %w", err)
		}
	}
	if err := s.ensureColumn(ctx, "traffic_events", "request_started_at", "TEXT"); err != nil {
		return fmt.Errorf("migrate traffic request start: %w", err)
	}
	return nil
}

func (s *Store) InsertMonitorHistory(ctx context.Context, item model.MonitorHistoryRecord) (bool, error) {
	return s.InsertMonitorHistoryBatch(ctx, []model.MonitorHistoryRecord{item})
}

// InsertMonitorHistoryBatch is idempotent by monitor, model and checked_at.
func (s *Store) InsertMonitorHistoryBatch(ctx context.Context, items []model.MonitorHistoryRecord) (bool, error) {
	if len(items) == 0 {
		return false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	insertedAny := false
	for _, item := range items {
		if item.MonitorID <= 0 || strings.TrimSpace(item.Model) == "" || item.CheckedAt.IsZero() {
			return false, errors.New("monitor_id, model and checked_at are required")
		}
		if item.IngestedAt.IsZero() {
			item.IngestedAt = time.Now().UTC()
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO monitor_history_records(
			monitor_id,model,checked_at,source_id,status,latency_ms,ping_latency_ms,status_code,error_class,reason_code,reason_fingerprint,ingested_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(monitor_id,model,checked_at) DO NOTHING`,
			item.MonitorID, item.Model, formatTelemetryTime(item.CheckedAt), item.SourceID, item.Status, item.LatencyMS,
			item.PingLatencyMS, item.StatusCode, safeTelemetryToken(item.ErrorClass), safeTelemetryToken(item.ReasonCode),
			telemetryFingerprint(item.ReasonFingerprint), formatTelemetryTime(item.IngestedAt))
		if err != nil {
			return false, err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return false, err
		}
		insertedAny = insertedAny || changed > 0
	}
	return insertedAny, tx.Commit()
}

// ListMonitorHistory returns newest-first results. modelName may be empty to
// include every monitored model; limit is normally 10 or 60.
func (s *Store) ListMonitorHistory(ctx context.Context, monitorID int64, modelName string, limit int) ([]model.MonitorHistoryRecord, error) {
	if monitorID <= 0 {
		return nil, errors.New("monitor_id must be positive")
	}
	if limit <= 0 || limit > 10_000 {
		limit = 60
	}
	query := `SELECT source_id,monitor_id,model,status,latency_ms,ping_latency_ms,status_code,error_class,reason_code,reason_fingerprint,checked_at,ingested_at
		FROM monitor_history_records WHERE monitor_id=?`
	args := []any{monitorID}
	if strings.TrimSpace(modelName) != "" {
		query += ` AND model=?`
		args = append(args, strings.TrimSpace(modelName))
	}
	query += ` ORDER BY checked_at DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.MonitorHistoryRecord, 0)
	for rows.Next() {
		var item model.MonitorHistoryRecord
		var checkedAt, ingestedAt string
		if err := rows.Scan(&item.SourceID, &item.MonitorID, &item.Model, &item.Status, &item.LatencyMS,
			&item.PingLatencyMS, &item.StatusCode, &item.ErrorClass, &item.ReasonCode, &item.ReasonFingerprint,
			&checkedAt, &ingestedAt); err != nil {
			return nil, err
		}
		item.CheckedAt, item.IngestedAt = parseTime(checkedAt), parseTime(ingestedAt)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) InsertTrafficSuccesses(ctx context.Context, items []model.TrafficSuccess) (int, error) {
	return s.insertTraffic(ctx, items, nil)
}

func (s *Store) InsertTrafficErrors(ctx context.Context, items []model.TrafficError) (int, error) {
	return s.insertTraffic(ctx, nil, items)
}

// InsertTrafficBatch atomically stores one operations poll. Re-reading an
// overlapping page is harmless because event_key is a one-way request digest.
func (s *Store) InsertTrafficBatch(ctx context.Context, successes []model.TrafficSuccess, failures []model.TrafficError) (int, error) {
	return s.insertTraffic(ctx, successes, failures)
}

func (s *Store) insertTraffic(ctx context.Context, successes []model.TrafficSuccess, failures []model.TrafficError) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	inserted := 0
	now := formatTelemetryTime(time.Now().UTC())
	for _, item := range successes {
		if item.EventKey == "" || item.AccountID <= 0 || item.CreatedAt.IsZero() {
			// Sub2API operations records may have a nullable account_id when a
			// request fails before routing. Skip an invalid row without rolling
			// back the valid evidence in the same polling batch.
			continue
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO traffic_events(event_key,account_id,model,upstream_model,kind,duration_ms,request_kind,created_at,request_started_at,ingested_at)
			VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_key) DO NOTHING`, telemetryFingerprint(item.EventKey), item.AccountID, item.Model,
			item.UpstreamModel, "success", item.DurationMS, safeTelemetryToken(item.Kind), formatTelemetryTime(item.CreatedAt), nullableTime(item.RequestStartedAt), now)
		if err != nil {
			return 0, err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		inserted += int(changed)
	}
	for _, item := range failures {
		if item.EventKey == "" || item.AccountID <= 0 || item.CreatedAt.IsZero() {
			continue
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO traffic_events(event_key,account_id,model,requested_model,upstream_model,kind,phase,error_type,severity,status_code,error_class,reason_code,reason_fingerprint,created_at,request_started_at,ingested_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(event_key) DO NOTHING`, telemetryFingerprint(item.EventKey), item.AccountID,
			item.Model, item.RequestedModel, item.UpstreamModel, "error", safeTelemetryToken(item.Phase), safeTelemetryToken(item.Type), safeTelemetryToken(item.Severity),
			item.StatusCode, safeTelemetryToken(item.ErrorClass), safeTelemetryToken(item.ReasonCode), telemetryFingerprint(item.ReasonFingerprint), formatTelemetryTime(item.CreatedAt), nullableTime(item.RequestStartedAt), now)
		if err != nil {
			return 0, err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		inserted += int(changed)
	}
	return inserted, tx.Commit()
}

func (s *Store) GetTrafficWindow(ctx context.Context, accountID int64, since, until time.Time) (model.TrafficWindow, error) {
	return s.getTrafficWindow(ctx, accountID, "", since, until)
}

func (s *Store) GetModelTrafficWindow(ctx context.Context, accountID int64, modelName string, since, until time.Time) (model.TrafficWindow, error) {
	if strings.TrimSpace(modelName) == "" {
		return model.TrafficWindow{}, errors.New("model is required")
	}
	return s.getTrafficWindow(ctx, accountID, strings.TrimSpace(modelName), since, until)
}

func (s *Store) getTrafficWindow(ctx context.Context, accountID int64, modelName string, since, until time.Time) (model.TrafficWindow, error) {
	if accountID <= 0 || since.IsZero() || until.IsZero() || !until.After(since) {
		return model.TrafficWindow{}, errors.New("account_id and a valid time window are required")
	}
	window := model.TrafficWindow{AccountID: accountID, Since: since.UTC(), Until: until.UTC()}
	query := `SELECT kind,error_class,duration_ms FROM traffic_events WHERE account_id=? AND created_at>=? AND created_at<?`
	args := []any{accountID, formatTelemetryTime(since), formatTelemetryTime(until)}
	if modelName != "" {
		query += ` AND (model=? OR requested_model=?)`
		args = append(args, modelName, modelName)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return window, err
	}
	defer rows.Close()
	durations := make([]int64, 0)
	for rows.Next() {
		var kind, class string
		var duration int64
		if err := rows.Scan(&kind, &class, &duration); err != nil {
			return window, err
		}
		if kind == "success" {
			window.SuccessCount++
			if duration >= 0 {
				durations = append(durations, duration)
			}
			continue
		}
		window.ErrorCount++
		switch class {
		case model.ErrorClassCredential:
			window.CredentialErrors++
		case model.ErrorClassInfrastructure:
			window.InfrastructureErrors++
		case model.ErrorClassCapacity:
			window.CapacityErrors++
		case model.ErrorClassSemantic:
			window.SemanticErrors++
		case model.ErrorClassClient:
			window.ClientErrors++
		case model.ErrorClassModelCapability:
			window.CapabilityErrors++
		default:
			window.UnknownErrors++
		}
	}
	if err := rows.Err(); err != nil {
		return window, err
	}
	window.EligibleCount = window.SuccessCount + window.CredentialErrors + window.InfrastructureErrors + window.CapacityErrors + window.UnknownErrors
	if window.EligibleCount > 0 {
		window.SuccessRate = float64(window.SuccessCount) * 100 / float64(window.EligibleCount)
	}
	if len(durations) > 0 {
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		position := int(math.Ceil(float64(len(durations))*0.9)) - 1
		if position < 0 {
			position = 0
		}
		window.P90DurationMS = durations[position]
	}
	return window, nil
}

func (s *Store) UpsertAccountModelCapability(ctx context.Context, item model.AccountModelCapability) error {
	if item.AccountID <= 0 || strings.TrimSpace(item.Model) == "" || item.LastObservedAt.IsZero() {
		return errors.New("account_id, model and last_observed_at are required")
	}
	if item.UpdatedAt.IsZero() {
		item.UpdatedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO account_model_capabilities(account_id,model,supported,success_count,failure_count,last_error_class,last_reason_code,last_observed_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,model) DO UPDATE SET supported=excluded.supported,success_count=excluded.success_count,
		failure_count=excluded.failure_count,last_error_class=excluded.last_error_class,last_reason_code=excluded.last_reason_code,
		last_observed_at=excluded.last_observed_at,updated_at=excluded.updated_at`, item.AccountID, item.Model, boolInt(item.Supported),
		item.SuccessCount, item.FailureCount, safeTelemetryToken(item.LastErrorClass), safeTelemetryToken(item.LastReasonCode), formatTelemetryTime(item.LastObservedAt), formatTelemetryTime(item.UpdatedAt))
	return err
}

func (s *Store) ListAccountModelCapabilities(ctx context.Context, accountID int64) ([]model.AccountModelCapability, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT account_id,model,supported,success_count,failure_count,last_error_class,last_reason_code,last_observed_at,updated_at
		FROM account_model_capabilities WHERE account_id=? ORDER BY model`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AccountModelCapability, 0)
	for rows.Next() {
		var item model.AccountModelCapability
		var supported int
		var observedAt, updatedAt string
		if err := rows.Scan(&item.AccountID, &item.Model, &supported, &item.SuccessCount, &item.FailureCount,
			&item.LastErrorClass, &item.LastReasonCode, &observedAt, &updatedAt); err != nil {
			return nil, err
		}
		item.Supported = supported == 1
		item.LastObservedAt, item.UpdatedAt = parseTime(observedAt), parseTime(updatedAt)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) SaveDecisionSnapshot(ctx context.Context, snapshot model.DecisionSnapshot) (bool, error) {
	return s.CommitDecisionSnapshot(ctx, snapshot, nil)
}

// CommitDecisionSnapshot optionally records the resulting audit event in the
// same transaction. A duplicate decision_id writes neither record.
func (s *Store) CommitDecisionSnapshot(ctx context.Context, snapshot model.DecisionSnapshot, event *model.Event) (bool, error) {
	if strings.TrimSpace(snapshot.DecisionID) == "" || snapshot.CheckedAt.IsZero() {
		return false, errors.New("decision_id and checked_at are required")
	}
	if snapshot.CreatedAt.IsZero() {
		snapshot.CreatedAt = time.Now().UTC()
	}
	evidence, err := json.Marshal(snapshot.Evidence)
	if err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO decision_snapshots(decision_id,monitor_id,account_id,checked_at,availability_state,load_stage,target_load_percent,action,action_result,reason_code,evidence_json,created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(decision_id) DO NOTHING`, snapshot.DecisionID, snapshot.MonitorID, snapshot.AccountID,
		formatTelemetryTime(snapshot.CheckedAt), snapshot.AvailabilityState, snapshot.LoadStage, snapshot.TargetLoadPercent,
		snapshot.Action, snapshot.ActionResult, snapshot.ReasonCode, string(evidence), formatTelemetryTime(snapshot.CreatedAt))
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if changed == 0 {
		return false, tx.Commit()
	}
	if event != nil {
		if event.CreatedAt.IsZero() {
			event.CreatedAt = snapshot.CreatedAt
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO events(type,severity,monitor_id,account_id,message,before_state,after_state,details,actor,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?)`, event.Type, event.Severity, event.MonitorID, event.AccountID, event.Message,
			event.BeforeState, event.AfterState, event.Details, event.Actor, formatTime(event.CreatedAt)); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}

func (s *Store) ListDecisionSnapshots(ctx context.Context, accountID int64, limit int) ([]model.DecisionSnapshot, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query := `SELECT id,decision_id,monitor_id,account_id,checked_at,availability_state,load_stage,target_load_percent,action,action_result,reason_code,evidence_json,created_at FROM decision_snapshots`
	args := make([]any, 0, 2)
	if accountID > 0 {
		query += ` WHERE account_id=?`
		args = append(args, accountID)
	}
	query += ` ORDER BY created_at DESC,id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.DecisionSnapshot, 0)
	for rows.Next() {
		var item model.DecisionSnapshot
		var monitorID, snapshotAccountID sql.NullInt64
		var checkedAt, evidence, createdAt string
		if err := rows.Scan(&item.ID, &item.DecisionID, &monitorID, &snapshotAccountID, &checkedAt, &item.AvailabilityState,
			&item.LoadStage, &item.TargetLoadPercent, &item.Action, &item.ActionResult, &item.ReasonCode, &evidence, &createdAt); err != nil {
			return nil, err
		}
		item.MonitorID, item.AccountID = nullInt64Pointer(monitorID), nullInt64Pointer(snapshotAccountID)
		item.CheckedAt, item.CreatedAt = parseTime(checkedAt), parseTime(createdAt)
		if err := json.Unmarshal([]byte(evidence), &item.Evidence); err != nil {
			return nil, fmt.Errorf("decode decision evidence: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// DeleteTelemetryBefore bounds database growth. Different retention windows
// can be supplied for monitor history, traffic and decision audit snapshots.
func (s *Store) DeleteTelemetryBefore(ctx context.Context, monitorBefore, trafficBefore, decisionBefore time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if !monitorBefore.IsZero() {
		if _, err := tx.ExecContext(ctx, `DELETE FROM monitor_history_records WHERE checked_at<?`, formatTelemetryTime(monitorBefore)); err != nil {
			return err
		}
	}
	if !trafficBefore.IsZero() {
		if _, err := tx.ExecContext(ctx, `DELETE FROM traffic_events WHERE created_at<?`, formatTelemetryTime(trafficBefore)); err != nil {
			return err
		}
	}
	if !decisionBefore.IsZero() {
		if _, err := tx.ExecContext(ctx, `DELETE FROM decision_snapshots WHERE created_at<?`, formatTelemetryTime(decisionBefore)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func formatTelemetryTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}

func telemetryFingerprint(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func safeTelemetryToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) > 80 {
		return "redacted"
	}
	for _, character := range value {
		if !(character == '_' || character == '-' || character == '.' || character == ':' ||
			(character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9')) {
			return "redacted"
		}
	}
	return value
}
