package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func (s *Store) migrateAgent(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS agent_providers (
			slot TEXT PRIMARY KEY, base_url TEXT NOT NULL DEFAULT '', model TEXT NOT NULL DEFAULT '',
			credential_nonce BLOB, credential_ciphertext BLOB, enabled INTEGER NOT NULL DEFAULT 0,
			timeout_seconds INTEGER NOT NULL DEFAULT 90, max_output_tokens INTEGER NOT NULL DEFAULT 4096,
			temperature REAL NOT NULL DEFAULT 0.1, last_validated_at TEXT, last_error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS agent_settings (
			id INTEGER PRIMARY KEY CHECK(id=1), enabled INTEGER NOT NULL DEFAULT 0, mode TEXT NOT NULL DEFAULT 'observe',
			analysis_interval_minutes INTEGER NOT NULL DEFAULT 30, emergency_cooldown_minutes INTEGER NOT NULL DEFAULT 5,
			context_token_budget INTEGER NOT NULL DEFAULT 16000, max_anomalies INTEGER NOT NULL DEFAULT 20,
			max_drilldowns INTEGER NOT NULL DEFAULT 8, retention_days INTEGER NOT NULL DEFAULT 90,
			observation_started_at TEXT, successful_observation_runs INTEGER NOT NULL DEFAULT 0,
			observation_proposed_actions INTEGER NOT NULL DEFAULT 0,
			observation_executable_actions INTEGER NOT NULL DEFAULT 0,
			observation_violations INTEGER NOT NULL DEFAULT 0,
			observation_structure_errors INTEGER NOT NULL DEFAULT 0,
			last_scheduled_at TEXT, last_emergency_at TEXT, updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS analysis_packets (
			id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, cutoff_at TEXT NOT NULL, previous_packet_id INTEGER,
			hash TEXT NOT NULL, token_estimate INTEGER NOT NULL DEFAULT 0, no_material_change INTEGER NOT NULL DEFAULT 0,
			packet_json TEXT NOT NULL, created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_analysis_packets_kind_created ON analysis_packets(kind,created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS availability_assessments (
			id INTEGER PRIMARY KEY AUTOINCREMENT, packet_id INTEGER NOT NULL, account_id INTEGER NOT NULL, state TEXT NOT NULL,
			availability_score REAL NOT NULL, performance_score REAL NOT NULL, stability_score REAL NOT NULL,
			capacity_score REAL NOT NULL, cost_score REAL NOT NULL, confidence REAL NOT NULL,
			evidence_conflict INTEGER NOT NULL DEFAULT 0, reasons_json TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_availability_assessments_account_created ON availability_assessments(account_id,created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_availability_assessments_packet ON availability_assessments(packet_id)`,
		`CREATE TABLE IF NOT EXISTS agent_conversations (
			id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS agent_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, trigger_reason TEXT NOT NULL DEFAULT '', status TEXT NOT NULL,
			provider_slot TEXT NOT NULL DEFAULT '', model TEXT NOT NULL DEFAULT '', packet_id INTEGER, conversation_id INTEGER,
			summary TEXT NOT NULL DEFAULT '', conclusion TEXT NOT NULL DEFAULT '', confidence REAL NOT NULL DEFAULT 0,
			actions_json TEXT NOT NULL DEFAULT '[]', error TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL, completed_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_runs_started ON agent_runs(started_at DESC)`,
		`CREATE TABLE IF NOT EXISTS agent_messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT, conversation_id INTEGER NOT NULL, role TEXT NOT NULL, content TEXT NOT NULL,
			run_id INTEGER, created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_messages_conversation ON agent_messages(conversation_id,id)`,
		`CREATE TABLE IF NOT EXISTS agent_tool_calls (
			id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL, tool TEXT NOT NULL, arguments_json TEXT NOT NULL,
			status TEXT NOT NULL, before_state TEXT NOT NULL DEFAULT '', after_state TEXT NOT NULL DEFAULT '',
			result TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_tool_calls_run ON agent_tool_calls(run_id,id)`,
		`CREATE TABLE IF NOT EXISTS score_policy_versions (
			id INTEGER PRIMARY KEY AUTOINCREMENT, scope_type TEXT NOT NULL, scope_id TEXT NOT NULL DEFAULT '', version INTEGER NOT NULL,
			status TEXT NOT NULL, config_json TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', agent_run_id INTEGER,
			created_by TEXT NOT NULL, activated_at TEXT, created_at TEXT NOT NULL,
			UNIQUE(scope_type,scope_id,version)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_score_policy_active ON score_policy_versions(scope_type,scope_id,status)`,
		`CREATE TABLE IF NOT EXISTS decision_outcomes (
			id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL, tool_call_id INTEGER, account_id INTEGER,
			predicted_success_rate_delta REAL NOT NULL DEFAULT 0, predicted_latency_delta_ms INTEGER NOT NULL DEFAULT 0,
			predicted_cost_delta REAL NOT NULL DEFAULT 0, evaluate_at TEXT NOT NULL, actual_success_rate_delta REAL,
			actual_latency_delta_ms INTEGER, actual_cost_delta REAL, verdict TEXT NOT NULL DEFAULT '', evaluated_at TEXT, created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_decision_outcomes_pending ON decision_outcomes(evaluated_at,evaluate_at)`,
		`CREATE TABLE IF NOT EXISTS agent_daily_reports (
			id INTEGER PRIMARY KEY AUTOINCREMENT, report_date TEXT NOT NULL UNIQUE, packet_id INTEGER, run_id INTEGER,
			status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, summary TEXT NOT NULL DEFAULT '', metrics_json TEXT NOT NULL DEFAULT '{}',
			advice_json TEXT NOT NULL DEFAULT '[]', error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_daily_reports_date ON agent_daily_reports(report_date DESC)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := s.ensureColumn(ctx, "agent_daily_reports", "attempts", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	for _, column := range []string{"observation_proposed_actions", "observation_executable_actions", "observation_violations", "observation_structure_errors"} {
		if err := s.ensureColumn(ctx, "agent_settings", column, "INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}
	now := formatTime(time.Now().UTC())
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO agent_settings(id,updated_at) VALUES(1,?)`, now)
	return err
}

func (s *Store) GetAgentSettings(ctx context.Context) (model.AgentSettings, error) {
	var item model.AgentSettings
	var enabled int
	var observation, lastScheduled, lastEmergency sql.NullString
	var updated string
	err := s.db.QueryRowContext(ctx, `SELECT enabled,mode,analysis_interval_minutes,emergency_cooldown_minutes,context_token_budget,
		max_anomalies,max_drilldowns,retention_days,observation_started_at,successful_observation_runs,
		observation_proposed_actions,observation_executable_actions,observation_violations,observation_structure_errors,
		last_scheduled_at,last_emergency_at,updated_at
		FROM agent_settings WHERE id=1`).Scan(&enabled, &item.Mode, &item.AnalysisIntervalMinutes, &item.EmergencyCooldownMinutes,
		&item.ContextTokenBudget, &item.MaxAnomalies, &item.MaxDrilldowns, &item.RetentionDays, &observation,
		&item.SuccessfulObservationRuns, &item.ObservationProposedActions, &item.ObservationExecutableActions,
		&item.ObservationViolations, &item.ObservationStructureErrors, &lastScheduled, &lastEmergency, &updated)
	if err != nil {
		return item, err
	}
	item.Enabled = enabled == 1
	item.ObservationStartedAt = parseNullableTime(observation)
	item.LastScheduledAt = parseNullableTime(lastScheduled)
	item.LastEmergencyAt = parseNullableTime(lastEmergency)
	item.UpdatedAt = parseTime(updated)
	return item, nil
}

func (s *Store) UpdateAgentSettings(ctx context.Context, item model.AgentSettings) error {
	if item.Mode != model.AgentModeObserve && item.Mode != model.AgentModeControl {
		return errors.New("invalid agent mode")
	}
	if item.AnalysisIntervalMinutes < 5 || item.EmergencyCooldownMinutes < 1 || item.ContextTokenBudget < 2000 || item.MaxAnomalies < 1 || item.MaxDrilldowns < 0 || item.RetentionDays < 7 {
		return errors.New("invalid agent settings")
	}
	item.UpdatedAt = time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `UPDATE agent_settings SET enabled=?,mode=?,analysis_interval_minutes=?,emergency_cooldown_minutes=?,
		context_token_budget=?,max_anomalies=?,max_drilldowns=?,retention_days=?,observation_started_at=?,successful_observation_runs=?,
		observation_proposed_actions=?,observation_executable_actions=?,observation_violations=?,observation_structure_errors=?,
		last_scheduled_at=?,last_emergency_at=?,updated_at=? WHERE id=1`, boolInt(item.Enabled), item.Mode,
		item.AnalysisIntervalMinutes, item.EmergencyCooldownMinutes, item.ContextTokenBudget, item.MaxAnomalies, item.MaxDrilldowns,
		item.RetentionDays, formatOptionalTime(item.ObservationStartedAt), item.SuccessfulObservationRuns,
		item.ObservationProposedActions, item.ObservationExecutableActions, item.ObservationViolations, item.ObservationStructureErrors,
		formatOptionalTime(item.LastScheduledAt), formatOptionalTime(item.LastEmergencyAt), formatTime(item.UpdatedAt))
	return err
}

func (s *Store) GetAgentProvider(ctx context.Context, slot string) (model.AgentProvider, error) {
	var item model.AgentProvider
	var enabled int
	var validated sql.NullString
	var updated string
	err := s.db.QueryRowContext(ctx, `SELECT slot,base_url,model,credential_nonce,credential_ciphertext,enabled,timeout_seconds,
		max_output_tokens,temperature,last_validated_at,last_error,updated_at FROM agent_providers WHERE slot=?`, slot).
		Scan(&item.Slot, &item.BaseURL, &item.Model, &item.CredentialNonce, &item.CredentialCiphertext, &enabled,
			&item.TimeoutSeconds, &item.MaxOutputTokens, &item.Temperature, &validated, &item.LastError, &updated)
	if err != nil {
		return item, err
	}
	item.Enabled = enabled == 1
	item.APIKeyConfigured = len(item.CredentialCiphertext) > 0
	item.LastValidatedAt = parseNullableTime(validated)
	item.UpdatedAt = parseTime(updated)
	return item, nil
}

func (s *Store) ListAgentProviders(ctx context.Context) ([]model.AgentProvider, error) {
	items := make([]model.AgentProvider, 0, 2)
	for _, slot := range []string{"primary", "fallback"} {
		item, err := s.GetAgentProvider(ctx, slot)
		if errors.Is(err, sql.ErrNoRows) {
			items = append(items, model.AgentProvider{Slot: slot, TimeoutSeconds: 90, MaxOutputTokens: 4096, Temperature: 0.1})
			continue
		}
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Store) CountConfiguredAgentProviders(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_providers WHERE length(credential_ciphertext)>0`).Scan(&count)
	return count, err
}

func (s *Store) UpsertAgentProvider(ctx context.Context, item model.AgentProvider) error {
	if item.Slot != "primary" && item.Slot != "fallback" {
		return errors.New("invalid provider slot")
	}
	item.UpdatedAt = time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `INSERT INTO agent_providers(slot,base_url,model,credential_nonce,credential_ciphertext,enabled,
		timeout_seconds,max_output_tokens,temperature,last_validated_at,last_error,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(slot) DO UPDATE SET base_url=excluded.base_url,model=excluded.model,credential_nonce=excluded.credential_nonce,
		credential_ciphertext=excluded.credential_ciphertext,enabled=excluded.enabled,timeout_seconds=excluded.timeout_seconds,
		max_output_tokens=excluded.max_output_tokens,temperature=excluded.temperature,last_validated_at=excluded.last_validated_at,
		last_error=excluded.last_error,updated_at=excluded.updated_at`, item.Slot, item.BaseURL, item.Model, item.CredentialNonce,
		item.CredentialCiphertext, boolInt(item.Enabled), item.TimeoutSeconds, item.MaxOutputTokens, item.Temperature,
		formatOptionalTime(item.LastValidatedAt), item.LastError, formatTime(item.UpdatedAt))
	return err
}

func (s *Store) UpdateAgentProviderStatus(ctx context.Context, slot, lastError string, validatedAt *time.Time) error {
	if slot != "primary" && slot != "fallback" {
		return errors.New("invalid provider slot")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE agent_providers SET last_error=?,last_validated_at=COALESCE(?,last_validated_at),
		updated_at=? WHERE slot=?`, lastError, formatOptionalTime(validatedAt), formatTime(time.Now().UTC()), slot)
	return err
}

func (s *Store) ResetAgentObservation(ctx context.Context, startedAt *time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE agent_settings SET mode='observe',observation_started_at=?,
		successful_observation_runs=0,observation_proposed_actions=0,observation_executable_actions=0,
		observation_violations=0,observation_structure_errors=0,updated_at=? WHERE id=1`,
		formatOptionalTime(startedAt), formatTime(time.Now().UTC()))
	return err
}

func (s *Store) RecordAgentObservation(ctx context.Context, proposed, executable, violations, structureErrors int) error {
	if proposed < 0 || executable < 0 || executable > proposed || violations < 0 || structureErrors < 0 {
		return errors.New("invalid observation counters")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE agent_settings SET
		observation_proposed_actions=observation_proposed_actions+?,
		observation_executable_actions=observation_executable_actions+?,
		observation_violations=observation_violations+?,
		observation_structure_errors=observation_structure_errors+?,updated_at=? WHERE id=1`,
		proposed, executable, violations, structureErrors, formatTime(time.Now().UTC()))
	return err
}

func (s *Store) AdvanceAgentSchedule(ctx context.Context, kind string, completed time.Time) (model.AgentSettings, bool, error) {
	activated := false
	if kind == model.AgentRunEmergency {
		_, err := s.db.ExecContext(ctx, `UPDATE agent_settings SET last_emergency_at=?,updated_at=? WHERE id=1`,
			formatTime(completed), formatTime(time.Now().UTC()))
		if err != nil {
			return model.AgentSettings{}, false, err
		}
		settings, err := s.GetAgentSettings(ctx)
		return settings, false, err
	}
	if kind != model.AgentRunScheduled {
		settings, err := s.GetAgentSettings(ctx)
		return settings, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.AgentSettings{}, false, err
	}
	defer tx.Rollback()
	var mode string
	var observation sql.NullString
	var runs, proposed, executable, violations, structureErrors int
	if err := tx.QueryRowContext(ctx, `SELECT mode,observation_started_at,successful_observation_runs,
		observation_proposed_actions,observation_executable_actions,observation_violations,observation_structure_errors
		FROM agent_settings WHERE id=1`).Scan(&mode, &observation, &runs, &proposed, &executable, &violations, &structureErrors); err != nil {
		return model.AgentSettings{}, false, err
	}
	runs++
	if mode == model.AgentModeObserve && observation.Valid {
		started := parseTime(observation.String)
		actionPass := proposed == 0 || float64(executable)/float64(proposed) >= .95
		if !started.IsZero() && completed.Sub(started) >= 24*time.Hour && runs >= 40 && actionPass && violations == 0 && structureErrors == 0 {
			mode = model.AgentModeControl
			activated = true
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_settings SET mode=?,successful_observation_runs=?,last_scheduled_at=?,updated_at=? WHERE id=1`,
		mode, runs, formatTime(completed), formatTime(time.Now().UTC())); err != nil {
		return model.AgentSettings{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return model.AgentSettings{}, false, err
	}
	settings, err := s.GetAgentSettings(ctx)
	return settings, activated, err
}

func (s *Store) SaveAnalysisPacket(ctx context.Context, packet *model.AnalysisPacket) error {
	if packet == nil || packet.CutoffAt.IsZero() {
		return errors.New("invalid analysis packet")
	}
	packet.CreatedAt = time.Now().UTC()
	payload, err := json.Marshal(packet)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO analysis_packets(kind,cutoff_at,previous_packet_id,hash,token_estimate,
		no_material_change,packet_json,created_at) VALUES(?,?,?,?,?,?,?,?)`, packet.Kind, formatTime(packet.CutoffAt), packet.PreviousPacketID,
		packet.Hash, packet.TokenEstimate, boolInt(packet.NoMaterialChange), string(payload), formatTime(packet.CreatedAt))
	if err != nil {
		return err
	}
	packet.ID, err = result.LastInsertId()
	if err != nil {
		return err
	}
	payload, err = json.Marshal(packet)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE analysis_packets SET packet_json=? WHERE id=?`, string(payload), packet.ID)
	return err
}

func (s *Store) SaveAnalysisPacketWithAssessments(ctx context.Context, packet *model.AnalysisPacket, states []model.AgentAccountState) error {
	if packet == nil || packet.CutoffAt.IsZero() {
		return errors.New("invalid analysis packet")
	}
	packet.CreatedAt = time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	payload, err := json.Marshal(packet)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO analysis_packets(kind,cutoff_at,previous_packet_id,hash,token_estimate,
		no_material_change,packet_json,created_at) VALUES(?,?,?,?,?,?,?,?)`, packet.Kind, formatTime(packet.CutoffAt),
		packet.PreviousPacketID, packet.Hash, packet.TokenEstimate, boolInt(packet.NoMaterialChange), string(payload),
		formatTime(packet.CreatedAt))
	if err != nil {
		return err
	}
	packet.ID, err = result.LastInsertId()
	if err != nil {
		return err
	}
	payload, err = json.Marshal(packet)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE analysis_packets SET packet_json=? WHERE id=?`, string(payload), packet.ID); err != nil {
		return err
	}
	now := formatTime(packet.CreatedAt)
	for _, state := range states {
		reasons, _ := json.Marshal(state.Reasons)
		if _, err := tx.ExecContext(ctx, `INSERT INTO availability_assessments(packet_id,account_id,state,availability_score,
			performance_score,stability_score,capacity_score,cost_score,confidence,evidence_conflict,reasons_json,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, packet.ID, state.AccountID, state.AvailabilityState, state.AvailabilityScore,
			state.PerformanceScore, state.StabilityScore, state.CapacityScore, state.CostScore, state.Confidence,
			boolInt(state.EvidenceConflict), string(reasons), now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) LatestAnalysisPacket(ctx context.Context, kind string) (model.AnalysisPacket, error) {
	var payload string
	err := s.db.QueryRowContext(ctx, `SELECT packet_json FROM analysis_packets WHERE kind=? ORDER BY id DESC LIMIT 1`, kind).Scan(&payload)
	var item model.AnalysisPacket
	if err != nil {
		return item, err
	}
	err = json.Unmarshal([]byte(payload), &item)
	return item, err
}

func (s *Store) GetAnalysisPacket(ctx context.Context, id int64) (model.AnalysisPacket, error) {
	var payload string
	err := s.db.QueryRowContext(ctx, `SELECT packet_json FROM analysis_packets WHERE id=?`, id).Scan(&payload)
	var item model.AnalysisPacket
	if err != nil {
		return item, err
	}
	err = json.Unmarshal([]byte(payload), &item)
	return item, err
}

func (s *Store) ListAnalysisPackets(ctx context.Context, limit int) ([]model.AnalysisPacket, error) {
	if limit < 1 || limit > 200 {
		limit = 30
	}
	rows, err := s.db.QueryContext(ctx, `SELECT packet_json FROM analysis_packets ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AnalysisPacket, 0)
	for rows.Next() {
		var payload string
		var item model.AnalysisPacket
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		if json.Unmarshal([]byte(payload), &item) == nil {
			items = append(items, item)
		}
	}
	return items, rows.Err()
}

func (s *Store) SaveAvailabilityAssessments(ctx context.Context, packetID int64, states []model.AgentAccountState) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := formatTime(time.Now().UTC())
	for _, state := range states {
		reasons, _ := json.Marshal(state.Reasons)
		_, err = tx.ExecContext(ctx, `INSERT INTO availability_assessments(packet_id,account_id,state,availability_score,
			performance_score,stability_score,capacity_score,cost_score,confidence,evidence_conflict,reasons_json,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, packetID, state.AccountID, state.AvailabilityState, state.AvailabilityScore,
			state.PerformanceScore, state.StabilityScore, state.CapacityScore, state.CostScore, state.Confidence,
			boolInt(state.EvidenceConflict), string(reasons), now)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListLatestAvailabilityAssessments(ctx context.Context) ([]model.AvailabilityAssessment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,packet_id,account_id,state,availability_score,performance_score,stability_score,
		capacity_score,cost_score,confidence,evidence_conflict,reasons_json,created_at FROM availability_assessments
		WHERE packet_id=(SELECT MAX(packet_id) FROM availability_assessments) ORDER BY account_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AvailabilityAssessment, 0)
	for rows.Next() {
		var item model.AvailabilityAssessment
		var conflict int
		var created string
		if err := rows.Scan(&item.ID, &item.PacketID, &item.AccountID, &item.State, &item.AvailabilityScore,
			&item.PerformanceScore, &item.StabilityScore, &item.CapacityScore, &item.CostScore, &item.Confidence,
			&conflict, &item.ReasonsJSON, &created); err != nil {
			return nil, err
		}
		item.EvidenceConflict = conflict == 1
		item.CreatedAt = parseTime(created)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) CreateAgentRun(ctx context.Context, run *model.AgentRun) error {
	if run == nil {
		return errors.New("run is required")
	}
	if run.StartedAt.IsZero() {
		run.StartedAt = time.Now().UTC()
	}
	if len(run.ActionsJSON) == 0 {
		run.ActionsJSON = json.RawMessage("[]")
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO agent_runs(kind,trigger_reason,status,provider_slot,model,packet_id,
		conversation_id,summary,conclusion,confidence,actions_json,error,started_at,completed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		run.Kind, run.Trigger, run.Status, run.ProviderSlot, run.Model, run.PacketID, run.ConversationID, run.Summary,
		run.Conclusion, run.Confidence, string(run.ActionsJSON), run.Error, formatTime(run.StartedAt), formatOptionalTime(run.CompletedAt))
	if err != nil {
		return err
	}
	run.ID, err = result.LastInsertId()
	return err
}

func (s *Store) UpdateAgentRun(ctx context.Context, run model.AgentRun) error {
	_, err := s.db.ExecContext(ctx, `UPDATE agent_runs SET status=?,provider_slot=?,model=?,packet_id=?,conversation_id=?,summary=?,
		conclusion=?,confidence=?,actions_json=?,error=?,completed_at=? WHERE id=?`, run.Status, run.ProviderSlot, run.Model,
		run.PacketID, run.ConversationID, run.Summary, run.Conclusion, run.Confidence, string(run.ActionsJSON), run.Error,
		formatOptionalTime(run.CompletedAt), run.ID)
	return err
}

func (s *Store) ListAgentRuns(ctx context.Context, limit int) ([]model.AgentRun, error) {
	if limit < 1 || limit > 200 {
		limit = 40
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,kind,trigger_reason,status,provider_slot,model,packet_id,conversation_id,
		summary,conclusion,confidence,actions_json,error,started_at,completed_at FROM agent_runs ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AgentRun, 0)
	for rows.Next() {
		var item model.AgentRun
		var packetID, conversationID sql.NullInt64
		var actions, started string
		var completed sql.NullString
		if err := rows.Scan(&item.ID, &item.Kind, &item.Trigger, &item.Status, &item.ProviderSlot, &item.Model,
			&packetID, &conversationID, &item.Summary, &item.Conclusion, &item.Confidence, &actions, &item.Error,
			&started, &completed); err != nil {
			return nil, err
		}
		item.PacketID = nullableInt64(packetID)
		item.ConversationID = nullableInt64(conversationID)
		item.ActionsJSON = json.RawMessage(actions)
		item.StartedAt = parseTime(started)
		item.CompletedAt = parseNullableTime(completed)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) AddAgentToolCall(ctx context.Context, item *model.AgentToolCall) error {
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO agent_tool_calls(run_id,tool,arguments_json,status,before_state,after_state,
		result,created_at) VALUES(?,?,?,?,?,?,?,?)`, item.RunID, item.Tool, string(item.Arguments), item.Status,
		item.BeforeState, item.AfterState, item.Result, formatTime(item.CreatedAt))
	if err != nil {
		return err
	}
	item.ID, err = result.LastInsertId()
	return err
}

func (s *Store) UpdateAgentToolCall(ctx context.Context, item model.AgentToolCall) error {
	if item.ID <= 0 {
		return errors.New("invalid agent tool call")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE agent_tool_calls SET status=?,before_state=?,after_state=?,result=? WHERE id=?`,
		item.Status, item.BeforeState, item.AfterState, item.Result, item.ID)
	return err
}

func (s *Store) FailPendingAgentToolCalls(ctx context.Context, reason string) error {
	if strings.TrimSpace(reason) == "" {
		reason = "服务重启前动作未完成，已停止补偿执行"
	}
	_, err := s.db.ExecContext(ctx, `UPDATE agent_tool_calls SET status='interrupted',result=? WHERE status='pending'`, reason)
	return err
}

func (s *Store) ListAgentToolCalls(ctx context.Context, runID int64) ([]model.AgentToolCall, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,run_id,tool,arguments_json,status,before_state,after_state,result,created_at
		FROM agent_tool_calls WHERE run_id=? ORDER BY id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AgentToolCall, 0)
	for rows.Next() {
		var item model.AgentToolCall
		var arguments, created string
		if err := rows.Scan(&item.ID, &item.RunID, &item.Tool, &arguments, &item.Status, &item.BeforeState,
			&item.AfterState, &item.Result, &created); err != nil {
			return nil, err
		}
		item.Arguments = json.RawMessage(arguments)
		item.CreatedAt = parseTime(created)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) CreatePolicyVersion(ctx context.Context, item *model.ScorePolicyVersion, activate bool) error {
	if item.ScopeType == "" || len(item.Config) == 0 {
		return errors.New("invalid score policy")
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
	now := time.Now().UTC()
	item.CreatedAt = now
	if activate {
		if _, err := tx.ExecContext(ctx, `UPDATE score_policy_versions SET status='superseded' WHERE scope_type=? AND scope_id=? AND status='active'`, item.ScopeType, item.ScopeID); err != nil {
			return err
		}
		item.Status = "active"
		item.ActivatedAt = &now
	} else {
		item.Status = "draft"
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO score_policy_versions(scope_type,scope_id,version,status,config_json,reason,
		agent_run_id,created_by,activated_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, item.ScopeType, item.ScopeID,
		item.Version, item.Status, string(item.Config), item.Reason, item.AgentRunID, item.CreatedBy,
		formatOptionalTime(item.ActivatedAt), formatTime(item.CreatedAt))
	if err != nil {
		return err
	}
	item.ID, err = result.LastInsertId()
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ActivatePolicyVersion(ctx context.Context, id int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := activatePolicyVersionTx(ctx, tx, id, time.Now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

func activatePolicyVersionTx(ctx context.Context, tx *sql.Tx, id int64, activatedAt time.Time) error {
	var scopeType, scopeID string
	if err := tx.QueryRowContext(ctx, `SELECT scope_type,scope_id FROM score_policy_versions WHERE id=?`, id).Scan(&scopeType, &scopeID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE score_policy_versions SET status='superseded' WHERE scope_type=? AND scope_id=? AND status='active'`, scopeType, scopeID); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE score_policy_versions SET status='active',activated_at=? WHERE id=?`, formatTime(activatedAt), id)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("score policy version was not activated")
	}
	return nil
}

// PublishPolicyVersion commits the materialized policy and its active-version
// pointer together. Exactly one projection kind is accepted per version.
func (s *Store) PublishPolicyVersion(ctx context.Context, id int64, settings *model.Settings, policies []model.Policy) error {
	if id <= 0 || (settings == nil) == (len(policies) == 0) {
		return errors.New("policy publication requires exactly one projection")
	}
	var values map[string]string
	var err error
	if settings != nil {
		values, err = settingsValues(*settings)
		if err != nil {
			return err
		}
	}
	seen := make(map[int64]struct{}, len(policies))
	for _, policy := range policies {
		if err := validatePolicy(policy); err != nil {
			return err
		}
		if _, exists := seen[policy.AccountID]; exists {
			return fmt.Errorf("duplicate account policy %d", policy.AccountID)
		}
		seen[policy.AccountID] = struct{}{}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	if settings != nil {
		if err := writeSettings(ctx, tx, values, now); err != nil {
			return err
		}
	} else {
		for _, policy := range policies {
			if err := upsertPolicy(ctx, tx, policy, now); err != nil {
				return err
			}
		}
	}
	if err := activatePolicyVersionTx(ctx, tx, id, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) GetPolicyVersion(ctx context.Context, id int64) (model.ScorePolicyVersion, error) {
	var item model.ScorePolicyVersion
	var config, created string
	var runID sql.NullInt64
	var activated sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT id,scope_type,scope_id,version,status,config_json,reason,agent_run_id,
		created_by,activated_at,created_at FROM score_policy_versions WHERE id=?`, id).Scan(&item.ID, &item.ScopeType,
		&item.ScopeID, &item.Version, &item.Status, &config, &item.Reason, &runID, &item.CreatedBy, &activated, &created)
	if err != nil {
		return item, err
	}
	item.Config = json.RawMessage(config)
	item.AgentRunID = nullableInt64(runID)
	item.ActivatedAt = parseNullableTime(activated)
	item.CreatedAt = parseTime(created)
	return item, nil
}

func (s *Store) ListActivePolicyVersions(ctx context.Context) ([]model.ScorePolicyVersion, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,scope_type,scope_id,version,status,config_json,reason,agent_run_id,
		created_by,activated_at,created_at FROM score_policy_versions WHERE status='active' ORDER BY scope_type,scope_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.ScorePolicyVersion, 0)
	for rows.Next() {
		var item model.ScorePolicyVersion
		var config, created string
		var runID sql.NullInt64
		var activated sql.NullString
		if err := rows.Scan(&item.ID, &item.ScopeType, &item.ScopeID, &item.Version, &item.Status, &config,
			&item.Reason, &runID, &item.CreatedBy, &activated, &created); err != nil {
			return nil, err
		}
		item.Config = json.RawMessage(config)
		item.AgentRunID = nullableInt64(runID)
		item.ActivatedAt = parseNullableTime(activated)
		item.CreatedAt = parseTime(created)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ListPolicyVersions(ctx context.Context, limit int) ([]model.ScorePolicyVersion, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,scope_type,scope_id,version,status,config_json,reason,agent_run_id,
		created_by,activated_at,created_at FROM score_policy_versions ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.ScorePolicyVersion, 0)
	for rows.Next() {
		var item model.ScorePolicyVersion
		var config, created string
		var runID sql.NullInt64
		var activated sql.NullString
		if err := rows.Scan(&item.ID, &item.ScopeType, &item.ScopeID, &item.Version, &item.Status, &config,
			&item.Reason, &runID, &item.CreatedBy, &activated, &created); err != nil {
			return nil, err
		}
		item.Config = json.RawMessage(config)
		item.AgentRunID = nullableInt64(runID)
		item.ActivatedAt = parseNullableTime(activated)
		item.CreatedAt = parseTime(created)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) AddDecisionOutcome(ctx context.Context, item *model.DecisionOutcome) error {
	if item == nil || item.RunID <= 0 || item.EvaluateAt.IsZero() {
		return errors.New("invalid decision outcome")
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO decision_outcomes(run_id,tool_call_id,account_id,
		predicted_success_rate_delta,predicted_latency_delta_ms,predicted_cost_delta,evaluate_at,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, item.RunID, item.ToolCallID, item.AccountID, item.PredictedSuccessRateDelta,
		item.PredictedLatencyDeltaMS, item.PredictedCostDelta, formatTime(item.EvaluateAt), formatTime(item.CreatedAt))
	if err != nil {
		return err
	}
	item.ID, err = result.LastInsertId()
	return err
}

func (s *Store) ListPendingDecisionOutcomes(ctx context.Context, until time.Time, limit int) ([]model.DecisionOutcome, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,run_id,tool_call_id,account_id,predicted_success_rate_delta,
		predicted_latency_delta_ms,predicted_cost_delta,evaluate_at,created_at FROM decision_outcomes
		WHERE evaluated_at IS NULL AND evaluate_at<=? ORDER BY evaluate_at LIMIT ?`, formatTime(until), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.DecisionOutcome, 0)
	for rows.Next() {
		var item model.DecisionOutcome
		var callID, accountID sql.NullInt64
		var evaluateAt, createdAt string
		if err := rows.Scan(&item.ID, &item.RunID, &callID, &accountID, &item.PredictedSuccessRateDelta,
			&item.PredictedLatencyDeltaMS, &item.PredictedCostDelta, &evaluateAt, &createdAt); err != nil {
			return nil, err
		}
		item.ToolCallID, item.AccountID = nullableInt64(callID), nullableInt64(accountID)
		item.EvaluateAt, item.CreatedAt = parseTime(evaluateAt), parseTime(createdAt)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) CompleteDecisionOutcome(ctx context.Context, item model.DecisionOutcome) error {
	_, err := s.db.ExecContext(ctx, `UPDATE decision_outcomes SET actual_success_rate_delta=?,actual_latency_delta_ms=?,
		actual_cost_delta=?,verdict=?,evaluated_at=? WHERE id=?`, item.ActualSuccessRateDelta, item.ActualLatencyDeltaMS,
		item.ActualCostDelta, item.Verdict, formatOptionalTime(item.EvaluatedAt), item.ID)
	return err
}

func (s *Store) ListRecentDecisionOutcomes(ctx context.Context, limit int) ([]model.DecisionOutcome, error) {
	if limit < 1 || limit > 500 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,run_id,tool_call_id,account_id,predicted_success_rate_delta,
		predicted_latency_delta_ms,predicted_cost_delta,evaluate_at,actual_success_rate_delta,
		actual_latency_delta_ms,actual_cost_delta,verdict,evaluated_at,created_at FROM decision_outcomes
		ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.DecisionOutcome, 0, limit)
	for rows.Next() {
		var item model.DecisionOutcome
		var callID, accountID sql.NullInt64
		var success, cost sql.NullFloat64
		var latency sql.NullInt64
		var evaluateAt, createdAt string
		var evaluatedAt sql.NullString
		if err := rows.Scan(&item.ID, &item.RunID, &callID, &accountID, &item.PredictedSuccessRateDelta,
			&item.PredictedLatencyDeltaMS, &item.PredictedCostDelta, &evaluateAt, &success, &latency, &cost,
			&item.Verdict, &evaluatedAt, &createdAt); err != nil {
			return nil, err
		}
		item.ToolCallID, item.AccountID = nullableInt64(callID), nullableInt64(accountID)
		if success.Valid {
			value := success.Float64
			item.ActualSuccessRateDelta = &value
		}
		if latency.Valid {
			value := latency.Int64
			item.ActualLatencyDeltaMS = &value
		}
		if cost.Valid {
			value := cost.Float64
			item.ActualCostDelta = &value
		}
		item.EvaluateAt, item.EvaluatedAt, item.CreatedAt = parseTime(evaluateAt), parseNullableTime(evaluatedAt), parseTime(createdAt)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetAgentRunPacketID(ctx context.Context, runID int64) (int64, error) {
	var packetID sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT packet_id FROM agent_runs WHERE id=?`, runID).Scan(&packetID); err != nil {
		return 0, err
	}
	if !packetID.Valid {
		return 0, sql.ErrNoRows
	}
	return packetID.Int64, nil
}

func (s *Store) UpsertDailyReport(ctx context.Context, item *model.AgentDailyReport) error {
	if item.ReportDate == "" {
		return errors.New("report date is required")
	}
	now := time.Now().UTC()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	if len(item.MetricsJSON) == 0 {
		item.MetricsJSON = json.RawMessage("{}")
	}
	if len(item.AdviceJSON) == 0 {
		item.AdviceJSON = json.RawMessage("[]")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO agent_daily_reports(report_date,packet_id,run_id,status,attempts,summary,metrics_json,
		advice_json,error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(report_date) DO UPDATE SET
		packet_id=excluded.packet_id,run_id=excluded.run_id,status=excluded.status,summary=excluded.summary,
		attempts=excluded.attempts,metrics_json=excluded.metrics_json,advice_json=excluded.advice_json,error=excluded.error,updated_at=excluded.updated_at`,
		item.ReportDate, item.PacketID, item.RunID, item.Status, item.Attempts, item.Summary, string(item.MetricsJSON), string(item.AdviceJSON),
		item.Error, formatTime(item.CreatedAt), formatTime(item.UpdatedAt))
	return err
}

func (s *Store) ListDailyReports(ctx context.Context, limit int) ([]model.AgentDailyReport, error) {
	if limit < 1 || limit > 365 {
		limit = 30
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,report_date,packet_id,run_id,status,attempts,summary,metrics_json,advice_json,error,
		created_at,updated_at FROM agent_daily_reports ORDER BY report_date DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AgentDailyReport, 0)
	for rows.Next() {
		var item model.AgentDailyReport
		var packetID, runID sql.NullInt64
		var metrics, advice, created, updated string
		if err := rows.Scan(&item.ID, &item.ReportDate, &packetID, &runID, &item.Status, &item.Attempts, &item.Summary, &metrics,
			&advice, &item.Error, &created, &updated); err != nil {
			return nil, err
		}
		item.PacketID, item.RunID = nullableInt64(packetID), nullableInt64(runID)
		item.MetricsJSON, item.AdviceJSON = json.RawMessage(metrics), json.RawMessage(advice)
		item.CreatedAt, item.UpdatedAt = parseTime(created), parseTime(updated)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetDailyReport(ctx context.Context, reportDate string) (model.AgentDailyReport, error) {
	var item model.AgentDailyReport
	var packetID, runID sql.NullInt64
	var metrics, advice, created, updated string
	err := s.db.QueryRowContext(ctx, `SELECT id,report_date,packet_id,run_id,status,attempts,summary,metrics_json,
		advice_json,error,created_at,updated_at FROM agent_daily_reports WHERE report_date=?`, reportDate).
		Scan(&item.ID, &item.ReportDate, &packetID, &runID, &item.Status, &item.Attempts, &item.Summary, &metrics,
			&advice, &item.Error, &created, &updated)
	if err != nil {
		return item, err
	}
	item.PacketID, item.RunID = nullableInt64(packetID), nullableInt64(runID)
	item.MetricsJSON, item.AdviceJSON = json.RawMessage(metrics), json.RawMessage(advice)
	item.CreatedAt, item.UpdatedAt = parseTime(created), parseTime(updated)
	return item, nil
}

func (s *Store) CreateConversation(ctx context.Context, title string) (model.AgentConversation, error) {
	now := time.Now().UTC()
	if len([]rune(title)) > 80 {
		title = string([]rune(title)[:80])
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO agent_conversations(title,created_at,updated_at) VALUES(?,?,?)`,
		strings.TrimSpace(title), formatTime(now), formatTime(now))
	if err != nil {
		return model.AgentConversation{}, err
	}
	id, err := result.LastInsertId()
	return model.AgentConversation{ID: id, Title: strings.TrimSpace(title), CreatedAt: now, UpdatedAt: now}, err
}

func (s *Store) AddAgentMessage(ctx context.Context, item *model.AgentMessage) error {
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO agent_messages(conversation_id,role,content,run_id,created_at) VALUES(?,?,?,?,?)`,
		item.ConversationID, item.Role, item.Content, item.RunID, formatTime(item.CreatedAt))
	if err != nil {
		return err
	}
	item.ID, err = result.LastInsertId()
	if err == nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE agent_conversations SET updated_at=? WHERE id=?`, formatTime(item.CreatedAt), item.ConversationID)
	}
	return err
}

func (s *Store) ListAgentMessages(ctx context.Context, conversationID int64, limit int) ([]model.AgentMessage, error) {
	if limit < 1 || limit > 200 {
		limit = 60
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,conversation_id,role,content,run_id,created_at FROM (
		SELECT id,conversation_id,role,content,run_id,created_at FROM agent_messages WHERE conversation_id=? ORDER BY id DESC LIMIT ?
	) ORDER BY id`, conversationID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.AgentMessage, 0)
	for rows.Next() {
		var item model.AgentMessage
		var runID sql.NullInt64
		var created string
		if err := rows.Scan(&item.ID, &item.ConversationID, &item.Role, &item.Content, &runID, &created); err != nil {
			return nil, err
		}
		item.RunID = nullableInt64(runID)
		item.CreatedAt = parseTime(created)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetAgentWindowStats(ctx context.Context, accountID int64, since, until time.Time, label string) (model.AgentWindowStats, error) {
	item := model.AgentWindowStats{Window: label, ErrorCategoryCounts: map[string]int{}}
	query := `SELECT kind,error_class,duration_ms FROM traffic_events WHERE created_at>=? AND created_at<?`
	args := []any{formatTelemetryTime(since), formatTelemetryTime(until)}
	if accountID > 0 {
		query += ` AND account_id=?`
		args = append(args, accountID)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return item, err
	}
	durations := make([]int64, 0)
	for rows.Next() {
		var kind, class string
		var duration int64
		if err := rows.Scan(&kind, &class, &duration); err != nil {
			rows.Close()
			return item, err
		}
		item.SampleCount++
		if kind == "success" {
			item.SuccessCount++
			item.EligibleCount++
			if duration >= 0 {
				durations = append(durations, duration)
			}
		} else {
			item.ErrorCount++
			item.ErrorCategoryCounts[class]++
			if class != model.ErrorClassClient && class != model.ErrorClassModelCapability && class != model.ErrorClassSemantic {
				item.EligibleCount++
			}
		}
	}
	if err := rows.Close(); err != nil {
		return item, err
	}
	if item.EligibleCount > 0 {
		item.SuccessRate = round2(float64(item.SuccessCount) * 100 / float64(item.EligibleCount))
	}
	if len(durations) > 0 {
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		item.P50DurationMS = percentile(durations, .50)
		item.P90DurationMS = percentile(durations, .90)
		item.P99DurationMS = percentile(durations, .99)
	}
	if accountID > 0 {
		_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE account_id=? AND created_at>=? AND created_at<? AND type='health_stage_changed'`,
			accountID, formatTime(since), formatTime(until)).Scan(&item.StateChanges)
		_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE account_id=? AND created_at>=? AND created_at<? AND type='automatic_pause'`,
			accountID, formatTime(since), formatTime(until)).Scan(&item.AutomaticPauseCount)
	}
	return item, nil
}

func (s *Store) ListAccountEvidence(ctx context.Context, accountID int64, limit int) ([]map[string]any, error) {
	if accountID <= 0 {
		return nil, errors.New("invalid account id")
	}
	if limit < 1 || limit > 50 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `SELECT kind,model,duration_ms,status_code,error_class,reason_code,created_at
		FROM traffic_events WHERE account_id=? ORDER BY created_at DESC LIMIT ?`, accountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var kind, modelName, errorClass, reasonCode, createdAt string
		var duration int64
		var statusCode int
		if err := rows.Scan(&kind, &modelName, &duration, &statusCode, &errorClass, &reasonCode, &createdAt); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{"kind": kind, "model": modelName, "duration_ms": duration,
			"status_code": statusCode, "error_class": errorClass, "reason_code": reasonCode, "created_at": createdAt})
	}
	return items, rows.Err()
}

func (s *Store) CleanupAgentData(ctx context.Context, before time.Time) error {
	cutoff := formatTime(before)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	statements := []string{
		`DELETE FROM agent_messages WHERE created_at<?`,
		`DELETE FROM agent_conversations WHERE updated_at<?`,
		`DELETE FROM agent_tool_calls WHERE created_at<?`,
		`DELETE FROM agent_runs WHERE started_at<?`,
		`DELETE FROM availability_assessments WHERE created_at<?`,
		`DELETE FROM analysis_packets WHERE created_at<?`,
		`DELETE FROM decision_outcomes WHERE created_at<?`,
		`DELETE FROM agent_daily_reports WHERE created_at<?`,
		`DELETE FROM score_policy_versions WHERE created_at<? AND status!='active' AND id NOT IN (
			SELECT id FROM (SELECT id,ROW_NUMBER() OVER(PARTITION BY scope_type,scope_id ORDER BY version DESC) AS position
			FROM score_policy_versions) WHERE position<=2
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement, cutoff); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func parseNullableTime(value sql.NullString) *time.Time {
	if !value.Valid || value.String == "" {
		return nil
	}
	parsed := parseTime(value.String)
	if parsed.IsZero() {
		return nil
	}
	return &parsed
}

func nullableInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}

func formatOptionalTime(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return formatTime(value.UTC())
}

func percentile(values []int64, ratio float64) int64 {
	if len(values) == 0 {
		return 0
	}
	position := int(math.Ceil(float64(len(values))*ratio)) - 1
	if position < 0 {
		position = 0
	}
	if position >= len(values) {
		position = len(values) - 1
	}
	return values[position]
}

func round2(value float64) float64 { return math.Round(value*100) / 100 }
