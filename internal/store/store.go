package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(path string, defaults model.Settings) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.migrateTelemetry(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.migrateAgent(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.migrateAgentV2(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.repairLegacyText(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	if err := store.seedSettings(context.Background(), defaults); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) repairLegacyText(ctx context.Context) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE events SET message=? WHERE type=? AND instr(message, ?) > 0`,
		"\u4e0a\u6e38\u8fde\u7eed\u68c0\u6d4b\u5f02\u5e38",
		"would_pause",
		"\ufffd",
	)
	return err
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS account_policies (
			account_id INTEGER PRIMARY KEY,
			monitor_id INTEGER,
			excluded INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			failure_threshold INTEGER,
			recovery_threshold INTEGER,
			flap_enabled INTEGER,
			flap_window_minutes INTEGER,
			flap_pause_threshold INTEGER,
			flap_recovery_threshold INTEGER,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS monitor_states (
			monitor_id INTEGER PRIMARY KEY,
			last_checked_at TEXT,
			last_status TEXT NOT NULL DEFAULT '',
			healthy_streak INTEGER NOT NULL DEFAULT 0,
			unhealthy_streak INTEGER NOT NULL DEFAULT 0,
			phase TEXT NOT NULL DEFAULT 'unknown',
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS monitor_observations (
			monitor_id INTEGER NOT NULL,
			checked_at TEXT NOT NULL,
			status TEXT NOT NULL,
			latency_ms INTEGER NOT NULL DEFAULT 0,
			availability_7d REAL NOT NULL DEFAULT 0,
			extra_ok INTEGER NOT NULL DEFAULT 0,
			extra_degraded INTEGER NOT NULL DEFAULT 0,
			extra_failed INTEGER NOT NULL DEFAULT 0,
			decrypt_failed INTEGER NOT NULL DEFAULT 0,
			score REAL NOT NULL DEFAULT 0,
			confidence REAL NOT NULL DEFAULT 0,
			reason_json TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			PRIMARY KEY(monitor_id,checked_at)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_observations_checked_at ON monitor_observations(checked_at)`,
		`CREATE INDEX IF NOT EXISTS idx_monitor_observations_monitor_checked ON monitor_observations(monitor_id,checked_at DESC)`,
		`CREATE TABLE IF NOT EXISTS monitor_health_states (
			monitor_id INTEGER PRIMARY KEY,
			stage TEXT NOT NULL DEFAULT 'frozen',
			score REAL NOT NULL DEFAULT 0,
			confidence REAL NOT NULL DEFAULT 0,
			baseline_latency_ms INTEGER NOT NULL DEFAULT 0,
			current_latency_ms INTEGER NOT NULL DEFAULT 0,
			availability_15m REAL NOT NULL DEFAULT 0,
			availability_1h REAL NOT NULL DEFAULT 0,
			availability_24h REAL NOT NULL DEFAULT 0,
			sample_count INTEGER NOT NULL DEFAULT 0,
			recovery_healthy_count INTEGER NOT NULL DEFAULT 0,
			last_two_healthy INTEGER NOT NULL DEFAULT 0,
			recovery_eligible INTEGER NOT NULL DEFAULT 0,
			next_recovery_condition TEXT NOT NULL DEFAULT '',
			hold_until TEXT,
			last_transition_at TEXT,
			reason_json TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS account_controls (
			account_id INTEGER PRIMARY KEY,
			monitor_id INTEGER,
			owns_pause INTEGER NOT NULL DEFAULT 0,
			owner TEXT NOT NULL DEFAULT '',
			expected_schedulable INTEGER,
			manual_override_until TEXT,
			last_observed_schedulable INTEGER,
			last_decision TEXT NOT NULL DEFAULT '',
			last_action_at TEXT,
			flap_active INTEGER NOT NULL DEFAULT 0,
			flap_triggered_at TEXT,
			flap_recovery_required INTEGER NOT NULL DEFAULT 0,
			cost_locked INTEGER NOT NULL DEFAULT 0,
			cost_source_id INTEGER,
			cost_pool TEXT NOT NULL DEFAULT '',
			load_pin_value INTEGER,
			load_pin_until TEXT,
			load_pin_owner TEXT NOT NULL DEFAULT '',
			load_pin_reason TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			type TEXT NOT NULL,
			severity TEXT NOT NULL,
			monitor_id INTEGER,
			account_id INTEGER,
			message TEXT NOT NULL,
			before_state TEXT NOT NULL DEFAULT '',
			after_state TEXT NOT NULL DEFAULT '',
			details TEXT NOT NULL DEFAULT '',
			actor TEXT NOT NULL DEFAULT 'system',
			created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_events_automatic_pause ON events(type,account_id,created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS sessions (
			token_hash TEXT PRIMARY KEY,
			csrf_token TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			expires_at TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS upstream_sources (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			provider TEXT NOT NULL,
			base_url TEXT NOT NULL,
			normalized_url TEXT NOT NULL UNIQUE,
			credential_nonce BLOB NOT NULL,
			credential_ciphertext BLOB NOT NULL,
			pause_below REAL NOT NULL,
			resume_at REAL NOT NULL,
			enabled INTEGER NOT NULL DEFAULT 1,
			balance REAL,
			unit TEXT NOT NULL DEFAULT '',
			low_streak INTEGER NOT NULL DEFAULT 0,
			recovery_streak INTEGER NOT NULL DEFAULT 0,
			balance_locked INTEGER NOT NULL DEFAULT 0,
			last_attempt_at TEXT,
			last_success_at TEXT,
			last_error TEXT NOT NULL DEFAULT '',
			selected_key_id TEXT NOT NULL DEFAULT '',
			routing_enabled INTEGER NOT NULL DEFAULT 0,
			routing_pool TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS upstream_key_rates (
			source_id INTEGER NOT NULL,
			external_id TEXT NOT NULL,
			name TEXT NOT NULL,
			key_hint TEXT NOT NULL DEFAULT '',
			group_id TEXT NOT NULL DEFAULT '',
			group_name TEXT NOT NULL DEFAULT '',
			rate_multiplier REAL,
			dynamic INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL,
			PRIMARY KEY(source_id,external_id),
			FOREIGN KEY(source_id) REFERENCES upstream_sources(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS upstream_groups (
			source_id INTEGER NOT NULL,
			external_id TEXT NOT NULL,
			name TEXT NOT NULL,
			rate_multiplier REAL NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(source_id,external_id),
			FOREIGN KEY(source_id) REFERENCES upstream_sources(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS upstream_group_failover_policies (
			source_id INTEGER NOT NULL,
			key_id TEXT NOT NULL,
			key_name TEXT NOT NULL DEFAULT '',
			key_hint TEXT NOT NULL DEFAULT '',
			enabled INTEGER NOT NULL DEFAULT 1,
			main_group_id TEXT NOT NULL,
			backup_group_id TEXT NOT NULL,
			emergency_group_id TEXT NOT NULL,
			pool TEXT NOT NULL DEFAULT '',
			version INTEGER NOT NULL DEFAULT 1,
			confirmed_version INTEGER NOT NULL DEFAULT 0,
			confirmed_at TEXT,
			confirmed_by TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(source_id,key_id),
			FOREIGN KEY(source_id) REFERENCES upstream_sources(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS upstream_group_failover_accounts (
			source_id INTEGER NOT NULL,
			key_id TEXT NOT NULL,
			account_id INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY(source_id,key_id,account_id),
			FOREIGN KEY(source_id,key_id) REFERENCES upstream_group_failover_policies(source_id,key_id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_group_failover_accounts_account ON upstream_group_failover_accounts(account_id)`,
		`CREATE TABLE IF NOT EXISTS upstream_group_failover_states (
			source_id INTEGER NOT NULL,
			key_id TEXT NOT NULL,
			current_tier TEXT NOT NULL DEFAULT '',
			observed_group_id TEXT NOT NULL DEFAULT '',
			previous_tier TEXT NOT NULL DEFAULT '',
			previous_stable_tier TEXT NOT NULL DEFAULT '',
			previous_group_id TEXT NOT NULL DEFAULT '',
			frozen INTEGER NOT NULL DEFAULT 0,
			freeze_reason TEXT NOT NULL DEFAULT '',
			last_error TEXT NOT NULL DEFAULT '',
			manual_hold_until TEXT,
			manual_override_until TEXT,
			cooldown_until TEXT,
			return_blocked_until TEXT,
			recovery_since TEXT,
			last_switch_at TEXT,
			last_transition_at TEXT,
			verification_started_at TEXT,
			healthy_since TEXT,
			recovery_healthy_count INTEGER NOT NULL DEFAULT 0,
			last_confirmed_at TEXT,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(source_id,key_id),
			FOREIGN KEY(source_id,key_id) REFERENCES upstream_group_failover_policies(source_id,key_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS upstream_group_transitions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			idempotency_key TEXT NOT NULL UNIQUE,
			source_id INTEGER NOT NULL,
			key_id TEXT NOT NULL,
			from_tier TEXT NOT NULL DEFAULT '',
			to_tier TEXT NOT NULL,
			from_group_id TEXT NOT NULL DEFAULT '',
			to_group_id TEXT NOT NULL,
			status TEXT NOT NULL,
			actor TEXT NOT NULL,
			reason TEXT NOT NULL DEFAULT '',
			evidence TEXT NOT NULL DEFAULT '',
			trigger TEXT NOT NULL DEFAULT '',
			packet_id INTEGER NOT NULL DEFAULT 0,
			run_id INTEGER NOT NULL DEFAULT 0,
			dry_run INTEGER NOT NULL DEFAULT 0,
			error TEXT NOT NULL DEFAULT '',
			manual INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			completed_at TEXT,
			FOREIGN KEY(source_id,key_id) REFERENCES upstream_group_failover_policies(source_id,key_id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_group_transitions_key_time ON upstream_group_transitions(source_id,key_id,created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_group_transitions_status_time ON upstream_group_transitions(status,created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS balance_account_locks (
			source_id INTEGER NOT NULL,
			account_id INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY(source_id,account_id),
			FOREIGN KEY(source_id) REFERENCES upstream_sources(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_balance_account_locks_account ON balance_account_locks(account_id)`,
		`CREATE TABLE IF NOT EXISTS cost_account_locks (
			account_id INTEGER PRIMARY KEY,
			source_id INTEGER NOT NULL,
			pool TEXT NOT NULL,
			rate_multiplier REAL NOT NULL,
			created_at TEXT NOT NULL,
			FOREIGN KEY(source_id) REFERENCES upstream_sources(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_cost_account_locks_source ON cost_account_locks(source_id)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate database: %w", err)
		}
	}
	columns := []struct {
		table      string
		name       string
		definition string
	}{
		{"account_policies", "flap_enabled", "INTEGER"},
		{"account_policies", "flap_window_minutes", "INTEGER"},
		{"account_policies", "flap_pause_threshold", "INTEGER"},
		{"account_policies", "flap_recovery_threshold", "INTEGER"},
		{"account_controls", "flap_active", "INTEGER NOT NULL DEFAULT 0"},
		{"account_controls", "flap_triggered_at", "TEXT"},
		{"account_controls", "flap_recovery_required", "INTEGER NOT NULL DEFAULT 0"},
		{"account_controls", "health_locked", "INTEGER NOT NULL DEFAULT 0"},
		{"account_controls", "manual_locked", "INTEGER NOT NULL DEFAULT 0"},
		{"account_controls", "cost_locked", "INTEGER NOT NULL DEFAULT 0"},
		{"account_controls", "cost_source_id", "INTEGER"},
		{"account_controls", "cost_pool", "TEXT NOT NULL DEFAULT ''"},
		{"account_controls", "owns_load_factor", "INTEGER NOT NULL DEFAULT 0"},
		{"account_controls", "original_load_factor", "INTEGER"},
		{"account_controls", "expected_load_factor", "INTEGER"},
		{"account_controls", "load_stage", "TEXT NOT NULL DEFAULT ''"},
		{"account_controls", "load_override_until", "TEXT"},
		{"account_controls", "load_pin_value", "INTEGER"},
		{"account_controls", "load_pin_until", "TEXT"},
		{"account_controls", "load_pin_owner", "TEXT NOT NULL DEFAULT ''"},
		{"account_controls", "load_pin_reason", "TEXT NOT NULL DEFAULT ''"},
		{"account_controls", "recovery_step", "INTEGER NOT NULL DEFAULT 0"},
		{"account_controls", "recovery_started_at", "TEXT"},
		{"upstream_sources", "selected_key_id", "TEXT NOT NULL DEFAULT ''"},
		{"upstream_sources", "routing_enabled", "INTEGER NOT NULL DEFAULT 0"},
		{"upstream_sources", "routing_pool", "TEXT NOT NULL DEFAULT ''"},
		{"upstream_sources", "credential_mode", "TEXT NOT NULL DEFAULT 'unknown'"},
		{"upstream_sources", "credential_migration_required", "INTEGER NOT NULL DEFAULT 0"},
		{"upstream_group_failover_policies", "pool", "TEXT NOT NULL DEFAULT ''"},
		{"upstream_group_failover_states", "previous_stable_tier", "TEXT NOT NULL DEFAULT ''"},
		{"upstream_group_failover_states", "last_error", "TEXT NOT NULL DEFAULT ''"},
		{"upstream_group_failover_states", "manual_override_until", "TEXT"},
		{"upstream_group_failover_states", "last_transition_at", "TEXT"},
		{"upstream_group_failover_states", "verification_started_at", "TEXT"},
		{"upstream_group_failover_states", "healthy_since", "TEXT"},
		{"upstream_group_failover_states", "recovery_healthy_count", "INTEGER NOT NULL DEFAULT 0"},
		{"upstream_group_transitions", "trigger", "TEXT NOT NULL DEFAULT ''"},
		{"upstream_group_transitions", "packet_id", "INTEGER NOT NULL DEFAULT 0"},
		{"upstream_group_transitions", "run_id", "INTEGER NOT NULL DEFAULT 0"},
		{"upstream_group_transitions", "dry_run", "INTEGER NOT NULL DEFAULT 0"},
		{"upstream_key_rates", "group_id", "TEXT NOT NULL DEFAULT ''"},
		{"account_policies", "healthy_score_threshold", "INTEGER"},
		{"account_policies", "watch_score_threshold", "INTEGER"},
		{"account_policies", "quarantine_score_threshold", "INTEGER"},
		{"account_policies", "minimum_samples", "INTEGER"},
		{"account_policies", "latency_warning_ms", "INTEGER"},
		{"account_policies", "latency_critical_ms", "INTEGER"},
		{"account_policies", "traffic_pause_below", "INTEGER"},
		{"account_policies", "traffic_healthy_at", "INTEGER"},
		{"account_policies", "hard_failures_10_threshold", "INTEGER"},
		{"account_policies", "persistent_slow_rate", "INTEGER"},
		{"account_policies", "score_policy_source", "TEXT NOT NULL DEFAULT ''"},
		{"account_policies", "score_policy_version_id", "INTEGER"},
	}
	for _, column := range columns {
		if err := s.ensureColumn(ctx, column.table, column.name, column.definition); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_events_automatic_pause ON events(type,account_id,created_at DESC)`); err != nil {
		return fmt.Errorf("create automatic pause index: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE account_controls SET health_locked=1 WHERE owns_pause=1 AND owner='automatic' AND health_locked=0 AND manual_locked=0`); err != nil {
		return fmt.Errorf("migrate health locks: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE account_controls SET manual_locked=1 WHERE owns_pause=1 AND owner='operator' AND manual_locked=0`); err != nil {
		return fmt.Errorf("migrate manual locks: %w", err)
	}
	return nil
}

func (s *Store) ensureColumn(ctx context.Context, table, name, definition string) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return fmt.Errorf("inspect %s schema: %w", table, err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var columnName, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return err
		}
		if columnName == name {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN `+name+` `+definition); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, name, err)
	}
	return nil
}

func (s *Store) seedSettings(ctx context.Context, defaults model.Settings) error {
	if defaults.FlapWindowMinutes < 1 {
		defaults.FlapWindowMinutes = 60
	}
	if defaults.FlapPauseThreshold < 1 {
		defaults.FlapPauseThreshold = 3
	}
	if defaults.FlapRecoveryThreshold < 1 {
		defaults.FlapRecoveryThreshold = 10
	}
	applyHealthSettingDefaults(&defaults)
	applyFailoverSettingDefaults(&defaults)
	values := map[string]string{
		"dry_run":                                   strconv.FormatBool(defaults.DryRun),
		"failure_threshold":                         strconv.Itoa(defaults.FailureThreshold),
		"recovery_threshold":                        strconv.Itoa(defaults.RecoveryThreshold),
		"manual_hold_minutes":                       strconv.Itoa(defaults.ManualHoldMinutes),
		"flap_window_minutes":                       strconv.Itoa(defaults.FlapWindowMinutes),
		"flap_pause_threshold":                      strconv.Itoa(defaults.FlapPauseThreshold),
		"flap_recovery_threshold":                   strconv.Itoa(defaults.FlapRecoveryThreshold),
		"health_engine_mode":                        defaults.HealthMode,
		"healthy_score_threshold":                   strconv.Itoa(defaults.HealthHealthyScore),
		"watch_score_threshold":                     strconv.Itoa(defaults.HealthWatchScore),
		"quarantine_score_threshold":                strconv.Itoa(defaults.HealthQuarantineScore),
		"minimum_samples":                           strconv.Itoa(defaults.HealthMinSamples),
		"latency_warning_ms":                        strconv.FormatInt(defaults.HealthLatencyWarningMS, 10),
		"latency_critical_ms":                       strconv.FormatInt(defaults.HealthLatencyCriticalMS, 10),
		"traffic_pause_below":                       strconv.Itoa(defaults.HealthTrafficPauseBelow),
		"traffic_healthy_at":                        strconv.Itoa(defaults.HealthTrafficHealthyAt),
		"hard_failures_10_threshold":                strconv.Itoa(defaults.HealthHardFailures10),
		"persistent_slow_rate":                      strconv.Itoa(defaults.HealthPersistentSlowRate),
		"quarantine_minutes":                        strconv.Itoa(defaults.HealthQuarantineMinutes),
		"recovery_window_size":                      strconv.Itoa(defaults.HealthRecoveryWindow),
		"recovery_required_successes":               strconv.Itoa(defaults.HealthRecoverySuccesses),
		"recovery_initial_percent":                  strconv.Itoa(defaults.HealthTrialPercent),
		"recovery_mid_percent":                      strconv.Itoa(defaults.HealthMidPercent),
		"degraded_load_percent":                     strconv.Itoa(defaults.HealthDegradedPercent),
		"recovery_stage_minutes":                    strconv.Itoa(defaults.HealthTrialMinutes),
		"load_manual_hold_minutes":                  strconv.Itoa(defaults.HealthLoadOverrideMinutes),
		"group_failover_account_fresh_minutes":      strconv.Itoa(defaults.FailoverAccountFreshMinutes),
		"group_failover_telemetry_fresh_minutes":    strconv.Itoa(defaults.FailoverTelemetryFreshMinutes),
		"group_failover_data_fresh_minutes":         strconv.Itoa(defaults.FailoverGroupFreshMinutes),
		"group_failover_agent_grace_seconds":        strconv.Itoa(defaults.FailoverAgentGraceSeconds),
		"group_failover_monitor_failures":           strconv.Itoa(defaults.FailoverMonitorFailures),
		"group_failover_no_traffic_failures":        strconv.Itoa(defaults.FailoverNoTrafficFailures),
		"group_failover_traffic_window_minutes":     strconv.Itoa(defaults.FailoverTrafficWindowMinutes),
		"group_failover_traffic_min_samples":        strconv.Itoa(defaults.FailoverTrafficMinSamples),
		"group_failover_traffic_success_below":      strconv.Itoa(defaults.FailoverTrafficSuccessBelow),
		"group_failover_consecutive_hard_errors":    strconv.Itoa(defaults.FailoverConsecutiveHardErrors),
		"group_failover_backup_verify_minutes":      strconv.Itoa(defaults.FailoverBackupVerifyMinutes),
		"group_failover_post_switch_monitors":       strconv.Itoa(defaults.FailoverPostSwitchMonitors),
		"group_failover_post_switch_requests":       strconv.Itoa(defaults.FailoverPostSwitchRequests),
		"group_failover_main_verify_minutes":        strconv.Itoa(defaults.FailoverMainVerifyMinutes),
		"group_failover_switch_cooldown_minutes":    strconv.Itoa(defaults.FailoverSwitchCooldownMinutes),
		"group_failover_manual_protection_minutes":  strconv.Itoa(defaults.FailoverManualProtectionMinutes),
		"group_failover_short_limit_window_minutes": strconv.Itoa(defaults.FailoverShortLimitWindowMinutes),
		"group_failover_short_limit_count":          strconv.Itoa(defaults.FailoverShortLimitCount),
		"group_failover_long_limit_window_minutes":  strconv.Itoa(defaults.FailoverLongLimitWindowMinutes),
		"group_failover_long_limit_count":           strconv.Itoa(defaults.FailoverLongLimitCount),
		"group_failover_recovery_window_minutes":    strconv.Itoa(defaults.FailoverRecoveryWindowMinutes),
		"group_failover_recovery_stable_minutes":    strconv.Itoa(defaults.FailoverRecoveryStableMinutes),
		"group_failover_recovery_monitor_successes": strconv.Itoa(defaults.FailoverRecoveryMonitorSuccesses),
		"group_failover_recovery_min_samples":       strconv.Itoa(defaults.FailoverRecoveryMinSamples),
		"group_failover_recovery_success_at":        strconv.Itoa(defaults.FailoverRecoverySuccessAt),
		"group_failover_return_retry_minutes":       strconv.Itoa(defaults.FailoverReturnRetryMinutes),
	}
	now := formatTime(time.Now())
	for key, value := range values {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO NOTHING`, key, value, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetSettings(ctx context.Context) (model.Settings, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key,value FROM settings`)
	if err != nil {
		return model.Settings{}, err
	}
	defer rows.Close()
	values := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return model.Settings{}, err
		}
		values[key] = value
	}
	settings := model.Settings{
		DryRun:                           parseBool(values["dry_run"], true),
		FailureThreshold:                 parseInt(values["failure_threshold"], 3),
		RecoveryThreshold:                parseInt(values["recovery_threshold"], 3),
		ManualHoldMinutes:                parseInt(values["manual_hold_minutes"], 10),
		FlapWindowMinutes:                parseInt(values["flap_window_minutes"], 60),
		FlapPauseThreshold:               parseInt(values["flap_pause_threshold"], 3),
		FlapRecoveryThreshold:            parseInt(values["flap_recovery_threshold"], 10),
		HealthMode:                       parseHealthMode(values["health_engine_mode"]),
		HealthHealthyScore:               parseInt(values["healthy_score_threshold"], 80),
		HealthWatchScore:                 parseInt(values["watch_score_threshold"], 60),
		HealthQuarantineScore:            parseInt(values["quarantine_score_threshold"], 35),
		HealthMinSamples:                 parseInt(values["minimum_samples"], 10),
		HealthLatencyWarningMS:           parseInt64(values["latency_warning_ms"], 8000),
		HealthLatencyCriticalMS:          parseInt64(values["latency_critical_ms"], 15000),
		HealthTrafficPauseBelow:          parseInt(values["traffic_pause_below"], 80),
		HealthTrafficHealthyAt:           parseInt(values["traffic_healthy_at"], 95),
		HealthHardFailures10:             parseInt(values["hard_failures_10_threshold"], 5),
		HealthPersistentSlowRate:         parseInt(values["persistent_slow_rate"], 40),
		HealthQuarantineMinutes:          parseInt(values["quarantine_minutes"], 5),
		HealthRecoveryWindow:             parseInt(values["recovery_window_size"], 10),
		HealthRecoverySuccesses:          parseInt(values["recovery_required_successes"], 8),
		HealthTrialPercent:               parseInt(values["recovery_initial_percent"], 25),
		HealthMidPercent:                 parseInt(values["recovery_mid_percent"], 50),
		HealthDegradedPercent:            parseInt(values["degraded_load_percent"], 50),
		HealthTrialMinutes:               parseInt(values["recovery_stage_minutes"], 5),
		HealthLoadOverrideMinutes:        parseInt(values["load_manual_hold_minutes"], 30),
		FailoverAccountFreshMinutes:      parseInt(values["group_failover_account_fresh_minutes"], 3),
		FailoverTelemetryFreshMinutes:    parseInt(values["group_failover_telemetry_fresh_minutes"], 6),
		FailoverGroupFreshMinutes:        parseInt(values["group_failover_data_fresh_minutes"], 30),
		FailoverAgentGraceSeconds:        parseInt(values["group_failover_agent_grace_seconds"], 90),
		FailoverMonitorFailures:          parseInt(values["group_failover_monitor_failures"], 3),
		FailoverNoTrafficFailures:        parseInt(values["group_failover_no_traffic_failures"], 5),
		FailoverTrafficWindowMinutes:     parseInt(values["group_failover_traffic_window_minutes"], 5),
		FailoverTrafficMinSamples:        parseInt(values["group_failover_traffic_min_samples"], 10),
		FailoverTrafficSuccessBelow:      parseInt(values["group_failover_traffic_success_below"], 20),
		FailoverConsecutiveHardErrors:    parseInt(values["group_failover_consecutive_hard_errors"], 5),
		FailoverBackupVerifyMinutes:      parseInt(values["group_failover_backup_verify_minutes"], 2),
		FailoverPostSwitchMonitors:       parseInt(values["group_failover_post_switch_monitors"], 2),
		FailoverPostSwitchRequests:       parseInt(values["group_failover_post_switch_requests"], 5),
		FailoverMainVerifyMinutes:        parseInt(values["group_failover_main_verify_minutes"], 5),
		FailoverSwitchCooldownMinutes:    parseInt(values["group_failover_switch_cooldown_minutes"], 15),
		FailoverManualProtectionMinutes:  parseInt(values["group_failover_manual_protection_minutes"], 30),
		FailoverShortLimitWindowMinutes:  parseInt(values["group_failover_short_limit_window_minutes"], 30),
		FailoverShortLimitCount:          parseInt(values["group_failover_short_limit_count"], 2),
		FailoverLongLimitWindowMinutes:   parseInt(values["group_failover_long_limit_window_minutes"], 360),
		FailoverLongLimitCount:           parseInt(values["group_failover_long_limit_count"], 3),
		FailoverRecoveryWindowMinutes:    parseInt(values["group_failover_recovery_window_minutes"], 30),
		FailoverRecoveryStableMinutes:    parseInt(values["group_failover_recovery_stable_minutes"], 30),
		FailoverRecoveryMonitorSuccesses: parseInt(values["group_failover_recovery_monitor_successes"], 10),
		FailoverRecoveryMinSamples:       parseInt(values["group_failover_recovery_min_samples"], 20),
		FailoverRecoverySuccessAt:        parseInt(values["group_failover_recovery_success_at"], 98),
		FailoverReturnRetryMinutes:       parseInt(values["group_failover_return_retry_minutes"], 120),
	}
	applyFailoverSettingDefaults(&settings)
	return settings, rows.Err()
}

func settingsValues(settings model.Settings) (map[string]string, error) {
	if settings.FailureThreshold < 1 || settings.RecoveryThreshold < 1 || settings.ManualHoldMinutes < 1 || settings.FlapWindowMinutes < 1 || settings.FlapPauseThreshold < 1 || settings.FlapRecoveryThreshold < 1 || !validHealthSettings(settings) || !validFailoverSettings(settings) {
		return nil, errors.New("settings values must be positive")
	}
	values := map[string]string{
		"dry_run":                                   strconv.FormatBool(settings.DryRun),
		"failure_threshold":                         strconv.Itoa(settings.FailureThreshold),
		"recovery_threshold":                        strconv.Itoa(settings.RecoveryThreshold),
		"manual_hold_minutes":                       strconv.Itoa(settings.ManualHoldMinutes),
		"flap_window_minutes":                       strconv.Itoa(settings.FlapWindowMinutes),
		"flap_pause_threshold":                      strconv.Itoa(settings.FlapPauseThreshold),
		"flap_recovery_threshold":                   strconv.Itoa(settings.FlapRecoveryThreshold),
		"health_engine_mode":                        settings.HealthMode,
		"healthy_score_threshold":                   strconv.Itoa(settings.HealthHealthyScore),
		"watch_score_threshold":                     strconv.Itoa(settings.HealthWatchScore),
		"quarantine_score_threshold":                strconv.Itoa(settings.HealthQuarantineScore),
		"minimum_samples":                           strconv.Itoa(settings.HealthMinSamples),
		"latency_warning_ms":                        strconv.FormatInt(settings.HealthLatencyWarningMS, 10),
		"latency_critical_ms":                       strconv.FormatInt(settings.HealthLatencyCriticalMS, 10),
		"traffic_pause_below":                       strconv.Itoa(settings.HealthTrafficPauseBelow),
		"traffic_healthy_at":                        strconv.Itoa(settings.HealthTrafficHealthyAt),
		"hard_failures_10_threshold":                strconv.Itoa(settings.HealthHardFailures10),
		"persistent_slow_rate":                      strconv.Itoa(settings.HealthPersistentSlowRate),
		"quarantine_minutes":                        strconv.Itoa(settings.HealthQuarantineMinutes),
		"recovery_window_size":                      strconv.Itoa(settings.HealthRecoveryWindow),
		"recovery_required_successes":               strconv.Itoa(settings.HealthRecoverySuccesses),
		"recovery_initial_percent":                  strconv.Itoa(settings.HealthTrialPercent),
		"recovery_mid_percent":                      strconv.Itoa(settings.HealthMidPercent),
		"degraded_load_percent":                     strconv.Itoa(settings.HealthDegradedPercent),
		"recovery_stage_minutes":                    strconv.Itoa(settings.HealthTrialMinutes),
		"load_manual_hold_minutes":                  strconv.Itoa(settings.HealthLoadOverrideMinutes),
		"group_failover_account_fresh_minutes":      strconv.Itoa(settings.FailoverAccountFreshMinutes),
		"group_failover_telemetry_fresh_minutes":    strconv.Itoa(settings.FailoverTelemetryFreshMinutes),
		"group_failover_data_fresh_minutes":         strconv.Itoa(settings.FailoverGroupFreshMinutes),
		"group_failover_agent_grace_seconds":        strconv.Itoa(settings.FailoverAgentGraceSeconds),
		"group_failover_monitor_failures":           strconv.Itoa(settings.FailoverMonitorFailures),
		"group_failover_no_traffic_failures":        strconv.Itoa(settings.FailoverNoTrafficFailures),
		"group_failover_traffic_window_minutes":     strconv.Itoa(settings.FailoverTrafficWindowMinutes),
		"group_failover_traffic_min_samples":        strconv.Itoa(settings.FailoverTrafficMinSamples),
		"group_failover_traffic_success_below":      strconv.Itoa(settings.FailoverTrafficSuccessBelow),
		"group_failover_consecutive_hard_errors":    strconv.Itoa(settings.FailoverConsecutiveHardErrors),
		"group_failover_backup_verify_minutes":      strconv.Itoa(settings.FailoverBackupVerifyMinutes),
		"group_failover_post_switch_monitors":       strconv.Itoa(settings.FailoverPostSwitchMonitors),
		"group_failover_post_switch_requests":       strconv.Itoa(settings.FailoverPostSwitchRequests),
		"group_failover_main_verify_minutes":        strconv.Itoa(settings.FailoverMainVerifyMinutes),
		"group_failover_switch_cooldown_minutes":    strconv.Itoa(settings.FailoverSwitchCooldownMinutes),
		"group_failover_manual_protection_minutes":  strconv.Itoa(settings.FailoverManualProtectionMinutes),
		"group_failover_short_limit_window_minutes": strconv.Itoa(settings.FailoverShortLimitWindowMinutes),
		"group_failover_short_limit_count":          strconv.Itoa(settings.FailoverShortLimitCount),
		"group_failover_long_limit_window_minutes":  strconv.Itoa(settings.FailoverLongLimitWindowMinutes),
		"group_failover_long_limit_count":           strconv.Itoa(settings.FailoverLongLimitCount),
		"group_failover_recovery_window_minutes":    strconv.Itoa(settings.FailoverRecoveryWindowMinutes),
		"group_failover_recovery_stable_minutes":    strconv.Itoa(settings.FailoverRecoveryStableMinutes),
		"group_failover_recovery_monitor_successes": strconv.Itoa(settings.FailoverRecoveryMonitorSuccesses),
		"group_failover_recovery_min_samples":       strconv.Itoa(settings.FailoverRecoveryMinSamples),
		"group_failover_recovery_success_at":        strconv.Itoa(settings.FailoverRecoverySuccessAt),
		"group_failover_return_retry_minutes":       strconv.Itoa(settings.FailoverReturnRetryMinutes),
	}
	return values, nil
}

func writeSettings(ctx context.Context, executor policyExecutor, values map[string]string, updatedAt time.Time) error {
	for key, value := range values {
		if _, err := executor.ExecContext(ctx, `INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, key, value, formatTime(updatedAt)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) UpdateSettings(ctx context.Context, settings model.Settings) error {
	values, err := settingsValues(settings)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := writeSettings(ctx, tx, values, time.Now().UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ListPolicies(ctx context.Context) (map[int64]model.Policy, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT account_id,score_policy_source,score_policy_version_id,monitor_id,excluded,enabled,failure_threshold,recovery_threshold,
		flap_enabled,flap_window_minutes,flap_pause_threshold,flap_recovery_threshold,healthy_score_threshold,
		watch_score_threshold,quarantine_score_threshold,minimum_samples,latency_warning_ms,latency_critical_ms,
		traffic_pause_below,traffic_healthy_at,hard_failures_10_threshold,persistent_slow_rate
		FROM account_policies`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[int64]model.Policy{}
	for rows.Next() {
		var policy model.Policy
		var monitorID, failure, recovery, flapEnabled, flapWindow, flapPause, flapRecovery sql.NullInt64
		var scorePolicyVersionID sql.NullInt64
		var healthy, watch, quarantine, minimumSamples, latencyWarning, latencyCritical sql.NullInt64
		var trafficPause, trafficHealthy, hardFailures10, persistentSlow sql.NullInt64
		var excluded, enabled int
		if err := rows.Scan(&policy.AccountID, &policy.ScorePolicySource, &scorePolicyVersionID, &monitorID, &excluded, &enabled, &failure, &recovery, &flapEnabled,
			&flapWindow, &flapPause, &flapRecovery, &healthy, &watch, &quarantine, &minimumSamples,
			&latencyWarning, &latencyCritical, &trafficPause, &trafficHealthy, &hardFailures10, &persistentSlow); err != nil {
			return nil, err
		}
		policy.Excluded = excluded == 1
		policy.Enabled = enabled == 1
		policy.MonitorID = nullInt64Pointer(monitorID)
		policy.ScorePolicyVersionID = nullInt64Pointer(scorePolicyVersionID)
		policy.FailureThreshold = nullIntPointer(failure)
		policy.RecoveryThreshold = nullIntPointer(recovery)
		policy.FlapEnabled = nullBoolPointer(flapEnabled)
		policy.FlapWindowMinutes = nullIntPointer(flapWindow)
		policy.FlapPauseThreshold = nullIntPointer(flapPause)
		policy.FlapRecoveryThreshold = nullIntPointer(flapRecovery)
		policy.HealthHealthyScore = nullIntPointer(healthy)
		policy.HealthWatchScore = nullIntPointer(watch)
		policy.HealthQuarantineScore = nullIntPointer(quarantine)
		policy.HealthMinSamples = nullIntPointer(minimumSamples)
		policy.HealthLatencyWarningMS = nullInt64Pointer(latencyWarning)
		policy.HealthLatencyCriticalMS = nullInt64Pointer(latencyCritical)
		policy.HealthTrafficPauseBelow = nullIntPointer(trafficPause)
		policy.HealthTrafficHealthyAt = nullIntPointer(trafficHealthy)
		policy.HealthHardFailures10 = nullIntPointer(hardFailures10)
		policy.HealthPersistentSlowRate = nullIntPointer(persistentSlow)
		result[policy.AccountID] = policy
	}
	return result, rows.Err()
}

type policyExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func validatePolicy(policy model.Policy) error {
	if policy.AccountID <= 0 {
		return errors.New("account_id is required")
	}
	for _, threshold := range []*int{policy.FailureThreshold, policy.RecoveryThreshold, policy.FlapWindowMinutes, policy.FlapPauseThreshold, policy.FlapRecoveryThreshold} {
		if threshold != nil && *threshold < 1 {
			return errors.New("policy thresholds must be positive")
		}
	}
	for _, score := range []*int{policy.HealthHealthyScore, policy.HealthWatchScore, policy.HealthQuarantineScore, policy.HealthMinSamples} {
		if score != nil && (*score < 1 || *score > 100) {
			return errors.New("policy score values must be between 1 and 100")
		}
	}
	for _, percent := range []*int{policy.HealthTrafficPauseBelow, policy.HealthTrafficHealthyAt, policy.HealthPersistentSlowRate} {
		if percent != nil && (*percent < 1 || *percent > 100) {
			return errors.New("policy percentage values must be between 1 and 100")
		}
	}
	if policy.HealthHardFailures10 != nil && *policy.HealthHardFailures10 < 1 {
		return errors.New("policy hard failure threshold must be positive")
	}
	for _, latency := range []*int64{policy.HealthLatencyWarningMS, policy.HealthLatencyCriticalMS} {
		if latency != nil && *latency < 1 {
			return errors.New("policy latency values must be positive")
		}
	}
	return nil
}

func upsertPolicy(ctx context.Context, executor policyExecutor, policy model.Policy, updatedAt time.Time) error {
	_, err := executor.ExecContext(ctx, `INSERT INTO account_policies(account_id,score_policy_source,score_policy_version_id,monitor_id,excluded,enabled,failure_threshold,
		recovery_threshold,flap_enabled,flap_window_minutes,flap_pause_threshold,flap_recovery_threshold,
		healthy_score_threshold,watch_score_threshold,quarantine_score_threshold,minimum_samples,latency_warning_ms,
		latency_critical_ms,traffic_pause_below,traffic_healthy_at,hard_failures_10_threshold,persistent_slow_rate,
		updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id) DO UPDATE SET
		score_policy_source=excluded.score_policy_source,score_policy_version_id=excluded.score_policy_version_id,
		monitor_id=excluded.monitor_id,excluded=excluded.excluded,enabled=excluded.enabled,
		failure_threshold=excluded.failure_threshold,recovery_threshold=excluded.recovery_threshold,
		flap_enabled=excluded.flap_enabled,flap_window_minutes=excluded.flap_window_minutes,
		flap_pause_threshold=excluded.flap_pause_threshold,flap_recovery_threshold=excluded.flap_recovery_threshold,
		healthy_score_threshold=excluded.healthy_score_threshold,watch_score_threshold=excluded.watch_score_threshold,
		quarantine_score_threshold=excluded.quarantine_score_threshold,minimum_samples=excluded.minimum_samples,
		latency_warning_ms=excluded.latency_warning_ms,latency_critical_ms=excluded.latency_critical_ms,
		traffic_pause_below=excluded.traffic_pause_below,traffic_healthy_at=excluded.traffic_healthy_at,
		hard_failures_10_threshold=excluded.hard_failures_10_threshold,persistent_slow_rate=excluded.persistent_slow_rate,
		updated_at=excluded.updated_at`, policy.AccountID, policy.ScorePolicySource, policy.ScorePolicyVersionID, policy.MonitorID, boolInt(policy.Excluded), boolInt(policy.Enabled),
		policy.FailureThreshold, policy.RecoveryThreshold, nullableBool(policy.FlapEnabled), policy.FlapWindowMinutes,
		policy.FlapPauseThreshold, policy.FlapRecoveryThreshold, policy.HealthHealthyScore, policy.HealthWatchScore,
		policy.HealthQuarantineScore, policy.HealthMinSamples, policy.HealthLatencyWarningMS,
		policy.HealthLatencyCriticalMS, policy.HealthTrafficPauseBelow, policy.HealthTrafficHealthyAt,
		policy.HealthHardFailures10, policy.HealthPersistentSlowRate, formatTime(updatedAt))
	return err
}

func (s *Store) UpsertPolicy(ctx context.Context, policy model.Policy) error {
	if err := validatePolicy(policy); err != nil {
		return err
	}
	return upsertPolicy(ctx, s.db, policy, time.Now().UTC())
}

// UpsertPolicies applies a policy projection as one database unit. Pool policy
// publication must never leave only a prefix of the pool on a new version.
func (s *Store) UpsertPolicies(ctx context.Context, policies []model.Policy) error {
	if len(policies) == 0 {
		return errors.New("policies are required")
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
	updatedAt := time.Now().UTC()
	for _, policy := range policies {
		if err := upsertPolicy(ctx, tx, policy, updatedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) GetMonitorState(ctx context.Context, monitorID int64) (model.MonitorState, error) {
	var state model.MonitorState
	var checked sql.NullString
	var updated string
	err := s.db.QueryRowContext(ctx, `SELECT monitor_id,last_checked_at,last_status,healthy_streak,unhealthy_streak,phase,updated_at FROM monitor_states WHERE monitor_id=?`, monitorID).
		Scan(&state.MonitorID, &checked, &state.LastStatus, &state.HealthyStreak, &state.UnhealthyStreak, &state.Phase, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return model.MonitorState{MonitorID: monitorID, Phase: model.PhaseUnknown}, nil
	}
	if err != nil {
		return model.MonitorState{}, err
	}
	state.LastCheckedAt = parseNullTime(checked)
	state.UpdatedAt = parseTime(updated)
	return state, nil
}

func (s *Store) UpsertMonitorState(ctx context.Context, state model.MonitorState) error {
	state.UpdatedAt = time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `INSERT INTO monitor_states(monitor_id,last_checked_at,last_status,healthy_streak,unhealthy_streak,phase,updated_at)
		VALUES(?,?,?,?,?,?,?) ON CONFLICT(monitor_id) DO UPDATE SET last_checked_at=excluded.last_checked_at,last_status=excluded.last_status,healthy_streak=excluded.healthy_streak,unhealthy_streak=excluded.unhealthy_streak,phase=excluded.phase,updated_at=excluded.updated_at`,
		state.MonitorID, nullableTime(state.LastCheckedAt), state.LastStatus, state.HealthyStreak, state.UnhealthyStreak, state.Phase, formatTime(state.UpdatedAt))
	return err
}

// InsertMonitorObservation persists one real monitor result. Re-reading the same
// checked_at value is expected and is reported as inserted=false.
func (s *Store) InsertMonitorObservation(ctx context.Context, observation model.MonitorObservation) (bool, error) {
	if observation.MonitorID <= 0 || observation.CheckedAt.IsZero() {
		return false, errors.New("monitor_id and checked_at are required")
	}
	if observation.CreatedAt.IsZero() {
		observation.CreatedAt = time.Now().UTC()
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO monitor_observations(
		monitor_id,checked_at,status,latency_ms,availability_7d,extra_ok,extra_degraded,extra_failed,decrypt_failed,score,confidence,reason_json,created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(monitor_id,checked_at) DO NOTHING`,
		observation.MonitorID, formatTime(observation.CheckedAt), observation.Status, observation.LatencyMS,
		observation.Availability7D, observation.ExtraOK, observation.ExtraDegraded, observation.ExtraFailed,
		boolInt(observation.DecryptFailed), observation.Score, observation.Confidence, observation.ReasonJSON,
		formatTime(observation.CreatedAt))
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

// ListMonitorObservations returns a monitor's observations from newest to oldest.
func (s *Store) ListMonitorObservations(ctx context.Context, monitorID int64, since time.Time, limit int) ([]model.MonitorObservation, error) {
	if limit < 1 || limit > 10000 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `SELECT monitor_id,checked_at,status,latency_ms,availability_7d,extra_ok,extra_degraded,extra_failed,decrypt_failed,score,confidence,reason_json,created_at
		FROM monitor_observations WHERE monitor_id=? AND checked_at>=? ORDER BY checked_at DESC LIMIT ?`, monitorID, formatTime(since), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.MonitorObservation, 0)
	for rows.Next() {
		var item model.MonitorObservation
		var checkedAt, createdAt string
		var decryptFailed int
		if err := rows.Scan(&item.MonitorID, &checkedAt, &item.Status, &item.LatencyMS, &item.Availability7D,
			&item.ExtraOK, &item.ExtraDegraded, &item.ExtraFailed, &decryptFailed, &item.Score,
			&item.Confidence, &item.ReasonJSON, &createdAt); err != nil {
			return nil, err
		}
		item.CheckedAt = parseTime(checkedAt)
		item.DecryptFailed = decryptFailed == 1
		item.CreatedAt = parseTime(createdAt)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) DeleteMonitorObservationsBefore(ctx context.Context, before time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM monitor_observations WHERE checked_at < ?`, formatTime(before))
	return err
}

func (s *Store) GetMonitorHealthState(ctx context.Context, monitorID int64) (model.MonitorHealthState, error) {
	var state model.MonitorHealthState
	var holdUntil, transitionedAt sql.NullString
	var lastTwoHealthy, recoveryEligible int
	var updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT monitor_id,stage,score,confidence,baseline_latency_ms,current_latency_ms,
		availability_15m,availability_1h,availability_24h,sample_count,recovery_healthy_count,last_two_healthy,
		recovery_eligible,next_recovery_condition,hold_until,last_transition_at,reason_json,updated_at
		FROM monitor_health_states WHERE monitor_id=?`, monitorID).Scan(
		&state.MonitorID, &state.Stage, &state.Score, &state.Confidence, &state.BaselineLatencyMS,
		&state.CurrentLatencyMS, &state.Availability15M, &state.Availability1H, &state.Availability24H,
		&state.SampleCount, &state.RecoveryHealthyCount, &lastTwoHealthy, &recoveryEligible,
		&state.NextRecoveryCondition, &holdUntil, &transitionedAt, &state.ReasonJSON, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return model.MonitorHealthState{MonitorID: monitorID, Stage: model.HealthStageFrozen}, nil
	}
	if err != nil {
		return model.MonitorHealthState{}, err
	}
	state.LastTwoHealthy = lastTwoHealthy == 1
	state.RecoveryEligible = recoveryEligible == 1
	state.HoldUntil = parseNullTime(holdUntil)
	if transitionedAt.Valid {
		state.LastTransitionAt = parseTime(transitionedAt.String)
	}
	state.UpdatedAt = parseTime(updatedAt)
	return state, nil
}

func (s *Store) UpsertMonitorHealthState(ctx context.Context, state model.MonitorHealthState) error {
	if state.MonitorID <= 0 {
		return errors.New("monitor_id is required")
	}
	if state.UpdatedAt.IsZero() {
		state.UpdatedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO monitor_health_states(
		monitor_id,stage,score,confidence,baseline_latency_ms,current_latency_ms,availability_15m,availability_1h,
		availability_24h,sample_count,recovery_healthy_count,last_two_healthy,recovery_eligible,next_recovery_condition,
		hold_until,last_transition_at,reason_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(monitor_id) DO UPDATE SET stage=excluded.stage,score=excluded.score,confidence=excluded.confidence,
		baseline_latency_ms=excluded.baseline_latency_ms,current_latency_ms=excluded.current_latency_ms,
		availability_15m=excluded.availability_15m,availability_1h=excluded.availability_1h,
		availability_24h=excluded.availability_24h,sample_count=excluded.sample_count,
		recovery_healthy_count=excluded.recovery_healthy_count,last_two_healthy=excluded.last_two_healthy,
		recovery_eligible=excluded.recovery_eligible,next_recovery_condition=excluded.next_recovery_condition,
		hold_until=excluded.hold_until,last_transition_at=excluded.last_transition_at,reason_json=excluded.reason_json,
		updated_at=excluded.updated_at`, state.MonitorID, state.Stage, state.Score, state.Confidence,
		state.BaselineLatencyMS, state.CurrentLatencyMS, state.Availability15M, state.Availability1H,
		state.Availability24H, state.SampleCount, state.RecoveryHealthyCount, boolInt(state.LastTwoHealthy),
		boolInt(state.RecoveryEligible), state.NextRecoveryCondition, nullableTime(state.HoldUntil),
		nullableTimeValue(state.LastTransitionAt), state.ReasonJSON, formatTime(state.UpdatedAt))
	return err
}

func (s *Store) GetControl(ctx context.Context, accountID int64) (model.AccountControl, error) {
	var control model.AccountControl
	var monitorID, expected, observed, costSourceID, originalLoadFactor, expectedLoadFactor, loadPinValue sql.NullInt64
	var overrideUntil, actionAt, flapTriggeredAt, loadOverrideUntil, loadPinUntil, recoveryStartedAt sql.NullString
	var owns, flapActive, healthLocked, manualLocked, costLocked, ownsLoadFactor int
	var updated string
	err := s.db.QueryRowContext(ctx, `SELECT account_id,monitor_id,owns_pause,owner,expected_schedulable,manual_override_until,last_observed_schedulable,last_decision,last_action_at,flap_active,flap_triggered_at,flap_recovery_required,health_locked,manual_locked,cost_locked,cost_source_id,cost_pool,owns_load_factor,original_load_factor,expected_load_factor,load_stage,load_override_until,load_pin_value,load_pin_until,load_pin_owner,load_pin_reason,recovery_step,recovery_started_at,updated_at FROM account_controls WHERE account_id=?`, accountID).
		Scan(&control.AccountID, &monitorID, &owns, &control.Owner, &expected, &overrideUntil, &observed, &control.LastDecision, &actionAt, &flapActive, &flapTriggeredAt, &control.FlapRecoveryRequired, &healthLocked, &manualLocked, &costLocked, &costSourceID, &control.CostPool, &ownsLoadFactor, &originalLoadFactor, &expectedLoadFactor, &control.LoadStage, &loadOverrideUntil, &loadPinValue, &loadPinUntil, &control.LoadPinOwner, &control.LoadPinReason, &control.RecoveryStep, &recoveryStartedAt, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return model.AccountControl{AccountID: accountID}, nil
	}
	if err != nil {
		return model.AccountControl{}, err
	}
	control.MonitorID = nullInt64Pointer(monitorID)
	control.OwnsPause = owns == 1
	control.ExpectedSchedulable = nullBoolPointer(expected)
	control.LastObserved = nullBoolPointer(observed)
	control.ManualOverrideUntil = parseNullTime(overrideUntil)
	control.LastActionAt = parseNullTime(actionAt)
	control.FlapActive = flapActive == 1
	control.HealthLocked = healthLocked == 1
	control.ManualLocked = manualLocked == 1
	control.CostLocked = costLocked == 1
	control.CostSourceID = nullInt64Pointer(costSourceID)
	control.OwnsLoadFactor = ownsLoadFactor == 1
	control.OriginalLoadFactor = nullIntPointer(originalLoadFactor)
	control.ExpectedLoadFactor = nullIntPointer(expectedLoadFactor)
	control.LoadOverrideUntil = parseNullTime(loadOverrideUntil)
	control.LoadPinValue = nullIntPointer(loadPinValue)
	control.LoadPinUntil = parseNullTime(loadPinUntil)
	control.RecoveryStartedAt = parseNullTime(recoveryStartedAt)
	if control.OwnsPause && control.Owner == "automatic" && !control.HealthLocked && !control.ManualLocked {
		control.HealthLocked = true
	}
	control.FlapTriggeredAt = parseNullTime(flapTriggeredAt)
	control.UpdatedAt = parseTime(updated)
	return control, nil
}

func (s *Store) UpsertControl(ctx context.Context, control model.AccountControl) error {
	control.UpdatedAt = time.Now().UTC()
	_, err := s.db.ExecContext(ctx, controlUpsertSQL,
		controlUpsertArguments(control)...)
	return err
}

const controlUpsertSQL = `INSERT INTO account_controls(account_id,monitor_id,owns_pause,owner,expected_schedulable,manual_override_until,last_observed_schedulable,last_decision,last_action_at,flap_active,flap_triggered_at,flap_recovery_required,health_locked,manual_locked,cost_locked,cost_source_id,cost_pool,owns_load_factor,original_load_factor,expected_load_factor,load_stage,load_override_until,load_pin_value,load_pin_until,load_pin_owner,load_pin_reason,recovery_step,recovery_started_at,updated_at)
	VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id) DO UPDATE SET monitor_id=excluded.monitor_id,owns_pause=excluded.owns_pause,owner=excluded.owner,expected_schedulable=excluded.expected_schedulable,manual_override_until=excluded.manual_override_until,last_observed_schedulable=excluded.last_observed_schedulable,last_decision=excluded.last_decision,last_action_at=excluded.last_action_at,flap_active=excluded.flap_active,flap_triggered_at=excluded.flap_triggered_at,flap_recovery_required=excluded.flap_recovery_required,health_locked=excluded.health_locked,manual_locked=excluded.manual_locked,cost_locked=excluded.cost_locked,cost_source_id=excluded.cost_source_id,cost_pool=excluded.cost_pool,owns_load_factor=excluded.owns_load_factor,original_load_factor=excluded.original_load_factor,expected_load_factor=excluded.expected_load_factor,load_stage=excluded.load_stage,load_override_until=excluded.load_override_until,load_pin_value=excluded.load_pin_value,load_pin_until=excluded.load_pin_until,load_pin_owner=excluded.load_pin_owner,load_pin_reason=excluded.load_pin_reason,recovery_step=excluded.recovery_step,recovery_started_at=excluded.recovery_started_at,updated_at=excluded.updated_at`

func controlUpsertArguments(control model.AccountControl) []any {
	return []any{control.AccountID, control.MonitorID, boolInt(control.OwnsPause), control.Owner,
		nullableBool(control.ExpectedSchedulable), nullableTime(control.ManualOverrideUntil), nullableBool(control.LastObserved),
		control.LastDecision, nullableTime(control.LastActionAt), boolInt(control.FlapActive),
		nullableTime(control.FlapTriggeredAt), control.FlapRecoveryRequired, boolInt(control.HealthLocked),
		boolInt(control.ManualLocked), boolInt(control.CostLocked), control.CostSourceID, control.CostPool,
		boolInt(control.OwnsLoadFactor), control.OriginalLoadFactor,
		control.ExpectedLoadFactor, control.LoadStage, nullableTime(control.LoadOverrideUntil), control.LoadPinValue,
		nullableTime(control.LoadPinUntil), control.LoadPinOwner, control.LoadPinReason, control.RecoveryStep,
		nullableTime(control.RecoveryStartedAt), formatTime(control.UpdatedAt)}
}

func (s *Store) AddEvent(ctx context.Context, event model.Event) error {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	_, err := insertEvent(ctx, s.db, event)
	return err
}

type sqlExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertEvent(ctx context.Context, exec sqlExecer, event model.Event) (sql.Result, error) {
	return exec.ExecContext(ctx, `INSERT INTO events(type,severity,monitor_id,account_id,message,before_state,after_state,details,actor,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		event.Type, event.Severity, event.MonitorID, event.AccountID, event.Message, event.BeforeState, event.AfterState, event.Details, event.Actor, formatTime(event.CreatedAt))
}

func upsertControl(ctx context.Context, exec sqlExecer, control model.AccountControl) error {
	control.UpdatedAt = time.Now().UTC()
	_, err := exec.ExecContext(ctx, controlUpsertSQL, controlUpsertArguments(control)...)
	return err
}

func (s *Store) CountAutomaticPauses(ctx context.Context, accountID int64, since, until time.Time) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='automatic_pause' AND account_id=? AND created_at>=? AND created_at<=?`, accountID, formatTime(since), formatTime(until)).Scan(&count)
	return count, err
}

func (s *Store) CommitAutomaticPause(ctx context.Context, control model.AccountControl, event model.Event, flap model.FlapPolicy) (model.AccountControl, int, bool, error) {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return control, 0, false, err
	}
	defer tx.Rollback()
	if _, err := insertEvent(ctx, tx, event); err != nil {
		return control, 0, false, err
	}
	var recent int
	windowStart := event.CreatedAt.Add(-time.Duration(flap.WindowMinutes) * time.Minute)
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='automatic_pause' AND account_id=? AND created_at>=? AND created_at<=?`, control.AccountID, formatTime(windowStart), formatTime(event.CreatedAt)).Scan(&recent); err != nil {
		return control, 0, false, err
	}
	activated := flap.Enabled && !control.FlapActive && recent >= flap.PauseThreshold
	if activated {
		triggeredAt := event.CreatedAt
		control.FlapActive = true
		control.FlapTriggeredAt = &triggeredAt
		control.FlapRecoveryRequired = flap.RecoveryThreshold
		details, _ := json.Marshal(map[string]any{
			"window_minutes":     flap.WindowMinutes,
			"recent_pause_count": recent,
			"pause_threshold":    flap.PauseThreshold,
			"recovery_threshold": flap.RecoveryThreshold,
		})
		activationEvent := model.Event{
			Type: "flap_protection_activated", Severity: "warning",
			MonitorID: event.MonitorID, AccountID: event.AccountID,
			Message:     "账号在滚动窗口内反复暂停，已启用抖动保护",
			BeforeState: "normal_recovery", AfterState: "flap_protected",
			Details: string(details), Actor: "scheduler", CreatedAt: event.CreatedAt,
		}
		if _, err := insertEvent(ctx, tx, activationEvent); err != nil {
			return control, 0, false, err
		}
	}
	control.RecentAutomaticPauses = recent
	if err := upsertControl(ctx, tx, control); err != nil {
		return control, 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return control, 0, false, err
	}
	return control, recent, activated, nil
}

func (s *Store) CommitControlEvents(ctx context.Context, control model.AccountControl, events ...model.Event) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	for _, event := range events {
		if event.CreatedAt.IsZero() {
			event.CreatedAt = now
		}
		if _, err := insertEvent(ctx, tx, event); err != nil {
			return err
		}
	}
	if err := upsertControl(ctx, tx, control); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ListEvents(ctx context.Context, limit int) ([]model.Event, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,type,severity,monitor_id,account_id,message,before_state,after_state,details,actor,created_at FROM events ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.Event, 0)
	for rows.Next() {
		var item model.Event
		var monitorID, accountID sql.NullInt64
		var created string
		if err := rows.Scan(&item.ID, &item.Type, &item.Severity, &monitorID, &accountID, &item.Message, &item.BeforeState, &item.AfterState, &item.Details, &item.Actor, &created); err != nil {
			return nil, err
		}
		item.MonitorID = nullInt64Pointer(monitorID)
		item.AccountID = nullInt64Pointer(accountID)
		item.CreatedAt = parseTime(created)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) CountUpstreamSources(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM upstream_sources`).Scan(&count)
	return count, err
}

func (s *Store) ListUpstreamSources(ctx context.Context) ([]model.UpstreamSource, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,name,provider,base_url,normalized_url,credential_nonce,credential_ciphertext,credential_mode,credential_migration_required,pause_below,resume_at,enabled,balance,unit,low_streak,recovery_streak,balance_locked,last_attempt_at,last_success_at,last_error,selected_key_id,routing_enabled,routing_pool,created_at,updated_at FROM upstream_sources ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.UpstreamSource, 0)
	for rows.Next() {
		item, err := scanUpstreamSource(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetUpstreamSource(ctx context.Context, id int64) (model.UpstreamSource, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id,name,provider,base_url,normalized_url,credential_nonce,credential_ciphertext,credential_mode,credential_migration_required,pause_below,resume_at,enabled,balance,unit,low_streak,recovery_streak,balance_locked,last_attempt_at,last_success_at,last_error,selected_key_id,routing_enabled,routing_pool,created_at,updated_at FROM upstream_sources WHERE id=?`, id)
	item, err := scanUpstreamSource(row)
	if errors.Is(err, sql.ErrNoRows) {
		return model.UpstreamSource{}, errors.New("上游配置不存在")
	}
	return item, err
}

type rowScanner interface {
	Scan(...any) error
}

func scanUpstreamSource(row rowScanner) (model.UpstreamSource, error) {
	var item model.UpstreamSource
	var enabled, locked, routingEnabled, migrationRequired int
	var balance sql.NullFloat64
	var lastAttempt, lastSuccess sql.NullString
	var created, updated string
	err := row.Scan(&item.ID, &item.Name, &item.Provider, &item.BaseURL, &item.NormalizedURL, &item.CredentialNonce, &item.CredentialCiphertext, &item.CredentialMode, &migrationRequired, &item.PauseBelow, &item.ResumeAt, &enabled, &balance, &item.Unit, &item.LowStreak, &item.RecoveryStreak, &locked, &lastAttempt, &lastSuccess, &item.LastError, &item.SelectedKeyID, &routingEnabled, &item.RoutingPool, &created, &updated)
	if err != nil {
		return model.UpstreamSource{}, err
	}
	item.Enabled = enabled == 1
	item.BalanceLocked = locked == 1
	item.RoutingEnabled = routingEnabled == 1
	item.MigrationRequired = migrationRequired == 1
	item.CredentialConfigured = len(item.CredentialCiphertext) > 0
	if balance.Valid {
		value := balance.Float64
		item.Balance = &value
	}
	item.LastAttemptAt = parseNullTime(lastAttempt)
	item.LastSuccessAt = parseNullTime(lastSuccess)
	item.CreatedAt = parseTime(created)
	item.UpdatedAt = parseTime(updated)
	return item, nil
}

func (s *Store) CreateUpstreamSource(ctx context.Context, source model.UpstreamSource) (model.UpstreamSource, error) {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `INSERT INTO upstream_sources(name,provider,base_url,normalized_url,credential_nonce,credential_ciphertext,credential_mode,credential_migration_required,pause_below,resume_at,enabled,selected_key_id,routing_enabled,routing_pool,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		source.Name, source.Provider, source.BaseURL, source.NormalizedURL, source.CredentialNonce, source.CredentialCiphertext, source.CredentialMode, boolInt(source.MigrationRequired), source.PauseBelow, source.ResumeAt, boolInt(source.Enabled), source.SelectedKeyID, boolInt(source.RoutingEnabled), source.RoutingPool, formatTime(now), formatTime(now))
	if err != nil {
		if stringsContains(err.Error(), "UNIQUE") {
			return model.UpstreamSource{}, errors.New("该上游地址已经配置")
		}
		return model.UpstreamSource{}, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return model.UpstreamSource{}, err
	}
	return s.GetUpstreamSource(ctx, id)
}

func (s *Store) UpdateUpstreamSource(ctx context.Context, source model.UpstreamSource) error {
	return s.UpdateUpstreamSourceWithPolicyInvalidation(ctx, source, false)
}

func (s *Store) UpdateUpstreamSourceWithPolicyInvalidation(ctx context.Context, source model.UpstreamSource, invalidatePolicies bool) error {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `UPDATE upstream_sources SET name=?,provider=?,base_url=?,normalized_url=?,credential_nonce=?,credential_ciphertext=?,credential_mode=?,credential_migration_required=?,pause_below=?,resume_at=?,enabled=?,selected_key_id=?,routing_enabled=?,routing_pool=?,updated_at=? WHERE id=?`,
		source.Name, source.Provider, source.BaseURL, source.NormalizedURL, source.CredentialNonce, source.CredentialCiphertext, source.CredentialMode, boolInt(source.MigrationRequired), source.PauseBelow, source.ResumeAt, boolInt(source.Enabled), source.SelectedKeyID, boolInt(source.RoutingEnabled), source.RoutingPool, formatTime(now), source.ID)
	if err != nil {
		if stringsContains(err.Error(), "UNIQUE") {
			return errors.New("该上游地址已经配置")
		}
		return err
	}
	if invalidatePolicies {
		if _, err := tx.ExecContext(ctx, `UPDATE upstream_group_failover_policies SET confirmed_version=0,confirmed_at=NULL,confirmed_by='',updated_at=? WHERE source_id=?`, formatTime(now), source.ID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE upstream_group_failover_states SET frozen=1,freeze_reason=?,last_error=?,manual_hold_until=NULL,manual_override_until=NULL,updated_at=? WHERE source_id=?`,
			"上游登录身份已变化，必须重新检查并确认三级分组策略", "上游登录身份已变化，必须重新检查并确认三级分组策略", formatTime(now), source.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) DeleteUpstreamSource(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM upstream_sources WHERE id=?`, id)
	return err
}

func (s *Store) SaveUpstreamSuccess(ctx context.Context, source model.UpstreamSource, rates []model.KeyRate, groups []model.UpstreamGroup) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `UPDATE upstream_sources SET balance=?,unit=?,low_streak=?,recovery_streak=?,balance_locked=?,last_attempt_at=?,last_success_at=?,last_error='',updated_at=? WHERE id=?`,
		source.Balance, source.Unit, source.LowStreak, source.RecoveryStreak, boolInt(source.BalanceLocked), nullableTime(source.LastAttemptAt), nullableTime(source.LastSuccessAt), formatTime(time.Now().UTC()), source.ID)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM upstream_key_rates WHERE source_id=?`, source.ID); err != nil {
		return err
	}
	for _, rate := range rates {
		if _, err := tx.ExecContext(ctx, `INSERT INTO upstream_key_rates(source_id,external_id,name,key_hint,group_id,group_name,rate_multiplier,dynamic,status,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			source.ID, rate.ExternalID, rate.Name, rate.KeyHint, rate.GroupID, rate.GroupName, rate.RateMultiplier, boolInt(rate.Dynamic), rate.Status, formatTime(time.Now().UTC())); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM upstream_groups WHERE source_id=?`, source.ID); err != nil {
		return err
	}
	for _, group := range groups {
		if _, err := tx.ExecContext(ctx, `INSERT INTO upstream_groups(source_id,external_id,name,rate_multiplier,updated_at) VALUES(?,?,?,?,?)`,
			source.ID, group.ExternalID, group.Name, group.RateMultiplier, formatTime(time.Now().UTC())); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) SaveUpstreamFailure(ctx context.Context, id int64, attemptedAt time.Time, message string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE upstream_sources SET last_attempt_at=?,last_error=?,updated_at=? WHERE id=?`, formatTime(attemptedAt), message, formatTime(time.Now().UTC()), id)
	return err
}

func (s *Store) ResetUpstreamControl(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE upstream_sources SET low_streak=0,recovery_streak=0,balance_locked=0,updated_at=? WHERE id=?`, formatTime(time.Now().UTC()), id)
	return err
}

func (s *Store) MarkUpstreamCredentialMigrationRequired(ctx context.Context, id int64) error {
	now := formatTime(time.Now().UTC())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE upstream_sources SET credential_mode='access_key',credential_migration_required=1,enabled=0,low_streak=0,recovery_streak=0,balance_locked=0,routing_enabled=0,updated_at=? WHERE id=?`, now, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM balance_account_locks WHERE source_id=?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM cost_account_locks WHERE source_id=?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE upstream_group_failover_policies SET confirmed_version=0,confirmed_at=NULL,confirmed_by='',updated_at=? WHERE source_id=?`, now, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ListUpstreamKeyRates(ctx context.Context, sourceID int64) ([]model.KeyRate, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT external_id,name,key_hint,group_id,group_name,rate_multiplier,dynamic,status FROM upstream_key_rates WHERE source_id=? ORDER BY name,external_id`, sourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.KeyRate, 0)
	for rows.Next() {
		var item model.KeyRate
		var multiplier sql.NullFloat64
		var dynamic int
		if err := rows.Scan(&item.ExternalID, &item.Name, &item.KeyHint, &item.GroupID, &item.GroupName, &multiplier, &dynamic, &item.Status); err != nil {
			return nil, err
		}
		item.Dynamic = dynamic == 1
		if multiplier.Valid {
			value := multiplier.Float64
			item.RateMultiplier = &value
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ListUpstreamGroups(ctx context.Context, sourceID int64) ([]model.UpstreamGroup, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT external_id,name,rate_multiplier FROM upstream_groups WHERE source_id=? ORDER BY rate_multiplier,name,external_id`, sourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.UpstreamGroup, 0)
	for rows.Next() {
		var item model.UpstreamGroup
		if err := rows.Scan(&item.ExternalID, &item.Name, &item.RateMultiplier); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) SyncBalanceLocks(ctx context.Context, sourceID int64, accountIDs []int64, active bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM balance_account_locks WHERE source_id=?`, sourceID); err != nil {
		return err
	}
	if active {
		now := formatTime(time.Now().UTC())
		for _, accountID := range accountIDs {
			if _, err := tx.ExecContext(ctx, `INSERT INTO balance_account_locks(source_id,account_id,created_at) VALUES(?,?,?)`, sourceID, accountID, now); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *Store) GetActiveBalanceLock(ctx context.Context, accountID int64) (*model.BalanceLock, error) {
	var item model.BalanceLock
	var created string
	err := s.db.QueryRowContext(ctx, `SELECT source_id,account_id,created_at FROM balance_account_locks WHERE account_id=? ORDER BY source_id LIMIT 1`, accountID).Scan(&item.SourceID, &item.AccountID, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	item.CreatedAt = parseTime(created)
	return &item, nil
}

func (s *Store) ListCostLocks(ctx context.Context) ([]model.CostLock, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT source_id,account_id,pool,rate_multiplier,created_at FROM cost_account_locks ORDER BY pool,account_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.CostLock, 0)
	for rows.Next() {
		var item model.CostLock
		var created string
		if err := rows.Scan(&item.SourceID, &item.AccountID, &item.Pool, &item.RateMultiplier, &created); err != nil {
			return nil, err
		}
		item.CreatedAt = parseTime(created)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) SyncCostLocks(ctx context.Context, locks []model.CostLock) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM cost_account_locks`); err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, item := range locks {
		if item.CreatedAt.IsZero() {
			item.CreatedAt = now
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO cost_account_locks(account_id,source_id,pool,rate_multiplier,created_at) VALUES(?,?,?,?,?)`,
			item.AccountID, item.SourceID, item.Pool, item.RateMultiplier, formatTime(item.CreatedAt)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) GetActiveCostLock(ctx context.Context, accountID int64) (*model.CostLock, error) {
	var item model.CostLock
	var created string
	err := s.db.QueryRowContext(ctx, `SELECT source_id,account_id,pool,rate_multiplier,created_at FROM cost_account_locks WHERE account_id=?`, accountID).
		Scan(&item.SourceID, &item.AccountID, &item.Pool, &item.RateMultiplier, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	item.CreatedAt = parseTime(created)
	return &item, nil
}

func stringsContains(value, needle string) bool {
	return strings.Contains(strings.ToUpper(value), strings.ToUpper(needle))
}

func (s *Store) CreateSession(ctx context.Context, token, csrf string, expiresAt time.Time) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `INSERT INTO sessions(token_hash,csrf_token,last_seen_at,expires_at,created_at) VALUES(?,?,?,?,?)`, hashToken(token), csrf, formatTime(now), formatTime(expiresAt), formatTime(now))
	return err
}

func (s *Store) ValidateSession(ctx context.Context, token string, idle time.Duration) (string, bool, error) {
	var csrf, expires string
	err := s.db.QueryRowContext(ctx, `SELECT csrf_token,expires_at FROM sessions WHERE token_hash=?`, hashToken(token)).Scan(&csrf, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if time.Now().UTC().After(parseTime(expires)) {
		_ = s.DeleteSession(ctx, token)
		return "", false, nil
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `UPDATE sessions SET last_seen_at=?,expires_at=? WHERE token_hash=?`, formatTime(now), formatTime(now.Add(idle)), hashToken(token))
	return csrf, err == nil, err
}

func (s *Store) DeleteSession(ctx context.Context, token string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE token_hash=?`, hashToken(token))
	return err
}

func (s *Store) CleanupSessions(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE expires_at < ?`, formatTime(time.Now().UTC()))
	return err
}

func hashToken(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }
func parseTime(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}
func parseNullTime(value sql.NullString) *time.Time {
	if !value.Valid || value.String == "" {
		return nil
	}
	parsed := parseTime(value.String)
	return &parsed
}
func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatTime(*value)
}
func nullableTimeValue(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return formatTime(value)
}
func nullableBool(value *bool) any {
	if value == nil {
		return nil
	}
	return boolInt(*value)
}
func nullBoolPointer(value sql.NullInt64) *bool {
	if !value.Valid {
		return nil
	}
	result := value.Int64 == 1
	return &result
}
func nullInt64Pointer(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}
func nullIntPointer(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	result := int(value.Int64)
	return &result
}
func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
func parseBool(value string, fallback bool) bool {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
func parseInt(value string, fallback int) int {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseInt64(value string, fallback int64) int64 {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseHealthMode(value string) string {
	switch value {
	case "legacy", "observe", "adaptive":
		return value
	default:
		return "observe"
	}
}

func applyHealthSettingDefaults(settings *model.Settings) {
	settings.HealthMode = parseHealthMode(settings.HealthMode)
	if settings.HealthHealthyScore < 1 {
		settings.HealthHealthyScore = 80
	}
	if settings.HealthWatchScore < 1 {
		settings.HealthWatchScore = 60
	}
	if settings.HealthQuarantineScore < 1 {
		settings.HealthQuarantineScore = 35
	}
	if settings.HealthMinSamples < 1 {
		settings.HealthMinSamples = 10
	}
	if settings.HealthLatencyWarningMS < 1 {
		settings.HealthLatencyWarningMS = 8000
	}
	if settings.HealthLatencyCriticalMS < 1 {
		settings.HealthLatencyCriticalMS = 15000
	}
	if settings.HealthTrafficPauseBelow < 1 {
		settings.HealthTrafficPauseBelow = 80
	}
	if settings.HealthTrafficHealthyAt <= settings.HealthTrafficPauseBelow {
		settings.HealthTrafficHealthyAt = 95
	}
	if settings.HealthHardFailures10 < 1 {
		settings.HealthHardFailures10 = 5
	}
	if settings.HealthPersistentSlowRate < 1 {
		settings.HealthPersistentSlowRate = 40
	}
	if settings.HealthQuarantineMinutes < 1 {
		settings.HealthQuarantineMinutes = 5
	}
	if settings.HealthRecoveryWindow < 1 {
		settings.HealthRecoveryWindow = 10
	}
	if settings.HealthRecoverySuccesses < 1 {
		settings.HealthRecoverySuccesses = 8
	}
	if settings.HealthTrialPercent < 1 {
		settings.HealthTrialPercent = 25
	}
	if settings.HealthMidPercent < 1 {
		settings.HealthMidPercent = 50
	}
	if settings.HealthDegradedPercent < 1 {
		settings.HealthDegradedPercent = 50
	}
	if settings.HealthTrialMinutes < 1 {
		settings.HealthTrialMinutes = 5
	}
	if settings.HealthLoadOverrideMinutes < 1 {
		settings.HealthLoadOverrideMinutes = 30
	}
}

func applyFailoverSettingDefaults(settings *model.Settings) {
	defaults := map[*int]int{
		&settings.FailoverAccountFreshMinutes:      3,
		&settings.FailoverTelemetryFreshMinutes:    6,
		&settings.FailoverGroupFreshMinutes:        30,
		&settings.FailoverAgentGraceSeconds:        90,
		&settings.FailoverMonitorFailures:          3,
		&settings.FailoverNoTrafficFailures:        5,
		&settings.FailoverTrafficWindowMinutes:     5,
		&settings.FailoverTrafficMinSamples:        10,
		&settings.FailoverTrafficSuccessBelow:      20,
		&settings.FailoverConsecutiveHardErrors:    5,
		&settings.FailoverBackupVerifyMinutes:      2,
		&settings.FailoverPostSwitchMonitors:       2,
		&settings.FailoverPostSwitchRequests:       5,
		&settings.FailoverMainVerifyMinutes:        5,
		&settings.FailoverSwitchCooldownMinutes:    15,
		&settings.FailoverManualProtectionMinutes:  30,
		&settings.FailoverShortLimitWindowMinutes:  30,
		&settings.FailoverShortLimitCount:          2,
		&settings.FailoverLongLimitWindowMinutes:   360,
		&settings.FailoverLongLimitCount:           3,
		&settings.FailoverRecoveryWindowMinutes:    30,
		&settings.FailoverRecoveryStableMinutes:    30,
		&settings.FailoverRecoveryMonitorSuccesses: 10,
		&settings.FailoverRecoveryMinSamples:       20,
		&settings.FailoverRecoverySuccessAt:        98,
		&settings.FailoverReturnRetryMinutes:       120,
	}
	for field, fallback := range defaults {
		if *field < 1 {
			*field = fallback
		}
	}
}

func validHealthSettings(settings model.Settings) bool {
	if parseHealthMode(settings.HealthMode) != settings.HealthMode {
		return false
	}
	return settings.HealthHealthyScore > settings.HealthWatchScore &&
		settings.HealthWatchScore > settings.HealthQuarantineScore &&
		settings.HealthQuarantineScore > 0 && settings.HealthHealthyScore <= 100 &&
		settings.HealthMinSamples > 0 && settings.HealthLatencyWarningMS > 0 &&
		settings.HealthLatencyCriticalMS > settings.HealthLatencyWarningMS &&
		settings.HealthTrafficPauseBelow > 0 && settings.HealthTrafficPauseBelow < settings.HealthTrafficHealthyAt &&
		settings.HealthTrafficHealthyAt <= 100 && settings.HealthHardFailures10 > 0 &&
		settings.HealthPersistentSlowRate > 0 && settings.HealthPersistentSlowRate <= 100 &&
		settings.HealthQuarantineMinutes > 0 && settings.HealthRecoveryWindow > 0 &&
		settings.HealthRecoverySuccesses > 0 && settings.HealthRecoverySuccesses <= settings.HealthRecoveryWindow &&
		settings.HealthTrialPercent > 0 && settings.HealthTrialPercent <= 100 &&
		settings.HealthMidPercent > 0 && settings.HealthMidPercent <= 100 &&
		settings.HealthDegradedPercent > 0 && settings.HealthDegradedPercent <= 100 &&
		settings.HealthTrialMinutes > 0 && settings.HealthLoadOverrideMinutes > 0
}

func validFailoverSettings(settings model.Settings) bool {
	return settings.FailoverAccountFreshMinutes > 0 &&
		settings.FailoverTelemetryFreshMinutes > 0 &&
		settings.FailoverGroupFreshMinutes > 0 &&
		settings.FailoverAgentGraceSeconds > 0 &&
		settings.FailoverMonitorFailures > 0 &&
		settings.FailoverNoTrafficFailures >= settings.FailoverMonitorFailures &&
		settings.FailoverTrafficWindowMinutes > 0 &&
		settings.FailoverTrafficMinSamples > 0 &&
		settings.FailoverTrafficSuccessBelow > 0 && settings.FailoverTrafficSuccessBelow < 100 &&
		settings.FailoverConsecutiveHardErrors > 0 &&
		settings.FailoverBackupVerifyMinutes > 0 &&
		settings.FailoverPostSwitchMonitors > 0 &&
		settings.FailoverPostSwitchRequests > 0 &&
		settings.FailoverMainVerifyMinutes > 0 &&
		settings.FailoverSwitchCooldownMinutes > 0 &&
		settings.FailoverManualProtectionMinutes > 0 &&
		settings.FailoverShortLimitWindowMinutes > 0 &&
		settings.FailoverShortLimitCount > 0 &&
		settings.FailoverLongLimitWindowMinutes > settings.FailoverShortLimitWindowMinutes &&
		settings.FailoverLongLimitCount > 0 &&
		settings.FailoverRecoveryWindowMinutes > 0 &&
		settings.FailoverRecoveryStableMinutes > 0 &&
		settings.FailoverRecoveryMonitorSuccesses > 0 &&
		settings.FailoverRecoveryMinSamples > 0 &&
		settings.FailoverRecoverySuccessAt > 0 && settings.FailoverRecoverySuccessAt <= 100 &&
		settings.FailoverReturnRetryMinutes > 0
}
