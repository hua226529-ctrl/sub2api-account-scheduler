package config

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddress         string
	BasePath              string
	DatabasePath          string
	Sub2APIBaseURL        string
	AdminAPIKey           string
	PollInterval          time.Duration
	RequestTimeout        time.Duration
	SessionIdleTimeout    time.Duration
	CookieSecure          bool
	InitialDryRun         bool
	FailureThreshold      int
	RecoveryThreshold     int
	ManualHoldMinutes     int
	FlapWindowMinutes     int
	FlapPauseThreshold    int
	FlapRecoveryThreshold int
	BalancePollInterval   time.Duration
	TelemetryPollInterval time.Duration
	CredentialKey         []byte
	AgentCredentialKey    []byte
	AllowInsecureUpstream bool
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddress:         env("LISTEN_ADDRESS", ":8323"),
		BasePath:              normalizeBasePath(env("BASE_PATH", "/scheduler/")),
		DatabasePath:          env("DATABASE_PATH", "/data/scheduler.db"),
		Sub2APIBaseURL:        strings.TrimRight(env("SUB2API_BASE_URL", "http://sub2api:8080"), "/"),
		AdminAPIKey:           strings.TrimSpace(os.Getenv("SUB2API_ADMIN_API_KEY")),
		PollInterval:          durationEnv("POLL_INTERVAL", 50*time.Second),
		RequestTimeout:        durationEnv("REQUEST_TIMEOUT", 12*time.Second),
		SessionIdleTimeout:    durationEnv("SESSION_IDLE_TIMEOUT", 30*time.Minute),
		CookieSecure:          boolEnv("COOKIE_SECURE", true),
		InitialDryRun:         boolEnv("DRY_RUN", true),
		FailureThreshold:      intEnv("FAILURE_THRESHOLD", 3),
		RecoveryThreshold:     intEnv("RECOVERY_THRESHOLD", 3),
		ManualHoldMinutes:     intEnv("MANUAL_HOLD_MINUTES", 10),
		FlapWindowMinutes:     intEnv("FLAP_WINDOW_MINUTES", 60),
		FlapPauseThreshold:    intEnv("FLAP_PAUSE_THRESHOLD", 3),
		FlapRecoveryThreshold: intEnv("FLAP_RECOVERY_THRESHOLD", 10),
		BalancePollInterval:   durationEnv("BALANCE_POLL_INTERVAL", 10*time.Minute),
		TelemetryPollInterval: durationEnv("TELEMETRY_POLL_INTERVAL", 2*time.Minute),
		AllowInsecureUpstream: boolEnv("ALLOW_INSECURE_UPSTREAMS", false),
	}
	if cfg.AdminAPIKey == "" {
		return Config{}, fmt.Errorf("SUB2API_ADMIN_API_KEY is required")
	}
	if cfg.PollInterval < 10*time.Second {
		return Config{}, fmt.Errorf("POLL_INTERVAL must be at least 10s")
	}
	if cfg.FailureThreshold < 1 || cfg.RecoveryThreshold < 1 {
		return Config{}, fmt.Errorf("thresholds must be positive")
	}
	if cfg.FlapWindowMinutes < 1 || cfg.FlapPauseThreshold < 1 || cfg.FlapRecoveryThreshold < 1 {
		return Config{}, fmt.Errorf("flap protection settings must be positive")
	}
	if cfg.BalancePollInterval < time.Minute {
		return Config{}, fmt.Errorf("BALANCE_POLL_INTERVAL must be at least 1m")
	}
	if cfg.TelemetryPollInterval < time.Minute {
		return Config{}, fmt.Errorf("TELEMETRY_POLL_INTERVAL must be at least 1m")
	}
	if raw := strings.TrimSpace(os.Getenv("UPSTREAM_CREDENTIAL_KEY")); raw != "" {
		key, err := decodeCredentialKey(raw)
		if err != nil {
			return Config{}, err
		}
		cfg.CredentialKey = key
	}
	if raw := strings.TrimSpace(os.Getenv("AGENT_CREDENTIAL_KEY")); raw != "" {
		key, err := decodeCredentialKey(raw)
		if err != nil {
			return Config{}, fmt.Errorf("AGENT_CREDENTIAL_KEY must be 32 bytes encoded as base64 or hex")
		}
		cfg.AgentCredentialKey = key
	}
	return cfg, nil
}

func decodeCredentialKey(raw string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(raw)
	if err == nil && len(key) == 32 {
		return key, nil
	}
	key, err = hex.DecodeString(raw)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("UPSTREAM_CREDENTIAL_KEY must be 32 bytes encoded as base64 or hex")
	}
	return key, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func boolEnv(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func intEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func normalizeBasePath(value string) string {
	value = "/" + strings.Trim(value, "/") + "/"
	if value == "//" {
		return "/"
	}
	return value
}
