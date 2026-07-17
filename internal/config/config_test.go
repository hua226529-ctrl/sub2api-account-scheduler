package config

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestDecodeCredentialKeySupportsBase64AndHex(t *testing.T) {
	raw := []byte(strings.Repeat("k", 32))
	for name, encoded := range map[string]string{
		"base64": base64.StdEncoding.EncodeToString(raw),
		"hex":    strings.Repeat("11", 32),
	} {
		t.Run(name, func(t *testing.T) {
			key, err := decodeCredentialKey(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if len(key) != 32 {
				t.Fatalf("decoded key length = %d", len(key))
			}
		})
	}
}

func TestLoadRequiresIndependentSchedulerSecret(t *testing.T) {
	t.Setenv("SUB2API_ADMIN_API_KEY", "upstream-secret")
	t.Setenv("SCHEDULER_ADMIN_SECRET", "")
	t.Setenv("ALLOW_LEGACY_ADMIN_KEY_LOGIN", "false")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "SCHEDULER_ADMIN_SECRET") {
		t.Fatalf("missing scheduler secret error = %v", err)
	}

	t.Setenv("SCHEDULER_ADMIN_SECRET", "scheduler-secret")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SchedulerAdminSecret != "scheduler-secret" || cfg.SchedulerAdminSecret == cfg.AdminAPIKey {
		t.Fatalf("secrets were not separated: %+v", cfg)
	}
}

func TestLoadLegacyAdminLoginRequiresExplicitFlag(t *testing.T) {
	t.Setenv("SUB2API_ADMIN_API_KEY", "legacy-secret")
	t.Setenv("SCHEDULER_ADMIN_SECRET", "")
	t.Setenv("ALLOW_LEGACY_ADMIN_KEY_LOGIN", "true")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AllowLegacyAdminLogin || cfg.SchedulerAdminSecret != cfg.AdminAPIKey {
		t.Fatalf("legacy compatibility not explicit: %+v", cfg)
	}
	if warning := cfg.LegacyAdminLoginWarning(); !strings.Contains(warning, "deprecated") {
		t.Fatalf("legacy compatibility warning = %q", warning)
	}
}

func TestLoadRejectsInvalidTrustedProxyCIDR(t *testing.T) {
	t.Setenv("SUB2API_ADMIN_API_KEY", "upstream-secret")
	t.Setenv("SCHEDULER_ADMIN_SECRET", "scheduler-secret")
	t.Setenv("TRUSTED_PROXY_CIDRS", "10.0.0.0/8,invalid")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "TRUSTED_PROXY_CIDRS") {
		t.Fatalf("invalid proxy CIDR error = %v", err)
	}
}
