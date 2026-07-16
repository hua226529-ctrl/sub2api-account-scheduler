package config

import (
	"encoding/base64"
	"os"
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

func TestControlplaneShadowModeDefaultsAndFailsClosed(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		previous, present := os.LookupEnv("CONTROLPLANE_SHADOW_MODE")
		if err := os.Unsetenv("CONTROLPLANE_SHADOW_MODE"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if present {
				_ = os.Setenv("CONTROLPLANE_SHADOW_MODE", previous)
			} else {
				_ = os.Unsetenv("CONTROLPLANE_SHADOW_MODE")
			}
		})
		mode, invalid := controlplaneShadowModeEnv()
		if mode != "off" || invalid {
			t.Fatalf("mode/invalid = %q/%v, want off/false", mode, invalid)
		}
	})

	tests := []struct {
		name    string
		value   string
		mode    string
		invalid bool
	}{
		{name: "empty", value: "   ", mode: "off"},
		{name: "off", value: "OFF", mode: "off"},
		{name: "log", value: " log ", mode: "log"},
		{name: "invalid", value: "database", mode: "off", invalid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("CONTROLPLANE_SHADOW_MODE", test.value)
			mode, invalid := controlplaneShadowModeEnv()
			if mode != test.mode || invalid != test.invalid {
				t.Fatalf("mode/invalid = %q/%v, want %q/%v", mode, invalid, test.mode, test.invalid)
			}
		})
	}
}
