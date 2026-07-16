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
