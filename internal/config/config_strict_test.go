package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/configyaml"
)

const strictValidProviderConfig = `provider_uuid: "550e8400-e29b-41d4-a716-446655440000"
provider_address: "manifest1abc"
key_name: "provider"
keyring_dir: "/home/provider/.manifest"
callback_base_url: "http://localhost:8080"
callback_secret: "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq"
placement_store_db_path: "/var/lib/fred/placements.db"
backends:
  - name: "mock"
    url: "http://localhost:9000"
    default: true
`

func TestConfigDecoderPreservesFormatsAndRejectsAmbiguousKeys(t *testing.T) {
	// Exercise actual Load so Viper cannot fold aliases before validation.
	var values map[string]any
	require.NoError(t, configyaml.Decode([]byte(strictValidProviderConfig), &values))
	encoded, err := json.Marshal(values)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, encoded, 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "mock", cfg.Backends[0].Name)
	for _, tc := range []struct {
		format, input, errorText string
	}{
		{"yaml", "production_mode: true\nProduction_Mode: false\n", "case-insensitive"},
		{"json", `{"production_mode":true,"Production_Mode":false}`, "case-insensitive"},
		{"json", `{"backends":[{"tls_skip_verify":false,"TLS_SKIP_VERIFY":true}]}`, "case-insensitive"},
		{"json", `{"production_mode":true,"production_mode":false}`, "duplicate"},
		{"json", `{"production_mode":true} {}`, ""},
	} {
		t.Run(tc.format+tc.input, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config."+tc.format)
			require.NoError(t, os.WriteFile(path, []byte(tc.input), 0o600))
			_, err := Load(path)
			require.Error(t, err)
			if tc.errorText != "" {
				require.ErrorContains(t, err, tc.errorText)
			}
		})
	}
	for _, tc := range []struct{ format, input string }{
		{"yaml", "Production_Mode: false\n"},
		{"json", `{"Production_Mode":false}`},
		{"toml", "Production_Mode = false\n"},
		{"env", "PRODUCTION_MODE=false\n"},
	} {
		path := filepath.Join(t.TempDir(), "config."+tc.format)
		require.NoError(t, os.WriteFile(path, []byte(tc.input), 0o600))
		v := viper.New()
		require.NoError(t, readConfigFile(v, path), "retain supported formats and unambiguous case variants")
		require.True(t, v.IsSet("production_mode"))
		require.False(t, v.GetBool("production_mode"))
	}
}

func TestLoadRejectsIgnoredConfiguration(t *testing.T) {
	const valid = strictValidProviderConfig
	for _, tc := range []struct{ name, suffix, errorText string }{
		{"valid", "", ""},
		{"root typo", "production_mod: true\n", "production_mod"},
		{"root case alias", "production_mode: true\nProduction_Mode: false\n", "case-insensitive"},
		{"nested case alias", "    Default: false\n", "case-insensitive"},
		{"nested typo", "    hmac_secrett: secret\n", "hmac_secrett"},
		{"second document", "---\nproduction_mode: true\n", "exactly one"},
		{"empty second document", "---\n", "exactly one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(valid+tc.suffix), 0o600))
			cfg, err := Load(path)
			if tc.errorText == "" {
				require.NoError(t, err)
				require.NotNil(t, cfg)
			} else {
				require.ErrorContains(t, err, tc.errorText)
				require.Nil(t, cfg)
			}
		})
	}
}

func TestExternalURLScopedIPv6(t *testing.T) {
	for _, tc := range []struct {
		url     string
		allowed bool
	}{
		{"https://[fe80::1%25eth0]", false},
		{"https://[::1%25lo]", false},
		{"https://[::%25lo]", false},
		{"https://[::ffff:127.0.0.1]", false},
		{"https://[fd12:3456::1%25eth0]", true},
		{"https://[2001:4860:4860::8888]", true},
		{"https://backend.internal", true},
	} {
		t.Run(tc.url, func(t *testing.T) {
			err := validateExternalURL(tc.url)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
