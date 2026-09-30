package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const providerdValidateConfigYAML = `
provider_uuid: "01234567-89ab-cdef-0123-456789abcdef"
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

func TestLoadStartupConfigRunsEveryPreSideEffectCheck(t *testing.T) {
	write := func(t *testing.T, contents string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
		return path
	}
	_, _, err := loadStartupConfig(write(t, providerdValidateConfigYAML))
	require.NoError(t, err)

	for name, test := range map[string]struct {
		contents string
		want     string
	}{
		"unknown key": {providerdValidateConfigYAML + "no_such_key: true\n", "failed to load config"},
		"log level":   {providerdValidateConfigYAML + "log_level: loud\n", "invalid log_level"},
		"sub-signer amount": {
			providerdValidateConfigYAML + "sub_signer_count: 2\nsub_signer_min_balance: \"ten\"\n",
			"invalid sub_signer_min_balance",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := loadStartupConfig(write(t, test.contents))
			require.ErrorContains(t, err, test.want)
		})
	}
}
