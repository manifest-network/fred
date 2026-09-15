package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfigRejectsIgnoredConfiguration(t *testing.T) {
	for _, tc := range []struct{ name, input, errorText string }{
		{"valid", "name: test\n", ""},
		{"root typo", "production_mod: true\n", "production_mod"},
		{"nested typo", "sku_profiles:\n  small:\n    memory_mbb: 512\n", "memory_mbb"},
		{"second document", "name: test\n---\nproduction_mode: true\n", "exactly one"},
		{"empty second document", "name: test\n---\n", "exactly one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.input), 0o600))
			cfg, err := loadConfig(path)
			if tc.errorText == "" {
				require.NoError(t, err)
				require.NotNil(t, cfg)
			} else {
				require.ErrorContains(t, err, tc.errorText)
			}
		})
	}
}
