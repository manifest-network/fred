package configyaml

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeOneStrictDocument(t *testing.T) {
	for _, tc := range []struct {
		name, input, errorText string
	}{
		{"preserve default", "name: backend\n", ""},
		{"nested field", "tls:\n  enabled: true\n", ""},
		{"root typo", "nmae: backend\n", "nmae"},
		{"nested typo", "tls:\n  enabeld: true\n", "enabeld"},
		{"duplicate key", "name: one\nname: two\n", "already defined"},
		{"extra document", "name: one\n---\nname: two\n", "exactly one"},
		{"extra empty document", "name: one\n---\n", "exactly one"},
		{"invalid tail", "name: one\n---\n[", "invalid trailing YAML"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var config struct {
				Name string `yaml:"name"`
				TLS  struct {
					Enabled bool `yaml:"enabled"`
				} `yaml:"tls"`
			}
			config.TLS.Enabled = true
			err := Decode([]byte(tc.input), &config)
			if tc.errorText != "" {
				require.ErrorContains(t, err, tc.errorText)
				return
			}
			require.NoError(t, err)
			require.True(t, config.TLS.Enabled)
		})
	}
}
