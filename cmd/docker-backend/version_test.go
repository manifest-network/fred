package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseFlags_Version is the ENG-254 test: docker-backend must answer
// `--version` by printing the build-injected version and signalling the
// caller to exit BEFORE any config is loaded — matching providerd, which
// already exposes `--version` via cobra. The whole point is that an operator
// can query the version without a valid config file present.
func TestParseFlags_Version(t *testing.T) {
	t.Run("--version prints version and signals exit", func(t *testing.T) {
		var out bytes.Buffer

		_, showVersion, err := parseFlags([]string{"--version"}, &out)

		require.NoError(t, err)
		assert.True(t, showVersion, "--version must signal the caller to exit before startup")
		assert.Equal(t, "docker-backend version "+version+"\n", out.String())
	})

	t.Run("-version single-dash form is also accepted", func(t *testing.T) {
		var out bytes.Buffer

		_, showVersion, err := parseFlags([]string{"-version"}, &out)

		require.NoError(t, err)
		assert.True(t, showVersion)
	})

	t.Run("without --version, config path resolves and nothing is printed", func(t *testing.T) {
		var out bytes.Buffer

		cfgPath, showVersion, err := parseFlags([]string{"--config", "/etc/custom.yaml"}, &out)

		require.NoError(t, err)
		assert.False(t, showVersion)
		assert.Equal(t, "/etc/custom.yaml", cfgPath)
		assert.Empty(t, out.String(), "normal flag parsing must not print anything")
	})

	t.Run("default config path is preserved when no flags are passed", func(t *testing.T) {
		var out bytes.Buffer

		cfgPath, showVersion, err := parseFlags(nil, &out)

		require.NoError(t, err)
		assert.False(t, showVersion)
		assert.Equal(t, "docker-backend.yaml", cfgPath)
	})
}
