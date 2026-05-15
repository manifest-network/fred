package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/k3s"
)

// TestApplyEnvOverrides_Kubeconfig pins the three-way KUBECONFIG branch
// in applyEnvOverrides:
//   - YAML-set KubeconfigPath wins (env never overrides).
//   - Empty YAML + single-path KUBECONFIG flows into KubeconfigPath.
//   - Empty YAML + multi-path KUBECONFIG (containing os.PathListSeparator)
//     is split into KubeconfigPathList; KubeconfigPath stays empty.
//
// Regression guard for Copilot id 3236654209: pre-fix, multi-path
// KUBECONFIG was silently dropped, causing resolveRESTConfig to fall
// through to in-cluster (silently overriding the operator's choice
// inside a Pod).
func TestApplyEnvOverrides_Kubeconfig(t *testing.T) {
	pathSep := string(os.PathListSeparator)

	t.Run("yaml-set path wins over env", func(t *testing.T) {
		t.Setenv("KUBECONFIG", "/env/path/kubeconfig.yaml")
		cfg := k3s.Config{KubeconfigPath: "/yaml/path/kubeconfig.yaml"}
		applyEnvOverrides(&cfg)
		assert.Equal(t, "/yaml/path/kubeconfig.yaml", cfg.KubeconfigPath)
		assert.Empty(t, cfg.KubeconfigPathList)
	})

	t.Run("single-path env populates KubeconfigPath", func(t *testing.T) {
		t.Setenv("KUBECONFIG", "/env/path/kubeconfig.yaml")
		cfg := k3s.Config{}
		applyEnvOverrides(&cfg)
		assert.Equal(t, "/env/path/kubeconfig.yaml", cfg.KubeconfigPath)
		assert.Empty(t, cfg.KubeconfigPathList)
	})

	t.Run("multi-path env populates KubeconfigPathList", func(t *testing.T) {
		t.Setenv("KUBECONFIG", "/path/a"+pathSep+"/path/b"+pathSep+"/path/c")
		cfg := k3s.Config{}
		applyEnvOverrides(&cfg)
		assert.Empty(t, cfg.KubeconfigPath, "single-path field must stay empty for multi-path env")
		require.Len(t, cfg.KubeconfigPathList, 3)
		assert.Equal(t, []string{"/path/a", "/path/b", "/path/c"}, cfg.KubeconfigPathList)
	})

	t.Run("unset env leaves both fields empty", func(t *testing.T) {
		t.Setenv("KUBECONFIG", "")
		cfg := k3s.Config{}
		applyEnvOverrides(&cfg)
		assert.Empty(t, cfg.KubeconfigPath)
		assert.Empty(t, cfg.KubeconfigPathList)
	})
}
