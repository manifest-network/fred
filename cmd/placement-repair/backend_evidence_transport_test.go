package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func TestRun_ApplyRequiresAuthenticatedBackendEvidenceBeforeAuthorityOpen(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		for _, transport := range []string{"HTTP", "TLS verification disabled"} {
			t.Run(transport+"/conflict="+map[bool]string{false: "false", true: "true"}[conflict], func(t *testing.T) {
				var requests atomic.Int32
				handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					requests.Add(1)
					w.WriteHeader(http.StatusServiceUnavailable)
				})
				server := httptest.NewUnstartedServer(handler)
				if transport == "HTTP" {
					server.Start()
				} else {
					server.StartTLS()
				}
				defer server.Close()
				dbPath := filepath.Join(t.TempDir(), "placements.db")
				configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
				if transport != "HTTP" {
					contents, err := os.ReadFile(configPath)
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(configPath, []byte(strings.Replace(string(contents), "    default: true", "    tls_skip_verify: true\n    default: true", 1)), 0o600))
				}
				backupPath := filepath.Join(t.TempDir(), "backup.db")
				args := []string{"-config", configPath, "-lease", repairCommandLease, "-backend", repairCommandBackend, "-apply", "-backup", backupPath}
				if conflict {
					args = append(args, "-resolve-conflict")
				} else {
					args = append(args, "-operation-id", repairCommandOperation)
				}
				dependencies := defaultCommandDependencies()
				bound := 0
				bind := dependencies.bindExactBackupTarget
				dependencies.bindExactBackupTarget = func(path string) (*placement.ExactBackupTarget, error) { bound++; return bind(path) }
				var output bytes.Buffer
				err := runWithDependencies(t.Context(), args, &output, &bytes.Buffer{}, dependencies)
				require.ErrorContains(t, err, "authenticated backend evidence requires certificate-verified HTTPS")
				require.Empty(t, output.String())
				require.Zero(t, bound)
				require.Zero(t, requests.Load())
				require.NoFileExists(t, dbPath)
				require.NoFileExists(t, backupPath)
			})
		}
	}
}

func TestRun_DryRunHTTPInventoryRemainsObservational(t *testing.T) {
	dbPath := createRepairCommandDatabase(t, false)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	server := httptest.NewServer(repairInventoryHandler(t, []backend.ProvisionInfo{}, []backend.RetainedLease{}))
	defer server.Close()
	configPath := writeRepairConfig(t, dbPath, server.URL, repairCommandBackend)
	var output bytes.Buffer
	err = run(t.Context(), repairArgs(t, configPath, dbPath), &output, &bytes.Buffer{})
	require.NoError(t, err)
	require.Contains(t, output.String(), "DRY RUN ONLY")
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
