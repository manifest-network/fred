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

func TestRun_MutationsRequireAuthenticatedBackendEvidence(t *testing.T) {
	for _, mode := range []string{"prepare", "initialize fresh"} {
		for _, transport := range []string{"HTTP", "TLS verification disabled"} {
			t.Run(mode+"/"+transport, func(t *testing.T) {
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
				configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
				if transport != "HTTP" {
					contents, err := os.ReadFile(configPath)
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(configPath, []byte(strings.Replace(string(contents), "    default: true", "    tls_skip_verify: true\n    default: true", 1)), 0o600))
				}
				args := freshInitializationArgs(t, configPath)
				backupPath := filepath.Join(t.TempDir(), "backup.db")
				if mode == "prepare" {
					args = []string{"-config", configPath, "-prepare", "-backup", backupPath, "-attest-drained", placement.LegacyPreparationDrainAttestation}
				}
				dependencies := legacyPreflightDependencies()
				bound := 0
				bind := dependencies.bindExactBackupTarget
				dependencies.bindExactBackupTarget = func(path string) (*placement.ExactBackupTarget, error) { bound++; return bind(path) }
				var output bytes.Buffer
				err := runWithDependencies(t.Context(), args, &output, &bytes.Buffer{}, dependencies)
				require.ErrorContains(t, err, "authenticated backend evidence requires certificate-verified HTTPS")
				require.Empty(t, output.String())
				require.Zero(t, bound, "transport authority must be constructed before binding a backup target")
				require.Zero(t, requests.Load())
				require.NoFileExists(t, dbPath)
				require.NoFileExists(t, backupPath)
			})
		}
	}
}

func TestRun_ReadOnlyHTTPInventoryRemainsObservational(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{preflightCommandProvisionLease: []byte("backend-a")})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	fixture := newInventoryServer(t, []backend.ProvisionInfo{{LeaseUUID: preflightCommandProvisionLease}}, nil)
	server := httptest.NewServer(fixture.Config.Handler)
	defer server.Close()
	configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
	var output bytes.Buffer
	err = runWithDependencies(t.Context(), []string{"-config", configPath}, &output, &bytes.Buffer{}, legacyPreflightDependencies(preflightCommandProvisionLease))
	require.NoError(t, err)
	require.Contains(t, output.String(), "OBSERVATION ONLY")
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
