package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

// This resolver is deliberately unreachable when the real Docker constructor
// refuses its runtime. It does not provide a way to bypass runtime admission.
type runtimeConstructionIdentityResolver struct {
	calls atomic.Int32
}

func (r *runtimeConstructionIdentityResolver) resolve(
	context.Context, Config, dockerClient, volumeManager,
) (backendidentity.VerifiedStorage, error) {
	r.calls.Add(1)
	return backendidentity.VerifiedStorage{}, errors.New("identity resolution must follow Docker runtime admission")
}

func TestNewBackendRefusesUnsupportedDockerBeforeOpeningStorage(t *testing.T) {
	for _, tc := range []struct {
		name          string
		apiVersion    string
		versionStatus int
		wantError     string
	}{
		{
			name: "daemon API below descriptor floor", apiVersion: "1.48",
			versionStatus: http.StatusOK, wantError: "Docker Engine 28.1+ (API 1.49+)",
		},
		{
			name: "daemon version request fails", apiVersion: "1.51",
			versionStatus: http.StatusServiceUnavailable, wantError: "version unavailable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var pings, versions, unexpected atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch {
				case req.URL.Path == "/_ping":
					pings.Add(1)
					w.Header().Set("API-Version", tc.apiVersion)
					w.WriteHeader(http.StatusOK)
				case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/version"):
					versions.Add(1)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.versionStatus)
					if tc.versionStatus != http.StatusOK {
						_, _ = io.WriteString(w, `{"message":"version unavailable"}`)
						return
					}
					_, _ = fmt.Fprintf(w, `{"ApiVersion":%q,"MinAPIVersion":"1.24"}`, tc.apiVersion)
				default:
					unexpected.Add(1)
					http.NotFound(w, req)
				}
			}))
			t.Cleanup(server.Close)

			root := t.TempDir()
			cfg := validConfig()
			cfg.DockerHost = server.URL
			cfg.VolumeMountPath = filepath.Join(root, "mount")
			cfg.VolumeDataPath = filepath.Join(cfg.VolumeMountPath, "volumes")
			cfg.VolumeFilesystem = "btrfs"
			cfg.CallbackDBPath = filepath.Join(root, "callbacks.db")
			cfg.DiagnosticsDBPath = filepath.Join(root, "diagnostics.db")
			cfg.ReleasesDBPath = filepath.Join(root, "releases.db")
			cfg.RetentionDBPath = filepath.Join(root, "retention.db")
			require.NoError(t, cfg.Validate(), "exercise runtime refusal after valid configuration")

			resolver := &runtimeConstructionIdentityResolver{}
			b, err := newBackend(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), resolver)
			require.ErrorContains(t, err, tc.wantError)
			assert.Nil(t, b)
			assert.EqualValues(t, 1, pings.Load(), "the real SDK must negotiate its API")
			assert.EqualValues(t, 1, versions.Load(), "construction must probe daemon capabilities")
			assert.Zero(t, unexpected.Load(), "no image, container, or identity query may follow refusal")
			assert.Zero(t, resolver.calls.Load(), "runtime admission must precede storage identity resolution")
			for _, path := range []string{
				cfg.CallbackDBPath, cfg.DiagnosticsDBPath, cfg.ReleasesDBPath,
				cfg.RetentionDBPath, cfg.VolumeMountPath, cfg.VolumeDataPath,
			} {
				_, statErr := os.Lstat(path)
				assert.ErrorIs(t, statErr, os.ErrNotExist, "refused construction created %s", path)
			}
			entries, err := os.ReadDir(root)
			require.NoError(t, err)
			assert.Empty(t, entries, "refused construction must not leave journals, identity markers, or volume paths")
		})
	}
}
