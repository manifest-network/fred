package docker

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// TestHealth_ProbesEachStore is the F31 regression: Backend.Health must fail
// when ANY persistence store is unreadable, and name the offending subsystem.
// It exercises all four store branches (not just one) so a mislabeled error or
// a wrong store reference in a copy-pasted block is caught. Docker Ping is
// healthy throughout, so the store is the only variable.
func TestHealth_ProbesEachStore(t *testing.T) {
	mock := &mockDockerClient{PingFn: func(context.Context) error { return nil }}
	dir := t.TempDir()

	cases := []struct {
		name    string
		wantMsg string
		wire    func(t *testing.T, b *Backend) interface{ Close() error }
	}{
		{"callback", "callback store unhealthy", func(t *testing.T, b *Backend) interface{ Close() error } {
			s, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: filepath.Join(dir, "cb.db")})
			require.NoError(t, err)
			b.callbackStore = s
			return s
		}},
		{"diagnostics", "diagnostics store unhealthy", func(t *testing.T, b *Backend) interface{ Close() error } {
			s, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: filepath.Join(dir, "dg.db")})
			require.NoError(t, err)
			b.diagnosticsStore = s
			return s
		}},
		{"release", "release store unhealthy", func(t *testing.T, b *Backend) interface{ Close() error } {
			s, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: filepath.Join(dir, "rl.db")})
			require.NoError(t, err)
			b.releaseStore = s
			return s
		}},
		{"retention", "retention store unhealthy", func(t *testing.T, b *Backend) interface{ Close() error } {
			s, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: filepath.Join(dir, "rt.db")})
			require.NoError(t, err)
			b.retentionStore = s
			return s
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Only the target store is wired (others nil → skipped), so it is the
			// first — and only — branch that can fail.
			b := &Backend{docker: mock}
			store := tc.wire(t, b)

			require.NoError(t, b.Health(context.Background()), "docker ok + store open → healthy")

			require.NoError(t, store.Close())
			err := b.Health(context.Background())
			require.Error(t, err, "closed store must make Health fail")
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}
