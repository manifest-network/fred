package placement

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestOpenStoreRejectsInsecureAuthorityFile(t *testing.T) {
	tests := map[string]struct {
		mutate func(*testing.T, string)
		want   string
	}{
		"group-readable": {
			mutate: func(t *testing.T, path string) { require.NoError(t, os.Chmod(path, 0o640)) },
			want:   "exact mode 0600",
		},
		"world-readable": {
			mutate: func(t *testing.T, path string) { require.NoError(t, os.Chmod(path, 0o644)) },
			want:   "exact mode 0600",
		},
		"hard-linked": {
			mutate: func(t *testing.T, path string) {
				require.NoError(t, os.Link(path, filepath.Join(filepath.Dir(path), "placement-alias.db")))
			},
			want: "exactly one hard link",
		},
		"symlink": {
			mutate: func(t *testing.T, path string) {
				target := filepath.Join(filepath.Dir(path), "placement-target.db")
				require.NoError(t, os.Rename(path, target))
				require.NoError(t, os.Symlink(filepath.Base(target), path))
			},
			want: "not a regular file",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "placement.db")
			require.NoError(t, InitializeFreshStoreContext(
				t.Context(), freshTestPlan(t, path, []string{"backend-a"}),
			))
			test.mutate(t, path)

			store, err := OpenStore(path, freshTestProviderUUID)
			if store != nil {
				_ = store.Close()
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, test.want)
		})
	}
}

func TestRuntimeAuthorityRejectsPermissionLinkAndSymlinkDrift(t *testing.T) {
	tests := map[string]func(*testing.T, string){
		"mode": func(t *testing.T, path string) {
			require.NoError(t, os.Chmod(path, 0o644))
		},
		"hard-link": func(t *testing.T, path string) {
			require.NoError(t, os.Link(path, filepath.Join(filepath.Dir(path), "placement-alias.db")))
		},
		"symlink": func(t *testing.T, path string) {
			target := filepath.Join(filepath.Dir(path), "placement-target.db")
			require.NoError(t, os.Rename(path, target))
			require.NoError(t, os.Symlink(filepath.Base(target), path))
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "placement.db")
			require.NoError(t, InitializeFreshStoreContext(
				t.Context(), freshTestPlan(t, path, []string{"backend-a"}),
			))
			info, err := os.Lstat(path)
			require.NoError(t, err)
			stat, ok := info.Sys().(*syscall.Stat_t)
			require.True(t, ok)
			require.Equal(t, uint64(1), stat.Nlink)
			require.Equal(t, os.FileMode(0o600), info.Mode())

			store, err := OpenStore(path, freshTestProviderUUID)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			baseline := store.CurrentAdmissionBaseline()
			require.True(t, baseline.Valid())
			scope, err := store.ScopeAdmission(baseline, []string{"backend-a"})
			require.NoError(t, err)
			mutate(t, path)

			operationID := requireOperationID(t, "99003")
			token, applied, writeErr := store.BeginNewAttempt(
				scope, "lease-after-metadata-drift", "backend-a", operationID,
				PayloadFingerprint{}, testBackendRequestSnapshot(t), testCallbackPair(operationID),
			)
			require.ErrorIs(t, writeErr, ErrRuntimeAuthorityUnavailable)
			require.ErrorIs(t, writeErr, ErrRuntimeAuthorityPathChanged)
			assert.False(t, applied)
			assert.False(t, token.Valid())
			healthErr := store.Healthy()
			require.ErrorIs(t, healthErr, ErrRuntimeAuthorityUnavailable)
			require.ErrorIs(t, healthErr, ErrRuntimeAuthorityPathChanged)
			assert.ErrorIs(t, store.Healthy(), ErrRuntimeAuthorityUnavailable,
				"authority drift must remain sticky for the process lifetime")
		})
	}
}

func TestRuntimeAuthorityRenameOverOpenWithdrawsAdmissionBeforeWrite(t *testing.T) {
	directory := t.TempDir()
	authorityPath := filepath.Join(directory, "placement.db")
	replacementPath := filepath.Join(directory, "replacement.db")
	for _, path := range []string{authorityPath, replacementPath} {
		require.NoError(t, InitializeFreshStoreContext(
			t.Context(), freshTestPlan(t, path, []string{"backend-a"}),
		))
	}

	store, err := OpenStore(authorityPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	baseline := store.CurrentAdmissionBaseline()
	require.True(t, baseline.Valid())
	scope, err := store.ScopeAdmission(baseline, []string{"backend-a"})
	require.NoError(t, err)

	// Linux permits an atomic rename over a pathname whose old inode remains
	// open. bbolt would otherwise continue writing the now-unlinked old file
	// while a future process opens a different authority at the configured path.
	require.NoError(t, os.Rename(replacementPath, authorityPath))

	operationID := requireOperationID(t, "99002")
	token, applied, err := store.BeginNewAttempt(
		scope, "lease-after-path-replacement", "backend-a", operationID,
		PayloadFingerprint{}, testBackendRequestSnapshot(t), testCallbackPair(operationID),
	)
	require.ErrorIs(t, err, ErrRuntimeAuthorityUnavailable)
	require.ErrorIs(t, err, ErrRuntimeAuthorityPathChanged)
	assert.False(t, applied)
	assert.False(t, token.Valid())

	healthErr := store.Healthy()
	require.ErrorIs(t, healthErr, ErrRuntimeAuthorityUnavailable)
	require.ErrorIs(t, healthErr, ErrRuntimeAuthorityPathChanged)
	assert.False(t, store.CurrentAdmissionBaseline().Valid())
	assert.False(t, store.InventoryBootstrapped())
	assert.False(t, store.BeginInventorySession().Valid())
	assert.Equal(t, StateUnusable, store.Lookup("lease-after-path-replacement").State())
	assert.Equal(t, LifecycleVerdictUnusable,
		store.CurrentLifecycle("lease-after-path-replacement").Verdict())

	require.NoError(t, store.Close())
	replacement, err := OpenStore(authorityPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replacement.Close() })
	assert.Equal(t, StateAbsent, replacement.Lookup("lease-after-path-replacement").State(),
		"the process must not write through to the replacement authority")
}

func TestRuntimeAuthorityHealthDetectsRenameOverOpen(t *testing.T) {
	directory := t.TempDir()
	authorityPath := filepath.Join(directory, "placement.db")
	replacementPath := filepath.Join(directory, "replacement.db")
	for _, path := range []string{authorityPath, replacementPath} {
		require.NoError(t, InitializeFreshStoreContext(
			t.Context(), freshTestPlan(t, path, []string{"backend-a"}),
		))
	}

	store, err := OpenStore(authorityPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.Healthy())
	require.NoError(t, os.Rename(replacementPath, authorityPath))

	healthErr := store.Healthy()
	require.ErrorIs(t, healthErr, ErrRuntimeAuthorityUnavailable)
	require.ErrorIs(t, healthErr, ErrRuntimeAuthorityPathChanged)

	// Subsequent filesystem changes cannot clear the sticky failure; reopening a
	// Store is the only recovery boundary.
	require.NoError(t, os.Remove(authorityPath))
	assert.Error(t, store.Healthy())
}

func TestRuntimeAuthorityWithdrawalLinearizesWithAdmittedCommit(t *testing.T) {
	store := newTestStore(t)
	path := store.db.Path()
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })

	key := []byte("admitted-before-withdrawal")
	entered := make(chan struct{})
	release := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- store.updateRuntimeAuthority(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte("authority_linearization_test"))
			if err != nil {
				return err
			}
			if err := bucket.Put(key, []byte("committed")); err != nil {
				return err
			}
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	require.NoError(t, os.Chmod(path, 0o640))
	healthDone := make(chan error, 1)
	healthStarted := make(chan struct{})
	go func() {
		close(healthStarted)
		healthDone <- store.Healthy()
	}()
	<-healthStarted
	select {
	case err := <-healthDone:
		t.Fatalf("authority withdrawal overtook admitted commit: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	writeErr := <-writeDone
	require.ErrorIs(t, writeErr, ErrRuntimeAuthorityUnavailable)
	require.ErrorIs(t, writeErr, ErrRuntimeAuthorityPathChanged)
	healthErr := <-healthDone
	require.ErrorIs(t, healthErr, ErrRuntimeAuthorityUnavailable)
	require.ErrorIs(t, healthErr, ErrRuntimeAuthorityPathChanged)

	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		assert.Equal(t, []byte("committed"),
			tx.Bucket([]byte("authority_linearization_test")).Get(key),
			"the write admitted before withdrawal may finish")
		return nil
	}))
	lateMutationRan := false
	err := store.updateRuntimeAuthority(func(*bolt.Tx) error {
		lateMutationRan = true
		return nil
	})
	require.ErrorIs(t, err, ErrRuntimeAuthorityUnavailable)
	assert.False(t, lateMutationRan, "no mutation may begin after withdrawal is published")
}
