//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
)

// The tests in this file run docker-backend the way an operator does, against
// a stopped backend's own files: once to seal its storage identity, and once
// to start it. Sealing is for an empty backend only, and a backend that finds
// a record it cannot account for refuses to start instead of guessing. Each
// test judges the binary by its exit, its output and what it left on disk.

// stateDir is where the fixture's backend keeps its journals.
func stateDir(b *Backend) string { return filepath.Dir(b.config) }

// journals are the backend's three authoritative journals.
func journals(b *Backend) []string {
	state := stateDir(b)
	return []string{
		filepath.Join(state, "callbacks.db"),
		filepath.Join(state, "releases.db"),
		filepath.Join(state, "retention.db"),
	}
}

// markers are the two files that record a sealed backend's storage identity:
// the marker on the volume filesystem, and its anchor beside the journals.
func markers(b *Backend) []string {
	return []string{
		filepath.Join(b.VolumePath, backendidentity.MarkerFilename),
		filepath.Join(stateDir(b), "callbacks.db.storage-identity-anchor.json"),
	}
}

// runBackend runs the stopped backend's binary with the given arguments and
// waits for it to exit. A binary still running after two minutes is killed:
// every command in this file is expected to finish or to refuse well before.
func runBackend(t *testing.T, b *Backend, args ...string) (output string, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	out, err := b.command(ctx, append([]string{"-config", b.config}, args...)...).CombinedOutput()
	require.NoError(t, ctx.Err(), "docker-backend neither finished nor refused:\n%s", out)
	return string(out), err
}

// seal runs the storage identity initializer on the stopped backend.
func seal(t *testing.T, b *Backend) (output string, err error) {
	t.Helper()
	return runBackend(t, b, "-initialize-storage-identity", "new")
}

// removeAll removes files a test takes away from a stopped backend.
func removeAll(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		require.NoError(t, os.Remove(path), "remove %s", path)
	}
}

// requireAbsent checks that none of the files exists.
func requireAbsent(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		_, err := os.Lstat(path)
		require.ErrorIs(t, err, os.ErrNotExist, "%s must not exist", path)
	}
}

// TestBackend_SealingASealedBackendOnlyVerifiesIt runs the initializer again
// on a backend that is already sealed and holds a workload. It must succeed
// without changing the identity, and the backend must start on the same
// identity with the workload still ready.
func TestBackend_SealingASealedBackendOnlyVerifiesIt(t *testing.T) {
	b := StartBackend(t)
	lease := stateful("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	sealed, ok := b.servingIdentity()
	require.True(t, ok)
	before := make(map[string][]byte)
	b.Stop()
	for _, path := range markers(b) {
		content, err := os.ReadFile(path) //nolint:gosec // G304: the fixture's own marker files
		require.NoError(t, err)
		before[path] = content
	}

	out, err := seal(t, b)
	require.NoError(t, err, "sealing a sealed backend must only verify it:\n%s", out)
	for path, content := range before {
		after, readErr := os.ReadFile(path) //nolint:gosec // G304: the fixture's own marker files
		require.NoError(t, readErr)
		require.Equal(t, content, after, "%s must be left as it was", path)
	}

	b.Start()
	again, ok := b.servingIdentity()
	require.True(t, ok)
	require.Equal(t, sealed, again, "the backend must keep its storage identity")
	requireStatus(t, b, lease.LeaseUUID, statusReady)
}

// TestBackend_SealRefusesJournalsWhoseMarkersAreGone takes both markers away
// from a sealed backend. Its journals are still bound to the old identity, so
// a second seal would mint a new identity over them: the initializer refuses
// and writes no marker.
func TestBackend_SealRefusesJournalsWhoseMarkersAreGone(t *testing.T) {
	b := StartBackend(t)
	b.Stop()
	removeAll(t, markers(b)...)

	out, err := seal(t, b)
	require.Error(t, err, "sealing must be refused:\n%s", out)
	require.Contains(t, out, "already identity-bound")
	require.Contains(t, out, "restore its marker pair")
	requireAbsent(t, markers(b)...)
}

// TestBackend_SealRefusesAJournalWithoutAStorageIdentity leaves one journal
// that carries no storage identity where an empty backend has none. That is
// what a backend that never ran the 0.14 line looks like. The initializer
// refuses it, says which release adopts it, and writes no marker.
func TestBackend_SealRefusesAJournalWithoutAStorageIdentity(t *testing.T) {
	b := StartBackend(t)
	b.Stop()
	removeAll(t, append(journals(b), markers(b)...)...)
	unsealed := journals(b)[0]
	db, err := bolt.Open(unsealed, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucket([]byte("pending_callbacks"))
		return createErr
	}))
	require.NoError(t, db.Close())
	GiveToServiceAccount(t, unsealed)

	out, err := seal(t, b)
	require.Error(t, err, "sealing must be refused:\n%s", out)
	require.Contains(t, out, "1 of 3 authoritative backend journals exist without a storage identity")
	require.Contains(t, out, "final 0.14 release")
	requireAbsent(t, markers(b)...)
	requireAbsent(t, journals(b)[1:]...)
}

// TestBackend_SealRefusesABackendThatIsNotEmpty takes the journals and the
// markers away from a backend that still runs a workload with a volume. With
// no record of them, the container and the volume are unaccounted for: the
// initializer refuses to seal over them, and both are left alone.
func TestBackend_SealRefusesABackendThatIsNotEmpty(t *testing.T) {
	b := StartBackend(t)
	lease := stateful("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	running := requireOneRunningContainer(t, b, lease.LeaseUUID)
	roots, err := volumeRoots(b, lease.LeaseUUID)
	require.NoError(t, err)
	require.NotEmpty(t, roots)
	b.Stop()
	removeAll(t, append(journals(b), markers(b)...)...)

	out, err := seal(t, b)
	require.Error(t, err, "sealing must be refused:\n%s", out)
	require.Contains(t, out, "new storage identity requires an empty backend")
	require.Contains(t, out, "containers=1")
	require.NotContains(t, out, "managed_volumes=0")
	requireAbsent(t, markers(b)[0])
	still := requireOneRunningContainer(t, b, lease.LeaseUUID)
	require.Equal(t, running.ID, still.ID, "the workload must be left running")
	after, err := volumeRoots(b, lease.LeaseUUID)
	require.NoError(t, err)
	require.Equal(t, roots, after, "the volume must be left in place")
}

// rewriteJournalRow rewrites one row of a stopped backend's journal, as JSON.
// It returns the row as it was, so the test can put it back, and as written,
// so the test can tell whether a refused start changed it.
func rewriteJournalRow(t *testing.T, path, bucket, key string, edit func(row map[string]any)) (original, rewritten []byte) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err, "open %s: is the backend stopped?", path)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		rows := tx.Bucket([]byte(bucket))
		require.NotNil(t, rows, "%s has no %q bucket", path, bucket)
		stored := rows.Get([]byte(key))
		require.NotNil(t, stored, "%s holds no row for %s", path, key)
		original = bytes.Clone(stored)
		decoder := json.NewDecoder(bytes.NewReader(stored))
		// Numbers are written back exactly as they were read.
		decoder.UseNumber()
		var row map[string]any
		require.NoError(t, decoder.Decode(&row))
		edit(row)
		var encodeErr error
		rewritten, encodeErr = json.Marshal(row)
		require.NoError(t, encodeErr)
		return rows.Put([]byte(key), rewritten)
	}))
	return original, rewritten
}

// readJournalRow reads one row of a stopped backend's journal.
func readJournalRow(t *testing.T, path, bucket, key string) (stored []byte) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 10 * time.Second, ReadOnly: true})
	require.NoError(t, err, "open %s: is the backend stopped?", path)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		stored = bytes.Clone(tx.Bucket([]byte(bucket)).Get([]byte(key)))
		return nil
	}))
	return stored
}

// restoreJournalRow puts a row back as rewriteJournalRow found it.
func restoreJournalRow(t *testing.T, path, bucket, key string, original []byte) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 10 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket)).Put([]byte(key), original)
	}))
}

// TestBackend_RefusesToStartOnAReleaseTheFirst014StartNeverCompleted strips a
// running lease's release down to what v0.13 recorded, three ways: with no
// items, with items and no sizing, and with both and no authority. The
// backend completed such a release from the containers it found; it now
// refuses to start, names the lease, and leaves the workload running. With
// the row put back, the same backend starts and reports the lease ready on
// the same container.
func TestBackend_RefusesToStartOnAReleaseTheFirst014StartNeverCompleted(t *testing.T) {
	for _, shape := range []struct {
		name   string
		strip  []string
		refuse string
	}{
		{
			name:   "no items",
			strip:  []string{"items", "resource_profiles", "runtime_authority", "operation_id"},
			refuse: "records no items",
		},
		{
			name:   "no sizing",
			strip:  []string{"resource_profiles", "runtime_authority", "operation_id"},
			refuse: "records no resource profiles",
		},
		{
			name:   "no authority",
			strip:  []string{"runtime_authority", "operation_id"},
			refuse: "records no runtime authority",
		},
	} {
		t.Run(shape.name, func(t *testing.T) {
			b := StartBackend(t)
			lease := stateless("tenant-a")
			require.NoError(t, b.Client.Apply(t.Context(), lease))
			requireStatus(t, b, lease.LeaseUUID, statusReady)
			running := requireOneRunningContainer(t, b, lease.LeaseUUID)
			b.Stop()
			releases := journals(b)[1]
			original, stripped := rewriteJournalRow(t, releases, "releases", lease.LeaseUUID, func(row map[string]any) {
				history, ok := row["releases"].([]any)
				require.True(t, ok, "a release row wraps its history")
				require.NotEmpty(t, history)
				active, ok := history[len(history)-1].(map[string]any)
				require.True(t, ok)
				require.Equal(t, "active", active["status"])
				for _, field := range shape.strip {
					require.Contains(t, active, field, "an active release records its %s", field)
					delete(active, field)
				}
			})

			out, err := runBackend(t, b)
			require.Error(t, err, "the backend must refuse to start:\n%s", out)
			require.Contains(t, out, shape.refuse)
			require.Contains(t, out, lease.LeaseUUID)
			require.Contains(t, out, "the release predates the first 0.14 start")
			still := requireOneRunningContainer(t, b, lease.LeaseUUID)
			require.Equal(t, running.ID, still.ID, "a refused start must leave the workload running")
			require.Equal(t, stripped, readJournalRow(t, releases, "releases", lease.LeaseUUID),
				"a refused start must leave the release as it found it")

			restoreJournalRow(t, releases, "releases", lease.LeaseUUID, original)
			b.Start()
			requireStatus(t, b, lease.LeaseUUID, statusReady)
			again := requireOneRunningContainer(t, b, lease.LeaseUUID)
			require.Equal(t, running.ID, again.ID, "the lease must come back on the same container")
		})
	}
}

// TestBackend_RefusesToStartOnARetentionThatRecordsNoSizing removes the
// recorded sizing from a retained lease's row, which is how a row retained
// before 0.14 looks. The backend sized such a row from its configuration
// once; it now refuses to start, names the lease, and changes nothing. With
// the row put back, the same backend starts and still holds the data.
func TestBackend_RefusesToStartOnARetentionThatRecordsNoSizing(t *testing.T) {
	b := StartBackend(t)
	lease := retire(t, b, "tenant-a", "")
	requireRetained(t, b, lease)
	roots, err := volumeRoots(b, lease)
	require.NoError(t, err)
	b.Stop()
	retention := journals(b)[2]
	original, stripped := rewriteJournalRow(t, retention, "retention", lease, func(row map[string]any) {
		entry, ok := row["entry"].(map[string]any)
		require.True(t, ok, "a retention row wraps its entry")
		require.Contains(t, entry, "resource_profiles", "a retained lease records its sizing")
		delete(entry, "resource_profiles")
	})

	out, err := runBackend(t, b)
	require.Error(t, err, "the backend must refuse to start:\n%s", out)
	require.Contains(t, out, "retained resource sizing")
	require.Contains(t, out, lease)
	require.Contains(t, out, "start the final 0.14 release once")
	after, err := volumeRoots(b, lease)
	require.NoError(t, err)
	require.Equal(t, roots, after, "a refused start must leave the retained volume in place")
	require.Equal(t, stripped, readJournalRow(t, retention, "retention", lease),
		"a refused start must leave the retention row as it found it")

	restoreJournalRow(t, retention, "retention", lease, original)
	b.Start()
	requireRetained(t, b, lease)
}
