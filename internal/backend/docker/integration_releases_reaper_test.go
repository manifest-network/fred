//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// TestIntegration_Docker_AgeReapedReleaseStillRestartable is the ENG-440 end-to-end
// regression. A lease that has run stably for >=90 days has a single provision-time
// "active" release in releases.db, now older than the reaper cutoff. On the next
// backend restart the synchronous startup reaper (RemoveOlderThan, run inside New()
// BEFORE Start()->recoverState) sees an all-old record. Pre-fix it whole-key-deleted
// the record, so recoverState left prov.StackManifest nil and Restart hard-failed
// ErrInvalidState "no stored manifest" (the custom-domain reconcile then looped every
// tick). With the keep-latest guard the lone active record survives, recoverState
// rehydrates the manifest, and the lease stays Restartable.
//
// This is the full chain the mock recover_state_test.go cannot reach: the REAL
// constructor reaper-before-recover ordering AND a REAL Restart succeeding afterward.
// Reconstruct/recover mechanics mirror TestIntegration_Docker_ColdStartRecovery; the
// Restart assertion mirrors TestIntegration_Docker_RestartLifecycle.
//
// "Ageing": instead of a tiny ReleasesMaxAge (which would also make the background
// cleanup loop interval ~0 and hot-spin a core for the second backend's lifetime),
// we keep the real 90d MaxAge and backdate the provisioned record's CreatedAt — a
// deterministic, flake-free simulation of 90 days passing.
func TestIntegration_Docker_AgeReapedReleaseStillRestartable(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)
	logger := slog.Default()
	ctx := context.Background()

	// Manual, reconstructable cfg (cannot use testBackendWithRealDocker: it hides
	// its tmpDir and randomizes Name, so it cannot be fed to a second New()). Mirror
	// TestIntegration_Docker_ColdStartRecovery, plus an explicit ReleasesDBPath so the
	// release record persists in tmpDir and is shared by both backend instances.
	cfg := DefaultConfig()
	cfg.SKUProfiles = defaultTestSKUProfiles()
	cfg.Name = fmt.Sprintf("test-eng440-%d", time.Now().UnixNano())
	cfg.CallbackSecret = testCallbackSecret
	cfg.HostAddress = "127.0.0.1"
	cfg.StartupVerifyDuration = 1 * time.Second
	cfg.ReconcileInterval = 1 * time.Hour // disable reconcile during the test
	cfg.ProvisionTimeout = 2 * time.Minute
	cfg.NetworkIsolation = ptrBool(false)
	tmpDir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(tmpDir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(tmpDir, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(tmpDir, "releases.db") // shared by b and b2
	for name, p := range cfg.SKUProfiles {                    // no volume FS -> drop DiskMB
		p.DiskMB = 0
		cfg.SKUProfiles[name] = p
	}

	// --- Backend #1: provision a real lease (writes the provision-time release). ---
	b, err := New(cfg, logger)
	require.NoError(t, err)
	require.NoError(t, b.Start(ctx))

	docker, err := NewDockerClient("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestContainers(t, docker, cfg.Name)
		cleanupTestNetworks(t, docker, cfg.Name)
		_ = docker.Close()
	})

	leaseUUID := fmt.Sprintf("eng440-%d", time.Now().UnixNano())
	appManifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	require.NoError(t, b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	}))
	cb := waitForCallback(t, callbackCh, leaseUUID, 2*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status, "provision must succeed")

	// Stop backend #1 WITHOUT deprovisioning: the busybox container keeps running
	// (recoverState will re-adopt it) and releases.db retains the lone active record.
	// Stop closes the release store, releasing the bbolt file lock for the backdate.
	require.NoError(t, b.Stop())

	// Simulate ~100 days elapsing so the 90d age reaper treats the record as old at
	// b2 construction.
	backdateReleaseRecords(t, cfg.ReleasesDBPath, leaseUUID, 100*24*time.Hour)

	// --- Backend #2: New() runs the synchronous startup reaper (RemoveOlderThan with
	//     the real 90d MaxAge) against the now-old record BEFORE Start()->recoverState.
	//     With the keep-latest guard the lone active record survives. ---
	b2, err := New(cfg, logger)
	require.NoError(t, err)
	require.NoError(t, b2.Start(ctx)) // recoverState rehydrates StackManifest from the surviving record
	t.Cleanup(func() { _ = b2.Stop() })

	// recoverState re-adopts the running container asynchronously, so the actor
	// may still be settling Status when Start returns — wait for Ready rather than
	// asserting it immediately (mirrors restore_metrics_test.go).
	require.Eventually(t, func() bool {
		b2.provisionsMu.RLock()
		defer b2.provisionsMu.RUnlock()
		p, ok := b2.provisions[leaseUUID]
		return ok && p.Status == backend.ProvisionStatusReady
	}, 30*time.Second, 100*time.Millisecond, "lease must recover as Ready")

	// THE ENG-440 ASSERTION: a real Restart must succeed. Pre-fix this returned
	// ErrInvalidState "no stored manifest" synchronously, because the reaped record
	// left the recovered StackManifest nil.
	require.NoError(t, b2.Restart(ctx, backend.RestartRequest{
		LeaseUUID:   leaseUUID,
		CallbackURL: callbackServer.URL,
	}), "Restart must not fail with 'no stored manifest' after the reap")
	cb = waitForCallback(t, callbackCh, leaseUUID, 2*time.Minute)
	assert.Equal(t, backend.CallbackStatusSuccess, cb.Status, "restart redeploy must succeed")

	// The restart callback reports Success before the actor flips Status=Ready, so
	// wait for Ready rather than reading it immediately — this was the source of the
	// AgeReapedReleaseStillRestartable flake.
	require.Eventually(t, func() bool {
		b2.provisionsMu.RLock()
		defer b2.provisionsMu.RUnlock()
		p, ok := b2.provisions[leaseUUID]
		return ok && p.Status == backend.ProvisionStatusReady
	}, 30*time.Second, 100*time.Millisecond, "lease must be Ready after restart")

	require.NoError(t, b2.Deprovision(ctx, leaseUUID))
}

// backdateReleaseRecords rewrites every release's CreatedAt for one lease to age ago,
// so the age reaper treats the record as expired. It opens the releases.db bbolt file
// directly (the lease's owning backend must be Stopped first so the file lock is free).
// The bucket name mirrors the on-disk constant shared.releasesBucketName ("releases").
func backdateReleaseRecords(t *testing.T, dbPath, leaseUUID string, age time.Duration) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	err = db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("releases"))
		if bucket == nil {
			return fmt.Errorf("releases bucket missing")
		}
		data := bucket.Get([]byte(leaseUUID))
		if data == nil {
			return fmt.Errorf("no release record for lease %s (provision should have written one)", leaseUUID)
		}
		var releases []shared.Release
		if err := json.Unmarshal(data, &releases); err != nil {
			return err
		}
		if len(releases) == 0 {
			return fmt.Errorf("empty release history for lease %s", leaseUUID)
		}
		backdated := time.Now().Add(-age)
		for i := range releases {
			releases[i].CreatedAt = backdated
		}
		encoded, err := json.Marshal(releases)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(leaseUUID), encoded)
	})
	require.NoError(t, err)
}
