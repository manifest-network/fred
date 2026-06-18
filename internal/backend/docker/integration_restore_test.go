//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// TestIntegration_Docker_RetainRestoreLifecycle exercises the full soft-delete
// + restore round-trip on real Docker + btrfs:
//
//  1. Provision a stateful redis:7 lease (VOLUME /data → btrfs subvolume).
//  2. Write a sentinel into /data inside the container.
//  3. Deprovision with RetainOnClose → the canonical volume is renamed into the
//     retained namespace (fred-retained-...) and a retention record is written.
//  4. Verify the canonical volume is gone, the retained volume + sentinel survive,
//     and the retention record is present and Active.
//  5. Restore into a NEW lease.
//  6. Verify the sentinel is readable from the new lease's container, the
//     retention record is deleted, the new lease's canonical volume exists, and
//     the retained volume is gone.
func TestIntegration_Docker_RetainRestoreLifecycle(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.VolumeDataPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
		cfg.RetainOnClose = true
		// Isolate the retention DB. testBackendWithRealDocker points the other
		// DBs at its own tmpDir but does NOT set RetentionDBPath; the default is
		// "retention.db" relative to cwd, which would pollute across tests.
		cfg.RetentionDBPath = filepath.Join(t.TempDir(), "retention.db")
		// Disable the background reaper so it doesn't race the test assertions.
		cfg.RetentionMaxAge = 0 // 0 = reaping disabled
		cfg.RetentionReapInterval = 0
	})

	ctx := context.Background()
	origLease := fmt.Sprintf("retain-restore-orig-%d", time.Now().UnixNano())

	// ── STEP 1: Provision a stateful lease ────────────────────────────────
	//
	// redis:7 declares VOLUME /data; the backend bind-mounts
	// {mountPath}/fred-{origLease}-app-0 → /data inside the container.
	appManifest := manifest.Manifest{
		Image:   "redis:7",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    origLease,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	cb := waitForCallback(t, callbackCh, origLease, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status)

	// ── STEP 2: Write sentinel file via execInContainer ───────────────────
	//
	// The bind-mount destination for redis:7's VOLUME /data is /data. The
	// sentinel is written at /data/sentinel.txt inside the container, which
	// lands at {mountPath}/fred-{origLease}-app-0/data/sentinel.txt on the host.
	containerID := getContainerID(t, origLease)
	assert.True(t, containerHasBindMount(t, containerID, "/data"),
		"container must have bind mount at /data for sentinel to persist")
	execInContainer(t, containerID, []string{"sh", "-c", "echo retain-restore-sentinel > /data/sentinel.txt"})

	// Confirm sentinel is visible on host NOW (before deprovision).
	canonicalVolumeID := canonicalVolumeName(origLease, manifest.DefaultServiceName, 0)
	hostSentinelPath := filepath.Join(mountPath, canonicalVolumeID, "data", "sentinel.txt")
	_, err = os.Stat(hostSentinelPath)
	require.NoError(t, err, "sentinel must exist on host before deprovision")

	// ── STEP 3: Deprovision with RetainOnClose ────────────────────────────
	err = b.Deprovision(ctx, origLease)
	require.NoError(t, err)

	// Wait for Deprovisioned callback.
	cb = waitForCallback(t, callbackCh, origLease, 30*time.Second)
	require.Equal(t, backend.CallbackStatusDeprovisioned, cb.Status)
	// ENG-329 ground truth: a real btrfs soft-delete must set Retained=true on the
	// terminal deprovision callback (the best-effort push's observed outcome).
	require.True(t, cb.Retained, "real btrfs retain must set the ground-truth Retained flag on the deprovision callback")

	// ── STEP 4a: Canonical volume is gone; retained volume exists ─────────
	canonicalPath := filepath.Join(mountPath, canonicalVolumeID)
	_, err = os.Stat(canonicalPath)
	assert.True(t, errors.Is(err, fs.ErrNotExist),
		"canonical volume must be gone after soft-delete deprovision")

	retainedVolumeID := retainedName(canonicalVolumeID)
	retainedPath := filepath.Join(mountPath, retainedVolumeID)
	_, err = os.Stat(retainedPath)
	require.NoError(t, err, "retained volume directory must exist after soft-delete")

	// ── STEP 4b: Sentinel is intact in the retained volume ────────────────
	retainedSentinelPath := filepath.Join(retainedPath, "data", "sentinel.txt")
	sentinelBytes, err := os.ReadFile(retainedSentinelPath)
	require.NoError(t, err, "sentinel must survive in the retained volume")
	assert.Contains(t, string(sentinelBytes), "retain-restore-sentinel")

	// ── STEP 4c: Retention record is present in the store ─────────────────
	rec, err := b.retentionStore.Get(origLease)
	require.NoError(t, err)
	require.NotNil(t, rec, "retention record must exist after soft-delete")
	assert.Equal(t, shared.RetentionStatusActive, rec.Status)
	assert.Equal(t, "test-tenant", rec.Tenant)
	require.Len(t, rec.RetainedVolumeNames, 1)
	assert.Equal(t, retainedVolumeID, rec.RetainedVolumeNames[0])

	// ── STEP 4d: GetProvision surfaces the queryable retention status ─────
	//
	// ENG-329 Part A on REAL retained state: with the provision gone from the
	// in-memory map, GetProvision must report Status=retained with a non-zero
	// RetainedUntil (CreatedAt + RetentionMaxAge) and the restore-shape Items —
	// the offline-tenant self-serve path.
	info, err := b.GetProvision(ctx, origLease)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, backend.ProvisionStatusRetained, info.Status, "GetProvision must report retained for soft-deleted state")
	assert.False(t, info.RetainedUntil.IsZero(), "retained provision must carry a RetainedUntil deadline")
	assert.Equal(t, rec.CreatedAt.Add(b.cfg.RetentionMaxAge), info.RetainedUntil, "RetainedUntil = CreatedAt + RetentionMaxAge")
	assert.Equal(t, "test-tenant", info.Tenant, "Tenant must be populated for the closed-lease authz fallback")
	require.NotEmpty(t, info.Items, "retained provision must carry the restore-shape Items")

	// ── STEP 5: Restore into a new lease ──────────────────────────────────
	newLease := fmt.Sprintf("retain-restore-new-%d", time.Now().UnixNano())

	err = b.Restore(ctx, backend.RestoreRequest{
		LeaseUUID:     newLease,
		FromLeaseUUID: origLease,
		Tenant:        "test-tenant",
		ProviderUUID:  "test-provider",
		// Items shape must match the retained set: ServiceName="app", Quantity=1.
		Items:       []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		CallbackURL: callbackServer.URL,
	})
	require.NoError(t, err)

	// Wait for restore callback (success).
	cb = waitForCallback(t, callbackCh, newLease, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status,
		"restore callback must report success; error: %s", cb.Error)

	// ── STEP 6a: Sentinel survives in the new lease's container ────────────
	newContainerID := getContainerID(t, newLease)
	sentinel := execInContainer(t, newContainerID,
		[]string{"cat", "/data/sentinel.txt"})
	assert.Contains(t, sentinel, "retain-restore-sentinel",
		"sentinel must be readable from the new lease's container after restore")

	// ── STEP 6b: Retention record is deleted after successful restore ──────
	rec, err = b.retentionStore.Get(origLease)
	require.NoError(t, err)
	assert.Nil(t, rec, "retention record must be removed after successful restore")

	// ── STEP 6c: New lease's canonical volume exists; retained is gone ─────
	newCanonicalVolumeID := canonicalVolumeName(newLease, manifest.DefaultServiceName, 0)
	newCanonicalPath := filepath.Join(mountPath, newCanonicalVolumeID)
	_, err = os.Stat(newCanonicalPath)
	assert.NoError(t, err, "new lease's canonical volume must exist after restore")

	_, err = os.Stat(retainedPath)
	assert.True(t, errors.Is(err, fs.ErrNotExist),
		"retained volume must be gone after successful restore")

	// Cleanup: hard-delete the new lease. Disable retention for this final
	// teardown so we don't leave an unreaped retained volume + record behind
	// (the reaper is off in this test).
	b.cfg.RetainOnClose = false
	err = b.Deprovision(ctx, newLease)
	require.NoError(t, err)
}

// TestIntegration_Docker_RetainGraceReap verifies that the background retention
// reaper hard-deletes a soft-deleted lease's record and retained volume once the
// (tiny) grace window elapses.
func TestIntegration_Docker_RetainGraceReap(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)

	// Grace window sized so the record is deterministically observable AFTER
	// deprovision completes but still reaps within the test's patience. CreatedAt
	// is stamped mid-deprovision while the Deprovisioned callback only fires at
	// the end (after teardown + renames), so a sub-second window would race the
	// callback and flake the "must exist immediately" assertions below.
	const graceMaxAge = 5 * time.Second
	const reapInterval = 250 * time.Millisecond

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.VolumeDataPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
		cfg.RetainOnClose = true
		cfg.RetentionDBPath = filepath.Join(t.TempDir(), "retention.db")
		cfg.RetentionMaxAge = graceMaxAge
		cfg.RetentionReapInterval = reapInterval
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("retain-reap-%d", time.Now().UnixNano())

	// Provision a stateful lease.
	appManifest := manifest.Manifest{
		Image:   "redis:7",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	cb := waitForCallback(t, callbackCh, leaseUUID, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status)

	// Deprovision with RetainOnClose — soft-delete.
	err = b.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)

	cb = waitForCallback(t, callbackCh, leaseUUID, 30*time.Second)
	require.Equal(t, backend.CallbackStatusDeprovisioned, cb.Status)

	// Record and retained volume must exist immediately after deprovision.
	retainedVolumeID := retainedName(canonicalVolumeName(leaseUUID, manifest.DefaultServiceName, 0))
	retainedPath := filepath.Join(mountPath, retainedVolumeID)
	_, err = os.Stat(retainedPath)
	require.NoError(t, err, "retained volume must exist immediately after soft-delete")

	rec, err := b.retentionStore.Get(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, rec, "retention record must exist immediately after soft-delete")

	// Wait for the periodic reaper to destroy record + volume. The reaper fires
	// every reapInterval (250ms) and deletes records older than graceMaxAge (5s),
	// so both must be gone within graceMaxAge + a few sweep cycles. Generous
	// wall-clock ceiling.
	require.Eventually(t, func() bool {
		r, err := b.retentionStore.Get(leaseUUID)
		if err != nil || r != nil {
			return false
		}
		_, statErr := os.Stat(retainedPath)
		return errors.Is(statErr, fs.ErrNotExist)
	}, 30*time.Second, 250*time.Millisecond,
		"retention record and retained volume must be reaped after RetentionMaxAge")
}
