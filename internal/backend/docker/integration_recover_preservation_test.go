//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// TestIntegration_Recover_PreservesReservationForContainerlessFailedLease is the
// real-Docker proof of ENG-567's pool-authoritative recoverState rule: the
// admission-pool reservation of a lease that is still tracked in b.provisions
// must survive recoverState even when the lease has NO container at all.
//
// The unit tests (recover_state_test.go) exercise this against a mocked
// ListManagedContainers. This test reproduces the same "tracked, held
// reservation, no container" state against a REAL Docker daemon:
//
//  1. Provision a real stateful lease (docker-small, DiskMB=1024) on a real
//     btrfs-quota'd volume — a real pool reservation is made and a real
//     container is created.
//  2. Kill the container out-of-band (SIGKILL) so Docker still reports it,
//     exited, then run recoverState so the lease observes the death and
//     (asynchronously, via the SM's Failing->Failed flow) settles into
//     Status=Failed. This is the ONLY way to reach a Failed lease: a plain
//     Ready lease with no container is dropped from b.provisions entirely,
//     but a crash takes Ready->Failed, which recoverState keeps tracked.
//  3. Remove the now-exited container out-of-band (RemoveContainer) — NOT via
//     Deprovision — so the tracked, Failed lease has no container left.
//  4. Run recoverState again and assert the pool reservation is unchanged.
//
// Before ENG-567, recoverState's per-status allowlist only preserved a Failed
// lease's pool key while VolumeCleanupAttempts > 0 (i.e., mid-Deprovision
// cleanup retry). A Ready->crash->GC'd Failed lease like this one has
// VolumeCleanupAttempts == 0 and was NOT on the allowlist, so the old code
// would drop its key on this recoverState pass — silently freeing disk the
// lease's volume still occupies on the real filesystem. ENG-567 replaced the
// allowlist with "preserve every lease still tracked in b.provisions", which
// this test locks in against the real pool + real Docker daemon.
func TestIntegration_Recover_PreservesReservationForContainerlessFailedLease(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	leaseUUID := fmt.Sprintf("recover-preserve-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	const sku = "docker-small" // stateful: DiskMB=1024 (defaultTestSKUProfiles)

	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.VolumeDataPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
		cfg.RetentionDBPath = filepath.Join(t.TempDir(), "retention.db")
		// Manual recoverState control only — no background reconcile loop and
		// no reconciler in this test, matching
		// TestIntegration_Reconciler_DetectsFailureWithoutRecoverState's pattern.
		cfg.ReconcileInterval = 1 * time.Hour
	})

	ctx := context.Background()

	// redis:7 declares VOLUME /data; "sleep" as the command keeps redis-server
	// from starting so it doesn't touch the quota'd volume on its own.
	appManifest := manifest.Manifest{Image: "redis:7", Command: []string{"sleep", "3600"}}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	// 1. Provision the real stateful lease.
	require.NoError(t, b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       tenant,
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: sku, Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	}))
	require.Equal(t, backend.CallbackStatusSuccess, waitForCallback(t, callbackCh, leaseUUID, 3*time.Minute).Status)

	// 2. The pool holds the lease's reserved disk footprint F.
	statsAfterProvision := b.pool.Stats()
	reservedDiskMB := statsAfterProvision.AllocatedDiskMB
	require.Positive(t, reservedDiskMB, "docker-small provision must reserve disk in the pool")
	assert.EqualValues(t, 1024, reservedDiskMB, "docker-small (defaultTestSKUProfiles) reserves 1024 MB")

	// 3. Kill the container out-of-band. It exits but remains visible to Docker
	// (and thus to recoverState) until explicitly removed.
	containerID := getContainerID(t, leaseUUID)
	killContainer(t, containerID)
	waitForContainerExited(t, containerID)

	// Run recoverState (via the exported RefreshState wrapper). It observes the
	// exited container and hands the death to the lease's actor; the actual
	// Ready->Failing->Failed settle is asynchronous, so poll for it below
	// rather than asserting immediately.
	require.NoError(t, b.RefreshState(ctx))
	waitForProvisionStatus(t, b, leaseUUID, backend.ProvisionStatusFailed, 30*time.Second)

	// Drain the failure callback the SM's Failing->Failed transition fires.
	// It may or may not have arrived yet depending on timing; don't block on it.
	select {
	case cb := <-callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status)
	case <-time.After(5 * time.Second):
	}

	// The lease is Failed but its container still physically exists, so the
	// reservation is still backed by a real (if dead) container at this point —
	// not yet the scenario under test.
	failedInfo := getProvisionInfo(t, b, leaseUUID)
	require.Equal(t, backend.ProvisionStatusFailed, failedInfo.Status)

	// 4. Remove the exited container out-of-band via the backend's own Docker
	// client — NOT via Deprovision — so the tracked Failed lease now has no
	// container left anywhere. This is the pre-ENG-567 danger zone: the
	// allowlist saw Failed + VolumeCleanupAttempts==0 (no Deprovision ever
	// ran) and dropped the pool key here.
	require.NoError(t, b.docker.RemoveContainer(ctx, containerID))

	// 5. Run recoverState again with the container gone.
	require.NoError(t, b.RefreshState(ctx))

	// 6. ENG-567: the reservation is PRESERVED because the lease is still
	// tracked in b.provisions, even though it has no container backing it.
	statsAfterRemoval := b.pool.Stats()
	assert.Equal(t, reservedDiskMB, statsAfterRemoval.AllocatedDiskMB,
		"pool-authoritative recoverState must preserve a tracked Failed lease's "+
			"reservation even after its last container is gone (ENG-567)")

	// The lease must still be tracked (not silently dropped from b.provisions).
	stillTracked := getProvisionInfo(t, b, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, stillTracked.Status)

	// Cleanup: release the reservation the normal way so t.Cleanup's container/
	// network sweep and the loopback unmount don't race a lingering allocation.
	require.NoError(t, b.Deprovision(ctx, leaseUUID))
}
