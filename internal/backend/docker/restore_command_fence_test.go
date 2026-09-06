package docker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// Once a restoring sweep has classified a destination Failed and begun its
// rollback, Restart must not make that same destination Ready until teardown,
// re-quarantine, and source-authority reversion are complete. Before the whole
// reconcile flow shared commandFence with Restart, the sweep released the fence
// after its initial Ready check; Restart could then win while stale rollback
// moved the newly-live volume back to the source namespace.
func TestReconcileRestoring_FailedRollbackExcludesRestartAdmission(t *testing.T) {
	const sourceLease = "0192f1a0-1111-4abc-8def-000000000701"
	const destinationLease = "0192f1a0-2222-4abc-8def-000000000702"
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	stack := restoreStackManifest()
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		destinationLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: destinationLease, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Status: backend.ProvisionStatusFailed, Quantity: 1, Items: items, StackManifest: stack,
		}},
	})
	// This test isolates command-fence exclusion, so model the successful XFS
	// quota proof that must precede restoring->active handback. The noop volume
	// backend cannot measure usage and now correctly leaves the finalizer intact.
	b.volumes = &mockVolumeManager{
		UsageFn: func(context.Context, string) (int64, error) { return 0, nil },
	}
	retentions := attachRetentionStore(t, b)
	profiles, err := shared.BuildSKUResourceSnapshot(items, b.cfg.GetSKUProfile)
	require.NoError(t, err)
	record := shared.RetentionEntry{
		OriginalLeaseUUID: sourceLease, NewLeaseUUID: destinationLease,
		Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		Items: items, ResourceProfiles: profiles, StackManifest: stack,
		Status: shared.RetentionStatusRestoring, Generation: 3, CreatedAt: time.Now(),
	}
	record = *putRestoringRetention(t, retentions, record)
	recordRestoreOperationOutcome(t, b, record, backend.CallbackStatusFailed)

	teardownEntered := make(chan struct{})
	allowTeardown := make(chan struct{})
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		close(teardownEntered)
		<-allowTeardown
		return nil
	}}
	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- b.reconcileRestoring(context.Background(), record) }()
	select {
	case <-teardownEntered:
	case <-time.After(time.Second):
		t.Fatal("restoring rollback did not enter teardown")
	}

	restartStarted := make(chan struct{})
	restartDone := make(chan error, 1)
	go func() {
		close(restartStarted)
		restartDone <- b.Restart(context.Background(), backend.RestartRequest{
			MaintenanceID: newTestMaintenanceID(t), LeaseUUID: destinationLease,
			CallbackURL: testMaintenanceLifecycleCallbackURL,
		})
	}()
	<-restartStarted
	select {
	case restartErr := <-restartDone:
		t.Fatalf("Restart crossed an in-progress restoring rollback: %v", restartErr)
	case <-time.After(100 * time.Millisecond):
	}

	close(allowTeardown)
	require.NoError(t, <-reconcileDone)
	require.ErrorIs(t, <-restartDone, backend.ErrNotProvisioned,
		"Restart may re-evaluate only after rollback removes the failed destination")

	current, err := retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, shared.RetentionStatusActive, current.Status)
	b.provisionsMu.RLock()
	_, destinationExists := b.provisions[destinationLease]
	b.provisionsMu.RUnlock()
	assert.False(t, destinationExists)
}
