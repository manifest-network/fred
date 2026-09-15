package docker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// Once a restoring sweep has classified a destination Failed and begun its
// rollback, Restart must not make that same destination Ready until teardown,
// re-quarantine, and source-authority reversion are complete. Before the whole
// reconcile flow shared commandFence with Restart, the sweep released the fence
// after its initial Ready check; Restart could then win while stale rollback
// moved the newly-live volume back to the source namespace.
func TestReconcileRestoring_FailedRollbackExcludesRestartAdmission(t *testing.T) {
	f := newInterruptedRestoreRecoveryFixture(t, 1, []string{"exited"}, backend.ProvisionStatusFailed, true)
	b := f.b
	destinationLease := f.spec.LeaseUUID
	require.NoError(t, b.recoverOperationIntents(t.Context()))
	require.Equal(t, []string{"remove:restore-container-0", "re-quarantine"}, f.snapshotEvents())

	// A late exact target remains owned by the durable Failed receipt while
	// source handback is pending. Reuse the fixture's strict inventory and
	// capture-before-removal checks; broad Compose Down is explicitly forbidden.
	late := f.containers[0]
	late.ContainerID = "restore-container-late"
	f.addContainer(late)

	teardownEntered := make(chan struct{})
	allowTeardown := make(chan struct{})
	var signalTeardown, releaseTeardown sync.Once
	t.Cleanup(func() { releaseTeardown.Do(func() { close(allowTeardown) }) })
	mock, ok := b.docker.(*mockDockerClient)
	require.True(t, ok)
	removeCaptured := mock.RemoveContainerFn
	mock.RemoveContainerFn = func(ctx context.Context, id string) error {
		if id != late.ContainerID {
			return fmt.Errorf("unexpected rollback removal target %q", id)
		}
		signalTeardown.Do(func() { close(teardownEntered) })
		select {
		case <-allowTeardown:
			return removeCaptured(ctx, id)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- b.reconcileRestoring(t.Context(), *f.source) }()
	select {
	case <-teardownEntered:
	case reconcileErr := <-reconcileDone:
		t.Fatalf("restoring rollback returned before exact removal: %v", reconcileErr)
	case <-time.After(3 * time.Second):
		t.Fatal("restoring rollback did not enter captured exact-container removal")
	}
	if unlock, available := b.commandFence.TryLock(destinationLease); available {
		unlock()
		t.Fatal("exact removal did not retain the destination command fence")
	}

	restartStarted := make(chan struct{})
	restartDone := make(chan error, 1)
	maintenanceID := newTestMaintenanceID(t)
	go func() {
		close(restartStarted)
		restartDone <- b.Restart(t.Context(), backend.RestartRequest{
			MaintenanceID: maintenanceID, LeaseUUID: destinationLease,
			CallbackURL: testMaintenanceLifecycleCallbackURL,
		})
	}()
	<-restartStarted
	select {
	case restartErr := <-restartDone:
		t.Fatalf("Restart crossed an in-progress restoring rollback: %v", restartErr)
	case <-time.After(100 * time.Millisecond):
	}

	releaseTeardown.Do(func() { close(allowTeardown) })
	require.NoError(t, waitForAsyncTestResult(t, reconcileDone, "failed restore source handback"))
	require.ErrorIs(t, waitForAsyncTestResult(t, restartDone, "Restart after failed restore handback"), backend.ErrNotProvisioned,
		"Restart may re-evaluate only after rollback removes the failed destination")

	assertInterruptedRestoreSettled(t, f)
	assert.Equal(t, []string{
		"remove:restore-container-0", "re-quarantine", "remove:restore-container-late",
		"measure-source-quota", "restore-source-quota",
	}, f.snapshotEvents(), "exact target retirement must precede complete source handback")
}
