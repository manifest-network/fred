package docker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// TestLiveRecoveryPublishesFailedAfterPredecessorTeardownFailure pins the
// ENG-1125 review's P1-1 variant: a re-provision whose predecessor teardown
// fails is ambiguous before it ever writes the candidate's callback pair, so
// its projection waits with the predecessor's pair. Recovery matches it by its
// exact pending operation, not by pair, and publishes Failed once it has
// proven the substrate absent.
func TestLiveRecoveryPublishesFailedAfterPredecessorTeardownFailure(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440102"
	payload := validManifestJSON("nginx:latest")
	var removalBlocked atomic.Bool
	removalBlocked.Store(true)
	mock := &mockDockerClient{
		RemoveContainerFn: func(context.Context, string) error {
			if removalBlocked.Load() {
				return errors.New("predecessor container is busy")
			}
			return nil
		},
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: leaseUUID, Status: backend.ProvisionStatusFailed,
			FailCount: 2, Quantity: 1, ContainerIDs: []string{"old-container"},
		}},
	})
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	require.NoError(t, b.pool.TryAllocate(leaseUUID+"-app-0", "docker-small", "tenant-a"))
	prepareFailedProvisionReplacement(t, b, mock, leaseUUID, "tenant-a", "docker-small", payload)
	predecessorPair := func() string {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		return b.provisions[leaseUUID].CallbackURL
	}()

	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
	require.NoError(t, b.Provision(context.Background(), req))
	awaitProvisionWorkerQuiescence(t, b, leaseUUID)
	b.provisionsMu.RLock()
	waiting := recoveredFromProvision(b.provisions[leaseUUID])
	b.provisionsMu.RUnlock()
	require.Equal(t, backend.ProvisionStatusProvisioning, waiting.Status)
	require.Equal(t, predecessorPair, waiting.CallbackURL,
		"the failed teardown left the predecessor's pair on the waiting projection")
	require.NotEqual(t, req.CallbackURL, waiting.CallbackURL)

	// The predecessor becomes removable. Nothing of the candidate is visible,
	// and a candidate create cannot be ruled out before the operation's deadline,
	// so let the deadline expire; recovery then proves exact absence.
	removalBlocked.Store(false)
	b.cfg.ProvisionTimeout = time.Nanosecond
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))

	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "recovery settles the failed re-provision")
	b.provisionsMu.RLock()
	failed := recoveredFromProvision(b.provisions[leaseUUID])
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, failed.Status, "the lease is never left Provisioning")
	assert.Equal(t, 3, failed.FailCount)
	assert.Equal(t, backend.ReasonInternal, failed.Reason,
		"a teardown failure observed nothing about the workload")
	assert.Empty(t, failed.ContainerIDs)
}
