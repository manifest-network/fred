package docker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func TestRecoveryCoordinatorDefersWhileActorOwnedCloseIsActive(t *testing.T) {
	const leaseUUID = "0192f1a0-1111-4abc-8def-0000000001a2"
	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseClose) }) })

	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: leaseUUID,
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusReady,
			ContainerIDs: []string{
				"container-a",
			},
		}},
	})
	installReadyRuntimeProofForTest(t, b, leaseUUID)
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		close(closeEntered)
		<-releaseClose
		return nil
	}}

	deprovisionDone := make(chan error, 1)
	go func() { deprovisionDone <- b.Deprovision(context.Background(), leaseUUID) }()
	select {
	case <-closeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("actor-owned close did not enter its physical callback")
	}

	var backgroundRan atomic.Bool
	acquired, err := b.recoveryCoordinator.WithLease(
		context.Background(), leaseUUID,
		func(shared.LeaseRecoveryScope) error {
			backgroundRan.Store(true)
			return nil
		},
	)
	require.NoError(t, err)
	assert.False(t, acquired,
		"background recovery must defer while the live command and actor own close")
	assert.False(t, backgroundRan.Load(),
		"a deferred recovery must not receive classifier authority")

	releaseOnce.Do(func() { close(releaseClose) })
	select {
	case err := <-deprovisionDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("actor-owned close did not finish")
	}
}
