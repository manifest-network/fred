package docker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestDeprovisionStoredPolicyDriftCompletesPhysicalClose(t *testing.T) {
	const lease = "f5ab7a6a-1111-4111-8111-111111111111"
	payload := []byte(`{"services":{"app":{"image":"busybox","labels":{"com.docker.compose.project":"legacy"},"user":"1:2:3"}}}`)
	_, err := manifest.ParsePayload(payload)
	require.Error(t, err, "new tenant admission must reject this historical policy")
	stack, err := manifest.ParseStoredPayload(payload)
	require.NoError(t, err)
	mock := &mockDockerClient{ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Status: backend.ProvisionStatusReady, Quantity: 1,
			Items:         []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
			StackManifest: stack,
		}},
	})
	seedProvisionReleaseFromProjectionForBackendTest(t, b, lease)
	physicalClose := false
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		physicalClose = true
		return nil
	}}
	b.volumes = &mockVolumeManager{}
	require.NoError(t, b.doDeprovisionForTest(t, t.Context(), lease))
	assert.True(t, physicalClose, "stored validation must allow the close to reach its owned substrate workflow")
	claims, err := b.closeSettlement.ListCloseIntents()
	require.NoError(t, err)
	assert.Empty(t, claims)
	closed, err := b.callbackStore.LookupClosedLeaseReceipts([]string{lease})
	require.NoError(t, err)
	require.Len(t, closed, 1, "physical close must converge to a durable terminal receipt")
}
