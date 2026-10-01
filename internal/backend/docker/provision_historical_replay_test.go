package docker

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestProvisionHistoricalReplayUsesJournalAdmissionAcrossItemOrder(t *testing.T) {
	const lease = "f5ab7a6a-2222-4222-8222-222222222222"
	payload := []byte(`{"services":{"app":{"image":"busybox","labels":{"com.docker.compose.project":"legacy"}},"worker":{"image":"busybox"}}}`)
	_, err := manifest.ParsePayload(payload)
	require.Error(t, err, "the existing payload is forbidden to fresh tenant admission")
	stack, err := manifest.ParseStoredPayload(payload)
	require.NoError(t, err)
	items := []backend.LeaseItem{
		{SKU: "docker-micro", Quantity: 1, ServiceName: "app"},
		{SKU: "docker-micro", Quantity: 1, ServiceName: "worker"},
	}
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Status: backend.ProvisionStatusReady, Quantity: 2, Items: items, StackManifest: stack,
		}},
	})
	active := seedProvisionReleaseFromProjectionForBackendTest(t, b, lease)
	request := newProvisionRequest(lease, "tenant-a", "docker-micro", 1, payload)
	request.Items = []backend.LeaseItem{items[1], items[0]}
	err = b.Provision(t.Context(), request)
	require.ErrorIs(t, err, backend.ErrAlreadyProvisioned,
		"historical replay must cross manifest admission and reach the existing provision's ordinary disposition")
	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "the ordinary refusal proves the exact request reached durable admission")
	require.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	current, err := b.releaseStore.LatestActive(lease)
	require.NoError(t, err)
	require.Equal(t, active, *current, "a replay refusal must preserve the active generation")
	acknowledgePendingCallbacksForTest(t, b.callbackStore)

	changed := request
	changed.Items = slices.Clone(request.Items)
	changed.Items[0].Quantity++
	changed.CallbackURL = testOperationCallbackURL("http://localhost/callbacks/provision")
	require.ErrorIs(t, b.Provision(t.Context(), changed), backend.ErrInvalidManifest,
		"changed topology cannot borrow historical payload admission")
}
