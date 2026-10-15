//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// replaceWithLegacyRuntimeContainer swaps the lease's only container for one
// a release before the runtime contract would have run: the same Fred and
// Compose labels and mounts except the runtime-contract label, attached to a
// per-tenant network instead of the lease's own.
func replaceWithLegacyRuntimeContainer(t *testing.T, ctx context.Context, leaseUUID, tenant string) {
	t.Helper()
	sdk, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()

	tenantNetwork := TenantNetworkName(tenant)
	_, err = sdk.NetworkCreate(ctx, tenantNetwork, networktypes.CreateOptions{
		Driver: "bridge",
		Labels: map[string]string{LabelManaged: "true", LabelTenant: tenant},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.NetworkRemove(context.Background(), tenantNetwork) })

	containerID := getContainerID(t, leaseUUID)
	current, err := sdk.ContainerInspect(ctx, containerID)
	require.NoError(t, err)
	labels := maps.Clone(current.Config.Labels)
	delete(labels, LabelRuntimeContract)
	config := *current.Config
	config.Labels = labels
	hostConfig := *current.HostConfig
	hostConfig.NetworkMode = container.NetworkMode(tenantNetwork)
	serviceName := labels[LabelServiceName]

	timeout := 1
	require.NoError(t, sdk.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout}))
	require.NoError(t, sdk.ContainerRemove(ctx, containerID, container.RemoveOptions{}))
	created, err := sdk.ContainerCreate(ctx, &config, &hostConfig, &networktypes.NetworkingConfig{
		EndpointsConfig: map[string]*networktypes.EndpointSettings{
			tenantNetwork: {Aliases: []string{serviceName}},
		},
	}, nil, current.Name)
	require.NoError(t, err)
	require.NoError(t, sdk.ContainerStart(ctx, created.ID, container.StartOptions{}))
}

func TestIntegration_Docker_RuntimeRefreshMovesALegacyLeaseToItsOwnNetwork(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(true)
		cfg.VolumeDataPath = mountPath
		cfg.VolumeMountPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
	})
	ctx := context.Background()
	leaseUUID := newIntegrationLeaseUUID()
	tenant := fmt.Sprintf("refresh-tenant-%d", time.Now().UnixNano())
	callbacks := newIntegrationCallbackAuthority(t, callbackServer.URL)
	payload, err := json.Marshal(manifest.Manifest{
		Image:   "redis:7",
		Command: []string{"redis-server", "--save", "1", "1"},
	})
	require.NoError(t, err)
	require.NoError(t, b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID: leaseUUID, Tenant: tenant, ProviderUUID: testProviderUUID,
		Items:       []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL: callbacks.operationURL, LifecycleCallbackURL: callbacks.lifecycleURL,
		Payload: payload,
	}))
	cb := waitForCallback(t, callbackCh, leaseUUID, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status)

	// Data written before the refresh.
	containerID := getContainerID(t, leaseUUID)
	execInContainer(t, containerID, []string{"redis-cli", "SET", "refresh_key", "refresh_value"})
	execInContainer(t, containerID, []string{"redis-cli", "SAVE"})

	// Make the lease look like one a release before the runtime contract
	// deployed, then let the backend observe it.
	replaceWithLegacyRuntimeContainer(t, ctx, leaseUUID, tenant)
	require.NoError(t, b.recoverState(ctx))
	waitForProvisionStatus(t, b, leaseUUID, backend.ProvisionStatusReady, 30*time.Second)
	before, err := b.RuntimeRefreshStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, before.Stale, "a container without the runtime contract is stale")

	_, err = b.StartRuntimeRefresh(ctx)
	require.NoError(t, err)
	var status RuntimeRefreshStatus
	require.Eventually(t, func() bool {
		status, err = b.RuntimeRefreshStatus(ctx)
		return err == nil && !status.Active
	}, 3*time.Minute, 500*time.Millisecond)
	require.Equal(t, 0, status.Stale)
	require.Len(t, status.Leases, 1)
	assert.Equal(t, RuntimeRefreshRefreshed, status.Leases[0].Outcome)

	refreshed := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, refreshed, 1)
	assert.Equal(t, status.Leases[0].MaintenanceID, refreshed[0].Labels[LabelMaintenanceID],
		"the attested maintenance is the one its new container carries")
	assert.Equal(t, currentRuntimeContract.label(), refreshed[0].Labels[LabelRuntimeContract])
	require.Len(t, refreshed[0].NetworkSettings.Networks, 1)
	_, onLeaseNetwork := refreshed[0].NetworkSettings.Networks[leaseNetworkName(leaseUUID)]
	assert.True(t, onLeaseNetwork, "the refreshed lease runs on its own network")

	result := execInContainer(t, refreshed[0].ID, []string{"redis-cli", "GET", "refresh_key"})
	assert.Contains(t, result, "refresh_value", "a refresh keeps the lease's data")
	assert.Equal(t, backend.ProvisionStatusReady, getProvisionInfo(t, b, leaseUUID).Status)

	require.NoError(t, b.Deprovision(ctx, leaseUUID))
}
