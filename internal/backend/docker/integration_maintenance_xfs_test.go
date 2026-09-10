//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

// TestIntegration_Maintenance_ExactReplaySurvivesBackendRestart_XFS proves the
// production Docker+XFS composition preserves exact replacement identity
// across a backend process restart. A completed update may be acknowledged
// again, but it cannot append another release, start another worker, emit a
// second callback, lose volume data, or drop the XFS project quota.
func TestIntegration_Maintenance_ExactReplaySurvivesBackendRestart_XFS(t *testing.T) {
	mountPath := setupXFSLoopback(t)
	dataPath := filepath.Join(mountPath, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0o700))
	callbackServer, callbackCh := startCallbackServer(t)
	ctx := context.Background()
	logger := slog.Default()

	cfg := DefaultConfig()
	cfg.SKUProfiles = defaultTestSKUProfiles()
	profile := cfg.SKUProfiles["docker-micro"]
	profile.DiskMB = 64
	cfg.SKUProfiles["docker-micro"] = profile
	cfg.Name = fmt.Sprintf("test-maintenance-xfs-%d", time.Now().UnixNano())
	cfg.CallbackSecret = testCallbackSecret
	cfg.HostAddress = "127.0.0.1"
	cfg.StartupVerifyDuration = time.Second
	cfg.ReconcileInterval = time.Hour
	cfg.ProvisionTimeout = 2 * time.Minute
	cfg.ContainerStopTimeout = time.Second
	cfg.NetworkIsolation = ptrBool(false)
	cfg.VolumeDataPath = dataPath
	cfg.VolumeMountPath = mountPath
	cfg.VolumeFilesystem = "xfs"
	journalPath := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(journalPath, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(journalPath, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(journalPath, "releases.db")
	cfg.RetentionDBPath = filepath.Join(journalPath, "retention.db")

	initializeFreshIntegrationStorageIdentity(t, ctx, cfg, logger)
	first, err := New(cfg, logger)
	require.NoError(t, err)
	require.NoError(t, first.Start(ctx))

	dockerClient, err := NewDockerClient("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestContainers(t, dockerClient, cfg.Name)
		cleanupTestNetworks(t, dockerClient, cfg.Name)
		_ = dockerClient.Close()
	})

	leaseUUID := newIntegrationLeaseUUID()
	callbacks := newIntegrationCallbackAuthority(t, callbackServer.URL)
	originalPayload, err := json.Marshal(manifest.Manifest{
		Image: "redis:7", Command: []string{"sleep", "3600"},
	})
	require.NoError(t, err)
	updatedPayload, err := json.Marshal(manifest.Manifest{
		Image: "redis:7-alpine", Command: []string{"sleep", "3600"},
	})
	require.NoError(t, err)
	items := []backend.LeaseItem{{
		SKU: "docker-micro", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}

	require.NoError(t, first.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID: leaseUUID, Tenant: "test-tenant", ProviderUUID: testProviderUUID,
		Items: items, CallbackURL: callbacks.operationURL,
		LifecycleCallbackURL: callbacks.lifecycleURL, Payload: originalPayload,
	}))
	require.Equal(t, backend.CallbackStatusSuccess,
		waitForCallback(t, callbackCh, leaseUUID, 3*time.Minute).Status)
	waitForProvisionStatus(t, first, leaseUUID, backend.ProvisionStatusReady, 30*time.Second)

	containerID := getContainerID(t, leaseUUID)
	require.True(t, containerHasBindMount(t, containerID, "/data"))
	execInContainer(t, containerID, []string{"sh", "-c", "echo xfs-maintenance-sentinel > /data/sentinel.txt"})
	volumeName := canonicalVolumeName(leaseUUID, manifest.DefaultServiceName, 0)
	assert.Equal(t, int64(64)*bytesPerMiB,
		xfsBhardBytes(t, mountPath, first.volumes, volumeName))

	maintenanceID := newTestMaintenanceID(t)
	request := backend.UpdateRequest{
		MaintenanceID: maintenanceID, LeaseUUID: leaseUUID,
		CallbackURL: callbacks.lifecycleURL, Payload: updatedPayload,
	}
	require.NoError(t, first.Update(ctx, request))
	require.Equal(t, backend.CallbackStatusSuccess,
		waitForCallback(t, callbackCh, leaseUUID, 3*time.Minute).Status)
	waitForProvisionStatus(t, first, leaseUUID, backend.ProvisionStatusReady, 30*time.Second)
	containers := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, containers, 1)
	requireProvisionContainerImage(t, containers[0], "redis:7-alpine")
	releases, err := first.releaseStore.List(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, 1, countMaintenanceRelease(releases, maintenanceID))
	require.NoError(t, first.Stop())

	restarted, err := New(cfg, logger)
	require.NoError(t, err)
	require.NoError(t, restarted.Start(ctx))
	t.Cleanup(func() { _ = restarted.Stop() })
	waitForProvisionStatus(t, restarted, leaseUUID, backend.ProvisionStatusReady, 30*time.Second)
	containers = inspectProvisionContainers(t, leaseUUID)
	require.Len(t, containers, 1)
	requireProvisionContainerImage(t, containers[0], "redis:7-alpine")

	authority, err := restarted.maintenanceSettlement.NewMaintenanceRequestAuthority(
		maintenanceID, shared.MaintenanceIntentUpdate, leaseUUID,
		callbacks.lifecycleURL, updatedPayload,
	)
	require.NoError(t, err)
	disposition, err := restarted.maintenanceSettlement.ProbeMaintenanceIntent(authority)
	require.NoError(t, err)
	require.Equal(t, shared.MaintenanceIntentAdmissionCompleted, disposition)

	require.NoError(t, restarted.Update(ctx, request),
		"an exact retry after restart must replay the durable completion")
	select {
	case callback := <-callbackCh:
		t.Fatalf("exact completed replay started a second XFS worker: %+v", callback)
	case <-time.After(500 * time.Millisecond):
	}
	releases, err = restarted.releaseStore.List(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, 1, countMaintenanceRelease(releases, maintenanceID),
		"exact replay cannot append a second replacement generation")
	assert.Equal(t, int64(64)*bytesPerMiB,
		xfsBhardBytes(t, mountPath, restarted.volumes, volumeName),
		"backend restart and exact replay must preserve the production XFS quota")
	sentinel, err := os.ReadFile(filepath.Join(dataPath, volumeName, "data", "sentinel.txt"))
	require.NoError(t, err)
	assert.Contains(t, string(sentinel), "xfs-maintenance-sentinel")

	require.NoError(t, restarted.Deprovision(ctx, leaseUUID))
}
