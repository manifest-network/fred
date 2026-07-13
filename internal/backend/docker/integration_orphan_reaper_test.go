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

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// TestIntegration_Docker_OrphanReaper_KeepsLiveLeaseWithRemovedContainers proves
// the ENG-505 fix end-to-end: a still-open lease whose containers were removed
// out-of-band (e.g. an operator prune) keeps an active release, so the cold-start
// orphan reaper must NOT destroy its stateful volume — the tenant data survives.
//
// RED (pre-fix): cleanupOrphanedVolumes finds a volume with no live provision and
// no retention record and destroys it → the sentinel read fails.
// GREEN (with fix): leaseHasActiveRelease protects it → the sentinel survives.
//
// Runs in CI (root + btrfs loopback); skips on a non-root host.
func TestIntegration_Docker_OrphanReaper_KeepsLiveLeaseWithRemovedContainers(t *testing.T) {
	mountPath := setupBtrfsLoopback(t) // t.Skip when not root
	callbackServer, callbackCh := startCallbackServer(t)

	tmpDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.SKUProfiles = defaultTestSKUProfiles() // docker-small is stateful (DiskMB > 0)
	cfg.Name = fmt.Sprintf("test-orphan-reaper-%d", time.Now().UnixNano())
	cfg.CallbackSecret = testCallbackSecret
	cfg.HostAddress = "127.0.0.1"
	cfg.StartupVerifyDuration = 1 * time.Second
	cfg.ReconcileInterval = 1 * time.Hour
	cfg.ProvisionTimeout = 3 * time.Minute
	cfg.NetworkIsolation = ptrBool(false)
	cfg.VolumeDataPath = mountPath
	cfg.VolumeFilesystem = "btrfs"
	// All stores under one tmpDir so they persist across the b -> b2 restart
	// (the active release must survive so the reaper can see it).
	cfg.CallbackDBPath = filepath.Join(tmpDir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(tmpDir, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(tmpDir, "releases.db")
	cfg.RetentionDBPath = filepath.Join(tmpDir, "retention.db")

	logger := slog.Default()
	b, err := New(cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, b.Start(ctx))

	docker, err := NewDockerClient("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestContainers(t, docker, cfg.Name)
		cleanupTestNetworks(t, docker, cfg.Name)
		_ = docker.Close()
	})

	leaseUUID := fmt.Sprintf("orphan-reaper-%d", time.Now().UnixNano())
	// redis:7 declares VOLUME /data; fred binds the managed stateful volume there.
	appManifest := manifest.Manifest{Image: "redis:7", Command: []string{"sleep", "3600"}}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	require.NoError(t, b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	}))
	cb := waitForCallback(t, callbackCh, leaseUUID, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status)

	// Write a tenant sentinel into the mounted volume.
	const sentinel = "keep-me-ENG-505"
	containerID := getContainerID(t, leaseUUID)
	require.True(t, containerHasBindMount(t, containerID, "/data"),
		"stateful volume must bind-mount /data")
	execInContainer(t, containerID, []string{"sh", "-c",
		fmt.Sprintf("printf '%%s' '%s' > /data/sentinel.txt", sentinel)})

	volName := fmt.Sprintf("fred-%s-%s-0", leaseUUID, manifest.DefaultServiceName)
	hostSentinel := filepath.Join(mountPath, volName, "data", "sentinel.txt")
	got, err := os.ReadFile(hostSentinel)
	require.NoError(t, err, "sentinel must exist on the host volume after provision")
	require.Equal(t, sentinel, string(got))

	// Remove the container OUT-OF-BAND (no Deprovision): the lease stays open, its
	// release stays active, and no retention record is created — the exact state
	// an operator prune of exited containers leaves behind.
	require.NoError(t, docker.client.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true}))

	// Cold restart: a new backend on the same stores runs recoverState (finds no
	// container) then cleanupOrphanedVolumes.
	require.NoError(t, b.Stop())
	b2, err := New(cfg, logger)
	require.NoError(t, err)
	require.NoError(t, b2.Start(ctx))
	t.Cleanup(func() { _ = b2.Stop() })

	// The volume — and the tenant data — must survive the reaper.
	got, err = os.ReadFile(hostSentinel)
	require.NoError(t, err,
		"ENG-505: reaper must not destroy the volume of a live lease with an active release")
	assert.Equal(t, sentinel, string(got))

	vols, err := b2.volumes.List()
	require.NoError(t, err)
	assert.Contains(t, vols, volName, "the live lease's volume must still be present")
}
