package leasesm

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestMaintenanceRecoveryProjectionAcceptsLegacyRuntimeAuthority(t *testing.T) {
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
		oldURL       = "https://old.example/callbacks/provision"
		newURL       = "https://new.example/callbacks/provision"
	)
	dir := t.TempDir()
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(dir, "releases.db"),
	})
	require.NoError(t, err)
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(dir, "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})

	oldAuthority, err := shared.NewLegacyRuntimeAuthority(
		"tenant-a", providerUUID, oldURL, oldURL,
	)
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}}
	profiles := []shared.SKUResourceSnapshot{{
		SKU: "sku-a", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
	}}
	source := shared.Release{
		Manifest: []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:    "stack", Items: items, ResourceProfiles: profiles,
		LegacyRuntimeAuthority: &oldAuthority,
		Status:                 "active", CreatedAt: time.Now().Add(-time.Minute),
	}
	require.NoError(t, releases.AppendActive(leaseUUID, source))
	active, sourceClaim, err := releases.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	newAuthority, err := shared.NewLegacyRuntimeAuthority(
		"tenant-a", providerUUID, newURL, newURL,
	)
	require.NoError(t, err)
	target := active
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	target.LegacyRuntimeAuthority = &newAuthority
	storageID, err := backendidentity.Parse("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	require.NoError(t, err)
	admission, err := callbacks.BeginMaintenanceIntent(shared.MaintenanceIntentSpec{
		Kind: shared.MaintenanceIntentRestart, SourceRelease: sourceClaim,
		TargetRelease: target, Backend: "docker-a", BackendStorageID: storageID,
	})
	require.NoError(t, err)
	appendClaim, err := callbacks.StartMaintenanceAppend(admission)
	require.NoError(t, err)

	reply := make(chan error, 1)
	message, err := NewMaintenanceRecoveredSuccessMsg(
		appendClaim.Intent(),
		MaintenanceRecoveryProjection{
			ContainerIDs:      []string{"container-a"},
			ServiceContainers: map[string][]string{"app": {"container-a"}},
		},
		reply,
	)
	require.NoError(t, err)
	recovered, ok := message.(maintenanceRecoveredMsg)
	require.True(t, ok)
	assert.True(t, recovered.success.applyRecoveredRuntimeAuthority)
	assert.Equal(t, newURL, recovered.success.recoveredCallbackURL)
	assert.Equal(t, newURL, recovered.success.recoveredLifecycleCallbackURL)

	state := &ProvisionState{}
	require.NotNil(t, recovered.success.OnSuccess)
	recovered.success.OnSuccess(state)
	assert.Equal(t, "tenant-a", state.Tenant)
	assert.Equal(t, providerUUID, state.ProviderUUID)
	assert.Equal(t, items, state.Items)
	assert.Equal(t, profiles, state.ResourceProfiles)
	require.NotNil(t, state.StackManifest)
}
