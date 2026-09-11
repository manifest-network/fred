package leasesm

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

func TestMaintenanceRecoveryProjectionAcceptsLegacyRuntimeAuthority(t *testing.T) {
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
		oldURL       = "https://old.example/callbacks/provision"
		newURL       = "https://new.example/callbacks/provision"
	)
	dir := t.TempDir()
	callbacks, releases, storage, gate := newBoundLeaseSMMaintenanceStores(t, dir, "docker-a")

	oldAuthority, err := shared.NewLegacyRuntimeAuthority(
		"tenant-a", providerUUID, oldURL, oldURL,
	)
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}}
	profiles := []shared.SKUResourceSnapshot{{
		SKU: "sku-a", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
	}}
	legacySource := shared.Release{
		Version:  1,
		Manifest: []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:    "stack", Status: "active", CreatedAt: time.Now().Add(-time.Minute),
	}
	// Seed the exact bare v0.13 wire shape, then drive the real stopped-upgrade
	// APIs. This preserves compatibility coverage without retaining a public
	// current-writer escape hatch for legacy authority.
	require.NoError(t, releases.Close())
	db, err := bolt.Open(filepath.Join(dir, "releases.db"), 0o600, nil)
	require.NoError(t, err)
	encoded, err := json.Marshal(struct {
		SchemaVersion uint8            `json:"schema_version"`
		Releases      []shared.Release `json:"releases"`
	}{SchemaVersion: 1, Releases: []shared.Release{legacySource}})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("releases")).Put([]byte(leaseUUID), encoded)
	}))
	require.NoError(t, db.Close())
	releases, err = shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: filepath.Join(dir, "releases.db")}, storage, gate,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})
	backfiller, err := shared.NewReleaseBackfiller(callbacks, releases)
	require.NoError(t, err)
	require.NoError(t, backfiller.BackfillLegacyActiveAuthorityContext(
		context.Background(), leaseUUID, legacySource, items, profiles,
	))
	backfilled, err := releases.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, backfilled)
	require.NoError(t, backfiller.BackfillLegacyRuntimeAuthorityContext(
		context.Background(), leaseUUID, *backfilled, oldAuthority,
	))
	settlement, err := shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	active, sourceClaim, err := settlement.ClaimLatestActive(leaseUUID)
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
	maintenanceID, err := maintenanceid.New()
	require.NoError(t, err)
	request, err := settlement.NewMaintenanceRequestAuthority(
		maintenanceID, shared.MaintenanceIntentRestart, leaseUUID, newURL, nil,
	)
	require.NoError(t, err)
	candidate, err := settlement.NewMaintenanceIntentCandidate(request, sourceClaim, target)
	require.NoError(t, err)
	admission, err := settlement.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	appendClaim, err := settlement.StartMaintenanceAppend(
		createdMaintenanceDispatch(t, admission),
	)
	require.NoError(t, err)
	targetClaim, err := settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	targetClaim, err = settlement.BindMaintenanceIntentTarget(targetClaim)
	require.NoError(t, err)
	bindLeaseSMMaintenanceExecutor(t, settlement)
	execution, err := settlement.StartMaintenanceExecution(targetClaim)
	require.NoError(t, err)
	physical := settlement.ExecuteMaintenance(t.Context(), execution)
	success, ok := physical.(shared.MaintenanceExecutionSuccess)
	require.True(t, ok)
	committed, err := settlement.ActivateMaintenance(success)
	require.NoError(t, err)

	message, _, err := NewMaintenanceRecoveredSuccessMsg(
		committed,
		MaintenanceRecoveryProjection{
			ContainerIDs:      []string{"container-a"},
			ServiceContainers: map[string][]string{"app": {"container-a"}},
		},
	)
	require.NoError(t, err)
	recovered, ok := message.envelope.message.(maintenanceRecoveredMsg)
	require.True(t, ok)
	assert.True(t, recovered.success.applyRecoveredRuntimeAuthority)
	assert.Equal(t, newURL, recovered.success.recoveredCallbackURL)
	assert.Equal(t, newURL, recovered.success.recoveredLifecycleCallbackURL)

	state := &ProvisionState{}
	require.NotNil(t, recovered.success.release)
	applyReplaceReleaseAuthority(state, recovered.success)
	assert.Equal(t, "tenant-a", state.Tenant)
	assert.Equal(t, providerUUID, state.ProviderUUID)
	assert.Equal(t, items, state.Items)
	assert.Equal(t, profiles, state.ResourceProfiles)
	require.NotNil(t, state.StackManifest)
}
