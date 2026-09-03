package shared

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func legacyMaintenanceRelease(t *testing.T, callbackBase string) Release {
	t.Helper()
	callbackURL := callbackBase + "/callbacks/provision"
	authority, err := NewLegacyRuntimeAuthority(
		"tenant-a",
		"22222222-2222-4222-8222-222222222222",
		callbackURL,
		callbackURL,
	)
	require.NoError(t, err)
	return Release{
		Manifest:               []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:                  "stack",
		Items:                  []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}},
		ResourceProfiles:       []SKUResourceSnapshot{{SKU: "sku-a", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}},
		LegacyRuntimeAuthority: &authority,
		Status:                 "active",
		CreatedAt:              time.Now().Add(-time.Minute),
	}
}

func TestLegacyMaintenanceJournalPreservesDisjointRuntimeAuthority(t *testing.T) {
	dir := t.TempDir()
	releases, err := NewReleaseStore(ReleaseStoreConfig{DBPath: filepath.Join(dir, "releases.db")})
	require.NoError(t, err)
	callbacks, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(dir, "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})

	leaseUUID := testLeaseUUID("legacy-maintenance")
	require.NoError(t, releases.AppendActive(
		leaseUUID,
		legacyMaintenanceRelease(t, "https://old.example"),
	))
	active, source, err := releases.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	rotated, err := NewLegacyRuntimeAuthority(
		active.LegacyRuntimeAuthority.Tenant(),
		active.LegacyRuntimeAuthority.ProviderUUID(),
		"https://new.example/callbacks/provision",
		"https://new.example/callbacks/provision",
	)
	require.NoError(t, err)
	target.LegacyRuntimeAuthority = &rotated

	admission, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
		Kind:             MaintenanceIntentRestart,
		SourceRelease:    source,
		TargetRelease:    target,
		Backend:          "docker-a",
		BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	})
	require.NoError(t, err)
	require.True(t, admission.MaintenanceID().Valid())
	assert.Empty(t, admission.TargetRelease().OperationID)
	assert.Nil(t, admission.TargetRelease().RuntimeAuthority)
	require.NotNil(t, admission.TargetRelease().LegacyRuntimeAuthority)
	assert.Equal(t, ReleaseAuthorityLegacy, mustRuntimeIdentity(t, admission.TargetRelease()).Class())
	assert.Equal(t, "tenant-a", admission.intent.Tenant())
	assert.Equal(t, rotated.LifecycleCallbackURL(), admission.intent.LifecycleCallbackURL())

	require.NoError(t, releases.CheckAppendMaintenanceCapacity(admission))
	appendClaim, err := callbacks.StartMaintenanceAppend(admission)
	require.NoError(t, err)
	targetClaim, err := releases.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	intent, err := callbacks.BindMaintenanceIntentTarget(appendClaim.Intent(), targetClaim)
	require.NoError(t, err)
	require.NoError(t, releases.ActivateMaintenance(targetClaim))
	completion, err := callbacks.ResolveMaintenanceIntent(intent, backend.CallbackStatusSuccess, "")
	require.NoError(t, err)
	assert.Equal(t, rotated.LifecycleCallbackURL(), completion.CallbackURL)

	committed, err := releases.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, committed)
	assert.Empty(t, committed.OperationID)
	assert.Nil(t, committed.RuntimeAuthority)
	require.NotNil(t, committed.LegacyRuntimeAuthority)
	assert.Equal(t, rotated, *committed.LegacyRuntimeAuthority)
}

func TestMaintenanceAppendRejectsRuntimeAuthorityClassOrPrincipalChange(t *testing.T) {
	for _, test := range []struct {
		name   string
		source Release
		target Release
	}{
		{
			name:   "legacy to typed",
			source: legacyMaintenanceRelease(t, "https://old.example"),
			target: validRuntimeAuthorityRelease(),
		},
		{
			name:   "typed to legacy",
			source: validRuntimeAuthorityRelease(),
			target: legacyMaintenanceRelease(t, "https://new.example"),
		},
		{
			name:   "legacy principal changes",
			source: legacyMaintenanceRelease(t, "https://old.example"),
			target: func() Release {
				release := legacyMaintenanceRelease(t, "https://new.example")
				authority, err := NewLegacyRuntimeAuthority(
					"tenant-b",
					release.LegacyRuntimeAuthority.ProviderUUID(),
					release.LegacyRuntimeAuthority.CallbackURL(),
					release.LegacyRuntimeAuthority.LifecycleCallbackURL(),
				)
				require.NoError(t, err)
				release.LegacyRuntimeAuthority = &authority
				return release
			}(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			releases, err := NewReleaseStore(ReleaseStoreConfig{DBPath: filepath.Join(dir, "releases.db")})
			require.NoError(t, err)
			callbacks, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(dir, "callbacks.db")})
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, callbacks.Close())
				require.NoError(t, releases.Close())
			})

			leaseUUID := testLeaseUUID("maintenance-authority-" + test.name)
			require.NoError(t, releases.AppendActive(leaseUUID, test.source))
			_, source, err := releases.ClaimLatestActive(leaseUUID)
			require.NoError(t, err)
			target := cloneRelease(test.target)
			target.Version = 0
			target.Status = "deploying"
			target.CreatedAt = time.Now()
			admission, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
				Kind:             MaintenanceIntentUpdate,
				SourceRelease:    source,
				TargetRelease:    target,
				Backend:          "docker-a",
				BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
			})
			require.NoError(t, err)
			require.Error(t, releases.CheckAppendMaintenanceCapacity(admission))
		})
	}
}

func mustRuntimeIdentity(t *testing.T, release Release) ReleaseRuntimeIdentity {
	t.Helper()
	identity, ok := release.RuntimeIdentity()
	require.True(t, ok)
	return identity
}
