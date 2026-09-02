package shared

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

const releaseRuntimeAuthorityOperationID = OperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")

func validReleaseRuntimeAuthority() *ReleaseRuntimeAuthority {
	authority, err := NewReleaseRuntimeAuthority(
		releaseRuntimeAuthorityOperationID,
		"tenant-a",
		"22222222-2222-4222-8222-222222222222",
		"https://fred.example/callbacks/provision?operation_id="+releaseRuntimeAuthorityOperationID.String(),
		"https://fred.example/callbacks/provision?lifecycle_id="+releaseRuntimeAuthorityOperationID.String(),
	)
	if err != nil {
		panic(err)
	}
	return &authority
}

func validRuntimeAuthorityRelease() Release {
	items := []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}}
	return Release{
		Manifest:         []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:            "stack",
		OperationID:      releaseRuntimeAuthorityOperationID,
		Items:            items,
		ResourceProfiles: []SKUResourceSnapshot{{SKU: "sku-a", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}},
		RuntimeAuthority: validReleaseRuntimeAuthority(),
		Status:           "active",
		CreatedAt:        time.Now(),
	}
}

func TestReleaseStoreRuntimeAuthorityIsCompleteAndDeepCloned(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: t.TempDir() + "/releases.db"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	release := validRuntimeAuthorityRelease()
	require.NoError(t, store.AppendActive(leaseUUID, release))

	release.RuntimeAuthority.tenant = "mutated-caller"
	first, err := store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, "tenant-a", first.RuntimeAuthority.Tenant())
	assert.NotSame(t, release.RuntimeAuthority, first.RuntimeAuthority)
	first.RuntimeAuthority.tenant = "mutated-reader"
	second, err := store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.Equal(t, "tenant-a", second.RuntimeAuthority.Tenant())
	assert.NotSame(t, first.RuntimeAuthority, second.RuntimeAuthority)
}

func TestReleaseStoreRejectsPartialOrMismatchedRuntimeAuthority(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Release)
	}{
		{name: "typed release without authority", mutate: func(r *Release) { r.RuntimeAuthority = nil }},
		{name: "legacy release with authority", mutate: func(r *Release) { r.OperationID = "" }},
		{name: "different authority token", mutate: func(r *Release) {
			r.RuntimeAuthority.operationID = "9a72fbc1-38c8-4f31-87f7-f689979b9324"
		}},
		{name: "invalid authority value", mutate: func(r *Release) {
			r.RuntimeAuthority = &ReleaseRuntimeAuthority{}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: t.TempDir() + "/releases.db"})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			release := validRuntimeAuthorityRelease()
			tt.mutate(&release)
			require.Error(t, store.AppendActive("550e8400-e29b-41d4-a716-446655440000", release))
		})
	}
}

func TestNewReleaseRuntimeAuthorityRejectsIncompleteOrDivergentInput(t *testing.T) {
	validCallback := "https://fred.example/callbacks/provision?operation_id=" + releaseRuntimeAuthorityOperationID.String()
	validLifecycle := "https://fred.example/callbacks/provision?lifecycle_id=" + releaseRuntimeAuthorityOperationID.String()
	for _, tt := range []struct {
		name                             string
		operationID                      OperationID
		tenant, provider, callback, life string
	}{
		{name: "invalid operation ID", tenant: "tenant-a", provider: "22222222-2222-4222-8222-222222222222", callback: validCallback, life: validLifecycle},
		{name: "missing tenant", operationID: releaseRuntimeAuthorityOperationID, provider: "22222222-2222-4222-8222-222222222222", callback: validCallback, life: validLifecycle},
		{name: "missing provider", operationID: releaseRuntimeAuthorityOperationID, tenant: "tenant-a", callback: validCallback, life: validLifecycle},
		{name: "noncanonical provider", operationID: releaseRuntimeAuthorityOperationID, tenant: "tenant-a", provider: "provider-a", callback: validCallback, life: validLifecycle},
		{name: "different callback token", operationID: releaseRuntimeAuthorityOperationID, tenant: "tenant-a", provider: "22222222-2222-4222-8222-222222222222", callback: "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324", life: validLifecycle},
		{name: "derived lifecycle", operationID: releaseRuntimeAuthorityOperationID, tenant: "tenant-a", provider: "22222222-2222-4222-8222-222222222222", callback: validCallback},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReleaseRuntimeAuthority(
				tt.operationID, tt.tenant, tt.provider, tt.callback, tt.life,
			)
			require.Error(t, err)
		})
	}
}

func TestReleaseHistoryWriteCeilingRollsBackAppend(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: t.TempDir() + "/releases.db"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	require.NoError(t, store.AppendActive(leaseUUID, validRuntimeAuthorityRelease()))
	before, err := store.List(leaseUUID)
	require.NoError(t, err)

	oversized := validRuntimeAuthorityRelease()
	oversized.Status = "failed"
	oversized.Error = strings.Repeat("x", maxAuthoritativeRecordBytes)
	err = store.Append(leaseUUID, oversized)
	require.ErrorIs(t, err, ErrReleaseHistoryCapacity)
	after, readErr := store.List(leaseUUID)
	require.NoError(t, readErr)
	assert.Equal(t, before, after, "oversized write must roll its bbolt transaction back")
}
