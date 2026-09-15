package docker

import (
	"context"
	"errors"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func TestUnaccountedManagedVolumesIncludeEveryReadProjection(t *testing.T) {
	b, retentions := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = t.TempDir()
	const liveUUID = "00000000-0000-4000-8000-000000000701"
	liveName := canonicalVolumeName(liveUUID, "app", 0)
	b.provisions[liveUUID] = &provision{ProvisionState: leasesm.ProvisionState{
		Items: []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 1}},
	}}
	managed := []string{
		liveName,
		canonicalVolumeName(liveUUID, "app", 1), // outside the live quantity
		retainedName(liveName),                  // a live row covers only canonical storage
		canonicalVolumeName("00000000-0000-4000-8000-000000000799", "app", 0),
	}
	for index, status := range []string{
		shared.RetentionStatusActive, shared.RetentionStatusReaping, shared.RetentionStatusRestoring,
	} {
		entry := retentionEntryFixture("volume-observer-"+status, "tenant-a", time.Now())
		entry.Status = status
		entry.NewLeaseUUID = "00000000-0000-4000-8000-000000000705"
		if status != shared.RetentionStatusRestoring {
			entry.NewLeaseUUID = ""
		}
		require.NoError(t, putRetentionForTest(t, retentions, entry))
		managed = append(managed, entry.RetainedVolumeNames...)
		managed = append(managed, canonicalFromRetained(entry.RetainedVolumeNames[0]))
		if index == 0 {
			managed = append(managed, retainedName(canonicalVolumeName(entry.OriginalLeaseUUID, "web", 1)))
		}
		if status == shared.RetentionStatusRestoring {
			managed = append(managed,
				retainedToNewCanonical(entry.RetainedVolumeNames[0], entry.OriginalLeaseUUID, entry.NewLeaseUUID),
				canonicalVolumeName(entry.NewLeaseUUID, "web", 1),
			)
		}
	}
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	spec.ProviderUUID = nominalDockerProviderUUID
	candidate, err := operations.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	admitted, err := operations.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, created := admitted.CreatedClaim()
	require.True(t, created)
	managed = append(managed, canonicalVolumeName(claim.LeaseUUID(), "app", 0))
	b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) { return managed, nil }}

	b.observeUnaccountedManagedVolumes(t.Context())
	assert.Equal(t, 4.0, promtestutil.ToFloat64(unaccountedManagedVolumes),
		"only unknown lease, extra replica, extra retained volume, and unexplained retained live alias are unaccounted")
	intents, err := operations.ListOperationIntents()
	require.NoError(t, err)
	assert.Contains(t, intents, claim, "observation cannot consume admitted operation authority")
}

func TestUnaccountedManagedVolumeObservationFailurePreservesLastCount(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = t.TempDir()
	volume := canonicalVolumeName("00000000-0000-4000-8000-000000000799", "app", 0)
	manager := &mockVolumeManager{ListFn: func() ([]string, error) { return []string{volume}, nil }}
	b.volumes = manager
	b.observeUnaccountedManagedVolumes(t.Context())
	require.Equal(t, 1.0, promtestutil.ToFloat64(unaccountedManagedVolumes))
	before := promtestutil.ToFloat64(unaccountedManagedVolumeObservationFailuresTotal)
	manager.ListFn = func() ([]string, error) { return nil, errors.New("inventory unavailable") }
	b.observeUnaccountedManagedVolumes(t.Context())
	assert.Equal(t, 1.0, promtestutil.ToFloat64(unaccountedManagedVolumes))
	assert.Equal(t, before+1, promtestutil.ToFloat64(unaccountedManagedVolumeObservationFailuresTotal))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	b.observeUnaccountedManagedVolumes(ctx)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(unaccountedManagedVolumes))
	assert.Equal(t, before+2, promtestutil.ToFloat64(unaccountedManagedVolumeObservationFailuresTotal))

	b.cfg.StorageAttestationTimeout = time.Second
	manager.ListForProofFn = func(ctx context.Context) ([]string, error) {
		deadline, bounded := ctx.Deadline()
		require.True(t, bounded, "the diagnostic pass must have its own storage-attestation budget")
		assert.LessOrEqual(t, time.Until(deadline), b.cfg.StorageAttestationTimeout)
		return nil, nil
	}
	b.observeUnaccountedManagedVolumes(t.Context())
	assert.Zero(t, promtestutil.ToFloat64(unaccountedManagedVolumes),
		"a successful empty observation replaces the old footprint")
}
