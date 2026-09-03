package docker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// Once an XFS delete-stage survives a failed Destroy, the final managed name
// may already be absent. The Backend-lifetime latch must prevent a subsequent
// reaper inventory from reading that absence as completion and deleting the
// record that still accounts for the pending project quota/open inode.
func TestVolumeRecoveryPendingPreventsHiddenDeleteStageReaperBypass(t *testing.T) {
	t.Parallel()

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440321"
	name := retainedName(canonicalVolumeName(leaseUUID, "app", 0))
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID: leaseUUID,
		Tenant:            "tenant-a",
		Status:            shared.RetentionStatusReaping,
		Items: []backend.LeaseItem{{
			SKU: "docker-small", ServiceName: "app", Quantity: 1,
		}},
		RetainedVolumeNames: []string{name},
		CreatedAt:           time.Now(),
	}))

	var listCalls atomic.Int32
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if listCalls.Add(1) == 1 {
				return []string{name}, nil
			}
			// Models the final namespace being gone while the dot-prefixed typed
			// XFS delete-stage remains intentionally hidden from ordinary List.
			return nil, nil
		},
		DestroyFn: func(context.Context, string) error {
			return ErrVolumeMutationRecoveryPending
		},
	}

	assert.False(t, b.destroyReapingVolumes(t.Context(), b.newManagedVolumeIndex(), leaseUUID))
	require.ErrorIs(t, b.terminalStorageAuthorityError(), ErrVolumeMutationRecoveryPending)
	require.ErrorIs(t, b.terminalStorageAuthorityError(), backendidentity.ErrMutationOutcomeAmbiguous)
	record, err := rs.Get(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, shared.RetentionStatusReaping, record.Status)

	assert.False(t, b.destroyReapingVolumes(t.Context(), b.newManagedVolumeIndex(), leaseUUID),
		"a stopped Backend must not re-infer completion from a now-empty visible namespace")
	assert.Equal(t, int32(1), listCalls.Load(), "the terminal gate must run before the second inventory")
	record, err = rs.Get(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, record, "the restart recovery vehicle/accounting record must survive")
}

func TestVolumeRecoveryPendingPreservesCloseFinalizersAndRejectsLiveRetry(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseDeprovisionLease(t, b, stores)
	var destroyCalls atomic.Int32
	b.volumes = &mockVolumeManager{DestroyFn: func(context.Context, string) error {
		destroyCalls.Add(1)
		return ErrVolumeMutationRecoveryPending
	}}

	err := b.doDeprovision(t.Context(), closeDeprovisionLeaseUUID)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	claim, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.True(t, found, "the exact close recovery capability must survive")
	assert.Zero(t, claim.CleanupAttempts(), "a recovery-required outcome is not an exhaustible ordinary retry")
	releases, readErr := stores.releases.List(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.NotEmpty(t, releases, "the active release must survive until fresh-process recovery")
	b.provisionsMu.RLock()
	projected := b.provisions[closeDeprovisionLeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projected)
	assert.Equal(t, backend.ProvisionStatusFailed, projected.Status)

	err = b.Deprovision(t.Context(), closeDeprovisionLeaseUUID)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending,
		"the stopped instance must reject a retry before it can infer from the partial namespace")
	assert.Equal(t, int32(1), destroyCalls.Load())

	closeCloseRecoveryBackend(t, b, stores)
}
