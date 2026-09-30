package shared

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

// TestReleaseCompactionDropsAFailedMaintenanceTarget records why the listing
// must tolerate a missing target: over its byte limit, compaction protects
// only the index-latest and latest-active rows, so a tenant's later large
// updates can drop the target row of their failed, effect-started update.
func TestReleaseCompactionDropsAFailedMaintenanceTarget(t *testing.T) {
	maintenanceID, err := maintenanceid.New()
	require.NoError(t, err)
	now := time.Now()
	history := []Release{
		{Version: 1, Image: "old", Status: "superseded", CreatedAt: now.Add(-3 * time.Hour)},
		{Version: 2, Image: "failed-target", Status: "failed", MaintenanceID: maintenanceID,
			CreatedAt: now.Add(-2 * time.Hour), Error: strings.Repeat("x", 1024)},
		{Version: 3, Image: "current", Status: "active", CreatedAt: now.Add(-time.Hour)},
	}
	// Later large updates leave room for only the protected rows, as a lease
	// whose history keeps hitting the byte limit eventually does.
	protectedOnly, err := marshalReleaseHistory(history[2:])
	require.NoError(t, err)
	compacted, removed, err := compactReleaseHistoryWithinLimit(history, time.Time{}, len(protectedOnly))
	require.NoError(t, err)
	require.Positive(t, removed)
	var versions []int
	for _, release := range compacted {
		versions = append(versions, release.Version)
	}
	assert.NotContains(t, versions, 2,
		"compaction removes the failed maintenance target row its receipt still names")
}

// TestFailedReceiptMissingItsTargetStaysLeaseLocal pins the containment: a
// failed receipt whose target row compaction dropped is reported as
// unverifiable, with no cleanup authority, instead of failing the backend-wide
// listing that every recovery pass and startup depend on.
func TestFailedReceiptMissingItsTargetStaysLeaseLocal(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement, err := NewMaintenanceSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)

	leaseUUID := uuid.NewString()
	maintenanceID, err := maintenanceid.New()
	require.NoError(t, err)
	now := time.Now().UTC()
	putRawReleaseHistory(t, stores.releases, leaseUUID, []Release{
		{Version: 1, Image: "old", Status: "superseded", CreatedAt: now.Add(-3 * time.Hour)},
		// Version 2, the failed update's target, has been compacted away.
		{Version: 3, Image: "current", Status: "active", CreatedAt: now.Add(-time.Hour)},
	})
	storageID, err := backendidentity.New()
	require.NoError(t, err)
	digest := strings.Repeat("a", 64)
	record := maintenanceCompletionRecord{
		Version: maintenanceCompletionRecordV1, MaintenanceID: maintenanceID,
		Kind: MaintenanceIntentUpdate, LeaseUUID: leaseUUID, RequestDigest: digest,
		CompletionSequence: 1, Backend: "docker-a", BackendStorageID: storageID.String(),
		Tenant: "tenant-a", ProviderUUID: "22222222-2222-4222-8222-222222222222",
		Status: backend.CallbackStatusFailed, Error: "update failed",
		EffectStarted: true, TargetReleaseVersion: 2, TargetReleaseDigest: digest,
		SettledAt: now,
	}
	require.NoError(t, stores.callbacks.update(func(tx *bolt.Tx) error {
		return archiveMaintenanceCompletionTx(tx, record)
	}))

	receipts, unverifiable, err := settlement.ListFailedMaintenanceReceipts()
	require.NoError(t, err, "one lease's compacted target must not fail the listing for every lease")
	assert.Empty(t, receipts, "an unverifiable receipt grants no cleanup authority")
	require.Len(t, unverifiable, 1)
	assert.Equal(t, leaseUUID, unverifiable[0].LeaseUUID)
	assert.Equal(t, maintenanceID, unverifiable[0].MaintenanceID)
	assert.ErrorContains(t, unverifiable[0].Cause, "failed maintenance receipt target release is missing")
}
