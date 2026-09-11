package docker

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateRetentionMetrics(t *testing.T) {
	updateRetentionMetrics(2048, 3, 512, 1, 0) // 2048 MB admission total, 3 active, 512 MB reaping (1 record), 0 partitions
	assert.Equal(t, float64(2048)*bytesPerMiB, testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(3), testutil.ToFloat64(retainedLeases))
	assert.Equal(t, float64(512)*bytesPerMiB, testutil.ToFloat64(retentionReapingBytes))
	assert.Equal(t, float64(1), testutil.ToFloat64(retentionReapingLeases))
}

func TestSetStaticPoolMetrics(t *testing.T) {
	cfg := Config{TotalDiskMB: 100, MaxRetainedDiskMB: 40}
	setStaticPoolMetrics(cfg)
	assert.Equal(t, float64(100)*bytesPerMiB, testutil.ToFloat64(diskPoolBytes))
	assert.Equal(t, float64(40)*bytesPerMiB, testutil.ToFloat64(retainedDiskCapBytes))
}

func TestSetStaticPoolMetrics_ResetsCapWhenUnset(t *testing.T) {
	setStaticPoolMetrics(Config{TotalDiskMB: 100, MaxRetainedDiskMB: 40})
	require.Equal(t, float64(40)*bytesPerMiB, testutil.ToFloat64(retainedDiskCapBytes))
	// A later construction with no cap must reset the gauge to 0 (no stale value).
	setStaticPoolMetrics(Config{TotalDiskMB: 100, MaxRetainedDiskMB: 0})
	assert.Equal(t, float64(0), testutil.ToFloat64(retainedDiskCapBytes))
}

func TestSampleCloseIntentMetrics_ReportsAggregateCountAndOldestAge(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	claim := beginCloseRecoveryIntent(t, b, stores, true, "")
	defer closeCloseRecoveryBackend(t, b, stores)

	b.sampleCloseIntentMetrics(claim.CreatedAt().Add(2 * time.Minute))
	assert.Equal(t, float64(1), testutil.ToFloat64(pendingCloseIntents))
	assert.InDelta(t, 120, testutil.ToFloat64(oldestCloseIntentAgeSeconds), 0.1)

	completeDestroyedCloseForTest(t, b, stores.close, claim)
	b.sampleCloseIntentMetrics(time.Now())
	assert.Equal(t, float64(0), testutil.ToFloat64(pendingCloseIntents))
	assert.Equal(t, float64(0), testutil.ToFloat64(oldestCloseIntentAgeSeconds))
}

func TestSampleLeaseMutationCapacityMetricsIncludesPermanentClosedUUID(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	defer closeCloseRecoveryBackend(t, b, stores)

	b.sampleLeaseMutationCapacityMetrics()
	capacity, err := stores.callbacks.LeaseMutationUUIDCapacity()
	require.NoError(t, err)
	receiptCapacity, err := stores.callbacks.CallbackReceiptCapacity()
	require.NoError(t, err)
	assert.Zero(t, capacity.Reserved)
	assert.Equal(t, float64(capacity.Limit), testutil.ToFloat64(leaseMutationUUIDSlotLimit))
	assert.Zero(t, testutil.ToFloat64(leaseMutationUUIDSlots))
	assert.Zero(t, receiptCapacity.Reserved)
	assert.Equal(t, float64(receiptCapacity.Limit), testutil.ToFloat64(callbackReceiptReservationLimit))
	assert.Zero(t, testutil.ToFloat64(callbackReceiptReservations))

	claim := beginCloseRecoveryIntent(t, b, stores, true, "")
	completeDestroyedCloseForTest(t, b, stores.close, claim)
	b.sampleLeaseMutationCapacityMetrics()

	assert.Equal(t, float64(1), testutil.ToFloat64(leaseMutationUUIDSlots),
		"a completed close permanently consumes its pre-reserved UUID slot")
	assert.Equal(t, float64(capacity.Limit), testutil.ToFloat64(leaseMutationUUIDSlotLimit))
	assert.Zero(t, testutil.ToFloat64(callbackReceiptReservations),
		"the closed-UUID fence reclaims its superseded receipt reservations")
	assert.Equal(t, float64(receiptCapacity.Limit), testutil.ToFloat64(callbackReceiptReservationLimit))
}

func TestSampleLeaseMutationCapacityMetricsIncludesCallbackReceiptReservations(t *testing.T) {
	b, stores := openCloseRecoveryBackend(t, t.TempDir(), &mockDockerClient{}, nil)
	defer closeCloseRecoveryBackend(t, b, stores)

	_, err := beginBoundDockerTestOperationIntent(
		t, stores.callbacks, dockerOperationIntentSpec(t, b.storageIdentity),
	)
	require.NoError(t, err)
	b.sampleLeaseMutationCapacityMetrics()

	capacity, err := stores.callbacks.CallbackReceiptCapacity()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), capacity.Reserved)
	assert.Equal(t, float64(capacity.Reserved), testutil.ToFloat64(callbackReceiptReservations))
	assert.Equal(t, float64(capacity.Limit), testutil.ToFloat64(callbackReceiptReservationLimit))
}

func TestSampleLeaseMutationCapacityMetricsPreservesLastGoodValuesOnReadFailure(t *testing.T) {
	b, stores := openCloseRecoveryBackend(t, t.TempDir(), &mockDockerClient{}, nil)
	b.sampleLeaseMutationCapacityMetrics()
	wantSlots := testutil.ToFloat64(leaseMutationUUIDSlots)
	wantLimit := testutil.ToFloat64(leaseMutationUUIDSlotLimit)
	wantReceipts := testutil.ToFloat64(callbackReceiptReservations)
	wantReceiptLimit := testutil.ToFloat64(callbackReceiptReservationLimit)
	errorsBefore := testutil.ToFloat64(callbackStoreErrorsTotal)

	require.NoError(t, stores.callbacks.Close())
	b.sampleLeaseMutationCapacityMetrics()

	assert.Equal(t, wantSlots, testutil.ToFloat64(leaseMutationUUIDSlots))
	assert.Equal(t, wantLimit, testutil.ToFloat64(leaseMutationUUIDSlotLimit))
	assert.Equal(t, wantReceipts, testutil.ToFloat64(callbackReceiptReservations))
	assert.Equal(t, wantReceiptLimit, testutil.ToFloat64(callbackReceiptReservationLimit))
	assert.Equal(t, errorsBefore+1, testutil.ToFloat64(callbackStoreErrorsTotal))
	b.stopCancel()
	require.NoError(t, stores.releases.Close())
}
