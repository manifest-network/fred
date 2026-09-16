package docker

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestRecoveryDeadlinesBoundRepeatedFutureObservations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var deadlines recoveryDeadlines[string]
		first := time.Now()
		admitted := first.Add(24 * time.Hour)
		timeout := 10 * time.Minute
		deadline := deadlines.observe("attempt-a", admitted, first, timeout)
		require.Equal(t, first.Add(timeout), deadline)
		for range 3 {
			time.Sleep(timeout / 2)
			now := time.Now()
			require.Equal(t, deadline, deadlines.observe("attempt-a", admitted, now, timeout))
		}
		require.False(t, time.Now().Before(deadline), "periodic observations must spend the original runtime window")

		// Neither a different attempt nor a process restart inherits an older
		// attempt's already-expired window. Future wall time remains uncertain.
		now := time.Now()
		require.Equal(t, now.Add(timeout), deadlines.observe("attempt-b", admitted, now, timeout))
		var restarted recoveryDeadlines[string]
		require.Equal(t, now.Add(timeout), restarted.observe("attempt-a", admitted, now, timeout))
	})
}

func TestRecoveryDeadlinesPreserveElapsedBudgetAndOnlyShorten(t *testing.T) {
	var deadlines recoveryDeadlines[string]
	now := time.Now()
	admitted := now.Add(-4 * time.Minute)
	deadline := deadlines.observe("attempt", admitted, now, 10*time.Minute)
	require.Equal(t, now.Add(6*time.Minute), deadline)
	require.Equal(t, deadline, deadlines.observe("attempt", admitted, now.Add(time.Minute), time.Hour))
	require.Equal(t, now.Add(time.Minute), deadlines.observe("attempt", admitted, now.Add(time.Minute), time.Minute))
}

func TestRecoveryDeadlinesPruneOnlyCompletedSnapshotEntries(t *testing.T) {
	var deadlines recoveryDeadlines[string]
	now := time.Now()
	admitted := now.Add(time.Hour)
	deadline := deadlines.observe("pending", admitted, now, time.Minute)
	deadlines.observe("completed", admitted, now, time.Minute)
	checkpoint := deadlines.checkpoint()
	concurrent := deadlines.observe("concurrent", admitted, now, time.Minute)
	deadlines.retainPending(checkpoint, map[string]struct{}{"pending": {}})

	require.Len(t, deadlines.entries, 2)
	require.Equal(t, deadline, deadlines.observe("pending", admitted, now.Add(time.Minute), time.Minute))
	require.Equal(t, concurrent, deadlines.observe("concurrent", admitted, now.Add(time.Minute), time.Minute),
		"an older journal snapshot must not reset a concurrently observed attempt")
	deadlines.retainPending(deadlines.checkpoint(), nil)
	require.Empty(t, deadlines.entries)
}

func TestRecoverOperationIntentBoundsClockRollbackAcrossSweeps(t *testing.T) {
	// Admit outside the synctest bubble. Its earlier clock then reproduces a
	// rollback after durable admission without altering journal rows or adding
	// a production clock injection point.
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	admission, err := beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	claim, created := admission.CreatedClaim()
	require.True(t, created)
	container := dockerIntentContainer(spec, "clock-rollback-container", spec.Items[0].SKU, 0)
	container.Status = "created"
	synctest.Test(t, func(t *testing.T) {
		b := newOperationIntentRecoveryBackend(t, store, storageID,
			[]ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID))
		b.cfg.ProvisionTimeout = 10 * time.Minute
		require.True(t, claim.CreatedAt().Round(0).After(time.Now()))
		require.NoError(t, b.recoverOperationIntents(context.Background()))
		for range 2 {
			time.Sleep(4 * time.Minute)
			require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
			pending, err := b.operationSettlement.ListOperationIntents()
			require.NoError(t, err)
			require.Len(t, pending, 1, "preserve late visibility before the original deadline")
		}
		time.Sleep(2 * time.Minute)
		require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
		pending, err := b.operationSettlement.ListOperationIntents()
		require.NoError(t, err)
		require.Empty(t, pending)
		callbacks, err := store.ListPending()
		require.NoError(t, err)
		require.Len(t, callbacks, 1)
		assert.Equal(t, backend.CallbackStatusFailed, callbacks[0].Status)
		require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
		require.Empty(t, b.operationRecoveryDeadlines.entries, "terminal attempts must not accumulate clock state")
	})
}

func TestRecoverMaintenanceBoundsClockRollbackAcrossSweeps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newMaintenanceRecoveryHarness(t)
		h.appendTarget(true)
		_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
		require.NoError(t, err)
		h.b.cfg.ProvisionTimeout = 10 * time.Minute
		source := h.containersFor(h.source, 2, "running", HealthStatusNone)
		target := h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)
		h.inventory.containers = append(source, target...)
		// Reopen a stopped journal whose admission predates a wall-clock
		// rollback. Keep diagnostics and their cleanup inside this bubble.
		require.NoError(t, h.callbacks.Close())
		futureMaintenanceAdmissionForTest(t, h.callbackPath, h.leaseUUID, time.Now().Add(24*time.Hour))
		h.reopen()
		require.NoError(t, h.b.recoverMaintenanceIntents(context.Background()))
		for range 2 {
			time.Sleep(4 * time.Minute)
			require.NoError(t, h.b.recoverMaintenanceIntents(context.Background()))
			pending, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			require.Empty(t, h.inventory.removed)
		}
		time.Sleep(2 * time.Minute)
		require.NoError(t, h.b.recoverMaintenanceIntents(context.Background()))
		h.assertSettled(backend.CallbackStatusFailed)
		assert.Equal(t, []string{target[0].ContainerID}, h.inventory.removed)
		require.NoError(t, h.b.recoverMaintenanceIntents(context.Background()))
		require.Empty(t, h.b.maintenanceRecoveryDeadlines.entries)
	})
}

func futureMaintenanceAdmissionForTest(t *testing.T, path, leaseUUID string, admittedAt time.Time) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("callback_lease_mutation_heads"))
		require.NotNil(t, bucket)
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(bucket.Get([]byte(leaseUUID)), &envelope); err != nil {
			return err
		}
		var maintenance map[string]json.RawMessage
		if err := json.Unmarshal(envelope["maintenance"], &maintenance); err != nil {
			return err
		}
		maintenance["created_at"], err = json.Marshal(admittedAt)
		if err != nil {
			return err
		}
		envelope["maintenance"], err = json.Marshal(maintenance)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(envelope)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(leaseUUID), encoded)
	}))
}
