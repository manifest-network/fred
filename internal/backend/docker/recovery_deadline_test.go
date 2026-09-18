package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestRecoveryDeadlinesBoundRepeatedFutureObservations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var deadlines recoveryDeadlines[string]
		first := time.Now()
		admitted := first.Add(24 * time.Hour)
		timeout := 10 * time.Minute
		deadline, _ := deadlines.observeFirst("attempt-a", admitted, first, timeout)
		require.Equal(t, first.Add(timeout), deadline)
		for range 3 {
			time.Sleep(timeout / 2)
			now := time.Now()
			observed1, _ := deadlines.observeFirst("attempt-a", admitted, now, timeout)
			require.Equal(t, deadline, observed1)
		}
		require.False(t, time.Now().Before(deadline), "periodic observations must spend the original runtime window")

		// Neither a different attempt nor a process restart inherits an older
		// attempt's already-expired window. Future wall time remains uncertain.
		now := time.Now()
		observed2, _ := deadlines.observeFirst("attempt-b", admitted, now, timeout)
		require.Equal(t, now.Add(timeout), observed2)
		var restarted recoveryDeadlines[string]
		observed3, _ := restarted.observeFirst("attempt-a", admitted, now, timeout)
		require.Equal(t, now.Add(timeout), observed3)
	})
}

func TestRecoveryDeadlinesPreserveElapsedBudgetAndOnlyShorten(t *testing.T) {
	var deadlines recoveryDeadlines[string]
	now := time.Now()
	admitted := now.Add(-4 * time.Minute)
	deadline, _ := deadlines.observeFirst("attempt", admitted, now, 10*time.Minute)
	require.Equal(t, now.Add(6*time.Minute), deadline)
	observed4, _ := deadlines.observeFirst("attempt", admitted, now.Add(time.Minute), time.Hour)
	require.Equal(t, deadline, observed4)
	observed5, _ := deadlines.observeFirst("attempt", admitted, now.Add(time.Minute), time.Minute)
	require.Equal(t, now.Add(time.Minute), observed5)
}

func TestRecoveryDeadlinesPruneOnlyCompletedSnapshotEntries(t *testing.T) {
	var deadlines recoveryDeadlines[string]
	now := time.Now()
	admitted := now.Add(time.Hour)
	deadline, _ := deadlines.observeFirst("pending", admitted, now, time.Minute)
	deadlines.observeFirst("completed", admitted, now, time.Minute)
	checkpoint := deadlines.checkpoint()
	concurrent, _ := deadlines.observeFirst("concurrent", admitted, now, time.Minute)
	deadlines.retainPending(checkpoint, map[string]struct{}{"pending": {}})

	require.Len(t, deadlines.entries, 2)
	observed6, _ := deadlines.observeFirst("pending", admitted, now.Add(time.Minute), time.Minute)
	require.Equal(t, deadline, observed6)
	observed7, _ := deadlines.observeFirst("concurrent", admitted, now.Add(time.Minute), time.Minute)
	require.Equal(t, concurrent, observed7,
		"an older journal snapshot must not reset a concurrently observed attempt")
	deadlines.retainPending(deadlines.checkpoint(), nil)
	require.Empty(t, deadlines.entries)
}

func TestMaintenanceReadinessDeadlinesIsolateAttemptsAndContainers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var deadlines recoveryDeadlines[maintenanceReadinessKey]
		first := maintenanceIntentKey{leaseUUID: "lease", maintenanceID: mustParseMaintenanceID(t, "ed1d4da8-3498-4513-bbf5-d6e41bb4a2b9")}
		next := maintenanceIntentKey{leaseUUID: "lease", maintenanceID: mustParseMaintenanceID(t, "6698efb9-73db-40c4-a70b-66d5fb99b48c")}
		initialKey := maintenanceReadinessKey{intent: first, containerID: "source"}
		now := time.Now()
		createdAt := now.Add(time.Hour)
		deadline, _ := deadlines.observeFirst(initialKey, createdAt, now, 5*time.Second)
		time.Sleep(5 * time.Second)
		observed8, _ := deadlines.observeFirst(initialKey, createdAt, time.Now(), 5*time.Second)
		require.False(t, time.Now().Before(observed8))
		replacementKey := maintenanceReadinessKey{intent: first, containerID: "replacement"}
		successorKey := maintenanceReadinessKey{intent: next, containerID: "source"}
		for _, key := range []maintenanceReadinessKey{replacementKey, successorKey} {
			observed9, _ := deadlines.observeFirst(key, createdAt, time.Now(), 5*time.Second)
			require.Equal(t, deadline.Add(5*time.Second), observed9)
		}
		checkpoint := deadlines.checkpoint()
		concurrentKey := maintenanceReadinessKey{intent: next, containerID: "concurrent"}
		deadlines.observeFirst(concurrentKey, createdAt, time.Now(), 5*time.Second)
		deadlines.retainMatching(checkpoint, func(key maintenanceReadinessKey) bool { return key.intent == first })
		require.Len(t, deadlines.entries, 3)
		require.Contains(t, deadlines.entries, initialKey)
		require.Contains(t, deadlines.entries, replacementKey)
		require.Contains(t, deadlines.entries, concurrentKey, "an older snapshot must preserve an observation created during inventory")
		require.NotContains(t, deadlines.entries, successorKey, "retention matches the exact attempt, not only its lease")
	})
}

func TestMaintenanceReadinessDeadlineSurvivesForwardClockCorrection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var deadlines recoveryDeadlines[string]
		first := time.Now()
		deadline, _ := deadlines.observeDeadline("container", first.Add(5*time.Second), false)
		time.Sleep(time.Second)
		// A corrected wall clock can make the next age calculation report an
		// already-old container, yielding a candidate of now. The first
		// monotonic minimum-age window must still run to completion.
		observed, fresh := deadlines.observeDeadline("container", time.Now(), false)
		require.False(t, fresh)
		require.Equal(t, deadline, observed)
		require.True(t, time.Now().Before(observed))
		time.Sleep(4 * time.Second)
		observed, _ = deadlines.observeDeadline("container", time.Now().Add(5*time.Second), false)
		require.False(t, time.Now().Before(observed), "a second rollback must not extend the startup wait")
	})
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
		var warnings bytes.Buffer
		b.logger = slog.New(slog.NewTextHandler(&warnings, nil))
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
		require.Equal(t, 1, strings.Count(warnings.String(), "future operation admission opened a bounded recovery window"))
		require.Contains(t, warnings.String(), claim.OperationID().Fingerprint())
		require.NotContains(t, warnings.String(), claim.OperationID().String(), "warnings must not expose a callback capability")
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
		reopened, found, err := h.b.maintenanceSettlement.GetMaintenanceIntent(h.leaseUUID)
		require.NoError(t, err)
		require.True(t, found)
		require.True(t, reopened.CreatedAt().Round(0).After(time.Now()),
			"the reopened journal must reproduce a future admission after wall-clock rollback")
		var warnings bytes.Buffer
		h.b.logger = slog.New(slog.NewTextHandler(&warnings, nil))
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
		require.Equal(t, 1, strings.Count(warnings.String(), "future maintenance admission opened a bounded recovery window"))
		require.Contains(t, warnings.String(), "maintenance_id="+reopened.MaintenanceID().String())
	})
}

func TestOperationPhysicalAbsenceSharesRecoveryClockRollbackDeadline(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	synctest.Test(t, func(t *testing.T) {
		b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)
		claim := startPendingOperationForRecoveryTest(t, b)
		b.cfg.ProvisionTimeout = 10 * time.Minute
		require.True(t, claim.CreatedAt().Round(0).After(time.Now()))
		forbidOperationRecoveryTeardown(t, b)
		require.NoError(t, b.recoverOperationIntents(t.Context()))
		time.Sleep(b.cfg.ProvisionTimeout + time.Minute)

		// Exercise the construction-bound physical classifier directly under
		// freshly minted recovery authority. The sweep must not settle or clean
		// the attempt first: cleanup subjects deliberately bypass this horizon.
		_, err := b.recoveryCoordinator.WithLease(t.Context(), claim.LeaseUUID(), func(scope shared.LeaseRecoveryScope) error {
			outcome, recoverErr := b.operationSettlement.RecoverOperationExecution(t.Context(), scope, claim)
			require.NoError(t, recoverErr)
			require.IsType(t, shared.OperationExecutionFailure{}, outcome,
				"physical absence must spend the window originally opened by the sweep")
			return nil
		})
		require.NoError(t, err)
	})
}

func TestRecoverMaintenanceBoundsContainerStartupAge(t *testing.T) {
	for _, test := range []struct {
		name      string
		createdAt func(time.Time) time.Time
		wait      time.Duration
	}{
		{"future", func(now time.Time) time.Time { return now.Add(time.Hour) }, 5 * time.Second},
		{"unknown", func(time.Time) time.Time { return time.Time{} }, 5 * time.Second},
		{"young", func(now time.Time) time.Time { return now.Add(-2 * time.Second) }, 3 * time.Second},
		{"old", func(now time.Time) time.Time { return now.Add(-time.Minute) }, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newMaintenanceRecoveryHarness(t)
				h.appendTarget(true)
				_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
				require.NoError(t, err)
				h.b.cfg.ProvisionTimeout = time.Minute
				h.b.cfg.StartupVerifyDuration = 5 * time.Second
				h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
				for index := range h.inventory.containers {
					h.inventory.containers[index].CreatedAt = test.createdAt(time.Now()).Round(0)
				}
				h.reopen()
				// Cold startup must preserve a pending age without failing the
				// backend or marking the exact target terminally failed.
				require.NoError(t, h.b.recoverState(t.Context()))
				if test.wait > 0 {
					time.Sleep(test.wait - time.Nanosecond)
					require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
					pending, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
					require.NoError(t, err)
					require.Len(t, pending, 1)
					callbacks, err := h.callbacks.ListPending()
					require.NoError(t, err)
					require.Empty(t, callbacks, "age deferral grants no terminal authority")
					require.Empty(t, h.inventory.removed, "pending readiness before the visibility deadline must preserve the target")
					time.Sleep(time.Nanosecond)
					require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
				}
				h.assertSettled(backend.CallbackStatusSuccess)
				require.Empty(t, h.inventory.removed)
				require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
				require.Empty(t, h.b.maintenanceReadinessDeadlines.entries)
			})
		})
	}
}

func TestMaintenanceFutureSourceAgeRequiresFreshReadinessEvidence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newMaintenanceRecoveryHarness(t)
		h.appendTarget(true)
		_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
		require.NoError(t, err)
		intent, found, err := h.b.maintenanceSettlement.GetMaintenanceIntent(h.leaseUUID)
		require.NoError(t, err)
		require.True(t, found)
		h.b.cfg.StartupVerifyDuration = 5 * time.Second
		h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
		for index := range h.inventory.containers {
			h.inventory.containers[index].CreatedAt = time.Now().Add(time.Hour).Round(0)
		}

		observe := func() shared.MaintenanceExecutionOutcome {
			var outcome shared.MaintenanceExecutionOutcome
			_, err := h.b.recoveryCoordinator.WithLease(t.Context(), h.leaseUUID, func(scope shared.LeaseRecoveryScope) error {
				var recoverErr error
				outcome, recoverErr = h.b.maintenanceSettlement.RecoverMaintenanceExecution(t.Context(), scope, intent)
				return recoverErr
			})
			require.NoError(t, err)
			return outcome
		}
		require.IsType(t, shared.MaintenanceExecutionAmbiguous{}, observe(), "future source age cannot prove restoration")
		time.Sleep(5 * time.Second)
		h.inventory.inspectErr = errors.New("fresh source inspection unavailable")
		require.IsType(t, shared.MaintenanceExecutionAmbiguous{}, observe(), "elapsed age cannot replace a fresh inspection")
		h.inventory.inspectErr = nil
		outcome := observe()
		require.IsType(t, shared.MaintenanceExecutionFailure{}, outcome)
		require.True(t, outcome.(shared.MaintenanceExecutionFailure).SourceRecovered())
		require.Empty(t, h.inventory.removed)
	})
}

func TestRecoverMaintenancePendingReadinessSpendsVisibilityBudget(t *testing.T) {
	for _, mode := range []string{"age", "health", "second physical inspection"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
					"web": {Image: "docker.io/library/nginx:1.27"},
				}}
				if mode != "age" {
					stack.Services["web"].HealthCheck = &manifest.HealthCheckConfig{Test: []string{"CMD", "true"}}
				}
				h := newMaintenanceRecoveryHarnessForAuthorityAtCallbackOptions(
					t, shared.MaintenanceIntentRestart, false, "", false, stack,
				)
				h.appendTarget(true)
				_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
				require.NoError(t, err)
				h.b.cfg.ProvisionTimeout = 3 * time.Second
				h.b.cfg.StartupVerifyDuration = 5 * time.Second
				h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusStarting)
				for index := range h.inventory.containers {
					h.inventory.containers[index].CreatedAt = time.Now().Add(time.Hour).Round(0)
				}
				if mode == "second physical inspection" {
					// The initial target check sees Ready; the separately bound physical
					// classifier sees a starting healthcheck. It must preserve the cause
					// and use the same deadline rather than abort startup or reset it.
					inspections := 0
					h.b.docker.(*mockDockerClient).InspectContainerFn = func(ctx context.Context, id string) (*ContainerInfo, error) {
						container, inspectErr := h.inventory.inspect(ctx, id)
						if inspectErr == nil {
							inspections++
							if inspections <= 2 {
								container.Health = HealthStatusHealthy
							}
						}
						return container, inspectErr
					}
				}
				require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
				time.Sleep(3*time.Second - time.Nanosecond)
				require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
				pending, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
				require.NoError(t, err)
				require.Len(t, pending, 1)
				require.Empty(t, h.inventory.removed)
				time.Sleep(time.Nanosecond)
				require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
				h.assertSettled(backend.CallbackStatusFailed)
				require.Len(t, h.inventory.removed, 2,
					"an uncommitted target can be cleaned after its visibility budget, even when minimum age is longer")
			})
		})
	}
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
