package placement

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"testing"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
)

// reopenFenced reopens the fixture's database with the named backends fenced.
func (fixture *reporterRecoveryFixture) reopenFenced(t *testing.T, fenced ...string) {
	t.Helper()
	require.NoError(t, fixture.reopened.Close())
	var err error
	fixture.reopened, err = OpenStore(
		fixture.dbPath, freshTestProviderUUID,
		WithCallbackRouteFactory(fixture.routes), WithFencedBackends(fenced),
	)
	require.NoError(t, err)
	store := fixture.reopened
	t.Cleanup(func() { _ = store.Close() })
	fixture.coordinator = nil
}

// newFencedReporterFixture interrupts a sweep in which both backends reported
// a positive, so each is a journaled reporter of the inherited marker.
func newFencedReporterFixture(t *testing.T) *reporterRecoveryFixture {
	t.Helper()
	return newReporterRecoveryFixture(t, func(interrupted *ReconciliationSweep) {
		require.NoError(t, interrupted.RecordProvision(reporterSilentBackend,
			testBackendStorageID(reporterSilentBackend),
			reporterRecoveryProvision(reporterUnrelatedLease, reporterSilentBackend)))
	})
}

func TestFencedReporterDoesNotHoldInheritedRecovery(t *testing.T) {
	fixture := newFencedReporterFixture(t)
	fixture.reopenFenced(t, reporterSilentBackend)
	require.Equal(t, InventoryRecoveryPending, fixture.reopened.InventoryReadiness())

	fixture.sweep(t, reporterRecoveryBackend)

	assert.NoError(t, fixture.reopened.leaseSideEffectError(reporterRecoveryLease),
		"the operator distrusts the fenced reporter; waiting for it would freeze every lease")
	assert.Equal(t, InventoryFencedReporterUnaccounted, fixture.reopened.InventoryReadiness())
	baseline := fixture.reopened.CurrentAdmissionBaseline()
	assert.True(t, baseline.Valid(), "work on leases with a row, such as owner recovery, continues")
	assert.False(t, baseline.AdmitsRecordless(),
		"the fenced reporter may hold a lease with no row, so nothing without a row is admitted")
	_, err := fixture.reopened.scopeAdmission(baseline, []string{reporterRecoveryBackend})
	assert.ErrorIs(t, err, ErrRecordlessAdmissionWithheld)
	metadata := persistedTopologyMetadata(t, fixture.reopened)
	assert.Zero(t, metadata.PendingInventorySweepID)
	assert.Equal(t, []string{reporterSilentBackend}, metadata.UnprojectedFencedReporters,
		"the fenced reporter's lost positives are recorded, not forgotten")
}

func TestUnprojectedFencedReporterClearsOnlyWhenItAnswersBothInventories(t *testing.T) {
	fixture := newFencedReporterFixture(t)
	fixture.reopenFenced(t, reporterSilentBackend)
	fixture.sweep(t, reporterRecoveryBackend)

	// The operator lifts the fence. The record survives the restart.
	fixture.reopen(t)
	require.Equal(t, InventoryFencedReporterUnaccounted, fixture.reopened.InventoryReadiness())
	fixture.sweep(t, reporterRecoveryBackend)
	assert.Equal(t, []string{reporterSilentBackend},
		persistedTopologyMetadata(t, fixture.reopened).UnprojectedFencedReporters,
		"silence accounts for nothing")

	fixture.sweep(t, reporterRecoveryBackend, reporterSilentBackend)
	assert.Nil(t, persistedTopologyMetadata(t, fixture.reopened).UnprojectedFencedReporters,
		"a paired, pinned answer accounts for whatever the backend holds")
	assert.Empty(t, fixture.reopened.unprojectedFencedReporters)
	assert.Equal(t, InventoryReady, fixture.reopened.InventoryReadiness())
	assert.True(t, fixture.reopened.CurrentAdmissionBaseline().AdmitsRecordless(), "admission resumes")
}

func TestFencedNonReporterRecordsNothing(t *testing.T) {
	// Only backend-a reported before the interruption: fencing backend-b
	// abandons nothing.
	fixture := newReporterRecoveryFixture(t, nil)
	fixture.reopenFenced(t, reporterSilentBackend)
	fixture.sweep(t, reporterRecoveryBackend)
	assert.Equal(t, InventoryReady, fixture.reopened.InventoryReadiness())
	assert.Nil(t, persistedTopologyMetadata(t, fixture.reopened).UnprojectedFencedReporters)
}

func TestFenceDoesNotExcuseAnUnfencedReporter(t *testing.T) {
	fixture := newFencedReporterFixture(t)
	fixture.reopenFenced(t, reporterRecoveryBackend)
	// backend-b reported too and is not fenced; its silence still holds.
	fixture.sweep(t, reporterRecoveryBackend)
	assert.Equal(t, InventoryRecoveryPending, fixture.reopened.InventoryReadiness())
	assert.Nil(t, persistedTopologyMetadata(t, fixture.reopened).UnprojectedFencedReporters)
}

func TestUntrackedMarkerExcusesExactlyTheFencedBackends(t *testing.T) {
	fixture := newReporterRecoveryFixture(t, nil)
	fixture.untrack(t)
	fixture.reopenFenced(t, reporterSilentBackend)

	fixture.sweep(t, reporterRecoveryBackend)
	assert.Equal(t, InventoryFencedReporterUnaccounted, fixture.reopened.InventoryReadiness(),
		"every unfenced backend answered; the fenced one stays unaccounted")
	assert.Equal(t, []string{reporterSilentBackend},
		persistedTopologyMetadata(t, fixture.reopened).UnprojectedFencedReporters,
		"an untracked chain cannot name its reporters, so every fenced backend is recorded")
}

func TestFencedBackendsMustBeActiveAndLeaveASurvivor(t *testing.T) {
	fixture := newReporterRecoveryFixture(t, nil)
	require.NoError(t, fixture.reopened.Close())
	for name, fenced := range map[string][]string{
		"unknown":    {"backend-z"},
		"all fenced": {reporterRecoveryBackend, reporterSilentBackend},
	} {
		store, err := OpenStore(fixture.dbPath, freshTestProviderUUID,
			WithCallbackRouteFactory(fixture.routes), WithFencedBackends(fenced))
		require.ErrorIs(t, err, ErrInvalidBackendTopology, name)
		require.Nil(t, store)
	}
}

func TestTopologyCannotChangeWhileABackendIsFenced(t *testing.T) {
	fixture := newReporterRecoveryFixture(t, nil)
	fixture.reopenFenced(t, reporterSilentBackend)
	names := []string{reporterRecoveryBackend, reporterSilentBackend, "backend-c"}
	require.ErrorIs(t, fixture.reopened.ConfigureBackendTopologyWithStorageIdentities(
		names, testBackendStorageIDs(names...),
	), ErrBackendTopologyInUse)
}

func TestUnprojectedFencedReporterCannotBeRemovedFromTheTopology(t *testing.T) {
	fixture := newFencedReporterFixture(t)
	fixture.reopenFenced(t, reporterSilentBackend)
	fixture.sweep(t, reporterRecoveryBackend)
	fixture.reopen(t)

	err := fixture.reopened.ConfigureBackendTopologyWithStorageIdentities(
		[]string{reporterRecoveryBackend}, testBackendStorageIDs(reporterRecoveryBackend),
	)
	require.ErrorIs(t, err, ErrBackendTopologyInUse)
	require.ErrorContains(t, err, "never projected")
}

func TestUnprojectedFencedReportersMetadataValidation(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a", "backend-b"}))
	valid := persistedTopologyMetadata(t, store)
	valid.UnprojectedFencedReporters = []string{"backend-b"}
	encoded, err := encodeTopologyMetadata(valid)
	require.NoError(t, err)
	decoded, err := decodeTopologyMetadata(encoded)
	require.NoError(t, err)
	assert.Equal(t, []string{"backend-b"}, decoded.UnprojectedFencedReporters)

	for name, reporters := range map[string][]string{
		"empty":    {},
		"inactive": {"backend-z"},
		"unsorted": {"backend-b", "backend-a"},
	} {
		invalid := persistedTopologyMetadata(t, store)
		invalid.UnprojectedFencedReporters = reporters
		require.Error(t, validateTopologyMetadata(invalid), name)
	}
	unconfigured := emptyTopologyMetadata()
	unconfigured.UnprojectedFencedReporters = []string{"backend-a"}
	require.Error(t, validateTopologyMetadata(unconfigured))
}

func TestRetiringAnUnprojectedFencedReporterRecordsRecordlessUnproven(t *testing.T) {
	fixture := newRetirementFixture(t)
	store, err := OpenStore(fixture.dbPath, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		metadata, err := loadTopologyMetadata(tx)
		if err != nil {
			return err
		}
		metadata.UnprojectedFencedReporters = []string{"backend-b", "backend-c"}
		return putTopologyMetadata(tx, metadata)
	}))
	require.NoError(t, store.Close())

	repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	plan, err := repair.PlanBackendRetirement("backend-c", fixture.pinC)
	require.NoError(t, err)
	assert.False(t, plan.Facts().PendingInventorySweep)
	assert.True(t, plan.Facts().RecordlessUnproven,
		"backend-c may hold a lease whose row a cleared recovery never wrote")
	publishRetirementBackup(t, repair)
	_, err = repair.RetireBackend(plan, LostBackendAttestationText)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())

	reopened, err := OpenStore(fixture.dbPath, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	metadata := persistedTopologyMetadata(t, reopened)
	assert.True(t, metadata.RetiredBackends["backend-c"].RecordlessUnproven)
	assert.Equal(t, []string{"backend-b"}, metadata.UnprojectedFencedReporters,
		"the retired name leaves the record; others stay")
	assert.True(t, reopened.RecordlessLeasesUnproven())
}

// A fenced backend is never asked in this process, so a chain that became
// untracked here cannot have lost its positives: nothing is recorded.
func TestInProcessUntrackedChainRecordsNoFencedReporter(t *testing.T) {
	const fencedName = "backend-c"
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	routes := testCallbackRoutes(t)
	store, err := newStoreForTest(dbPath, WithCallbackRouteFactory(routes))
	require.NoError(t, err)
	requireAdmissionBaseline(t, store, reporterRecoveryBackend, reporterSilentBackend, fencedName)
	require.NoError(t, store.Close())
	fenced, err := OpenStore(dbPath, freshTestProviderUUID,
		WithCallbackRouteFactory(routes), WithFencedBackends([]string{fencedName}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = fenced.Close() })

	reporter := &unrecordedPositiveInventoryBackend{
		executionTestBackend: &executionTestBackend{name: reporterRecoveryBackend},
		provisions:           reporterRecoveryProvision(reporterRecoveryLease, reporterRecoveryBackend),
		storageID:            testBackendStorageID(reporterRecoveryBackend),
	}
	coordinator := bindReporterRecovery(t, fenced, &reconciliationSweepReader{}, reporter,
		&executionTestBackend{name: reporterSilentBackend}, &executionTestBackend{name: fencedName})
	interrupted, err := coordinator.BeginSweep()
	require.NoError(t, err)
	provisions, err := interrupted.CollectProvisionInventory(t.Context(), reporterRecoveryBackend)
	require.NoError(t, err)
	require.NoError(t, interrupted.RejectProvisionInventory(provisions))
	interrupted.End()
	require.False(t, fenced.inventoryReporters.tracked)
	require.Equal(t, InventoryRecoveryPending, fenced.InventoryReadiness())

	sweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	t.Cleanup(sweep.End)
	for _, name := range []string{reporterRecoveryBackend, reporterSilentBackend} {
		var rows []backend.ProvisionInfo
		if name == reporterRecoveryBackend {
			rows = reporterRecoveryProvision(reporterRecoveryLease, reporterRecoveryBackend)
		}
		require.NoError(t, sweep.RecordProvision(name, testBackendStorageID(name), rows))
		require.NoError(t, sweep.RecordRetention(name, testBackendStorageID(name), nil))
	}
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(ReconciliationProjection{
		Placements: map[string]string{reporterRecoveryLease: reporterRecoveryBackend},
	})
	require.NoError(t, err)

	assert.Equal(t, InventoryReady, fenced.InventoryReadiness(), "every unfenced backend answered")
	assert.Nil(t, persistedTopologyMetadata(t, fenced).UnprojectedFencedReporters)
}

// Not parallel: reads a process-global gauge.
func TestUnprojectedFencedReporterIsVisibleLiveAndOffline(t *testing.T) {
	fixture := newFencedReporterFixture(t)
	fixture.reopenFenced(t, reporterSilentBackend)
	fixture.sweep(t, reporterRecoveryBackend)
	gauge := func(name string) float64 {
		return promtestutil.ToFloat64(metrics.PlacementUnprojectedFencedReporter.WithLabelValues(name))
	}
	assert.InDelta(t, 1, gauge(reporterSilentBackend), 0)
	assert.InDelta(t, 0, gauge(reporterRecoveryBackend), 0)
	require.NoError(t, fixture.reopened.Close())

	expectation, err := NewAuthorityExpectation(
		freshTestProviderUUID, []string{reporterRecoveryBackend, reporterSilentBackend},
	)
	require.NoError(t, err)
	report, err := InspectAuthorityFile(fixture.dbPath, expectation)
	require.NoError(t, err)
	assert.Equal(t, []string{reporterSilentBackend}, report.UnprojectedFencedReporters)

	// A new process starts with no series; the store republishes the record.
	metrics.PlacementUnprojectedFencedReporter.WithLabelValues(reporterSilentBackend).Set(0)
	fixture.reopen(t)
	assert.InDelta(t, 1, gauge(reporterSilentBackend), 0, "the record is published again at open")
	fixture.sweep(t, reporterRecoveryBackend, reporterSilentBackend)
	assert.InDelta(t, 0, gauge(reporterSilentBackend), 0)
}

// classify closes the fixture's store and inspects the stopped database as
// placement-repair -classify does.
func (fixture *reporterRecoveryFixture) classify(t *testing.T) AuthorityReport {
	t.Helper()
	require.NoError(t, fixture.reopened.Close())
	expectation, err := NewAuthorityExpectation(
		freshTestProviderUUID, []string{reporterRecoveryBackend, reporterSilentBackend},
	)
	require.NoError(t, err)
	report, err := InspectAuthorityFile(fixture.dbPath, expectation)
	require.NoError(t, err)
	return report
}

// untrack rewrites the pending sweep as one that predates reporter tracking.
func (fixture *reporterRecoveryFixture) untrack(t *testing.T) {
	t.Helper()
	require.NoError(t, fixture.reopened.db.Update(func(tx *bolt.Tx) error {
		metadata, err := loadTopologyMetadata(tx)
		if err != nil {
			return err
		}
		metadata.InventorySweepReporters = nil
		return putTopologyMetadata(tx, metadata)
	}))
}

// ENG-1119: an operator about to fence a backend checks the stopped database
// with -classify. fence_restart_would_record must name exactly the backends a
// store opened with them fenced inherits as reporters to record, so the store
// and the classifier share one rule.
func TestClassifyReportsWhatAFencedRestartWouldRecord(t *testing.T) {
	topology := []string{reporterRecoveryBackend, reporterSilentBackend}
	for _, test := range []struct {
		name          string
		fixture       func(*testing.T) *reporterRecoveryFixture
		wantReporters []string
		wantUntracked bool
		wantRecord    []string
	}{
		{
			name:          "tracked journal",
			fixture:       func(t *testing.T) *reporterRecoveryFixture { return newReporterRecoveryFixture(t, nil) },
			wantReporters: []string{reporterRecoveryBackend},
			wantRecord:    []string{reporterRecoveryBackend},
		},
		{
			name:          "tracked journal naming every backend",
			fixture:       newFencedReporterFixture,
			wantReporters: topology,
			wantRecord:    topology,
		},
		{
			name: "untracked chain",
			fixture: func(t *testing.T) *reporterRecoveryFixture {
				fixture := newReporterRecoveryFixture(t, nil)
				fixture.untrack(t)
				return fixture
			},
			wantUntracked: true,
			wantRecord:    topology,
		},
		{
			name: "no pending sweep",
			fixture: func(t *testing.T) *reporterRecoveryFixture {
				fixture := newReporterRecoveryFixture(t, nil)
				fixture.sweep(t, reporterRecoveryBackend)
				require.Equal(t, InventoryReady, fixture.reopened.InventoryReadiness())
				return fixture
			},
			wantRecord: []string{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := test.fixture(t)
			report := fixture.classify(t)
			assert.Equal(t, len(test.wantRecord) != 0, report.PendingInventorySweepID != 0)
			assert.Equal(t, test.wantReporters, report.InventorySweepReporters)
			assert.Equal(t, test.wantUntracked, report.InventorySweepUntracked)
			assert.Equal(t, test.wantRecord, report.FenceRestartWouldRecord)

			encoded, err := MarshalAuthorityReport(report)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(encoded, &fields))
			wantRecord, err := json.Marshal(test.wantRecord)
			require.NoError(t, err)
			assert.JSONEq(t, string(wantRecord), string(fields["fence_restart_would_record"]),
				"the key is always present and never null")
			_, pendingKey := fields["pending_inventory_sweep_id"]
			assert.Equal(t, report.PendingInventorySweepID != 0, pendingKey)
			_, untrackedKey := fields["inventory_sweep_untracked"]
			assert.Equal(t, test.wantUntracked, untrackedKey)
			_, reportersKey := fields["inventory_sweep_reporters"]
			assert.Equal(t, len(test.wantReporters) != 0, reportersKey)

			// A store opened with one backend fenced inherits exactly the
			// fenced members of fence_restart_would_record.
			for _, fenced := range topology {
				store, err := OpenStore(fixture.dbPath, freshTestProviderUUID,
					WithCallbackRouteFactory(fixture.routes), WithFencedBackends([]string{fenced}))
				require.NoError(t, err)
				want := []string{}
				if slices.Contains(report.FenceRestartWouldRecord, fenced) {
					want = []string{fenced}
				}
				store.mu.RLock()
				inherited := slices.Sorted(maps.Keys(store.inheritedFencedReporters))
				store.mu.RUnlock()
				if inherited == nil {
					inherited = []string{}
				}
				assert.Equal(t, want, inherited, "fenced %s", fenced)
				require.NoError(t, store.Close())
			}
		})
	}
}

// Not parallel: replaces the process-global logger.
func TestStoreOpenWarnsAboutInheritedFencedReporters(t *testing.T) {
	fixture := newReporterRecoveryFixture(t, nil)
	require.NoError(t, fixture.reopened.Close())
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	const warning = "fenced backends may hold leases an interrupted sweep never projected"
	for _, test := range []struct {
		fenced string
		warned bool
	}{
		// backend-b never reported in the interrupted sweep: fencing it
		// records nothing and warns about nothing.
		{reporterSilentBackend, false},
		{reporterRecoveryBackend, true},
	} {
		logs.Reset()
		store, err := OpenStore(fixture.dbPath, freshTestProviderUUID,
			WithCallbackRouteFactory(fixture.routes), WithFencedBackends([]string{test.fenced}))
		require.NoError(t, err)
		require.NoError(t, store.Close())
		if test.warned {
			assert.Contains(t, logs.String(), warning)
			assert.Contains(t, logs.String(), "fenced_reporters=["+test.fenced+"]")
		} else {
			assert.NotContains(t, logs.String(), warning)
		}
	}
}

// A scope issued before a fenced reporter was recorded cannot be spent after,
// and a restore into a new target is admission of a lease with no row.
func TestRecordedFencedReporterWithholdsEveryRecordlessAdmission(t *testing.T) {
	fixture := newFencedReporterFixture(t)
	fixture.reopenFenced(t, reporterSilentBackend)
	staleScope, err := fixture.reopened.scopeAdmission(
		fixture.reopened.CurrentAdmissionBaseline(), []string{reporterRecoveryBackend},
	)
	require.Error(t, err, "recovery is still pending at open")
	require.False(t, staleScope.Valid())

	fixture.sweep(t, reporterRecoveryBackend)
	store := fixture.reopened
	require.NotEmpty(t, store.unprojectedFencedReporters)

	source, err := store.reserveRestoreSource(reporterRecoveryLease)
	require.NoError(t, err)
	defer store.releaseRestoreSource(source)
	restoreOperation := requireOperationID(t, "8711")
	_, err = store.beginReservedRestore(store.CurrentAdmissionBaseline(), source,
		"00000000-0000-4000-8000-000000000711", restoreOperation,
		repairBackendRequestSnapshot(t), testCallbackPair(restoreOperation))
	require.ErrorIs(t, err, ErrRecordlessAdmissionWithheld)

	// A scope minted while admission was open is refused once a reporter is
	// recorded.
	store.mu.Lock()
	recorded := store.unprojectedFencedReporters
	store.unprojectedFencedReporters = map[string]struct{}{}
	store.mu.Unlock()
	scope, err := store.scopeAdmission(store.CurrentAdmissionBaseline(), []string{reporterRecoveryBackend})
	require.NoError(t, err)
	store.mu.Lock()
	store.unprojectedFencedReporters = recorded
	store.mu.Unlock()
	store.mu.Lock()
	err = store.validateAdmissionScopeLocked(scope)
	store.mu.Unlock()
	require.ErrorIs(t, err, ErrRecordlessAdmissionWithheld)
}
