package placement

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
)

// reporterRecoveryFixture drives the interrupted-sweep scenarios below: a
// complete initial sweep establishes the admission baseline, then a sweep in
// which only the named reporters return positives is interrupted after
// collection and before projection, and the store is reopened.
type reporterRecoveryFixture struct {
	dbPath      string
	routes      *CallbackRouteFactory
	reader      *reconciliationSweepReader
	reopened    *Store
	coordinator *ReconciliationCoordinator
}

// reopen closes the current store and opens the database again. A store binds
// one operation coordinator for its lifetime, so the fixture rebinds lazily.
func (fixture *reporterRecoveryFixture) reopen(t *testing.T) {
	t.Helper()
	if fixture.reopened != nil {
		require.NoError(t, fixture.reopened.Close())
	}
	var err error
	fixture.reopened, err = OpenStore(
		fixture.dbPath, freshTestProviderUUID, WithCallbackRouteFactory(fixture.routes),
	)
	require.NoError(t, err)
	store := fixture.reopened
	t.Cleanup(func() { _ = store.Close() })
	fixture.coordinator = nil
}

func (fixture *reporterRecoveryFixture) boundCoordinator(t *testing.T) *ReconciliationCoordinator {
	t.Helper()
	if fixture.coordinator == nil {
		fixture.coordinator = bindReporterRecovery(t, fixture.reopened, fixture.reader,
			&executionTestBackend{name: reporterRecoveryBackend},
			&executionTestBackend{name: reporterSilentBackend},
		)
	}
	return fixture.coordinator
}

const (
	reporterRecoveryLease   = "00000000-0000-4000-8000-000000000301"
	reporterUnrelatedLease  = "00000000-0000-4000-8000-000000000302"
	reporterRecoveryBackend = "backend-a"
	reporterSilentBackend   = "backend-b"
)

func bindReporterRecovery(
	t *testing.T,
	current *Store,
	reader *reconciliationSweepReader,
	clients ...backend.Backend,
) *ReconciliationCoordinator {
	t.Helper()
	base, err := current.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(clients...))
	coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
	require.NoError(t, err)
	return coordinator
}

func reporterRecoveryProvision(leaseUUID, backendName string) []backend.ProvisionInfo {
	return []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID, BackendName: backendName,
		ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
	}}
}

// newReporterRecoveryFixture interrupts a sweep in which backend-a reported a
// positive. interruptedExtra records any further endpoint responses for that
// sweep before the interruption.
func newReporterRecoveryFixture(
	t *testing.T,
	interruptedExtra func(*ReconciliationSweep),
) *reporterRecoveryFixture {
	t.Helper()
	fixture := &reporterRecoveryFixture{
		dbPath: filepath.Join(t.TempDir(), "placements.db"),
		routes: testCallbackRoutes(t),
		reader: &reconciliationSweepReader{lease: &billingtypes.Lease{
			Uuid: reporterRecoveryLease, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			State: billingtypes.LEASE_STATE_PENDING,
			Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
		}},
	}
	store, err := newStoreForTest(fixture.dbPath, WithCallbackRouteFactory(fixture.routes))
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(
		store, []string{reporterRecoveryBackend, reporterSilentBackend},
	))
	coordinator := bindReporterRecovery(t, store, fixture.reader,
		&executionTestBackend{name: reporterRecoveryBackend},
		&executionTestBackend{name: reporterSilentBackend},
	)
	initial, err := coordinator.BeginSweep()
	require.NoError(t, err)
	for _, backendName := range []string{reporterRecoveryBackend, reporterSilentBackend} {
		storageID := testBackendStorageID(backendName)
		require.NoError(t, initial.RecordProvision(backendName, storageID, nil))
		require.NoError(t, initial.RecordRetention(backendName, storageID, nil))
	}
	require.NoError(t, initial.SealInventory())
	_, err = initial.Project(ReconciliationProjection{})
	require.NoError(t, err)
	initial.End()
	require.True(t, store.CurrentAdmissionBaseline().Valid())

	interrupted, err := coordinator.BeginSweep()
	require.NoError(t, err)
	storageA := testBackendStorageID(reporterRecoveryBackend)
	require.NoError(t, interrupted.RecordProvision(reporterRecoveryBackend, storageA,
		reporterRecoveryProvision(reporterRecoveryLease, reporterRecoveryBackend)))
	if interruptedExtra != nil {
		interruptedExtra(interrupted)
	}
	// Closing the database before End models a crash after collection and
	// before any projection could commit.
	require.NoError(t, store.db.Close())
	interrupted.End()
	_ = store.Close()

	fixture.reopen(t)
	return fixture
}

// sweep runs one sweep on the reopened store in which exactly the named
// backends answer both endpoints. backend-a reports the recovery lease again
// whenever it answers; projection represents it on backend-a.
func (fixture *reporterRecoveryFixture) sweep(
	t *testing.T,
	answering ...string,
) *ProjectedReconciliationSweep {
	t.Helper()
	sweep, err := fixture.boundCoordinator(t).BeginSweep()
	require.NoError(t, err)
	placements := map[string]string{}
	for _, backendName := range answering {
		storageID := testBackendStorageID(backendName)
		var provisions []backend.ProvisionInfo
		if backendName == reporterRecoveryBackend {
			provisions = reporterRecoveryProvision(reporterRecoveryLease, reporterRecoveryBackend)
			placements[reporterRecoveryLease] = reporterRecoveryBackend
		}
		require.NoError(t, sweep.RecordProvision(backendName, storageID, provisions))
		require.NoError(t, sweep.RecordRetention(backendName, storageID, nil))
	}
	require.NoError(t, sweep.SealInventory())
	projected, err := sweep.Project(ReconciliationProjection{Placements: placements})
	require.NoError(t, err)
	t.Cleanup(sweep.End)
	return projected
}

func persistedTopologyMetadata(t *testing.T, store *Store) topologyMetadata {
	t.Helper()
	var metadata topologyMetadata
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		var err error
		metadata, err = loadTopologyMetadata(tx)
		return err
	}))
	return metadata
}

func TestInterruptedSweepRecoveryRequiresOnlyItsReporters(t *testing.T) {
	fixture := newReporterRecoveryFixture(t, nil)
	require.False(t, fixture.reopened.CurrentAdmissionBaseline().Valid(),
		"an inherited marker withdraws admission until its reporters answer again")
	require.ErrorIs(t, fixture.reopened.leaseSideEffectError(reporterUnrelatedLease),
		ErrUnprojectedInventoryPositive)
	require.Equal(t, InventoryRecoveryPending, fixture.reopened.InventoryReadiness())
	require.InDelta(t, 1, promtestutil.ToFloat64(metrics.PlacementInventoryRecoveryPending), 0)

	// backend-b never answered the interrupted sweep, so it cannot hold a lost
	// positive. Re-observing backend-a alone retires the inherited fence.
	fixture.sweep(t, reporterRecoveryBackend)

	assert.True(t, fixture.reopened.CurrentAdmissionBaseline().Valid(),
		"a silent non-reporter must not keep the whole provider fenced")
	assert.NoError(t, fixture.reopened.leaseSideEffectError(reporterUnrelatedLease))
	assert.Equal(t, InventoryReady, fixture.reopened.InventoryReadiness())
	assert.InDelta(t, 0, promtestutil.ToFloat64(metrics.PlacementInventoryRecoveryPending), 0)
	metadata := persistedTopologyMetadata(t, fixture.reopened)
	assert.Zero(t, metadata.PendingInventorySweepID)
	assert.Nil(t, metadata.InventorySweepReporters)
}

func TestInterruptedSweepRecoveryWaitsForEverySilentReporter(t *testing.T) {
	// Both backends reported before the interruption. backend-b is silent
	// after restart, so its possibly lost positive keeps recovery required.
	fixture := newReporterRecoveryFixture(t, func(interrupted *ReconciliationSweep) {
		require.NoError(t, interrupted.RecordProvision(reporterSilentBackend,
			testBackendStorageID(reporterSilentBackend),
			reporterRecoveryProvision(reporterUnrelatedLease, reporterSilentBackend)))
	})
	fixture.sweep(t, reporterRecoveryBackend)
	require.False(t, fixture.reopened.CurrentAdmissionBaseline().Valid(),
		"a reporter that has not answered again may hold an unrepresented positive")
	require.ErrorIs(t, fixture.reopened.leaseSideEffectError(reporterRecoveryLease),
		ErrUnprojectedInventoryPositive)
}

func TestInterruptedSweepRecoveryNeedsBothEndpointsFromEachReporter(t *testing.T) {
	for _, retentionAnswered := range []bool{false, true} {
		t.Run(map[bool]string{false: "provisions only", true: "both endpoints"}[retentionAnswered],
			func(t *testing.T) {
				fixture := newReporterRecoveryFixture(t, nil)
				sweep, err := fixture.boundCoordinator(t).BeginSweep()
				require.NoError(t, err)
				t.Cleanup(sweep.End)
				// The reporter's earlier positive is gone, so nothing is left to
				// project; only its endpoint coverage decides recovery.
				storageA := testBackendStorageID(reporterRecoveryBackend)
				require.NoError(t, sweep.RecordProvision(reporterRecoveryBackend, storageA, nil))
				if retentionAnswered {
					require.NoError(t, sweep.RecordRetention(reporterRecoveryBackend, storageA, nil))
				}
				require.NoError(t, sweep.SealInventory())
				_, err = sweep.Project(ReconciliationProjection{})
				require.NoError(t, err)
				assert.Equal(t, retentionAnswered, fixture.reopened.CurrentAdmissionBaseline().Valid(),
					"a reporter's retained positives stay unobserved until its retention endpoint answers")
			})
	}
}

func TestInterruptedSweepRecoveryBindsEachReporterToItsPinnedStorage(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(map[bool]string{false: "replacement storage", true: "pinned storage"}[pinned],
			func(t *testing.T) {
				fixture := newReporterRecoveryFixture(t, nil)
				sweep, err := fixture.boundCoordinator(t).BeginSweep()
				require.NoError(t, err)
				t.Cleanup(sweep.End)
				// Both answers are empty and paired; only the storage behind the
				// reporter's name differs. A replacement cannot vouch for the pinned
				// storage's lost positives, so the projection refuses it whole.
				storageID := testBackendStorageID("replacement-a")
				if pinned {
					storageID = testBackendStorageID(reporterRecoveryBackend)
				}
				require.NoError(t, sweep.RecordProvision(reporterRecoveryBackend, storageID, nil))
				require.NoError(t, sweep.RecordRetention(reporterRecoveryBackend, storageID, nil))
				require.NoError(t, sweep.SealInventory())
				_, err = sweep.Project(ReconciliationProjection{})
				if pinned {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, ErrBackendStorageIdentityMismatch)
				}
				assert.Equal(t, pinned, fixture.reopened.CurrentAdmissionBaseline().Valid())
				assert.Equal(t, !pinned, fixture.reopened.InventoryReadiness() == InventoryRecoveryPending)
			})
	}
}

func TestUntrackedInterruptedMarkerStillRequiresWholeTopology(t *testing.T) {
	fixture := newReporterRecoveryFixture(t, nil)
	// A marker written before reporter tracking carries no journal. Rewrite
	// the reopened database into that shape and reopen it again.
	require.NoError(t, fixture.reopened.db.Update(func(tx *bolt.Tx) error {
		metadata, err := loadTopologyMetadata(tx)
		if err != nil {
			return err
		}
		require.NotNil(t, metadata.InventorySweepReporters)
		metadata.InventorySweepReporters = nil
		return putTopologyMetadata(tx, metadata)
	}))
	fixture.reopen(t)

	fixture.sweep(t, reporterRecoveryBackend)
	require.False(t, fixture.reopened.CurrentAdmissionBaseline().Valid(),
		"an untracked marker cannot name its reporters, so every backend must answer")

	fixture.sweep(t, reporterRecoveryBackend, reporterSilentBackend)
	assert.True(t, fixture.reopened.CurrentAdmissionBaseline().Valid())
}

func TestSweepReporterIsJournaledBeforeItsPositiveCanBeLost(t *testing.T) {
	fixture := newReporterRecoveryFixture(t, func(interrupted *ReconciliationSweep) {
		// An empty response contributes no positive and is not journaled.
		require.NoError(t, interrupted.RecordProvision(reporterSilentBackend,
			testBackendStorageID(reporterSilentBackend), nil))
	})
	metadata := persistedTopologyMetadata(t, fixture.reopened)
	require.NotZero(t, metadata.PendingInventorySweepID)
	require.NotNil(t, metadata.InventorySweepReporters)
	assert.Equal(t, metadata.PendingInventorySweepID, metadata.InventorySweepReporters.SweepID)
	assert.Equal(t, []string{reporterRecoveryBackend}, metadata.InventorySweepReporters.Backends)
}

func TestSupersedingSweepKeepsEveryUnresolvedReporter(t *testing.T) {
	fixture := newReporterRecoveryFixture(t, nil)
	// A sweep in which only backend-b answers cannot resolve backend-a's
	// possibly lost positive; the chain keeps backend-a and adds backend-b.
	sweep, err := fixture.boundCoordinator(t).BeginSweep()
	require.NoError(t, err)
	require.NoError(t, sweep.RecordProvision(reporterSilentBackend,
		testBackendStorageID(reporterSilentBackend),
		reporterRecoveryProvision(reporterUnrelatedLease, reporterSilentBackend)))
	sweep.End()

	metadata := persistedTopologyMetadata(t, fixture.reopened)
	require.NotNil(t, metadata.InventorySweepReporters)
	assert.Equal(t, metadata.PendingInventorySweepID, metadata.InventorySweepReporters.SweepID)
	assert.Equal(t,
		[]string{reporterRecoveryBackend, reporterSilentBackend},
		metadata.InventorySweepReporters.Backends)
}

func TestSweepReporterJournalFailureInstallsNoBarrier(t *testing.T) {
	fixture := newReporterRecoveryFixture(t, nil)
	fixture.sweep(t, reporterRecoveryBackend)
	sweep, err := fixture.boundCoordinator(t).BeginSweep()
	require.NoError(t, err)
	t.Cleanup(sweep.End)
	require.NoError(t, fixture.reopened.db.Close())

	err = sweep.RecordProvision(reporterSilentBackend, testBackendStorageID(reporterSilentBackend),
		reporterRecoveryProvision(reporterUnrelatedLease, reporterSilentBackend))
	require.Error(t, err, "a reporter that cannot be journaled must discard its response")
	fixture.reopened.mu.RLock()
	defer fixture.reopened.mu.RUnlock()
	assert.Empty(t, fixture.reopened.unprojectedPositives[reporterUnrelatedLease],
		"no barrier may name a reporter the durable journal does not")
}

func TestInventorySweepReporterMetadataValidation(t *testing.T) {
	valid := func() topologyMetadata {
		topology := []string{reporterRecoveryBackend, reporterSilentBackend}
		fingerprint, err := topologyFingerprint(topology)
		require.NoError(t, err)
		return topologyMetadata{
			Schema:              topologyMetadataSchema,
			ProviderUUID:        freshTestProviderUUID,
			Topology:            topology,
			TopologyFingerprint: fingerprint,
			KnownBackends:       topology,
			TopologyID:          1,
			// A pending sweep with a journal naming one active reporter.
			InventorySweepSequence:  4,
			PendingInventorySweepID: 4,
			InventorySweepReporters: &inventorySweepReporters{
				SweepID: 4, Backends: []string{reporterRecoveryBackend},
			},
		}
	}
	require.NoError(t, validateTopologyMetadata(valid()))

	emptyJournal := valid()
	emptyJournal.InventorySweepReporters.Backends = []string{}
	require.NoError(t, validateTopologyMetadata(emptyJournal))

	untracked := valid()
	untracked.InventorySweepReporters = nil
	require.NoError(t, validateTopologyMetadata(untracked))

	for name, mutate := range map[string]func(*topologyMetadata){
		"without a pending sweep": func(m *topologyMetadata) { m.PendingInventorySweepID = 0 },
		"for another sweep":       func(m *topologyMetadata) { m.InventorySweepReporters.SweepID = 3 },
		"with a null list":        func(m *topologyMetadata) { m.InventorySweepReporters.Backends = nil },
		"naming an inactive backend": func(m *topologyMetadata) {
			m.InventorySweepReporters.Backends = []string{"backend-z"}
		},
		"out of canonical order": func(m *topologyMetadata) {
			m.InventorySweepReporters.Backends = []string{reporterSilentBackend, reporterRecoveryBackend}
		},
		"with a duplicate": func(m *topologyMetadata) {
			m.InventorySweepReporters.Backends = []string{reporterRecoveryBackend, reporterRecoveryBackend}
		},
	} {
		t.Run(name, func(t *testing.T) {
			metadata := valid()
			mutate(&metadata)
			assert.Error(t, validateTopologyMetadata(metadata))
		})
	}

	// The journal must round-trip through the strict decoder, and a JSON null
	// list must be refused rather than read as an empty journal.
	encoded, err := encodeTopologyMetadata(valid())
	require.NoError(t, err)
	decoded, err := decodeTopologyMetadata(encoded)
	require.NoError(t, err)
	require.NoError(t, validateTopologyMetadata(decoded))
	assert.Equal(t, valid().InventorySweepReporters, decoded.InventorySweepReporters)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	fields["inventory_sweep_reporters"] = json.RawMessage(`{"sweep_id":4,"backends":null}`)
	nullList, err := json.Marshal(fields)
	require.NoError(t, err)
	decoded, err = decodeTopologyMetadata(nullList)
	require.NoError(t, err)
	assert.Error(t, validateTopologyMetadata(decoded))

	// A repeated or case-aliased nested name must not silently rewrite the
	// journal, e.g. a later empty "backends" narrowing recovery to nothing.
	for name, raw := range map[string]string{
		"duplicate backends": `{"sweep_id":4,"backends":["backend-a"],"backends":[]}`,
		"duplicate sweep":    `{"sweep_id":4,"sweep_id":4,"backends":["backend-a"]}`,
		"case alias":         `{"sweep_id":4,"Backends":[]}`,
		"unknown field":      `{"sweep_id":4,"backends":["backend-a"],"extra":1}`,
	} {
		fields["inventory_sweep_reporters"] = json.RawMessage(raw)
		ambiguous, err := json.Marshal(fields)
		require.NoError(t, err)
		_, err = decodeTopologyMetadata(ambiguous)
		assert.Error(t, err, name)
	}
}

// A reporter journal must never outlive its marker: every clearing path retires
// both in one write, or the next open would reject the metadata.
func TestClearedInventoryMarkerRetiresItsReporterJournal(t *testing.T) {
	store, err := newStoreForTest(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, configureBackendTopologyForTest(store, []string{reporterRecoveryBackend}))
	reader := &reconciliationSweepReader{}
	coordinator := bindReporterRecovery(t, store, reader,
		&executionTestBackend{name: reporterRecoveryBackend})

	// Orderly zero-positive End clears the marker.
	sweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	require.NotNil(t, persistedTopologyMetadata(t, store).InventorySweepReporters)
	storageID := testBackendStorageID(reporterRecoveryBackend)
	require.NoError(t, sweep.RecordProvision(reporterRecoveryBackend, storageID, nil))
	require.NoError(t, sweep.RecordRetention(reporterRecoveryBackend, storageID, nil))
	require.NoError(t, sweep.SealInventory())
	sweep.End()
	metadata := persistedTopologyMetadata(t, store)
	assert.Zero(t, metadata.PendingInventorySweepID)
	assert.Nil(t, metadata.InventorySweepReporters)

	// A successful projection clears it too.
	sweep, err = coordinator.BeginSweep()
	require.NoError(t, err)
	require.NoError(t, sweep.RecordProvision(reporterRecoveryBackend, storageID,
		reporterRecoveryProvision(reporterRecoveryLease, reporterRecoveryBackend)))
	require.NoError(t, sweep.RecordRetention(reporterRecoveryBackend, storageID, nil))
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(ReconciliationProjection{
		Placements: map[string]string{reporterRecoveryLease: reporterRecoveryBackend},
	})
	require.NoError(t, err)
	sweep.End()
	metadata = persistedTopologyMetadata(t, store)
	assert.Zero(t, metadata.PendingInventorySweepID)
	assert.Nil(t, metadata.InventorySweepReporters)
}

// refreshFailingInventoryBackend answers both endpoints but cannot prove its
// listing is current.
type refreshFailingInventoryBackend struct {
	*unrecordedPositiveInventoryBackend
}

func (*refreshFailingInventoryBackend) RefreshState(context.Context) error {
	return errors.New("synthetic refresh failure")
}

// TestUnattributedEvidenceKeepsTheWholeTopologyRule drives the production
// collection and disposal path. Evidence a sweep would quarantine is durably
// recorded as leaving the chain on the whole-topology rule before it can be
// lost, so a restart cannot retire it on its reporter's word.
func TestUnattributedEvidenceKeepsTheWholeTopologyRule(t *testing.T) {
	const lease = reporterRecoveryLease
	row := backend.ProvisionInfo{
		LeaseUUID: lease, BackendName: reporterRecoveryBackend,
		ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
	}
	for _, test := range []struct {
		name     string
		client   func(*unrecordedPositiveInventoryBackend) backend.Backend
		collect  func(*testing.T, *ReconciliationSweep)
		reporter bool
	}{
		{
			name: "attributed control",
			collect: func(t *testing.T, sweep *ReconciliationSweep) {
				_, err := sweep.CollectProvisionInventory(t.Context(), reporterRecoveryBackend)
				require.NoError(t, err)
			},
			reporter: true,
		},
		{
			name: "replacement storage",
			client: func(client *unrecordedPositiveInventoryBackend) backend.Backend {
				client.storageID = testBackendStorageID("replacement-a")
				return client
			},
			collect: func(t *testing.T, sweep *ReconciliationSweep) {
				_, err := sweep.CollectProvisionInventory(t.Context(), reporterRecoveryBackend)
				require.NoError(t, err)
			},
		},
		{
			name: "failed refresh",
			client: func(client *unrecordedPositiveInventoryBackend) backend.Backend {
				return &refreshFailingInventoryBackend{unrecordedPositiveInventoryBackend: client}
			},
			collect: func(t *testing.T, sweep *ReconciliationSweep) {
				_, err := sweep.CollectProvisionInventory(t.Context(), reporterRecoveryBackend)
				require.NoError(t, err)
			},
		},
		{
			name: "same-backend endpoint overlap",
			client: func(client *unrecordedPositiveInventoryBackend) backend.Backend {
				client.retentions = []backend.RetainedLease{{LeaseUUID: lease}}
				return client
			},
			collect: func(t *testing.T, sweep *ReconciliationSweep) {
				_, err := sweep.CollectProvisionInventory(t.Context(), reporterRecoveryBackend)
				require.NoError(t, err)
				_, err = sweep.CollectRetentionInventory(t.Context(), reporterRecoveryBackend)
				require.NoError(t, err)
			},
		},
		{
			name: "rejected at disposal",
			collect: func(t *testing.T, sweep *ReconciliationSweep) {
				provisions, err := sweep.CollectProvisionInventory(t.Context(), reporterRecoveryBackend)
				require.NoError(t, err)
				require.NoError(t, sweep.RejectProvisionInventory(provisions))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			routes := testCallbackRoutes(t)
			store, err := newStoreForTest(dbPath, WithCallbackRouteFactory(routes))
			require.NoError(t, err)
			requireAdmissionBaseline(t, store, reporterRecoveryBackend, reporterSilentBackend)
			reporter := &unrecordedPositiveInventoryBackend{
				executionTestBackend: &executionTestBackend{name: reporterRecoveryBackend},
				provisions:           []backend.ProvisionInfo{row},
				storageID:            testBackendStorageID(reporterRecoveryBackend),
			}
			var client backend.Backend = reporter
			if test.client != nil {
				client = test.client(reporter)
			}
			reader := &reconciliationSweepReader{}
			coordinator := bindReporterRecovery(t, store, reader, client,
				&executionTestBackend{name: reporterSilentBackend})
			interrupted, err := coordinator.BeginSweep()
			require.NoError(t, err)
			test.collect(t, interrupted)
			assert.Equal(t, test.reporter, store.inventoryReporters.tracked,
				"the live process holds the rule the evidence requires")
			// Crash before any projection could represent the evidence.
			require.NoError(t, store.db.Close())
			interrupted.End()
			_ = store.Close()

			fixture := &reporterRecoveryFixture{dbPath: dbPath, routes: routes, reader: reader}
			fixture.reopen(t)
			metadata := persistedTopologyMetadata(t, fixture.reopened)
			require.NotZero(t, metadata.PendingInventorySweepID)
			if test.reporter {
				require.NotNil(t, metadata.InventorySweepReporters)
				assert.Equal(t, []string{reporterRecoveryBackend}, metadata.InventorySweepReporters.Backends)
			} else {
				assert.Nil(t, metadata.InventorySweepReporters,
					"unattributed evidence must reach disk as the whole-topology rule")
			}

			fixture.sweep(t, reporterRecoveryBackend)
			assert.Equal(t, test.reporter, fixture.reopened.CurrentAdmissionBaseline().Valid(),
				"only attributed evidence can be retired by its reporter alone")
			if !test.reporter {
				fixture.sweep(t, reporterRecoveryBackend, reporterSilentBackend)
				assert.True(t, fixture.reopened.CurrentAdmissionBaseline().Valid(),
					"whole-topology coverage still retires it")
			}
		})
	}
}

// TestDisposalKeepsTheWholeTopologyRuleIfCollectionMissedARejection pins the
// disposal backstop: a half RecordBackendInventory distrusts must leave the
// chain on the whole-topology rule even if collection-time attribution ever
// stops mirroring one of its checks. The responses are planted directly, as a
// collector that attributed them would have left them.
func TestDisposalKeepsTheWholeTopologyRuleIfCollectionMissedARejection(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, reporterRecoveryBackend, reporterSilentBackend)
	coordinator := bindReporterRecovery(t, store, &reconciliationSweepReader{},
		&executionTestBackend{name: reporterRecoveryBackend},
		&executionTestBackend{name: reporterSilentBackend},
	)
	sweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	t.Cleanup(sweep.End)
	storageA := testBackendStorageID(reporterRecoveryBackend)
	provisions := BackendProvisionInventory{
		backendName: reporterRecoveryBackend,
		provisions:  reporterRecoveryProvision(reporterRecoveryLease, reporterRecoveryBackend),
		storageID:   storageA,
		refreshErr:  errors.New("synthetic refresh failure"),
		sweep:       sweep, marker: sweep.marker, sweepID: sweep.fence.sweepID,
		receipt: &inventoryResponseMarker{},
	}
	retentions := BackendRetentionInventory{
		backendName: reporterRecoveryBackend, storageID: storageA,
		sweep: sweep, marker: sweep.marker, sweepID: sweep.fence.sweepID,
		receipt: &inventoryResponseMarker{},
	}
	sweep.mu.Lock()
	sweep.pendingProvisions[provisions.receipt] = provisions
	sweep.pendingRetentions[retentions.receipt] = retentions
	sweep.mu.Unlock()
	require.True(t, store.inventoryReporters.tracked)

	result, err := sweep.RecordBackendInventory(provisions, retentions)
	require.NoError(t, err)
	assert.Equal(t, BackendInventoryUntrusted, result.Disposition())
	assert.False(t, store.inventoryReporters.tracked)
	assert.Nil(t, persistedTopologyMetadata(t, store).InventorySweepReporters)
}

// TestInheritedFenceCoverageBindsReportersToTheirPins exercises the coverage
// predicate directly. Project refuses a mismatched identity before reaching
// it, so this is the only test that pins the check as defense in depth.
func TestInheritedFenceCoverageBindsReportersToTheirPins(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(map[bool]string{false: "replacement storage", true: "pinned storage"}[pinned],
			func(t *testing.T) {
				store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
				requireAdmissionBaseline(t, store, reporterRecoveryBackend, reporterSilentBackend)
				coordinator := bindReporterRecovery(t, store, &reconciliationSweepReader{},
					&executionTestBackend{name: reporterRecoveryBackend},
					&executionTestBackend{name: reporterSilentBackend},
				)
				sweep, err := coordinator.BeginSweep()
				require.NoError(t, err)
				t.Cleanup(sweep.End)
				storageID := testBackendStorageID("replacement-a")
				if pinned {
					storageID = testBackendStorageID(reporterRecoveryBackend)
				}
				// Record straight into the collection: attribution is not under test.
				require.NoError(t, sweep.collection.RecordProvision(reporterRecoveryBackend, storageID, nil))
				require.NoError(t, sweep.collection.RecordRetention(reporterRecoveryBackend, storageID, nil))
				require.NoError(t, sweep.SealInventory())

				store.mu.Lock()
				defer store.mu.Unlock()
				store.inventoryReporters = trackedSweepReporters().with(reporterRecoveryBackend)
				assert.Equal(t, pinned, store.inheritedFenceCoveredLocked(sweep.sealed))
			})
	}
}
