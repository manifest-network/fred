package placement

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
)

const (
	maintenanceLease = "11638ef8-1401-4f14-a355-1ae02afeb35b"
	maintenanceIDA   = "550e8400-e29b-41d4-a716-446655440000"
	maintenanceIDB   = "6ba7b811-9dad-41d1-80b4-00c04fd430c8"
)

type maintenanceTestAuthority struct {
	store       *Store
	revision    RecordRevision
	lifecycleID lifecycle.ID
}

func prepareMaintenanceLease(t *testing.T, store *Store) maintenanceTestAuthority {
	t.Helper()
	baseline := requireAdmissionBaseline(t, store, "backend-a")
	scope := requireAdmissionScope(t, store, baseline, "backend-a")
	operationID := requireOperationID(t, "9911")
	requestSnapshot, err := newBackendRequestSnapshot(
		"tenant-test",
		freshTestProviderUUID,
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	require.NoError(t, err)
	attempt, applied, err := store.beginNewAttempt(
		scope, maintenanceLease, "backend-a", operationID, PayloadFingerprint{},
		requestSnapshot, testCallbackPair(operationID),
	)
	require.NoError(t, err)
	require.True(t, applied)
	confirmed, err := confirmAttemptForTest(store, attempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	owner := store.Lookup(maintenanceLease)
	authorization := store.CurrentLifecycle(maintenanceLease)
	require.True(t, authorization.Authorized())
	return maintenanceTestAuthority{
		store: store, revision: owner.RecordRevision(), lifecycleID: authorization.ID(),
	}
}

func testMaintenanceCommand(
	t testing.TB,
	authority maintenanceTestAuthority,
	rawID string,
	kind MaintenanceCommandKind,
	payload []byte,
) PreparedMaintenanceCommand {
	t.Helper()
	id, err := maintenanceid.Parse(rawID)
	require.NoError(t, err)
	factory, err := lifecycle.NewRouteFactory("https://provider.test")
	require.NoError(t, err)
	route, err := factory.For(authority.lifecycleID)
	require.NoError(t, err)
	command, err := newMaintenanceCommand(
		id, maintenanceLease,
		runtimePrincipal{tenant: "tenant-test", providerUUID: freshTestProviderUUID},
		authority.revision, kind, payload, "backend-a",
		testBackendStorageID("backend-a"), route,
	)
	require.NoError(t, err)
	return PreparedMaintenanceCommand{
		issuer: authority.store, revision: authority.revision, command: command,
	}
}

func TestMaintenanceCommandConstructionMakesCallbackSplicingUnrepresentable(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	id, err := maintenanceid.Parse(maintenanceIDA)
	require.NoError(t, err)

	for name, callbackBase := range map[string]string{
		"fragment":            "https://provider.test#fragment",
		"lifecycle authority": "https://provider.test?lifecycle_id=" + authority.lifecycleID.String(),
		"operation authority": "https://provider.test?operation_id=550e8400-e29b-41d4-a716-446655440000",
	} {
		t.Run(name, func(t *testing.T) {
			factory, factoryErr := lifecycle.NewRouteFactory(callbackBase)
			assert.Nil(t, factory)
			require.Error(t, factoryErr)
		})
	}
	validFactory, err := lifecycle.NewRouteFactory("https://provider.test")
	require.NoError(t, err)
	validRoute, err := validFactory.For(authority.lifecycleID)
	require.NoError(t, err)

	_, err = newMaintenanceCommand(
		id, maintenanceLease,
		runtimePrincipal{tenant: "tenant-test", providerUUID: freshTestProviderUUID},
		authority.revision, MaintenanceCommandUpdate,
		bytes.Repeat([]byte{'x'}, maxMaintenancePayloadBytes+1), "backend-a",
		testBackendStorageID("backend-a"), validRoute,
	)
	assert.ErrorIs(t, err, ErrInvalidMaintenanceCommand)
}

func TestMaintenanceSettlementSurfaceRejectsCallerSelectedDurableOutcome(t *testing.T) {
	storeType := reflect.TypeOf((*Store)(nil))
	_, genericExposed := storeType.MethodByName("SettleMaintenanceCommand")
	assert.False(t, genericExposed,
		"production callers must not pass an arbitrary persisted outcome to settlement")

	coordinatorType := reflect.TypeOf((*MaintenanceCoordinator)(nil))
	for _, methodName := range []string{
		"SettleMaintenanceAccepted",
		"SettleMaintenanceNotProvisioned",
		"SettleMaintenanceInvalidState",
		"SettleMaintenanceValidationRejected",
		"SettleMaintenanceLeaseEnded",
		"SettleMaintenanceAuthorityRevoked",
		"SettleMaintenanceCapacityRefused",
		"SettleMaintenanceBackendUnavailable",
	} {
		_, storeExposed := storeType.MethodByName(methodName)
		assert.False(t, storeExposed, "raw Store settlement %s must stay private", methodName)
		_, exposed := coordinatorType.MethodByName(methodName)
		assert.False(t, exposed,
			"coordinator must derive %s rather than accept a caller-selected outcome", methodName)
	}
	_, completeExposed := coordinatorType.MethodByName("CompleteMaintenanceCall")
	assert.False(t, completeExposed,
		"backend return values must not be caller-selectable settlement evidence")
	for _, rawMethod := range []string{
		"TryClaimLeaseNow", "ReleaseLease", "PrepareMaintenanceCommand",
		"ReauthorizeMaintenanceCommand", "ExecuteMaintenance",
		"LookupMaintenanceCommand", "BeginMaintenanceCommand",
		"PendingMaintenanceCommands",
	} {
		_, exposed := coordinatorType.MethodByName(rawMethod)
		assert.False(t, exposed, "raw maintenance method %s must stay private", rawMethod)
	}
	applicationType := reflect.TypeOf((*MaintenanceApplication)(nil))
	_, executeExposed := applicationType.MethodByName("Execute")
	assert.True(t, executeExposed,
		"the construction-bound application must own authorization, invocation, and classification")
}

func TestStorePreparesMaintenanceOnlyFromItsCurrentAuthority(t *testing.T) {
	routes := testCallbackRoutes(t)
	store := newTestStore(t, WithCallbackRouteFactory(routes))
	authority := prepareMaintenanceLease(t, store)
	id, err := maintenanceid.Parse(maintenanceIDA)
	require.NoError(t, err)

	prepared, err := store.prepareMaintenanceCommand(
		id, maintenanceLease, MaintenanceCommandUpdate, []byte("payload"),
	)
	require.NoError(t, err)
	require.True(t, prepared.Valid())
	command := prepared.Command()
	assert.Equal(t, "tenant-test", command.Tenant())
	assert.Equal(t, freshTestProviderUUID, command.ProviderUUID())
	assert.Equal(t, "backend-a", command.BackendName())
	assert.Equal(t, testBackendStorageID("backend-a"), command.BackendStorageID())
	assert.Equal(t, authority.lifecycleID, command.LifecycleID())
	assert.Equal(t,
		"https://provider.test/proxy/callbacks/provision?trace=a%2Fb&lifecycle_id="+
			authority.lifecycleID.String(),
		command.CallbackURL(),
	)

	var zero PreparedMaintenanceCommand
	_, err = store.beginMaintenanceCommand(zero)
	assert.ErrorIs(t, err, ErrInvalidMaintenanceCommand)

	foreign := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	prepareMaintenanceLease(t, foreign)
	_, err = foreign.beginMaintenanceCommand(prepared)
	assert.ErrorIs(t, err, ErrInvalidMaintenanceCommand,
		"a capability issued by another aggregate cannot be replayed here")
}

func TestMaintenanceCommandJournalPreventsABAReplay(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	first := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("first"))

	admission, err := store.beginMaintenanceCommand(first)
	require.NoError(t, err)
	assert.True(t, admission.Pending())
	require.NoError(t, store.settleMaintenanceCommand(admission.Claim(), MaintenanceOutcomeAccepted))

	second := testMaintenanceCommand(t, authority, maintenanceIDB, MaintenanceCommandUpdate, []byte("second"))
	secondAdmission, err := store.beginMaintenanceCommand(second)
	require.NoError(t, err)
	require.NoError(t, store.settleMaintenanceCommand(secondAdmission.Claim(), MaintenanceOutcomeAccepted))

	lateFirst, err := store.beginMaintenanceCommand(first)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceOutcomeAccepted, lateFirst.Outcome())
	assert.False(t, lateFirst.Pending(), "a late A replay after B must never dispatch A again")
	claims, err := store.pendingMaintenanceCommands()
	require.NoError(t, err)
	assert.Empty(t, claims)
	record, found, err := store.LookupMaintenanceCommand(maintenanceLease, first.Command().ID())
	require.NoError(t, err)
	require.True(t, found)
	assert.Empty(t, record.Command().Payload(), "terminal receipts must not retain request bytes")
	assert.Equal(t, first.Command().PayloadHash(), record.Command().PayloadHash())
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		_, records, bucketErr := maintenanceCommandBuckets(tx)
		require.NoError(t, bucketErr)
		assert.NotContains(t,
			string(records.Get(maintenanceReceiptKey(maintenanceLease, first.Command().ID()))),
			`"payload":`,
		)
		return nil
	}))
}

func TestMaintenanceCommandJournalFencesPendingAndDivergentReplay(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	first := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("first"))
	admission, err := store.beginMaintenanceCommand(first)
	require.NoError(t, err)

	exact, err := store.beginMaintenanceCommand(first)
	require.NoError(t, err)
	assert.True(t, exact.Pending())
	assert.Equal(t, admission.Claim().Command().ID(), exact.Claim().Command().ID())

	_, err = store.beginMaintenanceCommand(
		testMaintenanceCommand(t, authority, maintenanceIDB, MaintenanceCommandRestart, nil),
	)
	assert.ErrorIs(t, err, ErrMaintenanceCommandConflict)

	divergent := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("changed"))
	_, err = store.beginMaintenanceCommand(divergent)
	assert.ErrorIs(t, err, ErrMaintenanceCommandConflict)
}

func TestPendingMaintenanceCommandSurvivesStoreRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placements.db")
	store, err := newStoreForTest(path)
	require.NoError(t, err)
	authority := prepareMaintenanceLease(t, store)
	command := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("durable"))
	_, err = store.beginMaintenanceCommand(command)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	reopened, err := newStoreForTest(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	claims, err := reopened.pendingMaintenanceCommands()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	assert.True(t, claims[0].Command().equal(command.Command()))
	require.NoError(t, reopened.settleMaintenanceCommand(
		claims[0], MaintenanceOutcomeValidationRejected,
	))

	replayCommand := testMaintenanceCommand(t, maintenanceTestAuthority{
		store:       reopened,
		revision:    reopened.Lookup(maintenanceLease).RecordRevision(),
		lifecycleID: reopened.CurrentLifecycle(maintenanceLease).ID(),
	}, maintenanceIDA, MaintenanceCommandUpdate, []byte("durable"))
	replay, err := reopened.beginMaintenanceCommand(replayCommand)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceOutcomeValidationRejected, replay.Outcome())
	assert.False(t, replay.Pending())
}

func TestMaintenanceCommandDetachesPayload(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	payload := []byte("original")
	command := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, payload)
	payload[0] = 'X'

	admission, err := store.beginMaintenanceCommand(command)
	require.NoError(t, err)
	got := admission.Claim().Command().Payload()
	got[0] = 'Y'
	assert.Equal(t, []byte("original"), admission.Claim().Command().Payload())
}

func TestMaintenanceCommandHealthRejectsDuplicateFieldsAndCrossDomainDrift(t *testing.T) {
	t.Run("duplicate command field", func(t *testing.T) {
		store := newTestStore(t)
		authority := prepareMaintenanceLease(t, store)
		command := testMaintenanceCommand(
			t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("payload"),
		)
		_, err := store.beginMaintenanceCommand(command)
		require.NoError(t, err)
		require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
			_, records, bucketErr := maintenanceCommandBuckets(tx)
			if bucketErr != nil {
				return bucketErr
			}
			key := maintenanceReceiptKey(maintenanceLease, command.Command().ID())
			encoded := records.Get(key)
			corrupt := bytes.Replace(
				encoded,
				[]byte(`"tenant":"tenant-test"`),
				[]byte(`"tenant":"tenant-test","tenant":"different"`),
				1,
			)
			return records.Put(key, corrupt)
		}))
		err = store.Healthy()
		require.Error(t, err)
		assert.ErrorContains(t, err, `duplicate field "tenant"`)
	})

	t.Run("placement revision drift", func(t *testing.T) {
		store := newTestStore(t)
		authority := prepareMaintenanceLease(t, store)
		command := testMaintenanceCommand(
			t, authority, maintenanceIDA, MaintenanceCommandRestart, nil,
		)
		_, err := store.beginMaintenanceCommand(command)
		require.NoError(t, err)
		require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
			_, records, bucketErr := maintenanceCommandBuckets(tx)
			if bucketErr != nil {
				return bucketErr
			}
			key := maintenanceReceiptKey(maintenanceLease, command.Command().ID())
			var record persistedMaintenanceCommand
			if decodeErr := json.Unmarshal(records.Get(key), &record); decodeErr != nil {
				return decodeErr
			}
			record.PlacementRevision++
			encoded, encodeErr := json.Marshal(record)
			if encodeErr != nil {
				return encodeErr
			}
			return records.Put(key, encoded)
		}))
		err = store.Healthy()
		require.Error(t, err)
		assert.ErrorContains(t, err, "pending command authority")
	})
}

func TestPendingMaintenanceStructurallyFencesPlacementMutations(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	command := testMaintenanceCommand(
		t, authority, maintenanceIDA, MaintenanceCommandRestart, nil,
	)
	_, err := store.beginMaintenanceCommand(command)
	require.NoError(t, err)

	operationID := requireOperationID(t, "9912")
	_, applied, err := store.beginOwnedAttempt(
		store.CurrentAdmissionBaseline(), authority.revision, "backend-a", operationID,
		PayloadFingerprint{}, testBackendRequestSnapshot(t), testCallbackPair(operationID),
	)
	assert.False(t, applied)
	assert.ErrorIs(t, err, ErrMaintenanceCommandConflict)

	deleted, err := store.deleteRecord(authority.revision)
	assert.False(t, deleted)
	assert.ErrorIs(t, err, ErrMaintenanceCommandConflict)

	retired, err := store.retireLifecycle(maintenanceLease, authority.lifecycleID)
	assert.False(t, retired.Retired())
	assert.ErrorIs(t, err, ErrMaintenanceCommandConflict)
	assert.True(t, store.CurrentLifecycle(maintenanceLease).Authorized(),
		"a pending command must preserve its exact lifecycle route")

	fence := store.BeginInventorySession()
	result, err := projectInventoryAtFenceForTest(t, store, fence, InventoryProjection{
		UntrustedPositives: map[string][]string{maintenanceLease: {"backend-a"}},
		Placements: map[string]string{
			"21638ef8-1401-4f14-a355-1ae02afeb35b": "backend-a",
		},
	})
	store.EndInventorySession(fence)
	require.NoError(t, err)
	assert.Contains(t, result.Fenced, maintenanceLease)
	assert.Equal(t, StateConfirmed, store.Lookup(maintenanceLease).State())
	assert.Equal(t, StateConfirmed,
		store.Lookup("21638ef8-1401-4f14-a355-1ae02afeb35b").State(),
		"a pending command must fence only its lease, not the inventory batch")
}

func TestMaintenanceHistoryCapacityRefusesForLiveLeaseWithoutForgettingIdentity(t *testing.T) {
	createdAt := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: createdAt.Add(time.Hour)}
	store := newTestStore(t, WithClock(clock.Now))
	authority := prepareMaintenanceLease(t, store)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		_, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		for range maxMaintenanceCommandsPerLease {
			id, idErr := maintenanceid.New()
			if idErr != nil {
				return idErr
			}
			command := testMaintenanceCommand(
				t, authority, id.String(), MaintenanceCommandRestart, nil,
			)
			encoded, encodeErr := encodeMaintenanceCommand(
				command.Command(), MaintenanceOutcomeAccepted, createdAt, createdAt.Add(time.Second),
			)
			if encodeErr != nil {
				return encodeErr
			}
			if putErr := records.Put(maintenanceReceiptKey(maintenanceLease, id), encoded); putErr != nil {
				return putErr
			}
		}
		return nil
	}))
	extra := testMaintenanceCommand(
		t, authority, maintenanceIDA, MaintenanceCommandRestart, nil,
	)
	_, err := store.beginMaintenanceCommand(extra)
	assert.ErrorIs(t, err, ErrMaintenanceHistoryFull)
	claims, pendingErr := store.pendingMaintenanceCommands()
	require.NoError(t, pendingErr)
	assert.Empty(t, claims)

	clock.now = createdAt.Add(100 * 365 * 24 * time.Hour)
	_, err = store.beginMaintenanceCommand(extra)
	assert.ErrorIs(t, err, ErrMaintenanceHistoryFull,
		"age cannot erase a live lease's exact command identity")
}

func TestMaintenanceSettlementClampsBackwardWallClock(t *testing.T) {
	createdAt := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: createdAt}
	store := newTestStore(t, WithClock(clock.Now))
	authority := prepareMaintenanceLease(t, store)
	command := testMaintenanceCommand(
		t, authority, maintenanceIDA, MaintenanceCommandRestart, nil,
	)
	admission, err := store.beginMaintenanceCommand(command)
	require.NoError(t, err)

	clock.now = createdAt.Add(-time.Hour)
	require.NoError(t, store.settleMaintenanceCommand(admission.Claim(), MaintenanceOutcomeAccepted))
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		_, records, bucketErr := maintenanceCommandBuckets(tx)
		require.NoError(t, bucketErr)
		_, outcome, storedCreatedAt, settledAt, decodeErr := decodeMaintenanceCommand(
			records.Get(maintenanceReceiptKey(maintenanceLease, command.Command().ID())),
		)
		require.NoError(t, decodeErr)
		assert.Equal(t, MaintenanceOutcomeAccepted, outcome)
		assert.Equal(t, storedCreatedAt, settledAt,
			"a backward wall clock must not create a self-corrupting receipt")
		return nil
	}))
}

func TestReclaimDetachedMaintenanceCommandsKeepsLiveLeaseAndReclaimsClosedLease(t *testing.T) {
	createdAt := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: createdAt}
	store := newTestStore(t, WithClock(clock.Now))
	authority := prepareMaintenanceLease(t, store)
	first := testMaintenanceCommand(
		t, authority, maintenanceIDA, MaintenanceCommandRestart, nil,
	)
	firstAdmission, err := store.beginMaintenanceCommand(first)
	require.NoError(t, err)
	require.NoError(t, store.settleMaintenanceCommand(firstAdmission.Claim(), MaintenanceOutcomeAccepted))

	clock.now = createdAt.Add(100 * 365 * 24 * time.Hour)
	removed, err := store.reclaimDetachedMaintenanceCommands()
	require.NoError(t, err)
	assert.Zero(t, removed, "live placement/lifecycle authority must retain every exact receipt")
	terminal, found, err := store.LookupMaintenanceCommand(maintenanceLease, first.Command().ID())
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, MaintenanceOutcomeAccepted, terminal.Outcome())

	deleted, err := store.deleteRecord(authority.revision)
	require.NoError(t, err)
	require.True(t, deleted)
	removed, err = store.reclaimDetachedMaintenanceCommands()
	require.NoError(t, err)
	assert.Zero(t, removed,
		"detached lifecycle authority still recognizes delayed callbacks and retains identity")
	_, found, err = store.LookupMaintenanceCommand(maintenanceLease, first.Command().ID())
	require.NoError(t, err)
	require.True(t, found)

	retired, err := store.retireLifecycle(maintenanceLease, authority.lifecycleID)
	require.NoError(t, err)
	require.True(t, retired.Retired())
	_, found, err = store.LookupMaintenanceCommand(maintenanceLease, first.Command().ID())
	require.NoError(t, err)
	assert.False(t, found,
		"removing the final lifecycle authority must reclaim its receipts atomically")
	removed, err = store.reclaimDetachedMaintenanceCommands()
	require.NoError(t, err)
	assert.Zero(t, removed,
		"ordinary authority deletion must leave no fleet-wide reclamation work")
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		_, records, bucketErr := maintenanceCommandBuckets(tx)
		require.NoError(t, bucketErr)
		assert.Nil(t, records.Get(maintenanceReceiptKey(maintenanceLease, first.Command().ID())))
		return nil
	}))
}

func TestMaintenanceReceiptsReclaimWhenPlacementLeavesAfterLifecycleRetirement(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	command := testMaintenanceCommand(
		t, authority, maintenanceIDA, MaintenanceCommandRestart, nil,
	)
	admission, err := store.beginMaintenanceCommand(command)
	require.NoError(t, err)
	require.NoError(t, store.settleMaintenanceCommand(admission.Claim(), MaintenanceOutcomeAccepted))

	retired, err := store.retireLifecycle(maintenanceLease, authority.lifecycleID)
	require.NoError(t, err)
	require.True(t, retired.Retired())
	_, found, err := store.LookupMaintenanceCommand(maintenanceLease, command.Command().ID())
	require.NoError(t, err)
	require.True(t, found,
		"placement authority still keeps the lease's exact retry history alive")

	deleted, err := store.deleteRecord(authority.revision)
	require.NoError(t, err)
	require.True(t, deleted)
	_, found, err = store.LookupMaintenanceCommand(maintenanceLease, command.Command().ID())
	require.NoError(t, err)
	assert.False(t, found,
		"deleting the final placement authority must reclaim terminal history in the same transaction")
	removed, err := store.reclaimDetachedMaintenanceCommands()
	require.NoError(t, err)
	assert.Zero(t, removed)
}

func TestMaintenanceReceiptReclamationFailureRollsBackFinalAuthorityRemoval(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	command := testMaintenanceCommand(
		t, authority, maintenanceIDA, MaintenanceCommandRestart, nil,
	)
	admission, err := store.beginMaintenanceCommand(command)
	require.NoError(t, err)
	require.NoError(t, store.settleMaintenanceCommand(admission.Claim(), MaintenanceOutcomeAccepted))
	deleted, err := store.deleteRecord(authority.revision)
	require.NoError(t, err)
	require.True(t, deleted)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		_, records, bucketErr := maintenanceCommandBuckets(tx)
		if bucketErr != nil {
			return bucketErr
		}
		return records.Put(
			maintenanceReceiptKey(maintenanceLease, command.Command().ID()),
			[]byte(`{"corrupt":true}`),
		)
	}))

	_, err = store.retireLifecycle(maintenanceLease, authority.lifecycleID)
	require.ErrorIs(t, err, ErrMaintenanceJournalCorrupt)
	authorization := store.authorizeLifecycle(maintenanceLease, authority.lifecycleID)
	assert.Equal(t, LifecycleVerdictTeardownOnly, authorization.Verdict(),
		"a reclamation failure must roll back deletion of the last callback authority")
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		assert.NotNil(t,
			tx.Bucket(lifecycleCapabilityBucketName).Get([]byte(maintenanceLease)))
		return nil
	}))
}
