package placement

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/metrics"
)

// Seed valid individually bounded journal rows, as an older provider could
// retain them. The fresh target still crosses the production WAL admission.
func seedMaintenancePressure(t *testing.T, s *Store, template MaintenanceCommand, count int) {
	t.Helper()
	require.NoError(t, s.db.Update(func(tx *bolt.Tx) error {
		pending, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		for index := range count {
			command := template
			command.leaseUUID = fmt.Sprintf("10000000-0000-4000-8000-%012d", index)
			encoded, err := encodeMaintenanceCommand(command, MaintenanceOutcomePending, s.now().UTC(), time.Time{})
			if err != nil {
				return err
			}
			if err := records.Put(maintenanceReceiptKey(command.leaseUUID, command.id), encoded); err != nil {
				return err
			}
			if err := pending.Put([]byte(command.leaseUUID), []byte(command.id.String())); err != nil {
				return err
			}
		}
		return nil
	}))
	// This fixture represents a stopped-provider upgrade; rebuild the same
	// projection that OpenStore creates from the pre-existing journal.
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		var err error
		s.maintenanceAccounting, err = loadMaintenanceAccounting(tx)
		return err
	}))
}

func TestFreshMaintenanceAdmissionBoundsAggregatePendingJournal(t *testing.T) {
	for _, reason := range []string{"count", "bytes"} {
		t.Run(reason, func(t *testing.T) {
			store := newTestStore(t)
			authority := prepareMaintenanceLease(t, store)
			template := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("candidate"))
			count := maxPendingMaintenanceCommands
			if reason == "bytes" {
				template = testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, bytes.Repeat([]byte("x"), maxMaintenancePayloadBytes))
				encoded, err := encodeMaintenanceCommand(template.command, MaintenanceOutcomePending, store.now().UTC(), time.Time{})
				require.NoError(t, err)
				count = int(maxPendingMaintenanceBytes/(int64(len(encoded))+pendingMaintenanceTransitionBytes)) + 1
			}
			previousTenant := template.command
			previousTenant.tenant = "incumbent-tenant"
			seedMaintenancePressure(t, store, previousTenant, count)
			fresh := testMaintenanceCommand(t, authority, maintenanceIDB, MaintenanceCommandUpdate, []byte("fresh bytes"))
			_, err := store.beginMaintenanceCommand(fresh)
			require.ErrorIs(t, err, ErrMaintenancePendingFull)
			require.EqualError(t, err, ErrMaintenancePendingFull.Error()+": "+reason)
			_, found, err := store.LookupMaintenanceCommand(maintenanceLease, fresh.command.ID())
			require.NoError(t, err)
			require.False(t, found, "capacity refusal must happen before the dispatch WAL is minted")
		})
	}
}

func TestPendingBudgetCannotBlockExactReplayOrAuthenticatedSettlement(t *testing.T) {
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
	authority, err := authority.coordinator.execution.MaintenanceCoordinator(&maintenanceProgressPayloads{})
	require.NoError(t, err)
	application, err := authority.Application(nil, 0)
	require.NoError(t, err)
	id := mustMaintenanceID(t, maintenanceIDA)
	request, err := NewMaintenanceApplicationRequest(id, maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("accepted update"))
	require.NoError(t, err)
	require.Equal(t, MaintenanceApplicationAccepted, application.Execute(t.Context(), request).Outcome())
	record, found, err := store.LookupMaintenanceCommand(maintenanceLease, id)
	require.NoError(t, err)
	require.True(t, found)
	seedMaintenancePressure(t, store, record.Command(), maxPendingMaintenanceCommands)
	result := application.Execute(t.Context(), request)
	require.Equal(t, MaintenanceApplicationAccepted, result.Outcome(), result.Err())
	require.NoError(t, applyMaintenanceCompletionForTest(t, authority, id, backend.CallbackStatusFailed))
	record, found, err = store.LookupMaintenanceCommand(maintenanceLease, id)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, MaintenanceOutcomeExecutionFailed, record.Outcome())
	require.Equal(t, MaintenanceApplicationBackendInvalidState, application.Execute(t.Context(), request).Outcome())
	require.NotContains(t, authority.coordinator.RuntimeController().PendingLeaseUUIDs(), maintenanceLease)
}

func TestMaintenanceWakeOnlyFollowsExactDurableCompletionAndCoalesces(t *testing.T) {
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
	authority, err := authority.coordinator.execution.MaintenanceCoordinator(&maintenanceProgressPayloads{})
	require.NoError(t, err)
	application, err := authority.Application(nil, 0)
	require.NoError(t, err)
	request, err := NewMaintenanceApplicationRequest(mustMaintenanceID(t, maintenanceIDA), maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("candidate"))
	require.NoError(t, err)
	require.Equal(t, MaintenanceApplicationAccepted, application.Execute(t.Context(), request).Outcome())
	require.Empty(t, store.maintenanceChanged)
	require.NoError(t, applyMaintenanceCompletionForTest(t, authority, maintenanceid.ID{}, backend.CallbackStatusSuccess))
	require.NoError(t, applyMaintenanceCompletionForTest(t, authority, mustMaintenanceID(t, maintenanceIDB), backend.CallbackStatusSuccess))
	require.Empty(t, store.maintenanceChanged, "generic and stale callbacks cannot schedule completion work")
	require.NoError(t, applyMaintenanceCompletionForTest(t, authority, request.id, backend.CallbackStatusSuccess))
	require.Len(t, store.maintenanceChanged, 1)
	require.NoError(t, applyMaintenanceCompletionForTest(t, authority, request.id, backend.CallbackStatusSuccess))
	require.Len(t, store.maintenanceChanged, 1, "duplicate receipts cannot create an unbounded wake queue")
	<-application.CompletionChanged()
	require.NoError(t, applyMaintenanceCompletionForTest(t, authority, request.id, backend.CallbackStatusSuccess))
	require.Empty(t, store.maintenanceChanged, "a committed duplicate does not repeatedly wake the recovery worker")
}

func TestMaintenancePressureReportsPhasesBytesAndOriginalAge(t *testing.T) {
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
	authority, err := authority.coordinator.execution.MaintenanceCoordinator(&maintenanceProgressPayloads{})
	require.NoError(t, err)
	application, err := authority.Application(nil, 0)
	require.NoError(t, err)
	request, err := NewMaintenanceApplicationRequest(mustMaintenanceID(t, maintenanceIDA), maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("candidate"))
	require.NoError(t, err)
	before := time.Now()
	require.Equal(t, MaintenanceApplicationAccepted, application.Execute(t.Context(), request).Outcome())
	accepted, err := store.maintenancePressure()
	require.NoError(t, err)
	require.Equal(t, 1, accepted[maintenanceCompletionOutstanding].count)
	require.Greater(t, accepted[maintenanceCompletionOutstanding].bytes, int64(len(request.payload)))
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.MaintenancePending.WithLabelValues(maintenanceCompletionOutstanding.String())), "Execute must publish the committed phase")
	require.Equal(t, float64(accepted[maintenanceCompletionOutstanding].bytes), testutil.ToFloat64(metrics.MaintenancePendingBytes.WithLabelValues(maintenanceCompletionOutstanding.String())))
	require.False(t, accepted[maintenanceCompletionOutstanding].oldest.Before(before))
	require.NoError(t, applyMaintenanceCompletionForTest(t, authority, request.id, backend.CallbackStatusSuccess))
	confirmed, err := store.maintenancePressure()
	require.NoError(t, err)
	require.Zero(t, confirmed[maintenanceCompletionOutstanding].count)
	require.Equal(t, 1, confirmed[maintenancePayloadConfirmed].count)
	require.Equal(t, accepted[maintenanceCompletionOutstanding].oldest, confirmed[maintenancePayloadConfirmed].oldest)
}

type completionDuringAcceptanceEvents struct{ after func() }

func (events completionDuringAcceptanceEvents) DispatchWithOrderedSettlement(_ backend.LeaseStatusEvent, call func() (bool, error)) (bool, error) {
	accepted, err := call()
	events.after()
	return accepted, err
}

func TestMaintenanceCompletionConsumedDuringDispatchIsRequeuedAfterUnlock(t *testing.T) {
	base, _ := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
	authority, err := base.coordinator.execution.MaintenanceCoordinator(&maintenanceProgressPayloads{})
	require.NoError(t, err)
	id := mustMaintenanceID(t, maintenanceIDA)
	var application *MaintenanceApplication
	events := completionDuringAcceptanceEvents{after: func() {
		require.NoError(t, applyMaintenanceCompletionForTest(t, authority, id, backend.CallbackStatusSuccess))
		<-application.CompletionChanged()
		require.NoError(t, application.RecoverPending(t.Context()), "a recovery pass cannot steal the live dispatch lock")
		require.Contains(t, base.coordinator.RuntimeController().PendingLeaseUUIDs(), maintenanceLease)
	}}
	application, err = authority.Application(events, 0)
	require.NoError(t, err)
	request, err := NewMaintenanceApplicationRequest(id, maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("candidate"))
	require.NoError(t, err)
	require.Equal(t, MaintenanceApplicationAccepted, application.Execute(t.Context(), request).Outcome())
	select {
	case <-application.CompletionChanged():
	default:
		t.Fatal("dispatch release lost the callback wake consumed while it held the mutex")
	}
	require.NoError(t, application.RecoverPending(t.Context()))
	require.NotContains(t, base.coordinator.RuntimeController().PendingLeaseUUIDs(), maintenanceLease)
}

func prepareTenantMaintenance(t *testing.T, store *Store, scope AdmissionScope, index int, tenant string, payload []byte) PreparedMaintenanceCommand {
	t.Helper()
	lease := fmt.Sprintf("20000000-0000-4000-8000-%012d", index)
	operationID := requireOperationID(t, fmt.Sprintf("%d", 5000+index))
	snapshot, err := newBackendRequestSnapshot(tenant, freshTestProviderUUID,
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}})
	require.NoError(t, err)
	attempt, applied, err := store.beginNewAttempt(scope, lease, "backend-a", operationID,
		PayloadFingerprint{}, snapshot, testCallbackPair(operationID))
	require.NoError(t, err)
	require.True(t, applied)
	confirmed, err := confirmAttemptForTest(store, attempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	command, err := store.prepareMaintenanceCommand(mustMaintenanceID(t, maintenanceIDA), lease, MaintenanceCommandUpdate, payload)
	require.NoError(t, err)
	return command
}

func TestMaintenanceReservationPreservesNewcomerAndSettlementCapacity(t *testing.T) {
	for _, dimension := range []string{"count", "bytes"} {
		t.Run(dimension, func(t *testing.T) {
			store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
			baseline := requireAdmissionBaseline(t, store, "backend-a")
			scope := requireAdmissionScope(t, store, baseline, "backend-a")
			payload := []byte("candidate")
			if dimension == "bytes" {
				payload = bytes.Repeat([]byte("x"), maxMaintenancePayloadBytes)
			}
			var admitted []MaintenanceCommandAdmission
			var refused PreparedMaintenanceCommand
			for index := range maxPendingMaintenanceCommands {
				command := prepareTenantMaintenance(t, store, scope, index, "tenant-a", payload)
				admission, err := store.beginMaintenanceCommand(command)
				if err != nil {
					require.ErrorIs(t, err, ErrMaintenancePendingFull)
					require.Equal(t, MaintenanceApplicationCapacityReserved, resultForMaintenanceBeginError(err).Outcome())
					require.EqualError(t, err, ErrMaintenancePendingFull.Error()+": reserved_"+dimension)
					refused = command
					break
				}
				admitted = append(admitted, admission)
			}
			require.True(t, refused.Valid())
			other := prepareTenantMaintenance(t, store, scope, 2000, "tenant-b", payload)
			_, err := store.beginMaintenanceCommand(other)
			require.NoError(t, err, "one tenant's pressure must not consume another tenant's reserved opportunity")
			spoofed := refused
			spoofed.command.tenant = "tenant-c"
			_, err = store.beginMaintenanceCommand(spoofed)
			require.ErrorIs(t, err, ErrMaintenanceCommandConflict, "the share follows the durable principal, never a caller-selected key")
			require.NoError(t, store.settleMaintenanceCommand(admitted[0].Claim(), MaintenanceOutcomeValidationRejected))
			_, err = store.beginMaintenanceCommand(refused)
			require.Equal(t, MaintenanceApplicationCapacityReserved, resultForMaintenanceBeginError(err).Outcome(), "the newcomer spent capacity; leave its reservation intact")
			for _, previous := range admitted[1:3] {
				require.NoError(t, store.settleMaintenanceCommand(previous.Claim(), MaintenanceOutcomeValidationRejected))
			}
			_, err = store.beginMaintenanceCommand(refused)
			require.NoError(t, err, "terminal settlement releases count and byte charges")
		})
	}
}

func TestMaintenanceAdmissionSerializesConcurrentFinalBorrowedSlot(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	baseline := requireAdmissionBaseline(t, store, "backend-a")
	scope := requireAdmissionScope(t, store, baseline, "backend-a")
	template := prepareTenantMaintenance(t, store, scope, 0, "tenant-a", []byte("x"))
	seedMaintenancePressure(t, store, template.command, maxPendingMaintenanceCommands-reservedMaintenanceCommands-1)
	first := prepareTenantMaintenance(t, store, scope, 100, "tenant-a", []byte("x"))
	second := prepareTenantMaintenance(t, store, scope, 101, "tenant-a", []byte("x"))
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for _, command := range []PreparedMaintenanceCommand{first, second} {
		workers.Go(func() { <-start; _, err := store.beginMaintenanceCommand(command); results <- err })
	}
	close(start)
	workers.Wait()
	close(results)
	accepted, refused := 0, 0
	for err := range results {
		if err == nil {
			accepted++
		} else {
			require.ErrorIs(t, err, ErrMaintenancePendingFull)
			refused++
		}
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, 1, refused)
}

func TestMaintenanceAccountingRebuildsOnOpenAndDoesNotPublishRolledBackWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placements.db")
	store, err := newStoreForTest(path, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	baseline := requireAdmissionBaseline(t, store, "backend-a")
	scope := requireAdmissionScope(t, store, baseline, "backend-a")
	var first PreparedMaintenanceCommand
	for index := range 32 {
		command := prepareTenantMaintenance(t, store, scope, index, "tenant-a", []byte("x"))
		if index == 0 {
			first = command
		}
		_, err := store.beginMaintenanceCommand(command)
		require.NoError(t, err)
	}
	before, err := store.maintenancePressure()
	require.NoError(t, err)
	store.mu.Lock()
	err = store.updateMaintenanceAuthority(func(journal *maintenanceJournalTransaction) error {
		encoded, err := encodeMaintenanceCommand(first.command, MaintenanceOutcomeValidationRejected, store.now().UTC(), store.now().UTC())
		if err != nil {
			return err
		}
		if err := journal.write(encoded); err != nil {
			return err
		}
		return errors.New("abort transaction after settlement")
	})
	store.mu.Unlock()
	require.ErrorContains(t, err, "abort transaction")
	after, err := store.maintenancePressure()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, store.Close())
	reopened, err := newStoreForTest(path, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	after, err = reopened.maintenancePressure()
	require.NoError(t, err)
	require.Equal(t, before, after)
	scope = requireAdmissionScope(t, reopened, reopened.CurrentAdmissionBaseline(), "backend-a")
	_, err = reopened.beginMaintenanceCommand(prepareTenantMaintenance(t, reopened, scope, 100, "tenant-a", []byte("x")))
	require.NoError(t, err, "aggregators may borrow beyond the old 16-command ceiling")
	_, err = reopened.beginMaintenanceCommand(prepareTenantMaintenance(t, reopened, scope, 101, "tenant-b", []byte("x")))
	require.NoError(t, err)
}

func TestMaintenanceGaugesFollowCommittedPhaseAndSettlement(t *testing.T) {
	base, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
	coordinator, err := base.coordinator.execution.MaintenanceCoordinator(&maintenanceProgressPayloads{})
	require.NoError(t, err)
	application, err := coordinator.Application(nil, 0)
	require.NoError(t, err)
	fixed := time.Now().Add(-time.Minute)
	store.now = func() time.Time { return fixed }
	command, err := store.prepareMaintenanceCommand(mustMaintenanceID(t, maintenanceIDA), maintenanceLease, MaintenanceCommandUpdate, []byte("exact bytes"))
	require.NoError(t, err)
	admission, err := store.beginMaintenanceCommand(command)
	require.NoError(t, err)
	check := func(phase maintenanceJournalPhase) {
		t.Helper()
		application.observePending()
		pressure, err := store.maintenancePressure()
		require.NoError(t, err)
		for _, label := range []maintenanceJournalPhase{maintenanceDeliveryOutstanding, maintenanceCompletionOutstanding, maintenancePayloadConfirmed} {
			expected := float64(0)
			if label == phase {
				expected = 1
			}
			require.Equal(t, expected, testutil.ToFloat64(metrics.MaintenancePending.WithLabelValues(label.String())))
			require.Equal(t, float64(pressure[label].bytes), testutil.ToFloat64(metrics.MaintenancePendingBytes.WithLabelValues(label.String())))
			require.Equal(t, expected*60, testutil.ToFloat64(metrics.MaintenancePendingOldestAge.WithLabelValues(label.String())))
		}
	}
	store.now = func() time.Time { return fixed.Add(time.Minute) }
	check(maintenanceDeliveryOutstanding)
	_, err = store.acceptMaintenanceUpdate(maintenanceDelivery{claim: admission.Claim()})
	require.NoError(t, err)
	check(maintenanceCompletionOutstanding)
	require.NoError(t, applyMaintenanceCompletionForTest(t, coordinator, command.command.id, backend.CallbackStatusSuccess))
	check(maintenancePayloadConfirmed)
	work, err := store.maintenanceWork(admission.Claim())
	require.NoError(t, err)
	confirmed := work.(confirmedMaintenanceUpdate)
	require.NoError(t, store.settleMaintenancePhase(confirmed.claim, maintenanceSettlement{outcome: MaintenanceOutcomeAccepted}, maintenancePayloadConfirmed))
	check(maintenanceCompleted)
}

func TestMaintenanceCompletionWakeFinalizesBeyondOrdinaryRecoveryBatch(t *testing.T) {
	for _, persistFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("persistence_failure_%t", persistFails), func(t *testing.T) {
			base, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
			scope := requireAdmissionScope(t, store, store.CurrentAdmissionBaseline(), "backend-a")
			for index := range maxMaintenanceRecoveryCommandsPerBackendPass + 1 {
				command := prepareTenantMaintenance(t, store, scope, index,
					"tenant-aggregator", []byte("waiting for backend"))
				admission, err := store.beginMaintenanceCommand(command)
				require.NoError(t, err)
				_, err = store.acceptMaintenanceUpdate(maintenanceDelivery{claim: admission.Claim()})
				require.NoError(t, err)
			}
			payloads := &maintenanceProgressPayloads{}
			if persistFails {
				payloads.err = errors.New("payload persistence unavailable")
			}
			authority, err := base.coordinator.execution.MaintenanceCoordinator(payloads)
			require.NoError(t, err)
			application, err := authority.Application(nil, 0)
			require.NoError(t, err)
			request, err := NewMaintenanceApplicationRequest(mustMaintenanceID(t, maintenanceIDA), maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("confirmed candidate"))
			require.NoError(t, err)
			require.Equal(t, MaintenanceApplicationAccepted, application.Execute(t.Context(), request).Outcome())
			application.recoveryCursor["backend-a"] = maintenanceLease + "\x00" + request.id.String()
			require.NoError(t, applyMaintenanceCompletionForTest(t, authority, request.id, backend.CallbackStatusSuccess))
			<-application.CompletionChanged()
			err = application.RecoverPending(t.Context())
			if persistFails {
				require.ErrorContains(t, err, "payload persistence unavailable")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, payloads.writes, "the completed lease must be selected on its wake, outside the ordinary32-entry batch")
			require.Equal(t, maintenanceLease, payloads.lease)
			require.Empty(t, store.maintenanceChanged, "permanent persistence failures must not self-schedule a busy loop")
			if !persistFails {
				require.NotContains(t, base.coordinator.RuntimeController().PendingLeaseUUIDs(), maintenanceLease)
			}
			expectedConfirmed := float64(0)
			if persistFails {
				expectedConfirmed = 1
			}
			require.Equal(t, expectedConfirmed, testutil.ToFloat64(metrics.MaintenancePending.WithLabelValues(maintenancePayloadConfirmed.String())), "recovery must publish settlement or retained debt")
		})
	}
}

func TestMaintenanceTransactionCannotOverspendReservationBeforeCommit(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	baseline := requireAdmissionBaseline(t, store, "backend-a")
	scope := requireAdmissionScope(t, store, baseline, "backend-a")
	var commands []PreparedMaintenanceCommand
	for index := range maxPendingMaintenanceCommands {
		commands = append(commands, prepareTenantMaintenance(t, store, scope, index, "tenant-a", []byte("x")))
	}
	store.mu.Lock()
	err := store.updateMaintenanceAuthority(func(journal *maintenanceJournalTransaction) error {
		for _, command := range commands {
			encoded, err := encodeMaintenanceCommand(command.command, MaintenanceOutcomePending, store.now().UTC(), time.Time{})
			if err != nil {
				return err
			}
			if err := journal.write(encoded); err != nil {
				return err
			}
		}
		return nil
	})
	store.mu.Unlock()
	require.ErrorIs(t, err, ErrMaintenancePendingFull)
	for _, command := range commands {
		_, found, err := store.LookupMaintenanceCommand(command.command.leaseUUID, command.command.id)
		require.NoError(t, err)
		require.False(t, found, "the failed transaction must not publish any partial admission")
	}
	pressure, err := store.maintenancePressure()
	require.NoError(t, err)
	require.Empty(t, pressure)
	_, err = store.beginMaintenanceCommand(commands[0])
	require.NoError(t, err, "rolled back reservations must not spend durable capacity")
}

func TestMaintenanceOpenPreservesAggregatorReplayAndSettlement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placements.db")
	store, err := newStoreForTest(path, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	baseline := requireAdmissionBaseline(t, store, "backend-a")
	scope := requireAdmissionScope(t, store, baseline, "backend-a")
	var commands []PreparedMaintenanceCommand
	for index := range 32 {
		commands = append(commands, prepareTenantMaintenance(t, store, scope, index, "tenant-a", []byte("historical candidate")))
	}
	// Encode valid older-provider rows whose principal and lifecycle authority
	// are all present, and whose aggregate exceeds the removed per-tenant share.
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		pending, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		for _, prepared := range commands {
			command := prepared.command
			encoded, err := encodeMaintenanceCommand(command, MaintenanceOutcomePending, store.now().UTC(), time.Time{})
			if err != nil {
				return err
			}
			if err := records.Put(maintenanceReceiptKey(command.leaseUUID, command.id), encoded); err != nil {
				return err
			}
			if err := pending.Put([]byte(command.leaseUUID), []byte(command.id.String())); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, store.Close())
	reopened, err := newStoreForTest(path, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	for _, previous := range commands {
		prepared, err := reopened.prepareMaintenanceCommand(previous.command.id, previous.command.leaseUUID, MaintenanceCommandUpdate, previous.command.payload)
		require.NoError(t, err)
		admission, err := reopened.beginMaintenanceCommand(prepared)
		require.NoError(t, err, "exact older work retains its admitted authority")
		require.True(t, admission.Pending())
		work, err := reopened.acceptMaintenanceUpdate(maintenanceDelivery{claim: admission.Claim()})
		require.NoError(t, err, "overbudget work can still advance its durable phase")
		accepted := work.(acceptedMaintenanceUpdate)
		require.NoError(t, reopened.settleMaintenancePhase(accepted.claim,
			maintenanceSettlement{outcome: MaintenanceOutcomeExecutionFailed}, maintenanceCompletionOutstanding))
	}
	pressure, err := reopened.maintenancePressure()
	require.NoError(t, err)
	for _, phase := range pressure {
		require.Zero(t, phase.count)
		require.Zero(t, phase.bytes)
		require.True(t, phase.oldest.IsZero())
	}
}

func TestMaintenanceReservationClassificationRequiresExactSourceRefusal(t *testing.T) {
	refusal := maintenanceReservationRefusal{reservation: maintenanceCountReserved}
	require.Equal(t, MaintenanceApplicationCapacityReserved, resultForMaintenanceBeginError(refusal).Outcome())
	for _, err := range []error{
		maintenanceReservationRefusal{},
		errors.New(refusal.Error()),
		fmt.Errorf("foreign call: %w", refusal),
		errors.Join(refusal, errors.New("authority unavailable")),
	} {
		require.Equal(t, MaintenanceApplicationServiceUnavailable, resultForMaintenanceBeginError(err).Outcome())
	}
}

func TestMaintenanceRecoveryDecodesOnlySelectedDurableCommands(t *testing.T) {
	base, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
	scope := requireAdmissionScope(t, store, store.CurrentAdmissionBaseline(), "backend-a")
	var last PreparedMaintenanceCommand
	for index := range maxMaintenanceRecoveryCommandsPerBackendPass + 1 {
		last = prepareTenantMaintenance(t, store, scope, index, "tenant-test", []byte("waiting"))
		admission, err := store.beginMaintenanceCommand(last)
		require.NoError(t, err)
		_, err = store.acceptMaintenanceUpdate(maintenanceDelivery{claim: admission.Claim()})
		require.NoError(t, err)
	}
	application, err := base.Application(nil, 0)
	require.NoError(t, err)
	// Corrupt an undispatched row after startup validation. A scheduling
	// snapshot must not parse its payload; selecting that exact command must.
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		_, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		return records.Put(maintenanceReceiptKey(last.command.leaseUUID, last.command.id), []byte("invalid receipt"))
	}))
	require.NoError(t, application.RecoverPending(t.Context()), "unselected payloads must not be decoded under the store read lock")
	require.ErrorIs(t, application.RecoverPending(t.Context()), ErrMaintenanceJournalCorrupt, "the next selected batch must validate its exact durable receipt")
}
