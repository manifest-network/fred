package placement

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/maintenanceid"
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
			seedMaintenancePressure(t, store, template.command, count)
			fresh := testMaintenanceCommand(t, authority, maintenanceIDB, MaintenanceCommandUpdate, []byte("fresh bytes"))
			_, err := store.beginMaintenanceCommand(fresh)
			require.ErrorIs(t, err, ErrMaintenancePendingFull)
			require.ErrorContains(t, err, reason)
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
