package maintenance

import (
	"context"
	"errors"
	"net/http"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func TestAmbiguousMaintenanceCircuitOpenReplayRemainsPending(t *testing.T) {
	for _, kind := range []Kind{KindRestart, KindUpdate} {
		t.Run(string(kind), func(t *testing.T) {
			store, _ := newPlacementAuthority(t, testLeaseA)
			client, calls := causalMaintenanceBackendForTest(t, store, http.StatusInternalServerError,
				`{"error":"response lost after accepting"}`)
			payloads := &fakePayloads{}
			service, runtime := newTestServiceWithRuntime(t, store, client, payloads, testLeaseA)
			command := Command{ID: requestID(t, testRequestA), LeaseUUID: testLeaseA,
				Tenant: testTenant, Kind: kind}
			if kind == KindUpdate {
				command.Payload = []byte(`services: {app: {image: alpine}}`)
			}
			for range 6 {
				require.Equal(t, OutcomeServiceUnavailable, service.Execute(t.Context(), command).Outcome())
			}
			require.Equal(t, 5, calls(), "the sixth retry must exercise the real HTTP circuit breaker")
			receipt, found, err := store.LookupMaintenanceCommand(testLeaseA, command.ID)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, placement.MaintenanceOutcomePending, receipt.Outcome(),
				"not sending a later retry cannot disprove earlier delivery")
			assert.Equal(t, command.Payload, receipt.Command().Payload())
			assertMaintenanceLaneHeld(t, runtime, testLeaseA)
		})
	}
}

func TestFirstMaintenanceAttemptWithOpenCircuitRemainsPendingAndRecovers(t *testing.T) {
	for _, kind := range []Kind{KindRestart, KindUpdate} {
		t.Run(string(kind), func(t *testing.T) {
			store, _ := newPlacementAuthority(t, testLeaseA)
			unavailable, calls := causalMaintenanceBackendForTest(t, store, http.StatusInternalServerError,
				`{"error":"backend is unavailable"}`)
			available, acceptedCalls := causalMaintenanceBackendForTest(t, store, http.StatusAccepted, "")
			routing := map[string]backend.Backend{"backend-a": unavailable}
			coordinator, runtime := maintenanceCoordinatorWithRuntimeForTest(t, store, testChain(testLeaseA),
				fakeRouter{backends: routing}, &fakePayloads{})
			service, err := NewService(Config{Coordinator: coordinator})
			require.NoError(t, err)
			// Read failures can open the shared backend breaker before this
			// command has ever been admitted or dispatched.
			for range 5 {
				_, err := unavailable.GetLoadStats(t.Context())
				require.Error(t, err)
			}
			require.Equal(t, 5, calls())
			command := Command{ID: requestID(t, testRequestA), LeaseUUID: testLeaseA,
				Tenant: testTenant, Kind: kind}
			if kind == KindUpdate {
				command.Payload = []byte("deferred update payload")
			}
			for range 2 {
				require.Equal(t, OutcomeServiceUnavailable, service.Execute(t.Context(), command).Outcome())
			}
			assert.Equal(t, 5, calls(), "neither the first attempt nor its retry reached the backend")
			receipt, found, err := store.LookupMaintenanceCommand(testLeaseA, command.ID)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, placement.MaintenanceOutcomePending, receipt.Outcome())
			assertMaintenanceLaneHeld(t, runtime, testLeaseA)
			other := command
			other.ID = requestID(t, testRequestB)
			assert.Equal(t, OutcomeAlreadyInProgress, service.Execute(t.Context(), other).Outcome(),
				"a new idempotency key conflicts while the first command remains recoverable")
			// Replacing the failed connection models the backend becoming
			// available without relying on a circuit-breaker wall-clock timeout.
			routing["backend-a"] = available
			require.NoError(t, service.RecoverPending(t.Context()))
			assert.Equal(t, 1, acceptedCalls(), "recovery executes the deferred command without a new tenant request")
			assertMaintenanceLaneReleased(t, runtime, testLeaseA)
			assert.Equal(t, OutcomeAccepted, service.Execute(t.Context(), command).Outcome())
			assert.Equal(t, 1, acceptedCalls(), "same-key terminal replay is read-only")
		})
	}
}

func TestAcceptedUpdateRecoveryIsIndependentOfBackendAvailability(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	acceptedClient, acceptedCalls := causalMaintenanceBackendForTest(t, store, http.StatusAccepted, "")
	unavailableClient, unavailableCalls := causalMaintenanceBackendForTest(t, store, http.StatusInternalServerError,
		`{"error":"unavailable"}`)
	routing := map[string]backend.Backend{"backend-a": acceptedClient}
	payloads := &fakePayloads{failures: 1}
	coordinator, runtime := maintenanceCoordinatorWithRuntimeForTest(t, store, testChain(testLeaseA),
		fakeRouter{backends: routing}, payloads)
	service, err := NewService(Config{Coordinator: coordinator})
	require.NoError(t, err)
	command := Command{ID: requestID(t, testRequestA), LeaseUUID: testLeaseA, Tenant: testTenant,
		Kind: KindUpdate, Payload: []byte(`services: {app: {image: alpine:3.23}}`)}
	require.Equal(t, OutcomeInternalFailure, service.Execute(t.Context(), command).Outcome())
	require.Equal(t, 1, acceptedCalls())
	routing["backend-a"] = unavailableClient
	require.NoError(t, service.RecoverPending(t.Context()))
	receipt, found, err := store.LookupMaintenanceCommand(testLeaseA, command.ID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomeAccepted, receipt.Outcome())
	assert.Zero(t, unavailableCalls(), "accepted work has no backend dispatch capability")
	assert.Equal(t, command.Payload, payloads.lastWrite())
	assertMaintenanceLaneReleased(t, runtime, testLeaseA)
}

func TestAcceptedUpdateTerminalLeaseRecoverySurvivesFailedSettlementAndRestart(t *testing.T) {
	store, path := newPlacementAuthority(t, testLeaseA)
	var state = billingtypes.LEASE_STATE_ACTIVE
	reader := chainFunc(func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		if state == billingtypes.LEASE_STATE_CLOSED {
			// Crash after the exact terminal observation and before its journal
			// transaction. The old phase must remain recoverable on reopen.
			require.NoError(t, store.Close())
		}
		return &billingtypes.Lease{Uuid: leaseUUID, Tenant: testTenant,
			ProviderUuid: testProviderUUID, State: state}, nil
	})
	var writes int
	brokenPayloads := maintenancePayloadWriterFunc(func(string, []byte) error {
		writes++
		return errors.New("payload authority is unavailable")
	})
	firstBackend := &fakeBackend{}
	coordinator, runtime := maintenanceCoordinatorWithRuntimeForTest(t, store, reader,
		fakeRouter{backend: firstBackend}, brokenPayloads)
	service, err := NewService(Config{Coordinator: coordinator})
	require.NoError(t, err)
	command := Command{ID: requestID(t, testRequestA), LeaseUUID: testLeaseA, Tenant: testTenant,
		Kind: KindUpdate, Payload: []byte("accepted payload before terminal observation")}
	require.Equal(t, OutcomeInternalFailure, service.Execute(t.Context(), command).Outcome())
	state = billingtypes.LEASE_STATE_CLOSED
	require.Error(t, service.RecoverPending(t.Context()), "failed terminal receipt must keep its live fence")
	assertMaintenanceLaneHeld(t, runtime, testLeaseA)

	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	receipt, found, err := reopened.LookupMaintenanceCommand(testLeaseA, command.ID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, placement.MaintenanceOutcomePending, receipt.Outcome())
	require.Equal(t, command.Payload, receipt.Command().Payload())
	endedChain := testChain(testLeaseA)
	endedChain.leases[testLeaseA].State = billingtypes.LEASE_STATE_CLOSED
	nextBackend := &fakeBackend{}
	nextCoordinator, nextRuntime := maintenanceCoordinatorWithRuntimeForTest(t, reopened, endedChain,
		fakeRouter{backend: nextBackend}, brokenPayloads)
	recovered, err := NewService(Config{Coordinator: nextCoordinator})
	require.NoError(t, err)
	require.NoError(t, recovered.RecoverPending(t.Context()))
	receipt, found, err = reopened.LookupMaintenanceCommand(testLeaseA, command.ID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomeLeaseEnded, receipt.Outcome())
	assert.Empty(t, receipt.Command().Payload())
	assert.Zero(t, nextBackend.updateCount())
	assert.Equal(t, 1, writes, "terminal recovery does not depend on repairing the payload authority")
	assertMaintenanceLaneReleased(t, nextRuntime, testLeaseA)
}

type maintenancePayloadWriterFunc func(string, []byte) error

func (write maintenancePayloadWriterFunc) OverwritePayload(leaseUUID string, data []byte) error {
	return write(leaseUUID, data)
}

func TestAcceptedUpdatePayloadCommitBeforeReceiptFailureRecoversLocally(t *testing.T) {
	store, path := newPlacementAuthority(t, testLeaseA)
	backendClient := &fakeBackend{}
	var committed []byte
	payloads := maintenancePayloadWriterFunc(func(leaseUUID string, data []byte) error {
		require.Equal(t, testLeaseA, leaseUUID)
		committed = append([]byte(nil), data...)
		// Simulate process loss after the payload transaction and before the
		// maintenance terminal receipt can commit.
		return store.Close()
	})
	service := newTestService(t, store, backendClient, payloads, testLeaseA)
	command := Command{ID: requestID(t, testRequestA), LeaseUUID: testLeaseA, Tenant: testTenant,
		Kind: KindUpdate, Payload: []byte("accepted exact bytes")}
	require.Equal(t, OutcomeInternalFailure, service.Execute(t.Context(), command).Outcome())
	require.Equal(t, command.Payload, committed)
	require.Equal(t, 1, backendClient.updateCount())

	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	nextBackend := &fakeBackend{}
	nextPayloads := &fakePayloads{}
	recovered, runtime := newTestServiceWithRuntime(t, reopened, nextBackend, nextPayloads, testLeaseA)
	require.NoError(t, recovered.RecoverPending(t.Context()))
	assert.Zero(t, nextBackend.updateCount())
	assert.Equal(t, committed, nextPayloads.lastWrite(), "only the exact accepted payload may be replayed")
	assertMaintenanceLaneReleased(t, runtime, testLeaseA)
}

func TestUpdateAcceptanceJournalFailurePreservesDeliveryOnRestart(t *testing.T) {
	store, path := newPlacementAuthority(t, testLeaseA)
	backendClient := &fakeBackend{update: func(backend.UpdateRequest) error {
		// Backend acceptance precedes the provider's durable phase transition.
		require.NoError(t, store.Close())
		return nil
	}}
	payloads := &fakePayloads{}
	service := newTestService(t, store, backendClient, payloads, testLeaseA)
	command := Command{ID: requestID(t, testRequestA), LeaseUUID: testLeaseA, Tenant: testTenant,
		Kind: KindUpdate, Payload: []byte("accepted before phase commit")}
	require.Equal(t, OutcomeInternalFailure, service.Execute(t.Context(), command).Outcome())
	assert.Zero(t, payloads.writeCount(), "uncommitted acceptance cannot mint local completion authority")

	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	client, calls := causalMaintenanceBackendForTest(t, reopened, http.StatusInternalServerError, `{"error":"unavailable"}`)
	recovered, runtime := newTestServiceWithRuntime(t, reopened, client, payloads, testLeaseA)
	for range 6 {
		_ = recovered.RecoverPending(t.Context())
	}
	require.Equal(t, 5, calls())
	receipt, found, err := reopened.LookupMaintenanceCommand(testLeaseA, command.ID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomePending, receipt.Outcome())
	assert.Equal(t, command.Payload, receipt.Command().Payload())
	assertMaintenanceLaneHeld(t, runtime, testLeaseA)
}
