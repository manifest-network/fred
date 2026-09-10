package maintenance

import (
	"net/http"
	"testing"

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
