package maintenance

import (
	"context"
	"crypto/sha256"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

type maintenanceReplayBackend struct {
	*fakeBackend
	replayed []byte
}

func (client *maintenanceReplayBackend) Provision(_ context.Context, request backend.ProvisionRequest) error {
	client.replayed = append([]byte(nil), request.Payload...)
	return nil
}

// Preserve the committed payload through an accepted update that rolls back,
// then use the actual reconciliation dispatch boundary after a later failure.
// A bad update must never become the bytes sent on that subsequent provision.
func TestRolledBackUpdateThenFailureReprovisionsCommittedPayload(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	defer func() { _ = store.Close() }()
	client := &maintenanceReplayBackend{fakeBackend: &fakeBackend{}}
	oldPayload := []byte(`{"services":{"app":{"image":"example.invalid/app:old"}}}`)
	payloads := &fakePayloads{writes: [][]byte{oldPayload}}
	chain := testChain(testLeaseA)
	chain.leases[testLeaseA].Items = []billingtypes.LeaseItem{{SkuUuid: "sku-a", Quantity: 1, ServiceName: "app"}}
	coordinator, runtime := maintenanceCoordinatorWithRuntimeForTest(t, store, chain,
		fakeRouter{backend: client}, payloads)
	service, err := NewService(Config{Coordinator: coordinator})
	require.NoError(t, err)
	command := Command{
		ID: requestID(t, testRequestA), LeaseUUID: testLeaseA, Tenant: testTenant,
		Kind: KindUpdate, Payload: []byte(`{"services":{"app":{"image":"example.invalid/app:bad"}}}`),
	}
	require.Equal(t, OutcomeAccepted, service.Execute(t.Context(), command).Outcome())
	require.Equal(t, oldPayload, payloads.lastWrite(), "202 must leave the committed payload intact")
	completeUpdateForTest(t, store, command.ID, backend.CallbackStatusFailed)
	require.NoError(t, service.RecoverPending(t.Context()))
	assertMaintenanceLaneReleased(t, runtime, testLeaseA)
	record, found, err := store.LookupMaintenanceCommand(testLeaseA, command.ID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, placement.MaintenanceOutcomeExecutionFailed, record.Outcome())
	require.Equal(t, 1, payloads.writeCount(), "rollback must preserve the pre-update payload")

	fixtureValue, ok := maintenanceCallbackFixtures.Load(store)
	require.True(t, ok)
	fixture := fixtureValue.(maintenanceCallbackFixture)
	reconciliation, err := fixture.execution.ReconciliationCoordinator(nil, nil)
	require.NoError(t, err)
	inventory := fixture.inventory.backends["backend-a"]
	inventory.mu.Lock()
	require.Len(t, inventory.provisions, 1)
	inventory.provisions[0].Status = backend.ProvisionStatusFailed
	inventory.mu.Unlock()
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	provisions, err := sweep.CollectProvisionInventory(t.Context(), "backend-a")
	require.NoError(t, err)
	retentions, err := sweep.CollectRetentionInventory(t.Context(), "backend-a")
	require.NoError(t, err)
	_, err = sweep.RecordBackendInventory(provisions, retentions)
	require.NoError(t, err)
	require.NoError(t, sweep.SealInventory())
	projected, err := sweep.Project(placement.ReconciliationProjection{
		Placements: map[string]string{testLeaseA: "backend-a"},
	})
	require.NoError(t, err)
	action, disposition, err := projected.ObserveLiveAction(t.Context(), testLeaseA)
	require.NoError(t, err)
	require.Equal(t, placement.ReconciliationObservationReady, disposition)
	defer reconciliation.ReleaseAction(action)
	replay := payloads.lastWrite()
	hash := sha256.Sum256(replay)
	fingerprint, err := placement.NewPayloadFingerprint(hash[:])
	require.NoError(t, err)
	result := reconciliation.Provision(t.Context(), action, replay, fingerprint)
	require.NoError(t, result.Err())
	require.True(t, result.Dispatch().CallAccepted())
	require.Equal(t, oldPayload, client.replayed)
	require.NotEqual(t, command.Payload, client.replayed)
}
