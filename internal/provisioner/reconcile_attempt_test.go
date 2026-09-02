package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

const (
	redeliveryTarget = "lease-redelivery-target"
	redeliverySource = "lease-redelivery-source"
	redeliveryTenant = "tenant-redelivery"
)

func redeliveryLease(uuid string, state billingtypes.LeaseState) billingtypes.Lease {
	return billingtypes.Lease{
		Uuid:         uuid,
		ProviderUuid: placementstore.ProviderUUID,
		Tenant:       redeliveryTenant,
		State:        state,
		Items: []billingtypes.LeaseItem{{
			SkuUuid: "sku-redelivery", Quantity: 2, ServiceName: "app",
		}},
	}
}

func redeliveryRequestSnapshot(
	t testing.TB,
	lease billingtypes.Lease,
) placement.BackendRequestSnapshot {
	t.Helper()
	snapshot, err := placement.NewBackendRequestSnapshot(
		lease.Tenant, lease.ProviderUuid, ExtractLeaseItems(&lease),
	)
	require.NoError(t, err)
	return snapshot
}

func beginRedeliveryProvisionAttempt(
	t testing.TB,
	store PlacementAuthorityStore,
	backendName string,
	operationID operation.OperationID,
	fingerprint placement.PayloadFingerprint,
) placement.AttemptToken {
	t.Helper()
	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	return beginTestNewPlacementAttemptWithSnapshot(
		t, store, redeliveryTarget, backendName, operationID, fingerprint,
		redeliveryRequestSnapshot(t, target),
	)
}

func redeliveryChain(
	live []billingtypes.Lease,
	extra map[string]billingtypes.Lease,
) *chaintest.MockClient {
	byID := make(map[string]billingtypes.Lease, len(live)+len(extra))
	var pending, active []billingtypes.Lease
	for _, lease := range live {
		byID[lease.Uuid] = lease
		switch lease.State {
		case billingtypes.LEASE_STATE_PENDING:
			pending = append(pending, lease)
		case billingtypes.LEASE_STATE_ACTIVE:
			active = append(active, lease)
		}
	}
	for uuid, lease := range extra {
		byID[uuid] = lease
	}
	return &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return append([]billingtypes.Lease(nil), pending...), nil
		},
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return append([]billingtypes.Lease(nil), active...), nil
		},
		GetLeaseFunc: func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			lease, ok := byID[leaseUUID]
			if !ok {
				return nil, nil
			}
			copy := lease
			return &copy, nil
		},
	}
}

func redeliveryRouter(
	t *testing.T,
	backends ...backend.Backend,
) *backend.Router {
	t.Helper()
	entries := make([]backend.BackendEntry, len(backends))
	for index, backendClient := range backends {
		entries[index] = backend.BackendEntry{
			Backend: backendClient,
			Match:   backend.MatchCriteria{SKUs: []string{"sku-redelivery"}},
		}
	}
	entries[len(entries)-1].IsDefault = true
	router, err := backend.NewRouter(backend.RouterConfig{Backends: entries})
	require.NoError(t, err)
	return router
}

func redeliveryReconciler(
	t *testing.T,
	store *placement.Store,
	router *backend.Router,
	chainClient ReconcilerChainClient,
	registry *operation.Registry,
) *Reconciler {
	return redeliveryReconcilerWithPayload(t, store, router, chainClient, registry, nil)
}

func redeliveryReconcilerWithPayload(
	t *testing.T,
	store *placement.Store,
	router *backend.Router,
	chainClient ReconcilerChainClient,
	registry *operation.Registry,
	payloadStore *payload.Store,
) *Reconciler {
	t.Helper()
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(payloadStore),
		operations:          registry,
	}
	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID:    placementstore.ProviderUUID,
		CallbackBaseURL: "https://provider.example/callbacks/provision",
	}, chainClient, noopAck, router, runtime, store)
	require.NoError(t, err)
	return reconciler
}

func requireCallbackOperationID(t *testing.T, callbackURL string) operation.OperationID {
	t.Helper()
	parsed, err := url.Parse(callbackURL)
	require.NoError(t, err)
	id, present, err := operation.ParseQuery(parsed.Query())
	require.NoError(t, err)
	require.True(t, present)
	return id
}

func TestReconciler_RedeliversNeverReceivedProvisionWithExactIdentity(t *testing.T) {
	backendA := &mockReconcilerBackend{
		name:         "backend-a",
		provisionErr: errors.New("connection reset before request reached backend"),
	}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("2f399495-6c7a-4e04-86b4-eb1d18372201")
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
	)
	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{
			redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING),
		}, nil),
		registry,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.provisionCalls, 1)
	firstRequest := backendA.provisionCalls[0]
	assert.Equal(t, operationID, requireCallbackOperationID(t, firstRequest.CallbackURL))
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
	assert.Zero(t, registry.Count())

	backendA.provisionErr = nil
	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.provisionCalls, 2)
	secondRequest := backendA.provisionCalls[1]
	assert.Equal(t, firstRequest, secondRequest,
		"every retry must reproduce the complete request, not merely its backend")
	assert.Equal(t, operationID, requireCallbackOperationID(t, secondRequest.CallbackURL))
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State())
	recovered, exists := registry.Lookup(redeliveryTarget)
	require.True(t, exists)
	assert.Equal(t, operationID, recovered.ID)
	assert.Equal(t, operation.KindProvision, recovered.Kind)
	assert.Equal(t, backendA.Name(), recovered.Backend)

	callback := registry.TryClaimCallback(redeliveryTarget, operationID)
	require.True(t, callback.Claimed(),
		"the backend callback must settle through the recovered registry record")
	assert.True(t, registry.FinishSettlement(callback.Claim()))
}

func TestReconciler_RedeliversProvisionAfterProviderRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	store1, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	armTestPlacementTopology(t, store1, backendTopologyNames(router))
	operationID, err := operation.ParseID("91bb340a-ed31-470f-84d7-dcd4fa955468")
	require.NoError(t, err)
	originalTarget := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	originalTarget.Items[0].CustomDomain = "original.example.test"
	beginTestNewPlacementAttemptWithSnapshot(
		t, store1, redeliveryTarget, backendA.Name(), operationID,
		placement.PayloadFingerprint{}, redeliveryRequestSnapshot(t, originalTarget),
	)
	require.NoError(t, store1.Close())

	store2, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store2.Close()) })
	reopened := store2.Lookup(redeliveryTarget)
	require.True(t, reopened.AttemptMetadata().Valid())
	assert.Equal(t, operation.KindProvision, reopened.AttemptMetadata().Kind())
	assert.Equal(t, operationID, reopened.AttemptMetadata().OperationID())
	durableCallbacks := reopened.AttemptMetadata().CallbackPair()
	require.NotEmpty(t, durableCallbacks.OperationURL())
	assert.NotContains(t, durableCallbacks.OperationURL(), "provider.example",
		"the attempt predates the restarted provider's callback-base configuration")
	registry := operation.NewRegistry()
	mutatedTarget := originalTarget
	mutatedTarget.Items = append([]billingtypes.LeaseItem(nil), originalTarget.Items...)
	mutatedTarget.Items[0].CustomDomain = "changed-after-dispatch.example.test"
	reconciler := redeliveryReconciler(
		t, store2, router,
		redeliveryChain([]billingtypes.Lease{mutatedTarget}, nil),
		registry,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.provisionCalls, 1)
	assert.Equal(t, operationID,
		requireCallbackOperationID(t, backendA.provisionCalls[0].CallbackURL))
	assert.Equal(t, durableCallbacks.OperationURL(), backendA.provisionCalls[0].CallbackURL)
	assert.Equal(t, durableCallbacks.LifecycleURL(), backendA.provisionCalls[0].LifecycleCallbackURL)
	require.Len(t, backendA.provisionCalls[0].Items, 1)
	assert.Equal(t, "original.example.test", backendA.provisionCalls[0].Items[0].CustomDomain,
		"mutable chain fields cannot rewrite an already-authorized exact request")
	recovered, exists := registry.Lookup(redeliveryTarget)
	require.True(t, exists)
	assert.Equal(t, operationID, recovered.ID)
}

func TestFleet_ProviderRestartRedeliversPersistedAttemptAcrossHTTPBoundary(t *testing.T) {
	fleet := newFleet(t, fleetOptions{backendCount: 2})
	fleet.addLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING, "sku-redelivery")
	leaseUUID := fleetLeaseUUID(redeliveryTarget)
	armTestPlacementTopology(t, fleet.placement, backendTopologyNames(fleet.router))
	operationID, err := operation.ParseID("f9ff2480-313f-48aa-a004-dda0221ad67f")
	require.NoError(t, err)
	scope, err := fleet.placement.ScopeAdmission(
		fleet.placement.CurrentAdmissionBaseline(), []string{"backend-1"},
	)
	require.NoError(t, err)
	_, begun, err := fleet.placement.BeginNewAttempt(
		scope, leaseUUID, "backend-1", operationID,
		placement.PayloadFingerprint{},
		redeliveryRequestSnapshot(t, *chaintest.NewMockLeaseWithSKU(
			leaseUUID, "tenant-1", fleet.providerUUID,
			billingtypes.LEASE_STATE_PENDING, "sku-redelivery",
		)),
		testPlacementCallbackPair(t, operationID),
	)
	require.NoError(t, err)
	require.True(t, begun)
	require.Zero(t, fleet.tracker.Operations().Count(),
		"the simulated process exits in the write-ahead window before registration")

	require.NoError(t, fleet.placement.Close())
	reopened, err := placementstore.NewStore(
		fleet.placementPath,
		placement.WithClock(func() time.Time { return time.Now().Add(-fleet.placementAge) }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	fleet.placement = reopened
	fleet.tracker = &fleetReconcilerTracker{
		testOperationRegistry: newTestOperationRegistry(),
		payloads:              fleet.payloads,
	}
	fleet.reconciler, err = NewReconciler(
		fleet.reconcilerCfg,
		fleet.chain,
		fleet.acknowledger,
		fleet.router,
		fleet.tracker,
		fleet.placement,
	)
	require.NoError(t, err)

	require.NoError(t, fleet.sweep())
	request, exists := fleet.backendAt(1).provisionRequest(leaseUUID)
	require.True(t, exists)
	assert.Equal(t, operationID, requireCallbackOperationID(t, request.CallbackURL))
	assert.Equal(t, leaseUUID, request.LeaseUUID)
	assert.Equal(t, fleet.providerUUID, request.ProviderUUID)
	assert.Zero(t, fleet.backendAt(2).provisionCount(leaseUUID),
		"redelivery cannot load-balance to a different HTTP backend")
	recovered, exists := fleet.tracker.Operations().Lookup(leaseUUID)
	require.True(t, exists)
	assert.Equal(t, operationID, recovered.ID)
	assert.Equal(t, "backend-1", recovered.Backend)
	assert.Equal(t, placement.StateConfirmed, fleet.placement.Lookup(leaseUUID).State())
}

func TestReconciler_AcceptedOldAttemptGetsFreshCallbackTimeoutWindow(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	store, err := placementstore.NewStore(
		filepath.Join(t.TempDir(), "placements.db"),
		placement.WithClock(func() time.Time { return time.Now().Add(-24 * time.Hour) }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("074b5679-30fb-44d8-a204-676094d826f2")
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
	)
	oldAttempt := store.Lookup(redeliveryTarget)
	require.Less(t, oldAttempt.SetAt, time.Now().Add(-23*time.Hour))

	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{
			redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING),
		}, nil),
		registry,
	)
	recoveryStarted := time.Now()
	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	recovered, exists := registry.Lookup(redeliveryTarget)
	require.True(t, exists)
	assert.False(t, recovered.StartedAt.Before(recoveryStarted),
		"redelivery acceptance starts a new volatile callback-wait window")
	rejecter := &mockRejecter{rejectFn: func(
		context.Context, []string, string,
	) (uint64, []string, error) {
		t.Fatal("freshly recovered operation must not be rejected as timed out")
		return 0, nil, nil
	}}
	NewTimeoutChecker(TimeoutCheckerConfig{
		Operations: registry,
		Rejecter:   rejecter,
		Timeout:    time.Hour,
	}).CheckOnce(t.Context())
	assert.True(t, registry.Contains(redeliveryTarget))
}

func TestReconciler_RedeliveryRebuildsExactPersistedPayload(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("77220174-9267-43b8-8284-4a2181a39f01")
	require.NoError(t, err)
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, payloadStore.Close()) })
	payloadBytes := []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`)
	require.True(t, payloadStore.Store(redeliveryTarget, payloadBytes))
	payloadHash := sha256.Sum256(payloadBytes)
	fingerprint, err := placement.NewPayloadFingerprint(payloadHash[:])
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, fingerprint,
	)
	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	target.MetaHash = payloadHash[:]
	reconciler := redeliveryReconcilerWithPayload(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{target}, nil),
		operation.NewRegistry(), payloadStore,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.provisionCalls, 1)
	request := backendA.provisionCalls[0]
	assert.Equal(t, payloadBytes, request.Payload)
	assert.Equal(t, hex.EncodeToString(payloadHash[:]), request.PayloadHash)
	assert.Equal(t, operationID, requireCallbackOperationID(t, request.CallbackURL))
}

func TestReconciler_RedeliveryRejectsPayloadDifferentFromDurableAttempt(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("2c8409f1-b041-43fd-a46f-6ccacde8f354")
	require.NoError(t, err)
	authorizedPayload := []byte(`{"version":"attempt-authorized"}`)
	authorizedHash := sha256.Sum256(authorizedPayload)
	fingerprint, err := placement.NewPayloadFingerprint(authorizedHash[:])
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, fingerprint,
	)
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, payloadStore.Close()) })
	require.True(t, payloadStore.Store(redeliveryTarget, []byte(`{"version":"persisted"}`)))
	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	target.MetaHash = authorizedHash[:]
	reconciler := redeliveryReconcilerWithPayload(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{target}, nil),
		operation.NewRegistry(), payloadStore,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Empty(t, backendA.provisionCalls,
		"payload-store state cannot override the exact durable attempt fingerprint")
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_RedeliveryUsesUpdatedActivePayloadFingerprint(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	seedTestTypedConfirmedPlacements(t, store, backendTopologyNames(router), map[string]string{
		redeliveryTarget: backendA.Name(),
	})
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, payloadStore.Close()) })
	updatedPayload := []byte(`{"version":"updated-after-create"}`)
	require.NoError(t, payloadStore.Put(redeliveryTarget, updatedPayload))
	updatedHash := sha256.Sum256(updatedPayload)
	fingerprint, err := placement.NewPayloadFingerprint(updatedHash[:])
	require.NoError(t, err)
	operationID, err := operation.ParseID("673d344a-40f5-48aa-a552-032bb9799797")
	require.NoError(t, err)
	current := store.Lookup(redeliveryTarget)
	_, applied, err := store.BeginOwnedAttempt(
		store.CurrentAdmissionBaseline(), current.RecordRevision(), backendA.Name(), operationID,
		fingerprint,
		redeliveryRequestSnapshot(t, redeliveryLease(
			redeliveryTarget, billingtypes.LEASE_STATE_ACTIVE,
		)),
		testPlacementCallbackPair(t, operationID),
	)
	require.NoError(t, err)
	require.True(t, applied)
	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_ACTIVE)
	createPayloadHash := sha256.Sum256([]byte(`{"version":"original-create"}`))
	target.MetaHash = createPayloadHash[:]
	reconciler := redeliveryReconcilerWithPayload(
		t, store, router, redeliveryChain([]billingtypes.Lease{target}, nil),
		operation.NewRegistry(), payloadStore,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.provisionCalls, 1)
	request := backendA.provisionCalls[0]
	assert.Equal(t, updatedPayload, request.Payload)
	assert.Equal(t, hex.EncodeToString(updatedHash[:]), request.PayloadHash)
	assert.NotEqual(t, hex.EncodeToString(createPayloadHash[:]), request.PayloadHash)
}

func TestReconciler_RedeliveryRefusalAndLocalTransportGatesSettleConservatively(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantState placement.State
	}{
		{name: "validation refusal", err: backend.ErrValidation, wantState: placement.StateAbsent},
		{name: "coded capacity refusal", err: backend.ErrCapacityRefused, wantState: placement.StateAbsent},
		{name: "circuit open", err: backend.ErrCircuitOpen, wantState: placement.StateAttempting},
		{name: "identity unbound", err: backend.ErrBackendStorageIdentityUnbound, wantState: placement.StateAttempting},
		{name: "identity mismatch", err: backend.ErrBackendStorageIdentityMismatch, wantState: placement.StateAttempting},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backendA := &mockReconcilerBackend{name: "backend-a", provisionErr: test.err}
			router := redeliveryRouter(t, backendA)
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, backendTopologyNames(router))
			operationID, err := operation.ParseID("c623a0f9-b6cc-4497-871c-75cf50fe24d5")
			require.NoError(t, err)
			beginRedeliveryProvisionAttempt(
				t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
			)
			reconciler := redeliveryReconciler(
				t, store, router,
				redeliveryChain([]billingtypes.Lease{
					redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING),
				}, nil),
				operation.NewRegistry(),
			)

			require.NoError(t, reconciler.ReconcileAll(t.Context()))
			assert.Equal(t, test.wantState, store.Lookup(redeliveryTarget).State())
		})
	}
}

func TestReconciler_RedeliveryRequiresExactTargetProvider(t *testing.T) {
	for _, test := range []struct {
		name         string
		providerUUID string
	}{
		{name: "missing provider"},
		{name: "different provider", providerUUID: "a6d6790d-d04b-48bd-ad91-675cb7a4b2ed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			backendA := &mockReconcilerBackend{name: "backend-a"}
			router := redeliveryRouter(t, backendA)
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, backendTopologyNames(router))
			operationID, err := operation.ParseID("37be74da-bcc8-4d49-85c0-480b997c196b")
			require.NoError(t, err)
			beginRedeliveryProvisionAttempt(
				t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
			)
			target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
			target.ProviderUuid = test.providerUUID
			registry := operation.NewRegistry()
			reconciler := redeliveryReconciler(
				t, store, router, redeliveryChain([]billingtypes.Lease{target}, nil), registry,
			)
			record := store.Lookup(redeliveryTarget)
			result := reconciler.redeliverPlacementAttempt(
				t.Context(), redeliveryTarget, record,
				record.AttemptMetadata(), registry.Snapshot(),
			)

			assert.Equal(t, attemptRedeliveryDeferred, result.outcome)
			require.Error(t, result.err)
			assert.Empty(t, backendA.provisionCalls)
			assert.Equal(t, placement.StateAttempting,
				store.Lookup(redeliveryTarget).State())
			assert.Zero(t, registry.Count())
		})
	}
}

func TestReconciler_RedeliversRestoreWithDurableSourceAndRecoveredKind(t *testing.T) {
	backendA := &mockReconcilerBackend{
		name:          "backend-a",
		restoreAccept: true,
		retentions:    []backend.RetainedLease{{LeaseUUID: redeliverySource}},
	}
	router := redeliveryRouter(t, backendA)
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	storeBeforeRestart, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	seedTestTypedConfirmedPlacements(t, storeBeforeRestart, backendTopologyNames(router), map[string]string{
		redeliverySource: backendA.Name(),
	})
	operationID, err := operation.ParseID("ed00a025-e09c-44c1-af07-46621cf919c1")
	require.NoError(t, err)
	originalTarget := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	originalTarget.Items[0].CustomDomain = "restore-original.example.test"
	claim, err := storeBeforeRestart.BeginAuthorizedRestore(
		storeBeforeRestart.CurrentAdmissionBaseline(),
		storeBeforeRestart.Lookup(redeliverySource).RecordRevision(),
		redeliveryTarget,
		operationID,
		redeliveryRequestSnapshot(t, originalTarget),
		testPlacementCallbackPair(t, operationID),
	)
	require.NoError(t, err)
	_, err = storeBeforeRestart.AbandonRestore(claim)
	require.NoError(t, err)
	metadata := storeBeforeRestart.Lookup(redeliveryTarget).AttemptMetadata()
	require.True(t, metadata.Valid())
	assert.Equal(t, operation.KindRestore, metadata.Kind())
	assert.Equal(t, redeliverySource, metadata.RestoreSourceLeaseUUID())
	require.NoError(t, storeBeforeRestart.Close())

	store, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	metadata = store.Lookup(redeliveryTarget).AttemptMetadata()
	require.True(t, metadata.Valid())
	assert.Equal(t, operation.KindRestore, metadata.Kind())
	assert.Equal(t, redeliverySource, metadata.RestoreSourceLeaseUUID())

	target := originalTarget
	target.Items = append([]billingtypes.LeaseItem(nil), originalTarget.Items...)
	target.Items[0].CustomDomain = "restore-changed-after-dispatch.example.test"
	source := redeliveryLease(redeliverySource, billingtypes.LEASE_STATE_CLOSED)
	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{target}, map[string]billingtypes.Lease{
			redeliverySource: source,
		}),
		registry,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.restoreCalls, 1)
	request := backendA.restoreCalls[0]
	assert.Equal(t, redeliveryTarget, request.LeaseUUID)
	assert.Equal(t, redeliverySource, request.FromLeaseUUID)
	require.Len(t, request.Items, 1)
	assert.Equal(t, "restore-original.example.test", request.Items[0].CustomDomain)
	assert.Equal(t, operationID, requireCallbackOperationID(t, request.CallbackURL))
	recovered, exists := registry.Lookup(redeliveryTarget)
	require.True(t, exists)
	assert.Equal(t, operationID, recovered.ID)
	assert.Equal(t, operation.KindRestore, recovered.Kind)
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_RedeliversRestoreAfterAuthorizedSourceWasPruned(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a", restoreAccept: true}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	seedTestTypedConfirmedPlacements(t, store, backendTopologyNames(router), map[string]string{
		redeliverySource: backendA.Name(),
	})
	operationID, err := operation.ParseID("4c5f5a83-dce8-4acb-96d9-610e73e66944")
	require.NoError(t, err)
	claim, err := store.BeginAuthorizedRestore(
		store.CurrentAdmissionBaseline(),
		store.Lookup(redeliverySource).RecordRevision(),
		redeliveryTarget,
		operationID,
		redeliveryRequestSnapshot(t, redeliveryLease(
			redeliveryTarget, billingtypes.LEASE_STATE_PENDING,
		)),
		testPlacementCallbackPair(t, operationID),
	)
	require.NoError(t, err)
	_, err = store.AbandonRestore(claim)
	require.NoError(t, err)
	deleted, err := store.DeleteRecord(store.Lookup(redeliverySource).RecordRevision())
	require.NoError(t, err)
	require.True(t, deleted)
	require.Equal(t, placement.StateAbsent, store.Lookup(redeliverySource).State())

	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(
		t, store, router, redeliveryChain([]billingtypes.Lease{target}, nil), registry,
	)
	record := store.Lookup(redeliveryTarget)
	result := reconciler.redeliverPlacementAttempt(
		t.Context(), redeliveryTarget, record, record.AttemptMetadata(), registry.Snapshot(),
	)
	require.Equal(t, attemptRedeliveryAccepted, result.outcome, result.err)
	require.Len(t, backendA.restoreCalls, 1)
	assert.Equal(t, redeliverySource, backendA.restoreCalls[0].FromLeaseUUID)
	assert.Equal(t, operationID,
		requireCallbackOperationID(t, backendA.restoreCalls[0].CallbackURL))
}

func TestReconciler_RestoreRedeliveryRejectsChangedSourceAuthority(t *testing.T) {
	tests := []struct {
		name           string
		mutateSource   func(*testing.T, *placement.Store)
		chainSource    billingtypes.Lease
		wantErrorMatch string
	}{
		{
			name: "wrong tenant",
			chainSource: func() billingtypes.Lease {
				lease := redeliveryLease(redeliverySource, billingtypes.LEASE_STATE_CLOSED)
				lease.Tenant = "another-tenant"
				return lease
			}(),
			wantErrorMatch: "tenant and provider",
		},
		{
			name: "wrong provider",
			chainSource: func() billingtypes.Lease {
				lease := redeliveryLease(redeliverySource, billingtypes.LEASE_STATE_CLOSED)
				lease.ProviderUuid = "b40f1a67-e1aa-4711-84d1-398dbca14c2f"
				return lease
			}(),
			wantErrorMatch: "tenant and provider",
		},
		{
			name: "source has pending operation",
			mutateSource: func(t *testing.T, store *placement.Store) {
				t.Helper()
				pendingID, err := operation.ParseID("374c9e1c-6c11-409f-bff6-b7de423276ba")
				require.NoError(t, err)
				_, applied, err := store.BeginOwnedAttempt(
					store.CurrentAdmissionBaseline(),
					store.Lookup(redeliverySource).RecordRevision(),
					"backend-a",
					pendingID,
					placement.PayloadFingerprint{},
					redeliveryRequestSnapshot(t, redeliveryLease(
						redeliverySource, billingtypes.LEASE_STATE_CLOSED,
					)),
					testPlacementCallbackPair(t, pendingID),
				)
				require.NoError(t, err)
				require.True(t, applied)
			},
			chainSource:    redeliveryLease(redeliverySource, billingtypes.LEASE_STATE_CLOSED),
			wantErrorMatch: "source placement",
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backendA := &mockReconcilerBackend{name: "backend-a", restoreAccept: true}
			router := redeliveryRouter(t, backendA)
			store := newTestPlacementAuthority(t)
			seedTestTypedConfirmedPlacements(t, store, backendTopologyNames(router), map[string]string{
				redeliverySource: backendA.Name(),
			})
			operationID, err := operation.ParseID(fmt.Sprintf(
				"74887a71-e917-46a5-bdfb-eef0ba8832%02d", index,
			))
			require.NoError(t, err)
			claim, err := store.BeginAuthorizedRestore(
				store.CurrentAdmissionBaseline(),
				store.Lookup(redeliverySource).RecordRevision(),
				redeliveryTarget,
				operationID,
				redeliveryRequestSnapshot(t, redeliveryLease(
					redeliveryTarget, billingtypes.LEASE_STATE_PENDING,
				)),
				testPlacementCallbackPair(t, operationID),
			)
			require.NoError(t, err)
			_, err = store.AbandonRestore(claim)
			require.NoError(t, err)
			if test.mutateSource != nil {
				test.mutateSource(t, store)
			}
			target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
			registry := operation.NewRegistry()
			reconciler := redeliveryReconciler(
				t, store, router,
				redeliveryChain([]billingtypes.Lease{target}, map[string]billingtypes.Lease{
					redeliverySource: test.chainSource,
				}),
				registry,
			)
			record := store.Lookup(redeliveryTarget)
			result := reconciler.redeliverPlacementAttempt(
				t.Context(), redeliveryTarget, record,
				record.AttemptMetadata(), registry.Snapshot(),
			)
			assert.Equal(t, attemptRedeliveryDeferred, result.outcome)
			require.Error(t, result.err)
			assert.Contains(t, result.err.Error(), test.wantErrorMatch)
			assert.Empty(t, backendA.restoreCalls)
			assert.Equal(t, placement.StateAttempting,
				store.Lookup(redeliveryTarget).State())
			assert.Zero(t, registry.Count())
		})
	}
}

func TestReconciler_MalformedAttemptMetadataIsQuarantinedWithoutDispatch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	storeBeforeCorruption, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	armTestPlacementTopology(t, storeBeforeCorruption, backendTopologyNames(router))
	operationID, err := operation.ParseID("196c55d6-6f70-4d8f-89d7-d20e841d89a5")
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, storeBeforeCorruption, backendA.Name(), operationID,
		placement.PayloadFingerprint{},
	)
	require.NoError(t, storeBeforeCorruption.Close())

	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("placements")).Put(
			[]byte(redeliveryTarget),
			[]byte(`{"attempt":"backend-a","set_at":"2026-08-25T15:00:00Z","revision":99}`),
		)
	}))
	require.NoError(t, db.Close())
	store, err := placement.OpenStore(dbPath, placementstore.ProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.Equal(t, placement.StateUnusable, store.Lookup(redeliveryTarget).State())

	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{
			redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING),
		}, nil),
		operation.NewRegistry(),
	)
	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Empty(t, backendA.provisionCalls)
	assert.Empty(t, backendA.restoreCalls)
	assert.Equal(t, placement.StateUnusable, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_CallbackContendingWithRedeliveryRetriesThenSettles(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	backendA := &mockReconcilerBackend{
		name: "backend-a",
		onProvision: func() {
			entered <- struct{}{}
			<-release
		},
	}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("d6f72c45-a274-400f-8f22-82601bc88cfa")
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
	)
	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(
		t, store, router, redeliveryChain([]billingtypes.Lease{target}, nil), registry,
	)

	done := make(chan error, 1)
	go func() { done <- reconciler.ReconcileAll(context.Background()) }()
	<-entered
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: registry,
		Placement:  store,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			return true, "tx-ack", nil
		}),
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   redeliveryTarget,
		Status:      backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})
	firstErr := service.HandleCallback(t.Context(), command)
	require.ErrorIs(t, firstErr, errCallbackRecoveryLeaseBusy,
		"inline callback contention must remain retryable")

	close(release)
	released = true
	require.NoError(t, <-done)
	require.True(t, registry.Contains(redeliveryTarget))
	require.NoError(t, service.HandleCallback(t.Context(), command))
	assert.False(t, registry.Contains(redeliveryTarget))
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_DownAttemptBackendDoesNotPauseHealthyBackendAdmission(t *testing.T) {
	backendA := &mockReconcilerBackend{
		name:         "backend-a",
		listErr:      errors.New("backend-a inventory outage"),
		provisionErr: backend.ErrCircuitOpen,
	}
	backendB := &mockReconcilerBackend{name: "backend-b"}
	router := redeliveryRouter(t, backendA, backendB)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("3c73fb65-d781-42d5-ac2a-94c015b08f4b")
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
	)
	healthyLease := redeliveryLease("lease-healthy-backend", billingtypes.LEASE_STATE_PENDING)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{
			redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING),
			healthyLease,
		}, nil),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
	require.Len(t, backendA.provisionCalls, 1)
	require.Len(t, backendB.provisionCalls, 1)
	assert.Equal(t, healthyLease.Uuid, backendB.provisionCalls[0].LeaseUUID)
	assert.NotEqual(t, healthyLease.Uuid, backendA.provisionCalls[0].LeaseUUID,
		"the exact attempt backend can never receive another lease through fallback")
}

func TestReconciler_TerminalAttemptConvergesByExactTeardown(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("95a6dd1f-0239-459f-80c0-d28148db6522")
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
	)
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Equal(t, []string{redeliveryTarget}, backendA.deprovisionCalls)
	record := store.Lookup(redeliveryTarget)
	assert.Equal(t, placement.StateConfirmed, record.State())
	assert.Equal(t, backendA.Name(), record.Backend,
		"successful teardown conservatively preserves possible retained-data affinity")
	assert.Empty(t, record.Attempt)
	assert.Equal(t, operationID.String(), store.CurrentLifecycle(redeliveryTarget).ID().String(),
		"queued operation/lifecycle callbacks retain their exact promoted generation")
}

func TestReconciler_TerminalAttemptAmbiguousTeardownRetainsAuthority(t *testing.T) {
	backendA := &mockReconcilerBackend{
		name:           "backend-a",
		deprovisionErr: errors.New("connection reset after teardown request"),
	}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("2a7053d8-1945-4c69-ad2d-5c51a69a0830")
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
	)
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_REJECTED)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Equal(t, []string{redeliveryTarget}, backendA.deprovisionCalls)
	record := store.Lookup(redeliveryTarget)
	assert.Equal(t, placement.StateAttempting, record.State())
	assert.Equal(t, operationID, record.AttemptMetadata().OperationID())
}

func TestReconciler_TerminalAttemptRequiresExactTargetSnapshot(t *testing.T) {
	for index, test := range []struct {
		name   string
		mutate func(*billingtypes.Lease)
	}{
		{
			name: "tenant changed",
			mutate: func(lease *billingtypes.Lease) {
				lease.Tenant = "another-tenant"
			},
		},
		{
			name: "provider changed",
			mutate: func(lease *billingtypes.Lease) {
				lease.ProviderUuid = "a6d6790d-d04b-48bd-ad91-675cb7a4b2ed"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backendA := &mockReconcilerBackend{name: "backend-a"}
			router := redeliveryRouter(t, backendA)
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, backendTopologyNames(router))
			operationID, err := operation.ParseID(fmt.Sprintf(
				"a55e1173-6b4d-4762-8081-e3c4b3c04d%02d", index,
			))
			require.NoError(t, err)
			beginRedeliveryProvisionAttempt(
				t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
			)
			terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
			test.mutate(&terminal)
			reconciler := redeliveryReconciler(
				t, store, router,
				redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
				operation.NewRegistry(),
			)

			require.NoError(t, reconciler.ReconcileAll(t.Context()))
			assert.Empty(t, backendA.deprovisionCalls,
				"mismatched chain identity cannot authorize destructive teardown")
			assert.Empty(t, backendA.provisionCalls)
			assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
		})
	}
}

func TestReconciler_DownTerminalAttemptBackendDoesNotBlockHealthyAdmission(t *testing.T) {
	backendA := &mockReconcilerBackend{
		name:           "backend-a",
		listErr:        errors.New("backend-a inventory outage"),
		deprovisionErr: backend.ErrCircuitOpen,
	}
	backendB := &mockReconcilerBackend{name: "backend-b"}
	router := redeliveryRouter(t, backendA, backendB)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("702db9f6-4494-4528-9b1b-24d2fd24c47d")
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
	)
	healthy := redeliveryLease("lease-healthy-terminal-peer", billingtypes.LEASE_STATE_PENDING)
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_EXPIRED)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(
			[]billingtypes.Lease{healthy},
			map[string]billingtypes.Lease{redeliveryTarget: terminal},
		),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendB.provisionCalls, 1)
	assert.Equal(t, healthy.Uuid, backendB.provisionCalls[0].LeaseUUID)
	assert.Equal(t, []string{redeliveryTarget}, backendA.deprovisionCalls)
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_TerminalAttemptPreservesConfirmedOwnerAffinity(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	seedTestTypedConfirmedPlacements(t, store, backendTopologyNames(router), map[string]string{
		redeliveryTarget: backendA.Name(),
	})
	operationID, err := operation.ParseID("99d64c34-94d3-483e-ab02-9c266c1595e6")
	require.NoError(t, err)
	_, applied, err := store.BeginOwnedAttempt(
		store.CurrentAdmissionBaseline(),
		store.Lookup(redeliveryTarget).RecordRevision(),
		backendA.Name(),
		operationID,
		placement.PayloadFingerprint{},
		redeliveryRequestSnapshot(t, redeliveryLease(
			redeliveryTarget, billingtypes.LEASE_STATE_ACTIVE,
		)),
		testPlacementCallbackPair(t, operationID),
	)
	require.NoError(t, err)
	require.True(t, applied)
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	record := store.Lookup(redeliveryTarget)
	assert.Equal(t, placement.StateConfirmed, record.State())
	assert.Equal(t, backendA.Name(), record.Backend)
	assert.Empty(t, record.Attempt)
	assert.Equal(t, operationID.String(), store.CurrentLifecycle(redeliveryTarget).ID().String())
}

func TestReconciler_TerminalAttemptClaimsFenceCallbackAndInventory(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	backendA := &mockReconcilerBackend{
		name: "backend-a",
		onDeprovision: func() {
			entered <- struct{}{}
			<-release
		},
	}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("27bed961-34b4-42b0-92d9-a6343a5da364")
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
	)
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
	chainClient := redeliveryChain(
		nil, map[string]billingtypes.Lease{redeliveryTarget: terminal},
	)
	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(t, store, router, chainClient, registry)

	done := make(chan error, 1)
	go func() { done <- reconciler.ReconcileAll(context.Background()) }()
	<-entered
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: registry,
		Placement:  store,
		Chain:      chainClient,
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   redeliveryTarget,
		Status:      backend.CallbackStatusFailed,
		Error:       "operation preempted by deprovision",
		OperationID: operationID.String(),
	})
	require.ErrorIs(t, service.HandleCallback(t.Context(), command), errCallbackRecoveryLeaseBusy)

	lifecycleID, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)
	fence := store.BeginInventorySession()
	projection, err := store.ProjectInventory(fence, placement.InventoryProjection{
		Placements: map[string]string{redeliveryTarget: backendA.Name()},
		Lifecycles: map[string]placement.LifecycleObservation{
			redeliveryTarget: {Kind: placement.LifecycleObservationTyped, ID: lifecycleID},
		},
	})
	store.EndInventorySession(fence)
	require.NoError(t, err)
	assert.Contains(t, projection.Fenced, redeliveryTarget)
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())

	close(release)
	released = true
	require.NoError(t, <-done)
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State())
	require.NoError(t, service.HandleCallback(t.Context(), command),
		"the bundled backend's queued exact failure must drain after claim release")
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State(),
		"terminal callback cannot discard conservative retained-data affinity")
}

func TestReconciler_TerminalRestoreAttemptClaimsSourceDuringTeardown(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	backendA := &mockReconcilerBackend{
		name: "backend-a",
		onDeprovision: func() {
			entered <- struct{}{}
			<-release
		},
	}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	seedTestTypedConfirmedPlacements(t, store, backendTopologyNames(router), map[string]string{
		redeliverySource: backendA.Name(),
	})
	operationID, err := operation.ParseID("a7b393b9-a272-4664-99eb-53bb66461bf2")
	require.NoError(t, err)
	claim, err := store.BeginAuthorizedRestore(
		store.CurrentAdmissionBaseline(),
		store.Lookup(redeliverySource).RecordRevision(),
		redeliveryTarget,
		operationID,
		redeliveryRequestSnapshot(t, redeliveryLease(
			redeliveryTarget, billingtypes.LEASE_STATE_PENDING,
		)),
		testPlacementCallbackPair(t, operationID),
	)
	require.NoError(t, err)
	_, err = store.AbandonRestore(claim)
	require.NoError(t, err)
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
		registry,
	)

	done := make(chan error, 1)
	go func() { done <- reconciler.ReconcileAll(context.Background()) }()
	<-entered
	assert.Equal(t, operation.LeaseClaimBusy,
		registry.TryClaimLeaseNow(redeliverySource).Outcome(),
		"source lifecycle mutations must not cross restore teardown")
	assert.Equal(t, operation.LeaseClaimBusy,
		registry.TryClaimLeaseNow(redeliveryTarget).Outcome())

	close(release)
	released = true
	require.NoError(t, <-done)
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_ConflictedAttemptCannotAuthorizeBackendCall(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	backendB := &mockReconcilerBackend{name: "backend-b"}
	router := redeliveryRouter(t, backendA, backendB)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID, err := operation.ParseID("847853b8-7ca8-41b8-8cdd-edf4e6d076e4")
	require.NoError(t, err)
	beginRedeliveryProvisionAttempt(
		t, store, backendA.Name(), operationID, placement.PayloadFingerprint{},
	)
	projectTestPlacementInventory(t, store, backendTopologyNames(router), placement.InventoryProjection{
		Complete: true,
		Conflicts: map[string][]string{
			redeliveryTarget: {backendA.Name(), backendB.Name()},
		},
	})
	conflicted := store.Lookup(redeliveryTarget)
	require.Equal(t, placement.StateUnusable, conflicted.State())
	assert.False(t, conflicted.AttemptMetadata().Valid(),
		"unusable ownership must not expose an executable operation capability")
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Empty(t, backendA.provisionCalls)
	assert.Empty(t, backendB.provisionCalls)
	assert.Empty(t, backendA.deprovisionCalls)
	assert.Empty(t, backendB.deprovisionCalls)
	assert.Equal(t, placement.StateUnusable, store.Lookup(redeliveryTarget).State())
}
