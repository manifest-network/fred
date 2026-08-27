package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// mockBackendRouter implements BackendRouter for testing.
type mockBackendRouter struct {
	routeFn                  func(sku string) backend.Backend
	routeForProvisionFn      func(ctx context.Context, sku string, inFlight map[string]int) backend.Backend
	routeForProvisionAmongFn func(ctx context.Context, sku string, eligible map[string]struct{}, inFlight map[string]int) backend.Backend
	getBackendByNameFn       func(name string) backend.Backend
	backendsFn               func() []backend.Backend
}

func (m *mockBackendRouter) Route(sku string) backend.Backend {
	if m.routeFn != nil {
		return m.routeFn(sku)
	}
	return nil
}

func (m *mockBackendRouter) GetBackendByName(name string) backend.Backend {
	if m.getBackendByNameFn != nil {
		return m.getBackendByNameFn(name)
	}
	return nil
}

func (m *mockBackendRouter) RouteForProvision(ctx context.Context, sku string, inFlight map[string]int) backend.Backend {
	if m.routeForProvisionFn != nil {
		return m.routeForProvisionFn(ctx, sku, inFlight)
	}
	// Default: fall back to Route for backward-compatible tests
	return m.Route(sku)
}

func (m *mockBackendRouter) RouteForProvisionAmong(
	ctx context.Context,
	sku string,
	eligible map[string]struct{},
	inFlight map[string]int,
) backend.Backend {
	if m.routeForProvisionAmongFn != nil {
		return m.routeForProvisionAmongFn(ctx, sku, eligible, inFlight)
	}
	candidate := m.RouteForProvision(ctx, sku, inFlight)
	if candidate == nil {
		return nil
	}
	if _, ok := eligible[candidate.Name()]; !ok {
		return nil
	}
	return candidate
}

func (m *mockBackendRouter) Backends() []backend.Backend {
	if m.backendsFn != nil {
		return m.backendsFn()
	}
	if candidate := m.Route(""); candidate != nil {
		return []backend.Backend{candidate}
	}
	return nil
}

// --- StartProvisioning tests ---

func TestOrchestrator_StartProvisioning_Success(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items: []billingtypes.LeaseItem{
			{SkuUuid: "sku-1", Quantity: 1},
		},
	}

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{})
	require.NoError(t, err)

	// Verify backend was called
	mb.mu.Lock()
	require.Len(t, mb.provisionCalls, 1)
	req := mb.provisionCalls[0]
	mb.mu.Unlock()

	assert.Equal(t, "lease-1", req.LeaseUUID)
	assert.Equal(t, "tenant-a", req.Tenant)
	assert.Equal(t, "prov-1", req.ProviderUUID)
	tracked, exists := tracker.Operations().Lookup("lease-1")
	require.True(t, exists)
	wantCallbackURL, err := BuildCallbackURLForOperation("http://localhost:8080", tracked.ID)
	require.NoError(t, err)
	wantLifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(wantCallbackURL, "")
	require.NoError(t, err)
	assert.Equal(t, wantCallbackURL, req.CallbackURL)
	assert.Equal(t, wantLifecycleCallbackURL, req.LifecycleCallbackURL)
	assert.Nil(t, req.Payload)
	assert.Empty(t, req.PayloadHash)

	// Should be tracked
	assert.True(t, tracker.IsInFlight("lease-1"))
}

func TestOrchestrator_StartProvisioning_WithPayload(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	payload := []byte(`{"image":"nginx"}`)
	hash := hashPayload(payload)

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{
		Payload:     payload,
		PayloadHash: hash,
	})
	require.NoError(t, err)

	mb.mu.Lock()
	req := mb.provisionCalls[0]
	mb.mu.Unlock()

	assert.Equal(t, payload, req.Payload)
	assert.Equal(t, hash, req.PayloadHash)
}

func TestOrchestrator_StartProvisioning_PayloadHashRequiresBothFields(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	// PayloadHash set but no Payload => hash should not be included
	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{
		PayloadHash: "abc123",
	})
	require.NoError(t, err)

	mb.mu.Lock()
	req := mb.provisionCalls[0]
	mb.mu.Unlock()

	assert.Empty(t, req.PayloadHash, "PayloadHash should not be set when Payload is nil")
}

func TestOrchestrator_StartProvisioning_NoBackend(t *testing.T) {
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return nil },
	}
	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoBackendAvailable)
	assert.False(t, tracker.IsInFlight("lease-1"))
}

func TestOrchestrator_StartProvisioning_AlreadyInFlight(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")

	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{})
	assert.NoError(t, err, "should return nil for idempotent skip")

	// Backend should not have been called
	mb.mu.Lock()
	assert.Empty(t, mb.provisionCalls)
	mb.mu.Unlock()
}

func TestOrchestrator_StartProvisioning_BackendFails(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend", provisionErr: errors.New("backend down")}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProvisioningFailed)

	// Should have been untracked after failure
	assert.False(t, tracker.IsInFlight("lease-1"))
}

func TestOrchestrator_StartProvisioning_BackendPanicAbortsCallingAndRetainsAttempt(t *testing.T) {
	client := &controlledProvisionBackend{mockManagerBackend: &mockManagerBackend{name: "backend-a"}}
	client.provision = func(context.Context, backend.ProvisionRequest) error {
		panic("backend implementation fault")
	}
	router := &mockBackendRouter{routeFn: func(string) backend.Backend { return client }}
	registry := operation.NewRegistry()
	store := newTestPlacementAuthority(t)
	armTestPlacementAdmission(t, store, router)
	orchestrator, err := NewProvisionOrchestrator(
		"prov-1", "http://localhost:8080", router, registry, store, nil,
	)
	require.NoError(t, err)
	lease := &billingtypes.Lease{
		Uuid: "lease-1", Tenant: "tenant-a",
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err = startTestProvisioning(t, orchestrator, context.Background(), lease, ProvisionOpts{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backend Provision panicked")
	assert.False(t, registry.Contains(lease.Uuid),
		"a backend panic must not strand the operation in Calling")
	record := store.Lookup(lease.Uuid)
	assert.Equal(t, placement.StateAttempting, record.State(),
		"a panic is ambiguous and must retain durable write-ahead intent")
	assert.Equal(t, client.Name(), record.Attempt)
}

type beginCallRejectingProvisionOperations struct {
	ProvisionOperations
}

func (*beginCallRejectingProvisionOperations) BeginCall(operation.Initiation) bool {
	return false
}

func TestOrchestrator_StartProvisioning_BeginCallFailureRefusesUnsentAttempt(t *testing.T) {
	for _, test := range []struct {
		name      string
		confirmed bool
		wantState placement.State
	}{
		{name: "absent placement", wantState: placement.StateAbsent},
		{name: "confirmed owner is preserved", confirmed: true, wantState: placement.StateConfirmed},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &mockManagerBackend{name: "backend-a"}
			router := &mockBackendRouter{
				routeFn: func(string) backend.Backend { return client },
				getBackendByNameFn: func(name string) backend.Backend {
					if name == client.Name() {
						return client
					}
					return nil
				},
			}
			registry := operation.NewRegistry()
			operations := &beginCallRejectingProvisionOperations{ProvisionOperations: registry}
			store := newTestPlacementAuthority(t)
			if test.confirmed {
				seedTestConfirmedPlacements(t, store, []string{client.Name()}, map[string]string{
					"lease-1": client.Name(),
				})
			}
			armTestPlacementAdmission(t, store, router)
			orchestrator, err := NewProvisionOrchestrator(
				"prov-1", "http://localhost:8080", router, operations, store, nil,
			)
			require.NoError(t, err)
			lease := &billingtypes.Lease{
				Uuid: "lease-1", Tenant: "tenant-a",
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}

			err = startTestProvisioning(t, orchestrator, t.Context(), lease, ProvisionOpts{})

			require.Error(t, err)
			client.mu.Lock()
			assert.Empty(t, client.provisionCalls,
				"a failed call-phase transition must never contact the backend")
			client.mu.Unlock()
			assert.False(t, registry.Contains(lease.Uuid))
			record := store.Lookup(lease.Uuid)
			assert.Equal(t, test.wantState, record.State())
			assert.Empty(t, record.Attempt,
				"the exact unsent attempt must be refused before aborting the operation")
			assert.False(t, record.AttemptOperationID().Valid())
			if test.confirmed {
				assert.Equal(t, client.Name(), record.Backend)
			}
		})
	}
}

func TestOrchestrator_StartProvisioning_EventSinkPanicDoesNotPreventDispatch(t *testing.T) {
	client := &mockManagerBackend{name: "backend-a"}
	router := &mockBackendRouter{routeFn: func(string) backend.Backend { return client }}
	registry := operation.NewRegistry()
	store := newTestPlacementAuthority(t)
	armTestPlacementAdmission(t, store, router)
	panics := metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
		metrics.LifecycleEventProvisionStarting,
	)
	before := promtestutil.ToFloat64(panics)
	orchestrator, err := NewProvisionOrchestrator(
		"prov-1", "http://localhost:8080", router, registry, store,
		provisionStartEventSinkFunc(func(string) { panic("event sink fault") }),
	)
	require.NoError(t, err)
	lease := &billingtypes.Lease{
		Uuid: "lease-1", Tenant: "tenant-a",
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	require.NoError(t, startTestProvisioning(
		t, orchestrator, t.Context(), lease, ProvisionOpts{},
	))

	client.mu.Lock()
	require.Len(t, client.provisionCalls, 1,
		"best-effort event delivery must not suppress provision dispatch")
	client.mu.Unlock()
	record, exists := registry.Lookup(lease.Uuid)
	require.True(t, exists)
	assert.Equal(t, operation.PhaseActive, record.Phase,
		"the recovered panic must not strand the operation in Calling")
	assert.Equal(t, placement.StateConfirmed, store.Lookup(lease.Uuid).State())
	assert.Equal(t, before+1, promtestutil.ToFloat64(panics))
}

type controlledProvisionBackend struct {
	*mockManagerBackend
	provision func(context.Context, backend.ProvisionRequest) error
}

func (controlled *controlledProvisionBackend) Provision(
	ctx context.Context,
	request backend.ProvisionRequest,
) error {
	controlled.mu.Lock()
	controlled.provisionCalls = append(controlled.provisionCalls, request)
	controlled.mu.Unlock()
	if controlled.provision != nil {
		return controlled.provision(ctx, request)
	}
	return controlled.provisionErr
}

type blockingProvisionStartSink struct {
	entered chan struct{}
	release chan struct{}
	events  *[]backend.ProvisionStatus
}

type blockingBeginAttemptStore struct {
	PlacementAuthorityStore
	entered chan struct{}
	release chan struct{}
}

func (store *blockingBeginAttemptStore) BeginNewAttempt(
	scope placement.AdmissionScope,
	leaseUUID, backendName string,
	operationID operation.OperationID,
) (placement.AttemptToken, bool, error) {
	close(store.entered)
	<-store.release
	return store.PlacementAuthorityStore.BeginNewAttempt(
		scope, leaseUUID, backendName, operationID,
	)
}

func (sink *blockingProvisionStartSink) PublishProvisionStarting(string) {
	if sink.events != nil {
		*sink.events = append(*sink.events, backend.ProvisionStatusProvisioning)
	}
	if sink.entered != nil {
		close(sink.entered)
		<-sink.release
	}
}

func TestOrchestratorCloseCannotCrossPreparingOrCalling(t *testing.T) {
	newOrchestrator := func(
		t *testing.T,
		client backend.Backend,
		store PlacementAuthorityStore,
		sink ProvisionStartEventSink,
	) (*ProvisionOrchestrator, *operation.Registry) {
		t.Helper()
		if store == nil {
			store = newTestPlacementAuthority(t)
		}
		registry := operation.NewRegistry()
		router := &mockBackendRouter{
			routeFn: func(string) backend.Backend { return client },
			getBackendByNameFn: func(name string) backend.Backend {
				if name == client.Name() {
					return client
				}
				return nil
			},
		}
		armTestPlacementAdmission(t, store, router)
		orchestrator, err := NewProvisionOrchestrator(
			"prov-1", "http://localhost:8080", router, registry,
			store, sink,
		)
		require.NoError(t, err)
		return orchestrator, registry
	}
	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	t.Run("close while preparing durable attempt", func(t *testing.T) {
		client := &controlledProvisionBackend{mockManagerBackend: &mockManagerBackend{name: "backend-a"}}
		store := &blockingBeginAttemptStore{
			PlacementAuthorityStore: newTestPlacementAuthority(t),
			entered:                 make(chan struct{}),
			release:                 make(chan struct{}),
		}
		orchestrator, registry := newOrchestrator(t, client, store, &blockingProvisionStartSink{})
		started := make(chan error, 1)
		go func() {
			started <- startTestProvisioning(t, orchestrator, context.Background(), lease, ProvisionOpts{})
		}()
		<-store.entered

		record, exists := registry.Lookup(lease.Uuid)
		require.True(t, exists)
		assert.Equal(t, operation.PhasePreparing, record.Phase)
		require.Error(t, orchestrator.Deprovision(context.Background(), lease.Uuid))
		client.mu.Lock()
		assert.Empty(t, client.provisionCalls)
		assert.Empty(t, client.deprovisionCalls)
		client.mu.Unlock()

		close(store.release)
		require.NoError(t, <-started)
		require.NoError(t, orchestrator.Deprovision(context.Background(), lease.Uuid))
	})

	t.Run("close during backend call", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		client := &controlledProvisionBackend{mockManagerBackend: &mockManagerBackend{name: "backend-a"}}
		client.provision = func(context.Context, backend.ProvisionRequest) error {
			close(entered)
			<-release
			return nil
		}
		orchestrator, registry := newOrchestrator(t, client, nil, &blockingProvisionStartSink{})
		started := make(chan error, 1)
		go func() {
			started <- startTestProvisioning(t, orchestrator, context.Background(), lease, ProvisionOpts{})
		}()
		<-entered

		record, exists := registry.Lookup(lease.Uuid)
		require.True(t, exists)
		assert.Equal(t, operation.PhaseCalling, record.Phase)
		require.Error(t, orchestrator.Deprovision(context.Background(), lease.Uuid))
		client.mu.Lock()
		assert.Empty(t, client.deprovisionCalls)
		client.mu.Unlock()

		close(release)
		require.NoError(t, <-started)
		require.NoError(t, orchestrator.Deprovision(context.Background(), lease.Uuid),
			"the close retry may proceed only after Provision returned")
		client.mu.Lock()
		assert.Equal(t, []string{lease.Uuid}, client.deprovisionCalls)
		client.mu.Unlock()
	})
}

func TestOrchestratorInlineTerminalEventFollowsSingleStartingEvent(t *testing.T) {
	for _, terminal := range []backend.ProvisionStatus{
		backend.ProvisionStatusReady,
		backend.ProvisionStatusFailed,
	} {
		t.Run(string(terminal), func(t *testing.T) {
			var events []backend.ProvisionStatus
			registry := operation.NewRegistry()
			client := &controlledProvisionBackend{mockManagerBackend: &mockManagerBackend{name: "backend-a"}}
			client.provision = func(_ context.Context, request backend.ProvisionRequest) error {
				record, exists := registry.Lookup(request.LeaseUUID)
				require.True(t, exists)
				assert.Equal(t, operation.PhaseCalling, record.Phase)
				claim := registry.TryClaimCallback(request.LeaseUUID, record.ID)
				require.True(t, claim.Claimed())
				events = append(events, terminal)
				require.True(t, registry.FinishSettlement(claim.Claim()))
				return nil
			}
			router := &mockBackendRouter{routeFn: func(string) backend.Backend { return client }}
			store := newTestPlacementAuthority(t)
			armTestPlacementAdmission(t, store, router)
			orchestrator, err := NewProvisionOrchestrator(
				"prov-1", "http://localhost:8080", router, registry,
				store, &blockingProvisionStartSink{events: &events},
			)
			require.NoError(t, err)
			lease := &billingtypes.Lease{
				Uuid: "lease-1", Tenant: "tenant-a",
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}

			require.NoError(t, startTestProvisioning(t, orchestrator, context.Background(), lease, ProvisionOpts{}))
			assert.Equal(t, []backend.ProvisionStatus{backend.ProvisionStatusProvisioning, terminal}, events)
			assert.False(t, registry.Contains(lease.Uuid))
		})
	}
}

func TestOrchestratorInlineCallbackSettlesWithoutDeadlockOrStaleStart(t *testing.T) {
	tests := []struct {
		name       string
		callback   backend.CallbackStatus
		backendErr error
		wantStatus backend.ProvisionStatus
	}{
		{name: "success", callback: backend.CallbackStatusSuccess, wantStatus: backend.ProvisionStatusReady},
		{name: "failure", callback: backend.CallbackStatusFailed, wantStatus: backend.ProvisionStatusFailed},
		{
			name: "success callback overrides synchronous error", callback: backend.CallbackStatusSuccess,
			backendErr: errors.New("backend returned an error after callback"),
			wantStatus: backend.ProvisionStatusReady,
		},
		{
			name: "failure callback overrides synchronous error", callback: backend.CallbackStatusFailed,
			backendErr: errors.New("backend returned an error after callback"),
			wantStatus: backend.ProvisionStatusFailed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := operation.NewRegistry()
			store := newTestPlacementAuthority(t)
			client := &controlledProvisionBackend{mockManagerBackend: &mockManagerBackend{name: "backend-a"}}
			router := &mockBackendRouter{
				routeFn: func(string) backend.Backend { return client },
				getBackendByNameFn: func(name string) backend.Backend {
					if name == client.Name() {
						return client
					}
					return nil
				},
			}
			armTestPlacementAdmission(t, store, router)
			publisher := newMockPublisher()
			orchestrator, err := NewProvisionOrchestrator(
				"prov-1", "http://localhost:8080", router, registry, store,
				provisionStartEventSinkFunc(func(leaseUUID string) {
					publishLeaseStatusEvent(
						publisher, leaseUUID, backend.ProvisionStatusProvisioning, "",
					)
				}),
			)
			require.NoError(t, err)
			chainClient := &chaintest.MockClient{
				GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
					return &billingtypes.Lease{
						Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
					}, nil
				},
				RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
					return 1, []string{"tx-reject"}, nil
				},
			}
			handlers := composeTestHandlerSet(t, testHandlerDeps{
				ChainClient:   chainClient,
				Orchestrator:  orchestrator,
				Placement:     testCallbackPlacement(t, orchestrator),
				Operations:    registry,
				Acknowledger:  &mockAcknowledger{},
				Publisher:     publisher,
				BackendRouter: router,
			})
			client.provision = func(ctx context.Context, request backend.ProvisionRequest) error {
				record, exists := registry.Lookup(request.LeaseUUID)
				require.True(t, exists)
				callback := newCallbackMsg(t, backend.CallbackPayload{
					LeaseUUID:   request.LeaseUUID,
					Backend:     client.Name(),
					Status:      test.callback,
					OperationID: record.ID.String(),
				})
				callback.SetContext(ctx)
				require.NoError(t, handlers.HandleBackendCallback(callback))
				return test.backendErr
			}
			lease := &billingtypes.Lease{
				Uuid: "lease-1", Tenant: "tenant-a",
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			require.NoError(t, startTestProvisioning(t, orchestrator, ctx, lease, ProvisionOpts{}))
			assert.NoError(t, ctx.Err(), "inline callback must not wait on its initiating call")
			assert.False(t, registry.Contains(lease.Uuid))

			publisher.mu.Lock()
			messages := append([]*message.Message(nil), publisher.published[TopicLeaseEvent]...)
			publisher.mu.Unlock()
			require.Len(t, messages, 2)
			var events [2]backend.LeaseStatusEvent
			for index := range messages {
				require.NoError(t, json.Unmarshal(messages[index].Payload, &events[index]))
			}
			assert.Equal(t, backend.ProvisionStatusProvisioning, events[0].Status)
			assert.Equal(t, test.wantStatus, events[1].Status)
		})
	}
}

// --- Deprovision tests ---

func TestOrchestrator_Deprovision_ViaInFlightTracking(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == "test-backend" {
				return mb
			}
			return nil
		},
	}
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "test-backend")
	tracked, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)

	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()

	// Successful cleanup finishes only the exact generation it claimed.
	assert.False(t, tracker.IsInFlight("lease-1"))
	assert.False(t, tracker.UntrackInFlightIfOperationID("lease-1", tracked.OperationID))
}

type panickingDeprovisionBackend struct {
	*mockManagerBackend
}

func (b *panickingDeprovisionBackend) Deprovision(context.Context, string) error {
	panic("backend deprovision panic")
}

type synchronousCallbackDeprovisionBackend struct {
	*mockManagerBackend
	callback func(context.Context, string) error
}

func (b *synchronousCallbackDeprovisionBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	b.mu.Lock()
	b.deprovisionCalls = append(b.deprovisionCalls, leaseUUID)
	b.mu.Unlock()
	return b.callback(ctx, leaseUUID)
}

func TestOrchestrator_Deprovision_InlineCallbackDoesNotWaitForClaim(t *testing.T) {
	tests := []struct {
		name        string
		status      backend.CallbackStatus
		retained    bool
		callbackErr string
		wantStatus  backend.ProvisionStatus
	}{
		{
			name:       "retained success",
			status:     backend.CallbackStatusDeprovisioned,
			retained:   true,
			wantStatus: backend.ProvisionStatusRetained,
		},
		{
			name:        "cleanup exhausted",
			status:      backend.CallbackStatusFailed,
			callbackErr: "volume cleanup exhausted",
			wantStatus:  backend.ProvisionStatusFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mb := &synchronousCallbackDeprovisionBackend{
				mockManagerBackend: &mockManagerBackend{name: "test-backend"},
			}
			router := &mockBackendRouter{getBackendByNameFn: func(name string) backend.Backend {
				if name == mb.Name() {
					return mb
				}
				return nil
			}}
			tracker := NewInFlightTracker()
			generation, tracked := tracker.TryTrackInFlightWithOperationID(
				"lease-1", "tenant-a", testItems("sku-1"), mb.Name(),
			)
			require.True(t, tracked)
			orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)
			pub := newMockPublisher()
			hs := composeTestHandlerSet(t, testHandlerDeps{
				Orchestrator: orch,
				Placement:    testCallbackPlacement(t, orch),
				Tracker:      tracker,
				Publisher:    pub,
			})
			callbackMsg := newCallbackMsg(t, backend.CallbackPayload{
				LeaseUUID:   "lease-1",
				Backend:     mb.Name(),
				Status:      tt.status,
				Error:       tt.callbackErr,
				Retained:    tt.retained,
				OperationID: generation.String(),
			})
			mb.callback = func(ctx context.Context, _ string) error {
				callbackMsg.SetContext(ctx)
				return hs.HandleBackendCallback(callbackMsg)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			require.NoError(t, orch.Deprovision(ctx, "lease-1"))
			assert.NoError(t, ctx.Err(), "the inline callback must return before the RPC context expires")
			assert.False(t, tracker.IsInFlight("lease-1"),
				"the close path settles its exact generation after the backend returns")

			pub.mu.Lock()
			msgs := append([]*message.Message(nil), pub.published[TopicLeaseEvent]...)
			pub.mu.Unlock()
			require.Len(t, msgs, 1)
			var event backend.LeaseStatusEvent
			require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
			assert.Equal(t, tt.wantStatus, event.Status)
			if tt.callbackErr != "" {
				assert.Equal(t, tt.callbackErr, event.Error)
			}
		})
	}
}

func TestOrchestrator_Deprovision_InlineCallbackThenCandidateFailureMovesRetryStateOutOfTracker(t *testing.T) {
	callbackBackend := &synchronousCallbackDeprovisionBackend{
		mockManagerBackend: &mockManagerBackend{name: "backend-a"},
	}
	candidateErr := errors.New("backend-b temporarily unavailable")
	failingBackend := &mockManagerBackend{name: "backend-b", deprovisionErr: candidateErr}
	byName := map[string]backend.Backend{
		callbackBackend.Name(): callbackBackend,
		failingBackend.Name():  failingBackend,
	}
	router := &mockBackendRouter{getBackendByNameFn: func(name string) backend.Backend {
		return byName[name]
	}}
	tracker := NewInFlightTracker()
	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-a", testItems("sku-1"), callbackBackend.Name(),
	)
	require.True(t, tracked)
	placements := &mockPlacementStore{}
	_, fenced, err := placements.SetConflictsIfNotNewer(
		map[string][]string{"lease-1": {callbackBackend.Name(), failingBackend.Name()}},
		placements.SnapshotRevision(),
	)
	require.NoError(t, err)
	require.Empty(t, fenced)

	orch := newTestProvisionOrchestrator(t,
		"prov-1", "http://localhost:8080", router, tracker, placements,
	)
	ackCalls := 0
	pub := newMockPublisher()
	hs := composeTestHandlerSet(t, testHandlerDeps{
		Orchestrator: orch,
		Placement:    testCallbackPlacement(t, orch),
		Tracker:      tracker,
		Publisher:    pub,
		Acknowledger: &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
			ackCalls++
			return true, "tx", nil
		}},
	})
	callbackMsg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Backend:     callbackBackend.Name(),
		Status:      backend.CallbackStatusSuccess,
		OperationID: generation.String(),
	})
	callbackDeliveries := 0
	callbackBackend.callback = func(ctx context.Context, _ string) error {
		// Model the one completion callback from the provision that the close
		// overtook. The idempotent close retry does not manufacture a second one.
		if callbackDeliveries > 0 {
			return nil
		}
		callbackDeliveries++
		callbackMsg.SetContext(ctx)
		return hs.HandleBackendCallback(callbackMsg)
	}

	deprovisionOwnedSuccessesBefore := promtestutil.ToFloat64(
		metrics.CallbackDeprovisionOwnedSuccessTotal,
	)
	err = orch.Deprovision(context.Background(), "lease-1")
	require.ErrorIs(t, err, ErrDeprovisionFailed)
	require.ErrorIs(t, err, candidateErr)
	assert.Equal(t, 1, callbackDeliveries)
	assert.Equal(t, deprovisionOwnedSuccessesBefore+1,
		promtestutil.ToFloat64(metrics.CallbackDeprovisionOwnedSuccessTotal),
		"a swallowed provision-success callback must remain observable")
	assert.False(t, tracker.IsInFlight("lease-1"),
		"a failed close must not leave ordinary provision load/timeout state behind")
	assert.Equal(t, []string{failingBackend.Name()},
		orch.rememberedDeprovisionCandidates("lease-1"),
		"the close retry must retain only candidates whose teardown is unresolved")
	require.NoError(t, hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Backend:     callbackBackend.Name(),
		Status:      backend.CallbackStatusSuccess,
		OperationID: generation.String(),
	})))
	assert.Zero(t, ackCalls,
		"a callback delivered after failed close ownership must not acknowledge")
	pub.mu.Lock()
	lateEvents := append([]*message.Message(nil), pub.published[TopicLeaseEvent]...)
	pub.mu.Unlock()
	assert.Empty(t, lateEvents,
		"a token-present callback cannot publish after its exact operation is gone")

	failingBackend.mu.Lock()
	failingBackend.deprovisionErr = nil
	failingBackend.mu.Unlock()
	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))
	assert.False(t, tracker.IsInFlight("lease-1"),
		"a successful close retry must leave no ordinary provision entry")
	assert.Empty(t, orch.rememberedDeprovisionCandidates("lease-1"),
		"successful teardown must retire process-local retry candidates")

	callbackBackend.mu.Lock()
	assert.Equal(t, []string{"lease-1", "lease-1"}, callbackBackend.deprovisionCalls)
	callbackBackend.mu.Unlock()
	failingBackend.mu.Lock()
	assert.Equal(t, []string{"lease-1", "lease-1"}, failingBackend.deprovisionCalls)
	failingBackend.mu.Unlock()
}

func TestOrchestrator_Deprovision_PanicReleasesExactGenerationClaim(t *testing.T) {
	mb := &panickingDeprovisionBackend{mockManagerBackend: &mockManagerBackend{name: "test-backend"}}
	router := &mockBackendRouter{getBackendByNameFn: func(name string) backend.Backend {
		if name == mb.Name() {
			return mb
		}
		return nil
	}}
	tracker := NewInFlightTracker()
	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-a", testItems("sku-1"), mb.Name(),
	)
	require.True(t, tracked)
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	assert.PanicsWithValue(t, "backend deprovision panic", func() {
		_ = orch.Deprovision(context.Background(), "lease-1")
	})
	current, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)
	assert.Equal(t, generation, current.OperationID)
	_, claimed := tracker.TryClaimInFlight("lease-1", generation)
	require.True(t, claimed, "panic unwinding must release the exact generation claim")
	require.True(t, tracker.ReleaseInFlightClaim("lease-1", generation))
}

func TestOrchestrator_Deprovision_ContendedGenerationFailsClosed(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{getBackendByNameFn: func(name string) backend.Backend {
		if name == mb.Name() {
			return mb
		}
		return nil
	}}
	tracker := NewInFlightTracker()
	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-a", testItems("sku-1"), mb.Name(),
	)
	require.True(t, tracked)
	_, claimed := tracker.TryClaimInFlight("lease-1", generation)
	require.True(t, claimed)
	t.Cleanup(func() { tracker.ReleaseInFlightClaim("lease-1", generation) })
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.ErrorIs(t, err, ErrDeprovisionFailed)
	mb.mu.Lock()
	assert.Empty(t, mb.deprovisionCalls, "deprovision must not proceed without owning the in-flight generation")
	mb.mu.Unlock()
	current, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)
	assert.Equal(t, generation, current.OperationID)
}

func TestOrchestrator_Deprovision_FallbackAllBackends(t *testing.T) {
	mb1 := &mockManagerBackend{name: "b1"}
	mb2 := &mockManagerBackend{name: "b2"}
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{mb1, mb2} },
	}
	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	mb1.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb1.deprovisionCalls)
	mb1.mu.Unlock()

	mb2.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb2.deprovisionCalls)
	mb2.mu.Unlock()
}

func TestOrchestrator_Deprovision_AllBackendsFail(t *testing.T) {
	mb1 := &mockManagerBackend{name: "b1", deprovisionErr: errors.New("fail")}
	mb2 := &mockManagerBackend{name: "b2", deprovisionErr: errors.New("fail")}
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{mb1, mb2} },
	}
	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDeprovisionFailed)
}

func TestOrchestrator_Deprovision_PartialBackendSuccessStillFails(t *testing.T) {
	backendErr := errors.New("fail")
	mb1 := &mockManagerBackend{name: "b1", deprovisionErr: backendErr}
	mb2 := &mockManagerBackend{name: "b2"}
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{mb1, mb2} },
	}
	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.ErrorIs(t, err, ErrDeprovisionFailed)
	assert.ErrorIs(t, err, backendErr)
}

func TestOrchestrator_Deprovision_UnresolvedPlacement_PartialSweepFails(t *testing.T) {
	for _, tt := range []struct {
		name string
		ps   *mockPlacementStore
	}{
		{name: "absent", ps: &mockPlacementStore{}},
		{
			name: "unusable",
			// A timestamp without Backend or Attempt is structurally present but
			// unusable, matching placement.Placement.State semantics.
			ps: &mockPlacementStore{setAt: map[string]time.Time{"lease-1": time.Now()}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			possibleHolderErr := errors.New("possible holder unavailable")
			possibleHolder := &mockManagerBackend{name: "backend-a", deprovisionErr: possibleHolderErr}
			noopPeer := &mockManagerBackend{name: "backend-b"}
			router := &mockBackendRouter{
				backendsFn: func() []backend.Backend { return []backend.Backend{possibleHolder, noopPeer} },
			}
			orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, NewInFlightTracker(), tt.ps)

			err := orch.Deprovision(context.Background(), "lease-1")
			require.ErrorIs(t, err, ErrDeprovisionFailed)
			assert.ErrorIs(t, err, possibleHolderErr,
				"a peer's no-op success must not mask failure on the possible holder")
		})
	}
}

func TestOrchestrator_Deprovision_UnusablePlacement_AllSuccessfulSweepFailsClosed(t *testing.T) {
	backendA := &mockManagerBackend{name: "backend-a"}
	backendB := &mockManagerBackend{name: "backend-b"}
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{backendA, backendB} },
	}
	// This models a conflict record written by a pre-candidate build: durable
	// evidence exists, but it does not identify the complete historical owner set.
	placements := &mockPlacementStore{
		conflicts: map[string]bool{"lease-1": true},
		setAt:     map[string]time.Time{"lease-1": time.Now()},
	}
	orch := newTestProvisionOrchestrator(t,
		"prov-1", "http://localhost:8080", router, NewInFlightTracker(), placements,
	)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.ErrorIs(t, err, ErrDeprovisionFailed)
	require.ErrorIs(t, err, ErrPlacementUnresolvable,
		"successful no-ops on today's router cannot account for an unknown former owner")

	backendA.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, backendA.deprovisionCalls)
	backendA.mu.Unlock()
	backendB.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, backendB.deprovisionCalls)
	backendB.mu.Unlock()
}

func TestOrchestrator_Deprovision_KnownConflictRequiresEveryNamedBackend(t *testing.T) {
	configured := &mockManagerBackend{name: "backend-b"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == configured.name {
				return configured
			}
			return nil
		},
		backendsFn: func() []backend.Backend { return []backend.Backend{configured} },
	}
	placements := &mockPlacementStore{}
	_, fenced, err := placements.SetConflictsIfNotNewer(map[string][]string{
		"lease-1": {"backend-a", "backend-b"},
	}, placements.SnapshotRevision())
	require.NoError(t, err)
	require.Empty(t, fenced)
	orch := newTestProvisionOrchestrator(t,
		"prov-1", "http://localhost:8080", router, NewInFlightTracker(), placements,
	)

	err = orch.Deprovision(context.Background(), "lease-1")
	require.ErrorIs(t, err, ErrDeprovisionFailed)
	require.ErrorIs(t, err, ErrPlacementUnresolvable,
		"backend-a remains a positive candidate after it is removed from the router")

	configured.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, configured.deprovisionCalls,
		"configured candidates are still swept best-effort")
	configured.mu.Unlock()
}

func TestOrchestrator_Deprovision_KnownConflictSucceedsAfterEveryCandidate(t *testing.T) {
	backendA := &mockManagerBackend{name: "backend-a"}
	backendB := &mockManagerBackend{name: "backend-b"}
	byName := map[string]backend.Backend{
		backendA.name: backendA,
		backendB.name: backendB,
	}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend { return byName[name] },
		backendsFn:         func() []backend.Backend { return []backend.Backend{backendA, backendB} },
	}
	placements := &mockPlacementStore{}
	_, fenced, err := placements.SetConflictsIfNotNewer(map[string][]string{
		"lease-1": {"backend-a", "backend-b"},
	}, placements.SnapshotRevision())
	require.NoError(t, err)
	require.Empty(t, fenced)
	orch := newTestProvisionOrchestrator(t,
		"prov-1", "http://localhost:8080", router, NewInFlightTracker(), placements,
	)

	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))
	backendA.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, backendA.deprovisionCalls)
	backendA.mu.Unlock()
	backendB.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, backendB.deprovisionCalls)
	backendB.mu.Unlock()
}

// TestOrchestrator_Deprovision_PlacementMissing_SweepsAllBackends is the ENG-335
// regression guard. With no placement and no in-flight entry, Deprovision must
// NOT route to a single default backend (the old SKU-route → defaultBackend
// phantom that reported success against docker-1 while the real volume on
// docker-2 was stranded). It must sweep ALL backends so the real holder is
// torn down.
func TestOrchestrator_Deprovision_PlacementMissing_SweepsAllBackends(t *testing.T) {
	mb1 := &mockManagerBackend{name: "docker-1"} // default/first — did NOT hold the lease
	mb2 := &mockManagerBackend{name: "docker-2"} // the actual holder
	mb3 := &mockManagerBackend{name: "docker-3"}
	router := &mockBackendRouter{
		// Route() would have returned the default (docker-1) — the phantom path.
		routeFn:    func(sku string) backend.Backend { return mb1 },
		backendsFn: func() []backend.Backend { return []backend.Backend{mb1, mb2, mb3} },
	}
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, NewInFlightTracker(), &mockPlacementStore{})

	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))

	for _, mb := range []*mockManagerBackend{mb1, mb2, mb3} {
		mb.mu.Lock()
		assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls, "backend %s must be swept", mb.name)
		mb.mu.Unlock()
	}
}

func TestOrchestrator_Deprovision_InFlightBackendNotFound_SweepsButReportsUnreachedOwner(t *testing.T) {
	mb := &mockManagerBackend{name: "real-backend"}
	recovered := &mockManagerBackend{name: "deleted-backend"}
	backendRestored := false
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if backendRestored && name == recovered.Name() {
				return recovered
			}
			return nil // in-flight backend initially gone
		},
		backendsFn: func() []backend.Backend { return []backend.Backend{mb} },
	}
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "t", testItems("sku-1"), "deleted-backend")
	_, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)

	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.ErrorIs(t, err, ErrDeprovisionFailed)
	require.ErrorIs(t, err, ErrPlacementUnresolvable)

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()
	assert.False(t, tracker.IsInFlight("lease-1"),
		"failed close retry state must not participate in provision load or timeout rejection")
	assert.Equal(t, []string{recovered.Name()}, orch.rememberedDeprovisionCandidates("lease-1"))

	backendRestored = true
	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))
	recovered.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, recovered.deprovisionCalls,
		"a retry must reach the formerly unconfigured positive candidate")
	recovered.mu.Unlock()
	assert.Empty(t, orch.rememberedDeprovisionCandidates("lease-1"))
}

// --- Placement integration tests ---

func TestOrchestrator_StartProvisioning_RecordsPlacement(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, ps)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{})
	require.NoError(t, err)

	assert.Equal(t, "test-backend", ps.Get("lease-1"), "placement should be recorded after successful provisioning")
}

func TestOrchestrator_StartProvisioning_PlacementAttemptErrorFailsClosed(t *testing.T) {
	// If the write-ahead placement fails, no backend may be contacted.
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()

	// Use a placement store that always errors on Set
	ps := &errorPlacementStore{setErr: errors.New("disk full")}
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, ps)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProvisioningFailed)
	assert.ErrorContains(t, err, "disk full")
	assert.False(t, tracker.IsInFlight("lease-1"))
	mb.mu.Lock()
	assert.Empty(t, mb.provisionCalls, "backend must not be called before durable intent exists")
	mb.mu.Unlock()
}

func TestOrchestrator_StartProvisioning_InventoryNotReadyFailsClosed(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeFn: func(string) backend.Backend { return mb },
	}
	store, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureBackendTopology(backendTopologyNames(router)))
	tracker := NewInFlightTracker()
	orch, err := NewProvisionOrchestrator(
		"prov-1", "http://localhost:8080", router, tracker.Operations(), store, nil,
	)
	require.NoError(t, err)

	err = startTestProvisioning(t, orch, context.Background(), &billingtypes.Lease{
		Uuid:   "lease-before-inventory",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}, ProvisionOpts{})
	require.ErrorIs(t, err, ErrProvisioningFailed)
	require.ErrorIs(t, err, placement.ErrInvalidAdmissionBaseline)
	assert.False(t, tracker.Operations().Contains("lease-before-inventory"),
		"failed durable admission must abort only its exact operation")
	mb.mu.Lock()
	assert.Empty(t, mb.provisionCalls, "inventory authority must be ready before any backend side effect")
	mb.mu.Unlock()
}

func TestOrchestrator_StartProvisioning_BackendFails_NoPlacement(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend", provisionErr: errors.New("backend down")}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, ps)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{})
	require.Error(t, err)

	assert.Empty(t, ps.Get("lease-1"), "placement should not be recorded when backend fails")
	p := ps.Lookup("lease-1")
	assert.Equal(t, placement.StateAttempting, p.State(), "ambiguous failure must retain its target")
	assert.Equal(t, "test-backend", p.Attempt)
}

func TestClassifyProvisionOutcome(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want provisionOutcome
	}{
		{name: "accepted", want: provisionOutcomeAccepted},
		{name: "unvalidated conflict is ambiguous", err: fmt.Errorf("wrapped: %w", backend.ErrAlreadyProvisioned), want: provisionOutcomeAmbiguous},
		{name: "validation refusal", err: fmt.Errorf("wrapped: %w", backend.ErrValidation), want: provisionOutcomeDefinitiveFailure},
		{name: "coded capacity refusal", err: fmt.Errorf("wrapped: %w", backend.ErrCapacityRefused), want: provisionOutcomeDefinitiveFailure},
		{name: "unvalidated capacity response is ambiguous", err: fmt.Errorf("wrapped: %w", backend.ErrInsufficientResources), want: provisionOutcomeAmbiguous},
		{name: "open circuit", err: fmt.Errorf("wrapped: %w", backend.ErrCircuitOpen), want: provisionOutcomeDefinitiveFailure},
		{name: "malformed response is ambiguous", err: backend.ErrMalformedErrorBody, want: provisionOutcomeAmbiguous},
		{name: "transport is ambiguous", err: errors.New("connection reset"), want: provisionOutcomeAmbiguous},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, classifyProvisionOutcome(tt.err))
		})
	}
}

func TestCapacityVerdictLabel(t *testing.T) {
	t.Parallel()
	assert.Equal(t, metrics.CapacityVerdictCodedRefusal,
		capacityVerdictLabel(fmt.Errorf("wrapped: %w", backend.ErrCapacityRefused)))
	assert.Equal(t, metrics.CapacityVerdictAmbiguous,
		capacityVerdictLabel(fmt.Errorf("wrapped: %w", backend.ErrInsufficientResources)))
}

func TestOrchestrator_StartProvisioning_AmbiguousAttemptSuppressesSecondBackendCall(t *testing.T) {
	first := &mockManagerBackend{name: "backend-a", provisionErr: errors.New("connection reset")}
	second := &mockManagerBackend{name: "backend-b"}
	routeCalls := 0
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{first, second} },
		routeForProvisionFn: func(context.Context, string, map[string]int) backend.Backend {
			routeCalls++
			if routeCalls == 1 {
				return first
			}
			return second
		},
	}
	tracker := NewInFlightTracker()
	ps := newTestPlacementAuthority(t)
	orch := newTestProvisionOrchestrator(t, "provider-1", "http://cb", router, tracker, ps)
	lease := &billingtypes.Lease{
		Uuid: "lease-ambiguous", Tenant: "tenant-a",
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	require.Error(t, startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{}))
	assert.False(t, tracker.IsInFlight(lease.Uuid), "ambiguous call releases ephemeral in-flight state")
	p := ps.Lookup(lease.Uuid)
	require.Equal(t, placement.StateAttempting, p.State())
	assert.Equal(t, "backend-a", p.Attempt)
	assert.True(t, p.AttemptOperationID().Valid(),
		"the durable ambiguous attempt must retain its exact typed operation identity")

	// This mirrors Watermill's immediate retry. Routing is free to select B,
	// but the durable unresolved attempt makes the retry a benign no-op.
	require.NoError(t, startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{}))
	first.mu.Lock()
	firstCalls := len(first.provisionCalls)
	first.mu.Unlock()
	second.mu.Lock()
	secondCalls := len(second.provisionCalls)
	second.mu.Unlock()
	assert.Equal(t, 1, firstCalls)
	assert.Zero(t, secondCalls, "unresolved backend A outcome must block a call to B")
	assert.False(t, tracker.IsInFlight(lease.Uuid), "benign durable-attempt skip must release its new tracker slot")
}

func TestOrchestrator_StartProvisioning_InsufficientResourcesRetainsAttemptAndSuppressesReroute(t *testing.T) {
	first := &mockManagerBackend{name: "backend-a", provisionErr: backend.ErrInsufficientResources}
	second := &mockManagerBackend{name: "backend-b"}
	routeCalls := 0
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{first, second} },
		routeForProvisionFn: func(context.Context, string, map[string]int) backend.Backend {
			routeCalls++
			if routeCalls == 1 {
				return first
			}
			return second
		},
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	orch := newTestProvisionOrchestrator(t, "provider-1", "http://cb", router, tracker, ps)
	lease := &billingtypes.Lease{
		Uuid: "lease-capacity-reroute", Tenant: "tenant-a",
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{})
	require.ErrorIs(t, err, backend.ErrInsufficientResources)
	p := ps.Lookup(lease.Uuid)
	require.Equal(t, placement.StateAttempting, p.State())
	assert.Equal(t, "backend-a", p.Attempt)

	require.NoError(t, startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{}))
	p = ps.Lookup(lease.Uuid)
	assert.Equal(t, placement.StateAttempting, p.State())
	assert.Equal(t, "backend-a", p.Attempt)
	second.mu.Lock()
	assert.Empty(t, second.provisionCalls,
		"an unvalidated 503 must not permit a second backend operation")
	second.mu.Unlock()
}

func TestOrchestrator_StartProvisioning_CodedCapacityRefusalClearsAttemptAndAllowsReroute(t *testing.T) {
	first := &mockManagerBackend{name: "backend-a", provisionErr: backend.ErrCapacityRefused}
	second := &mockManagerBackend{name: "backend-b"}
	routeCalls := 0
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{first, second} },
		routeForProvisionFn: func(context.Context, string, map[string]int) backend.Backend {
			routeCalls++
			if routeCalls == 1 {
				return first
			}
			return second
		},
	}
	placements := newTestPlacementAuthority(t)
	orchestrator := newTestProvisionOrchestrator(
		t, "provider-1", "http://cb", router, NewInFlightTracker(), placements,
	)
	lease := &billingtypes.Lease{
		Uuid: "lease-coded-capacity", Tenant: "tenant-a",
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := startTestProvisioning(t, orchestrator, t.Context(), lease, ProvisionOpts{})
	require.ErrorIs(t, err, backend.ErrCapacityRefused)
	assert.Equal(t, placement.StateAbsent, placements.Lookup(lease.Uuid).State(),
		"a coded refusal must clear its exact write-ahead attempt")

	require.NoError(t, startTestProvisioning(t, orchestrator, t.Context(), lease, ProvisionOpts{}))
	record := placements.Lookup(lease.Uuid)
	assert.Equal(t, placement.StateConfirmed, record.State())
	assert.Equal(t, "backend-b", record.Backend)
	first.mu.Lock()
	assert.Len(t, first.provisionCalls, 1)
	first.mu.Unlock()
	second.mu.Lock()
	assert.Len(t, second.provisionCalls, 1)
	second.mu.Unlock()
}

func TestOrchestrator_StartProvisioning_DefinitiveFailurePreservesConfirmedPin(t *testing.T) {
	pinned := &mockManagerBackend{name: "backend-a", provisionErr: backend.ErrCircuitOpen}
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{pinned} },
		getBackendByNameFn: func(name string) backend.Backend {
			if name == pinned.name {
				return pinned
			}
			return nil
		},
	}
	ps := &mockPlacementStore{}
	require.NoError(t, ps.Set("lease-active", pinned.name))
	orch := newTestProvisionOrchestrator(t, "provider-1", "http://cb", router, NewInFlightTracker(), ps)
	lease := &billingtypes.Lease{
		Uuid: "lease-active", Tenant: "tenant-a",
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	require.ErrorIs(t, startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{}), backend.ErrCircuitOpen)
	p := ps.Lookup(lease.Uuid)
	assert.Equal(t, placement.StateConfirmed, p.State())
	assert.Equal(t, pinned.name, p.Backend)
	assert.Empty(t, p.Attempt, "failure clears only the retry attempt")
}

func TestRouteForProvisionHonoringPlacement_AttemptDoesNotPin(t *testing.T) {
	attempted := &mockManagerBackend{name: "backend-a"}
	freelyRouted := &mockManagerBackend{name: "backend-b"}
	ps := &mockPlacementStore{}
	requireSetPlacementAttempt(t, ps, "lease-1", attempted.name)
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == attempted.name {
				return attempted
			}
			return nil
		},
		routeForProvisionFn: func(context.Context, string, map[string]int) backend.Backend {
			return freelyRouted
		},
	}

	got, err := routeForProvisionHonoringPlacement(context.Background(), router, ps, "lease-1", "sku-1", nil)
	require.NoError(t, err)
	assert.Same(t, freelyRouted, got)
}

func TestOrchestrator_StartProvisioning_UnvalidatedConflictRemainsAttempting(t *testing.T) {
	mb := &mockManagerBackend{name: "backend-a", provisionErr: backend.ErrAlreadyProvisioned}
	router := &mockBackendRouter{routeFn: func(string) backend.Backend { return mb }}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	orch := newTestProvisionOrchestrator(t, "provider-1", "http://cb", router, tracker, ps)
	lease := &billingtypes.Lease{
		Uuid: "lease-duplicate", Tenant: "tenant-a",
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{})
	require.ErrorIs(t, err, ErrProvisioningFailed)
	require.ErrorIs(t, err, backend.ErrAlreadyProvisioned)
	p := ps.Lookup(lease.Uuid)
	assert.Equal(t, placement.StateAttempting, p.State())
	assert.Equal(t, mb.name, p.Attempt)
	assert.Empty(t, p.Backend, "an unvalidated 409 must not confirm ownership")
	assert.False(t, tracker.IsInFlight(lease.Uuid), "inventory, not a callback, resolves the ambiguous attempt")

	// Watermill or reconciliation may retry immediately. The durable Attempt
	// must suppress another external call until a complete inventory settles it.
	require.NoError(t, startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{}))
	mb.mu.Lock()
	assert.Len(t, mb.provisionCalls, 1)
	mb.mu.Unlock()
}

func TestOrchestrator_Deprovision_ViaPlacement(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == "test-backend" {
				return mb
			}
			return nil
		},
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	ps.Set("lease-1", "test-backend")

	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, ps)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()

	// ENG-333: placement must survive deprovision; the reconciler is the sole pruner.
	assert.Equal(t, "test-backend", ps.Get("lease-1"), "placement must survive deprovision for restore affinity (ENG-333)")
}

func TestOrchestrator_Deprovision_ViaAttemptOnlyPlacement(t *testing.T) {
	mb := &mockManagerBackend{name: "attempt-backend"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == mb.name {
				return mb
			}
			return nil
		},
	}
	ps := &mockPlacementStore{}
	requireSetPlacementAttempt(t, ps, "lease-attempt", mb.name)
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://cb", router, NewInFlightTracker(), ps)

	require.NoError(t, orch.Deprovision(context.Background(), "lease-attempt"))
	mb.mu.Lock()
	assert.Equal(t, []string{"lease-attempt"}, mb.deprovisionCalls)
	mb.mu.Unlock()
	assert.Equal(t, placement.StateAttempting, ps.Lookup("lease-attempt").State(),
		"deprovision preserves affinity until retention-aware pruning")
}

// Deprovision trusts a resolvable placement ABSOLUTELY: it tears down on the
// recorded backend alone and reports success, without ever asking whether that
// backend actually holds the lease. That is deliberate and correct — a guessed
// deprovision across the fleet is the ENG-335 phantom-success bug this positive
// resolution replaced.
//
// But it is also the reason a WRONG placement record is dangerous in a second
// way, beyond aiming re-provisioning at the wrong machine: the close is reported
// clean while the real containers and volumes keep running somewhere else, and
// nothing surfaces the discrepancy. This test pins that consequence so the cost
// of manufacturing a placement record is visible in the test suite rather than
// only in a code comment.
//
// It is the companion to
// TestFleet_DegradedSweep_DoesNotManufacturePlacementFromRetention: that one
// stops a bad record from being written, this one shows what it would buy.
func TestOrchestrator_Deprovision_WrongPlacement_ReportsSuccessWithoutTouchingTheRealHolder(t *testing.T) {
	recorded := &mockManagerBackend{name: "recorded-backend"}
	realHolder := &mockManagerBackend{name: "real-holder"}

	byName := map[string]backend.Backend{
		"recorded-backend": recorded,
		"real-holder":      realHolder,
	}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend { return byName[name] },
		backendsFn:         func() []backend.Backend { return []backend.Backend{recorded, realHolder} },
	}

	// The placement names a backend that does NOT hold the lease.
	ps := &mockPlacementStore{}
	require.NoError(t, ps.Set("lease-1", "recorded-backend"))

	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, NewInFlightTracker(), ps)

	// Reported clean...
	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))

	// ...having torn down on the recorded backend only.
	recorded.mu.Lock()
	recordedCalls := append([]string(nil), recorded.deprovisionCalls...)
	recorded.mu.Unlock()
	assert.Equal(t, []string{"lease-1"}, recordedCalls)

	// The machine actually holding the lease was never contacted. In production
	// its containers and volumes would still be running, with the lease closed
	// on chain and the caller told the teardown succeeded.
	realHolder.mu.Lock()
	holderCalls := len(realHolder.deprovisionCalls)
	realHolder.mu.Unlock()
	assert.Zero(t, holderCalls,
		"the real holder is never contacted — which is exactly why placement must never be derived from incomplete data")
}

func TestOrchestrator_Deprovision_StalePlacement_SweepsButReportsUnreachedOwner(t *testing.T) {
	mb := &mockManagerBackend{name: "real-backend"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == "real-backend" {
				return mb
			}
			return nil // "removed-backend" is no longer configured
		},
		backendsFn: func() []backend.Backend { return []backend.Backend{mb} },
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	ps.Set("lease-1", "removed-backend") // stale placement → GetBackendByName misses

	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, ps)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.ErrorIs(t, err, ErrDeprovisionFailed)
	require.ErrorIs(t, err, ErrPlacementUnresolvable,
		"a best-effort sweep cannot report terminal success while a positively named owner was never contacted")

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()

	// ENG-333: stale placement survives; the reconciler prunes orphans later.
	assert.Equal(t, "removed-backend", ps.Get("lease-1"), "stale placement must survive deprovision (ENG-333)")
}

func TestOrchestrator_Deprovision_UnionsPlacementAndInFlight(t *testing.T) {
	mbPlacement := &mockManagerBackend{name: "placement-backend"}
	mbInFlight := &mockManagerBackend{name: "inflight-backend"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			switch name {
			case "placement-backend":
				return mbPlacement
			case "inflight-backend":
				return mbInFlight
			}
			return nil
		},
	}
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "inflight-backend")

	ps := &mockPlacementStore{}
	ps.Set("lease-1", "placement-backend")

	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, ps)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	// Both are positive evidence of a possible holder and must be contacted.
	mbPlacement.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mbPlacement.deprovisionCalls)
	mbPlacement.mu.Unlock()

	mbInFlight.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mbInFlight.deprovisionCalls)
	mbInFlight.mu.Unlock()
	assert.False(t, tracker.IsInFlight("lease-1"), "all positive candidates settled successfully")
}

func TestOrchestrator_Deprovision_PlacementAndInFlightFailureRetainsExactGeneration(t *testing.T) {
	placementBackend := &mockManagerBackend{name: "placement-backend"}
	inFlightErr := errors.New("in-flight backend unavailable")
	inFlightBackend := &mockManagerBackend{name: "inflight-backend", deprovisionErr: inFlightErr}
	byName := map[string]backend.Backend{
		placementBackend.Name(): placementBackend,
		inFlightBackend.Name():  inFlightBackend,
	}
	router := &mockBackendRouter{getBackendByNameFn: func(name string) backend.Backend { return byName[name] }}
	tracker := NewInFlightTracker()
	_, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-a", testItems("sku-1"), inFlightBackend.Name(),
	)
	require.True(t, tracked)
	ps := &mockPlacementStore{}
	require.NoError(t, ps.Set("lease-1", placementBackend.Name()))
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, ps)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.ErrorIs(t, err, ErrDeprovisionFailed)
	assert.ErrorIs(t, err, inFlightErr)
	assert.False(t, tracker.IsInFlight("lease-1"),
		"a candidate failure must move retry state out of provision load/timeout tracking")
	assert.Equal(t, []string{inFlightBackend.Name()}, orch.rememberedDeprovisionCandidates("lease-1"))

	inFlightBackend.mu.Lock()
	inFlightBackend.deprovisionErr = nil
	inFlightBackend.mu.Unlock()
	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))
	assert.False(t, tracker.IsInFlight("lease-1"))
	assert.Empty(t, orch.rememberedDeprovisionCandidates("lease-1"),
		"successful retry must retire the outstanding candidate")

	placementBackend.mu.Lock()
	assert.Equal(t, []string{"lease-1", "lease-1"}, placementBackend.deprovisionCalls,
		"idempotent retry re-settles every positive candidate")
	placementBackend.mu.Unlock()
	inFlightBackend.mu.Lock()
	assert.Equal(t, []string{"lease-1", "lease-1"}, inFlightBackend.deprovisionCalls)
	inFlightBackend.mu.Unlock()
}

func TestOrchestrator_Deprovision_MixedKnownAndUnknownCandidate_KnownFailureIsNotMasked(t *testing.T) {
	knownErr := errors.New("known holder unavailable")
	known := &mockManagerBackend{name: "known-backend", deprovisionErr: knownErr}
	peer := &mockManagerBackend{name: "unrelated-peer"}
	recovered := &mockManagerBackend{name: "removed-backend"}
	backendRestored := false
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == known.name {
				return known
			}
			if backendRestored && name == recovered.name {
				return recovered
			}
			return nil // the in-flight backend is no longer configured
		},
		backendsFn: func() []backend.Backend { return []backend.Backend{known, peer} },
	}

	ps := &mockPlacementStore{}
	require.NoError(t, ps.Set("lease-1", known.name))
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "tenant-a", testItems("sku-1"), "removed-backend")
	_, exists := tracker.GetInFlight("lease-1")
	require.True(t, exists)
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, ps)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.ErrorIs(t, err, ErrDeprovisionFailed)
	assert.ErrorIs(t, err, knownErr)

	known.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, known.deprovisionCalls)
	known.mu.Unlock()
	peer.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, peer.deprovisionCalls,
		"unknown candidate still requires the full fleet sweep")
	peer.mu.Unlock()
	assert.False(t, tracker.IsInFlight("lease-1"))
	assert.ElementsMatch(t, []string{known.Name(), recovered.Name()},
		orch.rememberedDeprovisionCandidates("lease-1"),
		"both failed and unconfigured candidates must remain retryable outside provision tracking")

	known.mu.Lock()
	known.deprovisionErr = nil
	known.mu.Unlock()
	backendRestored = true
	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))
	assert.Empty(t, orch.rememberedDeprovisionCandidates("lease-1"))
	known.mu.Lock()
	assert.Equal(t, []string{"lease-1", "lease-1"}, known.deprovisionCalls)
	known.mu.Unlock()
	recovered.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, recovered.deprovisionCalls,
		"retry must reach the previously unconfigured holder")
	recovered.mu.Unlock()
}

func TestOrchestrator_Deprovision_FallbackAllBackends_KeepsPlacement(t *testing.T) {
	mb1 := &mockManagerBackend{name: "b1"}
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{mb1} },
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	ps.Set("lease-1", "stale-backend") // stale — no backend will match

	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, ps)

	err := orch.Deprovision(context.Background(), "lease-1")
	require.ErrorIs(t, err, ErrDeprovisionFailed)
	require.ErrorIs(t, err, ErrPlacementUnresolvable)

	// ENG-333: placement survives even on the fallback path; the reconciler is the sole pruner.
	assert.Equal(t, "stale-backend", ps.Get("lease-1"), "placement must survive fallback deprovision (ENG-333)")
}

// TestOrchestrator_Deprovision_KeepsPlacement asserts that Deprovision does NOT delete
// the placement record (ENG-333). The placement is a derived index of where the lease's
// retained volumes live; the reconciler (cleanupOrphanedPlacements) is the sole pruner,
// gated on the lease being terminal on chain AND absent from all backends.
func TestOrchestrator_Deprovision_KeepsPlacement(t *testing.T) {
	mb := &mockManagerBackend{name: "backend-a"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == "backend-a" {
				return mb
			}
			return nil
		},
	}
	ps := &mockPlacementStore{}
	ps.Set("lease-1", "backend-a")

	orch := newTestProvisionOrchestrator(t, "provider-1", "http://cb", router, NewInFlightTracker(), ps)

	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))

	// Placement must SURVIVE deprovision (restore affinity); reconciler prunes later.
	assert.Equal(t, "backend-a", ps.Get("lease-1"), "placement must survive deprovision for restore affinity (ENG-333)")
}

func TestNewProvisionOrchestrator_RequiresCapabilities(t *testing.T) {
	tracker := NewInFlightTracker()
	var typedNilPlacement *placement.Store
	var typedNilAuthority PlacementAuthorityStore = typedNilPlacement

	tests := []struct {
		name       string
		operations *operation.Registry
		placements PlacementAuthorityStore
		wantErr    error
	}{
		{
			name:       "nil placement authority",
			operations: tracker.Operations(),
			wantErr:    ErrPlacementStoreUnavailable,
		},
		{
			name:       "typed nil placement authority",
			operations: tracker.Operations(),
			placements: typedNilAuthority,
			wantErr:    ErrPlacementStoreUnavailable,
		},
		{
			name:       "nil operation registry",
			placements: newTestPlacementAuthority(t),
			wantErr:    errors.New("operation registry is required"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orch, err := NewProvisionOrchestrator(
				"prov-1", "http://localhost:8080", &mockBackendRouter{},
				tt.operations, tt.placements, nil,
			)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr.Error())
			assert.Nil(t, orch)
		})
	}
}

func TestOrchestrator_StartProvisioningClaimed_RejectsInvalidClaimBeforeSideEffect(t *testing.T) {
	backendClient := &mockManagerBackend{name: "backend-a"}
	router := &mockBackendRouter{routeFn: func(string) backend.Backend { return backendClient }}
	registry := operation.NewRegistry()
	store := newTestPlacementAuthority(t)
	armTestPlacementAdmission(t, store, router)
	orchestrator, err := NewProvisionOrchestrator(
		"provider-1", "http://callback", router, registry, store, nil,
	)
	require.NoError(t, err)
	lease := &billingtypes.Lease{
		Uuid: "lease-1", Tenant: "tenant-a",
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err = orchestrator.StartProvisioningClaimed(
		context.Background(), operation.LeaseClaim{}, lease, ProvisionOpts{},
	)
	require.ErrorIs(t, err, ErrProvisioningFailed)
	assert.False(t, registry.Contains(lease.Uuid))
	assert.Equal(t, placement.StateAbsent, store.Lookup(lease.Uuid).State())
	backendClient.mu.Lock()
	assert.Empty(t, backendClient.provisionCalls)
	backendClient.mu.Unlock()
}

func TestOrchestrator_StartProvisioningClaimed_RejectsNonPendingLeaseBeforeSideEffect(t *testing.T) {
	for _, state := range []billingtypes.LeaseState{
		billingtypes.LEASE_STATE_ACTIVE,
		billingtypes.LEASE_STATE_CLOSED,
		billingtypes.LEASE_STATE_REJECTED,
		billingtypes.LEASE_STATE_EXPIRED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			backendClient := &mockManagerBackend{name: "backend-a"}
			router := &mockBackendRouter{routeFn: func(string) backend.Backend { return backendClient }}
			registry := operation.NewRegistry()
			store := newTestPlacementAuthority(t)
			armTestPlacementAdmission(t, store, router)
			orchestrator, err := NewProvisionOrchestrator(
				"provider-1", "http://callback", router, registry, store, nil,
			)
			require.NoError(t, err)
			lease := &billingtypes.Lease{
				Uuid: "lease-1", Tenant: "tenant-a", State: state,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}
			claimResult := registry.TryClaimLeaseNow(lease.Uuid)
			require.True(t, claimResult.Acquired())
			claim := claimResult.Claim()

			err = orchestrator.StartProvisioningClaimed(
				t.Context(), claim, lease, ProvisionOpts{},
			)
			require.ErrorIs(t, err, ErrProvisioningFailed)
			require.True(t, registry.ReleaseLease(claim))
			assert.False(t, registry.Contains(lease.Uuid))
			assert.Equal(t, placement.StateAbsent, store.Lookup(lease.Uuid).State())
			backendClient.mu.Lock()
			assert.Empty(t, backendClient.provisionCalls)
			backendClient.mu.Unlock()
		})
	}
}

// errorPlacementStore is a legacy raw-map fixture that injects write failures
// behind typed test adapters.
type errorPlacementStore struct {
	mockPlacementStore
	setErr                error
	batchFencedOnError    map[string]struct{}
	conflictFencedOnError map[string]struct{}
}

func (e *errorPlacementStore) SetAttempting(leaseUUID, backendName string) (uint64, error) {
	if e.setErr != nil {
		return 0, e.setErr
	}
	return e.mockPlacementStore.SetAttempting(leaseUUID, backendName)
}

func (e *errorPlacementStore) SetAttemptingIfNotNewer(
	leaseUUID, backendName string,
	maxRevision uint64,
) (uint64, bool, error) {
	if e.setErr != nil {
		return 0, false, e.setErr
	}
	return e.mockPlacementStore.SetAttemptingIfNotNewer(leaseUUID, backendName, maxRevision)
}

func (e *errorPlacementStore) Confirm(leaseUUID, backendName string) error {
	if e.setErr != nil {
		return e.setErr
	}
	return e.mockPlacementStore.Confirm(leaseUUID, backendName)
}

func (e *errorPlacementStore) ConfirmAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error) {
	if e.setErr != nil {
		return false, e.setErr
	}
	return e.mockPlacementStore.ConfirmAttemptIfRevision(leaseUUID, backendName, revision)
}

func (e *errorPlacementStore) ClearAttempt(leaseUUID, backendName string) error {
	if e.setErr != nil {
		return e.setErr
	}
	return e.mockPlacementStore.ClearAttempt(leaseUUID, backendName)
}

func (e *errorPlacementStore) ClearAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error) {
	if e.setErr != nil {
		return false, e.setErr
	}
	return e.mockPlacementStore.ClearAttemptIfRevision(leaseUUID, backendName, revision)
}

func (e *errorPlacementStore) SetBatch(placements map[string]string) error {
	_, _, err := e.SetBatchIfNotNewer(placements, ^uint64(0))
	return err
}

func (e *errorPlacementStore) SetBatchIfNotNewer(
	placements map[string]string,
	maxRevision uint64,
) (map[string]uint64, map[string]struct{}, error) {
	if e.setErr != nil {
		return nil, e.batchFencedOnError, e.setErr
	}
	return e.mockPlacementStore.SetBatchIfNotNewer(placements, maxRevision)
}

func (e *errorPlacementStore) SetConflictsIfNotNewer(
	conflicts map[string][]string,
	maxRevision uint64,
) (map[string]uint64, map[string]struct{}, error) {
	if e.setErr != nil {
		return nil, e.conflictFencedOnError, e.setErr
	}
	return e.mockPlacementStore.SetConflictsIfNotNewer(conflicts, maxRevision)
}

func (e *errorPlacementStore) ClearConflictsIfNotNewer(leases map[string]struct{}, maxRevision uint64) error {
	if e.setErr != nil {
		return e.setErr
	}
	return e.mockPlacementStore.ClearConflictsIfNotNewer(leases, maxRevision)
}

func TestOrchestrator_StartProvisioning_HonorsPlacement(t *testing.T) {
	// When a placement record exists, StartProvisioning must route to the
	// placement-pinned backend, not the least-loaded one (ENG-333).
	pinned := &mockManagerBackend{name: "backend-pinned"}
	leastLoaded := &mockManagerBackend{name: "backend-least"}

	byName := map[string]backend.Backend{
		"backend-pinned": pinned,
		"backend-least":  leastLoaded,
	}
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend {
			return []backend.Backend{pinned, leastLoaded}
		},
		getBackendByNameFn: func(name string) backend.Backend { return byName[name] },
		// RouteForProvision would normally pick the least-loaded backend.
		routeForProvisionFn: func(_ context.Context, _ string, _ map[string]int) backend.Backend {
			return leastLoaded
		},
	}

	ps := &mockPlacementStore{}
	ps.Set("lease-1", "backend-pinned")

	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "provider-1", "http://cb", router, tracker, ps)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "t",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	require.NoError(t, startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{}))

	// The placement-pinned backend must have received the Provision call.
	pinned.mu.Lock()
	pinnedCalls := len(pinned.provisionCalls)
	pinned.mu.Unlock()

	leastLoaded.mu.Lock()
	leastCalls := len(leastLoaded.provisionCalls)
	leastLoaded.mu.Unlock()

	assert.Equal(t, 1, pinnedCalls, "pinned backend must receive the Provision call")
	assert.Equal(t, 0, leastCalls, "least-loaded backend must NOT receive the Provision call")
}

// ENG-635: when a lease's placement record names a backend the router does not
// know, fred refuses rather than routing to a peer. Removing, renaming or
// pausing a backend that holds ACTIVE stateful leases previously made those
// leases look unplaced, and each one was re-provisioned on the least-loaded
// peer — a brand-new EMPTY volume while the real data sat on the absent
// machine. Unattended, on a timer, for every affected lease at once, and the
// caller saw success.
func TestOrchestrator_StartProvisioning_UnresolvablePlacement_ProvisionsNoBackend(t *testing.T) {
	peer := &mockManagerBackend{name: "backend-peer"}

	router := &mockBackendRouter{
		// The recorded backend is gone from the router.
		getBackendByNameFn: func(string) backend.Backend { return nil },
		// A peer IS available — this is exactly the situation in which the old
		// code silently substituted.
		routeForProvisionFn: func(_ context.Context, _ string, _ map[string]int) backend.Backend {
			return peer
		},
	}

	ps := &mockPlacementStore{}
	ps.Set("lease-1", "removed-backend")

	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "provider-1", "http://cb", router, tracker, ps)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "t",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{})

	require.Error(t, err)
	require.ErrorIs(t, err, ErrPlacementUnresolvable)

	// Assert on the call count, not just the error: the failure being guarded
	// against reports success to its caller, so an error return alone would not
	// prove the peer was left untouched.
	peer.mu.Lock()
	peerCalls := len(peer.provisionCalls)
	peer.mu.Unlock()
	assert.Zero(t, peerCalls, "no substitute backend may be provisioned")

	// The lease must not be left occupying an in-flight slot, or the next
	// attempt would be refused as a duplicate rather than retried.
	_, inFlight := tracker.GetInFlight("lease-1")
	assert.False(t, inFlight, "a refused provision must not leak an in-flight entry")
}

// The boundary guard. Without it, the change above could refuse everything and
// still look correct: a lease with NO placement record must keep routing freely,
// which is the path every new lease takes.
func TestOrchestrator_StartProvisioning_NoPlacementRecord_RoutesFreely(t *testing.T) {
	target := &mockManagerBackend{name: "backend-target"}

	router := &mockBackendRouter{
		backendsFn:         func() []backend.Backend { return []backend.Backend{target} },
		getBackendByNameFn: func(string) backend.Backend { return nil },
		routeForProvisionFn: func(_ context.Context, _ string, _ map[string]int) backend.Backend {
			return target
		},
	}

	// Placement store present but holding no record for this lease.
	ps := &mockPlacementStore{}

	tracker := NewInFlightTracker()
	orch := newTestProvisionOrchestrator(t, "provider-1", "http://cb", router, tracker, ps)

	lease := &billingtypes.Lease{
		Uuid:   "lease-unplaced",
		Tenant: "t",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	require.NoError(t, startTestProvisioning(t, orch, context.Background(), lease, ProvisionOpts{}))

	target.mu.Lock()
	calls := len(target.provisionCalls)
	target.mu.Unlock()
	assert.Equal(t, 1, calls, "a lease with no placement record must route freely")
}

func TestOrchestrator_StartProvisioning_IncrementsInsufficientResources(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		verdict string
	}{
		{name: "coded refusal", err: backend.ErrCapacityRefused, verdict: metrics.CapacityVerdictCodedRefusal},
		{name: "ambiguous legacy response", err: backend.ErrInsufficientResources, verdict: metrics.CapacityVerdictAmbiguous},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mb := &mockManagerBackend{name: "test-backend", provisionErr: test.err}
			router := &mockBackendRouter{routeFn: func(string) backend.Backend { return mb }}
			orchestrator := newTestProvisionOrchestrator(
				t, "prov-1", "http://localhost:8080", router, NewInFlightTracker(), nil,
			)
			counter := metrics.BackendInsufficientResourcesTotal.WithLabelValues(
				"test-backend", test.verdict,
			)
			before := promtestutil.ToFloat64(counter)
			lease := &billingtypes.Lease{
				Uuid: "lease-capacity-" + test.verdict, Tenant: "tenant-a",
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}

			err := startTestProvisioning(t, orchestrator, t.Context(), lease, ProvisionOpts{})
			require.ErrorIs(t, err, backend.ErrInsufficientResources)
			assert.Equal(t, before+1, promtestutil.ToFloat64(counter))
		})
	}
}
