package provisioner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

type callbackAcknowledgerFunc func(context.Context, string) (bool, string, error)

func (acknowledge callbackAcknowledgerFunc) Acknowledge(
	ctx context.Context,
	leaseUUID string,
) (bool, string, error) {
	return acknowledge(ctx, leaseUUID)
}

type callbackChainStub struct {
	getLease func(context.Context, string) (*billingtypes.Lease, error)
	reject   func(context.Context, []string, string) (uint64, []string, error)
}

func (chain *callbackChainStub) GetLease(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	if chain.getLease == nil {
		return nil, nil
	}
	return chain.getLease(ctx, leaseUUID)
}

func (chain *callbackChainStub) RejectLeases(
	ctx context.Context,
	leaseUUIDs []string,
	reason string,
) (uint64, []string, error) {
	if chain.reject == nil {
		return 0, nil, nil
	}
	return chain.reject(ctx, leaseUUIDs, reason)
}

type callbackPlacementSpy struct {
	confirm func(string, string, operation.OperationID) (bool, error)
	refuse  func(string, string, operation.OperationID) (bool, error)
}

type callbackClaimObserver struct {
	CallbackOperations
	secondAttempt chan struct{}
	attempts      atomic.Int32
	once          sync.Once
}

func (observer *callbackClaimObserver) TryClaimCallback(
	leaseUUID string,
	id operation.OperationID,
) operation.SettlementResult {
	result := observer.CallbackOperations.TryClaimCallback(leaseUUID, id)
	if observer.attempts.Add(1) >= 2 {
		observer.once.Do(func() { close(observer.secondAttempt) })
	}
	return result
}

func (placementSpy *callbackPlacementSpy) ConfirmOperation(
	leaseUUID, backendName string,
	id operation.OperationID,
) (bool, error) {
	if placementSpy.confirm == nil {
		return false, nil
	}
	return placementSpy.confirm(leaseUUID, backendName, id)
}

func (placementSpy *callbackPlacementSpy) RefuseOperation(
	leaseUUID, backendName string,
	id operation.OperationID,
) (bool, error) {
	if placementSpy.refuse == nil {
		return false, nil
	}
	return placementSpy.refuse(leaseUUID, backendName, id)
}

type callbackEventRecorder struct {
	events []backend.LeaseStatusEvent
	mu     sync.Mutex
}

func (recorder *callbackEventRecorder) PublishCallbackLeaseEvent(
	leaseUUID string,
	status backend.ProvisionStatus,
	errMsg string,
) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.events = append(recorder.events, backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    status,
		Error:     errMsg,
	})
}

type callbackDeprovisionRecorder struct {
	leaseUUID   string
	backendName string
	calls       int
}

func (recorder *callbackDeprovisionRecorder) ObserveCallbackDeprovisioned(
	leaseUUID, backendName string,
) {
	recorder.leaseUUID = leaseUUID
	recorder.backendName = backendName
	recorder.calls++
}

func trackCallbackOperation(
	t testing.TB,
	registry *operation.Registry,
	leaseUUID, backendName string,
	tokenRequired bool,
) operation.Token {
	t.Helper()
	result := registry.TryTrack(operation.TrackSpec{
		LeaseUUID:     leaseUUID,
		Tenant:        "tenant-a",
		Backend:       backendName,
		Kind:          operation.KindProvision,
		TokenRequired: tokenRequired,
	})
	require.True(t, result.Started())
	return result.Token()
}

func callbackWireID(t testing.TB, id operation.OperationID) string {
	t.Helper()
	require.True(t, id.Valid())
	return id.String()
}

func callbackCommand(t testing.TB, callback backend.CallbackPayload) CallbackCommand {
	t.Helper()
	command, err := NewCallbackCommand(callback)
	require.NoError(t, err)
	return command
}

func TestNewCallbackService_RequiresNonNilOperationAuthority(t *testing.T) {
	_, err := NewCallbackService(CallbackServiceConfig{})
	require.ErrorIs(t, err, errCallbackOperationsUnavailable)

	var typedNil *operation.Registry
	_, err = NewCallbackService(CallbackServiceConfig{Operations: typedNil})
	require.ErrorIs(t, err, errCallbackOperationsUnavailable)

	service, err := NewCallbackService(CallbackServiceConfig{
		Operations: operation.NewRegistry(),
		Payloads:   (*typedNilCallbackPayloadStore)(nil),
	})
	require.NoError(t, err)
	assert.Nil(t, service.payloads, "typed-nil optional capabilities must be normalized")
	require.ErrorIs(t,
		service.HandleCallback(context.Background(), CallbackCommand{}),
		errInvalidCallbackCommand,
		"the zero command must never authorize a registry lookup or mutation",
	)
}

type typedNilCallbackPayloadStore struct{}

func (*typedNilCallbackPayloadStore) Delete(string) {}

func TestNewCallbackCommand_ConvertsWireIdentityAtBoundary(t *testing.T) {
	legacy, err := NewCallbackCommand(backend.CallbackPayload{LeaseUUID: "legacy"})
	require.NoError(t, err)
	assert.True(t, legacy.valid)
	assert.False(t, legacy.token)
	assert.False(t, legacy.operationID.Valid())

	command, err := NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID:   "typed",
		OperationID: "123e4567-e89b-42d3-a456-426614174000",
	})
	require.NoError(t, err)
	assert.True(t, command.valid)
	assert.True(t, command.token)
	assert.Equal(t, "123e4567-e89b-42d3-a456-426614174000", command.operationID.String())

	_, err = NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID:   "malformed",
		OperationID: "not-a-uuid",
	})
	assert.ErrorIs(t, err, operation.ErrInvalidID)
}

func TestCallbackService_AuthorizesOnlyMatchingOperation(t *testing.T) {
	tests := []struct {
		name          string
		tokenRequired bool
		callbackID    func(testing.TB, operation.OperationID) string
		backend       string
		wantApplied   bool
	}{
		{
			name:          "tokenless legacy callback remains accepted for legacy operation",
			tokenRequired: false,
			callbackID:    func(testing.TB, operation.OperationID) string { return "" },
			wantApplied:   true,
		},
		{
			name:          "missing token is rejected when operation requires it",
			tokenRequired: true,
			callbackID:    func(testing.TB, operation.OperationID) string { return "" },
		},
		{
			name:          "exact token is accepted",
			tokenRequired: true,
			callbackID:    callbackWireID,
			backend:       "backend-a",
			wantApplied:   true,
		},
		{
			name:          "different nonzero token is rejected",
			tokenRequired: true,
			callbackID: func(t testing.TB, id operation.OperationID) string {
				t.Helper()
				return "d9428888-122b-41e1-b85c-61c67afba0c6"
			},
			backend: "backend-a",
		},
		{
			name:          "legacy metrics backend cannot redirect exact token",
			tokenRequired: true,
			callbackID:    callbackWireID,
			backend:       "backend-b",
			wantApplied:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := operation.NewRegistry()
			token := trackCallbackOperation(t, registry, "lease-1", "backend-a", tt.tokenRequired)
			var placementCalls atomic.Int32
			var acknowledgeCalls atomic.Int32
			service, err := NewCallbackService(CallbackServiceConfig{
				Operations: registry,
				Placement: &callbackPlacementSpy{confirm: func(
					leaseUUID, backendName string,
					id operation.OperationID,
				) (bool, error) {
					placementCalls.Add(1)
					assert.Equal(t, "lease-1", leaseUUID)
					assert.Equal(t, "backend-a", backendName)
					assert.Equal(t, token.ID(), id)
					return true, nil
				}},
				Acknowledger: callbackAcknowledgerFunc(func(
					context.Context, string,
				) (bool, string, error) {
					acknowledgeCalls.Add(1)
					return true, "tx", nil
				}),
			})
			require.NoError(t, err)

			err = service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
				LeaseUUID:   "lease-1",
				Backend:     tt.backend,
				Status:      backend.CallbackStatusSuccess,
				OperationID: tt.callbackID(t, token.ID()),
			}))
			require.NoError(t, err)
			if tt.wantApplied {
				assert.Equal(t, int32(1), placementCalls.Load())
				assert.Equal(t, int32(1), acknowledgeCalls.Load())
				assert.False(t, registry.Contains("lease-1"))
				return
			}
			assert.Zero(t, placementCalls.Load())
			assert.Zero(t, acknowledgeCalls.Load())
			assert.True(t, registry.Contains("lease-1"))
		})
	}
}

func TestCallbackService_SuccessSettlesExactDurableAttempt(t *testing.T) {
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a", true)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, "lease-1", "backend-a", token.ID())

	var acknowledgeCalls atomic.Int32
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations: registry,
		Placement:  store,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			acknowledgeCalls.Add(1)
			return true, "tx", nil
		}),
	})
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Backend:     "backend-a",
		Status:      backend.CallbackStatusSuccess,
		OperationID: callbackWireID(t, token.ID()),
	})))

	assert.Equal(t, int32(1), acknowledgeCalls.Load())
	assert.False(t, registry.Contains("lease-1"))
	confirmed := store.Lookup("lease-1")
	assert.Equal(t, placement.StateConfirmed, confirmed.State())
	assert.Equal(t, "backend-a", confirmed.Backend)
	assert.Empty(t, confirmed.Attempt)
	assert.False(t, confirmed.AttemptOperationID().Valid())
}

func TestCallbackService_FailureCannotClearDifferentDurableOperation(t *testing.T) {
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a", true)
	store := newTestPlacementAuthority(t)
	newerID, err := operation.ParseID("d9428888-122b-41e1-b85c-61c67afba0c6")
	require.NoError(t, err)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, "lease-1", "backend-a", newerID)

	var rejectCalls atomic.Int32
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations: registry,
		Placement:  store,
		Chain: &callbackChainStub{
			getLease: func(context.Context, string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING}, nil
			},
			reject: func(context.Context, []string, string) (uint64, []string, error) {
				rejectCalls.Add(1)
				return 1, []string{"tx"}, nil
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Backend:     "backend-a",
		Status:      backend.CallbackStatusFailed,
		OperationID: callbackWireID(t, token.ID()),
	})))

	assert.Equal(t, int32(1), rejectCalls.Load())
	assert.False(t, registry.Contains("lease-1"))
	preserved := store.Lookup("lease-1")
	assert.Equal(t, placement.StateAttempting, preserved.State())
	assert.Equal(t, newerID, preserved.AttemptOperationID())
}

func TestCallbackService_RetryableAcknowledgeFailureReleasesExactClaim(t *testing.T) {
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a", true)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, "lease-1", "backend-a", token.ID())

	var calls atomic.Int32
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations: registry,
		Placement:  store,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			if calls.Add(1) == 1 {
				return false, "", errors.New("chain unavailable")
			}
			return true, "tx", nil
		}),
	})
	require.NoError(t, err)
	callback := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Backend:     "backend-a",
		Status:      backend.CallbackStatusSuccess,
		OperationID: callbackWireID(t, token.ID()),
	})

	err = service.HandleCallback(context.Background(), callback)
	require.ErrorIs(t, err, ErrAcknowledgeFailed)
	record, exists := registry.Lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, operation.SettlementUnclaimed, record.Settlement)

	require.NoError(t, service.HandleCallback(context.Background(), callback))
	assert.Equal(t, int32(2), calls.Load())
	assert.False(t, registry.Contains("lease-1"))
}

func TestCallbackService_ConcurrentDuplicateCallbacksAcknowledgeOnce(t *testing.T) {
	registry := operation.NewRegistry()
	operations := &callbackClaimObserver{
		CallbackOperations: registry,
		secondAttempt:      make(chan struct{}),
	}
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a", true)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, "lease-1", "backend-a", token.ID())

	acknowledgeStarted := make(chan struct{})
	releaseAcknowledge := make(chan struct{})
	var acknowledgeCalls atomic.Int32
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations:        operations,
		Placement:         store,
		ClaimPollInterval: time.Millisecond,
		ClaimMaxWait:      time.Second,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			if acknowledgeCalls.Add(1) == 1 {
				close(acknowledgeStarted)
			}
			<-releaseAcknowledge
			return true, "tx", nil
		}),
	})
	require.NoError(t, err)
	callback := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Backend:     "backend-a",
		Status:      backend.CallbackStatusSuccess,
		OperationID: callbackWireID(t, token.ID()),
	})

	results := make(chan error, 2)
	go func() { results <- service.HandleCallback(context.Background(), callback) }()
	select {
	case <-acknowledgeStarted:
	case <-time.After(time.Second):
		t.Fatal("first callback did not reach acknowledgement")
	}
	go func() { results <- service.HandleCallback(context.Background(), callback) }()
	select {
	case <-operations.secondAttempt:
	case <-time.After(time.Second):
		t.Fatal("duplicate callback did not contend for the exact operation")
	}
	close(releaseAcknowledge)
	require.NoError(t, <-results)
	require.NoError(t, <-results)

	assert.Equal(t, int32(1), acknowledgeCalls.Load())
	assert.False(t, registry.Contains("lease-1"))
}

func TestCallbackService_DeprovisionOwnedCallbackIsObservationOnly(t *testing.T) {
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a", true)
	deprovision := registry.TryClaimDeprovision("lease-1", token.ID())
	require.True(t, deprovision.Claimed())
	events := &callbackEventRecorder{}
	observer := &callbackDeprovisionRecorder{}
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations:          registry,
		Events:              events,
		DeprovisionObserver: observer,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			t.Fatal("deprovision-owned callback must not acknowledge")
			return false, "", nil
		}),
	})
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Status:      backend.CallbackStatusDeprovisioned,
		OperationID: callbackWireID(t, token.ID()),
		Retained:    true,
	})))

	record, exists := registry.Lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, operation.SettlementDeprovision, record.Settlement)
	assert.Equal(t, 1, observer.calls)
	assert.Equal(t, "lease-1", observer.leaseUUID)
	assert.Equal(t, "backend-a", observer.backendName)
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusRetained, events.events[0].Status)
	assert.Equal(t, retainedLeaseNotice, events.events[0].Error)
	require.True(t, registry.FinishSettlement(deprovision.Claim()))
}

func TestCallbackService_TokenlessNonInFlightDeprovisionCannotRetireBackendCandidate(t *testing.T) {
	registry := operation.NewRegistry()
	events := &callbackEventRecorder{}
	observer := &callbackDeprovisionRecorder{}
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations:          registry,
		Events:              events,
		DeprovisionObserver: observer,
	})
	require.NoError(t, err)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID: "lease-1",
			Backend:   "body-supplied-backend",
			Status:    backend.CallbackStatusDeprovisioned,
			Retained:  true,
		},
	)))

	assert.Zero(t, observer.calls,
		"a v0.13 body backend is observational metadata, not deprovision authority")
	require.Len(t, events.events, 1,
		"the tokenless callback remains status-compatible with v0.13")
	assert.Equal(t, backend.ProvisionStatusRetained, events.events[0].Status)
	assert.Equal(t, retainedLeaseNotice, events.events[0].Error)
}

func TestCallbackService_MissingMutationCapabilityFailsClosedAndReleasesClaim(t *testing.T) {
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a", true)
	service, err := NewCallbackService(CallbackServiceConfig{Operations: registry})
	require.NoError(t, err)

	err = service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Backend:     "backend-a",
		Status:      backend.CallbackStatusSuccess,
		OperationID: callbackWireID(t, token.ID()),
	}))
	require.ErrorIs(t, err, errCallbackPlacementUnavailable)
	record, exists := registry.Lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, operation.SettlementUnclaimed, record.Settlement)

	claim := registry.TryClaimCallback("lease-1", token.ID())
	require.True(t, claim.Claimed(), "failed callback must release its exact claim")
	require.True(t, registry.FinishSettlement(claim.Claim()))
}
