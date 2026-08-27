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
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
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

type callbackPayloadRecorder struct {
	deleted []string
}

func (recorder *callbackPayloadRecorder) Delete(leaseUUID string) {
	recorder.deleted = append(recorder.deleted, leaseUUID)
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
	assert.Equal(t, callbackSelectorLegacy, legacy.selector)
	assert.False(t, legacy.operationID.Valid())
	assert.False(t, legacy.lifecycleID.Valid())

	command, err := NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID:   "typed",
		OperationID: "123e4567-e89b-42d3-a456-426614174000",
	})
	require.NoError(t, err)
	assert.True(t, command.valid)
	assert.Equal(t, callbackSelectorOperation, command.selector)
	assert.Equal(t, "123e4567-e89b-42d3-a456-426614174000", command.operationID.String())
	assert.False(t, command.lifecycleID.Valid())

	lifecycleCommand, err := NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID:   "lifecycle",
		LifecycleID: "123e4567-e89b-42d3-a456-426614174001",
	})
	require.NoError(t, err)
	assert.True(t, lifecycleCommand.valid)
	assert.Equal(t, callbackSelectorLifecycle, lifecycleCommand.selector)
	assert.Equal(t,
		"123e4567-e89b-42d3-a456-426614174001",
		lifecycleCommand.lifecycleID.String(),
	)
	assert.False(t, lifecycleCommand.operationID.Valid())

	_, err = NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID:   "malformed",
		OperationID: "not-a-uuid",
	})
	assert.ErrorIs(t, err, operation.ErrInvalidID)

	_, err = NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID:   "ambiguous",
		OperationID: "123e4567-e89b-42d3-a456-426614174000",
		LifecycleID: "123e4567-e89b-42d3-a456-426614174001",
	})
	assert.Error(t, err)
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
			lifecycleAuthority := newTestPlacementAuthority(t)
			var placementCalls atomic.Int32
			var acknowledgeCalls atomic.Int32
			service, err := NewCallbackService(CallbackServiceConfig{
				Operations:         registry,
				LifecycleAuthority: lifecycleAuthority,
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

func TestCallbackService_TerminalAcknowledgeErrorUsesCurrentLeaseState(t *testing.T) {
	readFailure := errors.New("chain read failed")
	tests := []struct {
		name         string
		lease        *billingtypes.Lease
		leaseErr     error
		wantRetry    bool
		wantReady    bool
		wantFinished bool
	}{
		{
			name:         "active lease publishes ready",
			lease:        &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_ACTIVE},
			wantReady:    true,
			wantFinished: true,
		},
		{
			name:         "close wins race with success callback",
			lease:        &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_CLOSED},
			wantFinished: true,
		},
		{
			name:         "missing lease is terminal without ready",
			wantFinished: true,
		},
		{
			name:      "pending lease remains retryable",
			lease:     &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			wantRetry: true,
		},
		{
			name:      "unknown lease state remains retryable",
			lease:     &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_UNSPECIFIED},
			wantRetry: true,
		},
		{
			name:      "failed exact read remains retryable",
			leaseErr:  readFailure,
			wantRetry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := operation.NewRegistry()
			token := trackCallbackOperation(t, registry, "lease-1", "backend-a", true)
			events := &callbackEventRecorder{}
			var reads atomic.Int32
			service, err := NewCallbackService(CallbackServiceConfig{
				Operations: registry,
				Chain: &callbackChainStub{getLease: func(
					context.Context, string,
				) (*billingtypes.Lease, error) {
					reads.Add(1)
					return tt.lease, tt.leaseErr
				}},
				Placement: &callbackPlacementSpy{confirm: func(
					leaseUUID, backendName string,
					id operation.OperationID,
				) (bool, error) {
					assert.Equal(t, "lease-1", leaseUUID)
					assert.Equal(t, "backend-a", backendName)
					assert.Equal(t, token.ID(), id)
					return true, nil
				}},
				Acknowledger: callbackAcknowledgerFunc(func(
					context.Context, string,
				) (bool, string, error) {
					return false, "", billingtypes.ErrLeaseNotPending
				}),
				Events: events,
			})
			require.NoError(t, err)

			err = service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
				LeaseUUID:   "lease-1",
				Status:      backend.CallbackStatusSuccess,
				OperationID: callbackWireID(t, token.ID()),
			}))
			assert.Equal(t, int32(1), reads.Load())
			if tt.wantRetry {
				require.ErrorIs(t, err, ErrAcknowledgeFailed)
				record, exists := registry.Lookup("lease-1")
				require.True(t, exists)
				assert.Equal(t, operation.SettlementUnclaimed, record.Settlement)
				assert.Empty(t, events.events)
				if tt.leaseErr != nil {
					assert.ErrorIs(t, err, tt.leaseErr)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantFinished, !registry.Contains("lease-1"))
			if tt.wantReady {
				require.Len(t, events.events, 1)
				assert.Equal(t, backend.ProvisionStatusReady, events.events[0].Status)
				return
			}
			assert.Empty(t, events.events, "a terminal lease must not be resurrected by a stale success callback")
		})
	}
}

func TestCallbackService_RejectResponseLossUsesCurrentLeaseState(t *testing.T) {
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a", true)
	payloads := &callbackPayloadRecorder{}
	events := &callbackEventRecorder{}
	var reads atomic.Int32
	var rejects atomic.Int32
	var refusals atomic.Int32
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations: registry,
		Chain: &callbackChainStub{
			getLease: func(context.Context, string) (*billingtypes.Lease, error) {
				if reads.Add(1) == 1 {
					return &billingtypes.Lease{
						Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING,
					}, nil
				}
				return &billingtypes.Lease{
					Uuid: "lease-1", State: billingtypes.LEASE_STATE_REJECTED,
				}, nil
			},
			reject: func(context.Context, []string, string) (uint64, []string, error) {
				rejects.Add(1)
				return 0, nil, billingtypes.ErrLeaseNotPending
			},
		},
		Placement: &callbackPlacementSpy{refuse: func(
			leaseUUID, backendName string,
			id operation.OperationID,
		) (bool, error) {
			refusals.Add(1)
			assert.Equal(t, "lease-1", leaseUUID)
			assert.Equal(t, "backend-a", backendName)
			assert.Equal(t, token.ID(), id)
			return true, nil
		}},
		Payloads: payloads,
		Events:   events,
	})
	require.NoError(t, err)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Status:      backend.CallbackStatusFailed,
		Error:       "backend failed",
		OperationID: callbackWireID(t, token.ID()),
	})))

	assert.Equal(t, int32(2), reads.Load(), "terminal reject errors must be resolved by an exact reread")
	assert.Equal(t, int32(1), rejects.Load())
	assert.Equal(t, int32(1), refusals.Load())
	assert.Equal(t, []string{"lease-1"}, payloads.deleted)
	assert.False(t, registry.Contains("lease-1"))
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusFailed, events.events[0].Status)
	assert.Equal(t, "backend failed", events.events[0].Error)
}

func TestCallbackService_FailureSettlementUsesCurrentLeaseState(t *testing.T) {
	tests := []struct {
		name            string
		initialLease    *billingtypes.Lease
		afterReject     *billingtypes.Lease
		afterRejectErr  error
		rejectErr       error
		wantRetry       bool
		wantReject      bool
		wantCleanup     bool
		wantFailedEvent bool
	}{
		{
			name:            "pending lease is rejected",
			initialLease:    &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			wantReject:      true,
			wantCleanup:     true,
			wantFailedEvent: true,
		},
		{
			name:            "active lease defers to reconciler without payload cleanup",
			initialLease:    &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_ACTIVE},
			wantFailedEvent: true,
		},
		{
			name:         "closed lease finishes without rejection",
			initialLease: &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_CLOSED},
			wantCleanup:  true,
		},
		{
			name:        "missing lease finishes without rejection",
			wantCleanup: true,
		},
		{
			name:         "unknown initial state remains retryable",
			initialLease: &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_UNSPECIFIED},
			wantRetry:    true,
		},
		{
			name:            "terminal reject verdict with active reread defers to reconciler",
			initialLease:    &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			afterReject:     &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_ACTIVE},
			rejectErr:       billingtypes.ErrLeaseNotPending,
			wantReject:      true,
			wantFailedEvent: true,
		},
		{
			name:         "terminal reject verdict with closed reread suppresses stale failed event",
			initialLease: &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			afterReject:  &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_CLOSED},
			rejectErr:    billingtypes.ErrLeaseNotPending,
			wantReject:   true,
			wantCleanup:  true,
		},
		{
			name:         "terminal reject verdict with expired reread suppresses stale failed event",
			initialLease: &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			afterReject:  &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_EXPIRED},
			rejectErr:    billingtypes.ErrLeaseNotPending,
			wantReject:   true,
			wantCleanup:  true,
		},
		{
			name:         "terminal reject verdict with missing reread suppresses stale failed event",
			initialLease: &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			rejectErr:    billingtypes.ErrLeaseNotFound,
			wantReject:   true,
			wantCleanup:  true,
		},
		{
			name:         "terminal reject verdict with pending reread remains retryable",
			initialLease: &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			afterReject:  &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			rejectErr:    billingtypes.ErrLeaseNotPending,
			wantRetry:    true,
			wantReject:   true,
		},
		{
			name:         "terminal reject verdict with unknown reread remains retryable",
			initialLease: &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			afterReject:  &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_UNSPECIFIED},
			rejectErr:    billingtypes.ErrLeaseNotPending,
			wantRetry:    true,
			wantReject:   true,
		},
		{
			name:           "terminal reject verdict with failed reread remains retryable",
			initialLease:   &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			afterRejectErr: errors.New("reread failed"),
			rejectErr:      billingtypes.ErrLeaseNotFound,
			wantRetry:      true,
			wantReject:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := operation.NewRegistry()
			token := trackCallbackOperation(t, registry, "lease-1", "backend-a", true)
			payloads := &callbackPayloadRecorder{}
			events := &callbackEventRecorder{}
			var reads atomic.Int32
			var rejects atomic.Int32
			var refusals atomic.Int32
			service, err := NewCallbackService(CallbackServiceConfig{
				Operations: registry,
				Chain: &callbackChainStub{
					getLease: func(context.Context, string) (*billingtypes.Lease, error) {
						if reads.Add(1) == 1 {
							return tt.initialLease, nil
						}
						return tt.afterReject, tt.afterRejectErr
					},
					reject: func(context.Context, []string, string) (uint64, []string, error) {
						rejects.Add(1)
						return 1, []string{"tx"}, tt.rejectErr
					},
				},
				Placement: &callbackPlacementSpy{refuse: func(
					leaseUUID, backendName string,
					id operation.OperationID,
				) (bool, error) {
					refusals.Add(1)
					assert.Equal(t, "lease-1", leaseUUID)
					assert.Equal(t, "backend-a", backendName)
					assert.Equal(t, token.ID(), id)
					return true, nil
				}},
				Payloads: payloads,
				Events:   events,
			})
			require.NoError(t, err)

			err = service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
				LeaseUUID:   "lease-1",
				Status:      backend.CallbackStatusFailed,
				OperationID: callbackWireID(t, token.ID()),
			}))
			if tt.wantRetry {
				require.Error(t, err)
				record, exists := registry.Lookup("lease-1")
				require.True(t, exists)
				assert.Equal(t, operation.SettlementUnclaimed, record.Settlement)
				assert.Empty(t, payloads.deleted)
				assert.Zero(t, refusals.Load())
				assert.Empty(t, events.events)
			} else {
				require.NoError(t, err)
				assert.False(t, registry.Contains("lease-1"))
				if tt.wantFailedEvent {
					require.Len(t, events.events, 1)
					assert.Equal(t, backend.ProvisionStatusFailed, events.events[0].Status)
				} else {
					assert.Empty(t, events.events,
						"a superseding terminal chain lifecycle must not receive a stale failed event")
				}
				assert.Equal(t, int32(1), refusals.Load())
			}
			assert.Equal(t, tt.wantReject, rejects.Load() == 1)
			assert.Equal(t, tt.wantCleanup, len(payloads.deleted) == 1)
		})
	}
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

func TestCallbackService_LegacyLifecycleObservationDoesNotSettleCurrentTypedOperation(t *testing.T) {
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a", true)
	lifecycleAuthority := newTestPlacementAuthority(t)
	seedTestConfirmedPlacements(t, lifecycleAuthority, []string{"backend-a"}, map[string]string{
		"lease-1": "backend-a",
	})
	events := &callbackEventRecorder{}
	observer := &callbackDeprovisionRecorder{}
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations:          registry,
		LifecycleAuthority:  lifecycleAuthority,
		Events:              events,
		DeprovisionObserver: observer,
		Acknowledger: callbackAcknowledgerFunc(func(context.Context, string) (bool, string, error) {
			t.Fatal("an observational lifecycle callback must not acknowledge")
			return false, "", nil
		}),
		Placement: &callbackPlacementSpy{
			confirm: func(string, string, operation.OperationID) (bool, error) {
				t.Fatal("an observational lifecycle callback must not confirm placement")
				return false, nil
			},
			refuse: func(string, string, operation.OperationID) (bool, error) {
				t.Fatal("an observational lifecycle callback must not refuse placement")
				return false, nil
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID: "lease-1",
			Status:    backend.CallbackStatusFailed,
			Error:     "container exited",
		},
	)))
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID: "lease-1",
			Status:    backend.CallbackStatusDeprovisioned,
			Retained:  true,
		},
	)))

	record, exists := registry.Lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, token.ID(), record.ID)
	assert.Equal(t, operation.SettlementUnclaimed, record.Settlement,
		"legacy lifecycle observations must leave exact settlement authority untouched")
	assert.Zero(t, observer.calls,
		"body metadata on an observational callback cannot retire a backend candidate")
	require.Len(t, events.events, 2)
	assert.Equal(t, backend.ProvisionStatusFailed, events.events[0].Status)
	assert.Equal(t, "container exited", events.events[0].Error)
	assert.Equal(t, backend.ProvisionStatusRetained, events.events[1].Status)
}

func TestCallbackService_LegacyNonInFlightDeprovisionCannotRetireBackendCandidate(t *testing.T) {
	registry := operation.NewRegistry()
	lifecycleAuthority := newTestPlacementAuthority(t)
	seedTestConfirmedPlacements(t, lifecycleAuthority, []string{"backend-a"}, map[string]string{
		"lease-1": "backend-a",
	})
	events := &callbackEventRecorder{}
	observer := &callbackDeprovisionRecorder{}
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations:          registry,
		LifecycleAuthority:  lifecycleAuthority,
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
		"the legacy callback remains status-compatible with v0.13")
	assert.Equal(t, backend.ProvisionStatusRetained, events.events[0].Status)
	assert.Equal(t, retainedLeaseNotice, events.events[0].Error)
}

func TestCallbackService_TypedLifecycleCapabilityIsRevocableAndObservationOnly(t *testing.T) {
	registry := operation.NewRegistry()
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174099")
	require.NoError(t, err)
	attempt := beginTestNewPlacementAttempt(
		t, store, "lease-1", "backend-a", operationID,
	)
	confirmed, err := store.ConfirmAttempt(attempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	lifecycleID, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)

	events := &callbackEventRecorder{}
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations:         registry,
		LifecycleAuthority: store,
		Events:             events,
	})
	require.NoError(t, err)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "lease-1",
			Backend:     "body-supplied-backend",
			Status:      backend.CallbackStatusSuccess,
			LifecycleID: lifecycleID.String(),
		},
	)))
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusReady, events.events[0].Status)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "lease-1",
			Backend:     "body-supplied-backend",
			Status:      backend.CallbackStatusFailed,
			Error:       "container exited",
			LifecycleID: lifecycleID.String(),
		},
	)))
	require.Len(t, events.events, 2)
	assert.Equal(t, backend.ProvisionStatusFailed, events.events[1].Status)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "lease-1",
			Status:      backend.CallbackStatusDeprovisioned,
			Retained:    true,
			LifecycleID: lifecycleID.String(),
		},
	)))
	require.Len(t, events.events, 3)
	assert.Equal(t, backend.ProvisionStatusRetained, events.events[2].Status)
	assert.True(t, store.AuthorizeLifecycle("lease-1", lifecycleID).Retired())

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "lease-1",
			Status:      backend.CallbackStatusFailed,
			LifecycleID: lifecycleID.String(),
		},
	)))
	assert.Len(t, events.events, 3, "a retired capability must be an idempotent no-op")
}

func TestCallbackService_TeardownOnlyCapabilityAcceptsOnlyTerminalConsume(t *testing.T) {
	registry := operation.NewRegistry()
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174097")
	require.NoError(t, err)
	attempt := beginTestNewPlacementAttempt(
		t, store, "lease-1", "backend-a", operationID,
	)
	confirmed, err := store.ConfirmAttempt(attempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	lifecycleID, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)

	placementRecord := store.Lookup("lease-1")
	deleted, err := store.DeleteRecord(placementRecord.RecordRevision())
	require.NoError(t, err)
	require.True(t, deleted)
	require.Equal(t, placement.LifecycleVerdictTeardownOnly,
		store.AuthorizeLifecycle("lease-1", lifecycleID).Verdict())

	events := &callbackEventRecorder{}
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations:         registry,
		LifecycleAuthority: store,
		Events:             events,
	})
	require.NoError(t, err)

	for _, status := range []backend.CallbackStatus{
		backend.CallbackStatusSuccess,
		backend.CallbackStatusFailed,
	} {
		require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
			backend.CallbackPayload{
				LeaseUUID:   "lease-1",
				Status:      status,
				LifecycleID: lifecycleID.String(),
			},
		)))
	}
	assert.Empty(t, events.events, "teardown-only authority cannot publish runtime state")
	assert.Equal(t, placement.LifecycleVerdictTeardownOnly,
		store.AuthorizeLifecycle("lease-1", lifecycleID).Verdict())

	staleID, err := lifecycle.ParseID("123e4567-e89b-42d3-a456-426614174096")
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "lease-1",
			Status:      backend.CallbackStatusDeprovisioned,
			Retained:    true,
			LifecycleID: staleID.String(),
		},
	)))
	assert.Empty(t, events.events, "a stale lifecycle cannot consume teardown authority")

	terminal := backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Status:      backend.CallbackStatusDeprovisioned,
		Retained:    true,
		LifecycleID: lifecycleID.String(),
	}
	require.NoError(t, service.HandleCallback(
		context.Background(), callbackCommand(t, terminal),
	))
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusRetained, events.events[0].Status)
	assert.True(t, store.AuthorizeLifecycle("lease-1", lifecycleID).Retired())

	require.NoError(t, service.HandleCallback(
		context.Background(), callbackCommand(t, terminal),
	))
	assert.Len(t, events.events, 1, "duplicate terminal consume must not republish")
}

func TestCallbackService_LegacyTeardownOnlyCapabilityIsTerminalOnly(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	projectTestPlacementInventory(t, store, []string{"backend-a"},
		placement.InventoryProjection{Placements: map[string]string{
			"legacy": "backend-a",
		}})
	record := store.Lookup("legacy")
	deleted, err := store.DeleteRecord(record.RecordRevision())
	require.NoError(t, err)
	require.True(t, deleted)
	require.Equal(t, placement.LifecycleVerdictTeardownOnly,
		store.AuthorizeLifecycle("legacy", lifecycle.ID{}).Verdict())

	events := &callbackEventRecorder{}
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations:         operation.NewRegistry(),
		LifecycleAuthority: store,
		Events:             events,
	})
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID: "legacy",
			Status:    backend.CallbackStatusFailed,
		},
	)))
	assert.Empty(t, events.events)

	terminal := backend.CallbackPayload{
		LeaseUUID: "legacy",
		Status:    backend.CallbackStatusDeprovisioned,
		Retained:  true,
	}
	require.NoError(t, service.HandleCallback(
		context.Background(), callbackCommand(t, terminal),
	))
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusRetained, events.events[0].Status)
	assert.True(t, store.AuthorizeLifecycle("legacy", lifecycle.ID{}).Retired())
}

func TestCallbackService_PlacementConflictWithdrawsLifecycleObservationAuthority(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a", "backend-b"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174098")
	require.NoError(t, err)
	attempt := beginTestNewPlacementAttempt(
		t, store, "lease-1", "backend-a", operationID,
	)
	confirmed, err := store.ConfirmAttempt(attempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	lifecycleID, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)

	projectTestPlacementInventory(t, store, []string{"backend-a", "backend-b"},
		placement.InventoryProjection{Conflicts: map[string][]string{
			"lease-1": {"backend-a", "backend-b"},
		}})
	require.Equal(t,
		placement.LifecycleVerdictUnusable,
		store.AuthorizeLifecycle("lease-1", lifecycleID).Verdict(),
	)

	events := &callbackEventRecorder{}
	service, err := NewCallbackService(CallbackServiceConfig{
		Operations:         operation.NewRegistry(),
		LifecycleAuthority: store,
		Events:             events,
	})
	require.NoError(t, err)
	for _, status := range []backend.CallbackStatus{
		backend.CallbackStatusFailed,
		backend.CallbackStatusDeprovisioned,
	} {
		require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
			backend.CallbackPayload{
				LeaseUUID:   "lease-1",
				Status:      status,
				LifecycleID: lifecycleID.String(),
			},
		)))
	}
	assert.Empty(t, events.events)
	assert.Equal(t,
		placement.LifecycleVerdictUnusable,
		store.AuthorizeLifecycle("lease-1", lifecycleID).Verdict(),
		"a conflicted backend cannot publish or retire lifecycle state",
	)
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
