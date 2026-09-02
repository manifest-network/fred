package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

func newTestV013PlacementAuthority(
	t testing.TB,
	placements map[string]string,
) *placement.Store {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("placements"))
		if err != nil {
			return err
		}
		for leaseUUID, backendName := range placements {
			value, marshalErr := json.Marshal(struct {
				Backend string    `json:"backend"`
				SetAt   time.Time `json:"set_at"`
			}{
				Backend: backendName,
				SetAt:   time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
			})
			if marshalErr != nil {
				return marshalErr
			}
			if err := bucket.Put([]byte(leaseUUID), value); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	backendSet := make(map[string]struct{})
	inventories := make(map[string]placement.BackendInventory)
	for leaseUUID, backendName := range placements {
		backendSet[backendName] = struct{}{}
		inventory := inventories[backendName]
		inventory.StorageIdentity = testBackendStorageID(backendName)
		inventory.Provisions = append(inventory.Provisions, leaseUUID)
		if inventory.ProvisionProviderUUIDs == nil {
			inventory.ProvisionProviderUUIDs = make(map[string]string)
		}
		inventory.ProvisionProviderUUIDs[leaseUUID] = ""
		if inventory.ProvisionItems == nil {
			inventory.ProvisionItems = make(map[string][]backend.LeaseItem)
		}
		inventory.ProvisionItems[leaseUUID] = []backend.LeaseItem{{
			SKU: "sku-test", Quantity: 1, ServiceName: "app",
		}}
		if inventory.Retentions == nil {
			inventory.Retentions = []string{}
		}
		inventories[backendName] = inventory
	}
	backendNames := make([]string, 0, len(backendSet))
	for backendName := range backendSet {
		backendNames = append(backendNames, backendName)
	}
	slices.Sort(backendNames)
	for backendName, inventory := range inventories {
		slices.Sort(inventory.Provisions)
		inventories[backendName] = inventory
	}
	preparer, err := placement.OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	leaseUUIDs := slices.Sorted(maps.Keys(placements))
	chainProof, err := placementstore.LegacyUpgradeChainProof(testProviderUUID, leaseUUIDs...)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	backupTarget, err := placement.BindExactBackupTarget(backupPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backupTarget.Close()) })
	capability, err := preparer.AuthorizePreparation(
		ctx, testProviderUUID, backendNames, inventories, chainProof, backupTarget,
		placement.LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	_, err = preparer.PrepareContext(
		ctx, testProviderUUID, backendNames, inventories,
		chainProof, capability,
	)
	require.NoError(t, err)
	require.NoError(t, preparer.Close())

	store, err := placement.OpenStore(dbPath, testProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

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
	confirm        func(string, string, operation.OperationID) (bool, error)
	refuse         func(string, string, operation.OperationID) (bool, error)
	claim          func(string, operation.OperationID) (placement.AttemptClaim, bool, error)
	releaseClaim   func(placement.AttemptClaim) bool
	confirmClaimed func(placement.AttemptClaim) (bool, error)
	refuseClaimed  func(placement.AttemptClaim) (bool, error)
}

type callbackStorageIdentityResolver map[string]backendidentity.ID

func (resolver callbackStorageIdentityResolver) ExpectedBackendStorageIdentity(
	backendName string,
) (backendidentity.ID, bool) {
	id, ok := resolver[backendName]
	return id, ok
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

func (placementSpy *callbackPlacementSpy) ClaimAttempt(
	leaseUUID string,
	id operation.OperationID,
) (placement.AttemptClaim, bool, error) {
	if placementSpy.claim == nil {
		return placement.AttemptClaim{}, false, nil
	}
	return placementSpy.claim(leaseUUID, id)
}

func (placementSpy *callbackPlacementSpy) ReleaseAttemptClaim(
	claim placement.AttemptClaim,
) bool {
	if placementSpy.releaseClaim == nil {
		return false
	}
	return placementSpy.releaseClaim(claim)
}

func (placementSpy *callbackPlacementSpy) ConfirmClaimedAttempt(
	claim placement.AttemptClaim,
) (bool, error) {
	if placementSpy.confirmClaimed == nil {
		return false, nil
	}
	return placementSpy.confirmClaimed(claim)
}

func (placementSpy *callbackPlacementSpy) RefuseClaimedAttempt(
	claim placement.AttemptClaim,
) (bool, error) {
	if placementSpy.refuseClaimed == nil {
		return false, nil
	}
	return placementSpy.refuseClaimed(claim)
}

func callbackPlacementStoreAdapter(store *placement.Store) *callbackPlacementSpy {
	return &callbackPlacementSpy{
		confirm:        store.ConfirmOperation,
		refuse:         store.RefuseOperation,
		claim:          store.ClaimAttempt,
		releaseClaim:   store.ReleaseAttemptClaim,
		confirmClaimed: store.ConfirmClaimedAttempt,
		refuseClaimed:  store.RefuseClaimedAttempt,
	}
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
) callbackOperationToken {
	t.Helper()
	claimResult := registry.TryClaimLeaseNow(leaseUUID)
	require.True(t, claimResult.Acquired())
	claim := claimResult.Claim()
	result := registry.TryInitiateClaimed(claim, operation.TrackSpec{
		LeaseUUID: leaseUUID,
		Tenant:    "tenant-a",
		Backend:   backendName,
		Kind:      operation.KindProvision,
	})
	require.True(t, result.Started())
	initiation := result.Capability()
	require.True(t, registry.BeginCall(initiation))
	require.Equal(t, operation.InitiationActivated, registry.Activate(initiation))
	require.True(t, registry.ReleaseLease(claim))
	return callbackOperationToken{id: initiation.ID()}
}

type callbackOperationToken struct{ id operation.OperationID }

func (token callbackOperationToken) ID() operation.OperationID { return token.id }

func callbackWireID(t testing.TB, id operation.OperationID) string {
	t.Helper()
	require.True(t, id.Valid())
	return id.String()
}

func callbackBackendStorageID(t testing.TB, value string) backendidentity.ID {
	t.Helper()
	id, err := backendidentity.Parse(value)
	require.NoError(t, err)
	return id
}

func callbackCommand(t testing.TB, callback backend.CallbackPayload) CallbackCommand {
	t.Helper()
	if callback.BackendStorageID == "" {
		callback.BackendStorageID = defaultCallbackTestStorageIdentity.String()
	}
	return callbackCommandRaw(t, callback)
}

func callbackCommandRaw(t testing.TB, callback backend.CallbackPayload) CallbackCommand {
	t.Helper()
	command, err := NewCallbackCommand(callback)
	require.NoError(t, err)
	return command
}

func TestNewCallbackService_RequiresCompleteAuthority(t *testing.T) {
	_, err := NewCallbackService(CallbackServiceConfig{})
	require.ErrorIs(t, err, errCallbackOperationsUnavailable)

	var typedNil *operation.Registry
	_, err = NewCallbackService(CallbackServiceConfig{Operations: typedNil})
	require.ErrorIs(t, err, errCallbackOperationsUnavailable)

	store := newTestPlacementAuthority(t)
	valid := CallbackServiceConfig{
		Operations: operation.NewRegistry(),
		Chain:      &callbackChainStub{},
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			return true, "", nil
		}),
		Placement:          store,
		StorageIdentities:  store,
		LifecycleAuthority: store,
		Payloads:           (*typedNilCallbackPayloadStore)(nil),
	}
	for _, test := range []struct {
		name string
		omit func(*CallbackServiceConfig)
		want error
	}{
		{name: "chain", omit: func(cfg *CallbackServiceConfig) { cfg.Chain = nil }, want: errCallbackChainUnavailable},
		{name: "acknowledger", omit: func(cfg *CallbackServiceConfig) { cfg.Acknowledger = nil }, want: errCallbackAcknowledgerUnavailable},
		{name: "placement", omit: func(cfg *CallbackServiceConfig) { cfg.Placement = nil }, want: errCallbackPlacementUnavailable},
		{name: "storage identities", omit: func(cfg *CallbackServiceConfig) { cfg.StorageIdentities = nil }, want: errCallbackStorageIdentityUnavailable},
		{name: "lifecycle authority", omit: func(cfg *CallbackServiceConfig) { cfg.LifecycleAuthority = nil }, want: errCallbackLifecycleUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.omit(&cfg)
			_, err := NewCallbackService(cfg)
			require.ErrorIs(t, err, test.want)
		})
	}

	service, err := NewCallbackService(valid)
	require.NoError(t, err)
	assert.Nil(t, service.payloads, "typed-nil optional capabilities must be normalized")
	require.ErrorIs(t,
		service.HandleCallback(context.Background(), CallbackCommand{}),
		errInvalidCallbackCommand,
		"the zero command must never authorize a registry lookup or mutation",
	)
}

func TestCallbackServiceRejectsMissingOrMismatchedStorageIdentityBeforeSettlement(t *testing.T) {
	t.Parallel()

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, leaseUUID, "docker-a")
	expected := callbackBackendStorageID(t, "6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	wrong := callbackBackendStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations:        registry,
		StorageIdentities: callbackStorageIdentityResolver{"docker-a": expected},
	})
	require.NoError(t, err)

	for _, test := range []struct {
		name      string
		storageID string
		want      error
	}{
		{name: "missing", want: errCallbackStorageIdentityMissing},
		{name: "mismatch", storageID: wrong.String(), want: errCallbackStorageIdentityMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := callbackCommandRaw(t, backend.CallbackPayload{
				LeaseUUID:        leaseUUID,
				Status:           backend.CallbackStatusSuccess,
				OperationID:      callbackWireID(t, token.ID()),
				BackendStorageID: test.storageID,
			})
			err := service.HandleCallback(context.Background(), command)
			assert.ErrorIs(t, err, test.want)
			record, exists := registry.Lookup(leaseUUID)
			require.True(t, exists)
			assert.Equal(t, token.ID(), record.ID)
		})
	}
}

type typedNilCallbackPayloadStore struct{}

func (*typedNilCallbackPayloadStore) Delete(string) {}

func TestNewCallbackCommand_ConvertsWireIdentityAtBoundary(t *testing.T) {
	legacy, err := NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID: "legacy",
		Status:    backend.CallbackStatusSuccess,
	})
	require.NoError(t, err)
	assert.True(t, legacy.valid)
	assert.Equal(t, callbackSelectorLegacy, legacy.selector)
	assert.False(t, legacy.operationID.Valid())
	assert.False(t, legacy.lifecycleID.Valid())

	command, err := NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID:   "typed",
		Status:      backend.CallbackStatusSuccess,
		OperationID: "123e4567-e89b-42d3-a456-426614174000",
	})
	require.NoError(t, err)
	assert.True(t, command.valid)
	assert.Equal(t, callbackSelectorOperation, command.selector)
	assert.Equal(t, "123e4567-e89b-42d3-a456-426614174000", command.operationID.String())
	assert.False(t, command.lifecycleID.Valid())

	lifecycleCommand, err := NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID:   "lifecycle",
		Status:      backend.CallbackStatusSuccess,
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
		Status:      backend.CallbackStatusSuccess,
		OperationID: "not-a-uuid",
	})
	assert.ErrorIs(t, err, operation.ErrInvalidID)

	_, err = NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID:   "ambiguous",
		Status:      backend.CallbackStatusSuccess,
		OperationID: "123e4567-e89b-42d3-a456-426614174000",
		LifecycleID: "123e4567-e89b-42d3-a456-426614174001",
	})
	assert.Error(t, err)
}

func TestNewCallbackCommand_RejectsStructurallyInvalidEnvelope(t *testing.T) {
	tests := []struct {
		name    string
		payload backend.CallbackPayload
		wantErr string
	}{
		{
			name:    "empty lease UUID",
			payload: backend.CallbackPayload{Status: backend.CallbackStatusSuccess},
			wantErr: "lease UUID is required",
		},
		{
			name:    "empty status",
			payload: backend.CallbackPayload{LeaseUUID: "lease"},
			wantErr: "invalid callback status",
		},
		{
			name: "unknown status",
			payload: backend.CallbackPayload{
				LeaseUUID: "lease", Status: backend.CallbackStatus("unknown"),
			},
			wantErr: "invalid callback status",
		},
		{
			name: "retained success",
			payload: backend.CallbackPayload{
				LeaseUUID: "lease", Status: backend.CallbackStatusSuccess, Retained: true,
			},
			wantErr: "retained flag requires deprovisioned status",
		},
		{
			name: "deprovisioned operation callback",
			payload: backend.CallbackPayload{
				LeaseUUID:   "lease",
				Status:      backend.CallbackStatusDeprovisioned,
				OperationID: "123e4567-e89b-42d3-a456-426614174000",
			},
			wantErr: "requires lifecycle or legacy authority",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, err := NewCallbackCommand(test.payload)
			assert.ErrorContains(t, err, test.wantErr)
			assert.False(t, command.valid)
		})
	}

	command, err := NewCallbackCommand(backend.CallbackPayload{
		LeaseUUID: "non-uuid-test-lease", Status: backend.CallbackStatusDeprovisioned,
		Retained: true,
	})
	require.NoError(t, err)
	assert.True(t, command.valid, "internal test lease IDs deliberately need not be UUIDs")
}

func TestCallbackService_AuthorizesOnlyMatchingOperation(t *testing.T) {
	tests := []struct {
		name        string
		callbackID  func(testing.TB, operation.OperationID) string
		backend     string
		wantApplied bool
	}{
		{
			name:       "missing token is structurally rejected",
			callbackID: func(testing.TB, operation.OperationID) string { return "" },
		},
		{
			name:        "exact token is accepted",
			callbackID:  callbackWireID,
			backend:     "backend-a",
			wantApplied: true,
		},
		{
			name: "different nonzero token is rejected",
			callbackID: func(t testing.TB, id operation.OperationID) string {
				t.Helper()
				return "d9428888-122b-41e1-b85c-61c67afba0c6"
			},
			backend: "backend-a",
		},
		{
			name:        "legacy metrics backend cannot redirect exact token",
			callbackID:  callbackWireID,
			backend:     "backend-b",
			wantApplied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := operation.NewRegistry()
			token := trackCallbackOperation(t, registry, "lease-1", "backend-a")
			lifecycleAuthority := newTestPlacementAuthority(t)
			var placementCalls atomic.Int32
			var acknowledgeCalls atomic.Int32
			service, err := newCallbackServiceForTest(CallbackServiceConfig{
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

func TestCallbackService_EventSinkPanicDoesNotRetryOrWedgeLaterCallback(t *testing.T) {
	const leaseUUID = "lease-1"

	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, leaseUUID, "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, leaseUUID, "backend-a", token.ID())

	var publishCalls int
	var observed []backend.LeaseStatusEvent
	events := callbackEventSinkFunc(func(
		leaseUUID string,
		status backend.ProvisionStatus,
		errMsg string,
	) {
		publishCalls++
		if publishCalls == 1 {
			panic("subscriber failure")
		}
		observed = append(observed, backend.LeaseStatusEvent{
			LeaseUUID: leaseUUID,
			Status:    status,
			Error:     errMsg,
		})
	})
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations:         registry,
		Placement:          store,
		LifecycleAuthority: store,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			return true, "tx-ack", nil
		}),
		Events: events,
	})
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
		metrics.LifecycleEventCallback,
	))
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   leaseUUID,
			Status:      backend.CallbackStatusSuccess,
			OperationID: token.ID().String(),
		},
	)))
	assert.False(t, registry.Contains(leaseUUID),
		"an observational sink panic must not turn completed settlement into a retry")
	assert.Equal(t, placement.StateConfirmed, store.Lookup(leaseUUID).State())
	assert.Equal(t, before+1, promtestutil.ToFloat64(
		metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(metrics.LifecycleEventCallback),
	))

	lifecycleID, err := lifecycle.FromOperationID(token.ID())
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   leaseUUID,
			Status:      backend.CallbackStatusFailed,
			Error:       "runtime failure",
			LifecycleID: lifecycleID.String(),
		},
	)))
	require.Len(t, observed, 1,
		"the callback after the panicking delivery must still reach the per-lease sink")
	assert.Equal(t, backend.ProvisionStatusFailed, observed[0].Status)
	assert.Equal(t, "runtime failure", observed[0].Error)
}

func TestCallbackService_SuccessSettlesExactDurableAttempt(t *testing.T) {
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, "lease-1", "backend-a", token.ID())

	var acknowledgeCalls atomic.Int32
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
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

func TestCallbackService_SuccessPlacementIOFailureAcknowledgesAndRecoversDurably(
	t *testing.T,
) {
	const leaseUUID = "lease-success-placement-io"
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, leaseUUID, "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, leaseUUID, "backend-a", token.ID())

	placementWriteErr := errors.New("placement disk temporarily unavailable")
	placementAdapter := callbackPlacementStoreAdapter(store)
	confirm := placementAdapter.confirm
	var confirmCalls atomic.Int32
	placementAdapter.confirm = func(
		leaseUUID, backendName string,
		operationID operation.OperationID,
	) (bool, error) {
		if confirmCalls.Add(1) == 1 {
			return false, placementWriteErr
		}
		return confirm(leaseUUID, backendName, operationID)
	}

	var (
		acknowledgeCalls atomic.Int32
		timeoutRejects   atomic.Int32
		chainActive      atomic.Bool
	)
	chain := &callbackChainStub{
		getLease: func(context.Context, string) (*billingtypes.Lease, error) {
			state := billingtypes.LEASE_STATE_PENDING
			if chainActive.Load() {
				state = billingtypes.LEASE_STATE_ACTIVE
			}
			return &billingtypes.Lease{Uuid: leaseUUID, State: state}, nil
		},
		reject: func(context.Context, []string, string) (uint64, []string, error) {
			timeoutRejects.Add(1)
			require.True(t, chainActive.Load(),
				"the positive callback must acknowledge before releasing its claim")
			return 0, nil, billingtypes.ErrLeaseNotPending
		},
	}
	events := &callbackEventRecorder{}
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: registry,
		Placement:  placementAdapter,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			if acknowledgeCalls.Add(1) == 1 {
				chainActive.Store(true)
				return true, "tx-ack", nil
			}
			return false, "", billingtypes.ErrLeaseNotPending
		}),
		Chain:  chain,
		Events: events,
	})
	require.NoError(t, err)
	callback := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID: leaseUUID, Status: backend.CallbackStatusSuccess,
		OperationID: token.ID().String(),
	})

	err = service.HandleCallback(context.Background(), callback)
	require.ErrorIs(t, err, placementWriteErr)
	assert.Equal(t, int32(1), acknowledgeCalls.Load(),
		"placement I/O must not prevent the authoritative positive result from reaching chain")
	record, exists := registry.Lookup(leaseUUID)
	require.True(t, exists, "transient placement failure must preserve volatile settlement authority")
	assert.Equal(t, operation.SettlementUnclaimed, record.Settlement)
	assert.Equal(t, placement.StateAttempting, store.Lookup(leaseUUID).State())
	assert.Empty(t, events.events, "Ready must wait for both chain and durable placement")

	NewTimeoutChecker(TimeoutCheckerConfig{
		Operations: registry,
		Rejecter:   chain,
		Timeout:    -time.Nanosecond,
	}).CheckOnce(context.Background())
	assert.Equal(t, int32(1), timeoutRejects.Load())
	assert.False(t, registry.Contains(leaseUUID),
		"an ACTIVE chain verdict lets timeout retire only the volatile operation")
	assert.Equal(t, placement.StateAttempting, store.Lookup(leaseUUID).State(),
		"timeout must leave exact durable evidence for callback recovery")

	require.NoError(t, service.HandleCallback(context.Background(), callback))
	assert.Equal(t, int32(2), acknowledgeCalls.Load())
	assert.Equal(t, placement.StateConfirmed, store.Lookup(leaseUUID).State())
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusReady, events.events[0].Status)
}

func TestCallbackService_FailurePlacementIOAfterRejectRetriesWithoutRejectingAgain(
	t *testing.T,
) {
	const leaseUUID = "lease-failure-placement-io"
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, leaseUUID, "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, leaseUUID, "backend-a", token.ID())

	placementWriteErr := errors.New("placement disk temporarily unavailable")
	placementAdapter := callbackPlacementStoreAdapter(store)
	refuse := placementAdapter.refuse
	var refuseCalls atomic.Int32
	placementAdapter.refuse = func(
		leaseUUID, backendName string,
		operationID operation.OperationID,
	) (bool, error) {
		if refuseCalls.Add(1) == 1 {
			return false, placementWriteErr
		}
		return refuse(leaseUUID, backendName, operationID)
	}

	chainState := billingtypes.LEASE_STATE_PENDING
	var rejectCalls atomic.Int32
	chain := &callbackChainStub{
		getLease: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{Uuid: leaseUUID, State: chainState}, nil
		},
		reject: func(context.Context, []string, string) (uint64, []string, error) {
			rejectCalls.Add(1)
			chainState = billingtypes.LEASE_STATE_REJECTED
			return 1, []string{"tx-reject"}, nil
		},
	}
	events := &callbackEventRecorder{}
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: registry,
		Placement:  placementAdapter,
		Chain:      chain,
		Events:     events,
	})
	require.NoError(t, err)
	callback := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID: leaseUUID, Status: backend.CallbackStatusFailed,
		OperationID: token.ID().String(), Error: "backend refused provision",
	})

	err = service.HandleCallback(context.Background(), callback)
	require.ErrorIs(t, err, placementWriteErr)
	assert.Equal(t, int32(1), rejectCalls.Load())
	record, exists := registry.Lookup(leaseUUID)
	require.True(t, exists, "transient placement failure must preserve exact settlement")
	assert.Equal(t, operation.SettlementUnclaimed, record.Settlement)
	assert.Equal(t, placement.StateAttempting, store.Lookup(leaseUUID).State())
	assert.Empty(t, events.events)

	require.NoError(t, service.HandleCallback(context.Background(), callback))
	assert.Equal(t, int32(1), rejectCalls.Load(),
		"retry must observe the terminal chain state instead of issuing a second rejection")
	assert.False(t, registry.Contains(leaseUUID))
	assert.Equal(t, placement.StateAbsent, store.Lookup(leaseUUID).State())
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusFailed, events.events[0].Status)
}

func TestCallbackService_FailureCannotClearDifferentDurableOperation(t *testing.T) {
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a")
	store := newTestPlacementAuthority(t)
	newerID, err := operation.ParseID("d9428888-122b-41e1-b85c-61c67afba0c6")
	require.NoError(t, err)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, "lease-1", "backend-a", newerID)

	var rejectCalls atomic.Int32
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
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
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, "lease-1", "backend-a", token.ID())

	var calls atomic.Int32
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
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
			name:      "missing lease remains retryable",
			wantRetry: true,
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
			token := trackCallbackOperation(t, registry, "lease-1", "backend-a")
			events := &callbackEventRecorder{}
			var reads atomic.Int32
			service, err := newCallbackServiceForTest(CallbackServiceConfig{
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
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a")
	payloads := &callbackPayloadRecorder{}
	events := &callbackEventRecorder{}
	var reads atomic.Int32
	var rejects atomic.Int32
	var refusals atomic.Int32
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
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
			name:      "missing lease remains retryable",
			wantRetry: true,
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
			name:         "terminal reject verdict with missing reread remains retryable",
			initialLease: &billingtypes.Lease{Uuid: "lease-1", State: billingtypes.LEASE_STATE_PENDING},
			rejectErr:    billingtypes.ErrLeaseNotFound,
			wantReject:   true,
			wantRetry:    true,
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
			token := trackCallbackOperation(t, registry, "lease-1", "backend-a")
			payloads := &callbackPayloadRecorder{}
			events := &callbackEventRecorder{}
			var reads atomic.Int32
			var rejects atomic.Int32
			var refusals atomic.Int32
			service, err := newCallbackServiceForTest(CallbackServiceConfig{
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
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	beginTestNewPlacementAttempt(t, store, "lease-1", "backend-a", token.ID())

	acknowledgeStarted := make(chan struct{})
	releaseAcknowledge := make(chan struct{})
	var acknowledgeCalls atomic.Int32
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
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

func TestCallbackService_DeprovisionOwnedExactCallbackSettlesDurableAttemptWithoutChainMutation(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    backend.CallbackStatus
		wantState placement.State
	}{
		{name: "success confirms affinity", status: backend.CallbackStatusSuccess, wantState: placement.StateConfirmed},
		{name: "failure refuses attempt", status: backend.CallbackStatusFailed, wantState: placement.StateAbsent},
	} {
		t.Run(test.name, func(t *testing.T) {
			const leaseUUID = "lease-1"
			const backendName = "backend-a"
			registry := operation.NewRegistry()
			token := trackCallbackOperation(t, registry, leaseUUID, backendName)
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, []string{backendName})
			beginTestNewPlacementAttempt(t, store, leaseUUID, backendName, token.ID())

			deprovision := registry.TryClaimDeprovision(leaseUUID, token.ID())
			require.True(t, deprovision.Claimed())
			service, err := newCallbackServiceForTest(CallbackServiceConfig{
				Operations: registry,
				Placement:  store,
				Acknowledger: callbackAcknowledgerFunc(func(
					context.Context, string,
				) (bool, string, error) {
					t.Fatal("deprovision-owned callback must not mutate chain state")
					return false, "", nil
				}),
			})
			require.NoError(t, err)
			require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
				backend.CallbackPayload{
					LeaseUUID:   leaseUUID,
					Status:      test.status,
					OperationID: callbackWireID(t, token.ID()),
				},
			)))

			record, exists := registry.Lookup(leaseUUID)
			require.True(t, exists)
			assert.Equal(t, operation.SettlementDeprovision, record.Settlement,
				"callback must leave close in control of the volatile operation")
			assert.Equal(t, test.wantState, store.Lookup(leaseUUID).State())
			assert.Empty(t, store.Lookup(leaseUUID).Attempt,
				"a 2xx callback response must never strand its durable write-ahead attempt")
			require.True(t, registry.FinishSettlement(deprovision.Claim()))
			assert.False(t, registry.Contains(leaseUUID))
		})
	}
}

func TestCallbackService_LegacyLifecycleObservationDoesNotSettleCurrentTypedOperation(t *testing.T) {
	const leaseUUID = "018f47a2-8b1c-7def-8123-456789abcde1"
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, leaseUUID, "backend-a")
	lifecycleAuthority := newTestV013PlacementAuthority(t, map[string]string{
		leaseUUID: "backend-a",
	})
	armTestPlacementTopology(t, lifecycleAuthority, []string{"backend-a"})
	events := &callbackEventRecorder{}
	observer := &callbackDeprovisionRecorder{}
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
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
			LeaseUUID: leaseUUID,
			Status:    backend.CallbackStatusFailed,
			Error:     "container exited",
		},
	)))
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID: leaseUUID,
			Status:    backend.CallbackStatusDeprovisioned,
			Retained:  true,
		},
	)))

	record, exists := registry.Lookup(leaseUUID)
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
	const leaseUUID = "018f47a2-8b1c-7def-8123-456789abcde1"
	registry := operation.NewRegistry()
	lifecycleAuthority := newTestV013PlacementAuthority(t, map[string]string{
		leaseUUID: "backend-a",
	})
	armTestPlacementTopology(t, lifecycleAuthority, []string{"backend-a"})
	events := &callbackEventRecorder{}
	observer := &callbackDeprovisionRecorder{}
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations:          registry,
		LifecycleAuthority:  lifecycleAuthority,
		Events:              events,
		DeprovisionObserver: observer,
	})
	require.NoError(t, err)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID: leaseUUID,
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
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
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

func TestCallbackService_LifecycleMetricsClassifyEveryReceivedCallback(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174095")
	require.NoError(t, err)
	attempt := beginTestNewPlacementAttempt(t, store, "lease-1", "backend-a", operationID)
	confirmed, err := store.ConfirmAttempt(attempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	lifecycleID, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)
	staleID, err := lifecycle.ParseID("123e4567-e89b-42d3-a456-426614174094")
	require.NoError(t, err)

	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations:         operation.NewRegistry(),
		LifecycleAuthority: store,
	})
	require.NoError(t, err)

	metric := func(outcome, verdict, status string) float64 {
		t.Helper()
		return promtestutil.ToFloat64(
			metrics.LifecycleCallbackOutcomesTotal.WithLabelValues(outcome, verdict, status),
		)
	}
	metricTotal := func(status string) float64 {
		t.Helper()
		var total float64
		for _, outcome := range []string{
			metrics.LifecycleCallbackOutcomeApplied,
			metrics.LifecycleCallbackOutcomeDropped,
			metrics.LifecycleCallbackOutcomeRetryable,
		} {
			for _, verdict := range []string{
				metrics.LifecycleCallbackVerdictAuthorized,
				metrics.LifecycleCallbackVerdictLegacy,
				metrics.LifecycleCallbackVerdictTeardownOnly,
				metrics.LifecycleCallbackVerdictRetired,
				metrics.LifecycleCallbackVerdictInvalid,
				metrics.LifecycleCallbackVerdictMissing,
				metrics.LifecycleCallbackVerdictStale,
				metrics.LifecycleCallbackVerdictUnusable,
				metrics.LifecycleCallbackVerdictUnavailable,
				metrics.LifecycleCallbackVerdictUnknown,
			} {
				total += metric(outcome, verdict, status)
			}
		}
		return total
	}
	received := func(status string) float64 {
		t.Helper()
		return promtestutil.ToFloat64(
			metrics.NonInFlightCallbacksTotal.WithLabelValues(labelBackendUnknown, status),
		)
	}

	appliedSuccessBefore := metric(
		metrics.LifecycleCallbackOutcomeApplied,
		metrics.LifecycleCallbackVerdictAuthorized,
		string(backend.CallbackStatusSuccess),
	)
	successTotalBefore := metricTotal(string(backend.CallbackStatusSuccess))
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "lease-1",
			Status:      backend.CallbackStatusSuccess,
			LifecycleID: lifecycleID.String(),
		},
	)))
	assert.Equal(t, 1.0, metric(
		metrics.LifecycleCallbackOutcomeApplied,
		metrics.LifecycleCallbackVerdictAuthorized,
		string(backend.CallbackStatusSuccess),
	)-appliedSuccessBefore)
	assert.Equal(t, 1.0, metricTotal(string(backend.CallbackStatusSuccess))-successTotalBefore,
		"one lifecycle callback must increment exactly one outcome series")

	receivedFailedBefore := received(string(backend.CallbackStatusFailed))
	failedTotalBefore := metricTotal(string(backend.CallbackStatusFailed))
	droppedStaleBefore := metric(
		metrics.LifecycleCallbackOutcomeDropped,
		metrics.LifecycleCallbackVerdictStale,
		string(backend.CallbackStatusFailed),
	)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "lease-1",
			Status:      backend.CallbackStatusFailed,
			LifecycleID: staleID.String(),
		},
	)))
	assert.Equal(t, 1.0, received(string(backend.CallbackStatusFailed))-receivedFailedBefore,
		"the compatibility metric must count a received lifecycle callback even when authorization drops it")
	assert.Equal(t, 1.0, metric(
		metrics.LifecycleCallbackOutcomeDropped,
		metrics.LifecycleCallbackVerdictStale,
		string(backend.CallbackStatusFailed),
	)-droppedStaleBefore)
	assert.Equal(t, 1.0, metricTotal(string(backend.CallbackStatusFailed))-failedTotalBefore,
		"one lifecycle callback must increment exactly one outcome series")

	deprovisionedTotalBefore := metricTotal(string(backend.CallbackStatusDeprovisioned))
	appliedTeardownBefore := metric(
		metrics.LifecycleCallbackOutcomeApplied,
		metrics.LifecycleCallbackVerdictAuthorized,
		string(backend.CallbackStatusDeprovisioned),
	)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "lease-1",
			Status:      backend.CallbackStatusDeprovisioned,
			LifecycleID: lifecycleID.String(),
		},
	)))
	assert.Equal(t, 1.0, metric(
		metrics.LifecycleCallbackOutcomeApplied,
		metrics.LifecycleCallbackVerdictAuthorized,
		string(backend.CallbackStatusDeprovisioned),
	)-appliedTeardownBefore)
	assert.Equal(t, 1.0,
		metricTotal(string(backend.CallbackStatusDeprovisioned))-deprovisionedTotalBefore,
		"one lifecycle callback must increment exactly one outcome series")

	receivedDeprovisionedBefore := received(string(backend.CallbackStatusDeprovisioned))
	deprovisionedTotalBefore = metricTotal(string(backend.CallbackStatusDeprovisioned))
	droppedRetiredBefore := metric(
		metrics.LifecycleCallbackOutcomeDropped,
		metrics.LifecycleCallbackVerdictRetired,
		string(backend.CallbackStatusDeprovisioned),
	)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "lease-1",
			Status:      backend.CallbackStatusDeprovisioned,
			LifecycleID: lifecycleID.String(),
		},
	)))
	assert.Equal(t, 1.0,
		received(string(backend.CallbackStatusDeprovisioned))-receivedDeprovisionedBefore,
		"a replay after retirement remains visible as received")
	assert.Equal(t, 1.0, metric(
		metrics.LifecycleCallbackOutcomeDropped,
		metrics.LifecycleCallbackVerdictRetired,
		string(backend.CallbackStatusDeprovisioned),
	)-droppedRetiredBefore)
	assert.Equal(t, 1.0,
		metricTotal(string(backend.CallbackStatusDeprovisioned))-deprovisionedTotalBefore,
		"one lifecycle callback must increment exactly one outcome series")

	noAuthority, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: operation.NewRegistry(),
	})
	require.NoError(t, err)
	retryableBefore := metric(
		metrics.LifecycleCallbackOutcomeRetryable,
		metrics.LifecycleCallbackVerdictUnavailable,
		string(backend.CallbackStatusSuccess),
	)
	successTotalBefore = metricTotal(string(backend.CallbackStatusSuccess))
	err = noAuthority.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "lease-2",
			Status:      backend.CallbackStatusSuccess,
			LifecycleID: lifecycleID.String(),
		},
	))
	require.ErrorIs(t, err, errCallbackLifecycleUnavailable)
	assert.Equal(t, 1.0, metric(
		metrics.LifecycleCallbackOutcomeRetryable,
		metrics.LifecycleCallbackVerdictUnavailable,
		string(backend.CallbackStatusSuccess),
	)-retryableBefore)
	assert.Equal(t, 1.0, metricTotal(string(backend.CallbackStatusSuccess))-successTotalBefore,
		"one lifecycle callback must increment exactly one outcome series")
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
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
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
	assert.Equal(t, placement.LifecycleVerdictMissing,
		store.AuthorizeLifecycle("lease-1", lifecycleID).Verdict())

	require.NoError(t, service.HandleCallback(
		context.Background(), callbackCommand(t, terminal),
	))
	assert.Len(t, events.events, 1, "duplicate terminal consume must not republish")
}

func TestCallbackService_LegacyTeardownOnlyCapabilityIsTerminalOnly(t *testing.T) {
	const leaseUUID = "018f47a2-8b1c-7def-8123-456789abcde2"
	store := newTestV013PlacementAuthority(t, map[string]string{
		leaseUUID: "backend-a",
	})
	armTestPlacementTopology(t, store, []string{"backend-a"})
	record := store.Lookup(leaseUUID)
	deleted, err := store.DeleteRecord(record.RecordRevision())
	require.NoError(t, err)
	require.True(t, deleted)
	require.Equal(t, placement.LifecycleVerdictTeardownOnly,
		store.AuthorizeLifecycle(leaseUUID, lifecycle.ID{}).Verdict())

	events := &callbackEventRecorder{}
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations:         operation.NewRegistry(),
		LifecycleAuthority: store,
		Events:             events,
	})
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID: leaseUUID,
			Status:    backend.CallbackStatusFailed,
		},
	)))
	assert.Empty(t, events.events)

	terminal := backend.CallbackPayload{
		LeaseUUID: leaseUUID,
		Status:    backend.CallbackStatusDeprovisioned,
		Retained:  true,
	}
	require.NoError(t, service.HandleCallback(
		context.Background(), callbackCommand(t, terminal),
	))
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusRetained, events.events[0].Status)
	assert.Equal(t, placement.LifecycleVerdictMissing,
		store.AuthorizeLifecycle(leaseUUID, lifecycle.ID{}).Verdict())
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
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
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
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a")
	service, err := newCallbackServiceForTest(CallbackServiceConfig{Operations: registry})
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

func TestCallbackService_ExactCallbackWithoutRegistryRequiresDurablePlacementAuthority(t *testing.T) {
	registry := operation.NewRegistry()
	service, err := newCallbackServiceForTest(CallbackServiceConfig{Operations: registry})
	require.NoError(t, err)
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174099")
	require.NoError(t, err)

	err = service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-1",
		Backend:     "backend-a",
		Status:      backend.CallbackStatusSuccess,
		OperationID: callbackWireID(t, operationID),
	}))
	require.ErrorIs(t, err, errCallbackPlacementUnavailable)
	_, exists := registry.Lookup("lease-1")
	assert.False(t, exists,
		"an exact callback must not manufacture volatile authority without durable placement")
}

func TestCallbackService_MissingStorageIdentityAuthorityFailsClosed(t *testing.T) {
	registry := operation.NewRegistry()
	token := trackCallbackOperation(t, registry, "lease-1", "backend-a")
	service, err := newCallbackService(CallbackServiceConfig{
		Operations: registry,
	})
	require.NoError(t, err)

	err = service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:        "lease-1",
		Backend:          "backend-a",
		Status:           backend.CallbackStatusSuccess,
		OperationID:      callbackWireID(t, token.ID()),
		BackendStorageID: "6ba7b811-9dad-41d1-80b4-00c04fd430c8",
	}))
	require.ErrorIs(t, err, errCallbackStorageIdentityUnavailable)
	record, exists := registry.Lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, operation.SettlementUnclaimed, record.Settlement)

	claim := registry.TryClaimCallback("lease-1", token.ID())
	require.True(t, claim.Claimed(), "failed identity verification must release exact settlement")
	require.True(t, registry.FinishSettlement(claim.Claim()))
}

func TestCallbackService_RecoversExactDurableAttemptWithoutRegistryRecord(t *testing.T) {
	tests := []struct {
		name       string
		status     backend.CallbackStatus
		wantState  placement.State
		wantAck    int
		wantReject int
	}{
		{
			name:      "success confirms and acknowledges",
			status:    backend.CallbackStatusSuccess,
			wantState: placement.StateConfirmed,
			wantAck:   1,
		},
		{
			name:       "failure refuses and rejects",
			status:     backend.CallbackStatusFailed,
			wantState:  placement.StateAbsent,
			wantReject: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, []string{"backend-a"})
			operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174090")
			require.NoError(t, err)
			beginTestNewPlacementAttempt(t, store, "lease-recovered", "backend-a", operationID)

			var acknowledgeCalls atomic.Int32
			var rejectCalls atomic.Int32
			operations := operation.NewRegistry()
			operations.BeginDrain()
			service, err := newCallbackServiceForTest(CallbackServiceConfig{
				Operations: operations,
				Placement:  store,
				Acknowledger: callbackAcknowledgerFunc(func(
					context.Context, string,
				) (bool, string, error) {
					acknowledgeCalls.Add(1)
					return true, "tx-ack", nil
				}),
				Chain: &callbackChainStub{
					getLease: func(context.Context, string) (*billingtypes.Lease, error) {
						return &billingtypes.Lease{
							Uuid: "lease-recovered", State: billingtypes.LEASE_STATE_PENDING,
						}, nil
					},
					reject: func(context.Context, []string, string) (uint64, []string, error) {
						rejectCalls.Add(1)
						return 1, []string{"tx-reject"}, nil
					},
				},
				LifecycleAuthority: store,
			})
			require.NoError(t, err)

			require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
				backend.CallbackPayload{
					LeaseUUID:   "lease-recovered",
					Status:      tt.status,
					Error:       "backend refused",
					Backend:     "body-controlled-backend",
					OperationID: operationID.String(),
				},
			)))

			current := store.Lookup("lease-recovered")
			assert.Equal(t, tt.wantState, current.State())
			assert.Empty(t, current.Attempt)
			assert.False(t, current.AttemptOperationID().Valid())
			assert.Equal(t, int32(tt.wantAck), acknowledgeCalls.Load())
			assert.Equal(t, int32(tt.wantReject), rejectCalls.Load())
			if tt.wantState == placement.StateConfirmed {
				assert.Equal(t, "backend-a", current.Backend,
					"the callback body must not select recovered ownership")
			}
		})
	}
}

func TestCallbackService_RecoveredFailureRetainsAttemptAcrossUnknownChainRead(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("a65e5ccb-2423-45be-8fc2-01388b16728e")
	require.NoError(t, err)
	beginTestNewPlacementAttempt(t, store, "lease-recovered-lag", "backend-a", operationID)

	payloads := &callbackPayloadRecorder{}
	var reads atomic.Int32
	var rejects atomic.Int32
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: operation.NewRegistry(),
		Placement:  store,
		Chain: &callbackChainStub{
			getLease: func(context.Context, string) (*billingtypes.Lease, error) {
				if reads.Add(1) == 1 {
					return nil, nil
				}
				return &billingtypes.Lease{
					Uuid: "lease-recovered-lag", State: billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
			reject: func(context.Context, []string, string) (uint64, []string, error) {
				rejects.Add(1)
				return 1, []string{"tx-reject"}, nil
			},
		},
		Payloads:           payloads,
		LifecycleAuthority: store,
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-recovered-lag",
		Status:      backend.CallbackStatusFailed,
		Error:       "backend refused",
		OperationID: operationID.String(),
	})

	err = service.HandleCallback(context.Background(), command)
	require.ErrorContains(t, err, "unknown state")
	assert.Equal(t, placement.StateAttempting, store.Lookup("lease-recovered-lag").State(),
		"an absent RPC view cannot erase exact redelivery authority")
	assert.Empty(t, payloads.deleted,
		"an absent RPC view cannot erase the payload needed by a later sweep")
	assert.Zero(t, rejects.Load())

	require.NoError(t, service.HandleCallback(context.Background(), command))
	assert.Equal(t, placement.StateAbsent, store.Lookup("lease-recovered-lag").State())
	assert.Equal(t, []string{"lease-recovered-lag"}, payloads.deleted)
	assert.Equal(t, int32(1), rejects.Load())
}

func TestCallbackService_RecoveredFailureFencesOlderPositiveInventory(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174091")
	require.NoError(t, err)
	beginTestNewPlacementAttempt(t, store, "lease-race", "backend-a", operationID)

	// Register the inventory snapshot before callback recovery claims the
	// attempt. Projection happens while chain settlement is blocked.
	fence := store.BeginInventorySession()
	getLeaseEntered := make(chan struct{})
	allowGetLease := make(chan struct{})
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: operation.NewRegistry(),
		Placement:  store,
		Chain: &callbackChainStub{
			getLease: func(context.Context, string) (*billingtypes.Lease, error) {
				close(getLeaseEntered)
				<-allowGetLease
				return &billingtypes.Lease{
					Uuid: "lease-race", State: billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
			reject: func(context.Context, []string, string) (uint64, []string, error) {
				return 1, []string{"tx-reject"}, nil
			},
		},
		LifecycleAuthority: store,
	})
	require.NoError(t, err)

	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-race",
		Status:      backend.CallbackStatusFailed,
		OperationID: operationID.String(),
	})
	callbackResult := make(chan error, 1)
	go func() {
		callbackResult <- service.HandleCallback(context.Background(), command)
	}()
	<-getLeaseEntered

	projection, err := store.ProjectInventory(fence, placement.InventoryProjection{
		Placements: map[string]string{"lease-race": "backend-a"},
	})
	store.EndInventorySession(fence)
	require.NoError(t, err)
	assert.Contains(t, projection.Fenced, "lease-race",
		"positive inventory cannot overtake a claimed negative callback")
	assert.Equal(t, placement.StateAttempting, store.Lookup("lease-race").State())

	close(allowGetLease)
	require.NoError(t, <-callbackResult)
	assert.Equal(t, placement.StateAbsent, store.Lookup("lease-race").State())
}

func TestCallbackService_RecoveredAttemptReleasesClaimForChainRetry(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174092")
	require.NoError(t, err)
	beginTestNewPlacementAttempt(t, store, "lease-retry", "backend-a", operationID)

	var calls atomic.Int32
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: operation.NewRegistry(),
		Placement:  store,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			if calls.Add(1) == 1 {
				return false, "", errors.New("temporary chain outage")
			}
			return true, "tx-ack", nil
		}),
		LifecycleAuthority: store,
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "lease-retry",
		Status:      backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})

	err = service.HandleCallback(context.Background(), command)
	require.ErrorIs(t, err, ErrAcknowledgeFailed)
	assert.Equal(t, placement.StateAttempting, store.Lookup("lease-retry").State(),
		"retryable chain failure must preserve the durable recovery authority")
	require.NoError(t, service.HandleCallback(context.Background(), command))
	assert.Equal(t, placement.StateConfirmed, store.Lookup("lease-retry").State())
	assert.Equal(t, int32(2), calls.Load())
}

func TestCallbackService_RecoveredCallbackFencesPlanAndDeprovision(t *testing.T) {
	const leaseUUID = "lease-recovery-claim"
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174093")
	require.NoError(t, err)
	beginTestNewPlacementAttempt(t, store, leaseUUID, "backend-a", operationID)

	tracker := newTestOperationRegistry()
	registry := tracker.Operations()
	backendClient := &mockManagerBackend{name: "backend-a"}
	router := &mockBackendRouter{
		routeFn: func(string) backend.Backend { return backendClient },
		getBackendByNameFn: func(name string) backend.Backend {
			if name == backendClient.name {
				return backendClient
			}
			return nil
		},
		backendsFn: func() []backend.Backend { return []backend.Backend{backendClient} },
	}
	orchestrator := newTestProvisionOrchestrator(
		t, "provider-a", "http://callback.example", router, tracker, store,
	)

	acknowledgeEntered := make(chan struct{})
	releaseAcknowledge := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseAcknowledge) }) })
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: registry,
		Placement:  store,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			close(acknowledgeEntered)
			<-releaseAcknowledge
			return true, "tx-ack", nil
		}),
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID: leaseUUID, Status: backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})

	callbackResult := make(chan error, 1)
	go func() { callbackResult <- service.HandleCallback(context.Background(), command) }()
	<-acknowledgeEntered

	assert.Equal(t, operation.LeaseClaimBusy, registry.TryClaimLeaseNow(leaseUUID).Outcome(),
		"a competing plan action cannot cross callback recovery")
	deprovisionErr := orchestrator.Deprovision(context.Background(), leaseUUID)
	require.ErrorIs(t, deprovisionErr, ErrDeprovisionFailed)
	backendClient.mu.Lock()
	assert.Empty(t, backendClient.deprovisionCalls,
		"deprovision must not contact a backend while callback recovery owns the lease")
	backendClient.mu.Unlock()

	releaseOnce.Do(func() { close(releaseAcknowledge) })
	require.NoError(t, <-callbackResult)
	assert.Equal(t, placement.StateConfirmed, store.Lookup(leaseUUID).State())
	available := registry.TryClaimLeaseNow(leaseUUID)
	require.True(t, available.Acquired(), "callback must release its exact Registry capability")
	require.True(t, registry.ReleaseLease(available.Claim()))
}

func TestCallbackService_InventoryConfirmedGenerationSettlesChainWithoutDemotion(t *testing.T) {
	tests := []struct {
		name        string
		status      backend.CallbackStatus
		chainState  billingtypes.LeaseState
		wantAck     int32
		wantReads   int32
		wantRejects int32
		wantEvent   backend.ProvisionStatus
	}{
		{
			name: "later success acknowledges", status: backend.CallbackStatusSuccess,
			chainState: billingtypes.LEASE_STATE_PENDING, wantAck: 1,
			wantEvent: backend.ProvisionStatusReady,
		},
		{
			name: "active stale failure preserves live owner", status: backend.CallbackStatusFailed,
			chainState: billingtypes.LEASE_STATE_ACTIVE, wantReads: 1,
		},
		{
			name: "pending failure still rejects and settles", status: backend.CallbackStatusFailed,
			chainState: billingtypes.LEASE_STATE_PENDING, wantReads: 1,
			wantRejects: 1, wantEvent: backend.ProvisionStatusFailed,
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const leaseUUID = "lease-inventory-before-callback"
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			storeBeforeRestart, err := placementstore.NewStore(dbPath)
			require.NoError(t, err)
			armTestPlacementTopology(t, storeBeforeRestart, []string{"backend-a"})
			operationID, err := operation.ParseID(fmt.Sprintf(
				"123e4567-e89b-42d3-a456-4266141741%02d", index,
			))
			require.NoError(t, err)
			generation, err := lifecycle.FromOperationID(operationID)
			require.NoError(t, err)
			beginTestNewPlacementAttempt(
				t, storeBeforeRestart, leaseUUID, "backend-a", operationID,
			)
			projectTestPlacementInventory(t, storeBeforeRestart, []string{"backend-a"}, placement.InventoryProjection{
				Placements: map[string]string{leaseUUID: "backend-a"},
				Lifecycles: map[string]placement.LifecycleObservation{
					leaseUUID: {
						Kind: placement.LifecycleObservationTyped,
						ID:   generation,
					},
				},
			})
			require.NoError(t, storeBeforeRestart.Close())

			store, err := placementstore.NewStore(dbPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			before := store.Lookup(leaseUUID)
			require.Equal(t, placement.StateConfirmed, before.State())
			require.Empty(t, before.Attempt)

			registry := operation.NewRegistry()
			var acknowledgeCalls atomic.Int32
			var chainReads atomic.Int32
			var rejectCalls atomic.Int32
			events := &callbackEventRecorder{}
			service, err := newCallbackServiceForTest(CallbackServiceConfig{
				Operations: registry,
				Placement:  store,
				Acknowledger: callbackAcknowledgerFunc(func(
					context.Context, string,
				) (bool, string, error) {
					acknowledgeCalls.Add(1)
					return true, "tx-ack", nil
				}),
				Chain: &callbackChainStub{
					getLease: func(context.Context, string) (*billingtypes.Lease, error) {
						chainReads.Add(1)
						return &billingtypes.Lease{
							Uuid: leaseUUID, State: test.chainState,
						}, nil
					},
					reject: func(context.Context, []string, string) (uint64, []string, error) {
						rejectCalls.Add(1)
						return 1, []string{"tx-reject"}, nil
					},
				},
				Events: events,
			})
			require.NoError(t, err)

			require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
				backend.CallbackPayload{
					LeaseUUID: leaseUUID, Status: test.status,
					OperationID: operationID.String(), Error: "stale failure",
				},
			)))
			assert.Equal(t, test.wantAck, acknowledgeCalls.Load())
			assert.Equal(t, test.wantReads, chainReads.Load())
			assert.Equal(t, test.wantRejects, rejectCalls.Load())
			if test.wantEvent != "" {
				require.Len(t, events.events, 1)
				assert.Equal(t, test.wantEvent, events.events[0].Status)
			} else {
				assert.Empty(t, events.events)
			}
			assert.Equal(t, before, store.Lookup(leaseUUID),
				"inventory-confirmed ownership must remain exact and unchanged")
			available := registry.TryClaimLeaseNow(leaseUUID)
			require.True(t, available.Acquired())
			require.True(t, registry.ReleaseLease(available.Claim()))
		})
	}
}

func TestCallbackService_PendingFailureSettlesAttemptWithOlderObservedOwner(t *testing.T) {
	const leaseUUID = "lease-older-generation-before-failure"
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	storeBeforeRestart, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	armTestPlacementTopology(t, storeBeforeRestart, []string{"backend-a"})
	newOperationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174110")
	require.NoError(t, err)
	olderOperationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174109")
	require.NoError(t, err)
	olderGeneration, err := lifecycle.FromOperationID(olderOperationID)
	require.NoError(t, err)
	beginTestNewPlacementAttempt(
		t, storeBeforeRestart, leaseUUID, "backend-a", newOperationID,
	)
	projectTestPlacementInventory(t, storeBeforeRestart, []string{"backend-a"}, placement.InventoryProjection{
		Placements: map[string]string{leaseUUID: "backend-a"},
		Lifecycles: map[string]placement.LifecycleObservation{
			leaseUUID: {
				Kind: placement.LifecycleObservationTyped,
				ID:   olderGeneration,
			},
		},
	})
	beforeRestart := storeBeforeRestart.Lookup(leaseUUID)
	require.Equal(t, placement.StateConfirmed, beforeRestart.State())
	require.Equal(t, "backend-a", beforeRestart.Attempt)
	require.Equal(t, newOperationID, beforeRestart.AttemptOperationID())
	require.NoError(t, storeBeforeRestart.Close())

	store, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	current := store.CurrentLifecycle(leaseUUID)
	require.Equal(t, placement.LifecycleVerdictTeardownOnly, current.Verdict(),
		"an unresolved newer attempt must fence reissuing the older generation for maintenance")
	require.Equal(t, "backend-a", current.Backend())

	var chainReads atomic.Int32
	var rejectCalls atomic.Int32
	events := &callbackEventRecorder{}
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: operation.NewRegistry(),
		Placement:  store,
		Chain: &callbackChainStub{
			getLease: func(context.Context, string) (*billingtypes.Lease, error) {
				chainReads.Add(1)
				return &billingtypes.Lease{
					Uuid: leaseUUID, State: billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
			reject: func(context.Context, []string, string) (uint64, []string, error) {
				rejectCalls.Add(1)
				return 1, []string{"tx-reject"}, nil
			},
		},
		Events: events,
	})
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID: leaseUUID, Status: backend.CallbackStatusFailed,
			OperationID: newOperationID.String(), Error: "new attempt failed",
		},
	)))

	assert.Equal(t, int32(1), chainReads.Load())
	assert.Equal(t, int32(1), rejectCalls.Load())
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusFailed, events.events[0].Status)
	settled := store.Lookup(leaseUUID)
	assert.Equal(t, placement.StateConfirmed, settled.State())
	assert.Equal(t, "backend-a", settled.Backend)
	assert.Empty(t, settled.Attempt)
	assert.False(t, settled.AttemptOperationID().Valid())
	current = store.CurrentLifecycle(leaseUUID)
	assert.True(t, current.Authorized())
	assert.Equal(t, olderGeneration, current.ID(),
		"failure of the newer attempt must not rotate or demote the older observed owner")
}

func TestCallbackService_RegistryBackedRestoreFencesRecoveredCallback(t *testing.T) {
	const (
		sourceLease = "restore-source"
		targetLease = "restore-target"
	)
	store := newTestPlacementAuthority(t)
	seedTestConfirmedPlacements(t, store, []string{"backend-a"}, map[string]string{
		sourceLease: "backend-a",
	})
	registry := operation.NewRegistry()
	targetClaim := registry.TryClaimLeaseNow(targetLease)
	require.True(t, targetClaim.Acquired())
	initiated := registry.TryInitiateClaimed(targetClaim.Claim(), operation.TrackSpec{
		LeaseUUID: targetLease, Tenant: "tenant-a", Kind: operation.KindRestore,
	})
	require.True(t, initiated.Started())
	initiation := initiated.Capability()
	restoreClaim, err := store.BeginAuthorizedRestore(
		store.CurrentAdmissionBaseline(),
		store.Lookup(sourceLease).RecordRevision(),
		targetLease,
		initiation.ID(),
		testBackendRequestSnapshot(t),
		testPlacementCallbackPair(t, initiation.ID()),
	)
	require.NoError(t, err)
	require.True(t, registry.BindBackend(initiation, "backend-a"))
	require.True(t, registry.BeginCall(initiation))
	require.Equal(t, operation.InitiationAborted, registry.AbortInitiation(initiation))
	assert.False(t, registry.Contains(targetLease),
		"the recovery window begins after synchronous restore removes its operation")

	var acknowledgeCalls atomic.Int32
	service, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: registry,
		Placement:  store,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			acknowledgeCalls.Add(1)
			return true, "tx-ack", nil
		}),
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID: targetLease, Status: backend.CallbackStatusSuccess,
		OperationID: initiation.ID().String(),
	})

	err = service.HandleCallback(context.Background(), command)
	require.ErrorIs(t, err, errCallbackRecoveryLeaseBusy)
	assert.Zero(t, acknowledgeCalls.Load())
	assert.Equal(t, placement.StateAttempting, store.Lookup(targetLease).State())

	confirmed, err := store.ConfirmRestore(restoreClaim)
	require.NoError(t, err)
	require.True(t, confirmed)
	require.True(t, registry.ReleaseLease(targetClaim.Claim()))

	require.NoError(t, service.HandleCallback(context.Background(), command))
	assert.Equal(t, int32(1), acknowledgeCalls.Load(),
		"retry uses the exact restore lifecycle generation after Registry exclusion ends")
	assert.Equal(t, placement.StateConfirmed, store.Lookup(targetLease).State())
	assert.Equal(t, "backend-a", store.Lookup(targetLease).Backend)
}
