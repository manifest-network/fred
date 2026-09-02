package provisioner

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

func TestManager_RecoversAmbiguousProvisionCallbackAcrossRegistryAndStoreRestart(t *testing.T) {
	tests := []struct {
		name       string
		status     backend.CallbackStatus
		wantState  placement.State
		wantChain  billingtypes.LeaseState
		wantAck    int32
		wantReject int32
	}{
		{
			name:      "positive callback",
			status:    backend.CallbackStatusSuccess,
			wantState: placement.StateConfirmed,
			wantChain: billingtypes.LEASE_STATE_ACTIVE,
			wantAck:   1,
		},
		{
			name:       "negative callback",
			status:     backend.CallbackStatusFailed,
			wantState:  placement.StateAbsent,
			wantChain:  billingtypes.LEASE_STATE_REJECTED,
			wantReject: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				leaseUUID    = "lease-ambiguous-restart"
				providerUUID = placementstore.ProviderUUID
				backendName  = "backend-a"
			)
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			backendClient := &mockManagerBackend{
				name:         backendName,
				provisionErr: errors.New("connection reset after remote dispatch"),
			}
			router, err := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
			})
			require.NoError(t, err)

			var chainState atomic.Int32
			chainState.Store(int32(billingtypes.LEASE_STATE_PENDING))
			var acknowledgeCalls atomic.Int32
			var rejectCalls atomic.Int32
			chainClient := &chaintest.MockClient{
				GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
					return &billingtypes.Lease{
						Uuid:         leaseUUID,
						ProviderUuid: providerUUID,
						Tenant:       "tenant-a",
						State:        billingtypes.LeaseState(chainState.Load()),
						Items: []billingtypes.LeaseItem{{
							SkuUuid: "sku-a", Quantity: 1,
						}},
					}, nil
				},
				GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
					if billingtypes.LeaseState(chainState.Load()) != billingtypes.LEASE_STATE_PENDING {
						return nil, nil
					}
					return []billingtypes.Lease{{
						Uuid: leaseUUID, ProviderUuid: providerUUID,
						State: billingtypes.LEASE_STATE_PENDING,
					}}, nil
				},
				AcknowledgeLeasesFunc: func(context.Context, []string) (uint64, []string, error) {
					acknowledgeCalls.Add(1)
					chainState.Store(int32(billingtypes.LEASE_STATE_ACTIVE))
					return 1, []string{"tx-ack"}, nil
				},
				RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
					rejectCalls.Add(1)
					chainState.Store(int32(billingtypes.LEASE_STATE_REJECTED))
					return 1, []string{"tx-reject"}, nil
				},
			}

			store1, err := placementstore.NewStore(dbPath)
			require.NoError(t, err)
			configureTestPlacementTopology(t, store1, []string{backendName})
			manager1, err := NewManager(ManagerConfig{
				ProviderUUID:    providerUUID,
				CallbackBaseURL: "http://callback.example",
				PlacementStore:  store1,
			}, router, chainClient)
			require.NoError(t, err)
			armTestPlacementAdmission(t, store1, router)
			firstStartCtx, cancelFirstStart := context.WithCancel(context.Background())
			firstStartErr := make(chan error, 1)
			go func() { firstStartErr <- manager1.Start(firstStartCtx) }()
			select {
			case <-manager1.Running():
			case <-time.After(5 * time.Second):
				t.Fatal("first manager did not start")
			}

			lease := &billingtypes.Lease{
				Uuid: leaseUUID, ProviderUuid: providerUUID, Tenant: "tenant-a",
				State: billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-a", Quantity: 1}},
			}
			err = startTestProvisioning(
				t, manager1.orchestrator, context.Background(), lease, ProvisionOpts{},
			)
			require.ErrorIs(t, err, ErrProvisioningFailed)
			beforeRestart := store1.Lookup(leaseUUID)
			require.Equal(t, placement.StateAttempting, beforeRestart.State())
			require.True(t, beforeRestart.AttemptOperationID().Valid())
			operationID := beforeRestart.AttemptOperationID()
			assert.Zero(t, manager1.operations.Count(),
				"ambiguous return removes only the ephemeral operation record")
			cancelFirstStart()
			require.NoError(t, manager1.Close())
			select {
			case err := <-firstStartErr:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("first manager did not stop")
			}
			require.NoError(t, store1.Close())

			store2, err := placementstore.NewStore(dbPath)
			require.NoError(t, err)
			manager2, err := NewManager(ManagerConfig{
				ProviderUUID:     providerUUID,
				CallbackBaseURL:  "http://callback.example",
				PlacementStore:   store2,
				AckBatchInterval: time.Millisecond,
				AckBatchSize:     1,
			}, router, chainClient)
			require.NoError(t, err)
			require.NotSame(t, manager1.operations, manager2.operations)
			assert.Zero(t, manager2.operations.Count(),
				"the replacement manager starts with a fresh registry")
			afterRestart := store2.Lookup(leaseUUID)
			require.Equal(t, operationID, afterRestart.AttemptOperationID(),
				"the exact callback authority must survive the bbolt reopen")

			startCtx, cancelStart := context.WithCancel(context.Background())
			startErr := make(chan error, 1)
			go func() { startErr <- manager2.Start(startCtx) }()
			select {
			case <-manager2.Running():
			case <-time.After(5 * time.Second):
				t.Fatal("replacement manager did not start")
			}

			require.NoError(t, manager2.PublishCallback(context.Background(), backend.CallbackPayload{
				LeaseUUID:        leaseUUID,
				Status:           tt.status,
				Error:            "backend refused",
				Backend:          "body-controlled-backend",
				BackendStorageID: testBackendStorageID(backendName).String(),
				OperationID:      operationID.String(),
			}))
			settledPlacement := store2.Lookup(leaseUUID)
			assert.Equal(t, tt.wantState, settledPlacement.State())
			assert.Empty(t, settledPlacement.Attempt,
				"either terminal callback must release the durable attempt gate")
			assert.False(t, settledPlacement.AttemptOperationID().Valid())
			assert.Equal(t, tt.wantChain, billingtypes.LeaseState(chainState.Load()))
			assert.Equal(t, tt.wantAck, acknowledgeCalls.Load())
			assert.Equal(t, tt.wantReject, rejectCalls.Load())
			if tt.wantState == placement.StateConfirmed {
				assert.Equal(t, backendName, settledPlacement.Backend)
			}

			cancelStart()
			require.NoError(t, manager2.Close())
			select {
			case err := <-startErr:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("replacement manager did not stop")
			}
			require.NoError(t, store2.Close())
		})
	}
}
