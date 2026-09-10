package provisioner

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

func rejectionTestPayloadStore(t *testing.T) (*payload.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "payloads.db")
	store, err := payload.NewStore(payload.StoreConfig{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, path
}

func rejectionTestLease(data []byte) *billingtypes.Lease {
	hash := sha256.Sum256(data)
	return &billingtypes.Lease{Uuid: handlerTestLeaseValidation, Tenant: "tenant-a",
		State: billingtypes.LEASE_STATE_PENDING, MetaHash: hash[:],
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}}}
}

func TestProvisionRejectionFailurePreservesExactPayloadForNextEvent(t *testing.T) {
	for _, corrupted := range []bool{false, true} {
		name := "backend validation"
		if corrupted {
			name = "payload hash mismatch"
		}
		t.Run(name, func(t *testing.T) {
			data := []byte(`{"image":"evil.io/malware","preserve":"exact bytes"}`)
			lease := rejectionTestLease(data)
			if corrupted {
				data = append(data, '\n')
			}
			ps, _ := rejectionTestPayloadStore(t)
			require.True(t, ps.Store(lease.Uuid, data))
			_, client := provisionResponseBackendForTest(t, "test-backend", http.StatusBadRequest,
				`{"error":"image not allowed","validation_code":"image_not_allowed"}`)
			var rejected int
			chain := &chaintest.MockClient{
				GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) { return lease, nil },
				RejectLeasesFunc: func(_ context.Context, leases []string, reason string) (uint64, []string, error) {
					require.Equal(t, []string{lease.Uuid}, leases)
					if corrupted {
						assert.Equal(t, "payload corrupted", reason)
					} else {
						assert.Equal(t, "image not allowed", reason)
					}
					rejected++
					if rejected == 1 {
						return 0, nil, errors.New("chain unavailable")
					}
					lease.State = billingtypes.LEASE_STATE_REJECTED
					return 1, []string{"rejection transaction"}, nil
				},
			}
			hs, _ := newTestHandlerSetWithBackend(t, chain, client, nil, ps)
			event := payload.Event{LeaseUUID: lease.Uuid, Tenant: lease.Tenant}
			require.ErrorContains(t, hs.HandlePayloadReceived(newPayloadEventMsg(t, event)), "chain unavailable")
			preserved, err := ps.Get(lease.Uuid)
			require.NoError(t, err)
			require.Equal(t, data, preserved, "failed chain rejection cannot consume recovery bytes")
			require.NoError(t, hs.HandlePayloadReceived(newPayloadEventMsg(t, event)))
			assert.Equal(t, 2, rejected)
			exists, err := ps.Has(lease.Uuid)
			require.NoError(t, err)
			assert.False(t, exists)
		})
	}
}

func TestProvisionRejectionRequiresPositiveCommitEvidence(t *testing.T) {
	for _, state := range []billingtypes.LeaseState{
		billingtypes.LEASE_STATE_PENDING, billingtypes.LEASE_STATE_ACTIVE,
		billingtypes.LEASE_STATE_UNSPECIFIED, billingtypes.LEASE_STATE_REJECTED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			data := []byte("exact original payload")
			lease := rejectionTestLease(data)
			ps, _ := rejectionTestPayloadStore(t)
			require.True(t, ps.Store(lease.Uuid, data))
			_, client := provisionResponseBackendForTest(t, "test-backend", http.StatusBadRequest,
				`{"error":"invalid manifest","validation_code":"invalid_manifest"}`)
			chain := &chaintest.MockClient{
				GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) { return lease, nil },
				RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
					lease.State = state
					return 0, nil, errors.New("response lost")
				},
			}
			hs, _ := newTestHandlerSetWithBackend(t, chain, client, nil, ps)
			err := hs.HandlePayloadReceived(newPayloadEventMsg(t, payload.Event{LeaseUUID: lease.Uuid, Tenant: lease.Tenant}))
			if state == billingtypes.LEASE_STATE_REJECTED {
				require.NoError(t, err, "exact observed terminal state resolves the lost acknowledgment")
				exists, err := ps.Has(lease.Uuid)
				require.NoError(t, err)
				assert.False(t, exists)
			} else {
				require.Error(t, err)
				preserved, err := ps.Get(lease.Uuid)
				require.NoError(t, err)
				assert.Equal(t, data, preserved)
			}
		})
	}
}

func TestProvisionRejectionZeroAcknowledgmentPreservesPayload(t *testing.T) {
	data := []byte("original bytes")
	lease := rejectionTestLease(data)
	ps, _ := rejectionTestPayloadStore(t)
	require.True(t, ps.Store(lease.Uuid, data))
	_, client := provisionResponseBackendForTest(t, "test-backend", http.StatusBadRequest, `{"error":"invalid manifest"}`)
	chain := &chaintest.MockClient{
		GetLeaseFunc:     func(context.Context, string) (*billingtypes.Lease, error) { return lease, nil },
		RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) { return 0, nil, nil },
	}
	hs, _ := newTestHandlerSetWithBackend(t, chain, client, nil, ps)
	require.ErrorContains(t, hs.HandlePayloadReceived(newPayloadEventMsg(t,
		payload.Event{LeaseUUID: lease.Uuid, Tenant: lease.Tenant})), "acknowledged 0 leases")
	preserved, err := ps.Get(lease.Uuid)
	require.NoError(t, err)
	assert.Equal(t, data, preserved)
}

func TestProvisionRejectionOwnsLeaseThroughChainMutationAndCleanup(t *testing.T) {
	data := []byte("exact payload")
	lease := rejectionTestLease(data)
	ps, _ := rejectionTestPayloadStore(t)
	require.True(t, ps.Store(lease.Uuid, data))
	_, client := provisionResponseBackendForTest(t, "test-backend", http.StatusBadRequest, `{"error":"invalid manifest"}`)
	entered, release := make(chan struct{}), make(chan struct{})
	var rejections atomic.Int32
	chain := &chaintest.MockClient{
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) { return lease, nil },
		RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
			rejections.Add(1)
			close(entered)
			<-release
			return 1, nil, nil
		},
	}
	hs, _ := newTestHandlerSetWithBackend(t, chain, client, nil, ps)
	event := payload.Event{LeaseUUID: lease.Uuid, Tenant: lease.Tenant}
	done := make(chan error, 1)
	go func() { done <- hs.HandlePayloadReceived(newPayloadEventMsg(t, event)) }()
	<-entered
	retryErr := hs.HandlePayloadReceived(newPayloadEventMsg(t, event))
	preserved, readErr := ps.Get(lease.Uuid)
	close(release)
	require.NoError(t, <-done)
	require.ErrorContains(t, retryErr, "claim is busy")
	require.NoError(t, readErr)
	assert.Equal(t, data, preserved)
	assert.EqualValues(t, 1, rejections.Load(), "a second lifecycle cannot enter during rejection")
}

func TestRejectedPayloadCleanupFailureRecoversAfterStoreReopen(t *testing.T) {
	data := []byte("exact rejected payload")
	lease := rejectionTestLease(data)
	ps, path := rejectionTestPayloadStore(t)
	require.True(t, ps.Store(lease.Uuid, data))
	_, client := provisionResponseBackendForTest(t, "test-backend", http.StatusBadRequest, `{"error":"invalid manifest"}`)
	chain := &chaintest.MockClient{
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) { return lease, nil },
		RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
			lease.State = billingtypes.LEASE_STATE_REJECTED
			require.NoError(t, ps.Close())
			return 1, nil, nil
		},
	}
	hs, _ := newTestHandlerSetWithBackend(t, chain, client, nil, ps)
	event := payload.Event{LeaseUUID: lease.Uuid, Tenant: lease.Tenant}
	require.ErrorContains(t, hs.HandlePayloadReceived(newPayloadEventMsg(t, event)), "clean rejected lease payload")

	reopened, err := payload.NewStore(payload.StoreConfig{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	preserved, err := reopened.Get(lease.Uuid)
	require.NoError(t, err)
	require.Equal(t, data, preserved)
	recovered, _ := newTestHandlerSetWithBackend(t, chain, client, nil, reopened)
	require.NoError(t, recovered.HandlePayloadReceived(newPayloadEventMsg(t, event)))
	exists, err := reopened.Has(lease.Uuid)
	require.NoError(t, err)
	assert.False(t, exists, "positive terminal state repairs interrupted cleanup after restart")
}
