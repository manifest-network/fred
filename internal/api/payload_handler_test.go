package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/testutil"
)

// mockPayloadPublisher implements PayloadPublisher for testing.
type mockPayloadPublisher struct {
	publishPayloadFunc func(event provisioner.PayloadEvent) error
	storePayloadFunc   func(leaseUUID string, payload []byte) bool
	deletePayloadFunc  func(leaseUUID string)

	publishedEvents []provisioner.PayloadEvent
	storedPayloads  map[string][]byte
	deletedUUIDs    []string
}

func newMockPayloadPublisher() *mockPayloadPublisher {
	return &mockPayloadPublisher{
		storedPayloads: make(map[string][]byte),
	}
}

func (m *mockPayloadPublisher) PublishPayload(event provisioner.PayloadEvent) error {
	if m.publishPayloadFunc != nil {
		return m.publishPayloadFunc(event)
	}
	m.publishedEvents = append(m.publishedEvents, event)
	return nil
}

func (m *mockPayloadPublisher) StorePayload(leaseUUID string, payload []byte) bool {
	if m.storePayloadFunc != nil {
		return m.storePayloadFunc(leaseUUID, payload)
	}
	if _, exists := m.storedPayloads[leaseUUID]; exists {
		return false
	}
	m.storedPayloads[leaseUUID] = payload
	return true
}

func (m *mockPayloadPublisher) DeletePayload(leaseUUID string) {
	if m.deletePayloadFunc != nil {
		m.deletePayloadFunc(leaseUUID)
		return
	}
	m.deletedUUIDs = append(m.deletedUUIDs, leaseUUID)
	delete(m.storedPayloads, leaseUUID)
}

// mockPayloadChainClient implements ChainClient for payload handler testing.
type mockPayloadChainClient struct {
	getLeaseFunc func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
}

func (m *mockPayloadChainClient) GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	if m.getLeaseFunc != nil {
		return m.getLeaseFunc(ctx, leaseUUID)
	}
	return nil, nil
}

func (m *mockPayloadChainClient) GetActiveLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	return nil, nil
}

func (m *mockPayloadChainClient) Ping(ctx context.Context) error {
	return nil
}

// Helper to create a request with mux vars set
func createPayloadRequest(t *testing.T, method, path, leaseUUID string, body []byte, authHeader string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"lease_uuid": leaseUUID})
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	return req
}

func TestPayloadHandler_InvalidUUID(t *testing.T) {
	handler := NewPayloadHandler(
		&mockPayloadChainClient{},
		newMockPayloadPublisher(),
		testutil.ValidUUID1,
		"manifest",
	)

	req := createPayloadRequest(t, "POST", "/v1/leases/invalid-uuid/data", "invalid-uuid", []byte("test"), "")
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestPayloadHandler_MissingAuth(t *testing.T) {
	handler := NewPayloadHandler(
		&mockPayloadChainClient{},
		newMockPayloadPublisher(),
		testutil.ValidUUID1,
		"manifest",
	)

	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, []byte("test"), "")
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestPayloadHandler_InvalidAuthFormat(t *testing.T) {
	handler := NewPayloadHandler(
		&mockPayloadChainClient{},
		newMockPayloadPublisher(),
		testutil.ValidUUID1,
		"manifest",
	)

	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, []byte("test"), "Basic abc123")
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestPayloadHandler_LeaseNotFound(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-1")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil // Lease not found
		},
	}

	handler := NewPayloadHandler(client, newMockPayloadPublisher(), testutil.ValidUUID1, "manifest")

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestPayloadHandler_LeaseNotPending(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-2")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE, // Not pending
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     metaHashBytes,
			}, nil
		},
	}

	handler := NewPayloadHandler(client, newMockPayloadPublisher(), testutil.ValidUUID1, "manifest")

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestPayloadHandler_TenantMismatch(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-3")
	otherKp := testutil.NewTestKeyPair("payload-handler-test-other")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       otherKp.Address, // Different tenant
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     metaHashBytes,
			}, nil
		},
	}

	handler := NewPayloadHandler(client, newMockPayloadPublisher(), testutil.ValidUUID1, "manifest")

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusForbidden, rr.Code)
}

func TestPayloadHandler_ProviderMismatch(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-4")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID2, // Different provider
				MetaHash:     metaHashBytes,
			}, nil
		},
	}

	handler := NewPayloadHandler(client, newMockPayloadPublisher(), testutil.ValidUUID1, "manifest")

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusForbidden, rr.Code)
}

func TestPayloadHandler_LeaseNoMetaHash(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-5")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     nil, // No meta hash
			}, nil
		},
	}

	handler := NewPayloadHandler(client, newMockPayloadPublisher(), testutil.ValidUUID1, "manifest")

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestPayloadHandler_MetaHashMismatch(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-6")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)
	// Different hash stored on chain
	differentHash := sha256.Sum256([]byte("different payload"))

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     differentHash[:], // Different from token's meta_hash
			}, nil
		},
	}

	handler := NewPayloadHandler(client, newMockPayloadPublisher(), testutil.ValidUUID1, "manifest")

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestPayloadHandler_PayloadHashMismatch(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-7")
	payload := []byte("test payload")
	differentPayload := []byte("different payload")
	metaHash := testutil.ComputePayloadHash(payload)
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     metaHashBytes,
			}, nil
		},
	}

	handler := NewPayloadHandler(client, newMockPayloadPublisher(), testutil.ValidUUID1, "manifest")

	// Token has correct meta_hash, but we send different payload
	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, differentPayload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestPayloadHandler_EmptyPayload(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-8")
	metaHash := testutil.ComputePayloadHash([]byte("something"))
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     metaHashBytes,
			}, nil
		},
	}

	handler := NewPayloadHandler(client, newMockPayloadPublisher(), testutil.ValidUUID1, "manifest")

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, []byte{}, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestPayloadHandler_PayloadConflict(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-9")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     metaHashBytes,
			}, nil
		},
	}

	publisher := newMockPayloadPublisher()
	// Pre-store a payload to simulate conflict
	publisher.storedPayloads[testutil.ValidUUID1] = []byte("existing payload")

	handler := NewPayloadHandler(client, publisher, testutil.ValidUUID1, "manifest")

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusConflict, rr.Code)
}

func TestPayloadHandler_Success(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-10")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     metaHashBytes,
			}, nil
		},
	}

	publisher := newMockPayloadPublisher()
	handler := NewPayloadHandler(client, publisher, testutil.ValidUUID1, "manifest")

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusAccepted, rr.Code)

	// Verify payload was stored
	_, exists := publisher.storedPayloads[testutil.ValidUUID1]
	assert.True(t, exists, "payload was not stored")

	// Verify event was published
	require.Len(t, publisher.publishedEvents, 1)

	event := publisher.publishedEvents[0]
	assert.Equal(t, testutil.ValidUUID1, event.LeaseUUID)
	assert.Equal(t, kp.Address, event.Tenant)
	assert.Equal(t, metaHash, event.MetaHashHex)
}

func TestPayloadHandler_PublishFailureRollback(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-11")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     metaHashBytes,
			}, nil
		},
	}

	publisher := newMockPayloadPublisher()
	publisher.publishPayloadFunc = func(event provisioner.PayloadEvent) error {
		return http.ErrAbortHandler // Simulate publish failure
	}

	handler := NewPayloadHandler(client, publisher, testutil.ValidUUID1, "manifest")

	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID1, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	// Verify payload was deleted (rollback)
	require.Len(t, publisher.deletedUUIDs, 1)
	assert.Equal(t, testutil.ValidUUID1, publisher.deletedUUIDs[0], "payload was not rolled back after publish failure")
}

func TestPayloadHandler_LeaseUUIDMismatch(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-12")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     metaHashBytes,
			}, nil
		},
	}

	handler := NewPayloadHandler(client, newMockPayloadPublisher(), testutil.ValidUUID1, "manifest")

	// Token signed for a different lease UUID
	tokenStr := testutil.CreateTestPayloadToken(kp, testutil.ValidUUID2, metaHash, time.Now())
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestPayloadHandler_ExpiredToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-handler-test-13")
	payload := []byte("test payload")
	metaHash := testutil.ComputePayloadHash(payload)
	metaHashBytes, _ := hex.DecodeString(metaHash)

	client := &mockPayloadChainClient{
		getLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       kp.Address,
				ProviderUuid: testutil.ValidUUID1,
				MetaHash:     metaHashBytes,
			}, nil
		},
	}

	handler := NewPayloadHandler(client, newMockPayloadPublisher(), testutil.ValidUUID1, "manifest")

	// Create expired token
	tokenStr := testutil.CreateExpiredPayloadToken(kp, testutil.ValidUUID1, metaHash)
	req := createPayloadRequest(t, "POST", "/v1/leases/"+testutil.ValidUUID1+"/data", testutil.ValidUUID1, payload, "Bearer "+tokenStr)
	rr := httptest.NewRecorder()

	handler.HandlePayloadUpload(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}
