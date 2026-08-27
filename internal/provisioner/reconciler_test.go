package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// noopAck is a no-op acknowledger for tests that don't exercise the ack path.
// The zero-value mockAcknowledger returns (true, "tx-hash", nil) by default.
var noopAck = &mockAcknowledger{}

type typedTestReconcilerRuntime struct {
	*mockInFlightTracker
	operations ReconcilerOperations
}

func (runtime *typedTestReconcilerRuntime) ReconcilerOperations() ReconcilerOperations {
	return runtime.operations
}

type blockingClearPlacementStore struct {
	PlacementStore
	entered chan struct{}
	release chan struct{}
}

type snapshotPairingPlacementStore struct {
	PlacementStore
	mu     sync.Mutex
	begins []uint64
	ends   []uint64
}

func (s *snapshotPairingPlacementStore) BeginInventorySnapshot() uint64 {
	revision := s.PlacementStore.BeginInventorySnapshot()
	s.mu.Lock()
	s.begins = append(s.begins, revision)
	s.mu.Unlock()
	return revision
}

func (s *snapshotPairingPlacementStore) EndInventorySnapshot(revision uint64) {
	s.PlacementStore.EndInventorySnapshot(revision)
	s.mu.Lock()
	s.ends = append(s.ends, revision)
	s.mu.Unlock()
}

func (s *snapshotPairingPlacementStore) calls() ([]uint64, []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.begins...), append([]uint64(nil), s.ends...)
}

func (s *blockingClearPlacementStore) ClearAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error) {
	close(s.entered)
	<-s.release
	return s.PlacementStore.ClearAttemptIfRevision(leaseUUID, backendName, revision)
}

type concurrentClearPlacementStore struct {
	PlacementStore
	entered chan string
	release chan struct{}
}

func (s *concurrentClearPlacementStore) ClearAttemptIfRevision(
	leaseUUID, backendName string,
	revision uint64,
) (bool, error) {
	s.entered <- leaseUUID
	<-s.release
	return s.PlacementStore.ClearAttemptIfRevision(leaseUUID, backendName, revision)
}

// mutateAfterSnapshotListStore arms after the inventory batch finishes, returns
// the next stable List snapshot, then mutates the target before the worker's
// immediate Lookup re-read. It pins the revision-only half of the post-sync
// placement boundary guard with the real bbolt store; the shared map mock cannot
// construct opaque revisions.
type mutateAfterSnapshotListStore struct {
	PlacementStore
	targetBackend  string
	targetLease    string
	mu             sync.Mutex
	mutateNextList bool
	mutationDone   bool
	mutationErr    error
}

// failNextBatchPlacementStore injects a one-shot failure at the exact durable
// boundary used to project positive fleet observations. Other placement writes
// continue to use the embedded store so tests can distinguish a projection
// failure from an entirely unavailable placement database.
type failNextBatchPlacementStore struct {
	PlacementStore
	mu      sync.Mutex
	failErr error
}

func (s *failNextBatchPlacementStore) SetBatchIfNotNewer(
	placements map[string]string,
	maxRevision uint64,
) (map[string]uint64, map[string]struct{}, error) {
	s.mu.Lock()
	err := s.failErr
	s.failErr = nil
	s.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	return s.PlacementStore.SetBatchIfNotNewer(placements, maxRevision)
}

type retentionErrorReconcilerBackend struct {
	*mockReconcilerBackend
	err error
}

func (b *retentionErrorReconcilerBackend) ListRetentions(context.Context) ([]backend.RetainedLease, error) {
	return nil, b.err
}

func (s *mutateAfterSnapshotListStore) SetBatchIfNotNewer(
	placements map[string]string,
	maxRevision uint64,
) (map[string]uint64, map[string]struct{}, error) {
	applied, fenced, err := s.PlacementStore.SetBatchIfNotNewer(placements, maxRevision)
	if err == nil {
		s.mu.Lock()
		s.mutateNextList = true
		s.mu.Unlock()
	}
	return applied, fenced, err
}

func (s *mutateAfterSnapshotListStore) List() map[string]placement.Placement {
	snapshot := s.PlacementStore.List()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mutateNextList {
		s.mutateNextList = false
		s.mutationDone = true
		_, s.mutationErr = s.SetAttempting(s.targetLease, s.targetBackend)
	}
	return snapshot
}

// mockReconcilerBackend implements backend.Backend for testing.
type mockReconcilerBackend struct {
	mu                        sync.Mutex
	name                      string
	provisions                []backend.ProvisionInfo
	retentions                []backend.RetainedLease // returned by ListRetentions
	provisionCalls            []backend.ProvisionRequest
	deprovisionCalls          []string
	listProvisionsCalls       int
	reconcileCustomDomainArgs []reconcileCustomDomainCall
	reconcileCustomDomainErr  error
	provisionErr              error
	deprovisionErr            error
	listErr                   error
	refreshErr                error
	onListProvisions          func()
}

type reconcileCustomDomainCall struct {
	leaseUUID string
	items     []backend.LeaseItem
}

func (m *mockReconcilerBackend) Name() string {
	return m.name
}

func (m *mockReconcilerBackend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.provisionCalls = append(m.provisionCalls, req)
	if m.provisionErr != nil {
		return m.provisionErr
	}
	return nil
}

func (m *mockReconcilerBackend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	info := backend.LeaseInfo{Host: "localhost"}
	return &info, nil
}

func (m *mockReconcilerBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deprovisionCalls = append(m.deprovisionCalls, leaseUUID)
	if m.deprovisionErr != nil {
		return m.deprovisionErr
	}
	return nil
}

func (m *mockReconcilerBackend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listProvisionsCalls++
	if m.onListProvisions != nil {
		m.onListProvisions()
	}
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.provisions, nil
}

func (m *mockReconcilerBackend) LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
	result := make([]backend.ProvisionInfo, 0, len(uuids))
	wanted := make(map[string]struct{}, len(uuids))
	for _, u := range uuids {
		wanted[u] = struct{}{}
	}
	for _, p := range m.provisions {
		if _, ok := wanted[p.LeaseUUID]; ok {
			result = append(result, p)
		}
	}
	return result, nil
}

func (m *mockReconcilerBackend) Health(ctx context.Context) error {
	return nil
}

func (m *mockReconcilerBackend) RefreshState(ctx context.Context) error {
	return m.refreshErr
}

func (m *mockReconcilerBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockReconcilerBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	return nil, backend.ErrNotProvisioned
}
func (m *mockReconcilerBackend) Restart(ctx context.Context, req backend.RestartRequest) error {
	return nil
}
func (m *mockReconcilerBackend) Update(ctx context.Context, req backend.UpdateRequest) error {
	return nil
}
func (m *mockReconcilerBackend) Restore(ctx context.Context, req backend.RestoreRequest) error {
	return backend.ErrNotRetained
}
func (m *mockReconcilerBackend) ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconcileCustomDomainArgs = append(m.reconcileCustomDomainArgs, reconcileCustomDomainCall{
		leaseUUID: leaseUUID,
		items:     append([]backend.LeaseItem(nil), items...),
	})
	return m.reconcileCustomDomainErr
}
func (m *mockReconcilerBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockReconcilerBackend) GetLoadStats(_ context.Context) (*backend.LoadStats, error) {
	return nil, nil
}

func (m *mockReconcilerBackend) ListRetentions(_ context.Context) ([]backend.RetainedLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.retentions) == 0 {
		return nil, nil
	}
	out := make([]backend.RetainedLease, len(m.retentions))
	copy(out, m.retentions)
	return out, nil
}

func TestNewReconciler_Validation(t *testing.T) {
	mockChain := &chaintest.MockClient{}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tests := []struct {
		name        string
		cfg         ReconcilerConfig
		chainClient ReconcilerChainClient
		ack         Acknowledger
		router      BackendRouter
		wantErr     string
	}{
		{
			name:        "missing chain client",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: nil,
			ack:         noopAck,
			router:      router,
			wantErr:     "chain client is required",
		},
		{
			name:        "missing acknowledger",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			ack:         nil,
			router:      router,
			wantErr:     "acknowledger is required",
		},
		{
			name:        "missing router",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			ack:         noopAck,
			router:      nil,
			wantErr:     "backend router is required",
		},
		{
			name:        "missing provider UUID",
			cfg:         ReconcilerConfig{CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			ack:         noopAck,
			router:      router,
			wantErr:     "provider UUID is required",
		},
		{
			name:        "missing callback URL",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid"},
			chainClient: mockChain,
			ack:         noopAck,
			router:      router,
			wantErr:     "callback base URL is required",
		},
		{
			name:        "valid config",
			cfg:         ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
			chainClient: mockChain,
			ack:         noopAck,
			router:      router,
			wantErr:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newReconciler(tt.cfg, tt.chainClient, tt.ack, tt.router, nil, nil)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}
		})
	}
}

func TestNewReconciler_PlacementRequiresSharedTracker(t *testing.T) {
	backendClient := &mockReconcilerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	_, err = newReconciler(
		ReconcilerConfig{ProviderUUID: "provider-1", CallbackBaseURL: "http://callback"},
		&chaintest.MockClient{}, noopAck, router, nil, &mockPlacementStore{},
	)
	require.EqualError(t, err, "in-flight tracker is required when placement store is enabled")
}

func TestNewReconciler_RequiresTypedAuthorities(t *testing.T) {
	backendClient := &mockReconcilerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	store, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil),
		operations:          operation.NewRegistry(),
	}
	cfg := ReconcilerConfig{ProviderUUID: "provider-1", CallbackBaseURL: "http://callback"}
	chainClient := &chaintest.MockClient{}

	reconciler, err := NewReconciler(cfg, chainClient, noopAck, router, runtime, store)
	require.NoError(t, err)
	require.Same(t, runtime.operations, reconciler.operations)
	require.Same(t, store, reconciler.placementAuthority)
	require.Same(t, store, reconciler.placementView)
	require.Nil(t, reconciler.legacyPlacement,
		"production construction must not retain raw placement mutation")
	require.Nil(t, reconciler.tracker, "production construction must not retain the raw tracker surface")

	_, err = NewReconciler(cfg, chainClient, noopAck, router, nil, store)
	require.EqualError(t, err, "reconciler runtime is required")

	var typedNilRuntime *typedTestReconcilerRuntime
	_, err = NewReconciler(cfg, chainClient, noopAck, router, typedNilRuntime, store)
	require.EqualError(t, err, "reconciler runtime is required")

	_, err = NewReconciler(cfg, chainClient, noopAck, router, runtime, nil)
	require.EqualError(t, err, "placement authority store is required")

	var typedNilStore *placement.Store
	_, err = NewReconciler(cfg, chainClient, noopAck, router, runtime, typedNilStore)
	require.EqualError(t, err, "placement authority store is required")

	runtimeWithoutOperations := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil),
	}
	_, err = NewReconciler(cfg, chainClient, noopAck, router, runtimeWithoutOperations, store)
	require.EqualError(t, err, "reconciler operations are required")
}

func TestReconciler_ChainCollectionFailurePreservesDurableTopologyBaseline(t *testing.T) {
	backendClient := &mockReconcilerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)

	store, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil),
		operations:          operation.NewRegistry(),
	}

	var pendingErr error
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return nil, pendingErr
		},
	}
	reconciler, err := NewReconciler(
		ReconcilerConfig{ProviderUUID: "provider-1", CallbackBaseURL: "http://callback"},
		chainClient, noopAck, router, runtime, store,
	)
	require.NoError(t, err)

	// A complete projection establishes durable topology-bound admission.
	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	baseline := store.CurrentAdmissionBaseline()
	require.True(t, baseline.Valid())
	scope, err := store.ScopeAdmission(baseline, backendTopologyNames(router))
	require.NoError(t, err)
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174000")
	require.NoError(t, err)
	attempt, applied, err := store.BeginNewAttempt(
		scope, "probe-before-error", "backend-a", operationID,
	)
	require.NoError(t, err)
	require.True(t, applied)
	cleared, err := store.RefuseAttempt(attempt)
	require.NoError(t, err)
	require.True(t, cleared)

	// A failed later collection authorizes no actions from that failed sweep, but
	// it must not erase the previously committed topology fact. Event admission
	// and a later partial sweep can still safely use that baseline.
	pendingErr = errors.New("chain list unavailable")
	require.ErrorIs(t, reconciler.ReconcileAll(t.Context()), pendingErr)
	assert.True(t, store.CurrentAdmissionBaseline().Valid())
	attempt, applied, err = store.BeginNewAttempt(
		scope, "probe-after-error", "backend-a", operationID,
	)
	require.NoError(t, err)
	require.True(t, applied)
	_, err = store.RefuseAttempt(attempt)
	require.NoError(t, err)
	require.Zero(t, promtestutil.ToFloat64(metrics.ReconcilerSweepComplete))
}

// TestHandleProvisionError_AlreadyProvisionedAmbiguous verifies that an
// unvalidated HTTP 409 remains transient but makes the incomplete sweep visible
// to operators. Authoritative inventory, not this response, settles ownership.
func TestHandleProvisionError_AlreadyProvisionedAmbiguous(t *testing.T) {
	mockChain := &chaintest.MockClient{}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	r, err := newReconciler(
		ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
		mockChain, noopAck, router, nil, nil,
	)
	require.NoError(t, err)

	lease := billingtypes.Lease{Uuid: "lease-1", Tenant: "tenant-a"}
	hadError := false
	r.handleProvisionError(
		context.Background(),
		fmt.Errorf("wrapped: %w", backend.ErrAlreadyProvisioned),
		"lease-1",
		lease,
		&hadError,
	)
	assert.True(t, hadError, "an unvalidated 409 must remain operationally visible")
}

// TestHandleProvisionError_MalformedErrorBodyIsTransient is the reconciler half
// of ENG-620/ENG-739: a backend 4xx whose body fred could not parse must not
// reject a PENDING lease or close an ACTIVE one on-chain.
//
// Before the fix, parseValidationError wrapped ANY 400 — including one an
// intermediary produced — in backend.ErrValidation, which lands in the
// permanent switch below and terminates the lease.
//
// Two independent things now prevent that, and this test passes on either:
// the sentinel does not wrap ErrValidation (pinned at the client by
// TestHTTPClient_MalformedErrorBody_IsNeverForwarded, which asserts
// NotErrorIs(err, ErrValidation)), and handleProvisionError has an explicit
// branch for it. What this test pins is the OUTCOME — no on-chain
// reject/close — which is the property that must hold however it is achieved.
func TestHandleProvisionError_MalformedErrorBodyIsTransient(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state billingtypes.LeaseState
	}{
		{"active lease is not closed", billingtypes.LEASE_STATE_ACTIVE},
		{"pending lease is not rejected", billingtypes.LEASE_STATE_PENDING},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var closed, rejected []string
			mockChain := &chaintest.MockClient{
				CloseLeasesFunc: func(_ context.Context, uuids []string, _ string) (uint64, []string, error) {
					mu.Lock()
					defer mu.Unlock()
					closed = append(closed, uuids...)
					return uint64(len(uuids)), []string{"tx-hash"}, nil
				},
				RejectLeasesFunc: func(_ context.Context, uuids []string, _ string) (uint64, []string, error) {
					mu.Lock()
					defer mu.Unlock()
					rejected = append(rejected, uuids...)
					return uint64(len(uuids)), []string{"tx-hash"}, nil
				},
			}
			mockBackend := &mockReconcilerBackend{name: "test"}
			router, _ := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
			})
			r, err := newReconciler(
				ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
				mockChain, noopAck, router, nil, nil,
			)
			require.NoError(t, err)

			lease := billingtypes.Lease{Uuid: "lease-1", Tenant: "tenant-a", State: tc.state}
			hadError := false
			r.handleProvisionError(
				context.Background(),
				fmt.Errorf("provision failed: %w", backend.ErrMalformedErrorBody),
				"lease-1", lease, &hadError,
			)

			mu.Lock()
			defer mu.Unlock()
			assert.Empty(t, closed, "an unparseable backend body must NOT close an active lease")
			assert.Empty(t, rejected, "an unparseable backend body must NOT reject a pending lease")
			assert.True(t, hadError, "must flag the cycle for retry")
		})
	}
}

// TestHandleProvisionError_CircuitOpenIsTransient verifies that a backend
// circuit-open error is treated as transient: the reconciler must NOT close an
// active lease on-chain (the breaker auto-recovers), and must flag the cycle
// for retry. A transient backend outage that trips the breaker previously
// permanently closed recoverable leases (ENG-498).
func TestHandleProvisionError_CircuitOpenIsTransient(t *testing.T) {
	var closed []string
	var mu sync.Mutex
	mockChain := &chaintest.MockClient{
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closed = append(closed, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	r, err := newReconciler(
		ReconcilerConfig{ProviderUUID: "test-uuid", CallbackBaseURL: "http://localhost"},
		mockChain, noopAck, router, nil, nil,
	)
	require.NoError(t, err)

	lease := billingtypes.Lease{Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_ACTIVE}
	hadError := false
	r.handleProvisionError(
		context.Background(),
		fmt.Errorf("provision failed: %w", backend.ErrCircuitOpen),
		"lease-1",
		lease,
		&hadError,
	)

	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, closed, "circuit-open must NOT close an active lease")
	assert.True(t, hadError, "circuit-open must flag the cycle for retry")
}

func TestReconciler_ReconcileAll_PendingNotProvisioned(t *testing.T) {
	// Setup: Pending lease on chain, not provisioned on backend
	// Expected: Start provisioning
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Empty - not provisioned
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify provisioning was started
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Len(t, mockBackend.provisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.provisionCalls[0].LeaseUUID)
}

func TestReconciler_ReconcileAll_PendingProvisionedReady(t *testing.T) {
	// Setup: Pending lease on chain, provisioned and ready on backend
	// Expected: Acknowledge the lease via the acknowledger
	var acknowledgedLeases []string
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			mu.Lock()
			defer mu.Unlock()
			acknowledgedLeases = append(acknowledgedLeases, leaseUUID)
			return true, "tx-hash", nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, ack, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was acknowledged via the acknowledger
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, acknowledgedLeases, 1)
	assert.Equal(t, "lease-1", acknowledgedLeases[0])
}

func TestReconciler_SkipsInFlightReadyLease(t *testing.T) {
	// Setup: Pending-ready lease that the main flow is already processing
	// (tracker.IsInFlight == true). Reconciler must NOT ack — the callback
	// handler owns the ack. Metric must be incremented.
	var acknowledgeCount int
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			mu.Lock()
			defer mu.Unlock()
			acknowledgeCount++
			return true, "tx-hash", nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockTracker := newMockInFlightTracker(nil)
	mockTracker.TrackInFlight("lease-1", "tenant-1", nil, "test")

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, ack, router, mockTracker, nil)
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.ReconcilerInflightSkipsTotal)
	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	mu.Lock()
	defer mu.Unlock()
	assert.Zero(t, acknowledgeCount, "reconciler must not ack an in-flight lease")
	after := promtestutil.ToFloat64(metrics.ReconcilerInflightSkipsTotal)
	assert.Equal(t, 1.0, after-before, "ReconcilerInflightSkipsTotal should increment by 1")
}

func TestReconciler_AcksNotInFlightReadyLease(t *testing.T) {
	// Setup: Pending-ready lease with a wired tracker that does NOT have it in-flight.
	// Expected: Reconciler acks normally (no race with main flow because main flow isn't running).
	var acknowledgeCount int
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			mu.Lock()
			defer mu.Unlock()
			acknowledgeCount++
			return true, "tx-hash", nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockTracker := newMockInFlightTracker(nil) // empty — lease is NOT in-flight

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, ack, router, mockTracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, acknowledgeCount, "reconciler must ack when main flow is NOT processing the lease")
}

func TestReconciler_InFlightFailedLeaseDefersUntilOperationSettles(t *testing.T) {
	// A fresh Failed inventory row may describe the previous incarnation while a
	// tracked operation is still reaching the backend. Defer until that exact
	// generation settles, then let a newer sweep reject from fresh evidence.
	var rejectedLeases []string
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailed},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockTracker := newMockInFlightTracker(nil)
	generation, tracked := mockTracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-1", nil, "test",
	)
	require.True(t, tracked)

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, mockTracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	mu.Lock()
	assert.Empty(t, rejectedLeases,
		"a Failed row must not reject while a possibly newer operation is still in flight")
	mu.Unlock()

	require.True(t, mockTracker.UntrackInFlightIfOperationID("lease-1", generation))
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"lease-1"}, rejectedLeases,
		"the next fresh sweep must reject after the operation has settled")
}

func TestReconciler_ReconcileAll_ActiveNotProvisioned(t *testing.T) {
	// Setup: Active lease on chain, not provisioned on backend (anomaly)
	// Expected: Log anomaly and attempt to provision
	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Empty - not provisioned
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify provisioning was attempted (anomaly recovery)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Len(t, mockBackend.provisionCalls, 1)
}

func TestReconciler_ReconcileAll_ActiveProvisioned(t *testing.T) {
	// Setup: Active lease on chain, provisioned on backend
	// Expected: Nothing (healthy state)
	var acknowledgeCount int
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			acknowledgeCount += len(leaseUUIDs)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify nothing was done
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	mu.Lock()
	ackCount := acknowledgeCount
	mu.Unlock()

	assert.Equal(t, 0, provisionCount)
	assert.Equal(t, 0, deprovisionCount)
	assert.Equal(t, 0, ackCount)
}

func TestReconciler_ReconcileAll_ActiveProvisioned_CallsReconcileCustomDomain(t *testing.T) {
	// On every healthy active lease, the reconciler must dispatch the
	// chain Items[].CustomDomain values to Backend.ReconcileCustomDomain
	// so the backend can detect and apply drift.
	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE,
					Items: []billingtypes.LeaseItem{
						{SkuUuid: "docker-small", Quantity: 1, ServiceName: "web", CustomDomain: "foo.example.com"},
					},
				},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady, BackendName: "test"},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	mockBackend.mu.Lock()
	calls := append([]reconcileCustomDomainCall(nil), mockBackend.reconcileCustomDomainArgs...)
	mockBackend.mu.Unlock()

	require.Len(t, calls, 1, "exactly one ReconcileCustomDomain call per healthy active lease")
	assert.Equal(t, "lease-1", calls[0].leaseUUID)
	require.Len(t, calls[0].items, 1)
	assert.Equal(t, "web", calls[0].items[0].ServiceName)
	assert.Equal(t, "foo.example.com", calls[0].items[0].CustomDomain)
}

func TestReconciler_ReconcileAll_ReconcileCustomDomainErrorDoesNotAbortTick(t *testing.T) {
	// A failure on ReconcileCustomDomain for one lease must not stop
	// processing of other leases on the same tick.
	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
				{Uuid: "lease-2", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady, BackendName: "test"},
			{LeaseUUID: "lease-2", Status: backend.ProvisionStatusReady, BackendName: "test"},
		},
		reconcileCustomDomainErr: assert.AnError,
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	// ReconcileAll surfaces the error (hadError=true), but we want both
	// leases to have been visited despite the first failing.
	_ = reconciler.ReconcileAll(t.Context())

	mockBackend.mu.Lock()
	count := len(mockBackend.reconcileCustomDomainArgs)
	mockBackend.mu.Unlock()
	assert.Equal(t, 2, count, "both leases must be reconciled despite per-lease errors")
}

func TestReconciler_ReconcileAll_OrphanProvision(t *testing.T) {
	// Setup: lease closed on chain, still provisioned on backend (orphan)
	// Expected: Deprovision the orphan
	mockChain := &chaintest.MockClient{
		// Absent from the PENDING/ACTIVE lists, and the chain confirms it closed
		// — the positive evidence ENG-654 requires before destroying state.
		GetLeaseFunc: chaintest.ClosedLeaseFunc("provider-1"),
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-lease", Status: backend.ProvisionStatusReady, BackendName: "test"},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify orphan was deprovisioned
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Len(t, mockBackend.deprovisionCalls, 1)
	assert.Equal(t, "orphan-lease", mockBackend.deprovisionCalls[0])
}

func TestReconciler_ReconcileAll_SkipsInFlightOrphan(t *testing.T) {
	// ENG-594: a lease created on-chain after the sweep's pending/active snapshot
	// but event-provisioned before the provisions fetch shows up in provisions yet
	// not in chainLeases, so it looks like an orphan. While the main provision flow
	// still owns it (tracker.IsInFlight == true) the reconciler must NOT deprovision
	// it — that would tear down a healthy lease mid-provision. A genuinely orphaned
	// (not in-flight) provision in the same sweep must still be deprovisioned.
	mockChain := &chaintest.MockClient{
		// Both leases are closed on chain — both are orphan candidates.
		GetLeaseFunc: chaintest.ClosedLeaseFunc("provider-1"),
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "inflight-orphan", Status: backend.ProvisionStatusReady, BackendName: "test"},
			{LeaseUUID: "real-orphan", Status: backend.ProvisionStatusReady, BackendName: "test"},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockTracker := newMockInFlightTracker(nil)
	mockTracker.TrackInFlight("inflight-orphan", "tenant-1", nil, "test")

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, mockTracker, nil)
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.ReconcilerInflightSkipsTotal)
	assert.NoError(t, reconciler.ReconcileAll(t.Context()))
	after := promtestutil.ToFloat64(metrics.ReconcilerInflightSkipsTotal)

	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Equal(t, []string{"real-orphan"}, mockBackend.deprovisionCalls,
		"in-flight lease must be skipped; only the genuine orphan is deprovisioned")
	assert.Equal(t, 1.0, after-before, "orphan in-flight skip must increment ReconcilerInflightSkipsTotal")
}

func TestReconciler_ProcessOrphan_TypedLeaseClaimFencesInventoryBoundary(t *testing.T) {
	mockBackend := &mockReconcilerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	require.NoError(t, err)

	operations := operation.NewRegistry()
	inventoryBoundary := operations.Snapshot()
	eventClaim := operations.TryClaimLeaseNow("orphan-lease")
	require.True(t, eventClaim.Acquired())

	r := &Reconciler{
		providerUUID:  "provider-1",
		chainClient:   &chaintest.MockClient{GetLeaseFunc: chaintest.ClosedLeaseFunc("provider-1")},
		backendRouter: router,
		operations:    operations,
	}
	info := backend.ProvisionInfo{
		LeaseUUID: "orphan-lease", ProviderUUID: "provider-1", BackendName: "backend-a",
	}
	var orphans, leaseErrors atomic.Int32
	r.processOrphan(t.Context(), "orphan-lease", info, inventoryBoundary, 0, &orphans, &leaseErrors)
	assert.Empty(t, mockBackend.deprovisionCalls,
		"a lifecycle claim acquired after inventory must fence orphan teardown")

	require.True(t, operations.ReleaseLease(eventClaim.Claim()))
	r.processOrphan(t.Context(), "orphan-lease", info, inventoryBoundary, 0, &orphans, &leaseErrors)
	assert.Empty(t, mockBackend.deprovisionCalls,
		"a completed lifecycle action must remain visible to the old boundary")

	r.processOrphan(t.Context(), "orphan-lease", info, operations.Snapshot(), 0, &orphans, &leaseErrors)
	assert.Equal(t, []string{"orphan-lease"}, mockBackend.deprovisionCalls,
		"a claim-free inventory boundary may perform the proven teardown")
	assert.Equal(t, int32(1), orphans.Load())
	assert.Zero(t, leaseErrors.Load())
}

func TestReconciler_ReconcileAll_ChainErrors(t *testing.T) {
	// Test that chain errors are handled gracefully
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tests := []struct {
		name    string
		setup   func() *chaintest.MockClient
		wantErr string
	}{
		{
			name: "get pending error",
			setup: func() *chaintest.MockClient {
				return &chaintest.MockClient{
					GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
						return nil, errors.New("chain unavailable")
					},
				}
			},
			wantErr: "failed to get pending leases",
		},
		{
			name: "get active error",
			setup: func() *chaintest.MockClient {
				return &chaintest.MockClient{
					GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
						return nil, errors.New("chain unavailable")
					},
				}
			},
			wantErr: "failed to get active leases",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockChain := tt.setup()

			reconciler, err := newReconciler(ReconcilerConfig{
				ProviderUUID:    "provider-1",
				CallbackBaseURL: "http://localhost:8080",
			}, mockChain, noopAck, router, nil, nil)
			require.NoError(t, err)

			ctx := t.Context()
			err = reconciler.ReconcileAll(ctx)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestReconciler_ReconcileAll_ContextCancellation(t *testing.T) {
	// Test that ReconcileAll respects context cancellation
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	// Cancel context before calling ReconcileAll
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = reconciler.ReconcileAll(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestReconciler_Start_ContextCancellation(t *testing.T) {
	// Test that Start respects context cancellation
	mockChain := &chaintest.MockClient{}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		Interval:        100 * time.Millisecond, // Short interval for test
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// Start in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- reconciler.Start(ctx)
	}()

	// Give it a moment to start, then cancel
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Should exit with context.Canceled
	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Error("Start() did not exit after context cancellation")
	}
}

func TestReconciler_DefaultInterval(t *testing.T) {
	mockChain := &chaintest.MockClient{}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Create with no interval specified
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		// Interval not set
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	// Verify default interval is 5 minutes
	assert.Equal(t, 5*time.Minute, reconciler.interval)
}

func TestReconciler_RunOnce(t *testing.T) {
	// Verify RunOnce calls ReconcileAll
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.RunOnce(ctx))

	// Verify provisioning was started
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Len(t, mockBackend.provisionCalls, 1)
}

func TestReconciler_ReconcileAll_SkipsInFlightLeases(t *testing.T) {
	// Test that reconciler skips leases that are already being provisioned
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Not provisioned yet
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Create a manager and mark the lease as in-flight
	manager, _ := newTestManager(t, ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, &chaintest.MockClient{})
	manager.TrackInFlight("lease-1", "tenant-1", testItems(""), "test")

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, manager, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify provisioning was NOT started (lease is in-flight)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestReconciler_MultipleBackends(t *testing.T) {
	// Test reconciliation with multiple backends
	mockChain := &chaintest.MockClient{GetLeaseFunc: chaintest.ClosedLeaseFunc("provider-1")}

	backend1 := &mockReconcilerBackend{
		name: "backend1",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-1", Status: backend.ProvisionStatusReady},
		},
	}
	backend2 := &mockReconcilerBackend{
		name: "backend2",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-2", Status: backend.ProvisionStatusReady},
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backend1, IsDefault: true},
			{Backend: backend2, Match: backend.MatchCriteria{SKUs: []string{"b2-sku"}}},
		},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Both orphans should be deprovisioned
	backend1.mu.Lock()
	b1Calls := len(backend1.deprovisionCalls)
	backend1.mu.Unlock()

	backend2.mu.Lock()
	b2Calls := len(backend2.deprovisionCalls)
	backend2.mu.Unlock()

	// Total deprovisions should be 2
	assert.Equal(t, 2, b1Calls+b2Calls)
}

func TestReconciler_ReconcileAll_PendingProvisioning(t *testing.T) {
	// Setup: Pending lease on chain, provisioning in progress on backend
	// Expected: Wait (do nothing) - let the callback complete the flow
	var acknowledgeCount int
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			acknowledgeCount += len(leaseUUIDs)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusProvisioning},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify no actions were taken
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	mu.Lock()
	ackCount := acknowledgeCount
	mu.Unlock()

	assert.Equal(t, 0, provisionCount)
	assert.Equal(t, 0, deprovisionCount)
	assert.Equal(t, 0, ackCount)
}

func TestReconciler_ReconcileAll_PendingFailed(t *testing.T) {
	// Setup: Pending lease on chain, provisioning failed on backend
	// Expected: Reject the lease so tenant's credit is released
	var rejectedLeases []string
	var rejectedReason string
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			rejectedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailed},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify no provisioning or deprovisioning
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	assert.Equal(t, 0, provisionCount)
	assert.Equal(t, 0, deprovisionCount)

	// Verify lease was rejected
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, rejectedLeases, 1)
	if len(rejectedLeases) > 0 {
		assert.Equal(t, "lease-1", rejectedLeases[0])
	}
	assert.Equal(t, "provisioning failed", rejectedReason)
}

func TestReconciler_ReconcileAll_AcknowledgeFailure(t *testing.T) {
	// Test that acknowledge failure is logged but doesn't stop reconciliation
	acknowledgeErr := errors.New("chain unavailable")

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: "lease-2", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	failingAck := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return false, "", acknowledgeErr
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
			{LeaseUUID: "lease-2", Status: backend.ProvisionStatusReady},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, failingAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	// ReconcileAll should succeed even if individual acknowledges fail
	// (errors are logged, not propagated)
	err = reconciler.ReconcileAll(ctx)
	assert.NoError(t, err)
}

func TestReconciler_ReconcileAll_DeprovisionFailure(t *testing.T) {
	// Test that deprovision failure during orphan cleanup is logged but continues
	deprovisionErr := errors.New("backend unavailable")

	mockChain := &chaintest.MockClient{
		// Both leases closed on chain - both provisions are orphans
		GetLeaseFunc: chaintest.ClosedLeaseFunc("provider-1"),
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-1", Status: backend.ProvisionStatusReady},
			{LeaseUUID: "orphan-2", Status: backend.ProvisionStatusReady},
		},
		deprovisionErr: deprovisionErr,
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	// ReconcileAll should succeed even if deprovisions fail
	// (errors are logged, not propagated)
	err = reconciler.ReconcileAll(ctx)
	assert.NoError(t, err)

	// Verify both deprovisions were attempted
	mockBackend.mu.Lock()
	deprovisionCount := len(mockBackend.deprovisionCalls)
	mockBackend.mu.Unlock()

	assert.Equal(t, 2, deprovisionCount)
}

func TestReconciler_ReconcileAll_SkipsOtherProviderOrphans(t *testing.T) {
	// Test that reconciler does NOT deprovision orphans belonging to other providers.
	// This is critical when multiple providers share the same backend.
	mockChain := &chaintest.MockClient{
		// No live leases for our provider; every lease closed on chain
		GetLeaseFunc: chaintest.ClosedLeaseFunc("provider-1"),
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			// Orphan belonging to a DIFFERENT provider - should NOT be deprovisioned
			{LeaseUUID: "other-provider-lease", ProviderUUID: "other-provider-uuid", Status: backend.ProvisionStatusReady},
			// Orphan belonging to OUR provider - should be deprovisioned
			{LeaseUUID: "our-orphan-lease", ProviderUUID: "provider-1", Status: backend.ProvisionStatusReady},
			// Orphan with empty provider UUID (legacy) - should be deprovisioned (conservative)
			{LeaseUUID: "legacy-orphan-lease", ProviderUUID: "", Status: backend.ProvisionStatusReady},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify only our orphans were deprovisioned (not the other provider's)
	mockBackend.mu.Lock()
	deprovisionCalls := mockBackend.deprovisionCalls
	mockBackend.mu.Unlock()

	// Should have deprovisioned 2 leases: our-orphan-lease and legacy-orphan-lease
	// Should NOT have deprovisioned other-provider-lease
	assert.Len(t, deprovisionCalls, 2)

	// Verify the other provider's lease was NOT deprovisioned
	assert.NotContains(t, deprovisionCalls, "other-provider-lease")
}

// TestReconciler_ConcurrentProvisioningRace is a regression test for the TOCTOU race
// condition between the reconciler and event-driven manager. It simulates multiple
// goroutines (representing manager and reconciler) racing to provision the same lease.
//
// The test verifies that despite concurrent attempts, exactly ONE provision call is
// made to the backend - preventing duplicate resource creation.
//
// Run with: go test -race -run TestReconciler_ConcurrentProvisioningRace -count=10
func TestReconciler_ConcurrentProvisioningRace(t *testing.T) {
	const leaseUUID = "race-test-lease"

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: leaseUUID, Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}

	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Not provisioned yet
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Create manager (shared between reconciler and simulated event handler)
	manager, err := newTestManager(t, ManagerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, router, &chaintest.MockClient{})
	require.NoError(t, err)

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, manager, nil)
	require.NoError(t, err)

	// Simulate concurrent provisioning attempts.
	// Capture ctx before spawning goroutines to avoid calling t.Context()
	// from a background goroutine, which can panic if the test exits early.
	const numGoroutines = 50
	ctx := t.Context()
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Channel to synchronize start
	start := make(chan struct{})

	// Half the goroutines simulate manager's TryTrackInFlight + Provision
	// Half simulate reconciler's startProvisioning (which also uses TryTrackInFlight)
	for i := range numGoroutines {
		go func(workerID int) {
			defer wg.Done()
			<-start

			// Simulate the atomic check-and-provision pattern used by both
			// manager.handleLeaseCreated and reconciler.startProvisioning
			if manager.TryTrackInFlight(leaseUUID, "tenant-1", testItems(""), "test") {
				// Only provision if we successfully tracked
				_ = mockBackend.Provision(ctx, backend.ProvisionRequest{
					LeaseUUID:    leaseUUID,
					Tenant:       "tenant-1",
					ProviderUUID: "provider-1",
					CallbackURL:  "http://localhost:8080/callbacks/provision",
				})
			}
		}(i)
	}

	// Start all goroutines simultaneously
	close(start)
	wg.Wait()

	// Verify exactly ONE provision call was made
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	mockBackend.mu.Unlock()

	assert.Equal(t, 1, provisionCount, "race condition detected!")

	// The lease should be tracked
	assert.True(t, manager.IsInFlight(leaseUUID))

	// Now test that reconciler.ReconcileAll also respects the in-flight tracking
	// Reset the mock to track new calls
	mockBackend.mu.Lock()
	mockBackend.provisionCalls = nil
	mockBackend.mu.Unlock()

	// Run reconciliation - should NOT provision again (already in-flight)
	assert.NoError(t, reconciler.ReconcileAll(t.Context()))

	mockBackend.mu.Lock()
	additionalProvisions := len(mockBackend.provisionCalls)
	mockBackend.mu.Unlock()

	assert.Equal(t, 0, additionalProvisions, "lease is in-flight")
}

func TestReconciler_ConcurrentReconciliation_NonBlocking(t *testing.T) {
	// Test that concurrent reconciliation attempts don't block - they skip instead.
	// This verifies the atomic flag approach works correctly.
	reconcileStarted := make(chan struct{})
	reconcileCanContinue := make(chan struct{})

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Signal that reconciliation has started
			close(reconcileStarted)
			// Wait for permission to continue (simulates long-running operation)
			<-reconcileCanContinue
			return nil, nil
		},
	}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	// Start first reconciliation in background.
	// Capture ctx before the goroutine to avoid calling t.Context() from a
	// background goroutine, which can panic if the test exits early.
	ctx := t.Context()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- reconciler.ReconcileAll(ctx)
	}()

	// Wait for first reconciliation to start
	<-reconcileStarted

	// Try second reconciliation - should return immediately (non-blocking)
	secondStart := time.Now()
	err = reconciler.ReconcileAll(t.Context())
	secondDuration := time.Since(secondStart)

	// Second call should return quickly (not block waiting for first)
	assert.Less(t, secondDuration, 100*time.Millisecond)

	// No error expected - it just skips
	assert.NoError(t, err)

	// Let first reconciliation complete
	close(reconcileCanContinue)
	assert.NoError(t, <-firstDone)

	// After first completes, a new reconciliation should be allowed
	mockChain.GetPendingLeasesFunc = func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
		return nil, nil
	}
	assert.NoError(t, reconciler.ReconcileAll(t.Context()))
}

func TestReconciler_ReconcileAll_ContextCancelledDuringLoop(t *testing.T) {
	// Test that context cancellation during the reconciliation loop is handled gracefully.
	// This tests the context checks we added within the loop iterations.
	var callCount int
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: "lease-2", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: "lease-3", Tenant: "tenant-3", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}

	// Use a backend that cancels context after first provision
	mockBackend := &mockCancellingBackend{
		name: "test",
		onProvision: func() {
			mu.Lock()
			callCount++
			currentCount := callCount
			mu.Unlock()
			if currentCount == 1 {
				cancel()
			}
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Use MaxWorkers=1 to ensure sequential processing for this cancellation test
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		MaxWorkers:      1,
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	err = reconciler.ReconcileAll(ctx)

	// Should return context.Canceled
	assert.ErrorIs(t, err, context.Canceled)

	// With sequential processing (MaxWorkers=1), should process exactly 1 before cancellation
	mu.Lock()
	finalCount := callCount
	mu.Unlock()

	// First provision triggers cancel, second should see cancelled context
	assert.Equal(t, 1, finalCount)
}

func TestReconciler_ReconcileAll_ContextCancelledDuringOrphanLoop(t *testing.T) {
	// Test that context cancellation during orphan cleanup loop is handled gracefully.
	ctx, cancel := context.WithCancel(context.Background())
	var deprovisionCount int
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		// All leases closed on chain - all provisions are orphans
		GetLeaseFunc: chaintest.ClosedLeaseFunc("provider-1"),
	}

	// Use a backend that cancels context after first deprovision
	mockBackend := &mockCancellingBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-1", Status: backend.ProvisionStatusReady},
			{LeaseUUID: "orphan-2", Status: backend.ProvisionStatusReady},
			{LeaseUUID: "orphan-3", Status: backend.ProvisionStatusReady},
		},
		onDeprovision: func() {
			mu.Lock()
			deprovisionCount++
			currentCount := deprovisionCount
			mu.Unlock()
			if currentCount == 1 {
				cancel()
			}
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Use MaxWorkers=1 to ensure sequential processing for this cancellation test
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		MaxWorkers:      1,
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	err = reconciler.ReconcileAll(ctx)

	// Should return context.Canceled
	assert.ErrorIs(t, err, context.Canceled)

	// With sequential processing (MaxWorkers=1), should process exactly 1 before cancellation
	mu.Lock()
	finalCount := deprovisionCount
	mu.Unlock()

	// First deprovision triggers cancel, second should see cancelled context
	assert.Equal(t, 1, finalCount)
}

// mockCancellingBackend is a test backend with callback hooks for testing cancellation behavior.
type mockCancellingBackend struct {
	name          string
	provisions    []backend.ProvisionInfo
	onProvision   func()
	onDeprovision func()
}

func (m *mockCancellingBackend) Name() string {
	return m.name
}

func (m *mockCancellingBackend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	if m.onProvision != nil {
		m.onProvision()
	}
	return nil
}

func (m *mockCancellingBackend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	return nil, nil
}

func (m *mockCancellingBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	if m.onDeprovision != nil {
		m.onDeprovision()
	}
	return nil
}

func (m *mockCancellingBackend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	return m.provisions, nil
}

func (m *mockCancellingBackend) LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
	return nil, nil
}

func (m *mockCancellingBackend) Health(ctx context.Context) error {
	return nil
}

func (m *mockCancellingBackend) RefreshState(ctx context.Context) error {
	return nil
}

func (m *mockCancellingBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockCancellingBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	return nil, backend.ErrNotProvisioned
}
func (m *mockCancellingBackend) Restart(ctx context.Context, req backend.RestartRequest) error {
	return nil
}
func (m *mockCancellingBackend) Update(ctx context.Context, req backend.UpdateRequest) error {
	return nil
}
func (m *mockCancellingBackend) Restore(ctx context.Context, req backend.RestoreRequest) error {
	return backend.ErrNotRetained
}
func (m *mockCancellingBackend) ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error {
	return nil
}
func (m *mockCancellingBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockCancellingBackend) GetLoadStats(_ context.Context) (*backend.LoadStats, error) {
	return nil, nil
}

func (m *mockCancellingBackend) ListRetentions(_ context.Context) ([]backend.RetainedLease, error) {
	return nil, nil
}

func TestReconciler_ReconcileAll_SKUBasedRouting(t *testing.T) {
	// Test that leases are routed to the correct backend based on SKU
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				// GPU lease should go to gpu-backend
				{
					Uuid:   "gpu-lease",
					Tenant: "tenant-1",
					State:  billingtypes.LEASE_STATE_PENDING,
					Items:  []billingtypes.LeaseItem{{SkuUuid: "gpu-a100-4x", Quantity: 1}},
				},
				// K8s lease should go to k8s-backend (default)
				{
					Uuid:   "k8s-lease",
					Tenant: "tenant-2",
					State:  billingtypes.LEASE_STATE_PENDING,
					Items:  []billingtypes.LeaseItem{{SkuUuid: "k8s-small", Quantity: 1}},
				},
				// Unknown SKU should go to default backend
				{
					Uuid:   "unknown-lease",
					Tenant: "tenant-3",
					State:  billingtypes.LEASE_STATE_PENDING,
					Items:  []billingtypes.LeaseItem{{SkuUuid: "unknown-sku", Quantity: 1}},
				},
			}, nil
		},
	}

	gpuBackend := &mockReconcilerBackend{name: "gpu-backend", provisions: []backend.ProvisionInfo{}}
	k8sBackend := &mockReconcilerBackend{name: "k8s-backend", provisions: []backend.ProvisionInfo{}}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: gpuBackend, Match: backend.MatchCriteria{SKUs: []string{"gpu-a100-4x"}}},
			{Backend: k8sBackend, Match: backend.MatchCriteria{SKUs: []string{"k8s-small"}}, IsDefault: true},
		},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify GPU lease went to GPU backend
	gpuBackend.mu.Lock()
	gpuCalls := gpuBackend.provisionCalls
	gpuBackend.mu.Unlock()

	assert.Len(t, gpuCalls, 1)
	if len(gpuCalls) > 0 {
		assert.Equal(t, "gpu-lease", gpuCalls[0].LeaseUUID)
		assert.Equal(t, "gpu-a100-4x", gpuCalls[0].RoutingSKU())
	}

	// Verify K8s and unknown leases went to K8s backend (default)
	k8sBackend.mu.Lock()
	k8sCalls := k8sBackend.provisionCalls
	k8sBackend.mu.Unlock()

	assert.Len(t, k8sCalls, 2)

	// Verify the SKUs are passed correctly
	skus := make(map[string]bool)
	for _, call := range k8sCalls {
		skus[call.RoutingSKU()] = true
	}
	assert.True(t, skus["k8s-small"])
	assert.True(t, skus["unknown-sku"])
}

func TestReconciler_MaxWorkers_Default(t *testing.T) {
	// Test that default MaxWorkers is applied
	mockChain := &chaintest.MockClient{}

	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		// MaxWorkers not set - should use default
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, DefaultReconcileWorkers, reconciler.maxWorkers)
}

func TestReconciler_MaxWorkers_Custom(t *testing.T) {
	// Test that custom MaxWorkers is respected
	mockChain := &chaintest.MockClient{}

	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		MaxWorkers:      5,
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 5, reconciler.maxWorkers)
}

func TestReconciler_ParallelProcessing(t *testing.T) {
	// Test that leases are processed in parallel
	var (
		mu             sync.Mutex
		concurrentMax  int
		currentWorkers int
		totalProcessed int
	)

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Create enough leases to test parallelism
			leases := make([]billingtypes.Lease, 20)
			for i := range leases {
				leases[i] = billingtypes.Lease{
					Uuid:   "lease-" + string(rune('a'+i)),
					Tenant: "tenant",
					State:  billingtypes.LEASE_STATE_PENDING,
				}
			}
			return leases, nil
		},
	}

	// Backend that tracks concurrent workers
	mockBackend := &mockConcurrencyBackend{
		name: "test",
		onProvision: func() {
			mu.Lock()
			currentWorkers++
			if currentWorkers > concurrentMax {
				concurrentMax = currentWorkers
			}
			mu.Unlock()

			// Simulate some work
			time.Sleep(10 * time.Millisecond)

			mu.Lock()
			currentWorkers--
			totalProcessed++
			mu.Unlock()
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		MaxWorkers:      5, // Limit to 5 concurrent workers
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	mu.Lock()
	defer mu.Unlock()

	// Verify all leases were processed
	assert.Equal(t, 20, totalProcessed)

	// Verify parallel processing occurred (more than 1 concurrent worker)
	assert.GreaterOrEqual(t, concurrentMax, 2)

	// Verify MaxWorkers limit was respected
	assert.LessOrEqual(t, concurrentMax, 5)
}

func TestReconciler_ParallelOrphanProcessing(t *testing.T) {
	// Test that orphans are processed in parallel
	var (
		mu             sync.Mutex
		concurrentMax  int
		currentWorkers int
		totalProcessed int
	)

	mockChain := &chaintest.MockClient{
		// All leases closed on chain - all provisions are orphans
		GetLeaseFunc: chaintest.ClosedLeaseFunc("provider-1"),
	}

	// Create many orphan provisions
	provisions := make([]backend.ProvisionInfo, 15)
	for i := range provisions {
		provisions[i] = backend.ProvisionInfo{
			LeaseUUID: "orphan-" + string(rune('a'+i)),
			Status:    backend.ProvisionStatusReady,
		}
	}

	mockBackend := &mockConcurrencyBackend{
		name:       "test",
		provisions: provisions,
		onDeprovision: func() {
			mu.Lock()
			currentWorkers++
			if currentWorkers > concurrentMax {
				concurrentMax = currentWorkers
			}
			mu.Unlock()

			// Simulate some work
			time.Sleep(10 * time.Millisecond)

			mu.Lock()
			currentWorkers--
			totalProcessed++
			mu.Unlock()
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
		MaxWorkers:      4, // Limit to 4 concurrent workers
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	mu.Lock()
	defer mu.Unlock()

	// Verify all orphans were processed
	assert.Equal(t, 15, totalProcessed)

	// Verify parallel processing occurred
	assert.GreaterOrEqual(t, concurrentMax, 2)

	// Verify MaxWorkers limit was respected
	assert.LessOrEqual(t, concurrentMax, 4)
}

func TestReconciler_ParallelBackendFetching(t *testing.T) {
	// Test that backend provisions are fetched in parallel
	var (
		mu             sync.Mutex
		concurrentMax  int
		currentWorkers int
		fetchCount     int
	)

	mockChain := &chaintest.MockClient{}

	// Create multiple backends that track concurrent fetches
	backend1 := &mockConcurrencyBackend{
		name: "backend-1",
		onListProvisions: func() {
			mu.Lock()
			currentWorkers++
			fetchCount++
			if currentWorkers > concurrentMax {
				concurrentMax = currentWorkers
			}
			mu.Unlock()

			// Simulate network latency
			time.Sleep(20 * time.Millisecond)

			mu.Lock()
			currentWorkers--
			mu.Unlock()
		},
	}

	backend2 := &mockConcurrencyBackend{
		name: "backend-2",
		onListProvisions: func() {
			mu.Lock()
			currentWorkers++
			fetchCount++
			if currentWorkers > concurrentMax {
				concurrentMax = currentWorkers
			}
			mu.Unlock()

			time.Sleep(20 * time.Millisecond)

			mu.Lock()
			currentWorkers--
			mu.Unlock()
		},
	}

	backend3 := &mockConcurrencyBackend{
		name: "backend-3",
		onListProvisions: func() {
			mu.Lock()
			currentWorkers++
			fetchCount++
			if currentWorkers > concurrentMax {
				concurrentMax = currentWorkers
			}
			mu.Unlock()

			time.Sleep(20 * time.Millisecond)

			mu.Lock()
			currentWorkers--
			mu.Unlock()
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backend1, Match: backend.MatchCriteria{SKUs: []string{"gpu-a100"}}},
			{Backend: backend2, Match: backend.MatchCriteria{SKUs: []string{"vm-basic"}}},
			{Backend: backend3, Match: backend.MatchCriteria{SKUs: []string{"k8s-small"}}, IsDefault: true},
		},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	mu.Lock()
	defer mu.Unlock()

	// Verify all backends were fetched
	assert.Equal(t, 3, fetchCount)

	// Verify parallel fetching occurred (all 3 should run concurrently)
	assert.GreaterOrEqual(t, concurrentMax, 2)
}

// mockConcurrencyBackend is a test backend for verifying parallel execution.
type mockConcurrencyBackend struct {
	name             string
	provisions       []backend.ProvisionInfo
	onProvision      func()
	onDeprovision    func()
	onListProvisions func()
}

func (m *mockConcurrencyBackend) Name() string {
	return m.name
}

func (m *mockConcurrencyBackend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	if m.onProvision != nil {
		m.onProvision()
	}
	return nil
}

func (m *mockConcurrencyBackend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	return nil, nil
}

func (m *mockConcurrencyBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	if m.onDeprovision != nil {
		m.onDeprovision()
	}
	return nil
}

func (m *mockConcurrencyBackend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	if m.onListProvisions != nil {
		m.onListProvisions()
	}
	return m.provisions, nil
}

func (m *mockConcurrencyBackend) LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
	return nil, nil
}

func (m *mockConcurrencyBackend) Health(ctx context.Context) error {
	return nil
}

func (m *mockConcurrencyBackend) RefreshState(ctx context.Context) error {
	return nil
}

func (m *mockConcurrencyBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockConcurrencyBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	return nil, backend.ErrNotProvisioned
}
func (m *mockConcurrencyBackend) Restart(ctx context.Context, req backend.RestartRequest) error {
	return nil
}
func (m *mockConcurrencyBackend) Update(ctx context.Context, req backend.UpdateRequest) error {
	return nil
}
func (m *mockConcurrencyBackend) Restore(ctx context.Context, req backend.RestoreRequest) error {
	return backend.ErrNotRetained
}
func (m *mockConcurrencyBackend) ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error {
	return nil
}
func (m *mockConcurrencyBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	return nil, backend.ErrNotProvisioned
}

func (m *mockConcurrencyBackend) GetLoadStats(_ context.Context) (*backend.LoadStats, error) {
	return nil, nil
}

func (m *mockConcurrencyBackend) ListRetentions(_ context.Context) ([]backend.RetainedLease, error) {
	return nil, nil
}

// mockInFlightTracker implements ReconcilerTracker for testing orphaned payload cleanup.
type mockInFlightTracker struct {
	payloadStore         *payload.Store
	inFlight             map[string]InFlightProvision
	nextGeneration       uint64
	mutationRevision     uint64
	lastMutation         map[string]uint64
	lastSnapshotRevision uint64
	reconcileClaims      map[string]struct{}
	mu                   sync.Mutex
	hasPayloadErr        error
	hasPayloadFunc       func(leaseUUID string) (bool, error) // optional override
}

func mockOperationID(sequence uint64) operation.OperationID {
	id, err := operation.ParseID(fmt.Sprintf("00000000-0000-4000-8000-%012x", sequence))
	if err != nil {
		panic(err)
	}
	return id
}

func newMockInFlightTracker(payloadStore *payload.Store) *mockInFlightTracker {
	return &mockInFlightTracker{
		payloadStore:    payloadStore,
		inFlight:        make(map[string]InFlightProvision),
		lastMutation:    make(map[string]uint64),
		reconcileClaims: make(map[string]struct{}),
	}
}

func (m *mockInFlightTracker) TryTrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool {
	_, ok := m.TryTrackInFlightWithOperationID(leaseUUID, tenant, items, backendName)
	return ok
}

func (m *mockInFlightTracker) TryTrackInFlightWithOperationID(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (operation.OperationID, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, claimed := m.reconcileClaims[leaseUUID]; claimed {
		return operation.OperationID{}, false
	}
	if _, exists := m.inFlight[leaseUUID]; exists {
		return operation.OperationID{}, false
	}
	m.nextGeneration++
	operationID := mockOperationID(m.nextGeneration)
	m.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID:   leaseUUID,
		Tenant:      tenant,
		Items:       items,
		Backend:     backendName,
		OperationID: operationID,
	}
	m.markMutationLocked(leaseUUID)
	return operationID, true
}

func (m *mockInFlightTracker) TryTrackInFlightWithOperationIDIfNotNewer(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	maxRevision uint64,
) (operation.OperationID, bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastMutation[leaseUUID] > maxRevision {
		return operation.OperationID{}, false, true
	}
	if _, claimed := m.reconcileClaims[leaseUUID]; !claimed {
		return operation.OperationID{}, false, false
	}
	if _, exists := m.inFlight[leaseUUID]; exists {
		return operation.OperationID{}, false, false
	}
	m.nextGeneration++
	operationID := mockOperationID(m.nextGeneration)
	m.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID:   leaseUUID,
		Tenant:      tenant,
		Items:       items,
		Backend:     backendName,
		OperationID: operationID,
	}
	m.markMutationLocked(leaseUUID)
	return operationID, true, false
}

func (m *mockInFlightTracker) TryClaimLeaseActionIfNotNewer(
	leaseUUID string,
	maxRevision uint64,
) (bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastMutation[leaseUUID] > maxRevision {
		return false, true
	}
	if _, exists := m.inFlight[leaseUUID]; exists {
		return false, false
	}
	if _, exists := m.reconcileClaims[leaseUUID]; exists {
		return false, false
	}
	m.reconcileClaims[leaseUUID] = struct{}{}
	return true, false
}

func (m *mockInFlightTracker) TryClaimLeaseAction(leaseUUID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.inFlight[leaseUUID]; exists {
		return false
	}
	if _, exists := m.reconcileClaims[leaseUUID]; exists {
		return false
	}
	m.reconcileClaims[leaseUUID] = struct{}{}
	m.markMutationLocked(leaseUUID)
	return true
}

func (m *mockInFlightTracker) ReleaseLeaseAction(leaseUUID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.reconcileClaims[leaseUUID]; !exists {
		return false
	}
	delete(m.reconcileClaims, leaseUUID)
	m.markMutationLocked(leaseUUID)
	return true
}

func (m *mockInFlightTracker) SnapshotMutationRevision() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	for leaseUUID, revision := range m.lastMutation {
		if revision <= m.lastSnapshotRevision {
			delete(m.lastMutation, leaseUUID)
		}
	}
	m.lastSnapshotRevision = m.mutationRevision
	return m.mutationRevision
}

func (m *mockInFlightTracker) markMutationLocked(leaseUUID string) {
	m.mutationRevision++
	m.lastMutation[leaseUUID] = m.mutationRevision
}

func (m *mockInFlightTracker) TryTrackRestoreInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool {
	_, ok := m.TryTrackRestoreInFlightWithOperationID(leaseUUID, tenant, items, backendName)
	return ok
}

func (m *mockInFlightTracker) TryTrackRestoreInFlightWithOperationID(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (operation.OperationID, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, claimed := m.reconcileClaims[leaseUUID]; claimed {
		return operation.OperationID{}, false
	}
	if _, exists := m.inFlight[leaseUUID]; exists {
		return operation.OperationID{}, false
	}
	m.nextGeneration++
	operationID := mockOperationID(m.nextGeneration)
	m.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID:   leaseUUID,
		Tenant:      tenant,
		Items:       items,
		Backend:     backendName,
		OperationID: operationID,
		Kind:        KindRestore,
	}
	m.markMutationLocked(leaseUUID)
	return operationID, true
}

func (m *mockInFlightTracker) TrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, exists := m.inFlight[leaseUUID]; exists && p.settlementClaimed {
		return
	}
	if _, claimed := m.reconcileClaims[leaseUUID]; claimed {
		return
	}
	m.nextGeneration++
	operationID := mockOperationID(m.nextGeneration)
	m.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID:   leaseUUID,
		Tenant:      tenant,
		Items:       items,
		Backend:     backendName,
		OperationID: operationID,
	}
	m.markMutationLocked(leaseUUID)
}

func (m *mockInFlightTracker) UntrackInFlight(leaseUUID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, exists := m.inFlight[leaseUUID]; exists && !p.settlementClaimed {
		delete(m.inFlight, leaseUUID)
		m.markMutationLocked(leaseUUID)
	}
}

func (m *mockInFlightTracker) UntrackInFlightIfOperationID(leaseUUID string, generation operation.OperationID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, exists := m.inFlight[leaseUUID]
	if !exists || p.OperationID != generation || p.settlementClaimed {
		return false
	}
	delete(m.inFlight, leaseUUID)
	m.markMutationLocked(leaseUUID)
	return true
}

func (m *mockInFlightTracker) TryClaimInFlight(leaseUUID string, generation operation.OperationID) (InFlightProvision, bool) {
	return m.tryClaimInFlight(leaseUUID, generation, inFlightSettlementTerminal)
}

func (m *mockInFlightTracker) TryClaimInFlightForDeprovision(
	leaseUUID string,
	generation operation.OperationID,
) (InFlightProvision, bool) {
	return m.tryClaimInFlight(leaseUUID, generation, inFlightSettlementDeprovision)
}

func (m *mockInFlightTracker) tryClaimInFlight(
	leaseUUID string,
	generation operation.OperationID,
	owner inFlightSettlementOwner,
) (InFlightProvision, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, exists := m.inFlight[leaseUUID]
	if !exists || p.OperationID != generation || p.settlementClaimed {
		return InFlightProvision{}, false
	}
	p.settlementClaimed = true
	p.settlementOwner = owner
	m.inFlight[leaseUUID] = p
	return p, true
}

func (m *mockInFlightTracker) ReleaseInFlightClaim(leaseUUID string, generation operation.OperationID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, exists := m.inFlight[leaseUUID]
	if !exists || p.OperationID != generation || !p.settlementClaimed {
		return false
	}
	p.settlementClaimed = false
	p.settlementOwner = inFlightSettlementUnclaimed
	m.inFlight[leaseUUID] = p
	return true
}

func (m *mockInFlightTracker) FinishClaimedInFlight(leaseUUID string, generation operation.OperationID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, exists := m.inFlight[leaseUUID]
	if !exists || p.OperationID != generation || !p.settlementClaimed {
		return false
	}
	delete(m.inFlight, leaseUUID)
	m.markMutationLocked(leaseUUID)
	return true
}

func (m *mockInFlightTracker) PopInFlight(leaseUUID string) (InFlightProvision, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, exists := m.inFlight[leaseUUID]
	if exists && p.settlementClaimed {
		return InFlightProvision{}, false
	}
	if exists {
		delete(m.inFlight, leaseUUID)
		m.markMutationLocked(leaseUUID)
	}
	return p, exists
}

func (m *mockInFlightTracker) GetInFlight(leaseUUID string) (InFlightProvision, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, exists := m.inFlight[leaseUUID]
	return p, exists
}

func (m *mockInFlightTracker) IsInFlight(leaseUUID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.inFlight[leaseUUID]
	return exists
}

func (m *mockInFlightTracker) InFlightCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.inFlight)
}

func (m *mockInFlightTracker) InFlightCountsByBackend() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := make(map[string]int, len(m.inFlight))
	for _, p := range m.inFlight {
		counts[p.Backend]++
	}
	return counts
}

func (m *mockInFlightTracker) GetInFlightLeases() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	leases := make([]string, 0, len(m.inFlight))
	for k := range m.inFlight {
		leases = append(leases, k)
	}
	return leases
}

func (m *mockInFlightTracker) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	return m.InFlightCount()
}

func (m *mockInFlightTracker) GetTimedOutProvisions(timeout time.Duration) []InFlightProvision {
	return nil
}

func (m *mockInFlightTracker) HasPayload(leaseUUID string) (bool, error) {
	if m.hasPayloadErr != nil {
		return false, m.hasPayloadErr
	}
	if m.hasPayloadFunc != nil {
		return m.hasPayloadFunc(leaseUUID)
	}
	if m.payloadStore == nil {
		return false, nil
	}
	return m.payloadStore.Has(leaseUUID)
}

func (m *mockInFlightTracker) PayloadStore() *payload.Store {
	return m.payloadStore
}

func TestReconciler_CleansUpOrphanedPayloads(t *testing.T) {
	// Create a temp dir for the payload store
	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	// Store payloads for various leases
	// pending-awaiting: pending lease with MetaHash but hasn't uploaded payload yet - simulates
	// a lease that's still waiting for payload (payload won't be in store, so nothing to clean)
	payloadStore.Store("closed-lease", []byte("closed payload"))      // Will be cleaned (chain confirms CLOSED)
	payloadStore.Store("nonexistent-lease", []byte("orphan payload")) // KEPT (chain has no record — ENG-654)
	payloadStore.Store("active-lease", []byte("active payload"))      // Retained for re-provisioning

	// Verify all payloads are stored
	require.Equal(t, 3, payloadStore.Count())

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				// Pending lease without payload (no MetaHash) - will be provisioned without payload
				{Uuid: "pending-no-payload", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "active-lease", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_ACTIVE},
				// Note: "closed-lease" and "nonexistent-lease" are not returned (not pending or active)
			}, nil
		},
		// The per-payload re-check that separates the two absent leases: one the
		// chain positively reports CLOSED, one it has never heard of. Absence
		// from the lists above cannot tell them apart, and only the first
		// authorises deleting the payload (ENG-654).
		GetLeaseFunc: func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			if leaseUUID == "closed-lease" {
				return chaintest.NewMockLease(leaseUUID, "tenant-3", "provider-1", billingtypes.LEASE_STATE_CLOSED), nil
			}
			return nil, nil
		},
	}

	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockTracker := newMockInFlightTracker(payloadStore)

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, mockTracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	require.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify orphaned payloads were cleaned up
	// active-lease: payload should be RETAINED (active leases keep payload for re-provisioning)
	hasActive, err := payloadStore.Has("active-lease")
	require.NoError(t, err)
	assert.True(t, hasActive, "expected active-lease payload to be retained for re-provisioning")

	// closed-lease: payload should be cleaned (chain positively reports CLOSED)
	hasClosed, err := payloadStore.Has("closed-lease")
	require.NoError(t, err)
	assert.False(t, hasClosed, "expected closed-lease payload to be cleaned up (chain confirms it closed)")

	// nonexistent-lease: payload should be KEPT. The ledger never deletes a
	// lease, so "the chain has no record" is not evidence the lease finished —
	// it is a phantom, a wrong chain, or a lagging node. Fred counts it and
	// leaves it alone (ENG-654).
	hasNonexistent, err := payloadStore.Has("nonexistent-lease")
	require.NoError(t, err)
	assert.True(t, hasNonexistent, "expected nonexistent-lease payload to be kept: absence is not evidence")

	// Verify count - active-lease and the unknown lease's payload remain
	assert.Equal(t, 2, payloadStore.Count())
}

// TestReconciler_ConcurrentReconcileAll tests that concurrent ReconcileAll calls
// are properly serialized by the atomic flag.
func TestReconciler_ReconcileAll_ActiveFailedExhausted(t *testing.T) {
	// Setup: Active lease on chain, failed provision with FailCount >= maxReprovisionAttempts
	// Expected: Close the lease and deprovision the backend resources
	var closedLeases []string
	var closedReason string
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			closedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{
				LeaseUUID:   "lease-1",
				Status:      backend.ProvisionStatusFailed,
				FailCount:   3, // >= DefaultMaxReprovisionAttempts (3)
				BackendName: "test",
			},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was closed (not rejected — it's ACTIVE, not PENDING)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, closedLeases, 1)
	assert.Equal(t, "lease-1", closedLeases[0])
	assert.Contains(t, closedReason, "failed 3 times")

	// Verify backend resources were released immediately
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.deprovisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.deprovisionCalls[0])

	// Verify NO re-provisioning was attempted
	assert.Empty(t, mockBackend.provisionCalls)
}

// ENG-635 at the reconciler tier, and the safety property that matters most
// about it: refusing to substitute must be TRANSIENT.
//
// The reconciler is the path that fires unattended, on a timer, for every
// affected lease at once. An ACTIVE lease whose recorded backend is missing
// from the router previously reached `ACTIVE && !isProvisioned` and was
// re-provisioned onto a peer, laying an empty volume over live data. It now
// refuses — but the refusal must reach handleProvisionError's transient default
// and never its reject/close branches. A backend is usually absent because it
// was paused, renamed or is mid-redeploy; closing paying leases on chain for
// that would convert an operator's maintenance window into permanent, chain-
// recorded tenant data loss (the ENG-498 class of bug).
func TestReconciler_ReconcileAll_UnresolvablePlacement_RefusesWithoutTerminating(t *testing.T) {
	var closedLeases, rejectedLeases []string
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}

	// A healthy peer that reports no provisions: the lease looks unprovisioned,
	// and this backend is exactly where the old code would have sent it.
	peer := &mockReconcilerBackend{name: "peer", provisions: []backend.ProvisionInfo{}}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: peer, IsDefault: true}},
	})

	// Placement names a backend that is not configured — the state produced by
	// removing or renaming a host.
	ps := &mockPlacementStore{}
	require.NoError(t, ps.Set("lease-1", "removed-backend"))

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)
	leaseErrorActions := metrics.ReconciliationActions.WithLabelValues(metrics.ActionLeaseError)
	leaseErrorsBefore := promtestutil.ToFloat64(leaseErrorActions)

	// The sweep itself must not fail: one refused lease is a per-lease error,
	// not a reason to abandon the rest of the fleet.
	assert.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Equal(t, 1.0, promtestutil.ToFloat64(leaseErrorActions)-leaseErrorsBefore,
		"an unconfigured durable owner must surface as an operator-visible lease error")

	peer.mu.Lock()
	provisionCalls := len(peer.provisionCalls)
	peer.mu.Unlock()
	assert.Zero(t, provisionCalls,
		"the lease must not be provisioned onto a peer — that is the empty-volume-over-live-data failure")

	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, closedLeases, "a missing backend must never close a paying ACTIVE lease on chain")
	assert.Empty(t, rejectedLeases, "a missing backend must never reject a lease on chain")

	// The placement record is the only pointer to where the data actually
	// lives; refusing must not discard it.
	assert.Equal(t, "removed-backend", ps.Get("lease-1"))
}

func TestReconciler_ReconcileAll_ActiveFailedBelowMax(t *testing.T) {
	// Setup: Active lease on chain, failed provision with FailCount < maxReprovisionAttempts
	// Expected: Attempt re-provisioning (not close)
	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{
				LeaseUUID:   "lease-1",
				Status:      backend.ProvisionStatusFailed,
				FailCount:   1, // < DefaultMaxReprovisionAttempts (3)
				BackendName: "test",
			},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify re-provisioning was attempted (not closed)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Len(t, mockBackend.provisionCalls, 1)
	assert.Equal(t, "lease-1", mockBackend.provisionCalls[0].LeaseUUID)

	// No deprovisions (that happens after close, not re-provision)
	assert.Empty(t, mockBackend.deprovisionCalls)
}

func TestReconciler_ConcurrentReconcileAll(t *testing.T) {
	// Create mock chain client that returns one pending lease.
	// Add a delay to ensure reconciliation takes some time, which allows us to
	// verify that concurrent calls are properly serialized by the atomic flag.
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Small delay to ensure concurrent ReconcileAll calls overlap.
			// Without this, the first call would complete before others even start,
			// making them sequential rather than concurrent.
			time.Sleep(10 * time.Millisecond)
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
	}

	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080/callbacks",
		MaxWorkers:      2,
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	// Start multiple concurrent ReconcileAll calls.
	// Capture ctx before spawning goroutines to avoid calling t.Context()
	// from a background goroutine, which can panic if the test exits early.
	const numGoroutines = 10
	ctx := t.Context()
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := make(chan struct{})
	completed := make(chan struct{}, numGoroutines)

	for range numGoroutines {
		go func() {
			defer wg.Done()
			<-start

			_ = reconciler.ReconcileAll(ctx)
			completed <- struct{}{}
		}()
	}

	// Start all goroutines simultaneously
	close(start)
	wg.Wait()
	close(completed)

	// Count how many completed
	var completedCount int
	for range completed {
		completedCount++
	}

	// All should complete (either by running or skipping)
	assert.Equal(t, numGoroutines, completedCount)

	// At most ONE provision call should have been made
	// (the atomic flag prevents concurrent reconciliation)
	mockBackend.mu.Lock()
	provisionCount := len(mockBackend.provisionCalls)
	mockBackend.mu.Unlock()

	assert.LessOrEqual(t, provisionCount, 1, "concurrent reconciliation not prevented!")
}

func TestReconciler_ReconcileAll_PendingValidationError_Rejects(t *testing.T) {
	// Setup: Pending lease on chain, not provisioned, backend returns ErrValidation
	// Expected: Reject the lease immediately (not retry forever)
	var rejectedLeases []string
	var rejectedReason string
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			rejectedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: fmt.Errorf("%w: bad-sku", backend.ErrUnknownSKU),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was rejected (not left to retry forever)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, rejectedLeases, 1)
	assert.Equal(t, "lease-1", rejectedLeases[0])
	assert.Equal(t, rejectReasonInvalidSKU, rejectedReason)
}

func TestReconciler_ReconcileAll_PreflightFailureKeepsLeaseFencedThroughRejection(t *testing.T) {
	rejectStarted := make(chan struct{})
	allowReject := make(chan struct{})
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING,
			}}, nil
		},
		RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
			close(rejectStarted)
			<-allowReject
			return 1, []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: fmt.Errorf("%w: bad-sku", backend.ErrUnknownSKU),
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	require.NoError(t, err)
	tracker := newMockInFlightTracker(nil)
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	done := make(chan error, 1)
	ctx := t.Context()
	go func() { done <- reconciler.ReconcileAll(ctx) }()
	select {
	case <-rejectStarted:
	case <-time.After(time.Second):
		t.Fatal("reconciler did not reach terminal rejection")
	}

	// doStartProvisioning has removed its failed in-flight entry by this point.
	// The worker's lease-action claim must remain, otherwise an event/restore or
	// close can enter between that cleanup and the terminal chain transaction.
	_, eventTracked := tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-racer", nil, "test",
	)
	close(allowReject)
	require.NoError(t, <-done)
	if eventTracked {
		current, exists := tracker.GetInFlight("lease-1")
		if exists {
			tracker.UntrackInFlightIfOperationID("lease-1", current.OperationID)
		}
	}
	assert.False(t, eventTracked, "the worker must retain its action fence through rejection")

	_, eventTracked = tracker.TryTrackInFlightWithOperationID(
		"lease-1", "tenant-after", nil, "test",
	)
	assert.True(t, eventTracked, "the action fence must be released after the worker finishes")
}

func TestReconciler_ReconcileAll_PendingCircuitOpen_Retries(t *testing.T) {
	// Setup: Pending lease on chain, not provisioned, backend returns ErrCircuitOpen.
	// Expected: the lease is NOT rejected. Circuit-open is transient (the breaker
	// auto-recovers), so it is retried next cycle rather than terminated on-chain
	// for a brief backend outage (ENG-498).
	var rejectedLeases []string
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: backend.ErrCircuitOpen,
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify the lease was NOT rejected — a transient breaker trip must not
	// terminate a recoverable lease.
	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, rejectedLeases, "circuit-open must not reject a pending lease")
}

func TestReconciler_ReconcileAll_PendingWithPayloadValidationError_Rejects(t *testing.T) {
	// Setup: Pending lease with MetaHash, payload available, backend returns ErrValidation
	// Expected: Reject the lease immediately
	var rejectedLeases []string
	var rejectedReason string
	var mu sync.Mutex

	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	payloadData := []byte("test payload")
	payloadHash := sha256.Sum256(payloadData)
	payloadStore.Store("lease-1", payloadData)

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_PENDING,
					MetaHash: payloadHash[:], // SHA-256 of payloadData
				},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			rejectedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: fmt.Errorf("%w: bad yaml", backend.ErrInvalidManifest),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockTracker := newMockInFlightTracker(payloadStore)

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, mockTracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was rejected
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, rejectedLeases, 1)
	assert.Equal(t, "lease-1", rejectedLeases[0])
	assert.Equal(t, rejectReasonInvalidManifest, rejectedReason)
}

func TestReconciler_ReconcileAll_ActiveNotProvisionedValidationError_Closes(t *testing.T) {
	// Setup: Active lease on chain, not provisioned (anomaly), backend returns ErrValidation
	// Expected: Close the lease (not reject — it's ACTIVE)
	var closedLeases []string
	var closedReason string
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			closedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: fmt.Errorf("%w: registry %q", backend.ErrImageNotAllowed, "evil.io"),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was closed (not rejected — it's ACTIVE)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, closedLeases, 1)
	assert.Equal(t, "lease-1", closedLeases[0])
	assert.Equal(t, rejectReasonImageNotAllowed, closedReason)
}

func TestReconciler_ReconcileAll_ActiveFailedValidationError_Closes(t *testing.T) {
	// Setup: Active lease, failed provision with FailCount < max, re-provision returns ErrValidation
	// Expected: Close the lease immediately (not keep retrying)
	var closedLeases []string
	var closedReason string
	var mu sync.Mutex

	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			closedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{
				LeaseUUID:   "lease-1",
				Status:      backend.ProvisionStatusFailed,
				FailCount:   1, // Below max — would normally re-provision
				BackendName: "test",
			},
		},
		provisionErr: fmt.Errorf("%w: removed-sku", backend.ErrUnknownSKU),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was closed (validation error is permanent)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, closedLeases, 1)
	assert.Equal(t, "lease-1", closedLeases[0])
	assert.Equal(t, rejectReasonInvalidSKU, closedReason)
}

// --- Placement store integration tests ---

func TestReconciler_PlacementSweepTrustRequiresSuccessfulSync(t *testing.T) {
	t.Parallel()

	newDependencies := func(t *testing.T) (ReconcilerChainClient, BackendRouter) {
		t.Helper()
		chainClient := &chaintest.MockClient{
			GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
				return nil, nil
			},
			GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
				return nil, nil
			},
		}
		b := &mockReconcilerBackend{name: "backend-a"}
		router, err := backend.NewRouter(backend.RouterConfig{
			Backends: []backend.BackendEntry{{Backend: b, IsDefault: true}},
		})
		require.NoError(t, err)
		return chainClient, router
	}

	t.Run("successful empty sync arms latch", func(t *testing.T) {
		chainClient, router := newDependencies(t)
		r, err := newReconciler(ReconcilerConfig{
			ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
		}, chainClient, noopAck, router, newMockInFlightTracker(nil), &mockPlacementStore{})
		require.NoError(t, err)

		require.NoError(t, r.ReconcileAll(t.Context()))
		require.True(t, r.placementSweepSeen.Load())
	})

	t.Run("failed empty sync does not arm latch", func(t *testing.T) {
		chainClient, router := newDependencies(t)
		r, err := newReconciler(ReconcilerConfig{
			ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
		}, chainClient, noopAck, router, newMockInFlightTracker(nil), &errorPlacementStore{setErr: errors.New("disk full")})
		require.NoError(t, err)

		require.NoError(t, r.ReconcileAll(t.Context()))
		require.False(t, r.placementSweepSeen.Load())
	})

	t.Run("later write failure disarms an earlier trust proof", func(t *testing.T) {
		chainClient, router := newDependencies(t)
		store := &errorPlacementStore{}
		r, err := newReconciler(ReconcilerConfig{
			ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
		}, chainClient, noopAck, router, newMockInFlightTracker(nil), store)
		require.NoError(t, err)

		require.NoError(t, r.ReconcileAll(t.Context()))
		require.True(t, r.placementSweepSeen.Load())
		store.setErr = errors.New("disk full")
		require.NoError(t, r.ReconcileAll(t.Context()))
		require.False(t, r.placementSweepSeen.Load(),
			"record absence is no longer authoritative after a failed durable sync")
	})

	t.Run("new process starts conservative", func(t *testing.T) {
		chainClient, router := newDependencies(t)
		r, err := newReconciler(ReconcilerConfig{
			ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
		}, chainClient, noopAck, router, newMockInFlightTracker(nil), &mockPlacementStore{})
		require.NoError(t, err)
		require.False(t, r.placementSweepSeen.Load())
	})

	t.Run("placement-disabled process never arms latch", func(t *testing.T) {
		chainClient, router := newDependencies(t)
		r, err := newReconciler(ReconcilerConfig{
			ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
		}, chainClient, noopAck, router, nil, nil)
		require.NoError(t, err)

		require.NoError(t, r.ReconcileAll(t.Context()))
		require.False(t, r.placementSweepSeen.Load())
	})
}

func TestReconciler_DegradedAdmissionRechecksPendingStateUnderLeaseClaim(t *testing.T) {
	const leaseUUID = "lease-became-active"
	var exposeLease bool
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			if !exposeLease {
				return nil, nil
			}
			return []billingtypes.Lease{{
				Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
			}}, nil
		},
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}
	healthy := &mockReconcilerBackend{name: "backend-a"}
	peer := &mockReconcilerBackend{name: "backend-b"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: healthy, IsDefault: true}, {Backend: peer},
	}})
	require.NoError(t, err)
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil), operations: operation.NewRegistry(),
	}
	reconciler, err := NewReconciler(
		ReconcilerConfig{ProviderUUID: "provider-1", CallbackBaseURL: "http://callback"},
		chainClient, noopAck, router, runtime, store,
	)
	require.NoError(t, err)
	require.NoError(t, reconciler.ReconcileAll(t.Context()), "establish durable baseline")
	require.True(t, store.CurrentAdmissionBaseline().Valid())

	exposeLease = true
	peer.mu.Lock()
	peer.listErr = errors.New("backend-b unavailable")
	peer.mu.Unlock()
	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	healthy.mu.Lock()
	assert.Empty(t, healthy.provisionCalls,
		"a PENDING list row that is ACTIVE at the authoritative read must not use degraded recordless admission")
	healthy.mu.Unlock()
	assert.False(t, runtime.operations.Contains(leaseUUID))
	assert.Equal(t, placement.StateAbsent, store.Lookup(leaseUUID).State())
}

func TestReconciler_DegradedAdmissionRejectsRouterEscapeBeforeAttempt(t *testing.T) {
	const leaseUUID = "lease-router-escape"
	lease := billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
	}
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{lease}, nil
		},
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			copy := lease
			return &copy, nil
		},
	}
	healthy := &mockReconcilerBackend{name: "backend-a"}
	excluded := &mockReconcilerBackend{
		name: "backend-b", listErr: errors.New("backend-b unavailable"),
	}
	router := &mockBackendRouter{
		routeFn: func(string) backend.Backend { return excluded },
		routeForProvisionAmongFn: func(
			context.Context, string, map[string]struct{}, map[string]int,
		) backend.Backend {
			return excluded // deliberately violate the routing port contract
		},
		backendsFn: func() []backend.Backend { return []backend.Backend{healthy, excluded} },
	}
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil), operations: operation.NewRegistry(),
	}
	reconciler, err := NewReconciler(
		ReconcilerConfig{ProviderUUID: "provider-1", CallbackBaseURL: "http://callback"},
		chainClient, noopAck, router, runtime, store,
	)
	require.NoError(t, err)
	fence := store.BeginInventorySession()
	_, err = store.ProjectInventory(fence, placement.InventoryProjection{Complete: true})
	store.EndInventorySession(fence)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	for _, candidate := range []*mockReconcilerBackend{healthy, excluded} {
		candidate.mu.Lock()
		assert.Empty(t, candidate.provisionCalls)
		candidate.mu.Unlock()
	}
	assert.False(t, runtime.operations.Contains(leaseUUID))
	assert.Equal(t, placement.StateAbsent, store.Lookup(leaseUUID).State(),
		"the typed scope must reject an excluded route before any durable attempt")
}

func TestReconciler_DegradedAdmissionRacingEventPathDispatchesExactlyOnce(t *testing.T) {
	const leaseUUID = "lease-event-reconcile-race"
	lease := &billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{*lease}, nil
		},
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			copy := *lease
			return &copy, nil
		},
	}
	healthy := &mockReconcilerBackend{name: "backend-a"}
	unavailable := &mockReconcilerBackend{
		name: "backend-b", listErr: errors.New("backend-b unavailable"),
	}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: healthy, IsDefault: true}, {Backend: unavailable},
	}})
	require.NoError(t, err)
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	registry := operation.NewRegistry()
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil), operations: registry,
	}
	reconciler, err := NewReconciler(
		ReconcilerConfig{ProviderUUID: "provider-1", CallbackBaseURL: "http://callback"},
		chainClient, noopAck, router, runtime, store,
	)
	require.NoError(t, err)
	fence := store.BeginInventorySession()
	_, err = store.ProjectInventory(fence, placement.InventoryProjection{Complete: true})
	store.EndInventorySession(fence)
	require.NoError(t, err)
	orchestrator, err := NewProvisionOrchestrator(
		"provider-1", "http://callback", router, registry, store, nil,
	)
	require.NoError(t, err)

	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		errs <- reconciler.ReconcileAll(t.Context())
	}()
	go func() {
		<-start
		claimResult := registry.TryClaimLeaseNow(leaseUUID)
		if !claimResult.Acquired() {
			errs <- nil
			return
		}
		claim := claimResult.Claim()
		err := orchestrator.StartProvisioningClaimed(
			t.Context(), claim, lease, ProvisionOpts{},
		)
		_ = registry.ReleaseLease(claim)
		errs <- err
	}()
	close(start)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)

	healthy.mu.Lock()
	assert.Len(t, healthy.provisionCalls, 1,
		"the shared lease claim and write-ahead CAS must collapse event/reconcile dispatch")
	healthy.mu.Unlock()
	unavailable.mu.Lock()
	assert.Empty(t, unavailable.provisionCalls)
	unavailable.mu.Unlock()
	assert.Equal(t, placement.StateConfirmed, store.Lookup(leaseUUID).State())
}

func TestReconciler_DegradedPositiveRetentionDefersOnlyThatLiveLease(t *testing.T) {
	const (
		retainedLease    = "lease-retained-live"
		independentLease = "lease-independent"
	)
	var exposeLeases bool
	leases := []billingtypes.Lease{
		{Uuid: retainedLease, Tenant: "tenant-retained", State: billingtypes.LEASE_STATE_PENDING},
		{Uuid: independentLease, Tenant: "tenant-independent", State: billingtypes.LEASE_STATE_PENDING},
	}
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			if !exposeLeases {
				return nil, nil
			}
			return slices.Clone(leases), nil
		},
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		GetLeaseFunc: func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			for _, lease := range leases {
				if lease.Uuid == leaseUUID {
					copy := lease
					return &copy, nil
				}
			}
			return nil, nil
		},
	}
	healthy := &mockReconcilerBackend{name: "backend-a"}
	peer := &mockReconcilerBackend{name: "backend-b"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: healthy, IsDefault: true}, {Backend: peer},
	}})
	require.NoError(t, err)
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil), operations: operation.NewRegistry(),
	}
	reconciler, err := NewReconciler(
		ReconcilerConfig{ProviderUUID: "provider-1", CallbackBaseURL: "http://callback"},
		chainClient, noopAck, router, runtime, store,
	)
	require.NoError(t, err)
	require.NoError(t, reconciler.ReconcileAll(t.Context()), "establish durable baseline")

	exposeLeases = true
	healthy.mu.Lock()
	healthy.retentions = []backend.RetainedLease{{LeaseUUID: retainedLease}}
	healthy.mu.Unlock()
	peer.mu.Lock()
	peer.listErr = errors.New("backend-b unavailable")
	peer.mu.Unlock()

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	healthy.mu.Lock()
	require.Len(t, healthy.provisionCalls, 1,
		"a positive retention must defer only its lease, not healthy-node admission")
	assert.Equal(t, independentLease, healthy.provisionCalls[0].LeaseUUID)
	healthy.mu.Unlock()
	peer.mu.Lock()
	assert.Empty(t, peer.provisionCalls)
	peer.mu.Unlock()
	assert.Equal(t, placement.StateAbsent, store.Lookup(retainedLease).State(),
		"partial retention evidence must neither manufacture placement nor permit provisioning")
	assert.Equal(t, placement.StateConfirmed, store.Lookup(independentLease).State())
}

func TestReconciler_DegradedConfirmedOwnerNeedsRetentionEvidenceBeforeReprovision(t *testing.T) {
	for _, test := range []struct {
		name              string
		retentionFailure  bool
		positiveRetention bool
		peerOutage        bool
	}{
		{name: "retention inventory failed", retentionFailure: true},
		{
			name:              "retention positively reports the lease during peer outage",
			positiveRetention: true,
			peerOutage:        true,
		},
		{name: "complete inventory positively reports the lease", positiveRetention: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const (
				ownedLease       = "lease-owned-active"
				independentLease = "lease-independent"
			)
			var exposeLeases bool
			active := billingtypes.Lease{
				Uuid: ownedLease, Tenant: "tenant-owned", State: billingtypes.LEASE_STATE_ACTIVE,
			}
			pending := billingtypes.Lease{
				Uuid: independentLease, Tenant: "tenant-independent", State: billingtypes.LEASE_STATE_PENDING,
			}
			chainClient := &chaintest.MockClient{
				GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
					if !exposeLeases {
						return nil, nil
					}
					return []billingtypes.Lease{pending}, nil
				},
				GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
					if !exposeLeases {
						return nil, nil
					}
					return []billingtypes.Lease{active}, nil
				},
				GetLeaseFunc: func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
					switch leaseUUID {
					case ownedLease:
						copy := active
						return &copy, nil
					case independentLease:
						copy := pending
						return &copy, nil
					default:
						return nil, nil
					}
				},
			}
			ownerBase := &mockReconcilerBackend{name: "backend-owner"}
			var owner backend.Backend = ownerBase
			var ownerRetentionFailure *retentionErrorReconcilerBackend
			if test.retentionFailure {
				ownerRetentionFailure = &retentionErrorReconcilerBackend{mockReconcilerBackend: ownerBase}
				owner = ownerRetentionFailure
			}
			healthy := &mockReconcilerBackend{name: "backend-healthy"}
			outage := &mockReconcilerBackend{name: "backend-outage"}
			router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
				{Backend: owner}, {Backend: healthy, IsDefault: true}, {Backend: outage},
			}})
			require.NoError(t, err)
			store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			runtime := &typedTestReconcilerRuntime{
				mockInFlightTracker: newMockInFlightTracker(nil), operations: operation.NewRegistry(),
			}
			reconciler, err := NewReconciler(
				ReconcilerConfig{ProviderUUID: "provider-1", CallbackBaseURL: "http://callback"},
				chainClient, noopAck, router, runtime, store,
			)
			require.NoError(t, err)
			require.NoError(t, reconciler.ReconcileAll(t.Context()), "establish durable baseline")
			require.NoError(t, store.Confirm(ownedLease, ownerBase.Name()))

			exposeLeases = true
			if test.retentionFailure {
				ownerRetentionFailure.err = errors.New("owner retentions unavailable")
			}
			if test.positiveRetention {
				ownerBase.mu.Lock()
				ownerBase.retentions = []backend.RetainedLease{{LeaseUUID: ownedLease}}
				ownerBase.mu.Unlock()
			}
			if test.peerOutage {
				outage.mu.Lock()
				outage.listErr = errors.New("unrelated backend unavailable")
				outage.mu.Unlock()
			}

			require.NoError(t, reconciler.ReconcileAll(t.Context()))

			ownerBase.mu.Lock()
			assert.Empty(t, ownerBase.provisionCalls,
				"negative provision evidence must not overwrite possibly retained owner data")
			ownerBase.mu.Unlock()
			healthy.mu.Lock()
			require.Len(t, healthy.provisionCalls, 1,
				"the owner-specific retention gate must not pause unrelated admission")
			assert.Equal(t, independentLease, healthy.provisionCalls[0].LeaseUUID)
			healthy.mu.Unlock()
			assert.Equal(t, placement.StateConfirmed, store.Lookup(ownedLease).State())
			assert.Equal(t, placement.StateConfirmed, store.Lookup(independentLease).State())
		})
	}
}

func TestReconciler_DegradedAdmissionRequiresOneBackendToAnswerBothInventories(t *testing.T) {
	const leaseUUID = "lease-no-common-responder"
	var exposeLease bool
	lease := billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
	}
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			if !exposeLease {
				return nil, nil
			}
			return []billingtypes.Lease{lease}, nil
		},
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			copy := lease
			return &copy, nil
		},
	}
	provisionsResponderBase := &mockReconcilerBackend{name: "backend-provisions"}
	provisionsResponder := &retentionErrorReconcilerBackend{
		mockReconcilerBackend: provisionsResponderBase,
	}
	retentionsResponder := &mockReconcilerBackend{name: "backend-retentions"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: provisionsResponder, IsDefault: true}, {Backend: retentionsResponder},
	}})
	require.NoError(t, err)
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil), operations: operation.NewRegistry(),
	}
	reconciler, err := NewReconciler(
		ReconcilerConfig{ProviderUUID: "provider-1", CallbackBaseURL: "http://callback"},
		chainClient, noopAck, router, runtime, store,
	)
	require.NoError(t, err)
	require.NoError(t, reconciler.ReconcileAll(t.Context()), "establish durable baseline")

	exposeLease = true
	provisionsResponder.err = errors.New("retention inventory unavailable")
	retentionsResponder.mu.Lock()
	retentionsResponder.listErr = errors.New("provision inventory unavailable")
	retentionsResponder.mu.Unlock()
	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	for _, candidate := range []*mockReconcilerBackend{
		provisionsResponderBase, retentionsResponder,
	} {
		candidate.mu.Lock()
		assert.Empty(t, candidate.provisionCalls)
		candidate.mu.Unlock()
	}
	assert.Equal(t, placement.StateAbsent, store.Lookup(leaseUUID).State())
	assert.False(t, runtime.operations.Contains(leaseUUID))
}

func TestReconciler_PlacementProjectionFailureDefersPositiveActiveFailure(t *testing.T) {
	const leaseUUID = "lease-positive-write-error"
	chainClient := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_ACTIVE,
			}}, nil
		},
	}
	b := &mockReconcilerBackend{
		name: "backend-a",
		provisions: []backend.ProvisionInfo{{
			LeaseUUID: leaseUUID, Status: backend.ProvisionStatusFailed,
			FailCount: 1, BackendName: "backend-a",
		}},
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: b, IsDefault: true}},
	})
	require.NoError(t, err)
	store := &failNextBatchPlacementStore{
		PlacementStore: &mockPlacementStore{},
		failErr:        errors.New("placement projection unavailable"),
	}
	payloadStore := newReconcilerPayloadStore(t, t.TempDir()+"/payloads.db")
	t.Cleanup(func() { require.NoError(t, payloadStore.Close()) })
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, newMockInFlightTracker(payloadStore), store)
	require.NoError(t, err)

	// The positive failed provision would normally trigger an immediate
	// re-provision. Because its placement projection failed, the observation must
	// instead become a lease-local fence for the remainder of this sweep.
	require.NoError(t, r.ReconcileAll(t.Context()))
	b.mu.Lock()
	assert.Empty(t, b.provisionCalls,
		"an unpersisted positive observation must not authorize same-sweep reprovisioning")
	b.mu.Unlock()
	require.Contains(t, r.placementAbsenceUntrusted, leaseUUID)
	assert.Contains(t, r.placementAbsenceUntrusted[leaseUUID], "backend-a")

	// The injected failure is one-shot. A later complete, durable projection
	// settles the marker and allows the ordinary ACTIVE/Failed repair path.
	require.NoError(t, r.ReconcileAll(t.Context()))
	b.mu.Lock()
	assert.Len(t, b.provisionCalls, 1)
	b.mu.Unlock()
	assert.NotContains(t, r.placementAbsenceUntrusted, leaseUUID)
}

func TestReconciler_InFlightPlacementExclusionOnlyWithholdsAbsenceForThatLease(t *testing.T) {
	pendingCalls := 0
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			pendingCalls++
			leases := []billingtypes.Lease{{
				Uuid: "lease-inflight", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
			}}
			if pendingCalls > 1 {
				leases = append(leases, billingtypes.Lease{
					Uuid: "lease-independent", Tenant: "tenant-b", State: billingtypes.LEASE_STATE_PENDING,
				})
			}
			return leases, nil
		},
	}
	b := &mockReconcilerBackend{name: "backend-a", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-inflight", Status: backend.ProvisionStatusProvisioning,
	}}}
	peer := &mockReconcilerBackend{name: "backend-b"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: b}, {Backend: peer, IsDefault: true}},
	})
	require.NoError(t, err)
	tracker := newMockInFlightTracker(nil)
	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		"lease-inflight", "tenant-a", testItems("sku-1"), "backend-a",
	)
	require.True(t, tracked)
	placements := &mockPlacementStore{}
	require.NoError(t, placements.Set("lease-inflight", "backend-a"))
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, tracker, placements)
	require.NoError(t, err)

	// The in-flight observation exactly matches an already-confirmed owner. It is
	// excluded for this sweep's lifecycle boundary, but carries no missing
	// placement information and must not create a persistent exception.
	require.NoError(t, r.ReconcileAll(t.Context()))
	require.True(t, r.placementSweepSeen.Load())
	assert.NotContains(t, r.placementAbsenceUntrusted, "lease-inflight")
	require.True(t, tracker.UntrackInFlightIfOperationID("lease-inflight", generation))

	b.mu.Lock()
	b.listErr = errors.New("backend-a unavailable")
	b.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))

	peer.mu.Lock()
	provisionCalls := append([]backend.ProvisionRequest(nil), peer.provisionCalls...)
	peer.mu.Unlock()
	require.Len(t, provisionCalls, 1,
		"the later degraded sweep should progress only the independently trusted lease")
	assert.Equal(t, "lease-independent", provisionCalls[0].LeaseUUID)
}

func TestReconciler_ExcludedAttemptMatchingPositiveRetiresDuringPartialSweep(t *testing.T) {
	const leaseUUID = "lease-attempt-owner-recovers"
	store, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	requireSetPlacementAttempt(t, store, leaseUUID, "backend-a")

	chainClient := &chaintest.MockClient{GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
		return []billingtypes.Lease{{
			Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
		}}, nil
	}}
	owner := &mockReconcilerBackend{name: "backend-a", provisions: []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID, Status: backend.ProvisionStatusReady,
	}}}
	peer := &mockReconcilerBackend{name: "backend-b"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: owner, IsDefault: true}, {Backend: peer},
	}})
	require.NoError(t, err)
	tracker := newMockInFlightTracker(nil)
	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		leaseUUID, "tenant-a", testItems("sku-1"), owner.Name(),
	)
	require.True(t, tracked)
	acknowledged := 0
	ack := &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
		acknowledged++
		return true, "tx", nil
	}}
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, ack, router, tracker, store)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.Contains(t, r.placementAbsenceUntrusted, leaseUUID,
		"the in-flight attempt must exclude its same-snapshot positive")
	// Model the normal callback fast path settling Attempt=A before the next
	// sweep. The following partial inventory must retire the marker from an exact
	// confirmed no-op, not rely on SetBatch performing a mutation.
	require.NoError(t, store.Confirm(leaseUUID, owner.Name()))
	require.True(t, tracker.UntrackInFlightIfOperationID(leaseUUID, generation))
	peer.mu.Lock()
	peer.listErr = errors.New("backend-b unavailable")
	peer.mu.Unlock()

	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.NotContains(t, r.placementAbsenceUntrusted, leaseUUID,
		"the exact lease's newly committed positive must retire its marker without waiting for backend-b")
	p := store.Lookup(leaseUUID)
	require.Equal(t, placement.StateConfirmed, p.State())
	assert.Equal(t, owner.Name(), p.Backend)
	assert.Empty(t, p.Attempt)
	assert.Equal(t, 1, acknowledged)
}

func TestReconciler_PrunesOnlyPositivelyTerminalPlacementAbsenceMarkers(t *testing.T) {
	store, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	unavailable := &mockReconcilerBackend{name: "backend-a", listErr: errors.New("backend unavailable")}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: unavailable, IsDefault: true},
	}})
	require.NoError(t, err)
	chainClient := &chaintest.MockClient{GetLeaseFunc: func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		switch leaseUUID {
		case "closed-lease":
			return chaintest.NewMockLease(
				leaseUUID, "tenant-a", "provider-1", billingtypes.LEASE_STATE_CLOSED,
			), nil
		case "live-but-missed-by-lists":
			return chaintest.NewMockLease(
				leaseUUID, "tenant-a", "provider-1", billingtypes.LEASE_STATE_ACTIVE,
			), nil
		case "query-error":
			return nil, errors.New("chain unavailable")
		default:
			return nil, nil
		}
	}}
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, newMockInFlightTracker(nil), store)
	require.NoError(t, err)
	r.placementAbsenceUntrusted = map[string]map[string]struct{}{
		"closed-lease":             nil,
		"live-but-missed-by-lists": nil,
		"unknown-to-chain":         nil,
		"query-error":              nil,
	}
	cleanupReasons := []string{
		metrics.CleanupSkipChainLive,
		metrics.CleanupSkipChainUnknown,
		metrics.CleanupSkipChainError,
	}
	cleanupSkipsBefore := make(map[string]float64, len(cleanupReasons))
	for _, reason := range cleanupReasons {
		cleanupSkipsBefore[reason] = promtestutil.ToFloat64(
			metrics.ReconcilerCleanupSkipsTotal.WithLabelValues(metrics.CleanupPassPlacement, reason),
		)
	}

	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.NotContains(t, r.placementAbsenceUntrusted, "closed-lease",
		"a positive terminal verdict may retire a marker during an unrelated outage")
	assert.Contains(t, r.placementAbsenceUntrusted, "live-but-missed-by-lists",
		"filtered non-atomic list absence must not erase a live lease's marker")
	assert.Contains(t, r.placementAbsenceUntrusted, "unknown-to-chain",
		"a nil point lookup is not proof that a lease is terminal")
	assert.Contains(t, r.placementAbsenceUntrusted, "query-error",
		"a failed point lookup must keep the fail-closed marker")
	for _, reason := range cleanupReasons {
		assert.Equal(t, cleanupSkipsBefore[reason], promtestutil.ToFloat64(
			metrics.ReconcilerCleanupSkipsTotal.WithLabelValues(metrics.CleanupPassPlacement, reason),
		), "marker retirement is bookkeeping, not withheld destructive cleanup (%s)", reason)
	}
}

func TestReconciler_CompletePlacementSyncDoesNotClearMarkersFromSilence(t *testing.T) {
	store, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureBackendTopology([]string{"backend-a"}))
	healthy := &mockReconcilerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: healthy, IsDefault: true},
	}})
	require.NoError(t, err)
	pointReads := 0
	chainClient := &chaintest.MockClient{GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
		pointReads++
		return chaintest.NewMockLease(
			"old-marker", "tenant-a", "provider-1", billingtypes.LEASE_STATE_ACTIVE,
		), nil
	}}
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, newMockInFlightTracker(nil), store)
	require.NoError(t, err)
	r.placementAbsenceUntrusted = map[string]map[string]struct{}{
		"old-marker": {"backend-a": {}},
	}

	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.Equal(t, 1, pointReads,
		"complete inventory silence is not proof that a previously excluded operation cannot commit")
	assert.Contains(t, r.placementAbsenceUntrusted, "old-marker",
		"a positive live chain verdict must keep the marker fail-closed")
}

func TestReconciler_PlacementMarkerCheckPanicDoesNotCrashFred(t *testing.T) {
	store, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	unavailable := &mockReconcilerBackend{name: "backend-a", listErr: errors.New("backend unavailable")}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: unavailable, IsDefault: true},
	}})
	require.NoError(t, err)
	chainClient := &chaintest.MockClient{GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
		panic("synthetic placement marker GetLease panic")
	}}
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, newMockInFlightTracker(nil), store)
	require.NoError(t, err)
	r.placementAbsenceUntrusted = map[string]map[string]struct{}{
		"panic-marker": {"backend-a": {}},
	}

	panics := metrics.ReconcilerPanicsTotal.WithLabelValues("check_placement_marker")
	before := promtestutil.ToFloat64(panics)
	var reconcileErr error
	require.NotPanics(t, func() {
		reconcileErr = r.ReconcileAll(t.Context())
	}, "one chain-client panic must stay inside its marker worker")
	require.NoError(t, reconcileErr)
	assert.Equal(t, before+1, promtestutil.ToFloat64(panics),
		"the recovered marker-check panic must be operationally visible")
	assert.Contains(t, r.placementAbsenceUntrusted, "panic-marker",
		"a panicking point lookup provides no terminal proof, so the marker must remain fail-closed")
}

func TestReconciler_PairsInventorySnapshotLifetimeOnEveryReturn(t *testing.T) {
	newDependencies := func(t *testing.T, chainClient *chaintest.MockClient) (*placement.Store, *snapshotPairingPlacementStore, *Reconciler) {
		t.Helper()
		store, err := placement.NewStore(t.TempDir() + "/placements.db")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		wrapped := &snapshotPairingPlacementStore{PlacementStore: store}
		b := &mockReconcilerBackend{name: "backend-a"}
		router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
			{Backend: b, IsDefault: true},
		}})
		require.NoError(t, err)
		r, err := newReconciler(ReconcilerConfig{
			ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
		}, chainClient, noopAck, router, newMockInFlightTracker(nil), wrapped)
		require.NoError(t, err)
		return store, wrapped, r
	}

	t.Run("successful sweep releases tombstone horizon", func(t *testing.T) {
		store, wrapped, r := newDependencies(t, &chaintest.MockClient{})
		require.NoError(t, r.ReconcileAll(t.Context()))
		begins, ends := wrapped.calls()
		require.Equal(t, begins, ends)
		require.Len(t, begins, 1)

		cutoff := store.SnapshotRevision()
		require.NoError(t, store.Confirm("post-sweep-delete", "backend-a"))
		require.NoError(t, store.Delete("post-sweep-delete"))
		applied, fenced, err := store.SetBatchIfNotNewer(
			map[string]string{"post-sweep-delete": "backend-a"}, cutoff,
		)
		require.NoError(t, err)
		assert.NotContains(t, fenced, "post-sweep-delete",
			"a delete after ReconcileAll returns must not be retained for the ended inventory")
		assert.Contains(t, applied, "post-sweep-delete")
	})

	t.Run("early chain error still releases snapshot", func(t *testing.T) {
		_, wrapped, r := newDependencies(t, &chaintest.MockClient{
			GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
				return nil, errors.New("chain unavailable")
			},
		})
		require.Error(t, r.ReconcileAll(t.Context()))
		begins, ends := wrapped.calls()
		assert.Equal(t, begins, ends)
		require.Len(t, begins, 1)
	})
}

func TestReconciler_UnrelatedDeleteDoesNotSuppressObservedPlacement(t *testing.T) {
	const (
		leaseUUID = "lease-observed"
		deleted   = "lease-deleted"
	)
	store, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureBackendTopology([]string{"backend-a", "backend-b"}))
	require.NoError(t, store.Confirm(deleted, "backend-a"))

	chainClient := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_ACTIVE,
			}}, nil
		},
	}
	defaultBackend := &mockReconcilerBackend{name: "backend-a"}
	var (
		deleteOnce sync.Once
		hookErr    error
	)
	ownerBackend := &mockReconcilerBackend{
		name: "backend-b",
		provisions: []backend.ProvisionInfo{{
			LeaseUUID: leaseUUID,
			Status:    backend.ProvisionStatusReady,
		}},
		onListProvisions: func() {
			deleteOnce.Do(func() { hookErr = store.Delete(deleted) })
		},
	}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: defaultBackend, IsDefault: true},
		{Backend: ownerBackend},
	}})
	require.NoError(t, err)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, newMockInFlightTracker(nil), store)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	require.NoError(t, hookErr)
	assert.Equal(t, placement.StateAbsent, store.Lookup(deleted).State(),
		"the unrelated deletion hook must run after the inventory boundary")
	p := store.Lookup(leaseUUID)
	require.Equal(t, placement.StateConfirmed, p.State())
	assert.Equal(t, "backend-b", p.Backend,
		"an unrelated post-snapshot delete must not fence this positive observation")

	ownerBackend.mu.Lock()
	ownerBackend.listErr = errors.New("backend-b unavailable")
	ownerBackend.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	defaultBackend.mu.Lock()
	defer defaultBackend.mu.Unlock()
	assert.Empty(t, defaultBackend.provisionCalls,
		"a later degraded sweep must defer the lease pinned to the silent owner")
}

func TestReconciler_ContradictoryPositiveQuarantinesAttemptAcrossRecovery(t *testing.T) {
	const leaseUUID = "lease-excluded-attempt"
	store, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureBackendTopology([]string{"backend-b", "backend-c"}))
	requireSetPlacementAttempt(t, store, leaseUUID, "backend-b")

	chainClient := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_ACTIVE,
			}}, nil
		},
	}
	attemptedBackend := &mockReconcilerBackend{name: "backend-b"}
	actualOwner := &mockReconcilerBackend{name: "backend-c", provisions: []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusReady,
	}}}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: attemptedBackend, IsDefault: true},
		{Backend: actualOwner},
	}})
	require.NoError(t, err)
	tracker := newMockInFlightTracker(nil)
	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		leaseUUID, "tenant-a", testItems("sku-1"), attemptedBackend.Name(),
	)
	require.True(t, tracked)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, tracker, store)
	require.NoError(t, err)

	// A positive C observation while the durable attempt still names B cannot
	// disprove a delayed B commit. It becomes a durable conflict union even though
	// the original operation was already active at the inventory boundary.
	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.NotContains(t, r.placementAbsenceUntrusted, leaseUUID,
		"the durable conflict, rather than a process-local absence marker, owns the safety decision")
	p := store.Lookup(leaseUUID)
	require.Equal(t, placement.StateUnusable, p.State())
	assert.True(t, p.Conflict)
	assert.ElementsMatch(t, []string{"backend-b", "backend-c"}, p.ConflictBackends)
	assert.Equal(t, "backend-b", p.Attempt)
	require.True(t, tracker.UntrackInFlightIfOperationID(leaseUUID, generation))

	// On a later degraded view, neither B's silence nor C's outage can shrink the
	// conflict, clear Attempt B, or authorize a fresh SKU-routed provision.
	actualOwner.mu.Lock()
	actualOwner.listErr = errors.New("backend-c unavailable")
	actualOwner.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	p = store.Lookup(leaseUUID)
	require.Equal(t, placement.StateUnusable, p.State())
	assert.True(t, p.Conflict)
	assert.ElementsMatch(t, []string{"backend-b", "backend-c"}, p.ConflictBackends)
	assert.Equal(t, "backend-b", p.Attempt)
	attemptedBackend.mu.Lock()
	assert.Empty(t, attemptedBackend.provisionCalls)
	attemptedBackend.mu.Unlock()

	// C's later recovery reaffirms one candidate but still cannot prove that the
	// ambiguous B request did not commit after an earlier inventory response.
	actualOwner.mu.Lock()
	actualOwner.listErr = nil
	actualOwner.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	p = store.Lookup(leaseUUID)
	require.Equal(t, placement.StateUnusable, p.State())
	assert.True(t, p.Conflict)
	assert.ElementsMatch(t, []string{"backend-b", "backend-c"}, p.ConflictBackends)
	assert.Equal(t, "backend-b", p.Attempt)
}

func TestReconciler_ContradictoryPositiveQuarantinesConfirmedOwnerAcrossRecovery(t *testing.T) {
	const leaseUUID = "lease-excluded-owner-change"
	store, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.ConfigureBackendTopology([]string{"backend-b", "backend-c"}))
	require.NoError(t, store.Confirm(leaseUUID, "backend-b"))

	chainClient := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_ACTIVE,
			}}, nil
		},
	}
	staleOwner := &mockReconcilerBackend{name: "backend-b"}
	actualOwner := &mockReconcilerBackend{name: "backend-c", provisions: []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusReady,
	}}}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: staleOwner, IsDefault: true},
		{Backend: actualOwner},
	}})
	require.NoError(t, err)
	tracker := newMockInFlightTracker(nil)
	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		leaseUUID, "tenant-a", testItems("sku-1"), staleOwner.Name(),
	)
	require.True(t, tracked)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, tracker, store)
	require.NoError(t, err)

	// C's positive observation cannot overwrite the durable B owner. The
	// contradiction is persisted as a conflict union while the operation
	// straddles this inventory boundary.
	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.NotContains(t, r.placementAbsenceUntrusted, leaseUUID)
	p := store.Lookup(leaseUUID)
	require.Equal(t, placement.StateUnusable, p.State())
	assert.True(t, p.Conflict)
	assert.ElementsMatch(t, []string{"backend-b", "backend-c"}, p.ConflictBackends)
	assert.Equal(t, "backend-b", p.Backend)
	require.True(t, tracker.UntrackInFlightIfOperationID(leaseUUID, generation))

	// B now reports a matching positive while C is silent. Reaffirming one
	// candidate cannot clear the durable conflict or forget C.
	staleOwner.mu.Lock()
	staleOwner.provisions = []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID, Status: backend.ProvisionStatusReady,
	}}
	staleOwner.mu.Unlock()
	actualOwner.mu.Lock()
	actualOwner.listErr = errors.New("backend-c unavailable")
	actualOwner.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	staleOwner.mu.Lock()
	assert.Empty(t, staleOwner.provisionCalls)
	staleOwner.mu.Unlock()
	p = store.Lookup(leaseUUID)
	require.Equal(t, placement.StateUnusable, p.State())
	assert.True(t, p.Conflict)
	assert.ElementsMatch(t, []string{"backend-b", "backend-c"}, p.ConflictBackends)
	assert.Equal(t, "backend-b", p.Backend)

	staleOwner.mu.Lock()
	staleOwner.provisions = nil
	staleOwner.mu.Unlock()
	actualOwner.mu.Lock()
	actualOwner.listErr = nil
	actualOwner.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	p = store.Lookup(leaseUUID)
	require.Equal(t, placement.StateUnusable, p.State())
	assert.True(t, p.Conflict)
	assert.ElementsMatch(t, []string{"backend-b", "backend-c"}, p.ConflictBackends)
	assert.Equal(t, "backend-b", p.Backend)
}

func TestReconciler_FencedContradictionSurvivesUnrelatedPlacementSyncFailure(t *testing.T) {
	const leaseUUID = "lease-fenced-write-error"
	store := &errorPlacementStore{}
	require.NoError(t, store.Confirm(leaseUUID, "backend-b"))
	store.setErr = errors.New("placement disk unavailable")
	store.batchFencedOnError = map[string]struct{}{leaseUUID: {}}

	chainClient := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_ACTIVE,
			}}, nil
		},
	}
	staleOwner := &mockReconcilerBackend{name: "backend-b"}
	actualOwner := &mockReconcilerBackend{name: "backend-c", provisions: []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusReady,
	}}}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: staleOwner, IsDefault: true},
		{Backend: actualOwner},
	}})
	require.NoError(t, err)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, newMockInFlightTracker(nil), store)
	require.NoError(t, err)

	// The C-positive contradicts durable owner B while placement persistence
	// fails. Its exact-lease exception must survive the failed synchronization.
	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.Contains(t, r.placementAbsenceUntrusted, leaseUUID)
	p := store.Lookup(leaseUUID)
	require.Equal(t, placement.StateConfirmed, p.State())
	assert.Equal(t, "backend-b", p.Backend)

	store.setErr = nil
	store.batchFencedOnError = nil
	actualOwner.mu.Lock()
	actualOwner.listErr = errors.New("backend-c unavailable")
	actualOwner.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.Contains(t, r.placementAbsenceUntrusted, leaseUUID)
	staleOwner.mu.Lock()
	assert.Empty(t, staleOwner.provisionCalls,
		"the surviving marker must gate an older confirmed owner on a degraded sweep")
	staleOwner.mu.Unlock()

	actualOwner.mu.Lock()
	actualOwner.listErr = nil
	actualOwner.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.Contains(t, r.placementAbsenceUntrusted, leaseUUID,
		"complete inventory cannot erase a multi-owner marker by silence")
	p = store.Lookup(leaseUUID)
	require.Equal(t, placement.StateUnusable, p.State())
	assert.True(t, p.Conflict)
	assert.ElementsMatch(t, []string{"backend-b", "backend-c"}, p.ConflictBackends)
	assert.Equal(t, "backend-b", p.Backend)
}

func TestReconciler_FencedConflictSurvivesPlacementSyncFailure(t *testing.T) {
	const leaseUUID = "lease-fenced-conflict-error"
	store := &errorPlacementStore{}
	require.NoError(t, store.Confirm(leaseUUID, "backend-a"))
	store.setErr = errors.New("placement disk unavailable")
	store.conflictFencedOnError = map[string]struct{}{leaseUUID: {}}

	chainClient := &chaintest.MockClient{GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
		return []billingtypes.Lease{{
			Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
		}}, nil
	}}
	backendA := &mockReconcilerBackend{name: "backend-a", provisions: []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID, Status: backend.ProvisionStatusReady,
	}}}
	backendB := &mockReconcilerBackend{name: "backend-b", provisions: []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID, Status: backend.ProvisionStatusReady,
	}}}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: backendA, IsDefault: true}, {Backend: backendB},
	}})
	require.NoError(t, err)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, newMockInFlightTracker(nil), store)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.Contains(t, r.placementAbsenceUntrusted, leaseUUID,
		"a revision-fenced conflict must remain lease-local safety evidence even when another placement write fails")
	assert.NotContains(t, r.placementAbsenceUntrusted, "unrelated-lease")
}

func TestReconciler_CallbackCompletionDuringInventoryDefersOnlyThatLease(t *testing.T) {
	const (
		completedLease   = "lease-callback-completed"
		independentLease = "lease-independent"
	)

	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: completedLease, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: independentLease, Tenant: "tenant-b", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	tracker := newMockInFlightTracker(nil)
	generation, tracked := tracker.TryTrackInFlightWithOperationID(
		completedLease, "tenant-a", testItems("sku-1"), "backend-a",
	)
	require.True(t, tracked)

	var callsMu sync.Mutex
	provisionCalls := 0
	backendClient := &mockConcurrencyBackend{
		name: "backend-a",
		onListProvisions: func() {
			// The chain snapshot above is already PENDING. Model a success callback
			// completing its exact generation while the fleet inventory is in progress;
			// placement-disabled deployments have no revision backstop, so the boundary
			// tracker snapshot must carry this lease through the rest of the sweep.
			if _, claimed := tracker.TryClaimInFlight(completedLease, generation); claimed {
				tracker.FinishClaimedInFlight(completedLease, generation)
			}
		},
		onProvision: func() {
			callsMu.Lock()
			provisionCalls++
			callsMu.Unlock()
		},
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, tracker, nil)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	callsMu.Lock()
	gotProvisionCalls := provisionCalls
	callsMu.Unlock()
	assert.Equal(t, 1, gotProvisionCalls,
		"the independent lease should progress, but the callback-completed lease must wait for a newer snapshot")
	assert.False(t, tracker.IsInFlight(completedLease))
	assert.True(t, tracker.IsInFlight(independentLease))
}

func TestReconciler_OperationCompletionDuringChainSnapshotDefersDestructiveAction(t *testing.T) {
	const (
		completedLease   = "lease-completed-during-chain-read"
		independentLease = "lease-independent"
		backendName      = "backend-a"
	)
	tracker := newMockInFlightTracker(nil)
	var rejectedMu sync.Mutex
	var rejected []string
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			generation, tracked := tracker.TryTrackInFlightWithOperationID(
				completedLease, "tenant-a", testItems("sku-1"), backendName,
			)
			require.True(t, tracked)
			_, claimed := tracker.TryClaimInFlight(completedLease, generation)
			require.True(t, claimed)
			require.True(t, tracker.FinishClaimedInFlight(completedLease, generation))
			return []billingtypes.Lease{
				{Uuid: completedLease, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: independentLease, Tenant: "tenant-b", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		RejectLeasesFunc: func(_ context.Context, leaseUUIDs []string, _ string) (uint64, []string, error) {
			rejectedMu.Lock()
			rejected = append(rejected, leaseUUIDs...)
			rejectedMu.Unlock()
			return uint64(len(leaseUUIDs)), []string{"tx"}, nil
		},
	}
	b := &mockReconcilerBackend{name: backendName, provisions: []backend.ProvisionInfo{
		{LeaseUUID: completedLease, BackendName: backendName, Status: backend.ProvisionStatusFailed},
		{LeaseUUID: independentLease, BackendName: backendName, Status: backend.ProvisionStatusFailed},
	}}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: b, IsDefault: true}},
	})
	require.NoError(t, err)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, tracker, nil)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	rejectedMu.Lock()
	gotRejected := append([]string(nil), rejected...)
	rejectedMu.Unlock()
	assert.Equal(t, []string{independentLease}, gotRejected,
		"a completed post-boundary operation must invalidate stale Failed status only for its lease")
}

func TestReconciler_PostBoundaryPlacementRevisionDefersOnlyThatLease(t *testing.T) {
	const (
		completedLease   = "lease-post-boundary"
		independentLease = "lease-independent"
		backendName      = "backend-a"
	)

	placements, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, placements.Close()) })
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: completedLease, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: independentLease, Tenant: "tenant-b", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	tracker := newMockInFlightTracker(nil)
	hookErr := make(chan error, 1)
	recordHookErr := func(err error) {
		select {
		case hookErr <- err:
		default:
		}
	}
	var callsMu sync.Mutex
	provisionCalls := 0
	backendClient := &mockConcurrencyBackend{
		name: backendName,
		onListProvisions: func() {
			// This complete operation starts after both causal-boundary snapshots and
			// finishes before inventory returns. Its confirmed revision is therefore
			// newer than the inventory even though no tracker entry remains for workers.
			generation, tracked := tracker.TryTrackInFlightWithOperationID(
				completedLease, "tenant-a", testItems("sku-1"), backendName,
			)
			if !tracked {
				recordHookErr(errors.New("failed to track post-boundary operation"))
				return
			}
			if _, err := placements.SetAttempting(completedLease, backendName); err != nil {
				recordHookErr(fmt.Errorf("set post-boundary attempt: %w", err))
				return
			}
			if err := placements.Confirm(completedLease, backendName); err != nil {
				recordHookErr(fmt.Errorf("confirm post-boundary operation: %w", err))
				return
			}
			if _, claimed := tracker.TryClaimInFlight(completedLease, generation); !claimed {
				recordHookErr(errors.New("failed to claim post-boundary operation"))
				return
			}
			if !tracker.FinishClaimedInFlight(completedLease, generation) {
				recordHookErr(errors.New("failed to finish post-boundary operation"))
			}
		},
		onProvision: func() {
			callsMu.Lock()
			provisionCalls++
			callsMu.Unlock()
		},
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
	})
	require.NoError(t, err)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, tracker, placements)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	select {
	case err := <-hookErr:
		require.NoError(t, err)
	default:
	}
	callsMu.Lock()
	gotProvisionCalls := provisionCalls
	callsMu.Unlock()
	assert.Equal(t, 1, gotProvisionCalls,
		"the independent lease should progress, but the post-boundary lease must wait for a newer snapshot")
	assert.False(t, tracker.IsInFlight(completedLease))
	assert.True(t, tracker.IsInFlight(independentLease))
	p := placements.Lookup(completedLease)
	assert.Equal(t, placement.StateConfirmed, p.State())
	assert.Equal(t, backendName, p.Backend)
}

func TestReconciler_PostSyncPlacementRevisionDefersBeforeLifecycleAction(t *testing.T) {
	const (
		targetLease      = "lease-post-sync-mutation"
		independentLease = "lease-independent"
		backendName      = "backend-a"
	)

	realStore, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, realStore.Close()) })
	require.NoError(t, realStore.Confirm(targetLease, backendName))
	store := &mutateAfterSnapshotListStore{
		PlacementStore: realStore,
		targetLease:    targetLease,
		targetBackend:  backendName,
	}
	acknowledgeCalls := 0
	ack := &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
		acknowledgeCalls++
		return true, "tx", nil
	}}
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: targetLease, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: independentLease, Tenant: "tenant-b", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	backendClient := &mockReconcilerBackend{name: backendName, provisions: []backend.ProvisionInfo{{
		LeaseUUID: targetLease, Status: backend.ProvisionStatusReady,
	}}}
	silentBackend := &mockReconcilerBackend{
		name:    "backend-silent",
		listErr: errors.New("backend-silent unavailable"),
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: backendClient, IsDefault: true},
			{Backend: silentBackend},
		},
	})
	require.NoError(t, err)
	tracker := newMockInFlightTracker(nil)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, ack, router, tracker, store)
	require.NoError(t, err)
	// The intentionally partial sweep still has prior durable authority for an
	// absent independent lease; this keeps the test focused on the post-sync
	// target mutation rather than the ordinary incomplete-inventory gate.
	r.placementSweepSeen.Store(true)

	require.NoError(t, r.ReconcileAll(t.Context()))
	store.mu.Lock()
	mutationErr := store.mutationErr
	mutationDone := store.mutationDone
	store.mu.Unlock()
	require.NoError(t, mutationErr)
	require.True(t, mutationDone)

	backendClient.mu.Lock()
	provisionCalls := append([]backend.ProvisionRequest(nil), backendClient.provisionCalls...)
	backendClient.mu.Unlock()
	require.Len(t, provisionCalls, 1,
		"the revision-mutated lease must defer while the independent lease progresses")
	assert.Equal(t, independentLease, provisionCalls[0].LeaseUUID)
	assert.Zero(t, acknowledgeCalls,
		"the ready target must not be acknowledged from a snapshot older than its placement revision")
	p := realStore.Lookup(targetLease)
	assert.Equal(t, placement.StateConfirmed, p.State(),
		"adding an attempt preserves the derived state, so only the opaque revision catches this race")
	assert.Equal(t, backendName, p.Attempt)
}

func TestReconciler_ReconcileAll_SyncsPlacementsFromBackends(t *testing.T) {
	// Setup: Two backends each with provisions. Placement store should be
	// synced with SetBatch from actual backend state.
	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
				{Uuid: "lease-2", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}

	b1 := &mockReconcilerBackend{
		name: "backend-1",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
		},
	}
	b2 := &mockReconcilerBackend{
		name: "backend-2",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-2", Status: backend.ProvisionStatusReady},
		},
	}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: b1, IsDefault: true},
			{Backend: b2, Match: backend.MatchCriteria{SKUs: []string{"b2-sku"}}},
		},
	})

	ps := &mockPlacementStore{}

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify placements were synced from backend state
	assert.Equal(t, "backend-1", ps.Get("lease-1"))
	assert.Equal(t, "backend-2", ps.Get("lease-2"))
	assert.Equal(t, 2, ps.Count())
}

func TestReconciler_ReconcileAll_StartProvisioning_RecordsPlacement(t *testing.T) {
	// Setup: Pending lease, not provisioned. After provisioning, placement should be recorded.
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	mb := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})

	ps := &mockPlacementStore{}

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify placement was recorded after provisioning
	assert.Equal(t, "test", ps.Get("lease-1"))
}

func TestReconciler_ReconcileAll_RejectLease_LeavesPlacementForGatedPruner(t *testing.T) {
	// Setup: Pending lease with a failed provision. Rejection must not eagerly
	// delete its placement: this sweep still positively reports backend state for
	// the lease, and only the revision-gated pruner may remove the record after a
	// later inventory proves it absent from both provisions and retentions.
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mb := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailed},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})

	ps := &mockPlacementStore{}
	ps.Set("lease-1", "test")

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	assert.Equal(t, "test", ps.Get("lease-1"),
		"rejection must leave placement for the retention-aware, revision-gated pruner")
}

func TestReconciler_ReconcileAll_OrphanDeprovision_CleansUpPlacement(t *testing.T) {
	// Setup: No lease on chain, provisioned on backend (orphan). The backend has no
	// retention for this lease, so the placement should eventually be pruned.
	//
	// ENG-333: processOrphan no longer eagerly deletes placement. The gated pruner
	// (cleanupOrphanedPlacements) is the sole owner. However, backendLeases is built
	// from the pre-sweep snapshot (allProvisions ∪ allRetentions), so the orphan's
	// lease UUID is present in backendLeases even after it is deprovisioned this
	// sweep. The pruner therefore keeps the placement this sweep (gate b: "still on
	// backend" = true in snapshot). It will be pruned on the NEXT sweep, when the
	// backend no longer reports the provision and there is no retention.
	// See TestReconciler_ReconcileAll_RetainedOrphan_KeepsPlacement for the retained-
	// lease case that ENG-333 was specifically designed to protect.
	mockChain := &chaintest.MockClient{}
	mb := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "orphan-1", Status: backend.ProvisionStatusReady, BackendName: "test"},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})

	ps := &mockPlacementStore{}
	ps.Set("orphan-1", "test")

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// ENG-333: placement is NOT eagerly deleted by processOrphan anymore. It
	// survives this sweep because the pre-sweep backendLeases snapshot still
	// contains orphan-1 (it was in allProvisions at the start of RunOnce).
	// The gated pruner will remove it on the next sweep once the backend no
	// longer reports the provision and there is no retention.
	assert.Equal(t, "test", ps.Get("orphan-1"), "placement survives the orphan-deprovision sweep (gated pruner owns deletion, ENG-333)")
}

func TestReconciler_ReconcileAll_CloseLease_CleansUpPlacement(t *testing.T) {
	// Setup: Active lease, failed provision exhausted retries. closeLease is called.
	//
	// ENG-333: cleanupTerminalLease (called by closeLease) no longer eagerly
	// deletes placement. The gated pruner is the sole owner. In this sweep:
	//   - backendLeases snapshot contains lease-1 (it was in allProvisions)
	//   - chainLeases snapshot contains lease-1 as ACTIVE
	// So the pruner keeps placement this sweep (both gate a and the chain-terminal
	// gate protect it). It will be pruned on the NEXT sweep, once the chain
	// reports the lease as closed/terminal and the backend no longer lists it.
	// See TestReconciler_ReconcileAll_RetainedOrphan_KeepsPlacement for the
	// retained-lease case that ENG-333 was specifically designed to protect.
	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mb := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{
				LeaseUUID:   "lease-1",
				Status:      backend.ProvisionStatusFailed,
				FailCount:   3,
				BackendName: "test",
			},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})

	ps := &mockPlacementStore{}
	ps.Set("lease-1", "test")

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// ENG-333: placement is NOT eagerly deleted by cleanupTerminalLease anymore.
	// It survives this sweep because the pre-sweep snapshots (backendLeases from
	// allProvisions, chainLeases from chain) still contain lease-1. The gated
	// pruner will remove it on the next sweep once the chain reports it as
	// terminal and the backend no longer lists it (and no retention exists).
	assert.Equal(t, "test", ps.Get("lease-1"), "placement survives the close-lease sweep (gated pruner owns deletion, ENG-333)")
}

// TestReconciler_ReconcileAll_RetainedOrphan_KeepsPlacement verifies the
// core ENG-333 invariant: when an orphan provision is deprovisioned and the
// backend retains its data (RetainOnClose pool), the placement record must
// survive the reconcile sweep so that a restore request can resolve the
// correct backend.
//
// lease-ret is an orphan (provisioned, absent from chain) that the backend
// ALSO retains. After the fix, processOrphan no longer eager-deletes its
// placement, so the gated pruner sees it: it is in backendLeases (here via
// BOTH allProvisions and allRetentions, since the mock's Deprovision is a
// no-op that does not remove the entry from m.provisions), so gate (b) keeps
// it. Pre-fix, processOrphan deleted the placement before the pruner ran and
// this assertion would FAIL — that is what this test guards against. (The
// retention-only gate-(b) path — placement kept because the lease is in
// allRetentions but NOT allProvisions — is isolated separately by
// TestReconciler_PrunesOrphanedPlacement / the Task 9 prune tests.)
func TestReconciler_ReconcileAll_RetainedOrphan_KeepsPlacement(t *testing.T) {
	// Setup: lease-ret is an orphan (no chain lease) but the backend retains it.
	mockChain := &chaintest.MockClient{}
	mb := &mockReconcilerBackend{
		name: "backend-a",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-ret", Status: backend.ProvisionStatusReady, BackendName: "backend-a"},
		},
		// Simulate a RetainOnClose backend: Deprovision soft-deletes the data and
		// ListRetentions returns the lease UUID to signal "data still here".
		retentions: []backend.RetainedLease{
			{LeaseUUID: "lease-ret"},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})

	ps := &mockPlacementStore{}
	ps.Set("lease-ret", "backend-a")

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// The placement MUST survive: the backend retained the lease's data, so
	// restore affinity must remain intact (ENG-333). If this assertion fails,
	// the eager delete was re-introduced or the gated pruner has a gate gap.
	assert.Equal(t, "backend-a", ps.Get("lease-ret"),
		"retained lease must keep its placement after orphan deprovision (ENG-333)")
}

// --- Gap tests: fetchAllProvisions, payload lifecycle, RefreshState ---

// TestDeferLease is the exhaustive table for the one safety decision ENG-356
// introduces. It is a pure function precisely so this can cover every
// combination without standing up a fleet.
//
// The asymmetry worth remembering while reading it: deferring is never
// destructive — it skips work — so over-deferring costs latency, while
// under-deferring costs a tenant their data.
func TestDeferLease(t *testing.T) {
	t.Parallel()

	snap := func(complete bool, answered map[string]bool) fleetSnapshot {
		return fleetSnapshot{provisions: nil, answered: answered, complete: complete}
	}

	tests := []struct {
		name            string
		snap            fleetSnapshot
		retentions      answeredSet
		isProvisioned   bool
		reportedBackend string
		placement       placement.Placement
		absenceTrusted  bool
		wantDefer       bool
		why             string
	}{
		{
			name: "complete sweep proceeds even with no placement",
			snap: snap(true, map[string]bool{"a": true}),
			why:  "a complete sweep saw every backend, so absence IS evidence",
		},
		{
			name:      "complete configured sweep still defers an unconfigured confirmed owner",
			snap:      snap(true, map[string]bool{"a": true}),
			placement: placement.Placement{Backend: "removed-backend"},
			wantDefer: true,
			why:       "fleet completeness cannot account for a recorded owner outside the configured set",
		},
		{
			name:          "complete sweep proceeds for a provisioned lease",
			snap:          snap(true, map[string]bool{"a": true}),
			isProvisioned: true,
		},
		{
			name:            "incomplete sweep proceeds when the lease was reported",
			snap:            snap(false, map[string]bool{"a": true, "b": false}),
			isProvisioned:   true,
			reportedBackend: "a",
			why:             "a backend that answered reported it; that is positive evidence",
		},
		{
			name:            "report from a different backend does not override a silent confirmed owner",
			snap:            snap(false, map[string]bool{"a": false, "b": true}),
			isProvisioned:   true,
			reportedBackend: "b",
			placement:       placement.Placement{Backend: "a"},
			wantDefer:       true,
			why:             "the B report cannot prove the durable A owner absent",
		},
		{
			name:            "matching report proceeds for a confirmed owner",
			snap:            snap(false, map[string]bool{"a": true, "b": false}),
			isProvisioned:   true,
			reportedBackend: "a",
			placement:       placement.Placement{Backend: "a"},
			why:             "the positively reporting backend matches durable affinity",
		},
		{
			name:          "reported lease with durable conflict still defers",
			snap:          snap(false, map[string]bool{"a": true, "b": false}),
			isProvisioned: true,
			placement:     placement.Placement{Conflict: true},
			wantDefer:     true,
			why:           "one positive status cannot resolve a known multi-backend ownership conflict",
		},
		{
			name:          "reported lease with unresolved attempt still defers",
			snap:          snap(false, map[string]bool{"a": true, "b": false}),
			isProvisioned: true,
			placement:     placement.Placement{Attempt: "b"},
			wantDefer:     true,
			why:           "a report from one backend cannot settle a different attempted owner",
		},
		{
			name:      "incomplete sweep proceeds when the owning backend answered",
			snap:      snap(false, map[string]bool{"a": true, "b": false}),
			placement: placement.Placement{Backend: "a"},
			wantDefer: false,
			why:       "backend a answered and did not report it, so it really is absent from a",
		},
		{
			name:       "confirmed owner provision absence defers when retention inventory is silent",
			snap:       snap(false, map[string]bool{"a": true, "b": false}),
			retentions: answeredSet{"a": false, "b": true},
			placement:  placement.Placement{Backend: "a"},
			wantDefer:  true,
			why:        "negative provision evidence cannot rule out retained data while the exact owner's retention inventory is unavailable",
		},
		{
			name:            "matching positive provision does not require retention absence",
			snap:            snap(false, map[string]bool{"a": true, "b": false}),
			retentions:      answeredSet{"a": false, "b": true},
			isProvisioned:   true,
			reportedBackend: "a",
			placement:       placement.Placement{Backend: "a"},
			why:             "a fresh matching positive provision does not depend on negative retention evidence",
		},
		{
			name:      "incomplete sweep DEFERS when the owning backend is silent",
			snap:      snap(false, map[string]bool{"a": true, "b": false}),
			placement: placement.Placement{Backend: "b"},
			wantDefer: true,
			why:       "the core feature: the lease may be live on b, unseen",
		},
		{
			name:      "incomplete sweep DEFERS an unplaced lease",
			snap:      snap(false, map[string]bool{"a": true, "b": false}),
			wantDefer: true,
			why:       "no record means fred cannot rule out that it lives on the silent backend",
		},
		{
			name:           "incomplete sweep proceeds for absent record after startup trust latch",
			snap:           snap(false, map[string]bool{"a": true, "b": false}),
			absenceTrusted: true,
			why:            "a prior complete durable sync makes record absence meaningful",
		},
		{
			name:      "attempt defers even when its backend answered until it is settled",
			snap:      snap(false, map[string]bool{"a": true, "b": false}),
			placement: placement.Placement{Attempt: "a"},
			wantDefer: true,
			why:       "attempt is an execution gate, not an affinity hint",
		},
		{
			name:      "incomplete sweep DEFERS a lease placed on an unconfigured backend",
			snap:      snap(false, map[string]bool{"a": true}),
			placement: placement.Placement{Backend: "removed-backend"},
			wantDefer: true,
			why:       "absent from `answered` entirely — the ENG-635 rule, reached via the same guard",
		},
		{
			name:      "every backend silent DEFERS everything unevidenced",
			snap:      snap(false, map[string]bool{"a": false, "b": false}),
			placement: placement.Placement{Backend: "a"},
			wantDefer: true,
			why:       "the degenerate end of the same axis, not a special case",
		},
		{
			name:          "every backend silent still proceeds for an evidenced lease",
			snap:          snap(false, map[string]bool{"a": false, "b": false}),
			isProvisioned: true,
			why:           "impossible in practice, but the rule is evidence-based, not fleet-based",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			retentions := tc.retentions
			if retentions == nil {
				retentions = tc.snap.answered
			}
			got := deferLease(
				tc.snap, retentions,
				tc.isProvisioned, tc.reportedBackend, tc.placement, tc.absenceTrusted,
			)
			assert.Equal(t, tc.wantDefer, got, tc.why)
		})
	}
}

func TestResolvePlacementAttempt_InventorySilenceNeverSettlesAttempt(t *testing.T) {
	t.Parallel()

	newStore := func(t *testing.T) *placement.Store {
		t.Helper()
		store, err := placement.NewStore(t.TempDir() + "/placements.db")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		return store
	}
	absentSnapshot := fleetSnapshot{
		answered:          answeredSet{"backend-a": true},
		reportedByBackend: map[string]map[string]struct{}{"backend-a": {}},
		complete:          true,
	}

	t.Run("attempt older than complete snapshot remains sticky", func(t *testing.T) {
		store := newStore(t)
		requireSetPlacementAttempt(t, store, "lease-1", "backend-a")
		cutoff := store.SnapshotRevision()
		r := &Reconciler{placementView: store, legacyPlacement: store}

		require.False(t, r.resolvePlacementAttempt(
			"lease-1", store.Lookup("lease-1"), absentSnapshot, cutoff, nil,
			answeredSet{"backend-a": true}, nil,
		))
		require.Equal(t, placement.StateAttempting, store.Lookup("lease-1").State())
	})

	t.Run("attempt newer than snapshot survives", func(t *testing.T) {
		store := newStore(t)
		cutoff := store.SnapshotRevision()
		requireSetPlacementAttempt(t, store, "lease-1", "backend-a")
		r := &Reconciler{placementView: store, legacyPlacement: store}

		require.False(t, r.resolvePlacementAttempt(
			"lease-1", store.Lookup("lease-1"), absentSnapshot, cutoff, nil,
			answeredSet{"backend-a": true}, nil,
		))
		require.Equal(t, placement.StateAttempting, store.Lookup("lease-1").State())
	})

	t.Run("positive report never clears attempt", func(t *testing.T) {
		store := newStore(t)
		requireSetPlacementAttempt(t, store, "lease-1", "backend-a")
		cutoff := store.SnapshotRevision()
		snap := absentSnapshot
		snap.reportedByBackend = map[string]map[string]struct{}{
			"backend-a": {"lease-1": {}},
		}
		r := &Reconciler{placementView: store, legacyPlacement: store}

		require.False(t, r.resolvePlacementAttempt(
			"lease-1", store.Lookup("lease-1"), snap, cutoff, nil,
			answeredSet{"backend-a": true}, nil,
		))
		require.Equal(t, placement.StateAttempting, store.Lookup("lease-1").State())
	})

	t.Run("call in flight when inventory began survives after untrack", func(t *testing.T) {
		store := newStore(t)
		requireSetPlacementAttempt(t, store, "lease-1", "backend-a")
		cutoff := store.SnapshotRevision()
		r := &Reconciler{placementView: store, legacyPlacement: store}

		require.False(t, r.resolvePlacementAttempt(
			"lease-1", store.Lookup("lease-1"), absentSnapshot, cutoff,
			map[string]struct{}{"lease-1": {}}, answeredSet{"backend-a": true}, nil,
		))
		require.Equal(t, placement.StateAttempting, store.Lookup("lease-1").State(),
			"an inventory that may predate the outbound call cannot disprove it")
	})

	t.Run("failed retention inventory cannot prove attempt absent", func(t *testing.T) {
		store := newStore(t)
		requireSetPlacementAttempt(t, store, "lease-1", "backend-a")
		cutoff := store.SnapshotRevision()
		r := &Reconciler{placementView: store, legacyPlacement: store}

		require.False(t, r.resolvePlacementAttempt(
			"lease-1", store.Lookup("lease-1"), absentSnapshot, cutoff, nil,
			answeredSet{"backend-a": false}, nil,
		))
		require.Equal(t, placement.StateAttempting, store.Lookup("lease-1").State())
	})

	t.Run("positive retention report never clears attempt", func(t *testing.T) {
		store := newStore(t)
		requireSetPlacementAttempt(t, store, "lease-1", "backend-a")
		cutoff := store.SnapshotRevision()
		r := &Reconciler{placementView: store, legacyPlacement: store}

		require.False(t, r.resolvePlacementAttempt(
			"lease-1", store.Lookup("lease-1"), absentSnapshot, cutoff, nil,
			answeredSet{"backend-a": true},
			map[string]map[string]struct{}{"backend-a": {"lease-1": {}}},
		))
		require.Equal(t, placement.StateAttempting, store.Lookup("lease-1").State())
	})

	t.Run("attempt settlement never invokes the legacy clear path", func(t *testing.T) {
		store := newStore(t)
		requireSetPlacementAttempt(t, store, "lease-1", "backend-a")
		cutoff := store.SnapshotRevision()
		blocking := &blockingClearPlacementStore{
			PlacementStore: store,
			entered:        make(chan struct{}),
			release:        make(chan struct{}),
		}
		r := &Reconciler{placementView: blocking, legacyPlacement: blocking}

		require.False(t, r.resolvePlacementAttempt(
			"lease-1", store.Lookup("lease-1"), absentSnapshot, cutoff, nil,
			answeredSet{"backend-a": true}, nil,
		))
		select {
		case <-blocking.entered:
			t.Fatal("inventory silence reached the placement clear path")
		default:
		}
		require.Equal(t, placement.StateAttempting, store.Lookup("lease-1").State())
	})
}

func TestReconciler_AmbiguousAttemptsNeverReachLegacyClearWorkers(t *testing.T) {
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: "lease-2", Tenant: "tenant-b", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}
	b := &mockReconcilerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: b, IsDefault: true}},
	})
	require.NoError(t, err)
	placements := &mockPlacementStore{}
	requireSetPlacementAttempt(t, placements, "lease-1", "backend-a")
	requireSetPlacementAttempt(t, placements, "lease-2", "backend-a")
	blocking := &concurrentClearPlacementStore{
		PlacementStore: placements,
		entered:        make(chan string, 2),
		release:        make(chan struct{}),
	}
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback", MaxWorkers: 2,
	}, chainClient, noopAck, router, newMockInFlightTracker(nil), blocking)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	select {
	case leaseUUID := <-blocking.entered:
		t.Fatalf("inventory silence attempted to clear sticky operation for %s", leaseUUID)
	default:
	}
	require.Equal(t, placement.StateAttempting, placements.Lookup("lease-1").State())
	require.Equal(t, placement.StateAttempting, placements.Lookup("lease-2").State())
}

// TestClassifyLease is the exhaustive table for the other safety decision the
// reconciler makes, and the reason classifyLease is pure. Read it alongside
// TestDeferLease: that one decides whether the sweep may act on a lease at all,
// this one decides whether the chain has positively said the lease is finished.
//
// Only leaseTerminal authorises destroying state. Everything else — including a
// lease the chain has never heard of — costs a cycle of cleanup latency, which
// is the cheap side of the trade (ENG-654).
func TestClassifyLease(t *testing.T) {
	t.Parallel()

	lease := func(state billingtypes.LeaseState) *billingtypes.Lease {
		return &billingtypes.Lease{Uuid: "lease-1", State: state}
	}

	tests := []struct {
		name         string
		lease        *billingtypes.Lease
		err          error
		wantLiveness leaseLiveness
		wantReason   string
		why          string
	}{
		{
			name:         "query error is not absence",
			lease:        nil,
			err:          assert.AnError,
			wantLiveness: leaseUnknown,
			wantReason:   metrics.CleanupSkipChainError,
			why:          "an unreachable chain says nothing about the lease",
		},
		{
			name:         "error alongside a lease still classifies as error",
			lease:        lease(billingtypes.LEASE_STATE_CLOSED),
			err:          assert.AnError,
			wantLiveness: leaseUnknown,
			wantReason:   metrics.CleanupSkipChainError,
			why:          "a partial result from a failed call is not evidence",
		},
		{
			name:         "no record is NOT closed",
			lease:        nil,
			wantLiveness: leaseUnknown,
			wantReason:   metrics.CleanupSkipChainUnknown,
			why:          "x/billing never deletes a lease, so absence means the chain never knew it",
		},
		{
			name:         "pending is live",
			lease:        lease(billingtypes.LEASE_STATE_PENDING),
			wantLiveness: leaseLive,
			wantReason:   metrics.CleanupSkipChainLive,
			why:          "the sweep's snapshot was stale; the main loop owns this lease",
		},
		{
			name:         "active is live",
			lease:        lease(billingtypes.LEASE_STATE_ACTIVE),
			wantLiveness: leaseLive,
			wantReason:   metrics.CleanupSkipChainLive,
		},
		{
			name:         "unspecified is unknown, not terminal",
			lease:        lease(billingtypes.LEASE_STATE_UNSPECIFIED),
			wantLiveness: leaseUnknown,
			wantReason:   metrics.CleanupSkipChainUnknownState,
			why:          "the zero value should never appear on chain; do not guess",
		},
		{
			name:         "a state this build has never heard of is unknown",
			lease:        lease(billingtypes.LeaseState(99)),
			wantLiveness: leaseUnknown,
			wantReason:   metrics.CleanupSkipChainUnknownState,
			why: "LeaseState is a bare int32 decoded as a raw varint with no validation, so a state " +
				"added to the chain after this binary shipped arrives as an unrecognized number. " +
				"Terminality must be an allowlist or fred would reap live leases the day the chain grows one",
		},
		{
			name:         "closed is terminal",
			lease:        lease(billingtypes.LEASE_STATE_CLOSED),
			wantLiveness: leaseTerminal,
		},
		{
			name:         "rejected is terminal",
			lease:        lease(billingtypes.LEASE_STATE_REJECTED),
			wantLiveness: leaseTerminal,
		},
		{
			name:         "expired is terminal",
			lease:        lease(billingtypes.LEASE_STATE_EXPIRED),
			wantLiveness: leaseTerminal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotLiveness, gotReason := classifyLease(tc.lease, tc.err)
			assert.Equal(t, tc.wantLiveness, gotLiveness, tc.why)
			assert.Equal(t, tc.wantReason, gotReason)
		})
	}
}

// TestReconciler_ReconcileAll_OrphanRecheck covers the three ways the chain can
// decline to confirm an orphan candidate. Each must leave the provision alone
// and count itself; none may reach Deprovision.
//
// Deliberately NOT parallel: it reads process-global Prometheus collectors.
func TestReconciler_ReconcileAll_OrphanRecheck(t *testing.T) {
	tests := []struct {
		name       string
		getLease   func(context.Context, string) (*billingtypes.Lease, error)
		wantReason string
		why        string
	}{
		{
			name: "chain reports the lease live",
			getLease: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
				return chaintest.NewMockLease(uuid, "tenant-1", "provider-1", billingtypes.LEASE_STATE_ACTIVE), nil
			},
			wantReason: metrics.CleanupSkipChainLive,
			why:        "the sweep's two list queries are not atomic; this lease was created between them",
		},
		{
			name: "chain has no record of the lease",
			getLease: func(_ context.Context, _ string) (*billingtypes.Lease, error) {
				return nil, nil
			},
			wantReason: metrics.CleanupSkipChainUnknown,
			why:        "absence is not evidence: a wrong or reset chain must not deprovision the fleet",
		},
		{
			name: "chain reports a state this build cannot classify",
			getLease: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
				l := chaintest.NewMockLease(uuid, "tenant-1", "provider-1", billingtypes.LeaseState(99))
				return l, nil
			},
			wantReason: metrics.CleanupSkipChainUnknownState,
			why:        "a chain newer than this fred build must not read as a fleet full of terminal leases",
		},
		{
			name: "chain query fails",
			getLease: func(_ context.Context, _ string) (*billingtypes.Lease, error) {
				return nil, assert.AnError
			},
			wantReason: metrics.CleanupSkipChainError,
			why:        "cleanup gates fail open; retry on the next sweep",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockChain := &chaintest.MockClient{GetLeaseFunc: tc.getLease}
			mockBackend := &mockReconcilerBackend{
				name: "test",
				provisions: []backend.ProvisionInfo{
					{LeaseUUID: "candidate", Status: backend.ProvisionStatusReady, BackendName: "test"},
				},
			}
			router, _ := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
			})

			reconciler, err := newReconciler(ReconcilerConfig{
				ProviderUUID:    "provider-1",
				CallbackBaseURL: "http://localhost:8080",
			}, mockChain, noopAck, router, nil, nil)
			require.NoError(t, err)

			skips := metrics.ReconcilerCleanupSkipsTotal.WithLabelValues(metrics.CleanupPassOrphan, tc.wantReason)
			before := promtestutil.ToFloat64(skips)
			require.NoError(t, reconciler.ReconcileAll(t.Context()))
			after := promtestutil.ToFloat64(skips)

			mockBackend.mu.Lock()
			defer mockBackend.mu.Unlock()
			assert.Empty(t, mockBackend.deprovisionCalls, tc.why)
			assert.Equal(t, 1.0, after-before, "the skip must be counted under reason %q", tc.wantReason)
		})
	}
}

// TestReconciler_ConfirmTerminal_BoundsTheChainLookup pins the liveness half of
// the re-check: the per-candidate lookup carries its own deadline.
//
// The reconcile context is the process lifetime, and neither the chain client
// nor gRPC bounds a query on its own, so an unanswered Lease RPC would otherwise
// stall the sweep holding it — and because ReconcileAll is CAS-guarded, every
// later tick would no-op behind it. The whole fail-open design (count, keep,
// retry next sweep) presumes the call returns.
//
// Asserting the deadline is present is deliberate: waiting out a real expiry
// would cost chainConfirmTimeout per run, and what matters is that the bound
// exists, not how long it is. The second half then pins that an expiry is
// classified as chain_error rather than as evidence of anything.
//
// Deliberately NOT parallel: it reads a process-global Prometheus collector.
func TestReconciler_ConfirmTerminal_BoundsTheChainLookup(t *testing.T) {
	var (
		mu          sync.Mutex
		sawDeadline bool
		budget      time.Duration
	)

	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, _ string) (*billingtypes.Lease, error) {
			deadline, ok := ctx.Deadline()
			mu.Lock()
			sawDeadline = ok
			if ok {
				budget = time.Until(deadline)
			}
			mu.Unlock()
			// Answer as the RPC layer would once the budget is spent.
			return nil, context.DeadlineExceeded
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "candidate", Status: backend.ProvisionStatusReady, BackendName: "test"},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	skips := metrics.ReconcilerCleanupSkipsTotal.
		WithLabelValues(metrics.CleanupPassOrphan, metrics.CleanupSkipChainError)
	before := promtestutil.ToFloat64(skips)

	// t.Context() has no deadline of its own, so any deadline the lookup sees is
	// one confirmTerminal imposed.
	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	mu.Lock()
	defer mu.Unlock()
	assert.True(t, sawDeadline, "the per-candidate chain lookup must carry a deadline")
	assert.Positive(t, budget)
	assert.LessOrEqual(t, budget, chainConfirmTimeout, "the budget must be the one confirmTerminal set")

	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.deprovisionCalls, "a timed-out lookup is not evidence the lease finished")
	assert.Equal(t, 1.0, promtestutil.ToFloat64(skips)-before, "expiry must count as chain_error")
}

// TestReconciler_CleanupOrphanedPayloads_ChainErrorKeepsPayload is the payload
// pass's error branch. A payload deleted out from under a live lease makes the
// NEXT sweep classify errPayloadNotAvailable as permanent and close a healthy
// ACTIVE lease on chain, so an unreachable chain must keep the payload.
func TestReconciler_CleanupOrphanedPayloads_ChainErrorKeepsPayload(t *testing.T) {
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: t.TempDir() + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	require.True(t, payloadStore.Store("absent-lease", []byte("manifest-bytes")))

	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(_ context.Context, _ string) (*billingtypes.Lease, error) {
			return nil, assert.AnError
		},
	}
	mockBackend := &mockReconcilerBackend{name: "test"}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, newMockInFlightTracker(payloadStore), nil)
	require.NoError(t, err)

	skips := metrics.ReconcilerCleanupSkipsTotal.WithLabelValues(metrics.CleanupPassPayload, metrics.CleanupSkipChainError)
	before := promtestutil.ToFloat64(skips)
	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	after := promtestutil.ToFloat64(skips)

	has, err := payloadStore.Has("absent-lease")
	require.NoError(t, err)
	assert.True(t, has, "a payload must survive a chain fred could not reach")
	assert.Equal(t, 1.0, after-before)
}

func TestReconciler_ReconcileAll_ListProvisionError_DefersWithoutAborting(t *testing.T) {
	// ENG-356 inverted this test. A backend failing ListProvisions used to abort
	// the ENTIRE sweep; it now marks that backend unanswered and defers only the
	// leases fred cannot positively place. With a single backend configured, the
	// whole fleet is unanswered, so the deferral covers everything — which is
	// exactly the old behavior's effect, reached deliberately rather than by
	// abandoning the sweep.
	//
	// What must NOT change is the safety property: the lease is neither
	// re-provisioned (it may be live on the unreachable backend) nor torn down.
	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:    "test",
		listErr: errors.New("docker daemon unavailable"),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	ctx := t.Context()
	require.NoError(t, reconciler.ReconcileAll(ctx),
		"an unreachable backend no longer fails the sweep")

	// The safety property, unchanged: nothing was provisioned onto a peer and
	// nothing was deprovisioned on the strength of data fred could not see.
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
	assert.Empty(t, mockBackend.deprovisionCalls)
}

func TestReconciler_ReconcileAll_ActiveFailedPayloadNotAvailable_Closes(t *testing.T) {
	// ACTIVE lease with failed provision (FailCount < max). Re-provisioning
	// requires a payload (MetaHash is set), but the payload store is empty.
	// errPayloadNotAvailable is a permanent failure — the lease should be closed.
	var closedLeases []string
	var closedReason string
	var mu sync.Mutex

	payloadHash := sha256.Sum256([]byte("some manifest"))

	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_ACTIVE,
					MetaHash: payloadHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			closedReason = reason
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{
				LeaseUUID:   "lease-1",
				Status:      backend.ProvisionStatusFailed,
				FailCount:   1, // Below max — would normally re-provision
				BackendName: "test",
			},
		},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Empty payload store — no payload available for re-provisioning
	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	tracker := newMockInFlightTracker(payloadStore)

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify the lease was closed (permanent failure, not transient)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, closedLeases, 1)
	assert.Equal(t, "lease-1", closedLeases[0])
	assert.Contains(t, closedReason, "payload not available")

	// Verify no provisioning was attempted (error happened before Provision call)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestReconciler_ReconcileAll_PendingWithMetaHash_NoPayload_Waits(t *testing.T) {
	// PENDING lease with MetaHash but the payload hasn't been uploaded yet.
	// The reconciler should do nothing — no provisioning, no rejection.
	// It just waits for the tenant to upload the payload.
	var rejectedLeases []string
	var mu sync.Mutex

	payloadHash := sha256.Sum256([]byte("future manifest"))

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_PENDING,
					MetaHash: payloadHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 0, nil, errors.New("should not be called")
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Not provisioned yet
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Empty payload store — payload not yet uploaded
	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	defer payloadStore.Close()

	tracker := newMockInFlightTracker(payloadStore)

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify no actions were taken — waiting for payload upload
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "should not provision without payload")
	assert.Empty(t, mockBackend.deprovisionCalls, "should not deprovision")

	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, rejectedLeases, "should not reject — lease is just waiting for payload")
}

// mustHasPayload calls Has() and fails the test on a read error.
func mustHasPayload(t *testing.T, store *payload.Store, leaseUUID string) bool {
	t.Helper()
	has, err := store.Has(leaseUUID)
	require.NoError(t, err)
	return has
}

// mutatePayloadDB applies fn to a CLOSED payload store's bbolt file.
//
// It exists to build on-disk states the store's own API deliberately cannot
// produce: bit-rot in the payload bucket, and a database written before the
// payload_hashes bucket existed (ENG-619). The bucket names below are the
// store's on-disk format, not part of its Go API. bbolt is single-writer per
// file, so the store must be closed before calling this.
func mutatePayloadDB(t *testing.T, dbPath string, fn func(tx *bolt.Tx) error) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Update(fn))
}

// newReconcilerPayloadStore opens a payload store under a stable path so a test
// can close it, mutate the file, and reopen it.
func newReconcilerPayloadStore(t *testing.T, dbPath string) *payload.Store {
	t.Helper()
	store, err := payload.NewStore(payload.StoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	return store
}

func TestReconciler_ReconcileAll_PayloadCorruption_DeletesCorruptPayload(t *testing.T) {
	// Real bit-rot: the stored payload bytes change while the hash recorded
	// beside them does not. That is what the separate payload_hashes bucket
	// exists to catch — a checksum living inside the record it describes would
	// rot along with it. The reconciler should:
	// 1. Delete the corrupt payload from the store
	// 2. NOT call Provision (hash check happens before)
	// 3. Treat it as an error (transient — payload needs re-upload)
	payloadData := []byte("original manifest payload")
	payloadHash := sha256.Sum256(payloadData)

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_PENDING,
					MetaHash: payloadHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	dbPath := t.TempDir() + "/payloads.db"
	store := newReconcilerPayloadStore(t, dbPath)
	require.True(t, store.Store("lease-1", payloadData))
	require.NoError(t, store.Close())

	// Rot the payload, leave its recorded hash alone.
	mutatePayloadDB(t, dbPath, func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("payloads")).Put([]byte("lease-1"), []byte("corrupted on disk"))
	})

	payloadStore := newReconcilerPayloadStore(t, dbPath)
	defer payloadStore.Close()
	require.True(t, mustHasPayload(t, payloadStore, "lease-1"))

	tracker := newMockInFlightTracker(payloadStore)

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	// ReconcileAll succeeds even with per-lease errors
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify corrupt payload was deleted from store
	assert.False(t, mustHasPayload(t, payloadStore, "lease-1"), "corrupt payload should be deleted from store")

	// Verify no provisioning was attempted (hash check happens before Provision)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestReconciler_ReconcileAll_UpdatedPayload_ProvisionsAndKeepsIt(t *testing.T) {
	// ENG-619, the whole point: a tenant /update replaces the stored manifest
	// but CANNOT change the lease's on-chain MetaHash, which is set once at
	// creation. Verifying an updated payload against MetaHash would read a
	// successful update as corruption, delete the payload, and then close the
	// ACTIVE lease on-chain — strictly worse than the silent revert it replaced.
	originalPayload := []byte("original manifest payload")
	updatedPayload := []byte("updated manifest payload with a new image")
	metaHash := sha256.Sum256(originalPayload)
	updatedHash := sha256.Sum256(updatedPayload)

	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_ACTIVE,
					MetaHash: metaHash[:], // still names the CREATE-time manifest
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // ACTIVE + unprovisioned ⇒ reprovision
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	payloadStore := newReconcilerPayloadStore(t, t.TempDir()+"/payloads.db")
	defer payloadStore.Close()
	require.True(t, payloadStore.Store("lease-1", originalPayload))
	require.NoError(t, payloadStore.Put("lease-1", updatedPayload)) // the /update

	tracker := newMockInFlightTracker(payloadStore)

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	// The updated payload survives — no delete, no lease closure.
	assert.True(t, mustHasPayload(t, payloadStore, "lease-1"), "an updated payload must not be treated as corruption")

	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.provisionCalls, 1, "the lease should be reprovisioned")
	req := mockBackend.provisionCalls[0]
	assert.Equal(t, updatedPayload, req.Payload, "reprovision must replay the UPDATED manifest, not the create-time one")
	assert.Equal(t, hex.EncodeToString(updatedHash[:]), req.PayloadHash,
		"payload_hash must describe the payload actually sent")
}

func TestReconciler_ReconcileAll_LegacyPayloadWithoutRecordedHash_UsesMetaHash(t *testing.T) {
	// A payload written before the payload_hashes bucket existed has no recorded
	// hash. It must fall back to the on-chain MetaHash rather than be read as a
	// mismatch — treating absence as corruption would delete every pre-upgrade
	// payload on the first sweep after a deploy.
	payloadData := []byte("legacy manifest payload")
	payloadHash := sha256.Sum256(payloadData)

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_PENDING,
					MetaHash: payloadHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{name: "test", provisions: []backend.ProvisionInfo{}}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	dbPath := t.TempDir() + "/payloads.db"
	store := newReconcilerPayloadStore(t, dbPath)
	require.True(t, store.Store("lease-1", payloadData))
	require.NoError(t, store.Close())

	// Drop the recorded hash, keeping the payload: a pre-ENG-619 database.
	mutatePayloadDB(t, dbPath, func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("payload_hashes")).Delete([]byte("lease-1"))
	})

	payloadStore := newReconcilerPayloadStore(t, dbPath)
	defer payloadStore.Close()

	tracker := newMockInFlightTracker(payloadStore)
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	assert.True(t, mustHasPayload(t, payloadStore, "lease-1"), "a legacy payload matching MetaHash must be kept")

	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	require.Len(t, mockBackend.provisionCalls, 1)
	assert.Equal(t, hex.EncodeToString(payloadHash[:]), mockBackend.provisionCalls[0].PayloadHash,
		"with no recorded hash the on-chain MetaHash is still the reference")
}

func TestReconciler_ReconcileAll_LegacyPayloadMismatchingMetaHash_IsDeleted(t *testing.T) {
	// The error branch of the fallback: a legacy payload that does NOT match
	// MetaHash is still corruption and must still be deleted.
	payloadData := []byte("legacy manifest payload")
	wrongHash := sha256.Sum256([]byte("different payload entirely"))

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_PENDING,
					MetaHash: wrongHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{name: "test", provisions: []backend.ProvisionInfo{}}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	dbPath := t.TempDir() + "/payloads.db"
	store := newReconcilerPayloadStore(t, dbPath)
	require.True(t, store.Store("lease-1", payloadData))
	require.NoError(t, store.Close())

	mutatePayloadDB(t, dbPath, func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("payload_hashes")).Delete([]byte("lease-1"))
	})

	payloadStore := newReconcilerPayloadStore(t, dbPath)
	defer payloadStore.Close()

	tracker := newMockInFlightTracker(payloadStore)
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	assert.False(t, mustHasPayload(t, payloadStore, "lease-1"), "a legacy payload that fails MetaHash is still corrupt")

	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestReconciler_ReconcileAll_PayloadStoreGetError_TransientError(t *testing.T) {
	// When PayloadStore.Get() returns a database error (not nil payload),
	// the reconciler should treat it as a transient error — NOT close the lease.
	// This prevents a disk hiccup from permanently terminating active leases.
	//
	// Uses ACTIVE + not provisioned (anomaly) to bypass HasPayload and go
	// directly to startProvisioningWithPayload → doStartProvisioning → Get().
	var closedLeases []string
	var mu sync.Mutex

	payloadData := []byte("test manifest")
	payloadHash := sha256.Sum256(payloadData)

	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_ACTIVE,
					MetaHash: payloadHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Not provisioned (anomaly path)
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	// Create payload store, store data, then close it to force Get() errors
	tmpDir := t.TempDir()
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: tmpDir + "/payloads.db",
	})
	require.NoError(t, err)
	payloadStore.Store("lease-1", payloadData)
	require.NoError(t, payloadStore.Close()) // Force "database not open" on Get()

	tracker := newMockInFlightTracker(payloadStore)

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	// ReconcileAll succeeds even with per-lease errors
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify lease was NOT closed (transient error, should retry next cycle)
	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, closedLeases, "DB error is transient — lease should NOT be closed")

	// Verify no provisioning was attempted (error aborted before Provision call)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
}

func TestReconciler_ReconcileAll_RefreshStateError_ContinuesWithStaleData(t *testing.T) {
	// When RefreshState returns an error, stale positive membership remains
	// conservative ownership evidence, but its status is deferred.
	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name: "test",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
		},
		refreshErr: errors.New("docker daemon busy"),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	placements := &mockPlacementStore{}
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, newMockInFlightTracker(nil), placements)
	require.NoError(t, err)

	ctx := t.Context()
	err = reconciler.ReconcileAll(ctx)
	assert.NoError(t, err, "should succeed despite RefreshState error")

	// Verify no unnecessary actions (ACTIVE + Ready → healthy with stale data)
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls)
	assert.Empty(t, mockBackend.deprovisionCalls)
	assert.Equal(t, "test", placements.Lookup("lease-1").Backend,
		"a stale positive with no conflicting durable fact remains conservative affinity")
}

func TestReconciler_PartialInventoryCannotReplaceSilentConfirmedOwner(t *testing.T) {
	var rejected, acknowledged int
	lease := billingtypes.Lease{
		Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING,
	}
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{lease}, nil
		},
		RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
			rejected++
			return 1, []string{"tx"}, nil
		},
	}
	ack := &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
		acknowledged++
		return true, "tx", nil
	}}
	backendA := &mockReconcilerBackend{
		name:    "backend-a",
		listErr: errors.New("backend-a unavailable"),
	}
	backendB := &mockReconcilerBackend{
		name: "backend-b",
		provisions: []backend.ProvisionInfo{{
			LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailed, FailCount: 100,
		}},
	}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: backendA, IsDefault: true}, {Backend: backendB},
	}})
	require.NoError(t, err)
	placements := &mockPlacementStore{}
	require.NoError(t, placements.Set("lease-1", "backend-a"))
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, ack, router, newMockInFlightTracker(nil), placements)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.Zero(t, rejected)
	assert.Zero(t, acknowledged)
	assert.Equal(t, "backend-a", placements.Lookup("lease-1").Backend,
		"a report from B cannot replace confirmed A until A authoritatively reports absence")
	backendB.mu.Lock()
	assert.Empty(t, backendB.provisionCalls)
	backendB.mu.Unlock()
}

func TestReconciler_PositiveDifferentBackendQuarantinesStickyAttempt(t *testing.T) {
	lease := billingtypes.Lease{
		Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE,
	}
	chainClient := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{lease}, nil
		},
	}
	backendA := &mockReconcilerBackend{name: "backend-a"}
	backendB := &mockReconcilerBackend{
		name: "backend-b",
		provisions: []backend.ProvisionInfo{{
			LeaseUUID: "lease-1", Status: backend.ProvisionStatusProvisioning,
		}},
	}
	backendC := &mockReconcilerBackend{name: "backend-c"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: backendA, IsDefault: true}, {Backend: backendB}, {Backend: backendC},
	}})
	require.NoError(t, err)
	placements, err := placement.NewStore(t.TempDir() + "/placements.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, placements.Close()) })
	requireSetPlacementAttempt(t, placements, "lease-1", "backend-a")
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, newMockInFlightTracker(nil), placements)
	require.NoError(t, err)

	// Even a complete inventory is not ordered after the outbound call against A.
	// A positive B is therefore contradictory evidence, not permission to move
	// affinity or erase the exact pending operation.
	require.NoError(t, r.ReconcileAll(t.Context()))
	p := placements.Lookup("lease-1")
	require.Equal(t, placement.StateUnusable, p.State())
	require.True(t, p.Conflict)
	require.ElementsMatch(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	require.Equal(t, "backend-a", p.Attempt)
	require.True(t, r.placementSweepSeen.Load())

	backendB.mu.Lock()
	backendB.listErr = errors.New("backend-b unavailable")
	backendB.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))

	backendA.mu.Lock()
	assert.Empty(t, backendA.provisionCalls)
	backendA.mu.Unlock()
	backendC.mu.Lock()
	assert.Empty(t, backendC.provisionCalls,
		"a contradictory positive must remain quarantined after its reporter goes silent")
	backendC.mu.Unlock()
}

func TestReconciler_RefreshFailureCannotActOnStaleFailedStatus(t *testing.T) {
	for _, state := range []billingtypes.LeaseState{
		billingtypes.LEASE_STATE_PENDING,
		billingtypes.LEASE_STATE_ACTIVE,
	} {
		t.Run(state.String(), func(t *testing.T) {
			var rejected, closed int
			lease := billingtypes.Lease{Uuid: "lease-1", Tenant: "tenant-1", State: state}
			chainClient := &chaintest.MockClient{
				GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
					if state == billingtypes.LEASE_STATE_PENDING {
						return []billingtypes.Lease{lease}, nil
					}
					return nil, nil
				},
				GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
					if state == billingtypes.LEASE_STATE_ACTIVE {
						return []billingtypes.Lease{lease}, nil
					}
					return nil, nil
				},
				RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
					rejected++
					return 1, []string{"tx"}, nil
				},
				CloseLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
					closed++
					return 1, []string{"tx"}, nil
				},
			}
			backendClient := &mockReconcilerBackend{
				name:       "backend-a",
				refreshErr: errors.New("refresh failed"),
				provisions: []backend.ProvisionInfo{{
					LeaseUUID: "lease-1",
					Status:    backend.ProvisionStatusFailed,
					FailCount: 100,
				}},
			}
			router, err := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: backendClient, IsDefault: true}},
			})
			require.NoError(t, err)
			r, err := newReconciler(ReconcilerConfig{
				ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
			}, chainClient, noopAck, router, nil, nil)
			require.NoError(t, err)

			require.NoError(t, r.ReconcileAll(t.Context()))
			assert.Zero(t, rejected)
			assert.Zero(t, closed)
			backendClient.mu.Lock()
			assert.Empty(t, backendClient.provisionCalls)
			backendClient.mu.Unlock()
		})
	}
}

func TestReconciler_DuplicateBackendOwnersDeferAndPersistQuarantine(t *testing.T) {
	var rejected, acked int
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING,
			}}, nil
		},
		RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
			rejected++
			return 1, []string{"tx"}, nil
		},
	}
	ack := &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
		acked++
		return true, "tx", nil
	}}
	backendA := &mockReconcilerBackend{name: "backend-a", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady,
	}}}
	backendB := &mockReconcilerBackend{name: "backend-b", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailed, FailCount: 100,
	}}}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: backendA, IsDefault: true}, {Backend: backendB},
	}})
	require.NoError(t, err)
	placements := &mockPlacementStore{}
	require.NoError(t, placements.Set("lease-1", "backend-a"))
	tracker := newMockInFlightTracker(nil)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, ack, router, tracker, placements)
	require.NoError(t, err)

	for range 10 {
		require.NoError(t, r.ReconcileAll(t.Context()))
	}
	assert.Zero(t, rejected)
	assert.Zero(t, acked)
	p := placements.Lookup("lease-1")
	assert.Equal(t, placement.StateUnusable, p.State())
	assert.True(t, p.Conflict, "contradictory positive owners must create a durable quarantine")
	assert.Equal(t, "backend-a", p.Backend,
		"quarantine must preserve the last confirmed owner instead of erasing evidence")
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
}

func TestReconciler_DuplicateOwnerQuarantineSurvivesPartialNextSweep(t *testing.T) {
	pendingCalls := 0
	chainClient := &chaintest.MockClient{GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
		pendingCalls++
		leases := []billingtypes.Lease{{
			Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING,
		}}
		if pendingCalls > 1 {
			leases = append(leases, billingtypes.Lease{
				Uuid: "lease-independent", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_PENDING,
			})
		}
		return leases, nil
	}}
	backendA := &mockReconcilerBackend{name: "backend-a", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady,
	}}}
	backendB := &mockReconcilerBackend{name: "backend-b", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady,
	}}}
	backendC := &mockReconcilerBackend{name: "backend-c"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: backendA}, {Backend: backendB}, {Backend: backendC, IsDefault: true},
	}})
	require.NoError(t, err)
	placements := &mockPlacementStore{}
	tracker := newMockInFlightTracker(nil)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, tracker, placements)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.True(t, r.placementSweepSeen.Load(),
		"a durable lease-local conflict must not suppress absence trust for the rest of the fleet")
	assert.True(t, placements.Lookup("lease-1").Conflict)

	backendA.mu.Lock()
	backendA.listErr = errors.New("backend-a unavailable")
	backendA.mu.Unlock()
	backendB.mu.Lock()
	backendB.listErr = errors.New("backend-b unavailable")
	backendB.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))

	backendC.mu.Lock()
	defer backendC.mu.Unlock()
	require.Len(t, backendC.provisionCalls, 1)
	assert.Equal(t, "lease-independent", backendC.provisionCalls[0].LeaseUUID,
		"the unrelated recordless lease must progress while the durable conflict remains quarantined")
	assert.True(t, placements.Lookup("lease-1").Conflict)
}

func TestReconciler_DurableDuplicateQuarantineSurvivesRestart(t *testing.T) {
	var rejected int
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING,
			}}, nil
		},
		RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
			rejected++
			return 1, []string{"tx"}, nil
		},
	}
	dbPath := t.TempDir() + "/placements.db"
	placements, err := placement.NewStore(dbPath)
	require.NoError(t, err)
	backendA := &mockReconcilerBackend{name: "backend-a", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady,
	}}}
	backendB := &mockReconcilerBackend{name: "backend-b", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady,
	}}}
	backendC := &mockReconcilerBackend{name: "backend-c"}
	newRouter := func(t *testing.T) BackendRouter {
		t.Helper()
		router, routerErr := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
			{Backend: backendA, IsDefault: true}, {Backend: backendB}, {Backend: backendC},
		}})
		require.NoError(t, routerErr)
		return router
	}
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, newRouter(t), newMockInFlightTracker(nil), placements)
	require.NoError(t, err)
	require.NoError(t, r.ReconcileAll(t.Context()))
	require.True(t, placements.Lookup("lease-1").Conflict)
	require.NoError(t, placements.Close())

	placements, err = placement.NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = placements.Close() })
	backendA.mu.Lock()
	backendA.provisions[0].Status = backend.ProvisionStatusFailed
	backendA.provisions[0].FailCount = 100
	backendA.mu.Unlock()
	backendB.mu.Lock()
	backendB.listErr = errors.New("backend-b unavailable")
	backendB.mu.Unlock()
	r, err = newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, newRouter(t), newMockInFlightTracker(nil), placements)
	require.NoError(t, err)
	require.NoError(t, r.ReconcileAll(t.Context()))

	assert.Zero(t, rejected, "one post-restart status cannot resolve a durable ownership conflict")
	assert.True(t, placements.Lookup("lease-1").Conflict)
	backendC.mu.Lock()
	defer backendC.mu.Unlock()
	assert.Empty(t, backendC.provisionCalls)
}

func TestReconciler_DurableConflictDoesNotResolveAgainstReducedConfiguration(t *testing.T) {
	var acked int
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING,
			}}, nil
		},
	}
	ack := &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
		acked++
		return true, "tx", nil
	}}
	backendA := &mockReconcilerBackend{name: "backend-a", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady,
	}}}
	backendB := &mockReconcilerBackend{name: "backend-b", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady,
	}}}

	dbPath := t.TempDir() + "/placements.db"
	placements, err := placement.NewStore(dbPath)
	require.NoError(t, err)
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: backendA, IsDefault: true}, {Backend: backendB},
	}})
	require.NoError(t, err)
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, ack, router, newMockInFlightTracker(nil), placements)
	require.NoError(t, err)
	require.NoError(t, r.ReconcileAll(t.Context()))
	p := placements.Lookup("lease-1")
	require.True(t, p.Conflict)
	require.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	require.NoError(t, placements.Close())

	// Simulate a restart after backend-a was removed from configuration. Seeing
	// backend-b alone is not a complete view of the durable conflict: backend-a
	// remains a possible owner and must not vanish with the config entry.
	placements, err = placement.NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = placements.Close() })
	reducedRouter, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: backendB, IsDefault: true},
	}})
	require.NoError(t, err)
	r, err = newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, ack, reducedRouter, newMockInFlightTracker(nil), placements)
	require.NoError(t, err)
	require.NoError(t, r.ReconcileAll(t.Context()))

	p = placements.Lookup("lease-1")
	assert.True(t, p.Conflict)
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.Zero(t, acked, "a reduced router must not manufacture a unique owner")

	// Complete absence from the reduced router is equally insufficient: the
	// removed candidate never answered either inventory, so the quarantine must
	// not be cleared and the PENDING lease must not be provisioned afresh.
	backendB.mu.Lock()
	backendB.provisions = nil
	backendB.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	p = placements.Lookup("lease-1")
	assert.True(t, p.Conflict)
	backendB.mu.Lock()
	assert.Empty(t, backendB.provisionCalls)
	backendB.mu.Unlock()
}

func TestReconciler_InventoryCannotAutoRepairUnreadablePlacementRecord(t *testing.T) {
	const leaseUUID = "lease-corrupt-placement"
	corruptRecord := []byte{0xff, 0xfe}
	dbPath := t.TempDir() + "/placements.db"
	store, err := placement.NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("placements")).Put([]byte(leaseUUID), corruptRecord)
	}))
	require.NoError(t, db.Close())
	store, err = placement.NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		if store != nil {
			require.NoError(t, store.Close())
		}
	})
	require.Equal(t, placement.StateUnusable, store.Lookup(leaseUUID).State())
	initialRevision := store.SnapshotRevision()

	chainClient := &chaintest.MockClient{GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
		return []billingtypes.Lease{{
			Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
		}}, nil
	}}
	owner := &mockReconcilerBackend{name: "backend-a", provisions: []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID, Status: backend.ProvisionStatusReady,
	}}}
	silentPeer := &mockReconcilerBackend{name: "backend-b", listErr: errors.New("backend-b unavailable")}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: owner, IsDefault: true}, {Backend: silentPeer},
	}})
	require.NoError(t, err)
	acknowledged := 0
	ack := &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
		acknowledged++
		return true, "tx", nil
	}}
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, ack, router, newMockInFlightTracker(nil), store)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.Equal(t, placement.StateUnusable, store.Lookup(leaseUUID).State(),
		"one reporter cannot erase the only durable evidence that a historical owner is unknown")
	assert.Zero(t, acknowledged, "an unreadable placement must remain fail-closed")

	silentPeer.mu.Lock()
	silentPeer.listErr = nil
	silentPeer.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.Equal(t, placement.StateUnusable, store.Lookup(leaseUUID).State(),
		"even complete current-fleet inventory cannot prove the unknown historical owner absent")
	assert.Equal(t, initialRevision, store.SnapshotRevision(),
		"operator-only quarantine must not be rewritten by reconciliation")
	assert.Zero(t, acknowledged)

	require.NoError(t, store.Close())
	store = nil
	db, err = bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	var persisted []byte
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		persisted = append([]byte(nil), tx.Bucket([]byte("placements")).Get([]byte(leaseUUID))...)
		return nil
	}))
	assert.Equal(t, corruptRecord, persisted,
		"reconciliation must preserve the unreadable bytes for operator diagnosis/repair")
}

func TestReconciler_DuplicateQuarantinePersistsWhenPeerGoesSilent(t *testing.T) {
	chainClient := &chaintest.MockClient{GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
		return []billingtypes.Lease{{
			Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING,
		}}, nil
	}}
	backendA := &mockReconcilerBackend{name: "backend-a", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-1", Status: backend.ProvisionStatusProvisioning,
	}}}
	backendB := &mockReconcilerBackend{name: "backend-b", provisions: []backend.ProvisionInfo{{
		LeaseUUID: "lease-1", Status: backend.ProvisionStatusProvisioning,
	}}}
	backendC := &mockReconcilerBackend{name: "backend-c"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: backendA, IsDefault: true}, {Backend: backendB}, {Backend: backendC},
	}})
	require.NoError(t, err)
	placements := &mockPlacementStore{}
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, newMockInFlightTracker(nil), placements)
	require.NoError(t, err)
	require.NoError(t, r.ReconcileAll(t.Context()))
	require.True(t, placements.Lookup("lease-1").Conflict)

	backendB.mu.Lock()
	backendB.provisions = nil
	backendB.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	p := placements.Lookup("lease-1")
	require.Equal(t, placement.StateUnusable, p.State())
	require.True(t, p.Conflict,
		"B's silence cannot prove that its earlier positive ownership or a delayed mutation is gone")
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)

	backendA.mu.Lock()
	backendA.listErr = errors.New("backend-a unavailable")
	backendA.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))
	backendB.mu.Lock()
	defer backendB.mu.Unlock()
	backendC.mu.Lock()
	defer backendC.mu.Unlock()
	assert.Empty(t, backendB.provisionCalls)
	assert.Empty(t, backendC.provisionCalls,
		"a durable conflict must not be substituted when one candidate later goes silent")
}

func TestReconciler_DuplicateRetentionQuarantinePersistsWhenPeerGoesSilent(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a", retentions: []backend.RetainedLease{{
		LeaseUUID: "lease-1",
	}}}
	backendB := &mockReconcilerBackend{name: "backend-b", retentions: []backend.RetainedLease{{
		LeaseUUID: "lease-1",
	}}}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: backendA, IsDefault: true}, {Backend: backendB},
	}})
	require.NoError(t, err)
	placements := &mockPlacementStore{}
	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, &chaintest.MockClient{}, noopAck, router, newMockInFlightTracker(nil), placements)
	require.NoError(t, err)

	require.NoError(t, r.ReconcileAll(t.Context()))
	p := placements.Lookup("lease-1")
	require.True(t, p.Conflict)
	require.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)

	backendB.mu.Lock()
	backendB.retentions = nil
	backendB.mu.Unlock()
	require.NoError(t, r.ReconcileAll(t.Context()))

	p = placements.Lookup("lease-1")
	require.Equal(t, placement.StateUnusable, p.State())
	assert.True(t, p.Conflict,
		"a complete inventory response cannot turn B's missing retention into causal non-execution proof")
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
}

func TestReconciler_RefreshFailureCannotClearAttemptOrArmTrust(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING,
			}}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "backend-a",
		refreshErr: errors.New("refresh failed"),
		provisions: nil, // stale empty cache is not proof of absence
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})
	require.NoError(t, err)
	placements := &mockPlacementStore{}
	requireSetPlacementAttempt(t, placements, "lease-1", "backend-a")
	tracker := newMockInFlightTracker(nil)

	r, err := newReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, mockChain, noopAck, router, tracker, placements)
	require.NoError(t, err)
	require.NoError(t, r.ReconcileAll(t.Context()))

	assert.False(t, r.placementSweepSeen.Load(), "failed refresh must keep the startup trust latch disarmed")
	assert.Equal(t, placement.StateAttempting, placements.Lookup("lease-1").State())
	mockBackend.mu.Lock()
	assert.Empty(t, mockBackend.provisionCalls, "stale negative inventory must not permit a retry")
	mockBackend.mu.Unlock()
}

func TestReconciler_ReconcileAll_HasPayloadError_CountsAsError(t *testing.T) {
	// When tracker.HasPayload() returns a database error, the reconciler
	// should not attempt provisioning and should count it as a lease error.
	// It should NOT reject the lease (the error is transient, not permanent).
	var rejectedLeases []string
	var mu sync.Mutex

	payloadHash := sha256.Sum256([]byte("some manifest"))

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_PENDING,
					MetaHash: payloadHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			defer mu.Unlock()
			rejectedLeases = append(rejectedLeases, leaseUUIDs...)
			return uint64(len(leaseUUIDs)), []string{"tx-hash"}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // Not provisioned
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tracker := newMockInFlightTracker(nil) // no payload store needed
	tracker.hasPayloadErr = errors.New("disk I/O error")

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	ctx := t.Context()
	// ReconcileAll succeeds even with per-lease errors
	assert.NoError(t, reconciler.ReconcileAll(ctx))

	// Verify no provisioning was attempted
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "should not provision when HasPayload errors")

	// Verify lease was NOT rejected (transient error, not permanent)
	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, rejectedLeases, "should not reject — HasPayload error is transient")
}

func TestReconciler_ReconcileAll_SetsLastSuccessTimestamp(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{},
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp)

	err = reconciler.ReconcileAll(t.Context())
	require.NoError(t, err)

	after := promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp)
	assert.Greater(t, after, before, "ReconcilerLastSuccessTimestamp should be updated after reconciliation")
	assert.Greater(t, after, float64(0), "ReconcilerLastSuccessTimestamp should be a positive unix timestamp")
}

func TestReconciler_CycleCompletenessRequiresInventoriesAndProjection(t *testing.T) {
	// Deliberately not parallel: this test verifies process-global Prometheus
	// collectors through before/after deltas.
	tests := []struct {
		name       string
		newBackend func() backend.Backend
		newStore   func() PlacementStore
	}{
		{
			name: "retention inventory outage",
			newBackend: func() backend.Backend {
				return &retentionErrorReconcilerBackend{
					mockReconcilerBackend: &mockReconcilerBackend{name: "backend-a"},
					err:                   errors.New("retentions unavailable"),
				}
			},
			newStore: func() PlacementStore { return &mockPlacementStore{} },
		},
		{
			name:       "placement projection write failure",
			newBackend: func() backend.Backend { return &mockReconcilerBackend{name: "backend-a"} },
			newStore: func() PlacementStore {
				return &failNextBatchPlacementStore{
					PlacementStore: &mockPlacementStore{},
					failErr:        errors.New("placement projection unavailable"),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chainClient := &chaintest.MockClient{}
			router, err := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: tt.newBackend(), IsDefault: true}},
			})
			require.NoError(t, err)
			r, err := newReconciler(ReconcilerConfig{
				ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
			}, chainClient, noopAck, router, newMockInFlightTracker(nil), tt.newStore())
			require.NoError(t, err)

			const previousSuccess = 123
			metrics.ReconcilerLastSuccessTimestamp.Set(previousSuccess)
			metrics.ReconcilerSweepComplete.Set(1)
			degraded := metrics.ReconciliationTotal.WithLabelValues(metrics.OutcomeDegraded)
			success := metrics.ReconciliationTotal.WithLabelValues(metrics.OutcomeSuccess)
			degradedBefore := promtestutil.ToFloat64(degraded)
			successBefore := promtestutil.ToFloat64(success)

			require.NoError(t, r.ReconcileAll(t.Context()))

			assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.ReconcilerSweepComplete),
				"an incomplete or unpersisted fleet projection must disarm sweep_complete")
			assert.Equal(t, float64(previousSuccess),
				promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp),
				"an incomplete or unpersisted fleet projection must not advance last-success")
			assert.Equal(t, degradedBefore+1, promtestutil.ToFloat64(degraded))
			assert.Equal(t, successBefore, promtestutil.ToFloat64(success))
		})
	}
}

func TestReconciler_InsufficientResources_IncrementsMetric(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-cap", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
					Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}}},
			}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:         "cap-backend",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: fmt.Errorf("no room: %w", backend.ErrInsufficientResources),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tracker := newMockInFlightTracker(nil)
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.BackendInsufficientResourcesTotal.WithLabelValues("cap-backend"))

	_ = reconciler.ReconcileAll(t.Context())

	after := promtestutil.ToFloat64(metrics.BackendInsufficientResourcesTotal.WithLabelValues("cap-backend"))
	assert.Equal(t, 1.0, after-before, "BackendInsufficientResourcesTotal should increment by 1 for reconciler path")
}

func TestReconciler_ReconcileAll_PartialFailureDoesNotUpdateTimestamp(t *testing.T) {
	// When a lease errors during provisioning the outcome is "partial",
	// and ReconcilerLastSuccessTimestamp must NOT be updated.
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-fail", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
					Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}}},
			}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:         "test",
		provisions:   []backend.ProvisionInfo{},
		provisionErr: errors.New("backend exploded"),
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	tracker := newMockInFlightTracker(nil)
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp)

	_ = reconciler.ReconcileAll(t.Context())

	after := promtestutil.ToFloat64(metrics.ReconcilerLastSuccessTimestamp)
	assert.Equal(t, before, after, "ReconcilerLastSuccessTimestamp should NOT be updated on partial failure")
}

// panickingBackend wraps mockReconcilerBackend to inject a panic into
// RefreshState for panic-recovery regression tests. Used only by the
// tests below.
type panickingBackend struct {
	*mockReconcilerBackend
	panicOnRefresh bool
}

func (p *panickingBackend) RefreshState(ctx context.Context) error {
	if p.panicOnRefresh {
		panic("synthetic RefreshState panic")
	}
	return p.mockReconcilerBackend.RefreshState(ctx)
}

// TestReconciler_FetchPanicDoesNotCrashFred pins the invariant that a
// panic inside a per-backend fetch goroutine (fetchAllProvisions) is
// recovered instead of propagating up and killing the fred process.
// Asserts: ReconcilerPanicsTotal{stage="fetch_provisions"} increments,
// reconcile returns an error, and other backends are unaffected.
func TestReconciler_FetchPanicDoesNotCrashFred(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
	}

	goodBackend := &mockReconcilerBackend{name: "good"}
	badBackend := &panickingBackend{
		mockReconcilerBackend: &mockReconcilerBackend{name: "bad"},
		panicOnRefresh:        true,
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: goodBackend, IsDefault: true},
			{Backend: badBackend},
		},
	})

	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return true, "tx", nil
		},
	}
	mockTracker := newMockInFlightTracker(nil)
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, ack, router, mockTracker, nil)
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.ReconcilerPanicsTotal.WithLabelValues("fetch_provisions"))

	// Must not crash fred, and — since ENG-356 — must not fail the sweep either.
	// A panicking backend is just one that did not answer: it is marked
	// unanswered and its leases are deferred, exactly like a backend that
	// returned an error or timed out. Treating a panic as a fleet-wide abort was
	// the same coupling this change removed for every other failure mode.
	assert.NoError(t, reconciler.ReconcileAll(t.Context()),
		"a panicking backend must degrade the sweep, not abort it")

	after := promtestutil.ToFloat64(metrics.ReconcilerPanicsTotal.WithLabelValues("fetch_provisions"))
	assert.Equal(t, before+1, after,
		"ReconcilerPanicsTotal{fetch_provisions} must increment by 1")

	// The healthy sibling backend must still have been queried despite
	// the bad one panicking — errgroup concurrency means the panic in
	// one task does not short-circuit the others.
	goodBackend.mu.Lock()
	goodListCalls := goodBackend.listProvisionsCalls
	goodBackend.mu.Unlock()
	assert.GreaterOrEqual(t, goodListCalls, 1,
		"healthy backend's ListProvisions must have been called despite sibling panic")
}

// TestReconciler_ProcessLeasePanicDoesNotCrashFred: a panic inside
// processLease (via the tracker's HasPayload call) must be recovered,
// counted, and NOT crash fred. Other leases in the same reconcile
// cycle must still be processed.
func TestReconciler_ProcessLeasePanicDoesNotCrashFred(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			// Two leases: one triggers a panic (has MetaHash → calls HasPayload),
			// one proceeds normally (no MetaHash → skips tracker).
			return []billingtypes.Lease{
				{Uuid: "lease-panic", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING, MetaHash: []byte{0x01, 0x02}},
				{Uuid: "lease-ok", Tenant: "tenant-2", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
	}

	mockBackend := &mockReconcilerBackend{name: "test"}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return true, "tx", nil
		},
	}

	mockTracker := newMockInFlightTracker(nil)
	mockTracker.hasPayloadFunc = func(leaseUUID string) (bool, error) {
		if leaseUUID == "lease-panic" {
			panic("synthetic HasPayload panic")
		}
		return false, nil
	}

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, ack, router, mockTracker, nil)
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.ReconcilerPanicsTotal.WithLabelValues("process_lease"))

	// Must not crash fred.
	_ = reconciler.ReconcileAll(t.Context())

	after := promtestutil.ToFloat64(metrics.ReconcilerPanicsTotal.WithLabelValues("process_lease"))
	assert.Equal(t, before+1, after,
		"ReconcilerPanicsTotal{process_lease} must increment by 1")

	// The lease without MetaHash should have been provisioned normally —
	// one panicking lease must not block processing of sibling leases.
	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	var okLeaseProvisioned bool
	for _, call := range mockBackend.provisionCalls {
		if call.LeaseUUID == "lease-ok" {
			okLeaseProvisioned = true
		}
	}
	assert.True(t, okLeaseProvisioned,
		"the non-panicking lease must still be processed; one bad lease must not block others")
}

type controlledReconcilerProvisionBackend struct {
	*mockReconcilerBackend
	provision func(context.Context, backend.ProvisionRequest) error
}

func (controlled *controlledReconcilerProvisionBackend) Provision(
	ctx context.Context,
	request backend.ProvisionRequest,
) error {
	controlled.mu.Lock()
	controlled.provisionCalls = append(controlled.provisionCalls, request)
	controlled.mu.Unlock()
	return controlled.provision(ctx, request)
}

func TestReconcilerTypedInitiationOrdersEventAndFencesCloseDuringCall(t *testing.T) {
	registry := operation.NewRegistry()
	var events []backend.ProvisionStatus
	client := &controlledReconcilerProvisionBackend{
		mockReconcilerBackend: &mockReconcilerBackend{name: "backend-a"},
	}
	client.provision = func(_ context.Context, request backend.ProvisionRequest) error {
		record, exists := registry.Lookup(request.LeaseUUID)
		require.True(t, exists)
		assert.Equal(t, operation.PhaseCalling, record.Phase)
		assert.Equal(t, []backend.ProvisionStatus{backend.ProvisionStatusProvisioning}, events)
		closeClaim := registry.TryClaimDeprovision(request.LeaseUUID, record.ID)
		assert.Equal(t, operation.SettlementBusy, closeClaim.Outcome())
		return nil
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
	})
	require.NoError(t, err)
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil),
		operations:          registry,
	}
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}}, nil
		},
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}
	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
		StartEvents: provisionStartEventSinkFunc(func(string) {
			events = append(events, backend.ProvisionStatusProvisioning)
		}),
	}, chainClient, noopAck, router, runtime, store)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(context.Background()))
	record, exists := registry.Lookup("lease-1")
	require.True(t, exists)
	assert.Equal(t, operation.PhaseActive, record.Phase)
	assert.Equal(t, []backend.ProvisionStatus{backend.ProvisionStatusProvisioning}, events)
}

func TestReconcilerInlineCallbackOverridesLaterSynchronousError(t *testing.T) {
	tests := []struct {
		name        string
		status      backend.CallbackStatus
		wantStatus  backend.ProvisionStatus
		wantAck     int
		wantRejects int
		wantState   placement.State
	}{
		{
			name: "success", status: backend.CallbackStatusSuccess,
			wantStatus: backend.ProvisionStatusReady, wantAck: 1,
			wantState: placement.StateConfirmed,
		},
		{
			name: "failure", status: backend.CallbackStatusFailed,
			wantStatus: backend.ProvisionStatusFailed, wantRejects: 1,
			wantState: placement.StateAbsent,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := operation.NewRegistry()
			store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			var acknowledgeCalls, rejectCalls int
			lease := billingtypes.Lease{
				Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}
			chainClient := &chaintest.MockClient{
				GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
					return []billingtypes.Lease{lease}, nil
				},
				GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
					copy := lease
					return &copy, nil
				},
				RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
					rejectCalls++
					return 1, []string{"tx-reject"}, nil
				},
			}
			events := &callbackEventRecorder{}
			callbacks, err := NewCallbackService(CallbackServiceConfig{
				Operations: registry,
				Chain:      chainClient,
				Placement:  store,
				Acknowledger: callbackAcknowledgerFunc(func(context.Context, string) (bool, string, error) {
					acknowledgeCalls++
					return true, "tx-ack", nil
				}),
				Events: events,
			})
			require.NoError(t, err)

			client := &controlledReconcilerProvisionBackend{
				mockReconcilerBackend: &mockReconcilerBackend{name: "backend-a"},
			}
			client.provision = func(ctx context.Context, request backend.ProvisionRequest) error {
				record, exists := registry.Lookup(request.LeaseUUID)
				if !exists {
					return errors.New("operation missing during backend call")
				}
				command, commandErr := NewCallbackCommand(backend.CallbackPayload{
					LeaseUUID: request.LeaseUUID, Backend: client.Name(), Status: test.status,
					Error: "inline terminal failure", OperationID: record.ID.String(),
				})
				if commandErr != nil {
					return commandErr
				}
				if callbackErr := callbacks.HandleCallback(ctx, command); callbackErr != nil {
					return callbackErr
				}
				return fmt.Errorf("late synchronous response: %w", backend.ErrValidation)
			}
			router, err := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
			})
			require.NoError(t, err)
			runtime := &typedTestReconcilerRuntime{
				mockInFlightTracker: newMockInFlightTracker(nil), operations: registry,
			}
			reconciler, err := NewReconciler(ReconcilerConfig{
				ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
			}, chainClient, noopAck, router, runtime, store)
			require.NoError(t, err)

			require.NoError(t, reconciler.ReconcileAll(t.Context()))
			assert.Equal(t, test.wantAck, acknowledgeCalls)
			assert.Equal(t, test.wantRejects, rejectCalls,
				"the synchronous validation error must not apply chain cleanup after the callback")
			assert.False(t, registry.Contains(lease.Uuid))
			assert.Equal(t, test.wantState, store.Lookup(lease.Uuid).State())
			events.mu.Lock()
			require.Len(t, events.events, 1)
			assert.Equal(t, test.wantStatus, events.events[0].Status)
			events.mu.Unlock()
		})
	}
}

func TestReconcilerBackendProvisionPanicAbortsCallingAndRetainsAttempt(t *testing.T) {
	registry := operation.NewRegistry()
	client := &controlledReconcilerProvisionBackend{
		mockReconcilerBackend: &mockReconcilerBackend{name: "backend-a"},
		provision: func(context.Context, backend.ProvisionRequest) error {
			panic("backend implementation fault")
		},
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
	})
	require.NoError(t, err)
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	lease := billingtypes.Lease{
		Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{lease}, nil
		},
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			copy := lease
			return &copy, nil
		},
	}
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil), operations: registry,
	}
	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, runtime, store)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.False(t, registry.Contains(lease.Uuid),
		"a backend panic must not strand the reconciler operation in Calling")
	record := store.Lookup(lease.Uuid)
	assert.Equal(t, placement.StateAttempting, record.State())
	assert.Equal(t, client.Name(), record.Attempt)
}

type beginCallRejectingReconcilerOperations struct {
	ReconcilerOperations
}

func (*beginCallRejectingReconcilerOperations) BeginCall(operation.Initiation) bool {
	return false
}

func TestReconcilerBeginCallFailureRefusesUnsentAttempt(t *testing.T) {
	registry := operation.NewRegistry()
	operations := &beginCallRejectingReconcilerOperations{ReconcilerOperations: registry}
	client := &mockReconcilerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
	})
	require.NoError(t, err)
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	lease := billingtypes.Lease{
		Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{lease}, nil
		},
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			copy := lease
			return &copy, nil
		},
	}
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil),
		operations:          operations,
	}
	reconciler, err := NewReconciler(
		ReconcilerConfig{ProviderUUID: "provider-1", CallbackBaseURL: "http://callback"},
		chainClient, noopAck, router, runtime, store,
	)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	client.mu.Lock()
	assert.Empty(t, client.provisionCalls,
		"a failed call-phase transition must never contact the backend")
	client.mu.Unlock()
	assert.False(t, registry.Contains(lease.Uuid))
	record := store.Lookup(lease.Uuid)
	assert.Equal(t, placement.StateAbsent, record.State())
	assert.Empty(t, record.Attempt,
		"the exact unsent attempt must be refused before aborting the operation")
	assert.False(t, record.AttemptOperationID().Valid())
}

func TestReconcilerEventSinkPanicDoesNotPreventProvisionDispatch(t *testing.T) {
	registry := operation.NewRegistry()
	client := &mockReconcilerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
	})
	require.NoError(t, err)
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	lease := billingtypes.Lease{
		Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{lease}, nil
		},
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			copy := lease
			return &copy, nil
		},
	}
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil), operations: registry,
	}
	panics := metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
		metrics.LifecycleEventProvisionStarting,
	)
	before := promtestutil.ToFloat64(panics)
	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
		StartEvents: provisionStartEventSinkFunc(func(string) { panic("event sink fault") }),
	}, chainClient, noopAck, router, runtime, store)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

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

func TestReconcilerAuthoritativeLeaseReadSkipsClosedLeaseBeforeProvision(t *testing.T) {
	registry := operation.NewRegistry()
	client := &mockReconcilerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
	})
	require.NoError(t, err)
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	runtime := &typedTestReconcilerRuntime{
		mockInFlightTracker: newMockInFlightTracker(nil),
		operations:          registry,
	}

	readEntered := make(chan struct{})
	releaseRead := make(chan struct{})
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}}, nil
		},
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			close(readEntered)
			select {
			case <-releaseRead:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_CLOSED,
			}, nil
		},
	}
	reconciler, err := NewReconciler(ReconcilerConfig{
		ProviderUUID: "provider-1", CallbackBaseURL: "http://callback",
	}, chainClient, noopAck, router, runtime, store)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- reconciler.ReconcileAll(t.Context()) }()
	select {
	case <-readEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for authoritative lease re-read")
	}
	assert.Equal(t, operation.LeaseClaimBusy, registry.TryClaimLeaseNow("lease-1").Outcome(),
		"the authoritative read must run under the lifecycle claim")
	close(releaseRead)
	require.NoError(t, <-done)

	client.mu.Lock()
	defer client.mu.Unlock()
	assert.Empty(t, client.provisionCalls,
		"a lease closed after inventory must never reach Provision")
	assert.False(t, registry.Contains("lease-1"))
}

func TestReconciler_doStartProvisioning_HonorsPlacement(t *testing.T) {
	// When a placement record exists, doStartProvisioning (via ReconcileAll) must
	// route to the placement-pinned backend, not the least-loaded default (ENG-333).
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_PENDING},
			}, nil
		},
	}

	// pinned is NOT the router default — without placement it would be bypassed.
	pinned := &mockReconcilerBackend{name: "backend-pinned"}
	// leastLoaded is the router default — what would be chosen without placement.
	leastLoaded := &mockReconcilerBackend{name: "backend-least"}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: pinned, Match: backend.MatchCriteria{SKUs: []string{"pinned-only-sku"}}},
			{Backend: leastLoaded, IsDefault: true},
		},
	})

	ps := &mockPlacementStore{}
	ps.Set("lease-1", "backend-pinned")

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	// The placement-pinned backend must have received the Provision call.
	pinned.mu.Lock()
	pinnedCalls := len(pinned.provisionCalls)
	pinned.mu.Unlock()

	leastLoaded.mu.Lock()
	leastCalls := len(leastLoaded.provisionCalls)
	leastLoaded.mu.Unlock()

	assert.Equal(t, 1, pinnedCalls, "pinned backend must receive the Provision call")
	assert.Equal(t, 0, leastCalls, "least-loaded (default) backend must NOT receive the Provision call")
}

// --- Placement pruning tests (ENG-333) ---

// statsErrRetentionBackend wraps MockBackend to make ListRetentions fail,
// exercising the "this record's backend did not answer /retentions → no prune"
// gate.
type statsErrRetentionBackend struct {
	*backend.MockBackend
}

func (b *statsErrRetentionBackend) ListRetentions(context.Context) ([]backend.RetainedLease, error) {
	return nil, errors.New("retentions unavailable")
}

// TestCleanupOrphanedPlacements_GateD is a white-box unit test that calls
// cleanupOrphanedPlacements directly with hand-built maps. It isolates gate
// (d) — the chain-terminal check — which the end-to-end reconciler tests
// cannot exercise cleanly: an active-but-backend-absent lease takes the
// anomaly re-provision path, which marks it in-flight and lets gate (c)
// short-circuit before gate (d) is reached. Calling the method directly also
// covers the otherwise-untested CLOSED-on-chain → prune branch.
//
// mockInFlightTracker (empty) is used because the Reconciler.tracker field is
// typed ReconcilerTracker, which DefaultInFlightTracker does not implement.
func TestCleanupOrphanedPlacements_GateD(t *testing.T) {
	ps := &mockPlacementStore{}
	ps.Set("active-chain-lease", "backend-a")  // ACTIVE on chain, absent from backends → keep (gate d)
	ps.Set("pending-chain-lease", "backend-b") // PENDING on chain, absent from backends → keep (gate d)
	ps.Set("closed-chain-lease", "backend-a")  // CLOSED on chain, absent from backends → prune
	ps.Set("off-chain-lease", "backend-a")     // absent from chain entirely → prune

	chainLeases := map[string]billingtypes.Lease{
		"active-chain-lease":  {Uuid: "active-chain-lease", State: billingtypes.LEASE_STATE_ACTIVE},
		"pending-chain-lease": {Uuid: "pending-chain-lease", State: billingtypes.LEASE_STATE_PENDING},
		"closed-chain-lease":  {Uuid: "closed-chain-lease", State: billingtypes.LEASE_STATE_CLOSED},
	}
	backendLeases := map[string]struct{}{} // empty — none on a backend
	tracker := newMockInFlightTracker(nil) // empty — none in-flight

	// cleanupOrphanedPlacements reads ONLY r.placementView, r.legacyPlacement,
	// and r.tracker (plus
	// the passed-in maps), so this hand-built literal is safe. Any future change
	// that makes the pruner read other Reconciler fields must set them here too,
	// or this white-box test will nil-panic.
	r := &Reconciler{placementView: ps, legacyPlacement: ps, tracker: tracker, interval: time.Minute}
	answered := answeredSet{"backend-a": true, "backend-b": true}
	pruned := r.cleanupOrphanedPlacements(context.Background(), chainLeases, backendLeases,
		answered, answered, time.Now().Add(time.Hour), ^uint64(0), nil,
		operation.TrackerSnapshot{}, 0)

	assert.Equal(t, 2, pruned)
	assert.Equal(t, "backend-a", ps.Get("active-chain-lease"), "gate d: ACTIVE on chain must keep")
	assert.Equal(t, "backend-b", ps.Get("pending-chain-lease"), "gate d: PENDING on chain must keep")
	assert.Equal(t, "", ps.Get("closed-chain-lease"), "gate d: CLOSED on chain must prune")
	assert.Equal(t, "", ps.Get("off-chain-lease"), "absent from chain must prune")
}

func TestCleanupOrphanedPlacements_TypedLeaseClaimFencesSnapshotAndDelete(t *testing.T) {
	store, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.Confirm("lease-1", "backend-a"))

	operations := operation.NewRegistry()
	inventoryBoundary := operations.Snapshot()
	eventClaim := operations.TryClaimLeaseNow("lease-1")
	require.True(t, eventClaim.Acquired())

	r := &Reconciler{
		placementView:      store,
		placementAuthority: store,
		operations:         operations,
		interval:           time.Minute,
	}
	answered := answeredSet{"backend-a": true}
	prune := func(snapshot operation.TrackerSnapshot) int {
		return r.cleanupOrphanedPlacements(
			context.Background(), nil, nil, answered, answered,
			time.Now().Add(time.Hour), store.SnapshotRevision(), nil, snapshot, 0,
		)
	}

	assert.Zero(t, prune(inventoryBoundary),
		"a lifecycle claim acquired after inventory must fence its placement")
	assert.Equal(t, placement.StateConfirmed, store.Lookup("lease-1").State())

	require.True(t, operations.ReleaseLease(eventClaim.Claim()))
	assert.Zero(t, prune(inventoryBoundary),
		"a completed lifecycle action must remain visible to the old inventory boundary")
	assert.Equal(t, placement.StateConfirmed, store.Lookup("lease-1").State())

	assert.Equal(t, 1, prune(operations.Snapshot()),
		"a claim-free inventory boundary may perform the proven cleanup")
	assert.Equal(t, placement.StateAbsent, store.Lookup("lease-1").State())
}

// TestCleanupOrphanedPlacements_PerRecordAnswered pins the ENG-654 rescoping:
// "did the owning backend report?" is a question about ONE record's backend, not
// about the fleet, and it is asked of both list endpoints because they fail
// independently. One silent machine must cost only its own records.
//
// Deliberately NOT parallel: it reads a process-global Prometheus collector.
func TestCleanupOrphanedPlacements_PerRecordAnswered(t *testing.T) {
	ps := &mockPlacementStore{}
	ps.Set("on-answering", "backend-a")      // both endpoints answered → prune
	ps.Set("provisions-silent", "backend-b") // /provisions failed → keep
	ps.Set("retentions-silent", "backend-c") // /retentions failed → keep
	ps.Set("unconfigured", "backend-decomm") // not configured at all → keep (ENG-635)

	// Every record is chain-terminal and on no backend: the ONLY thing that
	// differs between them is whether their own backend reported.
	chainLeases := map[string]billingtypes.Lease{}
	backendLeases := map[string]struct{}{}
	tracker := newMockInFlightTracker(nil)

	provisionsAnswered := answeredSet{"backend-a": true, "backend-b": false, "backend-c": true}
	retentionsAnswered := answeredSet{"backend-a": true, "backend-b": true, "backend-c": false}

	r := &Reconciler{placementView: ps, legacyPlacement: ps, tracker: tracker, interval: time.Minute}
	skips := metrics.ReconcilerCleanupSkipsTotal.
		WithLabelValues(metrics.CleanupPassPlacement, metrics.CleanupSkipBackendSilent)
	before := promtestutil.ToFloat64(skips)

	// now far in the future so the ENG-335 grace window is not what is being tested.
	pruned := r.cleanupOrphanedPlacements(context.Background(), chainLeases, backendLeases,
		provisionsAnswered, retentionsAnswered, time.Now().Add(time.Hour), ^uint64(0), nil,
		operation.TrackerSnapshot{}, 0)

	assert.Equal(t, 1, pruned)
	assert.Equal(t, "", ps.Get("on-answering"),
		"a record whose backend answered both endpoints must still be pruned, degraded sweep or not")
	assert.Equal(t, "backend-b", ps.Get("provisions-silent"), "/provisions silent for this backend must keep")
	assert.Equal(t, "backend-c", ps.Get("retentions-silent"), "/retentions silent for this backend must keep")
	assert.Equal(t, "backend-decomm", ps.Get("unconfigured"),
		"an unconfigured backend's record is the only pointer to its data; keep it")
	assert.Equal(t, 3.0, promtestutil.ToFloat64(skips)-before, "every withheld prune must be counted")
}

func TestCleanupOrphanedPlacements_ConflictCandidatesMustAllBeAccounted(t *testing.T) {
	old := time.Now().Add(-time.Hour)
	ps := &mockPlacementStore{
		conflicts: map[string]bool{
			"fully-accounted": true,
			"legacy-unknown":  true,
			"removed-owner":   true,
			"unknown-owners":  true,
		},
		conflictBackends: map[string][]string{
			"fully-accounted": {"backend-a", "backend-b"},
			"removed-owner":   {"backend-a", "backend-removed"},
			"unknown-owners":  {"backend-a", "backend-b"},
		},
		conflictOwnersUnknown: map[string]bool{
			"unknown-owners": true,
		},
		setAt: map[string]time.Time{
			"fully-accounted": old,
			"legacy-unknown":  old,
			"removed-owner":   old,
			"unknown-owners":  old,
		},
	}
	answered := answeredSet{"backend-a": true, "backend-b": true}
	r := &Reconciler{
		placementView:   ps,
		legacyPlacement: ps,
		tracker:         newMockInFlightTracker(nil),
		interval:        time.Minute,
	}

	pruned := r.cleanupOrphanedPlacements(
		context.Background(), nil, nil, answered, answered,
		time.Now(), ^uint64(0), nil, operation.TrackerSnapshot{}, 0,
	)

	assert.Equal(t, 1, pruned)
	assert.Equal(t, placement.StateAbsent, ps.Lookup("fully-accounted").State(),
		"a known conflict is removable only after every named candidate answered both inventories")
	assert.True(t, ps.Lookup("removed-owner").Conflict,
		"a removed candidate must remain durable rather than disappearing with router configuration")
	assert.True(t, ps.Lookup("legacy-unknown").Conflict,
		"a pre-candidate conflict cannot be cleared by today's configured fleet")
	assert.True(t, ps.Lookup("unknown-owners").Conflict,
		"a successful sweep of today's fleet cannot account for an unknown historical owner")
}

// TestReconciler_PrunesOrphanedPlacement verifies the happy-path prune:
// a placement whose lease is absent from chain, all backends, and the
// in-flight tracker is removed. A retained lease on the same backend
// must survive.
func TestReconciler_PrunesOrphanedPlacement(t *testing.T) {
	// "gone-lease" has a placement but is on no backend, not in-flight,
	// not on chain.  "retained-1" is still retained → must survive.
	ps := &mockPlacementStore{}
	// Backdate well past the 10m grace (default 5m interval × 2) so the orphan is
	// prunable this sweep; a freshly-set placement would be kept by the ENG-335
	// grace window (covered by TestCleanupOrphanedPlacements_GraceWindow).
	ps.setWithTime("gone-lease", "backend-a", time.Now().Add(-time.Hour))
	ps.Set("retained-1", "backend-a")

	mb := backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"})
	mb.SetRetentions([]backend.RetainedLease{{LeaseUUID: "retained-1"}})
	// ListProvisions returns empty — gone-lease has no active provision.

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})
	require.NoError(t, err)

	// Chain returns no leases → gone-lease is chain-terminal (absent).
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, &chaintest.MockClient{}, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	require.NoError(t, reconciler.RunOnce(t.Context()))

	assert.Equal(t, "", ps.Get("gone-lease"), "orphan placement must be pruned")
	assert.Equal(t, "backend-a", ps.Get("retained-1"), "retained lease placement must be kept")
}

// TestCleanupOrphanedPlacements_GraceWindow verifies ENG-335: a placement that
// is chain-terminal, absent from all backends, and not in-flight is still KEPT
// when it was set within 2× the reconcile interval (a lease that provisioned
// during a slow sweep is absent from the stale snapshot but is live). Once it
// ages past the grace window it is pruned.
func TestCleanupOrphanedPlacements_GraceWindow(t *testing.T) {
	const interval = time.Minute // grace = 2*interval = 2m
	t0 := time.Date(2026, 6, 18, 17, 11, 15, 0, time.UTC)

	ps := &mockPlacementStore{}
	ps.setWithTime("young-lease", "backend-a", t0) // set at t0

	// chain-terminal (absent from chain), absent from backends, not in-flight.
	chainLeases := map[string]billingtypes.Lease{}
	backendLeases := map[string]struct{}{}
	tracker := newMockInFlightTracker(nil)
	r := &Reconciler{placementView: ps, legacyPlacement: ps, tracker: tracker, interval: interval}
	answered := answeredSet{"backend-a": true}

	// now = t0 + 1m  → within the 2m grace → KEEP.
	pruned := r.cleanupOrphanedPlacements(context.Background(), chainLeases, backendLeases,
		answered, answered, t0.Add(time.Minute), ^uint64(0), nil,
		operation.TrackerSnapshot{}, 0)
	assert.Equal(t, 0, pruned, "young placement within grace must be kept")
	assert.Equal(t, "backend-a", ps.Get("young-lease"))

	// now = t0 + 2m + 1s → past grace → PRUNE.
	pruned = r.cleanupOrphanedPlacements(context.Background(), chainLeases, backendLeases,
		answered, answered, t0.Add(2*time.Minute+time.Second), ^uint64(0), nil,
		operation.TrackerSnapshot{}, 0)
	assert.Equal(t, 1, pruned, "aged placement past grace must be pruned")
	assert.Equal(t, "", ps.Get("young-lease"))
}

// TestReconciler_DoesNotPruneOnIncompleteRetentions verifies the retentions half
// of the attribution gate end-to-end: when the record's OWN backend fails
// ListRetentions, its placement must not be pruned (a backend outage must not
// wipe valid placements). The single configured backend here is the record's
// owner; TestCleanupOrphanedPlacements_PerRecordAnswered covers the case where
// the failing backend is somebody else's.
func TestReconciler_DoesNotPruneOnIncompleteRetentions(t *testing.T) {
	ps := &mockPlacementStore{}
	ps.Set("gone-lease", "backend-a")

	failing := &statsErrRetentionBackend{
		MockBackend: backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"}),
	}

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: failing, IsDefault: true}},
	})
	require.NoError(t, err)

	// Chain returns no leases → gone-lease would be chain-terminal, but its
	// backend not answering /retentions must block the prune.
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, &chaintest.MockClient{}, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	require.NoError(t, reconciler.RunOnce(t.Context()))

	assert.Equal(t, "backend-a", ps.Get("gone-lease"),
		"placement must NOT be pruned when its own backend's retentions could not be fetched")
}

// TestReconciler_DoesNotPruneActiveOrInFlight verifies the in-flight gate (c)
// end-to-end through ReconcileAll:
//   - (c) inflight-lease is in the in-flight tracker → kept
//
// active-lease is also kept, but by gate (b): it is provisioned on the backend,
// so it lands in allProvisions → backendLeases and is kept before gate (d) is
// ever evaluated. Gate (d) (chain-terminal) is NOT exercised by this test — it
// is covered directly by TestCleanupOrphanedPlacements_GateD. active-lease is
// included here only to confirm a healthy provisioned+ACTIVE lease survives.
func TestReconciler_DoesNotPruneActiveOrInFlight(t *testing.T) {
	ps := &mockPlacementStore{}
	ps.Set("active-lease", "backend-a")
	ps.Set("inflight-lease", "backend-a")

	// Use the test-local mockReconcilerBackend so we can pre-seed provisions.
	// active-lease is provisioned+ready (kept by gate b); inflight-lease has no
	// provision and is kept only by the in-flight gate (c).
	mb := &mockReconcilerBackend{
		name: "backend-a",
		provisions: []backend.ProvisionInfo{
			{LeaseUUID: "active-lease", Status: backend.ProvisionStatusReady, BackendName: "backend-a"},
		},
	}

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})
	require.NoError(t, err)

	// Inject in-flight tracker with inflight-lease already tracked.
	tracker := newMockInFlightTracker(nil)
	tracker.TrackInFlight("inflight-lease", "tenant-a", nil, "backend-a")

	// Chain: active-lease is ACTIVE; inflight-lease is not on chain.
	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "active-lease", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, ps)
	require.NoError(t, err)

	require.NoError(t, reconciler.RunOnce(t.Context()))

	// active-lease: gate (b) — provisioned on the backend → placement kept.
	assert.Equal(t, "backend-a", ps.Get("active-lease"),
		"active provisioned lease placement must be kept (gate b)")
	// inflight-lease: gate (c) — in-flight tracker → placement kept.
	assert.Equal(t, "backend-a", ps.Get("inflight-lease"),
		"in-flight lease placement must be kept (gate c)")
}

func TestReconciler_SyncsPlacementFromRetentions(t *testing.T) {
	// Setup: A backend with a retained lease. Chain returns no leases (so the
	// retained UUID cannot come from active-provision syncing — it must come
	// from the new fetchAllRetentions path).
	mb := backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"})
	mb.SetRetentions([]backend.RetainedLease{{LeaseUUID: "retained-1"}})
	// ListProvisions returns empty (no active provisions) so "retained-1"
	// can only appear in the placement store via the retentions path.

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})
	require.NoError(t, err)

	ps := &mockPlacementStore{}

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, &chaintest.MockClient{}, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	require.NoError(t, reconciler.RunOnce(t.Context()))

	assert.Equal(t, "backend-a", ps.Get("retained-1"),
		"reconciler must derive placement for a retained lease")
}

// listRetentionsFailBackend fails the test if ListRetentions is ever called.
// Used to prove the reconciler skips the retentions fetch when placement
// tracking is disabled (nil placementStore), where the result would be unused.
type listRetentionsFailBackend struct {
	*backend.MockBackend
	t *testing.T
}

func (b *listRetentionsFailBackend) ListRetentions(context.Context) ([]backend.RetainedLease, error) {
	b.t.Errorf("ListRetentions must not be called when placementStore is nil (ENG-333)")
	return nil, nil
}

func TestReconciler_SkipsRetentionFetch_WhenPlacementDisabled(t *testing.T) {
	// With placement tracking disabled (nil placementStore), retained-lease
	// placement is never derived/pruned, so the reconciler must not query
	// /retentions at all — avoiding pointless per-backend calls every sweep.
	mb := &listRetentionsFailBackend{
		MockBackend: backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"}),
		t:           t,
	}

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})
	require.NoError(t, err)

	// nil tracker AND nil placementStore => placement subsystem disabled.
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, &chaintest.MockClient{}, noopAck, router, nil, nil)
	require.NoError(t, err)

	// If fetchAllRetentions runs, the backend's ListRetentions t.Errorf's.
	require.NoError(t, reconciler.RunOnce(t.Context()))
}

// listRetentionsPanicBackend panics in ListRetentions, exercising the
// fetchAllRetentions panic-recovery path.
type listRetentionsPanicBackend struct {
	*backend.MockBackend
}

func (b *listRetentionsPanicBackend) ListRetentions(context.Context) ([]backend.RetainedLease, error) {
	panic("simulated retentions fetch panic")
}

func TestReconciler_RetentionFetchPanic_RecordsMetric(t *testing.T) {
	// A panic in a backend's ListRetentions must be recovered (RunOnce does not
	// crash) AND counted in ReconcilerPanicsTotal, like every other recovered
	// panic site (fetch_provisions / process_lease / process_orphan).
	before := promtestutil.ToFloat64(metrics.ReconcilerPanicsTotal.WithLabelValues("fetch_retentions"))

	mb := &listRetentionsPanicBackend{
		MockBackend: backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"}),
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mb, IsDefault: true}},
	})
	require.NoError(t, err)

	// Non-nil placementStore so the retentions fetch actually runs.
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, &chaintest.MockClient{}, noopAck, router, newMockInFlightTracker(nil), &mockPlacementStore{})
	require.NoError(t, err)

	require.NoError(t, reconciler.RunOnce(t.Context()), "retention-fetch panic must be recovered")

	after := promtestutil.ToFloat64(metrics.ReconcilerPanicsTotal.WithLabelValues("fetch_retentions"))
	assert.Equal(t, before+1, after,
		"recovered retention-fetch panic must increment ReconcilerPanicsTotal{fetch_retentions}")
}

// TestRestoreAffinity_EndToEnd_MultiBackend proves that, on a multi-backend
// pool, the reconciler derives the source lease's placement from the backend
// that RETAINS it — regardless of load-based routing. b1 is the least-loaded
// (naive routing would pick it), but the source lease is retained only on b2,
// so placement[source] must resolve to b2 (ENG-333).
func TestRestoreAffinity_EndToEnd_MultiBackend(t *testing.T) {
	b1 := backend.NewMockBackend(backend.MockBackendConfig{Name: "b1"})
	b2 := backend.NewMockBackend(backend.MockBackendConfig{Name: "b2"})

	// b2 retains the source lease; b1 retains nothing.
	b2.SetRetentions([]backend.RetainedLease{{LeaseUUID: "source"}})

	// b1 is the least-loaded backend — naive routing would pick the wrong one.
	b1.SetLoadStats(&backend.LoadStats{TotalCPUCores: 100, AllocatedCPUCores: 0})
	b2.SetLoadStats(&backend.LoadStats{TotalCPUCores: 100, AllocatedCPUCores: 90})

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{
			{Backend: b1, IsDefault: true},
			{Backend: b2, Match: backend.MatchCriteria{SKUs: []string{"b2-only-sku"}}},
		},
	})
	require.NoError(t, err)

	ps := &mockPlacementStore{}

	// Chain returns no leases — the reconciler must derive placement[source]
	// purely from b2's /retentions response, not from active-provision syncing.
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, &chaintest.MockClient{}, noopAck, router, newMockInFlightTracker(nil), ps)
	require.NoError(t, err)

	require.NoError(t, reconciler.RunOnce(t.Context()))

	assert.Equal(t, "b2", ps.Get("source"),
		"restore affinity: source placement must resolve to the retaining backend, not the least-loaded one")
}

func TestReconciler_ReconcileAll_OmittedProvisionNotDeprovisioned(t *testing.T) {
	// Directional safety (ENG-380): a provision present on chain but OMITTED from
	// the backend list must never be deprovisioned. orphans = backend - chain, so
	// an omitted entry can only shrink the deprovision set; it is re-checked next
	// tick. (The reconciler instead treats it as not-provisioned and re-provisions.)
	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "kept-lease", Tenant: "tenant-1", State: billingtypes.LEASE_STATE_ACTIVE},
			}, nil
		},
	}
	mockBackend := &mockReconcilerBackend{
		name:       "test",
		provisions: []backend.ProvisionInfo{}, // kept-lease omitted from the fetched page
	}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, nil, nil)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.deprovisionCalls, "an omitted-from-list provision must never be deprovisioned")
}

func TestReconciler_ReconcileAll_MalformedRecordedHash_KeepsPayload(t *testing.T) {
	// A corrupt recorded hash must abort the provision attempt, not fall back to
	// MetaHash. The fallback is only correct for a payload that never HAD a
	// recorded hash; applying it to a corrupt one would compare an updated
	// payload against the create-time commitment it legitimately diverges from,
	// delete a good manifest, and close a live lease on the next sweep.
	originalPayload := []byte("original manifest payload")
	updatedPayload := []byte("updated manifest payload")
	metaHash := sha256.Sum256(originalPayload)

	mockChain := &chaintest.MockClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{
					Uuid:     "lease-1",
					Tenant:   "tenant-1",
					State:    billingtypes.LEASE_STATE_ACTIVE,
					MetaHash: metaHash[:],
					Items:    []billingtypes.LeaseItem{{SkuUuid: "docker-micro", Quantity: 1}},
				},
			}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			t.Errorf("leases %v closed on-chain (%q) — a corrupt hash must not be terminal", leaseUUIDs, reason)
			return 0, nil, nil
		},
	}
	mockBackend := &mockReconcilerBackend{name: "test", provisions: []backend.ProvisionInfo{}}
	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	dbPath := t.TempDir() + "/payloads.db"
	store := newReconcilerPayloadStore(t, dbPath)
	require.True(t, store.Store("lease-1", originalPayload))
	require.NoError(t, store.Put("lease-1", updatedPayload))
	require.NoError(t, store.Close())

	// Corrupt the recorded hash, leaving the payload intact.
	mutatePayloadDB(t, dbPath, func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("payload_hashes")).Put([]byte("lease-1"), []byte{0x00})
	})

	payloadStore := newReconcilerPayloadStore(t, dbPath)
	defer payloadStore.Close()

	tracker := newMockInFlightTracker(payloadStore)
	reconciler, err := newReconciler(ReconcilerConfig{
		ProviderUUID:    "provider-1",
		CallbackBaseURL: "http://localhost:8080",
	}, mockChain, noopAck, router, tracker, nil)
	require.NoError(t, err)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	// The payload survives so a later sweep can still provision it once the
	// underlying read problem is resolved.
	assert.True(t, mustHasPayload(t, payloadStore, "lease-1"),
		"a corrupt recorded hash must not delete the payload")

	mockBackend.mu.Lock()
	defer mockBackend.mu.Unlock()
	assert.Empty(t, mockBackend.provisionCalls, "must not provision against an unverifiable payload")
}
