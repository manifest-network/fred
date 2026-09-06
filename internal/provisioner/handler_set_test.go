package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	handlerTestLeaseOne         = "11111111-1111-4111-8111-111111111111"
	handlerTestLeaseTwo         = "22222222-2222-4222-8222-222222222222"
	handlerTestLeaseA           = "33333333-3333-4333-8333-333333333333"
	handlerTestLeaseB           = "44444444-4444-4444-8444-444444444444"
	handlerTestLeaseDuplicate   = "55555555-5555-4555-8555-555555555555"
	handlerTestLeaseExpired     = "66666666-6666-4666-8666-666666666666"
	handlerTestLeaseNotInFlight = "77777777-7777-4777-8777-777777777777"
	handlerTestLeasePayload     = "88888888-8888-4888-8888-888888888888"
	handlerTestLeaseRetained    = "99999999-9999-4999-8999-999999999999"
	handlerTestLeaseUntracked   = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	handlerTestLeaseValidation  = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	handlerTestLeaseUnknown     = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

// mockAcknowledger implements Acknowledger for testing.
type mockAcknowledger struct {
	acknowledgeFn func(ctx context.Context, leaseUUID string) (bool, string, error)
}

func (m *mockAcknowledger) Acknowledge(ctx context.Context, leaseUUID string) (bool, string, error) {
	if m.acknowledgeFn != nil {
		return m.acknowledgeFn(ctx, leaseUUID)
	}
	return true, "tx-hash", nil
}

// mockPlacementStore is the legacy raw-map fixture used behind typed test
// adapters. Production accepts only narrow placement capability ports.
type mockPlacementStore struct {
	mu                    sync.Mutex
	placements            map[string]string
	attempts              map[string]string
	conflicts             map[string]bool
	conflictBackends      map[string][]string
	conflictOwnersUnknown map[string]bool
	setAt                 map[string]time.Time
	revision              uint64
}

func (m *mockPlacementStore) Lookup(leaseUUID string) placement.Placement {
	m.mu.Lock()
	defer m.mu.Unlock()
	conflictBackends := slices.Clone(m.conflictBackends[leaseUUID])
	return placement.Placement{
		Backend:          m.placements[leaseUUID],
		Attempt:          m.attempts[leaseUUID],
		SetAt:            m.setAt[leaseUUID],
		Conflict:         m.conflicts[leaseUUID],
		ConflictBackends: conflictBackends,
		ConflictOwnersUnknown: m.conflictOwnersUnknown[leaseUUID] ||
			(m.conflicts[leaseUUID] && len(conflictBackends) == 0),
	}
}

func (m *mockPlacementStore) Get(leaseUUID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.placements == nil {
		return ""
	}
	return m.placements[leaseUUID]
}

func (m *mockPlacementStore) Set(leaseUUID, backendName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.placements == nil {
		m.placements = make(map[string]string)
	}
	if m.setAt == nil {
		m.setAt = make(map[string]time.Time)
	}
	m.placements[leaseUUID] = backendName
	delete(m.attempts, leaseUUID)
	delete(m.conflicts, leaseUUID)
	delete(m.conflictBackends, leaseUUID)
	delete(m.conflictOwnersUnknown, leaseUUID)
	// Mirror the real Store.Set, which always restamps SetAt on an explicit
	// placement (provision/restore). SetBatch is the preserve-on-resync path.
	m.setAt[leaseUUID] = time.Now()
	m.revision++
	return nil
}

func (m *mockPlacementStore) SetAttempting(leaseUUID, backendName string) (uint64, error) {
	revision, _, err := m.SetAttemptingIfNotNewer(leaseUUID, backendName, ^uint64(0))
	return revision, err
}

func (m *mockPlacementStore) SetAttemptingIfNotNewer(
	leaseUUID, backendName string,
	maxRevision uint64,
) (uint64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.revision > maxRevision {
		return 0, false, nil
	}
	if backendName == "" {
		return 0, false, placement.ErrInvalidPlacement
	}
	if m.conflicts[leaseUUID] {
		return 0, false, placement.ErrUnusablePlacement
	}
	if attempt := m.attempts[leaseUUID]; attempt != "" {
		return 0, false, fmt.Errorf("%w: existing attempt %q", placement.ErrAttemptConflict, attempt)
	}
	if confirmed := m.placements[leaseUUID]; confirmed != "" && confirmed != backendName {
		return 0, false, fmt.Errorf("%w: confirmed backend %q", placement.ErrBackendConflict, confirmed)
	}
	if m.attempts == nil {
		m.attempts = make(map[string]string)
	}
	if m.setAt == nil {
		m.setAt = make(map[string]time.Time)
	}
	m.attempts[leaseUUID] = backendName
	if m.setAt[leaseUUID].IsZero() {
		m.setAt[leaseUUID] = time.Now()
	}
	m.revision++
	// This shared mock deliberately models opaque per-record revisions as zero;
	// race-specific revision tests use the real placement.Store.
	return 0, true, nil
}

func requireSetPlacementAttempt(t *testing.T, store interface {
	SetAttempting(leaseUUID, backendName string) (uint64, error)
}, leaseUUID, backendName string) uint64 {
	t.Helper()
	revision, err := store.SetAttempting(leaseUUID, backendName)
	require.NoError(t, err)
	return revision
}

func (m *mockPlacementStore) Confirm(leaseUUID, backendName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if backendName == "" {
		return placement.ErrInvalidPlacement
	}
	if m.conflicts[leaseUUID] {
		return placement.ErrUnusablePlacement
	}
	if m.attempts[leaseUUID] == "" && m.placements[leaseUUID] == backendName {
		return nil
	}
	if m.attempts[leaseUUID] != "" && m.attempts[leaseUUID] != backendName {
		return placement.ErrAttemptMismatch
	}
	if m.placements[leaseUUID] != "" && m.placements[leaseUUID] != backendName {
		return placement.ErrBackendConflict
	}
	if m.placements == nil {
		m.placements = make(map[string]string)
	}
	if m.setAt == nil {
		m.setAt = make(map[string]time.Time)
	}
	if m.setAt[leaseUUID].IsZero() {
		m.setAt[leaseUUID] = time.Now()
	}
	m.placements[leaseUUID] = backendName
	delete(m.attempts, leaseUUID)
	delete(m.conflicts, leaseUUID)
	delete(m.conflictBackends, leaseUUID)
	delete(m.conflictOwnersUnknown, leaseUUID)
	m.revision++
	return nil
}

func (m *mockPlacementStore) ConfirmAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error) {
	if revision != 0 {
		return false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.attempts[leaseUUID] != backendName {
		return false, nil
	}
	if m.placements == nil {
		m.placements = make(map[string]string)
	}
	if m.setAt == nil {
		m.setAt = make(map[string]time.Time)
	}
	if m.setAt[leaseUUID].IsZero() {
		m.setAt[leaseUUID] = time.Now()
	}
	m.placements[leaseUUID] = backendName
	delete(m.attempts, leaseUUID)
	delete(m.conflicts, leaseUUID)
	delete(m.conflictBackends, leaseUUID)
	delete(m.conflictOwnersUnknown, leaseUUID)
	m.revision++
	return true, nil
}

func (m *mockPlacementStore) ClearAttempt(leaseUUID, backendName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.attempts[leaseUUID] == "" {
		return nil
	}
	if m.attempts[leaseUUID] != backendName {
		return placement.ErrAttemptMismatch
	}
	delete(m.attempts, leaseUUID)
	if m.placements[leaseUUID] == "" {
		delete(m.setAt, leaseUUID)
	}
	m.revision++
	return nil
}

func (m *mockPlacementStore) ClearAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error) {
	// Placement revisions are deliberately opaque outside package placement;
	// ordinary literals therefore carry revision zero. This shared mock models
	// the current snapshot as zero; race-specific tests use a real Store.
	if revision != 0 {
		return false, nil
	}
	if err := m.ClearAttempt(leaseUUID, backendName); err != nil {
		return false, err
	}
	return true, nil
}

func (m *mockPlacementStore) Delete(leaseUUID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.placements, leaseUUID)
	delete(m.attempts, leaseUUID)
	delete(m.conflicts, leaseUUID)
	delete(m.conflictBackends, leaseUUID)
	delete(m.conflictOwnersUnknown, leaseUUID)
	delete(m.setAt, leaseUUID) // keep setAt in sync with the real store
	m.revision++
	return nil
}

func (m *mockPlacementStore) DeleteIfRevision(leaseUUID string, revision uint64) (bool, error) {
	if revision != 0 {
		return false, nil
	}
	if err := m.Delete(leaseUUID); err != nil {
		return false, err
	}
	return true, nil
}

func (m *mockPlacementStore) SetBatch(placements map[string]string) error {
	_, _, err := m.SetBatchIfNotNewer(placements, ^uint64(0))
	return err
}

func (m *mockPlacementStore) SetBatchIfNotNewer(
	placements map[string]string,
	maxRevision uint64,
) (map[string]uint64, map[string]struct{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// This shared mock has only a conservative global revision rather than the
	// real store's per-record clock. If any mock mutation is newer than the
	// cutoff, filter the whole batch without manufacturing another mutation.
	if m.revision > maxRevision {
		fenced := make(map[string]struct{}, len(placements))
		for leaseUUID := range placements {
			fenced[leaseUUID] = struct{}{}
		}
		return nil, fenced, nil
	}
	if m.placements == nil {
		m.placements = make(map[string]string)
	}
	if m.setAt == nil {
		m.setAt = make(map[string]time.Time)
	}
	applied := make(map[string]uint64)
	nextRevision := m.revision
	for k, v := range placements {
		currentBackend, backendExists := m.placements[k]
		currentAttempt, attemptExists := m.attempts[k]
		_, conflictExists := m.conflicts[k]
		_, setAtExists := m.setAt[k]
		recordExists := backendExists || attemptExists || conflictExists || setAtExists
		unusable := m.conflicts[k] ||
			(recordExists && currentBackend == "" && currentAttempt == "")

		// Match Store.SetBatchIfNotNewer: an exact positive observation is a
		// true no-op, including when a different unresolved attempt remains.
		if !unusable && backendExists && currentBackend == v && currentAttempt != v {
			continue
		}

		if !recordExists || unusable {
			// Creating or repairing an unusable record starts a fresh first-seen
			// interval and discards facts that made the old record unusable.
			m.setAt[k] = time.Now()
			delete(m.attempts, k)
		}
		m.placements[k] = v
		delete(m.conflicts, k)
		delete(m.conflictBackends, k)
		delete(m.conflictOwnersUnknown, k)
		if currentAttempt == v {
			delete(m.attempts, k)
		}
		nextRevision++
		applied[k] = nextRevision
	}
	m.revision = nextRevision
	return applied, nil, nil
}

func TestMockPlacementStore_SetBatchIfNotNewerPreservesNoOpAndFilterSemantics(t *testing.T) {
	store := &mockPlacementStore{}

	_, _, err := store.SetBatchIfNotNewer(nil, store.SnapshotRevision())
	require.NoError(t, err)
	assert.Zero(t, store.SnapshotRevision(), "an empty inventory must not advance the mock clock")

	_, _, err = store.SetBatchIfNotNewer(
		map[string]string{handlerTestLeaseOne: "backend-a"}, store.SnapshotRevision(),
	)
	require.NoError(t, err)
	cutoff := store.SnapshotRevision()
	require.NotZero(t, cutoff)
	before := store.Lookup(handlerTestLeaseOne)

	_, _, err = store.SetBatchIfNotNewer(
		map[string]string{handlerTestLeaseOne: "backend-a"}, cutoff,
	)
	require.NoError(t, err)
	assert.Equal(t, cutoff, store.SnapshotRevision(),
		"an exact inventory observation must remain usable as the same sweep's cutoff")
	assert.Equal(t, before, store.Lookup(handlerTestLeaseOne))

	_, _, err = store.SetBatchIfNotNewer(
		map[string]string{handlerTestLeaseOne: "backend-b"}, cutoff-1,
	)
	require.NoError(t, err)
	assert.Equal(t, cutoff, store.SnapshotRevision(), "a fully filtered batch must not advance the clock")
	assert.Equal(t, before, store.Lookup(handlerTestLeaseOne))

	_, set, err := store.SetAttemptingIfNotNewer(handlerTestLeaseOne, "backend-a", cutoff)
	require.NoError(t, err)
	assert.True(t, set, "the no-op inventory must not falsely fence a same-sweep attempt")
}

func (m *mockPlacementStore) SetConflictsIfNotNewer(
	conflicts map[string][]string,
	maxRevision uint64,
) (map[string]uint64, map[string]struct{}, error) {
	if len(conflicts) == 0 {
		return nil, nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fenced := make(map[string]struct{})
	applied := make(map[string]uint64)
	if m.conflicts == nil {
		m.conflicts = make(map[string]bool)
	}
	if m.conflictBackends == nil {
		m.conflictBackends = make(map[string][]string)
	}
	if m.conflictOwnersUnknown == nil {
		m.conflictOwnersUnknown = make(map[string]bool)
	}
	if m.setAt == nil {
		m.setAt = make(map[string]time.Time)
	}
	keys := slices.Sorted(maps.Keys(conflicts))
	batchFenced := m.revision > maxRevision
	for _, leaseUUID := range keys {
		if batchFenced {
			fenced[leaseUUID] = struct{}{}
			continue
		}
		reportedBackends := conflicts[leaseUUID]
		candidateSet := make(map[string]struct{}, len(reportedBackends)+len(m.conflictBackends[leaseUUID])+2)
		for _, backendName := range reportedBackends {
			if backendName != "" {
				candidateSet[backendName] = struct{}{}
			}
		}
		for _, backendName := range m.conflictBackends[leaseUUID] {
			candidateSet[backendName] = struct{}{}
		}
		if backendName := m.placements[leaseUUID]; backendName != "" {
			candidateSet[backendName] = struct{}{}
		}
		if backendName := m.attempts[leaseUUID]; backendName != "" {
			candidateSet[backendName] = struct{}{}
		}
		unknownOwners := m.conflictOwnersUnknown[leaseUUID] ||
			(m.conflicts[leaseUUID] && len(m.conflictBackends[leaseUUID]) == 0)
		normalizedCandidates := slices.Sorted(maps.Keys(candidateSet))
		if m.conflicts[leaseUUID] &&
			slices.Equal(m.conflictBackends[leaseUUID], normalizedCandidates) &&
			m.conflictOwnersUnknown[leaseUUID] == unknownOwners {
			continue
		}
		// A conflict is an additional durable quarantine, not permission to erase
		// the exact confirmed owner or outstanding attempt that led to it. Mirror
		// placement.Store so legacy reconciler tests exercise the same sticky facts.
		m.conflicts[leaseUUID] = true
		m.conflictBackends[leaseUUID] = normalizedCandidates
		m.conflictOwnersUnknown[leaseUUID] = unknownOwners
		if m.setAt[leaseUUID].IsZero() {
			m.setAt[leaseUUID] = time.Now()
		}
		m.revision++
		applied[leaseUUID] = m.revision
	}
	return applied, fenced, nil
}

func TestMockPlacementStore_SetConflictsIfNotNewerAppliesWholeEligibleBatch(t *testing.T) {
	store := &mockPlacementStore{}
	conflicts := map[string][]string{
		handlerTestLeaseB: {"backend-2", "backend-1"},
		handlerTestLeaseA: {"backend-4", "backend-3"},
	}

	applied, fenced, err := store.SetConflictsIfNotNewer(conflicts, store.SnapshotRevision())
	require.NoError(t, err)
	assert.Empty(t, fenced)
	assert.Len(t, applied, 2,
		"advancing the mock's global clock for one key must not fence another key in the same batch")
	revision := store.SnapshotRevision()
	assert.Equal(t, placement.StateUnusable, store.Lookup(handlerTestLeaseA).State())
	assert.Equal(t, placement.StateUnusable, store.Lookup(handlerTestLeaseB).State())

	applied, fenced, err = store.SetConflictsIfNotNewer(map[string][]string{
		handlerTestLeaseA: {"backend-3", "backend-4", "backend-3"},
		handlerTestLeaseB: {"backend-1", "backend-2"},
	}, revision)
	require.NoError(t, err)
	assert.Empty(t, applied)
	assert.Empty(t, fenced)
	assert.Equal(t, revision, store.SnapshotRevision(),
		"an idempotent multi-conflict batch must not advance the mock clock")
}

func (m *mockPlacementStore) ClearConflictsIfNotNewer(leases map[string]struct{}, maxRevision uint64) error {
	if len(leases) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for leaseUUID := range leases {
		if m.revision > maxRevision || !m.conflicts[leaseUUID] {
			continue
		}
		delete(m.conflicts, leaseUUID)
		delete(m.conflictBackends, leaseUUID)
		delete(m.conflictOwnersUnknown, leaseUUID)
		delete(m.setAt, leaseUUID)
	}
	m.revision++
	return nil
}

func (m *mockPlacementStore) SnapshotRevision() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.revision
}

func (m *mockPlacementStore) BeginInventorySnapshot() uint64 {
	return m.SnapshotRevision()
}

func (m *mockPlacementStore) EndInventorySnapshot(uint64) {}

func (m *mockPlacementStore) SetAt(leaseUUID string) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.setAt[leaseUUID]
	return t, ok
}

// setWithTime sets a placement with an explicit first-seen time (test helper
// for the reconciler grace-window tests).
func (m *mockPlacementStore) setWithTime(leaseUUID, backendName string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.placements == nil {
		m.placements = make(map[string]string)
	}
	if m.setAt == nil {
		m.setAt = make(map[string]time.Time)
	}
	m.placements[leaseUUID] = backendName
	delete(m.attempts, leaseUUID)
	delete(m.conflicts, leaseUUID)
	delete(m.conflictBackends, leaseUUID)
	delete(m.conflictOwnersUnknown, leaseUUID)
	m.setAt[leaseUUID] = t
	m.revision++
}

func (m *mockPlacementStore) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := len(m.placements)
	for leaseUUID := range m.attempts {
		if m.placements[leaseUUID] == "" {
			count++
		}
	}
	for leaseUUID := range m.conflicts {
		if m.placements[leaseUUID] == "" && m.attempts[leaseUUID] == "" {
			count++
		}
	}
	return count
}

func (m *mockPlacementStore) List() map[string]placement.Placement {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]placement.Placement, len(m.placements)+len(m.attempts))
	for leaseUUID, backendName := range m.placements {
		out[leaseUUID] = placement.Placement{
			Backend: backendName,
			Attempt: m.attempts[leaseUUID],
			SetAt:   m.setAt[leaseUUID],
		}
	}
	for leaseUUID, attempt := range m.attempts {
		if _, exists := out[leaseUUID]; exists {
			continue
		}
		out[leaseUUID] = placement.Placement{Attempt: attempt, SetAt: m.setAt[leaseUUID]}
	}
	for leaseUUID := range m.conflicts {
		conflictBackends := slices.Clone(m.conflictBackends[leaseUUID])
		out[leaseUUID] = placement.Placement{
			Conflict:              true,
			ConflictBackends:      conflictBackends,
			ConflictOwnersUnknown: m.conflictOwnersUnknown[leaseUUID] || len(conflictBackends) == 0,
			SetAt:                 m.setAt[leaseUUID],
		}
	}
	return out
}

func (m *mockPlacementStore) Healthy() error { return nil }
func (m *mockPlacementStore) Close() error   { return nil }

// newTestHandlerSet creates a HandlerSet with mocked dependencies for testing.
func newTestHandlerSet(
	t testing.TB,
	chainClient *chaintest.MockClient,
	mb *mockManagerBackend,
	ack *mockAcknowledger,
	payloadStore *payload.Store,
	publishers ...message.Publisher,
) (*HandlerSet, *testOperationRegistry) {
	var backendClient backend.Backend
	if mb != nil {
		backendClient = mb
	}
	return newTestHandlerSetWithBackend(
		t, chainClient, backendClient, ack, payloadStore, publishers...,
	)
}

func newTestHandlerSetWithBackend(
	t testing.TB,
	chainClient *chaintest.MockClient,
	backendClient backend.Backend,
	ack *mockAcknowledger,
	payloadStore *payload.Store,
	publishers ...message.Publisher,
) (*HandlerSet, *testOperationRegistry) {
	tracker := newTestOperationRegistry()
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend {
			return backendClient
		},
		getBackendByNameFn: func(name string) backend.Backend {
			if backendClient != nil && backendClient.Name() == name {
				return backendClient
			}
			return nil
		},
		backendsFn: func() []backend.Backend {
			if backendClient != nil {
				return []backend.Backend{backendClient}
			}
			return nil
		},
	}

	var publisher message.Publisher
	if len(publishers) > 0 {
		publisher = publishers[0]
	}
	orch := newTestProvisionOrchestrator(
		t, "prov-1", "http://localhost:8080", router, tracker, nil, chainClient,
	)
	hs := composeTestHandlerSet(t, testHandlerDeps{
		ChainClient:  chainClient,
		Orchestrator: orch,
		Placement:    tracker.callbackStore,
		Tracker:      tracker,
		Acknowledger: ack,
		PayloadStore: payloadStore,
		Publisher:    publisher,
	})
	return hs, tracker
}

func TestHandlerSet_InvalidEventCoordinatorIsRejectedAtConstruction(t *testing.T) {
	for _, test := range []struct {
		name   string
		events *HandlerEventCoordinator
	}{
		{name: "missing"},
		{name: "zero value", events: &HandlerEventCoordinator{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, err := NewHandlerSet(HandlerDeps{
				Events:    test.events,
				Callbacks: &typedNilCallbackApplication{},
			})
			require.Nil(t, handler)
			require.ErrorContains(t, err, "handler event coordinator is required")
		})
	}
}

func TestHandlerSet_MissingOrTypedNilCallbackApplicationIsRejectedAtConstruction(t *testing.T) {
	tracker := newTestOperationRegistry()
	orchestrator := newTestProvisionOrchestrator(
		t, "provider-1", "http://callback", &mockBackendRouter{}, tracker, nil,
	)
	for _, test := range []struct {
		name      string
		callbacks CallbackApplication
	}{
		{name: "missing"},
		{name: "typed nil", callbacks: (*typedNilCallbackApplication)(nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, err := NewHandlerSet(HandlerDeps{
				Events:    orchestrator.HandlerEvents(),
				Callbacks: test.callbacks,
			})
			require.Nil(t, handler)
			require.ErrorContains(t, err, "handler callback application is required")
		})
	}
}

// --- HandleLeaseCreated tests ---

func TestHandlerSet_HandleLeaseCreated_Success(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-a",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: handlerTestLeaseOne,
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseCreated(msg)
	assert.NoError(t, err)

	// Backend should have been called
	mb.mu.Lock()
	assert.Len(t, mb.provisionCalls, 1)
	mb.mu.Unlock()

	assert.True(t, tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandleLeaseCreated_WithMetaHash_SkipsProvisioning(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01, 0x02},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: handlerTestLeaseOne,
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseCreated(msg)
	assert.NoError(t, err)

	mb.mu.Lock()
	assert.Empty(t, mb.provisionCalls, "should not provision when MetaHash is set")
	mb.mu.Unlock()

	assert.False(t, tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandleLeaseCreated_DelayedTerminalStatesHaveNoSideEffects(t *testing.T) {
	for _, state := range []billingtypes.LeaseState{
		billingtypes.LEASE_STATE_ACTIVE,
		billingtypes.LEASE_STATE_CLOSED,
		billingtypes.LEASE_STATE_REJECTED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			mb := &mockManagerBackend{name: "test-backend"}
			mockChain := &chaintest.MockClient{GetLeaseFunc: func(
				context.Context,
				string,
			) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid: handlerTestLeaseOne, Tenant: "tenant-a", State: state,
					MetaHash: []byte{1},
					Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
				}, nil
			}}
			publisher := newMockPublisher()
			hs, tracker := newTestHandlerSet(t, mockChain, mb, nil, nil, publisher)

			require.NoError(t, hs.HandleLeaseCreated(newLeaseEventMsg(t, chain.LeaseEvent{
				Type: chain.LeaseCreated, LeaseUUID: handlerTestLeaseOne, Tenant: "tenant-a",
			})))

			assert.Empty(t, hs.awaitingPayload)
			assert.False(t, tracker.IsInFlight(handlerTestLeaseOne))
			mb.mu.Lock()
			assert.Empty(t, mb.provisionCalls)
			assert.Empty(t, mb.deprovisionCalls)
			mb.mu.Unlock()
			publisher.mu.Lock()
			assert.Empty(t, publisher.published[TopicLeaseEvent])
			publisher.mu.Unlock()
		})
	}
}

func TestHandlerSet_CreateClaimFencesChainReadThroughBackendDispatch(t *testing.T) {
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	mockChain := &chaintest.MockClient{GetLeaseFunc: func(
		context.Context,
		string,
	) (*billingtypes.Lease, error) {
		close(readStarted)
		<-releaseRead
		return &billingtypes.Lease{
			Uuid: handlerTestLeaseOne, Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING,
			Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
		}, nil
	}}
	mb := &mockManagerBackend{name: "test-backend"}
	hs, _ := newTestHandlerSet(t, mockChain, mb, nil, nil)
	message := newLeaseEventMsg(t, chain.LeaseEvent{
		Type: chain.LeaseCreated, LeaseUUID: handlerTestLeaseOne, Tenant: "tenant-a",
	})
	handled := make(chan error, 1)
	go func() { handled <- hs.HandleLeaseCreated(message) }()
	<-readStarted

	orchestrator := hs.events.orchestrator
	require.Error(t, orchestrator.Deprovision(context.Background(), handlerTestLeaseOne),
		"close must retry while the create handler owns the authoritative read")
	mb.mu.Lock()
	assert.Empty(t, mb.provisionCalls)
	assert.Empty(t, mb.deprovisionCalls)
	mb.mu.Unlock()

	close(releaseRead)
	require.NoError(t, <-handled)
	mb.mu.Lock()
	assert.Len(t, mb.provisionCalls, 1)
	mb.mu.Unlock()
}

func TestHandlerSet_HandleLeaseCreated_LeaseNotFound(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, billingtypes.ErrLeaseNotFound
		},
	}

	hs, _ := newTestHandlerSet(t, mockChain, nil, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: handlerTestLeaseOne,
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseCreated(msg)
	require.ErrorIs(t, err, billingtypes.ErrLeaseNotFound,
		"immutable-ledger NotFound is an uncertain read and must retry")
}

func TestHandlerSet_HandleLeaseCreated_ChainError(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, errors.New("chain unavailable")
		},
	}

	hs, _ := newTestHandlerSet(t, mockChain, nil, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: handlerTestLeaseOne,
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseCreated(msg)
	assert.Error(t, err, "should return error for retry")
}

func TestHandlerSet_HandleLeaseCreated_ValidationError_PublishesFailedEvent(t *testing.T) {
	pub := newMockPublisher()
	_, backendClient := provisionResponseBackendForTest(
		t, "test-backend", http.StatusBadRequest,
		`{"error":"unknown SKU: bad-sku","validation_code":"unknown_sku"}`,
	)
	rejectCalled := false
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:   leaseUUID,
				Tenant: "tenant-a",
				State:  billingtypes.LEASE_STATE_PENDING,
				Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			rejectCalled = true
			assert.Equal(t, []string{handlerTestLeaseValidation}, leaseUUIDs)
			assert.Equal(t, "invalid SKU", reason)
			return 1, []string{"tx-rej"}, nil
		},
	}

	hs, _ := newTestHandlerSetWithBackend(t, mockChain, backendClient, nil, nil, pub)

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: handlerTestLeaseValidation,
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseCreated(msg)
	assert.NoError(t, err)
	assert.True(t, rejectCalled, "lease should be rejected on chain")

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()
	require.Len(t, msgs, 2, "starting must precede the synchronous failure event")

	var starting, event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &starting))
	require.NoError(t, json.Unmarshal(msgs[1].Payload, &event))
	assert.Equal(t, backend.ProvisionStatusProvisioning, starting.Status)
	assert.Equal(t, handlerTestLeaseValidation, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, event.Status)
	assert.Equal(t, "invalid SKU", event.Error)
}

// --- HandleLeaseClosed tests ---

func TestHandlerSet_HandleLeaseClosed_Success(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_ACTIVE,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
	}

	hs, _ := newTestHandlerSet(t, mockChain, mb, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: handlerTestLeaseOne,
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseClosed(msg)
	assert.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{handlerTestLeaseOne}, mb.deprovisionCalls)
	mb.mu.Unlock()
}

func TestHandlerSet_HandleLeaseClosed_CleansUpPayload(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store(handlerTestLeaseOne, []byte("data"))

	hs, _ := newTestHandlerSet(t, mockChain, mb, nil, ps)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: handlerTestLeaseOne,
	})

	err = hs.HandleLeaseClosed(msg)
	assert.NoError(t, err)
	hasPayload, err := ps.Has(handlerTestLeaseOne)
	require.NoError(t, err)
	assert.False(t, hasPayload, "payload should be cleaned up")
}

func TestHandlerSet_HandleLeaseExpired_DelegatesToClosed(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{}

	hs, _ := newTestHandlerSet(t, mockChain, mb, nil, nil)
	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseExpired,
		LeaseUUID: handlerTestLeaseOne,
	})

	err := hs.HandleLeaseExpired(msg)
	assert.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{handlerTestLeaseOne}, mb.deprovisionCalls)
	mb.mu.Unlock()
}

// TestHandlerSet_HandleLeaseClosed_DoesNotEmitRetainedOnIntent verifies the
// ENG-329 change: processLeaseClose NO LONGER emits a retained event on close
// intent (the former optimistic :189 emit fired regardless of whether the
// backend actually retained). The notice now fires on observed ground truth
// from the deprovision callback (HandleBackendCallback), and the durable
// backstop is the queryable retention status. Close must publish no lease event.
func TestHandlerSet_HandleLeaseClosed_DoesNotEmitRetainedOnIntent(t *testing.T) {
	pub := newMockPublisher()
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_ACTIVE,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1"}},
			}, nil
		},
	}

	hs, _ := newTestHandlerSet(t, mockChain, mb, nil, nil, pub)

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: handlerTestLeaseRetained,
		Tenant:    "tenant-a",
	})

	err := hs.HandleLeaseClosed(msg)
	require.NoError(t, err)

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()

	assert.Empty(t, msgs, "close must not emit a retained (or any) lease event on intent")
}

// TestHandlerSet_HandleLeaseExpired_DoesNotEmitRetainedOnIntent mirrors the
// close case for the expiry path (it delegates to processLeaseClose).
func TestHandlerSet_HandleLeaseExpired_DoesNotEmitRetainedOnIntent(t *testing.T) {
	pub := newMockPublisher()
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{}

	hs, _ := newTestHandlerSet(t, mockChain, mb, nil, nil, pub)

	msg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseExpired,
		LeaseUUID: handlerTestLeaseExpired,
	})

	err := hs.HandleLeaseExpired(msg)
	require.NoError(t, err)

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()

	assert.Empty(t, msgs, "expire must not emit a retained (or any) lease event on intent")
}

// --- HandleBackendCallback tests ---

func requireProvisionCallbackOperation(
	t testing.TB,
	tracker *testOperationRegistry,
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) operation.OperationID {
	t.Helper()
	store := tracker.callbackPlacementStore()
	require.NotNil(t, store)
	coordinator := tracker.coordinator
	require.NotNil(t, coordinator)
	snapshot, err := store.MintBackendRequestSnapshot(tenant, items)
	require.NoError(t, err)
	id := beginTestNewPlacementAttemptWithSnapshot(
		t, store, callbackProvisionCoordinator(t, store, coordinator, backendName),
		leaseUUID, backendName, operation.OperationID{},
		placement.PayloadFingerprint{}, snapshot,
	)
	if id.Valid() {
		return id
	}
	metadata, tracked := tracker.GetInFlight(leaseUUID)
	require.True(t, tracked, "accepted fixture operation must remain callback-settleable")
	require.True(t, metadata.OperationID.Valid())
	return metadata.OperationID
}

func TestHandlerSet_HandleBackendCallback_Success(t *testing.T) {
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return true, "tx-abc", nil
		},
	}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, ack, nil)
	operationID := requireProvisionCallbackOperation(
		t, tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Should be untracked after successful ack
	assert.False(t, tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandleBackendCallback_MetricsBackendCannotRedirectCurrentOperation(t *testing.T) {
	for _, status := range []backend.CallbackStatus{backend.CallbackStatusSuccess, backend.CallbackStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			var ackCalls, leaseReads int
			chainClient := &chaintest.MockClient{
				GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
					leaseReads++
					return &billingtypes.Lease{Uuid: handlerTestLeaseOne, State: billingtypes.LEASE_STATE_PENDING}, nil
				},
			}
			ack := &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
				ackCalls++
				return true, "tx", nil
			}}
			f := newPlacementTestFixture(t, chainClient, ack)
			generation := requireProvisionCallbackOperation(
				t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
			)

			err := f.hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
				LeaseUUID:   handlerTestLeaseOne,
				Backend:     "metrics-only-backend",
				Status:      status,
				Error:       "backend failure",
				OperationID: generation.String(),
			}))
			require.NoError(t, err)
			assert.False(t, f.tracker.IsInFlight(handlerTestLeaseOne))
			p := f.store.Lookup(handlerTestLeaseOne)
			switch status {
			case backend.CallbackStatusSuccess:
				assert.Equal(t, 1, ackCalls)
				assert.Zero(t, leaseReads)
				assert.Equal(t, "test-backend", p.Backend,
					"the tracked operation, not callback JSON, selects placement")
				assert.Empty(t, p.Attempt)
			case backend.CallbackStatusFailed:
				assert.Zero(t, ackCalls)
				assert.Equal(t, 1, leaseReads)
				assert.Equal(t, placement.StateConfirmed, p.State())
				assert.Equal(t, "test-backend", p.Backend,
					"a failed callback cannot erase an already accepted owner")
			}
		})
	}
}

func TestHandlerSet_HandleBackendCallback_StaleSameBackendGenerationCannotSettleCurrentOperation(t *testing.T) {
	for _, status := range []backend.CallbackStatus{backend.CallbackStatusSuccess, backend.CallbackStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			var ackCalls, leaseReads int
			chainClient := &chaintest.MockClient{GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
				leaseReads++
				return &billingtypes.Lease{Uuid: handlerTestLeaseOne, State: billingtypes.LEASE_STATE_PENDING}, nil
			}}
			ack := &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
				ackCalls++
				return true, "tx", nil
			}}
			f := newPlacementTestFixture(t, chainClient, ack)
			generation := requireProvisionCallbackOperation(
				t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
			)
			staleGeneration, err := operation.ParseID("d9428888-122b-41e1-b85c-61c67afba0c6")
			require.NoError(t, err)

			require.NoError(t, f.hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
				LeaseUUID:   handlerTestLeaseOne,
				Backend:     "test-backend",
				Status:      status,
				Error:       "stale failure",
				OperationID: staleGeneration.String(),
			})))
			assert.Zero(t, ackCalls)
			assert.Zero(t, leaseReads)
			current, exists := f.tracker.GetInFlight(handlerTestLeaseOne)
			require.True(t, exists)
			assert.Equal(t, generation, current.OperationID)
			currentPlacement := f.store.Lookup(handlerTestLeaseOne)
			assert.Equal(t, placement.StateConfirmed, currentPlacement.State())
			assert.Equal(t, "test-backend", currentPlacement.Backend)
		})
	}
}

func TestHandlerSet_HandleBackendCallback_MatchingGenerationSettlesOperation(t *testing.T) {
	ackCalls := 0
	f := newPlacementTestFixture(t, &chaintest.MockClient{}, &mockAcknowledger{
		acknowledgeFn: func(context.Context, string) (bool, string, error) {
			ackCalls++
			return true, "tx", nil
		},
	})
	generation := requireProvisionCallbackOperation(
		t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	require.NoError(t, f.hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Backend:     "test-backend",
		Status:      backend.CallbackStatusSuccess,
		OperationID: generation.String(),
	})))
	assert.Equal(t, 1, ackCalls)
	assert.False(t, f.tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandleBackendCallback_CancelsWhileSettlementClaimIsContended(t *testing.T) {
	ackEntered := make(chan struct{})
	releaseAck := make(chan struct{})
	f := newPlacementTestFixture(t, &chaintest.MockClient{}, &mockAcknowledger{
		acknowledgeFn: func(context.Context, string) (bool, string, error) {
			close(ackEntered)
			<-releaseAck
			return true, "tx", nil
		},
	})
	generation := requireProvisionCallbackOperation(
		t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- f.hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
			LeaseUUID: handlerTestLeaseOne, Backend: "test-backend",
			Status: backend.CallbackStatusSuccess, OperationID: generation.String(),
		}))
	}()
	<-ackEntered

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Backend:     "test-backend",
		Status:      backend.CallbackStatusSuccess,
		OperationID: generation.String(),
	})
	ctx, cancel := context.WithCancel(msg.Context())
	msg.SetContext(ctx)
	done := make(chan error, 1)
	go func() {
		done <- f.hs.HandleBackendCallback(msg)
	}()

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	close(releaseAck)
	require.NoError(t, <-firstDone)
	assert.False(t, f.tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandleBackendCallback_PanicReleasesSettlementClaim(t *testing.T) {
	ack := &mockAcknowledger{
		acknowledgeFn: func(context.Context, string) (bool, string, error) {
			panic("acknowledger panic")
		},
	}
	f := newPlacementTestFixture(t, &chaintest.MockClient{}, ack)
	generation := requireProvisionCallbackOperation(
		t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	assert.PanicsWithValue(t, "acknowledger panic", func() {
		_ = f.hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
			LeaseUUID:   handlerTestLeaseOne,
			Backend:     "test-backend",
			Status:      backend.CallbackStatusSuccess,
			OperationID: generation.String(),
		}))
	})
	ack.acknowledgeFn = func(context.Context, string) (bool, string, error) {
		return true, "tx", nil
	}
	require.NoError(t, f.hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: handlerTestLeaseOne, Backend: "test-backend",
		Status: backend.CallbackStatusSuccess, OperationID: generation.String(),
	})), "panic unwinding must release callback settlement ownership")
	assert.False(t, f.tracker.IsInFlight(handlerTestLeaseOne))
}

// TestHandlerSet_HandleBackendCallback_UntrackedRestore_SkipsAck documents the
// bug ENG-358 fixes: a restore callback whose lease was NEVER tracked in-flight
// falls into the non-in-flight (restart/update) branch and is NOT acknowledged
// here — the lease then waits for the reconciler. This is why RestoreLease must
// register the lease in-flight.
func TestHandlerSet_HandleBackendCallback_UntrackedRestore_SkipsAck(t *testing.T) {
	var ackCalls int
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			ackCalls++
			return true, "tx", nil
		},
	}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{}

	hs, _ := newTestHandlerSet(t, mockChain, mb, ack, nil)
	// Intentionally do NOT track the lease in-flight.

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: handlerTestLeaseUntracked,
		Status:    backend.CallbackStatusSuccess,
		Backend:   "test-backend",
	})

	err := hs.HandleBackendCallback(msg)
	require.NoError(t, err)
	assert.Equal(t, 0, ackCalls, "an untracked restore callback must NOT be acknowledged inline (the bug ENG-358 fixes)")
}

func TestHandlerSet_HandleBackendCallback_Success_TerminalAckError(t *testing.T) {
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return false, "", billingtypes.ErrLeaseNotPending
		},
	}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: handlerTestLeaseOne, State: billingtypes.LEASE_STATE_CLOSED,
			}, nil
		},
	}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, ack, nil)
	operationID := requireProvisionCallbackOperation(
		t, tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err, "terminal ack error should be treated as success")
	assert.False(t, tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandleBackendCallback_Success_TerminalAckError_ActiveLeasePublishesReadyEvent(t *testing.T) {
	pub := newMockPublisher()
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return false, "", billingtypes.ErrLeaseNotPending
		},
	}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{State: billingtypes.LEASE_STATE_ACTIVE}, nil
		},
	}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, ack, nil, pub)
	operationID := requireProvisionCallbackOperation(
		t, tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)
	pub.mu.Lock()
	pub.published[TopicLeaseEvent] = nil
	pub.mu.Unlock()

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()
	require.Len(t, msgs, 1, "should publish ready event even on terminal ack error")

	var event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
	assert.Equal(t, handlerTestLeaseOne, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, event.Status)
	assert.Empty(t, event.Error)
}

func TestHandlerSet_HandleBackendCallback_Success_TransientAckError(t *testing.T) {
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return false, "", errors.New("chain timeout")
		},
	}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, ack, nil)
	operationID := requireProvisionCallbackOperation(
		t, tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})

	err := hs.HandleBackendCallback(msg)
	require.Error(t, err, "should return error for retry")
	assert.ErrorIs(t, err, ErrAcknowledgeFailed)

	// Should still be in-flight for retry
	assert.True(t, tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandleBackendCallback_Failed_PendingLease(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	rejectCalled := false
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			rejectCalled = true
			assert.Equal(t, []string{handlerTestLeaseOne}, leaseUUIDs)
			assert.Equal(t, "container crash", reason)
			return 1, []string{"tx-rej"}, nil
		},
	}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, ack, nil)
	operationID := requireProvisionCallbackOperation(
		t, tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusFailed,
		Error:       "container crash",
		OperationID: operationID.String(),
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)
	assert.True(t, rejectCalled)
	assert.False(t, tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandleBackendCallback_Failed_ActiveLease(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, ack, nil)
	operationID := requireProvisionCallbackOperation(
		t, tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusFailed,
		Error:       "re-provision failed",
		OperationID: operationID.String(),
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err, "active lease failure should not error")

	// Should be untracked so reconciler can pick it up
	assert.False(t, tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandleBackendCallback_Failed_RejectFails(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return 0, nil, errors.New("chain error")
		},
	}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, ack, nil)
	operationID := requireProvisionCallbackOperation(
		t, tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusFailed,
		Error:       "failed",
		OperationID: operationID.String(),
	})

	err := hs.HandleBackendCallback(msg)
	require.Error(t, err, "should return error for retry")

	// Should still be in-flight to prevent reconciler race
	assert.True(t, tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandleBackendCallback_Failed_EmptyReason(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	var receivedReason string
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			receivedReason = reason
			return 1, nil, nil
		},
	}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, ack, nil)
	operationID := requireProvisionCallbackOperation(
		t, tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusFailed,
		Error:       "", // Empty
		OperationID: operationID.String(),
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)
	assert.Equal(t, "provisioning failed", receivedReason, "should use default reason")
}

func TestHandlerSet_HandleBackendCallback_UnknownLease(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{}

	hs, _ := newTestHandlerSet(t, mockChain, mb, ack, nil)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID: handlerTestLeaseUnknown,
		Status:    backend.CallbackStatusSuccess,
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err, "should ignore callback for unknown lease")
}

// TestHandlerSet_HandleBackendCallback_NonInFlight_PublishesEvent verifies that
// callbacks for non-in-flight leases (restart/update completions) publish a
// status event so WebSocket clients see the ready/failed transition.
func TestHandlerSet_HandleBackendCallback_NonInFlight_PublishesEvent(t *testing.T) {
	const (
		restartLease         = "00000000-0000-4000-8000-000000000101"
		updateLease          = "00000000-0000-4000-8000-000000000102"
		closedRetainedLease  = "00000000-0000-4000-8000-000000000103"
		closedDestroyedLease = "00000000-0000-4000-8000-000000000104"
		wrongAuthorityLease  = "00000000-0000-4000-8000-000000000105"
	)
	lifecycleAuthority := newTestPlacementAuthority(t)
	coordinator := seedTestTypedConfirmedPlacements(t, lifecycleAuthority, []string{"test-backend"}, map[string]string{
		restartLease:         "test-backend",
		updateLease:          "test-backend",
		closedRetainedLease:  "test-backend",
		closedDestroyedLease: "test-backend",
	})
	events := &callbackEventRecorder{}

	hs := composeTestHandlerSet(t, testHandlerDeps{
		Tracker:        newTestOperationRegistry(),
		Placement:      lifecycleAuthority,
		Coordinator:    coordinator,
		CallbackEvents: events,
	})
	resetEvents := func() {
		events.mu.Lock()
		events.events = nil
		events.mu.Unlock()
	}
	snapshotEvents := func() []backend.LeaseStatusEvent {
		events.mu.Lock()
		defer events.mu.Unlock()
		return append([]backend.LeaseStatusEvent(nil), events.events...)
	}

	t.Run("success_publishes_ready", func(t *testing.T) {
		resetEvents()

		msg := newCallbackMsg(t, backend.CallbackPayload{
			LeaseUUID:   restartLease,
			Status:      backend.CallbackStatusSuccess,
			LifecycleID: lifecycleAuthority.CurrentLifecycle(restartLease).ID().String(),
		})

		err := hs.HandleBackendCallback(msg)
		require.NoError(t, err)

		published := snapshotEvents()
		require.Len(t, published, 1)
		event := published[0]
		assert.Equal(t, restartLease, event.LeaseUUID)
		assert.Equal(t, backend.ProvisionStatusReady, event.Status)
	})

	t.Run("failed_publishes_failed", func(t *testing.T) {
		resetEvents()

		msg := newCallbackMsg(t, backend.CallbackPayload{
			LeaseUUID:   updateLease,
			Status:      backend.CallbackStatusFailed,
			Error:       "container crashed",
			LifecycleID: lifecycleAuthority.CurrentLifecycle(updateLease).ID().String(),
		})

		err := hs.HandleBackendCallback(msg)
		require.NoError(t, err)

		published := snapshotEvents()
		require.Len(t, published, 1)
		event := published[0]
		assert.Equal(t, updateLease, event.LeaseUUID)
		assert.Equal(t, backend.ProvisionStatusFailed, event.Status)
		assert.Equal(t, "container crashed", event.Error)
	})

	// ENG-329: a deprovisioned callback emits the retained notice on observed
	// ground truth — only when payload.Retained is true.
	t.Run("deprovisioned_retained_publishes_retained", func(t *testing.T) {
		resetEvents()

		msg := newCallbackMsg(t, backend.CallbackPayload{
			LeaseUUID:   closedRetainedLease,
			Status:      backend.CallbackStatusDeprovisioned,
			Retained:    true,
			LifecycleID: lifecycleAuthority.CurrentLifecycle(closedRetainedLease).ID().String(),
		})

		err := hs.HandleBackendCallback(msg)
		require.NoError(t, err)

		published := snapshotEvents()
		require.Len(t, published, 1, "retained deprovision must emit exactly one retained event")
		event := published[0]
		assert.Equal(t, closedRetainedLease, event.LeaseUUID)
		assert.Equal(t, backend.ProvisionStatusRetained, event.Status)
		assert.NotEmpty(t, event.Error, "retained event should carry an informational message")
	})

	t.Run("operation_scoped_deprovisioned_is_rejected", func(t *testing.T) {
		resetEvents()

		err := hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
			LeaseUUID:   wrongAuthorityLease,
			Backend:     "test-backend",
			Status:      backend.CallbackStatusDeprovisioned,
			Retained:    true,
			OperationID: "123e4567-e89b-42d3-a456-426614174000",
		}))
		require.ErrorContains(t, err, "requires lifecycle or legacy authority")

		assert.Empty(t, snapshotEvents(),
			"a wrong-authority callback must not become a lifecycle observation")
	})

	t.Run("deprovisioned_not_retained_publishes_nothing", func(t *testing.T) {
		resetEvents()

		msg := newCallbackMsg(t, backend.CallbackPayload{
			LeaseUUID:   closedDestroyedLease,
			Status:      backend.CallbackStatusDeprovisioned,
			Retained:    false,
			LifecycleID: lifecycleAuthority.CurrentLifecycle(closedDestroyedLease).ID().String(),
		})

		err := hs.HandleBackendCallback(msg)
		require.NoError(t, err)

		assert.Empty(t, snapshotEvents(), "non-retain deprovision must not emit any lease event")
	})
}

func TestHandlerSet_HandleBackendCallback_GenerationScopedNonInFlightCallbacksAreIgnored(t *testing.T) {
	tests := []struct {
		name        string
		status      backend.CallbackStatus
		callbackErr string
	}{
		{
			name:   "success",
			status: backend.CallbackStatusSuccess,
		},
		{
			name:        "failure",
			status:      backend.CallbackStatusFailed,
			callbackErr: "custom-domain redeploy failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chainReads := 0
			chainRejects := 0
			chainClient := &chaintest.MockClient{
				GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
					chainReads++
					return &billingtypes.Lease{Uuid: handlerTestLeaseOne, State: billingtypes.LEASE_STATE_ACTIVE}, nil
				},
				RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
					chainRejects++
					return 1, []string{"tx-reject"}, nil
				},
			}
			ackCalls := 0
			ack := &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
				ackCalls++
				return false, "", billingtypes.ErrLeaseNotPending
			}}
			tracker := newTestOperationRegistry()
			olderGeneration, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174000")
			require.NoError(t, err)
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, []string{"test-backend"})
			placementBefore := store.Lookup(handlerTestLeaseOne)
			orch := newTestProvisionOrchestrator(t, "provider-1", "http://callback", &mockBackendRouter{}, tracker, store)
			pub := newMockPublisher()
			hs := composeTestHandlerSet(t, testHandlerDeps{
				ChainClient:  chainClient,
				Orchestrator: orch,
				Placement:    tracker.callbackStore,
				Tracker:      tracker,
				Acknowledger: ack,
				Publisher:    pub,
			})
			// Once no exact operation is tracked fred cannot distinguish a delayed
			// old result from a legitimate current status. The callback is therefore
			// observation-only and cannot publish an out-of-order terminal event.
			require.NoError(t, hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
				LeaseUUID:   handlerTestLeaseOne,
				Backend:     "test-backend",
				Status:      tt.status,
				Error:       tt.callbackErr,
				OperationID: olderGeneration.String(),
			})))

			assert.Zero(t, chainReads, "a stale callback must not inspect chain state")
			assert.Zero(t, chainRejects, "a stale callback must not reject the lease")
			assert.Zero(t, ackCalls, "a stale callback must not acknowledge the lease")
			assert.Equal(t, placementBefore, store.Lookup(handlerTestLeaseOne),
				"a callback without an in-flight claim must not mutate placement")

			pub.mu.Lock()
			msgs := append([]*message.Message(nil), pub.published[TopicLeaseEvent]...)
			pub.mu.Unlock()
			assert.Empty(t, msgs)
		})
	}
}

func TestHandlerSet_HandleBackendCallback_LateOperationDeprovisionIsRejected(t *testing.T) {
	tracker := newTestOperationRegistry()
	orch := newTestProvisionOrchestrator(t, "provider-1", "http://callback", &mockBackendRouter{}, tracker, nil)
	pub := newMockPublisher()
	hs := composeTestHandlerSet(t, testHandlerDeps{
		Orchestrator: orch,
		Placement:    tracker.callbackStore,
		Tracker:      tracker,
		Publisher:    pub,
	})

	err := hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Backend:     "backend-a",
		Status:      backend.CallbackStatusDeprovisioned,
		Retained:    true,
		OperationID: "123e4567-e89b-42d3-a456-426614174000",
	}))
	require.ErrorContains(t, err, "requires lifecycle or legacy authority")

	pub.mu.Lock()
	msgs := append([]*message.Message(nil), pub.published[TopicLeaseEvent]...)
	pub.mu.Unlock()
	assert.Empty(t, msgs)
}

func TestHandlerSet_HandleBackendCallback_UnknownStatus(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, ack, nil)
	operationID := requireProvisionCallbackOperation(
		t, tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      "weird-status",
		OperationID: operationID.String(),
	})

	err := hs.HandleBackendCallback(msg)
	require.ErrorContains(t, err, "invalid status")

	assert.True(t, tracker.IsInFlight(handlerTestLeaseOne),
		"a structurally invalid callback must not consume the current operation")
}

// --- HandlePayloadReceived tests ---

func TestHandlerSet_HandlePayloadReceived_Success(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	payloadData := []byte(`{"image":"nginx:latest"}`)
	payloadHash := sha256.Sum256(payloadData)
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: payloadHash[:],
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store(handlerTestLeaseOne, payloadData)

	hs, tracker := newTestHandlerSet(t, mockChain, mb, nil, ps)

	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-a",
		MetaHashHex: hashPayload(payloadData),
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err)

	mb.mu.Lock()
	require.Len(t, mb.provisionCalls, 1)
	req := mb.provisionCalls[0]
	mb.mu.Unlock()

	assert.Equal(t, payloadData, req.Payload)
	assert.True(t, tracker.IsInFlight(handlerTestLeaseOne))
}

func TestHandlerSet_HandlePayloadReceived_Success_PublishesProvisioningEvent(t *testing.T) {
	pub := newMockPublisher()
	mb := &mockManagerBackend{name: "test-backend"}
	payloadData := []byte(`{"image":"nginx:latest"}`)
	payloadHash := sha256.Sum256(payloadData)
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: payloadHash[:],
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store(handlerTestLeaseOne, payloadData)

	hs, _ := newTestHandlerSet(t, mockChain, mb, nil, ps, pub)

	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-a",
		MetaHashHex: hashPayload(payloadData),
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err)

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()
	require.Len(t, msgs, 1, "should publish provisioning event")

	var event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
	assert.Equal(t, handlerTestLeaseOne, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusProvisioning, event.Status)
	assert.Empty(t, event.Error)
}

func TestHandlerSet_HandlePayloadReceived_NilPayloadStore(t *testing.T) {
	mockChain := &chaintest.MockClient{}
	hs, _ := newTestHandlerSet(t, mockChain, nil, nil, nil)

	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID: handlerTestLeaseOne,
	})

	err := hs.HandlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil when payload store is nil")
}

func TestHandlerSet_HandlePayloadReceived_LeaseNotFoundPreservesPayload(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return nil, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store(handlerTestLeaseOne, []byte("data"))

	hs, _ := newTestHandlerSet(t, mockChain, nil, nil, ps)
	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID: handlerTestLeaseOne,
		Tenant:    "tenant-a",
	})

	err = hs.HandlePayloadReceived(msg)
	assert.Error(t, err, "unknown chain absence must be retried")
	hasPayload, err := ps.Has(handlerTestLeaseOne)
	require.NoError(t, err)
	assert.True(t, hasPayload, "unknown chain absence is not terminal evidence")
}

func TestHandlerSet_HandlePayloadReceived_ActiveLeasePreservesPayload(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_ACTIVE, // Not pending
			}, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store(handlerTestLeaseOne, []byte("data"))

	hs, _ := newTestHandlerSet(t, mockChain, nil, nil, ps)
	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID: handlerTestLeaseOne,
		Tenant:    "tenant-a",
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err)
	hasPayload, err := ps.Has(handlerTestLeaseOne)
	require.NoError(t, err)
	assert.True(t, hasPayload, "ACTIVE recovery requires the durable manifest")
}

func TestHandlerSet_HandlePayloadReceived_DeletesOnlyTerminalLeasePayload(t *testing.T) {
	for _, state := range []billingtypes.LeaseState{
		billingtypes.LEASE_STATE_CLOSED,
		billingtypes.LEASE_STATE_REJECTED,
		billingtypes.LEASE_STATE_EXPIRED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			mockChain := &chaintest.MockClient{
				GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
					return &billingtypes.Lease{Uuid: handlerTestLeaseOne, State: state}, nil
				},
			}
			ps, err := payload.NewStore(payload.StoreConfig{
				DBPath: filepath.Join(t.TempDir(), "payloads.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, ps.Close()) })
			require.True(t, ps.Store(handlerTestLeaseOne, []byte("data")))
			hs, _ := newTestHandlerSet(t, mockChain, nil, nil, ps)

			err = hs.HandlePayloadReceived(newPayloadEventMsg(t, payload.Event{
				LeaseUUID: handlerTestLeaseOne,
				Tenant:    "tenant-a",
			}))

			require.NoError(t, err)
			hasPayload, hasErr := ps.Has(handlerTestLeaseOne)
			require.NoError(t, hasErr)
			assert.False(t, hasPayload)
		})
	}
}

func TestHandlerSet_HandlePayloadReceived_ChainError(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok, "payload chain confirmation must be bounded")
			require.LessOrEqual(t, time.Until(deadline), chainConfirmTimeout)
			return nil, errors.New("chain error")
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store(handlerTestLeaseOne, []byte("data"))

	hs, _ := newTestHandlerSet(t, mockChain, nil, nil, ps)
	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID: handlerTestLeaseOne,
		Tenant:    "tenant-a",
	})

	err = hs.HandlePayloadReceived(msg)
	assert.Error(t, err, "should return error for retry")

	// Payload should be preserved for retry
	hasPayloadRetry, errRetry := ps.Has(handlerTestLeaseOne)
	require.NoError(t, errRetry)
	assert.True(t, hasPayloadRetry)
}

func TestHandlerSet_HandlePayloadReceived_HashMismatch(t *testing.T) {
	rejected := false
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			rejected = true
			assert.Equal(t, []string{handlerTestLeaseOne}, leaseUUIDs)
			assert.Equal(t, "payload corrupted", reason)
			return 1, []string{"tx-rej"}, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store(handlerTestLeaseOne, []byte("data"))

	hs, _ := newTestHandlerSet(t, mockChain, nil, nil, ps)
	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID:   handlerTestLeaseOne,
		Tenant:      "tenant-a",
		MetaHashHex: "0000000000000000000000000000000000000000000000000000000000000000",
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err, "should return nil after rejecting the lease")
	assert.True(t, rejected, "lease should be rejected on-chain")
	hasPayloadHash, errHash := ps.Has(handlerTestLeaseOne)
	require.NoError(t, errHash)
	assert.False(t, hasPayloadHash, "payload should be deleted after successful rejection")
}

func TestHandlerSet_HandlePayloadReceived_ValidationError_PublishesFailedEvent(t *testing.T) {
	pub := newMockPublisher()
	payloadData := []byte(`{"image":"evil.io/malware"}`)
	payloadHash := sha256.Sum256(payloadData)
	_, backendClient := provisionResponseBackendForTest(
		t, "test-backend", http.StatusBadRequest,
		`{"error":"image not allowed: evil.io/malware","validation_code":"image_not_allowed"}`,
	)
	rejectCalled := false
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: payloadHash[:],
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			rejectCalled = true
			assert.Equal(t, []string{handlerTestLeaseValidation}, leaseUUIDs)
			assert.Equal(t, "image not allowed", reason)
			return 1, []string{"tx-rej"}, nil
		},
	}

	tempDir := t.TempDir()
	ps, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(tempDir, "payloads.db"),
	})
	require.NoError(t, err)
	defer ps.Close()

	ps.Store(handlerTestLeaseValidation, payloadData)

	hs, _ := newTestHandlerSetWithBackend(t, mockChain, backendClient, nil, ps, pub)

	msg := newPayloadEventMsg(t, payload.Event{
		LeaseUUID:   handlerTestLeaseValidation,
		Tenant:      "tenant-a",
		MetaHashHex: hashPayload(payloadData),
	})

	err = hs.HandlePayloadReceived(msg)
	assert.NoError(t, err)
	assert.True(t, rejectCalled, "lease should be rejected on chain")

	// Payload should be cleaned up
	hasPayload, err := ps.Has(handlerTestLeaseValidation)
	require.NoError(t, err)
	assert.False(t, hasPayload, "payload should be deleted after validation error")

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()
	require.Len(t, msgs, 2, "starting must precede the synchronous failure event")

	var starting, event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &starting))
	require.NoError(t, json.Unmarshal(msgs[1].Payload, &event))
	assert.Equal(t, backend.ProvisionStatusProvisioning, starting.Status)
	assert.Equal(t, handlerTestLeaseValidation, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, event.Status)
	assert.Equal(t, "image not allowed", event.Error)
}

// --- truncateRejectReason tests ---

func TestTruncateRejectReason(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{"short string unchanged", "short error", "short error"},
		{"empty string unchanged", "", ""},
		{"exactly 256 bytes unchanged", strings.Repeat("a", 256), strings.Repeat("a", 256)},
		{"257 bytes truncated", strings.Repeat("a", 257), strings.Repeat("a", 253) + "..."},
		{"500 bytes truncated", strings.Repeat("b", 500), strings.Repeat("b", 253) + "..."},
		// "é" is 2 bytes (0xC3 0xA9). 128 runes = 256 bytes fits exactly.
		{"multibyte exactly at limit", strings.Repeat("\u00e9", 128), strings.Repeat("\u00e9", 128)},
		// 129 "é" = 258 bytes > 256. Truncated: must back up to rune boundary.
		// 253 bytes / 2 = 126 full runes (252 bytes) + "..." (3 bytes) = 255 bytes.
		{"multibyte over limit", strings.Repeat("\u00e9", 129), strings.Repeat("\u00e9", 126) + "..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateRejectReason(tt.input)
			assert.Equal(t, tt.expect, result)
			assert.LessOrEqual(t, len(result), maxRejectReasonLen, "must fit on-chain byte limit")
			assert.True(t, utf8.ValidString(result), "result should be valid UTF-8")
		})
	}
}

func TestValidationErrorToRejectReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		// Direct backend errors (docker backend path)
		{"unknown SKU direct", fmt.Errorf("%w: gpu-xl (profile: gpu-xl)", backend.ErrUnknownSKU), rejectReasonInvalidSKU},
		{"invalid manifest direct", fmt.Errorf("%w: %w", backend.ErrInvalidManifest, errors.New("unexpected end of JSON input")), rejectReasonInvalidManifest},
		{"image not allowed direct", fmt.Errorf("%w: registry %q; allowed registries: %v", backend.ErrImageNotAllowed, "evil.io", []string{"docker.io"}), rejectReasonImageNotAllowed},
		// Nested wrapping (e.g., docker backend wraps config error which wraps sentinel)
		{"unknown SKU nested", fmt.Errorf("%w: %w", backend.ErrValidation, fmt.Errorf("%w: bad-sku", backend.ErrUnknownSKU)), rejectReasonInvalidSKU},
		{"invalid manifest nested", fmt.Errorf("%w: %w", backend.ErrValidation, fmt.Errorf("%w: bad yaml", backend.ErrInvalidManifest)), rejectReasonInvalidManifest},
		{"image not allowed nested", fmt.Errorf("%w: %w", backend.ErrValidation, fmt.Errorf("%w: evil.io/malware", backend.ErrImageNotAllowed)), rejectReasonImageNotAllowed},
		// Catch-all
		{"unknown error", fmt.Errorf("%w: something unexpected", backend.ErrValidation), rejectReasonValidationError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, validationErrorToRejectReason(tt.err))
		})
	}
}

func TestHandlerSet_HandleBackendCallback_LongReasonTruncated(t *testing.T) {
	ack := &mockAcknowledger{}
	mb := &mockManagerBackend{name: "test-backend"}
	var receivedReason string
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:  leaseUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			receivedReason = reason
			return 1, nil, nil
		},
	}

	hs, tracker := newTestHandlerSet(t, mockChain, mb, ack, nil)
	operationID := requireProvisionCallbackOperation(
		t, tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	longReason := strings.Repeat("x", 500)
	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusFailed,
		Error:       longReason,
		OperationID: operationID.String(),
	})

	err := hs.HandleBackendCallback(msg)
	assert.NoError(t, err)
	assert.LessOrEqual(t, len(receivedReason), maxRejectReasonLen,
		"rejection reason should be truncated to fit on-chain limit")
	assert.True(t, strings.HasSuffix(receivedReason, "..."),
		"truncated reason should end with ellipsis")
}

// placementTestFixture holds the shared setup for placement-related callback tests.
type placementTestFixture struct {
	hs      *HandlerSet
	tracker *testOperationRegistry
	ps      *mockPlacementStore
	store   *placement.Store
	mb      *mockManagerBackend
}

// newPlacementTestFixture creates a HandlerSet wired with a mockPlacementStore.
func newPlacementTestFixture(
	t testing.TB,
	chainClient *chaintest.MockClient,
	ack *mockAcknowledger,
) placementTestFixture {
	mb := &mockManagerBackend{name: "test-backend"}
	ps := &mockPlacementStore{}
	tracker := newTestOperationRegistry()
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
		getBackendByNameFn: func(name string) backend.Backend {
			if name == mb.name {
				return mb
			}
			return nil
		},
		backendsFn: func() []backend.Backend { return []backend.Backend{mb} },
	}
	orch := newTestProvisionOrchestrator(t, "prov-1", "http://localhost:8080", router, tracker, ps)
	store := tracker.callbackStore
	require.NotNil(t, store)
	hs := composeTestHandlerSet(t, testHandlerDeps{
		ChainClient:  chainClient,
		Orchestrator: orch,
		Placement:    store,
		Tracker:      tracker,
		Acknowledger: ack,
	})
	return placementTestFixture{hs: hs, tracker: tracker, ps: ps, store: store, mb: mb}
}

func TestHandlerSet_HandleBackendCallback_Failed_PendingLease_CleansUpPlacement(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{Uuid: leaseUUID, State: billingtypes.LEASE_STATE_PENDING}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return 1, []string{"tx-rej"}, nil
		},
	}

	f := newPlacementTestFixture(t, mockChain, &mockAcknowledger{})
	requireSetPlacementAttempt(t, f.ps, handlerTestLeaseOne, "test-backend")
	operationID := requireProvisionCallbackOperation(
		t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusFailed,
		Error:       "container crash",
		OperationID: operationID.String(),
	})

	err := f.hs.HandleBackendCallback(msg)
	assert.NoError(t, err)
	assert.False(t, f.tracker.IsInFlight(handlerTestLeaseOne))
	assert.Empty(t, f.ps.Get(handlerTestLeaseOne), "placement should be deleted after rejection")
}

func TestHandlerSet_HandleBackendCallback_Failed_RejectFails_PreservesPlacement(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{Uuid: leaseUUID, State: billingtypes.LEASE_STATE_PENDING}, nil
		},
		RejectLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			return 0, nil, errors.New("chain error")
		},
	}

	f := newPlacementTestFixture(t, mockChain, &mockAcknowledger{})
	f.ps.Set(handlerTestLeaseOne, "test-backend")
	operationID := requireProvisionCallbackOperation(
		t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusFailed,
		Error:       "failed",
		OperationID: operationID.String(),
	})

	err := f.hs.HandleBackendCallback(msg)
	require.Error(t, err, "should return error for retry")

	// Placement must be preserved so the retry can still find the backend
	assert.True(t, f.tracker.IsInFlight(handlerTestLeaseOne), "should stay in-flight for retry")
	assert.Equal(t, "test-backend", f.ps.Get(handlerTestLeaseOne), "placement should be preserved when reject fails")
}

func TestHandlerSet_HandleBackendCallback_Failed_ActiveLease_PreservesPlacement(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{Uuid: leaseUUID, State: billingtypes.LEASE_STATE_ACTIVE}, nil
		},
	}

	f := newPlacementTestFixture(t, mockChain, &mockAcknowledger{})
	f.ps.Set(handlerTestLeaseOne, "test-backend")
	operationID := requireProvisionCallbackOperation(
		t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusFailed,
		Error:       "re-provision failed",
		OperationID: operationID.String(),
	})

	err := f.hs.HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Placement must be preserved — reconciler needs it to find the backend
	assert.False(t, f.tracker.IsInFlight(handlerTestLeaseOne), "should be untracked for reconciler")
	assert.Equal(t, "test-backend", f.ps.Get(handlerTestLeaseOne), "placement should be preserved for active lease")
}

func TestHandlerSet_HandleBackendCallback_Success_PreservesPlacement(t *testing.T) {
	mockChain := &chaintest.MockClient{}
	ack := &mockAcknowledger{
		acknowledgeFn: func(ctx context.Context, leaseUUID string) (bool, string, error) {
			return true, "tx-abc", nil
		},
	}

	f := newPlacementTestFixture(t, mockChain, ack)
	f.ps.Set(handlerTestLeaseOne, "test-backend")
	operationID := requireProvisionCallbackOperation(
		t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})

	err := f.hs.HandleBackendCallback(msg)
	assert.NoError(t, err)

	// Placement must be preserved — the lease is now ACTIVE and the container
	// could crash later, requiring reads/re-provision from the same backend.
	assert.False(t, f.tracker.IsInFlight(handlerTestLeaseOne))
	assert.Equal(t, "test-backend", f.ps.Get(handlerTestLeaseOne), "placement should be preserved after success")
}

func TestHandlerSet_HandleBackendCallback_Success_RepairsAttemptBeforeAcknowledge(t *testing.T) {
	ackCalled := false
	ack := &mockAcknowledger{acknowledgeFn: func(context.Context, string) (bool, string, error) {
		ackCalled = true
		return true, "tx-abc", nil
	}}
	f := newPlacementTestFixture(t, &chaintest.MockClient{}, ack)
	operationID := requireProvisionCallbackOperation(
		t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	err := f.hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusSuccess,
		Backend:     "test-backend",
		OperationID: operationID.String(),
	}))
	require.NoError(t, err)
	assert.True(t, ackCalled)
	p := f.store.Lookup(handlerTestLeaseOne)
	assert.Equal(t, placement.StateConfirmed, p.State())
	assert.Equal(t, "test-backend", p.Backend)
	assert.Empty(t, p.Attempt)
}

func TestHandlerSet_HandleBackendCallback_PendingFailure_DoesNotDeleteDifferentOwner(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{Uuid: handlerTestLeaseOne, State: billingtypes.LEASE_STATE_PENDING}, nil
		},
		RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
			return 1, []string{"tx-rej"}, nil
		},
	}
	f := newPlacementTestFixture(t, mockChain, &mockAcknowledger{})
	require.NoError(t, f.ps.Set(handlerTestLeaseOne, "newer-backend"))
	operationID := requireProvisionCallbackOperation(
		t, f.tracker, handlerTestLeaseOne, "tenant-a", testItems("sku-1"), "test-backend",
	)

	require.NoError(t, f.hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusFailed,
		Backend:     "test-backend",
		OperationID: operationID.String(),
	})))
	p := f.ps.Lookup(handlerTestLeaseOne)
	assert.Equal(t, placement.StateConfirmed, p.State())
	assert.Equal(t, "newer-backend", p.Backend, "stale callback must not delete newer ownership")
}

func TestHandlerSet_HandleBackendCallback_NonInFlightCallbackCannotSettleAttempt(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000000106"
	for _, generation := range []string{"", "123e4567-e89b-42d3-a456-426614174000"} {
		for _, status := range []backend.CallbackStatus{backend.CallbackStatusSuccess, backend.CallbackStatusFailed} {
			name := fmt.Sprintf("operation_%s/%s", generation, status)
			t.Run(name, func(t *testing.T) {
				f := newPlacementTestFixture(t, &chaintest.MockClient{}, &mockAcknowledger{})
				requireSetPlacementAttempt(t, f.ps, leaseUUID, "test-backend")

				require.NoError(t, f.hs.HandleBackendCallback(newCallbackMsg(t, backend.CallbackPayload{
					LeaseUUID:   leaseUUID,
					Status:      status,
					Backend:     "test-backend",
					OperationID: generation,
				})))
				p := f.ps.Lookup(leaseUUID)
				assert.Equal(t, placement.StateAttempting, p.State(),
					"a delayed non-in-flight callback must not settle a newer attempt")
				assert.Equal(t, "test-backend", p.Attempt)
				assert.Empty(t, p.Backend)
			})
		}
	}
}

// --- publishLeaseEvent tests ---

// mockPublisher implements message.Publisher for testing publishLeaseEvent.
type mockPublisher struct {
	mu         sync.Mutex
	published  map[string][]*message.Message // topic → messages
	publishErr error
}

func newMockPublisher() *mockPublisher {
	return &mockPublisher{published: make(map[string][]*message.Message)}
}

func (p *mockPublisher) Publish(topic string, messages ...*message.Message) error {
	if p.publishErr != nil {
		return p.publishErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published[topic] = append(p.published[topic], messages...)
	return nil
}

func (p *mockPublisher) Close() error { return nil }

func TestPublishLeaseEvent_PublishesToTopic(t *testing.T) {
	pub := newMockPublisher()
	hs := composeTestHandlerSet(t, testHandlerDeps{
		Publisher: pub,
	})

	hs.publishLeaseEvent(handlerTestLeaseOne, backend.ProvisionStatusReady, "")

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()

	require.Len(t, msgs, 1, "should publish exactly one message")

	var event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
	assert.Equal(t, handlerTestLeaseOne, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, event.Status)
	assert.Empty(t, event.Error)
	assert.False(t, event.Timestamp.IsZero(), "timestamp should be set")
}

func TestPublishLeaseEvent_IncludesError(t *testing.T) {
	pub := newMockPublisher()
	hs := composeTestHandlerSet(t, testHandlerDeps{
		Publisher: pub,
	})

	hs.publishLeaseEvent(handlerTestLeaseTwo, backend.ProvisionStatusFailed, "container crashed")

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()

	require.Len(t, msgs, 1)

	var event backend.LeaseStatusEvent
	require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
	assert.Equal(t, handlerTestLeaseTwo, event.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, event.Status)
	assert.Equal(t, "container crashed", event.Error)
}

func TestPublishLeaseEvent_NilPublisher(t *testing.T) {
	hs := composeTestHandlerSet(t, testHandlerDeps{
		Publisher: nil,
	})

	// Should not panic
	hs.publishLeaseEvent(handlerTestLeaseOne, backend.ProvisionStatusReady, "")
}

func TestPublishLeaseEvent_PublishError(t *testing.T) {
	pub := newMockPublisher()
	pub.publishErr = errors.New("pubsub down")
	hs := composeTestHandlerSet(t, testHandlerDeps{
		Publisher: pub,
	})

	// Should not panic — publish errors are logged, not propagated
	hs.publishLeaseEvent(handlerTestLeaseOne, backend.ProvisionStatusReady, "")
}

// --- Metric tests ---

func TestHandlerSet_HandleBackendCallback_ExactNonInFlightIsIgnoredByBoundAuthority(t *testing.T) {
	// Counter is labeled {backend, status}; the payload below lacks a Backend
	// field so sanitizeBackendName collapses it to "unknown".
	labeled := metrics.NonInFlightCallbacksTotal.WithLabelValues("unknown", "success")
	before := promtestutil.ToFloat64(labeled)

	hs := composeTestHandlerSet(t, testHandlerDeps{
		Tracker: newTestOperationRegistry(),
	})

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseNotInFlight,
		Status:      backend.CallbackStatusSuccess,
		OperationID: "123e4567-e89b-42d3-a456-426614174000",
	})

	err := hs.HandleBackendCallback(msg)
	require.NoError(t, err)

	after := promtestutil.ToFloat64(labeled)
	assert.Equal(t, before+1, after,
		"a stale exact callback is ignored only after the bound store proves no operation exists")
}

// TestHandleBackendCallback_DeprovisionedNonInFlight verifies that the new
// deprovisioned status increments the metric with the correct labels and
// does NOT publish a lease event (the lease is torn down, no transition to
// re-surface).
func TestHandleBackendCallback_DeprovisionedOperationIsRejected(t *testing.T) {
	labeled := metrics.NonInFlightCallbacksTotal.WithLabelValues("docker", "deprovisioned")
	before := promtestutil.ToFloat64(labeled)

	knownBackend := &mockManagerBackend{name: "docker"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == "docker" {
				return knownBackend
			}
			return nil
		},
	}
	pub := newMockPublisher()
	hs := composeTestHandlerSet(t, testHandlerDeps{
		Tracker:       newTestOperationRegistry(),
		Publisher:     pub,
		BackendRouter: router,
	})

	msg := newCallbackMsg(t, backend.CallbackPayload{
		LeaseUUID:   handlerTestLeaseOne,
		Status:      backend.CallbackStatusDeprovisioned,
		Backend:     "docker",
		OperationID: "123e4567-e89b-42d3-a456-426614174000",
	})
	require.ErrorContains(t, hs.HandleBackendCallback(msg),
		"requires lifecycle or legacy authority")

	assert.Equal(t, before, promtestutil.ToFloat64(labeled),
		"protocol-invalid callbacks must not enter non-in-flight accounting")

	pub.mu.Lock()
	msgs := pub.published[TopicLeaseEvent]
	pub.mu.Unlock()
	assert.Empty(t, msgs, "deprovisioned must not publish a lease event")
}

// TestHandleBackendCallback_SanitizesLabels verifies that unknown/missing
// backend names are collapsed to sentinel labels, bounding Prometheus
// cardinality against misbehaving senders. Callback status has already crossed
// the closed-enum CallbackCommand boundary. The backend allowlist is the set of
// backends known to the router.
func TestHandleBackendCallback_SanitizesLabels(t *testing.T) {
	knownBackend := &mockManagerBackend{name: "docker"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == "docker" {
				return knownBackend
			}
			return nil
		},
	}

	tests := []struct {
		name        string
		payloadBE   string
		payloadStat backend.CallbackStatus
		wantBackend string
		wantStatus  string
	}{
		{"empty backend routes to unknown", "", backend.CallbackStatusSuccess, "unknown", "success"},
		{"unrecognized backend routes to invalid", "not-configured", backend.CallbackStatusSuccess, "invalid", "success"},
		{"regex-valid but unknown backend routes to invalid", "docker-prod-02", backend.CallbackStatusSuccess, "invalid", "success"},
		{"known backend preserved", "docker", backend.CallbackStatusSuccess, "docker", "success"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			labeled := metrics.NonInFlightCallbacksTotal.WithLabelValues(tc.wantBackend, tc.wantStatus)
			before := promtestutil.ToFloat64(labeled)

			hs := composeTestHandlerSet(t, testHandlerDeps{
				Tracker:       newTestOperationRegistry(),
				Publisher:     newMockPublisher(),
				BackendRouter: router,
				Placement:     newTestPlacementAuthority(t),
			})
			msg := newCallbackMsg(t, backend.CallbackPayload{
				LeaseUUID:   handlerTestLeaseOne,
				Status:      tc.payloadStat,
				Backend:     tc.payloadBE,
				OperationID: "123e4567-e89b-42d3-a456-426614174000",
			})
			require.NoError(t, hs.HandleBackendCallback(msg))

			assert.Equal(t, 1.0, promtestutil.ToFloat64(labeled)-before)
		})
	}
}

func TestHandlerSet_LeasesAwaitingGauge_MatchesMapSize(t *testing.T) {
	// Two leases with MetaHash → both should be awaiting payload
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01, 0x02},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	hs, _ := newTestHandlerSet(t, mockChain, nil, nil, nil)

	// Create two leases awaiting payload
	for _, id := range []string{handlerTestLeaseOne, handlerTestLeaseTwo} {
		msg := newLeaseEventMsg(t, chain.LeaseEvent{
			Type:      chain.LeaseCreated,
			LeaseUUID: id,
			Tenant:    "tenant-a",
		})
		err := hs.HandleLeaseCreated(msg)
		assert.NoError(t, err)
	}
	assert.Equal(t, 2.0, promtestutil.ToFloat64(metrics.LeasesAwaitingPayload))

	// Close one lease → gauge should drop
	mockChain.GetLeaseFunc = func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		return nil, nil
	}
	closeMsg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: handlerTestLeaseOne,
		Tenant:    "tenant-a",
	})
	err := hs.HandleLeaseClosed(closeMsg)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.LeasesAwaitingPayload))

	// Close same lease again → gauge unchanged (idempotent)
	err = hs.HandleLeaseClosed(closeMsg)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.LeasesAwaitingPayload))

	// Close the other lease
	closeMsg2 := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: handlerTestLeaseTwo,
		Tenant:    "tenant-a",
	})
	err = hs.HandleLeaseClosed(closeMsg2)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.LeasesAwaitingPayload))
}

func TestHandlerSet_LeasesAwaitingGauge_DuplicateLeaseCreatedDoesNotDrift(t *testing.T) {
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01, 0x02},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	hs, _ := newTestHandlerSet(t, mockChain, nil, nil, nil)

	// Send the same lease-created event twice (Watermill retry)
	for range 2 {
		msg := newLeaseEventMsg(t, chain.LeaseEvent{
			Type:      chain.LeaseCreated,
			LeaseUUID: handlerTestLeaseDuplicate,
			Tenant:    "tenant-a",
		})
		err := hs.HandleLeaseCreated(msg)
		assert.NoError(t, err)
	}

	// With set-based gauge, duplicates don't cause drift
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.LeasesAwaitingPayload))

	// Single close should bring gauge to 0
	mockChain.GetLeaseFunc = func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		return nil, nil
	}
	closeMsg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseClosed,
		LeaseUUID: handlerTestLeaseDuplicate,
		Tenant:    "tenant-a",
	})
	err := hs.HandleLeaseClosed(closeMsg)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.LeasesAwaitingPayload))
}

func TestHandlerSet_LeasesAwaitingGauge_PayloadReceivedDecrementsGauge(t *testing.T) {
	store, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payload.db"),
	})
	require.NoError(t, err)
	defer store.Close()

	// Store payload so HandlePayloadReceived can read it
	ok := store.Store(handlerTestLeasePayload, []byte("manifest-data"))
	require.True(t, ok, "failed to store payload for test")

	mb := &mockManagerBackend{name: "test-backend"}
	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:     leaseUUID,
				Tenant:   "tenant-a",
				State:    billingtypes.LEASE_STATE_PENDING,
				MetaHash: []byte{0x01, 0x02},
				Items:    []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
			}, nil
		},
	}

	hs, _ := newTestHandlerSet(t, mockChain, mb, nil, store)

	// Create lease awaiting payload
	createMsg := newLeaseEventMsg(t, chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: handlerTestLeasePayload,
		Tenant:    "tenant-a",
	})
	err = hs.HandleLeaseCreated(createMsg)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.LeasesAwaitingPayload))

	// Payload received → gauge should drop
	payMsg := newLeaseEventMsg_raw(t, payload.Event{
		LeaseUUID: handlerTestLeasePayload,
		Tenant:    "tenant-a",
	})
	err = hs.HandlePayloadReceived(payMsg)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.LeasesAwaitingPayload))
}

// --- Helper ---

// newCallbackMsg creates a Watermill message from a CallbackPayload.
func newCallbackMsg(t *testing.T, payload backend.CallbackPayload) *message.Message {
	t.Helper()
	return newLeaseEventMsg_raw(t, payload)
}

// newLeaseEventMsg_raw creates a Watermill message from any JSON-serializable value.
func newLeaseEventMsg_raw(t *testing.T, v any) *message.Message {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return message.NewMessage(watermill.NewUUID(), data)
}
