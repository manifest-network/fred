package provisioner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// mockBackendRouter implements BackendRouter for testing.
type mockBackendRouter struct {
	routeFn            func(sku string) backend.Backend
	routeRoundRobinFn  func(sku string) backend.Backend
	getBackendByNameFn func(name string) backend.Backend
	backendsFn         func() []backend.Backend
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

func (m *mockBackendRouter) RouteRoundRobin(sku string) backend.Backend {
	if m.routeRoundRobinFn != nil {
		return m.routeRoundRobinFn(sku)
	}
	// Default: fall back to Route for backward-compatible tests
	return m.Route(sku)
}

func (m *mockBackendRouter) Backends() []backend.Backend {
	if m.backendsFn != nil {
		return m.backendsFn()
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
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items: []billingtypes.LeaseItem{
			{SkuUuid: "sku-1", Quantity: 1},
		},
	}

	err := orch.StartProvisioning(context.Background(), lease, ProvisionOpts{})
	require.NoError(t, err)

	// Verify backend was called
	mb.mu.Lock()
	require.Len(t, mb.provisionCalls, 1)
	req := mb.provisionCalls[0]
	mb.mu.Unlock()

	assert.Equal(t, "lease-1", req.LeaseUUID)
	assert.Equal(t, "tenant-a", req.Tenant)
	assert.Equal(t, "prov-1", req.ProviderUUID)
	assert.Equal(t, "http://localhost:8080/callbacks/provision", req.CallbackURL)
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
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	payload := []byte(`{"image":"nginx"}`)
	hash := hashPayload(payload)

	err := orch.StartProvisioning(context.Background(), lease, ProvisionOpts{
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
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	// PayloadHash set but no Payload => hash should not be included
	err := orch.StartProvisioning(context.Background(), lease, ProvisionOpts{
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
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := orch.StartProvisioning(context.Background(), lease, ProvisionOpts{})
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

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := orch.StartProvisioning(context.Background(), lease, ProvisionOpts{})
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
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := orch.StartProvisioning(context.Background(), lease, ProvisionOpts{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProvisioningFailed)

	// Should have been untracked after failure
	assert.False(t, tracker.IsInFlight("lease-1"))
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

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1", "")
	require.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()

	// Should have been popped from tracker
	assert.False(t, tracker.IsInFlight("lease-1"))
}

func TestOrchestrator_Deprovision_ViaSKURouting(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend {
			if sku == "sku-1" {
				return mb
			}
			return nil
		},
	}
	tracker := NewInFlightTracker()
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1", "sku-1")
	require.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()
}

func TestOrchestrator_Deprovision_FallbackAllBackends(t *testing.T) {
	mb1 := &mockManagerBackend{name: "b1"}
	mb2 := &mockManagerBackend{name: "b2"}
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{mb1, mb2} },
	}
	tracker := NewInFlightTracker()
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1", "")
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
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDeprovisionFailed)
}

func TestOrchestrator_Deprovision_PartialBackendSuccess(t *testing.T) {
	mb1 := &mockManagerBackend{name: "b1", deprovisionErr: errors.New("fail")}
	mb2 := &mockManagerBackend{name: "b2"}
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{mb1, mb2} },
	}
	tracker := NewInFlightTracker()
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	// At least one succeeds -> no error
	err := orch.Deprovision(context.Background(), "lease-1", "")
	assert.NoError(t, err)
}

func TestOrchestrator_Deprovision_InFlightBackendNotFound_FallsToSKU(t *testing.T) {
	mb := &mockManagerBackend{name: "real-backend"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			return nil // Backend gone
		},
		routeFn: func(sku string) backend.Backend {
			if sku == "sku-1" {
				return mb
			}
			return nil
		},
	}
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "t", testItems("sku-1"), "deleted-backend")

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1", "sku-1")
	require.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()
}

func TestOrchestrator_Deprovision_SKURoutingFails(t *testing.T) {
	mb := &mockManagerBackend{name: "backend", deprovisionErr: errors.New("unavailable")}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	err := orch.Deprovision(context.Background(), "lease-1", "sku-1")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDeprovisionFailed)
}

// --- Placement integration tests ---

func TestOrchestrator_StartProvisioning_RecordsPlacement(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, ps)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := orch.StartProvisioning(context.Background(), lease, ProvisionOpts{})
	require.NoError(t, err)

	assert.Equal(t, "test-backend", ps.Get("lease-1"), "placement should be recorded after successful provisioning")
}

func TestOrchestrator_StartProvisioning_PlacementErrorNonFatal(t *testing.T) {
	// Even if the placement store fails, StartProvisioning should succeed.
	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()

	// Use a placement store that always errors on Set
	ps := &errorPlacementStore{setErr: errors.New("disk full")}
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, ps)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := orch.StartProvisioning(context.Background(), lease, ProvisionOpts{})
	assert.NoError(t, err, "placement Set error should not fail provisioning")
	assert.True(t, tracker.IsInFlight("lease-1"))
}

func TestOrchestrator_StartProvisioning_BackendFails_NoPlacement(t *testing.T) {
	mb := &mockManagerBackend{name: "test-backend", provisionErr: errors.New("backend down")}
	router := &mockBackendRouter{
		routeFn: func(sku string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, ps)

	lease := &billingtypes.Lease{
		Uuid:   "lease-1",
		Tenant: "tenant-a",
		Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	}

	err := orch.StartProvisioning(context.Background(), lease, ProvisionOpts{})
	require.Error(t, err)

	assert.Empty(t, ps.Get("lease-1"), "placement should not be recorded when backend fails")
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

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, ps)

	err := orch.Deprovision(context.Background(), "lease-1", "")
	require.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()

	assert.Empty(t, ps.Get("lease-1"), "placement should be cleaned up after deprovision")
}

func TestOrchestrator_Deprovision_StalePlacement_FallsToSKU(t *testing.T) {
	mb := &mockManagerBackend{name: "real-backend"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			// "removed-backend" is no longer configured
			if name == "real-backend" {
				return mb
			}
			return nil
		},
		routeFn: func(sku string) backend.Backend {
			if sku == "sku-1" {
				return mb
			}
			return nil
		},
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	ps.Set("lease-1", "removed-backend") // stale placement

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, ps)

	err := orch.Deprovision(context.Background(), "lease-1", "sku-1")
	require.NoError(t, err)

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()

	// Placement should still be cleaned up after successful deprovision via SKU fallback
	assert.Empty(t, ps.Get("lease-1"))
}

func TestOrchestrator_Deprovision_PlacementTakesPriorityOverInFlight(t *testing.T) {
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

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, ps)

	err := orch.Deprovision(context.Background(), "lease-1", "")
	require.NoError(t, err)

	// Placement backend should have been used, not in-flight backend
	mbPlacement.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mbPlacement.deprovisionCalls)
	mbPlacement.mu.Unlock()

	mbInFlight.mu.Lock()
	assert.Empty(t, mbInFlight.deprovisionCalls)
	mbInFlight.mu.Unlock()
}

func TestOrchestrator_Deprovision_FallbackAllBackends_CleansPlacement(t *testing.T) {
	mb1 := &mockManagerBackend{name: "b1"}
	router := &mockBackendRouter{
		backendsFn: func() []backend.Backend { return []backend.Backend{mb1} },
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	ps.Set("lease-1", "stale-backend") // stale — no backend will match

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, ps)

	err := orch.Deprovision(context.Background(), "lease-1", "")
	require.NoError(t, err)

	assert.Empty(t, ps.Get("lease-1"), "stale placement should be cleaned up after fallback deprovision")
}

func TestOrchestrator_DeletePlacement(t *testing.T) {
	ps := &mockPlacementStore{}
	ps.Set("lease-1", "test-backend")

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", nil, nil, ps)

	orch.DeletePlacement("lease-1")
	assert.Empty(t, ps.Get("lease-1"), "placement should be deleted")
}

func TestOrchestrator_DeletePlacement_NilStore(t *testing.T) {
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", nil, nil, nil)

	// Should not panic
	orch.DeletePlacement("lease-1")
}

// Regression test: a typed-nil *placement.Store assigned to the PlacementStore
// interface is non-nil (Go interface holds type info). The orchestrator's
// != nil guards don't protect against this, so callers must use the interface
// type for the variable (not the concrete type) to ensure a true nil.
// See: https://go.dev/doc/faq#nil_error
func TestOrchestrator_TypedNilPlacementStore_Panics(t *testing.T) {
	var ps *placement.Store //nolint:staticcheck // intentionally testing typed-nil interface behavior
	var iface PlacementStore = ps

	// Precondition: a plain Go != nil check passes (this is the bug).
	// Note: testify's require.NotNil uses reflect and sees through the wrapper.
	require.False(t, iface == nil, "typed-nil interface must not be == nil") //nolint:staticcheck // intentionally testing this exact condition

	mb := &mockManagerBackend{name: "test-backend"}
	router := &mockBackendRouter{
		routeRoundRobinFn: func(string) backend.Backend { return mb },
	}
	tracker := NewInFlightTracker()
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, iface)

	// StartProvisioning passes the != nil check and calls Set on a nil receiver → panic
	assert.Panics(t, func() {
		_ = orch.StartProvisioning(context.Background(), &billingtypes.Lease{
			Uuid:   "lease-typed-nil",
			Tenant: "tenant-a",
			Items:  []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
		}, ProvisionOpts{})
	})
}

// errorPlacementStore is a PlacementStore that returns errors on write operations.
type errorPlacementStore struct {
	mockPlacementStore
	setErr error
}

func (e *errorPlacementStore) Set(leaseUUID, backendName string) error {
	if e.setErr != nil {
		return e.setErr
	}
	return e.mockPlacementStore.Set(leaseUUID, backendName)
}

func (e *errorPlacementStore) SetBatch(placements map[string]string) error {
	if e.setErr != nil {
		return e.setErr
	}
	return e.mockPlacementStore.SetBatch(placements)
}
