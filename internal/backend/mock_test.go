package backend

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMockBackend_Provision(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	err := mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
		Items:     []LeaseItem{{SKU: "gpu-a100", Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("Provision() error = %v", err)
	}

	// Verify provision exists
	p, exists := mock.GetProvision("lease-1")
	if !exists {
		t.Fatal("provision not found")
	}
	if p.LeaseUUID != "lease-1" {
		t.Errorf("LeaseUUID = %q, want %q", p.LeaseUUID, "lease-1")
	}
	if p.Status != ProvisionStatusProvisioning {
		t.Errorf("Status = %q, want %q", p.Status, ProvisionStatusProvisioning)
	}
}

func TestMockBackend_ProvisionDuplicate(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	// First provision
	err := mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
	})
	if err != nil {
		t.Fatalf("first Provision() error = %v", err)
	}

	// Duplicate should fail
	err = mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
	})
	if err == nil {
		t.Error("duplicate Provision() should return error")
	}
}

func TestMockBackend_ProvisionWithCallback(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{
		Name:           "test",
		ProvisionDelay: 10 * time.Millisecond,
	})

	// Set up callback
	var callbackReceived CallbackPayload
	var wg sync.WaitGroup
	wg.Add(1)

	mock.SetCallbackFunc(func(payload CallbackPayload) {
		callbackReceived = payload
		wg.Done()
	})

	// Provision
	err := mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
	})
	if err != nil {
		t.Fatalf("Provision() error = %v", err)
	}

	// Wait for callback
	wg.Wait()

	if callbackReceived.LeaseUUID != "lease-1" {
		t.Errorf("callback LeaseUUID = %q, want %q", callbackReceived.LeaseUUID, "lease-1")
	}
	if callbackReceived.Status != CallbackStatusSuccess {
		t.Errorf("callback Status = %q, want %q", callbackReceived.Status, CallbackStatusSuccess)
	}

	// Verify status changed to ready
	p, _ := mock.GetProvision("lease-1")
	if p.Status != ProvisionStatusReady {
		t.Errorf("Status = %q, want %q", p.Status, ProvisionStatusReady)
	}
}

func TestMockBackend_GetInfo(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	// Not provisioned
	_, err := mock.GetInfo(context.Background(), "nonexistent")
	if err != ErrNotProvisioned {
		t.Errorf("GetInfo() error = %v, want ErrNotProvisioned", err)
	}

	// Provision but still provisioning
	mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	})

	_, err = mock.GetInfo(context.Background(), "lease-1")
	if err != ErrNotProvisioned {
		t.Errorf("GetInfo() while provisioning error = %v, want ErrNotProvisioned", err)
	}

	// Mark as ready
	mock.SetProvisionStatus("lease-1", ProvisionStatusReady)

	info, err := mock.GetInfo(context.Background(), "lease-1")
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}
	if (*info)["host"] == "" {
		t.Error("info host is empty")
	}
	metadata, ok := (*info)["metadata"].(map[string]string)
	if !ok {
		t.Fatal("metadata is not map[string]string")
	}
	if metadata["lease_uuid"] != "lease-1" {
		t.Errorf("info metadata lease_uuid = %q, want %q", metadata["lease_uuid"], "lease-1")
	}
}

func TestMockBackend_Deprovision(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	// Provision first
	mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
	})

	// Deprovision
	err := mock.Deprovision(context.Background(), "lease-1")
	if err != nil {
		t.Fatalf("Deprovision() error = %v", err)
	}

	// Verify gone
	_, exists := mock.GetProvision("lease-1")
	if exists {
		t.Error("provision should not exist after deprovision")
	}

	// Deprovision nonexistent (should be idempotent)
	err = mock.Deprovision(context.Background(), "nonexistent")
	if err != nil {
		t.Errorf("Deprovision(nonexistent) error = %v, want nil", err)
	}
}

func TestMockBackend_ListProvisions(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	// Empty list
	provisions, err := mock.ListProvisions(context.Background())
	if err != nil {
		t.Fatalf("ListProvisions() error = %v", err)
	}
	if len(provisions) != 0 {
		t.Errorf("ListProvisions() returned %d, want 0", len(provisions))
	}

	// Add some provisions
	mock.Provision(context.Background(), ProvisionRequest{LeaseUUID: "lease-1"})
	mock.Provision(context.Background(), ProvisionRequest{LeaseUUID: "lease-2"})

	provisions, err = mock.ListProvisions(context.Background())
	if err != nil {
		t.Fatalf("ListProvisions() error = %v", err)
	}
	if len(provisions) != 2 {
		t.Errorf("ListProvisions() returned %d, want 2", len(provisions))
	}
}

func TestMockBackend_Clear(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	// Add provisions
	mock.Provision(context.Background(), ProvisionRequest{LeaseUUID: "lease-1"})
	mock.Provision(context.Background(), ProvisionRequest{LeaseUUID: "lease-2"})

	// Clear
	mock.Clear()

	// Verify empty
	provisions, _ := mock.ListProvisions(context.Background())
	if len(provisions) != 0 {
		t.Errorf("ListProvisions() after Clear() returned %d, want 0", len(provisions))
	}
}

func TestMockBackend_Name(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "my-mock"})
	if mock.Name() != "my-mock" {
		t.Errorf("Name() = %q, want %q", mock.Name(), "my-mock")
	}

	// Default name
	mock2 := NewMockBackend(MockBackendConfig{})
	if mock2.Name() != "mock" {
		t.Errorf("Name() = %q, want %q", mock2.Name(), "mock")
	}
}
