package backend

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockBackend_Provision(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	err := mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
		Items:     []LeaseItem{{SKU: "gpu-a100", Quantity: 1}},
	})
	require.NoError(t, err)

	// Verify provision exists
	p, exists := mock.GetMockProvision("lease-1")
	require.True(t, exists)
	assert.Equal(t, "lease-1", p.LeaseUUID)
	assert.Equal(t, ProvisionStatusProvisioning, p.Status)
}

func TestMockBackend_ProvisionDuplicate(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	// First provision
	err := mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
	})
	require.NoError(t, err)

	// Duplicate should fail
	err = mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
	})
	assert.Error(t, err)
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
	require.NoError(t, err)

	// Wait for callback
	wg.Wait()

	assert.Equal(t, "lease-1", callbackReceived.LeaseUUID)
	assert.Equal(t, CallbackStatusSuccess, callbackReceived.Status)

	// Verify status changed to ready
	p, _ := mock.GetMockProvision("lease-1")
	assert.Equal(t, ProvisionStatusReady, p.Status)
}

func TestMockBackend_GetInfo(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	// Not provisioned
	_, err := mock.GetInfo(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, ErrNotProvisioned)

	// Provision but still provisioning
	mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-1",
	})

	_, err = mock.GetInfo(context.Background(), "lease-1")
	assert.ErrorIs(t, err, ErrNotProvisioned)

	// Mark as ready
	mock.SetProvisionStatus("lease-1", ProvisionStatusReady)

	info, err := mock.GetInfo(context.Background(), "lease-1")
	require.NoError(t, err)
	assert.NotEmpty(t, info.Host)
	assert.Equal(t, "lease-1", info.Metadata["lease_uuid"])
}

func TestMockBackend_Deprovision(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	// Provision first
	mock.Provision(context.Background(), ProvisionRequest{
		LeaseUUID: "lease-1",
	})

	// Deprovision
	err := mock.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	// Verify gone
	_, exists := mock.GetMockProvision("lease-1")
	assert.False(t, exists)

	// Deprovision nonexistent (should be idempotent)
	err = mock.Deprovision(context.Background(), "nonexistent")
	assert.NoError(t, err)
}

func TestMockBackend_ListProvisions(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "test"})

	// Empty list
	provisions, err := mock.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Len(t, provisions, 0)

	// Add some provisions
	mock.Provision(context.Background(), ProvisionRequest{LeaseUUID: "lease-1"})
	mock.Provision(context.Background(), ProvisionRequest{LeaseUUID: "lease-2"})

	provisions, err = mock.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Len(t, provisions, 2)
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
	assert.Len(t, provisions, 0)
}

func TestMockBackend_Name(t *testing.T) {
	mock := NewMockBackend(MockBackendConfig{Name: "my-mock"})
	assert.Equal(t, "my-mock", mock.Name())

	// Default name
	mock2 := NewMockBackend(MockBackendConfig{})
	assert.Equal(t, "mock", mock2.Name())
}
