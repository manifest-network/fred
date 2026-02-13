package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// MockBackend is an in-memory backend for testing.
// It simulates provisioning with configurable delays.
type MockBackend struct {
	name string

	// Configuration
	provisionDelay time.Duration

	// State
	provisions map[string]*mockProvision
	mu         sync.Mutex

	// Callbacks to simulate async provisioning
	callbackFunc func(CallbackPayload)
}

type mockProvision struct {
	LeaseUUID    string
	ProviderUUID string
	Tenant       string
	SKU          string
	Quantity     int             // Number of units provisioned
	Status       ProvisionStatus // "provisioning", "ready", "failed"
	CreatedAt    time.Time
	Payload      []byte
	PayloadHash  string
}

// MockBackendConfig configures a mock backend.
type MockBackendConfig struct {
	Name           string
	ProvisionDelay time.Duration // Simulated provisioning time
}

// NewMockBackend creates a new mock backend for testing.
func NewMockBackend(cfg MockBackendConfig) *MockBackend {
	name := cfg.Name
	if name == "" {
		name = "mock"
	}

	return &MockBackend{
		name:           name,
		provisionDelay: cfg.ProvisionDelay,
		provisions:     make(map[string]*mockProvision),
	}
}

// SetCallbackFunc sets the function to call when provisioning completes.
// This simulates the backend calling fred's callback endpoint.
func (m *MockBackend) SetCallbackFunc(fn func(CallbackPayload)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callbackFunc = fn
}

// Name returns the backend's name.
func (m *MockBackend) Name() string {
	return m.name
}

// Provision simulates starting a provision operation.
func (m *MockBackend) Provision(ctx context.Context, req ProvisionRequest) error {
	m.mu.Lock()

	// Check if already provisioned
	if _, exists := m.provisions[req.LeaseUUID]; exists {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrAlreadyProvisioned, req.LeaseUUID)
	}

	// Create provision record
	provision := &mockProvision{
		LeaseUUID:    req.LeaseUUID,
		ProviderUUID: req.ProviderUUID,
		Tenant:       req.Tenant,
		SKU:          req.RoutingSKU(),
		Quantity:     req.TotalQuantity(),
		Status:       ProvisionStatusProvisioning,
		CreatedAt:    time.Now(),
		Payload:      req.Payload,
		PayloadHash:  req.PayloadHash,
	}
	m.provisions[req.LeaseUUID] = provision

	callbackFn := m.callbackFunc
	delay := m.provisionDelay
	m.mu.Unlock()

	// Only auto-transition to "ready" if delay or callback is configured.
	// This allows tests to verify the "provisioning" state when neither is set.
	if delay > 0 || callbackFn != nil {
		go func() {
			if delay > 0 {
				time.Sleep(delay)
			}

			m.mu.Lock()
			p, exists := m.provisions[req.LeaseUUID]
			if !exists {
				m.mu.Unlock()
				return // Deprovisioned while provisioning
			}
			p.Status = ProvisionStatusReady
			m.mu.Unlock()

			// Send callback
			if callbackFn != nil {
				callbackFn(CallbackPayload{
					LeaseUUID: req.LeaseUUID,
					Status:    CallbackStatusSuccess,
				})
			}
		}()
	}

	return nil
}

// GetInfo returns mock lease information including connection details.
func (m *MockBackend) GetInfo(ctx context.Context, leaseUUID string) (*LeaseInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	provision, exists := m.provisions[leaseUUID]
	if !exists {
		return nil, ErrNotProvisioned
	}

	if provision.Status != ProvisionStatusReady {
		return nil, ErrNotProvisioned
	}

	// Generate deterministic connection details from UUID
	hash := sha256.Sum256([]byte(leaseUUID))
	hostSuffix := hex.EncodeToString(hash[:4])

	info := LeaseInfo{
		"host":     fmt.Sprintf("mock-%s.example.com", hostSuffix),
		"port":     8080,
		"protocol": "https",
		"credentials": map[string]string{
			"token": hex.EncodeToString(hash[4:20]),
		},
		"metadata": map[string]string{
			"backend":    m.name,
			"lease_uuid": leaseUUID,
			"tenant":     provision.Tenant,
			"sku":        provision.SKU,
		},
	}
	return &info, nil
}

// Deprovision removes a provision.
func (m *MockBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.provisions, leaseUUID)
	return nil
}

// ListProvisions returns all provisions.
func (m *MockBackend) ListProvisions(ctx context.Context) ([]ProvisionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []ProvisionInfo
	for _, p := range m.provisions {
		result = append(result, ProvisionInfo{
			LeaseUUID:    p.LeaseUUID,
			ProviderUUID: p.ProviderUUID,
			Status:       p.Status,
			CreatedAt:    p.CreatedAt,
		})
	}

	return result, nil
}

// Health always returns nil (healthy) for mock backend.
func (m *MockBackend) Health(ctx context.Context) error {
	return nil
}

// RefreshState is a no-op for mock backend.
func (m *MockBackend) RefreshState(ctx context.Context) error {
	return nil
}

// GetProvision returns status information for a single provision (Backend interface).
func (m *MockBackend) GetProvision(ctx context.Context, leaseUUID string) (*ProvisionInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, exists := m.provisions[leaseUUID]
	if !exists {
		return nil, ErrNotProvisioned
	}

	return &ProvisionInfo{
		LeaseUUID:    p.LeaseUUID,
		ProviderUUID: p.ProviderUUID,
		Status:       p.Status,
		CreatedAt:    p.CreatedAt,
	}, nil
}

// GetLogs returns empty logs for mock backend (Backend interface).
func (m *MockBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.provisions[leaseUUID]; !exists {
		return nil, ErrNotProvisioned
	}

	return map[string]string{"0": "mock log output\n"}, nil
}

// Restart is a no-op for mock backend.
func (m *MockBackend) Restart(ctx context.Context, req RestartRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.provisions[req.LeaseUUID]; !exists {
		return ErrNotProvisioned
	}
	return nil
}

// Update is a no-op for mock backend.
func (m *MockBackend) Update(ctx context.Context, req UpdateRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.provisions[req.LeaseUUID]; !exists {
		return ErrNotProvisioned
	}
	return nil
}

// GetReleases returns empty releases for mock backend.
func (m *MockBackend) GetReleases(ctx context.Context, leaseUUID string) ([]ReleaseInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.provisions[leaseUUID]; !exists {
		return nil, ErrNotProvisioned
	}
	return nil, nil
}

// GetMockProvision returns the internal mock provision (for testing).
func (m *MockBackend) GetMockProvision(leaseUUID string) (*mockProvision, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, exists := m.provisions[leaseUUID]
	if !exists {
		return nil, false
	}

	// Return a copy
	copy := *p
	return &copy, true
}

// SetProvisionStatus manually sets a provision's status (for testing).
func (m *MockBackend) SetProvisionStatus(leaseUUID string, status ProvisionStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, exists := m.provisions[leaseUUID]; exists {
		p.Status = status
	}
}

// Clear removes all provisions (for testing).
func (m *MockBackend) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.provisions = make(map[string]*mockProvision)
}
