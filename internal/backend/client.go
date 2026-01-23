package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Backend defines the interface for interacting with a provisioning backend.
// Any backend (Kubernetes, GPU, VM, etc.) must implement these 4 operations.
type Backend interface {
	// Provision starts async provisioning of a resource.
	// The backend will call the callback URL when provisioning completes.
	Provision(ctx context.Context, req ProvisionRequest) error

	// GetInfo returns lease information including connection details.
	// Returns ErrNotProvisioned if the lease is not yet provisioned.
	GetInfo(ctx context.Context, leaseUUID string) (*LeaseInfo, error)

	// Deprovision releases resources for a lease. Must be idempotent.
	Deprovision(ctx context.Context, leaseUUID string) error

	// ListProvisions returns all currently provisioned resources.
	// Used for reconciliation to detect orphans.
	ListProvisions(ctx context.Context) ([]ProvisionInfo, error)

	// Name returns the backend's configured name.
	Name() string
}

// ProvisionRequest contains the data needed to provision a resource.
type ProvisionRequest struct {
	LeaseUUID    string `json:"lease_uuid"`
	Tenant       string `json:"tenant"`
	ProviderUUID string `json:"provider_uuid"`
	SKU          string `json:"sku"`
	CallbackURL  string `json:"callback_url"`
	Payload      []byte `json:"payload,omitempty"`
	PayloadHash  string `json:"payload_hash,omitempty"`
}

// ProvisionResponse is returned by the backend after accepting a provision request.
type ProvisionResponse struct {
	ProvisionID string `json:"provision_id"`
}

// LeaseInfo contains backend-specific information about a provisioned lease.
// The structure is flexible to allow different backends to return different data.
// Common fields include: host, port, protocol, credentials, metadata.
type LeaseInfo map[string]any

// ProvisionInfo describes a single provisioned resource.
type ProvisionInfo struct {
	LeaseUUID   string    `json:"lease_uuid"`
	Status      string    `json:"status"` // "provisioning", "ready", "failed"
	CreatedAt   time.Time `json:"created_at"`
	BackendName string    `json:"-"` // Set by reconciler, not from backend
}

// CallbackPayload is sent by backends to fred's callback endpoint.
type CallbackPayload struct {
	LeaseUUID string `json:"lease_uuid"`
	Status    string `json:"status"` // "success" or "failed"
	Error     string `json:"error,omitempty"`
}

// ErrNotProvisioned is returned when a lease is not yet provisioned.
var ErrNotProvisioned = fmt.Errorf("lease not provisioned")

// HTTPClient implements Backend using HTTP calls to a backend service.
type HTTPClient struct {
	name       string
	baseURL    string
	httpClient *http.Client
}

// HTTPClientConfig configures an HTTP backend client.
type HTTPClientConfig struct {
	Name    string
	BaseURL string
	Timeout time.Duration
}

// NewHTTPClient creates a new HTTP backend client.
func NewHTTPClient(cfg HTTPClientConfig) *HTTPClient {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &HTTPClient{
		name:    cfg.Name,
		baseURL: cfg.BaseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// Name returns the backend's configured name.
func (c *HTTPClient) Name() string {
	return c.name
}

// Provision sends a provision request to the backend.
func (c *HTTPClient) Provision(ctx context.Context, req ProvisionRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal provision request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/provision", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("provision request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("provision failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// GetInfo retrieves lease information including connection details.
func (c *HTTPClient) GetInfo(ctx context.Context, leaseUUID string) (*LeaseInfo, error) {
	url := fmt.Sprintf("%s/info/%s", c.baseURL, leaseUUID)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("get info request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotProvisioned
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get info failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var info LeaseInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode info response: %w", err)
	}

	return &info, nil
}

// Deprovision releases resources for a lease.
func (c *HTTPClient) Deprovision(ctx context.Context, leaseUUID string) error {
	body, err := json.Marshal(map[string]string{"lease_uuid": leaseUUID})
	if err != nil {
		return fmt.Errorf("marshal deprovision request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/deprovision", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("deprovision request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("deprovision failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// ListProvisions returns all provisioned resources from this backend.
func (c *HTTPClient) ListProvisions(ctx context.Context) ([]ProvisionInfo, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/provisions", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("list provisions request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list provisions failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result struct {
		Provisions []ProvisionInfo `json:"provisions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode provisions response: %w", err)
	}

	return result.Provisions, nil
}
