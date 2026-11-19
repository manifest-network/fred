//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// StandInProviderUUID is the provider of a backend started without a chain.
// A backend only echoes the value; a providerd ignores leases a backend
// reports for a provider other than its own.
const StandInProviderUUID = "11111111-1111-4111-8111-111111111111"

// ErrNotFound is what Get returns for a lease the backend does not know.
var ErrNotFound = errors.New("lease not found")

// Spec is what a lease should run.
type Spec struct {
	LeaseUUID string
	Tenant    string
	// SKU is one of the fixture's SKU UUIDs.
	SKU string
	// Services names the services of a stack manifest. Each gets one unit of
	// SKU. Empty for a manifest that describes a single workload.
	Services []string
	// Manifest is the tenant manifest (docs/manifest-guide.md). Restore
	// ignores it: on 0.14 a restore runs the source lease's manifest.
	Manifest map[string]any
}

// items is what the lease buys: one unit of the SKU, or one per service.
func (s Spec) items() []backend.LeaseItem {
	if len(s.Services) == 0 {
		return []backend.LeaseItem{{SKU: s.SKU, Quantity: 1}}
	}
	items := make([]backend.LeaseItem, len(s.Services))
	for i, service := range s.Services {
		items[i] = backend.LeaseItem{SKU: s.SKU, Quantity: 1, ServiceName: service}
	}
	return items
}

// Lease is a lease as the backend reports it.
type Lease struct {
	UUID   string
	Status string
}

// StatusError is a response outside 2xx.
type StatusError struct {
	Status int
	// Code is the backend's machine-readable discriminator, when it sent one.
	Code    string
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("backend answered %d (code %q): %s", e.Status, e.Code, e.Message)
}

// Is makes a 404 match ErrNotFound.
func (e *StatusError) Is(target error) bool {
	return target == ErrNotFound && e.Status == http.StatusNotFound
}

// Client talks to one backend the way providerd does: over mTLS, with every
// request signed, and every mutation sent on the path bound to the backend's
// storage identity. It maps onto the 0.14 endpoints.
type Client struct {
	http    *http.Client
	baseURL string
	secret  string
	// providerUUID is the provider every lease this client creates belongs
	// to. It is fixed when the backend is started.
	providerUUID string
	storageID    backendidentity.ID
	callbacks    *placement.CallbackRouteFactory
}

// Apply asks the backend to run the lease. The work is asynchronous: poll Get.
func (c *Client) Apply(ctx context.Context, spec Spec) error {
	payload, err := json.Marshal(spec.Manifest)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	operationURL, lifecycleURL, err := c.callbackURLs()
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, "/provision", true, backend.ProvisionRequest{
		LeaseUUID:            spec.LeaseUUID,
		Tenant:               spec.Tenant,
		ProviderUUID:         c.providerUUID,
		Items:                spec.items(),
		CallbackURL:          operationURL,
		LifecycleCallbackURL: lifecycleURL,
		Payload:              payload,
	}, nil)
}

// Get reports one lease, or ErrNotFound.
func (c *Client) Get(ctx context.Context, leaseUUID string) (Lease, error) {
	var info backend.ProvisionInfo
	if err := c.do(ctx, http.MethodGet, "/provisions/"+leaseUUID, false, nil, &info); err != nil {
		return Lease{}, err
	}
	return leaseOf(info), nil
}

// List reports every lease the backend runs, following its pagination.
func (c *Client) List(ctx context.Context) ([]Lease, error) {
	var leases []Lease
	for cursor := ""; ; {
		query := url.Values{"limit": {"1000"}}
		if cursor != "" {
			query.Set("continue", cursor)
		}
		var page backend.ListProvisionsResponse
		if err := c.do(ctx, http.MethodGet, "/provisions?"+query.Encode(), false, nil, &page); err != nil {
			return nil, err
		}
		for _, info := range page.Provisions {
			leases = append(leases, leaseOf(info))
		}
		if cursor = page.Continue; cursor == "" {
			return leases, nil
		}
	}
}

// Close asks the backend to stop the lease and release or retain its data.
func (c *Client) Close(ctx context.Context, leaseUUID string) error {
	body := struct {
		LeaseUUID string `json:"lease_uuid"`
	}{leaseUUID}
	return c.do(ctx, http.MethodPost, "/deprovision", true, body, nil)
}

// Restore asks the backend to run spec's lease on the data retained for the
// closed lease from. The work is asynchronous: poll Get.
func (c *Client) Restore(ctx context.Context, spec Spec, from string) error {
	operationURL, lifecycleURL, err := c.callbackURLs()
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, "/restore", true, backend.RestoreRequest{
		LeaseUUID:            spec.LeaseUUID,
		FromLeaseUUID:        from,
		Tenant:               spec.Tenant,
		ProviderUUID:         c.providerUUID,
		Items:                spec.items(),
		CallbackURL:          operationURL,
		LifecycleCallbackURL: lifecycleURL,
	}, nil)
}

func leaseOf(info backend.ProvisionInfo) Lease {
	return Lease{UUID: info.LeaseUUID, Status: string(info.Status)}
}

// callbackURLs mints the callback pair of one operation with providerd's own
// route factory, so the sink receives the URLs a real providerd would.
func (c *Client) callbackURLs() (operationURL, lifecycleURL string, err error) {
	id, err := operation.ParseID(uuid.NewString())
	if err != nil {
		return "", "", fmt.Errorf("mint operation ID: %w", err)
	}
	pair, err := c.callbacks.ForOperation(id)
	if err != nil {
		return "", "", fmt.Errorf("mint callback URLs: %w", err)
	}
	return pair.OperationURL(), pair.LifecycleURL(), nil
}

// do sends one signed request. A mutation goes to the path bound to the
// backend's storage identity; every request names that identity in its query.
func (c *Client) do(ctx context.Context, method, path string, mutation bool, in, out any) error {
	target, err := url.Parse(c.baseURL + path)
	if err != nil {
		return fmt.Errorf("build request URL: %w", err)
	}
	if mutation {
		if target.Path, err = backendidentity.BoundPath(c.storageID, target.Path); err != nil {
			return fmt.Errorf("bind request path: %w", err)
		}
	}
	query := target.Query()
	query.Set(backendidentity.QueryParameter, c.storageID.String())
	target.RawQuery = query.Encode()

	var body []byte
	if in != nil {
		if body, err = json.Marshal(in); err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.SignRequest(c.secret, req, body))

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var failure struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		if json.Unmarshal(data, &failure) != nil || failure.Error == "" {
			failure.Error = string(data)
		}
		return &StatusError{Status: resp.StatusCode, Code: failure.Code, Message: failure.Error}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
