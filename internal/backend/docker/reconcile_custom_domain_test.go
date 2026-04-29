package docker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestReconcileCustomDomain_NoProvision(t *testing.T) {
	// Lease isn't provisioned by this backend → silent no-op (and no error).
	b := newBackendForTest(&mockDockerClient{}, nil)

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "web", CustomDomain: "foo.example.com"},
	})
	require.NoError(t, err)
}

func TestReconcileCustomDomain_NotActive(t *testing.T) {
	// Reconcile only runs when the provision is in Ready state. Anything
	// else (Provisioning, Restarting, Updating, Failed, …) is "not the
	// right time": skip without error.
	for _, status := range []backend.ProvisionStatus{
		backend.ProvisionStatusProvisioning,
		backend.ProvisionStatusRestarting,
		backend.ProvisionStatusUpdating,
		backend.ProvisionStatusFailing,
		backend.ProvisionStatusFailed,
		backend.ProvisionStatusDeprovisioning,
		backend.ProvisionStatusUnknown,
	} {
		t.Run(string(status), func(t *testing.T) {
			provisions := map[string]*provision{
				"lease-1": {
					LeaseUUID: "lease-1",
					Status:    status,
					Items: []backend.LeaseItem{
						{SKU: "docker-small", ServiceName: "web", CustomDomain: ""},
					},
				},
			}
			b := newBackendForTest(&mockDockerClient{}, provisions)

			err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "web", CustomDomain: "foo.example.com"},
			})
			require.NoError(t, err)

			// In-memory state must be unchanged — Restart wasn't triggered.
			assert.Equal(t, "", b.provisions["lease-1"].Items[0].CustomDomain)
		})
	}
}

func TestReconcileCustomDomain_NoChange(t *testing.T) {
	// Items already match incoming → no-op even when Status is Ready.
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "web", CustomDomain: "foo.example.com"},
				{SKU: "docker-small", ServiceName: "db", CustomDomain: ""},
			},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "web", CustomDomain: "foo.example.com"},
		{SKU: "docker-small", ServiceName: "db", CustomDomain: ""},
	})
	require.NoError(t, err)
	assert.Equal(t, "foo.example.com", b.provisions["lease-1"].Items[0].CustomDomain)
	assert.Equal(t, "", b.provisions["lease-1"].Items[1].CustomDomain)
}

func TestReconcileCustomDomain_InvalidDomainSkipsThatItem(t *testing.T) {
	// A malformed/forbidden domain on one item must not poison the whole
	// reconcile. The bad item is left at its current value; valid items
	// proceed.
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "web", CustomDomain: ""},
				{SKU: "docker-small", ServiceName: "admin", CustomDomain: ""},
			},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.cfg.Ingress = IngressConfig{
		Enabled:        true,
		WildcardDomain: "barney0.manifest0.net",
		Entrypoint:     "websecure",
	}

	// "evil.barney0.manifest0.net" is a subdomain of the wildcard — chain
	// would have rejected it; Fred's defense-in-depth check must too. The
	// "admin" item is left unchanged ("" → "") so it's a no-op too. With
	// both items short-circuited, pending stays empty and the method must
	// return nil — assert that contract here so a future regression that
	// returns an error from the no-pending path gets caught.
	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "web", CustomDomain: "evil.barney0.manifest0.net"},
		{SKU: "docker-small", ServiceName: "admin", CustomDomain: ""}, // unchanged: also no-op
	})
	require.NoError(t, err, "validation-rejected items must be skipped without failing the whole reconcile")

	// Bad item not mutated.
	assert.Equal(t, "", b.provisions["lease-1"].Items[0].CustomDomain,
		"validation-rejected item must not have its CustomDomain mutated in memory")
	// Other unchanged item also untouched.
	assert.Equal(t, "", b.provisions["lease-1"].Items[1].CustomDomain)
}

func TestReconcileCustomDomain_IngressDisabledIsNoOp(t *testing.T) {
	// When the backend is configured with ingress disabled, applyIngressLabels
	// emits no Traefik labels at all — a Restart triggered for custom-domain
	// drift would recreate containers without LabelCustomDomain, and the next
	// recoverState tick would rebuild prov.Items[].CustomDomain back to "" from
	// the unlabeled containers. That puts the reconciler into a permanent
	// restart loop against the chain's non-empty value, which a tenant could
	// trigger by setting a custom_domain on a provider that runs ingress-less.
	// The fix: short-circuit the reconcile when ingress is disabled.
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "web", CustomDomain: ""},
			},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.cfg.Ingress = IngressConfig{Enabled: false}

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "web", CustomDomain: "foo.example.com"},
	})
	require.NoError(t, err)

	// In-memory state unchanged — no Restart triggered, no mutation.
	assert.Equal(t, "", b.provisions["lease-1"].Items[0].CustomDomain,
		"ingress-disabled backend must not mutate prov.Items in memory")
}

func TestReconcileCustomDomain_UnknownServiceNameSkipped(t *testing.T) {
	// Chain item references a service_name not present in the provision
	// (e.g. partial recovery, cross-version lease). Reconcile must not
	// panic or error; it silently skips that item.
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "web", CustomDomain: ""},
			},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "ghost", CustomDomain: "foo.example.com"},
	})
	require.NoError(t, err)
	assert.Equal(t, "", b.provisions["lease-1"].Items[0].CustomDomain)
}

// reconcileHarness wires up a Backend with a mock Docker client and a
// callback server so a redeploy triggered by ReconcileCustomDomain can be
// observed end-to-end. Returns the callback received once the worker
// finishes (success or failure), plus the captured CreateContainer params.
type reconcileHarness struct {
	b               *Backend
	server          *httptest.Server
	callbackPayload backend.CallbackPayload
	callbackDone    chan struct{}
	createParamsMu  sync.Mutex
	createParams    []CreateContainerParams
}

func newReconcileHarness(t *testing.T, prov *provision) *reconcileHarness {
	t.Helper()

	h := &reconcileHarness{
		callbackDone: make(chan struct{}),
	}

	mock := &mockDockerClient{
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			h.createParamsMu.Lock()
			h.createParams = append(h.createParams, params)
			h.createParamsMu.Unlock()
			return "new-c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&h.callbackPayload)
		w.WriteHeader(http.StatusOK)
		// Tolerate multiple callbacks (success after restart is one;
		// failure path may also fire). Close once.
		select {
		case <-h.callbackDone:
		default:
			close(h.callbackDone)
		}
	}))
	t.Cleanup(h.server.Close)

	prov.CallbackURL = h.server.URL
	provisions := map[string]*provision{prov.LeaseUUID: prov}

	b := newBackendForProvisionTest(t, mock, provisions)
	b.httpClient = h.server.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.cfg.Ingress = IngressConfig{
		Enabled:        true,
		WildcardDomain: "barney0.manifest0.net",
		Entrypoint:     "websecure",
	}
	h.b = b
	return h
}

func (h *reconcileHarness) waitForCallback(t *testing.T) {
	t.Helper()
	select {
	case <-h.callbackDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for restart callback")
	}
}

func (h *reconcileHarness) lastCreateParams(t *testing.T) CreateContainerParams {
	t.Helper()
	h.createParamsMu.Lock()
	defer h.createParamsMu.Unlock()
	require.NotEmpty(t, h.createParams, "CreateContainer was not called")
	return h.createParams[len(h.createParams)-1]
}

func TestReconcileCustomDomain_Set(t *testing.T) {
	// Empty → "foo.example.com": worker must rebuild containers with the
	// new CustomDomain on CreateContainerParams; in-memory state must
	// reflect the new value after success.
	manifest := &DockerManifest{Image: "nginx:latest"}
	prov := &provision{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		SKU:          "docker-small",
		Status:       backend.ProvisionStatusReady,
		Manifest:     manifest,
		ContainerIDs: []string{"old-c1"},
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: ""},
		},
		Quantity: 1,
	}
	h := newReconcileHarness(t, prov)

	err := h.b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "foo.example.com"},
	})
	require.NoError(t, err)

	h.waitForCallback(t)
	assert.Equal(t, backend.CallbackStatusSuccess, h.callbackPayload.Status)
	assert.Equal(t, "foo.example.com", h.lastCreateParams(t).CustomDomain,
		"CreateContainer must receive the new CustomDomain so secondary labels are emitted")

	h.b.provisionsMu.RLock()
	got := h.b.provisions["lease-1"].Items[0].CustomDomain
	h.b.provisionsMu.RUnlock()
	assert.Equal(t, "foo.example.com", got, "in-memory CustomDomain must reflect the applied value")
}

func TestReconcileCustomDomain_Cleared(t *testing.T) {
	// "foo.example.com" → "": worker must rebuild containers with no
	// secondary router; in-memory CustomDomain reverts to "".
	manifest := &DockerManifest{Image: "nginx:latest"}
	prov := &provision{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		SKU:          "docker-small",
		Status:       backend.ProvisionStatusReady,
		Manifest:     manifest,
		ContainerIDs: []string{"old-c1"},
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "foo.example.com"},
		},
		Quantity: 1,
	}
	h := newReconcileHarness(t, prov)

	err := h.b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: ""},
	})
	require.NoError(t, err)

	h.waitForCallback(t)
	assert.Equal(t, backend.CallbackStatusSuccess, h.callbackPayload.Status)
	assert.Equal(t, "", h.lastCreateParams(t).CustomDomain,
		"CreateContainer must receive the cleared CustomDomain so the secondary router is dropped")

	h.b.provisionsMu.RLock()
	got := h.b.provisions["lease-1"].Items[0].CustomDomain
	h.b.provisionsMu.RUnlock()
	assert.Equal(t, "", got)
}

func TestReconcileCustomDomain_Changed(t *testing.T) {
	// "foo.example.com" → "bar.example.com".
	manifest := &DockerManifest{Image: "nginx:latest"}
	prov := &provision{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		SKU:          "docker-small",
		Status:       backend.ProvisionStatusReady,
		Manifest:     manifest,
		ContainerIDs: []string{"old-c1"},
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "foo.example.com"},
		},
		Quantity: 1,
	}
	h := newReconcileHarness(t, prov)

	err := h.b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "bar.example.com"},
	})
	require.NoError(t, err)

	h.waitForCallback(t)
	assert.Equal(t, backend.CallbackStatusSuccess, h.callbackPayload.Status)
	assert.Equal(t, "bar.example.com", h.lastCreateParams(t).CustomDomain)

	h.b.provisionsMu.RLock()
	got := h.b.provisions["lease-1"].Items[0].CustomDomain
	h.b.provisionsMu.RUnlock()
	assert.Equal(t, "bar.example.com", got)
}

func TestReconcileCustomDomain_RestartSyncError_RollsBack(t *testing.T) {
	// Restart() returns a synchronous error (here: no stored manifest →
	// ErrInvalidState). ReconcileCustomDomain must roll back the in-memory
	// CustomDomain so the next reconciler tick retries cleanly.
	prov := &provision{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		SKU:          "docker-small",
		Status:       backend.ProvisionStatusReady,
		Manifest:     nil, // no manifest → Restart() returns ErrInvalidState
		StackManifest: nil,
		ContainerIDs: []string{"old-c1"},
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "old.example.com"},
		},
		Quantity: 1,
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{
		Enabled:        true,
		WildcardDomain: "barney0.manifest0.net",
		Entrypoint:     "websecure",
	}

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "new.example.com"},
	})
	require.Error(t, err, "ReconcileCustomDomain must surface synchronous Restart errors")
	assert.ErrorIs(t, err, backend.ErrInvalidState)

	b.provisionsMu.RLock()
	got := b.provisions["lease-1"].Items[0].CustomDomain
	b.provisionsMu.RUnlock()
	assert.Equal(t, "old.example.com", got,
		"in-memory CustomDomain must roll back to the pre-call value when Restart fails synchronously")
}

func TestReconcileCustomDomain_LegacyEmptyServiceName(t *testing.T) {
	// Legacy single-item lease addresses its only item by ServiceName="".
	// Reconcile must match on that and apply a no-op when domains match.
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "", CustomDomain: "foo.example.com"},
			},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "foo.example.com"},
	})
	require.NoError(t, err)
	assert.Equal(t, "foo.example.com", b.provisions["lease-1"].Items[0].CustomDomain)
}
