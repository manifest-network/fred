package docker

import (
	"context"
	"testing"

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
	// would have rejected it; Fred's defense-in-depth check must too.
	// "good.example.com" is fine and would normally trigger Restart, but
	// since this is a unit test without the restart plumbing, we expect
	// the call to either succeed (Restart accepted) or fail downstream.
	// Here we only assert the post-condition on the BAD item: it stayed
	// at its original empty value because validation rejected it before
	// any in-memory mutation.
	_ = b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "web", CustomDomain: "evil.barney0.manifest0.net"},
		{SKU: "docker-small", ServiceName: "admin", CustomDomain: ""}, // unchanged: also no-op
	})

	// Bad item not mutated.
	assert.Equal(t, "", b.provisions["lease-1"].Items[0].CustomDomain,
		"validation-rejected item must not have its CustomDomain mutated in memory")
	// Other unchanged item also untouched.
	assert.Equal(t, "", b.provisions["lease-1"].Items[1].CustomDomain)
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
