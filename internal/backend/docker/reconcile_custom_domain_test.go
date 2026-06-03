package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
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
				"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
					Status: status,
					Items: []backend.LeaseItem{
						{SKU: "docker-small", ServiceName: "web", CustomDomain: ""},
					}},
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
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "web", CustomDomain: "foo.example.com"},
				{SKU: "docker-small", ServiceName: "db", CustomDomain: ""},
			}},
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
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "web", CustomDomain: ""},
				{SKU: "docker-small", ServiceName: "admin", CustomDomain: ""},
			}},
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
	// both items short-circuited, no override is computed and the method must
	// return nil — assert that contract here so a future regression that
	// returns an error from the no-overrides path gets caught.
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
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "web", CustomDomain: ""},
			}},
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
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "web", CustomDomain: ""},
			}},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "ghost", CustomDomain: "foo.example.com"},
	})
	require.NoError(t, err)
	assert.Equal(t, "", b.provisions["lease-1"].Items[0].CustomDomain)
}

func TestReconcileCustomDomain_RestartSyncError_LeavesItemsUnchanged(t *testing.T) {
	// A synchronous redeploy error (here: no stored manifest -> ErrInvalidState)
	// must surface to the caller, and prov.Items must be UNCHANGED — the
	// reconciler no longer mutates prov.Items off-actor, so there is nothing to
	// roll back; the staged value only ever lands via the actor on success.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  "prov-1",
		SKU:           "docker-small",
		Status:        backend.ProvisionStatusReady,
		StackManifest: nil, // forces ErrInvalidState in the routeReplaceRestart prelude
		ContainerIDs:  []string{"old-c1"},
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "old.example.com"},
		},
		Quantity: 1},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	defer b.stopCancel()
	b.cfg.Ingress = IngressConfig{
		Enabled:        true,
		WildcardDomain: "barney0.manifest0.net",
		Entrypoint:     "websecure",
	}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return true }

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "new.example.com"},
	})
	require.Error(t, err, "ReconcileCustomDomain must surface synchronous redeploy errors")
	assert.ErrorIs(t, err, backend.ErrInvalidState)

	b.provisionsMu.RLock()
	got := b.provisions["lease-1"].Items[0].CustomDomain
	b.provisionsMu.RUnlock()
	assert.Equal(t, "old.example.com", got,
		"prov.Items must be unchanged on a failed redeploy (no off-actor mutation; commit is actor/success-only)")
}

func TestReconcileCustomDomain_LegacyEmptyServiceName(t *testing.T) {
	// Legacy single-item lease addresses its only item by ServiceName="".
	// Reconcile must match on that and apply a no-op when domains match.
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", ServiceName: "", CustomDomain: "foo.example.com"},
			}},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "foo.example.com"},
	})
	require.NoError(t, err)
	assert.Equal(t, "foo.example.com", b.provisions["lease-1"].Items[0].CustomDomain)
}

func TestReconcileCustomDomain_FreshSingleImage_ChainServiceNameEmpty(t *testing.T) {
	// ENG-264 regression. A freshly-provisioned single-image lease has its
	// lone item normalized to DefaultServiceName ("app") on the provision path
	// (NormalizeProvisionRequest), so the container carries
	// fred.service_name="app" and recoverState rebuilds
	// prov.Items[].ServiceName="app". A custom_domain set AFTER deploy arrives
	// here from chain via ExtractLeaseItems, which copies the RAW on-chain
	// service_name — "" for a single-image deploy created without
	// -service-name. Matching "app" against "" finds nothing, so drift is never
	// detected and the domain is silently dropped (no -custom router, no cert).
	//
	// The reconcile must normalize BOTH sides (a lone unnamed item → "app")
	// before matching. We assert drift IS detected by driving the synchronous
	// Restart-error path (StackManifest nil → ErrInvalidState): that error only
	// fires if an override was computed, i.e. chain "" matched container
	// "app". Before the fix this returns nil (no drift); after, it errors.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  "prov-1",
		SKU:           "docker-small",
		Status:        backend.ProvisionStatusReady,
		StackManifest: nil,
		ContainerIDs:  []string{"old-c1"},
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: ""},
		},
		Quantity: 1},
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
	require.Error(t, err,
		`drift must be detected: chain service_name="" must match container ServiceName="app" after normalization (ENG-264)`)
	assert.ErrorIs(t, err, backend.ErrInvalidState)

	// prov.Items is never mutated off-actor — value remains the original "".
	b.provisionsMu.RLock()
	got := b.provisions["lease-1"].Items[0].CustomDomain
	b.provisionsMu.RUnlock()
	assert.Equal(t, "", got, "prov.Items must be unchanged (no off-actor mutation; commit is actor/success-only)")
}

func TestReconcileCustomDomain_NotDNSReady_DoesNotEmit(t *testing.T) {
	// ENG-266: domain set on chain, container has none yet, DNS not pointing
	// here → reconcile must NOT stage a change (no -custom router, no order, no
	// Restart). With no change staged, the method returns nil and the in-memory
	// value stays "".
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Status: backend.ProvisionStatusReady,
		Items:  []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: ""}}},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return false }

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "new.example.com"},
	})
	require.NoError(t, err)
	assert.Equal(t, "", b.provisions["lease-1"].Items[0].CustomDomain,
		"not-ready domain must not be staged for emission")
}

func TestReconcileCustomDomain_DNSReady_Emits(t *testing.T) {
	// Same shape as NotDNSReady but DNS ready → drift detected → Restart
	// attempted. StackManifest nil makes Restart fail synchronously
	// (ErrInvalidState), which only fires if a change was staged.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Tenant: "t", ProviderUUID: "p", SKU: "docker-small",
		Status: backend.ProvisionStatusReady, StackManifest: nil, ContainerIDs: []string{"c1"},
		Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: ""}}, Quantity: 1},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return true }

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "new.example.com"},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestReconcileCustomDomain_AlreadyEmitted_NotTornDownOnDNSBlip(t *testing.T) {
	// Domain already emitted; DNS now NOT matching (transient). Must NOT remove
	// it — no change staged, no Restart, value preserved.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Status: backend.ProvisionStatusReady,
		Items:  []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: "live.example.com"}}},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return false }

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "live.example.com"},
	})
	require.NoError(t, err)
	assert.Equal(t, "live.example.com", b.provisions["lease-1"].Items[0].CustomDomain,
		"an already-emitted domain must not be removed on a transient DNS mismatch")
}

func TestReconcileCustomDomain_Clear_NotGated(t *testing.T) {
	// Chain clears the domain; removal must proceed regardless of DNS readiness
	// (here the Restart fails on nil manifest, proving a change was staged).
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Tenant: "t", ProviderUUID: "p", SKU: "docker-small",
		Status: backend.ProvisionStatusReady, StackManifest: nil, ContainerIDs: []string{"c1"},
		Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "old.example.com"}}, Quantity: 1},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return false } // irrelevant for clear

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: ""},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrInvalidState, "clear must still trigger a Restart (here failing on nil manifest)")
}

func TestReconcileCustomDomain_ChangedDomain_NewNotReady(t *testing.T) {
	// ENG-266: domain changes old->new while new's DNS isn't pointing here yet.
	// The gate must keep the OLD domain emitted (not switch, not tear down) and
	// stage no change (no Restart) until new resolves.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Status: backend.ProvisionStatusReady,
		Items:  []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: "old.example.com"}}},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return false }

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "new.example.com"},
	})
	require.NoError(t, err)
	assert.Equal(t, "old.example.com", b.provisions["lease-1"].Items[0].CustomDomain,
		"a not-ready new domain must not replace the working old one")
}

func TestReconcileCustomDomain_ChangedDomain_NewReady(t *testing.T) {
	// Same change but new's DNS is ready → drift detected → Restart attempted
	// (fails on nil manifest → ErrInvalidState). prov.Items is unchanged
	// because there is no off-actor mutation; the commit lands on success only.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Tenant: "t", ProviderUUID: "p", SKU: "docker-small",
		Status: backend.ProvisionStatusReady, StackManifest: nil, ContainerIDs: []string{"c1"},
		Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "old.example.com"}}, Quantity: 1},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return true }

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "new.example.com"},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrInvalidState)
	assert.Equal(t, "old.example.com", b.provisions["lease-1"].Items[0].CustomDomain,
		"prov.Items unchanged on failed redeploy (no off-actor mutation; commit is actor/success-only)")
}

func TestReconcileCustomDomain_MultiService_MixedReadiness(t *testing.T) {
	// Per-service independence: an already-emitted service is untouched while a
	// different service's not-ready new domain is deferred — same reconcile, no
	// Restart.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Status: backend.ProvisionStatusReady,
		Items: []backend.LeaseItem{
			{SKU: "docker-small", ServiceName: "frontend", CustomDomain: "live.example.com"},
			{SKU: "docker-small", ServiceName: "api", CustomDomain: ""},
		}},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	b.customDomainDNSReady = func(_ context.Context, d string) bool { return d == "live.example.com" }

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "frontend", CustomDomain: "live.example.com"},
		{SKU: "docker-small", ServiceName: "api", CustomDomain: "notready.example.com"},
	})
	require.NoError(t, err)
	byService := map[string]string{}
	for _, it := range b.provisions["lease-1"].Items {
		byService[it.ServiceName] = it.CustomDomain
	}
	assert.Equal(t, "live.example.com", byService["frontend"], "already-emitted service untouched")
	assert.Equal(t, "", byService["api"], "not-ready service deferred independently")
}

func TestReconcileCustomDomain_DeferredThenReady(t *testing.T) {
	// ENG-266 core behavior: a domain set while DNS isn't ready is deferred on
	// one tick, then emitted on a later tick once DNS resolves — same domain, no
	// chain change in between.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Tenant: "t", ProviderUUID: "p", SKU: "docker-small",
		Status: backend.ProvisionStatusReady, StackManifest: nil, ContainerIDs: []string{"c1"},
		Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: ""}}, Quantity: 1},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}

	ready := false
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return ready }
	chain := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "app.example.com"}}

	// Tick 1: DNS not ready → deferred, no change staged, no error.
	require.NoError(t, b.ReconcileCustomDomain(context.Background(), "lease-1", chain))
	assert.Equal(t, "", b.provisions["lease-1"].Items[0].CustomDomain, "tick 1: deferred while DNS not ready")

	// Tick 2: DNS now ready → drift detected → Restart attempted (nil manifest →
	// ErrInvalidState proves the override was computed this time).
	ready = true
	err := b.ReconcileCustomDomain(context.Background(), "lease-1", chain)
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrInvalidState, "tick 2: domain emitted once DNS becomes ready")
}

func TestReconcileCustomDomain_RecoverStateSwap_RedeploysNewDomain(t *testing.T) {
	// ENG-278 regression. A recoverState struct-swap that reverts
	// prov.Items[].CustomDomain to the OLD container-label value, landing in the
	// Ready window, must NOT cause the redeploy to render the old domain. The
	// override is captured read-only before the swap and re-applied by
	// ServiceName, so the redeploy renders NEW and the actor commits NEW.

	// Stack manifest with a routable port so the custom-domain Traefik label
	// (LabelCustomDomain) is actually rendered (ingress gate requires a port).
	stack := &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"app": {Image: "nginx:latest", Ports: map[string]manifest.PortConfig{"80/tcp": {}}},
		},
	}

	// Seed a release store with an ACTIVE stack release so recoverState restores
	// StackManifest after it rebuilds the (Ready) provision from labels.
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	defer releaseStore.Close()
	require.NoError(t, releaseStore.Append("lease-1", shared.Release{
		Manifest: manifestBytes, Image: "stack", Status: "active", CreatedAt: time.Now(),
	}))

	// mu guards both callbackPayload (written by the callback handler goroutine,
	// read at the assertion) and capturedProject (written by the compose Up mock).
	var mu sync.Mutex

	// Callback server signals redeploy completion.
	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		w.WriteHeader(http.StatusOK)
		mu.Lock()
		callbackPayload = p
		mu.Unlock()
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	// Fake managed container carrying the OLD domain label — what recoverState
	// rebuilds prov.Items from.
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{{
				ContainerID:   "old-app",
				LeaseUUID:     "lease-1",
				SKU:           "docker-small",
				Tenant:        "tenant-a",
				ProviderUUID:  "prov-1",
				CallbackURL:   callbackServer.URL,
				ServiceName:   "app",
				InstanceIndex: 0,
				Status:        "running",
				CustomDomain:  "old.example.com",
				Name:          "fred-lease-1-app-0",
			}}, nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	var capturedProject *composetypes.Project
	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			mu.Lock()
			capturedProject = project
			mu.Unlock()
			return nil
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{ID: "new-app-c1", Service: "app", State: "running"}}, nil
		},
	}

	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:            "tenant-a",
			ProviderUUID:      "prov-1",
			SKU:               "docker-small",
			Status:            backend.ProvisionStatusReady,
			StackManifest:     stack,
			ContainerIDs:      []string{"old-app"},
			ServiceContainers: map[string][]string{"app": {"old-app"}},
			CallbackURL:       callbackServer.URL,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "old.example.com"},
			},
			Quantity: 1},
		},
	}

	b := newBackendForProvisionTest(t, mock, provisions)
	b.compose = composeMock
	b.releaseStore = releaseStore
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}

	chain := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "new.example.com"}}

	// 1. Compute the override read-only while Ready (mirrors ReconcileCustomDomain
	//    under provisionsMu), with the new domain DNS-ready.
	// DNS readiness is supplied directly to computeCustomDomainOverrides here, so
	// b.customDomainDNSReady (the production probe) is intentionally not wired in this white-box test.
	b.provisionsMu.Lock()
	prov := b.provisions["lease-1"]
	overrides := b.computeCustomDomainOverrides(prov, chain, map[string]bool{"new.example.com": true})
	callbackURL := prov.CallbackURL
	b.provisionsMu.Unlock()
	require.Equal(t, map[string]string{"app": "new.example.com"}, overrides,
		`chain service_name "" must normalize to "app" and stage the new domain`)

	// 2. Concurrent recoverState lands in the Ready window: it rebuilds the
	//    provision from the OLD-labeled container and swaps the map.
	require.NoError(t, b.recoverState(context.Background()))
	b.provisionsMu.RLock()
	swapped := b.provisions["lease-1"].Items[0].CustomDomain
	b.provisionsMu.RUnlock()
	require.Equal(t, "old.example.com", swapped, "sanity: recoverState swapped prov.Items back to the old domain")

	// 3. Route the redeploy with the pre-swap override.
	require.NoError(t, b.routeReplaceRestart(context.Background(), "lease-1", callbackURL, overrides))

	// 4. Wait for the async redeploy to complete.
	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for redeploy callback")
	}
	mu.Lock()
	gotStatus := callbackPayload.Status
	mu.Unlock()
	require.Equal(t, backend.CallbackStatusSuccess, gotStatus)

	// 5. The redeploy rendered the NEW domain despite the swap.
	mu.Lock()
	proj := capturedProject
	mu.Unlock()
	require.NotNil(t, proj, "compose Up was not called")
	assert.Equal(t, "new.example.com", proj.Services["app"].Labels[LabelCustomDomain],
		"redeploy must render the new domain even though recoverState swapped prov.Items to the old one")

	// 6. The actor committed the new domain to prov.Items on success.
	b.provisionsMu.RLock()
	committed := b.provisions["lease-1"].Items[0].CustomDomain
	status := b.provisions["lease-1"].Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, "new.example.com", committed, "actor must commit the new domain to prov.Items on success")
	assert.Equal(t, backend.ProvisionStatusReady, status)

	b.stopCancel()
	b.wg.Wait()
}

func TestReconcileCustomDomain_ConcurrentRecoverState_NoRace(t *testing.T) {
	// Lock-discipline guard (run under -race): ReconcileCustomDomain and
	// recoverState hammering the same lease concurrently must not race and must
	// converge to the new domain. Correctness of the interleaving is pinned by
	// TestReconcileCustomDomain_RecoverStateSwap_RedeploysNewDomain.
	stack := &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"app": {Image: "nginx:latest", Ports: map[string]manifest.PortConfig{"80/tcp": {}}},
		},
	}
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	defer releaseStore.Close()
	require.NoError(t, releaseStore.Append("lease-1", shared.Release{
		Manifest: manifestBytes, Image: "stack", Status: "active", CreatedAt: time.Now(),
	}))

	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{{
				ContainerID: "old-app", LeaseUUID: "lease-1", SKU: "docker-small",
				Tenant: "tenant-a", ProviderUUID: "prov-1", CallbackURL: callbackServer.URL,
				ServiceName: "app", InstanceIndex: 0, Status: "running",
				CustomDomain: "old.example.com", Name: "fred-lease-1-app-0",
			}}, nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}
	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error { return nil },
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{ID: "new-app-c1", Service: "app", State: "running"}}, nil
		},
	}
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant: "tenant-a", ProviderUUID: "prov-1", SKU: "docker-small",
			Status: backend.ProvisionStatusReady, StackManifest: stack,
			ContainerIDs: []string{"old-app"}, ServiceContainers: map[string][]string{"app": {"old-app"}},
			CallbackURL: callbackServer.URL,
			Items:       []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "old.example.com"}},
			Quantity:    1}},
	}

	b := newBackendForProvisionTest(t, mock, provisions)
	b.compose = composeMock
	b.releaseStore = releaseStore
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return true }

	chain := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "new.example.com"}}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_ = b.recoverState(context.Background())
		}
	}()
	// Reconcile retries against the concurrent swap until the redeploy is
	// accepted (ErrInvalidState/no-op ticks are expected while a swap is mid-flight).
	require.Eventually(t, func() bool {
		_ = b.ReconcileCustomDomain(context.Background(), "lease-1", chain)
		select {
		case <-callbackReceived:
			return true
		default:
			return false
		}
	}, 5*time.Second, 20*time.Millisecond)
	wg.Wait()

	b.stopCancel()
	b.wg.Wait()
}

// recordingDNS wraps customDomainDNSReady to record which domains are resolved
// and return a per-domain readiness verdict. Uses the existing injectable seam
// (no test-only production code).
func recordingDNS(b *Backend, ready func(d string) bool) *[]string {
	var mu sync.Mutex
	var resolved []string
	b.customDomainDNSReady = func(_ context.Context, d string) bool {
		mu.Lock()
		resolved = append(resolved, d)
		mu.Unlock()
		return ready(d)
	}
	return &resolved
}

func TestReconcileCustomDomain_SteadyState_NoDNS(t *testing.T) {
	// A Ready lease whose custom domain already equals the chain value must
	// perform ZERO DNS lookups (ENG-277 primary acceptance).
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Status: backend.ProvisionStatusReady,
		Items:  []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: "live.example.com"}}},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	resolved := recordingDNS(b, func(string) bool { return true })

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "live.example.com"},
	})
	require.NoError(t, err)
	assert.Empty(t, *resolved, "an already-emitted unchanged domain must not be resolved")
}

func TestReconcileCustomDomain_CandidateOnly_ResolvesOnlyChanged(t *testing.T) {
	// Two services: one already-emitted (unchanged), one changed. Only the
	// changed domain is a candidate, so only it is resolved.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Tenant: "t", ProviderUUID: "p", SKU: "docker-small",
		Status: backend.ProvisionStatusReady, StackManifest: nil, ContainerIDs: []string{"c1"},
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "frontend", CustomDomain: "live.example.com"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "api", CustomDomain: ""},
		}, Quantity: 1},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	resolved := recordingDNS(b, func(string) bool { return true })

	// The changed (api) domain stages an override → routeReplaceRestart fails on
	// the nil StackManifest (ErrInvalidState); that only happens if api was
	// resolved+gated, confirming the candidate path ran end-to-end.
	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "frontend", CustomDomain: "live.example.com"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "api", CustomDomain: "new.example.com"},
	})
	require.ErrorIs(t, err, backend.ErrInvalidState)
	assert.Equal(t, []string{"new.example.com"}, *resolved,
		"only the changed domain is a candidate; the already-emitted one must not be resolved")
}

func TestReconcileCustomDomain_NotReady_NoDNS(t *testing.T) {
	// A not-Ready lease must perform ZERO DNS lookups (the pre-pass returns
	// before resolving). Pre-ENG-277 the all-domains precompute ran before the
	// Ready gate and would resolve here.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Status: backend.ProvisionStatusProvisioning,
		Items:  []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: ""}}},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	resolved := recordingDNS(b, func(string) bool { return true })

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "new.example.com"},
	})
	require.NoError(t, err)
	assert.Empty(t, *resolved, "a not-Ready lease must not resolve any custom domain")
}

func TestReconcileCustomDomain_InvalidChangedDomain_NoDNS(t *testing.T) {
	// A changed-but-invalid domain (subdomain of the wildcard) is rejected by
	// validation, so it is NOT a candidate and is never resolved (validate
	// before resolve). No override is staged.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Status: backend.ProvisionStatusReady,
		Items:  []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: ""}}},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	resolved := recordingDNS(b, func(string) bool { return true })

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "", CustomDomain: "evil.barney0.manifest0.net"},
	})
	require.NoError(t, err)
	assert.Empty(t, *resolved, "an invalid changed domain must not be resolved")
	assert.Equal(t, "", b.provisions["lease-1"].Items[0].CustomDomain)
}

func TestReconcileCustomDomain_AsyncRedeployFailure_DoesNotCommit(t *testing.T) {
	// "Commit only on success" on the ASYNC worker path: when the redeploy worker
	// fails (compose Up errors), the actor's success entry action
	// (onEnterReadyFromReplaceCompleted / OnSuccess) never runs, so the new domain
	// is NOT committed to prov.Items — the actor commits nothing on failure. The
	// existing TestReconcileCustomDomain_RestartSyncError_LeavesItemsUnchanged only
	// covers the synchronous prelude error (ErrInvalidState), never the worker;
	// this covers the worker-failure path the deleted CAS rollback used to.
	stack := &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"app": {Image: "nginx:latest", Ports: map[string]manifest.PortConfig{"80/tcp": {}}},
		},
	}

	var mu sync.Mutex
	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		w.WriteHeader(http.StatusOK)
		mu.Lock()
		callbackPayload = p
		mu.Unlock()
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}
	// Worker fails: compose Up always errors, so both the forward deploy and the
	// rollback fail -> the lease lands Failed (recovered==false), and OnSuccess
	// is never invoked.
	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			return fmt.Errorf("compose up failed (test)")
		},
	}

	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:            "tenant-a",
			ProviderUUID:      "prov-1",
			SKU:               "docker-small",
			Status:            backend.ProvisionStatusReady,
			StackManifest:     stack,
			ContainerIDs:      []string{"old-app"},
			ServiceContainers: map[string][]string{"app": {"old-app"}},
			CallbackURL:       callbackServer.URL,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "old.example.com"},
			},
			Quantity: 1},
		},
	}

	b := newBackendForProvisionTest(t, mock, provisions)
	b.compose = composeMock
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return true }

	// Drive the PUBLIC reconcile path. The route + ack succeed (Status flips to
	// Restarting) and ReconcileCustomDomain returns nil; the redeploy then fails
	// asynchronously in the worker.
	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "new.example.com"},
	})
	require.NoError(t, err, "route + ack succeed; the redeploy fails asynchronously in the worker")

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for failure callback")
	}

	// The redeploy genuinely failed (so we are exercising the failure path, not a
	// vacuous no-op).
	mu.Lock()
	gotStatus := callbackPayload.Status
	mu.Unlock()
	require.Equal(t, backend.CallbackStatusFailed, gotStatus,
		"the failure callback must have been delivered (guards against a vacuous pass)")

	// The actor committed nothing: prov.Items still carries the OLD domain, and
	// the lease is not left Ready-with-the-new-domain.
	b.provisionsMu.RLock()
	got := b.provisions["lease-1"].Items[0].CustomDomain
	status := b.provisions["lease-1"].Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, "old.example.com", got,
		"a failed worker redeploy must not commit the new domain to prov.Items (OnSuccess is success-only)")
	assert.NotEqual(t, backend.ProvisionStatusReady, status,
		"a failed redeploy must not land the lease back in Ready via the success entry action")

	b.stopCancel()
	b.wg.Wait()
}

func TestReconcileCustomDomain_WindowDeferralThenSelfHeal(t *testing.T) {
	// White-box (ENG-277 window safety). The candidate pre-pass and the main
	// diff read prov twice across the resolve window. If a recoverState swap
	// changes the emitted value in that window, a domain that was NOT a candidate
	// (so dnsReady lacks it) becomes a change the main pass sees → the asymmetric
	// gate DEFERS it that tick (no tear-down), and the NEXT tick's pre-pass
	// resolves it → emits. The swap is represented by two prov states.
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}

	chain := []backend.LeaseItem{{SKU: "docker-small", ServiceName: "", CustomDomain: "new.example.com"}}
	// Pre-pass state: domain already emitted == desired → NOT a candidate.
	provPrepass := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Items: []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: "new.example.com"}}}}
	// Window state after a recoverState swap: emitted reverted to old.
	provSwapped := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Items: []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", CustomDomain: "old.example.com"}}}}

	// The helpers require the caller to hold provisionsMu; we wrap each call to
	// match the production call shape (the prov values here are local, so the
	// lock guards nothing real in the test — it just honors the precondition).
	b.provisionsMu.RLock()
	cands := b.customDomainDNSCandidates(provPrepass, chain)
	b.provisionsMu.RUnlock()
	require.Empty(t, cands, "an already-emitted domain is not a DNS candidate")

	// Tick 1 main pass on the swapped state with the (empty) dnsReady: the now-
	// changed domain is unresolved → DEFERRED (emitted preserved, no override).
	b.provisionsMu.RLock()
	deferred := b.computeCustomDomainOverrides(provSwapped, chain, map[string]bool{})
	b.provisionsMu.RUnlock()
	assert.Empty(t, deferred, "an unresolved changed domain must be deferred, not applied")

	// Tick 2 pre-pass on the swapped state: now a candidate → resolved.
	b.provisionsMu.RLock()
	cands2 := b.customDomainDNSCandidates(provSwapped, chain)
	b.provisionsMu.RUnlock()
	require.Equal(t, []string{"new.example.com"}, cands2, "the changed domain is now a candidate")

	// Tick 2 main pass with the resolved-ready domain: SELF-HEAL → emits.
	b.provisionsMu.RLock()
	healed := b.computeCustomDomainOverrides(provSwapped, chain, map[string]bool{"new.example.com": true})
	b.provisionsMu.RUnlock()
	assert.Equal(t, map[string]string{"app": "new.example.com"}, healed, "next tick emits the new domain")
}
