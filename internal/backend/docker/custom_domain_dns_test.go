package docker

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

type fakeResolver struct {
	hosts map[string][]net.IPAddr
	errs  map[string]error
}

func (f *fakeResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	if err := f.errs[host]; err != nil {
		return nil, err
	}
	addrs, ok := f.hosts[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return addrs, nil
}

func ipAddrs(ips ...string) []net.IPAddr {
	out := make([]net.IPAddr, 0, len(ips))
	for _, ip := range ips {
		out = append(out, net.IPAddr{IP: net.ParseIP(ip)})
	}
	return out
}

func TestCustomDomainResolves_Resolving(t *testing.T) {
	res := &fakeResolver{hosts: map[string][]net.IPAddr{
		"app.example.com": ipAddrs("64.29.115.28"),
	}}
	assert.True(t, customDomainResolves(context.Background(), res, "app.example.com"))
}

func TestCustomDomainResolves_NXDOMAIN(t *testing.T) {
	res := &fakeResolver{hosts: map[string][]net.IPAddr{}}
	assert.False(t, customDomainResolves(context.Background(), res, "missing.example.com"),
		"NXDOMAIN on the tenant domain is not-ready (avoid ordering into a negative cache)")
}

func TestCustomDomainResolves_ServfailIsNotReady(t *testing.T) {
	res := &fakeResolver{errs: map[string]error{"app.example.com": errors.New("SERVFAIL")}}
	assert.False(t, customDomainResolves(context.Background(), res, "app.example.com"),
		"SERVFAIL / timeout is not-ready, not ready")
}

func TestCustomDomainResolves_EmptyAnswerIsNotReady(t *testing.T) {
	// A resolver returning success with zero addresses is not a usable answer.
	res := &fakeResolver{hosts: map[string][]net.IPAddr{"app.example.com": {}}}
	assert.False(t, customDomainResolves(context.Background(), res, "app.example.com"))
}

// TestCustomDomainResolves_IgnoresHostTopology_ENG618 is the core regression:
// the readiness gate must NOT require the tenant domain to resolve to the
// backend's own address. On the production topology host_address is the backend's
// PRIVATE br1 service-plane IP (internal_ip, 172.16.x), while a tenant custom
// domain legitimately CNAMEs to the host's PUBLIC ingress IP (64.29.x) — the same
// target the *.barneyN wildcard uses. A host-IP-overlap gate (the pre-ENG-618
// behavior) could therefore NEVER pass for a real custom domain, so once a
// container was recreated (reboot/reprovision) the -custom router was dropped and
// never re-attached (custom domain stuck 404 forever). Resolvability alone — what
// the ACME CA sees via public resolvers — is the correct signal.
func TestCustomDomainResolves_IgnoresHostTopology_ENG618(t *testing.T) {
	res := &fakeResolver{hosts: map[string][]net.IPAddr{
		// Public ingress IP; deliberately unrelated to any private host address.
		"dispersed-img-gen.manifest.network": ipAddrs("64.29.115.28"),
	}}
	assert.True(t, customDomainResolves(context.Background(), res, "dispersed-img-gen.manifest.network"),
		"a resolving custom domain must be ready even though it does not resolve to the backend's private host_address")
}

func TestCustomDomainReadyByQuorum(t *testing.T) {
	// mk builds a resolver whose view of the domain is "resolves" or "NXDOMAIN".
	mk := func(resolves bool) ipResolver {
		hosts := map[string][]net.IPAddr{}
		if resolves {
			hosts["app.example.com"] = ipAddrs("64.29.115.28")
		}
		return &fakeResolver{hosts: hosts}
	}
	cases := []struct {
		name   string
		votes  []bool // per-resolver: does the domain resolve from this vantage point?
		quorum int
		want   bool
	}{
		{"all three resolve", []bool{true, true, true}, 2, true},
		{"majority despite one stale NXDOMAIN", []bool{false, true, true}, 2, true},
		{"only one propagated", []bool{true, false, false}, 2, false},
		{"none resolve", []bool{false, false, false}, 2, false},
		{"no resolvers configured (gate stays closed)", []bool{}, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolvers := make([]ipResolver, len(tc.votes))
			for i, r := range tc.votes {
				resolvers[i] = mk(r)
			}
			got := customDomainReadyByQuorum(context.Background(), resolvers, "app.example.com", tc.quorum)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestCustomDomainReadyByQuorum_PublicDomainVsPrivateHost_ENG618 exercises the
// quorum gate under the exact production topology from ENG-618: every resolver
// sees the tenant domain resolve to the PUBLIC ingress IP, which never matches
// the backend's PRIVATE host_address. The gate must reach quorum and report
// READY — the pre-fix host-overlap gate returned false here on every tick.
func TestCustomDomainReadyByQuorum_PublicDomainVsPrivateHost_ENG618(t *testing.T) {
	const domain = "dispersed-img-gen.manifest.network"
	mk := func() ipResolver {
		return &fakeResolver{hosts: map[string][]net.IPAddr{
			domain: ipAddrs("64.29.115.28"), // public ingress IP; no private host in view at all
		}}
	}
	resolvers := []ipResolver{mk(), mk(), mk()}
	assert.True(t, customDomainReadyByQuorum(context.Background(), resolvers, domain, 2),
		"a domain resolving to the public ingress IP must be ready regardless of the backend's private host_address")
}

func TestDNSGateAllows_NilFuncIsAlwaysReady(t *testing.T) {
	b := &Backend{} // customDomainDNSReady nil
	assert.True(t, b.dnsGateAllows(context.Background(), "anything.example.com"),
		"nil checker (tests / gate disabled) must allow emission unconditionally")
}

func TestDNSGateAllows_DelegatesToFunc(t *testing.T) {
	called := ""
	b := &Backend{customDomainDNSReady: func(_ context.Context, d string) bool { called = d; return false }}
	assert.False(t, b.dnsGateAllows(context.Background(), "app.example.com"))
	assert.Equal(t, "app.example.com", called)
}

func TestDeferUnreadyCustomDomains(t *testing.T) {
	// ENG-266 provision-path gate: zero the CustomDomain of items whose DNS
	// isn't resolving yet (so provision emits no -custom router / no order),
	// preserve ready ones, leave empties untouched. Mutates in place.
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.customDomainDNSReady = func(_ context.Context, d string) bool { return d == "ready.example.com" }

	items := []backend.LeaseItem{
		{ServiceName: "app", CustomDomain: "notready.example.com"},
		{ServiceName: "api", CustomDomain: "ready.example.com"},
		{ServiceName: "db", CustomDomain: ""},
	}
	b.deferUnreadyCustomDomains(context.Background(), items, "lease-1", b.logger)

	assert.Equal(t, "", items[0].CustomDomain, "not-ready domain must be deferred (zeroed)")
	assert.Equal(t, "ready.example.com", items[1].CustomDomain, "ready domain must be preserved")
	assert.Equal(t, "", items[2].CustomDomain, "empty stays empty")
}

func TestDeferUnreadyCustomDomains_ZeroesStoredProvItems(t *testing.T) {
	// ENG-193 regression: enrichReserved deep-copies Items, so prov.Items is a
	// DISTINCT array from the caller's req.Items. Deferring an unready domain
	// must zero BOTH — otherwise the in-memory prov.Items keeps the domain while
	// the emitted container labels (built from req.Items) have it zeroed, so
	// ReconcileCustomDomain reads a stale "already-emitted" value and never
	// re-applies the domain once DNS goes live.
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return false } // defer all

	// prov.Items is a separate slice from reqItems (mirrors the deep-copy), with
	// equal values index-for-index.
	storedItems := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "x.example.com"}}
	b.provisionsMu.Lock()
	b.provisions["L1"] = &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Items: storedItems}}
	b.provisionsMu.Unlock()
	reqItems := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "x.example.com"}}

	b.deferUnreadyCustomDomains(context.Background(), reqItems, "L1", b.logger)

	assert.Equal(t, "", reqItems[0].CustomDomain, "caller's slice (label-emit path) must be zeroed")
	b.provisionsMu.RLock()
	got := b.provisions["L1"].Items[0].CustomDomain
	b.provisionsMu.RUnlock()
	assert.Equal(t, "", got, "stored prov.Items must be zeroed too (distinct array post-deep-copy)")
}

func TestNewResolvers(t *testing.T) {
	servers := []string{"1.1.1.1:53", "8.8.8.8:53", "9.9.9.9:53"}
	rs := newResolvers(servers)
	require.Len(t, rs, len(servers), "one independent resolver per server")
	for i, r := range rs {
		res, ok := r.(*net.Resolver)
		require.Truef(t, ok, "resolver %d must be a *net.Resolver", i)
		assert.True(t, res.PreferGo, "must use the Go resolver so the per-server Dial is honored")
		assert.NotNil(t, res.Dial, "must pin queries to a single server via Dial")
	}
}

func TestIngressConfig_DnsQuorum(t *testing.T) {
	cases := []struct {
		name       string
		configured int
		n          int
		want       int
	}{
		{"unset → majority of 3", 0, 3, 2},
		{"unset → majority of 5", 0, 5, 3},
		{"explicit 2 of 3", 2, 3, 2},
		{"above n clamps to n", 9, 3, 3},
		{"explicit 1", 1, 3, 1},
		{"no resolvers → 1", 0, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ic := IngressConfig{CustomDomainDNSQuorum: tc.configured}
			assert.Equal(t, tc.want, ic.dnsQuorum(tc.n))
		})
	}
}

func TestDeferUnreadyCustomDomains_SuppressesCustomLabel(t *testing.T) {
	// End-to-end for the provision gate: the helper's mutation must actually
	// suppress the -custom Traefik router label in the built project (ENG-266),
	// and leave it present when the domain is ready.
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.customDomainDNSReady = func(_ context.Context, d string) bool { return d == "ready.example.com" }

	buildWebLabels := func(domain string) map[string]string {
		params := baseProjectParams()
		params.Stack.Services["web"] = &manifest.Manifest{
			Image: "nginx:latest",
			Ports: map[string]manifest.PortConfig{"80/tcp": {}},
		}
		params.Items[0].CustomDomain = domain
		params.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
		b.deferUnreadyCustomDomains(context.Background(), params.Items, params.LeaseUUID, b.logger)
		return buildComposeProject(params).Services["web"].Labels
	}

	t.Run("not ready → no -custom router / no custom-domain label", func(t *testing.T) {
		labels := buildWebLabels("notready.example.com")
		assert.Empty(t, labels[LabelCustomDomain])
		router := CustomDomainRouterName("lease-1", "web")
		assert.NotContains(t, labels, "traefik.http.routers."+router+".rule")
	})
	t.Run("ready → -custom router emitted", func(t *testing.T) {
		labels := buildWebLabels("ready.example.com")
		assert.Equal(t, "ready.example.com", labels[LabelCustomDomain])
	})
}

func TestDeferUnreadyCustomDomains_MutatesUnderLock(t *testing.T) {
	// Regression for the provision-path data race: in the provision path `items`
	// aliases the stored prov.Items, which other goroutines read under
	// provisionsMu — so the deferral mutation must hold the lock. Run under
	// -race with a concurrent locked reader of the same backing array; this
	// fails without the lock and passes with it.
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return false } // defer all

	items := []backend.LeaseItem{{ServiceName: "app", CustomDomain: "notready.example.com"}}
	b.provisionsMu.Lock()
	b.provisions["lease-1"] = &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1", Items: items}}
	b.provisionsMu.Unlock()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			b.provisionsMu.RLock()
			_ = b.provisions["lease-1"].Items[0].CustomDomain // same backing array as items[0]
			b.provisionsMu.RUnlock()
		}
	}()

	b.deferUnreadyCustomDomains(context.Background(), items, "lease-1", b.logger)
	close(stop)
	wg.Wait()

	assert.Equal(t, "", items[0].CustomDomain, "not-ready domain deferred")
}

func TestIngressConfig_Validate_DNSResolvers(t *testing.T) {
	base := IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	t.Run("empty list is valid (defaults apply)", func(t *testing.T) {
		require.NoError(t, base.Validate())
	})
	t.Run("valid host:port entries (incl. IPv6)", func(t *testing.T) {
		ic := base
		ic.CustomDomainDNSResolvers = []string{"1.1.1.1:53", "[2001:4860:4860::8888]:53"}
		require.NoError(t, ic.Validate())
	})
	t.Run("missing port is rejected", func(t *testing.T) {
		ic := base
		ic.CustomDomainDNSResolvers = []string{"1.1.1.1"}
		require.Error(t, ic.Validate())
	})
	t.Run("empty entry is rejected", func(t *testing.T) {
		ic := base
		ic.CustomDomainDNSResolvers = []string{""}
		require.Error(t, ic.Validate())
	})
	t.Run("non-numeric port is rejected", func(t *testing.T) {
		ic := base
		ic.CustomDomainDNSResolvers = []string{"1.1.1.1:abc"}
		require.Error(t, ic.Validate())
	})
	t.Run("out-of-range port is rejected", func(t *testing.T) {
		ic := base
		ic.CustomDomainDNSResolvers = []string{"1.1.1.1:65536"}
		require.Error(t, ic.Validate())
	})
	t.Run("port zero is rejected", func(t *testing.T) {
		ic := base
		ic.CustomDomainDNSResolvers = []string{"1.1.1.1:0"}
		require.Error(t, ic.Validate())
	})
}
