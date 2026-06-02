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

func TestCustomDomainResolvesToHost_Match(t *testing.T) {
	res := &fakeResolver{hosts: map[string][]net.IPAddr{
		"app.example.com":         ipAddrs("203.0.113.8"),
		"s100-u028.manifest0.net": ipAddrs("203.0.113.8"),
	}}
	ok, err := customDomainResolvesToHost(context.Background(), res, "app.example.com", "s100-u028.manifest0.net")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestCustomDomainResolvesToHost_NoMatch_Proxied(t *testing.T) {
	// Orange-cloud: domain resolves to a CDN edge IP, not the host.
	res := &fakeResolver{hosts: map[string][]net.IPAddr{
		"app.example.com":         ipAddrs("104.16.0.1"),
		"s100-u028.manifest0.net": ipAddrs("203.0.113.8"),
	}}
	ok, err := customDomainResolvesToHost(context.Background(), res, "app.example.com", "s100-u028.manifest0.net")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCustomDomainResolvesToHost_HostIsLiteralIP(t *testing.T) {
	res := &fakeResolver{hosts: map[string][]net.IPAddr{
		"app.example.com": ipAddrs("203.0.113.8"),
	}}
	ok, err := customDomainResolvesToHost(context.Background(), res, "app.example.com", "203.0.113.8")
	require.NoError(t, err)
	assert.True(t, ok, "host given as a literal IP must not be looked up")
}

func TestCustomDomainResolvesToHost_DomainNXDOMAIN(t *testing.T) {
	res := &fakeResolver{hosts: map[string][]net.IPAddr{
		"s100-u028.manifest0.net": ipAddrs("203.0.113.8"),
	}}
	ok, err := customDomainResolvesToHost(context.Background(), res, "missing.example.com", "s100-u028.manifest0.net")
	require.NoError(t, err, "NXDOMAIN on the custom domain is not-ready, not an error")
	assert.False(t, ok)
}

func TestCustomDomainResolvesToHost_HostUnresolvable_IsError(t *testing.T) {
	res := &fakeResolver{
		hosts: map[string][]net.IPAddr{"app.example.com": ipAddrs("203.0.113.8")},
		errs:  map[string]error{"s100-u028.manifest0.net": errors.New("server misbehaving")},
	}
	_, err := customDomainResolvesToHost(context.Background(), res, "app.example.com", "s100-u028.manifest0.net")
	require.Error(t, err, "host resolution failure is a config/infra error, surfaced to caller")
}

func TestCustomDomainResolvesToHost_IPv6Match(t *testing.T) {
	res := &fakeResolver{hosts: map[string][]net.IPAddr{
		"app.example.com":         ipAddrs("2001:db8::1"),
		"s100-u028.manifest0.net": ipAddrs("203.0.113.8", "2001:db8::1"),
	}}
	ok, err := customDomainResolvesToHost(context.Background(), res, "app.example.com", "s100-u028.manifest0.net")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestCustomDomainResolvesToHost_HostAddressWithPort(t *testing.T) {
	// host_address may include a port (Config.Validate allows it); the readiness
	// check must resolve the host part, not the literal "host:port".
	res := &fakeResolver{hosts: map[string][]net.IPAddr{
		"app.example.com":         ipAddrs("203.0.113.8"),
		"s100-u028.manifest0.net": ipAddrs("203.0.113.8"),
	}}
	t.Run("literal IP with port", func(t *testing.T) {
		ok, err := customDomainResolvesToHost(context.Background(), res, "app.example.com", "203.0.113.8:443")
		require.NoError(t, err)
		assert.True(t, ok)
	})
	t.Run("hostname with port", func(t *testing.T) {
		ok, err := customDomainResolvesToHost(context.Background(), res, "app.example.com", "s100-u028.manifest0.net:8443")
		require.NoError(t, err)
		assert.True(t, ok)
	})
}

func TestCustomDomainReadyByQuorum(t *testing.T) {
	const host, hostIP = "s100-u028.manifest0.net", "203.0.113.8"
	// mk builds a resolver whose view of app.example.com is domainIP
	// ("" = NXDOMAIN); the host always resolves to hostIP.
	mk := func(domainIP string) ipResolver {
		hosts := map[string][]net.IPAddr{host: ipAddrs(hostIP)}
		if domainIP != "" {
			hosts["app.example.com"] = ipAddrs(domainIP)
		}
		return &fakeResolver{hosts: hosts}
	}
	cases := []struct {
		name      string
		domainIPs []string // per-resolver view of app.example.com
		quorum    int
		want      bool
	}{
		{"all three agree", []string{hostIP, hostIP, hostIP}, 2, true},
		{"majority despite one stale NXDOMAIN", []string{"", hostIP, hostIP}, 2, true},
		{"majority despite one proxied edge IP", []string{"104.16.0.1", hostIP, hostIP}, 2, true},
		{"only one propagated", []string{hostIP, "", ""}, 2, false},
		{"none resolve", []string{"", "", ""}, 2, false},
		{"no resolvers configured (gate stays closed)", []string{}, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolvers := make([]ipResolver, len(tc.domainIPs))
			for i, ip := range tc.domainIPs {
				resolvers[i] = mk(ip)
			}
			got, _ := customDomainReadyByQuorum(context.Background(), resolvers, "app.example.com", host, tc.quorum)
			assert.Equal(t, tc.want, got)
		})
	}
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
	// isn't pointing here yet (so provision emits no -custom router / no order),
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
}
