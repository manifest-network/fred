package docker

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolvers := make([]ipResolver, len(tc.domainIPs))
			for i, ip := range tc.domainIPs {
				resolvers[i] = mk(ip)
			}
			got := customDomainReadyByQuorum(context.Background(), resolvers, "app.example.com", host, tc.quorum)
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
