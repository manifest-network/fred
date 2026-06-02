package docker

import (
	"context"
	"fmt"
	"net"
	"sync"
)

// ipResolver is the subset of *net.Resolver the readiness check needs.
// Abstracted so tests can supply deterministic answers without network I/O.
type ipResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// customDomainResolvesToHost reports whether `domain` currently resolves to an
// IP this backend also serves on — the necessary-and-sufficient DNS condition
// for an HTTP-01 challenge against `domain` to reach this host's Traefik
// (routing itself is by Host header, independent of DNS record type).
//
// hostAddress is the backend's external address (cfg.HostAddress): a literal
// IP (used as-is) or a hostname (resolved). Returns:
//   - (true, nil)  when the IP sets overlap,
//   - (false, nil) when the domain doesn't resolve or shares no IP with the
//     host (not ready yet, or proxied/CDN — both correctly "don't order"),
//   - (false, err) only when the HOST itself can't be resolved (a config/infra
//     problem the caller should log and treat as "skip this tick").
func customDomainResolvesToHost(ctx context.Context, res ipResolver, domain, hostAddress string) (bool, error) {
	hostIPs, err := resolveToIPs(ctx, res, hostAddress)
	if err != nil {
		return false, fmt.Errorf("resolve host_address %q: %w", hostAddress, err)
	}
	if len(hostIPs) == 0 {
		return false, fmt.Errorf("host_address %q resolved to no IPs", hostAddress)
	}

	domainAddrs, derr := res.LookupIPAddr(ctx, domain)
	if derr != nil {
		// NXDOMAIN / SERVFAIL / timeout on the tenant domain == not ready yet.
		// This is the expected pre-propagation state, not an error.
		return false, nil
	}

	hostSet := make(map[string]struct{}, len(hostIPs))
	for _, ip := range hostIPs {
		hostSet[ip.String()] = struct{}{}
	}
	for _, a := range domainAddrs {
		if _, ok := hostSet[a.IP.String()]; ok {
			return true, nil
		}
	}
	return false, nil
}

// resolveToIPs returns the IPs for addr, treating a literal IP as a 1-element
// set without a lookup.
func resolveToIPs(ctx context.Context, res ipResolver, addr string) ([]net.IP, error) {
	if ip := net.ParseIP(addr); ip != nil {
		return []net.IP{ip}, nil
	}
	addrs, err := res.LookupIPAddr(ctx, addr)
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		ips = append(ips, a.IP)
	}
	return ips, nil
}

// newResolverForServer builds a *net.Resolver that sends every query to a
// single DNS server (host:port) instead of the system resolver — one
// independent vantage point for the quorum check.
func newResolverForServer(server string) *net.Resolver {
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, server)
		},
	}
}

// newResolvers builds one independent resolver per configured server.
func newResolvers(servers []string) []ipResolver {
	out := make([]ipResolver, 0, len(servers))
	for _, s := range servers {
		out = append(out, newResolverForServer(s))
	}
	return out
}

// majorityQuorum returns floor(n/2)+1 (3->2, 2->2, 1->1).
func majorityQuorum(n int) int { return n/2 + 1 }

// customDomainReadyByQuorum queries each resolver INDEPENDENTLY and returns
// true iff at least `quorum` of them see `domain` resolving to a host IP.
// Mirrors Let's Encrypt multi-perspective validation: one resolver's stale
// NXDOMAIN cache can't block issuance (others still count) and one
// early-propagated resolver can't force a premature order. A resolver whose
// host lookup errors casts a "not ready" vote for that perspective.
//
// The N lookups run concurrently and are all joined before return (no
// goroutine outlives this call); the buffered channel guarantees no sender
// blocks; ctx cancellation/timeout bounds every lookup. With 0 resolvers it
// returns false (gate stays closed rather than silently opening).
func customDomainReadyByQuorum(ctx context.Context, resolvers []ipResolver, domain, hostAddress string, quorum int) bool {
	if len(resolvers) == 0 {
		return false
	}
	votes := make(chan bool, len(resolvers))
	var wg sync.WaitGroup
	for _, r := range resolvers {
		wg.Add(1)
		go func(rr ipResolver) {
			defer wg.Done()
			ok, err := customDomainResolvesToHost(ctx, rr, domain, hostAddress)
			votes <- (err == nil && ok)
		}(r)
	}
	wg.Wait()
	close(votes)
	ready := 0
	for v := range votes {
		if v {
			ready++
		}
	}
	return ready >= quorum
}
