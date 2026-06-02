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
		return false, nil //nolint:nilerr // domain not resolving yet is "not ready", not an error to surface
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
	// host_address may carry a port (Config.Validate accepts e.g.
	// "203.0.113.8:443" / "example.com:8443" and strips it the same way);
	// resolve the host part, not the literal "host:port".
	if h, _, err := net.SplitHostPort(addr); err == nil {
		addr = h
	}
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
// whether at least `quorum` of them see `domain` resolving to a host IP, plus
// the first host-resolution error encountered (if any). Callers should log a
// non-nil error: it means host_address itself could not be resolved (a
// config/infra problem) which otherwise silently defers ALL issuance with no
// operator signal. A domain that simply doesn't resolve yet is NOT an error —
// it's a "not ready" vote.
//
// Mirrors Let's Encrypt multi-perspective validation: one resolver's stale
// NXDOMAIN cache can't block issuance (others still count) and one
// early-propagated resolver can't force a premature order.
//
// The N lookups run concurrently; once the outcome is decided (quorum reached,
// or no longer reachable) the remaining lookups are canceled so a slow/hung
// resolver can't add its full timeout to the readiness latency. All goroutines
// are still joined (wg.Wait) before return — none outlives this call — and the
// buffered channel guarantees no sender ever blocks. With 0 resolvers it
// returns (false, nil) — the gate stays closed rather than silently opening.
func customDomainReadyByQuorum(ctx context.Context, resolvers []ipResolver, domain, hostAddress string, quorum int) (bool, error) {
	if len(resolvers) == 0 {
		return false, nil
	}
	// Cancel outstanding lookups as soon as the result is decided.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type vote struct {
		ready   bool
		hostErr error
	}
	votes := make(chan vote, len(resolvers))
	var wg sync.WaitGroup
	for _, r := range resolvers {
		wg.Add(1)
		go func(rr ipResolver) {
			defer wg.Done()
			ok, err := customDomainResolvesToHost(cctx, rr, domain, hostAddress)
			votes <- vote{ready: err == nil && ok, hostErr: err}
		}(r)
	}

	ready, notReady := 0, 0
	maxNotReady := len(resolvers) - quorum // once notReady exceeds this, quorum is impossible
	var hostErr error
	for range resolvers {
		v := <-votes
		if v.ready {
			ready++
		} else {
			notReady++
		}
		if v.hostErr != nil && hostErr == nil {
			hostErr = v.hostErr
		}
		if ready >= quorum || notReady > maxNotReady {
			cancel() // decided — stop the remaining lookups
			break
		}
	}
	wg.Wait() // reap every goroutine (canceled lookups return promptly); no leak
	return ready >= quorum, hostErr
}
