package docker

import (
	"context"
	"net"
	"sync"
)

// ipResolver is the subset of *net.Resolver the readiness check needs.
// Abstracted so tests can supply deterministic answers without network I/O.
type ipResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// customDomainResolves reports whether `domain` currently resolves to at least
// one IP address — the necessary-and-sufficient DNS precondition for firing an
// HTTP-01 order without poisoning the CA's negative cache (ENG-266).
//
// It deliberately does NOT check that the domain resolves to any particular
// host IP (ENG-618). fred's `host_address` is the backend's PRIVATE br1
// service-plane IP (internal_ip), whereas a tenant custom domain legitimately
// CNAMEs to the host's PUBLIC ingress IP — the same target the *.barneyN
// wildcard uses. Comparing the two (the previous behavior) could never match on
// the production topology, so the gate would defer forever and a custom domain
// dropped by a container recreate (reboot/reprovision) was never re-attached.
// This mirrors the idiomatic multi-tenant TLS pattern: authorization (fred's
// on-chain custom_domain claim) decides *whether* to issue; DNS readiness only
// avoids ordering while the name is still NXDOMAIN, and the ACME challenge
// itself is the authoritative "does it actually point here" test.
//
// Returns:
//   - true  when the domain resolves to one or more addresses,
//   - false when it does not resolve yet (NXDOMAIN / SERVFAIL / timeout / empty
//     answer) — the expected pre-propagation state, not an error to surface.
func customDomainResolves(ctx context.Context, res ipResolver, domain string) bool {
	addrs, err := res.LookupIPAddr(ctx, domain)
	return err == nil && len(addrs) > 0
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
// whether at least `quorum` of them see `domain` resolving.
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
// returns false — the gate stays closed rather than silently opening.
func customDomainReadyByQuorum(ctx context.Context, resolvers []ipResolver, domain string, quorum int) bool {
	if len(resolvers) == 0 {
		return false
	}
	// Cancel outstanding lookups as soon as the result is decided.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	votes := make(chan bool, len(resolvers))
	var wg sync.WaitGroup
	for _, r := range resolvers {
		wg.Add(1)
		go func(rr ipResolver) {
			defer wg.Done()
			votes <- customDomainResolves(cctx, rr, domain)
		}(r)
	}

	ready, notReady := 0, 0
	maxNotReady := len(resolvers) - quorum // once notReady exceeds this, quorum is impossible
	for range resolvers {
		if <-votes {
			ready++
		} else {
			notReady++
		}
		if ready >= quorum || notReady > maxNotReady {
			cancel() // decided — let the remaining lookups return promptly
			break
		}
	}
	wg.Wait() // reap every goroutine; no leak
	return ready >= quorum
}
