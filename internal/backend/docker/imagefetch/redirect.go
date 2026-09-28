package imagefetch

import (
	"context"
	"errors"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
)

const registryRedirectRequests = 10

// registryRedirectChain owns the allowance and origin for one complete chain.
// Only package-issued response receipts carry it into the registry library's
// next request. Copies share its counter; neither response headers nor a new
// URL can mint another allowance halfway through a chain.
type registryRedirectChain struct {
	authority string
	hostname  string
	spent     atomic.Uint32
	// operation also bounds every other chain its metadata operation starts.
	operation *registryOperationAllowance
}

// registryOperationAllowance is one logical metadata operation's complete
// exchange allowance. A tag HEAD and the GET that it selects form one tag
// resolution, so a failed or redirected HEAD cannot buy the GET a second
// ten-exchange chain. Authentication renewal inside the operation also spends
// from it; this only narrows, and never widens, any chain's own allowance.
type registryOperationAllowance struct{ spent atomic.Uint32 }

type registryOperationAllowanceKey struct{}

func withRegistryOperationAllowance(ctx context.Context) context.Context {
	return context.WithValue(ctx, registryOperationAllowanceKey{}, &registryOperationAllowance{})
}

type registryRedirectReceiptKey struct{}

type registryRedirectReceipt struct {
	chain    *registryRedirectChain
	response *http.Response
}

func newRegistryRedirectChain(request *http.Request) *registryRedirectChain {
	return &registryRedirectChain{authority: request.URL.Host, hostname: request.URL.Hostname()}
}

func registryMetadataRedirectChain(request *http.Request) (*registryRedirectChain, error) {
	if request.Response == nil {
		chain := newRegistryRedirectChain(request)
		chain.operation, _ = request.Context().Value(registryOperationAllowanceKey{}).(*registryOperationAllowance)
		return chain, nil
	}
	previous := request.Response
	if previous.Request != nil {
		receipt, ok := previous.Request.Context().Value(registryRedirectReceiptKey{}).(registryRedirectReceipt)
		if ok && receipt.chain != nil && receipt.response == previous {
			return receipt.chain, nil
		}
	}
	return nil, errors.New("registry redirect has no issued chain receipt")
}

func (c *registryRedirectChain) admit(request *http.Request) (*http.Request, error) {
	if request.URL.Scheme != "https" {
		return nil, errors.New("image registries and redirects require HTTPS")
	}
	if request.URL.Hostname() != c.hostname && privateRegistryIP(request.URL.Hostname()) {
		return nil, errors.New("registry redirect to private or link-local IP is forbidden")
	}
	// An operation's count includes this chain's, so its claim is checked
	// first; a chain claim after a successful operation claim cannot fail.
	if (c.operation != nil && !claimRegistryExchange(&c.operation.spent)) || !claimRegistryExchange(&c.spent) {
		return nil, errors.New("registry exceeded redirect limit")
	}
	owned := request.Clone(request.Context())
	if owned.URL.Host != c.authority {
		// net/http ordinarily forwards credentials to subdomains. Registry
		// credentials belong to this exact original authority instead.
		owned.Header.Del("Authorization")
	}
	return owned, nil
}

func claimRegistryExchange(spent *atomic.Uint32) bool {
	for {
		current := spent.Load()
		if current >= registryRedirectRequests {
			return false
		}
		if spent.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (c *registryRedirectChain) stamp(response *http.Response, request *http.Request) {
	if response != nil {
		receipt := registryRedirectReceipt{chain: c, response: response}
		response.Request = request.WithContext(context.WithValue(request.Context(), registryRedirectReceiptKey{}, receipt))
	}
}

// registryRedirectTransport keeps a blob's redirects below authentication, so
// the authentication wrapper cannot reattach credentials to a different host.
type registryRedirectTransport struct {
	base  boundedTransport
	chain *registryRedirectChain
}

func (t registryRedirectTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	owned, err := t.chain.admit(request)
	if err != nil {
		return nil, err
	}
	response, err := t.base.RoundTrip(owned)
	t.chain.stamp(response, owned)
	return response, err
}

func privateRegistryIP(host string) bool {
	address, ok := registryIPLiteral(host)
	return ok && (address.IsLoopback() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsPrivate() || address.IsUnspecified())
}

// registryIPLiteral recognizes both canonical IPs and legacy inet_aton forms,
// matching the registry client's IP-literal policy. A trailing DNS root dot and
// an IPv6 zone cannot disguise a private address. DNS resolution stays with the
// configured transport, as it does in go-containerregistry's redirect policy.
func registryIPLiteral(host string) (netip.Addr, bool) {
	host = strings.TrimSuffix(host, ".")
	if address, err := netip.ParseAddr(host); err == nil {
		return address.WithZone("").Unmap(), true
	}
	parts := strings.Split(host, ".")
	if len(parts) > 4 {
		return netip.Addr{}, false
	}
	var encoded uint64
	for index, part := range parts {
		bits := 8
		if index == len(parts)-1 {
			bits = 8 * (4 - index)
		}
		value, err := strconv.ParseUint(part, 0, bits)
		if err != nil {
			return netip.Addr{}, false
		}
		encoded = encoded<<bits | value
	}
	return netip.AddrFrom4([4]byte{byte(encoded >> 24), byte(encoded >> 16), byte(encoded >> 8), byte(encoded)}), true
}
