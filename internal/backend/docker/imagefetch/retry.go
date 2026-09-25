package imagefetch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	registryauth "github.com/google/go-containerregistry/pkg/v1/remote/transport"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	registryAttempts      = 3
	registryRetryAfterMax = 30 * time.Second
)

var (
	errRegistryRedirectExpired = errors.New("registry redirected blob returned HTTP 403")
	errRegistryAttemptsSpent   = errors.New("registry request exhausted its attempt budget")
)

// registryTransport publishes metadata only after its complete bounded response
// arrives. Authentication and token redirects always use this metadata route.
type registryTransport struct{ base boundedTransport }

// registryBlobDispatch owns the descriptor's complete attempt allowance. Only
// requests registered by authenticatedBlobTransport can use blob-size limits;
// token requests cannot gain that authority by choosing a blob-looking URL or
// inheriting a context, including when a token redirects to the exact blob URL.
type registryBlobDispatch struct {
	base     boundedTransport
	attempts registryAttemptBudget
	active   sync.Map // exact *http.Request identities registered during auth calls
}

func (d *registryBlobDispatch) RoundTrip(request *http.Request) (*http.Response, error) {
	if _, authorized := d.active.Load(request); authorized {
		if !d.attempts.claim() {
			return nil, errRegistryAttemptsSpent
		}
		return (blobRedirectTransport{base: d.base}).RoundTrip(request)
	}
	metadata := d.base
	metadata.limit = min(metadata.limit, maxMetadataBytes)
	return (registryTransport{base: metadata}).RoundTrip(request)
}

// The authentication boundary reuses one exact request for a 401 renewal. Its
// separately created token requests have no registration. Keeping redirects
// below this boundary also prevents bearer credentials from being reattached
// on CDN hops.
type authenticatedBlobTransport struct {
	authentication http.RoundTripper
	dispatch       *registryBlobDispatch
}

func (t authenticatedBlobTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.dispatch.active.Store(request, struct{}{})
	defer t.dispatch.active.Delete(request)
	return t.authentication.RoundTrip(request)
}

func newRegistryBlobTransfer(ctx context.Context, ref name.Reference, descriptor ocispec.Descriptor, base boundedTransport) (*registryTransfer, error) {
	repo := ref.Context()
	location := url.URL{Scheme: "https", Host: repo.RegistryStr(), Path: fmt.Sprintf("/v2/%s/blobs/%s", repo.RepositoryStr(), descriptor.Digest)}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, location.String(), nil)
	if err != nil {
		return nil, err
	}
	dispatch := &registryBlobDispatch{base: base}
	authentication, err := registryauth.NewWithContext(ctx, repo.Registry, authn.Anonymous, dispatch, []string{repo.Scope(registryauth.PullScope)})
	if err != nil {
		return nil, err
	}
	return &registryTransfer{transport: authenticatedBlobTransport{authentication: authentication, dispatch: dispatch}, request: request, attempts: &dispatch.attempts, size: descriptor.Size}, nil
}

// Metadata retries use the same claim-before-dispatch rule, with a separate
// request-local owner. Blob claims instead live below authentication renewal.
type attemptedRegistryTransport struct {
	base     http.RoundTripper
	attempts *registryAttemptBudget
}

func (t attemptedRegistryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if !t.attempts.claim() {
		return nil, errRegistryAttemptsSpent
	}
	return t.base.RoundTrip(request)
}

type registryAttemptBudget struct{ spent atomic.Uint32 }

func (b *registryAttemptBudget) available() bool {
	return b != nil && b.spent.Load() < registryAttempts
}

func (b *registryAttemptBudget) claim() bool {
	for b.available() {
		spent := b.spent.Load()
		if spent < registryAttempts && b.spent.CompareAndSwap(spent, spent+1) {
			return true
		}
	}
	return false
}

func (t registryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return t.base.RoundTrip(req)
	}
	attempts := &registryAttemptBudget{}
	transfer := &registryTransfer{transport: attemptedRegistryTransport{base: t.base, attempts: attempts}, request: req, attempts: attempts}
	for {
		response, err := transfer.open(0)
		if err != nil {
			return nil, err
		}
		// Bound authentication/error metadata too, but preserve its status for
		// the registry client's challenge and redirect handling.
		body, err := readRegistryMetadata(response.Body)
		if err == nil {
			response.Body = io.NopCloser(bytes.NewReader(body))
			return response, nil
		}
		if !transfer.canRetry(err) {
			return nil, err
		}
	}
}

func readRegistryMetadata(body io.ReadCloser) ([]byte, error) {
	defer func() { _ = body.Close() }()
	return io.ReadAll(&budgetReader{reader: body, remaining: maxMetadataBytes})
}

// blobRedirectTransport keeps redirect traversal inside one transfer attempt.
// Its redirect policy preserves the registry client's private-IP refusal and
// binds credentials to the original host. The bounded transport enforces HTTPS
// and idle/byte limits on every hop.
type blobRedirectTransport struct{ base boundedTransport }

func (t blobRedirectTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	redirected := false
	client := http.Client{Transport: t.base, CheckRedirect: func(next *http.Request, via []*http.Request) error {
		if err := registryBlobRedirect(next, via); err != nil {
			return err
		}
		redirected = true
		return nil
	}}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	if redirected && response.StatusCode == http.StatusForbidden {
		_ = response.Body.Close()
		return nil, errRegistryRedirectExpired
	}
	return response, nil
}

func registryBlobRedirect(request *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("registry blob exceeded redirect limit")
	}
	original := via[0].URL
	if request.URL.Host != original.Host {
		// net/http permits credentials on subdomain redirects by default;
		// registry credentials belong to this exact registry authority only.
		request.Header.Del("Authorization")
	}
	if request.URL.Hostname() != original.Hostname() {
		ip := net.ParseIP(request.URL.Hostname())
		if ip != nil && (ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate() || ip.IsUnspecified()) {
			return errors.New("registry blob redirect to private or link-local IP is forbidden")
		}
	}
	return nil
}

type registryTransfer struct {
	transport http.RoundTripper
	request   *http.Request
	attempts  *registryAttemptBudget
	delay     time.Duration
	size      int64
}

func (t *registryTransfer) canRetry(err error) bool {
	return t.attempts.available() && t.request.Context().Err() == nil && transientRegistryError(err)
}

func transientRegistryError(err error) bool {
	var network net.Error
	var interrupted interruptedRegistryHeaders
	return errors.As(err, &interrupted) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errRegistryIdle) || errors.Is(err, errRegistryRedirectExpired) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, net.ErrClosed) ||
		(errors.As(err, &network) && network.Timeout())
}

func transientRegistryStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func (t *registryTransfer) open(offset int64) (*http.Response, error) {
	for {
		if err := t.request.Context().Err(); err != nil {
			return nil, err
		}
		if !t.attempts.available() {
			return nil, errRegistryAttemptsSpent
		}
		if spent := t.attempts.spent.Load(); spent > 0 {
			timer := time.NewTimer(max(time.Duration(spent)*100*time.Millisecond, t.delay))
			select {
			case <-timer.C:
			case <-t.request.Context().Done():
				timer.Stop()
				return nil, t.request.Context().Err()
			}
		}
		t.delay = 0
		request := t.request.Clone(t.request.Context())
		request.Header.Del("Range")
		if offset > 0 {
			request.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
		response, err := t.transport.RoundTrip(request)
		if err != nil {
			if t.canRetry(err) {
				continue
			}
			return nil, err
		}
		if transientRegistryStatus(response.StatusCode) && t.attempts.available() {
			t.delay = registryRetryAfter(response)
			_ = response.Body.Close()
			continue
		}
		return response, nil
	}
}

func registryRetryAfter(response *http.Response) time.Duration {
	if response.StatusCode != http.StatusTooManyRequests && response.StatusCode != http.StatusServiceUnavailable {
		return 0
	}
	value := response.Header.Get("Retry-After")
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		return time.Duration(min(seconds, int64(registryRetryAfterMax/time.Second))) * time.Second
	}
	if deadline, err := http.ParseTime(value); err == nil {
		return min(max(time.Until(deadline), 0), registryRetryAfterMax)
	}
	return 0
}

func (t *registryTransfer) openBlob(offset int64) (*http.Response, error) {
	requestedOffset := offset
	for {
		response, err := t.open(requestedOffset)
		if err != nil {
			return nil, err
		}
		if response.StatusCode == http.StatusRequestedRangeNotSatisfiable && requestedOffset > 0 && t.attempts.available() {
			// Some registries reject open-ended ranges. A new full GET shares
			// the existing attempt bound, then acceptBlob replays only the
			// retained prefix before publishing any new bytes.
			_ = response.Body.Close()
			requestedOffset = 0
			continue
		}
		if err := t.acceptBlob(response, offset); err != nil {
			if t.canRetry(err) {
				continue
			}
			return nil, err
		}
		return response, nil
	}
}

func (t *registryTransfer) acceptBlob(response *http.Response, offset int64) error {
	accepted := false
	defer func() {
		if !accepted {
			_ = response.Body.Close()
		}
	}()
	remaining := t.size - offset
	switch response.StatusCode {
	case http.StatusPartialContent:
		if offset == 0 || response.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", offset, t.size-1, t.size) {
			return errors.New("registry resumed blob has an invalid content range")
		}
	case http.StatusOK:
		remaining = t.size
	default:
		accepted = true
		return nil
	}
	if response.ContentLength >= 0 && response.ContentLength != remaining {
		return errors.New("registry blob length differs from its verified descriptor")
	}
	if offset > 0 && response.StatusCode == http.StatusOK {
		// Registries may ignore Range. Replay only this blob's prefix,
		// retaining the original hash and the single staging allocation.
		if _, err := io.CopyN(io.Discard, response.Body, offset); err != nil {
			return err
		}
	}
	accepted = true
	return nil
}

type resumingBody struct {
	transfer *registryTransfer
	body     io.ReadCloser
	offset   int64
	resume   bool
}

func (b *resumingBody) Read(p []byte) (int, error) {
	for {
		if b.resume {
			response, err := b.transfer.openBlob(b.offset)
			if err != nil {
				return 0, err
			}
			if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
				_ = response.Body.Close()
				return 0, fmt.Errorf("registry blob resume returned HTTP %d", response.StatusCode)
			}
			b.body, b.resume = response.Body, false
		}
		n, err := b.body.Read(p)
		b.offset += int64(n)
		if err != nil && b.offset < b.transfer.size && b.transfer.canRetry(err) {
			_ = b.body.Close()
			b.resume = true
			if n == 0 {
				continue
			}
			err = nil
		}
		return n, err
	}
}

func (b *resumingBody) Close() error { return b.body.Close() }
