package docker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// daemonLaunchOutcome separates a completed daemon exchange from its business
// result. A rejected Create/Start can leave a known partial cohort. A transport
// failure may still reach Docker later and never supplies completion authority.
// Only the concrete SDK adapters mint settled outcomes; the zero value is unknown.
type daemonLaunchOutcome struct {
	settled bool
	err     error
}

func (o daemonLaunchOutcome) completionError() error {
	if o.settled {
		return nil
	}
	return errors.Join(errors.New("docker launch request completion is unknown"), o.err)
}

// Each admitted Create/Start keeps a bounded daemon exchange alive after caller
// cancellation. It fits inside the backend's shutdown drain, allowing the exact
// launch outcome to settle before journals close or compensation starts.
const daemonLaunchRequestTimeout = 30 * time.Second

// Each Compose invocation owns a permanently bound transport scope. Closing
// admission before returning prevents a detached Compose goroutine from issuing
// a new Create/Start after the journal has settled. The parent context controls
// admission even if Compose replaces its own child request context.
type daemonLaunchScope struct {
	mu       sync.Mutex
	ctx      context.Context
	closed   bool
	pending  int
	unknown  bool
	drained  chan struct{}
	observer *daemonLaunchObserver
}

func newDaemonLaunchScope(ctx context.Context, observer *daemonLaunchObserver) *daemonLaunchScope {
	return &daemonLaunchScope{ctx: ctx, observer: observer, drained: make(chan struct{})}
}

// A nonzero-sized constructor lineage binds the direct SDK observer to the
// same client retained by image admission and creation.
type daemonLaunchObserver struct{ _ byte }

// run is the single invocation boundary for synchronous direct SDK launches.
// The callback is retained only by constructor-owned adapters; callers cannot
// provide an error or boolean in place of the observed HTTP exchange.
func (o *daemonLaunchObserver) run(ctx context.Context, invoke func(context.Context) error) daemonLaunchOutcome {
	if o == nil {
		return daemonLaunchOutcome{err: errors.New("launch has no bound Docker observer")}
	}
	scope := newDaemonLaunchScope(ctx, o)
	defer scope.close()
	return scope.finish(invoke(withDaemonLaunchScope(ctx, scope)))
}

func (s *daemonLaunchScope) close() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if s.pending == 0 {
			close(s.drained)
		}
	}
	s.mu.Unlock()
}

func (s *daemonLaunchScope) finish(err error) daemonLaunchOutcome {
	s.close()
	// Compose can return before a detached worker does. Close admission first,
	// then drain already admitted requests without inheriting its cancellation.
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), daemonLaunchRequestTimeout)
	defer cancel()
	select {
	case <-s.drained:
	case <-drainCtx.Done():
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return daemonLaunchOutcome{settled: !s.unknown && s.pending == 0, err: err}
}

func daemonContainerLaunchRequest(req *http.Request) bool {
	if req.Method != http.MethodPost {
		return false
	}
	_, endpoint, found := strings.Cut(req.URL.Path, "/containers/")
	return found && (endpoint == "create" || strings.HasSuffix(endpoint, "/start"))
}

func (s *daemonLaunchScope) roundTrip(next http.RoundTripper, req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("docker launch invocation has ended")
	}
	if err := s.ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if err := req.Context().Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if !daemonContainerLaunchRequest(req) {
		s.mu.Unlock()
		return next.RoundTrip(req)
	}
	s.pending++
	s.mu.Unlock()
	var responded bool
	defer func() {
		s.mu.Lock()
		s.pending--
		s.unknown = s.unknown || !responded
		if s.closed && s.pending == 0 {
			close(s.drained)
		}
		s.mu.Unlock()
	}()
	admitted := newAdmittedDaemonRequest(req)
	response, err := admitted.roundTrip(next)
	// Create and Start are non-streaming endpoints. A final daemon response
	// follows the handler's operation even when decoding the response body or
	// the returned business status subsequently fails in the SDK.
	responded = err == nil && daemonCompletedLaunchResponse(req, response)
	return response, err
}

// admittedDaemonRequest owns the detached context until its transport returns.
// A successful response transfers cancellation to its body; every other exit,
// including a transport panic, releases it here.
type admittedDaemonRequest struct {
	request *http.Request
	cancel  context.CancelFunc
}

func newAdmittedDaemonRequest(req *http.Request) admittedDaemonRequest {
	// Once dispatch is admitted, an actor transition or Stop may stop future
	// launches but cannot turn our own cancellation into unknown completion.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(req.Context()), daemonLaunchRequestTimeout)
	return admittedDaemonRequest{request: req.Clone(ctx), cancel: cancel}
}

func (request admittedDaemonRequest) roundTrip(next http.RoundTripper) (*http.Response, error) {
	handedOff := false
	defer func() {
		if !handedOff {
			request.cancel()
		}
	}()
	response, err := next.RoundTrip(request.request)
	if response != nil && response.Body != nil && err == nil {
		response.Body = daemonLaunchResponseBody{ReadCloser: response.Body, cancel: request.cancel}
		handedOff = true
	}
	return response, err
}

// A response header proves handler completion, but the SDK still needs the
// response body. Keep the bounded request context alive until it closes it.
type daemonLaunchResponseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b daemonLaunchResponseBody) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}

func daemonCompletedLaunchResponse(req *http.Request, response *http.Response) bool {
	if response == nil {
		return false
	}
	// These are the concrete non-streaming Create/Start API outcomes. Redirects
	// and gateway/unavailable statuses do not prove that a daemon handler ended.
	// The configured endpoint must be the trusted Docker API, not an HTTP proxy.
	switch response.StatusCode {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusInternalServerError:
		return true
	case http.StatusForbidden:
		// Docker authorization plugins may deny the request before the handler
		// or its response after the handler has finished. Both end this exchange;
		// neither proves the absence of an effect. Keep the business error and
		// require the same storage postcheck and exact cleanup as other failures.
		return true
	case http.StatusCreated:
		return strings.HasSuffix(req.URL.Path, "/containers/create")
	case http.StatusNoContent, http.StatusNotModified:
		return strings.HasSuffix(req.URL.Path, "/start")
	default:
		return false
	}
}

type daemonLaunchTransport struct {
	next  http.RoundTripper
	scope *daemonLaunchScope
}

func (t daemonLaunchTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.scope.roundTrip(t.next, req)
}

type daemonLaunchContextKey struct{}

// The direct SDK adapter installs this once before image admission/creation is
// bound to the client. Its synchronous Create/Start implementations preserve the
// supplied context; Compose instead uses the instance-bound transport above.
type daemonContextTransport struct {
	next     http.RoundTripper
	observer *daemonLaunchObserver
}

func (t daemonContextTransport) RoundTrip(req *http.Request) (response *http.Response, err error) {
	defer func() { response, err = boundImageInspectResponse(req, response, err) }()
	if scope, ok := req.Context().Value(instanceInspectionContextKey{}).(*instanceInspectionScope); ok {
		defer func() { scope.observe(t.observer, req, response, err) }()
	}
	if scope, ok := req.Context().Value(daemonLaunchContextKey{}).(*daemonLaunchScope); ok {
		if t.observer == nil || scope.observer != t.observer {
			return nil, errors.New("docker launch observer belongs to another client")
		}
		return scope.roundTrip(t.next, req)
	}
	return t.next.RoundTrip(req)
}

func withDaemonLaunchScope(ctx context.Context, scope *daemonLaunchScope) context.Context {
	return context.WithValue(ctx, daemonLaunchContextKey{}, scope)
}
