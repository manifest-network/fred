package docker

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
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

// Each Compose invocation owns a permanently bound transport scope. Closing
// admission before returning prevents a detached Compose goroutine from issuing
// a new Create/Start after the journal has settled. Direct SDK methods preserve
// their supplied context synchronously and use the same observer per call.
type daemonLaunchScope struct {
	mu       sync.Mutex
	closed   bool
	pending  int
	unknown  bool
	observer *daemonLaunchObserver
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
	scope := &daemonLaunchScope{observer: o}
	defer scope.close()
	return scope.finish(invoke(withDaemonLaunchScope(ctx, scope)))
}

func (s *daemonLaunchScope) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

func (s *daemonLaunchScope) finish(err error) daemonLaunchOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
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
	tracked := daemonContainerLaunchRequest(req)
	if tracked {
		s.pending++
	}
	s.mu.Unlock()
	var responded bool
	defer func() {
		if tracked {
			s.mu.Lock()
			s.pending--
			s.unknown = s.unknown || !responded
			s.mu.Unlock()
		}
	}()
	response, err := next.RoundTrip(req)
	// Create and Start are non-streaming endpoints. A final daemon response
	// follows the handler's operation even when decoding the response body or
	// the returned business status subsequently fails in the SDK.
	responded = err == nil && daemonCompletedLaunchResponse(req, response)
	return response, err
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
