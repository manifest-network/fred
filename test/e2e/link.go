//go:build integration

package e2e

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Link is the connection from providerd to one backend, with the test in the
// middle of it. It forwards every request and response unchanged, until a
// test asks it to lose one. A lost response: the backend receives the request
// and does the work, and its caller never hears back. A lost request: the
// backend never receives it, and its caller cannot tell the two apart.
//
// The link ends the caller's TLS with a certificate of its own, and calls the
// backend as providerd would, with the backend's client certificate. Request
// signatures cover the path and the body, which the link does not touch.
type Link struct {
	// URL is where the caller reaches the backend through the link.
	URL string
	// CAFile is the authority the caller must trust for URL.
	CAFile string

	target    *url.URL
	transport http.RoundTripper

	mu          sync.Mutex
	lose        *LostResponse
	loseRequest *LostRequest
	sent        map[string]Command
}

// Command is one request a caller sent through a Link, kept as it was sent.
type Command struct {
	link   *Link
	method string
	uri    string
	header http.Header
	body   []byte
}

// LostResponse is one response a Link was asked to lose.
type LostResponse struct {
	operation string
	settled   func() bool

	done    chan struct{}
	reached bool
}

// LostRequest is one request a Link was asked to lose.
type LostRequest struct {
	operation string
	done      chan struct{}
}

// errLost marks a response the link discards.
var errLost = errors.New("response lost by the test")

// lostResponseBudget is how long a link holds a response while it waits for
// the operation behind it to settle.
const lostResponseBudget = 2 * time.Minute

// StartLink puts a Link in front of a backend.
func StartLink(t *testing.T, b *Backend) *Link {
	t.Helper()
	target, err := url.Parse(b.URL)
	require.NoError(t, err)
	backendCA, err := os.ReadFile(b.CAFile)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(backendCA), "%s holds no certificate", b.CAFile)
	clientCert, err := tls.LoadX509KeyPair(b.ClientCertFile, b.ClientKeyFile)
	require.NoError(t, err)

	link := &Link{target: target, sent: map[string]Command{}, transport: &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      roots,
		Certificates: []tls.Certificate{clientCert},
	}}}
	proxy := &httputil.ReverseProxy{
		Rewrite:        func(r *httputil.ProxyRequest) { r.SetURL(target) },
		Transport:      link.transport,
		ModifyResponse: link.intercept,
		ErrorHandler: func(_ http.ResponseWriter, _ *http.Request, _ error) {
			// A lost response, or a backend that cannot be reached: either
			// way the caller's connection ends with no answer on it.
			panic(http.ErrAbortHandler)
		},
	}

	ca := newCertAuthority(t)
	certDER, keyDER := ca.sign(t, "docker-backend", true)
	key, err := x509.ParseECPrivateKey(keyDER)
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !link.record(r) || link.drops(r) {
			panic(http.ErrAbortHandler)
		}
		proxy.ServeHTTP(w, r)
	}))
	server.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: key}},
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	link.URL, link.CAFile = server.URL, ca.certFile
	return link
}

// LoseResponse makes the link lose the backend's next response to an
// operation, named by the last element of its path ("provision", "restart").
// The link holds that response until settled reports that the backend has
// finished the work, then ends the caller's connection without answering.
// settled runs off the test's goroutine and must not fail the test.
func (l *Link) LoseResponse(operation string, settled func() bool) *LostResponse {
	lost := &LostResponse{operation: operation, settled: settled, done: make(chan struct{})}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lose = lost
	return lost
}

// LoseRequest makes the link lose the caller's next request for an operation,
// named as in LoseResponse. The backend never receives it, and the caller's
// connection ends without an answer.
func (l *Link) LoseRequest(operation string) *LostRequest {
	lost := &LostRequest{operation: operation, done: make(chan struct{})}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.loseRequest = lost
	return lost
}

// drops reports whether the request is the one the link was asked to lose.
func (l *Link) drops(r *http.Request) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	lost := l.loseRequest
	if lost == nil || r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/"+lost.operation) {
		return false
	}
	l.loseRequest = nil
	close(lost.done)
	return true
}

// Require waits until the request was lost.
func (r *LostRequest) Require(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(lostResponseBudget):
		t.Fatalf("no %s was sent for the link to lose", r.operation)
	}
}

// intercept discards the response the link was asked to lose, and lets every
// other one through.
func (l *Link) intercept(response *http.Response) error {
	l.mu.Lock()
	lost := l.lose
	if lost == nil || !strings.HasSuffix(response.Request.URL.Path, "/"+lost.operation) {
		l.mu.Unlock()
		return nil
	}
	l.lose = nil
	l.mu.Unlock()

	deadline := time.Now().Add(lostResponseBudget)
	for !lost.reached && time.Now().Before(deadline) {
		if lost.reached = lost.settled(); !lost.reached {
			time.Sleep(500 * time.Millisecond)
		}
	}
	close(lost.done)
	return errLost
}

// Require waits until the response was lost, and fails the test unless the
// backend had finished the work by then.
func (r *LostResponse) Require(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(lostResponseBudget + time.Minute):
		t.Fatalf("the backend never answered a %s for the link to lose", r.operation)
	}
	require.True(t, r.reached, "the %s had not settled on the backend when its response was lost", r.operation)
}

// record keeps a copy of a command on its way to the backend, under the last
// element of its path. It reports false when the command cannot be read.
func (l *Link) record(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return true
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	operation := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sent[operation] = Command{link: l, method: r.Method, uri: r.URL.RequestURI(), header: r.Header.Clone(), body: body}
	return true
}

// Sent returns the last command of an operation that went through the link,
// named as in LoseResponse.
func (l *Link) Sent(t *testing.T, operation string) Command {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	command, sent := l.sent[operation]
	require.True(t, sent, "no %s went through the link", operation)
	return command
}

// Resend delivers the command to the backend once more, byte for byte, as a
// network that duplicates or delays a request would. It returns the status
// the backend answers with.
func (c Command) Resend(t *testing.T) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, c.method, c.link.target.String()+c.uri, bytes.NewReader(c.body))
	require.NoError(t, err)
	request.Header = c.header.Clone()
	response, err := c.link.transport.RoundTrip(request)
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}
