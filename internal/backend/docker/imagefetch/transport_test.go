package imagefetch

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type progressBody struct {
	ctx   context.Context
	bytes chan byte
}

func (b *progressBody) Read(out []byte) (int, error) {
	select {
	case value := <-b.bytes:
		out[0] = value
		return 1, nil
	case <-b.ctx.Done():
		return 0, context.Cause(b.ctx)
	}
}
func (*progressBody) Close() error { return nil }

func TestRegistryIdleTimeoutBoundsStalledHeadersAndBody(t *testing.T) {
	for _, headers := range []bool{false, true} {
		t.Run(map[bool]string{false: "body", true: "headers"}[headers], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := make(chan context.Context, 1)
				base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
					started <- req.Context()
					if headers {
						<-req.Context().Done()
						return nil, context.Cause(req.Context())
					}
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: &progressBody{ctx: req.Context(), bytes: make(chan byte)}}, nil
				})
				transport := boundedTransport{base: base, limit: 1 << 20}
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://registry.example/v2/blob", nil)
				require.NoError(t, err)
				finished := make(chan error, 1)
				go func() {
					response, err := transport.RoundTrip(request)
					if err == nil {
						_, err = io.ReadAll(response.Body)
						_ = response.Body.Close()
					}
					finished <- err
				}()
				work := <-started
				time.Sleep(29 * time.Second)
				require.NoError(t, work.Err())
				time.Sleep(time.Second)
				synctest.Wait()
				require.ErrorContains(t, <-finished, "no-progress timeout")
			})
		})
	}
}

func TestRegistryIdleTimeoutRenewsOnBytesAndCloseReleasesOwnership(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var work context.Context
		var body *progressBody
		base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			work = req.Context()
			body = &progressBody{ctx: work, bytes: make(chan byte, 1)}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
		})
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://registry.example/v2/blob", nil)
		require.NoError(t, err)
		response, err := (boundedTransport{base: base, limit: 1 << 20}).RoundTrip(request)
		require.NoError(t, err)
		for range 5 {
			time.Sleep(29 * time.Second)
			body.bytes <- 'x'
			var byte [1]byte
			_, err := io.ReadFull(response.Body, byte[:])
			require.NoError(t, err)
			require.NoError(t, work.Err(), "progressing transfers may exceed one idle interval")
		}
		require.NoError(t, response.Body.Close())
		require.ErrorIs(t, context.Cause(work), context.Canceled)
		time.Sleep(time.Minute)
		require.ErrorIs(t, context.Cause(work), context.Canceled, "closed transfer cannot later acquire a timeout result")
	})
}

func TestRegistryBlockedHTTPBodyHonorsCallerCancellation(t *testing.T) {
	handlerFinished := make(chan struct{})
	releaseHandler := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseHandler) }) }
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		// Returning would complete a valid empty chunked body and let EOF race
		// cancellation. Keep the response unfinished until the client observes it.
		<-releaseHandler
		close(handlerFinished)
	}))
	defer server.Close()
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v2/blob", nil)
	require.NoError(t, err)
	response, err := (boundedTransport{base: server.Client().Transport, limit: 1 << 20}).RoundTrip(request)
	require.NoError(t, err)
	defer response.Body.Close()
	readStarted := make(chan struct{})
	response.Body = &readStartedBody{ReadCloser: response.Body, started: readStarted}
	finished := make(chan error, 1)
	go func() { _, err := io.ReadAll(response.Body); finished <- err }()
	<-readStarted
	cancel()
	require.ErrorIs(t, <-finished, context.Canceled)
	release()
	<-handlerFinished
}

type readStartedBody struct {
	io.ReadCloser
	started chan struct{}
	once    sync.Once
}

func (b *readStartedBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	return b.ReadCloser.Read(p)
}
