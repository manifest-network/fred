package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Exercise large, structurally simple bodies through the real listener. Token
// bomb tests stop early and do not cover concurrent full-body verification.
// This is a bounded progress regression, not a deployment capacity benchmark.
func TestCallbackAdmissionProgressUnderConcurrentUnauthenticatedBodies(t *testing.T) {
	server := newCallbackAdmissionServer(t)
	listener := httptest.NewServer(server.server.Handler)
	defer listener.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	const workers, rounds = 8, 4
	body := []byte(`{"lease_uuid":"d144291f-a36f-47a4-8ccf-48afe590e29d","status":"failed","backend_storage_id":"` + callbackKeyringStorageA + `","padding":"` + strings.Repeat("x", 960<<10) + `"}`)
	signed := signedKeyringCallbackRequest(body, callbackKeyringSecretB)
	call := func(payload []byte, header http.Header) (int, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, listener.URL+"/callbacks/provision", bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		request.Header = header.Clone()
		request.Header.Set("X-Forwarded-For", callbackAdmissionClientIP)
		response, err := listener.Client().Do(request)
		if err != nil {
			return 0, err
		}
		defer func() { _ = response.Body.Close() }()
		_, err = io.Copy(io.Discard, response.Body)
		return response.StatusCode, err
	}
	status, err := call(body, signed.Header)
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, status, "consume the independent ingress token")
	start := make(chan struct{})
	first := make(chan struct{}, workers)
	results := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Go(func() {
			for round := range rounds {
				if round == 1 {
					first <- struct{}{}
					<-start
				}
				status, err := call(body, signed.Header)
				if err != nil || status != http.StatusTooManyRequests {
					// Always rendezvous so an assertion failure cannot strand
					// the test's other workers behind its start barrier.
					if round == 0 {
						first <- struct{}{}
						<-start
					}
					results <- fmt.Errorf("unauthenticated callback status=%d: %w", status, err)
					return
				}
			}
			results <- nil
		})
	}
	for range workers {
		<-first
	}
	close(start)
	valid := callbackAdmissionRequest(callbackKeyringStorageA, callbackKeyringSecretA, 1)
	validBody, err := io.ReadAll(valid.Body)
	require.NoError(t, err)
	status, err = call(validBody, valid.Header)
	group.Wait()
	close(results)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status, "forged same-NAT traffic must not consume the backend's verified budget")
	for result := range results {
		require.NoError(t, result)
	}
	require.NoError(t, ctx.Err(), "both authenticated progress and bounded invalid work must finish")
}
