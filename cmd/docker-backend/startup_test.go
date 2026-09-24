package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

type blockedBackendStartup struct {
	entered chan struct{}
	release <-chan struct{}
	err     error
}

func (b blockedBackendStartup) Start(ctx context.Context) error {
	close(b.entered)
	select {
	case <-b.release:
		return b.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestStartupServesUnavailableUntilBackendIsReady(t *testing.T) {
	for _, startupErr := range []error{nil, errors.New("storage recovery failed")} {
		name := "successful startup"
		if startupErr != nil {
			name = "failed startup"
		}
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = listener.Close() })
			server := &http.Server{ReadHeaderTimeout: time.Second}
			t.Cleanup(func() { _ = server.Close() })
			var runtimeCalls atomic.Int64
			runtime := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				runtimeCalls.Add(1)
				w.Header().Set(backendidentity.ResponseHeader, "runtime-identity")
				w.WriteHeader(http.StatusNoContent)
			})
			release := make(chan struct{})
			finishStartup := sync.OnceFunc(func() { close(release) })
			t.Cleanup(finishStartup)
			owner := blockedBackendStartup{entered: make(chan struct{}), release: release, err: startupErr}
			started := make(chan error, 1)
			go func() {
				_, err := serveStartingBackend(t.Context(), listener, server, runtime, owner)
				started <- err
			}()
			<-owner.entered
			client := &http.Client{Timeout: 2 * time.Second}
			t.Cleanup(client.CloseIdleConnections)
			url := "http://" + listener.Addr().String() + "/health"
			response, err := client.Get(url)
			require.NoError(t, err, "a bound listener must answer while storage startup is blocked")
			body, err := io.ReadAll(response.Body)
			require.NoError(t, response.Body.Close())
			require.NoError(t, err)
			require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
			require.Equal(t, "1", response.Header.Get("Retry-After"))
			require.Empty(t, response.Header.Get(backendidentity.ResponseHeader), "startup cannot attest a running backend")
			require.Contains(t, string(body), "backend startup in progress")
			require.Zero(t, runtimeCalls.Load())
			finishStartup()
			err = <-started
			if startupErr != nil {
				require.ErrorIs(t, err, startupErr)
				response, err = client.Get(url)
				if response != nil {
					_ = response.Body.Close()
				}
				require.Error(t, err, "failed startup must close the bound listener")
				require.Zero(t, runtimeCalls.Load())
				return
			}
			require.NoError(t, err)
			response, err = client.Get(url)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, http.StatusNoContent, response.StatusCode)
			require.Equal(t, "runtime-identity", response.Header.Get(backendidentity.ResponseHeader))
			require.EqualValues(t, 1, runtimeCalls.Load())
		})
	}
}
