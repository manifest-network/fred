package main

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
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
	stopped chan context.Context
	stopErr error
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

func (b blockedBackendStartup) StopContext(ctx context.Context) error {
	if b.stopped != nil {
		b.stopped <- ctx
	}
	return b.stopErr
}

func TestStartupPublishesListenerOnlyWhenBackendIsReady(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		name := "http"
		if encrypted {
			name = "https"
		}
		t.Run(name, func(t *testing.T) {
			for _, startupErr := range []error{nil, errors.New("storage recovery failed")} {
				name := "successful startup"
				if startupErr != nil {
					name = "failed startup"
				}
				t.Run(name, func(t *testing.T) {
					probe, err := net.Listen("tcp", "127.0.0.1:0")
					require.NoError(t, err)
					address := probe.Addr().String()
					require.NoError(t, probe.Close())
					server := &http.Server{Addr: address, ReadHeaderTimeout: time.Second}
					client := &http.Client{Timeout: 2 * time.Second}
					scheme := "http"
					if encrypted {
						certificateServer := httptest.NewTLSServer(http.NotFoundHandler())
						server.TLSConfig = certificateServer.TLS.Clone()
						client.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: certificateServer.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs, MinVersion: tls.VersionTLS12}}
						certificateServer.Close()
						scheme = "https"
					}
					t.Cleanup(client.CloseIdleConnections)
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
						_, err := startAndServeBackend(t.Context(), server, runtime, owner)
						started <- err
					}()
					<-owner.entered
					connection, err := net.DialTimeout("tcp", address, time.Second)
					if connection != nil {
						_ = connection.Close()
					}
					require.Error(t, err, "TCP readiness must remain false while recovery is blocked")
					require.Zero(t, runtimeCalls.Load())
					finishStartup()
					err = <-started
					if startupErr != nil {
						require.ErrorIs(t, err, startupErr)
						connection, err = net.DialTimeout("tcp", address, time.Second)
						if connection != nil {
							_ = connection.Close()
						}
						require.Error(t, err, "failed startup must not publish a listener")
						return
					}
					require.NoError(t, err)
					response, err := client.Get(scheme + "://" + address + "/health")
					require.NoError(t, err)
					require.NoError(t, response.Body.Close())
					require.Equal(t, http.StatusNoContent, response.StatusCode)
					require.Equal(t, "runtime-identity", response.Header.Get(backendidentity.ResponseHeader))
					require.EqualValues(t, 1, runtimeCalls.Load())
				})
			}
		})
	}
}

func TestStartupListenerConflictFailsBeforeBackendStart(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	owner := blockedBackendStartup{entered: make(chan struct{}), release: make(chan struct{}), stopped: make(chan context.Context, 1)}
	server := &http.Server{Addr: listener.Addr().String(), ReadHeaderTimeout: time.Second}
	_, err = startAndServeBackend(t.Context(), server, http.NotFoundHandler(), owner)
	require.ErrorContains(t, err, "probe HTTP listener")
	select {
	case <-owner.entered:
		t.Fatal("existing listener must be detected before storage changes")
	default:
	}
	require.Empty(t, owner.stopped, "failed probe owns no started backend")
}

func TestStartupReportsListenerClaimedDuringRecovery(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := probe.Addr().String()
	require.NoError(t, probe.Close())
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	t.Cleanup(finish)
	stopErr := errors.New("backend drain failed")
	owner := blockedBackendStartup{entered: make(chan struct{}), release: release, stopped: make(chan context.Context, 1), stopErr: stopErr}
	server := &http.Server{Addr: address, ReadHeaderTimeout: time.Second}
	started := make(chan error, 1)
	go func() {
		_, err := startAndServeBackend(t.Context(), server, http.NotFoundHandler(), owner)
		started <- err
	}()
	<-owner.entered
	competitor, err := net.Listen("tcp", address)
	require.NoError(t, err, "the startup probe must relinquish its listener")
	t.Cleanup(func() { _ = competitor.Close() })
	finish()
	err = <-started
	require.ErrorContains(t, err, "bind ready HTTP listener")
	require.ErrorIs(t, err, stopErr)
	shutdown := <-owner.stopped
	deadline, bounded := shutdown.Deadline()
	require.True(t, bounded, "failed final bind must drain under the process budget")
	require.WithinDuration(t, time.Now().Add(processShutdownTimeout), deadline, time.Second)
}
