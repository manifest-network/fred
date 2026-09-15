package backend

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestReadCapacityDoesNotOpenSharedCircuit(t *testing.T) {
	var infoCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, testBackendStorageIDA)
		if r.URL.Path == "/logs/lease" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"log response capacity exhausted","code":"insufficient_resources"}`))
			return
		}
		infoCalls.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
		Name: "read-capacity", BaseURL: server.URL, Secret: testIdentityClientKey,
		Timeout: 10 * time.Second, CBFailureThresh: 1,
	}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
	require.NoError(t, err)
	for range 3 {
		_, err := client.GetLogs(t.Context(), "lease", 100)
		require.Error(t, err)
		require.True(t, IsReadCapacity(fmt.Errorf("read logs: %w", err)))
		require.NotErrorIs(t, err, ErrCircuitOpen)
		require.NotErrorIs(t, err, ErrCapacityRefused, "read pressure supplies no durable mutation refusal proof")
		require.NotErrorIs(t, err, ErrInsufficientResources)
	}
	_, err = client.GetInfo(t.Context(), "lease")
	require.NoError(t, err, "a busy log slot must not fence other backend operations")
	require.Equal(t, int32(1), infoCalls.Load())
}

func TestReadCapacityTimeoutAndPanicRemainHealthFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler func(http.ResponseWriter, *http.Request, <-chan struct{})
	}{
		{name: "timeout", handler: func(_ http.ResponseWriter, r *http.Request, finish <-chan struct{}) {
			<-r.Context().Done()
			<-finish
		}},
		{name: "panic", handler: func(http.ResponseWriter, *http.Request, <-chan struct{}) { panic("backend worker failed") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finish := make(chan struct{})
			defer close(finish)
			h := NewLogsHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tc.handler(w, r, finish)
			}), 20*time.Millisecond)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(backendidentity.ResponseHeader, testBackendStorageIDA)
				h.ServeHTTP(w, r)
			}))
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			server.Start()
			defer server.Close()
			client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
				Name: "read-capacity-worker-failure", BaseURL: server.URL, Secret: testIdentityClientKey,
				Timeout: 10 * time.Second, CBFailureThresh: 1,
			}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
			require.NoError(t, err)
			_, err = client.GetLogs(t.Context(), "lease", 100)
			require.Error(t, err)
			require.False(t, IsReadCapacity(err))
			_, err = client.GetInfo(t.Context(), "lease")
			require.ErrorIs(t, err, ErrCircuitOpen)
		})
	}
}

func TestReadCapacityRequiresExactCompleteEnvelopeAndIdentity(t *testing.T) {
	const capacity = `{"error":"busy","code":"insufficient_resources"}`
	for _, tc := range []struct {
		name     string
		body     string
		identity string
		status   int
	}{
		{name: "bare 503", status: 503},
		{name: "timeout", body: `{"error":"request timeout"}`, status: 503},
		{name: "numeric tenant code", body: `{"error":"busy","code":503}`, status: 503},
		{name: "unknown code", body: `{"error":"busy","code":"future_code"}`, status: 503},
		{name: "missing error", body: `{"code":"insufficient_resources"}`, status: 503},
		{name: "HTML", body: `<h1>busy</h1>`, status: 503},
		{name: "duplicate code", body: `{"error":"busy","code":"insufficient_resources","code":"insufficient_resources"}`, status: 503},
		{name: "case alias", body: `{"error":"busy","Code":"insufficient_resources"}`, status: 503},
		{name: "unknown field", body: `{"error":"busy","code":"insufficient_resources","extra":true}`, status: 503},
		{name: "trailing object", body: capacity + `{}`, status: 503},
		{name: "bounded prefix hides suffix", body: capacity + strings.Repeat(" ", maxBackendErrorBytes-len(capacity)) + `{}`, status: 503},
		{name: "overflow whitespace", body: capacity + strings.Repeat(" ", maxBackendErrorBytes-len(capacity)+1), status: 503},
		{name: "missing identity", body: capacity, identity: "absent", status: 503},
		{name: "foreign identity", body: capacity, identity: testBackendStorageIDB, status: 503},
		{name: "wrong status", body: capacity, status: 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var infoCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				identity := tc.identity
				if identity == "" {
					identity = testBackendStorageIDA
				}
				if identity != "absent" {
					w.Header().Set(backendidentity.ResponseHeader, identity)
				}
				if r.URL.Path == "/logs/lease" {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
					return
				}
				infoCalls.Add(1)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer server.Close()
			client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
				Name: "read-capacity-negative", BaseURL: server.URL, Secret: testIdentityClientKey,
				Timeout: 10 * time.Second, CBFailureThresh: 1,
			}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
			require.NoError(t, err)
			_, err = client.GetLogs(t.Context(), "lease", 100)
			require.Error(t, err)
			require.False(t, IsReadCapacity(err))
			require.NotErrorIs(t, err, ErrCapacityRefused)
			_, err = client.GetInfo(t.Context(), "lease")
			require.ErrorIs(t, err, ErrCircuitOpen, "an unproven capacity response remains a backend failure")
			require.Zero(t, infoCalls.Load())
		})
	}
	require.False(t, IsReadCapacity(nil))
	require.False(t, IsReadCapacity(errors.New("backend read response capacity exhausted: busy")))
	require.False(t, IsReadCapacity(ErrInsufficientResources))
	require.False(t, IsReadCapacity(ErrCapacityRefused))
}

func TestReadCapacityAtWireLimitIsSharedByReadEndpoints(t *testing.T) {
	const capacity = `{"error":"busy","code":"insufficient_resources"}`
	body := capacity + strings.Repeat(" ", maxBackendErrorBytes-len(capacity))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, testBackendStorageIDA)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
		Name: "read-capacity-limit", BaseURL: server.URL, Secret: testIdentityClientKey,
		Timeout: 10 * time.Second, CBFailureThresh: 1,
	}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
	require.NoError(t, err)
	_, err = client.GetLogs(t.Context(), "lease", 100)
	require.True(t, IsReadCapacity(err), "%v", err)
	_, err = client.GetInfo(t.Context(), "lease")
	require.True(t, IsReadCapacity(err), "%v", err)
	_, err = client.GetProvision(t.Context(), "lease")
	require.True(t, IsReadCapacity(err), "%v", err)
}
