package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func TestInventoryRecoveryBypassesOpenTenantBreakerWithoutResettingIt(t *testing.T) {
	var inventoryCalls atomic.Int32
	var tenantCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, testBackendStorageIDA)
		switch r.URL.Path {
		case "/provisions", "/retentions":
			inventoryCalls.Add(1)
			require.NotEmpty(t, r.Header.Get(hmacauth.SignatureHeader))
			kind := strings.TrimPrefix(r.URL.Path, "/")
			if r.URL.Query().Get("continue") == "" {
				_, _ = fmt.Fprintf(w, `{"%s":[{"lease_uuid":%q}],"continue":"next"}`, kind, testBackendStorageIDA)
			} else {
				_, _ = fmt.Fprintf(w, `{"%s":[{"lease_uuid":%q}]}`, kind, testBackendStorageIDB)
			}
		default:
			tenantCalls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	identity := mustBackendStorageID(t, testBackendStorageIDA)
	client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
		Name: "recovered", BaseURL: server.URL, Secret: testIdentityClientKey,
		CBFailureThresh: 1, CBTimeout: time.Hour,
	}, &testStorageIdentityResolver{id: identity, bound: true})
	require.NoError(t, err)
	_, err = client.GetInfo(t.Context(), testBackendStorageIDA)
	require.Error(t, err)
	require.Equal(t, gobreaker.StateOpen, client.cb.State())

	provisions, observed, err := client.ListProvisionsWithIdentity(t.Context())
	require.NoError(t, err)
	require.Len(t, provisions, 2)
	require.Equal(t, identity, observed)
	retentions, observed, err := client.ListRetentionsWithIdentity(t.Context())
	require.NoError(t, err)
	require.Len(t, retentions, 2)
	require.Equal(t, identity, observed)
	require.EqualValues(t, 4, inventoryCalls.Load())
	require.Equal(t, gobreaker.StateOpen, client.cb.State(), "inventory cannot reset tenant transport health")
	_, err = client.LookupProvisions(t.Context(), []string{testBackendStorageIDA})
	require.ErrorIs(t, err, ErrCircuitOpen)
	require.EqualValues(t, 1, tenantCalls.Load(), "tenant lookup retains circuit admission")
}

func TestInventoryRecoveryFailurePreservesIdentityAndDoesNotTripTenantBreaker(t *testing.T) {
	client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
		Name: "recovered", BaseURL: "https://backend.example", Secret: testIdentityClientKey,
		CBFailureThresh: 1,
	}, &testStorageIdentityResolver{id: mustBackendStorageID(t, testBackendStorageIDA), bound: true})
	require.NoError(t, err)
	client.httpClient.Transport = causalOutcomeRoundTripper(func(r *http.Request) (*http.Response, error) {
		identity := testBackendStorageIDA
		if r.URL.Query().Get("continue") != "" {
			identity = testBackendStorageIDB
		}
		header := make(http.Header)
		header.Set(backendidentity.ResponseHeader, identity)
		return &http.Response{StatusCode: http.StatusOK, Header: header,
			Body: io.NopCloser(strings.NewReader(`{"provisions":[],"continue":"next"}`))}, nil
	})
	provisions, identity, err := client.ListProvisionsWithIdentity(t.Context())
	require.Error(t, err)
	require.Nil(t, provisions)
	require.Equal(t, backendidentity.ID{}, identity)
	require.Equal(t, gobreaker.StateClosed, client.cb.State())
	require.Zero(t, client.cb.Counts().Requests, "inventory failures have their own observation path")
}

func TestInventoryRecoverySerializesWholeWalkAndIncludesQueueInBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newUnboundHTTPClientForTest(HTTPClientConfig{
			Name: "serial-inventory", BaseURL: "http://backend.example", Timeout: 100 * time.Millisecond,
		})
		entered := make(chan struct{})
		var active, maximum atomic.Int32
		client.httpClient.Transport = causalOutcomeRoundTripper(func(r *http.Request) (*http.Response, error) {
			current := active.Add(1)
			if current > maximum.Load() {
				maximum.Store(current)
			}
			defer active.Add(-1)
			kind := strings.TrimPrefix(r.URL.Path, "/")
			delay := 60 * time.Millisecond
			if kind == "provisions" {
				close(entered)
				delay = 80 * time.Millisecond
			}
			select {
			case <-r.Context().Done():
				return nil, r.Context().Err()
			case <-time.After(delay):
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
					Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"%s":[]}`, kind)))}, nil
			}
		})
		first := make(chan error, 1)
		go func() { _, err := client.ListProvisions(t.Context()); first <- err }()
		<-entered
		started := time.Now()
		_, err := client.ListRetentions(t.Context())
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 100*time.Millisecond, time.Since(started), "queueing cannot reset the complete-walk deadline")
		require.NoError(t, <-first)
		require.EqualValues(t, 1, maximum.Load(), "one whole walk owns the recovery lane")
		require.Empty(t, client.inventorySlot, "failed walk releases admission")

		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = client.ListRetentions(canceled)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, client.inventorySlot)
		_, err = client.ListRetentions(t.Context())
		require.NoError(t, err, "a later walk can recover after timeout/cancellation")
	})
}
