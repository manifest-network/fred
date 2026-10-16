package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func signedRuntimeRefreshRequest(method, uri string, body []byte) *http.Request {
	req := httptest.NewRequest(method, uri, bytes.NewReader(body))
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(testSecret, req.Method, req.URL.RequestURI(), body))
	return req
}

func runtimeRefreshTestStatus() docker.RuntimeRefreshStatus {
	return docker.RuntimeRefreshStatus{
		Contract: 1, Active: true, Stale: 3, InFlight: 1, Refreshed: 1, Failed: 1, Skipped: 0,
		StaleLeases:         []string{},
		SharedNetworkLeases: []string{},
		Leases: []docker.RuntimeRefreshLease{
			{LeaseUUID: handlerTestLeaseUUID, MaintenanceID: handlerTestMaintenanceID, Outcome: docker.RuntimeRefreshRefreshed},
			{LeaseUUID: "650e8400-e29b-41d4-a716-446655440000", MaintenanceID: handlerTestMaintenanceID, Outcome: docker.RuntimeRefreshFailed},
		},
	}
}

func TestStartRuntimeRefreshStatusMapping(t *testing.T) {
	for _, tt := range []struct {
		name     string
		err      error
		wantCode int
	}{
		{name: "started", wantCode: http.StatusAccepted},
		{name: "already active", err: docker.ErrRuntimeRefreshActive, wantCode: http.StatusConflict},
		{name: "disabled", err: docker.ErrRuntimeRefreshDisabled, wantCode: http.StatusForbidden},
		{name: "inventory unavailable", err: errors.New("docker unavailable"), wantCode: http.StatusServiceUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			mock := &mockBackend{StartRuntimeRefreshFunc: func(context.Context) (docker.RuntimeRefreshStatus, error) {
				calls++
				if tt.err != nil {
					return docker.RuntimeRefreshStatus{}, tt.err
				}
				return runtimeRefreshTestStatus(), nil
			}}
			handler := NewServer(mock, testRequestKeys, slog.Default(), docker.DefaultMaxRequestBodySize).Handler()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, signedRuntimeRefreshRequest(http.MethodPost, "/runtime_refresh", nil))
			assert.Equal(t, tt.wantCode, response.Code, response.Body.String())
			assert.Equal(t, 1, calls)
			if tt.err == nil {
				var got docker.RuntimeRefreshStatus
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &got))
				assert.Equal(t, runtimeRefreshTestStatus(), got)
			}
		})
	}
}

func TestStartRuntimeRefreshTakesNoParameters(t *testing.T) {
	calls := 0
	mock := &mockBackend{StartRuntimeRefreshFunc: func(context.Context) (docker.RuntimeRefreshStatus, error) {
		calls++
		return runtimeRefreshTestStatus(), nil
	}}
	handler := NewServer(mock, testRequestKeys, slog.Default(), docker.DefaultMaxRequestBodySize).Handler()
	for _, body := range []string{`{}`, " {} \n"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, signedRuntimeRefreshRequest(http.MethodPost, "/runtime_refresh", []byte(body)))
		assert.Equal(t, http.StatusAccepted, response.Code, body)
	}
	for _, body := range []string{`{"max_concurrency":8}`, `[]`, `{} {}`, `not json`} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, signedRuntimeRefreshRequest(http.MethodPost, "/runtime_refresh", []byte(body)))
		assert.Equal(t, http.StatusBadRequest, response.Code, body)
	}
	assert.Equal(t, 2, calls, "a refused body never starts a run")
}

func TestRuntimeRefreshRoutesRequireAuthentication(t *testing.T) {
	mock := &mockBackend{}
	handler := NewServer(mock, testRequestKeys, slog.Default(), docker.DefaultMaxRequestBodySize).Handler()
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, "/runtime_refresh", nil))
		assert.Equal(t, http.StatusUnauthorized, response.Code, method)
	}
}

func TestGetRuntimeRefreshStatus(t *testing.T) {
	mock := &mockBackend{RuntimeRefreshStatusFunc: func(context.Context) (docker.RuntimeRefreshStatus, error) {
		return runtimeRefreshTestStatus(), nil
	}}
	handler := NewServer(mock, testRequestKeys, slog.Default(), docker.DefaultMaxRequestBodySize).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, signedRuntimeRefreshRequest(http.MethodGet, "/runtime_refresh", nil))
	require.Equal(t, http.StatusOK, response.Code)

	// Pin every field name and JSON type the cutover reads.
	var fields map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &fields))
	assert.Len(t, fields, 11)
	for _, name := range []string{"contract", "stale", "in_flight", "refreshed", "failed", "restored", "skipped"} {
		assert.IsType(t, float64(0), fields[name], name)
	}
	assert.IsType(t, true, fields["active"])
	assert.Equal(t, []any{}, fields["shared_network_leases"], "an empty list, never null")
	assert.Equal(t, []any{}, fields["stale_leases"], "an empty list, never null")
	leases, ok := fields["leases"].([]any)
	require.True(t, ok)
	require.Len(t, leases, 2)
	row, ok := leases[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{
		"lease_uuid": handlerTestLeaseUUID, "maintenance_id": handlerTestMaintenanceID, "outcome": "refreshed",
	}, row)

	failing := &mockBackend{RuntimeRefreshStatusFunc: func(context.Context) (docker.RuntimeRefreshStatus, error) {
		return docker.RuntimeRefreshStatus{}, errors.New("docker unavailable")
	}}
	response = httptest.NewRecorder()
	NewServer(failing, testRequestKeys, slog.Default(), docker.DefaultMaxRequestBodySize).Handler().
		ServeHTTP(response, signedRuntimeRefreshRequest(http.MethodGet, "/runtime_refresh", nil))
	assert.Equal(t, http.StatusServiceUnavailable, response.Code, "an unreadable inventory is never reported as zero stale leases")
}

// The routes are protected like /restart: HMAC plus the backend's runtime
// identity verification, every response carries the storage identity, and a
// request naming another storage identity is refused before the handler runs.
func TestRuntimeRefreshRoutesBindStorageIdentity(t *testing.T) {
	storageID, err := backendidentity.Parse("a8ff9194-0f55-4a31-854e-5f63b236ef3b")
	require.NoError(t, err)
	other := "b8ff9194-0f55-4a31-854e-5f63b236ef3b"
	starts := 0
	mock := &mockBackend{
		VerifyStorageIdentityFunc: func(context.Context) error { return nil },
		RuntimeRefreshStatusFunc: func(context.Context) (docker.RuntimeRefreshStatus, error) {
			return runtimeRefreshTestStatus(), nil
		},
		StartRuntimeRefreshFunc: func(context.Context) (docker.RuntimeRefreshStatus, error) {
			starts++
			return runtimeRefreshTestStatus(), nil
		},
	}
	server, err := NewIdentityBoundServer(mock, testRequestKeys, slog.Default(), docker.DefaultMaxRequestBodySize, storageID)
	require.NoError(t, err)
	handler := server.Handler()

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, signedRuntimeRefreshRequest(method,
			"/runtime_refresh?"+backendidentity.QueryParameter+"="+storageID.String(), nil))
		assert.Less(t, response.Code, 300, method)
		assert.Equal(t, storageID.String(), response.Header().Get(backendidentity.ResponseHeader), method)

		response = httptest.NewRecorder()
		handler.ServeHTTP(response, signedRuntimeRefreshRequest(method,
			"/runtime_refresh?"+backendidentity.QueryParameter+"="+other, nil))
		assert.Equal(t, http.StatusConflict, response.Code, "%s naming another storage identity is refused", method)
	}
	assert.Equal(t, 1, starts, "a request for another storage identity never starts a run")

	// The storage-bound mutation route requires the identity in its path.
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, signedRuntimeRefreshRequest(http.MethodPost,
		backendidentity.BoundPathPrefix+storageID.String()+"/runtime_refresh", nil))
	assert.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, signedRuntimeRefreshRequest(http.MethodPost,
		backendidentity.BoundPathPrefix+other+"/runtime_refresh", nil))
	assert.Equal(t, http.StatusNotFound, response.Code)
	assert.Equal(t, 2, starts)

	// Without runtime identity attestation nothing is served.
	unverified := &mockBackend{VerifyStorageIdentityFunc: func(context.Context) error { return errors.New("drift") }}
	server, err = NewIdentityBoundServer(unverified, testRequestKeys, slog.Default(), docker.DefaultMaxRequestBodySize, storageID)
	require.NoError(t, err)
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, signedRuntimeRefreshRequest(http.MethodPost, "/runtime_refresh", nil))
	assert.Equal(t, http.StatusServiceUnavailable, response.Code)
}
