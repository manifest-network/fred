package backend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/maintenanceid"
)

type causalOutcomeRoundTripper func(*http.Request) (*http.Response, error)

type decoratedHTTPBackend struct{ *HTTPClient }

func (roundTrip causalOutcomeRoundTripper) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	return roundTrip(request)
}

func TestCausalCallOutcomesAreZeroInvalidAndConservativeAdaptersCannotRefuse(t *testing.T) {
	t.Parallel()

	assert.False(t, (ProvisionCallOutcome{}).Valid())
	assert.False(t, (RestoreCallOutcome{}).Valid())
	assert.False(t, (MaintenanceCallOutcome{}).Valid())
	assert.False(t, (ProvisionCallOutcome{
		outcome: causalCallOutcome{disposition: callDispositionAccepted},
		refusal: ProvisionRefusalValidation,
	}).Accepted(), "a refusal category cannot be attached to acceptance")
	assert.False(t, (RestoreCallOutcome{
		outcome: causalCallOutcome{disposition: callDispositionRefused, err: ErrNotRetained},
	}).Refused(), "refusal without its closed category is invalid")

	provisionSuccess := ConservativeProvisionCallOutcome(nil)
	assert.True(t, provisionSuccess.Valid())
	assert.True(t, provisionSuccess.Accepted())
	provisionFailure := ConservativeProvisionCallOutcome(errors.Join(
		ErrValidation, context.DeadlineExceeded,
	))
	assert.True(t, provisionFailure.Valid())
	assert.True(t, provisionFailure.Ambiguous())
	assert.False(t, provisionFailure.Refused())
	assert.False(t, provisionFailure.NotDispatched())

	restoreFailure := ConservativeRestoreCallOutcome(errors.Join(
		ErrNotRetained, context.DeadlineExceeded,
	))
	assert.True(t, restoreFailure.Ambiguous())
	assert.False(t, restoreFailure.Refused())

	maintenanceFailure := ConservativeMaintenanceCallOutcome(errors.Join(
		ErrInvalidState, context.DeadlineExceeded,
	))
	assert.True(t, maintenanceFailure.Ambiguous())
	assert.False(t, maintenanceFailure.Refused())
	assert.Equal(t, MaintenanceRefusalNone, maintenanceFailure.Refusal())
}

func causalOutcomeClientForTest(
	t *testing.T,
	status int,
	body string,
) *HTTPClient {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return newUnboundHTTPClientForTest(HTTPClientConfig{Name: "causal-test", BaseURL: server.URL})
}

func TestHTTPClientMintsProvisionCausalOutcomeAtResponseBoundary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		status    int
		body      string
		accepted  bool
		refusal   ProvisionRefusal
		ambiguous bool
	}{
		{name: "accepted", status: http.StatusAccepted, accepted: true},
		{name: "validation envelope", status: http.StatusBadRequest, body: `{"error":"bad input"}`, refusal: ProvisionRefusalValidation},
		{name: "malformed validation", status: http.StatusBadRequest, body: `{"message":"proxy"}`, ambiguous: true},
		{name: "coded capacity", status: http.StatusServiceUnavailable, body: `{"error":"full","code":"insufficient_resources"}`, refusal: ProvisionRefusalCapacity},
		{name: "bare capacity", status: http.StatusServiceUnavailable, ambiguous: true},
		{name: "conflict", status: http.StatusConflict, body: `{"error":"exists"}`, ambiguous: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			outcome := causalOutcomeClientForTest(t, test.status, test.body).
				provisionCall(t.Context(), ProvisionRequest{})
			require.True(t, outcome.Valid())
			assert.Equal(t, test.accepted, outcome.Accepted())
			assert.Equal(t, test.refusal != ProvisionRefusalNone, outcome.Refused())
			assert.Equal(t, test.refusal, outcome.Refusal())
			assert.Equal(t, test.ambiguous, outcome.Ambiguous())
			assert.False(t, outcome.NotDispatched())
		})
	}
}

func TestInvokeFunctionsDoNotDelegateCausalAuthorityThroughDecorator(t *testing.T) {
	t.Parallel()
	client := causalOutcomeClientForTest(
		t, http.StatusBadRequest, `{"error":"bad input"}`,
	)
	decorated := &decoratedHTTPBackend{HTTPClient: client}
	var nilHTTPClient *HTTPClient
	maintenanceID, err := maintenanceid.Parse("2b0fb1e9-b9ad-4f52-a93d-69e8eb72830a")
	require.NoError(t, err)
	type observation struct {
		valid, refused, ambiguous, notDispatched bool
		err                                      error
	}
	tests := []struct {
		name   string
		invoke func(Backend) observation
	}{
		{
			name: "provision",
			invoke: func(target Backend) observation {
				outcome := InvokeProvision(t.Context(), target, ProvisionRequest{})
				return observation{outcome.Valid(), outcome.Refused(), outcome.Ambiguous(), outcome.NotDispatched(), outcome.Err()}
			},
		},
		{
			name: "restore",
			invoke: func(target Backend) observation {
				outcome := InvokeRestore(t.Context(), target, RestoreRequest{})
				return observation{outcome.Valid(), outcome.Refused(), outcome.Ambiguous(), outcome.NotDispatched(), outcome.Err()}
			},
		},
		{
			name: "restart",
			invoke: func(target Backend) observation {
				outcome := InvokeRestart(t.Context(), target, RestartRequest{MaintenanceID: maintenanceID})
				return observation{outcome.Valid(), outcome.Refused(), outcome.Ambiguous(), outcome.NotDispatched(), outcome.Err()}
			},
		},
		{
			name: "update",
			invoke: func(target Backend) observation {
				outcome := InvokeUpdate(t.Context(), target, UpdateRequest{MaintenanceID: maintenanceID})
				return observation{outcome.Valid(), outcome.Refused(), outcome.Ambiguous(), outcome.NotDispatched(), outcome.Err()}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			direct := test.invoke(client)
			require.True(t, direct.valid)
			assert.True(t, direct.refused)
			assert.False(t, direct.ambiguous)

			wrapped := test.invoke(decorated)
			require.True(t, wrapped.valid)
			assert.True(t, wrapped.ambiguous)
			assert.False(t, wrapped.refused)
			assert.False(t, wrapped.notDispatched)
			assert.ErrorIs(t, wrapped.err, ErrValidation)

			typedNil := test.invoke(nilHTTPClient)
			require.True(t, typedNil.valid)
			assert.True(t, typedNil.ambiguous)
			assert.False(t, typedNil.refused)
			assert.False(t, typedNil.notDispatched)
			assert.Error(t, typedNil.err)
		})
	}
}

func TestHTTPClientMintsRestoreCausalOutcomeAtResponseBoundary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		status    int
		body      string
		accepted  bool
		refusal   RestoreRefusal
		ambiguous bool
	}{
		{name: "accepted", status: http.StatusAccepted, accepted: true},
		{name: "bare not retained", status: http.StatusUnprocessableEntity, refusal: RestoreRefusalNotRetained},
		{name: "known demote refusal", status: http.StatusUnprocessableEntity, body: `{"error":"too large","code":"demote_exceeds_tier"}`, refusal: RestoreRefusalDemoteDataExceedsTier},
		{name: "unknown refusal code", status: http.StatusUnprocessableEntity, body: `{"error":"new verdict","code":"future"}`, ambiguous: true},
		{name: "bare invalid state", status: http.StatusConflict, refusal: RestoreRefusalInvalidState},
		{name: "already provisioned", status: http.StatusConflict, body: `{"error":"exists","code":"already_provisioned"}`, ambiguous: true},
		{name: "validation envelope", status: http.StatusBadRequest, body: `{"error":"bad input"}`, refusal: RestoreRefusalValidation},
		{name: "malformed validation", status: http.StatusBadRequest, body: `{"message":"proxy"}`, ambiguous: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			outcome := causalOutcomeClientForTest(t, test.status, test.body).
				restoreCall(t.Context(), RestoreRequest{})
			require.True(t, outcome.Valid())
			assert.Equal(t, test.accepted, outcome.Accepted())
			assert.Equal(t, test.refusal != RestoreRefusalNone, outcome.Refused())
			assert.Equal(t, test.refusal, outcome.Refusal())
			assert.Equal(t, test.ambiguous, outcome.Ambiguous())
			assert.False(t, outcome.NotDispatched())
		})
	}
}

func TestHTTPClientMintsMaintenanceRefusalCategoryAtResponseBoundary(t *testing.T) {
	t.Parallel()
	id, err := maintenanceid.Parse("2b0fb1e9-b9ad-4f52-a93d-69e8eb72830a")
	require.NoError(t, err)
	tests := []struct {
		name      string
		status    int
		body      string
		accepted  bool
		refusal   MaintenanceRefusal
		ambiguous bool
	}{
		{name: "accepted", status: http.StatusAccepted, accepted: true},
		{name: "validation", status: http.StatusBadRequest, body: `{"error":"bad input"}`, refusal: MaintenanceRefusalValidation},
		{name: "not provisioned", status: http.StatusNotFound, refusal: MaintenanceRefusalNotProvisioned},
		{name: "invalid state", status: http.StatusConflict, refusal: MaintenanceRefusalInvalidState},
		{name: "capacity", status: http.StatusServiceUnavailable, body: `{"error":"full","code":"insufficient_resources"}`, refusal: MaintenanceRefusalCapacity},
		{name: "malformed validation", status: http.StatusBadRequest, body: `{"message":"proxy"}`, ambiguous: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			outcome := causalOutcomeClientForTest(t, test.status, test.body).
				restartCall(t.Context(), RestartRequest{MaintenanceID: id})
			require.True(t, outcome.Valid())
			assert.Equal(t, test.accepted, outcome.Accepted())
			assert.Equal(t, test.refusal != MaintenanceRefusalNone, outcome.Refused())
			assert.Equal(t, test.refusal, outcome.Refusal())
			assert.Equal(t, test.ambiguous, outcome.Ambiguous())
		})
	}
}

func TestHTTPClientMintsNotDispatchedOnlyWhenCircuitPreventsClosure(t *testing.T) {
	t.Parallel()
	client := causalOutcomeClientForTest(t, http.StatusInternalServerError, "backend failure")
	client.cb = newUnboundHTTPClientForTest(HTTPClientConfig{
		Name: "one-failure-circuit", BaseURL: client.baseURL, CBFailureThresh: 1,
	}).cb

	first := client.provisionCall(t.Context(), ProvisionRequest{})
	require.True(t, first.Ambiguous())
	second := client.provisionCall(t.Context(), ProvisionRequest{})
	require.True(t, second.NotDispatched())
	assert.ErrorIs(t, second.Err(), ErrCircuitOpen)
}

func TestHTTPClientUpgradeGateProofCannotBeSpoofedByTransportError(t *testing.T) {
	t.Parallel()
	client := newUnboundHTTPClientForTest(HTTPClientConfig{
		Name: "causal-test", BaseURL: "https://backend.invalid",
	})
	client.httpClient.Transport = causalOutcomeRoundTripper(func(
		*http.Request,
	) (*http.Response, error) {
		return nil, fmt.Errorf("transport says: %w", ErrBackendUpgradeRequired)
	})

	outcome := client.provisionCall(t.Context(), ProvisionRequest{})
	require.True(t, outcome.Ambiguous())
	assert.False(t, outcome.NotDispatched())
	assert.ErrorIs(t, outcome.Err(), ErrBackendUpgradeRequired)
}

func TestIdentityBoundHTTPClientMintsNoDispatchAtPrivateUpgradeGate(t *testing.T) {
	t.Parallel()
	id := mustBackendStorageID(t, testBackendStorageIDA)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	client, err := newIdentityBoundHTTPClientForTest(HTTPClientConfig{
		Name: "backend-a", BaseURL: server.URL, Secret: testIdentityClientKey,
	}, &testStorageIdentityResolver{id: id, bound: true})
	require.NoError(t, err)

	outcome := client.provisionCall(t.Context(), ProvisionRequest{})
	require.True(t, outcome.NotDispatched())
	assert.False(t, outcome.Ambiguous())
	assert.ErrorIs(t, outcome.Err(), ErrBackendUpgradeRequired)
}
