package maintenance

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateValidationDetailSurvivesExactReplayAndReopen(t *testing.T) {
	store, path := newPlacementAuthority(t, testLeaseA)
	client, calls := causalMaintenanceBackendForTest(t, store, http.StatusBadRequest,
		`{"error":"services.web.image: registry is not allowed","validation_code":"image_not_allowed"}`)
	service := newTestService(t, store, client, &fakePayloads{}, testLeaseA)
	command := Command{
		ID: requestID(t, testRequestA), LeaseUUID: testLeaseA, Tenant: testTenant,
		Kind: KindUpdate, Payload: []byte("rejected manifest"),
	}
	const want = "services.web.image: registry is not allowed"
	for range 2 {
		result := service.Execute(t.Context(), command)
		require.Equal(t, OutcomeBackendValidation, result.Outcome(), result.Cause())
		assert.Equal(t, want, result.Detail())
	}
	assert.Equal(t, 1, calls(), "exact replay must use the recorded refusal")
	require.NoError(t, store.Close())
	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	recovered := newTestService(t, reopened, client, &fakePayloads{}, testLeaseA)
	result := recovered.Execute(t.Context(), command)
	require.Equal(t, OutcomeBackendValidation, result.Outcome(), result.Cause())
	assert.Equal(t, want, result.Detail())
	assert.Equal(t, 1, calls(), "reopened receipt must not contact the backend")
}

func TestAmbiguousUpdateResponseCannotExposeTenantDetail(t *testing.T) {
	for name, response := range map[string]struct {
		status int
		body   string
	}{
		"unknown refusal code": {http.StatusServiceUnavailable, `{"error":"private diagnostic","code":"future_code"}`},
		"unusable envelope":    {http.StatusBadRequest, `{"message":"private diagnostic"}`},
		"backend failure":      {http.StatusInternalServerError, `{"error":"private diagnostic"}`},
	} {
		t.Run(name, func(t *testing.T) {
			store, _ := newPlacementAuthority(t, testLeaseA)
			client, _ := causalMaintenanceBackendForTest(t, store, response.status, response.body)
			service := newTestService(t, store, client, &fakePayloads{}, testLeaseA)
			result := service.Execute(t.Context(), Command{
				ID: requestID(t, testRequestA), LeaseUUID: testLeaseA, Tenant: testTenant,
				Kind: KindUpdate, Payload: []byte("manifest"),
			})
			assert.Equal(t, OutcomeServiceUnavailable, result.Outcome())
			assert.Empty(t, result.Detail())
		})
	}
}
