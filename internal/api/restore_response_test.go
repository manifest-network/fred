package api

import (
	"context"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	maintenanceapp "github.com/manifest-network/fred/internal/provisioner/maintenance"
	restoreapp "github.com/manifest-network/fred/internal/provisioner/restore"
	"github.com/manifest-network/fred/internal/testutil"
)

const inventoryReconcilingBodyJSON = `{"error":"the provider is reconciling its inventory; retry shortly","code":503,"reason":"inventory_reconciling"}`

// TestRestoreResponseFor_EveryDeclaredOutcome requires an HTTP answer for
// every restore outcome, and an operator log message for every 5xx answer. A
// silent 503 is how ENG-2161 hid its cause.
func TestRestoreResponseFor_EveryDeclaredOutcome(t *testing.T) {
	statuses := map[string]struct {
		outcome restoreapp.Outcome
		status  int
	}{
		"OutcomeInvalid":                  {restoreapp.OutcomeInvalid, http.StatusInternalServerError},
		"OutcomeAccepted":                 {restoreapp.OutcomeAccepted, http.StatusAccepted},
		"OutcomeTargetNotPending":         {restoreapp.OutcomeTargetNotPending, http.StatusConflict},
		"OutcomeSourceNotFound":           {restoreapp.OutcomeSourceNotFound, http.StatusNotFound},
		"OutcomeSourceUnavailable":        {restoreapp.OutcomeSourceUnavailable, http.StatusServiceUnavailable},
		"OutcomeSourceBusy":               {restoreapp.OutcomeSourceBusy, http.StatusConflict},
		"OutcomeTargetBusy":               {restoreapp.OutcomeTargetBusy, http.StatusConflict},
		"OutcomeServiceUnavailable":       {restoreapp.OutcomeServiceUnavailable, http.StatusServiceUnavailable},
		"OutcomeNotRetained":              {restoreapp.OutcomeNotRetained, http.StatusNotFound},
		"OutcomeBackendInvalidState":      {restoreapp.OutcomeBackendInvalidState, http.StatusConflict},
		"OutcomeInsufficientResources":    {restoreapp.OutcomeInsufficientResources, http.StatusServiceUnavailable},
		"OutcomeBackendUnavailable":       {restoreapp.OutcomeBackendUnavailable, http.StatusServiceUnavailable},
		"OutcomeTierTooSmall":             {restoreapp.OutcomeTierTooSmall, http.StatusUnprocessableEntity},
		"OutcomeInvalidRequest":           {restoreapp.OutcomeInvalidRequest, http.StatusBadRequest},
		"OutcomeBackendMalformedResponse": {restoreapp.OutcomeBackendMalformedResponse, http.StatusBadGateway},
		"OutcomeInternalFailure":          {restoreapp.OutcomeInternalFailure, http.StatusInternalServerError},
		"OutcomeSourceLost":               {restoreapp.OutcomeSourceLost, http.StatusGone},
		"OutcomeTargetBillingOnly":        {restoreapp.OutcomeTargetBillingOnly, http.StatusBadRequest},
		"OutcomeTargetHasPayload":         {restoreapp.OutcomeTargetHasPayload, http.StatusConflict},
		"OutcomeInventoryReconciling":     {restoreapp.OutcomeInventoryReconciling, http.StatusServiceUnavailable},
	}
	declared := testutil.DeclaredConstantNames(t,
		filepath.Join("..", "provisioner", "restore", "service.go"), "Outcome")
	require.ElementsMatch(t, declared, slices.Collect(maps.Keys(statuses)),
		"decide the HTTP answer for every declared restore outcome")

	unknown := restoreResponseFor(restoreapp.Result{Outcome: restoreapp.Outcome(255)})
	require.Equal(t, http.StatusInternalServerError, unknown.status)
	for name, decision := range statuses {
		response := restoreResponseFor(restoreapp.Result{Outcome: decision.outcome})
		assert.Equal(t, decision.status, response.status, name)
		assert.NotEqual(t, unknown.logMessage, response.logMessage, "%s must not reach the unknown-outcome branch", name)
		if response.status >= http.StatusInternalServerError {
			assert.NotEmpty(t, response.logMessage, "%s answers %d and must name its operator event", name, response.status)
		}
		retryable := decision.outcome == restoreapp.OutcomeInventoryReconciling
		assert.Equal(t, retryable, response.retryAfter != "", "%s Retry-After", name)
	}
}

// TestRestoreLeaseInventoryReconcilingIsRetryable checks the complete answer
// a tenant sees for an ENG-2161 deferral, and the operator log naming it.
func TestRestoreLeaseInventoryReconcilingIsRetryable(t *testing.T) {
	logs := captureDefaultLogger(t)
	response := restoreLeaseWithOutcome(t, restoreapp.OutcomeInventoryReconciling)

	assert.Equal(t, http.StatusServiceUnavailable, response.Code)
	assert.Equal(t, "1", response.Header().Get("Retry-After"))
	assert.JSONEq(t, inventoryReconcilingBodyJSON, response.Body.String())
	record := logs.record(t, "restore deferred until the open inventory sweep projects")
	assert.Equal(t, slog.LevelWarn.String(), record[slog.LevelKey])
	assert.Equal(t, testutil.ValidUUID1, record["lease_uuid"])
	assert.Equal(t, fromLeaseUUID, record["from_lease"])
	assert.Contains(t, record, "cause")
}

// TestRestoreLeaseLogsEveryServerError drives each 5xx outcome through the
// handler: before ENG-2161 three restore 503 answers logged nothing.
func TestRestoreLeaseLogsEveryServerError(t *testing.T) {
	for _, outcome := range []restoreapp.Outcome{
		restoreapp.OutcomeSourceUnavailable, restoreapp.OutcomeServiceUnavailable,
		restoreapp.OutcomeInsufficientResources, restoreapp.OutcomeBackendUnavailable,
		restoreapp.OutcomeInventoryReconciling, restoreapp.OutcomeBackendMalformedResponse,
		restoreapp.OutcomeInvalid, restoreapp.OutcomeInternalFailure, restoreapp.Outcome(255),
	} {
		logs := captureDefaultLogger(t)
		response := restoreLeaseWithOutcome(t, outcome)
		require.GreaterOrEqual(t, response.Code, http.StatusInternalServerError, "outcome %d", outcome)
		message := restoreResponseFor(restoreapp.Result{Outcome: outcome}).logMessage
		record := logs.record(t, message)
		assert.Equal(t, testutil.ValidUUID1, record["lease_uuid"], "outcome %d", outcome)
	}
}

func restoreLeaseWithOutcome(t *testing.T, outcome restoreapp.Outcome) *httptest.ResponseRecorder {
	t.Helper()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	handlers := &Handlers{
		client: &mockChainClient{
			getLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{
					Uuid: uuid, Tenant: kp.Address, ProviderUuid: providerUUID,
					State: billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
		},
		providerUUID: providerUUID, bech32Prefix: "manifest",
		restoreService: restoreServiceFunc(func(context.Context, restoreapp.Command) restoreapp.Result {
			return restoreapp.Result{Outcome: outcome}
		}),
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/leases/"+leaseUUID+"/restore",
		strings.NewReader(`{"from_lease_uuid":"`+fromLeaseUUID+`"}`))
	request.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(kp, leaseUUID, time.Now()))
	request.SetPathValue("lease_uuid", leaseUUID)
	response := httptest.NewRecorder()
	handlers.RestoreLease(response, request)
	return response
}

// TestMaintenanceHandlers_EveryDeclaredOutcomeIsTranslated keeps every
// maintenance outcome out of the handler's unknown-outcome branch, and pins
// the ENG-2161 deferral's retryable answer for both maintenance kinds.
func TestMaintenanceHandlers_EveryDeclaredOutcomeIsTranslated(t *testing.T) {
	outcomes := map[string]maintenanceapp.Outcome{
		"OutcomeInvalid":              maintenanceapp.OutcomeInvalid,
		"OutcomeAccepted":             maintenanceapp.OutcomeAccepted,
		"OutcomeNotFound":             maintenanceapp.OutcomeNotFound,
		"OutcomeNoLongerActive":       maintenanceapp.OutcomeNoLongerActive,
		"OutcomeForbidden":            maintenanceapp.OutcomeForbidden,
		"OutcomeAlreadyInProgress":    maintenanceapp.OutcomeAlreadyInProgress,
		"OutcomeCommandConflict":      maintenanceapp.OutcomeCommandConflict,
		"OutcomeBackendInvalidState":  maintenanceapp.OutcomeBackendInvalidState,
		"OutcomeBackendValidation":    maintenanceapp.OutcomeBackendValidation,
		"OutcomeCapacityReserved":     maintenanceapp.OutcomeCapacityReserved,
		"OutcomeServiceUnavailable":   maintenanceapp.OutcomeServiceUnavailable,
		"OutcomeInternalFailure":      maintenanceapp.OutcomeInternalFailure,
		"OutcomeBackendLost":          maintenanceapp.OutcomeBackendLost,
		"OutcomeExpired":              maintenanceapp.OutcomeExpired,
		"OutcomeInventoryReconciling": maintenanceapp.OutcomeInventoryReconciling,
	}
	declared := testutil.DeclaredConstantNames(t,
		filepath.Join("..", "provisioner", "maintenance", "service.go"), "Outcome")
	require.ElementsMatch(t, declared, slices.Collect(maps.Keys(outcomes)),
		"translate every declared maintenance outcome")
	for name, outcome := range outcomes {
		service := maintenanceServiceFunc(func(context.Context, maintenanceapp.Command) maintenanceapp.Result {
			return maintenanceapp.NewResult(outcome, nil)
		})
		response, _ := maintenanceRequest(t, http.MethodPost, "restart", "",
			"550e8400-e29b-41d4-a716-446655440000", service)
		failsAsInternal := outcome == maintenanceapp.OutcomeInvalid || outcome == maintenanceapp.OutcomeInternalFailure
		assert.Equal(t, failsAsInternal, response.Code == http.StatusInternalServerError, "%s answered %d", name, response.Code)
	}

	for _, kind := range []struct{ path, body string }{{"restart", ""}, {"update", `{"payload":"dGVzdA=="}`}} {
		service := maintenanceServiceFunc(func(context.Context, maintenanceapp.Command) maintenanceapp.Result {
			return maintenanceapp.NewResult(maintenanceapp.OutcomeInventoryReconciling, nil)
		})
		response, _ := maintenanceRequest(t, http.MethodPost, kind.path, kind.body,
			"550e8400-e29b-41d4-a716-446655440000", service)
		assert.Equal(t, http.StatusServiceUnavailable, response.Code, kind.path)
		assert.Equal(t, "1", response.Header().Get("Retry-After"), kind.path)
		assert.JSONEq(t, inventoryReconcilingBodyJSON, response.Body.String(), kind.path)
	}
}
