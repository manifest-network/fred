package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	maintenanceapp "github.com/manifest-network/fred/internal/provisioner/maintenance"
	"github.com/manifest-network/fred/internal/testutil"
)

type maintenanceServiceFunc func(context.Context, maintenanceapp.Command) maintenanceapp.Result

func (execute maintenanceServiceFunc) Execute(
	ctx context.Context,
	command maintenanceapp.Command,
) maintenanceapp.Result {
	return execute(ctx, command)
}

func maintenanceRequest(
	t *testing.T,
	method, endpoint, body, id string,
	service MaintenanceService,
) (*httptest.ResponseRecorder, []maintenanceapp.Command) {
	t.Helper()
	keyPair := testutil.NewTestKeyPair("maintenance-handler")
	leaseUUID := testutil.ValidUUID1
	var commands []maintenanceapp.Command
	wrapped := maintenanceServiceFunc(func(
		_ context.Context,
		command maintenanceapp.Command,
	) maintenanceapp.Result {
		commands = append(commands, command)
		if service != nil {
			return service.Execute(t.Context(), command)
		}
		return maintenanceapp.NewResult(maintenanceapp.OutcomeAccepted, nil)
	})
	handlers := NewHandlers(HandlersConfig{
		MaintenanceService: wrapped,
		ProviderUUID:       testutil.ValidUUID2,
		Bech32Prefix:       "manifest",
	})
	request := httptest.NewRequest(method, "/v1/leases/"+leaseUUID+"/"+endpoint, strings.NewReader(body))
	request.SetPathValue("lease_uuid", leaseUUID)
	request.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(keyPair, leaseUUID, time.Now()))
	if id != "" {
		request.Header.Set(idempotencyKeyHeader, id)
	}
	response := httptest.NewRecorder()
	if endpoint == "restart" {
		handlers.RestartLease(response, request)
	} else {
		handlers.UpdateLease(response, request)
	}
	return response, commands
}

func TestMaintenanceHandlersRequireCanonicalIdempotencyKeyBeforeService(t *testing.T) {
	for name, id := range map[string]string{
		"missing":       "",
		"uppercase":     "550E8400-E29B-41D4-A716-446655440000",
		"not uuid v4":   "018f47a2-8b1c-7def-8123-456789abcdef",
		"non canonical": "550e8400e29b41d4a716446655440000",
	} {
		t.Run(name, func(t *testing.T) {
			response, commands := maintenanceRequest(
				t, http.MethodPost, "restart", "", id, nil,
			)
			assert.Equal(t, http.StatusBadRequest, response.Code)
			assert.Empty(t, commands)
		})
	}
}

func TestMaintenanceHandlersRejectDuplicateIdempotencyKeyBeforeService(t *testing.T) {
	keyPair := testutil.NewTestKeyPair("maintenance-handler-duplicate")
	leaseUUID := testutil.ValidUUID1
	called := false
	handlers := NewHandlers(HandlersConfig{
		MaintenanceService: maintenanceServiceFunc(func(
			context.Context, maintenanceapp.Command,
		) maintenanceapp.Result {
			called = true
			return maintenanceapp.NewResult(maintenanceapp.OutcomeAccepted, nil)
		}),
		ProviderUUID: testutil.ValidUUID2,
		Bech32Prefix: "manifest",
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/leases/"+leaseUUID+"/restart", nil)
	request.SetPathValue("lease_uuid", leaseUUID)
	request.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(keyPair, leaseUUID, time.Now()))
	request.Header.Add(idempotencyKeyHeader, "550e8400-e29b-41d4-a716-446655440000")
	request.Header.Add(idempotencyKeyHeader, "6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	response := httptest.NewRecorder()

	handlers.RestartLease(response, request)

	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.False(t, called)
}

func TestMaintenanceHandlersValidateIdempotencyKeyBeforeConsumingBearerToken(t *testing.T) {
	keyPair := testutil.NewTestKeyPair("maintenance-handler-admission-order")
	leaseUUID := testutil.ValidUUID1
	trackerCalls := 0
	handlers := NewHandlers(HandlersConfig{
		TokenTracker: &mockTokenTracker{tryUseFunc: func(string) error {
			trackerCalls++
			return nil
		}},
		MaintenanceService: maintenanceServiceFunc(func(
			context.Context, maintenanceapp.Command,
		) maintenanceapp.Result {
			t.Fatal("maintenance service called for an invalid idempotency key")
			return maintenanceapp.Result{}
		}),
		ProviderUUID: testutil.ValidUUID2,
		Bech32Prefix: "manifest",
	})

	for _, endpoint := range []string{"restart", "update"} {
		request := httptest.NewRequest(
			http.MethodPost, "/v1/leases/"+leaseUUID+"/"+endpoint,
			strings.NewReader(`{"payload":"dGVzdA=="}`),
		)
		request.SetPathValue("lease_uuid", leaseUUID)
		request.Header.Set(
			"Authorization",
			"Bearer "+testutil.CreateTestToken(keyPair, leaseUUID, time.Now()),
		)
		request.Header.Set(idempotencyKeyHeader, "not-a-canonical-uuidv4")
		response := httptest.NewRecorder()

		if endpoint == "restart" {
			handlers.RestartLease(response, request)
		} else {
			handlers.UpdateLease(response, request)
		}

		assert.Equal(t, http.StatusBadRequest, response.Code)
	}
	assert.Zero(t, trackerCalls, "invalid command identity must not consume replay authority")
}

func TestMaintenanceHandlersPassTypedExactCommandToService(t *testing.T) {
	const id = "550e8400-e29b-41d4-a716-446655440000"
	t.Run("restart", func(t *testing.T) {
		response, commands := maintenanceRequest(t, http.MethodPost, "restart", "", id, nil)
		require.Equal(t, http.StatusAccepted, response.Code)
		require.Len(t, commands, 1)
		assert.True(t, commands[0].ID.Valid())
		assert.Equal(t, id, commands[0].ID.String())
		assert.Equal(t, maintenanceapp.KindRestart, commands[0].Kind)
		assert.Empty(t, commands[0].Payload)
	})

	t.Run("update", func(t *testing.T) {
		response, commands := maintenanceRequest(
			t, http.MethodPost, "update", `{"payload":"dGVzdA=="}`, id, nil,
		)
		require.Equal(t, http.StatusAccepted, response.Code)
		require.Len(t, commands, 1)
		assert.Equal(t, maintenanceapp.KindUpdate, commands[0].Kind)
		assert.Equal(t, []byte("test"), commands[0].Payload)
	})
}

func TestMaintenanceHandlersTranslateServiceOutcomes(t *testing.T) {
	for name, test := range map[string]struct {
		outcome maintenanceapp.Outcome
		status  int
	}{
		"accepted":             {maintenanceapp.OutcomeAccepted, http.StatusAccepted},
		"not found":            {maintenanceapp.OutcomeNotFound, http.StatusNotFound},
		"lease ended":          {maintenanceapp.OutcomeNoLongerActive, http.StatusConflict},
		"forbidden":            {maintenanceapp.OutcomeForbidden, http.StatusForbidden},
		"busy":                 {maintenanceapp.OutcomeAlreadyInProgress, http.StatusConflict},
		"idempotency conflict": {maintenanceapp.OutcomeCommandConflict, http.StatusConflict},
		"invalid state":        {maintenanceapp.OutcomeBackendInvalidState, http.StatusConflict},
		"validation":           {maintenanceapp.OutcomeBackendValidation, http.StatusBadRequest},
		"temporarily down":     {maintenanceapp.OutcomeServiceUnavailable, http.StatusServiceUnavailable},
		"internal":             {maintenanceapp.OutcomeInternalFailure, http.StatusInternalServerError},
	} {
		t.Run(name, func(t *testing.T) {
			service := maintenanceServiceFunc(func(
				context.Context,
				maintenanceapp.Command,
			) maintenanceapp.Result {
				return maintenanceapp.NewResult(test.outcome, nil)
			})
			response, commands := maintenanceRequest(
				t, http.MethodPost, "restart", "",
				"550e8400-e29b-41d4-a716-446655440000", service,
			)
			assert.Equal(t, test.status, response.Code)
			assert.Len(t, commands, 1)
		})
	}
}
