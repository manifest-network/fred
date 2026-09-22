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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/healthprobe"
	"github.com/manifest-network/fred/internal/uuidv4"
)

func TestHealthObservationKeepsDiagnosticIdentityThroughFailure(t *testing.T) {
	const id = "00fa2e0e-10b5-4e3a-a4ad-122c487e4d08"
	for _, test := range []struct {
		name   string
		header []string
	}{
		{name: "provider probe", header: []string{id}},
		{name: "invalid value", header: []string{"private-value\nforged=true"}},
		{name: "multiple values", header: []string{id, id}},
		{name: "direct probe"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			s := &Server{logger: slog.New(slog.NewJSONHandler(&output, nil))}
			called := false
			handler := s.observeHealth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				healthprobe.Record(r.Context(), healthprobe.CallbackStore, 2*time.Second, context.DeadlineExceeded)
				http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
			}))
			request := httptest.NewRequest(http.MethodGet, "/health", nil)
			for _, value := range test.header {
				request.Header.Add(healthprobe.Header, value)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.True(t, called, "diagnostic header syntax does not bypass or replace the probe")
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
			returnedID := response.Header().Get(healthprobe.Header)
			_, err := uuidv4.Parse(returnedID, errors.New("invalid probe ID"))
			require.NoError(t, err)
			if test.name == "provider probe" {
				require.Equal(t, id, returnedID)
			} else {
				require.NotEqual(t, id, returnedID)
			}
			var logged map[string]any
			require.NoError(t, json.Unmarshal(output.Bytes(), &logged))
			require.Equal(t, returnedID, logged["probe_id"])
			require.Equal(t, float64(503), logged["status"])
			require.Equal(t, 1, bytes.Count(output.Bytes(), []byte("\n")))
			require.NotContains(t, output.String(), "private-value")
			require.Contains(t, output.String(), `"callback_store"`)
			require.Contains(t, output.String(), `"outcome":"deadline_exceeded"`)
		})
	}
}
