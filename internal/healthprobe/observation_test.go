package healthprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/uuidv4"
)

func TestHealthObservationAcceptsOnlyOneCanonicalDiagnosticID(t *testing.T) {
	const canonical = "7ae6f8d2-2b54-4f96-943c-3bb924e8d28d"
	for _, test := range []struct {
		name   string
		values []string
	}{
		{name: "missing"},
		{name: "canonical", values: []string{canonical}},
		{name: "uppercase", values: []string{strings.ToUpper(canonical)}},
		{name: "untrusted text", values: []string{"secret\nforged=true"}},
		{name: "oversized", values: []string{strings.Repeat("x", 4096)}},
		{name: "multiple", values: []string{canonical, canonical}},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := test.values
			_, observation := Start(t.Context(), values...)
			_, err := uuidv4.Parse(observation.ID(), errors.New("invalid ID"))
			require.NoError(t, err)
			if len(values) == 1 && values[0] == canonical {
				require.Equal(t, canonical, observation.ID())
			} else {
				require.NotEqual(t, canonical, observation.ID())
			}
		})
	}
	_, first := Start(t.Context())
	_, second := Start(t.Context())
	// An outbound caller starts a new observation even when its input context
	// already carries another request's diagnostic state.
	ctx, inherited := Start(t.Context())
	_, fresh := Start(ctx)
	require.NotEqual(t, first.ID(), second.ID())
	require.NotEqual(t, inherited.ID(), fresh.ID())
}

func TestHealthObservationLogsOnlyCompletedStagesWithoutRelabelingThem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var output bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&output, nil))
		ctx, observation := Start(t.Context())
		Record(ctx, StorageIdentity, time.Millisecond, nil)
		time.Sleep(2 * time.Second)
		Record(ctx, CallbackStore, 2*time.Second, context.Canceled)
		observation.Finish(logger, Server, 503, context.Canceled)
		var logged struct {
			ProbeID string `json:"probe_id"`
			Outcome string `json:"outcome"`
			Stages  map[string]struct {
				Outcome string `json:"outcome"`
			} `json:"stages"`
		}
		require.NoError(t, json.Unmarshal(output.Bytes(), &logged))
		require.Equal(t, observation.ID(), logged.ProbeID)
		require.Equal(t, "canceled", logged.Outcome)
		require.Len(t, logged.Stages, 2)
		require.Equal(t, "healthy", logged.Stages[string(StorageIdentity)].Outcome)
		require.Equal(t, "canceled", logged.Stages[string(CallbackStore)].Outcome)
		require.NotContains(t, logged.Stages, string(RetentionStore))
		require.Equal(t, 1, bytes.Count(output.Bytes(), []byte("\n")))
	})
}

func TestHealthObservationDoesNotLogFastSuccessOrRawError(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	_, successful := Start(t.Context())
	successful.Finish(logger, Client, 200, nil)
	require.Empty(t, output.String())
	_, failed := Start(t.Context())
	failed.Finish(logger, Client, 0, errors.New("https://secret.example/private-token"))
	require.Contains(t, output.String(), `"outcome":"failed"`)
	require.NotContains(t, output.String(), "private-token")
	require.NotContains(t, output.String(), "secret.example")
}

func TestHealthObservationLogsFastServerSuccessForTimeoutCorrelation(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	ctx, observation := Start(t.Context())
	Record(ctx, CallbackStore, time.Millisecond, nil)
	observation.Finish(logger, Server, 200, nil)
	require.Contains(t, output.String(), `"level":"INFO"`)
	require.Contains(t, output.String(), observation.ID())
	require.Contains(t, output.String(), `"outcome":"healthy"`)
	require.Contains(t, output.String(), `"callback_store"`)
}
