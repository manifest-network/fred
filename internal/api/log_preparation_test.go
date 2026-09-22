package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/testutil"
)

func TestLogsAuthorizationDoesNotOwnMaterializedResponse(t *testing.T) {
	var release chan struct{}
	var calls atomic.Int32
	client := &apiLogAdmissionBackend{getLogs: func(_ context.Context, lease string, tail int) (map[string]string, error) {
		calls.Add(1)
		require.Equal(t, 100, tail)
		return map[string]string{"web/0": lease}, nil
	}}
	handler, newRequests := newLogAdmissionAPIWithChain(t, time.Second, 0, client, func(_ context.Context, id string) {
		if id == testutil.ValidUUID1 {
			<-release
		}
	})
	synctest.Test(t, func(t *testing.T) {
		release = make(chan struct{})
		finish := sync.OnceFunc(func() { close(release) })
		defer finish()
		requests := newRequests()
		first := httptest.NewRecorder()
		done := make(chan struct{})
		go func() { defer close(done); handler.ServeHTTP(first, requests[0]) }()
		synctest.Wait()
		require.Zero(t, calls.Load())
		second := httptest.NewRecorder()
		handler.ServeHTTP(second, requests[1])
		require.Equal(t, http.StatusOK, second.Code, "an unrelated chain read must not monopolize the response-memory permit")
		require.Contains(t, second.Body.String(), testutil.ValidUUID3)
		require.NotContains(t, second.Body.String(), testutil.ValidUUID1)
		require.EqualValues(t, 1, calls.Load())
		finish()
		<-done
		require.Equal(t, http.StatusOK, first.Code)
		require.EqualValues(t, 2, calls.Load())
	})
}
