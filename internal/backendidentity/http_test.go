package backendidentity

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResponseMiddlewareRequiresValidConstruction(t *testing.T) {
	t.Parallel()

	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	wrapped, err := ResponseMiddleware(ID{}, next)
	assert.Nil(t, wrapped)
	assert.ErrorIs(t, err, ErrInvalidID)

	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	wrapped, err = ResponseMiddleware(id, nil)
	assert.Nil(t, wrapped)
	assert.Error(t, err)
}

func TestResponseMiddlewareRejectsAmbiguousIdentityBeforeDispatch(t *testing.T) {
	t.Parallel()

	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	other, err := Parse("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	require.NoError(t, err)

	tests := []struct {
		name       string
		rawQuery   string
		wantStatus int
		wantCalls  int32
	}{
		{name: "absent supports old provider", wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "matching", rawQuery: QueryParameter + "=" + id.String(), wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "duplicate", rawQuery: QueryParameter + "=" + id.String() + "&" + QueryParameter + "=" + id.String(), wantStatus: http.StatusBadRequest},
		{name: "encoded duplicate key", rawQuery: QueryParameter + "=" + id.String() + "&backend%5Fstorage%5Fid=" + id.String(), wantStatus: http.StatusBadRequest},
		{name: "missing value", rawQuery: QueryParameter, wantStatus: http.StatusBadRequest},
		{name: "noncanonical", rawQuery: QueryParameter + "=" + strings.ToUpper(id.String()), wantStatus: http.StatusBadRequest},
		{name: "malformed escape", rawQuery: QueryParameter + "=%ZZ", wantStatus: http.StatusBadRequest},
		{name: "mismatch", rawQuery: QueryParameter + "=" + other.String(), wantStatus: http.StatusConflict},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			wrapped, err := ResponseMiddleware(id, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			req.URL.RawQuery = test.rawQuery
			response := httptest.NewRecorder()
			wrapped.ServeHTTP(response, req)

			assert.Equal(t, test.wantStatus, response.Code)
			assert.Equal(t, test.wantCalls, calls.Load())
			assert.Equal(t, []string{id.String()}, response.Header().Values(ResponseHeader),
				"the observed identity must be present even on a rejected request")
		})
	}
}

func TestRequireBoundPathRejectsWrongIdentityBeforeDispatch(t *testing.T) {
	t.Parallel()

	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	other, err := Parse("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	require.NoError(t, err)

	var calls atomic.Int32
	wrapped, err := RequireBoundPath(id, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	require.NoError(t, err)

	for _, test := range []struct {
		name       string
		pathValue  string
		wantStatus int
		wantCalls  int32
	}{
		{name: "matching", pathValue: id.String(), wantStatus: http.StatusNoContent, wantCalls: 1},
		{name: "missing", wantStatus: http.StatusNotFound},
		{name: "noncanonical", pathValue: strings.ToUpper(id.String()), wantStatus: http.StatusNotFound},
		{name: "different", pathValue: other.String(), wantStatus: http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls.Store(0)
			req := httptest.NewRequest(http.MethodPost, "/provision", nil)
			req.SetPathValue("storage_id", test.pathValue)
			response := httptest.NewRecorder()
			wrapped.ServeHTTP(response, req)

			assert.Equal(t, test.wantStatus, response.Code)
			assert.Equal(t, test.wantCalls, calls.Load())
		})
	}

	invalid, err := RequireBoundPath(ID{}, http.NotFoundHandler())
	assert.Nil(t, invalid)
	assert.ErrorIs(t, err, ErrInvalidID)

	invalid, err = RequireBoundPath(id, nil)
	assert.Nil(t, invalid)
	assert.Error(t, err)
}
