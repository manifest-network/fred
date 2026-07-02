package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestHandleListRetentions_Paginates(t *testing.T) {
	all := []backend.RetainedLease{
		{LeaseUUID: "11111111-1111-1111-1111-111111111111"},
		{LeaseUUID: "22222222-2222-2222-2222-222222222222"},
		{LeaseUUID: "33333333-3333-3333-3333-333333333333"},
	}
	mb := &mockBackend{
		ListRetentionsFunc: func(context.Context) ([]backend.RetainedLease, error) { return all, nil },
	}

	// First page (limit=2): two items + continue = last UUID.
	w := httptest.NewRecorder()
	newMockHandler(mb).ServeHTTP(w, signedGetRequest("/retentions?limit=2"))
	require.Equal(t, http.StatusOK, w.Code)
	var p1 backend.ListRetentionsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p1))
	assert.Len(t, p1.Retentions, 2)
	assert.Equal(t, "22222222-2222-2222-2222-222222222222", p1.Continue)

	// Second page resumes from continue: one item, no continue.
	w2 := httptest.NewRecorder()
	newMockHandler(mb).ServeHTTP(w2, signedGetRequest("/retentions?limit=2&continue="+p1.Continue))
	require.Equal(t, http.StatusOK, w2.Code)
	var p2 backend.ListRetentionsResponse
	require.NoError(t, json.Unmarshal(w2.Body.Bytes(), &p2))
	assert.Len(t, p2.Retentions, 1)
	assert.Empty(t, p2.Continue)
}

// Proves the handler routes /retentions through the O(limit) paged store method
// (ListRetentionsPage) with the parsed cursor+limit, not the full-scan ListRetentions.
func TestHandleListRetentions_RoutesToPagedBackend(t *testing.T) {
	var gotAfter string
	var gotLimit int
	mb := &mockBackend{
		ListRetentionsPageFunc: func(_ context.Context, after string, limit int) ([]backend.RetainedLease, string, error) {
			gotAfter, gotLimit = after, limit
			return []backend.RetainedLease{{LeaseUUID: "x"}}, "x", nil
		},
		ListRetentionsFunc: func(context.Context) ([]backend.RetainedLease, error) {
			t.Fatal("handler must use the paged ListRetentionsPage, not the full-scan ListRetentions")
			return nil, nil
		},
	}
	w := httptest.NewRecorder()
	newMockHandler(mb).ServeHTTP(w, signedGetRequest("/retentions?limit=5&continue=11111111-1111-1111-1111-111111111111"))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "11111111-1111-1111-1111-111111111111", gotAfter)
	assert.Equal(t, 5, gotLimit)

	var resp backend.ListRetentionsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "x", resp.Continue)
}

func TestHandleListRetentions_MalformedContinueIs400(t *testing.T) {
	called := false
	mb := &mockBackend{
		ListRetentionsFunc: func(context.Context) ([]backend.RetainedLease, error) { called = true; return nil, nil },
	}
	w := httptest.NewRecorder()
	newMockHandler(mb).ServeHTTP(w, signedGetRequest("/retentions?limit=2&continue=not-a-uuid"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, called, "invalid pagination params must fail fast before the O(RETAINED) ListRetentions fetch")
}
