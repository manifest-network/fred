package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
