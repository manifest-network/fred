package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pagingRetentionServer serves /retentions using the real PaginateRetentions
// helper so the client loop is exercised against production paging semantics.
func pagingRetentionServer(t *testing.T, all []RetainedLease, calls *int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/retentions" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if calls != nil {
			*calls++
		}
		limit, cont, err := ParsePageParams(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		page, next := PaginateRetentions(all, cont, limit)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ListRetentionsResponse{Retentions: page, Continue: next})
	}))
}

// retUUID renders i as a lexically-sortable valid UUID. The client loop sends the
// last LeaseUUID back as the `continue` query param, which ParsePageParams
// validates as a UUID — so IDs in these end-to-end tests must be real UUIDs
// (zero-padded so lexical == numeric order).
func retUUID(i int) string {
	return fmt.Sprintf("%08d-0000-0000-0000-000000000000", i)
}

func manyRetentions(n int) []RetainedLease {
	out := make([]RetainedLease, n)
	for i := range out {
		out[i] = RetainedLease{LeaseUUID: retUUID(i)}
	}
	return out
}

func TestHTTPClient_ListRetentions_ReassemblesAllPages(t *testing.T) {
	all := manyRetentions(2500) // > default page size of 1000 -> 3 pages
	var calls int
	server := pagingRetentionServer(t, all, &calls)
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL})
	got, err := c.ListRetentions(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 2500)
	assert.Greater(t, calls, 1, "must walk multiple pages, not a single unpaginated GET")

	seen := make(map[string]int)
	for _, r := range got {
		seen[r.LeaseUUID]++
	}
	assert.Len(t, seen, 2500, "no duplicates across pages")
	assert.Equal(t, retUUID(0), got[0].LeaseUUID, "sorted order preserved")
}

// The cliff this ticket fixes: a retained inventory whose FULL list exceeds the
// per-response byte cap is fetched successfully because each PAGE stays under it.
// The pre-pagination single-GET client returns ErrResponseTooLarge here.
func TestHTTPClient_ListRetentions_LargeInventoryStaysUnderPerPageCap(t *testing.T) {
	all := manyRetentions(10000) // full list ~520 KiB
	server := pagingRetentionServer(t, all, nil)
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL, MaxRetentionsBytes: 128 << 10}) // 128 KiB per page
	got, err := c.ListRetentions(context.Background())
	require.NoError(t, err, "paginated fetch keeps every page under the byte cap")
	assert.Len(t, got, 10000)
}

// Acceptance (ENG-451): a ~20k retained inventory — whose single-response JSON
// (~1.04 MiB) exceeds the DEFAULT 1 MiB /retentions cap that the pre-pagination
// client used for the whole list — is now fetched successfully because each
// default-size page (1000 records) stays well under the per-page cap.
func TestHTTPClient_ListRetentions_20kInventoryUnderDefaultCap(t *testing.T) {
	all := manyRetentions(20000)
	server := pagingRetentionServer(t, all, nil)
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL}) // default 1 MiB per-page cap
	got, err := c.ListRetentions(context.Background())
	require.NoError(t, err, "20k retained leases fetch without ErrResponseTooLarge once paginated")
	assert.Len(t, got, 20000)
}

func TestHTTPClient_ListRetentions_SmallPageLimit(t *testing.T) {
	all := manyRetentions(25)
	server := pagingRetentionServer(t, all, nil)
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL, RetentionsPageLimit: 10})
	got, err := c.ListRetentions(context.Background())
	require.NoError(t, err)
	assert.Len(t, got, 25)
}

func TestHTTPClient_ListRetentions_NonAdvancingContinueErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ListRetentionsResponse{
			Retentions: []RetainedLease{{LeaseUUID: "stuck"}},
			Continue:   "stuck", // never advances
		})
	}))
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL})
	_, err := c.ListRetentions(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-advancing")
}

func TestHTTPClient_ListRetentions_BackCompatSinglePageNoContinue(t *testing.T) {
	// Old server: ignores limit/continue, returns full list, no continue field.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"retentions":[{"lease_uuid":"lease-a"},{"lease_uuid":"lease-b"}]}`))
	}))
	defer server.Close()

	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: server.URL})
	got, err := c.ListRetentions(context.Background())
	require.NoError(t, err)
	assert.Len(t, got, 2, "no continue field => stop after one page")
}

func TestNewHTTPClient_RetentionsPageLimitDefault(t *testing.T) {
	c := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: "http://example.com"})
	assert.Equal(t, DefaultRetentionsPageLimit, c.retentionsPageLimit, "zero config falls back to default")

	c2 := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: "http://example.com", RetentionsPageLimit: 250})
	assert.Equal(t, 250, c2.retentionsPageLimit, "explicit config is honored")

	c3 := NewHTTPClient(HTTPClientConfig{Name: "t", BaseURL: "http://example.com", RetentionsPageLimit: -5})
	assert.Equal(t, DefaultRetentionsPageLimit, c3.retentionsPageLimit, "negative config falls back to default")
}
