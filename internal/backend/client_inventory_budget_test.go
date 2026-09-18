package backend

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestInventoryWalkBudgetsAreCompleteOrError(t *testing.T) {
	for _, tt := range []struct {
		name                  string
		firstItems, lastItems int
		firstBytes, lastBytes int64
		wantError             bool
	}{
		{"item boundary", MaxInventoryItems - 1, 1, 1, 1, false},
		{"item overflow", MaxInventoryItems, 1, 1, 1, true},
		{"byte boundary", 1, 1, MaxInventoryBytes - 1, 1, false},
		{"byte overflow", 1, 1, MaxInventoryBytes, 1, true},
		{"empty pages still consume bytes", 0, 0, MaxInventoryBytes, 1, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			items, identity, err := walkKeysetPages(context.Background(), "test inventory", false, time.Second, func(context.Context, string) (inventoryPage[struct{}], error) {
				calls++
				if calls == 1 {
					return inventoryPage[struct{}]{items: make([]struct{}, tt.firstItems), next: "next", bodyBytes: tt.firstBytes}, nil
				}
				return inventoryPage[struct{}]{items: make([]struct{}, tt.lastItems), bodyBytes: tt.lastBytes}, nil
			})
			require.Equal(t, 2, calls)
			if tt.wantError {
				require.ErrorIs(t, err, ErrResponseTooLarge)
				require.Nil(t, items, "an oversized inventory cannot authorize absence from its prefix")
				require.Equal(t, backendidentity.ID{}, identity)
			} else {
				require.NoError(t, err)
				require.Len(t, items, tt.firstItems+tt.lastItems)
			}
		})
	}
}

func TestInventoryHTTPPagesPreserveCompleteWireSize(t *testing.T) {
	for _, endpoint := range []string{"provisions", "retentions"} {
		t.Run(endpoint, func(t *testing.T) {
			body := fmt.Sprintf(`{"%s":[],"continue":"next"}`, endpoint) + strings.Repeat(" ", 1024)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/"+endpoint, r.URL.Path)
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			client := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "wire-budget", BaseURL: server.URL})
			if endpoint == "provisions" {
				page, err := client.fetchProvisionsPage(context.Background(), "", false)
				require.NoError(t, err)
				require.EqualValues(t, len(body), page.bodyBytes)
				require.NotNil(t, page.items)
				require.Equal(t, "next", page.next)
			} else {
				page, err := client.fetchRetentionsPage(context.Background(), "", false)
				require.NoError(t, err)
				require.EqualValues(t, len(body), page.bodyBytes)
				require.NotNil(t, page.items)
				require.Equal(t, "next", page.next)
			}
		})
	}
}

func TestInventoryWalkDeadlineIncludesSuccessfulPages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		started := time.Now()
		items, identity, err := walkKeysetPages(context.Background(), "slow inventory", false, 100*time.Millisecond, func(ctx context.Context, _ string) (inventoryPage[struct{}], error) {
			calls++
			select {
			case <-ctx.Done():
				return inventoryPage[struct{}]{}, ctx.Err()
			case <-time.After(60 * time.Millisecond):
				// Each page individually fits the 100ms request timeout and
				// consumes almost no memory, but the whole walk must finish.
				return inventoryPage[struct{}]{items: []struct{}{}, next: fmt.Sprintf("%09d", calls), bodyBytes: 40}, nil
			}
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, items)
		require.Equal(t, backendidentity.ID{}, identity)
		require.Equal(t, 2, calls)
		require.Equal(t, 100*time.Millisecond, time.Since(started))
	})
}
