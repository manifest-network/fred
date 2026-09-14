package backend

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHTTPClientGetLogsPreservesBackendContentBudget(t *testing.T) {
	for _, test := range []struct {
		name string
		logs map[string]string
	}{
		{"full aggregate content", fullLogContentBudget()},
		{"JSON expansion", map[string]string{"web/0": strings.Repeat("\x00", 3<<20)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(test.logs)
			}))
			defer server.Close()
			client := newUnboundHTTPClientForTest(HTTPClientConfig{
				Name: "log-content-budget", BaseURL: server.URL, Timeout: 30 * time.Second,
			})
			logs, err := client.GetLogs(t.Context(), "lease-1", 100)
			require.NoError(t, err, "the default transport must carry valid backend output above 16 MiB")
			require.Equal(t, test.logs, logs)
		})
	}
}

func fullLogContentBudget() map[string]string {
	logs := make(map[string]string)
	remaining := MaxLogContentBytes
	for instance := 0; remaining > 0; instance++ {
		content := min(remaining, 5<<20)
		logs[fmt.Sprintf("web/%d", instance)] = strings.Repeat("x", content)
		remaining -= content
	}
	return logs
}
