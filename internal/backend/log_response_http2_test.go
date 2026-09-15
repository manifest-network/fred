package backend

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogsHandlerHTTP2ExtendsDeadlineBeforeRetrieval(t *testing.T) {
	for _, tc := range []struct {
		name string
		new  func(http.Handler, time.Duration, time.Duration) http.Handler
	}{
		{name: "backend", new: NewLogsHandler},
		{name: "tenant", new: NewTenantLogsHandler},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const serverWriteTimeout = 200 * time.Millisecond
			entered := make(chan int, 1)
			canceled := make(chan struct{})
			finish := make(chan struct{})
			release := sync.OnceFunc(func() { close(finish) })
			h := tc.new(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				entered <- r.ProtoMajor
				select {
				case <-finish:
					_, _ = io.WriteString(w, `{"web/0":"retrieval completed"}`)
				case <-r.Context().Done():
					close(canceled)
				}
			}), 3*time.Second, time.Second)
			server := httptest.NewUnstartedServer(h)
			server.EnableHTTP2 = true
			server.Config.WriteTimeout = serverWriteTimeout
			server.StartTLS()
			t.Cleanup(server.Close)
			t.Cleanup(release)
			client := server.Client()
			client.Timeout = 5 * time.Second

			type response struct {
				status int
				proto  int
				body   string
				err    error
			}
			result := make(chan response, 1)
			go func() {
				r, err := client.Get(server.URL + "/logs/lease")
				if err != nil {
					result <- response{err: err}
					return
				}
				defer r.Body.Close()
				body, err := io.ReadAll(r.Body)
				result <- response{status: r.StatusCode, proto: r.ProtoMajor, body: string(body), err: err}
			}()
			select {
			case proto := <-entered:
				require.Equal(t, 2, proto, "the regression requires the HTTP/2 stream deadline")
			case got := <-result:
				t.Fatalf("request ended before retrieval: %v", got.err)
			case <-time.After(5 * time.Second):
				t.Fatal("retrieval did not start")
			}

			// A real HTTP/2 stream irreversibly resets when the server's old
			// deadline expires. Setting a new deadline only at WriteHeader is
			// too late: retrieval must outlive that deadline before we release it.
			select {
			case <-canceled:
				t.Fatal("server write deadline canceled retrieval before its application deadline")
			case got := <-result:
				t.Fatalf("response ended before retrieval was released: %v", got.err)
			case <-time.After(3 * serverWriteTimeout):
			}
			release()
			select {
			case got := <-result:
				require.NoError(t, got.err)
				require.Equal(t, http.StatusOK, got.status)
				require.Equal(t, 2, got.proto)
				require.JSONEq(t, `{"web/0":"retrieval completed"}`, got.body)
			case <-time.After(5 * time.Second):
				t.Fatal("response did not finish after retrieval was released")
			}
		})
	}
}
