package backend

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/healthprobe"
	"github.com/manifest-network/fred/internal/uuidv4"
)

func TestHTTPHealthProbeIDSurvivesTransportCancellation(t *testing.T) {
	ids := make(chan string, 1)
	canceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		ids <- r.Header.Get(healthprobe.Header)
		<-r.Context().Done()
		close(canceled)
	}))
	t.Cleanup(server.Close)
	client := newUnboundHTTPClientForTest(HTTPClientConfig{
		Name: "health-correlation", BaseURL: server.URL, Timeout: time.Second,
	})
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() { result <- client.Health(ctx) }()
	var id string
	select {
	case id = <-ids:
	case err := <-result:
		t.Fatalf("health probe returned before reaching the server: %v", err)
	}
	_, err := uuidv4.Parse(id, errors.New("invalid probe ID"))
	require.NoError(t, err)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	<-canceled
}
