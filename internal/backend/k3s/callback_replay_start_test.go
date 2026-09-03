package k3s

import (
	"context"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

type k3sReplayRoundTripFunc func(*http.Request) (*http.Response, error)

func (f k3sReplayRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestBackend_Start_PendingCallbackReplayDoesNotWaitForDelivery(t *testing.T) {
	replayStarted := make(chan struct{})
	var replayStartedOnce sync.Once
	client := &http.Client{Transport: k3sReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		replayStartedOnce.Do(func() { close(replayStarted) })
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}

	cfg := validConfig()
	dir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(dir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(dir, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	b, err := newBackendWithTestIdentity(cfg, slog.Default())
	require.NoError(t, err)
	bindK3sTestStorageIdentity(t, b)
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           b.callbackStore,
		HTTPClient:      client,
		Secret:          string(cfg.CallbackSecret),
		StorageIdentity: b.storageIdentity,
		BeforeDelivery:  b.VerifyStorageIdentity,
		BeforeReplay:    b.VerifyStorageIdentity,
		Logger:          slog.Default(),
		StopCtx:         b.stopCtx,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: 2 * time.Second,
	})
	_, err = b.callbackStore.StoreEntry(shared.CallbackEntry{
		LeaseUUID:        "550e8400-e29b-41d4-a716-446655440000",
		CallbackURL:      "https://fred.example/callbacks/provision?lifecycle_id=550e8400-e29b-41d4-a716-446655440000",
		DeliveryKind:     shared.CallbackDeliveryKindLifecycle,
		Success:          false,
		Status:           backend.CallbackStatusFailed,
		BackendStorageID: b.storageIdentity.String(),
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)

	var stopOnce sync.Once
	var stopErr error
	stop := func() error {
		stopOnce.Do(func() { stopErr = b.Stop() })
		return stopErr
	}
	t.Cleanup(func() { _ = stop() })

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelStartup()
	startDone := make(chan error, 1)
	go func() { startDone <- b.Start(startupCtx) }()
	select {
	case startErr := <-startDone:
		require.NoError(t, startErr)
	case <-startupCtx.Done():
		b.stopCancel()
		<-startDone
		t.Fatal("Start waited for callback replay past its caller deadline")
	}

	select {
	case <-replayStarted:
	case <-time.After(time.Second):
		t.Fatal("tracked initial callback replay did not start")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- stop() }()
	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel and join the blocked initial callback replay")
	}
}
