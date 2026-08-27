package docker

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

type dockerReplayRoundTripFunc func(*http.Request) (*http.Response, error)

func (f dockerReplayRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestStart_PendingCallbackReplayDoesNotWaitForDelivery(t *testing.T) {
	replayStarted := make(chan struct{})
	var replayStartedOnce sync.Once
	client := &http.Client{Transport: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		replayStartedOnce.Do(func() { close(replayStarted) })
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}

	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
		CloseFn: func() error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	b.callbackStore = store
	b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:          store,
		HTTPClient:     client,
		Logger:         slog.Default(),
		StopCtx:        b.stopCtx,
		Backoff:        &zeroBackoff,
		AttemptTimeout: 2 * time.Second,
	})
	_, err = store.StoreEntry(shared.CallbackEntry{
		LeaseUUID:    "lease-start-replay",
		CallbackURL:  "https://fred.example/callback",
		DeliveryKind: shared.CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		CreatedAt:    time.Now(),
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
		// Unblock the pre-fix synchronous replay path before failing, so the
		// regression cannot strand a goroutine or locked bbolt store.
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
