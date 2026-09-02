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
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "test-daemon"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
		CloseFn: func() error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err)
	b.cfg.CallbackDBPath = dbPath
	for name, profile := range b.cfg.SKUProfiles {
		profile.DiskMB = 0
		b.cfg.SKUProfiles[name] = profile
	}
	id, err := initializeTestMarkerPair(
		dbPath+".storage-identity.json",
		dbPath+".storage-identity-anchor.json",
		b.cfg.Name,
		"test-daemon",
	)
	require.NoError(t, err)
	b.storageIdentity = id
	b.callbackStore = store
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           store,
		HTTPClient:      client,
		Secret:          durableCallbackTestSecret,
		StorageIdentity: id,
		BeforeDelivery:  b.VerifyStorageIdentity,
		BeforeReplay:    b.VerifyStorageIdentity,
		Logger:          slog.Default(),
		StopCtx:         b.stopCtx,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: 2 * time.Second,
	})
	_, err = store.StoreEntry(shared.CallbackEntry{
		LeaseUUID:        "550e8400-e29b-41d4-a716-446655440000",
		CallbackURL:      "https://fred.example/callbacks/provision?lifecycle_id=550e8400-e29b-41d4-a716-446655440000",
		DeliveryKind:     shared.CallbackDeliveryKindLifecycle,
		Success:          false,
		Status:           backend.CallbackStatusFailed,
		BackendStorageID: id.String(),
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
