package docker

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	store := b.callbackStore
	attestor := callbackStorageAttestorForTest(
		t, store, b.stopCtx, b.VerifyStorageIdentity,
	)
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           store,
		StorageAttestor: attestor,
		HTTPClient:      client,
		Secret:          durableCallbackTestSecret,
		Logger:          slog.Default(),

		Backoff:         &zeroBackoff,
		DeliveryTimeout: 2 * time.Second,
	})
	operations, ok := b.operationSettlement.(*shared.OperationSettlement)
	require.True(t, ok)
	maintenance, err := shared.NewMaintenanceSettlement(store, b.releaseStore)
	require.NoError(t, err)
	b.callbackPublisher = mustNewCallbackPublisherForTest(t, shared.CallbackPublisherConfig{
		OperationSettlement:   operations,
		MaintenanceSettlement: maintenance,
		StorageAttestor:       attestor,
		Logger:                slog.Default(),
	})
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	candidate, err := operations.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	admission, err := operations.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim := createdDockerOperationClaim(t, admission)
	proof := commitPreEffectOperationFailureForTest(t, operations, claim)
	require.NoError(t, b.callbackPublisher.PublishOperationFailureContext(
		context.Background(), proof, "test failure",
	))

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
