package api

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testutil"
)

type callbackOrderingSink struct {
	mu     sync.Mutex
	events []backend.LeaseStatusEvent
}

type callbackOrderingPublisher struct {
	next              CallbackPublisher
	lifecycleRequests atomic.Int32
}

func (publisher *callbackOrderingPublisher) PublishCallback(
	ctx context.Context,
	callback backend.CallbackPayload,
) error {
	if callback.LifecycleID != "" {
		publisher.lifecycleRequests.Add(1)
	}
	return publisher.next.PublishCallback(ctx, callback)
}

type callbackDeadlinePublisher struct {
	calls atomic.Int32
}

type callbackHTTPObservation struct {
	statusCode  int
	contentType string
	body        []byte
	elapsed     time.Duration
	err         error
}

type callbackObservingRoundTripper struct {
	base         http.RoundTripper
	observations chan<- callbackHTTPObservation
}

func (transport *callbackObservingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	started := time.Now()
	response, err := transport.base.RoundTrip(req)
	observation := callbackHTTPObservation{elapsed: time.Since(started), err: err}
	if response != nil {
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(body))
		observation.statusCode = response.StatusCode
		observation.contentType = response.Header.Get("Content-Type")
		observation.body = body
		observation.elapsed = time.Since(started)
		if observation.err == nil {
			observation.err = readErr
		}
	}
	transport.observations <- observation
	return response, err
}

func (publisher *callbackDeadlinePublisher) PublishCallback(
	ctx context.Context,
	_ backend.CallbackPayload,
) error {
	publisher.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func (sink *callbackOrderingSink) Publish(event backend.LeaseStatusEvent) {
	sink.mu.Lock()
	sink.events = append(sink.events, event)
	sink.mu.Unlock()
}

func (sink *callbackOrderingSink) snapshot() []backend.LeaseStatusEvent {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]backend.LeaseStatusEvent(nil), sink.events...)
}

// TestCallbackOrdering_ExactCompletionPrecedesLifecycleObservation composes
// both sides of the callback contract. The backend's durable per-lease outbox
// sends an operation-scoped success, Fred applies it synchronously through the
// real authenticated HTTP server, and only then may a typed container-death
// observation reach Fred. The server deliberately uses generic request/write
// deadlines shorter than the blocked chain acknowledgment; only the dedicated
// callback deadline keeps the exact request alive. This guards both ordering
// boundaries and the deployed timeout middleware that carries them.
func TestCallbackOrdering_ExactCompletionPrecedesLifecycleObservation(t *testing.T) {
	const (
		backendName  = "docker"
		providerUUID = "provider-1"
		secret       = "callback-ordering-secret-at-least-32-bytes"
	)
	leaseUUID := testutil.ValidUUID1

	ackReached := make(chan struct{})
	releaseAck := make(chan struct{})
	var ackReachedOnce sync.Once
	var releaseAckOnce sync.Once
	releaseAcknowledgement := func() {
		releaseAckOnce.Do(func() { close(releaseAck) })
	}
	t.Cleanup(releaseAcknowledgement)

	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{{
				Uuid:         leaseUUID,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
			}}, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			ackReachedOnce.Do(func() { close(ackReached) })
			select {
			case <-releaseAck:
				return uint64(len(leaseUUIDs)), []string{"tx-ack"}, nil
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			}
		},
	}

	mockBackend := backend.NewMockBackend(backend.MockBackendConfig{Name: backendName})
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{
		Backend:   mockBackend,
		IsDefault: true,
	}}})
	require.NoError(t, err)

	placementStore, err := placement.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, placementStore.Close()) })

	eventSink := &callbackOrderingSink{}
	manager, err := provisioner.NewManager(provisioner.ManagerConfig{
		ProviderUUID:     providerUUID,
		CallbackBaseURL:  "http://fred.invalid",
		PlacementStore:   placementStore,
		LeaseEventSink:   eventSink,
		AckBatchInterval: time.Millisecond,
	}, router, chainClient)
	require.NoError(t, err)

	managerCtx, cancelManager := context.WithCancel(context.Background())
	managerDone := make(chan error, 1)
	go func() { managerDone <- manager.Start(managerCtx) }()
	select {
	case <-manager.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("provision manager did not start")
	}
	t.Cleanup(func() {
		cancelManager()
		select {
		case <-managerDone:
		case <-time.After(5 * time.Second):
			t.Error("provision manager did not stop")
		}
		require.NoError(t, manager.Close())
	})

	tracked := manager.Operations().TryTrack(operation.TrackSpec{
		LeaseUUID:     leaseUUID,
		Tenant:        "tenant-1",
		Backend:       backendName,
		Kind:          operation.KindProvision,
		TokenRequired: true,
	})
	require.True(t, tracked.Started())
	operationID := tracked.Token().ID()
	require.NoError(t, placementStore.ConfigureBackendTopology([]string{backendName}))
	fence := placementStore.BeginInventorySession()
	_, err = placementStore.ProjectInventory(fence, placement.InventoryProjection{Complete: true})
	placementStore.EndInventorySession(fence)
	require.NoError(t, err)
	scope, err := placementStore.ScopeAdmission(
		placementStore.CurrentAdmissionBaseline(), []string{backendName},
	)
	require.NoError(t, err)
	_, begun, err := placementStore.BeginNewAttempt(
		scope, leaseUUID, backendName, operationID,
	)
	require.NoError(t, err)
	require.True(t, begun)
	manager.PublishProvisionStarting(leaseUUID)

	callbackPublisher := &callbackOrderingPublisher{next: manager}
	apiAddr := freePort(t)
	callbackServer, err := NewServer(ServerConfig{
		Addr:                       apiAddr,
		ProviderUUID:               providerUUID,
		Bech32Prefix:               "manifest",
		RateLimitRPS:               100,
		RateLimitBurst:             200,
		ReadTimeout:                time.Second,
		WriteTimeout:               25 * time.Millisecond,
		IdleTimeout:                time.Second,
		RequestTimeout:             25 * time.Millisecond,
		CallbackApplicationTimeout: 2 * time.Second,
		CallbackSecret:             secret,
	}, ServerDeps{
		ChainClient:       chainClient,
		CallbackPublisher: callbackPublisher,
		StatusChecker:     manager,
	})
	require.NoError(t, err)
	_, err = callbackServer.StartBackground()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbackServer.Shutdown(context.Background()))
	})
	callbackBaseURL := "http://" + apiAddr

	exactURL, err := provisioner.BuildCallbackURLForOperation(callbackBaseURL, operationID)
	require.NoError(t, err)
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(exactURL, "")
	require.NoError(t, err)

	callbackStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbackStore.Close()) })

	senderCtx, cancelSender := context.WithCancel(context.Background())
	t.Cleanup(cancelSender)
	zeroBackoff := [shared.CallbackMaxAttempts]time.Duration{}
	sender := shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:          callbackStore,
		HTTPClient:     &http.Client{},
		Secret:         secret,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		StopCtx:        senderCtx,
		Backoff:        &zeroBackoff,
		AttemptTimeout: 3 * time.Second,
	})

	exactDone := make(chan struct{})
	go func() {
		defer close(exactDone)
		sender.SendOperationCallback(
			leaseUUID, exactURL, backendName,
			backend.CallbackStatusSuccess, "",
		)
	}()

	select {
	case <-ackReached:
	case <-time.After(5 * time.Second):
		t.Fatal("exact callback did not reach the blocked chain acknowledgment")
	}

	lifecycleStarted := make(chan struct{})
	lifecycleDone := make(chan struct{})
	go func() {
		close(lifecycleStarted)
		defer close(lifecycleDone)
		sender.SendLifecycleCallback(
			leaseUUID, lifecycleURL, backendName,
			backend.CallbackStatusFailed, "container exited", false,
		)
	}()
	<-lifecycleStarted

	assert.Never(t, func() bool {
		return callbackPublisher.lifecycleRequests.Load() != 0
	}, 100*time.Millisecond, time.Millisecond,
		"lifecycle callback reached Fred before exact application completed")
	select {
	case <-lifecycleDone:
		t.Fatal("lifecycle delivery completed while exact application was blocked")
	default:
	}

	releaseAcknowledgement()
	select {
	case <-exactDone:
	case <-time.After(5 * time.Second):
		t.Fatal("exact callback did not complete")
	}
	select {
	case <-lifecycleDone:
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle callback did not complete after exact callback")
	}

	events := eventSink.snapshot()
	require.Len(t, events, 3)
	assert.Equal(t, backend.ProvisionStatusProvisioning, events[0].Status)
	assert.Equal(t, backend.ProvisionStatusReady, events[1].Status)
	assert.Equal(t, backend.ProvisionStatusFailed, events[2].Status)
	assert.Equal(t, "container exited", events[2].Error)
	assert.False(t, events[1].Timestamp.Before(events[0].Timestamp))
	assert.False(t, events[2].Timestamp.Before(events[1].Timestamp))
	assert.Equal(t, int32(1), callbackPublisher.lifecycleRequests.Load())

	pending, err := callbackStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

// TestCallbackOrdering_ProviderDeadlineKeepsDurableHead proves the timeout
// ladder on the deployed server path. The provider returns a retryable 503 at
// its application deadline before the sender attempt expires, so all immediate
// retries fail visibly and the callback remains the durable FIFO head.
func TestCallbackOrdering_ProviderDeadlineKeepsDurableHead(t *testing.T) {
	const secret = "callback-deadline-secret-at-least-32-bytes"
	leaseUUID := testutil.ValidUUID1
	publisher := &callbackDeadlinePublisher{}
	apiAddr := freePort(t)
	server, err := NewServer(ServerConfig{
		Addr:                       apiAddr,
		ProviderUUID:               "provider-1",
		Bech32Prefix:               "manifest",
		RateLimitRPS:               100,
		RateLimitBurst:             200,
		ReadTimeout:                time.Second,
		WriteTimeout:               10 * time.Millisecond,
		IdleTimeout:                time.Second,
		RequestTimeout:             10 * time.Millisecond,
		CallbackApplicationTimeout: 40 * time.Millisecond,
		CallbackSecret:             secret,
	}, ServerDeps{
		ChainClient:       &chaintest.MockClient{},
		CallbackPublisher: publisher,
	})
	require.NoError(t, err)
	_, err = server.StartBackground()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })

	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	zeroBackoff := [shared.CallbackMaxAttempts]time.Duration{}
	const attemptTimeout = 250 * time.Millisecond
	observations := make(chan callbackHTTPObservation, shared.CallbackMaxAttempts)
	sender := shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: &callbackObservingRoundTripper{
			base:         http.DefaultTransport,
			observations: observations,
		}},
		Secret:         secret,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		StopCtx:        t.Context(),
		Backoff:        &zeroBackoff,
		AttemptTimeout: attemptTimeout,
	})
	sender.SendLifecycleCallback(
		leaseUUID,
		provisioner.BuildCallbackURL("http://"+apiAddr),
		"docker",
		backend.CallbackStatusFailed,
		"container exited",
		false,
	)
	for attempt := 1; attempt <= shared.CallbackMaxAttempts; attempt++ {
		select {
		case observation := <-observations:
			require.NoError(t, observation.err, "callback attempt %d transport", attempt)
			assert.Equal(t, http.StatusServiceUnavailable, observation.statusCode, "callback attempt %d", attempt)
			assert.Equal(t, "application/json", observation.contentType, "callback attempt %d", attempt)
			assert.JSONEq(t,
				`{"error":"callback application timeout","code":503}`,
				string(observation.body),
				"callback attempt %d",
				attempt,
			)
			assert.Less(t, observation.elapsed, attemptTimeout,
				"Fred must serialize its retryable verdict before the sender cancels the attempt")
		case <-time.After(time.Second):
			t.Fatalf("callback attempt %d did not return an observable HTTP response", attempt)
		}
	}

	require.Eventually(t, func() bool {
		return publisher.calls.Load() == shared.CallbackMaxAttempts
	}, time.Second, time.Millisecond)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, leaseUUID, pending[0].LeaseUUID)
}
