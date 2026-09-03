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
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
	"github.com/manifest-network/fred/internal/testutil"
)

type callbackOrderingSink struct {
	mu     sync.Mutex
	events []backend.LeaseStatusEvent
}

func callbackTestStorageReattestation(context.Context) error { return nil }

func beginAPICallbackOperationIntent(
	t *testing.T,
	store *shared.CallbackStore,
	leaseUUID string,
	callbackURL string,
	backendName string,
	storageID backendidentity.ID,
) shared.OperationIntentAdmission {
	t.Helper()
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	admission, err := store.BeginOperationIntent(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            leaseUUID,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleURL,
		Backend:              backendName,
		BackendStorageID:     storageID,
		Tenant:               "tenant-1",
		ProviderUUID:         placementstore.ProviderUUID,
		Items: []backend.LeaseItem{{
			SKU: "sku-1", ServiceName: "app", Quantity: 1,
		}},
		ResourceProfiles: []shared.SKUResourceSnapshot{{
			SKU: "sku-1", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
		}},
		Manifest: []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
	})
	require.NoError(t, err)
	return admission
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
// persists an operation-scoped success followed by a typed container-death
// observation. Its tracked replay loop applies both through Fred's real
// authenticated HTTP server in durable FIFO order. The server deliberately
// uses generic request/write deadlines shorter than the blocked chain
// acknowledgment; only the dedicated callback deadline keeps the exact
// request alive. This guards both ordering boundaries and the deployed timeout
// middleware that carries them.
func TestCallbackOrdering_ExactCompletionPrecedesLifecycleObservation(t *testing.T) {
	const (
		backendName  = "docker"
		providerUUID = placementstore.ProviderUUID
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

	placementStore, err := placementstore.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, placementStore.Close()) })
	configureAPIPlacementTopology(t, placementStore, []string{backendName})
	fence := placementStore.BeginInventorySession()
	_, err = placementStore.ProjectInventory(fence, placement.InventoryProjection{
		Complete:                 true,
		BackendStorageIdentities: testAPIBackendStorageIDs(backendName),
		EmptyBackends:            []string{backendName},
	})
	placementStore.EndInventorySession(fence)
	require.NoError(t, err)

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

	claimResult := manager.MaintenanceClaims().TryClaimLeaseNow(leaseUUID)
	require.True(t, claimResult.Acquired())
	claim := claimResult.Claim()
	tracked := manager.RestoreOperations().TryInitiateClaimed(claim, operation.TrackSpec{
		LeaseUUID: leaseUUID,
		Tenant:    "tenant-1",
		Backend:   backendName,
		Kind:      operation.KindProvision,
	})
	require.True(t, tracked.Started())
	initiation := tracked.Capability()
	require.True(t, manager.RestoreOperations().BeginCall(initiation))
	require.Equal(t, operation.InitiationActivated, manager.RestoreOperations().Activate(initiation))
	require.True(t, manager.MaintenanceClaims().ReleaseLease(claim))
	operationID := initiation.ID()
	scope, err := placementStore.ScopeAdmission(
		placementStore.CurrentAdmissionBaseline(), []string{backendName},
	)
	require.NoError(t, err)
	callbackPair, err := placement.NewCallbackPair(
		operationID,
		"https://provider.test/callbacks/provision?operation_id="+operationID.String(),
		"https://provider.test/callbacks/provision?lifecycle_id="+operationID.String(),
	)
	require.NoError(t, err)
	_, begun, err := placementStore.BeginNewAttempt(
		scope, leaseUUID, backendName, operationID,
		placement.PayloadFingerprint{}, testAPIBackendRequestSnapshot(t), callbackPair,
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
	sender := shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           callbackStore,
		HTTPClient:      &http.Client{},
		Secret:          secret,
		BeforeDelivery:  callbackTestStorageReattestation,
		BeforeReplay:    callbackTestStorageReattestation,
		StorageIdentity: testAPIBackendStorageID(backendName),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		StopCtx:         senderCtx,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: 3 * time.Second,
	})
	beginAPICallbackOperationIntent(
		t, callbackStore, leaseUUID, exactURL, backendName,
		testAPIBackendStorageID(backendName),
	)

	// Durable Send methods only persist and notify. Queue both callbacks before
	// starting the tracked delivery owner so this test observes the outbox FIFO,
	// not goroutine scheduling or a Send return.
	sender.SendOperationCallback(
		leaseUUID, exactURL, backendName,
		backend.CallbackStatusSuccess, "",
	)
	sender.SendLifecycleCallback(
		leaseUUID, lifecycleURL, backendName,
		backend.CallbackStatusFailed, "container exited", false,
	)
	pending, err := callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)

	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		sender.RunReplayLoop()
	}()
	var stopReplayOnce sync.Once
	stopReplay := func() {
		stopReplayOnce.Do(func() {
			cancelSender()
			select {
			case <-replayDone:
			case <-time.After(5 * time.Second):
				t.Error("callback replay loop did not stop")
			}
		})
	}
	t.Cleanup(stopReplay)

	select {
	case <-ackReached:
	case <-time.After(5 * time.Second):
		t.Fatal("exact callback did not reach the blocked chain acknowledgment")
	}

	assert.Never(t, func() bool {
		return callbackPublisher.lifecycleRequests.Load() != 0
	}, 100*time.Millisecond, time.Millisecond,
		"lifecycle callback reached Fred before exact application completed")

	releaseAcknowledgement()
	require.Eventually(t, func() bool {
		return callbackPublisher.lifecycleRequests.Load() == 1 &&
			len(eventSink.snapshot()) == 3
	}, 5*time.Second, time.Millisecond,
		"lifecycle callback did not reach Fred after exact callback application")
	require.Eventually(t, func() bool {
		pending, listErr := callbackStore.ListPending()
		return listErr == nil && len(pending) == 0
	}, 5*time.Second, time.Millisecond,
		"replay loop did not drain the durable callback FIFO")
	stopReplay()

	events := eventSink.snapshot()
	require.Len(t, events, 3)
	assert.Equal(t, backend.ProvisionStatusProvisioning, events[0].Status)
	assert.Equal(t, backend.ProvisionStatusReady, events[1].Status)
	assert.Equal(t, backend.ProvisionStatusFailed, events[2].Status)
	assert.Equal(t, "container exited", events[2].Error)
	assert.False(t, events[1].Timestamp.Before(events[0].Timestamp))
	assert.False(t, events[2].Timestamp.Before(events[1].Timestamp))
	assert.Equal(t, int32(1), callbackPublisher.lifecycleRequests.Load())

	pending, err = callbackStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

// TestCallbackOutboxReplay_SettlesDurableAttemptAcrossBothProcessRestarts
// covers the complete write-ahead recovery window that static store tests
// cannot: Fred has persisted an Attempt but lost its volatile operation
// Registry, the backend has persisted the matching completion but exits before
// delivery, and both bbolt files are reopened by replacement processes. The
// backend replay must cross Fred's production storage-identity keyring and
// receive a synchronous 2xx only after the exact durable attempt and chain state
// are settled, then remove exactly that outbox entry. A second backend proves
// that signing its identity-bound row with the first backend's key returns 401
// and preserves both sides of the unresolved generation.
func TestCallbackOutboxReplay_SettlesDurableAttemptAcrossBothProcessRestarts(t *testing.T) {
	const (
		backendA     = "docker-a"
		backendB     = "docker-b"
		providerUUID = placementstore.ProviderUUID
		secretA      = "callback-restart-A-secret-at-least-32-bytes"
		secretB      = "callback-restart-B-secret-at-least-32-bytes"
	)
	leaseA := testutil.ValidUUID1
	leaseB := testutil.ValidUUID2
	operationA, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174090")
	require.NoError(t, err)
	operationB, err := operation.ParseID("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	require.NoError(t, err)
	placementPath := filepath.Join(t.TempDir(), "placements.db")
	outboxAPath := filepath.Join(t.TempDir(), "callbacks-a.db")
	outboxBPath := filepath.Join(t.TempDir(), "callbacks-b.db")
	apiAddr := freePort(t)
	exactURLA, err := provisioner.BuildCallbackURLForOperation("http://"+apiAddr, operationA)
	require.NoError(t, err)
	exactURLB, err := provisioner.BuildCallbackURLForOperation("http://"+apiAddr, operationB)
	require.NoError(t, err)

	// Fred #1 has made the write-ahead placement mutation. Its process-local
	// Registry is intentionally absent from the durable state we reopen below.
	placementStore1, err := placementstore.NewStore(placementPath)
	require.NoError(t, err)
	backendNames := []string{backendA, backendB}
	configureAPIPlacementTopology(t, placementStore1, backendNames)
	fence := placementStore1.BeginInventorySession()
	_, err = placementStore1.ProjectInventory(fence, placement.InventoryProjection{
		Complete:                 true,
		BackendStorageIdentities: testAPIBackendStorageIDs(backendNames...),
		EmptyBackends:            backendNames,
	})
	placementStore1.EndInventorySession(fence)
	require.NoError(t, err)
	scopeA, err := placementStore1.ScopeAdmission(
		placementStore1.CurrentAdmissionBaseline(), []string{backendA},
	)
	require.NoError(t, err)
	_, begun, err := placementStore1.BeginNewAttempt(
		scopeA, leaseA, backendA, operationA,
		placement.PayloadFingerprint{}, testAPIBackendRequestSnapshot(t),
		testAPICallbackPair(t, operationA),
	)
	require.NoError(t, err)
	require.True(t, begun)
	scopeB, err := placementStore1.ScopeAdmission(
		placementStore1.CurrentAdmissionBaseline(), []string{backendB},
	)
	require.NoError(t, err)
	_, begun, err = placementStore1.BeginNewAttempt(
		scopeB, leaseB, backendB, operationB,
		placement.PayloadFingerprint{}, testAPIBackendRequestSnapshot(t),
		testAPICallbackPair(t, operationB),
	)
	require.NoError(t, err)
	require.True(t, begun)
	require.NoError(t, placementStore1.Close())

	// Each backend process durably records its exact failure, then exits without
	// an HTTP attempt. These are the backend halves of two ambiguous transport
	// windows; only A will authenticate successfully after restart.
	outboxA1, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: outboxAPath})
	require.NoError(t, err)
	intentA := beginAPICallbackOperationIntent(
		t, outboxA1, leaseA, exactURLA, backendA, testAPIBackendStorageID(backendA),
	)
	_, err = outboxA1.ResolveOperationIntent(
		intentA.Claim, backend.CallbackStatusFailed, "remote provision failed after dispatch",
	)
	require.NoError(t, err)
	require.NoError(t, outboxA1.Close())
	outboxB1, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: outboxBPath})
	require.NoError(t, err)
	intentB := beginAPICallbackOperationIntent(
		t, outboxB1, leaseB, exactURLB, backendB, testAPIBackendStorageID(backendB),
	)
	_, err = outboxB1.ResolveOperationIntent(
		intentB.Claim, backend.CallbackStatusFailed, "remote provision failed after dispatch",
	)
	require.NoError(t, err)
	require.NoError(t, outboxB1.Close())

	placementStore2, err := placementstore.NewStore(placementPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, placementStore2.Close()) })
	require.Equal(t, operationA, placementStore2.Lookup(leaseA).AttemptOperationID())
	require.Equal(t, operationB, placementStore2.Lookup(leaseB).AttemptOperationID())

	var rejectCalls atomic.Int32
	chainClient := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		GetLeaseFunc: func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, ProviderUuid: providerUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		},
		RejectLeasesFunc: func(context.Context, []string, string) (uint64, []string, error) {
			rejectCalls.Add(1)
			return 1, []string{"tx-reject"}, nil
		},
	}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: backend.NewMockBackend(backend.MockBackendConfig{Name: backendA}), IsDefault: true},
		{Backend: backend.NewMockBackend(backend.MockBackendConfig{Name: backendB})},
	}})
	require.NoError(t, err)
	manager, err := provisioner.NewManager(provisioner.ManagerConfig{
		ProviderUUID:    providerUUID,
		CallbackBaseURL: "http://fred.invalid",
		PlacementStore:  placementStore2,
	}, router, chainClient)
	require.NoError(t, err)
	managerCtx, cancelManager := context.WithCancel(context.Background())
	managerDone := make(chan error, 1)
	go func() { managerDone <- manager.Start(managerCtx) }()
	select {
	case <-manager.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("replacement provision manager did not start")
	}
	t.Cleanup(func() {
		cancelManager()
		select {
		case <-managerDone:
		case <-time.After(5 * time.Second):
			t.Error("replacement provision manager did not stop")
		}
		require.NoError(t, manager.Close())
	})
	assert.Zero(t, manager.InFlightCount(), "replacement Registry must start empty")

	callbackServer, err := NewServer(ServerConfig{
		Addr: apiAddr, ProviderUUID: providerUUID, Bech32Prefix: "manifest",
		RateLimitRPS: 100, RateLimitBurst: 200,
		ReadTimeout: time.Second, WriteTimeout: time.Second, IdleTimeout: time.Second,
		RequestTimeout: time.Second, CallbackApplicationTimeout: 2 * time.Second,
		CallbackHMACSecrets: map[backendidentity.ID]string{
			testAPIBackendStorageID(backendA): secretA,
			testAPIBackendStorageID(backendB): secretB,
		},
	}, ServerDeps{
		ChainClient: chainClient, CallbackPublisher: manager, StatusChecker: manager,
	})
	require.NoError(t, err)
	_, err = callbackServer.StartBackground()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbackServer.Shutdown(context.Background())) })

	// Backend B first reopens its outbox with A's key. The HMAC-covered B identity
	// selects B's keyring slot, so A's key must receive 401, leave the row queued,
	// and leave B's durable attempt untouched.
	outboxB2, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: outboxBPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, outboxB2.Close()) })
	zeroBackoff := [shared.CallbackMaxAttempts]time.Duration{}
	wrongKeyResults := make(chan callbackHTTPObservation, shared.CallbackMaxAttempts)
	wrongKeySender := shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store: outboxB2,
		HTTPClient: &http.Client{Transport: &callbackObservingRoundTripper{
			base: http.DefaultTransport, observations: wrongKeyResults,
		}},
		Secret:          secretA,
		BeforeDelivery:  callbackTestStorageReattestation,
		BeforeReplay:    callbackTestStorageReattestation,
		StorageIdentity: testAPIBackendStorageID(backendB),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		StopCtx:         t.Context(), Backoff: &zeroBackoff, DeliveryTimeout: 3 * time.Second,
	})
	wrongKeySender.ReplayPendingCallbacks()
	for range shared.CallbackMaxAttempts {
		result := <-wrongKeyResults
		require.NoError(t, result.err)
		assert.Equal(t, http.StatusUnauthorized, result.statusCode)
	}
	pendingB, err := outboxB2.ListPending()
	require.NoError(t, err)
	require.Len(t, pendingB, 1, "cross-backend authentication failure must preserve the durable evidence")
	assert.Equal(t, operationB, placementStore2.Lookup(leaseB).AttemptOperationID())
	assert.Zero(t, rejectCalls.Load(), "A's key must not settle B's operation")

	// Backend A now reopens its durable outbox and replays through Fred's real
	// production keyring into the fresh Registry plus reopened placement store.
	outboxA2, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: outboxAPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, outboxA2.Close()) })
	senderA := shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store: outboxA2, HTTPClient: &http.Client{}, Secret: secretA,
		BeforeDelivery:  callbackTestStorageReattestation,
		BeforeReplay:    callbackTestStorageReattestation,
		StorageIdentity: testAPIBackendStorageID(backendA),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		StopCtx:         t.Context(), Backoff: &zeroBackoff, DeliveryTimeout: 3 * time.Second,
	})
	senderA.ReplayPendingCallbacks()

	pending, err := outboxA2.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "2xx application must remove only the replayed exact completion")
	settled := placementStore2.Lookup(leaseA)
	assert.Equal(t, placement.StateAbsent, settled.State())
	assert.Empty(t, settled.Attempt)
	assert.False(t, settled.AttemptOperationID().Valid())
	assert.Equal(t, int32(1), rejectCalls.Load())
	assert.Equal(t, operationB, placementStore2.Lookup(leaseB).AttemptOperationID(),
		"settling A must not consume B's cross-key-rejected generation")
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
	const deliveryTimeout = 250 * time.Millisecond
	observations := make(chan callbackHTTPObservation, shared.CallbackMaxAttempts)
	sender := shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: &callbackObservingRoundTripper{
			base:         http.DefaultTransport,
			observations: observations,
		}},
		Secret:          secret,
		BeforeDelivery:  callbackTestStorageReattestation,
		BeforeReplay:    callbackTestStorageReattestation,
		StorageIdentity: testAPIBackendStorageID("docker"),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		StopCtx:         t.Context(),
		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	})
	callbackURL, err := provisioner.BuildCallbackURL("http://" + apiAddr)
	require.NoError(t, err)
	sender.SendLifecycleCallback(
		leaseUUID,
		callbackURL,
		"docker",
		backend.CallbackStatusFailed,
		"container exited",
		false,
	)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "durable Send must return after persistence, before HTTP delivery")
	sender.ReplayPendingCallbacks()
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
			assert.Less(t, observation.elapsed, deliveryTimeout,
				"Fred must serialize its retryable verdict before the sender cancels the attempt")
		case <-time.After(time.Second):
			t.Fatalf("callback attempt %d did not return an observable HTTP response", attempt)
		}
	}

	require.Eventually(t, func() bool {
		return publisher.calls.Load() == shared.CallbackMaxAttempts
	}, time.Second, time.Millisecond)
	pending, err = store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, leaseUUID, pending[0].LeaseUUID)
}
