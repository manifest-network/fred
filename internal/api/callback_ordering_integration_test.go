package api

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/provisioner/callbackwire"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
	"github.com/manifest-network/fred/internal/testutil"
)

type callbackOrderingSink struct {
	mu     sync.Mutex
	events []backend.LeaseStatusEvent
}

type apiCallbackStorageVerifier struct {
	storageID backendidentity.ID
	gate      *backendidentity.StorageAuthorityGate
}

type apiCallbackOperationMutation struct {
	run func(context.Context) error
}

type apiAmbiguousProvisionBackend struct{ backend.Backend }

func (apiAmbiguousProvisionBackend) Provision(
	context.Context,
	backend.ProvisionRequest,
) error {
	return errors.New("ambiguous provision transport outcome")
}

func bindAPICallbackOperationExecutor(
	t *testing.T,
	settlement *shared.OperationSettlement,
) {
	t.Helper()
	err := shared.BindOperationSubstrateExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(context.Context, string, error) error { return nil },
		func(
			runner substratemutation.Runner,
			_ shared.OperationPhysicalSubject,
		) apiCallbackOperationMutation {
			return apiCallbackOperationMutation{run: func(ctx context.Context) error {
				return runner.Step(ctx, "API callback operation", func(context.Context) error {
					return nil
				})
			}}
		},
		func(
			ctx context.Context,
			mutation apiCallbackOperationMutation,
			_ shared.OperationPhysicalSubject,
		) error {
			return mutation.run(ctx)
		},
		func(
			_ context.Context,
			subject shared.OperationPhysicalSubject,
		) (shared.OperationPhysicalEvidence, error) {
			return shared.NewOperationTargetReady(
				subject,
				[]string{"app-0"},
				map[string][]string{"app": {"app-0"}},
			)
		},
	)
	require.NoError(t, err)
}

func (v apiCallbackStorageVerifier) StorageIdentity() backendidentity.ID { return v.storageID }

func (v apiCallbackStorageVerifier) StorageAuthorityGate() *backendidentity.StorageAuthorityGate {
	return v.gate
}

func (apiCallbackStorageVerifier) Verify(context.Context) error { return nil }

func newAPICallbackStorageAttestor(
	t *testing.T,
	journals apiCallbackJournals,
	stopCtx context.Context,
) *shared.CallbackStorageAttestor {
	t.Helper()
	attestor, err := shared.NewCallbackStorageAttestor(
		journals.callbacks,
		apiCallbackStorageVerifier{storageID: journals.storageID, gate: journals.gate},
		stopCtx,
	)
	require.NoError(t, err)
	return attestor
}

func newAPICallbackPublisher(
	t *testing.T,
	journals apiCallbackJournals,
	secret string,
	stopCtx context.Context,
	httpClient *http.Client,
) (*shared.CallbackSender, *shared.CallbackPublisher) {
	t.Helper()
	zeroBackoff := [shared.CallbackMaxAttempts]time.Duration{}
	attestor := newAPICallbackStorageAttestor(t, journals, stopCtx)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sender := shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           journals.callbacks,
		StorageAttestor: attestor,
		HTTPClient:      httpClient,
		Secret:          secret,
		Logger:          logger,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: 3 * time.Second,
	})
	publisher := mustNewCallbackPublisherForTest(t, shared.CallbackPublisherConfig{
		OperationSettlement:   journals.operations,
		MaintenanceSettlement: journals.maintenance,
		StorageAttestor:       attestor,
		Logger:                logger,
	})
	return sender, publisher
}

func mustNewCallbackPublisherForTest(
	t *testing.T,
	cfg shared.CallbackPublisherConfig,
) *shared.CallbackPublisher {
	t.Helper()
	publisher, err := shared.NewCallbackPublisher(cfg)
	require.NoError(t, err)
	return publisher
}

func beginAPICallbackOperationIntent(
	t *testing.T,
	settlement *shared.OperationSettlement,
	leaseUUID string,
	callbackURL string,
) shared.OperationIntentAdmission {
	t.Helper()
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	candidate, err := settlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            leaseUUID,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleURL,
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
	admission, err := settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	return admission
}

func commitAPICallbackOperationRefusal(
	t *testing.T,
	settlement *shared.OperationSettlement,
	claim shared.OperationIntentClaim,
) shared.OperationReleaseUncommitted {
	t.Helper()
	candidate, err := settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	failure, err := settlement.RefuseOperationExecution(candidate)
	require.NoError(t, err)
	uncommitted, err := settlement.CommitOperationFailure(failure)
	require.NoError(t, err)
	return uncommitted
}

type apiCallbackJournals struct {
	callbacks   *shared.CallbackStore
	releases    *shared.ReleaseStore
	operations  *shared.OperationSettlement
	maintenance *shared.MaintenanceSettlement
	storageID   backendidentity.ID
	gate        *backendidentity.StorageAuthorityGate
}

func (journals apiCallbackJournals) Close() error {
	return errors.Join(journals.callbacks.Close(), journals.releases.Close())
}

func openBoundAPICallbackStore(
	t *testing.T,
	dbPath string,
	backendName string,
) apiCallbackJournals {
	t.Helper()
	boundPath, err := shared.BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, boundPath.Close()) }()
	releasePath := dbPath + ".releases.db"
	boundReleases, err := shared.BindAuthoritativeStorePath(releasePath)
	require.NoError(t, err)
	defer func() { require.NoError(t, boundReleases.Close()) }()
	pair, err := backendidentity.BindMarkerPair(
		dbPath+".storage-identity.json",
		dbPath+".storage-identity-anchor.json",
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pair.Close()) }()
	storage, err := pair.InitializeWithStores(
		backendName,
		"api-callback-ordering-"+backendName,
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(
				pending backendidentity.PendingStorage,
				profile backendidentity.InitializationProfile,
			) error {
				if err := shared.PrepareBoundCallbackStoreStorage(boundPath, pending, profile); err != nil {
					return err
				}
				return shared.PrepareBoundReleaseStoreStorage(boundReleases, pending, profile)
			},
			Check: func(pending backendidentity.PendingStorage) error {
				if err := shared.CheckBoundCallbackStoreStorage(boundPath, pending); err != nil {
					return err
				}
				return shared.CheckBoundReleaseStoreStorage(boundReleases, pending)
			},
			Verify: func(verified backendidentity.VerifiedStorage) error {
				if err := shared.VerifyBoundCallbackStoreStorage(boundPath, verified); err != nil {
					return err
				}
				return shared.VerifyBoundReleaseStoreStorage(boundReleases, verified)
			},
		},
	)
	require.NoError(t, err)
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	store, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: dbPath}, storage, gate,
	)
	require.NoError(t, err)
	releases, err := shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: releasePath}, storage, gate,
	)
	require.NoError(t, err)
	operations, err := shared.NewOperationSettlement(store, releases)
	require.NoError(t, err)
	maintenance, err := shared.NewMaintenanceSettlement(store, releases)
	require.NoError(t, err)
	return apiCallbackJournals{
		callbacks: store, releases: releases, operations: operations,
		maintenance: maintenance, storageID: storage.ID(), gate: gate,
	}
}

type callbackOrderingPublisher struct {
	next              CallbackPublisher
	lifecycleRequests atomic.Int32
}

func (publisher *callbackOrderingPublisher) PublishCallback(
	ctx context.Context,
	proof hmacauth.VerifiedRequest,
) error {
	callback, _ := callbackwire.DecodeVerified(proof)
	if callback.Selector() == callbackwire.SelectorLifecycle {
		publisher.lifecycleRequests.Add(1)
	}
	return publisher.next.PublishCallback(ctx, proof)
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
	_ hmacauth.VerifiedRequest,
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
	proofVerifier, proofConsumer := hmacauth.NewCallbackProofBoundary()
	leaseUUID := testutil.ValidUUID1
	callbackJournals := openBoundAPICallbackStore(
		t, filepath.Join(t.TempDir(), "callbacks.db"), backendName,
	)
	callbackStore := callbackJournals.callbacks
	callbackStorageID := callbackJournals.storageID
	t.Cleanup(func() { require.NoError(t, callbackJournals.Close()) })

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
				Tenant:       "tenant-1",
				State:        billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{
					SkuUuid: "sku-1", ServiceName: "app", Quantity: 1,
				}},
			}}, nil
		},
		GetLeaseFunc: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, ProviderUuid: providerUUID, Tenant: "tenant-1",
				State: billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{
					SkuUuid: "sku-1", ServiceName: "app", Quantity: 1,
				}},
			}, nil
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

	mockBackend := apiAmbiguousProvisionBackend{Backend: backend.NewMockBackend(
		backend.MockBackendConfig{Name: backendName},
	)}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{
		Backend:   mockBackend,
		IsDefault: true,
	}}})
	require.NoError(t, err)

	apiAddr := freePort(t)
	callbackRoutes, err := placement.NewCallbackRouteFactory("http://" + apiAddr)
	require.NoError(t, err)
	placementPath := filepath.Join(t.TempDir(), "placements.db")
	placementStore, err := placementstore.NewStore(
		placementPath, placement.WithCallbackRouteFactory(callbackRoutes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, placementStore.Close()) })
	require.NoError(t, placementstore.ConfigureBackendTopologyWithStorageIdentities(placementStore,
		[]string{backendName}, map[string]backendidentity.ID{backendName: callbackStorageID},
	))
	seedCoordinator, err := placementStore.BindOperationCoordinator(nil)
	require.NoError(t, err)
	seedExecution, seedInventoryRuntime := bindAPIInventoryRuntime(
		t, seedCoordinator, router, chainClient,
	)
	seedReconciliation, err := seedExecution.ReconciliationCoordinator(nil, nil)
	require.NoError(t, err)
	registerAPIReconciliationInventory(t, seedReconciliation, seedInventoryRuntime)
	projectAPIPlacementInventory(t, seedReconciliation, []string{backendName},
		map[string]backendidentity.ID{backendName: callbackStorageID},
		placement.ReconciliationProjection{})
	seedProvision, err := seedExecution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	request, err := placement.NewProvisionEventRequest(leaseUUID, "tenant-1")
	require.NoError(t, err)
	result := seedProvision.ExecuteCurrentLease(context.Background(), request)
	require.Equal(t, placement.ProvisionEventUncertain, result.Disposition())
	require.Error(t, result.Err())
	operationID := placementStore.Lookup(leaseUUID).AttemptOperationID()
	require.True(t, operationID.Valid())
	require.NoError(t, placementStore.Close())
	placementStore, err = placementstore.NewStore(
		placementPath, placement.WithCallbackRouteFactory(callbackRoutes),
	)
	require.NoError(t, err)

	eventSink := &callbackOrderingSink{}
	manager, err := provisioner.NewManager(provisioner.ManagerConfig{
		ProviderUUID:          providerUUID,
		PlacementStore:        placementStore,
		LeaseEventSink:        eventSink,
		AckBatchInterval:      time.Millisecond,
		CallbackProofConsumer: proofConsumer,
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

	callbackPublisher := &callbackOrderingPublisher{next: manager}
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
		ChainClient:           chainClient,
		CallbackPublisher:     callbackPublisher,
		StatusChecker:         manager,
		CallbackProofVerifier: proofVerifier,
	})
	require.NoError(t, err)
	_, err = callbackServer.StartBackground()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbackServer.Shutdown(context.Background()))
	})
	callbackPair, err := callbackRoutes.ForOperation(operationID)
	require.NoError(t, err)
	exactURL := callbackPair.OperationURL()

	senderCtx, cancelSender := context.WithCancel(context.Background())
	t.Cleanup(cancelSender)
	sender, durablePublisher := newAPICallbackPublisher(
		t, callbackJournals, secret, senderCtx, &http.Client{},
	)
	bindAPICallbackOperationExecutor(t, callbackJournals.operations)
	admission := beginAPICallbackOperationIntent(
		t, callbackJournals.operations, leaseUUID, exactURL,
	)
	operationClaim, created := admission.CreatedClaim()
	require.True(t, created)
	releaseCandidate, err := callbackJournals.operations.PrepareOperationRelease(operationClaim)
	require.NoError(t, err)
	execution, err := callbackJournals.operations.StartOperationExecution(releaseCandidate)
	require.NoError(t, err)
	outcome := callbackJournals.operations.ExecuteOperation(t.Context(), execution)
	success, ok := outcome.(shared.OperationExecutionSuccess)
	require.True(t, ok)
	committed, err := callbackJournals.operations.CommitOperationSuccess(success)
	require.NoError(t, err)

	// Durable Send methods only persist and notify. Queue both callbacks before
	// starting the tracked delivery owner so this test observes the outbox FIFO,
	// not goroutine scheduling or a Send return.
	require.NoError(t, durablePublisher.PublishOperationSuccessContext(context.Background(), committed))
	runtimeProof, err := callbackJournals.releases.ProveRuntimeGeneration(leaseUUID)
	require.NoError(t, err)
	runtimePermit, err := durablePublisher.AuthorizeRuntimeObservationContext(
		context.Background(), runtimeProof,
	)
	require.NoError(t, err)
	require.NoError(t, durablePublisher.PublishLifecycleFailureContext(
		context.Background(), runtimePermit, "container exited",
	))
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
		return callbackPublisher.lifecycleRequests.Load() == 1
	}, 5*time.Second, time.Millisecond,
		"lifecycle callback did not reach Fred after exact callback application")
	require.Eventually(t, func() bool {
		pending, listErr := callbackStore.ListPending()
		return listErr == nil && len(pending) == 0
	}, 5*time.Second, time.Millisecond,
		"replay loop did not finish applying the lifecycle callback")
	require.Eventually(t, func() bool {
		return len(eventSink.snapshot()) == 2
	}, 5*time.Second, time.Millisecond,
		"lifecycle callback reached Fred but did not publish its lease event")
	require.Eventually(t, func() bool {
		pending, listErr := callbackStore.ListPending()
		return listErr == nil && len(pending) == 0
	}, 5*time.Second, time.Millisecond,
		"replay loop did not drain the durable callback FIFO")
	stopReplay()

	events := eventSink.snapshot()
	require.Len(t, events, 2)
	assert.Equal(t, backend.ProvisionStatusReady, events[0].Status)
	assert.Equal(t, backend.ProvisionStatusFailed, events[1].Status)
	assert.Equal(t, "container exited", events[1].Error)
	assert.False(t, events[1].Timestamp.Before(events[0].Timestamp))
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
	proofVerifier, proofConsumer := hmacauth.NewCallbackProofBoundary()
	leaseA := testutil.ValidUUID1
	leaseB := testutil.ValidUUID2
	placementPath := filepath.Join(t.TempDir(), "placements.db")
	outboxAPath := filepath.Join(t.TempDir(), "callbacks-a.db")
	outboxBPath := filepath.Join(t.TempDir(), "callbacks-b.db")
	journalsA1 := openBoundAPICallbackStore(t, outboxAPath, backendA)
	journalsB1 := openBoundAPICallbackStore(t, outboxBPath, backendB)
	storageIDA := journalsA1.storageID
	storageIDB := journalsB1.storageID
	apiAddr := freePort(t)
	callbackRoutes, err := placement.NewCallbackRouteFactory("http://" + apiAddr)
	require.NoError(t, err)

	// Fred #1 has made the write-ahead placement mutation. Its process-local
	// Registry is intentionally absent from the durable state we reopen below.
	placementStore1, err := placementstore.NewStore(
		placementPath, placement.WithCallbackRouteFactory(callbackRoutes),
	)
	require.NoError(t, err)
	backendNames := []string{backendA, backendB}
	require.NoError(t, placementstore.ConfigureBackendTopologyWithStorageIdentities(placementStore1,
		backendNames,
		map[string]backendidentity.ID{backendA: storageIDA, backendB: storageIDB},
	))
	seedCoordinator, err := placementStore1.BindOperationCoordinator(nil)
	require.NoError(t, err)
	seedBackendA := apiAmbiguousProvisionBackend{Backend: backend.NewMockBackend(
		backend.MockBackendConfig{Name: backendA},
	)}
	seedBackendB := apiAmbiguousProvisionBackend{Backend: backend.NewMockBackend(
		backend.MockBackendConfig{Name: backendB},
	)}
	seedRouter, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: seedBackendA, Match: backend.MatchCriteria{SKUs: []string{"sku-a"}}, IsDefault: true},
		{Backend: seedBackendB, Match: backend.MatchCriteria{SKUs: []string{"sku-b"}}},
	}})
	require.NoError(t, err)
	seedChain := &chaintest.MockClient{GetLeaseFunc: func(
		_ context.Context, leaseUUID string,
	) (*billingtypes.Lease, error) {
		sku := "sku-a"
		if leaseUUID == leaseB {
			sku = "sku-b"
		}
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: "tenant-1", ProviderUuid: providerUUID,
			State: billingtypes.LEASE_STATE_PENDING,
			Items: []billingtypes.LeaseItem{{
				SkuUuid: sku, ServiceName: "app", Quantity: 1,
			}},
		}, nil
	}}
	seedExecution, seedInventoryRuntime := bindAPIInventoryRuntime(
		t, seedCoordinator, seedRouter, seedChain,
	)
	seedReconciliation, err := seedExecution.ReconciliationCoordinator(nil, nil)
	require.NoError(t, err)
	registerAPIReconciliationInventory(t, seedReconciliation, seedInventoryRuntime)
	projectAPIPlacementInventory(t, seedReconciliation, backendNames,
		map[string]backendidentity.ID{backendA: storageIDA, backendB: storageIDB},
		placement.ReconciliationProjection{})
	seedProvision, err := seedExecution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	for _, leaseUUID := range []string{leaseA, leaseB} {
		request, requestErr := placement.NewProvisionEventRequest(leaseUUID, "tenant-1")
		require.NoError(t, requestErr)
		result := seedProvision.ExecuteCurrentLease(context.Background(), request)
		require.Equal(t, placement.ProvisionEventUncertain, result.Disposition())
		require.Error(t, result.Err())
	}
	operationA := placementStore1.Lookup(leaseA).AttemptOperationID()
	operationB := placementStore1.Lookup(leaseB).AttemptOperationID()
	require.True(t, operationA.Valid())
	require.True(t, operationB.Valid())
	callbackPairA, err := callbackRoutes.ForOperation(operationA)
	require.NoError(t, err)
	callbackPairB, err := callbackRoutes.ForOperation(operationB)
	require.NoError(t, err)
	exactURLA := callbackPairA.OperationURL()
	exactURLB := callbackPairB.OperationURL()
	require.NoError(t, placementStore1.Close())

	// Each backend process durably records its exact failure, then exits without
	// an HTTP attempt. These are the backend halves of two ambiguous transport
	// windows; only A will authenticate successfully after restart.
	_, publisherA1 := newAPICallbackPublisher(
		t, journalsA1, secretA, context.Background(), http.DefaultClient,
	)
	intentA := beginAPICallbackOperationIntent(
		t, journalsA1.operations, leaseA, exactURLA,
	)
	claimA, created := intentA.CreatedClaim()
	require.True(t, created)
	uncommittedA := commitAPICallbackOperationRefusal(t, journalsA1.operations, claimA)
	require.NoError(t, publisherA1.PublishOperationFailureContext(
		context.Background(), uncommittedA, "remote provision failed after dispatch",
	))
	require.NoError(t, journalsA1.Close())
	_, publisherB1 := newAPICallbackPublisher(
		t, journalsB1, secretB, context.Background(), http.DefaultClient,
	)
	intentB := beginAPICallbackOperationIntent(
		t, journalsB1.operations, leaseB, exactURLB,
	)
	claimB, created := intentB.CreatedClaim()
	require.True(t, created)
	uncommittedB := commitAPICallbackOperationRefusal(t, journalsB1.operations, claimB)
	require.NoError(t, publisherB1.PublishOperationFailureContext(
		context.Background(), uncommittedB, "remote provision failed after dispatch",
	))
	require.NoError(t, journalsB1.Close())

	placementStore2, err := placementstore.NewStore(
		placementPath, placement.WithCallbackRouteFactory(callbackRoutes),
	)
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
				Uuid: leaseUUID, Tenant: "tenant-1", ProviderUuid: providerUUID,
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
		ProviderUUID:          providerUUID,
		PlacementStore:        placementStore2,
		CallbackProofConsumer: proofConsumer,
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
			storageIDA: secretA,
			storageIDB: secretB,
		},
	}, ServerDeps{
		ChainClient: chainClient, CallbackPublisher: manager, StatusChecker: manager,
		CallbackProofVerifier: proofVerifier,
	})
	require.NoError(t, err)
	_, err = callbackServer.StartBackground()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbackServer.Shutdown(context.Background())) })

	// Backend B first reopens its outbox with A's key. The HMAC-covered B identity
	// selects B's keyring slot, so A's key must receive 401, leave the row queued,
	// and leave B's durable attempt untouched.
	journalsB2 := openBoundAPICallbackStore(t, outboxBPath, backendB)
	outboxB2 := journalsB2.callbacks
	require.Equal(t, storageIDB, journalsB2.storageID)
	t.Cleanup(func() { require.NoError(t, journalsB2.Close()) })
	zeroBackoff := [shared.CallbackMaxAttempts]time.Duration{}
	wrongKeyResults := make(chan callbackHTTPObservation, shared.CallbackMaxAttempts)
	wrongKeyCtx, cancelWrongKey := context.WithCancel(context.Background())
	wrongKeySender := shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store: outboxB2, StorageAttestor: newAPICallbackStorageAttestor(t, journalsB2, wrongKeyCtx),
		HTTPClient: &http.Client{Transport: &callbackObservingRoundTripper{
			base: http.DefaultTransport, observations: wrongKeyResults,
		}},
		Secret:  secretA,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Backoff: &zeroBackoff, DeliveryTimeout: 3 * time.Second,
	})
	wrongKeyDone := make(chan struct{})
	go func() {
		defer close(wrongKeyDone)
		wrongKeySender.RunReplayLoop()
	}()
	wrongKeySender.NotifyPendingCallbacks()
	for range shared.CallbackMaxAttempts {
		result := <-wrongKeyResults
		require.NoError(t, result.err)
		assert.Equal(t, http.StatusUnauthorized, result.statusCode)
	}
	cancelWrongKey()
	select {
	case <-wrongKeyDone:
	case <-time.After(time.Second):
		t.Fatal("wrong-key replay loop did not stop")
	}
	pendingB, err := outboxB2.ListPending()
	require.NoError(t, err)
	require.Len(t, pendingB, 1, "cross-backend authentication failure must preserve the durable evidence")
	assert.Equal(t, operationB, placementStore2.Lookup(leaseB).AttemptOperationID())
	assert.Zero(t, rejectCalls.Load(), "A's key must not settle B's operation")

	// Backend A now reopens its durable outbox and replays through Fred's real
	// production keyring into the fresh Registry plus reopened placement store.
	journalsA2 := openBoundAPICallbackStore(t, outboxAPath, backendA)
	outboxA2 := journalsA2.callbacks
	require.Equal(t, storageIDA, journalsA2.storageID)
	t.Cleanup(func() { require.NoError(t, journalsA2.Close()) })
	senderACtx, cancelSenderA := context.WithCancel(context.Background())
	senderA := shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store: outboxA2, StorageAttestor: newAPICallbackStorageAttestor(t, journalsA2, senderACtx),
		HTTPClient: &http.Client{}, Secret: secretA,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Backoff: &zeroBackoff, DeliveryTimeout: 3 * time.Second,
	})
	senderADone := make(chan struct{})
	go func() {
		defer close(senderADone)
		senderA.RunReplayLoop()
	}()
	senderA.NotifyPendingCallbacks()
	require.Eventually(t, func() bool {
		remaining, listErr := outboxA2.ListPending()
		return listErr == nil && len(remaining) == 0
	}, 5*time.Second, time.Millisecond)
	cancelSenderA()
	select {
	case <-senderADone:
	case <-time.After(time.Second):
		t.Fatal("successful replay loop did not stop")
	}

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
	proofVerifier, _ := hmacauth.NewCallbackProofBoundary()
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
		ChainClient:           &chaintest.MockClient{},
		CallbackPublisher:     publisher,
		CallbackProofVerifier: proofVerifier,
	})
	require.NoError(t, err)
	_, err = server.StartBackground()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })

	journals := openBoundAPICallbackStore(
		t, filepath.Join(t.TempDir(), "callbacks.db"), "docker-api-test",
	)
	store := journals.callbacks
	t.Cleanup(func() { require.NoError(t, journals.Close()) })

	zeroBackoff := [shared.CallbackMaxAttempts]time.Duration{}
	const deliveryTimeout = 250 * time.Millisecond
	observations := make(chan callbackHTTPObservation, shared.CallbackMaxAttempts)
	senderCtx, cancelSender := context.WithCancel(context.Background())
	attestor := newAPICallbackStorageAttestor(t, journals, senderCtx)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sender := shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store: store, StorageAttestor: attestor,
		HTTPClient: &http.Client{Transport: &callbackObservingRoundTripper{
			base:         http.DefaultTransport,
			observations: observations,
		}},
		Secret:          secret,
		Logger:          logger,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	})
	durablePublisher := mustNewCallbackPublisherForTest(t, shared.CallbackPublisherConfig{
		OperationSettlement:   journals.operations,
		MaintenanceSettlement: journals.maintenance,
		StorageAttestor:       attestor,
		Logger:                logger,
	})
	callbackOperationID, err := operation.ParseID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	callbackRoutes, err := placement.NewCallbackRouteFactory("http://" + apiAddr)
	require.NoError(t, err)
	callbackPair, err := callbackRoutes.ForOperation(callbackOperationID)
	require.NoError(t, err)
	callbackURL := callbackPair.OperationURL()
	admission := beginAPICallbackOperationIntent(t, journals.operations, leaseUUID, callbackURL)
	claim, created := admission.CreatedClaim()
	require.True(t, created)
	uncommitted := commitAPICallbackOperationRefusal(t, journals.operations, claim)
	require.NoError(t, durablePublisher.PublishOperationFailureContext(
		context.Background(), uncommitted, "container exited",
	))
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "durable Send must return after persistence, before HTTP delivery")
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		sender.RunReplayLoop()
	}()
	sender.NotifyPendingCallbacks()
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
	cancelSender()
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("deadline replay loop did not stop")
	}

	require.Eventually(t, func() bool {
		return publisher.calls.Load() == shared.CallbackMaxAttempts
	}, time.Second, time.Millisecond)
	pending, err = store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, leaseUUID, pending[0].LeaseUUID)
}
