package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/callbackurl"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func callbackStorageID(t *testing.T, value string) backendidentity.ID {
	t.Helper()
	id, err := backendidentity.Parse(value)
	require.NoError(t, err)
	return id
}

func beginCallbackSenderOperationIntent(
	t *testing.T,
	store *CallbackStore,
	leaseUUID string,
	callbackURL string,
	backendName string,
	storageID backendidentity.ID,
) OperationIntentAdmission {
	t.Helper()
	if !storageID.Valid() {
		storageID = callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	}
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	spec := testOperationIntentSpec(t, "callback-sender")
	spec.LeaseUUID = leaseUUID
	spec.CallbackURL = callbackURL
	spec.LifecycleCallbackURL = lifecycleURL
	admission, err := beginTestOperationIntent(t, store, spec, operationIntentTestIdentity{
		backend: backendName, storageID: storageID,
	})
	require.NoError(t, err)
	return admission
}

// replayPendingCallbacks is a synchronous full-drain harness for focused tests
// and benchmarks. Production has one scheduling surface: RunReplayLoop.
func (s *CallbackSender) replayPendingCallbacks() {
	if s.store == nil || s.stopCtx.Err() != nil {
		return
	}
	if err := s.attestor.verify(s.stopCtx); err != nil {
		s.logger.Error("callback replay suppressed by backend identity verification", "error", err)
		return
	}
	leaseUUIDs, err := s.store.callbackLeaseUUIDs()
	if err != nil {
		s.logger.Error("callback outbox discovery found durable corruption", "error", err)
		s.reportStoreError()
	}
	jobs := make(chan string, len(leaseUUIDs))
	for _, leaseUUID := range leaseUUIDs {
		jobs <- leaseUUID
	}
	close(jobs)
	var workers sync.WaitGroup
	for range min(callbackReplayWorkerLimit, len(leaseUUIDs)) {
		workers.Go(func() {
			for leaseUUID := range jobs {
				if s.stopCtx.Err() != nil {
					return
				}
				_ = s.replayLeaseWithLimit(leaseUUID, 0)
			}
		})
	}
	workers.Wait()
}

func TestCallbackSenderBindsHMACCoveredPayloadToStorageIdentity(t *testing.T) {
	t.Parallel()

	id := callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	const secret = "callback-storage-identity-test-secret"
	var received backend.CallbackPayload
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.NoError(t, hmacauth.Verify(
			secret, req.Method, req.URL.RequestURI(), body,
			req.Header.Get(hmacauth.SignatureHeader), 5*time.Minute,
		))
		require.NoError(t, json.Unmarshal(body, &received))
		return callbackHTTPResponse(http.StatusOK), nil
	})}
	sender := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: client,
		Secret:     secret,
		Logger:     slog.Default(),

		Backoff: &zeroBackoff,
	}, callbackSenderTestStorageIdentity(id))
	sender.sendOperationCallbackForTest(
		testLeaseUUID("storage-bound"),
		"https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000",
		"docker-a", backend.CallbackStatusSuccess, "",
	)
	assert.Equal(t, id.String(), received.BackendStorageID)

	// A v0.13 provider decoding its old DTO ignores the additive JSON field.
	var oldProvider struct {
		LeaseUUID string                 `json:"lease_uuid"`
		Status    backend.CallbackStatus `json:"status"`
	}
	body, err := json.Marshal(received)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, &oldProvider))
	assert.Equal(t, received.LeaseUUID, oldProvider.LeaseUUID)
	assert.Equal(t, received.Status, oldProvider.Status)
}

func TestCallbackSenderCopiedOutboxCannotReplayUnderDifferentStorageIdentity(t *testing.T) {
	t.Parallel()

	idA := callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	idB := callbackStorageID(t, "6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	failingClient := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return callbackHTTPResponse(http.StatusServiceUnavailable), nil
	})}
	senderA := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: failingClient, Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff,
	}, callbackSenderTestStorageIdentity(idA))
	leaseUUID := testLeaseUUID("copied-outbox")
	callbackURL := "https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "docker-a", idA)
	senderA.sendOperationCallbackForTest(
		leaseUUID, callbackURL,
		"docker-a", backend.CallbackStatusFailed, "definitively refused",
	)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, idA.String(), pending[0].BackendStorageID)

	var requests atomic.Int32
	senderB := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusOK), nil
		})},
		Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff,
	}, callbackSenderTestStorageIdentity(idB))
	senderB.replayPendingCallbacks()
	assert.Zero(t, requests.Load(), "mismatched durable evidence must not reach HTTP")
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Len(t, pending, 1, "mismatched durable evidence must remain quarantined")
}

func TestCallbackSenderBlockingIdentityProbeIsBoundedAndPersistsBeforeDeferring(t *testing.T) {
	t.Parallel()

	stores := openOperationHandoffStores(t, "docker-a")
	store := stores.callbacks
	id := stores.storage.ID()
	var requests atomic.Int32
	stopCtx := context.Background()
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusOK), nil
		})},
		Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff,
		StorageAttestor: newTestCallbackStorageAttestor(t, store, stopCtx, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}, 20*time.Millisecond),
	}, callbackSenderTestStorageIdentity(id))
	leaseUUID := testLeaseUUID("blocked-identity")
	callbackURL := "https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "docker-a", id)
	started := time.Now()
	sender.sendOperationCallbackForTest(
		leaseUUID, callbackURL,
		"docker-a", backend.CallbackStatusFailed, "definitively refused",
	)
	assert.Less(t, time.Since(started), 500*time.Millisecond)
	assert.Zero(t, requests.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, id.String(), pending[0].BackendStorageID)
}

func TestReplayPendingCallbacks_BlockingIdentityProbeIsBounded(t *testing.T) {
	t.Parallel()

	stores := openOperationHandoffStores(t, "docker-a")
	store := stores.callbacks
	probeFinished := make(chan error, 1)
	stopCtx := context.Background()
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: http.DefaultClient, Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff,
		StorageAttestor: newTestCallbackStorageAttestor(t, store, stopCtx, func(ctx context.Context) error {
			<-ctx.Done()
			probeFinished <- ctx.Err()
			return ctx.Err()
		}, 20*time.Millisecond),
	})

	started := time.Now()
	sender.replayPendingCallbacks()
	assert.Less(t, time.Since(started), 500*time.Millisecond)
	assert.ErrorIs(t, <-probeFinished, context.DeadlineExceeded)
}

func TestReplayPendingCallbacks_BlockingAttemptIdentityProbeIsBounded(t *testing.T) {
	t.Parallel()

	stores := openOperationHandoffStores(t, "docker-a")
	store := stores.callbacks
	leaseUUID := testLeaseUUID("blocked-attempt-identity")
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:        leaseUUID,
		CallbackURL:      "https://fred.example/callbacks/provision?lifecycle_id=550e8400-e29b-41d4-a716-446655440000",
		Backend:          "docker",
		BackendStorageID: stores.storage.ID().String(),
		DeliveryKind:     CallbackDeliveryKindLifecycle,
		Status:           backend.CallbackStatusFailed,
		CreatedAt:        time.Now(),
	}))

	var probes atomic.Int32
	var requests atomic.Int32
	stopCtx := context.Background()
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusOK), nil
		})},
		Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff,
		StorageAttestor: newTestCallbackStorageAttestor(t, store, stopCtx, func(ctx context.Context) error {
			if probes.Add(1) == 1 {
				return nil // replay discovery is healthy
			}
			<-ctx.Done() // the per-HTTP-attempt proof stalls
			return ctx.Err()
		}, 20*time.Millisecond),
	})

	started := time.Now()
	sender.replayPendingCallbacks()
	assert.Less(t, time.Since(started), 500*time.Millisecond)
	assert.GreaterOrEqual(t, probes.Load(), int32(2))
	assert.Zero(t, requests.Load(), "HTTP must wait for a bounded storage proof")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "a timed-out proof must leave the callback for replay")
}

func TestCallbackSenderPermanentDriftSuppressesEnqueue(t *testing.T) {
	t.Parallel()

	stores := openOperationHandoffStores(t, "docker-a")
	store := stores.callbacks
	spec := testOperationIntentSpec(t, "permanent-drift")
	_, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	storageID := callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	stopCtx := context.Background()
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: http.DefaultClient, Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff,
		StorageAttestor: newTestCallbackStorageAttestor(t, store, stopCtx, func(context.Context) error {
			return fmt.Errorf("%w: marker mismatch", backendidentity.ErrIdentityDrift)
		}, 0),
	}, callbackSenderTestStorageIdentity(storageID))
	sender.sendOperationCallbackForTest(
		spec.LeaseUUID, spec.CallbackURL, "docker-a", backend.CallbackStatusSuccess, "",
	)
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1, "identity drift must leave the write-ahead intent for recovery")
}

func TestCallbackSenderRechecksIdentityAfterWaitingForLeaseFIFO(t *testing.T) {
	t.Parallel()

	const leaseUUID = "018f47a2-8b1c-7def-8123-456789abcdef"
	stores := openOperationHandoffStores(t, "docker-a")
	store := stores.callbacks

	var requests atomic.Int32
	firstVerified := make(chan struct{})
	var probes atomic.Int32
	stopCtx := context.Background()
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret: "secret",
		Logger: slog.Default(),

		Backoff: &zeroBackoff,
		StorageAttestor: newTestCallbackStorageAttestor(t, store, stopCtx, func(context.Context) error {
			if probes.Add(1) == 1 {
				close(firstVerified)
				return nil
			}
			return fmt.Errorf("%w: volume root changed", backendidentity.ErrIdentityDrift)
		}, 0),
	}, callbackSenderTestStorageIdentity(
		callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	))
	callbackURL := "https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	beginCallbackSenderOperationIntent(
		t, store, leaseUUID, callbackURL, "docker-a", sender.storageIdentity,
	)

	// Model an older journal mutation holding this lease's FIFO while the
	// physical root changes. The sender may pass its first probe, but it must not
	// persist using that stale proof after it eventually acquires the keyed lock.
	unlockOlder := sender.lockLease(leaseUUID)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sender.sendOperationCallbackForTest(
			leaseUUID, callbackURL,
			"docker-a", backend.CallbackStatusSuccess, "",
		)
	}()
	select {
	case <-firstVerified:
	case <-time.After(time.Second):
		t.Fatal("callback did not complete its pre-FIFO identity probe")
	}
	require.Eventually(t, func() bool {
		sender.deliveryLocksMu.Lock()
		defer sender.deliveryLocksMu.Unlock()
		lock := sender.deliveryLocks[leaseUUID]
		return lock != nil && lock.refs == 2
	}, time.Second, time.Millisecond, "callback did not join the contended journal mutation")
	unlockOlder()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback did not return after the FIFO was released")
	}

	assert.Equal(t, int32(2), probes.Load(), "identity must be independently re-attested after lock wait")
	assert.Zero(t, requests.Load(), "permanent post-wait drift must suppress network delivery")
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "permanent post-wait drift must suppress durable enqueue")
}

func TestCallbackSenderCancellationDuringPostLockIdentityProbeSuppressesEnqueue(t *testing.T) {
	t.Parallel()

	stores := openOperationHandoffStores(t, "docker-a")
	store := stores.callbacks

	const leaseUUID = "018f47a2-8b1c-7def-8123-456789abcdee"
	const callbackURL = "https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	var requests atomic.Int32
	secondProbeStarted := make(chan struct{})
	var probes atomic.Int32
	stopCtx := context.Background()
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret: "secret",
		Logger: slog.Default(),

		Backoff: &zeroBackoff,
		StorageAttestor: newTestCallbackStorageAttestor(t, store, stopCtx, func(ctx context.Context) error {
			switch probes.Add(1) {
			case 1:
				return nil
			case 2:
				close(secondProbeStarted)
				<-ctx.Done()
				return ctx.Err()
			default:
				panic("unexpected identity probe")
			}
		}, 0),
	}, callbackSenderTestStorageIdentity(
		callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	))
	beginCallbackSenderOperationIntent(
		t, store, leaseUUID, callbackURL, "docker-a", sender.storageIdentity,
	)

	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	t.Cleanup(cancelOwner)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sender.sendOperationCallbackContextForTest(
			ownerCtx, leaseUUID, callbackURL, "docker-a",
			backend.CallbackStatusSuccess, "",
		)
	}()
	select {
	case <-secondProbeStarted:
	case <-time.After(time.Second):
		t.Fatal("callback did not enter its post-lock identity probe")
	}
	cancelOwner()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback did not return after owner cancellation")
	}

	assert.Equal(t, int32(2), probes.Load())
	assert.Zero(t, requests.Load(), "canceled owner must never reach callback HTTP")
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "canceled owner must not persist a stale completion")
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1, "cancellation must preserve the write-ahead intent for recovery")
}

// zeroBackoff is used in tests to eliminate retry delays.
var zeroBackoff = [CallbackMaxAttempts]time.Duration{}

// newTestSender creates a CallbackSender with zero backoff for fast tests.
func newTestSender(t *testing.T, store *CallbackStore, httpClient *http.Client, secret string) *CallbackSender {
	t.Helper()
	cfg := CallbackSenderConfig{
		Store:      store,
		HTTPClient: httpClient,
		Secret:     secret,
		Logger:     slog.Default(),

		Backoff: &zeroBackoff,
	}
	if store == nil {
		return mustNewEphemeralCallbackSenderForTest(t, cfg)
	}
	return mustNewDurableCallbackSender(t, cfg)
}

func newEphemeralCallbackSenderForTest(
	cfg CallbackSenderConfig,
	storageIdentity backendidentity.ID,
) (*CallbackSender, error) {
	if cfg.Store != nil {
		return nil, errors.New("ephemeral callback sender test fixture requires nil store")
	}
	return newCallbackSender(cfg, storageIdentity)
}

func mustNewEphemeralCallbackSenderForTest(
	t *testing.T,
	cfg CallbackSenderConfig,
	options ...callbackSenderTestAuthorityOption,
) *CallbackSender {
	t.Helper()
	stopCtx, identity := callbackSenderTestAuthority(t, options...)
	cfg.StorageAttestor = newSyntheticCallbackStorageAttestorForTest(t, stopCtx, identity)
	sender, err := newEphemeralCallbackSenderForTest(cfg, identity)
	require.NoError(t, err)
	return sender
}

func mustNewDurableCallbackSender(
	t *testing.T,
	cfg CallbackSenderConfig,
	options ...callbackSenderTestAuthorityOption,
) *CallbackSender {
	t.Helper()
	stopCtx, identity := callbackSenderTestAuthority(t, options...)
	if len(cfg.Secret) < hmacauth.MinSecretLength {
		cfg.Secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	}
	// Historical outbox tests deliberately use unbound stores to exercise wire
	// compatibility and corruption in isolation. Production construction rejects
	// that topology; keep the bypass package-local to tests.
	if cfg.Store != nil && cfg.Store.binding == nil {
		cfg.StorageAttestor = newSyntheticCallbackStorageAttestorForTest(t, stopCtx, identity)
		sender, err := newCallbackSender(cfg, identity)
		require.NoError(t, err)
		return sender
	}
	if cfg.StorageAttestor == nil {
		cfg.StorageAttestor = newTestCallbackStorageAttestor(
			t, cfg.Store, stopCtx, func(context.Context) error { return nil }, 0,
		)
	}
	return MustNewCallbackSender(cfg)
}

// callbackSenderTestAuthority keeps synthetic test-only transport fixtures
// honest without adding independently configurable lifetime or lineage fields
// to the production CallbackSenderConfig.
type callbackSenderTestAuthorityConfig struct {
	stopCtx   context.Context
	storageID backendidentity.ID
}

type callbackSenderTestAuthorityOption func(*callbackSenderTestAuthorityConfig)

func callbackSenderTestLifetime(stopCtx context.Context) callbackSenderTestAuthorityOption {
	return func(config *callbackSenderTestAuthorityConfig) { config.stopCtx = stopCtx }
}

func callbackSenderTestStorageIdentity(storageID backendidentity.ID) callbackSenderTestAuthorityOption {
	return func(config *callbackSenderTestAuthorityConfig) { config.storageID = storageID }
}

func callbackSenderTestAuthority(
	t testing.TB,
	options ...callbackSenderTestAuthorityOption,
) (context.Context, backendidentity.ID) {
	t.Helper()
	storageID, err := backendidentity.Parse("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	config := callbackSenderTestAuthorityConfig{
		stopCtx: context.Background(), storageID: storageID,
	}
	for _, option := range options {
		require.NotNil(t, option)
		option(&config)
	}
	require.NotNil(t, config.stopCtx)
	require.True(t, config.storageID.Valid())
	return config.stopCtx, config.storageID
}

func newSyntheticCallbackStorageAttestorForTest(
	t testing.TB,
	stopCtx context.Context,
	storageIdentity ...backendidentity.ID,
) *CallbackStorageAttestor {
	t.Helper()
	storageID, err := backendidentity.Parse("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	if len(storageIdentity) > 0 {
		storageID = storageIdentity[0]
	}
	gate := newTestStorageAuthorityGate(t)
	base := &boltStore{
		binding:              &openedStoreIdentityBinding{storageID: storageID},
		ctx:                  context.Background(),
		backendAuthorityGate: gate,
	}
	store := &CallbackStore{boltStore: base}
	attestor, err := NewCallbackStorageAttestor(
		store,
		callbackStorageVerifierForTest{
			storageID: storageID,
			gate:      gate,
			verify:    func(context.Context) error { return nil },
		},
		stopCtx,
	)
	require.NoError(t, err)
	return attestor
}

type callbackStorageVerifierForTest struct {
	storageID backendidentity.ID
	gate      *backendidentity.StorageAuthorityGate
	verify    func(context.Context) error
}

func (v callbackStorageVerifierForTest) StorageIdentity() backendidentity.ID {
	return v.storageID
}

func (v callbackStorageVerifierForTest) StorageAuthorityGate() *backendidentity.StorageAuthorityGate {
	return v.gate
}

func (v callbackStorageVerifierForTest) Verify(ctx context.Context) error {
	return v.verify(ctx)
}

func newTestCallbackStorageAttestor(
	t *testing.T,
	store *CallbackStore,
	stopCtx context.Context,
	verify func(context.Context) error,
	timeout time.Duration,
) *CallbackStorageAttestor {
	t.Helper()
	if verify == nil {
		verify = func(context.Context) error { return nil }
	}
	attestor, err := NewCallbackStorageAttestor(
		store, callbackStorageVerifierForTest{
			storageID: store.binding.storageID,
			gate:      store.backendAuthorityGate,
			verify:    verify,
		}, stopCtx,
	)
	require.NoError(t, err)
	if timeout > 0 {
		attestor.timeout = timeout
	}
	return attestor
}

type callbackRoundTripFunc func(*http.Request) (*http.Response, error)

func (f callbackRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func callbackHTTPResponse(status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       http.NoBody,
	}
}

func TestNewCallbackSender_ErrorsOnNilHTTPClient(t *testing.T) {
	_, err := newEphemeralCallbackSenderForTest(CallbackSenderConfig{
		Logger: slog.Default(),
	}, backendidentity.ID{})
	require.ErrorContains(t, err, "HTTP client")
}

func TestNewCallbackSender_ErrorsOnNilLogger(t *testing.T) {
	_, err := newEphemeralCallbackSenderForTest(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
	}, backendidentity.ID{})
	require.ErrorContains(t, err, "logger")
}

func TestNewCallbackSender_ErrorsOnMissingAttestorLifecycle(t *testing.T) {
	_, err := newEphemeralCallbackSenderForTest(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),
	}, backendidentity.ID{})
	require.ErrorContains(t, err, "storage attestor lifecycle")
}

func TestCallbackSenderConfigHasNoIndependentLifecycle(t *testing.T) {
	_, exposed := reflect.TypeFor[CallbackSenderConfig]().FieldByName("StopCtx")
	assert.False(t, exposed,
		"sender lifetime must be inherited from its exact storage attestor")
}

func TestNewCallbackSender_RequiresDurableAuthority(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	stopCtx := context.Background()
	valid := CallbackSenderConfig{
		Store:      stores.callbacks,
		HTTPClient: http.DefaultClient,
		Secret:     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		Logger:     slog.Default(),
	}
	valid.StorageAttestor = newTestCallbackStorageAttestor(
		t, stores.callbacks, stopCtx, nil, 0,
	)

	missingStore := valid
	missingStore.Store = nil
	_, err := NewCallbackSender(missingStore)
	require.ErrorContains(t, err, "durable store")

	missingSecret := valid
	missingSecret.Secret = ""
	_, err = NewCallbackSender(missingSecret)
	require.ErrorContains(t, err, "HMAC secret")

	weakSecret := valid
	weakSecret.Secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	_, err = NewCallbackSender(weakSecret)
	require.ErrorContains(t, err, "at least 32 bytes")

	missingStorageAuthority := valid
	missingStorageAuthority.StorageAttestor = nil
	_, err = NewCallbackSender(missingStorageAuthority)
	require.ErrorContains(t, err, "storage attestor")

	assert.Panics(t, func() { MustNewCallbackSender(missingStore) },
		"only the explicitly named Must constructor may panic on invalid wiring")
}

func TestCallbackStorageAttestorRejectsCrossWiredBackendGateWithCopiedIdentity(t *testing.T) {
	storesA := openOperationHandoffStores(t, "docker-a")
	storesB := openOperationHandoffStores(t, "docker-b")
	verifierBWithCopiedIdentity := callbackStorageVerifierForTest{
		storageID: storesA.storage.ID(),
		gate:      storesB.gate,
		verify: func(context.Context) error {
			return nil
		},
	}
	// Report A's copied durable identity but B's independent backend-lifetime
	// withdrawal gate. Matching UUID metadata must not make this verifier an
	// authority over A's open outbox.
	_, err := NewCallbackStorageAttestor(
		storesA.callbacks, verifierBWithCopiedIdentity, context.Background(),
	)
	require.ErrorContains(t, err, "another backend authority gate")
}

func TestCallbackStorageAttestorRejectsCanceledLifetime(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewCallbackStorageAttestor(
		stores.callbacks,
		callbackStorageVerifierForTest{
			storageID: stores.storage.ID(),
			gate:      stores.gate,
			verify:    func(context.Context) error { return nil },
		},
		stopCtx,
	)
	require.ErrorContains(t, err, "stop context is canceled")
}

func TestCallbackStorageAttestorRejectsClosedAndReopenedStoreInstance(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	stopCtx := context.Background()
	attestor := newTestCallbackStorageAttestor(t, stores.callbacks, stopCtx, nil, 0)
	require.NoError(t, stores.callbacks.Close())

	reopened, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	_, err = NewCallbackSender(CallbackSenderConfig{
		Store: reopened, StorageAttestor: attestor, HTTPClient: http.DefaultClient,
		Secret: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", Logger: slog.Default(),
	})
	require.ErrorContains(t, err, "exact callback storage attestor")
	require.Error(t, attestor.verify(context.Background()),
		"closing the exact store must revoke its previously minted attestor")
}

func TestNewCallbackSender_DefaultBackoff(t *testing.T) {
	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),
	})
	assert.Equal(t, defaultCallbackBackoff, s.backoff)
	assert.Equal(t, 2*time.Minute, backend.DefaultCallbackApplicationTimeout)
	assert.Equal(t, 2*time.Minute+15*time.Second, backend.DefaultCallbackDeliveryTimeout)
	assert.Greater(t, backend.DefaultCallbackDeliveryTimeout, backend.DefaultCallbackApplicationTimeout)
	assert.Equal(t, backend.DefaultCallbackDeliveryTimeout, s.deliveryTimeout)
	assert.Equal(t, DefaultCallbackReplayInterval, s.replayInterval)
}

func TestNewCallbackSender_CustomBackoff(t *testing.T) {
	custom := [CallbackMaxAttempts]time.Duration{0, 100 * time.Millisecond, 200 * time.Millisecond}
	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),

		Backoff: &custom,
	})
	assert.Equal(t, custom, s.backoff)
}

func TestNewCallbackSender_ErrorsOnNegativeReplayInterval(t *testing.T) {
	_, err := newEphemeralCallbackSenderForTest(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),

		ReplayInterval: -time.Second,
	}, backendidentity.ID{})
	require.ErrorContains(t, err, "replay interval")
}

func TestNewCallbackSender_ErrorsOnNegativeDeliveryTimeout(t *testing.T) {
	_, err := newEphemeralCallbackSenderForTest(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),

		DeliveryTimeout: -time.Nanosecond,
	}, backendidentity.ID{})
	require.ErrorContains(t, err, "delivery timeout")
}

func TestSendCallback_EmptyURL(t *testing.T) {
	s := newTestSender(t, nil, http.DefaultClient, "secret")
	// Should not panic, just log a warning
	s.sendOperationCallbackForTest(testLeaseUUID("lease-1"), "", "test-backend", backend.CallbackStatusSuccess, "")
}

func TestCallbackSender_TransportErrorNeverLogsCallbackCapability(t *testing.T) {
	const capability = "550e8400-e29b-41d4-a716-446655440000"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + capability
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	client := &http.Client{Transport: callbackRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("transport failed for %s", request.URL.String())
	})}
	sender := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: client,
		Secret:     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		Logger:     logger,

		Backoff: &zeroBackoff,
	})

	sender.sendOperationCallbackForTest(testLeaseUUID("log-redaction"), callbackURL, "docker", backend.CallbackStatusFailed, "failed")
	sender.sendOperationCallbackForTest(
		testLeaseUUID("log-redaction-invalid"),
		"https://fred.example/\x7f/callbacks/provision?operation_id="+capability,
		"docker", backend.CallbackStatusFailed, "failed",
	)

	assert.NotContains(t, output.String(), capability)
	assert.NotContains(t, output.String(), callbackURL)
}

func TestCallbackSender_ErrorResponseBodyNeverLogsCallbackCapability(t *testing.T) {
	const capability = "550e8400-e29b-41d4-a716-446655440000"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + capability
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Header:     make(http.Header),
			Body: io.NopCloser(bytes.NewBufferString(
				"upstream rejected " + callbackURL,
			)),
		}, nil
	})}
	sender := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: client, Secret: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		Logger: logger, Backoff: &zeroBackoff,
	})

	sender.sendOperationCallbackForTest(
		testLeaseUUID("response-log-redaction"), callbackURL,
		"docker", backend.CallbackStatusFailed, "failed",
	)

	assert.NotContains(t, output.String(), capability)
	assert.NotContains(t, output.String(), callbackURL)
}

func TestSendCallback_SuccessDelivery(t *testing.T) {
	const secret = "test-secret-32-chars-long-enough"
	var received backend.CallbackPayload
	var capturedBody []byte
	var capturedSig string
	var capturedMethod, capturedURI string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
		capturedBody, _ = io.ReadAll(r.Body)
		json.Unmarshal(capturedBody, &received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), secret)
	s.sendOperationCallbackForTest(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, "test-backend", backend.CallbackStatusSuccess, "")

	assert.Equal(t, testLeaseUUID("lease-1"), received.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusSuccess, received.Status)
	assert.Equal(t, "test-backend", received.Backend)

	// Verify HMAC signature is present and valid
	assert.NotEmpty(t, capturedSig, "HMAC signature header must be set")
	assert.NoError(t, hmacauth.Verify(secret, capturedMethod, capturedURI, capturedBody, capturedSig, time.Minute))
}

func TestSendCallback_FailurePayload(t *testing.T) {
	var received backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	s.sendOperationCallbackForTest(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, "test-backend", backend.CallbackStatusFailed, "image pull failed")

	assert.Equal(t, backend.CallbackStatusFailed, received.Status)
	assert.Equal(t, "image pull failed", received.Error)
}

func TestSendCallback_DurableSenderPersistsThenReplayRemoves(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, server.Client(), "secret")
	leaseUUID := testLeaseUUID("lease-1")
	callbackURL := server.URL + callbackurl.ProvisionPath
	beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "test-backend", s.storageIdentity)
	s.sendOperationCallbackForTest(
		leaseUUID, callbackURL, "test-backend",
		backend.CallbackStatusFailed, "definitively refused",
	)

	assert.Zero(t, requests.Load(), "durable command paths must not perform callback HTTP inline")
	assert.Zero(t, s.replayWake.pendingCount(),
		"durable publication must not address a sender before its replay loop subscribes")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "the outbox row must commit before replay owns delivery")

	s.replayPendingCallbacks()
	assert.Equal(t, int32(1), requests.Load())
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendCallback_DurableFailureCompletionRemainsPendingUntilReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, server.Client(), "secret")
	leaseUUID := testLeaseUUID("lease-1")
	callbackURL := server.URL + callbackurl.ProvisionPath
	beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "test-backend", s.storageIdentity)
	s.sendOperationCallbackForTest(leaseUUID, callbackURL, "test-backend", backend.CallbackStatusFailed, "error")

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, testLeaseUUID("lease-1"), pending[0].LeaseUUID)
}

func TestSendLifecycleCallback_CoalescesOlderPendingLifecycle(t *testing.T) {
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return callbackHTTPResponse(http.StatusInternalServerError), nil
	})}
	for _, tc := range []struct {
		name         string
		firstStatus  backend.CallbackStatus
		latestStatus backend.CallbackStatus
	}{
		{"failed to success", backend.CallbackStatusFailed, backend.CallbackStatusSuccess},
		{"success to failed", backend.CallbackStatusSuccess, backend.CallbackStatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
			require.NoError(t, err)
			defer store.Close()
			s := newTestSender(t, store, client, "secret")

			s.sendLifecycleCallbackForTest(testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", "docker", tc.firstStatus, "first", false)
			first, err := store.ListPending()
			require.NoError(t, err)
			require.Len(t, first, 1)

			s.sendLifecycleCallbackForTest(testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", "docker", tc.latestStatus, "latest", false)
			pending, err := store.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			assert.NotEqual(t, first[0].DeliveryID, pending[0].DeliveryID)
			assert.Greater(t, pending[0].Sequence, first[0].Sequence)
			assert.Equal(t, CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
			assert.Equal(t, tc.latestStatus, pending[0].Status)
		})
	}
}

func TestSendLifecycleCallback_DropsLateObservationBehindTerminal(t *testing.T) {
	var terminalRequests atomic.Int32
	var lateRequests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/terminal" + callbackurl.ProvisionPath:
			terminalRequests.Add(1)
		case "/late" + callbackurl.ProvisionPath:
			lateRequests.Add(1)
		}
		return callbackHTTPResponse(http.StatusServiceUnavailable), nil
	})}
	stores := openOperationHandoffStores(t, "docker-a")
	store := stores.callbacks
	sender := newTestSender(t, store, client, "secret")
	leaseUUID := testLeaseUUID("terminal-sender")

	sender.sendLifecycleCallbackForTest(
		leaseUUID, "https://fred.example/terminal/callbacks/provision", "docker",
		backend.CallbackStatusDeprovisioned, "", false,
	)
	sender.sendLifecycleCallbackForTest(
		leaseUUID, "https://fred.example/late/callbacks/provision", "docker",
		backend.CallbackStatusFailed, "delayed runtime observation", false,
	)

	assert.Zero(t, terminalRequests.Load(),
		"durable lifecycle publishers must never retry the terminal head inline")
	assert.Zero(t, lateRequests.Load(), "a runtime observation must never overtake or follow terminal retirement")
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, pending[0].Status)
}

func TestSendLifecycleCallback_RejectsInvalidURL(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	s := newTestSender(t, store, client, "secret")

	const id = "550e8400-e29b-41d4-a716-446655440000"
	for index, callbackURL := range []string{
		"https://fred.example/callbacks/provision?trace=keep&operation%5fid=" + id,
		"https://fred.example/callbacks/provision?trace=%ZZ&lifecycle_id=" + id,
		"https://fred.example/callbacks/provision?trace=x;y&lifecycle_id=" + id,
	} {
		s.sendLifecycleCallbackForTest(
			fmt.Sprintf("lease-%d", index), callbackURL, "docker",
			backend.CallbackStatusFailed, "container exited", false,
		)
	}

	assert.Zero(t, requests.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendOperationCallback_RejectsInvalidURL(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	s := newTestSender(t, store, client, "secret")
	const id = "550e8400-e29b-41d4-a716-446655440000"

	for index, callbackURL := range []string{
		"https://fred.example/callbacks/provision?trace=keep&lifecycle%5fid=" + id,
		"https://fred.example/callbacks/provision?operation_id=" + id + "&lifecycle_id=" + id,
		"https://fred.example/callbacks/provision?operation_id=" + id + "&operation_id=" + id,
		"https://fred.example/callbacks/provision?trace=%ZZ&operation_id=" + id,
		"https://fred.example/callbacks/provision?trace=x;y&operation_id=" + id,
	} {
		s.sendOperationCallbackForTest(
			fmt.Sprintf("lease-%d", index), callbackURL, "docker",
			backend.CallbackStatusSuccess, "",
		)
	}

	assert.Zero(t, requests.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendOperationCallback_AcceptsTypedAndLegacyURLs(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	s := newTestSender(t, store, client, "secret")
	typedLeaseUUID := testLeaseUUID("typed")
	typedURL := "https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	legacyLeaseUUID := testLeaseUUID("legacy")
	legacyURL := "https://fred.example/callbacks/provision?trace=keep"
	beginCallbackSenderOperationIntent(t, store, typedLeaseUUID, typedURL, "docker", s.storageIdentity)
	beginCallbackSenderOperationIntent(t, store, legacyLeaseUUID, legacyURL, "docker", s.storageIdentity)

	s.sendOperationCallbackForTest(
		typedLeaseUUID, typedURL,
		"docker", backend.CallbackStatusFailed, "definitively refused",
	)
	s.sendOperationCallbackForTest(
		legacyLeaseUUID, legacyURL,
		"docker", backend.CallbackStatusFailed, "definitively refused",
	)

	assert.Zero(t, requests.Load(), "accepted durable callbacks must only publish outbox facts")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)

	s.replayPendingCallbacks()
	assert.Equal(t, int32(2), requests.Load())
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendLifecycleCallback_AcceptsTypedLifecycleURL(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	s := newTestSender(t, store, client, "secret")

	s.sendLifecycleCallbackForTest(
		testLeaseUUID("lease-1"),
		"https://fred.example/callbacks/provision?trace=keep&lifecycle_id=550e8400-e29b-41d4-a716-446655440000",
		"docker",
		backend.CallbackStatusFailed,
		"container exited",
		false,
	)

	assert.Zero(t, requests.Load(), "accepted durable callbacks must only publish outbox facts")
	assert.Zero(t, s.replayWake.pendingCount(),
		"durable publication must not address a sender before its replay loop subscribes")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)

	s.replayPendingCallbacks()
	assert.Equal(t, int32(1), requests.Load())
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackSender_RejectsStatusOutsideDeliveryKind(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	s := newTestSender(t, store, client, "secret")

	s.sendOperationCallbackForTest(
		"lease-operation",
		"https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000",
		"docker",
		backend.CallbackStatusDeprovisioned,
		"",
	)
	s.sendOperationCallbackForTest(
		"lease-operation-unknown",
		"https://fred.example/callbacks/provision",
		"docker",
		backend.CallbackStatus("unknown"),
		"",
	)
	s.sendLifecycleCallbackForTest(
		"lease-lifecycle-unknown",
		"https://fred.example/callbacks/provision",
		"docker",
		backend.CallbackStatus("unknown"),
		"",
		false,
	)
	s.sendLifecycleCallbackForTest(
		"lease-retained",
		"https://fred.example/callbacks/provision",
		"docker",
		backend.CallbackStatusSuccess,
		"",
		true,
	)

	assert.Zero(t, requests.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendOperationCallback_MissingIntentReportsStoreErrorAndSuppressesHTTP(t *testing.T) {
	var requests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: client, Secret: "secret", Logger: slog.Default(),
		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})

	s.sendOperationCallbackForTest(
		testLeaseUUID("missing-intent"), "https://fred.example/callbacks/provision",
		"docker", backend.CallbackStatusSuccess, "",
	)

	assert.Zero(t, requests.Load(), "missing causal authority must suppress HTTP delivery")
	assert.Equal(t, int32(1), storeErrors.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendOperationCallback_TransportHarnessDoesNotRequireDurableIntent(t *testing.T) {
	var requests atomic.Int32
	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", Logger: slog.Default(),
		Backoff: &zeroBackoff,
	})

	s.sendOperationCallbackForTest(
		testLeaseUUID("ephemeral"), "https://fred.example/callbacks/provision",
		"docker", backend.CallbackStatusSuccess, "",
	)

	assert.Equal(t, int32(1), requests.Load())
}

func TestSendOperationCallback_StoreFailureSuppressesDirectDelivery(t *testing.T) {
	var requests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	leaseUUID := testLeaseUUID("lease-1")
	callbackURL := "https://fred.example/callbacks/provision"
	beginCallbackSenderOperationIntent(
		t, store, leaseUUID, callbackURL, "docker",
		callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	require.NoError(t, store.Close())
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})

	s.sendOperationCallbackForTest(
		leaseUUID, callbackURL, "docker",
		backend.CallbackStatusSuccess, "",
	)

	assert.Zero(t, requests.Load(), "configured persistence failure must fail closed past unknown older entries")
	assert.Equal(t, int32(1), storeErrors.Load())
	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "lease locks must be released and retired after a failed enqueue")
	s.deliveryLocksMu.Unlock()
}

func TestReplayPendingCallbacks_ListFailureSuppressesDelivery(t *testing.T) {
	var requests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	require.NoError(t, store.Close())
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})

	s.replayPendingCallbacks()

	assert.Zero(t, requests.Load(), "a failed durable listing must not guess that no older entry exists")
	assert.Equal(t, int32(1), storeErrors.Load())
}

func TestReplayPendingCallbacks_CorruptLeaseDoesNotBlockHealthyLease(t *testing.T) {
	var healthyRequests atomic.Int32
	var corruptRequests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/healthy" + callbackurl.ProvisionPath:
			healthyRequests.Add(1)
		case "/corrupt" + callbackurl.ProvisionPath:
			corruptRequests.Add(1)
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("healthy-lease"),
		CallbackURL:  "https://fred.example/healthy/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("corrupt-lease"),
		CallbackURL:  "https://fred.example/corrupt/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		leaseBucket := tx.Bucket(callbackV2BucketName).Bucket([]byte(testLeaseUUID("corrupt-lease")))
		require.NotNil(t, leaseBucket)
		return leaseBucket.Put(
			[]byte("123e4567-e89b-42d3-a456-426614174099"), []byte("{"))
	}))

	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})
	s.replayPendingCallbacks()

	assert.Equal(t, int32(1), healthyRequests.Load(),
		"corruption in another identifiable lease must not poison replay")
	assert.Zero(t, corruptRequests.Load())
	assert.Equal(t, int32(1), storeErrors.Load(), "the quarantined lease must remain observable")
	healthyPending, err := store.listPending(testLeaseUUID("healthy-lease"))
	require.NoError(t, err)
	assert.Empty(t, healthyPending)
	_, err = store.listPending(testLeaseUUID("corrupt-lease"))
	require.ErrorContains(t, err, "failed to decode callback entry")
	require.ErrorContains(t, store.Healthy(), "callback queue unhealthy",
		"per-lease delivery isolation must not hide corruption from health")
}

func TestReplayPendingCallbacks_SemanticPoisonNeverReachesTransport(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("semantic-ssrf-poison")
	entry := CallbackEntry{
		DeliveryID:   "550e8400-e29b-41d4-a716-446655440000",
		LeaseUUID:    leaseUUID,
		CallbackURL:  "http://169.254.169.254/latest/meta-data/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000",
		DeliveryKind: CallbackDeliveryKindOperation,
		Sequence:     1,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}
	data, err := marshalV2CallbackEntry(entry)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.Bucket(callbackV2BucketName).CreateBucket([]byte(leaseUUID))
		if err != nil {
			return err
		}
		return bucket.Put(callbackSequenceKey(1), data)
	}))
	var storeErrors atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: client, Secret: "secret", Logger: slog.Default(),
		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})

	sender.replayPendingCallbacks()
	assert.Zero(t, requests.Load(), "semantic corruption must be rejected before any outbound request")
	assert.Positive(t, storeErrors.Load())
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		assert.NotNil(t, tx.Bucket(callbackV2BucketName).Bucket([]byte(leaseUUID)).Get(callbackSequenceKey(1)))
		return nil
	}))
}

func TestReplayPendingCallbacks_StructuralDiscoveryErrorStillDrainsHealthyLease(t *testing.T) {
	var healthyRequests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/healthy"+callbackurl.ProvisionPath {
			healthyRequests.Add(1)
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("healthy-lease"),
		CallbackURL:  "https://fred.example/healthy/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		// V2 root values are invalid: every valid top-level key must own a
		// nested per-lease bucket. Preserve this value as corruption evidence.
		return tx.Bucket(callbackV2BucketName).
			Put([]byte("structural-corrupt-lease"), []byte("{"))
	}))

	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})
	s.replayPendingCallbacks()

	assert.Equal(t, int32(1), healthyRequests.Load(),
		"structural discovery errors must be reported without discarding valid lease jobs")
	assert.GreaterOrEqual(t, storeErrors.Load(), int32(1))
	healthyPending, err := store.listPending(testLeaseUUID("healthy-lease"))
	require.NoError(t, err)
	assert.Empty(t, healthyPending)
	require.ErrorContains(t, store.Healthy(), "is not a nested bucket")
}

func TestCallbackSender_DifferentLeasesDoNotShareDeliveryLock(t *testing.T) {
	blockedStarted := make(chan struct{})
	releaseBlocked := make(chan struct{})
	otherDelivered := make(chan struct{})
	var startOnce sync.Once
	var otherOnce sync.Once
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/blocked" + callbackurl.ProvisionPath:
			startOnce.Do(func() { close(blockedStarted) })
			<-releaseBlocked
		case "/other" + callbackurl.ProvisionPath:
			otherOnce.Do(func() { close(otherDelivered) })
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	s := newTestSender(t, nil, client, "secret")
	blockedDone := make(chan struct{})
	go func() {
		defer close(blockedDone)
		s.sendOperationCallbackForTest(testLeaseUUID("blocked-lease"), "https://fred.example/blocked/callbacks/provision", "docker", backend.CallbackStatusSuccess, "")
	}()
	<-blockedStarted

	otherDone := make(chan struct{})
	go func() {
		defer close(otherDone)
		s.sendOperationCallbackForTest(testLeaseUUID("other-lease"), "https://fred.example/other/callbacks/provision", "docker", backend.CallbackStatusSuccess, "")
	}()
	select {
	case <-otherDelivered:
	case <-time.After(time.Second):
		t.Fatal("an unrelated lease was head-of-line blocked")
	}
	<-otherDone
	close(releaseBlocked)
	<-blockedDone

	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "reference-counted keyed locks must not leak lease IDs")
	s.deliveryLocksMu.Unlock()
}

func TestCallbackSender_ReplayLoopDeliversPublishedCompletionWithoutRestart(t *testing.T) {
	var available atomic.Bool
	var attempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		if !available.Load() {
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	stopCtx, cancel := context.WithCancel(context.Background())
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff:        &zeroBackoff,
		ReplayInterval: 5 * time.Millisecond,
	}, callbackSenderTestLifetime(stopCtx))

	leaseUUID := testLeaseUUID("lease-1")
	callbackURL := "https://fred.example/callbacks/provision"
	beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "docker", s.storageIdentity)
	s.sendOperationCallbackForTest(
		leaseUUID, callbackURL, "docker",
		backend.CallbackStatusFailed, "definitively refused",
	)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "the exact completion must be durable before replay")
	assert.Zero(t, attempts.Load(), "durable settlement must not perform HTTP inline")

	available.Store(true)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		s.RunReplayLoop()
	}()
	require.Eventually(t, func() bool {
		pending, listErr := store.ListPending()
		return listErr == nil && len(pending) == 0
	}, time.Second, 5*time.Millisecond, "periodic replay must deliver without a backend restart")
	cancel()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("periodic replay loop did not stop with sender context")
	}
	assert.Positive(t, attempts.Load())
}

func TestCallbackSender_NotificationWakesTrackedReplayLoop(t *testing.T) {
	var attempts atomic.Int32
	initialExhausted := make(chan struct{})
	var exhaustedOnce sync.Once
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		attempt := attempts.Add(1)
		if attempt <= CallbackMaxAttempts {
			if attempt == CallbackMaxAttempts {
				exhaustedOnce.Do(func() { close(initialExhausted) })
			}
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("wake-lease"),
		CallbackURL:  "https://fred.example/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	stopCtx, cancel := context.WithCancel(context.Background())
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff:        &zeroBackoff,
		ReplayInterval: time.Hour,
	}, callbackSenderTestLifetime(stopCtx))

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		s.RunReplayLoop()
	}()
	select {
	case <-initialExhausted:
	case <-time.After(time.Second):
		t.Fatal("initial replay did not exhaust its delivery attempts")
	}
	s.NotifyPendingCallbacks()
	require.Eventually(t, func() bool {
		pending, listErr := store.ListPending()
		return listErr == nil && len(pending) == 0
	}, time.Second, time.Millisecond, "outbox notification must not wait for the periodic interval")
	cancel()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("notified replay loop did not stop with sender context")
	}
}

func TestCallbackSender_DirectIntentSettlementWakesTrackedReplayLoop(t *testing.T) {
	var attempts atomic.Int32
	delivered := make(chan struct{})
	var deliveredOnce sync.Once
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		deliveredOnce.Do(func() { close(delivered) })
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	stores := openOperationHandoffStores(t, "docker")
	store := stores.callbacks

	stopCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	initialReplayEntered := make(chan struct{})
	releaseInitialReplay := make(chan struct{})
	var replayCalls atomic.Int32
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff:        &zeroBackoff,
		ReplayInterval: time.Hour,
		StorageAttestor: newTestCallbackStorageAttestor(t, store, stopCtx, func(ctx context.Context) error {
			if replayCalls.Add(1) != 1 {
				return nil
			}
			close(initialReplayEntered)
			select {
			case <-releaseInitialReplay:
				return fmt.Errorf("suppress initial replay for wakeup test")
			case <-ctx.Done():
				return ctx.Err()
			}
		}, 0),
	})

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		s.RunReplayLoop()
	}()
	select {
	case <-initialReplayEntered:
	case <-time.After(time.Second):
		t.Fatal("tracked replay loop did not enter its initial pass")
	}

	leaseUUID := testLeaseUUID("direct-settlement-wake")
	callbackURL := "https://fred.example/callbacks/provision"
	admission := beginCallbackSenderOperationIntent(
		t, store, leaseUUID, callbackURL, "docker", s.storageIdentity,
	)
	claim, created := admission.CreatedClaim()
	require.True(t, created)
	_, err := store.ResolveOperationIntent(
		claim, backend.CallbackStatusFailed, "definitively refused",
	)
	require.NoError(t, err)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "direct settlement must commit before replay")
	assert.Zero(t, attempts.Load(), "direct settlement must not perform HTTP inline")

	// The first replay is deliberately suppressed after the completion is
	// durable. Only the store's post-commit notification can trigger delivery
	// before the one-hour periodic interval.
	close(releaseInitialReplay)
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("direct intent settlement did not wake the tracked replay loop")
	}
	require.Eventually(t, func() bool {
		pending, listErr := store.ListPending()
		return listErr == nil && len(pending) == 0
	}, time.Second, time.Millisecond)
	assert.Equal(t, int32(1), attempts.Load())
	assert.GreaterOrEqual(t, replayCalls.Load(), int32(2))

	cancel()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("direct-settlement replay loop did not stop with sender context")
	}
	store.replaySubscribersMu.Lock()
	subscriberCount := len(store.replaySubscribers)
	store.replaySubscribersMu.Unlock()
	assert.Zero(t, subscriberCount, "a stopped replay loop must unregister its wake channel")
}

func TestCallbackReplayMailbox_CoalescesByLeaseWithoutLosingStrongestFact(t *testing.T) {
	firstLease := testLeaseUUID("mailbox-first")
	secondLease := testLeaseUUID("mailbox-second")
	mailbox := newCallbackReplayMailbox()

	mailbox.publish(newCallbackReplayCommitWake(firstLease))
	mailbox.publish(newCallbackReplayCommitWake(secondLease))
	mailbox.publish(newCallbackReplayHandoffWake(firstLease))
	mailbox.publish(newCallbackReplayCommitWake(firstLease))

	select {
	case <-mailbox.ready:
	case <-time.After(time.Second):
		t.Fatal("typed replay mailbox did not publish readiness")
	}
	assert.Empty(t, mailbox.ready, "readiness must coalesce independently of lease facts")
	wakes := mailbox.take()
	require.Len(t, wakes, 2, "a full readiness channel must not discard another lease")
	got := make(map[string]callbackReplayWakeKind, len(wakes))
	for _, wake := range wakes {
		got[wake.leaseUUID] = wake.kind
	}
	assert.Equal(t, callbackReplayWakeHandoff, got[firstLease],
		"an ordinary commit must not downgrade a pending ownership handoff")
	assert.Equal(t, callbackReplayWakeCommit, got[secondLease])
	assert.Zero(t, mailbox.pendingCount())
}

func TestCallbackReplayMailbox_PublishRacingTakeDoesNotLoseLease(t *testing.T) {
	for iteration := range 100 {
		firstLease := testLeaseUUID(fmt.Sprintf("mailbox-race-first-%03d", iteration))
		secondLease := testLeaseUUID(fmt.Sprintf("mailbox-race-second-%03d", iteration))
		mailbox := newCallbackReplayMailbox()
		mailbox.publish(newCallbackReplayCommitWake(firstLease))
		<-mailbox.ready

		start := make(chan struct{})
		taken := make(chan []callbackReplayWake, 1)
		published := make(chan struct{})
		go func() {
			<-start
			taken <- mailbox.take()
		}()
		go func() {
			<-start
			mailbox.publish(newCallbackReplayCommitWake(secondLease))
			close(published)
		}()
		close(start)
		wakes := <-taken
		<-published
		for len(mailbox.ready) > 0 {
			<-mailbox.ready
			wakes = append(wakes, mailbox.take()...)
		}

		got := make(map[string]struct{}, len(wakes))
		for _, wake := range wakes {
			got[wake.leaseUUID] = struct{}{}
		}
		assert.Contains(t, got, firstLease)
		assert.Contains(t, got, secondLease)
		assert.Zero(t, mailbox.pendingCount())
	}
}

func TestCallbackSender_RunReplayLoopClaimsOneProcessLifetimeOwner(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	stopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: stores.callbacks,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff, ReplayInterval: time.Hour,
	}, callbackSenderTestLifetime(stopCtx))

	copied := *sender
	returned := make(chan struct{}, 2)
	for _, runner := range []*CallbackSender{sender, &copied} {
		go func() {
			runner.RunReplayLoop()
			returned <- struct{}{}
		}()
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("a duplicate replay-loop invocation did not fail closed")
	}
	require.Eventually(t, func() bool {
		stores.callbacks.replaySubscribersMu.Lock()
		defer stores.callbacks.replaySubscribersMu.Unlock()
		return len(stores.callbacks.replaySubscribers) == 1
	}, time.Second, time.Millisecond, "the one replay owner did not subscribe")
	select {
	case <-returned:
		t.Fatal("both replay-loop invocations returned while the owner context was live")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("the claimed replay owner did not stop after cancellation")
	}
}

func TestCallbackReplayQueue_TypedWakeTouchesOnlyItsLease(t *testing.T) {
	inFlightLease := testLeaseUUID("typed-wake-in-flight")
	otherLease := testLeaseUUID("typed-wake-other")
	queue := newCallbackReplayQueue()
	queue.discover([]string{inFlightLease}, false, false)
	leaseUUID, ready := queue.next()
	require.True(t, ready)
	require.Equal(t, inFlightLease, leaseUUID)
	queue.dispatched(inFlightLease)

	queue.wake(newCallbackReplayCommitWake(otherLease))
	assert.NotContains(t, queue.dirty, inFlightLease,
		"an unrelated commit must not restart an in-flight failed delivery")
	queue.completed(callbackReplayCompletion{
		leaseUUID: inFlightLease,
		outcome:   callbackReplayDeferred,
	})
	assert.Contains(t, queue.dormant, inFlightLease)

	queue.wake(newCallbackReplayCommitWake(inFlightLease))
	assert.Contains(t, queue.dormant, inFlightLease,
		"an appended suffix cannot overtake and restart its failed durable head")
	queue.wake(newCallbackReplayHandoffWake(inFlightLease))
	assert.NotContains(t, queue.dormant, inFlightLease)
	leaseUUID, ready = queue.next()
	require.True(t, ready)
	assert.Equal(t, inFlightLease, leaseUUID,
		"ownership handoff must promptly retry the exact dormant lease")
}

func TestCallbackReplayQueue_SameLeaseCommitRechecksInFlightDrain(t *testing.T) {
	leaseUUID := testLeaseUUID("typed-wake-same-lease")
	queue := newCallbackReplayQueue()
	queue.discover([]string{leaseUUID}, false, false)
	queue.dispatched(leaseUUID)

	queue.wake(newCallbackReplayCommitWake(leaseUUID))
	assert.Contains(t, queue.dirty, leaseUUID)
	queue.completed(callbackReplayCompletion{
		leaseUUID: leaseUUID,
		outcome:   callbackReplayEmpty,
	})
	next, ready := queue.next()
	require.True(t, ready, "a same-lease commit racing the empty check must be re-read")
	assert.Equal(t, leaseUUID, next)
}

func TestCallbackReplayCompletion_CancellationBreaksAFullCoordinatorBacklog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	completions := make(chan callbackReplayCompletion, callbackReplayWorkerLimit)
	for i := range callbackReplayWorkerLimit {
		completions <- callbackReplayCompletion{
			leaseUUID: testLeaseUUID(fmt.Sprintf("buffered-completion-%02d", i)),
			outcome:   callbackReplayEmpty,
		}
	}

	done := make(chan bool, 1)
	go func() {
		done <- publishCallbackReplayCompletion(ctx, completions, callbackReplayCompletion{
			leaseUUID: testLeaseUUID("completion-beyond-buffer"),
			outcome:   callbackReplayEmpty,
		})
	}()
	select {
	case <-done:
		t.Fatal("worker completion unexpectedly bypassed the full coordinator buffer")
	case <-time.After(10 * time.Millisecond):
	}
	cancel()
	select {
	case published := <-done:
		assert.False(t, published, "shutdown must retire the worker without publishing completion")
	case <-time.After(time.Second):
		t.Fatal("worker remained blocked behind a stopped replay coordinator")
	}
}

func TestCallbackReplay_CancellationBreaksMutationGateWait(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	store := stores.callbacks
	leaseUUID := testLeaseUUID("canceled-mutation-gate-wait")
	_, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:        leaseUUID,
		CallbackURL:      "https://fred.example/callbacks/provision",
		DeliveryKind:     CallbackDeliveryKindOperation,
		Status:           backend.CallbackStatusSuccess,
		BackendStorageID: stores.storage.ID().String(),
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)

	stopCtx, cancel := context.WithCancel(context.Background())
	var requests atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff, ReplayInterval: time.Hour,
	}, callbackSenderTestLifetime(stopCtx))

	unlockMutation := store.lockDeliveryLease(leaseUUID)
	t.Cleanup(unlockMutation)
	done := make(chan callbackReplayOutcome, 1)
	go func() { done <- sender.replayLeaseWithLimit(leaseUUID, 1) }()
	require.Eventually(t, func() bool {
		store.deliveryLocksMu.Lock()
		defer store.deliveryLocksMu.Unlock()
		lock := store.deliveryLocks[leaseUUID]
		return lock != nil && lock.refs == 2
	}, time.Second, time.Millisecond, "replay worker did not wait at the held mutation gate")

	cancel()
	select {
	case outcome := <-done:
		assert.Equal(t, callbackReplayDeferred, outcome)
	case <-time.After(time.Second):
		t.Fatal("canceled replay worker remained blocked at the mutation gate")
	}
	store.drainLocksMu.Lock()
	_, drainHeld := store.drainLocks[leaseUUID]
	store.drainLocksMu.Unlock()
	assert.False(t, drainHeld, "cancellation must release wire-drain ownership")
	assert.Zero(t, requests.Load(), "a replay blocked at the mutation gate must not reach HTTP")
}

func TestCallbackSender_CanceledDrainerHandsOffToTrackedPeer(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	store := stores.callbacks
	leaseUUID := testLeaseUUID("canceled-drainer-handoff")
	_, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:        leaseUUID,
		CallbackURL:      "https://fred.example/callbacks/provision",
		DeliveryKind:     CallbackDeliveryKindOperation,
		Status:           backend.CallbackStatusSuccess,
		BackendStorageID: stores.storage.ID().String(),
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)

	ownerStarted := make(chan struct{})
	var ownerStartedOnce sync.Once
	ownerClient := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		ownerStartedOnce.Do(func() { close(ownerStarted) })
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	owner := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: ownerClient,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff:        &zeroBackoff,
		ReplayInterval: time.Hour,
	}, callbackSenderTestLifetime(ownerCtx))
	ownerDone := make(chan struct{})
	go func() {
		defer close(ownerDone)
		owner.RunReplayLoop()
	}()
	select {
	case <-ownerStarted:
	case <-time.After(time.Second):
		t.Fatal("first replay loop did not acquire callback drain ownership")
	}

	peerDelivered := make(chan struct{})
	var peerDeliveredOnce sync.Once
	peerClient := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		peerDeliveredOnce.Do(func() { close(peerDelivered) })
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	peer := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: peerClient,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff:        &zeroBackoff,
		ReplayInterval: time.Hour,
	}, callbackSenderTestLifetime(peerCtx))
	// Seed the scheduler state produced when this peer previously discovered the
	// durable lease but lost drain election to owner. The one-hour periodic retry
	// makes the canceled owner's typed handoff the only prompt path to delivery.
	peerQueue := newCallbackReplayQueue()
	peerQueue.dormant[leaseUUID] = struct{}{}
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		peer.runReplayLoop(peerQueue)
	}()
	t.Cleanup(func() {
		cancelOwner()
		cancelPeer()
		for name, done := range map[string]<-chan struct{}{
			"owner": ownerDone,
			"peer":  peerDone,
		} {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Errorf("%s replay loop did not stop during cleanup", name)
			}
		}
	})
	require.Eventually(t, func() bool {
		store.replaySubscribersMu.Lock()
		defer store.replaySubscribersMu.Unlock()
		return len(store.replaySubscribers) == 2
	}, time.Second, time.Millisecond, "both replay owners did not subscribe")

	cancelOwner()
	select {
	case <-ownerDone:
	case <-time.After(time.Second):
		t.Fatal("canceled drain owner did not stop")
	}
	select {
	case <-peerDelivered:
	case <-time.After(time.Second):
		t.Fatal("peer did not take over callback delivery before the one-hour interval")
	}
	require.Eventually(t, func() bool {
		pending, listErr := store.ListPending()
		return listErr == nil && len(pending) == 0
	}, time.Second, time.Millisecond, "peer did not commit precise callback removal")

	cancelPeer()
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		t.Fatal("peer replay loop did not stop")
	}
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendCallback_ExactCompletionBlocksNewerLifecycleUntilFIFOCanDrain(t *testing.T) {
	var exactAvailable atomic.Bool
	var lifecycleAttempts atomic.Int32
	var deliveredMu sync.Mutex
	var deliveredOrder []backend.CallbackStatus
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/exact"+callbackurl.ProvisionPath && !exactAvailable.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/lifecycle"+callbackurl.ProvisionPath {
			lifecycleAttempts.Add(1)
		}
		deliveredMu.Lock()
		deliveredOrder = append(deliveredOrder, payload.Status)
		deliveredMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, http.DefaultClient, "secret")
	leaseUUID := testLeaseUUID("lease-1")
	exactURL := server.URL + "/exact" + callbackurl.ProvisionPath
	beginCallbackSenderOperationIntent(t, store, leaseUUID, exactURL, "docker", s.storageIdentity)
	s.sendOperationCallbackForTest(
		leaseUUID, exactURL, "docker",
		backend.CallbackStatusFailed, "definitively refused",
	)
	s.sendLifecycleCallbackForTest(
		leaseUUID, server.URL+"/lifecycle"+callbackurl.ProvisionPath,
		"docker", backend.CallbackStatusDeprovisioned, "", false,
	)

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, server.URL+"/exact"+callbackurl.ProvisionPath, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, server.URL+"/lifecycle"+callbackurl.ProvisionPath, pending[1].CallbackURL)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, pending[1].Status)
	assert.Less(t, pending[0].Sequence, pending[1].Sequence)
	assert.Zero(t, lifecycleAttempts.Load(), "new lifecycle callback must not overtake the exact completion")

	exactAvailable.Store(true)
	s.replayPendingCallbacks()
	deliveredMu.Lock()
	assert.Equal(t, []backend.CallbackStatus{
		backend.CallbackStatusFailed,
		backend.CallbackStatusDeprovisioned,
	}, deliveredOrder)
	deliveredMu.Unlock()
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackSender_ConcurrentReplayAndLiveEnqueueRemainFIFO(t *testing.T) {
	exactStarted := make(chan struct{})
	releaseExact := make(chan struct{})
	var exactOnce sync.Once
	var deliveredMu sync.Mutex
	var delivered []string
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		deliveredMu.Lock()
		delivered = append(delivered, req.URL.Path)
		deliveredMu.Unlock()
		if req.URL.Path == "/exact"+callbackurl.ProvisionPath {
			exactOnce.Do(func() { close(exactStarted) })
			<-releaseExact
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "https://fred.example/exact/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	s := newTestSender(t, store, client, "secret")

	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		s.replayPendingCallbacks()
	}()
	select {
	case <-exactStarted:
	case <-time.After(time.Second):
		t.Fatal("replay did not start the exact completion")
	}

	liveDone := make(chan struct{})
	go func() {
		defer close(liveDone)
		s.sendLifecycleCallbackForTest(
			testLeaseUUID("lease-1"),
			"https://fred.example/lifecycle/callbacks/provision",
			"docker",
			backend.CallbackStatusFailed,
			"container exited",
			false,
		)
	}()
	select {
	case <-liveDone:
	case <-time.After(time.Second):
		t.Fatal("live enqueue waited behind callback HTTP")
	}
	pending, err := store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.Len(t, pending, 2, "live enqueue must durably append while replay delivers the head")
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[1].DeliveryKind)
	deliveredMu.Lock()
	assert.Equal(t, []string{"/exact" + callbackurl.ProvisionPath}, delivered,
		"durable enqueue must not perform HTTP inline")
	deliveredMu.Unlock()

	close(releaseExact)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("replay did not finish")
	}
	deliveredMu.Lock()
	assert.Equal(t, []string{"/exact" + callbackurl.ProvisionPath, "/lifecycle" + callbackurl.ProvisionPath}, delivered,
		"the same drainer must re-list and preserve wire FIFO")
	deliveredMu.Unlock()
	pending, err = store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	assert.Empty(t, pending)
	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "concurrent replay/send must retire its mutation lock")
	s.deliveryLocksMu.Unlock()
	s.drainLocksMu.Lock()
	assert.Empty(t, s.drainLocks, "completed replay must retire its drainer lock")
	s.drainLocksMu.Unlock()
}

func TestCallbackSender_ConcurrentReplaysShareOneStoreDrainer(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("shared-drainer")
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    leaseUUID,
		CallbackURL:  "https://fred.example/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	senderA := newTestSender(t, store, client, "secret")
	senderB := newTestSender(t, store, client, "secret")

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		senderA.replayPendingCallbacks()
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first sender did not begin callback delivery")
	}

	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		senderB.replayPendingCallbacks()
	}()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second replay waited behind the elected wire drainer")
	}
	assert.Equal(t, int32(1), requests.Load(), "shared store must elect exactly one wire drainer per lease")

	releaseOnce.Do(func() { close(release) })
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first sender did not finish after callback release")
	}
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	assert.Empty(t, pending)
	store.drainLocksMu.Lock()
	assert.Empty(t, store.drainLocks, "shared drain ownership must retire after delivery")
	store.drainLocksMu.Unlock()
}

func TestCallbackSender_InFlightLifecycleReplacementSurvivesPreciseRemoval(t *testing.T) {
	oldStarted := make(chan struct{})
	releaseOld := make(chan struct{})
	var oldStartedOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOld) }) })
	var deliveredMu sync.Mutex
	var delivered []string
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		deliveredMu.Lock()
		delivered = append(delivered, req.URL.Path)
		deliveredMu.Unlock()
		if req.URL.Path == "/old"+callbackurl.ProvisionPath {
			oldStartedOnce.Do(func() { close(oldStarted) })
			<-releaseOld
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("in-flight-lifecycle-replacement")
	oldEntry, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    leaseUUID,
		CallbackURL:  "https://fred.example/old/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		Error:        "old observation",
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	sender := newTestSender(t, store, client, "secret")

	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		sender.replayPendingCallbacks()
	}()
	select {
	case <-oldStarted:
	case <-time.After(time.Second):
		t.Fatal("old lifecycle observation did not begin delivery")
	}

	enqueueDone := make(chan struct{})
	go func() {
		defer close(enqueueDone)
		sender.sendLifecycleCallbackForTest(
			leaseUUID,
			"https://fred.example/new/callbacks/provision",
			"docker",
			backend.CallbackStatusFailed,
			"new observation",
			false,
		)
	}()
	select {
	case <-enqueueDone:
	case <-time.After(time.Second):
		t.Fatal("lifecycle replacement waited behind callback HTTP")
	}
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 1, "new lifecycle observation must coalesce the in-flight durable row")
	assert.NotEqual(t, oldEntry.DeliveryID, pending[0].DeliveryID)
	assert.Equal(t, "https://fred.example/new/callbacks/provision", pending[0].CallbackURL)

	releaseOnce.Do(func() { close(releaseOld) })
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("drainer did not re-list the lifecycle replacement")
	}
	deliveredMu.Lock()
	assert.Equal(t, []string{
		"/old" + callbackurl.ProvisionPath,
		"/new" + callbackurl.ProvisionPath,
	}, delivered, "old precise removal must not delete its replacement")
	deliveredMu.Unlock()
	pending, err = store.listPending(leaseUUID)
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackSender_CanceledDrainerLeavesRowForReplacementSender(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	clientA := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		startedOnce.Do(func() { close(started) })
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("replace-canceled-drainer")
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    leaseUUID,
		CallbackURL:  "https://fred.example/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	stopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	senderA := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: clientA,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff: &zeroBackoff,
	}, callbackSenderTestLifetime(stopCtx))

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		senderA.replayPendingCallbacks()
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first sender did not begin callback delivery")
	}
	cancel()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("canceled sender did not release callback drain ownership")
	}
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 1, "cancellation must preserve the durable head")
	store.drainLocksMu.Lock()
	assert.Empty(t, store.drainLocks, "cancellation must retire drain ownership")
	store.drainLocksMu.Unlock()

	var replacementRequests atomic.Int32
	senderB := newTestSender(t, store, &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		replacementRequests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}, "secret")
	senderB.replayPendingCallbacks()
	assert.Equal(t, int32(1), replacementRequests.Load())
	pending, err = store.listPending(leaseUUID)
	require.NoError(t, err)
	assert.Empty(t, pending, "replacement sender must consume the preserved head")
}

func TestCallbackSender_ExpirySkipsBusyLeaseWithoutMutatingDrain(t *testing.T) {
	exactStarted := make(chan struct{})
	releaseExact := make(chan struct{})
	var exactOnce sync.Once
	var lifecycleRequests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/exact" + callbackurl.ProvisionPath:
			exactOnce.Do(func() {
				close(exactStarted)
				<-releaseExact
			})
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		case "/lifecycle" + callbackurl.ProvisionPath:
			lifecycleRequests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		default:
			return callbackHTTPResponse(http.StatusNotFound), nil
		}
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "https://fred.example/exact/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "https://fred.example/lifecycle/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	s := newTestSender(t, store, client, "secret")

	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		s.replayPendingCallbacks()
	}()
	select {
	case <-exactStarted:
	case <-time.After(time.Second):
		t.Fatal("replay did not begin exact delivery")
	}

	cleanupDone := make(chan struct{})
	var removed int
	var cleanupErr error
	go func() {
		defer close(cleanupDone)
		removed, cleanupErr = store.removeOlderThan(24 * time.Hour)
	}()
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup blocked behind an in-flight lease drain")
	}
	require.NoError(t, cleanupErr)
	assert.Zero(t, removed, "cleanup must skip a lease whose drain lock is busy")
	pending, err := store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.Len(t, pending, 2, "cleanup must not mutate an in-flight drain snapshot")

	close(releaseExact)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("replay did not stop after exact delivery failure")
	}
	assert.Zero(t, lifecycleRequests.Load(), "lifecycle suffix must not pass the failed exact head")
	pending, err = store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[1].DeliveryKind)

	removed, err = store.removeOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Zero(t, removed, "exact operation evidence remains non-expiring after delivery releases the lock")
	pending, err = store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
}

func TestDeliverCallback_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	ok := s.deliverCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, []byte(`{"test":true}`))
	assert.True(t, ok)
}

func TestDeliverCallback_RetriesOnServerError(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	ok := s.deliverCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, []byte(`{}`))
	assert.True(t, ok)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestDeliverCallback_AllRetriesFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	ok := s.deliverCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, []byte(`{}`))
	assert.False(t, ok)
}

func TestCallbackSenderRejectsRedirectEvenWhenSuppliedClientFollows(t *testing.T) {
	var redirectedRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	var redirectResponses atomic.Int32
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectResponses.Add(1)
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	client := redirect.Client()
	require.Nil(t, client.CheckRedirect, "test client must follow redirects by default")
	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff: &zeroBackoff,
	})

	delivered := s.deliverCallback(
		testLeaseUUID("redirect"), redirect.URL, []byte(`{"authority":"signed"}`),
	)

	assert.False(t, delivered)
	assert.Equal(t, int32(CallbackMaxAttempts), redirectResponses.Load())
	assert.Zero(t, redirectedRequests.Load(),
		"an HMAC-signed request must never be forwarded to a redirect target")
	assert.Nil(t, client.CheckRedirect, "constructor must not mutate the caller's client")
}

func TestCallbackSenderStripsAmbientCookieJarWithoutMutatingClient(t *testing.T) {
	cookies := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookies <- r.Header.Get("Cookie")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	callbackRequest, err := http.NewRequest(http.MethodPost, server.URL+callbackurl.ProvisionPath, nil)
	require.NoError(t, err)
	jar.SetCookies(callbackRequest.URL, []*http.Cookie{{Name: "ambient", Value: "credential"}})
	client := server.Client()
	client.Jar = jar
	require.NotEmpty(t, client.Jar.Cookies(callbackRequest.URL))

	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: client,
		Secret:     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		Logger:     slog.Default(),

		Backoff: &zeroBackoff,
	})
	delivered := s.deliverCallback(
		testLeaseUUID("ambient-cookie"), server.URL+callbackurl.ProvisionPath, []byte(`{"authority":"signed"}`),
	)

	assert.True(t, delivered)
	assert.Empty(t, <-cookies, "callback delivery must not carry ambient cookies")
	assert.Same(t, jar, client.Jar, "constructor must not mutate the caller's client")
	assert.NotEmpty(t, client.Jar.Cookies(callbackRequest.URL),
		"stripping sender authority must not clear the caller's cookie jar")
}

func TestDeliverCallback_ShutdownAbortsRetry(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	longBackoff := [CallbackMaxAttempts]time.Duration{0, 5 * time.Second, 5 * time.Second}
	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: server.Client(),
		Logger:     slog.Default(),

		Backoff: &longBackoff,
	}, callbackSenderTestLifetime(ctx))

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	ok := s.deliverCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, []byte(`{}`))
	assert.False(t, ok)
	assert.LessOrEqual(t, attempts.Load(), int32(2))
}

func TestDeliverCallback_ConfiguredDeliveryTimeoutOutlivesFormerCaps(t *testing.T) {
	// Scale the former 10-second sender and 30-second client caps down to
	// milliseconds. A synchronous Fred application that finishes after both
	// boundaries must still succeed when the sender owns the request deadline.
	const (
		formerSenderCap  = 10 * time.Millisecond
		formerClientCap  = 30 * time.Millisecond
		applicationDelay = 50 * time.Millisecond
		deliveryTimeout  = 200 * time.Millisecond
	)
	require.Greater(t, formerClientCap, formerSenderCap)
	require.Greater(t, applicationDelay, formerClientCap)
	require.Greater(t, deliveryTimeout, applicationDelay)

	var attempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		timer := time.NewTimer(applicationDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
			return callbackHTTPResponse(http.StatusNoContent), nil
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	})}
	assert.Zero(t, client.Timeout)
	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: client,
		Logger:     slog.Default(),

		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	})

	started := time.Now()
	ok := s.deliverCallback(testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", []byte(`{}`))

	assert.True(t, ok)
	assert.Equal(t, int32(1), attempts.Load())
	assert.GreaterOrEqual(t, time.Since(started), applicationDelay)
}

func TestDeliverCallback_AttemptDeadlineDefersRemainingRetries(t *testing.T) {
	const deliveryTimeout = 25 * time.Millisecond
	var attempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: client,
		Logger:     slog.Default(),

		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	})

	delivered := s.deliverCallback(
		testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", []byte(`{}`),
	)

	assert.False(t, delivered)
	assert.Equal(t, int32(1), attempts.Load(),
		"one exhausted request budget must defer to periodic replay instead of starting two more full-budget attempts")
}

func TestDeliverCallback_HTTPFailureRetrySharesInlineDeadline(t *testing.T) {
	const deliveryTimeout = time.Minute
	stopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	firstAttemptStarted := make(chan struct{})
	releaseFirstAttempt := make(chan struct{})
	secondAttemptStarted := make(chan struct{})
	var attempts atomic.Int32
	deadlines := make(chan time.Time, 2)
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		deadline, ok := req.Context().Deadline()
		if !ok {
			return nil, fmt.Errorf("callback request has no deadline")
		}
		deadlines <- deadline
		attempt := attempts.Add(1)
		switch attempt {
		case 1:
			close(firstAttemptStarted)
			select {
			case <-releaseFirstAttempt:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		case 2:
			close(secondAttemptStarted)
			<-req.Context().Done()
			return nil, req.Context().Err()
		default:
			return nil, fmt.Errorf("unexpected callback attempt %d", attempt)
		}
	})}
	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: client,
		Logger:     slog.Default(),

		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	}, callbackSenderTestLifetime(stopCtx))

	delivered := make(chan bool, 1)
	go func() {
		delivered <- s.deliverCallback(
			testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", []byte(`{}`),
		)
	}()
	select {
	case <-firstAttemptStarted:
	case <-time.After(time.Second):
		t.Fatal("first callback attempt did not start")
	}
	// Keep the first request parked until the test deliberately returns its
	// 503, avoiding sleeps and elapsed-time assertions for synchronization.
	close(releaseFirstAttempt)
	select {
	case <-secondAttemptStarted:
	case <-time.After(time.Second):
		t.Fatal("second callback attempt did not start")
	}
	cancel()
	select {
	case ok := <-delivered:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("callback delivery did not stop after sender cancellation")
	}

	assert.Equal(t, int32(2), attempts.Load(),
		"a failed response may use the remaining budget once but must not mint another full timeout")
	firstDeadline := <-deadlines
	secondDeadline := <-deadlines
	assert.True(t, firstDeadline.Equal(secondDeadline),
		"all retry attempts must inherit the exact same delivery deadline")
}

func TestDeliverCallback_StopContextCancelsInFlightRequest(t *testing.T) {
	stopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	requestStarted := make(chan struct{})
	requestCanceled := make(chan error, 1)
	var attempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		close(requestStarted)
		<-req.Context().Done()
		requestCanceled <- req.Context().Err()
		return nil, req.Context().Err()
	})}
	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: client,
		Logger:     slog.Default(),

		DeliveryTimeout: time.Minute,
	}, callbackSenderTestLifetime(stopCtx))

	delivered := make(chan bool, 1)
	go func() {
		delivered <- s.deliverCallback(
			testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", []byte(`{}`),
		)
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("callback request did not start")
	}
	cancel()

	select {
	case err := <-requestCanceled:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("request context did not inherit sender cancellation")
	}
	select {
	case ok := <-delivered:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("callback delivery did not stop after sender cancellation")
	}
	assert.Equal(t, int32(1), attempts.Load())
}

func TestReplayPendingCallbacks_NilStore(t *testing.T) {
	s := newTestSender(t, nil, http.DefaultClient, "secret")
	// Should not panic
	s.replayPendingCallbacks()
}

func TestReplayPendingCallbacks_EmptyStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, http.DefaultClient, "secret")
	s.replayPendingCallbacks()
}

func TestReplayPendingCallbacks_DeliversAndRemoves(t *testing.T) {
	var receivedMu sync.Mutex
	var received []backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		receivedMu.Lock()
		received = append(received, p)
		receivedMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  server.URL + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-2"),
		CallbackURL:  server.URL + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Error:        "pull failed",
		CreatedAt:    time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.replayPendingCallbacks()

	receivedMu.Lock()
	assert.Len(t, received, 2)
	receivedMu.Unlock()

	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestReplayPendingCallbacks_PartialFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/success"+callbackurl.ProvisionPath {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  server.URL + "/success" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-2"),
		CallbackURL:  server.URL + "/failure" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Error:        "error",
		CreatedAt:    time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.replayPendingCallbacks()

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, testLeaseUUID("lease-2"), pending[0].LeaseUUID)
}

func TestReplayPendingCallbacks_FailureBlocksOnlyItsLease(t *testing.T) {
	var blockedAttempts atomic.Int32
	var overtakingLifecycleAttempts atomic.Int32
	var otherLeaseAttempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/blocked-exact" + callbackurl.ProvisionPath:
			blockedAttempts.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		case "/same-lease-lifecycle" + callbackurl.ProvisionPath:
			overtakingLifecycleAttempts.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/other-lease" + callbackurl.ProvisionPath:
			otherLeaseAttempts.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("blocked-lease"),
		CallbackURL:  server.URL + "/blocked-exact" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("blocked-lease"),
		CallbackURL:  server.URL + "/same-lease-lifecycle" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("healthy-lease"),
		CallbackURL:  server.URL + "/other-lease" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))

	newTestSender(t, store, server.Client(), "secret").replayPendingCallbacks()

	assert.Equal(t, int32(CallbackMaxAttempts), blockedAttempts.Load())
	assert.Zero(t, overtakingLifecycleAttempts.Load(),
		"a newer lifecycle observation must not overtake the failed exact completion")
	assert.Equal(t, int32(1), otherLeaseAttempts.Load(),
		"one lease's delivery failure must not stop another lease's drain")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, testLeaseUUID("blocked-lease"), pending[0].LeaseUUID)
	assert.Equal(t, testLeaseUUID("blocked-lease"), pending[1].LeaseUUID)
}

func TestReplayPendingCallbacks_BlockedLeaseDoesNotDelayAnotherLease(t *testing.T) {
	blockedStarted := make(chan struct{})
	releaseBlocked := make(chan struct{})
	otherDelivered := make(chan struct{})
	var blockedOnce sync.Once
	var otherOnce sync.Once
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/blocked" + callbackurl.ProvisionPath:
			blockedOnce.Do(func() { close(blockedStarted) })
			<-releaseBlocked
		case "/other" + callbackurl.ProvisionPath:
			otherOnce.Do(func() { close(otherDelivered) })
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	for _, entry := range []CallbackEntry{
		{
			LeaseUUID:    testLeaseUUID("blocked-lease"),
			CallbackURL:  "https://fred.example/blocked/callbacks/provision",
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		},
		{
			LeaseUUID:    testLeaseUUID("other-lease"),
			CallbackURL:  "https://fred.example/other/callbacks/provision",
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		},
	} {
		require.NoError(t, store.storeValidTest(entry))
	}
	s := newTestSender(t, store, client, "secret")
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		s.replayPendingCallbacks()
	}()

	select {
	case <-blockedStarted:
	case <-time.After(time.Second):
		t.Fatal("blocked lease did not begin delivery")
	}
	select {
	case <-otherDelivered:
	case <-time.After(time.Second):
		t.Fatal("another lease was delayed behind the blocked replay")
	}
	close(releaseBlocked)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("replay did not finish after blocked lease was released")
	}
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackSender_FreshCommitBypassesSaturatedReplayBacklog(t *testing.T) {
	releaseOne := make(chan struct{}, 1)
	freshDelivered := make(chan struct{})
	var freshOnce sync.Once
	var active atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/fresh"+callbackurl.ProvisionPath {
			freshOnce.Do(func() { close(freshDelivered) })
			return callbackHTTPResponse(http.StatusNoContent), nil
		}
		active.Add(1)
		defer active.Add(-1)
		select {
		case <-releaseOne:
			return callbackHTTPResponse(http.StatusNoContent), nil
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "cb.db"),
	})
	require.NoError(t, err)
	defer store.Close()
	for i := range callbackReplayWorkerLimit * 4 {
		require.NoError(t, store.storeValidTest(CallbackEntry{
			LeaseUUID:    testLeaseUUID(fmt.Sprintf("saturated-backlog-%03d", i)),
			CallbackURL:  "https://fred.example/backlog" + callbackurl.ProvisionPath,
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		}))
	}

	stopCtx, cancel := context.WithCancel(context.Background())
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: client, Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff, ReplayInterval: time.Hour,
		DeliveryTimeout: time.Minute,
	}, callbackSenderTestLifetime(stopCtx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		sender.RunReplayLoop()
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("replay scheduler did not stop")
		}
	})
	require.Eventually(t, func() bool {
		return active.Load() == int32(callbackReplayWorkerLimit)
	}, time.Second, time.Millisecond, "initial backlog did not saturate every replay worker")

	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("fresh-during-saturation"),
		CallbackURL:  "https://fred.example/fresh" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	require.Eventually(t, func() bool { return sender.replayWake.pendingCount() == 0 },
		time.Second, time.Millisecond, "scheduler did not consume the durable commit wake")

	// Let exactly one saturated worker finish. The next dispatch must be the
	// newly discovered healthy lease, not the next lexicographic backlog item.
	releaseOne <- struct{}{}
	select {
	case <-freshDelivered:
	case <-time.After(time.Second):
		t.Fatal("fresh callback remained behind the saturated outage backlog")
	}
}

func TestCallbackSender_DropsExpiredLifecycleHeadBeforeWireDelivery(t *testing.T) {
	var requests atomic.Int32
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "cb.db"),
		MaxAge: time.Hour,
	})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("expired-lifecycle-head"),
		CallbackURL:  "https://fred.example/lifecycle" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		CreatedAt:    time.Now().Add(-2 * time.Hour),
	}))

	stopCtx, cancel := context.WithCancel(context.Background())
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff, ReplayInterval: time.Hour,
	}, callbackSenderTestLifetime(stopCtx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		sender.RunReplayLoop()
	}()
	require.Eventually(t, func() bool {
		pending, listErr := store.ListPending()
		return listErr == nil && len(pending) == 0
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("replay scheduler did not stop")
	}
	assert.Zero(t, requests.Load(), "expired observation must never race cleanup onto the wire")
}

func TestReplayPendingCallbacks_BoundsFanout(t *testing.T) {
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		current := active.Add(1)
		for {
			previous := maxActive.Load()
			if current <= previous || maxActive.CompareAndSwap(previous, current) {
				break
			}
		}
		<-release
		active.Add(-1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	for i := range callbackReplayWorkerLimit + 1 {
		require.NoError(t, store.storeValidTest(CallbackEntry{
			LeaseUUID:    testLeaseUUID(fmt.Sprintf("lease-%02d", i)),
			CallbackURL:  fmt.Sprintf("https://fred.example/%02d/callbacks/provision", i),
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		}))
	}
	s := newTestSender(t, store, client, "secret")
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		s.replayPendingCallbacks()
	}()

	require.Eventually(t, func() bool {
		return active.Load() == int32(callbackReplayWorkerLimit)
	}, time.Second, time.Millisecond)
	assert.Equal(t, int32(callbackReplayWorkerLimit), maxActive.Load(),
		"replay must not create one simultaneous retry chain per queued lease")
	close(release)
	select {
	case <-replayDone:
	case <-time.After(5 * time.Second):
		t.Fatal("bounded replay did not finish")
	}
	assert.LessOrEqual(t, maxActive.Load(), int32(callbackReplayWorkerLimit))
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestReplayPendingCallbacks_RecoversPerLeasePanicAndContinuesWorker(t *testing.T) {
	var healthyDelivered atomic.Int32
	var replayPanics atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/healthy"+callbackurl.ProvisionPath {
			healthyDelivered.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		}
		panic("synthetic callback transport panic")
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	for i := range callbackReplayWorkerLimit {
		require.NoError(t, store.storeValidTest(CallbackEntry{
			LeaseUUID:    testLeaseUUID(fmt.Sprintf("panic-lease-%02d", i)),
			CallbackURL:  fmt.Sprintf("https://fred.example/panic-%02d/callbacks/provision", i),
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		}))
	}
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("healthy-lease"),
		CallbackURL:  "https://fred.example/healthy/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff:       &zeroBackoff,
		OnReplayPanic: func(any) { replayPanics.Add(1) },
	})

	assert.NotPanics(t, s.replayPendingCallbacks)
	assert.Equal(t, int32(callbackReplayWorkerLimit), replayPanics.Load())
	assert.Equal(t, int32(1), healthyDelivered.Load(),
		"workers must continue consuming unrelated lease jobs after a recovered panic")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, callbackReplayWorkerLimit)
	for _, entry := range pending {
		assert.NotEqual(t, testLeaseUUID("healthy-lease"), entry.LeaseUUID)
	}
	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "panic recovery must release journal-mutation locks")
	s.deliveryLocksMu.Unlock()
	s.drainLocksMu.Lock()
	assert.Empty(t, s.drainLocks, "panic recovery must release wire-drain ownership")
	s.drainLocksMu.Unlock()
}

func TestReplayPendingCallbacks_CanceledSenderDoesNotWalkQueue(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	for i := range callbackReplayWorkerLimit * 2 {
		require.NoError(t, store.storeValidTest(CallbackEntry{
			LeaseUUID:    testLeaseUUID(fmt.Sprintf("lease-%02d", i)),
			CallbackURL:  fmt.Sprintf("https://fred.example/%02d/callbacks/provision", i),
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		}))
	}
	stopCtx, cancel := context.WithCancel(context.Background())
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),

		Backoff: &zeroBackoff,
	}, callbackSenderTestLifetime(stopCtx))
	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.replayPendingCallbacks()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled replay blocked while feeding the bounded worker pool")
	}
	assert.Zero(t, requests.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Len(t, pending, callbackReplayWorkerLimit*2)
}

func TestReplayPendingCallbacks_StopsLeaseAfterFirstFailureAfterReopen(t *testing.T) {
	var lifecycleAttempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/exact"+callbackurl.ProvisionPath {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		lifecycleAttempts.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	createdAt := time.Now()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  server.URL + "/exact" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		Backend:      "docker",
		CreatedAt:    createdAt,
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  server.URL + "/lifecycle" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		Backend:      "docker",
		Error:        "container exited",
		CreatedAt:    createdAt.Add(time.Second),
	}))
	require.NoError(t, store.Close())

	store, err = newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()
	newTestSender(t, store, server.Client(), "secret").replayPendingCallbacks()

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, server.URL+"/exact"+callbackurl.ProvisionPath, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, server.URL+"/lifecycle"+callbackurl.ProvisionPath, pending[1].CallbackURL)
	assert.Zero(t, lifecycleAttempts.Load(), "the failed exact completion is a per-lease FIFO barrier")
}

func TestReplayPendingCallbacks_LegacyV013EntryRemainsQuarantined(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	storeLegacyCallback(t, store, v013CallbackEntryForTest{
		LeaseUUID:   testLeaseUUID("lease-v013"),
		CallbackURL: server.URL + callbackurl.ProvisionPath,
		Success:     true,
		CreatedAt:   time.Now(),
	})
	require.ErrorIs(t, store.Healthy(), errLegacyCallbackOutboxNotDrained)
	newTestSender(t, store, server.Client(), "secret").replayPendingCallbacks()

	pending, err := store.ListPending()
	require.ErrorIs(t, err, errLegacyCallbackOutboxNotDrained)
	assert.Empty(t, pending, "a current runtime must never materialize a pre-identity callback")
	assert.Zero(t, requests.Load(),
		"a current sender must not restamp a pre-identity row with the mounted lineage")
	require.NoError(t, store.Close())

	reopened, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	assert.Nil(t, reopened)
	require.ErrorIs(t, err, errLegacyCallbackOutboxNotDrained,
		"current startup must reject an undeliverable pre-identity queue")
}

// TestReplayPendingCallbacks_PreservesStatusAndBackend verifies that current
// identity-bearing entries preserve both status classes and optional backend
// metadata across durable replay.
func TestReplayPendingCallbacks_PreservesStatusAndBackend(t *testing.T) {
	var receivedMu sync.Mutex
	var received []backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		receivedMu.Lock()
		received = append(received, p)
		receivedMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	// Current v2 rows are isolated from the v0.13 bucket and represent their
	// outcome exactly once through Status.
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-new"),
		CallbackURL:  server.URL + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusDeprovisioned,
		Backend:      "docker",
		CreatedAt:    time.Now(),
	}))
	// Current exact failure without optional backend metadata.
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-legacy"),
		CallbackURL:  server.URL + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Error:        "image pull failed",
		CreatedAt:    time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.replayPendingCallbacks()

	receivedMu.Lock()
	defer receivedMu.Unlock()
	require.Len(t, received, 2)
	byID := map[string]backend.CallbackPayload{}
	for _, p := range received {
		byID[p.LeaseUUID] = p
	}
	assert.Equal(t, backend.CallbackStatusDeprovisioned, byID[testLeaseUUID("lease-new")].Status)
	assert.Equal(t, "docker", byID[testLeaseUUID("lease-new")].Backend)
	assert.Equal(t, backend.CallbackStatusFailed, byID[testLeaseUUID("lease-legacy")].Status)
	assert.Empty(t, byID[testLeaseUUID("lease-legacy")].Backend)
}

// TestSendCallback_ThreadsRetainedFlag verifies (ENG-329 #7) that the lifecycle
// callback's retained argument is threaded into the wire payload AND persisted on the
// CallbackEntry, so a restart-replay keeps the flag.
func TestSendCallback_ThreadsRetainedFlag(t *testing.T) {
	var received backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		// Fail delivery so the entry stays in the store for the assertions below.
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, server.Client(), "secret")
	s.sendLifecycleCallbackForTest(testLeaseUUID("lease-r"), server.URL+callbackurl.ProvisionPath, "docker", backend.CallbackStatusDeprovisioned, "", true)

	// The publisher only commits the row. Replay owns wire delivery.
	s.replayPendingCallbacks()

	// Wire payload carried the flag.
	assert.True(t, received.Retained, "retained flag must be threaded into the wire payload")

	// Persisted entry retained the flag (delivery failed → entry remains).
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.True(t, pending[0].Retained, "retained flag must be persisted on the CallbackEntry")
}

// TestReplayPendingCallbacks_PreservesRetained verifies the replay path re-sends
// the persisted Retained flag (so a callback delivered only after restart still
// tells providerd the data was retained).
func TestReplayPendingCallbacks_PreservesRetained(t *testing.T) {
	var received backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-r"),
		CallbackURL:  server.URL + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusDeprovisioned,
		Backend:      "docker",
		Retained:     true,
		CreatedAt:    time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.replayPendingCallbacks()

	assert.Equal(t, testLeaseUUID("lease-r"), received.LeaseUUID)
	assert.True(t, received.Retained, "replayed callback must preserve the retained flag")
}

// TestSendCallbackV2PersistsOneOutcomeRepresentation pins the current schema:
// Status is authoritative and the isolated v2 bucket never persists a derived
// success bit which could contradict it.
func TestSendCallbackV2PersistsOneOutcomeRepresentation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // force persistence; never delivers
	}))
	defer server.Close()

	cases := []struct {
		name   string
		status backend.CallbackStatus
	}{
		{"success", backend.CallbackStatusSuccess},
		{"failed", backend.CallbackStatusFailed},
		{"deprovisioned", backend.CallbackStatusDeprovisioned},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "cb.db")
			store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
			require.NoError(t, err)
			defer store.Close()

			s := newTestSender(t, store, server.Client(), "secret")
			s.sendLifecycleCallbackForTest(
				testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath,
				"docker", tc.status, "", false,
			)

			pending, err := store.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			assert.Equal(t, tc.status, pending[0].Status)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				lease := tx.Bucket(callbackV2BucketName).Bucket([]byte(pending[0].LeaseUUID))
				require.NotNil(t, lease)
				data := lease.Get([]byte(pending[0].storageKey))
				require.NotNil(t, data)
				assert.False(t, bytes.Contains(data, []byte(`"success":`)))
				return nil
			}))
		})
	}
}

func TestReportDelivery_NilHook(t *testing.T) {
	s := newTestSender(t, nil, http.DefaultClient, "secret")
	// Should not panic
	s.reportDelivery("success")
	s.reportDelivery("failure")
}

func TestReportDelivery_WithHook(t *testing.T) {
	var outcomes []string
	s := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),

		OnDelivery: func(outcome string) { outcomes = append(outcomes, outcome) },
	})

	s.reportDelivery("success")
	s.reportDelivery("failure")

	assert.Equal(t, []string{"success", "failure"}, outcomes)
}

type endlessCallbackResponseBody struct {
	readBytes atomic.Int64
	closed    atomic.Bool
}

func (body *endlessCallbackResponseBody) Read(p []byte) (int, error) {
	for index := range p {
		p[index] = 'x'
	}
	body.readBytes.Add(int64(len(p)))
	return len(p), nil
}

func (body *endlessCallbackResponseBody) Close() error {
	body.closed.Store(true)
	return nil
}

func TestTrySendCallbackBoundsUnusedResponseBody(t *testing.T) {
	body := &endlessCallbackResponseBody{}
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
			Request:    req,
		}, nil
	})}
	sender := mustNewEphemeralCallbackSenderForTest(t, CallbackSenderConfig{
		HTTPClient: client,
		Logger:     slog.Default(),
	})

	outcome := sender.trySendCallback(
		context.Background(), testLeaseUUID("bounded-response"),
		"https://fred.example/callbacks/provision", []byte(`{}`),
	)

	assert.Equal(t, callbackAttemptDelivered, outcome)
	assert.LessOrEqual(t, body.readBytes.Load(), callbackResponseDrainLimit+1)
	assert.True(t, body.closed.Load(), "oversized response body must be closed after the bounded prefix")
}

func TestReplayPendingCallbacks_StoreErrorHookPanicIsContained(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "cb.db"),
	})
	require.NoError(t, err)
	require.NoError(t, store.Close())
	var hookCalls atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: http.DefaultClient, Secret: "secret",
		Logger: slog.Default(), Backoff: &zeroBackoff,
		OnStoreError: func() {
			hookCalls.Add(1)
			panic("store observer panic")
		},
	})

	assert.NotPanics(t, sender.replayPendingCallbacks)
	assert.NotPanics(t, sender.replayPendingCallbacks,
		"one bad observer invocation must not disable a future level-triggered replay")
	assert.Equal(t, int32(2), hookCalls.Load())
}

func TestReplayPendingCallbacks_DeliveryHookPanicPreservesFailedHead(t *testing.T) {
	var attempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		return callbackHTTPResponse(http.StatusServiceUnavailable), nil
	})}
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "cb.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("delivery-hook-panic"),
		CallbackURL:  "https://fred.example/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	var hookCalls atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: client, Secret: "secret", Logger: slog.Default(),
		Backoff: &zeroBackoff,
		OnDelivery: func(string) {
			hookCalls.Add(1)
			panic("delivery observer panic")
		},
	})

	assert.NotPanics(t, sender.replayPendingCallbacks)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "observer panic cannot discard a failed durable delivery")
	assert.Equal(t, int32(CallbackMaxAttempts), attempts.Load())

	assert.NotPanics(t, sender.replayPendingCallbacks,
		"the durable head must remain eligible for a future replay")
	assert.Equal(t, int32(2), hookCalls.Load())
	assert.Equal(t, int32(2*CallbackMaxAttempts), attempts.Load())
}
