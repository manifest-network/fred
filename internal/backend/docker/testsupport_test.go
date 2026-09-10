package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/callbackurl"
	"github.com/manifest-network/fred/internal/operationid"
)

func mustNewCallbackPublisherForTest(
	t *testing.T,
	cfg shared.CallbackPublisherConfig,
) *shared.CallbackPublisher {
	t.Helper()
	publisher, err := shared.NewCallbackPublisher(cfg)
	require.NoError(t, err)
	return publisher
}

// nominalDockerProviderUUID is the canonical provider identity used by broad
// Docker unit fixtures. Durable intent/release stores validate the same UUID
// shape as production, so a memorable non-UUID placeholder can no longer be
// used when a test upgrades from an ephemeral seam to the real journal.
const nominalDockerProviderUUID = "22222222-2222-4222-8222-222222222222"

const testMaintenanceLifecycleCallbackURL = "https://fred.example/callbacks/provision?lifecycle_id=550e8400-e29b-41d4-a716-446655440000"

const asyncTestResultTimeout = 5 * time.Second

const operationIntentTestSubstrateID = "docker-operation-intent-test-substrate"

var operationIntentTestStoreIdentities sync.Map // *shared.CallbackStore -> backendidentity.ID

type operationIntentTestAuthority struct {
	storage       backendidentity.VerifiedStorage
	gate          *backendidentity.StorageAuthorityGate
	releasePath   string
	retentionPath string
	mu            sync.Mutex
	releases      *shared.ReleaseStore
	retentions    *shared.RetentionStore
	settlement    *shared.OperationSettlement
	restore       *shared.RestoreSettlement
	close         *shared.CloseSettlement
}

var operationIntentTestAuthorities sync.Map // *shared.CallbackStore -> *operationIntentTestAuthority

// registerExistingOperationTestAuthority teaches the compact operation
// fixture helpers about an already construction-bound backend store set. It
// preserves the exact open journal identities instead of reopening parallel
// stores or manufacturing an unbound settlement.
func registerExistingOperationTestAuthority(
	b *Backend,
	callbacks *shared.CallbackStore,
	releases *shared.ReleaseStore,
	retentions *shared.RetentionStore,
	operations *shared.OperationSettlement,
	restore *shared.RestoreSettlement,
	closeSettlement *shared.CloseSettlement,
) {
	operationIntentTestStoreIdentities.Store(callbacks, b.storageIdentity)
	operationIntentTestAuthorities.Store(callbacks, &operationIntentTestAuthority{
		storage: b.storageAuthority, gate: b.storeAuthorityGate,
		releasePath: b.cfg.ReleasesDBPath, retentionPath: b.cfg.RetentionDBPath,
		releases: releases, retentions: retentions, settlement: operations,
		restore: restore, close: closeSettlement,
	})
}

type standaloneReleaseTestAuthority struct {
	config  shared.ReleaseStoreConfig
	storage backendidentity.VerifiedStorage
	gate    *backendidentity.StorageAuthorityGate
}

var standaloneReleaseTestAuthorities sync.Map // *shared.ReleaseStore -> *standaloneReleaseTestAuthority

type retentionFixtureAuthority struct {
	backend    *Backend
	callbacks  *shared.CallbackStore
	releases   *shared.ReleaseStore
	operations *shared.OperationSettlement
	restore    *shared.RestoreSettlement
	close      *shared.CloseSettlement
	storage    backendidentity.VerifiedStorage
	gate       *backendidentity.StorageAuthorityGate
}

var retentionFixtureAuthorities sync.Map // *shared.RetentionStore -> *retentionFixtureAuthority
var callbackFixtureBackends sync.Map     // *shared.CallbackStore -> *Backend

type dockerTestCallbackStorageVerifier struct {
	storageID backendidentity.ID
	gate      *backendidentity.StorageAuthorityGate
	verify    func(context.Context) error
}

func (v dockerTestCallbackStorageVerifier) StorageIdentity() backendidentity.ID {
	return v.storageID
}

func (v dockerTestCallbackStorageVerifier) StorageAuthorityGate() *backendidentity.StorageAuthorityGate {
	return v.gate
}

func (v dockerTestCallbackStorageVerifier) Verify(ctx context.Context) error {
	if v.verify == nil {
		return nil
	}
	return v.verify(ctx)
}

func callbackStorageAttestorForTest(
	t *testing.T,
	store *shared.CallbackStore,
	stopCtx context.Context,
	verify func(context.Context) error,
) *shared.CallbackStorageAttestor {
	t.Helper()
	var storageID backendidentity.ID
	var gate *backendidentity.StorageAuthorityGate
	if value, ok := operationIntentTestAuthorities.Load(store); ok {
		authority := value.(*operationIntentTestAuthority)
		storageID, gate = authority.storage.ID(), authority.gate
	} else if value, ok := callbackFixtureBackends.Load(store); ok {
		b := value.(*Backend)
		storageID, gate = b.storageIdentity, b.storeAuthorityGate
	} else {
		t.Fatal("callback store has no test storage authority")
	}
	attestor, err := shared.NewCallbackStorageAttestor(
		store,
		dockerTestCallbackStorageVerifier{storageID: storageID, gate: gate, verify: verify},
		stopCtx,
	)
	require.NoError(t, err)
	return attestor
}

type callbackAckRoundTripper func(*http.Request) (*http.Response, error)

func reserveProvisionAdmissionForTest(t *testing.T, b *Backend, claim shared.OperationIntentClaim) shared.ProvisionAdmission {
	t.Helper()
	admission, err := b.operationSettlement.ReserveProvisionResources(b.pool, claim)
	require.NoError(t, err)
	return admission
}

func (f callbackAckRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// acknowledgePendingCallbacksForTest models the only production path which
// removes an outbox row: successful authenticated transport followed by exact
// durable removal. Tests use it when a later command depends on Fred having
// acknowledged the preceding completion.
func acknowledgePendingCallbacksForTest(t *testing.T, store *shared.CallbackStore) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var delivered atomic.Int32
	sender, err := shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:           store,
		StorageAttestor: callbackStorageAttestorForTest(t, store, ctx, allowTestCallbackDelivery),
		HTTPClient: &http.Client{Transport: callbackAckRoundTripper(func(req *http.Request) (*http.Response, error) {
			delivered.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       http.NoBody,
				Request:    req,
			}, nil
		})},
		Secret: durableCallbackTestSecret,
		Logger: slog.Default(),

		Backoff: &zeroBackoff,
	})
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sender.RunReplayLoop()
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(asyncTestResultTimeout):
			t.Error("callback replay loop did not stop")
		}
	}()
	sender.NotifyPendingCallbacks()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		pending, listErr := store.ListPending()
		if !assert.NoError(collect, listErr, "read callback acknowledgment state") {
			return
		}
		assert.Empty(collect, pending, "callbacks still pending after %d successful transports", delivered.Load())
	}, asyncTestResultTimeout, time.Millisecond, "successful transport did not acknowledge pending callbacks")
}

// newBoundOperationIntentTestStore preserves the compact constructor shape
// used by operation-journal tests while opening the same identity-bound store
// capability as production. Keeping the binding in this package-level fixture
// means external-package tests never need a constructor that can forge an
// OperationIntentCandidate or OperationIntentProbe.
func newBoundOperationIntentTestStore(
	t *testing.T,
	cfg shared.CallbackStoreConfig,
) (*shared.CallbackStore, error) {
	t.Helper()
	boundPath, err := shared.BindAuthoritativeStorePath(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = boundPath.Close() }()
	releasePath := cfg.DBPath + ".operation-releases.db"
	boundReleases, err := shared.BindAuthoritativeStorePath(releasePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = boundReleases.Close() }()
	retentionPath := cfg.DBPath + ".operation-retentions.db"
	boundRetentions, err := shared.BindAuthoritativeStorePath(retentionPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = boundRetentions.Close() }()
	pair, err := backendidentity.BindMarkerPair(
		cfg.DBPath+".storage-identity.json",
		cfg.DBPath+".storage-identity-anchor.json",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = pair.Close() }()
	storage, err := pair.InitializeWithStores(
		"docker",
		operationIntentTestSubstrateID,
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(
				pending backendidentity.PendingStorage,
				profile backendidentity.InitializationProfile,
			) error {
				if err := shared.PrepareBoundCallbackStoreStorage(boundPath, pending, profile); err != nil {
					return err
				}
				if err := shared.PrepareBoundReleaseStoreStorage(boundReleases, pending, profile); err != nil {
					return err
				}
				return shared.PrepareBoundRetentionStoreStorage(boundRetentions, pending, profile)
			},
			Check: func(pending backendidentity.PendingStorage) error {
				if err := shared.CheckBoundCallbackStoreStorage(boundPath, pending); err != nil {
					return err
				}
				if err := shared.CheckBoundReleaseStoreStorage(boundReleases, pending); err != nil {
					return err
				}
				return shared.CheckBoundRetentionStoreStorage(boundRetentions, pending)
			},
			Verify: func(verified backendidentity.VerifiedStorage) error {
				if err := shared.VerifyBoundCallbackStoreStorage(boundPath, verified); err != nil {
					return err
				}
				if err := shared.VerifyBoundReleaseStoreStorage(boundReleases, verified); err != nil {
					return err
				}
				return shared.VerifyBoundRetentionStoreStorage(boundRetentions, verified)
			},
		},
	)
	if err != nil {
		return nil, err
	}
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	if err != nil {
		return nil, err
	}
	store, err := shared.OpenIdentityBoundCallbackStore(cfg, storage, gate)
	if err != nil {
		return nil, err
	}
	operationIntentTestStoreIdentities.Store(store, storage.ID())
	operationIntentTestAuthorities.Store(store, &operationIntentTestAuthority{
		storage: storage, gate: gate, releasePath: releasePath, retentionPath: retentionPath,
	})
	return store, nil
}

func operationSettlementForCallbackTest(
	t *testing.T,
	callbacks *shared.CallbackStore,
) (*shared.ReleaseStore, *shared.OperationSettlement) {
	t.Helper()
	value, ok := operationIntentTestAuthorities.Load(callbacks)
	require.True(t, ok, "callback test store has no paired release authority")
	authority := value.(*operationIntentTestAuthority)
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.settlement != nil {
		return authority.releases, authority.settlement
	}
	releases, err := shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: authority.releasePath}, authority.storage, authority.gate,
	)
	require.NoError(t, err)
	settlement, err := shared.NewOperationSettlement(callbacks, releases)
	require.NoError(t, err)
	retentions, err := shared.OpenIdentityBoundRetentionStore(
		shared.RetentionStoreConfig{DBPath: authority.retentionPath}, authority.storage, authority.gate,
	)
	require.NoError(t, err)
	restore, err := shared.NewRestoreSettlement(settlement, retentions)
	require.NoError(t, err)
	closeSettlement, err := shared.NewCloseSettlement(callbacks, releases, retentions)
	require.NoError(t, err)
	authority.releases = releases
	authority.retentions = retentions
	authority.settlement = settlement
	authority.restore = restore
	authority.close = closeSettlement
	t.Cleanup(func() {
		_ = retentions.Close()
		_ = releases.Close()
	})
	return releases, settlement
}

func operationHandoffForCallbackTest(
	t *testing.T,
	callbacks *shared.CallbackStore,
) (*shared.ReleaseStore, *shared.RetentionStore, *shared.OperationSettlement, *shared.RestoreSettlement, *shared.CloseSettlement) {
	t.Helper()
	releases, operations := operationSettlementForCallbackTest(t, callbacks)
	value, ok := operationIntentTestAuthorities.Load(callbacks)
	require.True(t, ok, "callback test store has no paired handoff authority")
	authority := value.(*operationIntentTestAuthority)
	authority.mu.Lock()
	defer authority.mu.Unlock()
	return releases, authority.retentions, operations, authority.restore, authority.close
}

// seedV013OperationReleaseForTest writes the normalized, authorityless release
// row produced by stopped v0.13 adoption before the ReleaseStore is opened.
// Keeping the compatibility fixture at the file boundary avoids exposing a
// runtime API that can manufacture one.
func seedV013OperationReleaseForTest(
	t *testing.T,
	callbacks *shared.CallbackStore,
	leaseUUID string,
	release shared.Release,
) {
	t.Helper()
	value, ok := operationIntentTestAuthorities.Load(callbacks)
	require.True(t, ok, "callback test store has no paired release authority")
	authority := value.(*operationIntentTestAuthority)
	authority.mu.Lock()
	defer authority.mu.Unlock()
	require.Nil(t, authority.releases, "legacy release fixture must precede journal-pair open")
	require.Nil(t, release.RuntimeAuthority)
	require.Nil(t, release.LegacyRuntimeAuthority)
	require.False(t, release.OperationID.Valid())
	require.Equal(t, "active", release.Status)
	if release.Version == 0 {
		release.Version = 1
	}
	data, err := json.Marshal(struct {
		SchemaVersion uint8            `json:"schema_version"`
		Releases      []shared.Release `json:"releases"`
	}{SchemaVersion: 1, Releases: []shared.Release{release}})
	require.NoError(t, err)
	db, err := bolt.Open(authority.releasePath, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("releases"))
		if bucket == nil {
			return errors.New("legacy release fixture: releases bucket is missing")
		}
		return bucket.Put([]byte(leaseUUID), data)
	}))
	require.NoError(t, db.Close())
}

func closeOperationSettlementForCallbackTest(
	t *testing.T,
	callbacks *shared.CallbackStore,
) {
	t.Helper()
	value, ok := operationIntentTestAuthorities.Load(callbacks)
	require.True(t, ok, "callback test store has no paired release authority")
	authority := value.(*operationIntentTestAuthority)
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.releases != nil {
		require.NoError(t, authority.releases.Close())
	}
	if authority.retentions != nil {
		require.NoError(t, authority.retentions.Close())
	}
	authority.releases = nil
	authority.retentions = nil
	authority.settlement = nil
	authority.restore = nil
	authority.close = nil
}

func operationSettlementServiceForCallbackTest(
	t *testing.T,
	callbacks *shared.CallbackStore,
) *shared.OperationSettlement {
	t.Helper()
	_, settlement := operationSettlementForCallbackTest(t, callbacks)
	return settlement
}

// bindBackendToOperationIntentTestStore moves every storage-bound dependency
// together when a callback durability test replaces the backend's journal.
// Swapping only callbackStore/storageIdentity creates a combination production
// construction cannot represent and correctly fails callback attestation.
func bindBackendToOperationIntentTestStore(
	t *testing.T,
	b *Backend,
	callbacks *shared.CallbackStore,
) {
	t.Helper()
	releases, operations := operationSettlementForCallbackTest(t, callbacks)
	value, ok := operationIntentTestAuthorities.Load(callbacks)
	require.True(t, ok, "callback test store has no paired storage authority")
	authority := value.(*operationIntentTestAuthority)
	authority.mu.Lock()
	retentions := authority.retentions
	restore := authority.restore
	closeSettlement := authority.close
	storage := authority.storage
	gate := authority.gate
	authority.mu.Unlock()
	require.NotNil(t, retentions)
	require.NotNil(t, restore)
	require.NotNil(t, closeSettlement)
	maintenance, err := shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	backfiller, err := shared.NewReleaseBackfiller(callbacks, releases)
	require.NoError(t, err)

	b.callbackStore = callbacks
	b.releaseStore = releases
	b.retentionStore = retentions
	b.operationSettlement = operations
	b.releaseBackfiller = backfiller
	b.restoreSettlement = restore
	b.maintenanceSettlement = maintenance
	b.closeSettlement = closeSettlement
	b.releaseCapacityPlanner = operations
	b.storageIdentity = storage.ID()
	b.storageAuthority = storage
	b.storeAuthorityGate = gate
	b.storageVerifier = testDockerRuntimeStorageVerifier{id: storage.ID()}
	// Rebuild the complete construction graph around this journal pair. Merely
	// swapping Backend fields would leave the opaque executors and recovery
	// coordinator bound to the previous stores—a state production construction
	// cannot create and the capability checks correctly reject.
	require.NoError(t, bindBackendTestPhysicalExecutors(b, operations, maintenance))
	bindBackendTestCloseExecutor(t, b, closeSettlement)
	rebuildCallbackSender(b, testCallbackClient)
	registerExistingOperationTestAuthority(
		b, callbacks, releases, retentions, operations, restore, closeSettlement,
	)
	bindBackendRecoveryCoordinatorForTest(t, b)
}

// operationSettlementTestDecorator is implemented only by test fault-injection
// adapters which preserve the exact production coordinator underneath. Keeping
// this seam test-local lets fixtures recover that coordinator without adding a
// second mutable Backend field or weakening the production interface.
type operationSettlementTestDecorator interface {
	wrappedOperationSettlementForTest() operationSettlementService
}

func concreteOperationSettlementForTest(
	service operationSettlementService,
) (*shared.OperationSettlement, bool) {
	for range 16 {
		switch current := service.(type) {
		case *shared.OperationSettlement:
			return current, current != nil
		case operationSettlementTestDecorator:
			service = current.wrappedOperationSettlementForTest()
			if service == nil {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return nil, false
}

func callbackPublisherForCallbackTest(
	t *testing.T,
	callbacks *shared.CallbackStore,
) *shared.CallbackPublisher {
	t.Helper()
	releases, operations := operationSettlementForCallbackTest(t, callbacks)
	maintenance, err := shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	attestor := callbackStorageAttestorForTest(
		t, callbacks, context.Background(), allowTestCallbackDelivery,
	)
	return mustNewCallbackPublisherForTest(t, shared.CallbackPublisherConfig{
		OperationSettlement: operations, MaintenanceSettlement: maintenance,
		StorageAttestor: attestor, Logger: slog.Default(),
	})
}

func listOperationIntentsForCallbackTest(
	t *testing.T,
	callbacks *shared.CallbackStore,
) ([]shared.OperationIntentClaim, error) {
	t.Helper()
	return operationSettlementServiceForCallbackTest(t, callbacks).ListOperationIntents()
}

// commitPendingOperationReleaseForTest models the exact crash window after an
// operation's active Release commits but before its semantic callback settles
// the pending intent. The release is derived exclusively from the store-issued
// claim; tests cannot splice a caller-authored Release into that generation.
func commitPendingOperationReleaseForTest(
	t *testing.T,
	operations operationSettlementService,
	leaseUUID string,
) shared.OperationReleaseCommitted {
	t.Helper()
	claims, err := operations.ListOperationIntents()
	require.NoError(t, err)
	var claim shared.OperationIntentClaim
	for _, candidate := range claims {
		if candidate.LeaseUUID() == leaseUUID {
			claim = candidate
			break
		}
	}
	require.NotNil(t, claim, "pending operation claim not found for %q", leaseUUID)
	return commitOperationSuccessForTest(t, operations, claim)
}

func newBoundCallbackStoreForTest(
	t *testing.T,
	cfg shared.CallbackStoreConfig,
) (*shared.CallbackStore, error) {
	return newBoundOperationIntentTestStore(t, cfg)
}

func newBoundReleaseStoreForTest(
	t *testing.T,
	cfg shared.ReleaseStoreConfig,
) (*shared.ReleaseStore, error) {
	t.Helper()
	boundPath, err := shared.BindAuthoritativeStorePath(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = boundPath.Close() }()
	pair, err := backendidentity.BindMarkerPair(
		cfg.DBPath+".storage-identity.json",
		cfg.DBPath+".storage-identity-anchor.json",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = pair.Close() }()
	storage, err := pair.InitializeWithStores(
		"docker",
		"docker-test-substrate",
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(pending backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
				return shared.PrepareBoundReleaseStoreStorage(boundPath, pending, profile)
			},
			Check: func(pending backendidentity.PendingStorage) error {
				return shared.CheckBoundReleaseStoreStorage(boundPath, pending)
			},
			Verify: func(verified backendidentity.VerifiedStorage) error {
				return shared.VerifyBoundReleaseStoreStorage(boundPath, verified)
			},
		},
	)
	if err != nil {
		return nil, err
	}
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	if err != nil {
		return nil, err
	}
	store, err := shared.OpenIdentityBoundReleaseStore(cfg, storage, gate)
	if err != nil {
		return nil, err
	}
	standaloneReleaseTestAuthorities.Store(store, &standaloneReleaseTestAuthority{
		config: cfg, storage: storage, gate: gate,
	})
	return store, nil
}

func newBoundRetentionStoreForTest(
	t *testing.T,
	cfg shared.RetentionStoreConfig,
) (*shared.RetentionStore, error) {
	t.Helper()
	callbackPath := cfg.DBPath + ".fixture-callbacks"
	releasePath := cfg.DBPath + ".fixture-releases"
	callbackBound, err := shared.BindAuthoritativeStorePath(callbackPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = callbackBound.Close() }()
	releaseBound, err := shared.BindAuthoritativeStorePath(releasePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = releaseBound.Close() }()
	retentionBound, err := shared.BindAuthoritativeStorePath(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = retentionBound.Close() }()
	pair, err := backendidentity.BindMarkerPair(
		cfg.DBPath+".storage-identity.json",
		cfg.DBPath+".storage-identity-anchor.json",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = pair.Close() }()
	storage, err := pair.InitializeWithStores(
		"docker",
		"docker-test-substrate",
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(pending backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
				if err := shared.PrepareBoundCallbackStoreStorage(callbackBound, pending, profile); err != nil {
					return err
				}
				if err := shared.PrepareBoundReleaseStoreStorage(releaseBound, pending, profile); err != nil {
					return err
				}
				return shared.PrepareBoundRetentionStoreStorage(retentionBound, pending, profile)
			},
			Check: func(pending backendidentity.PendingStorage) error {
				if err := shared.CheckBoundCallbackStoreStorage(callbackBound, pending); err != nil {
					return err
				}
				if err := shared.CheckBoundReleaseStoreStorage(releaseBound, pending); err != nil {
					return err
				}
				return shared.CheckBoundRetentionStoreStorage(retentionBound, pending)
			},
			Verify: func(verified backendidentity.VerifiedStorage) error {
				if err := shared.VerifyBoundCallbackStoreStorage(callbackBound, verified); err != nil {
					return err
				}
				if err := shared.VerifyBoundReleaseStoreStorage(releaseBound, verified); err != nil {
					return err
				}
				return shared.VerifyBoundRetentionStoreStorage(retentionBound, verified)
			},
		},
	)
	if err != nil {
		return nil, err
	}
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	if err != nil {
		return nil, err
	}
	callbacks, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: callbackPath}, storage, gate,
	)
	if err != nil {
		return nil, err
	}
	releases, err := shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: releasePath}, storage, gate,
	)
	if err != nil {
		_ = callbacks.Close()
		return nil, err
	}
	retentions, err := shared.OpenIdentityBoundRetentionStore(cfg, storage, gate)
	if err != nil {
		_ = releases.Close()
		_ = callbacks.Close()
		return nil, err
	}
	operations, err := shared.NewOperationSettlement(callbacks, releases)
	if err != nil {
		_ = retentions.Close()
		_ = releases.Close()
		_ = callbacks.Close()
		return nil, err
	}
	restore, err := shared.NewRestoreSettlement(operations, retentions)
	if err != nil {
		_ = retentions.Close()
		_ = releases.Close()
		_ = callbacks.Close()
		return nil, err
	}
	closeSettlement, err := shared.NewCloseSettlement(callbacks, releases, retentions)
	if err != nil {
		_ = retentions.Close()
		_ = releases.Close()
		_ = callbacks.Close()
		return nil, err
	}
	operationIntentTestStoreIdentities.Store(callbacks, storage.ID())
	operationIntentTestAuthorities.Store(callbacks, &operationIntentTestAuthority{
		storage: storage, gate: gate, releasePath: releasePath, retentionPath: cfg.DBPath,
		releases: releases, retentions: retentions, settlement: operations,
		restore: restore, close: closeSettlement,
	})
	retentionFixtureAuthorities.Store(retentions, &retentionFixtureAuthority{
		callbacks: callbacks, releases: releases, operations: operations,
		restore: restore, close: closeSettlement, storage: storage, gate: gate,
	})
	t.Cleanup(func() {
		operationIntentTestAuthorities.Delete(callbacks)
		operationIntentTestStoreIdentities.Delete(callbacks)
		retentionFixtureAuthorities.Delete(retentions)
		_ = releases.Close()
		_ = callbacks.Close()
	})
	return retentions, nil
}

// bindBackendToRetentionFixtureStore replaces a backend's authoritative
// journals only as one complete, identity-bound set. Tests may not swap a raw
// RetentionStore under settlements that were constructed for another lineage.
func bindBackendToRetentionFixtureStore(
	t *testing.T,
	b *Backend,
	retentions *shared.RetentionStore,
) {
	t.Helper()
	value, ok := retentionFixtureAuthorities.Load(retentions)
	require.True(t, ok, "retention store has no construction-bound journal set")
	authority := value.(*retentionFixtureAuthority)
	authority.backend = b
	maintenance, err := shared.NewMaintenanceSettlement(authority.callbacks, authority.releases)
	require.NoError(t, err)
	backfiller, err := shared.NewReleaseBackfiller(authority.callbacks, authority.releases)
	require.NoError(t, err)

	b.callbackStore = authority.callbacks
	b.releaseStore = authority.releases
	b.retentionStore = retentions
	b.operationSettlement = authority.operations
	b.releaseBackfiller = backfiller
	b.restoreSettlement = authority.restore
	b.maintenanceSettlement = maintenance
	b.closeSettlement = authority.close
	b.releaseCapacityPlanner = authority.operations
	b.storageIdentity = authority.storage.ID()
	b.storageAuthority = authority.storage
	b.storeAuthorityGate = authority.gate
	b.storageVerifier = testDockerRuntimeStorageVerifier{id: authority.storage.ID()}
	require.NoError(t, bindBackendTestPhysicalExecutors(
		b, authority.operations, maintenance,
	))
	bindBackendTestCloseExecutor(t, b, authority.close)
	registerExistingOperationTestAuthority(
		b, authority.callbacks, authority.releases, retentions,
		authority.operations, authority.restore, authority.close,
	)
	rebuildCallbackSender(b, testCallbackClient)
	bindBackendRecoveryCoordinatorForTest(t, b)
}

func operationIntentTestStoreIdentity(
	t *testing.T,
	store *shared.CallbackStore,
) backendidentity.ID {
	t.Helper()
	storageID, ok := operationIntentTestStoreIdentities.Load(store)
	require.True(t, ok, "callback journal must come from the identity-bound operation test fixture")
	return storageID.(backendidentity.ID)
}

// recordRestoreOperationOutcome constructs the same causally-complete restore
// state as production: the Restoring finalizer supplies immutable destination
// authority and the operation journal independently supplies Pending,
// Succeeded, or Failed. Tests that intentionally exercise missing evidence do
// not call this helper.
func recordRestoreOperationOutcome(
	t *testing.T,
	b *Backend,
	entry shared.RetentionEntry,
	status backend.CallbackStatus,
) shared.OperationRecoveryState {
	t.Helper()
	if b.callbackStore == nil || b.operationSettlement == nil || b.releaseStore == nil {
		attachBoundOperationHandoffStores(t, b)
	}
	settlement := b.operationSettlement
	manifestBytes, err := json.Marshal(entry.StackManifest)
	require.NoError(t, err)
	spec := shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentRestore,
		LeaseUUID:            entry.NewLeaseUUID,
		CallbackURL:          entry.DestinationCallbackURL,
		LifecycleCallbackURL: entry.DestinationLifecycleCallbackURL,
		Tenant:               entry.Tenant,
		ProviderUUID:         entry.ProviderUUID,
		Items:                append([]backend.LeaseItem(nil), entry.DestinationItems...),
		ResourceProfiles:     shared.CloneSKUResourceSnapshot(entry.DestinationResourceProfiles),
		EffectiveItems:       append([]backend.LeaseItem(nil), entry.DestinationItems...),
		Manifest:             manifestBytes,
		SourceLeaseUUID:      entry.OriginalLeaseUUID,
		SourceGeneration:     entry.Generation,
	}
	var operationClaim shared.OperationIntentClaim
	claims, err := settlement.ListOperationIntents()
	require.NoError(t, err)
	for _, claim := range claims {
		if claim.LeaseUUID() == entry.NewLeaseUUID &&
			claim.OperationID() == entry.DestinationOperationID &&
			claim.CallbackURL() == entry.DestinationCallbackURL {
			operationClaim = claim
			break
		}
	}
	if !operationClaim.Valid() {
		operationCandidate, candidateErr := settlement.NewOperationIntentCandidate(spec)
		require.NoError(t, candidateErr)
		admission, admissionErr := settlement.BeginOperationIntent(operationCandidate)
		require.NoError(t, admissionErr)
		operationClaim = createdDockerOperationClaim(t, admission)
	}
	if status == "" {
		return operationClaim
	}
	errMsg := ""
	if status == backend.CallbackStatusFailed {
		errMsg = interruptedOperationFailure
	}
	publisher := b.callbackPublisher
	if publisher == nil {
		rebuildCallbackSender(b, testCallbackClient)
		publisher = b.callbackPublisher
	}
	if status == backend.CallbackStatusFailed {
		proof := commitPreEffectOperationFailureForTest(t, settlement, operationClaim)
		err = publisher.PublishOperationFailureContext(context.Background(), proof, errMsg)
	} else {
		committed := commitOperationSuccessForTest(t, settlement, operationClaim)
		err = publisher.PublishOperationSuccessContext(context.Background(), committed)
	}
	require.NoError(t, err)
	probe, err := settlement.NewOperationIntentProbe(
		entry.NewLeaseUUID, entry.DestinationCallbackURL,
	)
	require.NoError(t, err)
	state, err := settlement.LookupOperationRecovery(probe)
	require.NoError(t, err)
	return state
}

// waitForAsyncTestResult keeps concurrency regressions local to the assertion
// that owns the goroutine. Without a bounded receive, a broken lock handoff can
// leave the entire docker package waiting for the global `go test` timeout.
func waitForAsyncTestResult(t *testing.T, results <-chan error, operation string) error {
	t.Helper()
	timer := time.NewTimer(asyncTestResultTimeout)
	defer timer.Stop()
	select {
	case err := <-results:
		return err
	case <-timer.C:
		t.Fatalf("timeout waiting for %s", operation)
		return context.DeadlineExceeded
	}
}

func waitForTestSignal(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()
	timer := time.NewTimer(asyncTestResultTimeout)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timeout waiting for %s", operation)
	}
}

func testResourceProfiles(t *testing.T, items []backend.LeaseItem) []shared.SKUResourceSnapshot {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SKUProfiles = defaultTestSKUProfiles()
	b := &Backend{cfg: cfg}
	resolved := make(map[string]SKUProfile)
	for _, item := range items {
		if _, ok := resolved[item.SKU]; ok {
			continue
		}
		profile, err := cfg.GetSKUProfile(item.SKU)
		require.NoError(t, err)
		resolved[item.SKU] = profile
	}
	profiles, err := b.snapshotResourceProfiles(items, resolved)
	require.NoError(t, err)
	return profiles
}

// retentionFixtureResourceProfiles snapshots the resource configuration that
// the construction-bound backend owns at fixture publication time. This models
// production's immutable operation/retention snapshot; it does not let tests
// reinterpret an existing row after configuration changes.
func retentionFixtureResourceProfiles(
	t *testing.T,
	authority *retentionFixtureAuthority,
	items []backend.LeaseItem,
) []shared.SKUResourceSnapshot {
	t.Helper()
	if authority == nil || authority.backend == nil {
		return testResourceProfiles(t, items)
	}
	resolved := make(map[string]SKUProfile)
	for _, item := range items {
		if _, ok := resolved[item.SKU]; ok {
			continue
		}
		profile, err := authority.backend.cfg.GetSKUProfile(item.SKU)
		require.NoError(t, err)
		resolved[item.SKU] = profile
	}
	profiles, err := authority.backend.snapshotResourceProfiles(items, resolved)
	require.NoError(t, err)
	return profiles
}

func retentionFixtureStackManifest(items []backend.LeaseItem) *manifest.StackManifest {
	services := make(map[string]*manifest.Manifest, len(items))
	for _, item := range items {
		service := item.ServiceName
		if service == "" {
			service = manifest.DefaultServiceName
		}
		services[service] = &manifest.Manifest{Image: "nginx:1.25"}
	}
	return &manifest.StackManifest{Services: services}
}

func newTestRestoreCallbackAuthority(t *testing.T) (shared.OperationID, string, string) {
	t.Helper()
	operationID := mustDockerOperationID(uuid.NewString())
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID.String()
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	return operationID, callbackURL, lifecycleCallbackURL
}

func mustTestOperationIDFromCallbackURL(t testing.TB, callbackURL string) shared.OperationID {
	t.Helper()
	parsed, err := url.Parse(callbackURL)
	require.NoError(t, err)
	query, err := url.ParseQuery(parsed.RawQuery)
	require.NoError(t, err)
	operationID, err := operationid.Parse(query.Get(backend.CallbackOperationIDQueryParameter))
	require.NoError(t, err)
	require.True(t, operationID.Valid(), "callback must contain a canonical UUIDv4 operation ID")
	return operationID
}

func mustDockerOperationID(text string) shared.OperationID {
	id, err := operationid.Parse(text)
	if err != nil {
		panic(err)
	}
	return id
}

func mustTestReleaseRuntimeAuthority(
	t *testing.T,
	operationID shared.OperationID,
	tenant, providerUUID, callbackURL, lifecycleCallbackURL string,
) *shared.ReleaseRuntimeAuthority {
	t.Helper()
	authority, err := shared.NewReleaseRuntimeAuthority(
		operationID, tenant, providerUUID, callbackURL, lifecycleCallbackURL,
	)
	require.NoError(t, err)
	return &authority
}

// testOperationCallbackURL upgrades ordinary callback-server URLs used by
// positive-path tests to the exact operation authority production receives
// from providerd. Explicit operation identities are preserved so tests that
// assert a particular token continue to exercise that token.
func testOperationCallbackURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return raw
	}
	if _, exists := query[backend.CallbackOperationIDQueryParameter]; exists {
		return raw
	}
	query.Set(backend.CallbackOperationIDQueryParameter, uuid.NewString())
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func retentionFixtureAuthorityForTest(
	t *testing.T,
	store *shared.RetentionStore,
) *retentionFixtureAuthority {
	t.Helper()
	authority, ok := retentionFixtureAuthorities.Load(store)
	require.True(t, ok, "retention store must come from a typed fixture journal set")
	return authority.(*retentionFixtureAuthority)
}

func canonicalRetentionFixtureUUID(label string) string {
	if backend.IsCanonicalLeaseUUID(label) {
		return label
	}
	return uuid.NewSHA1(uuid.Nil, []byte("fred-retention-fixture:"+label)).String()
}

// recordedRetentionFixtureSubstrate closes over the exact subject minted by
// the fixture settlement. It exists only to finish setup through the same
// closed physical-evidence algebra as production; tests cannot turn a lease ID
// or a caller-selected status into terminal close authority.
type recordedRetentionFixtureSubstrate func(context.Context) error

func newRecordedRetentionFixtureSettlement(
	t *testing.T,
	authority *retentionFixtureAuthority,
	store *shared.RetentionStore,
) *shared.CloseSettlement {
	t.Helper()
	settlement, err := shared.NewCloseSettlement(
		authority.callbacks, authority.releases, store,
	)
	require.NoError(t, err)
	authorize := func(ctx context.Context, _ string) (context.Context, func(), error) {
		return ctx, func() {}, nil
	}
	complete := func(context.Context, string, error) error { return nil }
	err = shared.BindCloseSubstrateExecutor(
		settlement,
		authorize,
		complete,
		func(runner substratemutation.Runner, _ shared.ClosePhysicalSubject) recordedRetentionFixtureSubstrate {
			return func(ctx context.Context) error {
				return runner.Step(ctx, "fixture: retained substrate established", func(context.Context) error {
					return nil
				})
			}
		},
		func(
			ctx context.Context,
			capability recordedRetentionFixtureSubstrate,
			_ shared.ClosePhysicalSubject,
		) error {
			return capability(ctx)
		},
		func(
			_ context.Context,
			subject shared.ClosePhysicalSubject,
		) (shared.ClosePhysicalEvidence, error) {
			proof, proofErr := settlement.ProveRetention(subject.Intent())
			if proofErr != nil {
				return shared.ClosePhysicalEvidence{}, proofErr
			}
			return shared.NewCloseRetained(subject, proof)
		},
	)
	require.NoError(t, err)
	return settlement
}

// putActiveRetention records a retained close through the same operation and
// close settlements as production. It intentionally cannot preserve a
// caller-authored status, generation, or timestamp.
func putActiveRetentionViaSettlement(
	t *testing.T,
	store *shared.RetentionStore,
	desired shared.RetentionEntry,
) shared.RetentionEntry {
	t.Helper()
	require.Equal(t, shared.RetentionStatusActive, desired.Status)
	authority := retentionFixtureAuthorityForTest(t, store)
	desired.OriginalLeaseUUID = canonicalRetentionFixtureUUID(desired.OriginalLeaseUUID)
	if desired.Tenant == "" {
		desired.Tenant = "tenant-a"
	}
	if !backend.IsCanonicalLeaseUUID(desired.ProviderUUID) {
		desired.ProviderUUID = nominalDockerProviderUUID
	}
	if len(desired.Items) == 0 {
		desired.Items = []backend.LeaseItem{{
			SKU: "docker-micro", Quantity: 1, ServiceName: "app",
		}}
	}
	if len(desired.ResourceProfiles) == 0 {
		desired.ResourceProfiles = retentionFixtureResourceProfiles(t, authority, desired.Items)
	}
	if desired.StackManifest == nil || len(desired.StackManifest.Services) == 0 {
		desired.StackManifest = retentionFixtureStackManifest(desired.Items)
	}
	manifestBytes, err := json.Marshal(desired.StackManifest)
	require.NoError(t, err)
	_, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
	candidate, err := authority.operations.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind: shared.OperationIntentProvision, LeaseUUID: desired.OriginalLeaseUUID,
		CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleCallbackURL,
		Tenant: desired.Tenant, ProviderUUID: desired.ProviderUUID,
		Items: desired.Items, ResourceProfiles: desired.ResourceProfiles,
		EffectiveItems: desired.Items, Manifest: manifestBytes,
	})
	require.NoError(t, err)
	operation, err := authority.operations.BeginOperationIntent(candidate)
	require.NoError(t, err)
	_, created := operation.CreatedClaim()
	require.True(t, created)
	fixtureClose := newRecordedRetentionFixtureSettlement(t, authority, store)
	request, err := fixtureClose.NewCloseRequest(desired.OriginalLeaseUUID, true)
	require.NoError(t, err)
	admission, err := fixtureClose.BeginClose(request)
	require.NoError(t, err)
	claim := admission.Claim()
	names := desired.RetainedVolumeNames
	if len(names) == 0 {
		names = []string{retainedName(canonicalVolumeName(
			desired.OriginalLeaseUUID, desired.Items[0].ServiceName, 0,
		))}
	}
	ok, err := fixtureClose.RecordRetention(claim, desired.Partition, names)
	require.NoError(t, err)
	require.True(t, ok)
	execution, err := fixtureClose.StartCloseExecution(claim)
	require.NoError(t, err)
	outcome := fixtureClose.ExecuteClose(context.Background(), execution)
	retained, ok := outcome.(shared.CloseExecutionRetained)
	require.Truef(t, ok, "fixture retention close remained nonterminal: %T", outcome)
	_, err = fixtureClose.CompleteClose(retained)
	require.NoError(t, err)
	acknowledgePendingCallbacksForTest(t, authority.callbacks)
	entry, err := store.Get(desired.OriginalLeaseUUID)
	require.NoError(t, err)
	if entry == nil {
		entries, listErr := store.List()
		require.NoError(t, listErr)
		t.Fatalf(
			"active retention fixture was not persisted under its source lease: source=%q close_source=%q stored=%+v",
			desired.OriginalLeaseUUID, claim.LeaseUUID(), entries,
		)
	}
	return *entry
}

func putRetentionForTest(
	t *testing.T,
	store *shared.RetentionStore,
	desired shared.RetentionEntry,
) error {
	t.Helper()
	switch desired.Status {
	case shared.RetentionStatusActive:
		putActiveRetentionViaSettlement(t, store, desired)
	case shared.RetentionStatusRestoring:
		putRestoringRetention(t, store, desired)
	case shared.RetentionStatusReaping:
		active := desired
		active.Status = shared.RetentionStatusActive
		active = putActiveRetentionViaSettlement(t, store, active)
		candidate := activeRetentionCandidateForTest(t, store, active.OriginalLeaseUUID)
		_, ok, err := store.BeginReaping(candidate)
		require.NoError(t, err)
		require.True(t, ok)
	default:
		t.Fatalf("typed retention fixture cannot construct status %q", desired.Status)
	}
	return nil
}

// reapingProofForTest selects the exact tombstone authority a production
// recovery sweep would receive. Tests must not turn a lease UUID into cleanup
// authority through a raw mutator or proof-minting backdoor.
func reapingProofForTest(
	t *testing.T,
	store *shared.RetentionStore,
	leaseUUID string,
) shared.ReapingRetentionProof {
	t.Helper()
	leaseUUID = canonicalRetentionFixtureUUID(leaseUUID)
	proofs, err := store.ListReapingProofs()
	require.NoError(t, err)
	for _, proof := range proofs {
		if proof.Entry().OriginalLeaseUUID == leaseUUID {
			return proof
		}
	}
	require.FailNow(t, "reaping retention proof not found", "lease_uuid=%s", leaseUUID)
	return shared.ReapingRetentionProof{}
}

// claimRetentionForTest drives the public operation/restore capability path.
// Its argument shape mirrors the removed raw store mutator only so older
// Docker tests can state their destination request compactly; no argument is
// written directly to RetentionStore.
func claimRetentionForTest(
	t *testing.T,
	store *shared.RetentionStore,
	orig, destination string,
	maxAge time.Duration,
	destinationItems []backend.LeaseItem,
	destinationProfiles []shared.SKUResourceSnapshot,
	operationID shared.OperationID,
	callbackURL, lifecycleCallbackURL string,
) (*shared.RetentionEntry, error) {
	t.Helper()
	active, err := store.Get(orig)
	if err != nil {
		return nil, err
	}
	if active == nil {
		return nil, shared.ErrNoRetention
	}
	parsedURL, err := url.Parse(callbackURL)
	if err != nil {
		return nil, err
	}
	query, err := url.ParseQuery(parsedURL.RawQuery)
	if err != nil {
		return nil, err
	}
	parsedOperationID, err := operationid.Parse(
		query.Get(backend.CallbackOperationIDQueryParameter),
	)
	if err != nil {
		return nil, err
	}
	if parsedOperationID != operationID {
		return nil, fmt.Errorf(
			"restore operation ID %q differs from callback authority %q",
			operationID, parsedOperationID,
		)
	}
	manifestBytes, err := json.Marshal(active.StackManifest)
	if err != nil {
		return nil, err
	}
	healthCheckServices := make([]string, 0, len(active.StackManifest.Services))
	for service, serviceManifest := range active.StackManifest.Services {
		if serviceManifest != nil && serviceManifest.HasActiveHealthCheck() {
			healthCheckServices = append(healthCheckServices, service)
		}
	}
	slices.Sort(healthCheckServices)
	rawAuthority, ok := retentionFixtureAuthorities.Load(store)
	if !ok {
		return nil, errors.New("retention store has no typed fixture journal set")
	}
	authority := rawAuthority.(*retentionFixtureAuthority)
	operationSpec := shared.OperationIntentSpec{
		Kind: shared.OperationIntentRestore, LeaseUUID: destination,
		CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleCallbackURL,
		Tenant: active.Tenant, ProviderUUID: active.ProviderUUID,
		Items: destinationItems, ResourceProfiles: destinationProfiles,
		EffectiveItems: destinationItems, HealthCheckServices: healthCheckServices,
		Manifest:        manifestBytes,
		SourceLeaseUUID: orig, SourceGeneration: active.Generation + 1,
	}
	var claim shared.OperationIntentClaim
	claims, err := authority.operations.ListOperationIntents()
	if err != nil {
		return nil, err
	}
	for _, durable := range claims {
		if durable.LeaseUUID() == destination &&
			durable.OperationID() == operationID &&
			durable.CallbackURL() == callbackURL {
			claim = durable
			break
		}
	}
	if !claim.Valid() {
		operationCandidate, candidateErr := authority.operations.NewOperationIntentCandidate(operationSpec)
		if candidateErr != nil {
			return nil, candidateErr
		}
		admission, admissionErr := authority.operations.BeginOperationIntent(operationCandidate)
		if admissionErr != nil {
			return nil, admissionErr
		}
		var created bool
		claim, created = admission.CreatedClaim()
		if !created {
			return nil, errors.New("restore operation replay has no current durable claim")
		}
	}
	restoreCandidate, err := authority.restore.PrepareRestoreClaim(claim)
	if err != nil {
		return nil, err
	}
	proof, err := authority.restore.ClaimForRestore(restoreCandidate, maxAge)
	if err != nil {
		return nil, err
	}
	entry := proof.Entry()
	return &entry, nil
}

// putRestoringRetention seeds a restore finalizer through the same durable
// transition used by production. Tests must not manufacture nominal restoring
// rows: doing so bypasses the destination resource and callback
// authorities that make recovery safe after a restart.
func putRestoringRetention(
	t *testing.T,
	store *shared.RetentionStore,
	desired shared.RetentionEntry,
) *shared.RetentionEntry {
	t.Helper()
	require.Equal(t, shared.RetentionStatusRestoring, desired.Status)
	require.NotEmpty(t, desired.OriginalLeaseUUID)
	authority := retentionFixtureAuthorityForTest(t, store)

	if desired.NewLeaseUUID == "" {
		desired.NewLeaseUUID = desired.OriginalLeaseUUID + "-restore"
	}
	desired.NewLeaseUUID = canonicalRetentionFixtureUUID(desired.NewLeaseUUID)
	if desired.ProviderUUID == "" {
		desired.ProviderUUID = nominalDockerProviderUUID
	}
	if len(desired.Items) == 0 {
		desired.Items = []backend.LeaseItem{{
			SKU: "docker-micro", Quantity: 1, ServiceName: "app",
		}}
	}
	if len(desired.ResourceProfiles) == 0 {
		desired.ResourceProfiles = retentionFixtureResourceProfiles(t, authority, desired.Items)
	}
	if desired.StackManifest == nil {
		desired.StackManifest = retentionFixtureStackManifest(desired.Items)
	}
	destinationItems := desired.DestinationItems
	usedSourceItems := len(destinationItems) == 0
	if len(destinationItems) == 0 {
		destinationItems = append([]backend.LeaseItem(nil), desired.Items...)
	}
	destinationProfiles := desired.DestinationResourceProfiles
	if len(destinationProfiles) == 0 && usedSourceItems {
		destinationProfiles = shared.CloneSKUResourceSnapshot(desired.ResourceProfiles)
	}
	if len(destinationProfiles) == 0 {
		destinationProfiles = retentionFixtureResourceProfiles(t, authority, destinationItems)
	}

	operationID := desired.DestinationOperationID
	callbackURL := desired.DestinationCallbackURL
	lifecycleCallbackURL := desired.DestinationLifecycleCallbackURL
	if operationID.IsZero() && callbackURL == "" && lifecycleCallbackURL == "" {
		operationID, callbackURL, lifecycleCallbackURL = newTestRestoreCallbackAuthority(t)
	} else {
		require.True(t, operationID.Valid(), "fixture operation ID must be a canonical UUIDv4")
		require.NotEmpty(t, callbackURL)
		require.NotEmpty(t, lifecycleCallbackURL)
	}

	active := desired
	active.Status = shared.RetentionStatusActive
	active.NewLeaseUUID = ""
	active.RestoringSince = time.Time{}
	active.DestinationItems = nil
	active.DestinationResourceProfiles = nil
	active.DestinationOperationID = shared.OperationID{}
	active.DestinationCallbackURL = ""
	active.DestinationLifecycleCallbackURL = ""
	if desired.Generation > 0 {
		active.Generation = desired.Generation - 1
	}
	active = putActiveRetentionViaSettlement(t, store, active)
	claimed, err := claimRetentionForTest(
		t, store, active.OriginalLeaseUUID, desired.NewLeaseUUID, 0,
		destinationItems, destinationProfiles, operationID,
		callbackURL, lifecycleCallbackURL,
	)
	require.NoError(t, err)
	return claimed
}

func activeRetentionCandidateForTest(
	t *testing.T,
	store *shared.RetentionStore,
	leaseUUID string,
) shared.ActiveRetentionCandidate {
	t.Helper()
	candidates, err := store.ListActiveCandidates()
	require.NoError(t, err)
	for _, candidate := range candidates {
		if candidate.Entry().OriginalLeaseUUID == leaseUUID {
			return candidate
		}
	}
	t.Fatalf("active retention candidate %q not found", leaseUUID)
	return shared.ActiveRetentionCandidate{}
}

func mustParseMaintenanceID(t *testing.T, raw string) shared.MaintenanceID {
	t.Helper()
	id, err := parseContainerMaintenanceID(raw)
	require.NoError(t, err)
	return id
}

func newTestMaintenanceID(t *testing.T) shared.MaintenanceID {
	t.Helper()
	return mustParseMaintenanceID(t, uuid.NewString())
}

func newTestMaintenanceIntentSpec(
	t *testing.T,
	settlement *shared.MaintenanceSettlement,
	id shared.MaintenanceID,
	kind shared.MaintenanceIntentKind,
	source shared.MaintenanceSourceClaim,
	target shared.Release,
) shared.MaintenanceIntentCandidate {
	t.Helper()
	identity, ok := target.RuntimeIdentity()
	require.True(t, ok)
	payload := []byte(nil)
	if kind != shared.MaintenanceIntentRestart {
		payload = target.Manifest
	}
	request, err := settlement.NewMaintenanceRequestAuthority(
		id, kind, source.LeaseUUID(), identity.LifecycleCallbackURL(), payload,
	)
	require.NoError(t, err)
	candidate, err := settlement.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)
	return candidate
}

func createdTestMaintenanceDispatch(
	t *testing.T,
	admission shared.MaintenanceIntentAdmission,
) shared.MaintenanceIntentDispatch {
	t.Helper()
	dispatch, ok := admission.CreatedDispatch()
	require.True(t, ok, "maintenance admission must carry first-dispatch authority")
	return dispatch
}

func newTestMaintenanceSettlement(
	t *testing.T,
	callbacks *shared.CallbackStore,
	releases *shared.ReleaseStore,
) *shared.MaintenanceSettlement {
	t.Helper()
	settlement, err := shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	return settlement
}

type testDockerStorageIdentity struct{}

// attachBoundOperationHandoffStores gives lightweight Backend fixtures the
// same single-lineage callback/release/retention journal set as production.
// Operation and restore tests must not assemble independently bound or unbound
// stores now that cross-store authority is carried by sealed capabilities.
func attachBoundOperationHandoffStores(t *testing.T, b *Backend) {
	t.Helper()
	if b.callbackStore != nil && b.releaseStore != nil && b.retentionStore != nil &&
		b.storageIdentity.Valid() && b.storeAuthorityGate != nil {
		bindExecutors := false
		if b.operationSettlement == nil {
			settlement, err := shared.NewOperationSettlement(b.callbackStore, b.releaseStore)
			require.NoError(t, err)
			b.operationSettlement = settlement
			bindExecutors = true
		}
		if b.releaseBackfiller == nil {
			backfiller, err := shared.NewReleaseBackfiller(b.callbackStore, b.releaseStore)
			require.NoError(t, err)
			b.releaseBackfiller = backfiller
		}
		if b.restoreSettlement == nil {
			operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
			require.True(t, ok)
			restore, err := shared.NewRestoreSettlement(operations, b.retentionStore)
			require.NoError(t, err)
			b.restoreSettlement = restore
		}
		if b.maintenanceSettlement == nil {
			b.maintenanceSettlement = newTestMaintenanceSettlement(
				t, b.callbackStore, b.releaseStore,
			)
		}
		if bindExecutors {
			operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
			require.True(t, ok)
			require.NoError(t, bindBackendTestPhysicalExecutors(
				b, operations, b.maintenanceSettlement,
			))
		}
		if b.closeSettlement == nil {
			closeSettlement, err := shared.NewCloseSettlement(
				b.callbackStore, b.releaseStore, b.retentionStore,
			)
			require.NoError(t, err)
			b.closeSettlement = closeSettlement
			bindBackendTestCloseExecutor(t, b, closeSettlement)
		}
		if b.releaseCapacityPlanner == nil {
			b.releaseCapacityPlanner = b.operationSettlement
		}
		if operations, ok := concreteOperationSettlementForTest(b.operationSettlement); ok {
			retentionFixtureAuthorities.Store(b.retentionStore, &retentionFixtureAuthority{
				backend: b, callbacks: b.callbackStore, releases: b.releaseStore,
				operations: operations, restore: b.restoreSettlement, close: b.closeSettlement,
			})
			registerExistingOperationTestAuthority(
				b, b.callbackStore, b.releaseStore, b.retentionStore,
				operations, b.restoreSettlement, b.closeSettlement,
			)
		}
		bindBackendRecoveryCoordinatorForTest(t, b)
		return
	}
	dir := t.TempDir()
	b.cfg.CallbackDBPath = filepath.Join(dir, "callbacks.db")
	b.cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	b.cfg.RetentionDBPath = filepath.Join(dir, "retention.db")
	dockerClient, volumes := fullStorageClientsForTest(b)
	storage, err := (testDockerStorageIdentity{}).resolve(
		context.Background(), b.cfg, dockerClient, volumes,
	)
	require.NoError(t, err)
	// Mirror production construction: withdrawing authoritative storage also
	// cancels every backend worker. A no-op failure hook leaves tests in a state
	// production cannot represent and masks fail-stop regressions.
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) { b.stopCancel() })
	require.NoError(t, err)
	callbacks, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath}, storage, gate,
	)
	require.NoError(t, err)
	releases, err := shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: b.cfg.ReleasesDBPath}, storage, gate,
	)
	require.NoError(t, err)
	retentions, err := shared.OpenIdentityBoundRetentionStore(
		shared.RetentionStoreConfig{DBPath: b.cfg.RetentionDBPath}, storage, gate,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
		require.NoError(t, retentions.Close())
	})
	b.callbackStore = callbacks
	b.releaseStore = releases
	settlement, err := shared.NewOperationSettlement(callbacks, releases)
	require.NoError(t, err)
	b.operationSettlement = settlement
	backfiller, err := shared.NewReleaseBackfiller(callbacks, releases)
	require.NoError(t, err)
	b.releaseBackfiller = backfiller
	restore, err := shared.NewRestoreSettlement(settlement, retentions)
	require.NoError(t, err)
	b.restoreSettlement = restore
	maintenanceSettlement, err := shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	b.maintenanceSettlement = maintenanceSettlement
	closeSettlement, err := shared.NewCloseSettlement(callbacks, releases, retentions)
	require.NoError(t, err)
	b.closeSettlement = closeSettlement
	b.releaseCapacityPlanner = settlement
	b.retentionStore = retentions
	retentionFixtureAuthorities.Store(retentions, &retentionFixtureAuthority{
		backend: b, callbacks: callbacks, releases: releases, operations: settlement,
		restore: restore, close: closeSettlement,
	})
	b.storageIdentity = storage.ID()
	b.storageAuthority = storage
	b.storeAuthorityGate = gate
	b.storageVerifier = testDockerRuntimeStorageVerifier{id: storage.ID()}
	require.NoError(t, bindBackendTestPhysicalExecutors(
		b, settlement, maintenanceSettlement,
	))
	bindBackendTestCloseExecutor(t, b, closeSettlement)
	registerExistingOperationTestAuthority(
		b, callbacks, releases, retentions, settlement, restore, closeSettlement,
	)
	bindBackendRecoveryCoordinatorForTest(t, b)
}

// bindBackendTestCloseExecutor gives lightweight fixtures the same
// construction-bound close workflow and classifier as production.
func bindBackendTestCloseExecutor(
	t *testing.T,
	b *Backend,
	settlement *shared.CloseSettlement,
) {
	t.Helper()
	ops, err := storageMutationOperationsForTest(b)
	require.NoError(t, err)
	require.NoError(t, shared.BindCloseSubstrateExecutor(
		settlement,
		b.authorizeStorageMutation,
		b.completeStorageMutation,
		buildCloseSubstrate(b, ops),
		runCloseSubstrate,
		b.classifyClosePhysical,
	))
}

// newCallbackTestServer returns an httptest server whose advertised URL is a
// complete Fred callback endpoint. Mutating URL is test-only metadata;
// httptest.Server uses its listener for Client and Close.
func newCallbackTestServer(handler http.Handler) *httptest.Server {
	server := httptest.NewServer(handler)
	server.URL += callbackurl.ProvisionPath
	return server
}

func (testDockerStorageIdentity) resolve(
	_ context.Context,
	cfg Config,
	_ dockerClient,
	_ volumeManager,
) (backendidentity.VerifiedStorage, error) {
	const substrateID = "test-docker-substrate"
	markerPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json"
	anchorPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity-anchor.json"
	paths, err := bindDockerStorageInitializationPaths(cfg, markerPath, anchorPath)
	if err != nil {
		return backendidentity.VerifiedStorage{}, err
	}
	defer func() { _ = paths.Close() }()
	hooks := backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileFresh,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			if err := shared.PrepareBoundCallbackStoreStorage(paths.callbacks, storage, profile); err != nil {
				return err
			}
			if err := shared.PrepareBoundReleaseStoreStorage(paths.releases, storage, profile); err != nil {
				return err
			}
			return shared.PrepareBoundRetentionStoreStorage(paths.retention, storage, profile)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			if err := shared.CheckBoundCallbackStoreStorage(paths.callbacks, storage); err != nil {
				return err
			}
			if err := shared.CheckBoundReleaseStoreStorage(paths.releases, storage); err != nil {
				return err
			}
			return shared.CheckBoundRetentionStoreStorage(paths.retention, storage)
		},
		Verify: func(storage backendidentity.VerifiedStorage) error {
			return verifyBoundDockerAuthoritativeStoreSet(paths, storage)
		},
	}
	return paths.markers.InitializeWithStores(cfg.Name, substrateID, hooks)
}

type testDockerRuntimeStorageVerifier struct {
	id       backendidentity.ID
	identity func() backendidentity.ID
	verify   func(context.Context) error
}

func (verifier testDockerRuntimeStorageVerifier) StorageIdentity() backendidentity.ID {
	if verifier.identity != nil {
		return verifier.identity()
	}
	return verifier.id
}
func (verifier testDockerRuntimeStorageVerifier) Verify(ctx context.Context) error {
	if verifier.verify != nil {
		return verifier.verify(ctx)
	}
	return nil
}

func newBackendWithTestIdentity(t *testing.T, cfg Config, logger *slog.Logger) (*Backend, error) {
	t.Helper()
	cfg.DockerHost = newStorageIdentityDockerServer(t, nil).URL
	b, err := newBackend(t.Context(), cfg, logger, testDockerStorageIdentity{})
	if err == nil {
		callbackFixtureBackends.Store(b.callbackStore, b)
		b.operationSettlement = noopOperationIntentJournal{
			operationSettlementService: b.operationSettlement,
		}
	}
	return b, err
}

// openBoundCallbackStoreForBackendTest creates the same identity-bound callback
// journal used by production while keeping unit fixtures independent from the
// rest of the authoritative store set. Calling it again with the same paths
// exercises the committed-marker restart path.
func openBoundCallbackStoreForBackendTest(
	t *testing.T,
	b *Backend,
	dbPath string,
	substrateID string,
) *shared.CallbackStore {
	t.Helper()
	boundPath, err := shared.BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, boundPath.Close()) }()
	pair, err := backendidentity.BindMarkerPair(
		dbPath+".storage-identity.json",
		dbPath+".storage-identity-anchor.json",
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pair.Close()) }()
	storage, err := pair.InitializeWithStores(
		b.Name(),
		substrateID,
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(
				pending backendidentity.PendingStorage,
				profile backendidentity.InitializationProfile,
			) error {
				return shared.PrepareBoundCallbackStoreStorage(boundPath, pending, profile)
			},
			Check: func(pending backendidentity.PendingStorage) error {
				return shared.CheckBoundCallbackStoreStorage(boundPath, pending)
			},
			Verify: func(verified backendidentity.VerifiedStorage) error {
				return shared.VerifyBoundCallbackStoreStorage(boundPath, verified)
			},
		},
	)
	require.NoError(t, err)
	b.storageIdentity = storage.ID()
	backendName := b.Name()
	b.storageVerifier = testDockerRuntimeStorageVerifier{
		id: storage.ID(),
		verify: func(context.Context) error {
			return backendidentity.VerifyMarkerPair(
				dbPath+".storage-identity.json",
				dbPath+".storage-identity-anchor.json",
				backendName,
				substrateID,
				storage.ID(),
			)
		},
	}
	store, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: dbPath}, storage, b.storeAuthorityGate,
	)
	require.NoError(t, err)
	return store
}

// openBoundCloseStoresForBackendTest creates the same three-store storage
// lineage consumed by CloseSettlement. Close tests must not manufacture a
// release fence from independently initialized callback/release databases.
func openBoundCloseStoresForBackendTest(
	t *testing.T,
	b *Backend,
	dir string,
	substrateID string,
) (*shared.CallbackStore, *shared.ReleaseStore, *shared.RetentionStore) {
	t.Helper()
	callbackPath := filepath.Join(dir, "callbacks.db")
	releasePath := filepath.Join(dir, "releases.db")
	retentionPath := filepath.Join(dir, "retention.db")
	markerPath := filepath.Join(dir, "storage-identity.json")
	anchorPath := filepath.Join(dir, "storage-identity-anchor.json")

	callbackBound, err := shared.BindAuthoritativeStorePath(callbackPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, callbackBound.Close()) }()
	releaseBound, err := shared.BindAuthoritativeStorePath(releasePath)
	require.NoError(t, err)
	defer func() { require.NoError(t, releaseBound.Close()) }()
	retentionBound, err := shared.BindAuthoritativeStorePath(retentionPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, retentionBound.Close()) }()

	pair, err := backendidentity.BindMarkerPair(markerPath, anchorPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, pair.Close()) }()
	storage, err := pair.InitializeWithStores(
		b.Name(), substrateID,
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(
				pending backendidentity.PendingStorage,
				profile backendidentity.InitializationProfile,
			) error {
				if err := shared.PrepareBoundCallbackStoreStorage(callbackBound, pending, profile); err != nil {
					return err
				}
				if err := shared.PrepareBoundReleaseStoreStorage(releaseBound, pending, profile); err != nil {
					return err
				}
				return shared.PrepareBoundRetentionStoreStorage(retentionBound, pending, profile)
			},
			Check: func(pending backendidentity.PendingStorage) error {
				if err := shared.CheckBoundCallbackStoreStorage(callbackBound, pending); err != nil {
					return err
				}
				if err := shared.CheckBoundReleaseStoreStorage(releaseBound, pending); err != nil {
					return err
				}
				return shared.CheckBoundRetentionStoreStorage(retentionBound, pending)
			},
			Verify: func(verified backendidentity.VerifiedStorage) error {
				if err := shared.VerifyBoundCallbackStoreStorage(callbackBound, verified); err != nil {
					return err
				}
				if err := shared.VerifyBoundReleaseStoreStorage(releaseBound, verified); err != nil {
					return err
				}
				return shared.VerifyBoundRetentionStoreStorage(retentionBound, verified)
			},
		},
	)
	require.NoError(t, err)
	b.storageIdentity = storage.ID()
	b.storageAuthority = storage
	backendName := b.Name()
	b.storageVerifier = testDockerRuntimeStorageVerifier{
		id: storage.ID(),
		verify: func(context.Context) error {
			return backendidentity.VerifyMarkerPair(
				markerPath, anchorPath, backendName, substrateID, storage.ID(),
			)
		},
	}
	callbacks, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: callbackPath}, storage, b.storeAuthorityGate,
	)
	require.NoError(t, err)
	releases, err := shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: releasePath}, storage, b.storeAuthorityGate,
	)
	require.NoError(t, err)
	retentions, err := shared.OpenIdentityBoundRetentionStore(
		shared.RetentionStoreConfig{DBPath: retentionPath}, storage, b.storeAuthorityGate,
	)
	require.NoError(t, err)
	return callbacks, releases, retentions
}

func attachBoundMaintenanceCallbackStore(t *testing.T, b *Backend) *shared.CallbackStore {
	t.Helper()
	// Maintenance admission is defined by the paired callback/release journal,
	// not by a callback store in isolation. Build the same single-lineage store
	// graph as production so callers cannot exercise an impossible half-bound
	// maintenance service.
	attachBoundOperationHandoffStores(t, b)
	return b.callbackStore
}

func attachRestoreAuthorityCallbackStore(t *testing.T, b *Backend) *shared.CallbackStore {
	t.Helper()
	attachBoundOperationHandoffStores(t, b)
	require.NotNil(t, b.callbackStore)
	require.NotNil(t, b.operationSettlement)
	return b.callbackStore
}

// newNominalProvisionComposeExecutor models the successful Compose substrate
// used by broad provision tests. It derives the PS cohort from the exact
// service keys emitted by buildComposeProject, rather than returning an empty
// inventory that now (correctly) fails the exact-cohort safety check.
func newNominalProvisionComposeExecutor() *mockComposeExecutor {
	var mu sync.Mutex
	projects := make(map[string][]composeContainerSummary)
	return &mockComposeExecutor{
		UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
			serviceNames := make([]string, 0, len(project.Services))
			for serviceName := range project.Services {
				serviceNames = append(serviceNames, serviceName)
			}
			sort.Strings(serviceNames)
			containers := make([]composeContainerSummary, 0, len(serviceNames))
			for index, serviceName := range serviceNames {
				containers = append(containers, composeContainerSummary{
					ID:      fmt.Sprintf("container-%d", index+1),
					Service: serviceName,
					State:   "running",
				})
			}
			mu.Lock()
			projects[project.Name] = containers
			mu.Unlock()
			return nil
		},
		PSFn: func(_ context.Context, projectName string) ([]composeContainerSummary, error) {
			mu.Lock()
			defer mu.Unlock()
			return append([]composeContainerSummary(nil), projects[projectName]...), nil
		},
	}
}

type noopOperationIntentJournal struct {
	operationSettlementService
	store *shared.CallbackStore
}

func (j noopOperationIntentJournal) wrappedOperationSettlementForTest() operationSettlementService {
	return j.operationSettlementService
}

func (noopOperationIntentJournal) ListOperationRecoveryStates() (
	[]shared.OperationRecoveryState,
	error,
) {
	return nil, nil
}

func (noopOperationIntentJournal) ListFailedOperationReceipts() (
	[]shared.FailedOperationReceipt,
	error,
) {
	return nil, nil
}

func (noopOperationIntentJournal) NewOperationIntentProbe(
	string, string,
) (shared.OperationIntentProbe, error) {
	return shared.OperationIntentProbe{}, nil
}

func (j noopOperationIntentJournal) NewOperationIntentCandidate(
	spec shared.OperationIntentSpec,
) (shared.OperationIntentCandidate, error) {
	if j.operationSettlementService == nil {
		return shared.OperationIntentCandidate{}, errors.New(
			"nominal operation-intent fixture has no claim-minting store",
		)
	}
	callbackURL, err := url.Parse(spec.CallbackURL)
	if err != nil {
		return shared.OperationIntentCandidate{}, err
	}
	callbackQuery, err := url.ParseQuery(callbackURL.RawQuery)
	if err != nil {
		return shared.OperationIntentCandidate{}, err
	}
	if callbackQuery.Has(backend.CallbackOperationIDQueryParameter) {
		return shared.OperationIntentCandidate{}, errors.New(
			"nominal operation-intent fixture cannot replace typed operation authority",
		)
	}
	if spec.LifecycleCallbackURL != "" {
		lifecycleURL, parseErr := url.Parse(spec.LifecycleCallbackURL)
		if parseErr != nil {
			return shared.OperationIntentCandidate{}, parseErr
		}
		lifecycleQuery, parseErr := url.ParseQuery(lifecycleURL.RawQuery)
		if parseErr != nil {
			return shared.OperationIntentCandidate{}, parseErr
		}
		if lifecycleQuery.Has(backend.CallbackLifecycleIDQueryParameter) {
			return shared.OperationIntentCandidate{}, errors.New(
				"nominal operation-intent fixture cannot replace typed lifecycle authority",
			)
		}
	}

	// These unit fixtures intentionally use memorable non-UUID identities and
	// tokenless callback routes. Mint the opaque claim through the real journal
	// under fresh internal UUID keys, while leaving the request-facing values
	// untouched. A unique key preserves the old always-created behavior without
	// manufacturing a claim or weakening production validation.
	spec.LeaseUUID = uuid.NewString()
	spec.ProviderUUID = uuid.NewString()
	if spec.SourceLeaseUUID != "" {
		spec.SourceLeaseUUID = uuid.NewString()
	}
	return j.operationSettlementService.NewOperationIntentCandidate(spec)
}

func (noopOperationIntentJournal) ProbeOperationIntent(
	shared.OperationIntentProbe,
) (shared.OperationIntentAdmissionDisposition, error) {
	return shared.OperationIntentAdmissionNone, nil
}

func (j noopOperationIntentJournal) BeginOperationIntent(
	candidate shared.OperationIntentCandidate,
) (shared.OperationIntentAdmission, error) {
	if j.operationSettlementService == nil {
		return shared.OperationIntentAdmission{}, errors.New(
			"nominal operation-intent fixture has no claim-minting store",
		)
	}
	return j.operationSettlementService.BeginOperationIntent(candidate)
}

func (j noopOperationIntentJournal) ListOperationIntents() ([]shared.OperationIntentClaim, error) {
	if j.operationSettlementService == nil {
		return nil, nil
	}
	return j.operationSettlementService.ListOperationIntents()
}

func (noopOperationIntentJournal) ResolveOperationIntent(
	shared.OperationIntentClaim,
	backend.CallbackStatus,
	string,
) (shared.CallbackEntry, error) {
	return shared.CallbackEntry{}, nil
}

// durableTestOperationIntentJournal delegates to the production bbolt store.
// The identity-bound store itself mints every operation capability; the test
// adapter cannot supply or override storage lineage.
type durableTestOperationIntentJournal struct {
	operationSettlementService
	store *shared.CallbackStore
}

func (j durableTestOperationIntentJournal) wrappedOperationSettlementForTest() operationSettlementService {
	return j.operationSettlementService
}

func (j durableTestOperationIntentJournal) NewOperationIntentProbe(
	leaseUUID, callbackURL string,
) (shared.OperationIntentProbe, error) {
	return j.operationSettlementService.NewOperationIntentProbe(leaseUUID, callbackURL)
}

func (j durableTestOperationIntentJournal) NewOperationIntentCandidate(
	spec shared.OperationIntentSpec,
) (shared.OperationIntentCandidate, error) {
	return j.operationSettlementService.NewOperationIntentCandidate(spec)
}

func (j durableTestOperationIntentJournal) ProbeOperationIntent(
	probe shared.OperationIntentProbe,
) (shared.OperationIntentAdmissionDisposition, error) {
	return j.operationSettlementService.ProbeOperationIntent(probe)
}

func (j durableTestOperationIntentJournal) BeginOperationIntent(
	candidate shared.OperationIntentCandidate,
) (shared.OperationIntentAdmission, error) {
	return j.operationSettlementService.BeginOperationIntent(candidate)
}

func (j durableTestOperationIntentJournal) ListOperationIntents() ([]shared.OperationIntentClaim, error) {
	return j.operationSettlementService.ListOperationIntents()
}

// bindTestStorageIdentity seals a stateless test Backend so Start tests can
// exercise the phase they name rather than failing at the production-only
// explicit lineage precondition first.
func bindTestStorageIdentity(t *testing.T, b *Backend, dockerClient *mockDockerClient) {
	t.Helper()
	const daemonID = "test-daemon"
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "callbacks.db")
	b.cfg.CallbackDBPath = dbPath
	b.cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	b.cfg.RetentionDBPath = filepath.Join(dir, "retention.db")
	for name, profile := range b.cfg.SKUProfiles {
		profile.DiskMB = 0
		b.cfg.SKUProfiles[name] = profile
	}
	dockerClient.DaemonInfoFn = func(context.Context) (DaemonSecurityInfo, error) {
		return DaemonSecurityInfo{SystemID: daemonID}, nil
	}
	if journal, ok := b.operationSettlement.(noopOperationIntentJournal); ok && journal.store != nil {
		require.NoError(t, journal.store.Close())
	}
	callbacks, releases, retentions := openBoundCloseStoresForBackendTest(t, b, dir, daemonID)
	t.Cleanup(func() {
		_ = retentions.Close()
		_ = releases.Close()
		_ = callbacks.Close()
	})
	operations, err := shared.NewOperationSettlement(callbacks, releases)
	require.NoError(t, err)
	restore, err := shared.NewRestoreSettlement(operations, retentions)
	require.NoError(t, err)
	maintenance, err := shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	closeSettlement, err := shared.NewCloseSettlement(callbacks, releases, retentions)
	require.NoError(t, err)
	b.callbackStore = callbacks
	b.releaseStore = releases
	b.retentionStore = retentions
	b.operationSettlement = noopOperationIntentJournal{
		store: callbacks, operationSettlementService: operations,
	}
	backfiller, err := shared.NewReleaseBackfiller(callbacks, releases)
	require.NoError(t, err)
	b.releaseBackfiller = backfiller
	b.restoreSettlement = restore
	b.maintenanceSettlement = maintenance
	b.closeSettlement = closeSettlement
	b.releaseCapacityPlanner = operations
	require.NoError(t, bindBackendTestPhysicalExecutors(b, operations, maintenance))
	bindBackendTestCloseExecutor(t, b, closeSettlement)
	retentionFixtureAuthorities.Store(retentions, &retentionFixtureAuthority{
		backend: b, callbacks: callbacks, releases: releases, operations: operations,
		restore: restore, close: closeSettlement,
	})
	registerExistingOperationTestAuthority(
		b, callbacks, releases, retentions, operations, restore, closeSettlement,
	)
	rebuildCallbackSender(b, testCallbackClient)
	id := b.storageIdentity
	b.storageVerifier = testDockerRuntimeStorageVerifier{
		id: id,
		verify: func(ctx context.Context) error {
			info, err := dockerClient.DaemonInfo(ctx)
			if err != nil {
				return err
			}
			return backendidentity.VerifyMarkerPair(
				filepath.Join(dir, "storage-identity.json"),
				filepath.Join(dir, "storage-identity-anchor.json"),
				b.cfg.Name,
				info.SystemID,
				id,
			)
		},
	}
	bindBackendRecoveryCoordinatorForTest(t, b)
}

func initializeTestMarkerPair(
	primaryPath, anchorPath, backendName, substrateID string,
) (backendidentity.ID, error) {
	pair, err := backendidentity.BindMarkerPair(primaryPath, anchorPath)
	if err != nil {
		return backendidentity.ID{}, err
	}
	defer func() { _ = pair.Close() }()
	storage, err := pair.InitializeWithStores(
		backendName,
		substrateID,
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(backendidentity.PendingStorage, backendidentity.InitializationProfile) error {
				return nil
			},
			Check:  func(backendidentity.PendingStorage) error { return nil },
			Verify: func(backendidentity.VerifiedStorage) error { return nil },
		},
	)
	if err != nil {
		return backendidentity.ID{}, err
	}
	return storage.ID(), nil
}

// actorFor resolves the lease actor for leaseUUID, creating and starting
// one if absent. Test-only: production code uses routeToLease to deliver
// messages without ever exposing an actor pointer to the caller. Tests
// retain direct access for synthetic scenario setup (installing
// workers entries, poking SM state, asserting invariants) that can't
// go through the message path.
func (b *Backend) actorFor(leaseUUID string) *leasesm.LeaseActor {
	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	return b.actorForLocked(leaseUUID)
}

func (b *Backend) actorOwnsMaintenance(leaseUUID string, id shared.MaintenanceID) bool {
	b.actorsMu.Lock()
	actor := b.actors[leaseUUID]
	b.actorsMu.Unlock()
	return actor != nil && actor.OwnsMaintenance(id)
}

// handleContainerDeath routes the same generation-bound observation as the
// production event path. Tests synchronize on the resulting state, callback,
// or quiescence claim instead of a test-only message acknowledgement.
func (b *Backend) handleContainerDeath(containerID string) {
	leaseUUID, found := b.findLeaseByContainerID(containerID)
	if !found {
		return
	}
	generation, err := b.releaseStore.ProveRuntimeGeneration(leaseUUID)
	if err != nil {
		return
	}
	observation, err := leasesm.NewContainerDiedObservation(containerID, generation)
	if err != nil || !b.routeActorObservation(observation) {
		return
	}
}

func mustContainerDiedObservation(
	t *testing.T,
	containerID string,
	runtime shared.RuntimeGenerationProof,
) leasesm.ActorObservation {
	t.Helper()
	observation, err := leasesm.NewContainerDiedObservation(containerID, runtime)
	require.NoError(t, err)
	return observation
}

// installReadyRuntimeProofForTest upgrades a compact actor fixture into the
// exact durable runtime generation now required to route observations. The
// operation success is committed and acknowledged through the real typed
// handoff; its transport is the test no-op client so setup itself cannot emit a
// lifecycle callback into the test's observer.
func installReadyRuntimeProofForTest(
	t *testing.T,
	b *Backend,
	leaseUUID string,
) shared.RuntimeGenerationProof {
	t.Helper()
	require.True(t, backend.IsCanonicalLeaseUUID(leaseUUID))
	attachBoundOperationHandoffStores(t, b)

	b.provisionsMu.Lock()
	projection := b.provisions[leaseUUID]
	require.NotNil(t, projection)
	if projection.Tenant == "" {
		projection.Tenant = "tenant-a"
	}
	if projection.ProviderUUID == "" {
		projection.ProviderUUID = nominalDockerProviderUUID
	}
	if len(projection.Items) == 0 {
		quantity := len(projection.ContainerIDs)
		if quantity == 0 {
			quantity = 1
		}
		projection.Items = []backend.LeaseItem{{
			SKU: "docker-small", ServiceName: manifest.DefaultServiceName, Quantity: quantity,
		}}
		projection.Quantity = quantity
		projection.SKU = projection.Items[0].SKU
	}
	if len(projection.ResourceProfiles) == 0 {
		projection.ResourceProfiles = testResourceProfiles(t, projection.Items)
	}
	if projection.StackManifest == nil {
		services := make(map[string]*manifest.Manifest, len(projection.Items))
		for _, item := range projection.Items {
			services[item.ServiceName] = &manifest.Manifest{Image: "busybox"}
		}
		projection.StackManifest = &manifest.StackManifest{Services: services}
	}
	callbackBase := projection.CallbackURL
	if callbackBase == "" {
		callbackBase = "https://fred.example/callbacks/provision"
	}
	parsed, err := url.Parse(callbackBase)
	require.NoError(t, err)
	query := parsed.Query()
	var operationID shared.OperationID
	if raw := query.Get(backend.CallbackOperationIDQueryParameter); raw != "" {
		operationID = mustDockerOperationID(raw)
	} else {
		operationID = mustDockerOperationID(uuid.NewString())
		if parsed.RawQuery == "" {
			parsed.RawQuery = backend.CallbackOperationIDQueryParameter + "=" + operationID.String()
		} else {
			parsed.RawQuery += "&" + backend.CallbackOperationIDQueryParameter + "=" + operationID.String()
		}
	}
	projection.CallbackURL = parsed.String()
	projection.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(projection.CallbackURL, "")
	require.NoError(t, err)
	projection.ActiveOperationID = operationID
	items := slices.Clone(projection.Items)
	profiles := shared.CloneSKUResourceSnapshot(projection.ResourceProfiles)
	stack := projection.StackManifest
	tenant, providerUUID := projection.Tenant, projection.ProviderUUID
	callbackURL, lifecycleURL := projection.CallbackURL, projection.LifecycleCallbackURL
	b.provisionsMu.Unlock()

	payload, err := json.Marshal(stack)
	require.NoError(t, err)
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind: shared.OperationIntentProvision, LeaseUUID: leaseUUID,
		CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleURL,
		Tenant: tenant, ProviderUUID: providerUUID,
		Items: items, ResourceProfiles: profiles, EffectiveItems: items, Manifest: payload,
	})
	require.NoError(t, err)
	admission, err := b.operationSettlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim := createdDockerOperationClaim(t, admission)
	committed := commitOperationSuccessForTest(t, b.operationSettlement, claim)
	rebuildCallbackSender(b, testCallbackClient)
	require.NoError(t, b.callbackPublisher.PublishOperationSuccessContext(context.Background(), committed))
	acknowledgePendingCallbacksForTest(t, b.callbackStore)
	active, err := b.releaseStore.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	b.provisionsMu.Lock()
	b.provisions[leaseUUID].ActiveReleaseVersion = active.Version
	b.provisionsMu.Unlock()
	proof, err := b.releaseStore.ProveRuntimeGeneration(leaseUUID)
	require.NoError(t, err)
	return proof
}

func actorOperationClaimForTest(
	t *testing.T,
	leaseUUID string,
) shared.ProvisionAdmission {
	t.Helper()
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	callbacks, err := newBoundCallbackStoreForTest(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "actor-command-callbacks.db"),
	})
	require.NoError(t, err)
	_, settlement := operationSettlementForCallbackTest(t, callbacks)
	candidate, err := settlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind: shared.OperationIntentProvision, LeaseUUID: leaseUUID,
		CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleURL,
		Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		Items: []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}},
		ResourceProfiles: []shared.SKUResourceSnapshot{{
			SKU: "sku-a", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
		}},
		EffectiveItems: []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}},
		Manifest:       []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
	})
	require.NoError(t, err)
	admission, err := settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, created := admission.CreatedClaim()
	require.True(t, created)
	pool := shared.NewResourcePool(8, 8192, 16384, func(string) (shared.SKUProfile, error) {
		return shared.SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}, nil
	}, nil)
	resources, err := settlement.ReserveProvisionResources(pool, claim)
	require.NoError(t, err)
	return resources
}

func actorOperationProofsForTest(
	t *testing.T,
	leaseUUID string,
	kind shared.OperationIntentKind,
) (shared.OperationReleaseUncommitted, shared.OperationReleaseCommitted) {
	t.Helper()
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	callbacks, err := newBoundCallbackStoreForTest(t, shared.CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "actor-callbacks.db")})
	require.NoError(t, err)
	_, settlement := operationSettlementForCallbackTest(t, callbacks)
	spec := shared.OperationIntentSpec{
		Kind: kind, LeaseUUID: leaseUUID, CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleURL,
		Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		Items:            []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}},
		ResourceProfiles: []shared.SKUResourceSnapshot{{SKU: "sku-a", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}},
		Manifest:         []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
	}
	if kind == shared.OperationIntentRestore {
		spec.SourceLeaseUUID = durableCallbackTestLeaseUUID2
		spec.SourceGeneration = 1
	}
	candidate, err := settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	admission, err := settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, ok := admission.CreatedClaim()
	require.True(t, ok)
	failure := commitPreEffectOperationFailureForTest(t, settlement, claim)
	committed := commitOperationSuccessForTest(t, settlement, claim)
	return failure, committed
}

// --- Migration test fixtures (Task 1, plan §Task 1.4) -----------------------
//
// These fakes back the recover-time-migration tests in migrate_test.go. They
// model the three substrates that migration touches (docker engine + compose,
// volume backend, release store) so tests can hand-craft legacy-shaped state
// and assert what the migration pipeline did with it.
//
// The fakes intentionally implement just enough of each interface for the
// migration paths to compile and run end-to-end; behaviour beyond that (e.g.
// concurrent Up calls, partial Down failures) is not modelled because the
// migration tests don't exercise it.

// fakeDocker is the shared test-side state object referenced from the
// fakeDockerClient and fakeComposeExecutor wired into the Backend by
// newMigrationTestBackend. Tests set fields on this struct to control mock
// behaviour, and read them to assert what production code did.
//
// Lumping docker-engine and compose state into a single struct keeps the
// migration tests in the plan readable (one fake to set up, one fake to
// assert against).
type fakeDocker struct {
	// Docker engine side.
	containers []ContainerInfo             // returned by ListManagedContainers
	mounts     map[string][]ContainerMount // containerID → mounts (test-only)

	// EnsureTenantNetwork hook. Default is a silent-success no-op so tests
	// that flip cfg.NetworkIsolation on (e.g. the migration-network test)
	// don't have to wire the mock directly. Tests that need to *capture*
	// the call can override this with a closure.
	ensureTenantNetwork func(ctx context.Context, tenant string) (string, error)

	// RemoveContainer hook. Default (nil) is silent success. Tests that need
	// to *capture* removals (e.g. the -prev grace-cleanup test) override this.
	removeContainer func(ctx context.Context, name string) error
	stopContainer   func(ctx context.Context, containerID string, timeout time.Duration) error

	// Compose side.
	composeUpErr           error  // returned by the compose mutation seam's Up method if non-nil
	lastComposeProjectName string // captured project.Name from the most recent Up call
	lastComposeProject     *composetypes.Project
}

// fakeVolumeBackend records RenameVolume calls and stubs the rest of the
// volumeManager interface with no-ops. RenameVolume is the new method
// introduced by Task 10; the migration logic (Task 9) will call it via a
// type assertion so this fake compiles cleanly today.
type fakeVolumeBackend struct {
	renames   [][2]string // (oldName, newName) pairs, in call order
	destroyed []string    // volume ids passed to Destroy, in call order
}

// Create returns a deterministic path so any production code that calls it
// during a test does not blow up; the migration tests do not assert on it.
func (f *fakeVolumeBackend) Create(_ context.Context, id string, _ int64) (string, bool, error) {
	return filepath.Join("/var/lib/fred/volumes", id), true, nil
}

func (f *fakeVolumeBackend) EnsureQuota(_ context.Context, _ string, _ int64) error { return nil }

func (f *fakeVolumeBackend) Destroy(_ context.Context, id string) error {
	f.destroyed = append(f.destroyed, id)
	return nil
}
func (f *fakeVolumeBackend) List() ([]string, error) { return nil, nil }
func (f *fakeVolumeBackend) ListForProof(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}
func (f *fakeVolumeBackend) Validate() error { return nil }
func (f *fakeVolumeBackend) AttestManagedVolume(context.Context, managedVolumeName) error {
	return nil
}
func (f *fakeVolumeBackend) RequireNoInterruptedVolumeMutations(context.Context) error { return nil }
func (f *fakeVolumeBackend) RecoverInterruptedVolumeMutations(context.Context) error   { return nil }

// RenameVolume captures the rename request. Returns nil unconditionally —
// migration tests assert on the recorded renames slice rather than on a
// returned error.
func (f *fakeVolumeBackend) RenameVolume(_ context.Context, oldName, newName string) error {
	f.renames = append(f.renames, [2]string{oldName, newName})
	return nil
}

// renamed reports whether (oldName → newName) appears in the rename log.
func (f *fakeVolumeBackend) renamed(oldName, newName string) bool {
	for _, r := range f.renames {
		if r[0] == oldName && r[1] == newName {
			return true
		}
	}
	return false
}

// HostPath returns a deterministic test path under /var/lib/fred/volumes
// matching the convention production code uses. Tests asserting on
// migration bind paths can predict the value.
func (f *fakeVolumeBackend) HostPath(name string) string {
	return filepath.Join("/var/lib/fred/volumes", name)
}

func (f *fakeVolumeBackend) Usage(_ context.Context, _ string) (int64, error) {
	return 0, errors.ErrUnsupported
}

func (f *fakeVolumeBackend) Kind() string { return "fake" }

// fakeReleaseStore wraps a real *shared.ReleaseStore with the test-side
// helpers expected by the migration tests. The wrapped store is real so the
// production code path (which talks to *shared.ReleaseStore directly) is
// exercised; `releases` is a setup map that tests populate and Seed flushes
// into the backing store.
type fakeReleaseStore struct {
	Store     *shared.ReleaseStore
	backend   *Backend
	authority *standaloneReleaseTestAuthority
	releases  map[string][]byte // leaseUUID → manifest payload to pre-seed
}

// Seed flushes the test-side `releases` map into the backing release store
// as "active" entries dated now. Call this after populating `releases` and
// before invoking the production code under test.
func (f *fakeReleaseStore) Seed(t *testing.T) {
	t.Helper()
	releases := make(map[string]shared.Release, len(f.releases))
	for uuid, data := range f.releases {
		releases[uuid] = shared.Release{
			Manifest:  data,
			Status:    "active",
			CreatedAt: time.Now(),
		}
	}
	f.seedV013Releases(t, releases)
}

func (f *fakeReleaseStore) SeedRelease(
	t *testing.T,
	leaseUUID string,
	release shared.Release,
) {
	t.Helper()
	f.seedV013Releases(t, map[string]shared.Release{leaseUUID: release})
}

func (f *fakeReleaseStore) seedV013Releases(
	t *testing.T,
	releases map[string]shared.Release,
) {
	t.Helper()
	require.NotNil(t, f.Store)
	require.NotNil(t, f.backend)
	require.NotNil(t, f.authority)
	require.NoError(t, f.Store.Close())
	f.Store = nil
	f.backend.releaseStore = nil
	db, err := bolt.Open(f.authority.config.DBPath, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("releases"))
		if bucket == nil {
			return errors.New("v0.13 release fixture: releases bucket is missing")
		}
		for leaseUUID, release := range releases {
			require.Nil(t, release.RuntimeAuthority)
			require.Nil(t, release.LegacyRuntimeAuthority)
			require.False(t, release.OperationID.Valid())
			if release.Version == 0 {
				release.Version = 1
			}
			data, marshalErr := json.Marshal(struct {
				SchemaVersion uint8            `json:"schema_version"`
				Releases      []shared.Release `json:"releases"`
			}{SchemaVersion: 1, Releases: []shared.Release{release}})
			if marshalErr != nil {
				return marshalErr
			}
			if err := bucket.Put([]byte(leaseUUID), data); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
	store, err := shared.OpenIdentityBoundReleaseStore(
		f.authority.config, f.authority.storage, f.authority.gate,
	)
	require.NoError(t, err)
	f.Store = store
	f.backend.releaseStore = store
	operations, err := shared.NewOperationSettlement(f.backend.callbackStore, store)
	require.NoError(t, err)
	restore, err := shared.NewRestoreSettlement(operations, f.backend.retentionStore)
	require.NoError(t, err)
	maintenance, err := shared.NewMaintenanceSettlement(f.backend.callbackStore, store)
	require.NoError(t, err)
	closeSettlement, err := shared.NewCloseSettlement(
		f.backend.callbackStore, store, f.backend.retentionStore,
	)
	require.NoError(t, err)
	f.backend.operationSettlement = operations
	backfiller, err := shared.NewReleaseBackfiller(f.backend.callbackStore, store)
	require.NoError(t, err)
	f.backend.releaseBackfiller = backfiller
	f.backend.restoreSettlement = restore
	f.backend.maintenanceSettlement = maintenance
	f.backend.closeSettlement = closeSettlement
	f.backend.releaseCapacityPlanner = operations
	bindBackendTestCloseExecutor(t, f.backend, closeSettlement)
	retentionFixtureAuthorities.Store(f.backend.retentionStore, &retentionFixtureAuthority{
		backend: f.backend, callbacks: f.backend.callbackStore, releases: store, operations: operations,
		restore: restore, close: closeSettlement,
	})
	rebuildCallbackSender(f.backend, testCallbackClient)
	bindBackendRecoveryCoordinatorForTest(t, f.backend)
}

// hasWrappedRelease reports whether the latest release for uuid carries a
// stack-shaped manifest (auto-wrapped or natively stack). Heuristic: look
// for the top-level "services" key in the stored JSON.
func (f *fakeReleaseStore) hasWrappedRelease(uuid string) bool {
	rel, err := f.Store.LatestActive(uuid)
	if err != nil || rel == nil {
		return false
	}
	return bytes.Contains(rel.Manifest, []byte(`"services"`))
}

// newMigrationTestBackend constructs a Backend wired with the migration-test
// fakes. Returns the Backend plus pointers to each fake so the test can drive
// its inputs and assert on captured outputs.
//
// The release store is real (backed by a bbolt DB in t.TempDir()) so any
// production read/write goes through the same paths as in production; closed
// automatically via t.Cleanup. Tests that seeded `fakeRel.releases` must
// invoke `fakeRel.Seed(t)` before triggering recoverState.
func newMigrationTestBackend(t *testing.T) (*Backend, *fakeDocker, *fakeVolumeBackend, *fakeReleaseStore) {
	t.Helper()

	state := &fakeDocker{mounts: make(map[string][]ContainerMount)}
	fakeVol := &fakeVolumeBackend{}

	mock := &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			// Splice in mounts from the shared state map so the
			// list payload matches what production code receives
			// from types.Container.Mounts inline.
			out := make([]ContainerInfo, len(state.containers))
			for i, c := range state.containers {
				if ms, ok := state.mounts[c.ContainerID]; ok {
					c.Mounts = append(c.Mounts, ms...)
				}
				out[i] = c
			}
			return out, nil
		},
		// StopContainer + RemoveContainer default to silent success so
		// the migration's stop-legacy / -prev-cleanup paths don't blow
		// up the test runtime (mockDockerClient.RemoveContainer panics
		// by default). RenameContainer default already returns nil
		// silently in the underlying mock.
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			if state.stopContainer != nil {
				return state.stopContainer(ctx, containerID, timeout)
			}
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, name string) error {
			if state.removeContainer != nil {
				return state.removeContainer(ctx, name)
			}
			return nil
		},
		EnsureTenantNetworkFn: func(ctx context.Context, tenant string) (string, error) {
			if state.ensureTenantNetwork != nil {
				return state.ensureTenantNetwork(ctx, tenant)
			}
			// Default: silent success. Mirrors production's idempotency:
			// a network create on an existing network returns the existing
			// ID without error.
			return "net-id-" + tenant, nil
		},
		// recoverState's cleanupOrphanedNetworks sweep runs when isolation
		// is enabled. Default to a clean network list so migration tests
		// that enable isolation don't crash; tests that need a non-empty
		// list can override on the mock directly.
		ListManagedNetworksFn: func(_ context.Context) ([]networktypes.Inspect, error) {
			return nil, nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			for i := range state.containers {
				if state.containers[i].ContainerID == containerID {
					c := state.containers[i]
					// Splice in mounts from the shared state map.
					// Production InspectContainer populates Mounts
					// directly from resp.Mounts; the test mock has to
					// merge the test-side mounts setup since
					// state.containers entries are constructed with
					// just label-bearing fields.
					if ms, ok := state.mounts[containerID]; ok {
						c.Mounts = append(c.Mounts, ms...)
					}
					// Fixtures that omit Status model the default successful Stop
					// hook above. Production InspectContainer always returns a Docker
					// state; expose the corresponding explicit quiescent state to the
					// migration's post-stop proof without changing the stale list
					// snapshot used by the broader recoverState fixture.
					if c.Status == "" {
						c.Status = "exited"
					}
					return &c, nil
				}
			}
			return nil, fmt.Errorf("not found: %s", containerID)
		},
	}

	fakeCompose := &mockComposeExecutor{
		UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
			if project == nil {
				return state.composeUpErr
			}
			state.lastComposeProjectName = project.Name
			state.lastComposeProject = project
			if state.composeUpErr != nil {
				return state.composeUpErr
			}
			// Simulate compose successfully creating containers: append one
			// post-migration ContainerInfo per service in the project, with
			// Status:"running" so waitForHealthy doesn't block. Production
			// compose creates real containers; the test fake mirrors that
			// behaviour at the ContainerInfo abstraction so downstream code
			// (resolveContainerIDsByName, ListManagedContainers) sees a
			// post-Up state consistent with production semantics.
			for _, svc := range project.Services {
				if svc.ContainerName == "" {
					continue
				}
				state.containers = append(state.containers, ContainerInfo{
					ContainerID: "post-mig-" + svc.ContainerName,
					Name:        svc.ContainerName,
					Status:      "running",
					Health:      HealthStatusNone,
				})
			}
			return nil
		},
	}

	b := newBackendForTest(mock, nil)
	b.compose = fakeCompose
	b.volumes = fakeVol
	attachBoundOperationHandoffStores(t, b)
	relStore := b.releaseStore
	fakeRel := &fakeReleaseStore{
		Store: relStore, backend: b,
		authority: &standaloneReleaseTestAuthority{
			config:  shared.ReleaseStoreConfig{DBPath: b.cfg.ReleasesDBPath},
			storage: b.storageAuthority,
			gate:    b.storeAuthorityGate,
		},
		releases: make(map[string][]byte),
	}
	t.Cleanup(func() {
		if fakeRel.Store != nil {
			_ = fakeRel.Store.Close()
		}
	})
	// Upgrade tests assume managed volume sources live under this root.
	b.cfg.VolumeDataPath = "/var/lib/fred/volumes"
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	return b, state, fakeVol, fakeRel
}

// volDestroyer reaches the destroy capability that volumeManager deliberately withholds,
// which production code obtains only inside volumeOp (volume_destroy.go, ENG-658).
//
// Integration tests drive a real filesystem manager directly to build and tear down
// fixtures — precisely the case the choke point does not serve, since there is no lease
// asserting ownership and no retention record to resolve one from. Narrowing here rather
// than widening volumeManager keeps the production seam intact: b.volumes still cannot
// reach Destroy in any non-test file.
func volDestroyer(tb testing.TB, vm volumeManager) volumeDestroyer {
	tb.Helper()
	d, ok := vm.(volumeDestroyer)
	require.True(tb, ok, "volume manager %T cannot destroy; fixture teardown needs it", vm)
	return d
}

// volumeSet is a mutable stand-in for the volumes on disk: List reports what is present and
// Destroy removes it, so a test that destroys and then re-enumerates sees what a real
// filesystem would.
//
// Static ListFn closures were fine while nothing re-read the root mid-operation. The reaping
// finalizer now CONFIRMS a lease's footprint is gone before dropping its record (ENG-687) —
// a destroy is an os.RemoveAll that treats an already-absent path as done, so "every destroy
// succeeded" is also what a vanished mount looks like — and a fixture whose List ignores its
// own destroys makes that confirmation unsatisfiable.
type volumeSet struct {
	mu        sync.Mutex
	present   map[string]bool
	destroyed []string
	destroyFn func(id string) error // optional: fail or observe before removal
}

func newVolumeSet(names ...string) *volumeSet {
	vs := &volumeSet{present: make(map[string]bool, len(names))}
	for _, n := range names {
		vs.present[n] = true
	}
	return vs
}

func (v *volumeSet) list() ([]string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]string, 0, len(v.present))
	for n := range v.present {
		out = append(out, n)
	}
	sort.Strings(out) // stable, so failure output reads the same twice
	return out, nil
}

func (v *volumeSet) destroy(_ context.Context, id string) error {
	if v.destroyFn != nil {
		if err := v.destroyFn(id); err != nil {
			return err // still on disk: leave it present
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.present, id)
	v.destroyed = append(v.destroyed, id)
	return nil
}

func (v *volumeSet) rename(oldName, newName string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	oldPresent := v.present[oldName]
	newPresent := v.present[newName]
	switch {
	case oldPresent && !newPresent:
		delete(v.present, oldName)
		v.present[newName] = true
		return nil
	case !oldPresent && newPresent:
		return nil // idempotent retry: the previous rename already completed
	case oldPresent && newPresent:
		return fmt.Errorf("both old volume %q and new volume %q are present", oldName, newName)
	default:
		return fmt.Errorf("neither old volume %q nor new volume %q is present", oldName, newName)
	}
}

// names returns the volumes destroyed so far, in call order.
func (v *volumeSet) names() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.destroyed...)
}

// manager wires the set into a mockVolumeManager.
func (v *volumeSet) manager() *mockVolumeManager {
	return &mockVolumeManager{
		ListFn: v.list, DestroyFn: v.destroy, RenameVolumeFn: v.rename,
	}
}

// rollbackRestoreAdoption preserves the former phase-level fixture seam for
// tests that exercise physical/quota handback independently of operation-intent
// settlement. Production callers must choose one of the accepted/unaccepted
// wrappers, which makes the settlement owner explicit.
func (b *Backend) prepareRestoreAdoptionRollbackForTest(
	ctx context.Context,
	leaseUUID string,
	rec *shared.RetentionEntry,
	dropProvision bool,
	logger *slog.Logger,
) ([]shared.SKUResourceSnapshot, bool) {
	stopTimeout := b.cfg.ContainerStopTimeout
	if stopTimeout <= 0 {
		stopTimeout = 30 * time.Second
	}
	b.provisionsMu.RLock()
	var recordedIDs []string
	if provision := b.provisions[leaseUUID]; provision != nil {
		recordedIDs = slices.Clone(provision.ContainerIDs)
	}
	b.provisionsMu.RUnlock()
	teardownOp := teardownOpRestoreRollback
	if dropProvision {
		teardownOp = teardownOpRestorePrelude
	}
	if _, err := b.teardownLeaseContainers(
		ctx, leaseUUID, recordedIDs, stopTimeout, teardownOp, logger,
	); err != nil && !dropProvision {
		return nil, false
	}
	for _, retained := range rec.RetainedVolumeNames {
		canonical := retainedToNewCanonical(retained, rec.OriginalLeaseUUID, leaseUUID)
		if err := b.renameIfPresent(ctx, canonical, retained); err != nil {
			if dropProvision {
				b.removeProvision(leaseUUID)
			}
			return nil, false
		}
	}
	profiles, err := b.restoreRetainedVolumeQuotas(ctx, rec)
	if err != nil {
		if dropProvision {
			b.removeProvision(leaseUUID)
		}
		return nil, false
	}
	return profiles, true
}

func (b *Backend) rollbackRestoreAdoption(
	ctx context.Context,
	leaseUUID string,
	allocatedIDs []string,
	rec *shared.RetentionEntry,
	dropProvision bool,
	logger *slog.Logger,
) bool {
	resourceProfiles, prepared := b.prepareRestoreAdoptionRollbackForTest(
		ctx, leaseUUID, rec, dropProvision, logger,
	)
	if !prepared {
		return false
	}
	return b.completeRestoreAdoptionRollback(
		leaseUUID, allocatedIDs, rec, resourceProfiles, dropProvision, logger,
	)
}
