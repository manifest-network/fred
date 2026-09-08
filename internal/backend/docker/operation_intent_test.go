package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

type fixedOperationIntentProbeJournal struct {
	operationSettlementService
	disposition shared.OperationIntentAdmissionDisposition
}

type blockingOperationIntentJournal struct {
	operationSettlementService
	delegate operationSettlementService
	began    chan struct{}
	release  chan struct{}
	once     sync.Once
}

type transientResolveOperationIntentJournal struct {
	callbackPublicationService
	mu       sync.Mutex
	failures int
	calls    int
}

func (j *transientResolveOperationIntentJournal) failSettlement() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.calls++
	if j.failures > 0 {
		j.failures--
		return errors.New("transient operation settlement failure")
	}
	return nil
}

func (j *transientResolveOperationIntentJournal) PublishOperationSuccessContext(
	ctx context.Context,
	proof shared.OperationReleaseCommitted,
) error {
	if err := j.failSettlement(); err != nil {
		return err
	}
	return j.callbackPublicationService.PublishOperationSuccessContext(ctx, proof)
}

func (j *transientResolveOperationIntentJournal) PublishOperationFailureContext(
	ctx context.Context,
	proof shared.OperationReleaseUncommitted,
	errMsg string,
) error {
	if err := j.failSettlement(); err != nil {
		return err
	}
	return j.callbackPublicationService.PublishOperationFailureContext(ctx, proof, errMsg)
}

func (j *transientResolveOperationIntentJournal) resolveCalls() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.calls
}

func (j *blockingOperationIntentJournal) ProbeOperationIntent(
	probe shared.OperationIntentProbe,
) (shared.OperationIntentAdmissionDisposition, error) {
	return j.delegate.ProbeOperationIntent(probe)
}

func (j *blockingOperationIntentJournal) NewOperationIntentProbe(
	leaseUUID, callbackURL string,
) (shared.OperationIntentProbe, error) {
	return j.delegate.NewOperationIntentProbe(leaseUUID, callbackURL)
}

func (j *blockingOperationIntentJournal) NewOperationIntentCandidate(
	spec shared.OperationIntentSpec,
) (shared.OperationIntentCandidate, error) {
	return j.delegate.NewOperationIntentCandidate(spec)
}

func (j *blockingOperationIntentJournal) BeginOperationIntent(
	candidate shared.OperationIntentCandidate,
) (shared.OperationIntentAdmission, error) {
	j.once.Do(func() { close(j.began) })
	<-j.release
	return j.delegate.BeginOperationIntent(candidate)
}

func (j *blockingOperationIntentJournal) ListOperationIntents() ([]shared.OperationIntentClaim, error) {
	return j.delegate.ListOperationIntents()
}

func (j *blockingOperationIntentJournal) wrappedOperationSettlementForTest() operationSettlementService {
	return j.delegate
}

func (j fixedOperationIntentProbeJournal) ProbeOperationIntent(
	shared.OperationIntentProbe,
) (shared.OperationIntentAdmissionDisposition, error) {
	return j.disposition, nil
}

func (fixedOperationIntentProbeJournal) NewOperationIntentProbe(
	string, string,
) (shared.OperationIntentProbe, error) {
	// This fake's ProbeOperationIntent deliberately answers without inspecting
	// its argument, so it does not need (and cannot forge) journal authority.
	return shared.OperationIntentProbe{}, nil
}

func (fixedOperationIntentProbeJournal) NewOperationIntentCandidate(
	shared.OperationIntentSpec,
) (shared.OperationIntentCandidate, error) {
	return shared.OperationIntentCandidate{}, errors.New(
		"unexpected operation candidate construction after exact probe",
	)
}

func (fixedOperationIntentProbeJournal) BeginOperationIntent(
	shared.OperationIntentCandidate,
) (shared.OperationIntentAdmission, error) {
	return shared.OperationIntentAdmission{}, errors.New("unexpected BeginOperationIntent after exact probe")
}

func (fixedOperationIntentProbeJournal) ListOperationIntents() ([]shared.OperationIntentClaim, error) {
	return nil, nil
}

func (fixedOperationIntentProbeJournal) ResolveOperationIntent(
	shared.OperationIntentClaim,
	backend.CallbackStatus,
	string,
) (shared.CallbackEntry, error) {
	return shared.CallbackEntry{}, errors.New("unexpected ResolveOperationIntent after exact probe")
}

var dockerOperationIntentID = mustDockerOperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")

func beginDockerTestOperationIntent(
	t *testing.T,
	store *shared.CallbackStore,
	spec shared.OperationIntentSpec,
	_ backendidentity.ID,
) (shared.OperationIntentAdmission, error) {
	t.Helper()
	_, settlement := operationSettlementForCallbackTest(t, store)
	candidate, err := settlement.NewOperationIntentCandidate(spec)
	if err != nil {
		return shared.OperationIntentAdmission{}, err
	}
	return settlement.BeginOperationIntent(candidate)
}

func beginBoundDockerTestOperationIntent(
	t *testing.T,
	store *shared.CallbackStore,
	spec shared.OperationIntentSpec,
) (shared.OperationIntentAdmission, error) {
	t.Helper()
	_, settlement := operationSettlementForCallbackTest(t, store)
	candidate, err := settlement.NewOperationIntentCandidate(spec)
	if err != nil {
		return shared.OperationIntentAdmission{}, err
	}
	return settlement.BeginOperationIntent(candidate)
}

func dockerTestOperationIntentProbe(
	t *testing.T,
	store *shared.CallbackStore,
	spec shared.OperationIntentSpec,
) shared.OperationIntentProbe {
	t.Helper()
	_, settlement := operationSettlementForCallbackTest(t, store)
	probe, err := settlement.NewOperationIntentProbe(spec.LeaseUUID, spec.CallbackURL)
	require.NoError(t, err)
	return probe
}

func createdDockerOperationClaim(
	t *testing.T,
	admission shared.OperationIntentAdmission,
) shared.OperationIntentClaim {
	t.Helper()
	claim, created := admission.CreatedClaim()
	require.True(t, created, "operation admission must carry first-dispatch authority")
	return claim
}

func TestReleaseHistoryCapacityPlannerDoesNotReturnTypedNilStore(t *testing.T) {
	t.Parallel()

	var b Backend
	if planner := b.releaseHistoryCapacityPlanner(); planner != nil {
		t.Fatalf("nil release store returned a non-nil capacity planner of type %T", planner)
	}
}

func dockerOperationIntentSpec(t *testing.T, _ backendidentity.ID) shared.OperationIntentSpec {
	t.Helper()
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + dockerOperationIntentID.String()
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	items := []backend.LeaseItem{{
		SKU: "docker-micro", ServiceName: "app", Quantity: 1,
	}}
	return shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            "550e8400-e29b-41d4-a716-446655440000",
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleURL,
		Tenant:               "tenant-a",
		ProviderUUID:         "22222222-2222-4222-8222-222222222222",
		Items:                items,
		ResourceProfiles:     testResourceProfiles(t, items),
		Manifest:             validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"}),
	}
}

func putRestoreIntentFinalizer(
	t *testing.T,
	retentions *shared.RetentionStore,
	spec shared.OperationIntentSpec,
	entry shared.RetentionEntry,
) *shared.RetentionEntry {
	t.Helper()
	stack, err := manifest.ParsePayload(spec.Manifest)
	require.NoError(t, err)
	entry.StackManifest = stack
	entry.DestinationItems = append([]backend.LeaseItem(nil), spec.Items...)
	entry.DestinationResourceProfiles = shared.CloneSKUResourceSnapshot(spec.ResourceProfiles)
	entry.DestinationOperationID = dockerOperationIntentID
	entry.DestinationCallbackURL = spec.CallbackURL
	entry.DestinationLifecycleCallbackURL = spec.LifecycleCallbackURL
	return putRestoringRetention(t, retentions, entry)
}

func dockerIntentContainer(spec shared.OperationIntentSpec, id, sku string, index int) ContainerInfo {
	stack, _ := manifest.ParsePayload(spec.Manifest)
	image := ""
	if stack != nil && stack.Services[spec.Items[0].ServiceName] != nil {
		image = stack.Services[spec.Items[0].ServiceName].Image
	}
	effectiveItems := spec.EffectiveItems
	if len(effectiveItems) == 0 {
		effectiveItems = spec.Items
	}
	return ContainerInfo{
		ContainerID:          id,
		LeaseUUID:            spec.LeaseUUID,
		Tenant:               spec.Tenant,
		ProviderUUID:         spec.ProviderUUID,
		BackendName:          DefaultConfig().Name,
		SKU:                  sku,
		ServiceName:          spec.Items[0].ServiceName,
		InstanceIndex:        index,
		CallbackURL:          spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL,
		Image:                image,
		CustomDomain:         effectiveItems[0].CustomDomain,
		Status:               "running",
		Health:               HealthStatusNone,
		CreatedAt:            time.Now(),
		Name:                 id,
	}
}

func newOperationIntentRecoveryBackend(
	t *testing.T,
	store *shared.CallbackStore,
	_ backendidentity.ID,
	containers []ContainerInfo,
	provisions map[string]*provision,
) *Backend {
	t.Helper()
	inventory := append([]ContainerInfo(nil), containers...)
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return append([]ContainerInfo(nil), inventory...), nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			for _, container := range inventory {
				if container.ContainerID == containerID {
					copy := container
					return &copy, nil
				}
			}
			return nil, assert.AnError
		},
	}
	b := newBackendForTest(mock, provisions)
	// Recovery preserves nonterminal substrate until the durable provisioning
	// visibility horizon expires. Unit fixtures use a short horizon unless a
	// test explicitly exercises deferral across periodic sweeps.
	b.cfg.ContainerStartTimeout = 20 * time.Millisecond
	b.cfg.ProvisionTimeout = 20 * time.Millisecond
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		inventory = nil
		return nil
	}}
	b.storageIdentity = operationIntentTestStoreIdentity(t, store)
	b.callbackStore = store
	releaseStore, retentionStore, settlement, restoreSettlement, closeSettlement := operationHandoffForCallbackTest(t, store)
	b.releaseStore = releaseStore
	backfiller, err := shared.NewReleaseBackfiller(store, releaseStore)
	require.NoError(t, err)
	b.releaseBackfiller = backfiller
	b.operationSettlement = settlement
	b.retentionStore = retentionStore
	b.restoreSettlement = restoreSettlement
	b.closeSettlement = closeSettlement
	b.releaseCapacityPlanner = settlement
	b.maintenanceSettlement = newTestMaintenanceSettlement(t, store, releaseStore)
	require.NoError(t, bindBackendTestPhysicalExecutors(
		b, b.operationSettlement.(*shared.OperationSettlement), b.maintenanceSettlement,
	))
	bindBackendTestCloseExecutor(t, b, closeSettlement)
	// Every fixture container carrying the pending operation callback represents
	// substrate that could only have been created after the durable Started
	// boundary. Mirror that production invariant explicitly; a bare Begin row is
	// pre-effect authority and is intentionally ineligible to attest a container.
	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	for _, claim := range claims {
		if claim.ExecutionPhase() != shared.OperationExecutionBeforeEffects {
			continue
		}
		observedCandidate := false
		for _, container := range inventory {
			if container.LeaseUUID == claim.LeaseUUID() &&
				(container.CallbackURL == claim.CallbackURL() ||
					container.LifecycleCallbackURL == claim.LifecycleCallbackURL()) {
				observedCandidate = true
				break
			}
		}
		if !observedCandidate {
			continue
		}
		candidate, candidateErr := b.operationSettlement.PrepareOperationRelease(claim)
		require.NoError(t, candidateErr)
		_, startErr := b.operationSettlement.StartOperationExecution(candidate)
		require.NoError(t, startErr)
	}
	authorityValue, ok := operationIntentTestAuthorities.Load(store)
	require.True(t, ok)
	authority := authorityValue.(*operationIntentTestAuthority)
	b.storageAuthority = authority.storage
	b.storeAuthorityGate = authority.gate
	b.storageVerifier = testDockerRuntimeStorageVerifier{id: authority.storage.ID()}
	b.callbackPublisher = callbackPublisherForCallbackTest(t, store)
	bindBackendRecoveryCoordinatorForTest(t, b)
	return b
}

// startPendingOperationForRecoveryTest models a crash after the durable
// execution boundary but before any candidate container is visible. Fixtures
// with a candidate container cross this boundary automatically because such a
// container is unrepresentable before Started; empty fixtures must choose the
// side of the write-ahead window explicitly.
func startPendingOperationForRecoveryTest(t *testing.T, b *Backend) shared.OperationIntentClaim {
	t.Helper()
	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	candidate, err := b.operationSettlement.PrepareOperationRelease(claims[0])
	require.NoError(t, err)
	_, err = b.operationSettlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	started, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, started, 1)
	require.Equal(t, shared.OperationExecutionStarted, started[0].ExecutionPhase())
	return started[0]
}

func readyIntentProjection(spec shared.OperationIntentSpec, containerIDs ...string) map[string]*provision {
	effectiveItems := spec.EffectiveItems
	if len(effectiveItems) == 0 {
		effectiveItems = spec.Items
	}
	stack, _ := manifest.ParsePayload(spec.Manifest)
	return map[string]*provision{
		spec.LeaseUUID: {
			ProvisionState: leasesm.ProvisionState{
				LeaseUUID:            spec.LeaseUUID,
				Tenant:               spec.Tenant,
				ProviderUUID:         spec.ProviderUUID,
				Status:               backend.ProvisionStatusReady,
				CallbackURL:          spec.CallbackURL,
				LifecycleCallbackURL: spec.LifecycleCallbackURL,
				ActiveOperationID:    dockerOperationIntentID,
				Items:                append([]backend.LeaseItem(nil), effectiveItems...),
				ResourceProfiles:     shared.CloneSKUResourceSnapshot(spec.ResourceProfiles),
				ContainerIDs:         append([]string(nil), containerIDs...),
				StackManifest:        stack,
			},
		},
	}
}

func TestRecoverOperationIntent_RestartAfterSubstrateSuccessBeforeCallback(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	spec := dockerOperationIntentSpec(t, storageID)
	oldOperationID := mustDockerOperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
	oldCallbackURL := "https://fred.example/callbacks/provision?operation_id=" + oldOperationID.String()
	oldLifecycleURL, err := backend.ResolveLifecycleCallbackURL(oldCallbackURL, "")
	require.NoError(t, err)
	oldAuthority, err := shared.NewReleaseRuntimeAuthority(
		oldOperationID, spec.Tenant, spec.ProviderUUID, oldCallbackURL, oldLifecycleURL,
	)
	require.NoError(t, err)
	releases, settlement := operationSettlementForCallbackTest(t, store)
	seedProvisionReleaseForLeaseTest(t, store, releases, settlement, spec.LeaseUUID, shared.Release{
		Manifest:    spec.Manifest,
		Image:       "stack",
		OperationID: oldOperationID,
		Items: []backend.LeaseItem{{
			SKU: spec.Items[0].SKU, ServiceName: spec.Items[0].ServiceName, Quantity: 2,
		}},
		ResourceProfiles: shared.CloneSKUResourceSnapshot(spec.ResourceProfiles),
		RuntimeAuthority: &oldAuthority,
		Status:           "active", CreatedAt: time.Now(),
	})
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	closeOperationSettlementForCallbackTest(t, store)
	require.NoError(t, store.Close(), "simulate the backend process crashing with only the intent durable")

	store, err = newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID),
	)
	b.cfg.ProvisionTimeout = time.Hour
	// A same-manifest previous release is not sufficient: quantity/domain
	// changes can retain identical payload bytes while changing the exact
	// container cohort. Recovery must supersede this stale topology.

	require.NoError(t, b.recoverOperationIntents(context.Background()))
	intents, err := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	release, err := b.releaseStore.LatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, release)
	assert.Equal(t, spec.Manifest, release.Manifest)
	assert.Equal(t, spec.Items, release.Items)
	require.NotNil(t, b.provisions[spec.LeaseUUID].StackManifest)
}

func TestRecoverOperationIntent_BoundsContainerInspection(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID),
	)
	b.recoveryDockerReadTimeout = 10 * time.Millisecond
	b.docker = &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{container}, nil
		},
		InspectContainerFn: func(ctx context.Context, _ string) (*ContainerInfo, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	started := time.Now()
	err = b.recoverOperationIntents(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), time.Second,
		"a stalled Docker inspection must not wedge operation recovery")
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Len(t, intents, 1, "timed-out inspection must preserve the exact operation intent")
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending, "timed-out inspection must not manufacture a completion")
}

func TestRecoverOperationIntent_FinalVisibilityReadTimeoutPreservesIntentForRetry(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)
	startPendingOperationForRecoveryTest(t, b)
	b.recoveryDockerReadTimeout = 10 * time.Millisecond
	b.cfg.ContainerStartTimeout = 20 * time.Millisecond
	b.cfg.ProvisionTimeout = time.Hour
	lists := 0
	b.docker = &mockDockerClient{ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
		lists++
		if lists == 1 {
			return nil, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	teardownCalls := 0
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		teardownCalls++
		return nil
	}}

	require.NoError(t, b.recoverOperationIntents(context.Background()),
		"startup must defer a young Started operation after one empty observation")
	b.cfg.ProvisionTimeout = time.Nanosecond
	started := time.Now()
	err = b.recoverLiveOperationIntents(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), time.Second)
	assert.Equal(t, 2, lists, "a timed-out final read must return instead of spinning")
	assert.Zero(t, teardownCalls)
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr,
		"a read-only uncertain observation preserves retryability; it does not latch mutation ambiguity")
	require.Len(t, intents, 1)
	closeOperationSettlementForCallbackTest(t, store)
	require.NoError(t, store.Close())
	store, err = newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	intents, listErr = listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Len(t, intents, 1)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)
}

func TestRecoverOperationIntent_WaitsInProcessForProvisionHealth(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.HealthCheckServices = []string{spec.Items[0].ServiceName}
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	container.Health = HealthStatusStarting
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID),
	)
	b.cfg.ProvisionTimeout = 3 * time.Second
	teardownCalls := 0
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		teardownCalls++
		return nil
	}}
	dockerMock := b.docker.(*mockDockerClient)
	inspectCalls := 0
	dockerMock.InspectContainerFn = func(context.Context, string) (*ContainerInfo, error) {
		inspectCalls++
		observed := container
		if inspectCalls > 1 {
			observed.Health = HealthStatusHealthy
		}
		return &observed, nil
	}

	startedAt := time.Now()
	err = b.recoverOperationIntents(context.Background())
	require.NoError(t, err)
	assert.Less(t, time.Since(startedAt), time.Second,
		"startup must defer a young transitional generation")
	assert.Zero(t, teardownCalls, "startup must not convert transitional health into destructive failure recovery")
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	require.Len(t, intents, 1)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)

	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	intents, listErr = listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr = store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status,
		"a later healthy observation in the same startup must settle the operation normally")
	assert.GreaterOrEqual(t, inspectCalls, 2)
}

func TestRecoverOperationIntent_ReobservesEmptyInventoryBeforeSettlement(t *testing.T) {
	for _, kind := range []shared.OperationIntentKind{
		shared.OperationIntentProvision,
		shared.OperationIntentRestore,
	} {
		t.Run(string(kind), func(t *testing.T) {
			storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
			require.NoError(t, err)
			store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })

			spec := dockerOperationIntentSpec(t, storageID)
			spec.Kind = kind
			if kind == shared.OperationIntentRestore {
				spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
				spec.SourceGeneration = 1
			}
			_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
			require.NoError(t, err)
			container := dockerIntentContainer(spec, "late-container", spec.Items[0].SKU, 0)
			b := newOperationIntentRecoveryBackend(
				t, store, storageID, nil, readyIntentProjection(spec, container.ContainerID),
			)
			startPendingOperationForRecoveryTest(t, b)
			b.cfg.ContainerStartTimeout = 10 * time.Millisecond
			b.cfg.ProvisionTimeout = time.Hour
			if kind == shared.OperationIntentRestore {
				retentions := attachRetentionStore(t, b)
				putRestoreIntentFinalizer(t, retentions, spec, shared.RetentionEntry{
					OriginalLeaseUUID: spec.SourceLeaseUUID,
					NewLeaseUUID:      spec.LeaseUUID,
					Tenant:            spec.Tenant,
					ProviderUUID:      spec.ProviderUUID,
					Items:             slices.Clone(spec.Items),
					Status:            shared.RetentionStatusRestoring,
					Generation:        spec.SourceGeneration,
				})
			}

			lists := 0
			b.docker.(*mockDockerClient).ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
				lists++
				if lists == 1 {
					return nil, nil
				}
				return []ContainerInfo{container}, nil
			}
			b.docker.(*mockDockerClient).InspectContainerFn = func(_ context.Context, containerID string) (*ContainerInfo, error) {
				if containerID != container.ContainerID {
					return nil, fmt.Errorf("unknown test container %q", containerID)
				}
				copy := container
				return &copy, nil
			}

			require.NoError(t, b.recoverOperationIntents(context.Background()))
			intents, listErr := listOperationIntentsForCallbackTest(t, store)
			require.NoError(t, listErr)
			require.Len(t, intents, 1,
				"one empty startup observation must preserve a Started operation")
			require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
			assert.GreaterOrEqual(t, lists, 2,
				"a later recovery sweep must re-observe instead of reusing empty inventory")
			intents, listErr = listOperationIntentsForCallbackTest(t, store)
			require.NoError(t, listErr)
			assert.Empty(t, intents)
			pending, listErr := store.ListPending()
			require.NoError(t, listErr)
			var operationCallbacks []shared.CallbackEntry
			for _, callback := range pending {
				if callback.CallbackURL == spec.CallbackURL {
					operationCallbacks = append(operationCallbacks, callback)
				}
			}
			require.Len(t, operationCallbacks, 1)
			assert.Equal(t, backend.CallbackStatusSuccess, operationCallbacks[0].Status,
				"the late exact cohort must be classified instead of overwritten by an absence failure")
		})
	}
}

func TestRecoverOperationIntent_RetriesTransientSettlementOnNextPass(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID),
	)
	journal := &transientResolveOperationIntentJournal{
		callbackPublicationService: b.callbackPublisher, failures: 1,
	}
	b.callbackPublisher = journal

	err = b.recoverOperationIntents(context.Background())
	require.ErrorContains(t, err, "transient operation settlement failure")
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	require.Len(t, intents, 1, "a failed commit must retain its sealed recovery capability")
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)
	release, releaseErr := b.releaseStore.LatestActive(spec.LeaseUUID)
	require.NoError(t, releaseErr)
	require.NotNil(t, release, "the substrate commit remains the level-triggered success proof")

	require.NoError(t, b.recoverLiveOperationIntents(context.Background()),
		"the next periodic pass must retry the durable operation without a restart")
	assert.Equal(t, 2, journal.resolveCalls())
	intents, listErr = listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr = store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
}

func TestRecoverLiveOperationIntent_RetriesFailedActorSettlementWithoutDroppingProjection(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	projections := readyIntentProjection(spec)
	projections[spec.LeaseUUID].Status = backend.ProvisionStatusFailed
	projections[spec.LeaseUUID].Message = "image startup failed"
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, projections)
	startPendingOperationForRecoveryTest(t, b)
	journal := &transientResolveOperationIntentJournal{
		callbackPublicationService: b.callbackPublisher, failures: 1,
	}
	b.callbackPublisher = journal

	err = b.recoverLiveOperationIntents(context.Background())
	require.ErrorContains(t, err, "transient operation settlement failure")
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	assert.Equal(t, 2, journal.resolveCalls())

	b.provisionsMu.RLock()
	projection := b.provisions[spec.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection, "settlement retry must not run cold-recovery projection cleanup")
	assert.Equal(t, backend.ProvisionStatusFailed, projection.Status)
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, "image startup failed", pending[0].Error)
}

func TestRecoverLiveOperationIntent_DefersLeaseWithActiveCommand(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)
	listCalls := 0
	b.docker = &mockDockerClient{ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
		listCalls++
		return nil, nil
	}}

	unlock := b.commandFence.Lock(spec.LeaseUUID)
	started := time.Now()
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	assert.Less(t, time.Since(started), time.Second,
		"background recovery must never wait behind an active tenant command")
	assert.Zero(t, listCalls, "a busy operation must not be observed as crash recovery")
	unlock()
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Len(t, intents, 1)
}

func TestRecoverOperationIntent_TearsDownCreateThatAppearsAfterFirstDown(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)
	startPendingOperationForRecoveryTest(t, b)
	b.cfg.ContainerStartTimeout = 20 * time.Millisecond
	b.cfg.ProvisionTimeout = time.Hour
	late := []ContainerInfo{
		dockerIntentContainer(spec, "late-after-down", spec.Items[0].SKU, 0),
		dockerIntentContainer(spec, "later-after-targeted-remove", spec.Items[0].SKU, 0),
	}
	lists := 0
	lateVisible := false
	removed := make([]string, 0, len(late))
	b.docker = &mockDockerClient{ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
		lists++
		if lateVisible && len(removed) < len(late) {
			return []ContainerInfo{late[len(removed)]}, nil
		}
		return nil, nil
	}, InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
		for _, container := range late {
			if container.ContainerID == containerID {
				copy := container
				return &copy, nil
			}
		}
		return nil, fmt.Errorf("inspect unexpected container %q", containerID)
	}, RemoveContainerFn: func(_ context.Context, containerID string) error {
		if len(removed) >= len(late) || containerID != late[len(removed)].ContainerID {
			return fmt.Errorf("remove unexpected container %q", containerID)
		}
		removed = append(removed, containerID)
		return nil
	}}
	downCalls := 0
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		downCalls++
		lateVisible = true
		return nil
	}}

	require.NoError(t, b.recoverOperationIntents(context.Background()))
	assert.Zero(t, downCalls, "startup must defer an empty Started generation")
	b.cfg.ProvisionTimeout = time.Nanosecond
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	assert.Equal(t, 1, downCalls,
		"positive survivor evidence must not be delegated back to the disproved Compose sweep")
	assert.Equal(t, []string{late[0].ContainerID, late[1].ContainerID}, removed,
		"each cohort published after Down or a targeted removal must be removed by exact container identity")
	assert.GreaterOrEqual(t, lists, 5)
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
}

func TestTeardownRecoveredOperationRejectsLateContradictoryAuthority(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *ContainerInfo)
		want   string
	}{
		{
			name: "different operation generation",
			mutate: func(t *testing.T, container *ContainerInfo) {
				container.CallbackURL = "https://fred.example/callbacks/provision?operation_id=11111111-1111-4111-8111-111111111111"
				var err error
				container.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(container.CallbackURL, "")
				require.NoError(t, err)
			},
			want: "another callback generation",
		},
		{
			name: "different principal",
			mutate: func(_ *testing.T, container *ContainerInfo) {
				container.Tenant = "tenant-b"
			},
			want: "does not match the intent",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
			require.NoError(t, err)
			store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := dockerOperationIntentSpec(t, storageID)
			_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
			require.NoError(t, err)
			b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)
			claim := startPendingOperationForRecoveryTest(t, b)

			foreign := dockerIntentContainer(spec, "late-contradictory-authority", spec.Items[0].SKU, 0)
			test.mutate(t, &foreign)
			visible := false
			removeCalls := 0
			b.docker = &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					if visible {
						return []ContainerInfo{foreign}, nil
					}
					return nil, nil
				},
				InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
					copy := foreign
					return &copy, nil
				},
				RemoveContainerFn: func(context.Context, string) error {
					removeCalls++
					return nil
				},
			}
			b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
				visible = true
				return nil
			}}

			bindBackendRecoveryCoordinatorForTest(t, b)
			acquired, scopeErr := b.recoveryCoordinator.WithLease(
				t.Context(), spec.LeaseUUID,
				func(scope shared.LeaseRecoveryScope) error {
					outcome, cleanupErr := b.operationSettlement.CleanupRecoveredOperation(t.Context(), scope, claim)
					require.NoError(t, cleanupErr)
					ambiguous, ok := outcome.(shared.OperationExecutionAmbiguous)
					require.True(t, ok, "contradictory authority must retain an ambiguous outcome")
					err = ambiguous.Cause()
					return nil
				},
			)
			require.NoError(t, scopeErr)
			require.True(t, acquired)
			require.ErrorContains(t, err, test.want)
			assert.Zero(t, removeCalls,
				"lease UUID alone must not authorize deletion of a late contradictory container")
		})
	}
}

func TestRecoverState_RemovesLateCohortForDurablyFailedOperation(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	admission, err := beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	_, settlement := operationSettlementForCallbackTest(t, store)
	claim := createdDockerOperationClaim(t, admission)
	uncommitted := commitPreEffectOperationFailureForTest(t, settlement, claim)
	err = callbackPublisherForCallbackTest(t, store).PublishOperationFailureContext(
		context.Background(), uncommitted, interruptedOperationFailure,
	)
	require.NoError(t, err)

	late := dockerIntentContainer(spec, "late-after-settlement", spec.Items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{late}, readyIntentProjection(spec, late.ContainerID),
	)
	removed := false
	removeCalls := 0
	dockerMock := b.docker.(*mockDockerClient)
	dockerMock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
		if removed {
			return nil, nil
		}
		return []ContainerInfo{late}, nil
	}
	dockerMock.RemoveContainerFn = func(context.Context, string) error {
		removeCalls++
		removed = true
		return nil
	}

	require.NoError(t, b.recoverState(context.Background()))
	assert.Equal(t, 1, removeCalls,
		"the terminal Failed head must remain active cleanup authority for a late daemon Create")
	b.provisionsMu.RLock()
	_, published := b.provisions[spec.LeaseUUID]
	b.provisionsMu.RUnlock()
	assert.False(t, published, "a failed operation's late cohort must never be published Ready")
	states, stateErr := operationSettlementServiceForCallbackTest(t, store).ListOperationRecoveryStates()
	require.NoError(t, stateErr)
	require.Len(t, states, 1)
	_, remainsFailed := states[0].(shared.OperationFailed)
	assert.True(t, remainsFailed,
		"cleanup must preserve the level-triggered terminal failure tombstone")
}

func TestRecoverState_RemovesArchivedFailedGenerationWithoutTouchingSuccessor(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	failedSpec := dockerOperationIntentSpec(t, storageID)
	failedAdmission, err := beginDockerTestOperationIntent(t, store, failedSpec, storageID)
	require.NoError(t, err)
	_, settlement := operationSettlementForCallbackTest(t, store)
	failedClaim := createdDockerOperationClaim(t, failedAdmission)
	uncommitted := commitPreEffectOperationFailureForTest(t, settlement, failedClaim)
	err = callbackPublisherForCallbackTest(t, store).PublishOperationFailureContext(
		context.Background(), uncommitted, interruptedOperationFailure,
	)
	require.NoError(t, err)
	failedCallbacks, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, failedCallbacks, 1)
	acknowledgePendingCallbacksForTest(t, store)

	successorSpec := dockerOperationIntentSpec(t, storageID)
	successorID := mustDockerOperationID("7ba7b810-9dad-41d1-80b4-00c04fd430c8")
	successorSpec.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + successorID.String()
	successorSpec.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(
		successorSpec.CallbackURL, "",
	)
	require.NoError(t, err)
	_, err = beginDockerTestOperationIntent(t, store, successorSpec, storageID)
	require.NoError(t, err, "a successor must archive, not erase, the terminal failure fence")

	lateFailed := dockerIntentContainer(
		failedSpec, "late-failed-generation", failedSpec.Items[0].SKU, 0,
	)
	liveSuccessor := dockerIntentContainer(
		successorSpec, "live-successor-generation", successorSpec.Items[0].SKU, 0,
	)
	inventory := []ContainerInfo{lateFailed, liveSuccessor}
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)
	dockerMock := b.docker.(*mockDockerClient)
	listCalls := 0
	dockerMock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
		listCalls++
		if listCalls == 1 {
			// Model A becoming visible after the failed-receipt cleanup read but
			// before ordinary inventory. The same precompiled receipt fence must
			// still keep A out of B's projection; the next level-triggered sweep
			// then removes it.
			return []ContainerInfo{liveSuccessor}, nil
		}
		return slices.Clone(inventory), nil
	}
	dockerMock.InspectContainerFn = func(_ context.Context, containerID string) (*ContainerInfo, error) {
		for _, current := range inventory {
			if current.ContainerID == containerID {
				copy := current
				return &copy, nil
			}
		}
		return nil, fmt.Errorf("unknown test container %q", containerID)
	}
	var removed []string
	dockerMock.RemoveContainerFn = func(_ context.Context, containerID string) error {
		removed = append(removed, containerID)
		inventory = slices.DeleteFunc(inventory, func(current ContainerInfo) bool {
			return current.ContainerID == containerID
		})
		return nil
	}

	require.NoError(t, b.recoverState(context.Background()))
	assert.Empty(t, removed, "the first cleanup observation preceded the late Create")
	b.provisionsMu.RLock()
	projection := b.provisions[successorSpec.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection)
	assert.Equal(t, successorSpec.CallbackURL, projection.CallbackURL)
	assert.Equal(t, []string{liveSuccessor.ContainerID}, projection.ContainerIDs)
	assert.Equal(t, backend.ProvisionStatusReady, projection.Status)

	require.NoError(t, b.recoverState(context.Background()))
	assert.Equal(t, []string{lateFailed.ContainerID}, removed,
		"the archived receipt authorizes only its exact failed callback generation")
	claims, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	require.Len(t, claims, 1)
	assert.Equal(t, successorID, claims[0].OperationID())
	receipts, listErr := operationSettlementServiceForCallbackTest(t, store).ListFailedOperationReceipts()
	require.NoError(t, listErr)
	require.Len(t, receipts, 1)
	assert.Equal(t, dockerOperationIntentID, receipts[0].OperationID())
}

func TestPublishAwaitedProvisionSuccessesIsAllOrNone(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	first := dockerOperationIntentSpec(t, storageID)
	firstAdmission, err := beginDockerTestOperationIntent(t, store, first, storageID)
	require.NoError(t, err)
	second := dockerOperationIntentSpec(t, storageID)
	second.LeaseUUID = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	secondOperationID := mustDockerOperationID("7ba7b810-9dad-41d1-80b4-00c04fd430c8")
	second.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + secondOperationID.String()
	second.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(second.CallbackURL, "")
	require.NoError(t, err)
	secondAdmission, err := beginDockerTestOperationIntent(t, store, second, storageID)
	require.NoError(t, err)

	projections := readyIntentProjection(first, "first-container")
	for leaseUUID, projection := range readyIntentProjection(second, "wrong-second-container") {
		projections[leaseUUID] = projection
	}
	projections[first.LeaseUUID].Status = backend.ProvisionStatusProvisioning
	projections[second.LeaseUUID].Status = backend.ProvisionStatusProvisioning
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, projections)
	firstStack, err := manifest.ParsePayload(first.Manifest)
	require.NoError(t, err)
	secondStack, err := manifest.ParsePayload(second.Manifest)
	require.NoError(t, err)
	decisions := []recoveredIntentDecision{
		{
			claim: createdDockerOperationClaim(t, firstAdmission),
			readyProjection: &recoveredOperationReadyPromotion{
				containerIDs:      []string{"first-container"},
				serviceContainers: map[string][]string{"app": {"first-container"}},
				stackManifest:     firstStack,
			},
		},
		{
			claim: createdDockerOperationClaim(t, secondAdmission),
			readyProjection: &recoveredOperationReadyPromotion{
				containerIDs:      []string{"second-container"},
				serviceContainers: map[string][]string{"app": {"second-container"}},
				stackManifest:     secondStack,
			},
		},
	}

	err = b.publishAwaitedProvisionSuccesses(decisions)
	require.ErrorContains(t, err, "does not match its recovered projection")
	assert.Equal(t, backend.ProvisionStatusProvisioning, b.provisions[first.LeaseUUID].Status,
		"a later same-count/wrong-ID failure must not partially publish an earlier promotion")
	assert.Equal(t, backend.ProvisionStatusProvisioning, b.provisions[second.LeaseUUID].Status)
}

func TestProvisionIntentRecoveryDeadline_BoundsFutureAdmissionClock(t *testing.T) {
	observedAt := time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)
	timeout := 45 * time.Minute

	assert.Equal(t, observedAt.Add(timeout), provisionIntentRecoveryDeadline(
		observedAt.Add(24*time.Hour), observedAt, timeout,
	), "a persisted future timestamp after clock rollback must not extend recovery forever")
	assert.Equal(t, observedAt.Add(15*time.Minute), provisionIntentRecoveryDeadline(
		observedAt.Add(-30*time.Minute), observedAt, timeout,
	), "an ordinary timestamp must spend only the original operation's remaining budget")
}

func TestRecoverOperationIntent_CreatedProvisionCannotWedgeStartup(t *testing.T) {
	metricBefore := testutil.ToFloat64(operationIntentRecoveryTimeoutExhaustionsTotal.WithLabelValues(
		operationRecoveryTimeoutProvision,
	))
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	container.Status = "created"
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID),
	)
	b.cfg.ContainerStartTimeout = 20 * time.Millisecond
	b.cfg.ProvisionTimeout = time.Hour
	compose := b.compose.(*mockComposeExecutor)
	originalDown := compose.DownFn
	teardownCalls := 0
	compose.DownFn = func(ctx context.Context, leaseUUID string, timeout time.Duration) error {
		teardownCalls++
		return originalDown(ctx, leaseUUID, timeout)
	}

	startedAt := time.Now()
	require.NoError(t, b.recoverOperationIntents(context.Background()))
	assert.Less(t, time.Since(startedAt), time.Second,
		"startup must defer a young inert cohort without blocking daemon readiness")
	assert.Zero(t, teardownCalls)
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	require.Len(t, intents, 1)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)

	b.cfg.ProvisionTimeout = time.Nanosecond
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	assert.Equal(t, 1, teardownCalls, "the live recovery lane must tear down an expired inert cohort")
	intents, listErr = listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr = store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, metricBefore+1, testutil.ToFloat64(
		operationIntentRecoveryTimeoutExhaustionsTotal.WithLabelValues(operationRecoveryTimeoutProvision),
	))
}

func TestRecoverOperationIntent_PausedProvisionWithoutHealthCheckCannotWedgeStartup(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	container.Status = "paused"
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID),
	)
	b.cfg.ContainerStartTimeout = 20 * time.Millisecond
	b.cfg.ProvisionTimeout = time.Hour
	compose := b.compose.(*mockComposeExecutor)
	originalDown := compose.DownFn
	teardownCalls := 0
	compose.DownFn = func(ctx context.Context, leaseUUID string, timeout time.Duration) error {
		teardownCalls++
		return originalDown(ctx, leaseUUID, timeout)
	}

	startedAt := time.Now()
	require.NoError(t, b.recoverOperationIntents(context.Background()))
	assert.Less(t, time.Since(startedAt), time.Second,
		"startup must defer a young paused cohort without blocking daemon readiness")
	assert.Zero(t, teardownCalls)
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	require.Len(t, intents, 1)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)

	b.cfg.ProvisionTimeout = time.Nanosecond
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	assert.Equal(t, 1, teardownCalls, "the live recovery lane must tear down an expired paused cohort")
	intents, listErr = listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr = store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
}

func TestRecoverOperationIntent_NonterminalCandidateFreezesLegacyPredecessorBeforeTeardown(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	candidate := dockerOperationIntentSpec(t, storageID)
	predecessorSpec := candidate
	predecessorSpec.CallbackURL = "https://legacy.example/callbacks/provision?route=v013"
	predecessorSpec.LifecycleCallbackURL = ""
	seedV013OperationReleaseForTest(t, store, candidate.LeaseUUID, shared.Release{
		Manifest:         slices.Clone(predecessorSpec.Manifest),
		Image:            "stack",
		Items:            slices.Clone(predecessorSpec.Items),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(predecessorSpec.ResourceProfiles),
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	})
	_, err = beginDockerTestOperationIntent(t, store, candidate, storageID)
	require.NoError(t, err)
	predecessor := dockerIntentContainer(
		predecessorSpec, "predecessor-container", predecessorSpec.Items[0].SKU, 0,
	)
	current := dockerIntentContainer(candidate, "candidate-container", candidate.Items[0].SKU, 0)
	current.Status = "created"
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{predecessor, current},
		readyIntentProjection(candidate, current.ContainerID),
	)
	b.cfg.ContainerStartTimeout = time.Minute
	b.cfg.ProvisionTimeout = time.Nanosecond
	compose := b.compose.(*mockComposeExecutor)
	originalDown := compose.DownFn
	teardownCalls := 0
	predecessorFrozenAtTeardown := false
	compose.DownFn = func(ctx context.Context, leaseUUID string, timeout time.Duration) error {
		teardownCalls++
		active, readErr := b.releaseStore.LatestActive(candidate.LeaseUUID)
		if readErr != nil {
			return fmt.Errorf("read predecessor at teardown boundary: %w", readErr)
		}
		predecessorFrozenAtTeardown = active != nil && active.LegacyRuntimeAuthority != nil
		if !predecessorFrozenAtTeardown {
			return errors.New("legacy predecessor authority was not frozen before teardown")
		}
		return originalDown(ctx, leaseUUID, timeout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	require.NoError(t, b.recoverOperationIntents(ctx),
		"a validated predecessor makes the interrupted replacement terminal without waiting out the candidate timeout")
	assert.Equal(t, 1, teardownCalls)
	assert.True(t, predecessorFrozenAtTeardown)
	active, readErr := b.releaseStore.LatestActive(candidate.LeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, active)
	require.NotNil(t, active.LegacyRuntimeAuthority)
	assert.Equal(t, predecessorSpec.CallbackURL, active.LegacyRuntimeAuthority.CallbackURL())
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
}

func TestRecoverOperationIntent_NonterminalCandidateRejectsContradictoryPredecessorBeforeTeardown(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	candidate := dockerOperationIntentSpec(t, storageID)
	predecessorSpec := candidate
	predecessorSpec.CallbackURL = "https://legacy.example/callbacks/provision?route=v013"
	predecessorSpec.LifecycleCallbackURL = ""
	seedV013OperationReleaseForTest(t, store, candidate.LeaseUUID, shared.Release{
		Manifest:         slices.Clone(predecessorSpec.Manifest),
		Image:            "stack",
		Items:            slices.Clone(predecessorSpec.Items),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(predecessorSpec.ResourceProfiles),
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	})
	_, err = beginDockerTestOperationIntent(t, store, candidate, storageID)
	require.NoError(t, err)
	predecessor := dockerIntentContainer(
		predecessorSpec, "predecessor-container", predecessorSpec.Items[0].SKU, 0,
	)
	predecessor.Tenant = "tenant-b"
	current := dockerIntentContainer(candidate, "candidate-container", candidate.Items[0].SKU, 0)
	current.Status = "created"
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{predecessor, current},
		readyIntentProjection(candidate, current.ContainerID),
	)
	b.cfg.ContainerStartTimeout = time.Minute
	b.cfg.ProvisionTimeout = time.Minute
	teardownCalls := 0
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		teardownCalls++
		return nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = b.recoverOperationIntents(ctx)
	require.ErrorContains(t, err, "different tenant or provider")
	assert.Zero(t, teardownCalls, "contradictory predecessor evidence cannot mint lease-wide teardown authority")
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Len(t, intents, 1)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)
	active, readErr := b.releaseStore.LatestActive(candidate.LeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, active)
	assert.Nil(t, active.LegacyRuntimeAuthority,
		"contradictory observations must not be frozen into durable predecessor authority")
}

func TestStart_CreatedProvisionIntentConvergesInsteadOfRestartLoop(t *testing.T) {
	var (
		inventoryMu sync.Mutex
		inventory   []ContainerInfo
		teardown    int
	)
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			inventoryMu.Lock()
			defer inventoryMu.Unlock()
			return slices.Clone(inventory), nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			inventoryMu.Lock()
			defer inventoryMu.Unlock()
			for _, current := range inventory {
				if current.ContainerID == containerID {
					copy := current
					return &copy, nil
				}
			}
			return nil, fmt.Errorf("container %s is absent", containerID)
		},
		CloseFn: func() error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.cfg.ContainerStartTimeout = 20 * time.Millisecond
	b.volumes = &mockVolumeManager{}
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		inventoryMu.Lock()
		defer inventoryMu.Unlock()
		teardown++
		inventory = nil
		return nil
	}}

	store := b.callbackStore
	b.callbackStore = store
	b.operationSettlement = operationSettlementServiceForCallbackTest(t, store)
	blockedDelivery := &http.Client{Transport: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	rebuildCallbackSender(b, blockedDelivery)
	attachReleaseStore(t, b)
	attachRetentionStore(t, b)

	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	_, err := beginDockerTestOperationIntent(t, store, spec, b.storageIdentity)
	require.NoError(t, err)
	startPendingOperationForRecoveryTest(t, b)
	created := dockerIntentContainer(spec, "created-container", spec.Items[0].SKU, 0)
	created.Status = "created"
	inventory = []ContainerInfo{created}

	started := time.Now()
	require.NoError(t, b.Start(context.Background()))
	assert.Less(t, time.Since(started), time.Second,
		"the exact inert cohort must not force the supervisor into an endless restart loop")
	assert.Zero(t, teardown, "startup takes one observation and defers a young Started cohort")
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	require.Len(t, intents, 1)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)

	b.cfg.ProvisionTimeout = time.Nanosecond
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	assert.Equal(t, 1, teardown)
	intents, listErr = listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr = store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	require.NoError(t, b.Stop())
}

func TestStart_PromotesProvisionThatBecomesReadyDuringBoundedRecovery(t *testing.T) {
	var inventoryMu sync.Mutex
	inspectCalls := 0
	var restarting ContainerInfo
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			inventoryMu.Lock()
			defer inventoryMu.Unlock()
			return []ContainerInfo{restarting}, nil
		},
		InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
			inventoryMu.Lock()
			defer inventoryMu.Unlock()
			inspectCalls++
			observed := restarting
			if inspectCalls > 1 {
				observed.Status = "running"
			}
			return &observed, nil
		},
		CloseFn: func() error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.cfg.ProvisionTimeout = 3 * time.Second
	b.volumes = &mockVolumeManager{}
	teardownCalls := 0
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		teardownCalls++
		return nil
	}}

	store := b.callbackStore
	b.callbackStore = store
	b.operationSettlement = operationSettlementServiceForCallbackTest(t, store)
	blockedDelivery := &http.Client{Transport: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	rebuildCallbackSender(b, blockedDelivery)
	attachReleaseStore(t, b)
	attachRetentionStore(t, b)
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	_, err := beginDockerTestOperationIntent(t, store, spec, b.storageIdentity)
	require.NoError(t, err)
	startPendingOperationForRecoveryTest(t, b)
	restarting = dockerIntentContainer(spec, "restarting-container", spec.Items[0].SKU, 0)
	restarting.Status = "restarting"

	started := time.Now()
	require.NoError(t, b.Start(context.Background()))
	assert.Less(t, time.Since(started), time.Second,
		"startup must not wait out a tenant operation's convergence window")
	assert.Zero(t, teardownCalls)
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	assert.GreaterOrEqual(t, inspectCalls, 2)
	b.provisionsMu.RLock()
	projection := b.provisions[spec.LeaseUUID]
	require.NotNil(t, projection)
	assert.Equal(t, backend.ProvisionStatusReady, projection.Status)
	b.provisionsMu.RUnlock()
	release, readErr := b.releaseStore.LatestActive(spec.LeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, release)
	assert.Equal(t, dockerOperationIntentID, release.OperationID)
	intents, readErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, readErr)
	assert.Empty(t, intents)
	pending, readErr := store.ListPending()
	require.NoError(t, readErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	require.NoError(t, b.Stop())
}

func TestStart_PendingRestoreOwnsEmptyInventoryUntilLateExactCohort(t *testing.T) {
	var (
		inventoryMu             sync.Mutex
		operationInventoryReads int
		sawStateRecovery        bool
		late                    ContainerInfo
		b                       *Backend
	)
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			inventoryMu.Lock()
			defer inventoryMu.Unlock()
			// recoverState owns recoverMu across both its projection inventory and
			// closed-substrate postcheck. Keep that whole phase empty. Once Start
			// hands sole ownership to operation recovery, expose one empty strict
			// observation and then the daemon-side Create. This synchronization is
			// tied to the phase boundary being tested, not to elapsed wall time or
			// the number of incidental inventory reads within recoverState.
			stateRecoveryActive := !b.recoverMu.TryLock()
			if !stateRecoveryActive {
				b.recoverMu.Unlock()
			}
			if stateRecoveryActive {
				sawStateRecovery = true
				return nil, nil
			}
			if !sawStateRecovery {
				return nil, nil // Any identity inventory before recoverState.
			}
			operationInventoryReads++
			if operationInventoryReads == 1 {
				return nil, nil
			}
			return []ContainerInfo{late}, nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			inventoryMu.Lock()
			defer inventoryMu.Unlock()
			if containerID != late.ContainerID {
				return nil, fmt.Errorf("unknown test container %q", containerID)
			}
			copy := late
			return &copy, nil
		},
		CloseFn: func() error { return nil },
	}
	b = newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.cfg.ContainerStartTimeout = 5 * time.Millisecond
	// ProvisionTimeout is durable operation policy, not a test scheduler. Give
	// admission ample headroom while startupRecoveryTimeout remains the hard test
	// bound; the semantic inventory transition above still occurs on the first
	// 5ms visibility poll, so this adds no sleep to the successful path.
	b.cfg.ProvisionTimeout = time.Minute
	b.startupRecoveryTimeout = 2 * time.Second
	b.volumes = &mockVolumeManager{UsageFn: func(context.Context, string) (int64, error) {
		return 0, nil
	}}
	teardownCalls := 0
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		teardownCalls++
		return nil
	}}
	attachReleaseStore(t, b)
	retentions := attachRetentionStore(t, b)
	callbacks := attachRestoreAuthorityCallbackStore(t, b)
	operations := operationSettlementServiceForCallbackTest(t, callbacks)
	b.operationSettlement = operations
	b.releaseCapacityPlanner = operations
	bindBackendRecoveryCoordinatorForTest(t, b)
	rebuildCallbackSender(b, &http.Client{Transport: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})})
	// Start launches replay and verification loops before it enters recovery.
	// Join them even when a recovery assertion fails before the explicit Stop
	// below, so later store cleanups cannot close bbolt files under live loops.
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})

	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	spec.Kind = shared.OperationIntentRestore
	spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174001"
	spec.SourceGeneration = 1
	late = dockerIntentContainer(spec, "late-restore-container", spec.Items[0].SKU, 0)
	putRestoreIntentFinalizer(t, retentions, spec, shared.RetentionEntry{
		OriginalLeaseUUID: spec.SourceLeaseUUID,
		NewLeaseUUID:      spec.LeaseUUID,
		Tenant:            spec.Tenant,
		ProviderUUID:      spec.ProviderUUID,
		Items:             slices.Clone(spec.Items),
		RetainedVolumeNames: []string{
			retainedName(canonicalVolumeName(spec.SourceLeaseUUID, spec.Items[0].ServiceName, 0)),
		},
		Status:     shared.RetentionStatusRestoring,
		Generation: spec.SourceGeneration,
	})
	_, err := beginDockerTestOperationIntent(t, callbacks, spec, b.storageIdentity)
	require.NoError(t, err)
	startPendingOperationForRecoveryTest(t, b)

	started := time.Now()
	require.NoError(t, b.Start(context.Background()))
	assert.Less(t, time.Since(started), time.Second,
		"one young empty observation must not hold backend startup open")
	assert.Zero(t, teardownCalls,
		"retention recovery must not consume the finalizer before pending operation recovery")
	inventoryMu.Lock()
	observedStateRecovery := sawStateRecovery
	observedOperationReads := operationInventoryReads
	inventoryMu.Unlock()
	assert.True(t, observedStateRecovery)
	assert.Equal(t, 1, observedOperationReads,
		"startup performs one observation and leaves a young Started generation durable")
	intents, readErr := b.operationSettlement.ListOperationIntents()
	require.NoError(t, readErr)
	require.Len(t, intents, 1)
	source, readErr := retentions.Get(spec.SourceLeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, source, "the first empty view must preserve restore ownership")

	// The live level-triggered lane owns the second independent observation.
	// A missing volatile projection is deliberately not failure evidence for an
	// inherited Started operation; the late exact cohort therefore commits.
	require.NoError(t, b.recoverLiveOperationIntents(t.Context()))
	require.NoError(t, b.reconcileRetentions(t.Context()))
	inventoryMu.Lock()
	observedOperationReads = operationInventoryReads
	inventoryMu.Unlock()
	assert.GreaterOrEqual(t, observedOperationReads, 2)
	b.provisionsMu.RLock()
	projection := b.provisions[spec.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection)
	assert.Equal(t, backend.ProvisionStatusReady, projection.Status)
	assert.Equal(t, []string{late.ContainerID}, projection.ContainerIDs)
	release, readErr := b.releaseStore.LatestActive(spec.LeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, release)
	assert.Equal(t, dockerOperationIntentID, release.OperationID)
	source, readErr = retentions.Get(spec.SourceLeaseUUID)
	require.NoError(t, readErr)
	assert.Nil(t, source, "successful reconciliation must consume the exact source finalizer")
	assert.Zero(t, teardownCalls, "successful late restore must never enter rollback teardown")
	pending, readErr := callbacks.ListPending()
	require.NoError(t, readErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	require.NoError(t, b.Stop())
}

func TestRecoverOperationIntent_ProvisionHealthDeadlineIsTerminal(t *testing.T) {
	metricBefore := testutil.ToFloat64(operationIntentRecoveryTimeoutExhaustionsTotal.WithLabelValues(
		operationRecoveryTimeoutProvision,
	))
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.HealthCheckServices = []string{spec.Items[0].ServiceName}
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	container.Health = HealthStatusStarting
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID),
	)
	b.cfg.ProvisionTimeout = time.Hour
	dockerMock := b.docker.(*mockDockerClient)
	inspectCalls := 0
	dockerMock.InspectContainerFn = func(context.Context, string) (*ContainerInfo, error) {
		inspectCalls++
		observed := container
		return &observed, nil
	}
	compose := b.compose.(*mockComposeExecutor)
	originalDown := compose.DownFn
	teardownCalls := 0
	compose.DownFn = func(ctx context.Context, leaseUUID string, timeout time.Duration) error {
		teardownCalls++
		return originalDown(ctx, leaseUUID, timeout)
	}

	started := time.Now()
	require.NoError(t, b.recoverOperationIntents(context.Background()))
	assert.Less(t, time.Since(started), time.Second)
	assert.Zero(t, teardownCalls)
	b.cfg.ProvisionTimeout = time.Nanosecond
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	assert.Equal(t, 1, teardownCalls)
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.GreaterOrEqual(t, inspectCalls, 2,
		"the live lane must re-attest the still-unready cohort before exact cleanup")
	assert.Equal(t, metricBefore+1, testutil.ToFloat64(
		operationIntentRecoveryTimeoutExhaustionsTotal.WithLabelValues(operationRecoveryTimeoutProvision),
	))
}

func TestRecoverOperationIntent_FailedSiblingMakesProvisionTerminal(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.Items[0].Quantity = 2
	spec.ResourceProfiles = testResourceProfiles(t, spec.Items)
	spec.HealthCheckServices = []string{spec.Items[0].ServiceName}
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	failed := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	failed.Status = "exited"
	starting := dockerIntentContainer(spec, "container-2", spec.Items[0].SKU, 1)
	starting.Health = HealthStatusStarting
	b := newOperationIntentRecoveryBackend(
		t,
		store,
		storageID,
		[]ContainerInfo{failed, starting},
		readyIntentProjection(spec, failed.ContainerID, starting.ContainerID),
	)
	b.cfg.ProvisionTimeout = time.Minute
	compose := b.compose.(*mockComposeExecutor)
	originalDown := compose.DownFn
	teardownCalls := 0
	compose.DownFn = func(ctx context.Context, leaseUUID string, timeout time.Duration) error {
		teardownCalls++
		return originalDown(ctx, leaseUUID, timeout)
	}

	started := time.Now()
	require.NoError(t, b.recoverOperationIntents(context.Background()))
	assert.Less(t, time.Since(started), time.Second,
		"a failed sibling makes success impossible; recovery must not wait out the health deadline")
	assert.Equal(t, 1, teardownCalls)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
}

func TestRecoverOperationIntent_CancellationPreservesNonterminalProvision(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.HealthCheckServices = []string{spec.Items[0].ServiceName}
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	container.Health = HealthStatusStarting
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID),
	)
	b.cfg.ProvisionTimeout = time.Minute
	teardownCalls := 0
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		teardownCalls++
		return nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = b.recoverOperationIntents(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, teardownCalls)
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Len(t, intents, 1)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)
}

func TestProvisionReleaseAppendFailureRetainsIntentUntilRestartRecovery(t *testing.T) {
	callbackPath := filepath.Join(t.TempDir(), "callbacks.db")
	callbackStore, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	storageID := operationIntentTestStoreIdentity(t, callbackStore)
	spec := dockerOperationIntentSpec(t, storageID)
	admission, err := beginDockerTestOperationIntent(t, callbackStore, spec, storageID)
	require.NoError(t, err)

	failedReleaseStore, settlement := operationSettlementForCallbackTest(t, callbackStore)

	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{container}, nil
		},
		InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
			copy := container
			return &copy, nil
		},
	}
	downCalls := 0
	b := newBackendForProvisionTest(t, mock, nil)
	b.compose = &mockComposeExecutor{
		UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error { return nil },
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{
				ID: container.ContainerID, Service: spec.Items[0].ServiceName, State: "running",
			}}, nil
		},
		DownFn: func(context.Context, string, time.Duration) error {
			downCalls++
			return nil
		},
	}
	b.callbackStore = callbackStore
	b.operationSettlement = operationSettlementServiceForCallbackTest(t, callbackStore)
	attestor := callbackStorageAttestorForTest(
		t, callbackStore, b.stopCtx, func(context.Context) error {
			return b.terminalStorageAuthorityError()
		},
	)
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           callbackStore,
		StorageAttestor: attestor,
		HTTPClient:      &http.Client{},
		Secret:          durableCallbackTestSecret,
		Logger:          b.logger,
	})
	b.maintenanceSettlement = newTestMaintenanceSettlement(t, callbackStore, failedReleaseStore)
	b.callbackPublisher = mustNewCallbackPublisherForTest(t, shared.CallbackPublisherConfig{
		OperationSettlement:   settlement,
		MaintenanceSettlement: b.maintenanceSettlement,
		StorageAttestor:       attestor,
		Logger:                b.logger,
	})
	b.releaseStore = failedReleaseStore
	b.operationSettlement = settlement
	b.storageIdentity = storageID
	b.storageVerifier = testDockerRuntimeStorageVerifier{id: storageID}
	installTestStorageMutationAdapters(b)
	require.NoError(t, bindBackendTestPhysicalExecutors(
		b, settlement, b.maintenanceSettlement,
	))
	b.cfg.StartupVerifyDuration = time.Millisecond
	claim := createdDockerOperationClaim(t, admission)
	candidate, err := settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	execution, err := settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	physical := settlement.ExecuteOperation(context.Background(), execution)
	success, ok := physical.(shared.OperationExecutionSuccess)
	require.True(t, ok, "physical result = %T, want exact target ready", physical)
	require.NoError(t, failedReleaseStore.Close(), "inject release commit failure after successful substrate")
	_, commitErr := settlement.CommitOperationSuccess(success)
	require.Error(t, commitErr)
	assert.Zero(t, downCalls, "ambiguous durable settlement must retain successful substrate")
	// No failure proof exists for this branch, so a false terminal callback is
	// structurally unavailable; only restart recovery may classify the retained
	// Started intent.
	intents, err := listOperationIntentsForCallbackTest(t, callbackStore)
	require.NoError(t, err)
	assert.Len(t, intents, 1)
	pending, err := callbackStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
	closeOperationSettlementForCallbackTest(t, callbackStore)
	require.NoError(t, callbackStore.Close(), "simulate process exit with exact intent retained")

	callbackStore, err = newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbackStore.Close()) })
	recovered := newOperationIntentRecoveryBackend(
		t, callbackStore, storageID, []ContainerInfo{container},
		readyIntentProjection(spec, container.ContainerID),
	)
	recoveredReleaseStore := recovered.releaseStore

	require.NoError(t, recovered.recoverOperationIntents(context.Background()))
	intents, err = listOperationIntentsForCallbackTest(t, callbackStore)
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err = callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	active, err := recoveredReleaseStore.LatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, spec.Items, active.Items)
}

func TestProvisionVolumeDurabilityAmbiguityRetainsIntentAndStopsBackend(t *testing.T) {
	mutationCause := errors.New("xfs final-name parent sync failed")
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.volumes = &mockVolumeManager{CreateFn: func(context.Context, string, int64) (string, bool, error) {
		return "", false, fmt.Errorf("%w: %w", backendidentity.ErrMutationOutcomeAmbiguous, mutationCause)
	}}
	callbackStore := b.callbackStore
	settlement, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	candidateIntent, err := settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	admission, err := settlement.BeginOperationIntent(candidateIntent)
	require.NoError(t, err)
	claim := createdDockerOperationClaim(t, admission)
	candidate, err := settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	execution, err := settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	physical := settlement.ExecuteOperation(context.Background(), execution)
	ambiguous, ok := physical.(shared.OperationExecutionAmbiguous)
	require.True(t, ok, "physical result = %T, want ambiguous", physical)
	provisionErr := ambiguous.Cause()
	require.ErrorIs(t, provisionErr, backendidentity.ErrMutationOutcomeAmbiguous)
	require.ErrorIs(t, provisionErr, mutationCause)
	select {
	case <-b.stopCtx.Done():
	default:
		t.Fatal("volume durability ambiguity did not stop the backend lifetime")
	}

	refusalErr := b.refuseOperationIntent(claim, provisionErr)
	require.ErrorIs(t, refusalErr, backendidentity.ErrMutationOutcomeAmbiguous)
	assertOperationIntentRetainedAfterLatchedRestart(t, b, callbackStore, spec.LeaseUUID)
}

func TestProvisionTeardownFailureRetainsIntentPoolAndVolumesForRecovery(t *testing.T) {
	const teardownFailure = "docker inventory unavailable during failed provision cleanup"
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return &ImageInfo{ID: "image-1", Volumes: map[string]struct{}{`/data`: {}}}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, errors.New(teardownFailure)
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.ContainerReadonlyRootfs = ptrBool(false)
	bindTestStorageIdentity(t, b, mock)
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	b.operationSettlement = operations
	profile := b.cfg.SKUProfiles["docker-micro"]
	profile.DiskMB = 512
	b.cfg.SKUProfiles["docker-micro"] = profile
	b.compose = &mockComposeExecutor{
		UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
			return errors.New("injected compose up failure")
		},
		DownFn: func(context.Context, string, time.Duration) error {
			return errors.New("injected compose down failure")
		},
	}
	volumeRoot := t.TempDir()
	b.cfg.VolumeDataPath = volumeRoot
	var destroyCalls int
	b.volumes = &mockVolumeManager{
		defaultDir: volumeRoot,
		CreateFn: func(_ context.Context, id string, _ int64) (string, bool, error) {
			path := filepath.Join(volumeRoot, id)
			require.NoError(t, os.MkdirAll(path, 0o755))
			return path, true, nil
		},
		DestroyFn: func(context.Context, string) error {
			destroyCalls++
			return nil
		},
	}
	store := b.callbackStore
	spec := dockerOperationIntentSpec(t, b.storageIdentity)

	require.NoError(t, b.Provision(context.Background(), backend.ProvisionRequest{
		LeaseUUID:            spec.LeaseUUID,
		Tenant:               spec.Tenant,
		ProviderUUID:         spec.ProviderUUID,
		Items:                slices.Clone(spec.Items),
		CallbackURL:          spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL,
		Payload:              slices.Clone(spec.Manifest),
	}))
	require.Eventually(t, func() bool {
		acquired, err := b.withRecoveryLeaseExclusion(
			t.Context(), spec.LeaseUUID, func() error { return nil },
		)
		return err == nil && acquired
	}, 3*time.Second, 10*time.Millisecond,
		"the accepted provision worker did not reach a quiescent recovery boundary")

	assert.Zero(t, destroyCalls, "managed volumes must remain untouched while a container may still mount them")
	assert.NoError(t, b.terminalStorageAuthorityError(),
		"an ordinary retriable cleanup error must not withdraw storage authority")
	select {
	case <-b.stopCtx.Done():
		t.Fatal("a durable Started intent makes an ordinary cleanup error live-recoverable")
	default:
	}
	allocation := b.pool.GetAllocation(spec.LeaseUUID + "-app-0")
	require.NotNil(t, allocation, "the full reservation must remain while substrate cleanup is incomplete")
	b.provisionsMu.RLock()
	retained := b.provisions[spec.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, retained)
	assert.Equal(t, spec.Items, retained.Items)
	intents, err := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, spec.LeaseUUID, intents[0].LeaseUUID())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "the actor must not erase recovery authority with a failed callback")
	b.stopCancel()
	b.wg.Wait()
}

func TestProvisionFailurePreservesCreatedVolumesForRecovery(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return &ImageInfo{ID: "image-1", Volumes: map[string]struct{}{`/data`: {}}}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.ContainerReadonlyRootfs = ptrBool(false)
	bindTestStorageIdentity(t, b, mock)
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	b.operationSettlement = operations
	profile := b.cfg.SKUProfiles["docker-micro"]
	profile.DiskMB = 512
	b.cfg.SKUProfiles["docker-micro"] = profile
	b.compose = &mockComposeExecutor{
		UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
			return errors.New("injected compose up failure")
		},
		DownFn: func(context.Context, string, time.Duration) error { return nil },
	}
	volumeRoot := t.TempDir()
	b.cfg.VolumeDataPath = volumeRoot
	var destroyCalls int
	b.volumes = &mockVolumeManager{
		defaultDir: volumeRoot,
		CreateFn: func(_ context.Context, id string, _ int64) (string, bool, error) {
			path := filepath.Join(volumeRoot, id)
			require.NoError(t, os.MkdirAll(path, 0o755))
			return path, true, nil
		},
		DestroyFn: func(context.Context, string) error {
			destroyCalls++
			return nil
		},
	}
	store := b.callbackStore
	spec := dockerOperationIntentSpec(t, b.storageIdentity)

	require.NoError(t, b.Provision(context.Background(), backend.ProvisionRequest{
		LeaseUUID:            spec.LeaseUUID,
		Tenant:               spec.Tenant,
		ProviderUUID:         spec.ProviderUUID,
		Items:                slices.Clone(spec.Items),
		CallbackURL:          spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL,
		Payload:              slices.Clone(spec.Manifest),
	}))
	require.Eventually(t, func() bool {
		acquired, err := b.withRecoveryLeaseExclusion(
			t.Context(), spec.LeaseUUID, func() error { return nil },
		)
		return err == nil && acquired
	}, 3*time.Second, 10*time.Millisecond,
		"the accepted provision worker did not reach a quiescent recovery boundary")

	assert.Zero(t, destroyCalls,
		"same-turn cleanup cannot destroy a volume which a late container may still mount")
	assert.NoError(t, b.terminalStorageAuthorityError())
	require.NotNil(t, b.pool.GetAllocation(spec.LeaseUUID+"-app-0"),
		"capacity must stay reserved until exact recovery consumes the operation intent")
	intents, err := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, spec.LeaseUUID, intents[0].LeaseUUID())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "nonterminal recovery authority must suppress a failed callback")
	b.stopCancel()
	b.wg.Wait()
}

// assertOperationIntentRetainedAfterLatchedRestart models the only legal read
// after a fail-stop: close the process-owned journals and reopen them under a
// fresh authority gate. The latched gate deliberately rejects every read and
// write, so inspecting it in-process would test a state production cannot use.
func assertOperationIntentRetainedAfterLatchedRestart(
	t *testing.T,
	b *Backend,
	callbacks *shared.CallbackStore,
	leaseUUID string,
) {
	t.Helper()
	closeOperationSettlementForCallbackTest(t, callbacks)
	require.NoError(t, callbacks.Close())

	restartGate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	reopenedCallbacks, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath}, b.storageAuthority, restartGate,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedCallbacks.Close() })
	reopenedReleases, err := shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: b.cfg.ReleasesDBPath}, b.storageAuthority, restartGate,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedReleases.Close() })
	operations, err := shared.NewOperationSettlement(reopenedCallbacks, reopenedReleases)
	require.NoError(t, err)
	intents, err := operations.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, leaseUUID, intents[0].LeaseUUID())
	pending, err := reopenedCallbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending,
		"an ambiguous mutation must preserve the WAL without publishing a terminal callback")
}

func TestProvisionRejectsPredecessorPrincipalMismatchBeforeTeardown(t *testing.T) {
	oldItems := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 1}}
	oldSpec := shared.OperationIntentSpec{
		LeaseUUID:            "550e8400-e29b-41d4-a716-446655440000",
		CallbackURL:          "https://old.example/callbacks/provision",
		LifecycleCallbackURL: "https://old.example/callbacks/lifecycle",
		Tenant:               "tenant-b",
		ProviderUUID:         "33333333-3333-4333-8333-333333333333",
		Items:                oldItems,
		ResourceProfiles:     testResourceProfiles(t, oldItems),
		Manifest:             validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"}),
	}
	old := readyIntentProjection(oldSpec)[oldSpec.LeaseUUID]
	old.Status = backend.ProvisionStatusFailed
	var downCalls int
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{oldSpec.LeaseUUID: old})
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		downCalls++
		return nil
	}}

	req := backend.ProvisionRequest{
		LeaseUUID:    oldSpec.LeaseUUID,
		Tenant:       "tenant-a",
		ProviderUUID: nominalDockerProviderUUID,
		Items:        []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 1}},
		CallbackURL: testOperationCallbackURL(
			"https://new.example/callbacks/provision",
		),
		Payload: validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"}),
	}
	err := b.Provision(context.Background(), req)
	require.ErrorContains(t, err, "different tenant or provider")
	assert.Zero(t, downCalls, "a caller must not acquire teardown authority across principals")
	b.provisionsMu.RLock()
	retained := b.provisions[oldSpec.LeaseUUID]
	b.provisionsMu.RUnlock()
	assert.Same(t, old, retained)
}

func TestProvisionRejectedBeforePredecessorTeardownPreservesProjectionAndAccounting(t *testing.T) {
	var inventory []ContainerInfo
	var removed []string
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return slices.Clone(inventory), nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			for _, container := range inventory {
				if container.ContainerID == containerID {
					copy := container
					return &copy, nil
				}
			}
			return nil, errors.New("container not found")
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			removed = append(removed, containerID)
			for index, container := range inventory {
				if container.ContainerID == containerID {
					inventory = append(inventory[:index], inventory[index+1:]...)
					break
				}
			}
			return nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.cfg.SKUProfiles = defaultTestSKUProfiles()
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		return errors.New("force exact fallback removal")
	}}
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	bindBackendToOperationIntentTestStore(t, b, store)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	releases := attachReleaseStore(t, b)

	oldItems := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 2}}
	oldProfiles := testResourceProfiles(t, oldItems)
	oldOperationID := mustDockerOperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
	oldCallbackURL := "https://fred.example/callbacks/provision?operation_id=" + oldOperationID.String()
	oldLifecycleURL, err := backend.ResolveLifecycleCallbackURL(oldCallbackURL, "")
	require.NoError(t, err)
	oldAuthority, err := shared.NewReleaseRuntimeAuthority(
		oldOperationID, "tenant-a", nominalDockerProviderUUID, oldCallbackURL, oldLifecycleURL,
	)
	require.NoError(t, err)
	oldManifest := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	oldSpec := shared.OperationIntentSpec{
		LeaseUUID:            durableCallbackTestLeaseUUID,
		CallbackURL:          oldCallbackURL,
		LifecycleCallbackURL: oldLifecycleURL,
		Tenant:               "tenant-a",
		ProviderUUID:         nominalDockerProviderUUID,
		Items:                oldItems,
		ResourceProfiles:     oldProfiles,
		Manifest:             oldManifest,
	}
	inventory = []ContainerInfo{
		dockerIntentContainer(oldSpec, "actual-predecessor-survivor", oldItems[0].SKU, 1),
	}
	old := readyIntentProjection(oldSpec, "stale-projection-container")[oldSpec.LeaseUUID]
	b.provisionsMu.Lock()
	b.provisions[oldSpec.LeaseUUID] = old
	b.provisionsMu.Unlock()
	// Pin an actor to the prior Ready state, then expose the runtime Failed
	// projection to Provision. Admission rejects before StartOperationExecution,
	// so the predecessor remains the sole substrate/accounting authority and no
	// teardown capability is ever minted.
	b.actorFor(oldSpec.LeaseUUID)
	b.provisionsMu.Lock()
	old.Status = backend.ProvisionStatusFailed
	b.provisionsMu.Unlock()
	oldIDs, oldAllocations, err := resolvedProvisionAllocations(
		oldSpec.LeaseUUID, oldItems, oldProfiles,
	)
	require.NoError(t, err)
	for _, allocation := range oldAllocations {
		require.NoError(t, b.pool.TryAllocateResolved(
			allocation.ID, oldSpec.Tenant, allocation.Resources,
		))
	}
	operations, ok := b.operationSettlement.(*shared.OperationSettlement)
	require.True(t, ok)
	seedProvisionReleaseForLeaseTest(t, b.callbackStore, releases, operations, oldSpec.LeaseUUID, shared.Release{
		Manifest:         oldManifest,
		Image:            "stack",
		OperationID:      oldOperationID,
		Items:            slices.Clone(oldItems),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
		RuntimeAuthority: &oldAuthority,
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	})

	candidate := dockerOperationIntentSpec(t, b.storageIdentity)
	candidate.Items = []backend.LeaseItem{
		{SKU: "docker-micro", ServiceName: "app", Quantity: 1},
		{SKU: "docker-micro", ServiceName: "cache", Quantity: 1},
	}
	candidate.Manifest = validStackManifestJSON(map[string]string{
		"app": "docker.io/library/nginx:1.27", "cache": "docker.io/library/redis:7",
	})
	err = b.Provision(context.Background(), backend.ProvisionRequest{
		LeaseUUID:            candidate.LeaseUUID,
		Tenant:               candidate.Tenant,
		ProviderUUID:         candidate.ProviderUUID,
		Items:                slices.Clone(candidate.Items),
		CallbackURL:          candidate.CallbackURL,
		LifecycleCallbackURL: candidate.LifecycleCallbackURL,
		Payload:              slices.Clone(candidate.Manifest),
	})
	require.Error(t, err, "the actor pinned to Ready must reject a provision transition")
	assert.Empty(t, removed,
		"an actor rejection before Started must not mint predecessor teardown authority")
	b.provisionsMu.RLock()
	restored := b.provisions[oldSpec.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, restored)
	assert.Equal(t, backend.ProvisionStatusFailed, restored.Status)
	assert.Equal(t, oldItems, restored.Items)
	assert.Equal(t, []string{"stale-projection-container"}, restored.ContainerIDs)
	for _, allocationID := range oldIDs {
		assert.NotNil(t, b.pool.GetAllocation(allocationID))
	}
	assert.Nil(t, b.pool.GetAllocation(oldSpec.LeaseUUID+"-cache-0"))
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
}

func TestFailedReplacementProvisionRetainsIntentAndAccountingForRecovery(t *testing.T) {
	var inventory []ContainerInfo
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return &ImageInfo{ID: "image-1", Volumes: map[string]struct{}{`/data`: {}}}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return slices.Clone(inventory), nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			for _, container := range inventory {
				if container.ContainerID == containerID {
					copy := container
					return &copy, nil
				}
			}
			return nil, errors.New("container not found")
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.cfg.SKUProfiles = defaultTestSKUProfiles()
	b.cfg.ContainerReadonlyRootfs = ptrBool(false)
	b.compose = &mockComposeExecutor{
		DownFn: func(context.Context, string, time.Duration) error {
			inventory = nil
			return nil
		},
		UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
			return errors.New("injected replacement compose failure")
		},
	}
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	bindBackendToOperationIntentTestStore(t, b, store)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	attestor := callbackStorageAttestorForTest(
		t, store, b.stopCtx, func(context.Context) error { return b.terminalStorageAuthorityError() },
	)
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           store,
		StorageAttestor: attestor,
		HTTPClient:      testCallbackClient,
		Secret:          durableCallbackTestSecret,
		Logger:          b.logger,

		Backoff:         &zeroBackoff,
		DeliveryTimeout: testCallbackDeliveryTimeout,
	})
	operationSettlement, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	b.callbackPublisher = mustNewCallbackPublisherForTest(t, shared.CallbackPublisherConfig{
		OperationSettlement:   operationSettlement,
		MaintenanceSettlement: b.maintenanceSettlement,
		StorageAttestor:       attestor,
		Logger:                b.logger,
	})
	releases := attachReleaseStore(t, b)

	oldItems := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 1}}
	oldProfiles := testResourceProfiles(t, oldItems)
	oldOperationID := mustDockerOperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
	oldCallbackURL := "https://fred.example/callbacks/provision?operation_id=" + oldOperationID.String()
	oldLifecycleURL, err := backend.ResolveLifecycleCallbackURL(oldCallbackURL, "")
	require.NoError(t, err)
	oldAuthority, err := shared.NewReleaseRuntimeAuthority(
		oldOperationID, "tenant-a", nominalDockerProviderUUID, oldCallbackURL, oldLifecycleURL,
	)
	require.NoError(t, err)
	manifestPayload := validStackManifestJSON(map[string]string{"app": "docker.io/library/redis:7"})
	oldSpec := shared.OperationIntentSpec{
		LeaseUUID:            durableCallbackTestLeaseUUID,
		CallbackURL:          oldCallbackURL,
		LifecycleCallbackURL: oldLifecycleURL,
		Tenant:               "tenant-a",
		ProviderUUID:         nominalDockerProviderUUID,
		Items:                oldItems,
		ResourceProfiles:     oldProfiles,
		Manifest:             manifestPayload,
	}
	inventory = []ContainerInfo{dockerIntentContainer(oldSpec, "old-container", oldItems[0].SKU, 0)}
	old := readyIntentProjection(oldSpec, "old-container")[oldSpec.LeaseUUID]
	old.Status = backend.ProvisionStatusFailed
	b.provisionsMu.Lock()
	b.provisions[oldSpec.LeaseUUID] = old
	b.provisionsMu.Unlock()
	_, oldAllocations, err := resolvedProvisionAllocations(oldSpec.LeaseUUID, oldItems, oldProfiles)
	require.NoError(t, err)
	for _, allocation := range oldAllocations {
		require.NoError(t, b.pool.TryAllocateResolved(allocation.ID, oldSpec.Tenant, allocation.Resources))
	}
	operations, ok := b.operationSettlement.(*shared.OperationSettlement)
	require.True(t, ok)
	seedProvisionReleaseForLeaseTest(t, b.callbackStore, releases, operations, oldSpec.LeaseUUID, shared.Release{
		Manifest:         manifestPayload,
		Image:            "stack",
		OperationID:      oldOperationID,
		Items:            slices.Clone(oldItems),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
		RuntimeAuthority: &oldAuthority,
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	})
	volumeRoot := t.TempDir()
	b.cfg.VolumeDataPath = volumeRoot
	volumeName := canonicalVolumeName(oldSpec.LeaseUUID, "app", 0)
	volumePath := filepath.Join(volumeRoot, volumeName)
	require.NoError(t, os.MkdirAll(volumePath, 0o755))
	var destroyCalls int
	b.volumes = &mockVolumeManager{
		defaultDir: volumeRoot,
		CreateFn: func(_ context.Context, id string, _ int64) (string, bool, error) {
			require.Equal(t, volumeName, id)
			return volumePath, false, nil
		},
		DestroyFn: func(context.Context, string) error {
			destroyCalls++
			return nil
		},
	}

	candidate := dockerOperationIntentSpec(t, b.storageIdentity)
	candidate.Items = slices.Clone(oldItems)
	candidate.Manifest = slices.Clone(manifestPayload)
	require.NoError(t, b.Provision(context.Background(), backend.ProvisionRequest{
		LeaseUUID:            candidate.LeaseUUID,
		Tenant:               candidate.Tenant,
		ProviderUUID:         candidate.ProviderUUID,
		Items:                slices.Clone(candidate.Items),
		CallbackURL:          candidate.CallbackURL,
		LifecycleCallbackURL: candidate.LifecycleCallbackURL,
		Payload:              slices.Clone(candidate.Manifest),
	}))
	require.Eventually(t, func() bool {
		acquired, acquireErr := b.withRecoveryLeaseExclusion(
			t.Context(), candidate.LeaseUUID, func() error { return nil },
		)
		return acquireErr == nil && acquired
	}, 3*time.Second, 10*time.Millisecond,
		"failed replacement worker did not yield exact durable recovery ownership")
	select {
	case <-b.stopCtx.Done():
		t.Fatal("ordinary per-lease ambiguity must not fail-stop the whole backend")
	default:
	}
	assert.Zero(t, destroyCalls, "a reused predecessor volume is never candidate-created cleanup")
	_, statErr := os.Stat(volumePath)
	require.NoError(t, statErr)
	assert.NotNil(t, b.pool.GetAllocation(oldSpec.LeaseUUID+"-app-0"),
		"candidate accounting must remain conservative until cold recovery restores the predecessor")
	b.provisionsMu.RLock()
	retained := b.provisions[oldSpec.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, retained)
	assert.Equal(t, candidate.CallbackURL, retained.CallbackURL)
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	require.Len(t, intents, 1)
	assert.Equal(t, candidate.LeaseUUID, intents[0].LeaseUUID())
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending, "an ambiguous replacement must not settle through the predecessor route")
}

func TestRecoverOperationIntent_CrashBeforeMutationSettlesExactFailure(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)

	require.NoError(t, b.recoverOperationIntents(context.Background()))
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, interruptedOperationFailure, pending[0].Error)
}

func TestMutationPostcheckAmbiguityRetainsIntentUntilRestartRecovery(t *testing.T) {
	const daemonID = operationIntentTestSubstrateID
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	cfg := Config{Name: "docker", CallbackDBPath: dbPath}
	markerPath := cfg.CallbackDBPath + ".storage-identity.json"
	anchorPath := cfg.CallbackDBPath + ".storage-identity-anchor.json"
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	storageID := operationIntentTestStoreIdentity(t, store)

	postcheckUnavailable := false
	dockerClient := &mockDockerClient{DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
		if postcheckUnavailable {
			return DaemonSecurityInfo{}, errors.New("daemon identity probe unavailable")
		}
		return DaemonSecurityInfo{SystemID: daemonID}, nil
	}}
	composeClient := &mockComposeExecutor{UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
		postcheckUnavailable = true
		return nil
	}}
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	b := &Backend{
		cfg: cfg, docker: dockerClient, compose: composeClient,
		volumes: &noopVolumeManager{}, storageIdentity: storageID,
		logger: slog.Default(), stopCtx: stopCtx, stopCancel: stop,
	}
	installMutationTestVerifier(t, b, func(ctx context.Context) error {
		if err := b.terminalStorageAuthorityError(); err != nil {
			return err
		}
		info, err := dockerClient.DaemonInfo(ctx)
		if err != nil {
			return err
		}
		return backendidentity.VerifyMarkerPair(
			markerPath, anchorPath, cfg.Name, info.SystemID, storageID,
		)
	})
	installTestStorageMutationAdapters(b)

	spec := dockerOperationIntentSpec(t, storageID)
	admission, err := beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	releaseStore, settlement := operationSettlementForCallbackTest(t, store)
	b.releaseStore = releaseStore
	b.operationSettlement = settlement
	uncommitted := commitPreEffectOperationFailureForTest(
		t, settlement, createdDockerOperationClaim(t, admission),
	)

	requests := 0
	attestor := callbackStorageAttestorForTest(
		t, store, b.stopCtx, b.VerifyStorageIdentity,
	)
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           store,
		StorageAttestor: attestor,
		HTTPClient: &http.Client{Transport: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
		})},
		Secret: durableCallbackTestSecret,
		Logger: b.logger,
	})
	b.maintenanceSettlement = newTestMaintenanceSettlement(t, store, releaseStore)
	b.callbackPublisher = mustNewCallbackPublisherForTest(t, shared.CallbackPublisherConfig{
		OperationSettlement:   settlement,
		MaintenanceSettlement: b.maintenanceSettlement,
		StorageAttestor:       attestor,
		Logger:                b.logger,
	})

	err = b.mutationAdapter().composeUp(context.Background(), &composetypes.Project{}, composeUpOpts{})
	require.Error(t, err)
	assert.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	select {
	case <-b.stopCtx.Done():
	default:
		t.Fatal("ambiguous mutation did not latch the backend lifetime")
	}

	// Model a later failure after the guarded mutation error was deliberately
	// swallowed (for example, old-container cleanup logs and continues, or
	// volume-owner detection defaults to root). Refusal must consult the
	// lifetime latch rather than consume the intent based only on this later,
	// apparently definitive error.
	laterCause := fmt.Errorf("%w: capacity changed after cleanup", backend.ErrInsufficientResources)
	refusalErr := b.refuseOperationIntent(createdDockerOperationClaim(t, admission), laterCause)
	require.Error(t, refusalErr)
	assert.ErrorIs(t, refusalErr, laterCause)
	assert.ErrorIs(t, refusalErr, backendidentity.ErrMutationOutcomeAmbiguous)
	intents, err := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	assert.Len(t, intents, 1, "a swallowed mutation ambiguity must still preserve the durable intent")
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "an ambiguous refusal must not publish an exact failure")

	require.Error(t, b.callbackPublisher.PublishOperationFailureContext(
		context.Background(), uncommitted, "late terminal callback",
	))
	assert.Zero(t, requests, "a callback after the ambiguity latch must not reach HTTP")
	intents, err = listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	assert.Len(t, intents, 1, "callback suppression must retain the durable intent")
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "an unclassified completion must not replace the intent")
	closeOperationSettlementForCallbackTest(t, store)
	require.NoError(t, store.Close(), "simulate process exit with the intent durable")

	// A fresh process can re-attest the same marker/daemon and classify the
	// retained intent against substrate inventory. No matching containers means
	// the pre-mutation outcome is now provable and becomes an exact failure.
	postcheckUnavailable = false
	store, err = newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	recovered := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)
	require.NoError(t, recovered.recoverOperationIntents(context.Background()))
	intents, err = listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err = store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, interruptedOperationFailure, pending[0].Error)
}

func TestRefusedOperationIntent_ResponseLossLeavesExactFailureForRedelivery(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	spec := dockerOperationIntentSpec(t, storageID)
	admission, err := beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)

	// The synchronous response is deliberately ignored: this models the peer
	// losing the response after the backend durably refused the operation.
	err = b.refuseOperationIntent(createdDockerOperationClaim(t, admission), backend.ErrAlreadyProvisioned)
	require.ErrorIs(t, err, backend.ErrAlreadyProvisioned)
	closeOperationSettlementForCallbackTest(t, store)
	require.NoError(t, store.Close())

	store, err = newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	intents, err := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)

	disposition, err := operationSettlementServiceForCallbackTest(t, store).ProbeOperationIntent(
		dockerTestOperationIntentProbe(t, store, spec),
	)
	require.NoError(t, err)
	assert.Equal(t, shared.OperationIntentAdmissionCompleted, disposition,
		"an exact retry must acknowledge the durable failure instead of starting work")
}

func TestDeprovisionSettlesIntentWhenProjectionWasNeverPublished(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)

	require.NoError(t, b.doDeprovisionForTest(t, context.Background(), spec.LeaseUUID))
	intents, err := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Contains(t, pending[0].Error, "preempted by lease close")
}

func TestExactOperationRetryBypassesMutableSemanticValidation(t *testing.T) {
	const operationID = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.operationSettlement = fixedOperationIntentProbeJournal{
		disposition: shared.OperationIntentAdmissionCompleted,
	}

	t.Run("provision survives removed SKU and now-invalid manifest", func(t *testing.T) {
		err := b.Provision(context.Background(), backend.ProvisionRequest{
			LeaseUUID: "550e8400-e29b-41d4-a716-446655440000", Tenant: "tenant-a", ProviderUUID: "provider-a",
			CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleURL,
			Items:   []backend.LeaseItem{{SKU: "removed-sku", ServiceName: "app", Quantity: 1}},
			Payload: []byte(`not-json-anymore`),
		})
		require.NoError(t, err)
	})

	t.Run("restore survives deleted source retention", func(t *testing.T) {
		err := b.Restore(context.Background(), backend.RestoreRequest{
			LeaseUUID: "550e8400-e29b-41d4-a716-446655440000", Tenant: "tenant-a", ProviderUUID: "provider-a",
			FromLeaseUUID: "123e4567-e89b-42d3-a456-426614174000",
			CallbackURL:   callbackURL, LifecycleCallbackURL: lifecycleURL,
			Items: []backend.LeaseItem{{SKU: "removed-sku", ServiceName: "app", Quantity: 1}},
		})
		require.NoError(t, err)
	})
}

func TestProvisionIntentToReservationWindowIsFencedAgainstDeprovision(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, _ string, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	store := b.callbackStore
	b.callbackStore = store
	rebuildCallbackSender(b, testCallbackClient)
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	journal := &blockingOperationIntentJournal{
		operationSettlementService: operations,
		delegate:                   operations,
		began:                      make(chan struct{}),
		release:                    make(chan struct{}),
	}
	b.operationSettlement = journal
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
		require.NoError(t, store.Close())
	})
	req := newProvisionRequest(
		"550e8400-e29b-41d4-a716-446655440000", "tenant-a", "docker-small", 1,
		validManifestJSON("nginx:latest"),
	)
	req.CallbackURL = "http://localhost/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8"

	provisionDone := make(chan error, 1)
	go func() { provisionDone <- b.Provision(context.Background(), req) }()
	select {
	case <-journal.began:
	case err := <-provisionDone:
		t.Fatalf("provision returned before persisting its intent: %v", err)
	case <-time.After(time.Second):
		t.Fatal("provision did not persist its intent")
	}

	deprovisionDone := make(chan error, 1)
	go func() { deprovisionDone <- b.Deprovision(context.Background(), req.LeaseUUID) }()
	select {
	case err := <-deprovisionDone:
		t.Fatalf("deprovision escaped through the intent-to-reservation window: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(journal.release)
	require.NoError(t, <-provisionDone)
	select {
	case err := <-deprovisionDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("deprovision did not complete after provision published its reservation")
	}
	b.provisionsMu.RLock()
	_, exists := b.provisions[req.LeaseUUID]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)
}

func TestRestoreIntentToReservationWindowIsFencedAgainstDeprovision(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	statefulProfile := b.cfg.SKUProfiles["docker-small"]
	statefulProfile.DiskMB = 1024
	b.cfg.SKUProfiles["docker-small"] = statefulProfile
	store := b.callbackStore
	b.callbackStore = store
	rebuildCallbackSender(b, testCallbackClient)
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	journal := &blockingOperationIntentJournal{
		operationSettlementService: operations,
		delegate:                   operations,
		began:                      make(chan struct{}),
		release:                    make(chan struct{}),
	}
	b.operationSettlement = journal
	retentions := attachRetentionStore(t, b)
	const sourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
	require.NoError(t, putRetentionForTest(t, retentions, shared.RetentionEntry{
		OriginalLeaseUUID: sourceLeaseUUID,
		Tenant:            "tenant-a",
		ProviderUUID:      nominalDockerProviderUUID,
		Items: []backend.LeaseItem{{
			SKU: "docker-small", ServiceName: manifest.DefaultServiceName, Quantity: 1,
		}},
		StackManifest: restoreStackManifest(),
		Status:        shared.RetentionStatusActive,
		Generation:    1,
		CreatedAt:     time.Now(),
	}))
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
		require.NoError(t, store.Close())
	})
	req := restoreRequest(
		"550e8400-e29b-41d4-a716-446655440000",
		sourceLeaseUUID,
		"http://localhost/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8",
	)

	restoreCtx, cancelRestore := context.WithCancel(context.Background())
	restoreDone := make(chan error, 1)
	go func() { restoreDone <- b.Restore(restoreCtx, req) }()
	select {
	case <-journal.began:
	case err := <-restoreDone:
		t.Fatalf("restore returned before persisting its intent: %v", err)
	case <-time.After(time.Second):
		t.Fatal("restore did not persist its intent")
	}

	deprovisionDone := make(chan error, 1)
	go func() { deprovisionDone <- b.Deprovision(context.Background(), req.LeaseUUID) }()
	select {
	case err := <-deprovisionDone:
		t.Fatalf("deprovision escaped through the restore intent-to-reservation window: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Once the journal barrier opens, force an explicit pre-worker rejection.
	// The fence must remain held through rollback and exact intent settlement.
	cancelRestore()
	close(journal.release)
	require.Error(t, <-restoreDone)
	select {
	case err := <-deprovisionDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("deprovision did not complete after restore settled its reservation")
	}
	b.provisionsMu.RLock()
	_, exists := b.provisions[req.LeaseUUID]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)
}

func TestRecoverOperationIntent_PartialProvisionCohortIsTornDownBeforeFailureSettlement(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.Items[0].Quantity = 2
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{container}, nil)
	volume0 := canonicalVolumeName(spec.LeaseUUID, spec.Items[0].ServiceName, 0)
	volume1 := canonicalVolumeName(spec.LeaseUUID, spec.Items[0].ServiceName, 1)
	volumes := newVolumeSet(volume0, volume1)
	b.volumes = volumes.manager()

	// First-pass state recovery must restore the complete immutable reservation
	// and both volume claims, even though Docker exposes only one of two expected
	// containers. That authority remains published throughout teardown.
	require.NoError(t, b.recoverState(context.Background()))
	b.provisionsMu.RLock()
	recovered := b.provisions[spec.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, recovered)
	assert.Equal(t, 2, recovered.Quantity)
	assert.Equal(t, spec.Items, recovered.Items)
	assert.NotNil(t, b.pool.GetAllocation(spec.LeaseUUID+"-app-0"))
	assert.NotNil(t, b.pool.GetAllocation(spec.LeaseUUID+"-app-1"))
	claims, claimErr := b.snapshotVolumeClaims()
	require.NoError(t, claimErr)
	_, claimed0 := claims.owner(canonicalVolumeName(spec.LeaseUUID, "app", 0))
	_, claimed1 := claims.owner(canonicalVolumeName(spec.LeaseUUID, "app", 1))
	assert.True(t, claimed0)
	assert.True(t, claimed1)

	require.NoError(t, b.recoverOperationIntents(context.Background()))
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	_, getErr := b.GetProvision(context.Background(), spec.LeaseUUID)
	assert.ErrorIs(t, getErr, backend.ErrNotProvisioned)
	assert.Nil(t, b.pool.GetAllocation(spec.LeaseUUID+"-app-0"))
	assert.Nil(t, b.pool.GetAllocation(spec.LeaseUUID+"-app-1"))

	// Failed-operation settlement has no authority to infer that unattributed
	// paths are disposable. Exact teardown above owns what it can prove; anything
	// left behind remains for explicit operator attribution.
	assert.ElementsMatch(t, []string{volume0, volume1}, volumes.names(),
		"startup must preserve paths whose creation cannot be attributed to the failed operation")
}

func TestRecoverOperationIntent_PredecessorSubsetRestoresReleaseAndLeavesUnattributedCandidateVolume(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	oldItems := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 2}}
	oldProfiles := testResourceProfiles(t, oldItems)
	oldManifest := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	oldOperationID := mustDockerOperationID("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	oldCallbackURL := "https://fred.example/callbacks/provision?operation_id=" + oldOperationID.String()
	oldLifecycleURL, err := backend.ResolveLifecycleCallbackURL(oldCallbackURL, "")
	require.NoError(t, err)
	oldAuthority, err := shared.NewReleaseRuntimeAuthority(
		oldOperationID,
		"tenant-a",
		"22222222-2222-4222-8222-222222222222",
		oldCallbackURL,
		oldLifecycleURL,
	)
	require.NoError(t, err)

	candidate := dockerOperationIntentSpec(t, storageID)
	candidate.Items = []backend.LeaseItem{
		{SKU: "docker-micro", ServiceName: "app", Quantity: 2},
		{SKU: "docker-micro", ServiceName: "cache", Quantity: 1},
	}
	candidate.ResourceProfiles = testResourceProfiles(t, candidate.Items)
	candidate.Manifest = validStackManifestJSON(map[string]string{
		"app":   "docker.io/library/nginx:1.27",
		"cache": "docker.io/library/redis:7",
	})
	oldSpec := candidate
	oldSpec.CallbackURL = oldCallbackURL
	oldSpec.LifecycleCallbackURL = oldLifecycleURL
	oldSpec.Items = oldItems
	oldSpec.EffectiveItems = oldItems
	oldSpec.ResourceProfiles = oldProfiles
	oldSpec.Manifest = oldManifest
	// Model a crash after one of the two predecessor containers was removed.
	survivor := dockerIntentContainer(oldSpec, "old-container-1", oldItems[0].SKU, 1)
	b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{survivor}, nil)
	operations, ok := b.operationSettlement.(*shared.OperationSettlement)
	require.True(t, ok)
	seedProvisionReleaseForLeaseTest(t, store, b.releaseStore, operations, candidate.LeaseUUID, shared.Release{
		Manifest:         oldManifest,
		Image:            "stack",
		OperationID:      oldOperationID,
		Items:            oldItems,
		ResourceProfiles: oldProfiles,
		RuntimeAuthority: &oldAuthority,
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	})
	_, err = beginDockerTestOperationIntent(t, store, candidate, storageID)
	require.NoError(t, err)
	startPendingOperationForRecoveryTest(t, b)
	oldVolume0 := canonicalVolumeName(candidate.LeaseUUID, "app", 0)
	oldVolume1 := canonicalVolumeName(candidate.LeaseUUID, "app", 1)
	freshCandidateVolume := canonicalVolumeName(candidate.LeaseUUID, "cache", 0)
	volumes := newVolumeSet(oldVolume0, oldVolume1, freshCandidateVolume)
	b.volumes = volumes.manager()

	require.NoError(t, b.recoverState(context.Background()))
	assert.NotNil(t, b.pool.GetAllocation(candidate.LeaseUUID+"-app-0"))
	assert.NotNil(t, b.pool.GetAllocation(candidate.LeaseUUID+"-app-1"))
	assert.NotNil(t, b.pool.GetAllocation(candidate.LeaseUUID+"-cache-0"),
		"the complete candidate intent must remain reserved during cleanup")

	b.cfg.ProvisionTimeout = time.Nanosecond
	require.NoError(t, b.recoverOperationIntents(context.Background()))
	b.provisionsMu.RLock()
	recovered := b.provisions[candidate.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, recovered)
	assert.Equal(t, backend.ProvisionStatusFailed, recovered.Status)
	assert.Equal(t, oldItems, recovered.Items)
	assert.NotNil(t, b.pool.GetAllocation(candidate.LeaseUUID+"-app-0"))
	assert.NotNil(t, b.pool.GetAllocation(candidate.LeaseUUID+"-app-1"))
	assert.Nil(t, b.pool.GetAllocation(candidate.LeaseUUID+"-cache-0"))
	claims, claimErr := b.snapshotVolumeClaims()
	require.NoError(t, claimErr)
	_, old0Claimed := claims.owner(oldVolume0)
	_, old1Claimed := claims.owner(oldVolume1)
	_, freshClaimed := claims.owner(freshCandidateVolume)
	assert.True(t, old0Claimed)
	assert.True(t, old1Claimed)
	assert.False(t, freshClaimed)

	assert.ElementsMatch(t, []string{freshCandidateVolume}, volumes.names(),
		"a candidate-only path is preserved for operator attribution, never inferred disposable")
}

func TestRecoverOperationIntent_LegacyPredecessorFreezesAuthorityBeforeTeardown(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	candidate := dockerOperationIntentSpec(t, storageID)
	candidate.Items = []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "replacement", Quantity: 1,
	}}
	candidate.ResourceProfiles = testResourceProfiles(t, candidate.Items)
	candidate.Manifest = validStackManifestJSON(map[string]string{
		"replacement": "docker.io/library/redis:7",
	})

	oldItems := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 2}}
	oldProfiles := testResourceProfiles(t, oldItems)
	oldManifest := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	oldSpec := shared.OperationIntentSpec{
		LeaseUUID:            candidate.LeaseUUID,
		CallbackURL:          "https://legacy.example/callbacks/provision?route=v013",
		LifecycleCallbackURL: "", // v0.13 stored only callback_url; recovery derives the pair.
		Tenant:               candidate.Tenant,
		ProviderUUID:         candidate.ProviderUUID,
		Items:                oldItems,
		ResourceProfiles:     oldProfiles,
		Manifest:             oldManifest,
	}
	seedV013OperationReleaseForTest(t, store, candidate.LeaseUUID, shared.Release{
		Manifest:         oldManifest,
		Image:            "stack",
		Items:            slices.Clone(oldItems),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	})
	_, err = beginDockerTestOperationIntent(t, store, candidate, storageID)
	require.NoError(t, err)
	// Model a crash after one predecessor instance was removed. The candidate
	// intentionally uses a different SKU so the first recovery pass must account
	// this survivor from the active Release before legacy identity is frozen.
	survivor := dockerIntentContainer(oldSpec, "legacy-old-container-1", oldItems[0].SKU, 1)
	b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{survivor}, nil)
	startPendingOperationForRecoveryTest(t, b)
	b.cfg.ProvisionTimeout = time.Nanosecond

	require.NoError(t, b.recoverState(context.Background()))
	require.NoError(t, b.recoverOperationIntents(context.Background()))
	active, readErr := b.releaseStore.LatestActive(candidate.LeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, active)
	require.NotNil(t, active.LegacyRuntimeAuthority,
		"legacy identity must commit before teardown removes its last witness")
	assert.Equal(t, oldSpec.Tenant, active.LegacyRuntimeAuthority.Tenant())
	assert.Equal(t, oldSpec.ProviderUUID, active.LegacyRuntimeAuthority.ProviderUUID())
	assert.Equal(t, oldSpec.CallbackURL, active.LegacyRuntimeAuthority.CallbackURL())
	restoredLifecycle, resolveErr := backend.ResolveLifecycleCallbackURL(oldSpec.CallbackURL, "")
	require.NoError(t, resolveErr)
	assert.Equal(t, restoredLifecycle, active.LegacyRuntimeAuthority.LifecycleCallbackURL())
	b.provisionsMu.RLock()
	restored := b.provisions[candidate.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, restored)
	assert.Equal(t, backend.ProvisionStatusFailed, restored.Status)
	assert.Equal(t, oldItems, restored.Items)
	assert.Equal(t, oldSpec.CallbackURL, restored.CallbackURL)
	assert.Empty(t, restored.ContainerIDs)
	assert.NotNil(t, b.pool.GetAllocation(candidate.LeaseUUID+"-app-0"))
	assert.NotNil(t, b.pool.GetAllocation(candidate.LeaseUUID+"-app-1"))
	assert.Nil(t, b.pool.GetAllocation(candidate.LeaseUUID+"-replacement-0"))

	// A second zero-survivor rebuild reads only the durable release authority;
	// no in-memory container identity from the first process may be required.
	fresh := newBackendForTest(&mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}, nil)
	fresh.storageIdentity = operationIntentTestStoreIdentity(t, store)
	fresh.callbackStore = store
	fresh.operationSettlement = operationSettlementServiceForCallbackTest(t, store)
	fresh.releaseStore = b.releaseStore
	fresh.retentionStore = b.retentionStore
	fresh.restoreSettlement = b.restoreSettlement
	fresh.maintenanceSettlement = b.maintenanceSettlement
	fresh.closeSettlement = b.closeSettlement
	fresh.releaseCapacityPlanner = b.releaseCapacityPlanner
	fresh.callbackPublisher = b.callbackPublisher
	require.NoError(t, fresh.recoverState(context.Background()))
	fresh.provisionsMu.RLock()
	secondRestart := fresh.provisions[candidate.LeaseUUID]
	fresh.provisionsMu.RUnlock()
	require.NotNil(t, secondRestart)
	assert.Equal(t, oldSpec.Tenant, secondRestart.Tenant)
	assert.Equal(t, oldSpec.ProviderUUID, secondRestart.ProviderUUID)
	assert.Equal(t, oldSpec.CallbackURL, secondRestart.CallbackURL)
	assert.Equal(t, oldItems, secondRestart.Items)
	assert.NotNil(t, fresh.pool.GetAllocation(candidate.LeaseUUID+"-app-0"))
	assert.NotNil(t, fresh.pool.GetAllocation(candidate.LeaseUUID+"-app-1"))
}

func TestRecoverState_ProactivelyFreezesCompleteLegacyRuntimeAuthority(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	items := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 1}}
	profiles := testResourceProfiles(t, items)
	legacySpec := shared.OperationIntentSpec{
		LeaseUUID:            durableCallbackTestLeaseUUID,
		CallbackURL:          "https://legacy.example/callbacks/provision?route=v013",
		LifecycleCallbackURL: "",
		Tenant:               "tenant-a",
		ProviderUUID:         nominalDockerProviderUUID,
		Items:                items,
		ResourceProfiles:     profiles,
		Manifest:             validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"}),
	}
	seedV013OperationReleaseForTest(t, store, legacySpec.LeaseUUID, shared.Release{
		Manifest:         legacySpec.Manifest,
		Image:            "stack",
		Items:            slices.Clone(items),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(profiles),
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	})
	container := dockerIntentContainer(legacySpec, "legacy-container", items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{container}, nil)
	require.NoError(t, b.recoverState(context.Background()))
	active, readErr := b.releaseStore.LatestActive(legacySpec.LeaseUUID)
	require.NoError(t, readErr)
	require.NotNil(t, active)
	require.NotNil(t, active.LegacyRuntimeAuthority)
	assert.Equal(t, legacySpec.CallbackURL, active.LegacyRuntimeAuthority.CallbackURL())

	for restart := 1; restart <= 2; restart++ {
		fresh := newBackendForTest(&mockDockerClient{
			ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
		}, nil)
		fresh.storageIdentity = operationIntentTestStoreIdentity(t, store)
		fresh.callbackStore = store
		fresh.operationSettlement = operationSettlementServiceForCallbackTest(t, store)
		fresh.releaseStore = b.releaseStore
		fresh.retentionStore = b.retentionStore
		fresh.restoreSettlement = b.restoreSettlement
		fresh.maintenanceSettlement = b.maintenanceSettlement
		fresh.closeSettlement = b.closeSettlement
		fresh.releaseCapacityPlanner = b.releaseCapacityPlanner
		fresh.callbackPublisher = b.callbackPublisher
		require.NoError(t, fresh.recoverState(context.Background()), "restart %d", restart)
		fresh.provisionsMu.RLock()
		recovered := fresh.provisions[legacySpec.LeaseUUID]
		fresh.provisionsMu.RUnlock()
		require.NotNil(t, recovered, "restart %d", restart)
		assert.Equal(t, backend.ProvisionStatusFailed, recovered.Status)
		assert.Equal(t, legacySpec.Tenant, recovered.Tenant)
		assert.Equal(t, legacySpec.CallbackURL, recovered.CallbackURL)
		assert.NotNil(t, fresh.pool.GetAllocation(legacySpec.LeaseUUID+"-app-0"))
	}
}

func TestRecoverOperationIntentRejectsAmbiguousLegacyPredecessor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]ContainerInfo) []ContainerInfo
		want   string
	}{
		{
			name: "mixed callback generation",
			mutate: func(containers []ContainerInfo) []ContainerInfo {
				containers[1].CallbackURL = "https://other.example/callbacks/provision"
				return containers
			},
			want: "mixed callback identities",
		},
		{
			name: "foreign principal",
			mutate: func(containers []ContainerInfo) []ContainerInfo {
				containers[1].Tenant = "tenant-b"
				return containers
			},
			want: "different tenant or provider",
		},
		{
			name: "out of release instance",
			mutate: func(containers []ContainerInfo) []ContainerInfo {
				containers[1].InstanceIndex = 2
				return containers
			},
			want: "not in the exact released instance set",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
			require.NoError(t, err)
			store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			candidate := dockerOperationIntentSpec(t, storageID)
			oldItems := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 2}}
			oldProfiles := testResourceProfiles(t, oldItems)
			oldSpec := shared.OperationIntentSpec{
				LeaseUUID:            candidate.LeaseUUID,
				CallbackURL:          "https://legacy.example/callbacks/provision",
				LifecycleCallbackURL: "",
				Tenant:               candidate.Tenant,
				ProviderUUID:         candidate.ProviderUUID,
				Items:                oldItems,
				ResourceProfiles:     oldProfiles,
				Manifest:             validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"}),
			}
			seedV013OperationReleaseForTest(t, store, candidate.LeaseUUID, shared.Release{
				Manifest:         oldSpec.Manifest,
				Image:            "stack",
				Items:            slices.Clone(oldItems),
				ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
				Status:           "active",
				CreatedAt:        time.Now().Add(-time.Hour),
			})
			_, err = beginDockerTestOperationIntent(t, store, candidate, storageID)
			require.NoError(t, err)
			containers := tc.mutate([]ContainerInfo{
				dockerIntentContainer(oldSpec, "legacy-0", oldItems[0].SKU, 0),
				dockerIntentContainer(oldSpec, "legacy-1", oldItems[0].SKU, 1),
			})
			b := newOperationIntentRecoveryBackend(t, store, storageID, containers, nil)
			startPendingOperationForRecoveryTest(t, b)
			var downCalls int
			b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
				downCalls++
				return nil
			}}
			err = b.recoverOperationIntents(context.Background())
			require.ErrorContains(t, err, tc.want)
			assert.Zero(t, downCalls)
			intents, listErr := listOperationIntentsForCallbackTest(t, store)
			require.NoError(t, listErr)
			assert.Len(t, intents, 1)
			active, readErr := b.releaseStore.LatestActive(candidate.LeaseUUID)
			require.NoError(t, readErr)
			require.NotNil(t, active)
			assert.Nil(t, active.LegacyRuntimeAuthority,
				"ambiguous evidence must not be promoted into durable teardown authority")
		})
	}
}

func TestRecoverOperationIntent_RestoreOwnsPendingBeforeRetentionReconciliation(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.Kind = shared.OperationIntentRestore
	spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
	spec.SourceGeneration = 1
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)
	retentions := attachRetentionStore(t, b)
	putRestoreIntentFinalizer(t, retentions, spec, shared.RetentionEntry{
		OriginalLeaseUUID: spec.SourceLeaseUUID,
		NewLeaseUUID:      spec.LeaseUUID,
		Tenant:            spec.Tenant,
		ProviderUUID:      spec.ProviderUUID,
		Items:             slices.Clone(spec.Items),
		Status:            shared.RetentionStatusRestoring,
		Generation:        spec.SourceGeneration,
	})

	require.NoError(t, b.recoverOperationIntents(context.Background()))
	record, err := retentions.Get(spec.SourceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, shared.RetentionStatusRestoring, record.Status,
		"operation recovery must leave source handback to retention reconciliation")
	intents, listErr := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	var destinationCallbacks []shared.CallbackEntry
	for _, callback := range pending {
		if callback.CallbackURL == spec.CallbackURL {
			destinationCallbacks = append(destinationCallbacks, callback)
		}
	}
	require.Len(t, destinationCallbacks, 1)
	assert.Equal(t, backend.CallbackStatusFailed, destinationCallbacks[0].Status)
}

func TestStartRollsBackExactPartialRestoreIntent(t *testing.T) {
	var volumeMutations int
	specStorageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	spec := dockerOperationIntentSpec(t, specStorageID)
	spec.Kind = shared.OperationIntentRestore
	spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
	// The typed retained-close fixture starts at generation zero; its first
	// restore claim is therefore generation one. The test needs exact agreement,
	// not a caller-manufactured arbitrary generation.
	spec.SourceGeneration = 1
	spec.Items[0].Quantity = 2
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	var inventoryMu sync.Mutex
	containerPresent := true
	containerRemovals := 0
	mock := &mockDockerClient{
		PingFn:  func(context.Context) error { return nil },
		CloseFn: func() error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			inventoryMu.Lock()
			defer inventoryMu.Unlock()
			if !containerPresent {
				return nil, nil
			}
			return []ContainerInfo{container}, nil
		},
		InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
			copy := container
			return &copy, nil
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			inventoryMu.Lock()
			defer inventoryMu.Unlock()
			if containerID != container.ContainerID {
				return fmt.Errorf("remove unexpected container %q", containerID)
			}
			containerPresent = false
			containerRemovals++
			return nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error {
			volumeMutations++
			return nil
		},
		DestroyFn: func(context.Context, string) error {
			volumeMutations++
			return nil
		},
		UsageFn: func(context.Context, string) (int64, error) { return 0, nil },
	}
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		return errors.New("force exact partial-restore teardown fallback")
	}}
	store := b.callbackStore
	b.callbackStore = store
	b.operationSettlement = operationSettlementServiceForCallbackTest(t, store)
	rebuildCallbackSender(b, testCallbackClient)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
		require.NoError(t, store.Close())
	})
	_, err = beginDockerTestOperationIntent(t, store, spec, b.storageIdentity)
	require.NoError(t, err)

	attachReleaseStore(t, b)
	retentions := attachRetentionStore(t, b)
	putRestoreIntentFinalizer(t, retentions, spec, shared.RetentionEntry{
		OriginalLeaseUUID: spec.SourceLeaseUUID,
		NewLeaseUUID:      spec.LeaseUUID,
		Tenant:            spec.Tenant,
		ProviderUUID:      spec.ProviderUUID,
		Items:             append([]backend.LeaseItem(nil), spec.Items...),
		RetainedVolumeNames: []string{
			"fred-retained-" + spec.SourceLeaseUUID + "-app-0",
			"fred-retained-" + spec.SourceLeaseUUID + "-app-1",
		},
		StackManifest: restoreStackManifest(),
		Status:        shared.RetentionStatusRestoring,
		Generation:    spec.SourceGeneration,
		CreatedAt:     time.Now(),
	})

	require.NoError(t, b.Start(context.Background()))
	assert.Equal(t, 1, containerRemovals,
		"a survivor after successful Compose Down must be removed by exact ID")
	assert.Equal(t, 2, volumeMutations,
		"the exact partial cohort must re-quarantine both destination volume names")
	record, err := retentions.Get(spec.SourceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, shared.RetentionStatusActive, record.Status)
	assert.Equal(t, spec.SourceGeneration+1, record.Generation)
	intents, err := listOperationIntentsForCallbackTest(t, store)
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	require.NoError(t, b.Stop())
}

func TestRestoreIntent_ExactReadyFinalizesDuringReconciliation(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.Kind = shared.OperationIntentRestore
	spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
	// The typed retention fixture publishes generation 0, then the restore claim
	// advances it exactly once. Bind the operation to that actual source
	// generation instead of attempting to author an arbitrary store revision.
	spec.SourceGeneration = 1
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID),
	)
	retentions := attachRetentionStore(t, b)
	putRestoreIntentFinalizer(t, retentions, spec, shared.RetentionEntry{
		OriginalLeaseUUID: spec.SourceLeaseUUID,
		NewLeaseUUID:      spec.LeaseUUID,
		Tenant:            spec.Tenant,
		ProviderUUID:      spec.ProviderUUID,
		Items:             append([]backend.LeaseItem(nil), spec.Items...),
		Status:            shared.RetentionStatusRestoring,
		Generation:        spec.SourceGeneration,
	})

	require.NoError(t, b.recoverOperationIntents(context.Background()))
	require.NoError(t, b.reconcileRetentions(context.Background()))
	record, err := retentions.Get(spec.SourceLeaseUUID)
	require.NoError(t, err)
	assert.Nil(t, record, "exact Ready authority settles the intent before consuming the finalizer")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
}

func TestReconcileRetentions_ReportsIncompleteSourceQuarantine(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = &mockVolumeManager{RenameVolumeFn: func(string, string) error { return assert.AnError }}
	retentions := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, retentions, shared.RetentionEntry{
		OriginalLeaseUUID:   "123e4567-e89b-42d3-a456-426614174000",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-123e4567-e89b-42d3-a456-426614174000-app-0"},
		Generation:          1,
	}))

	err := b.reconcileRetentions(context.Background())
	require.ErrorIs(t, err, assert.AnError)
}

func TestRecoverOperationIntent_ExactContainersRebuildProjectionWithoutCurrentSKUConfig(t *testing.T) {
	tests := []struct {
		name       string
		sku        string
		projection func(shared.OperationIntentSpec, string) map[string]*provision
		want       string
		wantOK     bool
	}{
		{
			name: "missing volatile projection is rebuilt from exact substrate",
			sku:  "docker-micro",
			projection: func(shared.OperationIntentSpec, string) map[string]*provision {
				return nil
			},
			wantOK: true,
		},
		{
			name: "removed SKU survives from immutable intent",
			sku:  "removed-sku",
			projection: func(spec shared.OperationIntentSpec, containerID string) map[string]*provision {
				return readyIntentProjection(spec, containerID)
			},
			wantOK: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
			require.NoError(t, err)
			store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := dockerOperationIntentSpec(t, storageID)
			spec.Items[0].SKU = tt.sku
			spec.ResourceProfiles[0].SKU = tt.sku
			_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
			require.NoError(t, err)
			container := dockerIntentContainer(spec, "container-1", tt.sku, 0)
			b := newOperationIntentRecoveryBackend(
				t, store, storageID, []ContainerInfo{container}, tt.projection(spec, container.ContainerID),
			)

			err = b.recoverOperationIntents(context.Background())
			if tt.wantOK {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.want)
			}
			intents, listErr := listOperationIntentsForCallbackTest(t, store)
			require.NoError(t, listErr)
			if tt.wantOK {
				assert.Empty(t, intents)
				b.provisionsMu.RLock()
				recovered := b.provisions[spec.LeaseUUID]
				b.provisionsMu.RUnlock()
				require.NotNil(t, recovered,
					"exact durable intent plus exact Ready substrate must reconstruct the volatile projection")
				assert.Equal(t, backend.ProvisionStatusReady, recovered.Status)
				assert.Equal(t, []string{container.ContainerID}, recovered.ContainerIDs)
				assert.Equal(t, dockerOperationIntentID, recovered.ActiveOperationID)
			} else {
				assert.Len(t, intents, 1)
			}
		})
	}
}

func TestRecoverOperationIntent_RequiresExactManifestImageAndEffectiveDomain(t *testing.T) {
	tests := []struct {
		name       string
		mutateSpec func(*shared.OperationIntentSpec)
		mutate     func([]ContainerInfo)
		want       string
		wantOK     bool
	}{
		{
			name: "wrong image",
			mutate: func(containers []ContainerInfo) {
				containers[0].Image = "docker.io/library/busybox:latest"
			},
			want: "image does not match",
		},
		{
			name: "inconsistent sibling domains",
			mutateSpec: func(spec *shared.OperationIntentSpec) {
				spec.Items[0].Quantity = 2
				spec.Items[0].CustomDomain = "app.example.com"
			},
			mutate: func(containers []ContainerInfo) {
				containers[1].CustomDomain = "other.example.com"
			},
			want: "inconsistent custom-domain",
		},
		{
			name: "DNS-deferred desired domain",
			mutateSpec: func(spec *shared.OperationIntentSpec) {
				spec.Items[0].CustomDomain = "later.example.com"
				spec.EffectiveItems = append([]backend.LeaseItem(nil), spec.Items...)
				spec.EffectiveItems[0].CustomDomain = ""
			},
			mutate: func([]ContainerInfo) {},
			wantOK: true,
		},
		{
			name: "effective domain mismatch",
			mutateSpec: func(spec *shared.OperationIntentSpec) {
				spec.Items[0].CustomDomain = "app.example.com"
				spec.EffectiveItems = append([]backend.LeaseItem(nil), spec.Items...)
			},
			mutate: func(containers []ContainerInfo) {
				containers[0].CustomDomain = "other.example.com"
			},
			want: "does not match durable effective items",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
			require.NoError(t, err)
			store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := dockerOperationIntentSpec(t, storageID)
			if tt.mutateSpec != nil {
				tt.mutateSpec(&spec)
			}
			_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
			require.NoError(t, err)
			containers := make([]ContainerInfo, 0, spec.Items[0].Quantity)
			ids := make([]string, 0, spec.Items[0].Quantity)
			for index := range spec.Items[0].Quantity {
				id := fmt.Sprintf("container-%d", index)
				containers = append(containers, dockerIntentContainer(spec, id, spec.Items[0].SKU, index))
				ids = append(ids, id)
			}
			tt.mutate(containers)
			b := newOperationIntentRecoveryBackend(
				t, store, storageID, containers, readyIntentProjection(spec, ids...),
			)

			err = b.recoverOperationIntents(context.Background())
			if tt.wantOK {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.want)
			intents, listErr := listOperationIntentsForCallbackTest(t, store)
			require.NoError(t, listErr)
			assert.Len(t, intents, 1)
		})
	}
}

func TestAwaitAsyncAcceptance_CancellationWithoutActorAckIsUnknown(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ack := make(chan error, 1)

	state, err := b.awaitAsyncAcceptance(ctx, ack)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, asyncAcceptanceUnknown, state)
	ack <- nil // the actor may accept immediately after the caller observed cancellation
}

func TestStrictManagedInventoryRejectsUnattributableSubstrate(t *testing.T) {
	const operationID = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	valid := map[string]string{
		LabelManaged:              "true",
		LabelBackendName:          "docker",
		LabelLeaseUUID:            "550e8400-e29b-41d4-a716-446655440000",
		LabelTenant:               "tenant-a",
		LabelProviderUUID:         "provider-a",
		LabelSKU:                  "docker-micro",
		LabelServiceName:          manifest.DefaultServiceName,
		LabelInstanceIndex:        "0",
		LabelCallbackURL:          callbackURL,
		LabelLifecycleCallbackURL: lifecycleURL,
	}
	require.NoError(t, validateStrictManagedContainerLabels("container-1", "docker", valid))

	v013MigrationStack := make(map[string]string, len(valid))
	for key, value := range valid {
		v013MigrationStack[key] = value
	}
	v013MigrationStack[LabelServiceName] = manifest.DefaultServiceName
	v013MigrationStack[LabelBackendName] = ""
	v013MigrationStack[LabelProviderUUID] = ""
	v013MigrationStack[LabelCallbackURL] = ""
	v013MigrationStack[LabelLifecycleCallbackURL] = ""
	require.Error(t, validateStrictManagedContainerLabels(
		"v0.13-migration-stack", "docker", v013MigrationStack,
	), "unsupported v0.13 omissions must be rejected before they can acquire current write authority")
	v013MigrationStack[LabelProviderUUID] = "partially-restored-provider"
	require.Error(t, validateStrictManagedContainerLabels(
		"partial-v0.13-migration-stack", "docker", v013MigrationStack,
	), "a partial authority omission is not a writer-recognizable compatibility shape")

	for _, test := range []struct {
		name  string
		label string
		value string
	}{
		{name: "missing lease", label: LabelLeaseUUID},
		{name: "missing tenant", label: LabelTenant},
		{name: "missing provider", label: LabelProviderUUID},
		{name: "missing SKU", label: LabelSKU},
		{name: "missing service", label: LabelServiceName},
		{name: "missing operation callback", label: LabelCallbackURL},
		{name: "wrong backend", label: LabelBackendName, value: "docker-other"},
		{name: "mismatched lifecycle callback", label: LabelLifecycleCallbackURL,
			value: "https://other.example/callbacks/provision?lifecycle_id=" + operationID},
	} {
		t.Run(test.name, func(t *testing.T) {
			labels := make(map[string]string, len(valid))
			for key, value := range valid {
				labels[key] = value
			}
			labels[test.label] = test.value
			require.Error(t, validateStrictManagedContainerLabels("container-1", "docker", labels))
		})
	}
}
