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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

type fixedOperationIntentProbeJournal struct {
	disposition shared.OperationIntentAdmissionDisposition
}

type blockingOperationIntentJournal struct {
	delegate operationIntentJournal
	began    chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (j *blockingOperationIntentJournal) ProbeOperationIntent(
	probe shared.OperationIntentProbe,
) (shared.OperationIntentAdmissionDisposition, error) {
	return j.delegate.ProbeOperationIntent(probe)
}

func (j *blockingOperationIntentJournal) BeginOperationIntent(
	spec shared.OperationIntentSpec,
) (shared.OperationIntentAdmission, error) {
	j.once.Do(func() { close(j.began) })
	<-j.release
	return j.delegate.BeginOperationIntent(spec)
}

func (j *blockingOperationIntentJournal) ResolveOperationIntent(
	claim shared.OperationIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) (shared.CallbackEntry, error) {
	return j.delegate.ResolveOperationIntent(claim, status, errMsg)
}

func (j fixedOperationIntentProbeJournal) ProbeOperationIntent(
	shared.OperationIntentProbe,
) (shared.OperationIntentAdmissionDisposition, error) {
	return j.disposition, nil
}

func (fixedOperationIntentProbeJournal) BeginOperationIntent(
	shared.OperationIntentSpec,
) (shared.OperationIntentAdmission, error) {
	return shared.OperationIntentAdmission{}, errors.New("unexpected BeginOperationIntent after exact probe")
}

func (fixedOperationIntentProbeJournal) ResolveOperationIntent(
	shared.OperationIntentClaim,
	backend.CallbackStatus,
	string,
) (shared.CallbackEntry, error) {
	return shared.CallbackEntry{}, errors.New("unexpected ResolveOperationIntent after exact probe")
}

const dockerOperationIntentID = shared.OperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")

func TestReleaseHistoryCapacityPlannerDoesNotReturnTypedNilStore(t *testing.T) {
	t.Parallel()

	var b Backend
	if planner := b.releaseHistoryCapacityPlanner(); planner != nil {
		t.Fatalf("nil release store returned a non-nil capacity planner of type %T", planner)
	}
}

func dockerOperationIntentSpec(t *testing.T, storageID backendidentity.ID) shared.OperationIntentSpec {
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
		Backend:              "docker",
		BackendStorageID:     storageID,
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
	storageID backendidentity.ID,
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
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		inventory = nil
		return nil
	}}
	b.storageIdentity = storageID
	b.callbackStore = store
	b.operationIntents = store
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releaseStore.Close()) })
	b.releaseStore = releaseStore
	return b
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
				Items:                append([]backend.LeaseItem(nil), effectiveItems...),
				ContainerIDs:         append([]string(nil), containerIDs...),
				StackManifest:        stack,
			},
			ResourceProfiles: shared.CloneSKUResourceSnapshot(spec.ResourceProfiles),
		},
	}
}

func TestRecoverOperationIntent_RestartAfterSubstrateSuccessBeforeCallback(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)
	require.NoError(t, store.Close(), "simulate the backend process crashing with only the intent durable")

	store, err = shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(
		t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID),
	)
	// A same-manifest previous release is not sufficient: quantity/domain
	// changes can retain identical payload bytes while changing the exact
	// container cohort. Recovery must supersede this stale topology.
	require.NoError(t, b.releaseStore.Append(spec.LeaseUUID, shared.Release{
		Manifest: spec.Manifest,
		Image:    "stack",
		Items: []backend.LeaseItem{{
			SKU: spec.Items[0].SKU, ServiceName: spec.Items[0].ServiceName, Quantity: 2,
		}},
		ResourceProfiles: shared.CloneSKUResourceSnapshot(spec.ResourceProfiles),
		Status:           "active", CreatedAt: time.Now(),
	}))

	require.NoError(t, b.recoverOperationIntents(context.Background(), nil))
	intents, err := store.ListOperationIntents()
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
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = store.BeginOperationIntent(spec)
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
	err = b.recoverOperationIntents(context.Background(), nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), time.Second,
		"a stalled Docker inspection must not wedge operation recovery")
	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Len(t, intents, 1, "timed-out inspection must preserve the exact operation intent")
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending, "timed-out inspection must not manufacture a completion")
}

func TestProvisionReleaseAppendFailureRetainsIntentUntilRestartRecovery(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	callbackPath := filepath.Join(t.TempDir(), "callbacks.db")
	callbackStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = callbackStore.BeginOperationIntent(spec)
	require.NoError(t, err)

	releasePath := filepath.Join(t.TempDir(), "releases.db")
	failedReleaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	require.NoError(t, failedReleaseStore.Close(), "inject release Append failure after substrate success")

	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
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
	b.operationIntents = callbackStore
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:      callbackStore,
		HTTPClient: &http.Client{},
		Secret:     durableCallbackTestSecret,
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
		BeforeDelivery: func(context.Context) error {
			return b.terminalStorageAuthorityError()
		},
		BeforeReplay: func(context.Context) error {
			return b.terminalStorageAuthorityError()
		},
		StorageIdentity: storageID,
	})
	b.releaseStore = failedReleaseStore
	b.cfg.StartupVerifyDuration = time.Millisecond
	stack, err := manifest.ParsePayload(spec.Manifest)
	require.NoError(t, err)
	req := backend.ProvisionRequest{
		LeaseUUID:            spec.LeaseUUID,
		CallbackURL:          spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL,
		Tenant:               spec.Tenant,
		ProviderUUID:         spec.ProviderUUID,
		Items:                append([]backend.LeaseItem(nil), spec.Items...),
		Payload:              append([]byte(nil), spec.Manifest...),
	}

	callbackErr, reason, _, _, provisionErr := b.doProvision(
		context.Background(), req, stack,
		spec.ResourceProfiles, b.logger,
	)
	require.ErrorIs(t, provisionErr, backendidentity.ErrMutationOutcomeAmbiguous)
	assert.Equal(t, leasesm.ErrMsgInternal, callbackErr)
	assert.Equal(t, backend.ReasonInternal, reason)
	assert.Zero(t, downCalls, "ambiguous durable settlement must retain successful substrate")
	select {
	case <-b.stopCtx.Done():
	default:
		t.Fatal("release settlement ambiguity did not latch the backend lifetime")
	}
	// Model the actor's terminal error path. The lifetime latch/stop context
	// must suppress this callback before its atomic settlement can consume the
	// exact operation intent.
	b.sendOperationCallbackWithURL(
		spec.LeaseUUID, spec.CallbackURL, backend.CallbackStatusFailed, callbackErr,
	)
	intents, err := callbackStore.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1)
	pending, err := callbackStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
	require.NoError(t, callbackStore.Close(), "simulate process exit with exact intent retained")

	callbackStore, err = shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbackStore.Close()) })
	recovered := newOperationIntentRecoveryBackend(
		t, callbackStore, storageID, []ContainerInfo{container},
		readyIntentProjection(spec, container.ContainerID),
	)
	require.NoError(t, recovered.releaseStore.Close())
	recoveredReleaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recoveredReleaseStore.Close()) })
	recovered.releaseStore = recoveredReleaseStore

	require.NoError(t, recovered.recoverOperationIntents(context.Background(), nil))
	intents, err = callbackStore.ListOperationIntents()
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
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	callbackStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbackStore.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	admission, err := callbackStore.BeginOperationIntent(spec)
	require.NoError(t, err)

	mutationCause := errors.New("xfs final-name parent sync failed")
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	b.storageIdentity = storageID
	b.callbackStore = callbackStore
	b.operationIntents = callbackStore
	b.volumes = &mockVolumeManager{CreateFn: func(context.Context, string, int64) (string, bool, error) {
		return "", false, fmt.Errorf("%w: %w", backendidentity.ErrMutationOutcomeAmbiguous, mutationCause)
	}}
	stack, err := manifest.ParsePayload(spec.Manifest)
	require.NoError(t, err)
	req := backend.ProvisionRequest{
		LeaseUUID:            spec.LeaseUUID,
		CallbackURL:          spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL,
		Tenant:               spec.Tenant,
		ProviderUUID:         spec.ProviderUUID,
		Items:                append([]backend.LeaseItem(nil), spec.Items...),
		Payload:              append([]byte(nil), spec.Manifest...),
	}

	_, _, _, _, provisionErr := b.doProvision(
		context.Background(), req, stack, spec.ResourceProfiles, b.logger,
	)
	require.ErrorIs(t, provisionErr, backendidentity.ErrMutationOutcomeAmbiguous)
	require.ErrorIs(t, provisionErr, mutationCause)
	select {
	case <-b.stopCtx.Done():
	default:
		t.Fatal("volume durability ambiguity did not stop the backend lifetime")
	}

	refusalErr := b.refuseOperationIntent(&admission.Claim, provisionErr)
	require.ErrorIs(t, refusalErr, backendidentity.ErrMutationOutcomeAmbiguous)
	intents, err := callbackStore.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1, "typed volume ambiguity must preserve the exact operation intent")
	pending, err := callbackStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "an ambiguous volume result must not publish a failed callback")
}

func TestProvisionTeardownFailureRetainsIntentPoolAndVolumesAndStopsBackend(t *testing.T) {
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
	var destroyCalls int
	b.volumes = &mockVolumeManager{
		defaultDir: t.TempDir(),
		DestroyFn: func(context.Context, string) error {
			destroyCalls++
			return nil
		},
	}
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	b.callbackStore = store
	b.operationIntents = store
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           store,
		HTTPClient:      testCallbackClient,
		Secret:          durableCallbackTestSecret,
		Logger:          b.logger,
		StopCtx:         b.stopCtx,
		BeforeDelivery:  func(context.Context) error { return b.terminalStorageAuthorityError() },
		BeforeReplay:    func(context.Context) error { return b.terminalStorageAuthorityError() },
		StorageIdentity: b.storageIdentity,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: testCallbackDeliveryTimeout,
	})
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
	select {
	case <-b.stopCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("incomplete failed-provision teardown did not stop the backend")
	}
	b.wg.Wait()

	assert.Zero(t, destroyCalls, "managed volumes must remain untouched while a container may still mount them")
	require.ErrorIs(t, b.terminalStorageAuthorityError(), backendidentity.ErrMutationOutcomeAmbiguous)
	allocation := b.pool.GetAllocation(spec.LeaseUUID + "-app-0")
	require.NotNil(t, allocation, "the full reservation must remain while substrate cleanup is incomplete")
	b.provisionsMu.RLock()
	retained := b.provisions[spec.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, retained)
	assert.Equal(t, spec.Items, retained.Items)
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, spec.LeaseUUID, intents[0].LeaseUUID())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "the actor must not erase recovery authority with a failed callback")
}

func TestProvisionVolumeRecoveryPendingRetainsIntentReservationAndSuppressesCallback(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return &ImageInfo{ID: "image-1", Volumes: map[string]struct{}{`/data`: {}}}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.ContainerReadonlyRootfs = ptrBool(false)
	bindTestStorageIdentity(t, b, mock)
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
			return fmt.Errorf("injected staged-delete finalization failure: %w", ErrVolumeMutationRecoveryPending)
		},
	}
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	b.callbackStore = store
	b.operationIntents = store
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           store,
		HTTPClient:      testCallbackClient,
		Secret:          durableCallbackTestSecret,
		Logger:          b.logger,
		StopCtx:         b.stopCtx,
		BeforeDelivery:  func(context.Context) error { return b.terminalStorageAuthorityError() },
		BeforeReplay:    func(context.Context) error { return b.terminalStorageAuthorityError() },
		StorageIdentity: b.storageIdentity,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: testCallbackDeliveryTimeout,
	})
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
	select {
	case <-b.stopCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("volume recovery obligation did not stop the backend")
	}
	b.wg.Wait()

	assert.Equal(t, 1, destroyCalls)
	require.ErrorIs(t, b.terminalStorageAuthorityError(), ErrVolumeMutationRecoveryPending)
	require.NotNil(t, b.pool.GetAllocation(spec.LeaseUUID+"-app-0"),
		"capacity must stay reserved until startup consumes the staged-delete recovery record")
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, spec.LeaseUUID, intents[0].LeaseUUID())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending,
		"a typed storage-recovery obligation must prevent callback settlement from erasing the WAL")
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
		CallbackURL:  "https://new.example/callbacks/provision",
		Payload:      validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"}),
	}
	err := b.Provision(context.Background(), req)
	require.ErrorContains(t, err, "different tenant or provider")
	assert.Zero(t, downCalls, "a caller must not acquire teardown authority across principals")
	b.provisionsMu.RLock()
	retained := b.provisions[oldSpec.LeaseUUID]
	b.provisionsMu.RUnlock()
	assert.Same(t, old, retained)
}

func TestRecoverOperationIntentRejectsCrossPrincipalPredecessorBeforeTeardown(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	candidate := dockerOperationIntentSpec(t, storageID)
	_, err = store.BeginOperationIntent(candidate)
	require.NoError(t, err)

	oldOperationID := shared.OperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
	oldCallbackURL := "https://fred.example/callbacks/provision?operation_id=" + oldOperationID.String()
	oldLifecycleURL, err := backend.ResolveLifecycleCallbackURL(oldCallbackURL, "")
	require.NoError(t, err)
	oldAuthority, err := shared.NewReleaseRuntimeAuthority(
		oldOperationID,
		"tenant-b",
		"33333333-3333-4333-8333-333333333333",
		oldCallbackURL,
		oldLifecycleURL,
	)
	require.NoError(t, err)
	oldSpec := candidate
	oldSpec.Tenant = oldAuthority.Tenant()
	oldSpec.ProviderUUID = oldAuthority.ProviderUUID()
	oldSpec.CallbackURL = oldCallbackURL
	oldSpec.LifecycleCallbackURL = oldLifecycleURL
	oldContainer := dockerIntentContainer(oldSpec, "old-container", oldSpec.Items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{oldContainer}, nil)
	var downCalls int
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		downCalls++
		return nil
	}}
	require.NoError(t, b.releaseStore.AppendActive(candidate.LeaseUUID, shared.Release{
		Manifest:         candidate.Manifest,
		Image:            "stack",
		OperationID:      oldOperationID,
		Items:            slices.Clone(candidate.Items),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(candidate.ResourceProfiles),
		RuntimeAuthority: &oldAuthority,
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	}))

	err = b.recoverOperationIntents(context.Background(), nil)
	require.ErrorContains(t, err, "different tenant or provider")
	assert.Zero(t, downCalls, "recovery must reject cross-principal teardown before mutation")
	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Len(t, intents, 1)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)
}

func TestProvisionRejectedAfterPredecessorTeardownRestoresProjectionAndAccounting(t *testing.T) {
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
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		return errors.New("force exact fallback removal")
	}}
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	b.callbackStore = store
	b.operationIntents = store
	releases := attachReleaseStore(t, b)

	oldItems := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 2}}
	oldProfiles := testResourceProfiles(t, oldItems)
	oldOperationID := shared.OperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
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
	// projection to Provision. Its explicit rejection exercises the post-teardown
	// rollback boundary without starting a worker.
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
	require.NoError(t, releases.AppendActive(oldSpec.LeaseUUID, shared.Release{
		Manifest:         oldManifest,
		Image:            "stack",
		OperationID:      oldOperationID,
		Items:            slices.Clone(oldItems),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
		RuntimeAuthority: &oldAuthority,
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	}))

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
	assert.Equal(t, []string{"actual-predecessor-survivor"}, removed,
		"teardown must use the fresh classified survivor, never the stale projection ID")
	b.provisionsMu.RLock()
	restored := b.provisions[oldSpec.LeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, restored)
	assert.Equal(t, backend.ProvisionStatusFailed, restored.Status)
	assert.Equal(t, oldItems, restored.Items)
	assert.Empty(t, restored.ContainerIDs)
	for _, allocationID := range oldIDs {
		assert.NotNil(t, b.pool.GetAllocation(allocationID))
	}
	assert.Nil(t, b.pool.GetAllocation(oldSpec.LeaseUUID+"-cache-0"))
	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
}

func TestFailedReplacementProvisionRetainsIntentAndAccountingForColdRecovery(t *testing.T) {
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
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	b.callbackStore = store
	b.operationIntents = store
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           store,
		HTTPClient:      testCallbackClient,
		Secret:          durableCallbackTestSecret,
		Logger:          b.logger,
		StopCtx:         b.stopCtx,
		BeforeDelivery:  func(context.Context) error { return b.terminalStorageAuthorityError() },
		BeforeReplay:    func(context.Context) error { return b.terminalStorageAuthorityError() },
		StorageIdentity: b.storageIdentity,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: testCallbackDeliveryTimeout,
	})
	releases := attachReleaseStore(t, b)

	oldItems := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 1}}
	oldProfiles := testResourceProfiles(t, oldItems)
	oldOperationID := shared.OperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
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
	require.NoError(t, releases.AppendActive(oldSpec.LeaseUUID, shared.Release{
		Manifest:         manifestPayload,
		Image:            "stack",
		OperationID:      oldOperationID,
		Items:            slices.Clone(oldItems),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
		RuntimeAuthority: &oldAuthority,
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	}))
	volumeRoot := t.TempDir()
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
	select {
	case <-b.stopCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("failed replacement did not latch the backend for cold recovery")
	}
	b.wg.Wait()
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
	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	require.Len(t, intents, 1)
	assert.Equal(t, candidate.LeaseUUID, intents[0].LeaseUUID())
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending, "the stopped backend must not settle through the predecessor route")
}

func TestRecoverOperationIntent_CrashBeforeMutationSettlesExactFailure(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)

	require.NoError(t, b.recoverOperationIntents(context.Background(), nil))
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, interruptedOperationFailure, pending[0].Error)
}

func TestMutationPostcheckAmbiguityRetainsIntentUntilRestartRecovery(t *testing.T) {
	const daemonID = "daemon-a"
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	cfg := Config{Name: "docker", CallbackDBPath: dbPath}
	markerPath := cfg.CallbackDBPath + ".storage-identity.json"
	anchorPath := cfg.CallbackDBPath + ".storage-identity-anchor.json"
	storageID, err := initializeTestMarkerPair(
		markerPath, anchorPath, cfg.Name, daemonID,
	)
	require.NoError(t, err)

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
	b.mutations = storageMutationAdapters{backend: b}

	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	spec := dockerOperationIntentSpec(t, storageID)
	admission, err := store.BeginOperationIntent(spec)
	require.NoError(t, err)

	requests := 0
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
		})},
		Secret:          durableCallbackTestSecret,
		Logger:          b.logger,
		StopCtx:         b.stopCtx,
		BeforeDelivery:  b.VerifyStorageIdentity,
		BeforeReplay:    b.VerifyStorageIdentity,
		StorageIdentity: storageID,
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
	refusalErr := b.refuseOperationIntent(&admission.Claim, laterCause)
	require.Error(t, refusalErr)
	assert.ErrorIs(t, refusalErr, laterCause)
	assert.ErrorIs(t, refusalErr, backendidentity.ErrMutationOutcomeAmbiguous)
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1, "a swallowed mutation ambiguity must still preserve the durable intent")
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "an ambiguous refusal must not publish an exact failure")

	b.sendOperationCallbackWithURL(
		spec.LeaseUUID, spec.CallbackURL, backend.CallbackStatusSuccess, "",
	)
	assert.Zero(t, requests, "a callback after the ambiguity latch must not reach HTTP")
	intents, err = store.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1, "callback suppression must retain the durable intent")
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "an unclassified completion must not replace the intent")
	require.NoError(t, store.Close(), "simulate process exit with the intent durable")

	// A fresh process can re-attest the same marker/daemon and classify the
	// retained intent against substrate inventory. No matching containers means
	// the pre-mutation outcome is now provable and becomes an exact failure.
	postcheckUnavailable = false
	store, err = shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	recovered := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)
	require.NoError(t, recovered.recoverOperationIntents(context.Background(), nil))
	intents, err = store.ListOperationIntents()
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
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	spec := dockerOperationIntentSpec(t, storageID)
	admission, err := store.BeginOperationIntent(spec)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)

	// The synchronous response is deliberately ignored: this models the peer
	// losing the response after the backend durably refused the operation.
	err = b.refuseOperationIntent(&admission.Claim, backend.ErrAlreadyProvisioned)
	require.ErrorIs(t, err, backend.ErrAlreadyProvisioned)
	require.NoError(t, store.Close())

	store, err = shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)

	disposition, err := store.ProbeOperationIntent(shared.OperationIntentProbe{
		LeaseUUID: spec.LeaseUUID, CallbackURL: spec.CallbackURL, Backend: spec.Backend,
		BackendStorageID: storageID,
	})
	require.NoError(t, err)
	assert.Equal(t, shared.OperationIntentAdmissionCompleted, disposition,
		"an exact retry must acknowledge the durable failure instead of starting work")
}

func TestDeprovisionSettlesIntentWhenProjectionWasNeverPublished(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)

	require.NoError(t, b.doDeprovision(context.Background(), spec.LeaseUUID))
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Contains(t, pending[0].Error, "preempted by deprovision")
}

func TestExactOperationRetryBypassesMutableSemanticValidation(t *testing.T) {
	const operationID = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.operationIntents = fixedOperationIntentProbeJournal{
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
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath})
	require.NoError(t, err)
	b.callbackStore = store
	rebuildCallbackSender(b, testCallbackClient)
	journal := &blockingOperationIntentJournal{
		delegate: store,
		began:    make(chan struct{}),
		release:  make(chan struct{}),
	}
	b.operationIntents = journal
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
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath})
	require.NoError(t, err)
	b.callbackStore = store
	rebuildCallbackSender(b, testCallbackClient)
	journal := &blockingOperationIntentJournal{
		delegate: store,
		began:    make(chan struct{}),
		release:  make(chan struct{}),
	}
	b.operationIntents = journal
	retentions := attachRetentionStore(t, b)
	const sourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
	require.NoError(t, retentions.Put(shared.RetentionEntry{
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
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.Items[0].Quantity = 2
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{container}, nil)
	volumes := newVolumeSet(
		canonicalVolumeName(spec.LeaseUUID, spec.Items[0].ServiceName, 0),
		canonicalVolumeName(spec.LeaseUUID, spec.Items[0].ServiceName, 1),
	)
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

	require.NoError(t, b.recoverOperationIntents(context.Background(), nil))
	intents, listErr := store.ListOperationIntents()
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

	// Only after settlement and the internal state rebuild do the fresh volumes
	// become ordinary orphans. The existing startup pass then removes both.
	require.NoError(t, b.cleanupOrphanedVolumes(context.Background()))
	assert.ElementsMatch(t, []string{
		canonicalVolumeName(spec.LeaseUUID, "app", 0),
		canonicalVolumeName(spec.LeaseUUID, "app", 1),
	}, volumes.names())
}

func TestRecoverOperationIntent_PredecessorSubsetRestoresReleaseAndReapsOnlyCandidateOrphan(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	oldItems := []backend.LeaseItem{{SKU: "docker-micro", ServiceName: "app", Quantity: 2}}
	oldProfiles := testResourceProfiles(t, oldItems)
	oldManifest := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	const oldOperationID = shared.OperationID("9a72fbc1-38c8-4f31-87f7-f689979b9324")
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
	_, err = store.BeginOperationIntent(candidate)
	require.NoError(t, err)

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
	require.NoError(t, b.releaseStore.AppendActive(candidate.LeaseUUID, shared.Release{
		Manifest:         oldManifest,
		Image:            "stack",
		OperationID:      oldOperationID,
		Items:            oldItems,
		ResourceProfiles: oldProfiles,
		RuntimeAuthority: &oldAuthority,
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	}))
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

	require.NoError(t, b.recoverOperationIntents(context.Background(), nil))
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

	require.NoError(t, b.cleanupOrphanedVolumes(context.Background()))
	assert.Equal(t, []string{freshCandidateVolume}, volumes.names(),
		"the older release protects only its exact names; the candidate-only volume is reaped")
}

func TestRecoverOperationIntent_LegacyPredecessorFreezesAuthorityBeforeTeardown(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
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
	_, err = store.BeginOperationIntent(candidate)
	require.NoError(t, err)

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
	// Model a crash after one predecessor instance was removed. The candidate
	// intentionally uses a different SKU so the first recovery pass must account
	// this survivor from the active Release before legacy identity is frozen.
	survivor := dockerIntentContainer(oldSpec, "legacy-old-container-1", oldItems[0].SKU, 1)
	b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{survivor}, nil)
	require.NoError(t, b.releaseStore.AppendActive(candidate.LeaseUUID, shared.Release{
		Manifest:         oldManifest,
		Image:            "stack",
		Items:            slices.Clone(oldItems),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	}))

	require.NoError(t, b.recoverState(context.Background()))
	require.NoError(t, b.recoverOperationIntents(context.Background(), nil))
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
	fresh.storageIdentity = storageID
	fresh.callbackStore = store
	fresh.operationIntents = store
	fresh.releaseStore = b.releaseStore
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
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
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
	container := dockerIntentContainer(legacySpec, "legacy-container", items[0].SKU, 0)
	b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{container}, nil)
	require.NoError(t, b.releaseStore.AppendActive(legacySpec.LeaseUUID, shared.Release{
		Manifest:         legacySpec.Manifest,
		Image:            "stack",
		Items:            slices.Clone(items),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(profiles),
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	}))
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
		fresh.storageIdentity = storageID
		fresh.callbackStore = store
		fresh.operationIntents = store
		fresh.releaseStore = b.releaseStore
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
			store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			candidate := dockerOperationIntentSpec(t, storageID)
			_, err = store.BeginOperationIntent(candidate)
			require.NoError(t, err)
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
			containers := tc.mutate([]ContainerInfo{
				dockerIntentContainer(oldSpec, "legacy-0", oldItems[0].SKU, 0),
				dockerIntentContainer(oldSpec, "legacy-1", oldItems[0].SKU, 1),
			})
			b := newOperationIntentRecoveryBackend(t, store, storageID, containers, nil)
			var downCalls int
			b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
				downCalls++
				return nil
			}}
			require.NoError(t, b.releaseStore.AppendActive(candidate.LeaseUUID, shared.Release{
				Manifest:         oldSpec.Manifest,
				Image:            "stack",
				Items:            slices.Clone(oldItems),
				ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
				Status:           "active",
				CreatedAt:        time.Now().Add(-time.Hour),
			}))

			err = b.recoverOperationIntents(context.Background(), nil)
			require.ErrorContains(t, err, tc.want)
			assert.Zero(t, downCalls)
			intents, listErr := store.ListOperationIntents()
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

func TestRecoverOperationIntent_RestoreWaitsForCleanRetentionReconciliation(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.Kind = shared.OperationIntentRestore
	spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
	spec.SourceGeneration = 2
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, nil)

	err = b.recoverOperationIntents(context.Background(), assert.AnError)
	require.ErrorContains(t, err, "retention reconciliation failed")
	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Len(t, intents, 1)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)
}

func TestRestoreIntent_PartialReadySubstratePreservesFinalizerBeforeRecovery(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.Kind = shared.OperationIntentRestore
	spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
	spec.SourceGeneration = 2
	spec.Items[0].Quantity = 2
	_, err = store.BeginOperationIntent(spec)
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

	err = b.preflightOperationIntentRecovery(context.Background())
	require.ErrorContains(t, err, "partial substrate")
	record, err := retentions.Get(spec.SourceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, record, "preflight must preserve the source finalizer")
	assert.Equal(t, shared.RetentionStatusRestoring, record.Status)
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1)
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestStartPreflightsPartialRestoreIntentBeforeRetentionMutation(t *testing.T) {
	var volumeMutations int
	specStorageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	spec := dockerOperationIntentSpec(t, specStorageID)
	spec.Kind = shared.OperationIntentRestore
	spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
	spec.SourceGeneration = 2
	spec.Items[0].Quantity = 2
	container := dockerIntentContainer(spec, "container-1", spec.Items[0].SKU, 0)
	mock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{container}, nil
		},
		InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
			copy := container
			return &copy, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	spec.BackendStorageID = b.storageIdentity
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error {
			volumeMutations++
			return nil
		},
		DestroyFn: func(context.Context, string) error {
			volumeMutations++
			return nil
		},
	}
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath})
	require.NoError(t, err)
	b.callbackStore = store
	b.operationIntents = store
	rebuildCallbackSender(b, testCallbackClient)
	t.Cleanup(func() {
		b.stopCancel()
		require.NoError(t, store.Close())
	})
	_, err = store.BeginOperationIntent(spec)
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
	})

	err = b.Start(context.Background())
	require.ErrorContains(t, err, "preflight interrupted operations")
	require.ErrorContains(t, err, "partial substrate")
	assert.Zero(t, volumeMutations,
		"startup must classify ambiguous restore evidence before reconcile/cleanup can rename or destroy it")
	record, err := retentions.Get(spec.SourceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, shared.RetentionStatusRestoring, record.Status)
	assert.Equal(t, spec.SourceGeneration, record.Generation)
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1)
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestRestoreIntent_ExactReadyFinalizesDuringReconciliation(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.Kind = shared.OperationIntentRestore
	spec.SourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174000"
	spec.SourceGeneration = 2
	_, err = store.BeginOperationIntent(spec)
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

	require.NoError(t, b.preflightOperationIntentRecovery(context.Background()))
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
	require.NoError(t, retentions.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "123e4567-e89b-42d3-a456-426614174000",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-123e4567-e89b-42d3-a456-426614174000-app-0"},
		Generation:          1,
	}))

	err := b.reconcileRetentions(context.Background())
	require.ErrorIs(t, err, assert.AnError)
}

func TestRecoverOperationIntent_ExactContainersRequireProjectionButNotCurrentSKUConfig(t *testing.T) {
	tests := []struct {
		name       string
		sku        string
		projection func(shared.OperationIntentSpec, string) map[string]*provision
		want       string
		wantOK     bool
	}{
		{
			name: "missing recovered projection",
			sku:  "docker-micro",
			projection: func(shared.OperationIntentSpec, string) map[string]*provision {
				return nil
			},
			want: "strict substrate does not have an exact recovered projection",
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
			store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := dockerOperationIntentSpec(t, storageID)
			spec.Items[0].SKU = tt.sku
			spec.ResourceProfiles[0].SKU = tt.sku
			_, err = store.BeginOperationIntent(spec)
			require.NoError(t, err)
			container := dockerIntentContainer(spec, "container-1", tt.sku, 0)
			b := newOperationIntentRecoveryBackend(
				t, store, storageID, []ContainerInfo{container}, tt.projection(spec, container.ContainerID),
			)

			err = b.recoverOperationIntents(context.Background(), nil)
			if tt.wantOK {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.want)
			}
			intents, listErr := store.ListOperationIntents()
			require.NoError(t, listErr)
			if tt.wantOK {
				assert.Empty(t, intents)
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
			store, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := dockerOperationIntentSpec(t, storageID)
			if tt.mutateSpec != nil {
				tt.mutateSpec(&spec)
			}
			_, err = store.BeginOperationIntent(spec)
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

			err = b.recoverOperationIntents(context.Background(), nil)
			if tt.wantOK {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.want)
			intents, listErr := store.ListOperationIntents()
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
	require.NoError(t, validateStrictManagedContainerLabels(
		"v0.13-migration-stack", "docker", v013MigrationStack,
	), "the raw inventory must surface the exact all-at-once v0.13 omission for whole-cohort rejection")
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
