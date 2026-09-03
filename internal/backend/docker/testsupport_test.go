package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/callbackurl"
)

// nominalDockerProviderUUID is the canonical provider identity used by broad
// Docker unit fixtures. Durable intent/release stores validate the same UUID
// shape as production, so a memorable non-UUID placeholder can no longer be
// used when a test upgrades from an ephemeral seam to the real journal.
const nominalDockerProviderUUID = "22222222-2222-4222-8222-222222222222"

const asyncTestResultTimeout = 5 * time.Second

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

func newTestRestoreCallbackAuthority(t *testing.T) (shared.OperationID, string, string) {
	t.Helper()
	operationID := shared.OperationID(uuid.NewString())
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID.String()
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	return operationID, callbackURL, lifecycleCallbackURL
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

// putRestoringRetention seeds a restore finalizer through the same durable
// transition used by production. Tests must not manufacture nominal restoring
// rows with Put: doing so bypasses the destination resource and callback
// authorities that make recovery safe after a restart.
func putRestoringRetention(
	t *testing.T,
	store *shared.RetentionStore,
	desired shared.RetentionEntry,
) *shared.RetentionEntry {
	t.Helper()
	require.Equal(t, shared.RetentionStatusRestoring, desired.Status)
	require.NotEmpty(t, desired.OriginalLeaseUUID)

	if desired.NewLeaseUUID == "" {
		desired.NewLeaseUUID = desired.OriginalLeaseUUID + "-restore"
	}
	if len(desired.Items) == 0 {
		desired.Items = []backend.LeaseItem{{
			SKU: "docker-micro", Quantity: 1, ServiceName: "app",
		}}
	}
	if len(desired.ResourceProfiles) == 0 {
		desired.ResourceProfiles = testResourceProfiles(t, desired.Items)
	}
	if desired.StackManifest == nil {
		desired.StackManifest = restoreStackManifest()
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
		destinationProfiles = testResourceProfiles(t, destinationItems)
	}

	operationID := desired.DestinationOperationID
	callbackURL := desired.DestinationCallbackURL
	lifecycleCallbackURL := desired.DestinationLifecycleCallbackURL
	if operationID == "" && callbackURL == "" && lifecycleCallbackURL == "" {
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
	active.DestinationOperationID = ""
	active.DestinationCallbackURL = ""
	active.DestinationLifecycleCallbackURL = ""
	if desired.Generation > 0 {
		active.Generation = desired.Generation - 1
	}
	require.NoError(t, store.Put(active))

	claimed, err := store.ClaimForRestoreWithAuthority(
		desired.OriginalLeaseUUID,
		desired.NewLeaseUUID,
		0,
		destinationItems,
		destinationProfiles,
		operationID,
		callbackURL,
		lifecycleCallbackURL,
	)
	require.NoError(t, err)
	if !desired.RestoringSince.IsZero() && !desired.RestoringSince.Equal(claimed.RestoringSince) {
		claimed.RestoringSince = desired.RestoringSince
		require.NoError(t, store.Put(*claimed))
	}
	return claimed
}

type testDockerStorageIdentity struct{}

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

func newBackendWithTestIdentity(cfg Config, logger *slog.Logger) (*Backend, error) {
	b, err := newBackend(context.Background(), cfg, logger, testDockerStorageIdentity{})
	if err == nil {
		b.operationIntents = noopOperationIntentJournal{}
	}
	return b, err
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
	store *shared.CallbackStore
}

func (noopOperationIntentJournal) ProbeOperationIntent(
	shared.OperationIntentProbe,
) (shared.OperationIntentAdmissionDisposition, error) {
	return shared.OperationIntentAdmissionNone, nil
}

func (j noopOperationIntentJournal) BeginOperationIntent(
	spec shared.OperationIntentSpec,
) (shared.OperationIntentAdmission, error) {
	if j.store == nil {
		return shared.OperationIntentAdmission{}, errors.New(
			"nominal operation-intent fixture has no claim-minting store",
		)
	}
	callbackURL, err := url.Parse(spec.CallbackURL)
	if err != nil {
		return shared.OperationIntentAdmission{}, err
	}
	callbackQuery, err := url.ParseQuery(callbackURL.RawQuery)
	if err != nil {
		return shared.OperationIntentAdmission{}, err
	}
	if callbackQuery.Has(backend.CallbackOperationIDQueryParameter) {
		return shared.OperationIntentAdmission{}, errors.New(
			"nominal operation-intent fixture cannot replace typed operation authority",
		)
	}
	if spec.LifecycleCallbackURL != "" {
		lifecycleURL, parseErr := url.Parse(spec.LifecycleCallbackURL)
		if parseErr != nil {
			return shared.OperationIntentAdmission{}, parseErr
		}
		lifecycleQuery, parseErr := url.ParseQuery(lifecycleURL.RawQuery)
		if parseErr != nil {
			return shared.OperationIntentAdmission{}, parseErr
		}
		if lifecycleQuery.Has(backend.CallbackLifecycleIDQueryParameter) {
			return shared.OperationIntentAdmission{}, errors.New(
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
	return j.store.BeginOperationIntent(spec)
}

func (j noopOperationIntentJournal) ListOperationIntents() ([]shared.OperationIntentClaim, error) {
	if j.store == nil {
		return nil, nil
	}
	return j.store.ListOperationIntents()
}

func (noopOperationIntentJournal) ResolveOperationIntent(
	shared.OperationIntentClaim,
	backend.CallbackStatus,
	string,
) (shared.CallbackEntry, error) {
	return shared.CallbackEntry{}, nil
}

// durableTestOperationIntentJournal delegates to the production bbolt store
// while supplying the physical-storage identity omitted by isolated Backend
// fixtures. Keeping Backend.storageIdentity invalid intentionally preserves the
// package-test mutation-admission bypass; the durable journal still constructs
// and verifies real opaque operation claims.
type durableTestOperationIntentJournal struct {
	store     *shared.CallbackStore
	storageID backendidentity.ID
}

func (j durableTestOperationIntentJournal) ProbeOperationIntent(
	probe shared.OperationIntentProbe,
) (shared.OperationIntentAdmissionDisposition, error) {
	if !probe.BackendStorageID.Valid() {
		probe.BackendStorageID = j.storageID
	}
	return j.store.ProbeOperationIntent(probe)
}

func (j durableTestOperationIntentJournal) BeginOperationIntent(
	spec shared.OperationIntentSpec,
) (shared.OperationIntentAdmission, error) {
	if !spec.BackendStorageID.Valid() {
		spec.BackendStorageID = j.storageID
	}
	return j.store.BeginOperationIntent(spec)
}

func (j durableTestOperationIntentJournal) ListOperationIntents() ([]shared.OperationIntentClaim, error) {
	return j.store.ListOperationIntents()
}

func (j durableTestOperationIntentJournal) ResolveOperationIntent(
	claim shared.OperationIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) (shared.CallbackEntry, error) {
	return j.store.ResolveOperationIntent(claim, status, errMsg)
}

// bindTestStorageIdentity seals a stateless test Backend so Start tests can
// exercise the phase they name rather than failing at the production-only
// explicit lineage precondition first.
func bindTestStorageIdentity(t *testing.T, b *Backend, dockerClient *mockDockerClient) {
	t.Helper()
	const daemonID = "test-daemon"
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	b.cfg.CallbackDBPath = dbPath
	for name, profile := range b.cfg.SKUProfiles {
		profile.DiskMB = 0
		b.cfg.SKUProfiles[name] = profile
	}
	dockerClient.DaemonInfoFn = func(context.Context) (DaemonSecurityInfo, error) {
		return DaemonSecurityInfo{SystemID: daemonID}, nil
	}
	id, err := initializeTestMarkerPair(
		dbPath+".storage-identity.json",
		dbPath+".storage-identity-anchor.json",
		b.cfg.Name,
		daemonID,
	)
	require.NoError(t, err)
	b.storageIdentity = id
	b.storageVerifier = testDockerRuntimeStorageVerifier{
		id: id,
		verify: func(ctx context.Context) error {
			info, err := dockerClient.DaemonInfo(ctx)
			if err != nil {
				return err
			}
			return backendidentity.VerifyMarkerPair(
				dbPath+".storage-identity.json",
				dbPath+".storage-identity-anchor.json",
				b.cfg.Name,
				info.SystemID,
				id,
			)
		},
	}
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

// handleContainerDeath synchronously dispatches a container death to the
// owning lease's actor and waits for processing to complete. Exists as a
// test helper so direct-call unit tests can keep their synchronous
// assertion style; production code routes die events via
// b.routeToLease(uuid, leasesm.ContainerDiedMsg{...}).
func (b *Backend) handleContainerDeath(containerID string) {
	leaseUUID, found := b.findLeaseByContainerID(containerID)
	if !found {
		return
	}
	done := make(chan struct{})
	if !b.routeToLease(leaseUUID, leasesm.ContainerDiedMsg{ContainerID: containerID, Done: done}) {
		return
	}
	select {
	case <-done:
	case <-b.stopCtx.Done():
	}
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
	composeUpErr           error  // returned by composeExecutor.Up if non-nil
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
	Store    *shared.ReleaseStore
	releases map[string][]byte // leaseUUID → manifest payload to pre-seed
}

// Seed flushes the test-side `releases` map into the backing release store
// as "active" entries dated now. Call this after populating `releases` and
// before invoking the production code under test.
func (f *fakeReleaseStore) Seed(t *testing.T) {
	t.Helper()
	for uuid, data := range f.releases {
		require.NoError(t, f.Store.Append(uuid, shared.Release{
			Manifest:  data,
			Status:    "active",
			CreatedAt: time.Now(),
		}))
	}
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

	dbPath := filepath.Join(t.TempDir(), "releases.db")
	relStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = relStore.Close() })
	fakeRel := &fakeReleaseStore{Store: relStore, releases: make(map[string][]byte)}

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
	b.releaseStore = relStore
	// Migration tests assume managed volume sources live under this root
	// — matches the fixture mounts used by migrate_test.go.
	b.cfg.VolumeDataPath = "/var/lib/fred/volumes"
	// MigrationReadyTimeout must be short in tests so verifyStartup's
	// no-healthcheck path (fixed wait + inspect) doesn't bloat suite
	// runtime. StartupVerifyDuration covers the same bound at the
	// per-poll level.
	b.cfg.MigrationReadyTimeout = 500 * time.Millisecond
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	// MigrationGracePeriod: keep short so the background -prev cleanup
	// goroutine doesn't outlive t.Cleanup teardown.
	b.cfg.MigrationGracePeriod = 100 * time.Millisecond

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

// names returns the volumes destroyed so far, in call order.
func (v *volumeSet) names() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.destroyed...)
}

// manager wires the set into a mockVolumeManager.
func (v *volumeSet) manager() *mockVolumeManager {
	return &mockVolumeManager{ListFn: v.list, DestroyFn: v.destroy}
}

// rollbackRestoreAdoption preserves the former phase-level fixture seam for
// tests that exercise physical/quota handback independently of operation-intent
// settlement. Production callers must choose one of the accepted/unaccepted
// wrappers, which makes the settlement owner explicit.
func (b *Backend) rollbackRestoreAdoption(
	ctx context.Context,
	leaseUUID string,
	allocatedIDs []string,
	rec *shared.RetentionEntry,
	dropProvision bool,
	logger *slog.Logger,
) bool {
	resourceProfiles, prepared := b.prepareRestoreAdoptionRollback(
		ctx, leaseUUID, rec, dropProvision, logger,
	)
	if !prepared {
		return false
	}
	return b.completeRestoreAdoptionRollback(
		leaseUUID, allocatedIDs, rec, resourceProfiles, dropProvision, logger,
	)
}
