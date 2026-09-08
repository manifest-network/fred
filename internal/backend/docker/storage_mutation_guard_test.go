package docker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestBackendStorageAuthorityLifetimePublishesFirstFailureAndCancels(t *testing.T) {
	t.Parallel()

	stopCtx, stopCancel, failures, gate, err := newBackendStorageAuthorityLifetime()
	require.NoError(t, err)
	t.Cleanup(stopCancel)
	require.NotNil(t, failures)
	assert.Equal(t, 1, cap(failures), "the durability-boundary hook must never wait for the daemon")
	b := &Backend{terminalStorageAuthorityFailure: failures}
	assert.Equal(t, failures, b.TerminalStorageAuthorityFailure())

	first := fmt.Errorf("%w: injected volume cleanup evidence", backendidentity.ErrMutationOutcomeAmbiguous)
	require.ErrorIs(t, gate.Latch(first), first)
	select {
	case got := <-b.TerminalStorageAuthorityFailure():
		require.ErrorIs(t, got, first)
	case <-time.After(time.Second):
		t.Fatal("terminal storage-authority failure was not published")
	}
	select {
	case <-stopCtx.Done():
	default:
		t.Fatal("terminal storage-authority failure did not cancel the backend lifetime")
	}

	second := fmt.Errorf("%w: later independent failure", backendidentity.ErrIdentityDrift)
	require.ErrorIs(t, gate.Latch(second), first, "the gate must retain its exact first cause")
	select {
	case extra := <-b.TerminalStorageAuthorityFailure():
		t.Fatalf("terminal storage-authority channel published more than once: %v", extra)
	default:
	}
	assert.Nil(t, (*Backend)(nil).TerminalStorageAuthorityFailure())
	assert.Nil(t, (&Backend{}).TerminalStorageAuthorityFailure())
}

// TestStorageMutationGuard_ClosesFrontDoorMutationTOCTOU models the exact
// queueing window the adapter exists for: the request/front-door check passes,
// the sealed marker disappears while work waits, and only then does the actor
// reach Compose. The second attestation must fail-stop without invoking Compose.
func TestStorageMutationGuard_ClosesFrontDoorMutationTOCTOU(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{Name: "docker-a", CallbackDBPath: filepath.Join(dir, "callbacks.db")}
	markerPath := cfg.CallbackDBPath + ".storage-identity.json"
	anchorPath := cfg.CallbackDBPath + ".storage-identity-anchor.json"
	const daemonID = "daemon-a"
	id, err := initializeTestMarkerPair(markerPath, anchorPath, cfg.Name, daemonID)
	require.NoError(t, err)

	composeCalls := 0
	dockerClient := &mockDockerClient{DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
		return DaemonSecurityInfo{SystemID: daemonID}, nil
	}}
	composeClient := &mockComposeExecutor{UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
		composeCalls++
		return nil
	}}
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	b := &Backend{
		cfg:             cfg,
		docker:          dockerClient,
		compose:         composeClient,
		volumes:         &noopVolumeManager{},
		storageIdentity: id,
		stopCtx:         stopCtx,
		stopCancel:      stop,
	}
	installMarkerMutationTestVerifier(t, b, markerPath, anchorPath, daemonID)
	installTestStorageMutationAdapters(b)

	require.NoError(t, b.requireStorageIdentity(context.Background()), "front-door attestation")
	require.NoError(t, os.Remove(markerPath), "simulate storage replacement after request admission")

	err = b.mutationAdapter().composeUp(context.Background(), &composetypes.Project{}, composeUpOpts{})
	require.Error(t, err)
	assert.ErrorIs(t, err, backendidentity.ErrIdentityDrift)
	assert.Equal(t, 0, composeCalls, "Compose must not receive a mutation after lineage drift")
	select {
	case <-b.stopCtx.Done():
	default:
		t.Fatal("permanent identity drift did not cancel the backend lifetime")
	}
}

func TestParseStoragePathComponent_RejectsTraversalSyntax(t *testing.T) {
	t.Parallel()

	valid, err := parseStoragePathComponent("fred-550e8400-e29b-41d4-a716-446655440000-app-0")
	require.NoError(t, err)
	assert.Equal(t, storagePathComponent("fred-550e8400-e29b-41d4-a716-446655440000-app-0"), valid)

	for _, value := range []string{
		"",
		".",
		"..",
		"../outside",
		"fred-volume/../../outside",
		`fred-volume\..\outside`,
		"fred..volume",
		"/absolute",
		"fred-volume\x00",
	} {
		t.Run(fmt.Sprintf("%q", value), func(t *testing.T) {
			t.Parallel()
			_, parseErr := parseStoragePathComponent(value)
			require.Error(t, parseErr)
		})
	}
}

func TestParseManagedVolumeName_RequiresCanonicalIdentity(t *testing.T) {
	t.Parallel()

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	for _, value := range []string{
		"fred-" + leaseUUID + "-app-0",
		"fred-" + leaseUUID + "-api-worker-12",
		"fred-retained-" + leaseUUID + "-app-0",
		"fred-" + leaseUUID + "-0", // stopped v0.13 upgrade
		"fred-retained-" + leaseUUID + "-0",
	} {
		value := value
		t.Run("accept_"+value, func(t *testing.T) {
			t.Parallel()
			name, err := parseManagedVolumeName(value)
			require.NoError(t, err)
			assert.Equal(t, value, name.value())
		})
	}

	for _, value := range []string{
		"other-" + leaseUUID + "-app-0",
		"fred-550E8400-E29B-41D4-A716-446655440000-app-0",
		"fred-00000000-0000-0000-0000-000000000000-app-0",
		"fred-" + leaseUUID,
		"fred-" + leaseUUID + "-app-01",
		"fred-" + leaseUUID + "-app--1",
		"fred-" + leaseUUID + "-App-0",
		"fred-" + leaseUUID + "-app.example-0",
		"fred-" + leaseUUID + "-/../../outside-0",
		`fred-` + leaseUUID + `-app\..\outside-0`,
	} {
		value := value
		t.Run("reject_"+value, func(t *testing.T) {
			t.Parallel()
			_, err := parseManagedVolumeName(value)
			require.Error(t, err)
		})
	}
}

func TestRemoveManagedVolumeSubtree_ConfinesRecursiveDeletion(t *testing.T) {
	t.Parallel()

	volumeRoot := t.TempDir()
	volumeName, err := parseManagedVolumeName("fred-550e8400-e29b-41d4-a716-446655440000-app-0")
	require.NoError(t, err)
	wpName, err := parseStoragePathComponent(writablePathSubdir)
	require.NoError(t, err)

	volumePath := volumeName.hostPath(volumeRoot)
	wpPath := filepath.Join(volumePath, writablePathSubdir)
	require.NoError(t, os.MkdirAll(wpPath, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(wpPath, "tenant-data"), []byte("x"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath, "keep"), []byte("x"), 0o600))

	require.NoError(t, removeManagedVolumeSubtree(volumeRoot, volumeName, wpName))
	assert.NoDirExists(t, wpPath)
	assert.FileExists(t, filepath.Join(volumePath, "keep"), "only the fixed writable-path subtree may be removed")
}

func TestRemoveManagedVolumeSubtree_RejectsEscapingSymlink(t *testing.T) {
	t.Parallel()

	volumeRoot := t.TempDir()
	outside := t.TempDir()
	volumeName, err := parseManagedVolumeName("fred-550e8400-e29b-41d4-a716-446655440000-app-0")
	require.NoError(t, err)
	wpName, err := parseStoragePathComponent(writablePathSubdir)
	require.NoError(t, err)

	outsideWP := filepath.Join(outside, writablePathSubdir)
	require.NoError(t, os.Mkdir(outsideWP, 0o700))
	victim := filepath.Join(outsideWP, "victim")
	require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))
	require.NoError(t, os.Symlink(outside, volumeName.hostPath(volumeRoot)))

	require.Error(t, removeManagedVolumeSubtree(volumeRoot, volumeName, wpName))
	assert.FileExists(t, victim, "descriptor-relative deletion must not follow a symlink outside the storage root")
}

func TestRemoveManagedVolumeSubtree_RejectsCrossVolumeSymlink(t *testing.T) {
	t.Parallel()

	volumeRoot := t.TempDir()
	source, err := parseManagedVolumeName("fred-550e8400-e29b-41d4-a716-446655440000-app-0")
	require.NoError(t, err)
	target, err := parseManagedVolumeName("fred-550e8400-e29b-41d4-a716-446655440000-app-1")
	require.NoError(t, err)
	wpName, err := parseStoragePathComponent(writablePathSubdir)
	require.NoError(t, err)

	targetWP := filepath.Join(target.hostPath(volumeRoot), writablePathSubdir)
	require.NoError(t, os.MkdirAll(targetWP, 0o700))
	victim := filepath.Join(targetWP, "tenant-data")
	require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))
	require.NoError(t, os.Symlink(target.value(), source.hostPath(volumeRoot)))

	require.Error(t, removeManagedVolumeSubtree(volumeRoot, source, wpName))
	assert.FileExists(t, victim, "one managed volume must never redirect cleanup into another volume")
}

func TestWritablePathVolumeComponent_RequiresExactManagedSubtree(t *testing.T) {
	t.Parallel()

	volumeRoot := t.TempDir()
	const name = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	validPath := filepath.Join(volumeRoot, name, writablePathSubdir)
	got, err := writablePathVolumeComponent(volumeRoot, validPath)
	require.NoError(t, err)
	assert.Equal(t, managedVolumeName(name), got)

	outside := filepath.Join(filepath.Dir(volumeRoot), "outside", writablePathSubdir)
	for _, candidate := range []string{
		outside,
		filepath.Join(volumeRoot, "unmanaged", writablePathSubdir),
		filepath.Join(volumeRoot, name, "nested", writablePathSubdir),
		filepath.Join(volumeRoot, name, "not-writable-path"),
	} {
		_, parseErr := writablePathVolumeComponent(volumeRoot, candidate)
		require.Error(t, parseErr, "candidate %q must be rejected", candidate)
	}
}

func TestStorageMutationGuard_PostcheckRejectsSuccessAfterIdentityDrift(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{Name: "docker-a", CallbackDBPath: filepath.Join(dir, "callbacks.db")}
	markerPath := cfg.CallbackDBPath + ".storage-identity.json"
	anchorPath := cfg.CallbackDBPath + ".storage-identity-anchor.json"
	const daemonID = "daemon-a"
	id, err := initializeTestMarkerPair(markerPath, anchorPath, cfg.Name, daemonID)
	require.NoError(t, err)

	var removeErr error
	dockerClient := &mockDockerClient{DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
		return DaemonSecurityInfo{SystemID: daemonID}, nil
	}}
	composeClient := &mockComposeExecutor{UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
		removeErr = os.Remove(markerPath)
		return nil
	}}
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	b := &Backend{
		cfg: cfg, docker: dockerClient, compose: composeClient,
		volumes: &noopVolumeManager{}, storageIdentity: id,
		stopCtx: stopCtx, stopCancel: stop,
	}
	installMarkerMutationTestVerifier(t, b, markerPath, anchorPath, daemonID)
	installTestStorageMutationAdapters(b)

	err = b.mutationAdapter().composeUp(context.Background(), &composetypes.Project{}, composeUpOpts{})
	require.NoError(t, removeErr)
	require.Error(t, err)
	assert.ErrorIs(t, err, backendidentity.ErrIdentityDrift)
	assert.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	assert.Contains(t, err.Error(), "post-mutation storage verification")
}

func TestStorageMutationGuard_PostcheckJoinsMutationAndIdentityErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{Name: "docker-a", CallbackDBPath: filepath.Join(dir, "callbacks.db")}
	markerPath := cfg.CallbackDBPath + ".storage-identity.json"
	anchorPath := cfg.CallbackDBPath + ".storage-identity-anchor.json"
	const daemonID = "daemon-a"
	id, err := initializeTestMarkerPair(markerPath, anchorPath, cfg.Name, daemonID)
	require.NoError(t, err)

	mutationErr := errors.New("compose transport failed")
	var removeErr error
	dockerClient := &mockDockerClient{DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
		return DaemonSecurityInfo{SystemID: daemonID}, nil
	}}
	composeClient := &mockComposeExecutor{UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
		removeErr = os.Remove(markerPath)
		return mutationErr
	}}
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	b := &Backend{
		cfg: cfg, docker: dockerClient, compose: composeClient,
		volumes: &noopVolumeManager{}, storageIdentity: id,
		stopCtx: stopCtx, stopCancel: stop,
	}
	installMarkerMutationTestVerifier(t, b, markerPath, anchorPath, daemonID)
	installTestStorageMutationAdapters(b)

	err = b.mutationAdapter().composeUp(context.Background(), &composetypes.Project{}, composeUpOpts{})
	require.NoError(t, removeErr)
	require.Error(t, err)
	assert.ErrorIs(t, err, mutationErr, "the raw mutation cause must remain inspectable")
	assert.ErrorIs(t, err, backendidentity.ErrIdentityDrift, "the postcheck cause must remain inspectable")
	assert.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous,
		"a failed postcheck must expose the typed ambiguity cause")
}

func TestStorageMutationGuard_ManagerAmbiguityLatchesBackendAfterSuccessfulPostcheck(t *testing.T) {
	t.Parallel()

	mutationCause := errors.New("xfs parent directory sync failed")
	managerErr := fmt.Errorf("%w: %w", backendidentity.ErrMutationOutcomeAmbiguous, mutationCause)
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	storeAuthorityGate, err := backendidentity.NewStorageAuthorityGate(func(error) { stop() })
	require.NoError(t, err)
	b := &Backend{
		volumes: &mockVolumeManager{CreateFn: func(context.Context, string, int64) (string, bool, error) {
			return "", false, managerErr
		}},
		stopCtx: stopCtx, stopCancel: stop,
		storeAuthorityGate: storeAuthorityGate,
	}
	installMutationTestVerifier(t, b, func(context.Context) error { return nil })
	installTestStorageMutationAdapters(b)

	_, _, err = b.mutationAdapter().createVolume(context.Background(),
		"fred-550e8400-e29b-41d4-a716-446655440000-app-0", 100)
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	require.ErrorIs(t, err, mutationCause)
	require.ErrorIs(t, b.terminalStorageAuthorityError(), backendidentity.ErrMutationOutcomeAmbiguous)
	select {
	case <-b.stopCtx.Done():
	default:
		t.Fatal("typed manager ambiguity did not stop the backend lifetime")
	}
}

func TestStorageMutationGuard_VolumeRecoveryPendingLatchesBackendAfterSuccessfulPostcheck(t *testing.T) {
	t.Parallel()

	mutationCause := errors.New("xfs delete-stage still has open-unlinked inodes")
	managerErr := fmt.Errorf("%w: %w", ErrVolumeMutationRecoveryPending, mutationCause)
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	storeAuthorityGate, err := backendidentity.NewStorageAuthorityGate(func(error) { stop() })
	require.NoError(t, err)
	b := &Backend{
		volumes: &mockVolumeManager{DestroyFn: func(context.Context, string) error {
			return managerErr
		}},
		stopCtx: stopCtx, stopCancel: stop,
		storeAuthorityGate: storeAuthorityGate,
	}
	installMutationTestVerifier(t, b, func(context.Context) error { return nil })
	installTestStorageMutationAdapters(b)

	sink := b.volumes.(volumeDestroyer)
	err = b.mutationAdapter().destroyVolume(context.Background(), sink,
		"fred-550e8400-e29b-41d4-a716-446655440000-app-0")
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	require.ErrorIs(t, err, mutationCause)
	require.ErrorIs(t, b.terminalStorageAuthorityError(), ErrVolumeMutationRecoveryPending)
	select {
	case <-b.stopCtx.Done():
	default:
		t.Fatal("typed volume recovery obligation did not stop the backend lifetime")
	}
}

func TestStorageMutationGuard_ManagerRenameAmbiguityLatchesBackendAfterSuccessfulPostcheck(t *testing.T) {
	t.Parallel()

	mutationCause := errors.New("xfs rename parent directory sync failed")
	managerErr := fmt.Errorf("%w: %w", backendidentity.ErrMutationOutcomeAmbiguous, mutationCause)
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	storeAuthorityGate, err := backendidentity.NewStorageAuthorityGate(func(error) { stop() })
	require.NoError(t, err)
	b := &Backend{
		volumes: &mockVolumeManager{RenameVolumeFn: func(string, string) error {
			return managerErr
		}},
		stopCtx: stopCtx, stopCancel: stop,
		storeAuthorityGate: storeAuthorityGate,
	}
	installMutationTestVerifier(t, b, func(context.Context) error { return nil })
	installTestStorageMutationAdapters(b)

	err = b.mutationAdapter().renameVolume(
		context.Background(),
		"fred-550e8400-e29b-41d4-a716-446655440000-0",
		"fred-550e8400-e29b-41d4-a716-446655440000-app-0",
	)
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	require.ErrorIs(t, err, mutationCause)
	require.ErrorIs(t, b.terminalStorageAuthorityError(), backendidentity.ErrMutationOutcomeAmbiguous)
	select {
	case <-b.stopCtx.Done():
	default:
		t.Fatal("typed XFS rename ambiguity did not stop the backend lifetime")
	}
}

func TestStorageMutationGuard_CanceledEffectReceivesIndependentPostcheck(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{Name: "docker-a", CallbackDBPath: filepath.Join(dir, "callbacks.db")}
	markerPath := cfg.CallbackDBPath + ".storage-identity.json"
	anchorPath := cfg.CallbackDBPath + ".storage-identity-anchor.json"
	const daemonID = "daemon-a"
	id, err := initializeTestMarkerPair(markerPath, anchorPath, cfg.Name, daemonID)
	require.NoError(t, err)

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	dockerClient := &mockDockerClient{DaemonInfoFn: func(ctx context.Context) (DaemonSecurityInfo, error) {
		if err := ctx.Err(); err != nil {
			return DaemonSecurityInfo{}, err
		}
		return DaemonSecurityInfo{SystemID: daemonID}, nil
	}}
	composeClient := &mockComposeExecutor{UpFn: func(ctx context.Context, _ *composetypes.Project, _ composeUpOpts) error {
		cancelCaller()
		<-ctx.Done()
		return ctx.Err()
	}}
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	b := &Backend{
		cfg: cfg, docker: dockerClient, compose: composeClient,
		volumes: &noopVolumeManager{}, storageIdentity: id,
		stopCtx: stopCtx, stopCancel: stop,
	}
	installMutationTestVerifier(t, b, func(ctx context.Context) error { return ctx.Err() })
	installTestStorageMutationAdapters(b)

	err = b.mutationAdapter().composeUp(callerCtx, &composetypes.Project{}, composeUpOpts{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous,
		"the raw call error cannot manufacture a storage-authority failure")
	require.NoError(t, b.terminalStorageAuthorityError())
	require.NoError(t, b.stopCtx.Err())
}

func TestStorageMutationGuard_IndependentPostcheckFailureStillLatches(t *testing.T) {
	stopCtx, stop := context.WithCancel(t.Context())
	t.Cleanup(stop)
	id, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	b := &Backend{storageIdentity: id, stopCtx: stopCtx, stopCancel: stop,
		recoveryDockerReadTimeout: 10 * time.Millisecond}
	installMutationTestVerifier(t, b, func(ctx context.Context) error {
		require.NoError(t, ctx.Err(), "postcheck starts with an independent budget")
		<-ctx.Done()
		return ctx.Err()
	})
	callerCtx, cancel := context.WithCancel(t.Context())
	cancel()
	err = b.completeStorageMutation(callerCtx, "test completed effect", context.Canceled)
	require.ErrorIs(t, err, context.Canceled, "the original effect error remains visible")
	require.ErrorIs(t, err, context.DeadlineExceeded, "the independent postcheck has a finite bound")
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	require.ErrorIs(t, b.terminalStorageAuthorityError(), backendidentity.ErrMutationOutcomeAmbiguous)
	require.ErrorIs(t, b.stopCtx.Err(), context.Canceled)
}

func TestStorageMutationGuard_BackendStopPreventsMutation(t *testing.T) {
	t.Parallel()

	composeCalls := 0
	stopCtx, stop := context.WithCancel(context.Background())
	stop()
	b := &Backend{
		compose: &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
			composeCalls++
			return nil
		}},
		stopCtx: stopCtx,
	}
	installMutationTestVerifier(t, b, nil)
	installTestStorageMutationAdapters(b)

	err := b.mutationAdapter().composeDown(context.Background(), "fred-lease", time.Second)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.Equal(t, 0, composeCalls)
}

func TestStorageMutationGuard_BackendStopCancelsInFlightMutation(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	stopCtx, stop := context.WithCancel(context.Background())
	b := &Backend{
		compose: &mockComposeExecutor{UpFn: func(ctx context.Context, _ *composetypes.Project, _ composeUpOpts) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}},
		stopCtx:    stopCtx,
		stopCancel: stop,
	}
	installMutationTestVerifier(t, b, func(ctx context.Context) error { return ctx.Err() })
	installTestStorageMutationAdapters(b)

	result := make(chan error, 1)
	go func() {
		result <- b.mutationAdapter().composeUp(context.Background(), &composetypes.Project{}, composeUpOpts{})
	}()
	<-started
	stop()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("backend stop did not cancel the in-flight substrate mutation")
	}
}

// installMutationTestVerifier gives Backend-literal tests an explicit typed
// verifier. Production has no zero-identity bypass; these focused tests must
// therefore say which verification behavior they are exercising instead of
// accidentally depending on an uninitialized Backend being mutation-capable.
func installMutationTestVerifier(
	t *testing.T,
	b *Backend,
	verify func(context.Context) error,
) {
	t.Helper()
	if b.storeAuthorityGate == nil {
		cancel := b.stopCancel
		gate, err := backendidentity.NewStorageAuthorityGate(func(error) {
			if cancel != nil {
				cancel()
			}
		})
		require.NoError(t, err)
		b.storeAuthorityGate = gate
	}
	if !b.storageIdentity.Valid() {
		id, err := backendidentity.Parse("550e8400-e29b-41d4-a716-446655440000")
		require.NoError(t, err)
		b.storageIdentity = id
	}
	b.storageVerifier = testDockerRuntimeStorageVerifier{
		id:     b.storageIdentity,
		verify: verify,
	}
}

func installMarkerMutationTestVerifier(
	t *testing.T,
	b *Backend,
	markerPath, anchorPath, daemonID string,
) {
	t.Helper()
	installMutationTestVerifier(t, b, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := backendidentity.VerifyMarkerPair(
			markerPath, anchorPath, b.cfg.Name, daemonID, b.storageIdentity,
		); err != nil {
			if b.stopCancel != nil {
				b.stopCancel()
			}
			return fmt.Errorf("%w: %w", backendidentity.ErrIdentityDrift, err)
		}
		return nil
	})
}

type panicOnAuthorizationReleaseContext struct {
	context.Context
	done <-chan struct{}
}

func (ctx panicOnAuthorizationReleaseContext) Done() <-chan struct{} { return ctx.done }

func (panicOnAuthorizationReleaseContext) AfterFunc(func()) func() bool {
	return func() bool { panic("release boom") }
}

func newBackgroundMutationBracketTestCoordinator(
	t *testing.T,
	b *Backend,
	volumes *mockVolumeManager,
) *backgroundMaintenanceCoordinator {
	t.Helper()
	docker := &mockDockerClient{}
	compose := &mockComposeExecutor{}
	b.docker = docker
	b.compose = compose
	b.volumes = volumes
	coordinator, err := newBackgroundMaintenanceCoordinator(
		b,
		newStorageMutationOperations(b, docker, compose, volumes),
	)
	require.NoError(t, err)
	return coordinator
}

func TestBackgroundMutationBracket_ActionPanicLatchesAmbiguityAndStillCompletes(t *testing.T) {
	t.Parallel()
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	b := &Backend{stopCtx: stopCtx, stopCancel: stop}
	verifyCalls := 0
	installMutationTestVerifier(t, b, func(context.Context) error {
		verifyCalls++
		return nil
	})
	coordinator := newBackgroundMutationBracketTestCoordinator(t, b, &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn: func(context.Context) error {
			panic("action boom")
		},
	})

	err := coordinator.recoverInterruptedVolumes(context.Background())
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	assert.ErrorContains(t, err, "action panicked")
	assert.Equal(t, 2, verifyCalls,
		"completion attestation must still run after the action panics")
	require.ErrorIs(t, b.terminalStorageAuthorityError(), backendidentity.ErrMutationOutcomeAmbiguous)
}

func TestBackgroundMutationBracket_CompletionPanicLatchesAmbiguityAndReleases(t *testing.T) {
	t.Parallel()
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	b := &Backend{stopCtx: stopCtx, stopCancel: stop}
	verifyCalls := 0
	installMutationTestVerifier(t, b, func(context.Context) error {
		verifyCalls++
		if verifyCalls == 2 {
			panic("completion boom")
		}
		return nil
	})
	actionCalled := false
	coordinator := newBackgroundMutationBracketTestCoordinator(t, b, &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn: func(context.Context) error {
			actionCalled = true
			return nil
		},
	})

	err := coordinator.recoverInterruptedVolumes(context.Background())
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	assert.ErrorContains(t, err, "completion attestation panicked")
	assert.True(t, actionCalled)
	require.ErrorIs(t, b.terminalStorageAuthorityError(), backendidentity.ErrMutationOutcomeAmbiguous)
}

func TestBackgroundMutationBracket_ReleasePanicLatchesAmbiguity(t *testing.T) {
	t.Parallel()
	baseCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	b := &Backend{
		stopCtx: panicOnAuthorizationReleaseContext{
			Context: baseCtx,
			done:    make(chan struct{}),
		},
		stopCancel: stop,
	}
	installMutationTestVerifier(t, b, func(context.Context) error { return nil })
	actionCalled := false
	coordinator := newBackgroundMutationBracketTestCoordinator(t, b, &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn: func(context.Context) error {
			actionCalled = true
			return nil
		},
	})

	err := coordinator.recoverInterruptedVolumes(context.Background())
	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	assert.ErrorContains(t, err, "authorization release panicked")
	assert.True(t, actionCalled)
	require.ErrorIs(t, b.terminalStorageAuthorityError(), backendidentity.ErrMutationOutcomeAmbiguous)
}

func TestBackgroundMutationBracket_OrdinaryFailureRemainsRetryable(t *testing.T) {
	t.Parallel()
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	b := &Backend{stopCtx: stopCtx, stopCancel: stop}
	installMutationTestVerifier(t, b, func(context.Context) error { return nil })
	want := errors.New("transient volume cleanup failure")
	coordinator := newBackgroundMutationBracketTestCoordinator(t, b, &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn: func(context.Context) error { return want },
	})

	err := coordinator.recoverInterruptedVolumes(context.Background())
	require.ErrorIs(t, err, want)
	assert.NotErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	assert.NoError(t, b.terminalStorageAuthorityError())
	select {
	case <-stopCtx.Done():
		t.Fatal("ordinary retryable background failure stopped the backend")
	default:
	}
}

func TestBackgroundMutationBracket_AuthorizationPanicRefusesWithoutLatching(t *testing.T) {
	t.Parallel()
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	b := &Backend{stopCtx: stopCtx, stopCancel: stop}
	installMutationTestVerifier(t, b, func(context.Context) error {
		panic("authorization boom")
	})
	actionCalled := false
	coordinator := newBackgroundMutationBracketTestCoordinator(t, b, &mockVolumeManager{
		RecoverInterruptedVolumeMutationsFn: func(context.Context) error {
			actionCalled = true
			return nil
		},
	})

	err := coordinator.recoverInterruptedVolumes(context.Background())
	assert.ErrorContains(t, err, "authorization panicked")
	assert.NotErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	assert.False(t, actionCalled)
	assert.NoError(t, b.terminalStorageAuthorityError())
}

func TestVolumeRootWatch_MissingPinnedRootIsPermanentDrift(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "volumes")
	require.NoError(t, os.Mkdir(root, 0o700))
	var watch volumeRootWatch
	require.NoError(t, watch.pin(root))
	require.NoError(t, os.Remove(root))

	err := watch.verify(root)
	require.Error(t, err)
	assert.ErrorIs(t, err, errVolumeRootIdentityDrift)
}
