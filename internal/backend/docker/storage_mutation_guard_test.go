package docker

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

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
	b.mutations = storageMutationAdapters{backend: b}

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
	b.mutations = storageMutationAdapters{backend: b}

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
	b.mutations = storageMutationAdapters{backend: b}

	err = b.mutationAdapter().composeUp(context.Background(), &composetypes.Project{}, composeUpOpts{})
	require.NoError(t, removeErr)
	require.Error(t, err)
	assert.ErrorIs(t, err, mutationErr, "the raw mutation cause must remain inspectable")
	assert.ErrorIs(t, err, backendidentity.ErrIdentityDrift, "the postcheck cause must remain inspectable")
	assert.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous,
		"a failed postcheck must expose the typed ambiguity cause")
}

func TestStorageMutationGuard_CanceledPostcheckLatchesAmbiguity(t *testing.T) {
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
	b.mutations = storageMutationAdapters{backend: b}

	err = b.mutationAdapter().composeUp(callerCtx, &composetypes.Project{}, composeUpOpts{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	select {
	case <-b.stopCtx.Done():
	default:
		t.Fatal("a canceled postcheck did not latch the backend lifetime")
	}
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
	b.mutations = storageMutationAdapters{backend: b}

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
	b.mutations = storageMutationAdapters{backend: b}

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

// TestStorageMutationGuard_IsTheOnlyRawCapabilityCaller is a compile-adjacent
// architectural invariant. Interfaces make Destroy unavailable to most code,
// but Docker and Compose cannot express a read/write split without extensive
// upstream adapters. The AST check prevents a future call site from silently
// bypassing the last-moment lineage attestation.
func TestStorageMutationGuard_IsTheOnlyRawCapabilityCaller(t *testing.T) {
	t.Parallel()

	mutators := map[string]map[string]bool{
		"docker": {
			"PullImage": true, "ResolveImageUser": true,
			"CreateContainer": true, "StartContainer": true,
			"StopContainer": true, "RenameContainer": true, "RemoveContainer": true,
			"EnsureTenantNetwork": true, "RemoveTenantNetworkIfEmpty": true,
			"DetectVolumeOwner": true, "DetectWritablePaths": true, "ExtractImageContent": true,
		},
		"compose": {"Up": true, "Down": true},
		"volumes": {"Create": true, "EnsureQuota": true, "RenameVolume": true},
	}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "storage_mutation_guard.go" {
			continue
		}
		if name == "volume_lock.go" {
			source, readErr := os.ReadFile(name)
			require.NoError(t, readErr)
			assert.Contains(t, string(source), ".mutationAdapter().createVolume(",
				"the ownership/striping choke point must use the pre/post-attested volume capability")
		}
		if name == "volume_destroy.go" {
			source, readErr := os.ReadFile(name)
			require.NoError(t, readErr)
			assert.Contains(t, string(source), ".mutationAdapter().destroyVolume(",
				"the ownership/striping choke point must use the pre/post-attested volume capability")
		}
		file, parseErr := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, parseErr, name)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if selector.Sel.Name == "Destroy" {
				pos := fset.Position(selector.Pos())
				t.Errorf("raw Destroy mutation outside storage guard at %s", pos)
				return true
			}
			capability, ok := selector.X.(*ast.SelectorExpr)
			if !ok || !mutators[capability.Sel.Name][selector.Sel.Name] {
				return true
			}
			pos := fset.Position(selector.Pos())
			t.Errorf("raw %s.%s mutation outside storage guard at %s", capability.Sel.Name, selector.Sel.Name, pos)
			return true
		})
	}
}

// TestStorageMutationGuard_RawMutatorsArePostAttested pins the second half of
// the choke-point contract. Keeping raw calls in one file is insufficient if a
// future adapter returns before re-attesting the storage lineage: a successful
// call during a root replacement would again look definitive to its caller.
func TestStorageMutationGuard_RawMutatorsArePostAttested(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "storage_mutation_guard.go", nil, 0)
	require.NoError(t, err)

	mutators := map[string]map[string]bool{
		"docker": {
			"PullImage": true, "ResolveImageUser": true,
			"StopContainer": true, "RenameContainer": true, "RemoveContainer": true,
			"EnsureTenantNetwork": true, "RemoveTenantNetworkIfEmpty": true,
			"DetectVolumeOwner": true, "DetectWritablePaths": true, "ExtractImageContent": true,
		},
		"compose": {"Up": true, "Down": true},
		"volumes": {"Create": true, "EnsureQuota": true, "RenameVolume": true},
	}
	checked := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil {
			continue
		}
		var rawMutation, postcheck bool
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch called := call.Fun.(type) {
			case *ast.Ident:
				if called.Name == "buildStatefulVolumeBindsContext" {
					rawMutation = true
				}
			case *ast.SelectorExpr:
				if called.Sel.Name == "completeMutation" {
					postcheck = true
				}
				if called.Sel.Name == "Destroy" {
					rawMutation = true
				}
				if owner, ok := called.X.(*ast.Ident); ok && owner.Name == "os" && called.Sel.Name == "RemoveAll" {
					rawMutation = true
				}
				if capability, ok := called.X.(*ast.SelectorExpr); ok && mutators[capability.Sel.Name][called.Sel.Name] {
					rawMutation = true
				}
			}
			return true
		})
		if !rawMutation {
			continue
		}
		checked++
		assert.True(t, postcheck, "%s calls a raw mutator without completeMutation", fn.Name.Name)
	}
	assert.Greater(t, checked, 0, "the invariant must discover the guarded raw mutation methods")
}
