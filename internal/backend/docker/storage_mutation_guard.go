package docker

// This file is the backend's substrate-mutation choke point.
//
// A request-level identity check is not sufficient: Docker work is commonly
// queued behind a lease actor and a mount or daemon can be replaced while the
// request waits. Every operation below therefore joins the backend lifetime and
// re-attests the configured storage lineage immediately before and after
// handing the mutation to Docker, Compose, or the volume manager. A failed
// postcheck makes the outcome ambiguous and latches the Backend for its
// remaining lifetime. Code outside this file must not call those mutating
// capabilities directly.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/fsidentity"
	"github.com/manifest-network/fred/internal/util"
)

// storageMutationOperations is the construction-only raw substrate. It is
// captured by settlement-bound builders and never retained by Backend or
// returned to a workflow.
type storageMutationOperations struct {
	docker  dockerMutationSink
	compose composeMutationSink
	volumes volumeMutationSink
	backend *Backend
}

// resolveBackgroundStorageStep preserves retryability for ordinary idempotent
// convergence failures, while making a broken call stack terminal once an
// effect may have reached the substrate. It accepts only the bracket result,
// never a writer or a target, so it cannot become another mutation facade.
func (b *Backend) resolveBackgroundStorageStep(
	operation string,
	result substratemutation.StepResult,
) error {
	if result.Kind() == substratemutation.Invalid {
		return fmt.Errorf("%s: background mutation bracket returned an invalid result", operation)
	}
	if result.EffectEntered() && result.Panicked() {
		return b.latchAmbiguousOperationOutcome(
			operation+" panic-safe background mutation bracket",
			result.Err(),
		)
	}
	return result.Err()
}

// newBackgroundMaintenanceCoordinator consumes the raw construction bundle and
// returns only complete, target-free workflows. perform and every per-target
// closure below are lexical construction details: Backend never retains either
// the raw sinks or a generic mutation facade.
func newBackgroundMaintenanceCoordinator(
	backend *Backend,
	ops storageMutationOperations,
) (*backgroundMaintenanceCoordinator, error) {
	if backend == nil || ops.backend != backend || util.IsNilInterface(ops.docker) ||
		util.IsNilInterface(ops.compose) || util.IsNilInterface(ops.volumes) {
		return nil, errors.New("background maintenance requires complete construction-bound substrate operations")
	}
	perform := func(
		ctx context.Context,
		operation string,
		action func(context.Context) error,
	) error {
		if action == nil {
			return fmt.Errorf("%s: background substrate action is unavailable", operation)
		}
		result := substratemutation.RunStep(
			ctx,
			operation,
			backend.authorizeStorageMutation,
			backend.completeStorageMutation,
			action,
		)
		return backend.resolveBackgroundStorageStep(operation, result)
	}

	removeContainer := backgroundContainerRemove(func(ctx context.Context, id string) error {
		return perform(ctx, "background remove container", func(ctx context.Context) error {
			return ops.docker.RemoveContainer(ctx, id)
		})
	})
	renameVolume := backgroundVolumeRename(func(ctx context.Context, oldName, newName string) error {
		return perform(ctx, "background rename volume", func(ctx context.Context) error {
			return ops.volumes.RenameVolume(ctx, oldName, newName)
		})
	})
	ensureVolumeQuota := backgroundVolumeQuota(func(ctx context.Context, id string, sizeMB int64) error {
		return perform(ctx, "background ensure volume quota", func(ctx context.Context) error {
			return ops.volumes.EnsureQuota(ctx, id, sizeMB)
		})
	})
	destroyVolumes := backgroundVolumeDestroyCapability{
		destroyFn: func(ctx context.Context, id string) error {
			return perform(ctx, "background destroy volume", func(ctx context.Context) error {
				return ops.volumes.Destroy(ctx, id)
			})
		},
	}
	teardown := backgroundTeardownCapability{
		downFn: func(ctx context.Context, leaseUUID string, timeout time.Duration) error {
			return perform(ctx, "background compose down", func(ctx context.Context) error {
				return ops.compose.Down(ctx, composeProjectName(leaseUUID), timeout)
			})
		},
		removeFn: removeContainer,
	}
	removeTenantNetwork := backgroundTenantNetworkRemove(func(ctx context.Context, tenant string) error {
		return perform(ctx, "background remove tenant network", func(ctx context.Context) error {
			return ops.docker.RemoveTenantNetworkIfEmpty(ctx, tenant)
		})
	})

	return &backgroundMaintenanceCoordinator{
		recoverInterruptedVolumesFn: func(ctx context.Context) error {
			return perform(ctx, "recover interrupted volume mutations", func(ctx context.Context) error {
				return ops.volumes.RecoverInterruptedVolumeMutations(ctx)
			})
		},
		recoverClosedLeasesFn: func(ctx context.Context) (map[string]struct{}, error) {
			return backend.recoverClosedLeaseSubstrateUsing(ctx, removeContainer)
		},
		reconcileRetentionsFn: func(ctx context.Context) error {
			return backend.reconcileRetentionsUsing(ctx, renameVolume, teardown, destroyVolumes, ensureVolumeQuota)
		},
		reconcileVolumeQuotasFn: func(ctx context.Context) error {
			return backend.reconcileVolumeQuotasUsing(ctx, ensureVolumeQuota)
		},
		cleanupOrphanedNetworksFn: func(ctx context.Context) {
			backend.cleanupOrphanedNetworksUsing(ctx, removeTenantNetwork)
		},
		reapExpiredRetentionsFn: func(ctx context.Context) (int, error) {
			return backend.reapExpiredRetentionsUsing(ctx, destroyVolumes)
		},
		runRetentionSweepFn: func(ctx context.Context) error {
			return backend.runRetentionSweepUsing(ctx, renameVolume, teardown, destroyVolumes, ensureVolumeQuota)
		},
	}, nil
}

// newStorageMutationOperations captures raw writers into a construction-only
// value consumed immediately by exact settlement-bound executors.
func newStorageMutationOperations(
	backend *Backend,
	docker dockerMutationSink,
	compose composeMutationSink,
	volumes volumeMutationSink,
) storageMutationOperations {
	return storageMutationOperations{backend: backend, docker: docker, compose: compose, volumes: volumes}
}

// storageMutations is a per-execution facade. Its scope is derived only from
// the opaque Started subject; callers can choose an operation but cannot
// substitute another lease, tenant, project, or canonical volume namespace.
type storageMutations struct {
	runner       substratemutation.Runner
	ops          storageMutationOperations
	leaseUUID    string
	tenant       string
	providerUUID string
	callbackURL  string
	lifecycleURL string
	allowedLease map[string]struct{}
	predecessor  *shared.Release
	cleanupOnly  bool
}

// exactOperationTeardown is the only raw container-teardown projection of a
// per-execution storageMutations value. It is constructed and consumed inside
// one Runner.Step by teardownLeaseContainersWith. Its Started subject fixes the
// lease/project, while every discovered container is freshly re-attested before
// the raw writer is reached.
//
// Keeping these raw calls in the mutation choke-point is load-bearing: the
// orchestration helper retains only teardownMutationCapability and therefore
// cannot recover an unscoped Docker or Compose writer.
type exactOperationTeardown struct {
	mutations *storageMutations
}

func (c exactOperationTeardown) composeDown(
	ctx context.Context,
	leaseUUID string,
	timeout time.Duration,
) error {
	if c.mutations == nil {
		return errors.New("exact operation teardown is unavailable")
	}
	if err := c.mutations.requireLease(leaseUUID, "compose down"); err != nil {
		return err
	}
	return c.mutations.ops.compose.Down(ctx, composeProjectName(leaseUUID), timeout)
}

func (c exactOperationTeardown) removeContainer(ctx context.Context, id string) error {
	if c.mutations == nil {
		return errors.New("exact operation teardown is unavailable")
	}
	if err := c.mutations.requireContainer(ctx, id); err != nil {
		return err
	}
	return c.mutations.ops.docker.RemoveContainer(ctx, id)
}

func newOperationStorageMutations(
	runner substratemutation.Runner,
	subject shared.OperationPhysicalSubject,
	ops storageMutationOperations,
) *storageMutations {
	intent := subject.Intent()
	allowed := map[string]struct{}{subject.LeaseUUID(): {}}
	if intent.Valid() && intent.Kind() == shared.OperationIntentRestore && intent.SourceLeaseUUID() != "" {
		allowed[intent.SourceLeaseUUID()] = struct{}{}
	}
	tenant, providerUUID, callbackURL, lifecycleURL := "", "", "", ""
	if intent.Valid() {
		tenant = intent.Tenant()
		providerUUID = intent.ProviderUUID()
		callbackURL = intent.CallbackURL()
		lifecycleURL = intent.LifecycleCallbackURL()
	}
	if receipt, historical := subject.FailedReceiptCleanup(); historical {
		tenant = receipt.Tenant()
		providerUUID = receipt.ProviderUUID()
		callbackURL = receipt.CallbackURL()
		lifecycleURL = receipt.LifecycleCallbackURL()
	}
	var predecessor *shared.Release
	if release, ok := subject.PredecessorRelease(); ok {
		predecessor = &release
	}
	return &storageMutations{
		runner: runner, ops: ops, leaseUUID: subject.LeaseUUID(), tenant: tenant,
		providerUUID: providerUUID, callbackURL: callbackURL,
		lifecycleURL: lifecycleURL, allowedLease: allowed, predecessor: predecessor,
	}
}

func newMaintenanceStorageMutations(
	runner substratemutation.Runner,
	subject shared.MaintenancePhysicalSubject,
	ops storageMutationOperations,
) *storageMutations {
	intent := subject.Intent()
	tenant, providerUUID, callbackURL, lifecycleURL := "", "", "", ""
	if intent.Valid() {
		tenant = intent.Tenant()
		providerUUID = intent.ProviderUUID()
		callbackURL = intent.CallbackURL()
		lifecycleURL = intent.LifecycleCallbackURL()
	}
	if receipt, historical := subject.FailedReceiptCleanup(); historical {
		tenant = receipt.Tenant()
		providerUUID = receipt.ProviderUUID()
		if target, ok := receipt.TargetRelease(); ok {
			if authority, valid := runtimeIdentityForRelease(&target); valid {
				callbackURL = authority.CallbackURL()
				lifecycleURL = authority.LifecycleCallbackURL()
			}
		}
	}
	return &storageMutations{
		runner: runner, ops: ops, leaseUUID: subject.LeaseUUID(), tenant: tenant,
		providerUUID: providerUUID, callbackURL: callbackURL,
		lifecycleURL: lifecycleURL,
		allowedLease: map[string]struct{}{subject.LeaseUUID(): {}},
	}
}

func (m *storageMutations) ownsLease(leaseUUID string) bool {
	if m == nil || leaseUUID == "" {
		return false
	}
	_, ok := m.allowedLease[leaseUUID]
	return ok
}

func (m *storageMutations) requireLease(leaseUUID, operation string) error {
	if m == nil || !m.ownsLease(leaseUUID) {
		return fmt.Errorf("%s target lease %q differs from Started subject", operation, leaseUUID)
	}
	return nil
}

func (m *storageMutations) pullImage(ctx context.Context, image string, timeout time.Duration) error {
	return m.runner.Prepare(ctx, "pull image", func(ctx context.Context) error {
		return m.ops.docker.PullImage(ctx, image, timeout)
	})
}

func (m *storageMutations) admitImage(ctx context.Context, reference string) (admitted imageexec.Image, err error) {
	err = m.runner.Prepare(ctx, "admit image", func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, m.ops.backend.cfg.ImagePullTimeout)
		defer cancel()
		admitted, err = m.ops.docker.AdmitImage(ctx, reference)
		return err
	})
	return admitted, err
}

func (m *storageMutations) resolveImageUser(ctx context.Context, image imageexec.Image, user string) (uid, gid int, err error) {
	err = m.runner.Step(ctx, "inspect image user", func(ctx context.Context) error {
		uid, gid, err = m.ops.docker.ResolveImageUser(ctx, image, user)
		return err
	})
	return uid, gid, err
}

func (m *storageMutations) detectVolumeOwner(ctx context.Context, image imageexec.Image, paths []string) (uid, gid int, err error) {
	err = m.runner.Step(ctx, "inspect image volume owner", func(ctx context.Context) error {
		uid, gid, err = m.ops.docker.DetectVolumeOwner(ctx, image, paths)
		return err
	})
	return uid, gid, err
}

func (m *storageMutations) detectWritablePaths(ctx context.Context, detection writablePathDetection) (paths []string, err error) {
	err = m.runner.Step(ctx, "inspect image writable paths", func(ctx context.Context) error {
		paths, err = m.ops.docker.DetectWritablePaths(ctx, detection.image, detection.uid, candidateWritableParents)
		return err
	})
	return paths, err
}

func (m *storageMutations) effectEntered() bool {
	return m != nil && m.runner.EffectEntered()
}

func (m *storageMutations) composeUp(ctx context.Context, project *composetypes.Project, images map[string]imageexec.Image, opts composeUpOpts) error {
	if project == nil || project.Name != composeProjectName(m.leaseUUID) {
		return fmt.Errorf("compose up project differs from Started lease %q", m.leaseUUID)
	}
	prepared, err := m.ops.compose.PrepareProject(project, images)
	if err != nil {
		return err
	}
	return m.runner.Step(ctx, "compose up", func(ctx context.Context) error {
		return m.ops.compose.Up(ctx, prepared, opts)
	})
}

func (m *storageMutations) composeDown(ctx context.Context, leaseUUID string, timeout time.Duration) error {
	if err := m.requireLease(leaseUUID, "compose down"); err != nil {
		return err
	}
	return m.runner.Step(ctx, "compose down", func(ctx context.Context) error {
		return m.ops.compose.Down(ctx, composeProjectName(leaseUUID), timeout)
	})
}

func (m *storageMutations) requireContainer(ctx context.Context, id string) error {
	if m == nil || id == "" {
		return errors.New("container target is empty")
	}
	info, err := m.ops.backend.docker.InspectContainer(ctx, id)
	if err != nil {
		return fmt.Errorf("attest container %q before mutation: %w", id, err)
	}
	if info == nil || info.ContainerID != id || !m.ownsLease(info.LeaseUUID) ||
		info.BackendName != m.ops.backend.cfg.Name {
		return fmt.Errorf("container %q differs from Started subject", id)
	}
	// Cleanup-only close authority deliberately carries no principal or callback
	// capability. Its exact durable lease/backend/storage lineage authorizes
	// removal of every managed survivor in that namespace without relabeling it
	// as a current runtime generation.
	if m.cleanupOnly {
		return nil
	}
	if info.Tenant == m.tenant && info.ProviderUUID == m.providerUUID &&
		info.CallbackURL == m.callbackURL && info.LifecycleCallbackURL == m.lifecycleURL {
		return nil
	}
	if m.predecessor != nil {
		if authority, ok := runtimeIdentityForRelease(m.predecessor); ok &&
			containerMatchesReleaseRuntimeIdentity(*info, authority) {
			return nil
		}
	}
	return fmt.Errorf("container %q runtime generation differs from Started subject", id)
}

func (m *storageMutations) removeContainer(ctx context.Context, id string) error {
	if err := m.requireContainer(ctx, id); err != nil {
		return err
	}
	return m.runner.Step(ctx, "remove container", func(ctx context.Context) error {
		return m.ops.docker.RemoveContainer(ctx, id)
	})
}

func (m *storageMutations) ensureTenantNetwork(ctx context.Context, tenant string) (networkID string, err error) {
	if m == nil || tenant == "" || tenant != m.tenant {
		return "", fmt.Errorf("tenant network target %q differs from Started subject", tenant)
	}
	err = m.runner.Step(ctx, "ensure tenant network", func(ctx context.Context) error {
		networkID, err = m.ops.docker.EnsureTenantNetwork(ctx, tenant)
		return err
	})
	return networkID, err
}

func (m *storageMutations) createVolume(ctx context.Context, id string, sizeMB int64) (path string, created bool, err error) {
	name, parseErr := parseManagedVolumeName(id)
	if parseErr != nil || !m.volumeNameInScope(name) {
		return "", false, fmt.Errorf("create volume target %q differs from Started subject", id)
	}
	err = m.runner.Step(ctx, "create volume", func(ctx context.Context) error {
		path, created, err = m.ops.volumes.Create(ctx, id, sizeMB)
		return err
	})
	return path, created, err
}

func (m *storageMutations) volumeNameInScope(name managedVolumeName) bool {
	if m == nil {
		return false
	}
	_, ok := m.allowedLease[managedVolumeLeaseUUID(name)]
	return ok
}

func (m *storageMutations) destroyVolume(ctx context.Context, id string) error {
	name, err := parseManagedVolumeName(id)
	if err != nil || !m.volumeNameInScope(name) {
		return fmt.Errorf("destroy volume target %q differs from Started subject", id)
	}
	return m.runner.Step(ctx, "destroy volume", func(ctx context.Context) error {
		return m.ops.volumes.Destroy(ctx, id)
	})
}

func (m *storageMutations) canDestroyVolumes() bool { return m != nil && m.ops.volumes != nil }

func (m *storageMutations) renameVolume(ctx context.Context, oldName, newName string) error {
	oldParsed, oldErr := parseManagedVolumeName(oldName)
	newParsed, newErr := parseManagedVolumeName(newName)
	if oldErr != nil || newErr != nil || !m.volumeNameInScope(oldParsed) || !m.volumeNameInScope(newParsed) {
		return fmt.Errorf("volume rename %q -> %q differs from Started subject", oldName, newName)
	}
	return m.runner.Step(ctx, "rename volume", func(ctx context.Context) error {
		return m.ops.volumes.RenameVolume(ctx, oldName, newName)
	})
}

func (m *storageMutations) removePath(ctx context.Context, path string) error {
	volume, err := writablePathVolumeComponent(m.ops.backend.cfg.VolumeDataPath, path)
	if err != nil || !m.volumeNameInScope(volume) {
		return fmt.Errorf("writable path target differs from Started subject: %w", err)
	}
	return m.runner.Step(ctx, "remove managed writable path", func(ctx context.Context) error {
		wp, err := parseStoragePathComponent(writablePathSubdir)
		if err != nil {
			return err
		}
		return removeManagedVolumeSubtree(m.ops.backend.cfg.VolumeDataPath, volume, wp)
	})
}

func (m *storageMutations) extractImageContent(ctx context.Context, image imageexec.Image, paths []string, destination string, maxBytes, maxEntries int64) (failures map[string]error, err error) {
	volume, scopeErr := writablePathVolumeComponent(m.ops.backend.cfg.VolumeDataPath, destination)
	if scopeErr != nil || !m.volumeNameInScope(volume) {
		return nil, fmt.Errorf("image extraction target differs from Started subject: %w", scopeErr)
	}
	err = m.runner.Step(ctx, "extract image content", func(ctx context.Context) error {
		failures = m.ops.docker.ExtractImageContent(ctx, image, paths, destination, maxBytes, maxEntries)
		return nil
	})
	return failures, err
}

func (m *storageMutations) prepareStatefulVolumeBinds(ctx context.Context, hostPath string, volumes []string, uid, gid int) (binds map[string]string, err error) {
	name, parseErr := writablePathVolumeComponent(m.ops.backend.cfg.VolumeDataPath, filepath.Join(hostPath, writablePathSubdir))
	if parseErr != nil || !m.volumeNameInScope(name) {
		return nil, fmt.Errorf("stateful volume path differs from Started subject: %w", parseErr)
	}
	err = m.runner.Step(ctx, "prepare stateful volume binds", func(ctx context.Context) error {
		binds, err = buildStatefulVolumeBindsContext(ctx, hostPath, volumes, uid, gid)
		return err
	})
	return binds, err
}

// newBackendStorageAuthorityLifetime binds terminal storage withdrawal to both
// backend-worker cancellation and one daemon-visible failure notification. The
// first-error channel has capacity one and the hook never blocks: it can run at
// the end of an identity-bound bbolt commit and must not introduce a dependency
// on the HTTP server's receive loop.
func newBackendStorageAuthorityLifetime() (
	context.Context,
	context.CancelFunc,
	<-chan error,
	*backendidentity.StorageAuthorityGate,
	error,
) {
	stopCtx, stopCancel := context.WithCancel(context.Background())
	terminalFailure := make(chan error, 1)
	gate, err := backendidentity.NewStorageAuthorityGate(func(cause error) {
		select {
		case terminalFailure <- cause:
		default:
		}
		stopCancel()
	})
	if err != nil {
		stopCancel()
		return nil, nil, nil, nil, err
	}
	return stopCtx, stopCancel, terminalFailure, gate, nil
}

// storagePathComponent is a single, relative filesystem name that has crossed
// the storage mutation boundary's lexical validation. Keeping it distinct from
// string prevents a request- or store-derived identifier from being handed to
// a destructive filesystem primitive without first proving that it cannot
// carry path traversal syntax.
type storagePathComponent string

// parseStoragePathComponent validates one filesystem component. The explicit
// slash and ".." checks are deliberately stricter than filepath.IsLocal: fred's
// managed storage names never need either spelling, and rejecting both slash
// forms keeps the token safe if a database is moved between Unix and Windows.
func parseStoragePathComponent(value string) (storagePathComponent, error) {
	switch {
	case value == "":
		return "", errors.New("storage path component is empty")
	case value == ".", value == "..":
		return "", fmt.Errorf("storage path component is reserved: %q", value)
	case !filepath.IsLocal(value), filepath.IsAbs(value):
		return "", fmt.Errorf("storage path component is not local: %q", value)
	case filepath.Clean(value) != value, filepath.Base(value) != value:
		return "", fmt.Errorf("storage path component is not a single clean name: %q", value)
	case strings.ContainsAny(value, `/\\`), strings.Contains(value, ".."), strings.ContainsRune(value, 0):
		return "", fmt.Errorf("storage path component contains reserved path syntax: %q", value)
	default:
		return storagePathComponent(value), nil
	}
}

func openAttestedManagedVolumeRoot(root *os.Root, volumeID managedVolumeName) (*os.Root, error) {
	before, err := root.Lstat(volumeID.value())
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("managed volume entry %q is not a real directory", volumeID.value())
	}
	volumeRoot, err := root.OpenRoot(volumeID.value())
	if err != nil {
		return nil, err
	}
	after, err := volumeRoot.Stat(".")
	if err != nil {
		_ = volumeRoot.Close()
		return nil, err
	}
	if !os.SameFile(before, after) {
		_ = volumeRoot.Close()
		return nil, fmt.Errorf("managed volume entry %q changed while opening it", volumeID.value())
	}
	return volumeRoot, nil
}

// removeManagedVolumeSubtree performs recursive deletion under an attested
// managed-volume descriptor. Opening the volume first is load-bearing: rooting
// one combined "volume/_wp" path at volume_data_path would still permit the
// volume component to be an in-root symlink to another tenant's volume.
func removeManagedVolumeSubtree(rootPath string, volumeID managedVolumeName, subtree storagePathComponent) error {
	validatedSubtree, err := parseStoragePathComponent(string(subtree))
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("open storage root %q: %w", rootPath, err)
	}
	defer func() { _ = root.Close() }()
	volumeRoot, err := openAttestedManagedVolumeRoot(root, volumeID)
	if err != nil {
		return fmt.Errorf("open managed volume %q: %w", volumeID.value(), err)
	}
	defer func() { _ = volumeRoot.Close() }()
	if err := volumeRoot.RemoveAll(string(validatedSubtree)); err != nil {
		return fmt.Errorf("remove subtree %q from managed volume %q: %w", validatedSubtree, volumeID.value(), err)
	}
	return nil
}

func managedDirectoryExistsAtRoot(root *os.Root, name managedVolumeName) (bool, error) {
	info, err := root.Lstat(name.value())
	if err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return false, fmt.Errorf("managed volume entry %q is not a real directory", name.value())
		}
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// renameAtStorageRoot atomically renames one exact managed-volume entry under
// an open root descriptor. Both the source and destination are typed names;
// Lstat rejects symlinks and non-directory collisions before the rename, while
// os.Root prevents either lookup from escaping if an ancestor changes.
//
// Idempotency semantics:
//   - neither exists  -> error
//   - only old exists -> rename
//   - only new exists -> nil (a previous attempt already renamed it)
//   - both exist      -> error (never merge or pick a winner)
func renameAtStorageRoot(ctx context.Context, rootPath string, oldName, newName managedVolumeName) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := fsidentity.OpenDirectory(rootPath)
	if err != nil {
		return fmt.Errorf("open storage root %q: %w", rootPath, err)
	}
	defer func() { _ = root.Close() }()

	entryIsRealDirectory := func(name managedVolumeName) (bool, error) {
		info, statErr := root.Lstat(name.value())
		if statErr == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return false, fmt.Errorf("managed volume entry %q is not a real directory", name.value())
			}
			return true, nil
		}
		if errors.Is(statErr, os.ErrNotExist) {
			return false, nil
		}
		return false, statErr
	}
	oldExists, err := entryIsRealDirectory(oldName)
	if err != nil {
		return fmt.Errorf("stat old managed volume %q: %w", oldName.value(), err)
	}
	newExists, err := entryIsRealDirectory(newName)
	if err != nil {
		return fmt.Errorf("stat new managed volume %q: %w", newName.value(), err)
	}
	switch {
	case !oldExists && newExists:
		return nil
	case oldExists && newExists:
		return fmt.Errorf("both old (%s) and new (%s) volume paths exist; manual intervention required",
			oldName.hostPath(rootPath), newName.hostPath(rootPath))
	case !oldExists && !newExists:
		return fmt.Errorf("neither old (%s) nor new (%s) volume path exists",
			oldName.hostPath(rootPath), newName.hostPath(rootPath))
	}
	// The no-replace flag is the mutation-time authority. A stat-then-Rename
	// sequence can overwrite a destination created after the check (an empty
	// directory is replaceable on Linux); RENAME_NOREPLACE makes that race fail
	// closed in the kernel while retaining the same pinned parent descriptor.
	if err := root.RenameNoReplace(oldName.value(), newName.value()); err != nil {
		return fmt.Errorf("rename managed volume %q to %q: %w", oldName.value(), newName.value(), err)
	}
	return nil
}

// writablePathVolumeComponent proves that path is exactly
// {volumeRoot}/{managed-volume}/_wp. It returns only the managed volume token;
// the eventual deletion path is reconstructed from that token and the fixed
// _wp component rather than reusing the caller-supplied path.
func writablePathVolumeComponent(volumeRoot, path string) (managedVolumeName, error) {
	if volumeRoot == "" || !filepath.IsAbs(volumeRoot) || filepath.Clean(volumeRoot) != volumeRoot {
		return "", fmt.Errorf("volume root must be a non-empty clean absolute path: %q", volumeRoot)
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", fmt.Errorf("writable-path cleanup target must be a clean absolute path: %q", path)
	}

	relative, err := filepath.Rel(volumeRoot, path)
	if err != nil {
		return "", fmt.Errorf("resolve writable-path cleanup target relative to volume root: %w", err)
	}
	if !filepath.IsLocal(relative) || filepath.Clean(relative) != relative {
		return "", fmt.Errorf("writable-path cleanup target escapes volume root: %q", path)
	}
	if filepath.Base(relative) != writablePathSubdir {
		return "", fmt.Errorf("writable-path cleanup target must end in %q: %q", writablePathSubdir, path)
	}

	volumeName := filepath.Dir(relative)
	component, err := parseManagedVolumeName(volumeName)
	if err != nil {
		return "", fmt.Errorf("validate writable-path cleanup volume: %w", err)
	}
	return component, nil
}

// requireMutationAdmission makes a stopped or permanently drifted backend a
// synchronous API refusal. Mutation adapters still re-attest later at the raw
// choke point; this first check prevents a request from publishing actor/store
// state when shutdown had already made every eventual substrate write illegal.
func (b *Backend) requireMutationAdmission(ctx context.Context, operation string) error {
	_, done, err := b.authorizeStorageMutation(ctx, operation+" admission")
	if err != nil {
		return err
	}
	done()
	return nil
}

// terminalStorageAuthorityError returns the backend-lifetime storage failure,
// if any. Callers use this at durable settlement boundaries: a mutation error
// may have been deliberately downgraded to a default by an intermediate helper,
// but the lifetime latch must still prevent that later code from consuming the
// write-ahead evidence.
func (b *Backend) terminalStorageAuthorityError() error {
	if b == nil {
		return nil
	}
	return b.storeAuthorityGate.Error()
}

// latchTerminalStorageAuthority records the first terminal backend-storage
// failure before canceling the backend lifetime. It intentionally does not
// acquire identityVerifyMu: an identity-bound store can invoke its failure hook
// from inside Backend.verifyStorageIdentity while that mutex is already held.
func (b *Backend) latchTerminalStorageAuthority(cause error) error {
	if b == nil || cause == nil {
		return cause
	}
	return b.storeAuthorityGate.Latch(cause)
}

// latchIdentityVerificationFailureLocked mirrors the terminal cause into the
// verifier-local cache and the backend-wide latch. The caller must hold
// identityVerifyMu. Keeping the global latch separately locked is what makes a
// bound-store failure hook safe when it fires under the same verification.
func (b *Backend) latchIdentityVerificationFailureLocked(cause error) error {
	if b.identityDriftErr == nil {
		b.identityDriftErr = cause
	}
	return b.latchTerminalStorageAuthority(b.identityDriftErr)
}

// latchAmbiguousOperationOutcome permanently closes this Backend instance
// after an operation has changed tenant substrate but its durable recovery
// record could not be committed. Continuing would let a later callback consume
// the exact write-ahead intent without leaving enough evidence to classify the
// substrate after a crash. A fresh process must re-open the stores, re-attest
// the substrate, and resolve the retained intent from inventory.
func (b *Backend) latchAmbiguousOperationOutcome(operation string, cause error) error {
	ambiguousErr := fmt.Errorf("%w: %s: %w",
		backendidentity.ErrMutationOutcomeAmbiguous, operation, cause)
	b.identityVerifyMu.Lock()
	if !errors.Is(b.identityDriftErr, backendidentity.ErrMutationOutcomeAmbiguous) {
		b.identityDriftErr = ambiguousErr
	}
	identityErr := b.identityDriftErr
	b.identityVerifyMu.Unlock()

	// A post-mutation verification first reports its lower-level drift through
	// VerifyStorageIdentity and is then classified here as outcome-ambiguous.
	// Promote that same causal chain in the backend latch; otherwise callers see
	// only the raw drift and can mistake the substrate side effect for definitely
	// uncommitted. The wrapped original cause remains observable with errors.Is.
	return b.storeAuthorityGate.PromoteAmbiguous(identityErr)
}

// authorize returns a context canceled when either the operation or Backend
// stops, then re-attests storage identity under that joined lifetime. The
// context is passed to the actual mutator, closing the gap where shutdown could
// begin after verification but before the daemon receives the request.
func (b *Backend) authorizeStorageMutation(ctx context.Context, operation string) (context.Context, func(), error) {
	if b == nil {
		return nil, nil, fmt.Errorf("%s: Docker backend is required", operation)
	}
	if ctx == nil {
		return nil, nil, fmt.Errorf("%s: context is required", operation)
	}
	if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil {
		return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
	}

	joined, cancel := context.WithCancel(ctx)
	stopAfter := func() bool { return false }
	if b.stopCtx != nil {
		if err := b.stopCtx.Err(); err != nil {
			cancel()
			if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil {
				return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
			}
			return nil, nil, fmt.Errorf("%s: backend stopped: %w", operation, err)
		}
		stopAfter = context.AfterFunc(b.stopCtx, cancel)
	}
	done := func() {
		defer cancel()
		stopAfter()
	}
	// context.AfterFunc deliberately schedules asynchronously. Re-read the
	// parent synchronously after registration so a stop racing the first check
	// cannot slip a mutation through before the callback goroutine runs.
	if b.stopCtx != nil {
		if err := b.stopCtx.Err(); err != nil {
			done()
			if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil {
				return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
			}
			return nil, nil, fmt.Errorf("%s: backend stopped while authorizing storage: %w", operation, err)
		}
	}
	if err := joined.Err(); err != nil {
		done()
		if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil {
			return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
		}
		return nil, nil, fmt.Errorf("%s: operation canceled before storage authorization: %w", operation, err)
	}
	if err := b.requireStorageIdentity(joined); err != nil {
		done()
		if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil {
			return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
		}
		return nil, nil, fmt.Errorf("%s: %w", operation, err)
	}
	if err := joined.Err(); err != nil {
		done()
		if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil {
			return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
		}
		return nil, nil, fmt.Errorf("%s: backend stopped after storage authorization: %w", operation, err)
	}
	if b.stopCtx != nil {
		if err := b.stopCtx.Err(); err != nil {
			done()
			if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil {
				return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
			}
			return nil, nil, fmt.Errorf("%s: backend stopped after storage authorization: %w", operation, err)
		}
	}
	return joined, done, nil
}

// completeMutation re-attests storage lineage after a raw mutator returns.
// Pre-attestation prevents a known-wrong substrate from receiving work; this
// post-attestation prevents a mutation whose substrate changed during the call
// from being reported as a definitive success or failure. In that window the
// side effect is causally ambiguous, so callers must retain their durable
// intent/finalizer and let recovery classify the substrate.
//
// Always preserve the raw mutation error. errors.Join gives callers both the
// transport/filesystem result and any stronger identity-drift cause, including
// when both happened concurrently.
func (b *Backend) completeStorageMutation(ctx context.Context, operation string, mutationErr error) error {
	// The effect's expired deadline says nothing about whether storage identity
	// remains valid. Every live/recovery/background executor gives the mandatory
	// postcheck its own bounded read, including during shutdown. A successful
	// attestation preserves the raw call error and its ambiguous outcome; an
	// independently failed attestation still withdraws this storage authority.
	postCtx, cancelPostcheck := b.recoveryDockerReadContext(context.WithoutCancel(ctx))
	defer cancelPostcheck()
	postcheckErr := b.requireStorageIdentity(postCtx)
	if postcheckErr != nil {
		// Latch every failed postcheck, not only a proved permanent identity
		// contradiction. Once a raw side effect ran, even a timeout leaves its
		// target/outcome unknown; allowing a later callback or compensating
		// cleanup in this process would turn missing evidence into a guess. A new
		// process re-opens the durable stores and classifies the retained intent
		// against freshly attested substrate evidence.
		postcheckErr = b.latchAmbiguousOperationOutcome(
			operation+" post-mutation storage verification", postcheckErr,
		)
	} else if errors.Is(mutationErr, backendidentity.ErrMutationOutcomeAmbiguous) ||
		errors.Is(mutationErr, ErrVolumeMutationRecoveryPending) {
		// A manager can prove that a side effect reached its live name while its
		// durability acknowledgement remained unknown (for example, XFS rename
		// succeeded but the parent-directory fsync failed), or retain a durable
		// cleanup capability after a known-incomplete delete. The identity
		// postcheck cannot consume either recovery obligation, so preserve the
		// manager's typed cause and fail-stop exactly as for a failed postcheck.
		mutationErr = b.latchAmbiguousOperationOutcome(operation, mutationErr)
	}
	return errors.Join(mutationErr, postcheckErr)
}
