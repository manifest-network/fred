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

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/fsidentity"
)

// storageMutationAdapters is deliberately held by Backend instead of passed
// around as three raw clients. That makes storage authority an explicit
// capability: callers can request a guarded mutation, but cannot accidentally
// retain a raw Docker/Compose/volume mutation method across an actor wait.
type storageMutationAdapters struct {
	backend *Backend
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

// mutationAdapter supports the small number of package tests which construct a
// Backend literal. Production Backends always receive the stored adapter in New.
// The fallback is returned by value and never published, so concurrent test
// calls cannot race on lazy initialization.
func (b *Backend) mutationAdapter() storageMutationAdapters {
	if b != nil && b.mutations.backend != nil {
		return b.mutations
	}
	return storageMutationAdapters{backend: b}
}

// requireMutationAdmission makes a stopped or permanently drifted backend a
// synchronous API refusal. Mutation adapters still re-attest later at the raw
// choke point; this first check prevents a request from publishing actor/store
// state when shutdown had already made every eventual substrate write illegal.
func (b *Backend) requireMutationAdmission(ctx context.Context, operation string) error {
	_, done, err := b.mutationAdapter().authorize(ctx, operation+" admission")
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
func (m storageMutationAdapters) authorize(ctx context.Context, operation string) (context.Context, func(), error) {
	if m.backend == nil {
		return nil, nil, fmt.Errorf("%s: Docker backend is required", operation)
	}
	if ctx == nil {
		return nil, nil, fmt.Errorf("%s: context is required", operation)
	}
	if authorityErr := m.backend.terminalStorageAuthorityError(); authorityErr != nil {
		return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
	}

	joined, cancel := context.WithCancel(ctx)
	stopAfter := func() bool { return false }
	if m.backend.stopCtx != nil {
		if err := m.backend.stopCtx.Err(); err != nil {
			cancel()
			if authorityErr := m.backend.terminalStorageAuthorityError(); authorityErr != nil {
				return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
			}
			return nil, nil, fmt.Errorf("%s: backend stopped: %w", operation, err)
		}
		stopAfter = context.AfterFunc(m.backend.stopCtx, cancel)
	}
	done := func() {
		stopAfter()
		cancel()
	}
	// context.AfterFunc deliberately schedules asynchronously. Re-read the
	// parent synchronously after registration so a stop racing the first check
	// cannot slip a mutation through before the callback goroutine runs.
	if m.backend.stopCtx != nil {
		if err := m.backend.stopCtx.Err(); err != nil {
			done()
			if authorityErr := m.backend.terminalStorageAuthorityError(); authorityErr != nil {
				return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
			}
			return nil, nil, fmt.Errorf("%s: backend stopped while authorizing storage: %w", operation, err)
		}
	}
	if err := joined.Err(); err != nil {
		done()
		if authorityErr := m.backend.terminalStorageAuthorityError(); authorityErr != nil {
			return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
		}
		return nil, nil, fmt.Errorf("%s: operation canceled before storage authorization: %w", operation, err)
	}
	if err := m.backend.requireStorageIdentity(joined); err != nil {
		done()
		if authorityErr := m.backend.terminalStorageAuthorityError(); authorityErr != nil {
			return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
		}
		return nil, nil, fmt.Errorf("%s: %w", operation, err)
	}
	if err := joined.Err(); err != nil {
		done()
		if authorityErr := m.backend.terminalStorageAuthorityError(); authorityErr != nil {
			return nil, nil, fmt.Errorf("%s: %w", operation, authorityErr)
		}
		return nil, nil, fmt.Errorf("%s: backend stopped after storage authorization: %w", operation, err)
	}
	if m.backend.stopCtx != nil {
		if err := m.backend.stopCtx.Err(); err != nil {
			done()
			if authorityErr := m.backend.terminalStorageAuthorityError(); authorityErr != nil {
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
func (m storageMutationAdapters) completeMutation(ctx context.Context, operation string, mutationErr error) error {
	postcheckErr := m.backend.requireStorageIdentity(ctx)
	if postcheckErr != nil {
		// Latch every failed postcheck, not only a proved permanent identity
		// contradiction. Once a raw side effect ran, even a timeout leaves its
		// target/outcome unknown; allowing a later callback or compensating
		// cleanup in this process would turn missing evidence into a guess. A new
		// process re-opens the durable stores and classifies the retained intent
		// against freshly attested substrate evidence.
		postcheckErr = m.backend.latchAmbiguousOperationOutcome(
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
		mutationErr = m.backend.latchAmbiguousOperationOutcome(operation, mutationErr)
	}
	return errors.Join(mutationErr, postcheckErr)
}

func (m storageMutationAdapters) composeUp(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
	ctx, done, err := m.authorize(ctx, "compose up")
	if err != nil {
		return err
	}
	defer done()
	mutationErr := m.backend.compose.Up(ctx, project, opts)
	return m.completeMutation(ctx, "compose up", mutationErr)
}

func (m storageMutationAdapters) composeDown(ctx context.Context, projectName string, timeout time.Duration) error {
	ctx, done, err := m.authorize(ctx, "compose down")
	if err != nil {
		return err
	}
	defer done()
	mutationErr := m.backend.compose.Down(ctx, projectName, timeout)
	return m.completeMutation(ctx, "compose down", mutationErr)
}

func (m storageMutationAdapters) pullImage(ctx context.Context, imageName string, timeout time.Duration) error {
	ctx, done, err := m.authorize(ctx, "pull image")
	if err != nil {
		return err
	}
	defer done()
	mutationErr := m.backend.docker.PullImage(ctx, imageName, timeout)
	return m.completeMutation(ctx, "pull image", mutationErr)
}

func (m storageMutationAdapters) resolveImageUser(ctx context.Context, imageName, userOverride string) (int, int, error) {
	ctx, done, err := m.authorize(ctx, "inspect image user")
	if err != nil {
		return 0, 0, err
	}
	defer done()
	uid, gid, mutationErr := m.backend.docker.ResolveImageUser(ctx, imageName, userOverride)
	return uid, gid, m.completeMutation(ctx, "inspect image user", mutationErr)
}

func (m storageMutationAdapters) detectVolumeOwner(ctx context.Context, imageName string, paths []string) (int, int, error) {
	ctx, done, err := m.authorize(ctx, "inspect image volume owner")
	if err != nil {
		return 0, 0, err
	}
	defer done()
	uid, gid, mutationErr := m.backend.docker.DetectVolumeOwner(ctx, imageName, paths)
	return uid, gid, m.completeMutation(ctx, "inspect image volume owner", mutationErr)
}

func (m storageMutationAdapters) detectWritablePaths(ctx context.Context, imageName string, uid int, parents []string) ([]string, error) {
	ctx, done, err := m.authorize(ctx, "inspect image writable paths")
	if err != nil {
		return nil, err
	}
	defer done()
	paths, mutationErr := m.backend.docker.DetectWritablePaths(ctx, imageName, uid, parents)
	return paths, m.completeMutation(ctx, "inspect image writable paths", mutationErr)
}

func (m storageMutationAdapters) extractImageContent(ctx context.Context, imageName string, paths []string, destDir string, maxBytes, maxEntries int64) (map[string]error, error) {
	ctx, done, err := m.authorize(ctx, "extract image content")
	if err != nil {
		return nil, err
	}
	defer done()
	failures := m.backend.docker.ExtractImageContent(ctx, imageName, paths, destDir, maxBytes, maxEntries)
	return failures, m.completeMutation(ctx, "extract image content", nil)
}

func (m storageMutationAdapters) stopContainer(ctx context.Context, id string, timeout time.Duration) error {
	ctx, done, err := m.authorize(ctx, "stop container")
	if err != nil {
		return err
	}
	defer done()
	mutationErr := m.backend.docker.StopContainer(ctx, id, timeout)
	return m.completeMutation(ctx, "stop container", mutationErr)
}

func (m storageMutationAdapters) renameContainer(ctx context.Context, id, newName string) error {
	ctx, done, err := m.authorize(ctx, "rename container")
	if err != nil {
		return err
	}
	defer done()
	mutationErr := m.backend.docker.RenameContainer(ctx, id, newName)
	return m.completeMutation(ctx, "rename container", mutationErr)
}

func (m storageMutationAdapters) removeContainer(ctx context.Context, id string) error {
	ctx, done, err := m.authorize(ctx, "remove container")
	if err != nil {
		return err
	}
	defer done()
	mutationErr := m.backend.docker.RemoveContainer(ctx, id)
	return m.completeMutation(ctx, "remove container", mutationErr)
}

func (m storageMutationAdapters) ensureTenantNetwork(ctx context.Context, tenant string) (string, error) {
	ctx, done, err := m.authorize(ctx, "ensure tenant network")
	if err != nil {
		return "", err
	}
	defer done()
	networkID, mutationErr := m.backend.docker.EnsureTenantNetwork(ctx, tenant)
	return networkID, m.completeMutation(ctx, "ensure tenant network", mutationErr)
}

func (m storageMutationAdapters) removeTenantNetworkIfEmpty(ctx context.Context, tenant string) error {
	ctx, done, err := m.authorize(ctx, "remove tenant network")
	if err != nil {
		return err
	}
	defer done()
	mutationErr := m.backend.docker.RemoveTenantNetworkIfEmpty(ctx, tenant)
	return m.completeMutation(ctx, "remove tenant network", mutationErr)
}

func (m storageMutationAdapters) createVolume(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
	ctx, done, err := m.authorize(ctx, "create volume")
	if err != nil {
		return "", false, err
	}
	defer done()
	// This is the raw storage sink behind createManagedVolume. That caller holds
	// the volume-name stripe; this adapter contributes mutation authorization and
	// the mandatory postcheck.
	hostPath, created, mutationErr := m.backend.volumes.Create(ctx, id, sizeMB) //nolint:forbidigo
	return hostPath, created, m.completeMutation(ctx, "create volume", mutationErr)
}

func (m storageMutationAdapters) recoverInterruptedVolumeMutations(ctx context.Context) error {
	ctx, done, err := m.authorize(ctx, "recover interrupted volume mutations")
	if err != nil {
		return err
	}
	defer done()
	mutationErr := m.backend.volumes.RecoverInterruptedVolumeMutations(ctx)
	return m.completeMutation(ctx, "recover interrupted volume mutations", mutationErr)
}

func (m storageMutationAdapters) destroyVolume(ctx context.Context, sink volumeDestroyer, id string) error {
	ctx, done, err := m.authorize(ctx, "destroy volume")
	if err != nil {
		return err
	}
	defer done()
	// volumeOp is the only caller and has already established exact ownership
	// under the volume-name stripe. This adapter adds mutation authorization and
	// the mandatory postcheck around the raw sink.
	mutationErr := sink.Destroy(ctx, id) //nolint:forbidigo
	return m.completeMutation(ctx, "destroy volume", mutationErr)
}

func (m storageMutationAdapters) ensureVolumeQuota(ctx context.Context, id string, sizeMB int64) error {
	ctx, done, err := m.authorize(ctx, "ensure volume quota")
	if err != nil {
		return err
	}
	defer done()
	mutationErr := m.backend.volumes.EnsureQuota(ctx, id, sizeMB)
	return m.completeMutation(ctx, "ensure volume quota", mutationErr)
}

func (m storageMutationAdapters) renameVolume(ctx context.Context, oldName, newName string) error {
	ctx, done, err := m.authorize(ctx, "rename volume")
	if err != nil {
		return err
	}
	defer done()
	mutationErr := m.backend.volumes.RenameVolume(ctx, oldName, newName)
	return m.completeMutation(ctx, "rename volume", mutationErr)
}

func (m storageMutationAdapters) removePath(ctx context.Context, path string) error {
	ctx, done, err := m.authorize(ctx, "remove tenant path")
	if err != nil {
		return err
	}
	defer done()
	if err := ctx.Err(); err != nil {
		return err
	}
	volume, err := writablePathVolumeComponent(m.backend.cfg.VolumeDataPath, path)
	if err != nil {
		return err
	}
	wpComponent, err := parseStoragePathComponent(writablePathSubdir)
	if err != nil {
		return fmt.Errorf("validate writable-path directory name: %w", err)
	}
	mutationErr := removeManagedVolumeSubtree(m.backend.cfg.VolumeDataPath, volume, wpComponent)
	return m.completeMutation(ctx, "remove tenant path", mutationErr)
}

func (m storageMutationAdapters) prepareStatefulVolumeBinds(ctx context.Context, hostPath string, imageVolumes []string, uid, gid int) (map[string]string, error) {
	ctx, done, err := m.authorize(ctx, "prepare stateful volume binds")
	if err != nil {
		return nil, err
	}
	defer done()
	binds, mutationErr := buildStatefulVolumeBindsContext(ctx, hostPath, imageVolumes, uid, gid)
	return binds, m.completeMutation(ctx, "prepare stateful volume binds", mutationErr)
}
