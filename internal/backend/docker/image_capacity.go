package docker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/containerd/platforms"
	"github.com/distribution/reference"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/system"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
	"github.com/manifest-network/fred/internal/backend/shared"
)

const imageMiB = int64(1 << 20)

func (c *Config) validateImageCapacity() error {
	if c.ImageDataPath != "" && !filepath.IsAbs(c.ImageDataPath) {
		return errors.New("image_data_path must be absolute")
	}
	if c.ImageMaxSizeMB == 0 {
		c.ImageMaxSizeMB = 10240
	}
	if c.ImageDiskMinFreeMB == 0 {
		c.ImageDiskMinFreeMB = 2048
	}
	if c.ImageGCHighPercent == 0 {
		c.ImageGCHighPercent = 85
	}
	if c.ImageGCLowPercent == 0 {
		c.ImageGCLowPercent = 75
	}
	if c.ImageMaxSizeMB < 0 || c.ImageDiskMinFreeMB < 0 ||
		c.ImageMaxSizeMB > math.MaxInt64/imageMiB/8 ||
		c.ImageMaxSizeMB > math.MaxInt64/imageMiB-c.ImageDiskMinFreeMB {
		return errors.New("image size and disk headroom must be positive and fit in bytes")
	}
	if c.ImageGCLowPercent < 1 || c.ImageGCLowPercent >= c.ImageGCHighPercent || c.ImageGCHighPercent > 99 {
		return errors.New("image GC thresholds require 0 < low < high < 100")
	}
	return nil
}

// imageCacheDaemon deliberately excludes all container mutation capabilities.
// Docker's non-force removal is the final guard against racing container users.
type imageCacheDaemon interface {
	Info(context.Context) (system.Info, error)
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error)
	ImageList(context.Context, image.ListOptions) ([]image.Summary, error)
	ContainerList(context.Context, container.ListOptions) ([]container.Summary, error)
}

type filesystemCapacity interface {
	capacity(string) (diskCapacity, error)
}

type localFilesystemCapacity struct{}
type diskCapacity struct{ total, available, blockBytes uint64 }

func (localFilesystemCapacity) capacity(path string) (diskCapacity, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return diskCapacity{}, fmt.Errorf("statfs %s: %w", path, err)
	}
	if stat.Bsize <= 0 || stat.Blocks == 0 || stat.Blocks > math.MaxUint64/uint64(stat.Bsize) {
		return diskCapacity{}, fmt.Errorf("invalid filesystem capacity for %s", path)
	}
	return diskCapacity{total: stat.Blocks * uint64(stat.Bsize), available: stat.Bavail * uint64(stat.Bsize), blockBytes: uint64(stat.Bsize)}, nil
}

func (d diskCapacity) usage() int {
	if d.total == 0 || d.available > d.total {
		return 100
	}
	return int(100 * (1 - float64(d.available)/float64(d.total)))
}

// imageCapacityManager owns short allocation/publication critical sections.
// Active admissions exclude collection, but never hold its gate across registry
// I/O or Docker import. Durable loader debits preserve that exclusion on crash.
type imageCapacityManager struct {
	daemon     imageCacheDaemon
	runtime    *imageexec.Admitter
	loader     *imagefetch.Loader
	docker     *DockerClient
	stageRoot  string
	pins       *shared.ImagePinJournal
	backfiller *shared.ImagePinBackfiller
	fs         filesystemCapacity
	cfg        Config
	gate       chan struct{}
	owner      *imageCacheOwnership
	access     imageCacheParticipation
	active     int
	staging    int64
	probing    int64
	stageSlots chan struct{}
	probeGate  chan struct{}
}

func newImageCapacityManager(ctx context.Context, b *Backend, docker *DockerClient) (*imageCapacityManager, error) {
	info, err := docker.client.Info(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireBoundedImageStore(info); err != nil {
		return nil, err
	}
	pins, err := shared.NewImagePinJournal(b.callbackStore, b.releaseStore, b.retentionStore)
	if err != nil {
		return nil, err
	}
	manager := &imageCapacityManager{
		daemon: docker.client, runtime: docker.images, docker: docker, pins: pins,
		fs: localFilesystemCapacity{}, cfg: b.cfg, gate: make(chan struct{}, 1),
		stageSlots: make(chan struct{}, 4), probeGate: make(chan struct{}, 1),
	}
	if manager.cfg.ProductionMode {
		if err := manager.checkFilesystems(ctx, false, false); err != nil {
			return nil, err
		}
		manager.owner, err = claimImageCacheOwnership(ctx, docker.client, b.storageAuthority)
		if err != nil {
			return nil, err
		}
		manager.access = manager.owner
	} else {
		manager.access, err = claimSharedImageCache(ctx, docker.client)
		if err != nil {
			return nil, err
		}
	}
	manager.stageRoot, err = filepath.Abs(b.cfg.CallbackDBPath + ".image-staging")
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(manager.stageRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create private image staging directory: %w", err)
	}
	manager.loader, err = docker.newImageLoader(manager.stageRoot, manager.cfg.ImageMaxSizeMB*imageMiB)
	if err != nil {
		return nil, fmt.Errorf("create bounded image loader: %w", err)
	}
	if err := manager.loader.CleanupAbandoned(); err != nil {
		return nil, fmt.Errorf("recover image staging: %w", err)
	}
	pending, err := manager.loader.PendingBytes()
	if err != nil {
		return nil, err
	}
	imageImportPendingBytes.Set(float64(pending))
	manager.backfiller, err = newImagePinBackfiller(b, docker, pins)
	if err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *imageCapacityManager) lock(ctx context.Context) error {
	select {
	case m.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *imageCapacityManager) unlock() { <-m.gate }

func (m *imageCapacityManager) paths(ctx context.Context) ([]string, error) {
	info, err := m.daemon.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover Docker image filesystem: %w", err)
	}
	if !filepath.IsAbs(info.DockerRootDir) {
		return nil, errors.New("docker daemon returned no absolute data root for image headroom")
	}
	containerd := daemonUsesContainerd(info)
	root := info.DockerRootDir
	if m.cfg.ImageDataPath != "" {
		root = m.cfg.ImageDataPath
	} else if containerd {
		return nil, errors.New("containerd image store requires an explicit image_data_path")
	}
	paths := []string{root}
	if root != info.DockerRootDir {
		paths = append(paths, info.DockerRootDir)
	}
	if m.stageRoot != "" {
		paths = append(paths, m.stageRoot)
	}
	for _, path := range []string{m.cfg.CallbackDBPath, m.cfg.ReleasesDBPath, m.cfg.RetentionDBPath, m.cfg.DiagnosticsDBPath} {
		if path != "" {
			paths = append(paths, filepath.Dir(path))
		}
	}
	if m.cfg.VolumeDataPath != "" {
		paths = append(paths, m.cfg.VolumeDataPath)
	}
	return paths, nil
}

func (m *imageCapacityManager) headroom(ctx context.Context, pulling bool) error {
	if err := m.access.verify(ctx); err != nil {
		return err
	}
	return m.checkFilesystems(ctx, pulling, true)
}

func (m *imageCapacityManager) checkFilesystems(ctx context.Context, pulling, checkSpace bool) error {
	paths, err := m.paths(ctx)
	if err != nil {
		return err
	}
	var pending int64
	if m.loader != nil {
		pending, err = m.loader.UnknownBytes()
		if err != nil {
			return err
		}
	}
	for index, path := range paths {
		floor := m.cfg.ImageDiskMinFreeMB * imageMiB
		// Stage downloads are bounded before they can reach Docker. This is a
		// sampled headroom requirement, not a physical reservation against
		// concurrent unrelated filesystem writers.
		if pulling && (path == m.stageRoot || (m.stageRoot == "" && index == 0)) {
			floor += m.cfg.ImageMaxSizeMB * imageMiB
		}
		if checkSpace {
			if err := requireImageImportSpace(m.fs, []string{path}, pending, floor); err != nil {
				return fmt.Errorf("image disk admission: %w", err)
			}
		} else if _, err := m.fs.capacity(path); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) requireImageDiskHeadroom(ctx context.Context) error {
	if b.imageCapacity == nil {
		return nil // Backends without an executable Docker runtime have no image store.
	}
	return b.imageCapacity.headroom(ctx, false)
}

// selectedPin includes the restore source because its retention row is the
// authority for content until the destination's own pin is committed.
func (m *imageCapacityManager) selectedPin(mutations *storageMutations, ref string) (*shared.ImagePin, error) {
	var payload []byte
	lease := mutations.leaseUUID
	if subject := mutations.operationSubject; subject.Valid() {
		payload = subject.Intent().Manifest()
		if source := subject.Intent().SourceLeaseUUID(); source != "" {
			lease = source
		}
	} else if subject := mutations.maintenanceSubject; subject.Valid() {
		target, ok := subject.TargetRelease()
		if !ok {
			return nil, errors.New("image pin has no Started maintenance release")
		}
		payload = target.Manifest
	} else {
		return nil, errors.New("image pin has no Started operation")
	}
	return m.pins.Lookup(lease, payload, ref)
}

func (m *imageCapacityManager) prepare(ctx context.Context, mutations *storageMutations, ref string, pull bool) (imageexec.Image, error) {
	admission, err := m.beginAdmission(ctx, mutations, ref)
	if err != nil {
		return imageexec.Image{}, err
	}
	defer admission.close()
	pin := admission.state.pin
	info, err := m.daemon.Info(ctx)
	if err != nil {
		return imageexec.Image{}, err
	}
	if daemonUsesContainerd(info) {
		if err := requireBoundedImageStore(info); err != nil {
			return imageexec.Image{}, err
		}
	}
	resolved, err := m.resolveImage(ctx, ref, pin, pull)
	if err != nil {
		return imageexec.Image{}, err
	}
	if daemonUsesContainerd(info) {
		if err := m.verifyUnpacked(ctx, mutations.inspectionOrigin, resolved); err != nil {
			return imageexec.Image{}, err
		}
	}
	if err := m.lock(ctx); err != nil {
		return imageexec.Image{}, err
	}
	defer m.unlock()
	if err := m.pins.Pin(mutations.inspectionOrigin, ref, resolved.image.ID(), resolved.pullDigest, resolved.image.Platform(), resolved.importBytes); err != nil {
		return imageexec.Image{}, err
	}
	return resolved.image, nil
}

func (m *imageCapacityManager) verifyUnpacked(ctx context.Context, origin shared.ImageInspectionOrigin, resolved resolvedImage) error {
	select {
	case m.probeGate <- struct{}{}:
		defer func() { <-m.probeGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := m.docker.requireImageInspectionsSettled(ctx); err != nil {
		return err
	}
	allocation, err := m.reserveUnpack(ctx, resolved.importBytes)
	if err != nil {
		return err
	}
	defer allocation.close()
	return m.docker.verifyImageUnpacked(ctx, resolved.image, origin)
}

// resolvedImage keeps verified allocation evidence with the exact local image.
// A legacy zero allowance never authorizes potentially deferred extraction.
type resolvedImage struct {
	image       imageexec.Image
	pullDigest  string
	importBytes int64
}

// resolveImage never falls back to a mutable tag when pinned content is absent.
// Its caller serializes collection until the returned identity is persisted.
func (m *imageCapacityManager) resolveImage(ctx context.Context, ref string, pin *shared.ImagePin, pull bool) (resolvedImage, error) {
	var resolved resolvedImage
	var err error
	switch {
	case pin != nil:
		resolved.image, err = m.runtime.ReAdmit(ctx, pin.ImageID, pin.Platform, ref)
		resolved.pullDigest, resolved.importBytes = pin.PullDigest, pin.ImportBytes
		if err != nil && errdefs.IsNotFound(err) && pin.PullDigest != "" {
			resolved, err = m.ingestRecovery(ctx, ref, pin)
			if err == nil && (resolved.image.ID() != pin.ImageID || !platforms.OnlyStrict(pin.Platform).Match(resolved.image.Platform())) {
				return resolvedImage{}, errors.New("imported image differs from the pinned execution identity")
			}
		}
	case pull:
		resolved, err = m.ingest(ctx, ref, ref)
	default:
		resolved.image, err = m.runtime.Admit(ctx, ref)
		var required *imageexec.MaterializationRequired
		if errors.As(err, &required) {
			resolved, err = m.ingest(ctx, ref, required.Reference())
			if err == nil && resolved.image.ID() != required.ID() {
				return resolvedImage{}, errors.New("imported image differs from the selected platform manifest")
			}
		}
	}
	if err != nil {
		return resolvedImage{}, err
	}
	inspection, err := m.daemon.ImageInspect(ctx, resolved.image.ID())
	if err != nil {
		return resolvedImage{}, err
	}
	if inspection.Size < 0 {
		return resolvedImage{}, errors.New("docker returned a negative image size")
	}
	if err := m.headroom(ctx, false); err != nil {
		return resolvedImage{}, err
	}
	if resolved.pullDigest == "" {
		resolved.pullDigest = imageRecoveryDigest(ref, resolved.image.ID(), inspection)
	}
	info, err := m.daemon.Info(ctx)
	if err != nil {
		return resolvedImage{}, err
	}
	if daemonUsesContainerd(info) && resolved.importBytes == 0 {
		if resolved.pullDigest == "" {
			return resolvedImage{}, errors.New("legacy containerd image needs an immutable repository digest for bounded extraction")
		}
		recoveryPin := &shared.ImagePin{ImageID: resolved.image.ID(), PullDigest: resolved.pullDigest, Platform: resolved.image.Platform(), ImportBytes: resolved.importBytes}
		verified, err := m.ingestRecovery(ctx, ref, recoveryPin)
		if err != nil {
			return resolvedImage{}, err
		}
		if verified.image.ID() != resolved.image.ID() || !platforms.OnlyStrict(resolved.image.Platform()).Match(verified.image.Platform()) {
			return resolvedImage{}, errors.New("verified image differs from the local execution identity")
		}
		resolved = verified
	}
	return resolved, nil
}

// ingest is the only backend route from a registry reference to daemon image
// writes. The loader owns the exact verified bytes; Docker never re-fetches
// content from a tenant-controlled registry after validation.
func (m *imageCapacityManager) ingest(ctx context.Context, original, source string) (resolvedImage, error) {
	return m.ingestBounded(ctx, original, source, m.loader, m.cfg.ImageMaxSizeMB*imageMiB)
}

// Recovery has already selected a durable immutable identity. Its verification
// budget follows that admission's saved bound, never a subsequently lowered
// new-image policy. Legacy rows use a finite budget derived from host capacity.
func (m *imageCapacityManager) ingestRecovery(ctx context.Context, original string, pin *shared.ImagePin) (resolvedImage, error) {
	budget := pin.ImportBytes
	if budget == 0 {
		paths, err := m.paths(ctx)
		if err != nil {
			return resolvedImage{}, err
		}
		pending, err := m.loader.PendingBytes()
		if err != nil {
			return resolvedImage{}, err
		}
		budget = math.MaxInt64 / 8
		floor := m.cfg.ImageDiskMinFreeMB * imageMiB
		for _, path := range paths {
			space, err := m.fs.capacity(path)
			if err != nil {
				return resolvedImage{}, err
			}
			if uint64(floor) > space.available || uint64(pending) > space.available-uint64(floor) {
				return resolvedImage{}, errors.New("no headroom for bounded legacy image recovery")
			}
			// One staging allowance plus a two-allowance peak import ceiling.
			budget = min(budget, int64(min(uint64(math.MaxInt64), space.available-uint64(floor)-uint64(pending)))/3)
		}
	}
	loader, err := m.loader.WithBudget(budget)
	if err != nil {
		return resolvedImage{}, err
	}
	return m.ingestBounded(ctx, original, pin.PullDigest, loader, budget)
}

func (m *imageCapacityManager) ingestBounded(ctx context.Context, original, source string, loader *imagefetch.Loader, budget int64) (resolvedImage, error) {
	info, err := m.daemon.Info(ctx)
	if err != nil {
		return resolvedImage{}, err
	}
	if err := requireBoundedImageStore(info); err != nil {
		return resolvedImage{}, err
	}
	if err := m.requireSettledHelpers(ctx, info); err != nil {
		return resolvedImage{}, err
	}
	resolution, err := loader.Resolve(ctx, source, daemonImagePlatform(info))
	if err != nil {
		return resolvedImage{}, fmt.Errorf("resolve immutable image: %w", err)
	}
	if cached, ok, err := m.cachedImage(ctx, original, resolution); err != nil || ok {
		return cached, err
	}
	// Classic Docker has already extracted locally available content. Looking
	// up the resolved digest reuses it without importing mutable tag content.
	if !daemonUsesContainerd(info) {
		local, localErr := m.runtime.Admit(ctx, resolution.SourceReference())
		if localErr == nil {
			inspection, err := m.daemon.ImageInspect(ctx, local.ID())
			if err != nil {
				return resolvedImage{}, err
			}
			if inspection.Size < 0 || inspection.Size > budget {
				return resolvedImage{}, errors.New("new image exceeds image_max_size_mb")
			}
			admitted, err := m.runtime.ReAdmit(ctx, local.ID(), local.Platform(), original)
			return resolvedImage{image: admitted, pullDigest: resolution.SourceReference()}, err
		}
	}
	staging, err := m.reserveStaging(ctx, budget)
	if err != nil {
		return resolvedImage{}, err
	}
	defer staging.close()
	prepared, err := loader.PrepareResolved(ctx, resolution)
	if err != nil {
		return resolvedImage{}, fmt.Errorf("prepare bounded image: %w", err)
	}
	defer func() { _ = prepared.Close() }()
	admission, err := m.reserveImport(ctx, loader, prepared)
	if err != nil {
		return resolvedImage{}, err
	}
	defer m.observeImportDebit()
	defer func() { _ = admission.Close() }()
	loaded, err := loader.ImportAdmitted(ctx, admission)
	if err != nil {
		return resolvedImage{}, err
	}
	id := loaded.ConfigID()
	if daemonUsesContainerd(info) {
		id = loaded.ManifestID()
	}
	admitted, err := m.runtime.ReAdmit(ctx, id, loaded.Platform(), original)
	return resolvedImage{image: admitted, pullDigest: loaded.SourceReference(), importBytes: prepared.ImportBytes()}, err
}

// cachedImage reuses only exact immutable content vouched for by an existing
// pin. A tag-resolution capability chooses the lookup; arbitrary caller IDs
// cannot manufacture a host-wide verification record.
func (m *imageCapacityManager) cachedImage(ctx context.Context, original string, resolution imagefetch.Resolution) (resolvedImage, bool, error) {
	pins, err := m.pins.List()
	if err != nil {
		return resolvedImage{}, false, err
	}
	for _, pin := range pins {
		if pin.ImportBytes == 0 || pin.PullDigest != resolution.SourceReference() || !platforms.OnlyStrict(resolution.Platform()).Match(pin.Platform) {
			continue
		}
		admitted, err := m.runtime.ReAdmit(ctx, pin.ImageID, pin.Platform, original)
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return resolvedImage{}, false, err
		}
		return resolvedImage{image: admitted, pullDigest: pin.PullDigest, importBytes: pin.ImportBytes}, true, nil
	}
	return resolvedImage{}, false, nil
}

// reserveImport atomically checks all outstanding allocations and publishes
// this import's durable debit. ImageLoad runs after the gate is released.
func (m *imageCapacityManager) reserveImport(ctx context.Context, loader *imagefetch.Loader, prepared *imagefetch.Prepared) (*imagefetch.ImportAdmission, error) {
	if err := m.lock(ctx); err != nil {
		return nil, err
	}
	defer m.unlock()
	if err := m.access.verify(ctx); err != nil {
		return nil, err
	}
	info, err := m.daemon.Info(ctx)
	if err != nil {
		return nil, err
	}
	if err := m.snapshotSettledHelpers(ctx, info); err != nil {
		return nil, err
	}
	if err := m.headroom(ctx, false); err != nil {
		return nil, err
	}
	if err := m.importHeadroom(ctx, prepared.ImportBytes()); err != nil {
		return nil, err
	}
	admission, err := loader.ReserveImport(ctx, prepared)
	m.observeImportDebit()
	return admission, err
}

func (m *imageCapacityManager) requireSettledHelpers(ctx context.Context, info system.Info) error {
	if !daemonUsesContainerd(info) {
		return nil
	}
	return m.docker.requireImageInspectionsSettled(ctx)
}

// snapshotSettledHelpers never waits while an allocation/GC gate is owned.
// A live creator may finish after this snapshot; callers can wait for it before
// reacquiring the gate, while unknown receipts remain a conservative refusal.
func (m *imageCapacityManager) snapshotSettledHelpers(ctx context.Context, info system.Info) error {
	if !daemonUsesContainerd(info) {
		return nil
	}
	if m.docker == nil || m.docker.inspections == nil {
		return errors.New("docker image inspection owner is not bound")
	}
	owner := m.docker.inspections
	if err := errors.Join(ctx.Err(), owner.authority(), owner.lifetime.Err()); err != nil {
		return err
	}
	changed, err := owner.admissionWait()
	if err != nil {
		return err
	}
	if changed != nil {
		return errors.New("image inspection Create remains in progress")
	}
	return nil
}

func (m *imageCapacityManager) observeImportDebit() {
	if pending, err := m.loader.PendingBytes(); err == nil {
		imageImportPendingBytes.Set(float64(pending))
	}
}

func (m *imageCapacityManager) importHeadroom(ctx context.Context, extra int64) error {
	info, err := m.daemon.Info(ctx)
	if err != nil {
		return err
	}
	pending, err := m.loader.PendingBytes()
	if err != nil {
		return err
	}
	if extra < 0 || extra > math.MaxInt64-pending || m.staging > math.MaxInt64-pending-extra ||
		m.probing > math.MaxInt64-pending-extra-m.staging {
		return errors.New("image import allowance exceeds accounting range")
	}
	paths := []string{info.DockerRootDir}
	if m.cfg.ImageDataPath != "" {
		paths = append(paths, m.cfg.ImageDataPath)
	}
	return requireImageImportSpace(m.fs, paths, pending+extra+m.staging+m.probing, m.cfg.ImageDiskMinFreeMB*imageMiB)
}

func daemonUsesContainerd(info system.Info) bool {
	if strings.Contains(info.Driver, "snapshotter") {
		return true
	}
	for _, status := range info.DriverStatus {
		if len(status) == 2 && strings.Contains(status[1], "containerd") {
			return true
		}
	}
	return false
}

// The peak allowance models diff-based overlay extraction. Copying an entire
// parent snapshot per layer (for example the vfs driver) needs a different
// budget and cannot consume the same import capability.
func requireBoundedImageStore(info system.Info) error {
	if info.Driver == "overlay2" && !daemonUsesContainerd(info) {
		return nil
	}
	if info.Driver == "overlayfs" && daemonUsesContainerd(info) {
		return nil
	}
	return fmt.Errorf("bounded image import requires overlay2 or containerd overlayfs, got %q", info.Driver)
}

func daemonImagePlatform(info system.Info) ocispec.Platform {
	return platforms.Normalize(ocispec.Platform{OS: info.OSType, Architecture: info.Architecture})
}

func requireImageImportSpace(fs filesystemCapacity, paths []string, extra, floor int64) error {
	if extra < 0 || floor < 0 || extra > math.MaxInt64-floor {
		return errors.New("invalid bounded image import allocation")
	}
	for _, path := range paths {
		capacity, err := fs.capacity(path)
		if err != nil {
			return err
		}
		// Prepared content charges data in 4 KiB units. Scale the entire
		// allowance conservatively if this filesystem allocates larger blocks;
		// the normal deployed XFS layout keeps its original allowance.
		allocation := uint64(extra)
		if capacity.blockBytes > 4096 {
			factor := 1 + (capacity.blockBytes-1)/4096
			if allocation > uint64(math.MaxInt64-floor)/factor {
				return errors.New("image import allocation exceeds filesystem accounting range")
			}
			allocation *= factor
		}
		required := allocation + uint64(floor)
		if capacity.available < required {
			return fmt.Errorf("image import admission: %s has %d bytes available, requires %d", path, capacity.available, required)
		}
	}
	return nil
}

func imageRecoveryDigest(ref, id string, inspection image.InspectResponse) string {
	parsed, err := reference.ParseAnyReference(ref)
	if err != nil {
		return ""
	}
	named, ok := parsed.(reference.Named)
	if !ok {
		return ""
	}
	// With a containerd store, admitted IDs identify selected OCI manifests;
	// the content-addressed leaf can be fetched directly from its original repo.
	if inspection.Descriptor != nil && inspection.Descriptor.Digest.String() == id {
		return reference.TrimNamed(named).Name() + "@" + id
	}
	// Classic Docker IDs identify configs, so recovery needs the daemon's
	// repository manifest digest. Never borrow another registry's alias.
	for _, candidate := range inspection.RepoDigests {
		parsed, err := reference.ParseAnyReference(candidate)
		if err != nil {
			continue
		}
		canonical, ok := parsed.(reference.Canonical)
		if ok && canonical.Name() == named.Name() {
			return canonical.String()
		}
	}
	return ""
}

func (m *imageCapacityManager) collect(ctx context.Context) error {
	if m.active != 0 {
		imageGCTotal.WithLabelValues("inhibited").Inc()
		return nil
	}
	info, err := m.daemon.Info(ctx)
	if err != nil {
		return err
	}
	if err := m.snapshotSettledHelpers(ctx, info); err != nil {
		imageGCTotal.WithLabelValues("inhibited").Inc()
		return err
	}
	if m.loader != nil {
		pending, err := m.loader.PendingBytes()
		if err != nil {
			return err
		}
		imageImportPendingBytes.Set(float64(pending))
		if pending != 0 {
			imageGCTotal.WithLabelValues("inhibited").Inc()
			return nil
		}
	}
	if err := m.access.verify(ctx); err != nil {
		return err
	}
	protected, err := m.pins.Collect(ctx)
	if err != nil {
		imageGCTotal.WithLabelValues("inhibited").Inc()
		return err
	}
	if !protected.Complete() {
		imageGCTotal.WithLabelValues("inhibited").Inc()
		return nil
	}
	if m.owner == nil {
		imageGCTotal.WithLabelValues("shared").Inc()
		return nil // Development daemons can be shared with independent backends.
	}
	if err := m.owner.verify(ctx); err != nil {
		return err
	}
	paths, err := m.paths(ctx)
	if err != nil {
		return err
	}
	capacity, err := m.fs.capacity(paths[0])
	if err != nil {
		return err
	}
	if m.loader != nil {
		if _, err := m.loader.PendingBytes(); err != nil {
			return err
		}
	}
	if capacity.usage() < m.cfg.ImageGCHighPercent && m.headroom(ctx, true) == nil {
		imageGCTotal.WithLabelValues("below_threshold").Inc()
		return nil
	}
	containers, err := m.daemon.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return fmt.Errorf("image GC container inventory: %w", err)
	}
	// The daemon's full inventory also protects unmanaged/stopped users.
	containerImages := make(map[string]bool, len(containers))
	for _, c := range containers {
		containerImages[c.ImageID] = true
	}
	images, err := m.daemon.ImageList(ctx, image.ListOptions{All: true})
	if err != nil {
		return fmt.Errorf("image GC image inventory: %w", err)
	}
	slices.SortFunc(images, func(a, b image.Summary) int {
		if a.Created < b.Created {
			return -1
		}
		if a.Created > b.Created {
			return 1
		}
		return strings.Compare(a.ID, b.ID)
	})
	for _, candidate := range images {
		if !protected.CanRemove(candidate.ID) || containerImages[candidate.ID] {
			continue
		}
		if err := m.owner.remove(ctx, protected, candidate.ID); err != nil {
			if errdefs.IsConflict(err) || errdefs.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("remove unused image: %w", err)
		}
		imageGCTotal.WithLabelValues("removed").Inc()
		capacity, err = m.fs.capacity(paths[0])
		if err != nil {
			return err
		}
		if capacity.usage() <= m.cfg.ImageGCLowPercent && m.headroom(ctx, true) == nil {
			break
		}
	}
	return nil
}

func (b *Backend) imageGCLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(b.stopCtx, 30*time.Second)
			err := b.collectImagesSafely(ctx)
			cancel()
			if err != nil {
				b.logger.Warn("image garbage collection deferred", "error", err)
			}
		}
	}
}

// A collector iteration is a background/foreign-daemon boundary. A panic
// retains every remaining image and cannot silently terminate future sweeps.
func (b *Backend) collectImagesSafely(ctx context.Context) (err error) {
	defer func() {
		if value := recover(); value != nil {
			imageGCTotal.WithLabelValues("panic").Inc()
			err = fmt.Errorf("image collection panicked: %v", value)
		}
		if err != nil {
			imageGCTotal.WithLabelValues("error").Inc()
		}
	}()
	return b.collectImages(ctx)
}

func (b *Backend) collectImages(ctx context.Context) error {
	if err := b.imageCapacity.lock(ctx); err != nil {
		return err
	}
	defer b.imageCapacity.unlock()
	ctx, done, err := b.authorizeStorageMutation(ctx, "collect images")
	if err != nil {
		return err
	}
	defer done()
	err = b.imageCapacity.collect(ctx)
	return b.completeStorageMutation(ctx, "collect images", err)
}
