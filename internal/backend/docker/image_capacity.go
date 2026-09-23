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
type diskCapacity struct{ total, available, device, blockBytes uint64 }

func (localFilesystemCapacity) capacity(path string) (diskCapacity, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return diskCapacity{}, fmt.Errorf("statfs %s: %w", path, err)
	}
	if stat.Bsize <= 0 || stat.Blocks == 0 || stat.Blocks > math.MaxUint64/uint64(stat.Bsize) {
		return diskCapacity{}, fmt.Errorf("invalid filesystem capacity for %s", path)
	}
	var identity unix.Stat_t
	if err := unix.Stat(path, &identity); err != nil {
		return diskCapacity{}, err
	}
	return diskCapacity{total: stat.Blocks * uint64(stat.Bsize), available: stat.Bavail * uint64(stat.Bsize), device: identity.Dev, blockBytes: uint64(stat.Bsize)}, nil
}

func (d diskCapacity) usage() int {
	if d.total == 0 || d.available > d.total {
		return 100
	}
	return int(100 * (1 - float64(d.available)/float64(d.total)))
}

// imageCapacityManager serializes pulls, pin publication and GC. A collector
// can never remove the just-pulled image in the interval before its pin commits.
// Its disk checks also cover the filesystems housing all control journals.
type imageCapacityManager struct {
	daemon    imageCacheDaemon
	runtime   *imageexec.Admitter
	loader    *imagefetch.Loader
	docker    *DockerClient
	stageRoot string
	pins      *shared.ImagePinJournal
	fs        filesystemCapacity
	cfg       Config
	gate      chan struct{}
	owner     *imageCacheOwnership
	access    imageCacheParticipation
}

func newImageCapacityManager(ctx context.Context, b *Backend, docker *DockerClient) (*imageCapacityManager, error) {
	pins, err := shared.NewImagePinJournal(b.callbackStore, b.releaseStore, b.retentionStore)
	if err != nil {
		return nil, err
	}
	manager := &imageCapacityManager{
		daemon: docker.client, runtime: docker.images, docker: docker, pins: pins,
		fs: localFilesystemCapacity{}, cfg: b.cfg, gate: make(chan struct{}, 1),
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
	if _, err := manager.loader.PendingBytes(); err != nil {
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
		pending, err = m.loader.PendingBytes()
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
	if err := m.lock(ctx); err != nil {
		return imageexec.Image{}, err
	}
	defer m.unlock()
	info, err := m.daemon.Info(ctx)
	if err != nil {
		return imageexec.Image{}, err
	}
	if daemonUsesContainerd(info) {
		if err := requireBoundedImageStore(info); err != nil {
			return imageexec.Image{}, err
		}
		if err := m.docker.requireImageInspectionsSettled(); err != nil {
			return imageexec.Image{}, err
		}
	}
	if err := m.collect(ctx); err != nil {
		return imageexec.Image{}, err
	}
	pin, err := m.selectedPin(mutations, ref)
	if err != nil {
		return imageexec.Image{}, err
	}
	resolved, err := m.resolveImage(ctx, ref, pin, pull)
	if err != nil {
		return imageexec.Image{}, err
	}
	if daemonUsesContainerd(info) {
		if err := m.importHeadroom(ctx, resolved.importBytes); err != nil {
			return imageexec.Image{}, err
		}
		if err := m.docker.verifyImageUnpacked(ctx, resolved.image, mutations.inspectionOrigin); err != nil {
			return imageexec.Image{}, err
		}
	}
	if err := m.pins.Pin(mutations.inspectionOrigin, ref, resolved.image.ID(), resolved.pullDigest, resolved.image.Platform(), resolved.importBytes); err != nil {
		return imageexec.Image{}, err
	}
	return resolved.image, nil
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
			resolved, err = m.ingest(ctx, ref, pin.PullDigest)
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
	if inspection.Size < 0 || inspection.Size > m.cfg.ImageMaxSizeMB*imageMiB {
		// A pinned image belongs to another restorable generation and must not
		// be destroyed if the operator has since lowered the admission cap.
		if pin == nil {
			inventory, pinErr := m.pins.Collect(ctx)
			if pinErr == nil && m.owner != nil {
				_ = m.owner.remove(ctx, inventory, resolved.image.ID())
			}
		}
		return resolvedImage{}, fmt.Errorf("image size %d exceeds image_max_size_mb %d", inspection.Size, m.cfg.ImageMaxSizeMB)
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
		verified, err := m.ingest(ctx, ref, resolved.pullDigest)
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
	if err := m.headroom(ctx, true); err != nil {
		return resolvedImage{}, err
	}
	info, err := m.daemon.Info(ctx)
	if err != nil {
		return resolvedImage{}, err
	}
	if err := requireBoundedImageStore(info); err != nil {
		return resolvedImage{}, err
	}
	prepared, err := m.loader.Prepare(ctx, source, daemonImagePlatform(info))
	if err != nil {
		return resolvedImage{}, fmt.Errorf("prepare bounded image: %w", err)
	}
	defer func() { _ = prepared.Close() }()
	if err := m.access.verify(ctx); err != nil {
		return resolvedImage{}, err
	}
	if err := m.headroom(ctx, false); err != nil {
		return resolvedImage{}, err
	}
	if err := m.importHeadroom(ctx, prepared.ImportBytes()); err != nil {
		return resolvedImage{}, err
	}
	loaded, err := m.loader.Import(ctx, prepared)
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

func (m *imageCapacityManager) importHeadroom(ctx context.Context, extra int64) error {
	info, err := m.daemon.Info(ctx)
	if err != nil {
		return err
	}
	pending, err := m.loader.PendingBytes()
	if err != nil {
		return err
	}
	if extra < 0 || extra > math.MaxInt64-pending {
		return errors.New("image import allowance exceeds accounting range")
	}
	paths := []string{info.DockerRootDir}
	if m.cfg.ImageDataPath != "" {
		paths = append(paths, m.cfg.ImageDataPath)
	}
	return requireImageImportSpace(m.fs, paths, pending+extra, m.cfg.ImageDiskMinFreeMB*imageMiB)
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
	if err := m.access.verify(ctx); err != nil {
		return err
	}
	protected, err := m.pins.Collect(ctx)
	if err != nil {
		return err
	}
	if m.owner == nil {
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
			err := b.collectImages(ctx)
			cancel()
			if err != nil {
				b.logger.Warn("image garbage collection deferred", "error", err)
			}
		}
	}
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
