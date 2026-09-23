package docker

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/system"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

type imageCapacityFS map[string]diskCapacity

func (f imageCapacityFS) capacity(path string) (diskCapacity, error) {
	capacity, ok := f[path]
	if !ok {
		return diskCapacity{}, errors.New("filesystem unavailable")
	}
	return capacity, nil
}

func imageCapacityFixture(t *testing.T) (*imageCapacityManager, *dockerSDKView, imageCapacityFS) {
	t.Helper()
	b := newBackendForTest(&mockDockerClient{}, nil)
	attachBoundOperationHandoffStores(t, b)
	pins, err := shared.NewImagePinJournal(b.callbackStore, b.releaseStore, b.retentionStore)
	require.NoError(t, err)
	cfg := DefaultConfig()
	cfg.CallbackDBPath = "/journals/callbacks.db"
	cfg.ReleasesDBPath = "/journals/releases.db"
	cfg.RetentionDBPath = "/journals/retention.db"
	cfg.DiagnosticsDBPath = "/journals/diagnostics.db"
	cfg.ImageMaxSizeMB = 10
	cfg.ImageDiskMinFreeMB = 2
	fs := imageCapacityFS{
		"/images":   {total: 100 * uint64(imageMiB), available: 50 * uint64(imageMiB), device: 1},
		"/journals": {total: 100 * uint64(imageMiB), available: 50 * uint64(imageMiB), device: 2},
	}
	daemon := &dockerSDKView{
		info:          func(context.Context) (system.Info, error) { return system.Info{DockerRootDir: "/images"}, nil },
		containerList: func(context.Context, container.ListOptions) ([]container.Summary, error) { return nil, nil },
		imageList:     func(context.Context, image.ListOptions) ([]image.Summary, error) { return nil, nil },
	}
	marker := volume.Volume{Name: imageCacheOwnerVolume, Driver: "local", Labels: map[string]string{
		imageCacheOwnerStorageLabel: b.storageIdentity.String(), imageCacheOwnerBackendLabel: b.cfg.Name, imageCacheOwnerModeLabel: "exclusive",
	}}
	daemon.volumeInspect = func(context.Context, string) (volume.Volume, error) { return marker, nil }
	owner, err := claimImageCacheOwnership(t.Context(), daemon, b.storageAuthority)
	require.NoError(t, err)
	return &imageCapacityManager{daemon: daemon, pins: pins, fs: fs, cfg: cfg, gate: make(chan struct{}, 1), owner: owner, access: owner}, daemon, fs
}

func TestImageCapacityRequiresIndependentProductionFilesystem(t *testing.T) {
	m, _, fs := imageCapacityFixture(t)
	m.cfg.ProductionMode = true
	require.NoError(t, m.headroom(t.Context(), true))
	fs["/journals"] = fs["/images"]
	require.ErrorContains(t, m.headroom(t.Context(), false), "separate filesystem")
	m.cfg.ProductionMode = false
	require.NoError(t, m.headroom(t.Context(), false))
	m.cfg.ProductionMode = true
	fs["/journals"] = diskCapacity{total: 100 * uint64(imageMiB), available: 50 * uint64(imageMiB), device: 2}
	m.cfg.VolumeDataPath = "/volumes"
	fs["/volumes"] = fs["/images"]
	require.ErrorContains(t, m.headroom(t.Context(), true), "separate filesystem")
}

func TestImageCapacityContainerdRequiresExplicitIsolatedContentPath(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	m.cfg.ProductionMode = true
	daemon.info = func(context.Context) (system.Info, error) {
		return system.Info{DockerRootDir: "/images", Driver: "overlayfs", DriverStatus: [][2]string{{"driver-type", "io.containerd.snapshotter.v1"}}}, nil
	}
	require.ErrorContains(t, m.headroom(t.Context(), true), "explicit image_data_path")
	m.cfg.ImageDataPath = "/containerd"
	fs["/containerd"] = fs["/images"]
	require.NoError(t, m.headroom(t.Context(), true))
	fs["/containerd"] = fs["/journals"]
	require.ErrorContains(t, m.headroom(t.Context(), true), "share the isolated")
}

func TestImageCapacityRefusesPullAndLaunchBeforeDiskFloor(t *testing.T) {
	m, _, fs := imageCapacityFixture(t)
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: 11 * uint64(imageMiB)}
	require.ErrorContains(t, m.headroom(t.Context(), true), "requires 12582912")
	require.NoError(t, m.headroom(t.Context(), false))
	fs["/journals"] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(imageMiB)}
	require.ErrorContains(t, m.headroom(t.Context(), false), "/journals")
	delete(fs, "/journals")
	require.ErrorContains(t, m.headroom(t.Context(), false), "unavailable")
}

func TestImageCapacityCollectsOldUnusedImagesToLowWatermark(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: 10 * uint64(imageMiB)}
	daemon.containerList = func(_ context.Context, opts container.ListOptions) ([]container.Summary, error) {
		require.True(t, opts.All)
		return []container.Summary{{ImageID: "running"}, {ImageID: "stopped"}}, nil
	}
	daemon.imageList = func(context.Context, image.ListOptions) ([]image.Summary, error) {
		return []image.Summary{{ID: fixtureImageID("new"), Created: 4}, {ID: "running", Created: 0}, {ID: fixtureImageID("old"), Created: 1}, {ID: "stopped", Created: 2}}, nil
	}
	var removed []string
	daemon.imageRemove = func(_ context.Context, id string, opts image.RemoveOptions) ([]image.DeleteResponse, error) {
		require.False(t, opts.Force)
		require.False(t, opts.PruneChildren)
		removed = append(removed, id)
		fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: 30 * uint64(imageMiB)}
		return nil, nil
	}
	require.NoError(t, m.collect(t.Context()))
	require.Equal(t, []string{fixtureImageID("old")}, removed)
	// The collector uses actual filesystem availability, not image.Size sums,
	// so shared Docker layers cannot falsely satisfy the stop threshold.
	require.NoError(t, m.collect(t.Context()))
	require.Len(t, removed, 1)
}

func TestImageCapacityInventoryErrorKeepsImages(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: 10 * uint64(imageMiB)}
	daemon.containerList = func(context.Context, container.ListOptions) ([]container.Summary, error) {
		return nil, errors.New("inventory failed")
	}
	daemon.imageRemove = func(context.Context, string, image.RemoveOptions) ([]image.DeleteResponse, error) {
		t.Fatal("incomplete inventory must never remove an image")
		return nil, nil
	}
	require.ErrorContains(t, m.collect(t.Context()), "inventory failed")
}

func TestImageCapacityPreservesConflictsWithoutForcedDeletion(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: 10 * uint64(imageMiB)}
	daemon.imageList = func(context.Context, image.ListOptions) ([]image.Summary, error) {
		return []image.Summary{{ID: fixtureImageID("many-tags"), RepoTags: []string{"example/a:1", "example/b:1"}}}, nil
	}
	daemon.imageRemove = func(_ context.Context, id string, opts image.RemoveOptions) ([]image.DeleteResponse, error) {
		require.Equal(t, fixtureImageID("many-tags"), id, "mutable tag aliases must not become deletion targets")
		require.False(t, opts.Force)
		return nil, errdefs.Conflict(errors.New("image acquired a container user or multiple tags"))
	}
	require.NoError(t, m.collect(t.Context()))
	require.Error(t, m.headroom(t.Context(), true), "unreclaimable capacity must refuse another pull")
}

func TestImageCapacitySharedDevelopmentDaemonNeverDeletesImages(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	m.owner = nil
	daemon.volumeInspect = func(context.Context, string) (volume.Volume, error) {
		return volume.Volume{Name: imageCacheOwnerVolume, Driver: "local", Labels: map[string]string{imageCacheOwnerModeLabel: "shared"}}, nil
	}
	shared, err := claimSharedImageCache(t.Context(), daemon)
	require.NoError(t, err)
	m.access = shared
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: 10 * uint64(imageMiB)}
	daemon.imageList = func(context.Context, image.ListOptions) ([]image.Summary, error) {
		t.Fatal("without durable exclusive ownership, another backend may pin every image")
		return nil, nil
	}
	require.NoError(t, m.collect(t.Context()))
	require.Error(t, m.headroom(t.Context(), true))
}

func TestImageCapacityLostOwnershipPreventsDeletion(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: 10 * uint64(imageMiB)}
	daemon.volumeInspect = func(context.Context, string) (volume.Volume, error) {
		return volume.Volume{}, errdefs.NotFound(errors.New("owner marker removed"))
	}
	require.ErrorContains(t, m.collect(t.Context()), "ownership unavailable")
}

func TestImageRecoveryDigestStaysInOriginalRegistry(t *testing.T) {
	const id = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	inspection := image.InspectResponse{RepoDigests: []string{
		"untrusted.example/alias@" + id,
		"registry.example/app@" + id,
	}}
	require.Equal(t, "registry.example/app@"+id, imageRecoveryDigest("registry.example/app:latest", id, inspection))
	require.Empty(t, imageRecoveryDigest("other.example/app:latest", id, inspection))
}

func TestImageCapacityRecoversMissingPinByDigestWithoutResolvingTag(t *testing.T) {
	m, daemon, _ := imageCapacityFixture(t)
	id := fixtureImageID("pinned")
	present := false
	ref := "registry.example/app:latest"
	pullDigest := "registry.example/app@" + fixtureImageID("manifest")
	mock := &mockDockerClient{InspectImageFn: func(_ context.Context, inspected string) (*ImageInfo, error) {
		require.Equal(t, id, inspected, "a moved mutable tag must never be re-resolved")
		if !present {
			return nil, errdefs.NotFound(errors.New("image missing"))
		}
		return &ImageInfo{ID: id}, nil
	}}
	m.runtime = mock.imageAdmitter()
	daemon.imagePull = func(_ context.Context, pulled string, _ image.PullOptions) (io.ReadCloser, error) {
		require.Equal(t, pullDigest, pulled)
		present = true
		return io.NopCloser(strings.NewReader("{}")), nil
	}
	daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: id, Size: imageMiB}, nil
	}
	pin := &shared.ImagePin{ImageID: id, PullDigest: pullDigest, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}}
	admitted, digest, err := m.resolveImage(t.Context(), ref, pin, true)
	require.NoError(t, err)
	require.Equal(t, id, admitted.ID())
	require.Equal(t, ref, admitted.Reference())
	require.Equal(t, pullDigest, digest)
}

func TestImageCapacityRejectsAndRemovesOversizedUnpinnedImage(t *testing.T) {
	m, daemon, _ := imageCapacityFixture(t)
	id := fixtureImageID("oversized")
	m.runtime = (&mockDockerClient{InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
		return &ImageInfo{ID: id}, nil
	}}).imageAdmitter()
	daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: id, Size: 11 * imageMiB}, nil
	}
	removed := false
	daemon.imageRemove = func(_ context.Context, removedID string, opts image.RemoveOptions) ([]image.DeleteResponse, error) {
		require.Equal(t, id, removedID)
		require.False(t, opts.Force)
		removed = true
		return nil, nil
	}
	_, _, err := m.resolveImage(t.Context(), "registry.example/app:1", nil, false)
	require.ErrorContains(t, err, "exceeds image_max_size_mb")
	require.True(t, removed)
	removed = false
	_, _, err = m.resolveImage(t.Context(), "registry.example/app:1", &shared.ImagePin{
		ImageID: id, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
	}, false)
	require.ErrorContains(t, err, "exceeds image_max_size_mb")
	require.False(t, removed, "lowering the cap must preserve retained images")
}

func TestImageCapacityConfigRejectsInvalidThresholds(t *testing.T) {
	for _, config := range []Config{
		{ImageGCLowPercent: 90, ImageGCHighPercent: 80},
		{ImageGCHighPercent: 100}, {ImageMaxSizeMB: -1}, {ImageDiskMinFreeMB: -1},
	} {
		require.Error(t, config.validateImageCapacity())
	}
	var config Config
	require.NoError(t, config.validateImageCapacity())
	require.Equal(t, int64(10240), config.ImageMaxSizeMB)
}
