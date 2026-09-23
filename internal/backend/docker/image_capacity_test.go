package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/system"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
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
	stageRoot := t.TempDir()
	fs := imageCapacityFS{
		"/images":   {total: 100 * uint64(imageMiB), available: 50 * uint64(imageMiB), device: 1},
		"/journals": {total: 100 * uint64(imageMiB), available: 50 * uint64(imageMiB), device: 2},
		stageRoot:   {total: 100 * uint64(imageMiB), available: 50 * uint64(imageMiB), device: 2},
	}
	daemon := &dockerSDKView{
		info: func(context.Context) (system.Info, error) {
			return system.Info{DockerRootDir: "/images", Driver: "overlay2", OSType: "linux", Architecture: "amd64"}, nil
		},
		containerList: func(context.Context, container.ListOptions) ([]container.Summary, error) { return nil, nil },
		imageList:     func(context.Context, image.ListOptions) ([]image.Summary, error) { return nil, nil },
	}
	marker := volume.Volume{Name: imageCacheOwnerVolume, Driver: "local", Labels: map[string]string{
		imageCacheOwnerStorageLabel: b.storageIdentity.String(), imageCacheOwnerBackendLabel: b.cfg.Name, imageCacheOwnerModeLabel: "exclusive",
	}}
	daemon.volumeInspect = func(context.Context, string) (volume.Volume, error) { return marker, nil }
	owner, err := claimImageCacheOwnership(t.Context(), daemon, b.storageAuthority)
	require.NoError(t, err)
	m := &imageCapacityManager{daemon: daemon, pins: pins, fs: fs, cfg: cfg, stageRoot: stageRoot, gate: make(chan struct{}, 1), owner: owner, access: owner}
	attachImageCapacityLoader(t, m, func(_ context.Context, input io.Reader) (image.LoadResponse, error) {
		_, err := io.Copy(io.Discard, input)
		return image.LoadResponse{}, errors.Join(err, errors.New("unexpected daemon import"))
	})
	return m, daemon, fs
}

func TestImageCapacityAllowsSharedProductionFilesystem(t *testing.T) {
	m, _, fs := imageCapacityFixture(t)
	m.cfg.ProductionMode = true
	require.NoError(t, m.headroom(t.Context(), true))
	fs["/journals"] = fs["/images"]
	fs[m.stageRoot] = fs["/images"]
	m.cfg.VolumeDataPath = "/volumes"
	fs["/volumes"] = fs["/images"]
	require.NoError(t, m.headroom(t.Context(), true), "bounded staging and import must support one production filesystem")
	require.NoError(t, m.headroom(t.Context(), false))
}

func TestImageCapacityContainerdRequiresExplicitContentPathAndChecksBothStores(t *testing.T) {
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
	require.NoError(t, m.headroom(t.Context(), true), "separate content and Docker filesystems are also supported")
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(imageMiB), device: 1}
	require.ErrorContains(t, m.headroom(t.Context(), true), "/images", "a healthy content store cannot hide a full Docker root")
	fs["/images"] = fs["/journals"]
	fs["/containerd"] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(imageMiB), device: 2}
	require.ErrorContains(t, m.headroom(t.Context(), true), "/containerd")
}

func TestImageCapacityRefusesPullAndLaunchBeforeDiskFloor(t *testing.T) {
	m, _, fs := imageCapacityFixture(t)
	fs[m.stageRoot] = diskCapacity{total: 100 * uint64(imageMiB), available: 11 * uint64(imageMiB)}
	require.ErrorContains(t, m.headroom(t.Context(), true), "requires 12582912")
	require.NoError(t, m.headroom(t.Context(), false))
	fs[m.stageRoot] = fs["/images"]
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(imageMiB)}
	require.ErrorContains(t, m.headroom(t.Context(), false), "/images")
	fs["/images"] = fs[m.stageRoot]
	fs["/journals"] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(imageMiB)}
	require.ErrorContains(t, m.headroom(t.Context(), false), "/journals")
	delete(fs, "/journals")
	require.ErrorContains(t, m.headroom(t.Context(), false), "unavailable")
}

func TestImageCapacityImportAllowanceAccountsForFilesystemBlockSize(t *testing.T) {
	const extra, floor = int64(8 << 10), int64(1 << 20)
	fs := imageCapacityFS{
		"/docker":  {available: uint64(floor + extra), blockBytes: 4 << 10},
		"/content": {available: uint64(floor + extra), blockBytes: 64 << 10},
	}
	require.NoError(t, requireImageImportSpace(fs, []string{"/docker"}, extra, floor))
	require.ErrorContains(t, requireImageImportSpace(fs, []string{"/docker", "/content"}, extra, floor), "/content")

	// Two 4 KiB units can each allocate a 64 KiB block. Reserve those blocks
	// on this filesystem while retaining the same byte-denominated free floor.
	const required = floor + 2*(64<<10)
	fs["/content"] = diskCapacity{available: uint64(required - 1), blockBytes: 64 << 10}
	require.ErrorContains(t, requireImageImportSpace(fs, []string{"/docker", "/content"}, extra, floor), "requires 1179648")
	fs["/content"] = diskCapacity{available: uint64(required), blockBytes: 64 << 10}
	require.NoError(t, requireImageImportSpace(fs, []string{"/docker", "/content"}, extra, floor), "the allocation scales, but the free-space floor must not")
}

func TestImageCapacityImportAllowanceRejectsOverflow(t *testing.T) {
	for _, test := range []struct {
		name       string
		extra      int64
		floor      int64
		blockBytes uint64
		wantError  string
	}{
		{name: "negative allocation", extra: -1, blockBytes: 4096, wantError: "invalid bounded image import allocation"},
		{name: "negative floor", floor: -1, blockBytes: 4096, wantError: "invalid bounded image import allocation"},
		{name: "allocation plus floor overflow", extra: math.MaxInt64, floor: 1, blockBytes: 4096, wantError: "invalid bounded image import allocation"},
		{name: "large block multiplication overflow", extra: math.MaxInt64/16 + 1, blockBytes: 64 << 10, wantError: "exceeds filesystem accounting range"},
		{name: "scaled allocation plus floor overflow", extra: math.MaxInt64 / 16, floor: 16, blockBytes: 64 << 10, wantError: "exceeds filesystem accounting range"},
		{name: "extreme block size", extra: math.MaxInt64, blockBytes: math.MaxUint64, wantError: "exceeds filesystem accounting range"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := imageCapacityFS{"/images": {available: math.MaxUint64, blockBytes: test.blockBytes}}
			require.ErrorContains(t, requireImageImportSpace(fs, []string{"/images"}, test.extra, test.floor), test.wantError,
				"arithmetic overflow must refuse import even when reported free space is enormous")
		})
	}
}

func TestImageCapacityImportRequiresBoundedStore(t *testing.T) {
	for _, test := range []struct {
		name string
		info system.Info
		ok   bool
	}{
		{name: "classic overlay2", info: system.Info{Driver: "overlay2"}, ok: true},
		{name: "containerd overlayfs", info: system.Info{Driver: "overlayfs", DriverStatus: [][2]string{{"driver-type", "io.containerd.snapshotter.v1"}}}, ok: true},
		{name: "vfs copies parent snapshots", info: system.Info{Driver: "vfs"}},
		{name: "unattested overlayfs", info: system.Info{Driver: "overlayfs"}},
		{name: "mismatched overlay2 snapshotter", info: system.Info{Driver: "overlay2", DriverStatus: [][2]string{{"driver-type", "io.containerd.snapshotter.v1"}}}},
		{name: "unknown store", info: system.Info{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := requireBoundedImageStore(test.info)
			if test.ok {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "requires overlay2 or containerd overlayfs")
			}
		})
	}
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
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(imageMiB)}
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
	fs["/images"] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(imageMiB)}
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

type imageCapacityImporter func(context.Context, io.Reader) (image.LoadResponse, error)

func (f imageCapacityImporter) ImageLoad(ctx context.Context, input io.Reader, _ ...client.ImageLoadOption) (image.LoadResponse, error) {
	return f(ctx, input)
}

func imageCapacityRegistryImage(t *testing.T, contents string) v1.Image {
	t.Helper()
	var data bytes.Buffer
	writer := tar.NewWriter(&data)
	require.NoError(t, writer.WriteHeader(&tar.Header{Name: "data", Mode: 0o644, Size: int64(len(contents))}))
	_, err := io.WriteString(writer, contents)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data.Bytes())), nil
	})
	require.NoError(t, err)
	img, err := mutate.AppendLayers(empty.Image, layer)
	require.NoError(t, err)
	config, err := img.ConfigFile()
	require.NoError(t, err)
	config = config.DeepCopy()
	config.OS = "linux"
	config.Architecture = "amd64"
	img, err = mutate.ConfigFile(img, config)
	require.NoError(t, err)
	return img
}

func attachImageCapacityLoader(t *testing.T, m *imageCapacityManager, importer imageCapacityImporter) {
	t.Helper()
	loader, err := imagefetch.NewLoader(importer, m.stageRoot, m.cfg.ImageMaxSizeMB*imageMiB)
	require.NoError(t, err)
	m.loader = loader
}

func TestImageCapacityRecoversMissingPinByDigestWithoutResolvingTag(t *testing.T) {
	m, daemon, _ := imageCapacityFixture(t)
	var requestsMu sync.Mutex
	var requestedManifests []string
	registryHandler := registry.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/manifests/") {
			requestsMu.Lock()
			requestedManifests = append(requestedManifests, r.URL.Path)
			requestsMu.Unlock()
		}
		registryHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	ref := strings.TrimPrefix(server.URL, "http://") + "/app:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	original := imageCapacityRegistryImage(t, "original pinned bytes")
	require.NoError(t, remote.Write(tag, original, remote.WithContext(t.Context()), remote.WithAuth(authn.Anonymous)))
	configID, err := original.ConfigName()
	require.NoError(t, err)
	manifestID, err := original.Digest()
	require.NoError(t, err)
	id := configID.String()
	pullDigest := tag.Context().Digest(manifestID.String()).Name()

	// The mutable tag now selects a different config and manifest. Recovery
	// must retrieve the saved digest, then execute only the saved config ID.
	require.NoError(t, remote.Write(tag, imageCapacityRegistryImage(t, "replacement tag bytes"), remote.WithContext(t.Context()), remote.WithAuth(authn.Anonymous)))
	requestsMu.Lock()
	requestedManifests = nil
	requestsMu.Unlock()
	present := false
	mock := &mockDockerClient{InspectImageFn: func(_ context.Context, inspected string) (*ImageInfo, error) {
		require.Equal(t, id, inspected, "a moved mutable tag must never be re-resolved")
		if !present {
			return nil, errdefs.NotFound(errors.New("image missing"))
		}
		return &ImageInfo{ID: id}, nil
	}}
	m.runtime = mock.imageAdmitter()
	var importedArchive []byte
	attachImageCapacityLoader(t, m, func(_ context.Context, input io.Reader) (image.LoadResponse, error) {
		var err error
		importedArchive, err = io.ReadAll(input)
		if err != nil {
			return image.LoadResponse{}, err
		}
		present = true
		return image.LoadResponse{Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})
	daemon.imageInspect = func(_ context.Context, inspected string, _ ...client.ImageInspectOption) (image.InspectResponse, error) {
		require.Equal(t, id, inspected)
		return image.InspectResponse{ID: id, Size: imageMiB}, nil
	}
	pin := &shared.ImagePin{ImageID: id, PullDigest: pullDigest, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}}
	resolved, err := m.resolveImage(t.Context(), ref, pin, true)
	require.NoError(t, err)
	require.Equal(t, id, resolved.image.ID())
	require.Equal(t, ref, resolved.image.Reference())
	require.Equal(t, pullDigest, resolved.pullDigest)
	require.Positive(t, resolved.importBytes, "the returned identity must carry its verified allocation")
	require.True(t, present, "admission must follow completion of the exact-content import")
	requestsMu.Lock()
	requests := append([]string(nil), requestedManifests...)
	requestsMu.Unlock()
	require.Equal(t, []string{"/v2/app/manifests/" + manifestID.String()}, requests)
	archive := tar.NewReader(bytes.NewReader(importedArchive))
	var configBytes []byte
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if header.Name == "blobs/sha256/"+configID.Hex {
			configBytes, err = io.ReadAll(archive)
			require.NoError(t, err)
		}
	}
	expectedConfig, err := original.RawConfigFile()
	require.NoError(t, err)
	require.Equal(t, expectedConfig, configBytes, "Docker must receive the original pinned config bytes")
	staged, err := os.ReadDir(m.stageRoot)
	require.NoError(t, err)
	require.Len(t, staged, 1, "completed import must release temporary content and retain only durable import accounting")
	require.Equal(t, "image-import-debit-v1", staged[0].Name())
	require.False(t, staged[0].IsDir())
}

func TestImageCapacityLegacyContainerdPinReingestsToEstablishAllowance(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	m.cfg.ImageDataPath = "/containerd"
	fs["/containerd"] = fs["/images"]
	daemon.info = func(context.Context) (system.Info, error) {
		return system.Info{DockerRootDir: "/images", Driver: "overlayfs", DriverStatus: [][2]string{{"driver-type", "io.containerd.snapshotter.v1"}}, OSType: "linux", Architecture: "amd64"}, nil
	}
	server := httptest.NewServer(registry.New())
	t.Cleanup(server.Close)
	ref := strings.TrimPrefix(server.URL, "http://") + "/app:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	fixture := imageCapacityRegistryImage(t, "cached content without an extraction allowance")
	require.NoError(t, remote.Write(tag, fixture, remote.WithContext(t.Context()), remote.WithAuth(authn.Anonymous)))
	manifestID, err := fixture.Digest()
	require.NoError(t, err)
	id := manifestID.String()
	pullDigest := tag.Context().Digest(id).Name()
	m.runtime = (&mockDockerClient{InspectImageFn: func(_ context.Context, inspected string) (*ImageInfo, error) {
		require.Equal(t, id, inspected)
		return &ImageInfo{ID: id}, nil // already cached; existence alone does not prove bounded extraction
	}}).imageAdmitter()
	daemon.imageInspect = func(_ context.Context, inspected string, _ ...client.ImageInspectOption) (image.InspectResponse, error) {
		require.Equal(t, id, inspected)
		return image.InspectResponse{ID: id, Size: imageMiB}, nil
	}
	imports := 0
	attachImageCapacityLoader(t, m, func(_ context.Context, input io.Reader) (image.LoadResponse, error) {
		if _, err := io.Copy(io.Discard, input); err != nil {
			return image.LoadResponse{}, err
		}
		imports++
		return image.LoadResponse{Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})
	pin := &shared.ImagePin{ImageID: id, PullDigest: pullDigest, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}}
	resolved, err := m.resolveImage(t.Context(), ref, pin, false)
	require.NoError(t, err)
	require.Equal(t, 1, imports, "a cached legacy manifest still needs verified preparation and import before deferred unpack")
	require.Equal(t, id, resolved.image.ID())
	require.Equal(t, ref, resolved.image.Reference())
	require.Equal(t, pullDigest, resolved.pullDigest)
	require.Positive(t, resolved.importBytes)
	require.Zero(t, pin.ImportBytes, "the returned evidence must await the journal's identity-bound publication")
}

func TestImageCapacityVerifiedContainerdPinNeedsNoRegistry(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	m.cfg.ImageDataPath = "/containerd"
	fs["/containerd"] = fs["/images"]
	daemon.info = func(context.Context) (system.Info, error) {
		return system.Info{DockerRootDir: "/images", Driver: "overlayfs", DriverStatus: [][2]string{{"driver-type", "io.containerd.snapshotter.v1"}}, OSType: "linux", Architecture: "amd64"}, nil
	}
	server := httptest.NewServer(http.NotFoundHandler())
	ref := strings.TrimPrefix(server.URL, "http://") + "/app:latest"
	server.Close() // a durable verified pin remains executable while its registry is offline
	id := fixtureImageID("verified-containerd-pin")
	m.runtime = (&mockDockerClient{InspectImageFn: func(_ context.Context, inspected string) (*ImageInfo, error) {
		require.Equal(t, id, inspected, "a cached pin must only inspect its immutable ID")
		return &ImageInfo{ID: id}, nil
	}}).imageAdmitter()
	daemon.imageInspect = func(_ context.Context, inspected string, _ ...client.ImageInspectOption) (image.InspectResponse, error) {
		require.Equal(t, id, inspected)
		return image.InspectResponse{ID: id, Size: imageMiB}, nil
	}
	pin := &shared.ImagePin{
		ImageID: id, PullDigest: strings.TrimSuffix(ref, ":latest") + "@" + id,
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}, ImportBytes: 3 * imageMiB,
	}
	resolved, err := m.resolveImage(t.Context(), ref, pin, false)
	require.NoError(t, err)
	require.Equal(t, id, resolved.image.ID())
	require.Equal(t, ref, resolved.image.Reference())
	require.Equal(t, pin.PullDigest, resolved.pullDigest)
	require.Equal(t, pin.ImportBytes, resolved.importBytes, "cached resolution must preserve its previously verified allowance")
}

func TestImageCapacityRechecksImportHeadroomAfterVerifiedStage(t *testing.T) {
	for _, fullPath := range []string{"/images", "/containerd"} {
		t.Run(fullPath, func(t *testing.T) {
			m, daemon, fs := imageCapacityFixture(t)
			m.cfg.ImageDataPath = "/containerd"
			daemon.info = func(context.Context) (system.Info, error) {
				return system.Info{DockerRootDir: "/images", Driver: "overlayfs", DriverStatus: [][2]string{{"driver-type", "io.containerd.snapshotter.v1"}}, OSType: "linux", Architecture: "amd64"}, nil
			}
			fs["/containerd"] = fs["/images"]
			fs[fullPath] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(m.cfg.ImageDiskMinFreeMB * imageMiB)}
			require.NoError(t, m.headroom(t.Context(), true), "the staging budget and ordinary disk floors are available")
			server := httptest.NewServer(registry.New())
			t.Cleanup(server.Close)
			ref := strings.TrimPrefix(server.URL, "http://") + "/app:latest"
			tag, err := name.NewTag(ref)
			require.NoError(t, err)
			require.NoError(t, remote.Write(tag, imageCapacityRegistryImage(t, "verified staged bytes"), remote.WithContext(t.Context()), remote.WithAuth(authn.Anonymous)))
			imported := false
			attachImageCapacityLoader(t, m, func(_ context.Context, input io.Reader) (image.LoadResponse, error) {
				imported = true
				_, err := io.Copy(io.Discard, input)
				return image.LoadResponse{}, errors.Join(err, errors.New("unexpected daemon import"))
			})
			_, err = m.resolveImage(t.Context(), ref, nil, true)
			require.ErrorContains(t, err, "image import admission: "+fullPath)
			require.False(t, imported, "verified bytes must not reach Docker without their import allocation")
			staged, err := os.ReadDir(m.stageRoot)
			require.NoError(t, err)
			require.Empty(t, staged, "refused imports must release verified staging storage")
		})
	}
}

func TestImageCapacityOutstandingImportDebitBlocksNextDownloadOnSharedFilesystem(t *testing.T) {
	m, _, fs := imageCapacityFixture(t)
	m.cfg.ProductionMode = true
	server := httptest.NewServer(registry.New())
	t.Cleanup(server.Close)
	ref := strings.TrimPrefix(server.URL, "http://") + "/app:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	require.NoError(t, remote.Write(tag, imageCapacityRegistryImage(t, "content with ambiguous daemon completion"), remote.WithContext(t.Context()), remote.WithAuth(authn.Anonymous)))
	imports := 0
	attachImageCapacityLoader(t, m, func(_ context.Context, input io.Reader) (image.LoadResponse, error) {
		imports++
		_, err := io.Copy(io.Discard, input)
		return image.LoadResponse{}, errors.Join(err, errors.New("lost daemon completion"))
	})
	_, err = m.ingest(t.Context(), ref, ref)
	require.ErrorContains(t, err, "lost daemon completion")
	pending, err := m.loader.PendingBytes()
	require.NoError(t, err)
	require.Positive(t, pending, "an ambiguous import must retain its complete allocation")

	// Every path shares one filesystem. It can fit a new maximum-size download
	// plus the ordinary floor, but not while the prior import may still unpack.
	available := (m.cfg.ImageMaxSizeMB+m.cfg.ImageDiskMinFreeMB)*imageMiB + pending - 1
	for path, capacity := range fs {
		capacity.available = uint64(available)
		capacity.device = 1
		capacity.blockBytes = 4096
		fs[path] = capacity
	}
	require.NoError(t, m.headroom(t.Context(), false), "the ordinary floor and outstanding import fit without a new download")
	server.Close()
	_, err = m.ingest(t.Context(), ref, ref)
	require.ErrorContains(t, err, "image disk admission:")
	require.ErrorContains(t, err, m.stageRoot, "the new staging allowance must be charged together with pending imports before registry I/O")
	require.Equal(t, 1, imports)
	stillPending, err := m.loader.PendingBytes()
	require.NoError(t, err)
	require.Equal(t, pending, stillPending, "refusing another download cannot forgive the earlier unknown outcome")
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
	_, err := m.resolveImage(t.Context(), "registry.example/app:1", nil, false)
	require.ErrorContains(t, err, "exceeds image_max_size_mb")
	require.True(t, removed)
	removed = false
	_, err = m.resolveImage(t.Context(), "registry.example/app:1", &shared.ImagePin{
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
