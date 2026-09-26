package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/system"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
)

func TestImageCapacityCachedResolutionCannotPinNonRunnableRecoverySource(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*ocispec.Manifest)
	}{
		{name: "artifact", mutate: func(manifest *ocispec.Manifest) { manifest.ArtifactType = "application/vnd.fixture.non-runnable" }},
		{name: "missing layers", mutate: func(manifest *ocispec.Manifest) { manifest.Layers = nil }},
		{name: "extra layer", mutate: func(manifest *ocispec.Manifest) { manifest.Layers = append(manifest.Layers, manifest.Layers[0]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newImageFlightFixture(t, nil, nil)
			malicious := dockerReplayRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				response, err := f.transport.RoundTrip(request)
				if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(request.URL.Path, "/manifests/") {
					return response, err
				}
				raw, err := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				if err != nil || closeErr != nil {
					return nil, errors.Join(err, closeErr)
				}
				var manifest ocispec.Manifest
				if err := json.Unmarshal(raw, &manifest); err != nil {
					return nil, err
				}
				test.mutate(&manifest)
				raw, err = json.Marshal(manifest)
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(raw))
				response.ContentLength = int64(len(raw))
				response.Header.Set("Content-Length", fmt.Sprint(len(raw)))
				response.Header.Set("Docker-Content-Digest", digest.FromBytes(raw).String())
				return response, nil
			})
			var imports atomic.Int64
			attachImageCapacityLoader(t, f.m, func(context.Context, io.Reader) (image.LoadResponse, error) {
				imports.Add(1)
				return image.LoadResponse{}, errors.New("non-runnable manifest must never import")
			}, imagefetch.WithRegistryTransport(nativeRegistryTransport(t, malicious)))
			f.local.Store(true) // The matching classic config is already cached locally.
			const lease = "550e8400-e29b-41d4-a716-446655440088"
			var preparationErr error
			pins, runs := imagePreparationSubjects(t, map[string]string{lease: f.ref}, nil, func(ctx context.Context, mutations *storageMutations) error {
				_, preparationErr = f.m.prepare(ctx, mutations, f.ref, true)
				return preparationErr
			})
			f.m.pins = pins
			runs[lease]()
			require.Error(t, preparationErr, "manifest metadata must agree with the cached config before issuing recovery authority")
			saved, err := pins.List()
			require.NoError(t, err)
			require.Empty(t, saved, "cached config identity cannot authorize a non-runnable future recovery source")
			require.Zero(t, imports.Load())
			require.Zero(t, f.downloads.Load())
		})
	}
}

func TestImageCapacityUnknownImportAllowsUnrelatedCollectionAndRetainsAccounting(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	server := httptest.NewTLSServer(registry.New())
	t.Cleanup(server.Close)
	ref := strings.TrimPrefix(server.URL, "https://") + "/app:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	require.NoError(t, remote.Write(tag, imageCapacityRegistryImage(t, "unknown completion"), remote.WithTransport(server.Client().Transport)))
	attachImageCapacityLoader(t, m, func(_ context.Context, reader io.Reader) (image.LoadResponse, error) {
		_, err := io.Copy(io.Discard, reader)
		return image.LoadResponse{}, errors.Join(err, errors.New("lost import response"))
	}, imagefetch.WithRegistryTransport(server.Client().Transport.(*http.Transport)))
	prepared, err := m.loader.Prepare(t.Context(), ref, daemonImagePlatform(system.Info{OSType: "linux", Architecture: "amd64"}))
	require.NoError(t, err)
	defer func() { require.NoError(t, prepared.Close()) }()
	_, err = m.loader.Import(t.Context(), prepared)
	require.ErrorContains(t, err, "lost import response")
	pending, err := m.loader.PendingBytes()
	require.NoError(t, err)
	require.Positive(t, pending)
	fs["/images"] = diskCapacity{total: uint64(100 * imageMiB), available: uint64(10 * imageMiB)}
	unused := fixtureImageID("unrelated unused image")
	daemon.imageList = func(context.Context, image.ListOptions) ([]image.Summary, error) {
		return []image.Summary{{ID: unused}}, nil
	}
	var removed []string
	daemon.imageRemove = func(_ context.Context, id string, options image.RemoveOptions) ([]image.DeleteResponse, error) {
		require.False(t, options.Force)
		removed = append(removed, id)
		fs["/images"] = diskCapacity{total: uint64(100 * imageMiB), available: uint64(80 * imageMiB)}
		return nil, nil
	}
	require.NoError(t, m.collect(t.Context()))
	require.Equal(t, []string{unused}, removed, "unknown import debt cannot disable unrelated image collection")
	after, err := m.loader.PendingBytes()
	require.NoError(t, err)
	require.Equal(t, pending, after, "deleting another image does not settle an unknown request")
	fs["/images"] = diskCapacity{available: uint64(m.cfg.ImageDiskMinFreeMB*imageMiB + pending - 1)}
	require.Error(t, m.headroom(t.Context(), false), "ordinary launches must still account for unknown writes")
	require.Error(t, m.importHeadroom(t.Context(), 0), "new imports must still account for unknown writes")
}

func TestImageImportDaemonPanicReleasesCapacityGate(t *testing.T) {
	m, daemon, _ := imageCapacityFixture(t)
	daemon.info = func(context.Context) (system.Info, error) { panic("daemon inspection panic") }
	require.PanicsWithValue(t, "daemon inspection panic", func() {
		_, _ = m.reserveImport(t.Context(), m.loader, nil)
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, m.lock(ctx), "foreign code panic must not strand the provider-wide gate")
	m.unlock()
}

func TestImageCapacityProtectsResolvedImageUntilPinPublication(t *testing.T) {
	m, daemon, fs := imageCapacityFixture(t)
	server := httptest.NewTLSServer(registry.New())
	t.Cleanup(server.Close)
	ref := strings.TrimPrefix(server.URL, "https://") + "/app:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	fixture := imageCapacityRegistryImage(t, "resolved content awaiting its pin")
	require.NoError(t, remote.Write(tag, fixture, remote.WithTransport(server.Client().Transport)))
	id, err := fixture.ConfigName()
	require.NoError(t, err)
	m.runtime = (&mockDockerClient{InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
		return &ImageInfo{ID: id.String()}, nil
	}}).imageAdmitter()
	attachImageCapacityLoader(t, m, func(context.Context, io.Reader) (image.LoadResponse, error) {
		return image.LoadResponse{}, errors.New("local content must not be imported")
	}, imagefetch.WithRegistryTransport(server.Client().Transport.(*http.Transport)))
	resolved, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
		close(resolved)
		<-resume
		return image.InspectResponse{ID: id.String(), Size: imageMiB}, nil
	}
	fs["/images"] = diskCapacity{total: uint64(100 * imageMiB), available: uint64(10 * imageMiB)}
	daemon.imageList = func(context.Context, image.ListOptions) ([]image.Summary, error) {
		select {
		case <-resolved:
			return []image.Summary{{ID: id.String()}}, nil
		default:
			return nil, nil
		}
	}
	var removed []string
	daemon.imageRemove = func(_ context.Context, id string, _ image.RemoveOptions) ([]image.DeleteResponse, error) {
		removed = append(removed, id)
		return nil, nil
	}
	const lease = "550e8400-e29b-41d4-a716-446655440001"
	results := make(chan string, 1)
	runs := imagePreparationExecutions(t, m, map[string]string{lease: ref}, results)
	finished := make(chan struct{})
	go func() { defer close(finished); runs[lease]() }()
	<-resolved
	require.NoError(t, m.lock(t.Context()))
	busyBefore := testutil.ToFloat64(imageGCTotal.WithLabelValues("busy"))
	inhibitedBefore := testutil.ToFloat64(imageGCTotal.WithLabelValues("inhibited"))
	err = m.collect(t.Context())
	busyAfter := testutil.ToFloat64(imageGCTotal.WithLabelValues("busy"))
	inhibitedAfter := testutil.ToFloat64(imageGCTotal.WithLabelValues("inhibited"))
	m.unlock()
	require.NoError(t, err)
	require.Empty(t, removed, "resolved content is owned before its durable pin exists")
	release()
	<-finished
	require.Equal(t, busyBefore+1, busyAfter, "an admitted preparation postpones collection as ordinary live work")
	require.Equal(t, inhibitedBefore, inhibitedAfter, "live preparation must not trigger the uncertainty alert")
	require.Equal(t, lease, <-results)
	pins, err := m.pins.List()
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.Equal(t, id.String(), pins[0].ImageID)
}

func TestImageCapacityReusesClassicMultiPlatformConfigAboveNewLimit(t *testing.T) {
	m, daemon, _ := imageCapacityFixture(t)
	m.cfg.ImageMaxSizeMB = 1
	var observe atomic.Bool
	var configGets, layerGets atomic.Int64
	var configPath string
	registryHandler := registry.New()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if observe.Load() && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
			if r.URL.Path == configPath {
				configGets.Add(1)
			} else {
				layerGets.Add(1)
			}
		}
		registryHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	ref := strings.TrimPrefix(server.URL, "https://") + "/multi:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	native := imageCapacityRegistryImage(t, strings.Repeat("previously extracted", 100_000))
	foreignConfig, err := native.ConfigFile()
	require.NoError(t, err)
	foreignConfig.Architecture = "arm64"
	foreign, err := mutate.ConfigFile(native, foreignConfig)
	require.NoError(t, err)
	index := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: foreign, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "arm64"}}},
		mutate.IndexAddendum{Add: native, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}})
	require.NoError(t, remote.WriteIndex(tag, index, remote.WithTransport(server.Client().Transport)))
	configID, err := native.ConfigName()
	require.NoError(t, err)
	leafID, err := native.Digest()
	require.NoError(t, err)
	indexID, err := index.Digest()
	require.NoError(t, err)
	require.NotEqual(t, leafID, indexID)
	m.runtime = (&mockDockerClient{InspectImageFn: func(_ context.Context, inspected string) (*ImageInfo, error) {
		if inspected != configID.String() {
			return nil, errdefs.NotFound(errors.New("classic store has no selected manifest alias"))
		}
		return &ImageInfo{ID: configID.String()}, nil
	}}).imageAdmitter()
	daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: configID.String(), Size: 3 * imageMiB, RepoDigests: []string{tag.Context().Digest(indexID.String()).Name()}}, nil
	}
	attachImageCapacityLoader(t, m, func(context.Context, io.Reader) (image.LoadResponse, error) {
		return image.LoadResponse{}, errors.New("existing multi-platform content must not be imported")
	}, imagefetch.WithRegistryTransport(server.Client().Transport.(*http.Transport)))
	configPath = "/v2/multi/blobs/" + configID.String()
	observe.Store(true)
	const lease = "550e8400-e29b-41d4-a716-446655440001"
	results := make(chan string, 1)
	runs := imagePreparationExecutions(t, m, map[string]string{lease: ref}, results)
	runs[lease]()
	require.Equal(t, lease, <-results)
	require.EqualValues(t, 1, configGets.Load(), "selection verifies the bounded config metadata once")
	require.Zero(t, layerGets.Load(), "local config reuse must not download or revalidate old layers against new ingestion limits")
	pins, err := m.pins.List()
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.Equal(t, configID.String(), pins[0].ImageID)
	require.Equal(t, tag.Context().Digest(leafID.String()).Name(), pins[0].PullDigest)
}
