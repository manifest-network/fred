//go:build integration

package docker

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// localPlatformSecurityRegistry serves only a tiny test-owned OCI index and its
// blobs over loopback. It exercises the daemon's real pull/index/manifest path
// without a public registry, build service, credentials, or executable workload.
func localPlatformSecurityRegistry(t *testing.T) (imageName string, leaf digest.Digest) {
	t.Helper()
	var layer bytes.Buffer
	tw := tar.NewWriter(&layer)
	body := []byte(uuid.NewString())
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "fixture", Mode: 0o644, Size: int64(len(body))}))
	_, err := tw.Write(body)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, err = zw.Write(layer.Bytes())
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	type registryObject struct {
		data      []byte
		mediaType string
		digest    digest.Digest
	}
	objects := make(map[string]registryObject)
	add := func(data []byte, mediaType, kind string) ocispec.Descriptor {
		d := digest.FromBytes(data)
		objects["/v2/app/"+kind+"/"+d.String()] = registryObject{data: data, mediaType: mediaType, digest: d}
		return ocispec.Descriptor{Digest: d, Size: int64(len(data)), MediaType: mediaType}
	}
	layerDescriptor := add(compressed.Bytes(), ocispec.MediaTypeImageLayerGzip, "blobs")
	foreignArch := "arm64"
	if runtime.GOARCH == foreignArch {
		foreignArch = "amd64"
	}
	var manifests []ocispec.Descriptor
	for _, architecture := range []string{runtime.GOARCH, foreignArch} {
		labels := map[string]string{"app.owner": "native-safe"}
		if architecture == foreignArch {
			// Foreign-platform metadata must never be substituted for the native
			// metadata checked by admission, even though it shares the index ID.
			labels = map[string]string{"traefik.enable": "true"}
		}
		platform := ocispec.Platform{OS: "linux", Architecture: architecture}
		config := ocispec.Image{
			Platform: platform,
			Config:   ocispec.ImageConfig{Labels: labels, Cmd: []string{"/fixture-not-executable"}},
			RootFS:   ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromBytes(layer.Bytes())}},
		}
		configJSON, err := json.Marshal(config)
		require.NoError(t, err)
		configDescriptor := add(configJSON, ocispec.MediaTypeImageConfig, "blobs")
		manifestJSON, err := json.Marshal(ocispec.Manifest{
			Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest,
			Config: configDescriptor, Layers: []ocispec.Descriptor{layerDescriptor},
		})
		require.NoError(t, err)
		descriptor := add(manifestJSON, ocispec.MediaTypeImageManifest, "manifests")
		descriptor.Platform = &platform
		manifests = append(manifests, descriptor)
		if architecture == runtime.GOARCH {
			leaf = descriptor.Digest
		}
	}
	indexJSON, err := json.Marshal(ocispec.Index{
		Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex, Manifests: manifests,
	})
	require.NoError(t, err)
	indexDescriptor := add(indexJSON, ocispec.MediaTypeImageIndex, "manifests")
	objects["/v2/app/manifests/latest"] = objects["/v2/app/manifests/"+indexDescriptor.Digest.String()]
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
		if r.URL.Path == "/v2/" {
			w.WriteHeader(http.StatusOK)
			return
		}
		object, exists := objects[r.URL.Path]
		if !exists {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", object.mediaType)
		w.Header().Set("Docker-Content-Digest", object.digest.String())
		w.Header().Set("Content-Length", fmt.Sprint(len(object.data)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(object.data)
		}
	}))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://") + "/app:latest", leaf
}

func TestIntegration_Docker_MultiPlatformImageExecutesCheckedLeaf(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)
	sdk := newImageSecurityFixtureClient(t)
	imageName, expectedLeaf := localPlatformSecurityRegistry(t)
	leafReference := strings.TrimSuffix(imageName, ":latest") + "@" + expectedLeaf.String()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		_, _ = sdk.ImageRemove(cleanupCtx, imageName, image.RemoveOptions{})
		_, _ = sdk.ImageRemove(cleanupCtx, leafReference, image.RemoveOptions{})
	})
	require.NoError(t, docker.PullImage(ctx, imageName, time.Minute))
	raw, _, err := sdk.ImageInspectWithRaw(ctx, imageName)
	require.NoError(t, err)
	prepared, err := docker.AdmitImage(ctx, imageName)
	require.NoError(t, err)
	assert.Equal(t, imageName, prepared.Reference())
	assert.Equal(t, runtime.GOARCH, prepared.Platform().Architecture)
	if raw.Descriptor != nil && raw.Descriptor.MediaType == ocispec.MediaTypeImageIndex {
		assert.Equal(t, expectedLeaf.String(), prepared.ID(), "containerd execution must pin the native manifest, not the parent index")
		assert.NotEqual(t, raw.ID, prepared.ID())
	} else {
		assert.Equal(t, raw.ID, prepared.ID(), "classic stores already expose an immutable config ID")
	}
	h := newIntegrationInspectionHarness(t, docker, imageName)
	h.execute(t, func(work context.Context, origin shared.ImageInspectionOrigin) error {
		helper, err := docker.openImageInspection(work, h.image, origin)
		require.NoError(t, err)
		defer func() { require.NoError(t, helper.close()) }()
		created, err := sdk.ContainerInspect(work, helper.containerID)
		require.NoError(t, err)
		assert.Equal(t, prepared.ID(), created.Image)
		assert.Equal(t, prepared.ID(), created.Config.Image)
		assert.Equal(t, "native-safe", created.Config.Labels["app.owner"])
		assert.NotContains(t, created.Config.Labels, "traefik.enable")
		assert.False(t, created.State.Running, "the image fixture must never be started")
		return nil
	})
}
