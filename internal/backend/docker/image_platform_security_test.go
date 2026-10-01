package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func platformSecurityJSON(id, mediaType string) string {
	return fmt.Sprintf(`{"Id":%q,"Os":"linux","Architecture":"amd64","Config":{},"Descriptor":{"mediaType":%q,"digest":%q}}`, id, mediaType, id)
}

func TestImageInspectionHelperRequiresLocalExactPlatformBeforeCreate(t *testing.T) {
	materialized, created := false, false
	h := newInspectionHarnessWithClient(t, func(daemon *inspectionDaemon) *DockerClient {
		docker := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
			switch {
			case strings.HasSuffix(req.URL.Path, "/images/create"):
				t.Fatal("image admission must never issue an unbounded registry pull")
				return nil, fmt.Errorf("unexpected registry pull")
			case strings.HasSuffix(req.URL.Path, "/containers/create"):
				require.True(t, materialized)
				body, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				req.Body = io.NopCloser(bytes.NewReader(body))
				var config container.Config
				require.NoError(t, json.Unmarshal(body, &config))
				assert.Equal(t, otherTestImageID, config.Image)
				assert.Equal(t, "linux/amd64", req.URL.Query().Get("platform"))
				created = true
				return daemon.request(t, req)
			case strings.Contains(req.URL.Path, "/containers/"):
				return daemon.request(t, req)
			case strings.Contains(req.URL.Path, otherTestImageID):
				if !materialized {
					return imageSecurityResponse(http.StatusNotFound, `{"message":"no standalone leaf record"}`), nil
				}
				return imageSecurityResponse(http.StatusOK, platformSecurityJSON(otherTestImageID, ocispec.MediaTypeImageManifest)), nil
			case req.URL.Query().Get("platform") != "":
				assert.Contains(t, req.URL.Path, testImageID)
				return imageSecurityResponse(http.StatusOK, platformSecurityJSON(otherTestImageID, ocispec.MediaTypeImageManifest)), nil
			default:
				return imageSecurityResponse(http.StatusOK, platformSecurityJSON(testImageID, ocispec.MediaTypeImageIndex)), nil
			}
		})
		_, err := docker.AdmitImage(t.Context(), "registry.example/app:latest")
		var required *imageexec.MaterializationRequired
		require.ErrorAs(t, err, &required)
		require.Equal(t, otherTestImageID, required.ID())
		require.False(t, created)
		// The ingestion integration test covers the exact-content import. This
		// fixture now exposes the independently loaded immutable leaf.
		materialized = true
		return docker
	})
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		session, err := h.client.openImageInspection(ctx, h.image, origin)
		require.NoError(t, err)
		return session.close()
	})
	assert.True(t, created)
}

func TestImageRuntimeConstructionRejectsOldDockerAPI(t *testing.T) {
	mutations := 0
	api, err := client.NewClientWithOpts(client.WithHost("http://docker.invalid"), client.WithVersion("1.47"), client.WithHTTPClient(&http.Client{
		Transport: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodGet {
				mutations++
			}
			return imageSecurityResponse(http.StatusOK, `{"ApiVersion":"1.51"}`), nil
		}),
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = api.Close() })
	images, creator, err := imageexec.NewDockerRuntime(t.Context(), api)
	assert.Nil(t, images)
	assert.Nil(t, creator)
	require.ErrorContains(t, err, "Docker Engine 28.1+")
	assert.Zero(t, mutations)
}
