package docker

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/docker/docker/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

func TestRequireImageOnlyReadsLocalContent(t *testing.T) {
	for _, state := range []string{"local", "missing", "missing selected leaf", "canceled"} {
		t.Run(state, func(t *testing.T) {
			reads := 0
			docker := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
				require.Equal(t, http.MethodGet, req.Method, "availability checks cannot import or create")
				require.Contains(t, req.URL.Path, "/images/")
				reads++
				if state == "missing" || (state == "missing selected leaf" && strings.Contains(req.URL.Path, otherTestImageID)) {
					return imageSecurityResponse(http.StatusNotFound, `{"message":"local image missing"}`), nil
				}
				if state == "missing selected leaf" {
					if req.URL.Query().Get("platform") != "" {
						return imageSecurityResponse(http.StatusOK, platformSecurityJSON(otherTestImageID, ocispec.MediaTypeImageManifest)), nil
					}
					return imageSecurityResponse(http.StatusOK, platformSecurityJSON(testImageID, ocispec.MediaTypeImageIndex)), nil
				}
				return imageSecurityResponse(http.StatusOK, fmt.Sprintf(`{"Id":%q,"Os":"linux","Architecture":"amd64","Config":{}}`, testImageID)), nil
			})
			ctx := t.Context()
			if state == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err := docker.RequireImage(ctx, "registry.example/app:latest")
			switch state {
			case "local":
				require.NoError(t, err)
			case "missing":
				require.True(t, errdefs.IsNotFound(err))
			case "missing selected leaf":
				var required *imageexec.MaterializationRequired
				require.ErrorAs(t, err, &required)
				require.Equal(t, otherTestImageID, required.ID())
			case "canceled":
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, reads)
			}
		})
	}
}
