package docker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	composeapi "github.com/docker/compose/v5/pkg/api"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestImageInspectionComposeBuildLabelsRemainNeutralThroughRecovery(t *testing.T) {
	for _, oldHelper := range []bool{false, true} {
		name := "compose image"
		if oldHelper {
			name = "older helper without stamp keys"
		}
		t.Run(name, func(t *testing.T) {
			h := newInspectionHarnessWithClient(t, func(daemon *inspectionDaemon) *DockerClient {
				if !oldHelper {
					daemon.imageLabels = map[string]string{
						composeapi.ProjectLabel: "foreign-project", composeapi.ServiceLabel: "foreign-service",
						composeapi.VersionLabel: "foreign-version", composeapi.ImageBuilderLabel: "classic",
						"app.owner": "tenant",
					}
				}
				return newImageSecurityDockerClient(t, func(request *http.Request) (*http.Response, error) {
					return daemon.request(t, request)
				})
			})
			h.daemon.copy = func(_ context.Context, path string) (io.ReadCloser, error) { return inspectionTar(t, path), nil }
			h.daemon.removeErr = errors.New("retain exact helper across restart")
			h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
				_, err := h.client.readFileFromImage(ctx, h.image, "/etc/passwd", origin)
				require.ErrorContains(t, err, "retain exact helper")
				return err
			})
			require.Len(t, h.daemon.containers, 1)
			for _, actual := range h.daemon.containers {
				for _, key := range []string{composeapi.ProjectLabel, composeapi.ServiceLabel, composeapi.VersionLabel, composeapi.ImageBuilderLabel} {
					value, present := actual.Config.Labels[key]
					require.True(t, present, "helper must write %s explicitly", key)
					require.Empty(t, value, "helper must not inherit Compose ownership")
					if oldHelper {
						// Helpers created before a key was owned carry no
						// stamp at all; recovery must still accept them.
						delete(actual.Config.Labels, key)
					}
				}
				if !oldHelper {
					require.Equal(t, "tenant", actual.Config.Labels["app.owner"])
				}
			}
			h.reopen(t)
			h.daemon.removeErr = nil
			report, err := h.owner.Recover(t.Context())
			require.NoError(t, err)
			require.Empty(t, report.pending)
			require.Empty(t, h.daemon.containers)
			require.Zero(t, h.daemon.volumes)
			receipts, err := h.owner.journal.List()
			require.NoError(t, err)
			require.Empty(t, receipts)
		})
	}
}
