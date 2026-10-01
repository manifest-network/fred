package docker

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestImageInspectionInheritedLabelsUseAdmissionPolicyDuringCleanup(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "immediate cleanup"
		if restart {
			name = "restart recovery"
		}
		t.Run(name, func(t *testing.T) {
			inherited := map[string]string{"traefİk.x": "verbatim tenant metadata", "org.example.owner": "tenant"}
			for key := range inherited {
				require.False(t, manifest.IsReservedLabelKey(key))
			}
			h := newInspectionHarnessWithClient(t, func(daemon *inspectionDaemon) *DockerClient {
				daemon.imageLabels = inherited
				return newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) { return daemon.request(t, req) })
			})
			if restart {
				h.daemon.removeErr = errors.New("cleanup interrupted")
			}
			h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
				session, err := h.client.openImageInspection(ctx, h.image, origin)
				require.NoError(t, err, "admitted image labels cannot invalidate the helper identity")
				actual := h.daemon.containers[session.containerID]
				for key, value := range inherited {
					assert.Equal(t, value, actual.Config.Labels[key])
				}
				err = session.close()
				if restart {
					require.ErrorContains(t, err, "cleanup interrupted")
				} else {
					require.NoError(t, err)
				}
				return err
			})
			if restart {
				h.reopen(t)
				h.daemon.removeErr = nil
				report, err := h.owner.Recover(t.Context())
				require.NoError(t, err)
				assert.Empty(t, report.pending)
			}
			assert.Empty(t, h.daemon.containers)
			assert.Zero(t, h.daemon.volumes)
			receipts, err := h.owner.journal.List()
			require.NoError(t, err)
			assert.Empty(t, receipts)
			require.NoError(t, h.client.requireImageInspectionsSettled(t.Context()))
		})
	}
}
