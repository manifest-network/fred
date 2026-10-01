package docker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func pendingInspectionFenceHarness(t *testing.T) *inspectionHarness {
	t.Helper()
	h := newInspectionHarness(t)
	h.daemon.createErr = errors.New("lost helper Create response")
	h.daemon.delayCreate = true
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		_, err := h.client.readFileFromImage(ctx, h.image, "/etc/passwd", origin)
		require.ErrorContains(t, err, "lost helper Create response")
		return err
	})
	receipts, err := h.owner.journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.False(t, receipts[0].CreationSettled())
	require.NotNil(t, h.daemon.late)
	return h
}

func TestImageInspectionOperatorFenceStillRequiresExactCleanup(t *testing.T) {
	for _, scenario := range []string{"absent", "present", "foreign", "remove fails"} {
		t.Run(scenario, func(t *testing.T) {
			h := pendingInspectionFenceHarness(t)
			// Deliver the delayed Create before the offline fence. The operator
			// drains prior requests without asserting that their effects are absent.
			if scenario != "absent" {
				actual := *h.daemon.late
				// An older process could have dispatched a helper whose image
				// volumes were inherited. Upgrade recovery must still reap them.
				actual.HostConfig = nil
				if scenario == "foreign" {
					actual.Config.Labels["fred.inspection.backend"] = "another-backend"
				}
				h.daemon.containers[actual.ID] = actual
				h.daemon.volumes++
			}
			if scenario == "remove fails" {
				h.daemon.removeErr = errors.New("daemon remove unavailable")
			}
			h.stop()
			require.NoError(t, h.callbacks.Close())
			inspection, err := shared.InspectDockerRecovery(t.Context(), h.dbPath, h.authority.storage)
			require.NoError(t, err)
			require.Equal(t, 1, inspection.UnknownHelpers)
			backup := filepath.Join(t.TempDir(), "before-helper-fence.db")
			result, err := shared.RepairDockerRecovery(t.Context(), h.dbPath, h.authority.storage,
				inspection.Acknowledgement, backup, func(context.Context) error {
					return backendidentity.VerifyMarkerPair(h.dbPath+".storage-identity.json", h.dbPath+".storage-identity-anchor.json",
						"docker", operationIntentTestSubstrateID, h.authority.storage.ID())
				})
			require.NoError(t, err)
			require.Equal(t, "DOCKER_EFFECTS_FENCED", result.Verdict)
			require.Zero(t, h.daemon.removes, "offline fencing cannot remove a helper")
			old, err := shared.InspectDockerRecovery(t.Context(), backup, h.authority.storage)
			require.NoError(t, err)
			require.Equal(t, 1, old.UnknownHelpers, "backup preserves the unfenced receipt")

			h.reopen(t)
			receipts, err := h.owner.journal.List()
			require.NoError(t, err)
			require.Len(t, receipts, 1, "repair retains exact helper cleanup ownership")
			require.Empty(t, receipts[0].ContainerID())
			require.True(t, receipts[0].CreationSettled())
			report, err := h.owner.Recover(t.Context())
			require.NoError(t, err)
			receipts, err = h.owner.journal.List()
			require.NoError(t, err)
			switch scenario {
			case "absent":
				require.Empty(t, report.pending)
				require.Empty(t, receipts)
				require.Zero(t, h.daemon.removes)
			case "present":
				require.Empty(t, report.pending)
				require.Empty(t, receipts)
				require.Empty(t, h.daemon.containers)
				require.Equal(t, []bool{true}, h.daemon.removeVolumes)
				require.Zero(t, h.daemon.volumes)
			case "foreign":
				require.Len(t, report.pending, 1)
				require.Len(t, receipts, 1)
				require.Len(t, h.daemon.containers, 1)
				require.Zero(t, h.daemon.removes)
			case "remove fails":
				require.Len(t, report.pending, 1)
				require.Len(t, receipts, 1)
				require.Len(t, h.daemon.containers, 1)
				h.daemon.removeErr = nil
				report, err = h.owner.Recover(t.Context())
				require.NoError(t, err)
				require.Empty(t, report.pending)
				receipts, err = h.owner.journal.List()
				require.NoError(t, err)
				require.Empty(t, receipts)
				require.Empty(t, h.daemon.containers)
			}
		})
	}
}
