package docker

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// The Docker protocol is real HTTP; markers, journal schemas, operation claims
// and unknown helper receipts come from the existing construction-bound fixture.
// Every runtime journal handle is closed before either public operator wrapper.
func stoppedDockerRepairConfig(t *testing.T, daemonID func() string) (Config, *inspectionHarness) {
	t.Helper()
	h := pendingInspectionFenceHarness(t)
	h.stop()
	require.NoError(t, h.callbacks.Close())
	require.NoError(t, h.authority.releases.Close())
	require.NoError(t, h.authority.retentions.Close())
	server := newStorageIdentityDockerServerWithDaemonID(t, nil, daemonID)
	cfg := storageIdentityIntegrationConfig(t, server.URL)
	cfg.CallbackDBPath = h.dbPath
	cfg.ReleasesDBPath = h.authority.releasePath
	cfg.RetentionDBPath = h.authority.retentionPath
	require.NoError(t, cfg.Validate())
	return cfg, h
}

func TestDockerRecoveryPublicWrappersUseStoppedReadOnlySubstrateProof(t *testing.T) {
	cfg, h := stoppedDockerRepairConfig(t, func() string { return operationIntentTestSubstrateID })
	paths := []string{cfg.CallbackDBPath, cfg.ReleasesDBPath, cfg.RetentionDBPath,
		cfg.CallbackDBPath + ".storage-identity.json", cfg.CallbackDBPath + ".storage-identity-anchor.json"}
	original := make(map[string][]byte)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		original[path] = data
	}
	inspection, err := InspectUnsettledDockerEffectsForConfig(t.Context(), cfg, discardStorageIdentityLogger())
	require.NoError(t, err, "offline inspection has no runtime gate or open runtime journal handles")
	require.Equal(t, "DOCKER_EFFECTS_INSPECTED", inspection.Verdict)
	require.Equal(t, 1, inspection.UnknownHelpers)
	for path, before := range original {
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, before, after, "inspection must not mutate %s", path)
	}
	backup := filepath.Join(t.TempDir(), "callbacks.before-repair.db")
	result, err := RepairUnsettledDockerEffectsForConfig(t.Context(), cfg, discardStorageIdentityLogger(), inspection.Acknowledgement, backup)
	require.NoError(t, err, "offline repair must reverify substrate while owning the callback journal exclusively")
	require.Equal(t, "DOCKER_EFFECTS_FENCED", result.Verdict)
	require.Equal(t, backup, result.Backup)
	old, err := shared.InspectDockerRecovery(t.Context(), backup, h.authority.storage)
	require.NoError(t, err)
	require.Equal(t, inspection.SnapshotSHA256, old.SnapshotSHA256)
	require.Equal(t, 1, old.UnknownHelpers)
	post, err := InspectUnsettledDockerEffectsForConfig(t.Context(), cfg, discardStorageIdentityLogger())
	require.NoError(t, err)
	require.Zero(t, post.UnknownHelpers)
	for path, before := range original {
		if path == cfg.CallbackDBPath {
			continue
		}
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, before, after, "repair must not modify unrelated authority %s", path)
	}
}

func TestDockerRecoveryPublicWrappersRefuseDaemonDriftAfterProbe(t *testing.T) {
	for _, action := range []string{"inspect", "repair"} {
		t.Run(action, func(t *testing.T) {
			var identity atomic.Value
			identity.Store(operationIntentTestSubstrateID)
			var replaceAfterRead atomic.Bool
			cfg, _ := stoppedDockerRepairConfig(t, func() string {
				observed := identity.Load().(string)
				if replaceAfterRead.Swap(false) {
					// The first probe observes the original daemon. Every subsequent
					// attestation must see its replacement; no read-count assumption.
					identity.Store("replacement-daemon")
				}
				return observed
			})
			inspection, err := InspectUnsettledDockerEffectsForConfig(t.Context(), cfg, discardStorageIdentityLogger())
			require.NoError(t, err)
			before, err := os.ReadFile(cfg.CallbackDBPath)
			require.NoError(t, err)
			backup := filepath.Join(t.TempDir(), "must-not-exist.db")
			replaceAfterRead.Store(true)
			if action == "inspect" {
				_, err = InspectUnsettledDockerEffectsForConfig(t.Context(), cfg, discardStorageIdentityLogger())
			} else {
				_, err = RepairUnsettledDockerEffectsForConfig(t.Context(), cfg, discardStorageIdentityLogger(), inspection.Acknowledgement, backup)
			}
			require.ErrorIs(t, err, backendidentity.ErrIdentityDrift)
			after, err := os.ReadFile(cfg.CallbackDBPath)
			require.NoError(t, err)
			require.Equal(t, before, after)
			_, err = os.Stat(backup)
			require.ErrorIs(t, err, os.ErrNotExist, "drift must refuse before backup publication or repair")
		})
	}
}
