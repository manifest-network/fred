package docker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// The stopped drain proof reports a close whose remaining volumes all have a
// delete stage on disk as delete-held, not pending, with the same predicate
// the running backend uses for its close-churn skip. A close with a live,
// unstaged volume stays pending.
func TestClassifyStoppedDrainSeparatesDeleteHeldCloses(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	const (
		heldMixed = "550e8400-e29b-41d4-a716-446655440801" // one staged slot, one with no volume
		heldBoth  = "550e8400-e29b-41d4-a716-446655440802" // the staged volume is still on disk
		unheld    = "550e8400-e29b-41d4-a716-446655440803" // a live volume without a stage
	)
	seedCloseLeaseFixture(t, b, stores, heldMixed, "", 2)
	seedCloseLeaseFixture(t, b, stores, heldBoth, "", 1)
	seedCloseLeaseFixture(t, b, stores, unheld, "", 1)
	b.volumes = &mockVolumeManager{DestroyFn: func(context.Context, string) error {
		return errors.New("injected pending destroy")
	}}
	installTestStorageMutationAdapters(b)
	for _, lease := range []string{heldMixed, heldBoth, unheld} {
		require.Error(t, b.doDeprovisionForTest(t, t.Context(), lease))
	}
	closeCloseRecoveryBackend(t, b, stores)

	inspection, err := shared.InspectCallbackStoreReadOnly(filepath.Join(dir, "callbacks.db"))
	require.NoError(t, err)
	require.Equal(t, 3, inspection.Pending)

	volumeRoot := t.TempDir()
	mkdir := func(name string) { require.NoError(t, os.Mkdir(filepath.Join(volumeRoot, name), 0o700)) }
	mkdir(mustXFSDeleteStage(t, xfsDeleteTestProjectID, canonicalVolumeName(heldMixed, "app", 0)).value())
	mkdir(mustXFSDeleteStage(t, xfsDeleteTestProjectID+1, canonicalVolumeName(heldBoth, "app", 0)).value())
	mkdir(canonicalVolumeName(heldBoth, "app", 0))
	mkdir(canonicalVolumeName(unheld, "app", 0))

	report, err := ClassifyStoppedDrain(inspection, volumeRoot)
	require.NoError(t, err)
	assert.Equal(t, StoppedDrainReport{Pending: 1, DeleteHeld: 2}, report)

	report, err = ClassifyStoppedDrain(inspection, "")
	require.NoError(t, err)
	assert.Equal(t, StoppedDrainReport{Pending: 3}, report, "without a volume root nothing is held")

	// A second, unheld volume of the mixed close keeps it pending.
	mkdir(canonicalVolumeName(heldMixed, "app", 1))
	report, err = ClassifyStoppedDrain(inspection, volumeRoot)
	require.NoError(t, err)
	assert.Equal(t, StoppedDrainReport{Pending: 2, DeleteHeld: 1}, report)

	// A malformed entry in a reserved stage namespace fails the proof, as it
	// fails Start.
	mkdir(xfsDeleteStagePrefix + "not-a-stage")
	_, err = ClassifyStoppedDrain(inspection, volumeRoot)
	require.Error(t, err)
}

// A maintenance head is never delete-held: it waits on its own work.
func TestClassifyStoppedDrainNeverExcusesAMaintenanceHead(t *testing.T) {
	const lease = "550e8400-e29b-41d4-a716-446655440804"
	volumeRoot := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(volumeRoot,
		mustXFSDeleteStage(t, xfsDeleteTestProjectID, canonicalVolumeName(lease, "app", 0)).value()), 0o700))
	inspection := shared.CallbackStoreInspection{Pending: 1, PendingHeads: []shared.PendingLeaseMutationHead{{
		Kind: shared.PendingMaintenanceHead, LeaseUUID: lease,
	}}}
	report, err := ClassifyStoppedDrain(inspection, volumeRoot)
	require.NoError(t, err)
	assert.Equal(t, StoppedDrainReport{Pending: 1}, report)
}
