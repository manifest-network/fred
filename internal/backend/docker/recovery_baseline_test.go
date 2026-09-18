package docker

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoveryBaselineReuseStillDetectsMutationDuringPublicationHandoff(t *testing.T) {
	b := &Backend{provisions: make(map[string]*provision)}
	for _, lease := range []string{"unchanged", "changed", "removed", "replaced"} {
		projection := fullRecoveredProvision().materialize()
		projection.LeaseUUID = lease
		b.provisions[lease] = projection
	}
	baseline := b.snapshotProvisionRecoveryBaseline()
	unchangedSnapshot := baseline["unchanged"].value

	// An actor changes a callback generation while inventory is being gathered.
	// Its new snapshot must be detached; unchanged snapshots may be reused.
	b.provisionsMu.Lock()
	b.provisions["changed"].CallbackURL = "https://new.example/callbacks/provision"
	b.provisions["changed"].ContainerIDs[0] = "new-before-publication"
	delete(b.provisions, "removed")
	b.provisions["replaced"] = fullRecoveredProvision().materialize()
	b.provisions["added"] = fullRecoveredProvision().materialize()
	changes := b.changedProvisionRecoveryLeases(baseline)
	require.Len(t, changes, 4)
	b.refreshProvisionRecoveryBaselineLocked(baseline, changes)
	assert.Empty(t, b.changedProvisionRecoveryLeases(baseline))
	assert.Same(t, &unchangedSnapshot.Items[0], &baseline["unchanged"].value.Items[0],
		"the unchanged detached snapshot must not be cloned again")
	assert.NotContains(t, baseline, "removed")
	b.provisionsMu.Unlock()

	// Mutations in the unlocked actorsMu -> provisionsMu hand-off must still
	// invalidate both refreshed and reused snapshots, including in-place slices.
	b.provisionsMu.Lock()
	b.provisions["changed"].CallbackURL = "https://later.example/callbacks/provision"
	b.provisions["changed"].ContainerIDs[0] = "late-container"
	b.provisions["unchanged"].ServiceContainers["app"][0] = "late-unchanged-container"
	b.provisions["added"].ResourceProfiles[0].ScratchDiskMB++
	changes = b.changedProvisionRecoveryLeases(baseline)
	assert.Equal(t, map[string]string{
		"changed": "projection changed", "unchanged": "projection changed", "added": "projection changed",
	}, changes)
	assert.Equal(t, "new-before-publication", baseline["changed"].value.ContainerIDs[0])
	assert.Equal(t, "c1", baseline["unchanged"].value.ServiceContainers["app"][0])
	b.provisionsMu.Unlock()
}

func BenchmarkRecoveryPublicationBaselineStableFleet(b *testing.B) {
	backend := &Backend{provisions: make(map[string]*provision, 1000)}
	for index := range 1000 {
		lease := fmt.Sprintf("lease-%d", index)
		projection := fullRecoveredProvision().materialize()
		projection.LeaseUUID = lease
		backend.provisions[lease] = projection
	}
	baseline := backend.snapshotProvisionRecoveryBaseline()
	b.ReportAllocs()
	for b.Loop() {
		backend.provisionsMu.Lock()
		changed := backend.changedProvisionRecoveryLeases(baseline)
		backend.refreshProvisionRecoveryBaselineLocked(baseline, changed)
		backend.provisionsMu.Unlock()
	}
}
