package shared

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

const (
	unfrozenSizingLease    = "11111111-2222-4333-8444-555555555555"
	frozenSizingLease      = "66666666-7777-4888-8999-aaaaaaaaaaaa"
	unfrozenSizingProvider = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
)

func unfrozenSizingEntry(lease string) RetentionEntry {
	entry := sampleEntry(lease)
	entry.ProviderUUID = unfrozenSizingProvider
	entry.Items = []backend.LeaseItem{{SKU: "sku-1", Quantity: 2, ServiceName: "app"}}
	entry.Generation = 3
	return entry
}

func unfrozenSizingProfiles() []SKUResourceSnapshot {
	return []SKUResourceSnapshot{{SKU: "sku-1", CPUCores: 1, MemoryMB: 512, DiskMB: 100}}
}

func unfrozenSizingCandidateForTest(t *testing.T, store *RetentionStore, lease string) UnfrozenSizingCandidate {
	t.Helper()
	candidates, err := store.ListUnfrozenSizingCandidates()
	require.NoError(t, err)
	for _, candidate := range candidates {
		if candidate.Entry().OriginalLeaseUUID == lease {
			return candidate
		}
	}
	t.Fatalf("no unfrozen sizing candidate for %s", lease)
	return UnfrozenSizingCandidate{}
}

func TestFreezeResourceProfilesAddsSizingOnceToTheInspectedRow(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	legacy := unfrozenSizingEntry(unfrozenSizingLease)
	require.NoError(t, stores.retentions.putUnsafe(legacy))
	frozen := unfrozenSizingEntry(frozenSizingLease)
	frozen.ResourceProfiles = unfrozenSizingProfiles()
	require.NoError(t, stores.retentions.putUnsafe(frozen))

	candidates, err := stores.retentions.ListUnfrozenSizingCandidates()
	require.NoError(t, err)
	require.Len(t, candidates, 1, "a row that already carries sizing is never a candidate")
	candidate := candidates[0]
	require.True(t, candidate.Valid())
	require.Equal(t, unfrozenSizingLease, candidate.Entry().OriginalLeaseUUID)

	require.NoError(t, stores.retentions.FreezeResourceProfiles(candidate, unfrozenSizingProfiles()))
	stored, err := stores.retentions.Get(unfrozenSizingLease)
	require.NoError(t, err)
	require.Equal(t, unfrozenSizingProfiles(), stored.ResourceProfiles)
	require.Equal(t, legacy.Status, stored.Status)
	require.Equal(t, legacy.Generation, stored.Generation, "freezing adds authority without starting a generation")
	require.Equal(t, legacy.RetainedVolumeNames, stored.RetainedVolumeNames)

	candidates, err = stores.retentions.ListUnfrozenSizingCandidates()
	require.NoError(t, err)
	require.Empty(t, candidates)
	require.ErrorIs(t,
		stores.retentions.FreezeResourceProfiles(candidate, []SKUResourceSnapshot{{SKU: "sku-1", CPUCores: 2, MemoryMB: 1024, DiskMB: 900}}),
		errUnfrozenSizingChanged, "a reused candidate cannot replace frozen sizing")
	stored, err = stores.retentions.Get(unfrozenSizingLease)
	require.NoError(t, err)
	require.Equal(t, unfrozenSizingProfiles(), stored.ResourceProfiles)
}

func TestFreezeResourceProfilesRefusesASnapshotThatDoesNotCoverTheRow(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	require.NoError(t, stores.retentions.putUnsafe(unfrozenSizingEntry(unfrozenSizingLease)))
	candidate := unfrozenSizingCandidateForTest(t, stores.retentions, unfrozenSizingLease)
	before, err := stores.retentions.getRaw(unfrozenSizingLease)
	require.NoError(t, err)

	err = stores.retentions.FreezeResourceProfiles(candidate, []SKUResourceSnapshot{{SKU: "other", CPUCores: 1, MemoryMB: 512, DiskMB: 100}})
	require.ErrorContains(t, err, "freeze resource profiles")
	err = stores.retentions.FreezeResourceProfiles(candidate, nil)
	require.Error(t, err)
	after, err := stores.retentions.getRaw(unfrozenSizingLease)
	require.NoError(t, err)
	require.Equal(t, before, after, "a refused freeze leaves the row byte-identical")
}

func TestFreezeResourceProfilesRefusesAnotherJournalsCandidate(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	other := openOperationHandoffStores(t, "docker-b")
	require.NoError(t, stores.retentions.putUnsafe(unfrozenSizingEntry(unfrozenSizingLease)))
	require.NoError(t, other.retentions.putUnsafe(unfrozenSizingEntry(unfrozenSizingLease)))
	candidate := unfrozenSizingCandidateForTest(t, stores.retentions, unfrozenSizingLease)

	err := other.retentions.FreezeResourceProfiles(candidate, unfrozenSizingProfiles())
	require.ErrorContains(t, err, "another journal lineage")
	stored, err := other.retentions.Get(unfrozenSizingLease)
	require.NoError(t, err)
	require.Empty(t, stored.ResourceProfiles)
}

func TestFreezeResourceProfilesRefusesARowChangedAfterInspection(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	require.NoError(t, stores.retentions.putUnsafe(unfrozenSizingEntry(unfrozenSizingLease)))
	candidate := unfrozenSizingCandidateForTest(t, stores.retentions, unfrozenSizingLease)
	changed := unfrozenSizingEntry(unfrozenSizingLease)
	changed.RetainedVolumeNames = []string{"vol-a"}
	require.NoError(t, stores.retentions.putUnsafe(changed))

	require.ErrorIs(t, stores.retentions.FreezeResourceProfiles(candidate, unfrozenSizingProfiles()), errUnfrozenSizingChanged)
}
