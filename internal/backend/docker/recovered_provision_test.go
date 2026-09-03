package docker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// fullRecoveredProvision builds a recoveredProvision with every field set to a
// distinct non-zero value, so materialize round-tripping can be asserted
// field-by-field.
func fullRecoveredProvision() recoveredProvision {
	return recoveredProvision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         "lease-1",
			Tenant:            "tenant-a",
			ProviderUUID:      "prov-1",
			SKU:               "docker-small",
			Status:            backend.ProvisionStatusReady,
			Quantity:          2,
			CreatedAt:         time.Unix(1700000000, 0),
			FailCount:         3,
			LastError:         "boom",
			CallbackURL:       "http://cb/callbacks/provision",
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
			ContainerIDs:      []string{"c1", "c2"},
			StackManifest:     nil,
			ServiceContainers: map[string][]string{"app": {"c1", "c2"}},
		},
		resourceProfiles: []shared.SKUResourceSnapshot{{
			SKU: "docker-small", CPUCores: 0.5, MemoryMB: 512, ScratchDiskMB: 64,
		}},
		volumeCleanupAttempts: 4,
	}
}

func TestRecoveredProvision_Materialize_RoundTripsEveryField(t *testing.T) {
	rec := fullRecoveredProvision()
	p := rec.materialize()
	require.NotNil(t, p)
	assert.Equal(t, rec.ProvisionState, p.ProvisionState, "ProvisionState must round-trip wholesale")
	assert.Equal(t, rec.resourceProfiles, p.ResourceProfiles, "resource profiles must round-trip")
	assert.Equal(t, rec.volumeCleanupAttempts, p.VolumeCleanupAttempts, "wrapper field must round-trip")
	p.ResourceProfiles[0].ScratchDiskMB = 2048
	assert.Equal(t, int64(64), rec.resourceProfiles[0].ScratchDiskMB,
		"materialize must not alias the recovered snapshot")
}

func TestRecoveredFromProvision_ClonesReferenceFields(t *testing.T) {
	src := &provision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         "lease-1",
			Status:            backend.ProvisionStatusFailing,
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			ResourceProfiles:  []shared.SKUResourceSnapshot{{SKU: "docker-small", CPUCores: 0.5, MemoryMB: 512, ScratchDiskMB: 64}},
			ContainerIDs:      []string{"c1"},
			ServiceContainers: map[string][]string{"app": {"c1"}},
		},
		ResourceProfiles: []shared.SKUResourceSnapshot{{
			SKU: "docker-small", CPUCores: 0.5, MemoryMB: 512, ScratchDiskMB: 64,
		}},
		VolumeCleanupAttempts: 2,
	}
	rec := recoveredFromProvision(src)
	// Mutating the clone must not touch the source's backing arrays/maps.
	rec.Items[0].ServiceName = "mutated"
	rec.ContainerIDs[0] = "mutated"
	rec.ServiceContainers["app"][0] = "mutated"
	rec.ProvisionState.ResourceProfiles[0].ScratchDiskMB = 1024
	rec.resourceProfiles[0].ScratchDiskMB = 2048
	assert.Equal(t, "app", src.Items[0].ServiceName, "Items must be cloned")
	assert.Equal(t, "c1", src.ContainerIDs[0], "ContainerIDs must be cloned")
	assert.Equal(t, "c1", src.ServiceContainers["app"][0], "ServiceContainers must be deep-cloned")
	assert.Equal(t, int64(64), src.ProvisionState.ResourceProfiles[0].ScratchDiskMB,
		"embedded provision-state resource profiles must be cloned")
	assert.Equal(t, int64(64), src.ResourceProfiles[0].ScratchDiskMB, "resource profiles must be cloned")
	assert.Equal(t, 2, rec.volumeCleanupAttempts, "wrapper field carried")
}

func TestRecoveredFromProvision_PreservesNilVsEmpty(t *testing.T) {
	// slices.Clone preserves nil-vs-empty, so a kept entry's reference fields
	// keep the same nil-ness they had before normalization (byte-equivalent to
	// the prior preserve-by-pointer path; the old append([]T(nil), ...) idiom
	// collapsed a non-nil empty slice to nil).
	t.Run("nil stays nil", func(t *testing.T) {
		rec := recoveredFromProvision(&provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1"}})
		assert.Nil(t, rec.Items)
		assert.Nil(t, rec.ContainerIDs)
		assert.Nil(t, rec.ServiceContainers)
		assert.Nil(t, rec.resourceProfiles)
	})
	t.Run("non-nil empty stays non-nil empty", func(t *testing.T) {
		rec := recoveredFromProvision(&provision{ProvisionState: leasesm.ProvisionState{
			LeaseUUID:    "L1",
			Items:        []backend.LeaseItem{},
			ContainerIDs: []string{},
		}, ResourceProfiles: []shared.SKUResourceSnapshot{}})
		assert.NotNil(t, rec.Items, "non-nil empty Items must stay non-nil")
		assert.Empty(t, rec.Items)
		assert.NotNil(t, rec.ContainerIDs, "non-nil empty ContainerIDs must stay non-nil")
		assert.Empty(t, rec.ContainerIDs)
		assert.NotNil(t, rec.resourceProfiles, "non-nil empty resource profiles must stay non-nil")
		assert.Empty(t, rec.resourceProfiles)
	})
}
