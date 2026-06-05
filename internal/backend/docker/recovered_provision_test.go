package docker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
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
			CallbackURL:       "http://cb",
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
			ContainerIDs:      []string{"c1", "c2"},
			StackManifest:     nil,
			ServiceContainers: map[string][]string{"app": {"c1", "c2"}},
		},
		volumeCleanupAttempts: 4,
	}
}

func TestRecoveredProvision_Materialize_RoundTripsEveryField(t *testing.T) {
	rec := fullRecoveredProvision()
	p := rec.materialize()
	require.NotNil(t, p)
	assert.Equal(t, rec.ProvisionState, p.ProvisionState, "ProvisionState must round-trip wholesale")
	assert.Equal(t, rec.volumeCleanupAttempts, p.VolumeCleanupAttempts, "wrapper field must round-trip")
}

func TestRecoveredFromProvision_ClonesReferenceFields(t *testing.T) {
	src := &provision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         "lease-1",
			Status:            backend.ProvisionStatusFailing,
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			ContainerIDs:      []string{"c1"},
			ServiceContainers: map[string][]string{"app": {"c1"}},
		},
		VolumeCleanupAttempts: 2,
	}
	rec := recoveredFromProvision(src)
	// Mutating the clone must not touch the source's backing arrays/maps.
	rec.Items[0].ServiceName = "mutated"
	rec.ContainerIDs[0] = "mutated"
	rec.ServiceContainers["app"][0] = "mutated"
	assert.Equal(t, "app", src.Items[0].ServiceName, "Items must be cloned")
	assert.Equal(t, "c1", src.ContainerIDs[0], "ContainerIDs must be cloned")
	assert.Equal(t, "c1", src.ServiceContainers["app"][0], "ServiceContainers must be deep-cloned")
	assert.Equal(t, 2, rec.volumeCleanupAttempts, "wrapper field carried")
}
