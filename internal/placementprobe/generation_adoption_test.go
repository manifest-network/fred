package placementprobe

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

type generationCandidateStub struct {
	lease   string
	backend string
}

func (candidate generationCandidateStub) LeaseUUID() string { return candidate.lease }
func (candidate generationCandidateStub) Backend() string   { return candidate.backend }

func TestRequireGenerationAdoptionEvidenceRequiresTheSoleActiveOwner(t *testing.T) {
	active := []backend.ProvisionInfo{{LeaseUUID: probeTargetLease}}
	retained := []backend.RetainedLease{{LeaseUUID: probeTargetLease}}
	tests := []struct {
		name        string
		inventories map[string]Inventory
		want        error
	}{
		{
			name: "active on the lease backend only",
			inventories: map[string]Inventory{
				"backend-a": completeInventory(active, nil),
				"backend-b": completeInventory(nil, nil),
			},
		},
		{
			name: "retained on the lease backend",
			inventories: map[string]Inventory{
				"backend-a": completeInventory(nil, retained),
				"backend-b": completeInventory(nil, nil),
			},
			want: ErrGenerationOwnerEvidence,
		},
		{
			name: "absent everywhere",
			inventories: map[string]Inventory{
				"backend-a": completeInventory(nil, nil),
				"backend-b": completeInventory(nil, nil),
			},
			want: ErrGenerationOwnerEvidence,
		},
		{
			name: "active on another backend",
			inventories: map[string]Inventory{
				"backend-a": completeInventory(nil, nil),
				"backend-b": completeInventory(active, nil),
			},
			want: ErrGenerationOwnerEvidence,
		},
		{
			name: "also retained on another backend",
			inventories: map[string]Inventory{
				"backend-a": completeInventory(active, nil),
				"backend-b": completeInventory(nil, retained),
			},
			want: ErrGenerationOwnerEvidence,
		},
		{
			name: "one backend unprobed",
			inventories: map[string]Inventory{
				"backend-a": completeInventory(active, nil),
			},
			want: ErrIncompleteInventory,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence, err := RequireGenerationAdoptionEvidence(
				[]string{"backend-a", "backend-b"}, test.inventories,
				generationCandidateStub{lease: probeTargetLease, backend: "backend-a"},
			)
			if test.want == nil {
				require.NoError(t, err)
				assert.NotEqual(t, placement.RepairInventorySnapshot{}, evidence)
				return
			}
			require.ErrorIs(t, err, test.want)
			assert.Equal(t, placement.RepairInventorySnapshot{}, evidence)
		})
	}
}

func TestRequireGenerationAdoptionEvidenceRefusesAnUnconfiguredOrMissingCandidate(t *testing.T) {
	inventories := map[string]Inventory{
		"backend-a": completeInventory([]backend.ProvisionInfo{{LeaseUUID: probeTargetLease}}, nil),
		"backend-b": completeInventory(nil, nil),
	}
	configured := []string{"backend-a", "backend-b"}
	_, err := RequireGenerationAdoptionEvidence(configured, inventories,
		generationCandidateStub{lease: probeTargetLease, backend: "backend-z"})
	assert.ErrorIs(t, err, ErrIncompleteInventory)
	_, err = RequireGenerationAdoptionEvidence(configured, inventories,
		generationCandidateStub{lease: "not-a-lease", backend: "backend-a"})
	assert.ErrorIs(t, err, ErrIncompleteInventory)
	_, err = RequireGenerationAdoptionEvidence(configured, inventories, nil)
	assert.ErrorIs(t, err, ErrIncompleteInventory)
	var typedNil *generationCandidateStub
	_, err = RequireGenerationAdoptionEvidence(configured, inventories, typedNil)
	assert.ErrorIs(t, err, ErrIncompleteInventory)
}
