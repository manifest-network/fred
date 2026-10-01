package placementprobe

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func TestProbeRetirementTargetRefusesATargetNoPlanMinted(t *testing.T) {
	liveness, err := ProbeRetirementTarget(t.Context(), nil, placement.RetirementProbeTarget{})
	require.ErrorContains(t, err, "retirement probe target is invalid")
	require.Equal(t, retirementTargetLivenessInvalid, liveness)
}
