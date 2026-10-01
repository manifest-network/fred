package placementprobe

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/config"
)

func fencedProbeConfig() *config.Config {
	return &config.Config{Backends: []config.BackendConfig{
		{Name: "backend-a", URL: "https://backend-a.example", HMACSecret: "backend-a-secret-0123456789abcdef0123"},
		{Name: "backend-b", URL: "https://backend-b.example", HMACSecret: "backend-b-secret-0123456789abcdef0123", Fenced: true},
	}}
}

func TestFleetObservationRefusesWhileAnyBackendIsFenced(t *testing.T) {
	cfg := fencedProbeConfig()

	_, err := NewClients(cfg)
	require.ErrorIs(t, err, ErrIncompleteInventory)
	require.ErrorIs(t, err, backend.ErrBackendFenced)

	pins := fixedIdentityResolver{
		"backend-a": probeStorageID("550e8400-e29b-41d4-a716-446655440000"),
		"backend-b": probeStorageID("6ba7b811-9dad-41d1-80b4-00c04fd430c8"),
	}
	_, err = NewIdentityBoundClients(cfg, pins)
	require.ErrorIs(t, err, ErrIncompleteInventory)
	require.ErrorIs(t, err, backend.ErrBackendFenced)

	_, err = NewAuthenticatedFleet(cfg)
	require.ErrorIs(t, err, backend.ErrBackendFenced,
		"offline repair evidence cannot come from a fenced backend")
}

func TestRetirementTargetFencedRendersForThePlan(t *testing.T) {
	text, err := RetirementTargetFenced.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "fenced", string(text))
}
