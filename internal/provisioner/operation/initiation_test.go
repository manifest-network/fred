package operation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func validInitiationItems() []backend.LeaseItem {
	return []backend.LeaseItem{{SKU: "sku-a", Quantity: 1, ServiceName: "app"}}
}

func TestPurposeInitiationsMakeInvalidKindBackendCombinationsUnconstructable(t *testing.T) {
	items := validInitiationItems()
	provision, err := NewProvisionInitiation(
		"lease-a", "tenant-a", items, "backend-a",
	)
	require.NoError(t, err)
	assert.True(t, provision.valid())
	assert.Equal(t, KindProvision, provision.spec.kind)
	assert.Equal(t, "backend-a", provision.spec.backend)

	restore, err := NewRestoreInitiation("lease-b", "tenant-a", items)
	require.NoError(t, err)
	assert.True(t, restore.valid())
	assert.Equal(t, KindRestore, restore.spec.kind)
	assert.Empty(t, restore.spec.backend,
		"fresh restore cannot select a backend before placement admission")

	recoveredProvision, err := NewRecoveredProvision(
		"lease-c", "tenant-a", items, "backend-a",
	)
	require.NoError(t, err)
	assert.True(t, recoveredProvision.valid())
	assert.Equal(t, KindProvision, recoveredProvision.spec.kind)

	recoveredRestore, err := NewRecoveredRestore(
		"lease-d", "tenant-a", items, "backend-a",
	)
	require.NoError(t, err)
	assert.True(t, recoveredRestore.valid())
	assert.Equal(t, KindRestore, recoveredRestore.spec.kind)
	assert.Equal(t, "backend-a", recoveredRestore.spec.backend,
		"recovery must carry the exact durable backend")

	assert.False(t, (ProvisionInitiation{}).valid())
	assert.False(t, (RestoreInitiation{}).valid())
	assert.False(t, (RecoveredOperation{}).valid())
}

func TestPurposeInitiationConstructorsRejectIncompleteOrInvalidMetadata(t *testing.T) {
	items := validInitiationItems()
	for _, test := range []struct {
		name      string
		construct func() error
	}{
		{name: "provision without lease", construct: func() error {
			_, err := NewProvisionInitiation("", "tenant-a", items, "backend-a")
			return err
		}},
		{name: "provision without tenant", construct: func() error {
			_, err := NewProvisionInitiation("lease-a", "", items, "backend-a")
			return err
		}},
		{name: "provision without items", construct: func() error {
			_, err := NewProvisionInitiation("lease-a", "tenant-a", nil, "backend-a")
			return err
		}},
		{name: "provision without backend", construct: func() error {
			_, err := NewProvisionInitiation("lease-a", "tenant-a", items, "")
			return err
		}},
		{name: "restore with invalid item", construct: func() error {
			_, err := NewRestoreInitiation(
				"lease-a", "tenant-a", []backend.LeaseItem{{SKU: "sku-a"}},
			)
			return err
		}},
		{name: "recovered restore without backend", construct: func() error {
			_, err := NewRecoveredRestore("lease-a", "tenant-a", items, "")
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, test.construct())
		})
	}
}

func TestPurposeInitiationDetachesItems(t *testing.T) {
	items := validInitiationItems()
	provision, err := NewProvisionInitiation(
		"lease-a", "tenant-a", items, "backend-a",
	)
	require.NoError(t, err)
	items[0].SKU = "mutated"
	assert.Equal(t, "sku-a", provision.spec.items[0].SKU)
}

func TestSettlementAuthorityRejectsZeroPurposeArmsWithoutRegistryMutation(t *testing.T) {
	registry := newTestRegistry()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	claim := authority.TryClaimLeaseNow("lease-a")
	require.True(t, claim.Acquired())
	defer authority.ReleaseLease(claim.Claim())

	assert.Equal(t, TrackInvalid,
		authority.TryInitiateProvisionClaimed(
			claim.Claim(), ProvisionInitiation{},
		).Outcome())
	assert.Equal(t, TrackInvalid,
		authority.TryInitiateRestoreClaimed(
			claim.Claim(), RestoreInitiation{},
		).Outcome())
	assert.False(t, authority.Contains("lease-a"))
}
