package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReleaseRuntimeIdentityIsAValidatedDisjointProjection(t *testing.T) {
	typedRelease := validRuntimeAuthorityRelease()
	typed, ok := typedRelease.RuntimeIdentity()
	require.True(t, ok)
	assert.Equal(t, ReleaseAuthorityTyped, typed.Class())
	assert.Equal(t, typedRelease.OperationID, typed.OperationID())
	assert.Equal(t, typedRelease.RuntimeAuthority.Tenant(), typed.Tenant())

	legacyAuthority, err := NewLegacyRuntimeAuthority(
		"tenant-a",
		"22222222-2222-4222-8222-222222222222",
		"https://fred.example/callbacks/provision",
		"https://fred.example/callbacks/provision",
	)
	require.NoError(t, err)
	legacyRelease := typedRelease
	legacyRelease.OperationID = ""
	legacyRelease.RuntimeAuthority = nil
	legacyRelease.LegacyRuntimeAuthority = &legacyAuthority
	legacy, ok := legacyRelease.RuntimeIdentity()
	require.True(t, ok)
	assert.Equal(t, ReleaseAuthorityLegacy, legacy.Class())
	assert.Empty(t, legacy.OperationID())
	assert.Equal(t, legacyAuthority.CallbackURL(), legacy.CallbackURL())

	for _, release := range []Release{
		{},
		{RuntimeAuthority: &ReleaseRuntimeAuthority{}},
		{LegacyRuntimeAuthority: &LegacyRuntimeAuthority{}},
		func() Release {
			mixed := typedRelease
			mixed.LegacyRuntimeAuthority = &legacyAuthority
			return mixed
		}(),
		func() Release {
			mismatched := typedRelease
			mismatched.OperationID = "9a72fbc1-38c8-4f31-87f7-f689979b9324"
			return mismatched
		}(),
		func() Release {
			mismatched := legacyRelease
			mismatched.OperationID = releaseRuntimeAuthorityOperationID
			return mismatched
		}(),
	} {
		_, ok := release.RuntimeIdentity()
		assert.False(t, ok)
	}
}
