package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInventoryProjectorIsOneStoreTopologyBoundAuthority(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-b", "backend-a"}))

	projector, err := store.BindInventoryProjector([]string{"backend-a", "backend-b"})
	require.NoError(t, err)
	require.True(t, projector.Valid())
	same, err := store.BindInventoryProjector([]string{"backend-b", "backend-a"})
	require.NoError(t, err)
	assert.Same(t, projector, same)

	_, err = store.BindInventoryProjector([]string{"backend-a"})
	require.ErrorIs(t, err, ErrInventoryProjectorConflict)
}

func TestInventoryProjectorRejectsZeroForeignAndStaleSnapshots(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a"}))
	projector, err := store.BindInventoryProjector([]string{"backend-a"})
	require.NoError(t, err)

	var zero InventoryProjector
	assert.False(t, zero.Valid())
	assert.Nil(t, zero.BeginCollection())
	_, err = zero.Project(InventoryFence{}, InventoryProjection{})
	require.ErrorIs(t, err, ErrInventoryProjectorConflict)

	foreignStore := newTestStore(t)
	require.NoError(t, configureBackendTopologyForTest(foreignStore, []string{"backend-a"}))
	foreign, err := foreignStore.BindInventoryProjector([]string{"backend-a"})
	require.NoError(t, err)
	foreignSession := foreign.BeginCollection()
	id := testBackendStorageID("backend-a")
	require.NoError(t, foreignSession.RecordProvision("backend-a", id, nil))
	require.NoError(t, foreignSession.RecordRetention("backend-a", id, nil))
	foreignSnapshot, err := foreignSession.Seal()
	require.NoError(t, err)
	fence := store.BeginInventorySession()
	_, err = projector.Project(fence, InventoryProjection{AbsenceEvidence: foreignSnapshot})
	store.EndInventorySession(fence)
	require.ErrorIs(t, err, ErrInvalidInventoryEvidence)

	session := projector.BeginCollection()
	require.NoError(t, session.RecordProvision("backend-a", id, nil))
	require.NoError(t, session.RecordRetention("backend-a", id, nil))
	stale, err := session.Seal()
	require.NoError(t, err)
	projector.BeginCollection()
	fence = store.BeginInventorySession()
	_, err = projector.Project(fence, InventoryProjection{AbsenceEvidence: stale})
	store.EndInventorySession(fence)
	require.ErrorIs(t, err, ErrInvalidInventoryEvidence)
}

func TestInventoryProjectionRejectsMalformedCandidateSets(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a", "backend-b"}))
	projector, err := store.BindInventoryProjector([]string{"backend-a", "backend-b"})
	require.NoError(t, err)

	for _, projection := range []InventoryProjection{
		{Conflicts: map[string][]string{"lease": {"backend-a", "backend-a"}}},
		{Conflicts: map[string][]string{"lease": {"backend-a", ""}}},
		{UntrustedPositives: map[string][]string{"lease": {"backend-a", "backend-a"}}},
		{UntrustedPositives: map[string][]string{"lease": {""}}},
	} {
		fence := store.BeginInventorySession()
		_, err := projector.Project(fence, projection)
		store.EndInventorySession(fence)
		require.ErrorIs(t, err, ErrInvalidPlacement)
	}
}
