package inventory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func testStorageID(t *testing.T, text string) backendidentity.ID {
	t.Helper()
	id, err := backendidentity.Parse(text)
	require.NoError(t, err)
	return id
}

func testProvisions(backendName string, leaseUUIDs ...string) []backend.ProvisionInfo {
	provisions := make([]backend.ProvisionInfo, 0, len(leaseUUIDs))
	for _, leaseUUID := range leaseUUIDs {
		provisions = append(provisions, backend.ProvisionInfo{
			LeaseUUID: leaseUUID, BackendName: backendName,
		})
	}
	return provisions
}

func TestCollectorRejectsEmptyTopology(t *testing.T) {
	_, err := NewCollector(nil)
	require.ErrorIs(t, err, ErrInvalidTopology)
}

func TestNilSessionRecordMethodsAreTotal(t *testing.T) {
	var session *Session
	storageID := testStorageID(t, "018f47a2-8b1c-4def-8123-456789abcdef")
	require.ErrorIs(t,
		session.RecordProvision("backend-a", storageID, nil),
		ErrInvalidSession,
	)
	require.ErrorIs(t,
		session.RecordRetention("backend-a", storageID, nil),
		ErrInvalidSession,
	)
}

func TestSnapshotRequiresPairedCurrentCollectorEvidence(t *testing.T) {
	backendA := testStorageID(t, "018f47a2-8b1c-4def-8123-456789abcdef")
	otherStorage := testStorageID(t, "118f47a2-8b1c-4def-8123-456789abcdef")
	collector, err := NewCollector([]string{"backend-a", "backend-b"})
	require.NoError(t, err)

	t.Run("partial fleet can prove one exact owner absent", func(t *testing.T) {
		session := collector.Begin()
		require.NoError(t, session.RecordProvision("backend-a", backendA, nil))
		require.NoError(t, session.RecordRetention("backend-a", backendA, nil))
		snapshot, err := session.Seal()
		require.NoError(t, err)
		assert.True(t, snapshot.OwnerAbsent(
			collector.Binding(), "backend-a", backendA, "lease-a",
		))
		assert.False(t, snapshot.OwnerAbsent(
			collector.Binding(), "backend-b", backendA, "lease-a",
		), "an unavailable peer contributes no negative evidence")
	})

	t.Run("one endpoint cannot stand in for the other", func(t *testing.T) {
		session := collector.Begin()
		require.NoError(t, session.RecordProvision("backend-a", backendA, nil))
		snapshot, err := session.Seal()
		require.NoError(t, err)
		assert.False(t, snapshot.OwnerAbsent(
			collector.Binding(), "backend-a", backendA, "lease-a",
		))
	})

	t.Run("positive in either endpoint defeats absence", func(t *testing.T) {
		for _, provisionHasLease := range []bool{false, true} {
			session := collector.Begin()
			provisionLeases, retentionLeases := []backend.ProvisionInfo(nil), []string{"lease-a"}
			if provisionHasLease {
				provisionLeases, retentionLeases = testProvisions("backend-a", "lease-a"), nil
			}
			require.NoError(t, session.RecordProvision(
				"backend-a", backendA, provisionLeases,
			))
			require.NoError(t, session.RecordRetention(
				"backend-a", backendA, retentionLeases,
			))
			snapshot, err := session.Seal()
			require.NoError(t, err)
			assert.True(t, snapshot.LeasePresent(collector.Binding(), "lease-a"))
			assert.False(t, snapshot.OwnerAbsent(
				collector.Binding(), "backend-a", backendA, "lease-a",
			))
		}
	})

	t.Run("mixed physical identities cannot prove absence", func(t *testing.T) {
		session := collector.Begin()
		require.NoError(t, session.RecordProvision("backend-a", backendA, nil))
		require.NoError(t, session.RecordRetention("backend-a", otherStorage, nil))
		snapshot, err := session.Seal()
		require.NoError(t, err)
		assert.False(t, snapshot.OwnerAbsent(
			collector.Binding(), "backend-a", backendA, "lease-a",
		))
	})
}

func TestSnapshotRejectsForgedForeignAndStaleAuthority(t *testing.T) {
	storageID := testStorageID(t, "218f47a2-8b1c-4def-8123-456789abcdef")
	collector, err := NewCollector([]string{"backend-a"})
	require.NoError(t, err)
	foreign, err := NewCollector([]string{"backend-a"})
	require.NoError(t, err)

	assert.False(t, (Snapshot{}).ValidFor(collector.Binding()), "zero snapshot is forged")
	session := collector.Begin()
	require.NoError(t, session.RecordProvision("backend-a", storageID, nil))
	require.NoError(t, session.RecordRetention("backend-a", storageID, nil))
	snapshot, err := session.Seal()
	require.NoError(t, err)
	assert.False(t, snapshot.ValidFor(foreign.Binding()))
	assert.True(t, snapshot.ValidFor(collector.Binding()))

	collector.Begin()
	assert.False(t, snapshot.ValidFor(collector.Binding()), "new epoch revokes old evidence")
}

func TestSnapshotSeparatesTrustedOwnerFromConservativeReporter(t *testing.T) {
	storageID := testStorageID(t, "318f47a2-8b1c-4def-8123-456789abcdef")
	collector, err := NewCollector([]string{"backend-a", "backend-b"})
	require.NoError(t, err)
	session := collector.Begin()
	require.NoError(t, session.RecordProvision(
		"backend-a", storageID, testProvisions("backend-a", "trusted"),
	))
	require.NoError(t, session.RecordRetention("backend-a", storageID, []string{"retained"}))
	require.NoError(t, session.RecordUntrusted("backend-b", []string{"untrusted"}))
	snapshot, err := session.Seal()
	require.NoError(t, err)

	assert.True(t, snapshot.TrustedReporter(
		collector.Binding(), "backend-a", "trusted",
	))
	assert.True(t, snapshot.Reporter(
		collector.Binding(), "backend-b", "untrusted",
	))
	assert.False(t, snapshot.TrustedReporter(
		collector.Binding(), "backend-b", "untrusted",
	))
	assert.True(t, snapshot.RetentionReporter(
		collector.Binding(), "backend-a", "retained",
	))
	assert.Equal(t, []string{"backend-a"},
		snapshot.RetentionReporters(collector.Binding(), "retained"))
	assert.False(t, snapshot.RetentionReporter(
		collector.Binding(), "backend-a", "trusted",
	))
	assert.True(t, snapshot.LeasePresent(collector.Binding(), "untrusted"))
	assert.False(t, snapshot.Complete(collector.Binding()))
}

func TestCollectorFixesBackendIdentityAndTopologyAtConstruction(t *testing.T) {
	storageID := testStorageID(t, "318f47a2-8b1c-4def-8123-456789abcdef")
	collector, err := NewCollector([]string{"backend-b", "backend-a"})
	require.NoError(t, err)
	assert.True(t, collector.Binding().MatchesTopology([]string{"backend-a", "backend-b"}))
	assert.False(t, collector.Binding().MatchesTopology([]string{"backend-a"}))

	session := collector.Begin()
	assert.ErrorIs(t,
		session.RecordProvision("backend-c", storageID, nil), ErrInvalidSession)
	require.NoError(t, session.RecordProvision("backend-a", storageID, nil))
	assert.ErrorIs(t,
		session.RecordProvision("backend-a", storageID, nil), ErrInvalidSession)
}

func TestProvisionRowsAreBackendBoundUniqueAndDetached(t *testing.T) {
	storageID := testStorageID(t, "418f47a2-8b1c-4def-8123-456789abcdef")
	collector, err := NewCollector([]string{"backend-a", "backend-b"})
	require.NoError(t, err)

	t.Run("backend identity is part of the recorded row", func(t *testing.T) {
		session := collector.Begin()
		err := session.RecordProvision("backend-a", storageID, []backend.ProvisionInfo{{
			LeaseUUID: "lease-a", BackendName: "backend-b",
		}})
		require.ErrorIs(t, err, ErrInvalidSession)
	})

	t.Run("duplicate lease rows cannot collapse silently", func(t *testing.T) {
		session := collector.Begin()
		err := session.RecordProvision("backend-a", storageID, []backend.ProvisionInfo{
			{LeaseUUID: "lease-a", BackendName: "backend-a"},
			{LeaseUUID: "lease-a", BackendName: "backend-a"},
		})
		require.ErrorIs(t, err, ErrInvalidSession)
	})

	t.Run("returned lifecycle generation cannot mutate sealed evidence", func(t *testing.T) {
		session := collector.Begin()
		require.NoError(t, session.RecordProvision(
			"backend-a", storageID, []backend.ProvisionInfo{{
				LeaseUUID: "lease-a", BackendName: "backend-a",
				LifecycleGeneration: &backend.LifecycleGenerationObservation{
					Kind: backend.LifecycleGenerationTyped,
					ID:   "123e4567-e89b-42d3-a456-426614174000",
				},
			}},
		))
		snapshot, sealErr := session.Seal()
		require.NoError(t, sealErr)
		row, present := snapshot.Provision(collector.Binding(), "backend-a", "lease-a")
		require.True(t, present)
		generation := row.LifecycleGeneration()
		require.NotNil(t, generation)
		generation.ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

		rowAgain, present := snapshot.Provision(collector.Binding(), "backend-a", "lease-a")
		require.True(t, present)
		assert.Equal(t, "123e4567-e89b-42d3-a456-426614174000",
			rowAgain.LifecycleGeneration().ID)
		_, foreign := snapshot.Provision(collector.Binding(), "backend-b", "lease-a")
		assert.False(t, foreign)
	})
}
