package inventory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleReporterObservationRequiresCompleteLeaseEvidence(t *testing.T) {
	for _, scenario := range []string{"unrelated overlap", "missing peer", "mismatched peer", "same lease overlap", "multiple reporters", "absent"} {
		t.Run(scenario, func(t *testing.T) {
			collector, err := NewCollector([]string{"backend-a", "backend-b"})
			require.NoError(t, err)
			session := collector.Begin()
			storageA := testStorageID(t, "418f47a2-8b1c-4def-8123-456789abcdef")
			storageB := testStorageID(t, "518f47a2-8b1c-4def-8123-456789abcdef")
			leases := []string{"other"}
			if scenario != "absent" {
				leases = append(leases, "target")
			}
			require.NoError(t, session.RecordProvision("backend-a", storageA, testProvisions("backend-a", leases...)))
			retentions := []string{"other"}
			if scenario == "same lease overlap" {
				retentions = append(retentions, "target")
			}
			require.NoError(t, session.RecordRetention("backend-a", storageA, retentions))
			if scenario != "missing peer" {
				peerLeases := []string(nil)
				if scenario == "multiple reporters" {
					peerLeases = []string{"target"}
				}
				require.NoError(t, session.RecordProvision("backend-b", storageB, testProvisions("backend-b", peerLeases...)))
				retentionID := storageB
				if scenario == "mismatched peer" {
					retentionID = storageA
				}
				require.NoError(t, session.RecordRetention("backend-b", retentionID, nil))
			}
			snapshot, err := session.Seal()
			require.NoError(t, err)
			binding := collector.Binding()
			proof := snapshot.PairedTopology(binding).SingleReporter("target")
			if scenario != "unrelated overlap" {
				assert.False(t, proof.Matches(binding, "target", "backend-a"))
				assert.False(t, proof.Matches(binding, "target", "backend-b"),
					"a conflicting second reporter cannot become sole authority by iteration order")
				return
			}
			assert.False(t, snapshot.Complete(binding))
			assert.True(t, snapshot.PairedTopology(binding).ValidFor(binding))
			assert.True(t, proof.Matches(binding, "target", "backend-a"))
			assert.False(t, proof.Matches(binding, "other", "backend-a"))
			assert.False(t, proof.Matches(binding, "target", "backend-b"))
			foreign, err := NewCollector([]string{"backend-a", "backend-b"})
			require.NoError(t, err)
			assert.False(t, proof.Matches(foreign.Binding(), "target", "backend-a"))
			collector.Begin()
			assert.False(t, proof.Matches(binding, "target", "backend-a"))
			assert.False(t, snapshot.PairedTopology(binding).ValidFor(binding))
		})
	}
}

func TestPairedCoverageProvesOnlyTheNamedPairedBackends(t *testing.T) {
	collector, err := NewCollector([]string{"backend-a", "backend-b", "backend-c"})
	require.NoError(t, err)
	session := collector.Begin()
	storageA := testStorageID(t, "418f47a2-8b1c-4def-8123-456789abcdef")
	storageB := testStorageID(t, "518f47a2-8b1c-4def-8123-456789abcdef")
	storageC := testStorageID(t, "618f47a2-8b1c-4def-8123-456789abcdef")
	// backend-a answers both endpoints; backend-b answers only provisions;
	// backend-c answers both endpoints under two different identities.
	require.NoError(t, session.RecordProvision("backend-a", storageA, testProvisions("backend-a", "lease")))
	require.NoError(t, session.RecordRetention("backend-a", storageA, nil))
	require.NoError(t, session.RecordProvision("backend-b", storageB, nil))
	require.NoError(t, session.RecordProvision("backend-c", storageC, nil))
	require.NoError(t, session.RecordRetention("backend-c", storageA, nil))
	snapshot, err := session.Seal()
	require.NoError(t, err)
	binding := collector.Binding()

	assert.True(t, snapshot.PairedCoverage(binding, []string{"backend-a"}).ValidFor(binding))
	assert.True(t, snapshot.PairedCoverage(binding, []string{}).ValidFor(binding),
		"an empty requirement is proved by the sealed collection itself")
	for name, backends := range map[string][]string{
		"one endpoint only":        {"backend-a", "backend-b"},
		"mismatched identity":      {"backend-c"},
		"unconfigured backend":     {"backend-a", "backend-z"},
		"duplicate backend":        {"backend-a", "backend-a"},
		"silent backend is absent": {"backend-a", "backend-b", "backend-c"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.False(t, snapshot.PairedCoverage(binding, backends).ValidFor(binding))
		})
	}

	assert.False(t, PairedCoverageObservation{}.ValidFor(binding), "the zero value is invalid")
	foreign, err := NewCollector([]string{"backend-a", "backend-b", "backend-c"})
	require.NoError(t, err)
	assert.False(t, snapshot.PairedCoverage(foreign.Binding(), []string{"backend-a"}).ValidFor(foreign.Binding()))
	proof := snapshot.PairedCoverage(binding, []string{"backend-a"})
	collector.Begin()
	assert.False(t, proof.ValidFor(binding), "a newer collection epoch retires the proof")
}
