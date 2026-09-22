package inventory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPairedOverlapCannotBeSplicedOrUpgradeRejectedEvidence(t *testing.T) {
	for _, scenario := range []string{
		"paired", "split", "rejected before", "rejected after", "foreign storage",
		"mismatched pair", "foreign binding", "another reporter", "stale epoch", "zero snapshot",
	} {
		t.Run(scenario, func(t *testing.T) {
			storageID := testStorageID(t, "418f47a2-8b1c-4def-8123-456789abcdef")
			foreignID := testStorageID(t, "418f47a2-8b1c-4def-8123-456789abcdee")
			collector, err := NewCollector([]string{"backend-a", "backend-b"})
			require.NoError(t, err)
			session := collector.Begin()
			if scenario == "rejected before" {
				require.NoError(t, session.RecordUntrusted("backend-a", []string{"lease"}))
			}
			rows := testProvisions("backend-a", "lease")
			if scenario == "split" || scenario == "mismatched pair" {
				require.NoError(t, session.RecordProvision("backend-a", storageID, rows))
				retentionID := storageID
				if scenario == "mismatched pair" {
					retentionID = foreignID
				}
				require.NoError(t, session.RecordRetention("backend-a", retentionID, []string{"lease"}))
			} else {
				_, err = session.RecordBackend("backend-a", storageID, rows, []string{"lease"})
				require.NoError(t, err)
			}
			if scenario == "rejected after" {
				require.NoError(t, session.RecordUntrusted("backend-a", []string{"lease"}))
			}
			if scenario == "another reporter" {
				require.NoError(t, session.RecordUntrusted("backend-b", []string{"lease"}))
			}
			snapshot, err := session.Seal()
			require.NoError(t, err)
			binding := collector.Binding()
			switch scenario {
			case "foreign storage":
				storageID = foreignID
			case "foreign binding":
				foreign, createErr := NewCollector([]string{"backend-a", "backend-b"})
				require.NoError(t, createErr)
				binding = foreign.Binding()
			case "stale epoch":
				collector.Begin()
			case "zero snapshot":
				snapshot = Snapshot{}
			}
			observation, present := snapshot.PairedOverlap(binding, "backend-a", "lease", storageID)
			wantPresent := scenario == "paired" || scenario == "split"
			assert.Equal(t, wantPresent, present)
			if wantPresent {
				assert.Equal(t, "lease", observation.Provision().LeaseUUID())
				assert.Equal(t, "backend-a", observation.Provision().BackendName())
			}
			_, positive := snapshot.Provision(binding, "backend-a", "lease")
			assert.False(t, positive)
			assert.False(t, snapshot.TrustedReporter(binding, "backend-a", "lease"))
			assert.False(t, snapshot.OwnerAbsent(binding, "backend-a", storageID, "lease"))
			assert.False(t, snapshot.Complete(binding))
		})
	}
}
