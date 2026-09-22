package inventory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestSnapshotAmbiguousMembershipHasNoTrustedArm(t *testing.T) {
	for _, construction := range []string{"paired", "split provision first", "split retention first"} {
		for _, rejection := range []string{"overlap", "explicit before", "explicit after"} {
			t.Run(construction+"/"+rejection, func(t *testing.T) {
				storageID := testStorageID(t, "418f47a2-8b1c-4def-8123-456789abcdef")
				collector, err := NewCollector([]string{"backend-a"})
				require.NoError(t, err)
				session := collector.Begin()
				rows := testProvisions("backend-a", "ambiguous", "healthy")
				rows[0].LifecycleGeneration = &backend.LifecycleGenerationObservation{
					Kind: backend.LifecycleGenerationTyped,
					ID:   "123e4567-e89b-42d3-a456-426614174000",
				}
				retentions := []string{"retained"}
				if rejection == "overlap" {
					retentions = append(retentions, "ambiguous")
				}
				if rejection == "explicit before" {
					require.NoError(t, session.RecordUntrusted("backend-a", []string{"ambiguous"}))
				}
				switch construction {
				case "paired":
					observation, recordErr := session.RecordBackend("backend-a", storageID, rows, retentions)
					require.NoError(t, recordErr)
					if rejection != "explicit after" {
						require.Equal(t, []string{"ambiguous"}, observation.UntrustedLeaseUUIDs())
						escaped := observation.UntrustedLeaseUUIDs()
						escaped[0] = "healthy"
						assert.Equal(t, []string{"ambiguous"}, observation.UntrustedLeaseUUIDs())
					}
				case "split provision first":
					require.NoError(t, session.RecordProvision("backend-a", storageID, rows))
					require.NoError(t, session.RecordRetention("backend-a", storageID, retentions))
				case "split retention first":
					require.NoError(t, session.RecordRetention("backend-a", storageID, retentions))
					require.NoError(t, session.RecordProvision("backend-a", storageID, rows))
				}
				if rejection == "explicit after" {
					require.NoError(t, session.RecordUntrusted("backend-a", []string{"ambiguous"}))
				}
				snapshot, err := session.Seal()
				require.NoError(t, err)
				binding := collector.Binding()
				_, trusted := snapshot.Provision(binding, "backend-a", "ambiguous")
				assert.False(t, trusted, "the stale provision cannot expose lifecycle or principal authority")
				assert.False(t, snapshot.TrustedReporter(binding, "backend-a", "ambiguous"))
				assert.False(t, snapshot.RetentionReporter(binding, "backend-a", "ambiguous"))
				assert.Empty(t, snapshot.RetentionReporters(binding, "ambiguous"))
				assert.True(t, snapshot.UntrustedReporter(binding, "backend-a", "ambiguous"))
				assert.True(t, snapshot.Reporter(binding, "backend-a", "ambiguous"))
				assert.True(t, snapshot.LeasePresent(binding, "ambiguous"))
				assert.Equal(t, []string{"backend-a"}, snapshot.LeaseReporters(binding, "ambiguous"))
				assert.Contains(t, snapshot.LeaseUUIDs(binding), "ambiguous")
				assert.False(t, snapshot.OwnerAbsent(binding, "backend-a", storageID, "ambiguous"))
				assert.False(t, snapshot.Complete(binding))
				assert.Empty(t, snapshot.EmptyBackends(binding))
				assert.Equal(t, storageID, snapshot.StorageIdentities(binding)["backend-a"])
				_, trusted = snapshot.Provision(binding, "backend-a", "healthy")
				assert.True(t, trusted)
				assert.True(t, snapshot.TrustedReporter(binding, "backend-a", "healthy"))
				assert.True(t, snapshot.RetentionReporter(binding, "backend-a", "retained"))
			})
		}
	}
}

func TestRejectedSingleEndpointCannotExposeTrustedPayload(t *testing.T) {
	for _, endpoint := range []string{"provision", "retention"} {
		for _, rejectFirst := range []bool{false, true} {
			t.Run(endpoint+"/"+map[bool]string{false: "reject after", true: "reject before"}[rejectFirst], func(t *testing.T) {
				storageID := testStorageID(t, "418f47a2-8b1c-4def-8123-456789abcdef")
				collector, err := NewCollector([]string{"backend-a"})
				require.NoError(t, err)
				session := collector.Begin()
				if rejectFirst {
					require.NoError(t, session.RecordUntrusted("backend-a", []string{"rejected"}))
				}
				if endpoint == "provision" {
					require.NoError(t, session.RecordProvision("backend-a", storageID, testProvisions("backend-a", "rejected")))
				} else {
					require.NoError(t, session.RecordRetention("backend-a", storageID, []string{"rejected"}))
				}
				if !rejectFirst {
					require.NoError(t, session.RecordUntrusted("backend-a", []string{"rejected"}))
				}
				snapshot, err := session.Seal()
				require.NoError(t, err)
				binding := collector.Binding()
				_, present := snapshot.Provision(binding, "backend-a", "rejected")
				assert.False(t, present)
				assert.False(t, snapshot.RetentionReporter(binding, "backend-a", "rejected"))
				assert.False(t, snapshot.TrustedReporter(binding, "backend-a", "rejected"))
				assert.True(t, snapshot.UntrustedReporter(binding, "backend-a", "rejected"))
				assert.True(t, snapshot.LeasePresent(binding, "rejected"))
			})
		}
	}
}
