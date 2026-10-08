package shared

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

// unrecordedV013Release is an adopted v0.13 active release before any backfill:
// no topology, resource profiles, or callback principal.
func unrecordedV013Release() Release {
	return Release{
		Version:   1,
		Manifest:  []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:     "stack",
		Status:    "active",
		CreatedAt: time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC),
	}
}

func v013Items() ([]backend.LeaseItem, []SKUResourceSnapshot) {
	return []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}},
		[]SKUResourceSnapshot{{SKU: "sku-a", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}}
}

func v013Authority(t *testing.T, callbackBase string) LegacyRuntimeAuthority {
	t.Helper()
	authority, err := NewLegacyRuntimeAuthority(
		"tenant-a", "22222222-2222-4222-8222-222222222222",
		callbackBase+"/callbacks/provision", callbackBase+"/callbacks/provision",
	)
	require.NoError(t, err)
	return authority
}

func TestFreezeLegacyRuntimeAuthorityOnlyFreezesAnObservation(t *testing.T) {
	items, profiles := v013Items()
	observed := v013Authority(t, "https://old.example")
	freezable := unrecordedV013Release()
	freezable.Items, freezable.ResourceProfiles = items, profiles

	freeze, err := FreezeLegacyRuntimeAuthority(testLeaseUUID("freeze"), freezable, observed)
	require.NoError(t, err)
	assert.True(t, freeze.valid())
	assert.False(t, LegacyRuntimeAuthorityFreeze{}.valid(), "the zero value is invalid")

	for _, tc := range []struct {
		name     string
		lease    string
		release  func() Release
		observed LegacyRuntimeAuthority
		err      string
	}{
		{"recorded authority", testLeaseUUID("freeze"), func() Release {
			recorded := freezable
			recorded.LegacyRuntimeAuthority = &observed
			return recorded
		}, observed, "already durable"},
		{"typed release", testLeaseUUID("freeze"), func() Release {
			typed := validRuntimeAuthorityRelease()
			typed.Version, typed.Status = 1, "active"
			return typed
		}, observed, "typed release authority"},
		{"no recorded topology", testLeaseUUID("freeze"), unrecordedV013Release, observed, "fully backfilled"},
		{"superseded release", testLeaseUUID("freeze"), func() Release {
			superseded := freezable
			superseded.Status = "superseded"
			return superseded
		}, observed, "positive active version"},
		{"invalid observation", testLeaseUUID("freeze"), func() Release { return freezable }, LegacyRuntimeAuthority{}, "is invalid"},
		{"no lease", "", func() Release { return freezable }, observed, "requires a lease"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := FreezeLegacyRuntimeAuthority(tc.lease, tc.release(), tc.observed)
			require.ErrorContains(t, err, tc.err)
		})
	}
}

// Every backfill writes through backfillActiveRelease. A replay of authority
// already recorded writes nothing, so maintenance rows recorded afterwards
// cannot refuse it (ENG-1313). A write still may not re-encode a history that
// holds a maintenance row, and an unresolved maintenance target refuses even a
// replay.
func TestReleaseBackfillMaintenanceFences(t *testing.T) {
	items, profiles := v013Items()
	recordedAuthority := v013Authority(t, "https://old.example")
	targetAuthority := v013Authority(t, "https://new.example")

	// recorded returns the v0.13 active row with the fields that the named
	// backfill writes already present or absent.
	recorded := func(backfill string, present bool) Release {
		release := unrecordedV013Release()
		release.Items, release.ResourceProfiles = items, profiles
		release.LegacyRuntimeAuthority = &recordedAuthority
		if present {
			return release
		}
		switch backfill {
		case "topology":
			release.Items, release.ResourceProfiles = nil, nil
		case "resource profiles":
			release.ResourceProfiles = nil
		}
		// Maintenance requires a recorded principal, but a raw history can
		// still name one only on the maintenance row.
		release.LegacyRuntimeAuthority = nil
		return release
	}
	maintenanceRow := func(status string) Release {
		row := unrecordedV013Release()
		row.Version = 2
		row.Status = status
		row.Items, row.ResourceProfiles = items, profiles
		row.LegacyRuntimeAuthority = &targetAuthority
		row.MaintenanceID = newTestMaintenanceID(t)
		row.CreatedAt = time.Now().UTC()
		if status == "failed" {
			row.Reason, row.Message = backend.ReasonImagePullFailed, backend.MsgImagePullFailed
		}
		return row
	}
	backfill := func(t *testing.T, releases *ReleaseStore, leaseUUID, name string) error {
		t.Helper()
		switch name {
		case "topology":
			return releases.backfillLegacyActiveAuthority(leaseUUID, unrecordedV013Release(), items, profiles)
		case "resource profiles":
			return releases.backfillActiveResourceProfiles(leaseUUID, 1, items, profiles)
		case "runtime authority":
			fence := unrecordedV013Release()
			fence.Items, fence.ResourceProfiles = items, profiles
			freeze, err := FreezeLegacyRuntimeAuthority(leaseUUID, fence, recordedAuthority)
			require.NoError(t, err)
			return releases.backfillLegacyRuntimeAuthority(freeze)
		}
		t.Fatalf("unknown backfill %q", name)
		return nil
	}

	for _, name := range []string{"topology", "resource profiles", "runtime authority"} {
		for _, tc := range []struct {
			fence   string
			present bool
			status  string
			refused bool
		}{
			{"replay after failed maintenance", true, "failed", false},
			{"write under failed maintenance", false, "failed", true},
			{"replay under unresolved maintenance", true, "deploying", true},
		} {
			t.Run(name+"/"+tc.fence, func(t *testing.T) {
				releases, err := newUnboundReleaseStoreForTest(ReleaseStoreConfig{
					DBPath: filepath.Join(t.TempDir(), "releases.db"),
				})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, releases.Close()) })
				leaseUUID := testLeaseUUID("backfill-fences")
				history := []Release{recorded(name, tc.present), maintenanceRow(tc.status)}
				require.NoError(t, validateReleaseHistory(history), "the fixture is a readable history")
				before := writeRawReleaseHistoryForTest(t, releases, leaseUUID, history)

				err = backfill(t, releases, leaseUUID, name)
				if tc.refused {
					require.ErrorIs(t, err, ErrMaintenanceReleaseClaimRequired)
				} else {
					require.NoError(t, err)
				}
				assert.Equal(t, before, readRawReleaseHistoryForTest(t, releases, leaseUUID),
					"the history bytes must not change")
			})
		}
	}
}

func writeRawReleaseHistoryForTest(t *testing.T, releases *ReleaseStore, leaseUUID string, history []Release) []byte {
	t.Helper()
	encoded, err := encodeReleaseHistory(history)
	require.NoError(t, err)
	require.NoError(t, releases.update(func(tx *bolt.Tx) error {
		return tx.Bucket(releasesBucketName).Put([]byte(leaseUUID), encoded)
	}))
	return slices.Clone(encoded)
}

func readRawReleaseHistoryForTest(t *testing.T, releases *ReleaseStore, leaseUUID string) []byte {
	t.Helper()
	var raw []byte
	require.NoError(t, releases.view(func(tx *bolt.Tx) error {
		raw = slices.Clone(tx.Bucket(releasesBucketName).Get([]byte(leaseUUID)))
		return nil
	}))
	return raw
}
