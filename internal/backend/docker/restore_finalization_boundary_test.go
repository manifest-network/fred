package docker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestFinalizeRestoredLeaseStrictRejectsInvalidDurableAuthority(t *testing.T) {
	const sourceLease = "restore-source"
	const destinationLease = "restore-destination"
	validItems := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}

	tests := []struct {
		name           string
		mutateRecord   func(*shared.RetentionEntry)
		effectiveItems []backend.LeaseItem
		wantError      string
	}{
		{
			name:         "nil manifest",
			mutateRecord: func(rec *shared.RetentionEntry) { rec.StackManifest = nil },
			wantError:    "manifest is required",
		},
		{
			name:           "invalid effective quantity",
			effectiveItems: []backend.LeaseItem{{SKU: "docker-small", Quantity: 0, ServiceName: "app"}},
			wantError:      "validate restored effective items",
		},
		{
			name: "invalid source quantity",
			mutateRecord: func(rec *shared.RetentionEntry) {
				rec.Items = []backend.LeaseItem{{SKU: "docker-small", Quantity: 0, ServiceName: "app"}}
			},
			wantError: "validate restore source items",
		},
		{
			name: "manifest topology mismatch",
			mutateRecord: func(rec *shared.RetentionEntry) {
				rec.Items = []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "db"}}
			},
			effectiveItems: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "db"}},
			wantError:      "validate restored item shape",
		},
		{
			name:         "wrong destination authority",
			mutateRecord: func(rec *shared.RetentionEntry) { rec.NewLeaseUUID = "another-destination" },
			wantError:    "does not own destination",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newBackendForTest(&mockDockerClient{}, nil)
			retentions := attachRetentionStore(t, b)
			releases := attachReleaseStore(t, b)
			rec := shared.RetentionEntry{
				OriginalLeaseUUID: sourceLease,
				NewLeaseUUID:      destinationLease,
				Tenant:            "tenant-a",
				ProviderUUID:      "provider-a",
				Items:             validItems,
				StackManifest:     restoreStackManifest(),
				Status:            shared.RetentionStatusRestoring,
				Generation:        3,
				CreatedAt:         time.Now(),
			}
			claimed := putRestoringRetention(t, retentions, rec)
			rec = *claimed
			if tc.mutateRecord != nil {
				tc.mutateRecord(&rec)
			}
			effectiveItems := tc.effectiveItems
			if effectiveItems == nil {
				effectiveItems = validItems
			}

			err := b.finalizeRestoredLeaseStrict(destinationLease, &rec, effectiveItems)
			require.ErrorContains(t, err, tc.wantError)
			history, listErr := releases.List(destinationLease)
			require.NoError(t, listErr)
			assert.Empty(t, history, "invalid authority must be rejected before an active release is appended")
			stored, getErr := retentions.Get(sourceLease)
			require.NoError(t, getErr)
			require.NotNil(t, stored, "invalid authority must not consume the source finalizer")
		})
	}
}

func TestFinalizeRestoredLeaseStrictRejectsNilRecord(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	err := b.finalizeRestoredLeaseStrict("restore-destination", nil, []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}})
	require.ErrorContains(t, err, "source finalizer is required")
}

func TestFinalizeRestoredLeaseStrictBindsLiveIdentityAndManifest(t *testing.T) {
	const sourceLease = "restore-source"
	const destinationLease = "restore-destination"
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}

	tests := []struct {
		name       string
		mutateLive func(*provision)
		wantError  string
	}{
		{
			name: "tenant mismatch",
			mutateLive: func(live *provision) {
				live.Tenant = "tenant-b"
			},
			wantError: "identity does not match",
		},
		{
			name: "provider mismatch",
			mutateLive: func(live *provision) {
				live.ProviderUUID = "provider-b"
			},
			wantError: "identity does not match",
		},
		{
			name: "manifest mismatch",
			mutateLive: func(live *provision) {
				live.StackManifest = restoreStackManifest()
				live.StackManifest.Services["app"].Image = "docker.io/library/nginx:1.28"
			},
			wantError: "manifest does not match",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newBackendForTest(&mockDockerClient{}, nil)
			retentions := attachRetentionStore(t, b)
			releases := attachReleaseStore(t, b)
			resourceProfiles := testResourceProfiles(t, items)
			rec := shared.RetentionEntry{
				OriginalLeaseUUID: sourceLease,
				NewLeaseUUID:      destinationLease,
				Tenant:            "tenant-a",
				ProviderUUID:      "provider-a",
				Items:             items,
				ResourceProfiles:  resourceProfiles,
				StackManifest:     restoreStackManifest(),
				Status:            shared.RetentionStatusRestoring,
				Generation:        3,
				CreatedAt:         time.Now(),
			}
			claimed := putRestoringRetention(t, retentions, rec)
			rec = *claimed
			live := &provision{ //exhaustruct:enforce
				ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
					LeaseUUID:            destinationLease,
					Tenant:               rec.Tenant,
					ProviderUUID:         rec.ProviderUUID,
					SKU:                  items[0].SKU,
					Status:               backend.ProvisionStatusReady,
					Quantity:             1,
					CreatedAt:            time.Now(),
					FailCount:            0,
					LastError:            "",
					Reason:               "",
					Message:              "",
					CallbackURL:          "",
					LifecycleCallbackURL: "",
					Items:                items,
					ResourceProfiles:     shared.CloneSKUResourceSnapshot(resourceProfiles),
					ContainerIDs:         nil,
					StackManifest:        restoreStackManifest(),
					ServiceContainers:    nil,
				},
				ResourceProfiles:      resourceProfiles,
				VolumeCleanupAttempts: 0,
			}
			tc.mutateLive(live)
			b.provisions[destinationLease] = live

			err := b.finalizeRestoredLeaseStrict(destinationLease, &rec, items)
			require.ErrorContains(t, err, tc.wantError)
			history, listErr := releases.List(destinationLease)
			require.NoError(t, listErr)
			assert.Empty(t, history, "identity mismatch must not append a destination release")
			stored, getErr := retentions.Get(sourceLease)
			require.NoError(t, getErr)
			require.NotNil(t, stored, "identity mismatch must preserve the source finalizer")
		})
	}
}
