package docker

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// TestDoDeprovision_ContainerlessLease_PurgesStrandedReleaseHistory proves ENG-410's
// close-time fix: a lease whose container was already gone at on-chain close has release
// history but no provision entry (recoverState rebuilds b.provisions from live containers
// only), so a deprovision RPC hits the !exists short-circuit before the terminal
// releaseStore.Delete and leaves a stale "active" record that audit-lease-status flags
// until the 90-day RemoveOlderThan TTL. The short-circuit must still purge that history.
func TestDoDeprovision_ContainerlessLease_PurgesStrandedReleaseHistory(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	rel := attachReleaseStore(t, b)
	require.NoError(t, rel.Append("u1", shared.Release{Image: "stack", Status: "active", CreatedAt: time.Now()}))

	// No provision entry for u1 → doDeprovision takes the !exists path.
	require.NoError(t, b.doDeprovision(context.Background(), "u1"))

	releases, err := rel.List("u1")
	require.NoError(t, err)
	assert.Empty(t, releases, "containerless deprovision must purge stranded release history (ENG-410)")
}

// A legacy-to-stack migration deliberately keeps the renamed `-prev` containers
// for a short rollback window. A lease can close during that window. Compose Down
// owns only the replacement stack, so Deprovision must synchronously remove every
// rollback container while the durable migration topology still exists, and must
// retain that topology when any removal is uncertain so the retry remains exact.
func TestDoDeprovision_DuringLegacyMigrationGraceConsumesRollbackCohort(t *testing.T) {
	const (
		leaseUUID    = "11111111-1111-4111-8111-111111111111"
		providerUUID = "22222222-2222-4222-8222-222222222222"
	)
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "app"}}

	for _, tc := range []struct {
		name              string
		removeErr         error
		wantErr           bool
		wantReleaseRemain bool
	}{
		{name: "success purges release after exact cleanup"},
		{
			name:              "uncertain cleanup preserves release for retry",
			removeErr:         errors.New("docker unavailable"),
			wantErr:           true,
			wantReleaseRemain: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var removed []string
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					return []ContainerInfo{
						{
							ContainerID: "prev-id-0", Name: "fred-" + leaseUUID + "-app-0-prev",
							LeaseUUID: leaseUUID, Tenant: "t1", ProviderUUID: providerUUID,
							BackendName: "docker", SKU: "docker-micro", InstanceIndex: 0,
							Image: "busybox", CallbackURL: "https://fred.example/callbacks/provision",
							Status: "exited",
						},
						{
							ContainerID: "prev-id-1", Name: "fred-" + leaseUUID + "-app-1-prev",
							LeaseUUID: leaseUUID, Tenant: "t1", ProviderUUID: providerUUID,
							BackendName: "docker", SKU: "docker-micro", InstanceIndex: 1,
							Image: "busybox", CallbackURL: "https://fred.example/callbacks/provision",
							Status: "exited",
						},
					}, nil
				},
				RemoveContainerFn: func(_ context.Context, id string) error {
					removed = append(removed, id)
					if id == "prev-id-0" {
						return tc.removeErr
					}
					return nil
				},
			}
			b := newBackendForProvisionTest(t, mock, map[string]*provision{
				leaseUUID: {ProvisionState: leasesm.ProvisionState{
					LeaseUUID: leaseUUID, Tenant: "t1", ProviderUUID: providerUUID,
					Status: backend.ProvisionStatusReady, Items: items, Quantity: 2,
				}},
			})
			resourceProfiles := testResourceProfiles(t, items)
			b.provisions[leaseUUID].ResourceProfiles = resourceProfiles
			releases := attachReleaseStore(t, b)
			require.NoError(t, releases.Append(leaseUUID, shared.Release{
				Manifest:         []byte(`{"image":"busybox"}`),
				Items:            items,
				ResourceProfiles: resourceProfiles,
				Status:           "active",
				CreatedAt:        time.Now(),
				LegacyMigration:  true,
			}))
			// Model a successful restart/update inside the grace window. The
			// migration row is now superseded, but it is still the sole exact
			// authority for the rollback containers.
			require.NoError(t, releases.Append(leaseUUID, shared.Release{
				Manifest:         []byte(`{"image":"busybox:updated"}`),
				Items:            items,
				ResourceProfiles: resourceProfiles,
				Status:           "deploying",
				CreatedAt:        time.Now(),
			}))
			require.NoError(t, releases.ActivateLatest(leaseUUID))

			err := b.doDeprovision(context.Background(), leaseUUID)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, []string{
				"prev-id-0",
				"prev-id-1",
			}, removed, "cleanup must remove the re-attested immutable rollback IDs")

			got, listErr := releases.List(leaseUUID)
			require.NoError(t, listErr)
			if tc.wantReleaseRemain {
				require.Len(t, got, 2, "failed cleanup must retain its exact retry authority")
				assert.True(t, got[0].LegacyMigration)
				assert.Equal(t, items, got[0].Items)
				assert.Equal(t, "active", got[1].Status)
				return
			}
			assert.Empty(t, got, "release may be purged only after every rollback container is gone")
			_, stillTracked := b.provisions[leaseUUID]
			assert.False(t, stillTracked)
		})
	}
}

func TestDoDeprovision_ReleaseDeleteFailureRemainsRetryable(t *testing.T) {
	const leaseUUID = "u1"
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}}
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: leaseUUID, Tenant: "tenant-1", ProviderUUID: "provider-1",
			Status: backend.ProvisionStatusReady, Items: items, Quantity: 1,
		}},
	})
	b.provisions[leaseUUID].ResourceProfiles = testResourceProfiles(t, items)

	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	b.releaseStore = releases
	require.NoError(t, releases.Append(leaseUUID, shared.Release{
		Manifest: []byte(`{"image":"busybox"}`), Items: items,
		ResourceProfiles: testResourceProfiles(t, items),
		Status:           "active", CreatedAt: time.Now(),
	}))

	closeStoreOnDestroy := true
	b.volumes = &mockVolumeManager{DestroyFn: func(context.Context, string) error {
		if closeStoreOnDestroy {
			closeStoreOnDestroy = false
			return releases.Close()
		}
		return nil
	}}

	err = b.doDeprovision(context.Background(), leaseUUID)
	require.ErrorContains(t, err, "retire release history")
	prov, exists := b.provisions[leaseUUID]
	require.True(t, exists, "failed release retirement must preserve an in-memory retry owner")
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)

	reopened, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	b.releaseStore = reopened
	stored, err := reopened.List(leaseUUID)
	require.NoError(t, err)
	require.Len(t, stored, 1, "the failed transaction must leave exact release authority intact")

	require.NoError(t, b.doDeprovision(context.Background(), leaseUUID))
	_, exists = b.provisions[leaseUUID]
	assert.False(t, exists)
	stored, err = reopened.List(leaseUUID)
	require.NoError(t, err)
	assert.Empty(t, stored)
}

// TestDeprovisionGiveUp_WritesReapingTombstone verifies a give-up (max volume
// cleanup attempts) writes a reaping tombstone so the footprint keeps counting + the
// sweep auto-retries, instead of a silent uncounted leak. ENG-376 site 3.
//
// The assertion is on Items, not on volume names: the record's job is to say how big the
// abandoned footprint is (computeReapingDiskMB sums leaseDiskMB(Items) into the admission
// projection), not which volumes to destroy — the finalizer derives that from disk on
// every pass (ENG-676). Asserting the projection rather than the name list is also
// strictly closer to the property ENG-376 exists to protect.
func TestDeprovisionGiveUp_WritesReapingTombstone(t *testing.T) {
	const (
		leaseUUID    = "11111111-1111-4111-8111-111111111111"
		providerUUID = "22222222-2222-4222-8222-222222222222"
	)
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: leaseUUID, Tenant: "t1", ProviderUUID: providerUUID,
			Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"app": {Image: "nginx:latest"},
			}},
		}, VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1}, // next failure → give up
	})
	withMicroSKU(b, 1024)
	rs := attachRetentionStore(t, b) // RetainOnClose stays false → non-retain destroy arm
	claim, found, err := b.acquireCloseIntent(
		context.Background(),
		leaseUUID,
		true,
		"t1",
		providerUUID,
		b.provisions[leaseUUID].Items,
		b.provisions[leaseUUID].StackManifest,
		"",
		"",
	)
	require.NoError(t, err)
	require.True(t, found)
	for range maxVolumeCleanupAttempts - 1 {
		claim, err = b.callbackStore.IncrementCloseCleanupAttempts(claim)
		require.NoError(t, err)
	}

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{canonicalVolumeName(leaseUUID, "app", 0)}, nil
		},
		DestroyFn: func(_ context.Context, _ string) error { return errors.New("EBUSY") },
	}

	// The give-up branch returns nil to the actor (it abandons to manual cleanup and
	// fires a failed callback), so do not assert on Deprovision's return value here —
	// the load-bearing assertions are the tombstone + the leak counter below.
	_ = b.Deprovision(context.Background(), leaseUUID)

	// Poll for the reaping tombstone.
	var got *shared.RetentionEntry
	require.Eventually(t, func() bool {
		g, e := rs.Get(leaseUUID)
		if e != nil || g == nil {
			return false
		}
		got = g
		return true
	}, 5*time.Second, 20*time.Millisecond, "reaping tombstone must be written at give-up")

	assert.Equal(t, shared.RetentionStatusReaping, got.Status)
	assert.Equal(t,
		[]backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}}, got.Items,
		"Items is the accounting fact the admission projection reads; it must be the FULL lease set")
	mb, count, err := b.computeReapingDiskMB()
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, int64(1024), mb,
		"the abandoned footprint must still be counted against admission after the give-up")
	assert.Greater(t, testutil.ToFloat64(retentionLeakedTotal), leakBefore)
}

// seedUnenumerableRecord makes every retention ENUMERATION fail while single-key reads and
// writes keep working — the shape a partially-degraded store actually has, and the one the
// give-up path must survive.
//
// It exploits a real asymmetry in the store rather than inventing one. RetentionStore.filter
// (which backs List/ListExpired/ListReaping/ListRestoring, and therefore the ownership
// table) unmarshals the FULL RetentionEntry for every record, so one bad record fails all
// four. scanIndex — the fail-closed integrity check NewRetentionStore runs at open —
// unmarshals only three string fields, so a record with a well-formed uuid/tenant/status and
// a type-mismatched Items passes open and breaks every enumeration afterwards.
//
// Written through a direct bolt handle before the store is constructed, mirroring
// backdateReleaseRecords (integration_releases_reaper_test.go); the store must not be open
// at the same time (file lock).
func seedUnenumerableRecord(t *testing.T, dbPath string) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bkt, berr := tx.CreateBucketIfNotExists([]byte("retention"))
		if berr != nil {
			return berr
		}
		// "items" must be an array; a string parses under scanIndex's narrow struct (which
		// never looks at it) and fails under filter's full one.
		return bkt.Put([]byte("corrupt-lease"),
			[]byte(`{"original_lease_uuid":"corrupt-lease","tenant":"t1","status":"active","items":"not-an-array"}`))
	}))
	require.NoError(t, db.Close())
}

// TestDeprovisionGiveUp_UnenumerableStore_StillRecordsTheFootprint is ENG-676.
//
// When the retention store cannot be enumerated the ownership table cannot be resolved, so
// the close destroys nothing and retries until it gives up. The give-up then releases the
// lease's pool allocation and deletes its provision — correct, since nothing can retry
// after that — which leaves the reaping record as the ONLY thing still counting the bytes
// on disk. It used to compute that record's volume names through the same unreadable table
// and, failing, write no record at all: the footprint ended up counted by nothing (no pool
// key, no active record, no reaping record) and admission over-committed against real disk
// permanently.
//
// The record no longer carries a destroy plan, so there is nothing left for a degraded
// store to prevent it computing. This fails on any change that reintroduces a precondition
// between the give-up and the record.
func TestDeprovisionGiveUp_UnenumerableStore_StillRecordsTheFootprint(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retention.db")
	seedUnenumerableRecord(t, dbPath)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}, VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1}, // next failure → give up
	})
	withMicroSKU(b, 1024)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: dbPath})
	require.NoError(t, err, "the store still OPENS — that is what makes this silent in production")
	t.Cleanup(func() { _ = rs.Close() })
	b.retentionStore = rs
	_, listErr := rs.List()
	require.Error(t, listErr, "precondition: enumeration fails, so ownership is unprovable")

	// The volumes are on disk and enumerable; only the store is degraded.
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{"fred-u1-app-0"}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("must not destroy %q while ownership is unprovable", id)
			return nil
		},
	}
	require.NoError(t, b.pool.TryAllocate("u1-app-0", "docker-micro", "t1"))

	require.NoError(t, b.doDeprovision(context.Background(), "u1"))

	got, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, got, "ENG-676: the give-up must record the abandoned footprint even when ownership is unprovable")
	assert.Equal(t, shared.RetentionStatusReaping, got.Status)
	assert.Equal(t,
		[]backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}}, got.Items,
		"Items is what keeps the bytes counted; without it the footprint is counted by nobody")
	assert.Empty(t, got.RetainedVolumeNames,
		"the record must NOT carry a destroy plan derived from an ownership table it could not read")

	// CHARACTERIZATION of the residual, pinned deliberately rather than left to prose.
	//
	// The record is durable, but the projection cannot absorb it yet: every refresh scans
	// the store, and the store is exactly what is broken. So the give-up releases the live
	// reservation while retained is still frozen at its pre-write value, and for the
	// duration of the degradation the abandoned bytes are counted by neither pool term.
	//
	// This is NOT specific to the give-up — every live→retained hand-off releases the live
	// reservation without checking that the refresh succeeded, including the ordinary
	// retain-path close, which predates ENG-676. It is tracked separately; what ENG-676
	// changes is that the FACT survives, so the projection self-corrects the moment the
	// store is readable (asserted below) instead of there being nothing to recount from.
	assert.Equal(t, int64(0), b.pool.Stats().AllocatedDiskMB,
		"the give-up is terminal: the live reservation is released and the provision deleted")
	assert.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB,
		"KNOWN WINDOW: the projection cannot see the new record while the store is unenumerable")
}

// TestDeprovisionGiveUp_UnenumerableStore_AccountingRecoversOnRepair is the other half of
// the invariant, and the one that says why writing the record is worth anything at all: the
// footprint is uncounted only for as long as the store is broken. Repair it and the very
// next refresh picks the record up, with no operator action and nothing to reconstruct.
//
// Before ENG-676 there was no record to recover FROM, so the same repair changed nothing
// and the bytes stayed uncounted until someone noticed by hand. This test fails if the
// give-up ever stops writing the record.
func TestDeprovisionGiveUp_UnenumerableStore_AccountingRecoversOnRepair(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retention.db")
	seedUnenumerableRecord(t, dbPath)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}, VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1},
	})
	withMicroSKU(b, 1024)
	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	b.retentionStore = rs
	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{"fred-u1-app-0"}, nil },
		DestroyFn: func(_ context.Context, _ string) error { return nil },
	}
	require.NoError(t, b.pool.TryAllocate("u1-app-0", "docker-micro", "t1"))
	require.NoError(t, b.doDeprovision(context.Background(), "u1"))
	require.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB, "precondition: uncounted during the window")

	// Repair: drop the unenumerable record and reopen. This is the operator fixing the
	// store, not fred doing anything clever.
	require.NoError(t, rs.Close())
	db, derr := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	require.NoError(t, derr)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("retention")).Delete([]byte("corrupt-lease"))
	}))
	require.NoError(t, db.Close())

	repaired, rerr := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: dbPath})
	require.NoError(t, rerr)
	t.Cleanup(func() { _ = repaired.Close() })
	b.retentionStore = repaired

	b.refreshRetentionAccounting()

	assert.Positive(t, b.pool.Stats().RetainedDiskMB,
		"the durable record is what lets the projection self-correct once the store is readable")
}

// TestReapingFootprint_CoversBothNamespaces preserves the hazard the old
// "record BOTH namespaces" fallback existed for, at the layer that now owns it.
//
// A retain-path give-up can strand a lease part-way through its renames, leaving some
// volumes canonical and some already quarantined under fred-retained-*. The tombstone used
// to record both spellings of every name because a stored plan could not know which one
// was really on disk — and getting that wrong meant the sweep "succeeded" against a
// non-existent canonical name, dropped the tombstone, and left the fred-retained-* volume
// on disk and untracked (ENG-376).
//
// Derivation replaces the guesswork with a look, so the property to pin is that the look
// covers both namespaces — and, unlike the old fallback, that it reports volumes belonging
// to OTHER leases nowhere in the result.
func TestReapingFootprint_CoversBothNamespaces(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) {
		return []string{
			"fred-u1-app-0",          // still canonical: the rename never happened
			"fred-retained-u1-app-1", // already quarantined by a prior attempt
			"fred-u2-app-0",          // another lease entirely
			"fred-retained-u2-app-0", // another lease's retained data
		}, nil
	}}

	got, err := b.newManagedVolumeIndex().footprint("u1")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"fred-u1-app-0", "fred-retained-u1-app-1"}, got,
		"both namespaces of THIS lease, and nothing belonging to another")
}

// TestReapingFootprint_ListError_IsNotAnEmptyFootprint is the error branch, and it is
// load-bearing: the caller DELETES the record when the footprint is empty, so conflating
// "looked and found nothing" with "could not look" would drop the record that is both the
// retry vehicle and the accounting for bytes still on disk.
func TestReapingFootprint_ListError_IsNotAnEmptyFootprint(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) { return nil, errors.New("statfs EIO") }}

	got, err := b.newManagedVolumeIndex().footprint("u1")
	require.Error(t, err)
	assert.Nil(t, got)
}

// TestListVolumeIDs_AbsentRootIsAnError is where the absent-vs-empty distinction is now
// GUARANTEED rather than reconstructed, and it is the reason the reaping finalizer needs no
// root probe at all.
//
// listVolumeIDs used to map a missing directory to (nil, nil), collapsing "this node holds
// no volumes" and "the volume root is gone" into one value. Any caller wanting the
// difference back had to stat separately — and a separate stat is a separate point in time,
// so an unmount landing between the probe and the read produced a confident, empty,
// error-free answer. The reaping finalizer acts on that answer by DELETING the record that
// accounts for the bytes, so the ambiguity was one unmount away from silent
// data-accounting loss. Keeping the question inside the single syscall that can answer it
// is what makes that race impossible rather than merely unlikely: there is no second
// observation to disagree with the first.
//
// A configured root cannot legitimately be absent at runtime — newVolumeManager statfs's it
// at construction — so ENOENT here always means "it disappeared underneath us".
func TestListVolumeIDs_AbsentRootIsAnError(t *testing.T) {
	ids, err := listVolumeIDs(filepath.Join(t.TempDir(), "not-mounted"))
	require.Error(t, err, "an absent volume root is uncertainty, never an empty node")
	assert.Nil(t, ids)

	// ...and a present-but-empty root still reports emptiness, so the guarantee is a real
	// distinction rather than a blanket refusal.
	empty, err := listVolumeIDs(t.TempDir())
	require.NoError(t, err)
	assert.Empty(t, empty)
}

// TestReapingFootprint_AbsentVolumeRoot_IsNotAnEmptyFootprint pins that the finalizer's
// derivation inherits that guarantee rather than re-deriving it.
func TestReapingFootprint_AbsentVolumeRoot_IsNotAnEmptyFootprint(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.VolumeDataPath = filepath.Join(t.TempDir(), "not-mounted")
	b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) { return listVolumeIDs(b.cfg.VolumeDataPath) }}

	got, err := b.newManagedVolumeIndex().footprint("u1")
	require.Error(t, err, "an absent volume root must be uncertainty, not an empty footprint")
	assert.Nil(t, got)
}

// TestDestroyReapingVolumes_AbsentVolumeRoot_KeepsTheRecord is the consequence of the
// above at the layer that acts on it: the record survives, and nothing is destroyed.
func TestDestroyReapingVolumes_AbsentVolumeRoot_KeepsTheRecord(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.VolumeDataPath = filepath.Join(t.TempDir(), "not-mounted")
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return listVolumeIDs(b.cfg.VolumeDataPath) },
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("must not destroy %q while the volume root is unverifiable", id)
			return nil
		},
	}
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID: "u1", Tenant: "t1", Status: shared.RetentionStatusReaping,
		Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		CreatedAt: time.Now(),
	}))
	skipBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable))

	assert.False(t, b.destroyReapingVolumes(context.Background(), b.newManagedVolumeIndex(), "u1"),
		"an unverifiable root means the record cannot be dropped")

	got, err := rs.Get("u1")
	require.NoError(t, err)
	assert.NotNil(t, got, "the record — and the footprint it accounts for — must survive")
	assert.Equal(t, skipBefore+1,
		testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable)))
}

// TestDeprovision_PartialFailure_AuthorsCleanupFailed drives the partial-failure
// branch (a stuck container that RemoveContainer cannot remove) and verifies the
// provision is left with the curated ReasonCleanupFailed / MsgCleanupFailed pair,
// while the verbose detail is retained operator-side in LastError (ENG-508).
func TestDeprovision_PartialFailure_AuthorsCleanupFailed(t *testing.T) {
	const lease = "u1"
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error {
			return errors.New("container removal blocked: device or resource busy")
		},
		// The fallback re-discovers by label (ENG-647); an empty listing keeps this
		// test's subject the RECORDED container, exactly as before.
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
			ContainerIDs: []string{"c-stuck"},
		}},
	})
	// Force compose.Down to fail so teardown falls back to per-container removal,
	// where the mocked RemoveContainer error populates errs → partial-failure branch.
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error {
			return errors.New("compose project metadata missing")
		},
	}

	err := b.doDeprovision(context.Background(), lease)
	require.Error(t, err, "partial container-removal failure must surface an error")

	prov, ok := b.provisions[lease]
	require.True(t, ok, "partial failure must keep the provision visible for retry")
	assert.Equal(t, backend.ReasonCleanupFailed, prov.Reason,
		"partial deprovision failure must author ReasonCleanupFailed")
	assert.Equal(t, backend.MsgCleanupFailed, prov.Message,
		"Message must be the curated MsgCleanupFailed const (tenant-facing)")
	assert.Contains(t, prov.LastError, "deprovision partially failed",
		"verbose detail must be retained operator-side in LastError")
	assert.Contains(t, prov.LastError, "container removal blocked",
		"LastError must retain the underlying (operator-only) failure text")
	assert.NotEqual(t, prov.Message, prov.LastError,
		"tenant-facing Message must be the curated const, distinct from the verbose LastError")
}

// TestDoDeprovision_ComposeDownFails_RemovesEveryRecordedContainer pins the
// compensating behaviour that keeps ENG-372 closed when compose Down does not
// finish what it started.
//
// docker/compose v5 changed teardown to run its per-container removals on the
// errgroup's DERIVED context (`eg, ctx := errgroup.WithContext(ctx)` in
// pkg/compose/down.go) where v2 discarded it (`eg, _ :=`). Under v2, one
// container failing to be removed left its siblings to finish; under v5 the
// first failure cancels the group, so siblings can be aborted part-way through
// ContainerStop/ContainerRemove — and a container that is never removed never
// has its anonymous volumes reaped. That is exactly the leak ENG-372 exists to
// prevent, and it is invisible: Down returns an error, teardown continues, and
// the volume simply accumulates.
//
// fred's compensation is the per-container fallback below, which is only
// sufficient if it covers EVERY container in the record rather than stopping at
// the first one, or at the one compose happened to fail on. This test pins that
// coverage. It is deliberately a unit test: the cancellation lives inside
// compose, and inducing a deterministic mid-flight removal failure against a
// real daemon would be racy. What fred owns — and what actually keeps the leak
// closed — is the fan-out asserted here. The reaping itself (RemoveContainer
// passing RemoveVolumes:true) is pinned live by
// TestIntegration_Docker_RemoveContainer_RemovesAnonymousVolumes; together the
// two cover the whole path.
//
// TestDeprovision_PartialFailure_AuthorsCleanupFailed covers the arm where that
// fallback itself fails; this one covers the arm where it must succeed.
func TestDoDeprovision_ComposeDownFails_RemovesEveryRecordedContainer(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-00000000000d"
	recorded := []string{"c-web-0", "c-web-1", "c-sidecar-0"}

	var removed []string
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, id string) error {
			removed = append(removed, id)
			return nil
		},
		// Daemon reports nothing (labels lost / already delisted), so the recorded list
		// is the only source left. That is the case this test exists to pin; the
		// complementary case — an EMPTY record and a daemon that does report the
		// containers — is TestDoDeprovision_ComposeDownFails_RemovesDiscoveredContainersWhenRecordIsEmpty.
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "t", Status: backend.ProvisionStatusReady,
			Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 3, ServiceName: "app"}},
			ContainerIDs: recorded,
		}},
	})

	// Simulate v5 aborting teardown part-way: Down reports failure, and the
	// containers it did not get to are still present for the fallback to reap.
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error {
			return errors.New("compose down canceled after first removal failed")
		},
	}

	require.NoError(t, b.doDeprovision(context.Background(), lease),
		"a failed Down whose fallback removes every container is a successful deprovision")

	assert.ElementsMatch(t, recorded, removed,
		"every recorded container must be removed individually when compose Down fails; "+
			"a container skipped here keeps its anonymous volumes forever (ENG-372)")
}

// TestDoDeprovision_Success_ReleasesPoolReservation locks the Release-pairing
// invariant the pool-authoritative recoverState rule depends on (ENG-567): a
// successful (non-retain) deprovision must release the lease's pool reservation
// and remove it from b.provisions, so recoverState never preserves a phantom key
// for an untracked lease.
func TestDoDeprovision_Success_ReleasesPoolReservation(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-00000000000b"
	existing := map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "t", Status: backend.ProvisionStatusReady,
			Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			ContainerIDs: []string{"c1"},
		}},
	}
	b := newBackendForProvisionTest(t, &mockDockerClient{}, existing)
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-small", "t"))
	require.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=1024 MB")

	require.NoError(t, b.doDeprovision(context.Background(), lease))

	got := b.pool.Stats()
	assert.Equal(t, int64(0), got.AllocatedDiskMB, "deprovision must release the pool reservation")
	assert.Equal(t, 0, got.AllocationCount)
	b.provisionsMu.RLock()
	_, stillTracked := b.provisions[lease]
	b.provisionsMu.RUnlock()
	assert.False(t, stillTracked, "deprovisioned lease must be removed from b.provisions")
}

// TestDoDeprovision_GiveUp_ReleasesPoolReservation locks the give-up arm of the
// Release-pairing invariant: when volume cleanup fails maxVolumeCleanupAttempts
// times, doDeprovision gives up — it must releaseLive() and Delete the lease,
// so no untracked lease is left holding a pool key.
func TestDoDeprovision_GiveUp_ReleasesPoolReservation(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-00000000000c"
	existing := map[string]*provision{
		lease: {
			ProvisionState: leasesm.ProvisionState{
				LeaseUUID: lease, Tenant: "t", Status: backend.ProvisionStatusFailed,
				Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			},
			VolumeCleanupAttempts: 2, // next attempt (3) == maxVolumeCleanupAttempts → give up
		},
	}
	b := newBackendForProvisionTest(t, &mockDockerClient{}, existing)
	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, _ string) error {
		return errors.New("volume stuck")
	}}
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-small", "t"))
	require.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=1024 MB")

	// Give-up returns nil (provision deleted, manual cleanup logged).
	require.NoError(t, b.doDeprovision(context.Background(), lease))

	got := b.pool.Stats()
	assert.Equal(t, int64(0), got.AllocatedDiskMB, "give-up must release the reservation before deleting the lease")
	assert.Equal(t, 0, got.AllocationCount)
	b.provisionsMu.RLock()
	_, stillTracked := b.provisions[lease]
	b.provisionsMu.RUnlock()
	assert.False(t, stillTracked, "given-up lease must be removed from b.provisions")
}

// TestDoDeprovision_ComposeDownFails_RemovesDiscoveredContainersWhenRecordIsEmpty is
// the complement of TestDoDeprovision_ComposeDownFails_RemovesEveryRecordedContainer,
// and the reason the fallback had to stop trusting the record (ENG-647).
//
// A lease that never reached Ready carries NO ContainerIDs — Restore reserves the
// entry empty and only the success paths fill it in, and recoverState's status-keyed
// merge preserves an existing Failed entry by pointer, so it is never backfilled while
// the process runs. Walking that list removes nothing precisely when a container
// leaked. Discovery by fred label is what closes it.
func TestDoDeprovision_ComposeDownFails_RemovesDiscoveredContainersWhenRecordIsEmpty(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-00000000000e"

	var removed []string
	mock := &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{ContainerID: "c-web-0", LeaseUUID: lease, SKU: "docker-small"},
				{ContainerID: "c-web-1", LeaseUUID: lease, SKU: "docker-small"},
				{ContainerID: "c-other", LeaseUUID: "someone-else", SKU: "docker-small"},
			}, nil
		},
		RemoveContainerFn: func(_ context.Context, id string) error {
			removed = append(removed, id)
			return nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "t", Status: backend.ProvisionStatusFailed,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
			// ContainerIDs deliberately absent: the failed-restore shape.
		}},
	})
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error {
			return errors.New("compose down canceled after first removal failed")
		},
	}

	require.NoError(t, b.doDeprovision(context.Background(), lease),
		"a failed Down whose fallback removes every container is a successful deprovision")

	assert.ElementsMatch(t, []string{"c-web-0", "c-web-1"}, removed,
		"the lease's containers must be found by label when the record names none; "+
			"anything missed here keeps its anonymous volumes forever (ENG-372)")
}

// ---------------------------------------------------------------------------
// ENG-647 retryability guard: a close must not eat an in-flight restore's data.
//
// While a record is restoring, the ORIGINAL lease's retained data lives under the NEW
// lease's canonical names, so closing the new lease sees volumes that look like its
// own. Destroying them, or re-retaining them under the closing lease, permanently
// kills the original lease's restore.
// ---------------------------------------------------------------------------

// seedRestoringInto puts a restoring record whose data has been adopted into
// newLease's namespace, and returns the canonical name that adoption produced.
//
// The record's SKU is docker-micro so that the accounting tests below — which pin
// their backend to a single known profile via withMicroSKU — can resolve its
// footprint. A SKU the profile map does not carry silently sizes the record at 0 MB,
// which would make a "the bytes stay counted" assertion pass vacuously.
func seedRestoringInto(
	t *testing.T,
	rs *shared.RetentionStore,
	orig, newLease string,
	destinationItems []backend.LeaseItem,
) string {
	t.Helper()
	retained := retainedName(canonicalVolumeName(orig, "app", 0))
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   orig,
		Tenant:              "tenant-a",
		ProviderUUID:        "provider-a",
		Status:              shared.RetentionStatusActive,
		Items:               slices.Clone(destinationItems),
		ResourceProfiles:    testResourceProfiles(t, destinationItems),
		StackManifest:       restoreStackManifest(),
		RetainedVolumeNames: []string{retained},
		CreatedAt:           time.Now(),
	}))
	operationID, callbackURL, lifecycleURL := restoreDestinationAuthority(t)
	_, err := rs.ClaimForRestoreWithAuthority(
		orig,
		newLease,
		0,
		destinationItems,
		testResourceProfiles(t, destinationItems),
		operationID,
		callbackURL,
		lifecycleURL,
	)
	require.NoError(t, err)
	return retainedToNewCanonical(retained, orig, newLease)
}

func TestDoDeprovision_BlocksUncommittedRestoreBeforeRetainMutation(t *testing.T) {
	const lease = "u2"
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}}
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 2,
			Items: items,
		}},
	})
	b.cfg.RetainOnClose = true
	rs := attachRetentionStore(t, b)
	attachReleaseStore(t, b)
	claimedVol := seedRestoringInto(t, rs, "u1", lease, items)

	var renames [][2]string
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			// app-0 is u1's data adopted into u2's namespace; app-1 is genuinely u2's.
			return []string{claimedVol, canonicalVolumeName(lease, "app", 1)}, nil
		},
		RenameVolumeFn: func(old, new string) error {
			renames = append(renames, [2]string{old, new})
			return nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("retain path must not destroy %q", id)
			return nil
		},
	}

	err := b.ensureCommittedRestoreDestinationForClose(lease)
	require.ErrorContains(t, err, "has not durably committed ownership")
	assert.Empty(t, renames, "an uncommitted restore must be rejected before any volume mutation")

	orig, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, orig, "the original record must be untouched")
	assert.Equal(t, shared.RetentionStatusRestoring, orig.Status)
	assert.Equal(t, []string{retainedName(canonicalVolumeName("u1", "app", 0))}, orig.RetainedVolumeNames,
		"and must still name the data reconcileRestoring will re-quarantine")
}

// The non-retain path has the same pre-mutation ownership gate.
func TestDoDeprovision_BlocksUncommittedRestoreBeforeDestroyMutation(t *testing.T) {
	const lease = "u2"
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}}
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 2,
			Items: items,
		}},
	})
	// RetainOnClose stays false — the pure destroy path.
	rs := attachRetentionStore(t, b)
	attachReleaseStore(t, b)
	claimedVol := seedRestoringInto(t, rs, "u1", lease, items)

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
	}

	err := b.ensureCommittedRestoreDestinationForClose(lease)
	require.ErrorContains(t, err, "has not durably committed ownership")
	assert.Empty(t, destroyed, "an uncommitted restore must be rejected before any destroy")
	assert.NotContains(t, destroyed, claimedVol)
}

// TestDoDeprovision_RestoringClaimLookupFails_DoesNotDestroy pins the fail-safe: if the
// retention store cannot be read we cannot tell our volumes from an in-flight
// restore's, so we destroy neither and let the lease retry.
func TestDoDeprovision_RestoringClaimLookupFails_DoesNotDestroy(t *testing.T) {
	const lease = "u2"
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusFailed, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Close()) // every retention read now fails

	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("must not destroy %q when the claim lookup failed — the volume may belong to an in-flight restore", id)
			return nil
		},
	}

	err := b.doDeprovision(context.Background(), lease)
	require.Error(t, err, "an unreadable retention store must keep the lease Failed for retry, not report success")
}

// ---------------------------------------------------------------------------
// ENG-647 (PR #217 review): leaving a claimed volume on disk must not also hand
// its capacity back.
//
// A restoring record is counted by NEITHER projection — computeRetainedDiskMB skips
// every non-active status, and the admission pool is active+reaping — so while a
// restore is in flight the ONLY thing counting those bytes is the new lease's live
// pool allocation. The guard above stops the close from destroying the volume; if the
// close still released that allocation, the footprint would be counted by nobody and
// admission could over-commit against bytes that are physically present.
// ---------------------------------------------------------------------------

// claimedCloseBackend builds a lease whose canonical volume is claimed by an in-flight
// restore of another lease, with the restore's live allocation held in the pool.
func claimedCloseBackend(t *testing.T, retainOnClose bool) (*Backend, *shared.RetentionStore, string) {
	t.Helper()
	const lease = "u2"
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusFailed, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	withMicroSKU(b, 512)
	b.cfg.RetainOnClose = retainOnClose
	rs := attachRetentionStore(t, b)
	attachReleaseStore(t, b)
	items := b.provisions[lease].Items
	claimedVol := seedRestoringInto(t, rs, "u1", lease, items)

	// The restore's live allocation — the only thing counting the adopted bytes.
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-micro", "tenant-a"))
	b.refreshRetentionAccounting()
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "precondition: the restore's bytes are counted live")
	require.Zero(t, b.pool.Stats().RetainedDiskMB, "precondition: a restoring record counts as retained NOWHERE")

	return b, rs, claimedVol
}

func TestDoDeprovision_NonRetainPath_KeepsAllocationReservedForClaimedVolume(t *testing.T) {
	b, _, claimedVol := claimedCloseBackend(t, false)

	var destroyed []string
	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, id string) error {
		destroyed = append(destroyed, id)
		return nil
	}}

	require.ErrorContains(t, b.ensureCommittedRestoreDestinationForClose("u2"), "has not durably committed ownership")

	assert.NotContains(t, destroyed, claimedVol, "the claimed volume must survive the close")
	assert.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB,
		"the bytes are still on disk, so they must still be reserved — releasing here counts a real "+
			"footprint nowhere and lets admission over-commit against it (ENG-647)")
	assert.Equal(t, 1, b.pool.Stats().AllocationCount)
}

func TestDoDeprovision_RetainPath_KeepsAllocationReservedForClaimedVolume(t *testing.T) {
	b, _, claimedVol := claimedCloseBackend(t, true)

	b.volumes = &mockVolumeManager{
		ListFn:         func() ([]string, error) { return []string{claimedVol}, nil },
		RenameVolumeFn: func(_, _ string) error { return nil },
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("retain path must not destroy %q", id)
			return nil
		},
	}

	require.ErrorContains(t, b.ensureCommittedRestoreDestinationForClose("u2"), "has not durably committed ownership")

	assert.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB,
		"same on the retain arm: the claimed volume is excluded from THIS lease's retained record, "+
			"so nothing else counts it (ENG-647)")
}

// An exact active Release is the durable proof that restore ownership transferred.
// When the source finalizer still lingers, public close admission must first publish
// its complete close journal and then hand that finalizer off before the actor mutates
// containers or volumes.
func TestDeprovision_ReadyRestoreDestinationFinalizesSourceBeforeClose(t *testing.T) {
	const orig = "0192f1a0-1111-4abc-8def-000000000501"
	const destination = "0192f1a0-2222-4abc-8def-000000000502"
	const providerUUID = "22222222-2222-4222-8222-222222222222"
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	stack := restoreStackManifest()
	profiles := testResourceProfiles(t, items)
	operationID, callbackURL, lifecycleURL := restoreDestinationAuthority(t)
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		destination: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: destination, Tenant: "tenant-a", ProviderUUID: providerUUID,
			Status: backend.ProvisionStatusReady, Quantity: 1, Items: items, StackManifest: stack,
			CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleURL,
		}},
	})
	bindTestStorageIdentity(t, b, mock)
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	b.callbackStore = callbacks
	b.operationIntents = callbacks
	b.provisions[destination].ResourceProfiles = profiles
	b.cfg.RetainOnClose = true
	rs := attachRetentionStore(t, b)
	releases := attachReleaseStore(t, b)
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	runtimeAuthority, err := shared.NewReleaseRuntimeAuthority(
		operationID, "tenant-a", providerUUID, callbackURL, lifecycleURL,
	)
	require.NoError(t, err)
	require.NoError(t, releases.Append(destination, shared.Release{
		Manifest: manifestBytes, Image: "stack", OperationID: operationID,
		Items: items, ResourceProfiles: profiles, RuntimeAuthority: &runtimeAuthority,
		Status: "active", CreatedAt: time.Now(),
	}))
	retainedSource := retainedName(canonicalVolumeName(orig, "app", 0))
	canonicalDestination := canonicalVolumeName(destination, "app", 0)
	restored := shared.RetentionEntry{
		OriginalLeaseUUID: orig, NewLeaseUUID: destination,
		Tenant: "tenant-a", ProviderUUID: providerUUID,
		Status: shared.RetentionStatusRestoring, Generation: 3,
		Items: items, StackManifest: stack,
		DestinationItems: items, DestinationResourceProfiles: profiles,
		DestinationOperationID: operationID,
		DestinationCallbackURL: callbackURL, DestinationLifecycleCallbackURL: lifecycleURL,
		RetainedVolumeNames: []string{retainedSource}, CreatedAt: time.Now(),
	}
	putRestoringRetention(t, rs, restored)

	var renames [][2]string
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{canonicalDestination}, nil },
		RenameVolumeFn: func(oldName, newName string) error {
			renames = append(renames, [2]string{oldName, newName})
			return nil
		},
		DestroyFn: func(_ context.Context, name string) error {
			t.Fatalf("destination retain-on-close policy must not destroy %q", name)
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), destination))

	source, err := rs.Get(orig)
	require.NoError(t, err)
	assert.Nil(t, source, "successful restore ownership must not resurrect under the source lease")
	destinationRecord, err := rs.Get(destination)
	require.NoError(t, err)
	require.NotNil(t, destinationRecord, "the destination's retain-on-close policy must own the bytes")
	assert.Equal(t, shared.RetentionStatusActive, destinationRecord.Status)
	assert.Equal(t, "tenant-a", destinationRecord.Tenant)
	assert.Equal(t, [][2]string{{canonicalDestination, retainedName(canonicalDestination)}}, renames)
}

// If the exact Release proof cannot be read, close must fail before the actor status,
// close journal, containers, or volumes are touched.
func TestDeprovision_ReadyRestoreDestinationFailsClosedWhenCommitProofUnreadable(t *testing.T) {
	const orig = "0192f1a0-1111-4abc-8def-000000000511"
	const destination = "0192f1a0-2222-4abc-8def-000000000512"
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	stack := restoreStackManifest()
	profiles := testResourceProfiles(t, items)
	operationID, callbackURL, lifecycleURL := restoreDestinationAuthority(t)
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		destination: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: destination, Tenant: "tenant-a", ProviderUUID: "provider-a",
			Status: backend.ProvisionStatusReady, Quantity: 1, Items: items, StackManifest: stack,
			CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleURL,
		}},
	})
	b.provisions[destination].ResourceProfiles = profiles
	rs := attachRetentionStore(t, b)
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "closed-releases.db"),
	})
	require.NoError(t, err)
	require.NoError(t, releaseStore.Close())
	b.releaseStore = releaseStore
	putRestoringRetention(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: orig, NewLeaseUUID: destination,
		Tenant: "tenant-a", ProviderUUID: "provider-a",
		Status: shared.RetentionStatusRestoring, Generation: 3,
		Items: items, StackManifest: stack,
		DestinationItems: items, DestinationResourceProfiles: profiles,
		DestinationOperationID: operationID,
		DestinationCallbackURL: callbackURL, DestinationLifecycleCallbackURL: lifecycleURL,
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(orig, "app", 0))},
		CreatedAt:           time.Now(),
	})

	volumeTouched := false
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			volumeTouched = true
			return nil, nil
		},
		RenameVolumeFn: func(_, _ string) error { volumeTouched = true; return nil },
		DestroyFn:      func(context.Context, string) error { volumeTouched = true; return nil },
	}

	err = b.Deprovision(context.Background(), destination)
	require.ErrorContains(t, err, "has not durably committed ownership")
	assert.False(t, volumeTouched, "failed ownership finalization must precede every volume mutation")

	b.provisionsMu.RLock()
	status := b.provisions[destination].Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status,
		"failed preflight must not admit the actor's Ready -> Deprovisioning transition")
	source, getErr := rs.Get(orig)
	require.NoError(t, getErr)
	require.NotNil(t, source)
	assert.Equal(t, shared.RetentionStatusRestoring, source.Status)
}

// A close racing an uncommitted restore is refused. The subsequent restore
// rollback hands the live reservation to retained accounting without a gap.
func TestDoDeprovision_ClaimedVolumeAllocation_ReleasedByRestoreRollback(t *testing.T) {
	b, rs, claimedVol := claimedCloseBackend(t, false)
	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, name string) error {
			destroyed = append(destroyed, name)
			return nil
		},
		RenameVolumeFn: func(_, _ string) error { return nil },
		UsageFn:        func(context.Context, string) (int64, error) { return 0, nil },
	}

	require.ErrorContains(t, b.ensureCommittedRestoreDestinationForClose("u2"), "has not durably committed ownership")
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "close holds the reservation")
	assert.NotContains(t, destroyed, claimedVol,
		"an uncommitted destination close must preserve bytes owned by its source finalizer")

	// The restore rollback now runs (boot or retention sweep) and finishes the job.
	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	b.provisionsMu.RLock()
	require.Equal(t, backend.ProvisionStatusFailed, b.provisions["u2"].Status)
	b.provisionsMu.RUnlock()
	require.False(t, b.leaseActorProcessingOrQueued("u2"))
	require.NoError(t, b.reconcileRestoring(context.Background(), *entry))

	reverted, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, reverted)
	require.Equal(t, shared.RetentionStatusActive, reverted.Status, "the rollback completed")

	assert.Zero(t, b.pool.Stats().AllocatedDiskMB, "the live allocation is released only now")
	assert.Equal(t, int64(512), b.pool.Stats().RetainedDiskMB,
		"and the same bytes are counted as retained instead — re-counted BEFORE released, never a gap")
	assertRetentionAccountingConsistent(t, b, "accounting must be consistent after the hand-off")
}

// TestDoDeprovision_NoClaimedVolume_StillReleasesAllocation is the control: the
// reservation is held because of the claimed volume, not because the close stopped
// releasing. Without this the tests above would pass against a release that never runs.
func TestDoDeprovision_NoClaimedVolume_StillReleasesAllocation(t *testing.T) {
	const lease = "22222222-2222-4222-8222-222222222222"
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"app": {Image: "nginx:latest"},
			}},
		}},
	})
	withMicroSKU(b, 512)
	attachRetentionStore(t, b) // no restoring record → nothing is claimed
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-micro", "tenant-a"))
	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, _ string) error { return nil }}

	require.NoError(t, b.doDeprovision(context.Background(), lease))

	assert.Zero(t, b.pool.Stats().AllocatedDiskMB,
		"an ordinary close destroys its volumes and must still return their capacity")
	assert.Zero(t, b.pool.Stats().AllocationCount)
}

// An uncommitted restore cannot enter the close give-up path at all. This is
// stronger than filtering adopted names out of a tombstone after teardown failed.
func TestDeprovision_UncommittedRestoreCannotEnterGiveUp(t *testing.T) {
	const lease = "u2"
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "app"}}
	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 2,
			Items: items,
		}, VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1}, // next failure → give up
	})
	withMicroSKU(b, 512)
	rs := attachRetentionStore(t, b)
	claimedVol := seedRestoringInto(t, rs, "u1", lease, items) // fred-u2-app-0 IS u1's adopted data
	ownVol := canonicalVolumeName(lease, "app", 1)

	var destroyed []string
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{claimedVol, ownVol}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return errors.New("EBUSY")
		},
	}

	err := b.ensureCommittedRestoreDestinationForClose(lease)
	require.ErrorContains(t, err, "has not durably committed ownership")

	tomb, err := rs.Get(lease)
	require.NoError(t, err)
	assert.Nil(t, tomb, "close refusal must not manufacture a reaping tombstone")
	assert.Empty(t, destroyed, "close refusal must precede all volume cleanup")

	// And the original record is untouched, so its restore is still possible.
	orig, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, orig)
	assert.Equal(t, shared.RetentionStatusRestoring, orig.Status)
}

// The restore finalizer is durable sizing authority across refresh. A blocked
// close cannot create the former uncounted window before rollback completes.
func TestDoDeprovision_ClaimedVolume_AccountingAcrossRecoveryAndSweep(t *testing.T) {
	b, rs, claimedVol := claimedCloseBackend(t, false)
	b.volumes = &mockVolumeManager{
		DestroyFn:      func(_ context.Context, _ string) error { return nil },
		RenameVolumeFn: func(_, _ string) error { return nil },
		ListFn:         func() ([]string, error) { return []string{claimedVol}, nil },
		UsageFn:        func(context.Context, string) (int64, error) { return 0, nil },
	}

	// (1) The close: the reservation is held, because the bytes are still on disk and
	// a restoring record is counted by neither projection.
	require.ErrorContains(t, b.ensureCommittedRestoreDestinationForClose("u2"), "has not durably committed ownership")
	assert.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "close holds the reservation")
	assert.Zero(t, b.pool.Stats().RetainedDiskMB)

	// (2) The next reconcile tick reconstructs the exact reservation from the
	// restoring finalizer even when Docker reports no destination survivors.
	dockerMock, ok := b.docker.(*mockDockerClient)
	require.True(t, ok)
	dockerMock.ListManagedContainersFn = func(_ context.Context) ([]ContainerInfo, error) { return nil, nil }
	require.NoError(t, b.recoverState(context.Background()))
	assert.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB,
		"the restore finalizer must keep adopted bytes reserved across refresh")
	assert.Zero(t, b.pool.Stats().RetainedDiskMB,
		"the same bytes must not also be counted as retained")

	// (3) The retention sweep — the real end of the window. It re-quarantines the
	// volume, reverts the record, and the bytes become retained-counted again.
	require.NoError(t, b.runRetentionSweep(context.Background()))

	reverted, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, reverted)
	assert.Equal(t, shared.RetentionStatusActive, reverted.Status, "the sweep completed the rollback")
	assert.Equal(t, int64(512), b.pool.Stats().RetainedDiskMB,
		"the bytes are counted again — the window closes here, not at the reconcile tick")
	assertRetentionAccountingConsistent(t, b, "accounting is consistent once the sweep has run")
}
