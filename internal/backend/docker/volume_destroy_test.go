package docker

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

const (
	volumeClaimSourceLease = "0192f1a0-3111-4abc-8def-000000000731"
	volumeClaimTargetLease = "0192f1a0-3222-4abc-8def-000000000732"
	volumeClaimOtherLease  = "0192f1a0-3333-4abc-8def-000000000733"
)

// restoringInto seeds the record this whole file exists for: orig's retained data is
// being restored into newLease, so on disk it currently wears fred-{newLease}-web-0.
func restoringInto(t *testing.T, rs *shared.RetentionStore, orig, newLease string) {
	t.Helper()
	putRestoringRetention(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID:   orig,
		NewLeaseUUID:        newLease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          3,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "web"}},
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(orig, "web", 0))},
	})
}

// TestVolumeClaims_AdoptedNameIsOwnedByTheOriginalLease is the load-bearing assertion of
// the owner table, and the reason a name cannot be trusted on its own: while a record is
// restoring, the ORIGINAL lease's data sits under the NEW lease's canonical name, so a
// close of the new lease sees a volume that looks like its own. It is not (ENG-647).
//
// Ported from TestRestoringClaimedVolumes_ClaimsOnlyMatchingRestoringRecords, which
// tested the per-site helper this table replaced.
func TestVolumeClaims_AdoptedNameIsOwnedByTheOriginalLease(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)

	restoringInto(t, rs, volumeClaimSourceLease, volumeClaimTargetLease)
	// An ACTIVE record for another lease: its canonical name is claimed by ITS lease.
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID:   volumeClaimOtherLease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(volumeClaimOtherLease, "web", 0))},
	}))

	claims, err := b.snapshotVolumeClaims()
	require.NoError(t, err)

	adoptedName := canonicalVolumeName(volumeClaimTargetLease, "web", 0)
	adopted, ok := claims.owner(adoptedName)
	require.True(t, ok, "the adopted canonical name must be claimed; unclaimed means destroyable")
	assert.Equal(t, volumeClaimSourceLease, adopted.owner,
		"the adopted volume belongs to the ORIGINAL lease — attributing it to u2 would authorize the very close ENG-647 fixed")
	assert.Equal(t, claimAdopted, adopted.kind)

	// The original lease's own canonical name stays claimed too: a soft-delete
	// interrupted part-way through its renames can leave it on disk.
	src, ok := claims.owner(canonicalVolumeName(volumeClaimSourceLease, "web", 0))
	require.True(t, ok)
	assert.Equal(t, volumeClaimSourceLease, src.owner)
	assert.Equal(t, claimRestoreSrc, src.kind)

	// The unrelated ACTIVE record claims its own canonical, owned by itself.
	other, ok := claims.owner(canonicalVolumeName(volumeClaimOtherLease, "web", 0))
	require.True(t, ok)
	assert.Equal(t, volumeClaimOtherLease, other.owner)
	assert.Equal(t, claimRetained, other.kind)

	// And the decision the table exists to make.
	_, mayU2 := claims.mayDestroy(adoptedName, volumeClaimTargetLease)
	assert.False(t, mayU2, "a close of u2 must not be permitted to destroy u1's data")
	_, mayU1 := claims.mayDestroy(adoptedName, volumeClaimSourceLease)
	assert.True(t, mayU1, "u1 owns those bytes, so u1 may act on them")
	_, mayNobody := claims.mayDestroy(adoptedName, "")
	assert.False(t, mayNobody, "a collector asserting no ownership must be refused by ANY claim")
}

// TestVolumeClaims_LiveProvisionClaimsItsCanonicalNames covers the live-provision half
// of the ownership table, including that the name is built by canonicalVolumeName rather
// than a second copy of the format string.
func TestVolumeClaims_LiveProvisionClaimsItsCanonicalNames(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		volumeClaimSourceLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: volumeClaimSourceLease, Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 2,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "app"}},
		}},
	})

	claims, err := b.snapshotVolumeClaims()
	require.NoError(t, err)

	for _, idx := range []int{0, 1} {
		name := canonicalVolumeName(volumeClaimSourceLease, "app", idx)
		claim, ok := claims.owner(name)
		require.True(t, ok, "every instance of a live provision must be claimed, not just the first")
		assert.Equal(t, volumeClaim{kind: claimLive, owner: volumeClaimSourceLease}, claim)
	}
	_, ok := claims.owner(canonicalVolumeName(volumeClaimSourceLease, "app", 2))
	assert.False(t, ok, "an index beyond Quantity is not claimed — that is what makes a leak collectable")
}

// TestVolumeClaims_ReapingRecordClaimsNothing pins a deliberate hole: a reaping record is
// a scheduled DESTROY, not an assertion of ownership. Claiming its names would make the
// finalizer unable to execute its own tombstone.
func TestVolumeClaims_ReapingRecordClaimsNothing(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID:   volumeClaimOtherLease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusReaping,
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(volumeClaimOtherLease, "web", 0))},
	}))

	claims, err := b.snapshotVolumeClaims()
	require.NoError(t, err)
	_, ok := claims.owner("fred-u9-web-0")
	assert.False(t, ok, "a tombstone must not claim its own names, or the finalizer could never run")
	assert.Empty(t, claims.byName)
}

// TestVolumeClaims_KeysAreNeverRetainedNames is a structural invariant, not a behaviour:
// every key the table produces is a CANONICAL name. destroyReapingVolumes relies on it to
// skip the claim resolution entirely when a tombstone carries only fred-retained-* names.
// If a future claim kind ever keys a retained name, that elision silently stops
// protecting anything — so this test fails first.
func TestVolumeClaims_KeysAreNeverRetainedNames(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		volumeClaimSourceLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: volumeClaimSourceLease, Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	restoringInto(t, rs, volumeClaimTargetLease, volumeClaimOtherLease)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID:   "0192f1a0-3444-4abc-8def-000000000734",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-0192f1a0-3444-4abc-8def-000000000734-web-0"},
	}))

	claims, err := b.snapshotVolumeClaims()
	require.NoError(t, err)
	require.NotEmpty(t, claims.byName, "the guard would pass vacuously on an empty table")
	for name := range claims.byName {
		assert.False(t, isRetainedVolume(name),
			"claim key %q is in the retained namespace; destroyReapingVolumes' hot-path elision assumes none are", name)
	}
}

// TestVolumeClaims_NilStore_YieldsLiveClaimsOnly covers the ~100 fixtures that run with
// no retention store: no record can exist, so no record-derived claim can either — and
// that must not read as an error, which would refuse every destroy.
func TestVolumeClaims_NilStore_YieldsLiveClaimsOnly(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		volumeClaimSourceLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: volumeClaimSourceLease, Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	require.Nil(t, b.retentionStore, "precondition: this fixture has no retention store")

	claims, err := b.snapshotVolumeClaims()
	require.NoError(t, err)
	_, ok := claims.owner(canonicalVolumeName(volumeClaimSourceLease, "app", 0))
	assert.True(t, ok, "live provisions still claim their volumes without a retention store")
}

// TestVolumeClaims_StoreReadError_ReturnsNoTable pins the fail-safe shape: a read error
// must surface as an error and NEVER as a partial table, because a missing entry reads as
// "unclaimed", which is the direction that destroys data.
func TestVolumeClaims_StoreReadError_ReturnsNoTable(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		volumeClaimSourceLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: volumeClaimSourceLease, Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Close()) // every subsequent read errors

	claims, err := b.snapshotVolumeClaims()
	require.Error(t, err)
	assert.Nil(t, claims, "a partial table is worse than none: its gaps read as 'unclaimed'")
}

// TestVolumeOp_Destroy_RefusesAVolumeAnotherLeaseOwns is the primitive doing the job the
// six call sites used to do individually.
func TestVolumeOp_Destroy_RefusesAVolumeAnotherLeaseOwns(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		volumeClaimTargetLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: volumeClaimTargetLease, Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	restoringInto(t, rs, volumeClaimSourceLease, volumeClaimTargetLease)

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}

	before := testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteDeprovisionDestroy, destroyRefusedClaimed))
	op := b.volumeOp(volumeClaimTargetLease, slog.Default())
	installTestStorageMutationAdapters(b)
	rep := op.destroy(volumeDestroyCapabilityForTest(b), context.Background(), destroySiteDeprovisionDestroy,
		canonicalVolumeName(volumeClaimTargetLease, "web", 0), // source's data, adopted under target's name
		canonicalVolumeName(volumeClaimTargetLease, "app", 0), // target's own volume
	)

	assert.Equal(t, []string{canonicalVolumeName(volumeClaimTargetLease, "app", 0)}, destroyed,
		"the closing lease's own volume is still destroyed — the refusal is per name, not per call")
	assert.Equal(t, []string{canonicalVolumeName(volumeClaimTargetLease, "web", 0)}, rep.Claimed)
	assert.Empty(t, rep.Errs)
	assert.NoError(t, rep.err(), "a claimed volume is not an error — another owner has it, and that resolves itself")
	assert.True(t, rep.leftOnDisk(), "but the bytes ARE still on disk, so the caller must keep them counted")
	assert.InDelta(t, before+1,
		testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteDeprovisionDestroy, destroyRefusedClaimed)), 0.0001)
}

// TestVolumeOp_Destroy_UnclaimedIsDestroyable covers the collector's case: owner "" may
// destroy anything nothing claims, and nothing else.
func TestVolumeOp_Destroy_UnreadableClaims_DestroysNothing(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Close())

	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("must not destroy %q when ownership could not be established", id)
			return nil
		},
	}

	before := testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteReaping, destroyRefusedUnreadable))
	rep := destroyVolumesForTest(b, "u1", context.Background(), destroySiteReaping, "fred-u1-app-0", "fred-u1-app-1")

	assert.Empty(t, rep.Destroyed)
	assert.Len(t, rep.Unproven, 2)
	require.Error(t, rep.err(),
		"unlike a claimed volume, an unprovable one IS a failure: the caller could not do its job and must retry")
	assert.True(t, rep.leftOnDisk())
	assert.InDelta(t, before+2,
		testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteReaping, destroyRefusedUnreadable)), 0.0001,
		"counted per volume, so the refusal is visible at the same granularity as the decision")
}

func TestVolumeOp_Destroy_ReportsPerNameFailures(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			if id == "fred-u1-app-1" {
				return assert.AnError
			}
			return nil
		},
	}

	rep := destroyVolumesForTest(b, "u1", context.Background(), destroySiteDeprovisionDestroy,
		"fred-u1-app-0", "fred-u1-app-1")

	assert.Equal(t, []string{"fred-u1-app-0"}, rep.Destroyed)
	assert.Len(t, rep.Errs, 1)
	assert.ErrorContains(t, rep.err(), "fred-u1-app-1")
	assert.True(t, rep.leftOnDisk())
	assert.Zero(t, rep.refused(), "a failure is not a refusal; only one of the two is self-healing")
}

// TestVolumeOp_ResolvesClaimsAtMostOnce pins the cost property that makes a per-name
// primitive affordable: one ownership read per operation, not per volume. Driven by
// closing the store AFTER the first resolution — a second read would fail.
func TestVolumeOp_ResolvesClaimsAtMostOnce(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	restoringInto(t, rs, volumeClaimSourceLease, volumeClaimTargetLease)

	op := b.volumeOp(volumeClaimTargetLease, slog.Default())
	_, err := op.claims()
	require.NoError(t, err)
	require.NoError(t, rs.Close())

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}
	installTestStorageMutationAdapters(b)
	rep := op.destroy(volumeDestroyCapabilityForTest(b), context.Background(), destroySiteDeprovisionDestroy,
		canonicalVolumeName(volumeClaimTargetLease, "web", 0),
		canonicalVolumeName(volumeClaimTargetLease, "app", 0),
	)

	assert.Empty(t, rep.Unproven, "the op must reuse its resolved table rather than re-reading a now-closed store")
	assert.Equal(t, []string{canonicalVolumeName(volumeClaimTargetLease, "web", 0)}, rep.Claimed)
	assert.Equal(t, []string{canonicalVolumeName(volumeClaimTargetLease, "app", 0)}, destroyed)
}

// TestVolumeOp_Partition_SplitsMineFromForeign covers the query half of the primitive.
// The close path's retain arm needs it because its hazard is a RENAME into
// fred-retained-{closingLease}-*, which loses the data just as thoroughly as a destroy.
func TestVolumeOp_Partition_SplitsMineFromForeign(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		volumeClaimTargetLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: volumeClaimTargetLease, Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	restoringInto(t, rs, volumeClaimSourceLease, volumeClaimTargetLease)

	mine, foreign, err := b.volumeOp(volumeClaimTargetLease, slog.Default()).partition([]string{
		canonicalVolumeName(volumeClaimTargetLease, "app", 0),
		canonicalVolumeName(volumeClaimTargetLease, "web", 0),
		canonicalVolumeName(volumeClaimTargetLease, "unclaimed", 0),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{canonicalVolumeName(volumeClaimTargetLease, "app", 0), canonicalVolumeName(volumeClaimTargetLease, "unclaimed", 0)}, mine,
		"unclaimed names stay with the asking lease — that is what keeps a leaked volume collectable")
	assert.Equal(t, []string{canonicalVolumeName(volumeClaimTargetLease, "web", 0)}, foreign)
}

// TestVolumeOp_Partition_StoreError_Refuses: same fail-safe as destroy. Returning an
// empty foreign set here would let the caller retain another lease's volume under its own
// name, which is the rename half of ENG-647.
func TestVolumeOp_Partition_StoreError_Refuses(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Close())

	mine, foreign, err := b.volumeOp(volumeClaimTargetLease, slog.Default()).partition([]string{canonicalVolumeName(volumeClaimTargetLease, "app", 0)})
	require.Error(t, err)
	assert.Nil(t, mine)
	assert.Nil(t, foreign)
}

// TestAssertVolumeDestroyer_AcceptsEveryRealManager guards the startup check against
// becoming a tripwire on a healthy configuration: all four production managers must pass.
func TestAssertVolumeDestroyer_AcceptsEveryRealManager(t *testing.T) {
	for _, vm := range []volumeManager{
		&noopVolumeManager{},
		&xfsVolumeManager{},
		&btrfsVolumeManager{},
		&zfsVolumeManager{},
	} {
		assert.NoError(t, assertVolumeDestroyer(vm), "%T is a production manager and must be destroy-capable", vm)
	}
}

// ---------------------------------------------------------------------------
// ENG-658 definition of done: a close of the new lease during an in-flight restore
// cannot reach the original lease's data by ANY path.
//
// The point of writing it this way is that it is one invariant asserted against the
// choke point, driven through every entry point that can reach a destroy — not seven
// per-site guards that each have to be remembered. A new destroy path added later is
// covered the moment it routes through volumeOp.destroy, and cannot be added any other
// way (Destroy is off the volumeManager interface, and forbidigo pins the assertion to
// volume_destroy.go).
// ---------------------------------------------------------------------------

// chokePointFixture builds the collision: the destination lease is being closed while
// an in-flight restore of the source lease has adopted its data into the destination
// namespace.
//
//	chokePointAdopted  ← source's retained data, adopted (MUST survive every path)
//	chokePointOwn      ← destination's own volume (SHOULD be reaped)
func chokePointFixture(t *testing.T, retainOnClose bool) (b *Backend, rs *shared.RetentionStore, destroyed *[]string, renamed *[][2]string) {
	t.Helper()
	const lease = chokePointDestinationLease
	b = newBackendForProvisionTest(t, &mockDockerClient{
		RemoveContainerFn:       func(_ context.Context, _ string) error { return nil },
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) { return nil, nil },
	}, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 2,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "app"}},
		}},
	})
	withMicroSKU(b, 512)
	b.cfg.RetainOnClose = retainOnClose
	rs = attachRetentionStore(t, b)
	require.Equal(t, chokePointAdopted, seedRestoringInto(
		t, b, chokePointSourceLease, lease,
		[]backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "app"}},
	),
		"precondition: the restore adopts source data under the destination's canonical name")

	destroyed, renamed = &[]string{}, &[][2]string{}
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{chokePointAdopted, chokePointOwn, chokePointOrphan}, nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			*destroyed = append(*destroyed, id)
			return nil
		},
		RenameVolumeFn: func(oldName, newName string) error {
			*renamed = append(*renamed, [2]string{oldName, newName})
			return nil
		},
	}
	return b, rs, destroyed, renamed
}

const (
	chokePointSourceLease      = "0192f1a0-1111-4abc-8def-000000000731"
	chokePointDestinationLease = "0192f1a0-2222-4abc-8def-000000000732"
	chokePointAdopted          = "fred-" + chokePointDestinationLease + "-app-0"
	chokePointOwn              = "fred-" + chokePointDestinationLease + "-app-1"
	chokePointOrphan           = "fred-leaked-99" // owned by nobody: a genuine leak
)

// assertAdoptedDataSurvived is the single invariant, checked identically for every entry
// point. "Survived" means three things, and all three matter: the bytes are not
// destroyed, they are not re-quarantined under the CLOSING lease (which would leave the
// source record naming a path that no longer exists — the rename half of ENG-647), and
// the source record still points at them.
func assertAdoptedDataSurvived(t *testing.T, rs *shared.RetentionStore, destroyed []string, renamed [][2]string) {
	t.Helper()
	assert.NotContains(t, destroyed, chokePointAdopted,
		"the adopted volume is the source's retained data; destroying it is unrecoverable and kills the restore")
	for _, r := range renamed {
		if r[0] == chokePointAdopted {
			assert.False(t, strings.HasPrefix(r[1], retainedName(leaseVolumePrefix(chokePointDestinationLease))),
				"re-retaining the adopted volume under the destination strands the source record just as permanently as destroying it")
		}
	}
	rec, err := rs.Get(chokePointSourceLease)
	require.NoError(t, err)
	require.NotNil(t, rec, "the source record is the adopted volume's finalizer and must survive")
	assert.Equal(t, []string{retainedName(canonicalVolumeName(chokePointSourceLease, "app", 0))}, rec.RetainedVolumeNames,
		"and must still name the data reconcileRestoring will re-quarantine")
}

func TestDestroyChokePoint_InFlightRestoreDataIsUnreachableByAnyPath(t *testing.T) {
	// There is deliberately no synthetic destination-reaping-tombstone case:
	// ensureCommittedRestoreDestinationForClose rejects this unresolved restore
	// before the production close path can acquire or publish such authority.
	cases := []struct {
		name          string
		retainOnClose bool
		// setup arranges the entry point's preconditions.
		setup func(t *testing.T, b *Backend, rs *shared.RetentionStore)
		drive func(t *testing.T, b *Backend)
		// wantReaped names a volume this path MUST still remove, so a path that simply
		// does nothing cannot satisfy the invariant. Empty means the path is not
		// expected to destroy anything (the retain arm renames instead).
		wantReaped string
	}{
		{
			name:          "uncommitted close, non-retain",
			retainOnClose: false,
			drive: func(t *testing.T, b *Backend) {
				_ = b.doDeprovisionForTest(t, context.Background(), chokePointDestinationLease)
			},
		},
		{
			name:          "uncommitted close, retain",
			retainOnClose: true,
			drive: func(t *testing.T, b *Backend) {
				_ = b.doDeprovisionForTest(t, context.Background(), chokePointDestinationLease)
			},
		},
		{
			name:          "uncommitted close, retention cap breached",
			retainOnClose: true,
			setup: func(_ *testing.T, b *Backend, _ *shared.RetentionStore) {
				b.cfg.MaxRetainedDiskMB = 1 // any stateful lease breaches
			},
			drive: func(t *testing.T, b *Backend) {
				_ = b.doDeprovisionForTest(t, context.Background(), chokePointDestinationLease)
			},
		},
		{
			name: "provision-failure cleanup",
			// Driven at the primitive with this site's label rather than through
			// doProvision, and deliberately so: createdVolumeIDs only ever names volumes
			// Create reported as newly created, and an adopted volume already exists, so
			// a full doProvision run could not put the adopted name in front of this site
			// at all — the test would pass without proving anything. Asserting the
			// primitive refuses the name IF it ever arrives is the honest version, and it
			// is what protects the site if that structural argument ever stops holding.
			drive: func(t *testing.T, b *Backend) {
				destroyVolumesForTest(b, chokePointDestinationLease, context.Background(), destroySiteProvisionCleanup,
					chokePointAdopted, chokePointOwn)
			},
			wantReaped: chokePointOwn,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, rs, destroyed, renamed := chokePointFixture(t, tc.retainOnClose)
			if tc.setup != nil {
				tc.setup(t, b, rs)
			}
			tc.drive(t, b)

			assertAdoptedDataSurvived(t, rs, *destroyed, *renamed)
			if tc.wantReaped != "" {
				assert.Contains(t, *destroyed, tc.wantReaped,
					"this path must still reap what it legitimately owns, or the case proves nothing")
			}
		})

		t.Run(tc.name+" / ownership unreadable", func(t *testing.T) {
			b, rs, destroyed, renamed := chokePointFixture(t, tc.retainOnClose)
			if tc.setup != nil {
				tc.setup(t, b, rs)
			}
			require.NoError(t, rs.Close()) // every ownership read now fails
			tc.drive(t, b)

			assert.Empty(t, *destroyed,
				"with ownership unprovable, no path may destroy anything: we cannot tell ours from theirs, "+
					"and only one of those two mistakes is reversible")
			for _, r := range *renamed {
				assert.NotEqual(t, chokePointAdopted, r[0],
					"nor may any path rename the adopted volume while ownership is unprovable")
			}
		})
	}
}
