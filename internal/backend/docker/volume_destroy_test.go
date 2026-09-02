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

	restoringInto(t, rs, "u1", "u2")
	// An ACTIVE record for another lease: its canonical name is claimed by ITS lease.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "u3",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-u3-web-0"},
	}))

	claims, err := b.snapshotVolumeClaims()
	require.NoError(t, err)

	adopted, ok := claims.owner("fred-u2-web-0")
	require.True(t, ok, "the adopted canonical name must be claimed; unclaimed means destroyable")
	assert.Equal(t, "u1", adopted.owner,
		"the adopted volume belongs to the ORIGINAL lease — attributing it to u2 would authorize the very close ENG-647 fixed")
	assert.Equal(t, claimAdopted, adopted.kind)

	// The original lease's own canonical name stays claimed too: a soft-delete
	// interrupted part-way through its renames can leave it on disk.
	src, ok := claims.owner("fred-u1-web-0")
	require.True(t, ok)
	assert.Equal(t, "u1", src.owner)
	assert.Equal(t, claimRestoreSrc, src.kind)

	// The unrelated ACTIVE record claims its own canonical, owned by itself.
	other, ok := claims.owner("fred-u3-web-0")
	require.True(t, ok)
	assert.Equal(t, "u3", other.owner)
	assert.Equal(t, claimRetained, other.kind)

	// And the decision the table exists to make.
	_, mayU2 := claims.mayDestroy("fred-u2-web-0", "u2")
	assert.False(t, mayU2, "a close of u2 must not be permitted to destroy u1's data")
	_, mayU1 := claims.mayDestroy("fred-u2-web-0", "u1")
	assert.True(t, mayU1, "u1 owns those bytes, so u1 may act on them")
	_, mayNobody := claims.mayDestroy("fred-u2-web-0", "")
	assert.False(t, mayNobody, "a collector asserting no ownership must be refused by ANY claim")
}

// TestVolumeClaims_LiveProvisionClaimsItsCanonicalNames covers the half of the table that
// replaces cleanupOrphanedVolumes' hand-built expected set — including that the name is
// built by canonicalVolumeName rather than a second copy of the format string.
func TestVolumeClaims_LiveProvisionClaimsItsCanonicalNames(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 2,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "app"}},
		}},
	})

	claims, err := b.snapshotVolumeClaims()
	require.NoError(t, err)

	for _, idx := range []int{0, 1} {
		name := canonicalVolumeName("u1", "app", idx)
		claim, ok := claims.owner(name)
		require.True(t, ok, "every instance of a live provision must be claimed, not just the first")
		assert.Equal(t, volumeClaim{kind: claimLive, owner: "u1"}, claim)
	}
	_, ok := claims.owner(canonicalVolumeName("u1", "app", 2))
	assert.False(t, ok, "an index beyond Quantity is not claimed — that is what makes a leak collectable")
}

// TestVolumeClaims_ReapingRecordClaimsNothing pins a deliberate hole: a reaping record is
// a scheduled DESTROY, not an assertion of ownership. Claiming its names would make the
// finalizer unable to execute its own tombstone.
func TestVolumeClaims_ReapingRecordClaimsNothing(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "u9",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusReaping,
		RetainedVolumeNames: []string{"fred-retained-u9-web-0"},
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
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	restoringInto(t, rs, "u2", "u3")
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "u4",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-u4-web-0"},
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
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	require.Nil(t, b.retentionStore, "precondition: this fixture has no retention store")

	claims, err := b.snapshotVolumeClaims()
	require.NoError(t, err)
	_, ok := claims.owner(canonicalVolumeName("u1", "app", 0))
	assert.True(t, ok, "live provisions still claim their volumes without a retention store")
}

// TestVolumeClaims_StoreReadError_ReturnsNoTable pins the fail-safe shape: a read error
// must surface as an error and NEVER as a partial table, because a missing entry reads as
// "unclaimed", which is the direction that destroys data.
func TestVolumeClaims_StoreReadError_ReturnsNoTable(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
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
		"u2": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u2", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	restoringInto(t, rs, "u1", "u2")

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}

	before := testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteDeprovisionDestroy, destroyRefusedClaimed))
	op := b.volumeOp("u2", slog.Default())
	rep := op.destroy(context.Background(), destroySiteDeprovisionDestroy,
		"fred-u2-web-0",                     // u1's data, adopted under u2's name
		canonicalVolumeName("u2", "app", 0), // u2's own volume
	)

	assert.Equal(t, []string{canonicalVolumeName("u2", "app", 0)}, destroyed,
		"the closing lease's own volume is still destroyed — the refusal is per name, not per call")
	assert.Equal(t, []string{"fred-u2-web-0"}, rep.Claimed)
	assert.Empty(t, rep.Errs)
	assert.NoError(t, rep.err(), "a claimed volume is not an error — another owner has it, and that resolves itself")
	assert.True(t, rep.leftOnDisk(), "but the bytes ARE still on disk, so the caller must keep them counted")
	assert.InDelta(t, before+1,
		testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteDeprovisionDestroy, destroyRefusedClaimed)), 0.0001)
}

// TestVolumeOp_Destroy_UnclaimedIsDestroyable covers the collector's case: owner "" may
// destroy anything nothing claims, and nothing else.
func TestVolumeOp_Destroy_UnclaimedIsDestroyable(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	restoringInto(t, rs, "u1", "u2")

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}

	rep := b.volumeOp("", slog.Default()).destroy(context.Background(), destroySiteOrphanGC,
		"fred-leaked-orphan-0", "fred-u2-web-0")

	assert.Equal(t, []string{"fred-leaked-orphan-0"}, destroyed)
	assert.Equal(t, []string{"fred-u2-web-0"}, rep.Claimed,
		"a collector asserts no ownership, so any claim refuses it")
}

// TestVolumeOp_Destroy_UnreadableClaims_DestroysNothing is the fail-safe branch, and the
// one CLAUDE.md specifically requires be tested: uncertainty must collapse toward keeping.
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
	rep := b.volumeOp("u1", slog.Default()).destroy(context.Background(), destroySiteReaping, "fred-u1-app-0", "fred-u1-app-1")

	assert.Empty(t, rep.Destroyed)
	assert.Len(t, rep.Unproven, 2)
	require.Error(t, rep.err(),
		"unlike a claimed volume, an unprovable one IS a failure: the caller could not do its job and must retry")
	assert.True(t, rep.leftOnDisk())
	assert.InDelta(t, before+2,
		testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteReaping, destroyRefusedUnreadable)), 0.0001,
		"counted per volume, so the refusal is visible at the same granularity as the decision")
}

// TestVolumeOp_Destroy_ReportsPerNameFailures keeps the three outcomes distinct — a
// caller that cannot tell "gone" from "failed" from "refused" releases capacity it
// shouldn't.
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

	rep := b.volumeOp("u1", slog.Default()).destroy(context.Background(), destroySiteDeprovisionDestroy,
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
	restoringInto(t, rs, "u1", "u2")

	op := b.volumeOp("u2", slog.Default())
	_, err := op.claims()
	require.NoError(t, err)
	require.NoError(t, rs.Close())

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}
	rep := op.destroy(context.Background(), destroySiteDeprovisionDestroy, "fred-u2-web-0", "fred-u2-app-0")

	assert.Empty(t, rep.Unproven, "the op must reuse its resolved table rather than re-reading a now-closed store")
	assert.Equal(t, []string{"fred-u2-web-0"}, rep.Claimed)
	assert.Equal(t, []string{"fred-u2-app-0"}, destroyed)
}

// TestVolumeOp_Partition_SplitsMineFromForeign covers the query half of the primitive.
// The close path's retain arm needs it because its hazard is a RENAME into
// fred-retained-{closingLease}-*, which loses the data just as thoroughly as a destroy.
func TestVolumeOp_Partition_SplitsMineFromForeign(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"u2": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u2", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	restoringInto(t, rs, "u1", "u2")

	mine, foreign, err := b.volumeOp("u2", slog.Default()).partition([]string{
		canonicalVolumeName("u2", "app", 0), "fred-u2-web-0", "fred-u2-unclaimed-0",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{canonicalVolumeName("u2", "app", 0), "fred-u2-unclaimed-0"}, mine,
		"unclaimed names stay with the asking lease — that is what keeps a leaked volume collectable")
	assert.Equal(t, []string{"fred-u2-web-0"}, foreign)
}

// TestVolumeOp_Partition_StoreError_Refuses: same fail-safe as destroy. Returning an
// empty foreign set here would let the caller retain another lease's volume under its own
// name, which is the rename half of ENG-647.
func TestVolumeOp_Partition_StoreError_Refuses(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Close())

	mine, foreign, err := b.volumeOp("u2", slog.Default()).partition([]string{"fred-u2-app-0"})
	require.Error(t, err)
	assert.Nil(t, mine)
	assert.Nil(t, foreign)
}

// destroylessVolumeManager satisfies volumeManager but not volumeDestroyer, which is only
// expressible now that Destroy has left the interface. It stands in for the shape that
// would otherwise be invisible: a decorator wrapped around b.volumes (for logging or
// metrics) that forwards the interface it was given and silently drops the capability
// obtained by assertion. Merovius' objection to optional interfaces, made testable.
type destroylessVolumeManager struct{ volumeManager }

// TestVolumeOp_Destroy_NoDestroyCapability_DestroysNothing pins the refusal rather than a
// panic, and pins that assertVolumeDestroyer would have caught it at startup.
func TestVolumeOp_Destroy_NoDestroyCapability_DestroysNothing(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = destroylessVolumeManager{&mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("must not reach the underlying manager: the wrapper does not offer the capability, and %q is not ours to assume about", id)
			return nil
		},
	}}

	require.Error(t, assertVolumeDestroyer(b.volumes),
		"a manager without the capability must fail the daemon at startup, not at reap time")

	before := testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteOrphanGC, destroyRefusedNoDestroyer))
	rep := b.volumeOp("", slog.Default()).destroy(context.Background(), destroySiteOrphanGC, "fred-orphan-0")

	assert.Empty(t, rep.Destroyed)
	assert.Equal(t, []string{"fred-orphan-0"}, rep.Unproven)
	require.Error(t, rep.err())
	assert.InDelta(t, before+1,
		testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteOrphanGC, destroyRefusedNoDestroyer)), 0.0001)
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

// chokePointFixture builds the collision: lease u2 is being closed while an in-flight
// restore of u1 has adopted u1's data into u2's namespace.
//
//	fred-u2-app-0  ← u1's retained data, adopted (MUST survive every path)
//	fred-u2-app-1  ← u2's own volume     (SHOULD be reaped, so no case passes vacuously)
func chokePointFixture(t *testing.T, retainOnClose bool) (b *Backend, rs *shared.RetentionStore, destroyed *[]string, renamed *[][2]string) {
	t.Helper()
	const lease = "u2"
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
		t, rs, "u1", lease,
		[]backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "app"}},
	),
		"precondition: the restore adopts u1's data under u2's canonical name")

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
	chokePointAdopted = "fred-u2-app-0"  // u1's data wearing u2's name
	chokePointOwn     = "fred-u2-app-1"  // u2's own volume
	chokePointOrphan  = "fred-leaked-99" // owned by nobody: a genuine leak
)

// assertAdoptedDataSurvived is the single invariant, checked identically for every entry
// point. "Survived" means three things, and all three matter: the bytes are not
// destroyed, they are not re-quarantined under the CLOSING lease (which would leave u1's
// record naming a path that no longer exists — the rename half of ENG-647), and u1's
// record still points at them.
func assertAdoptedDataSurvived(t *testing.T, rs *shared.RetentionStore, destroyed []string, renamed [][2]string) {
	t.Helper()
	assert.NotContains(t, destroyed, chokePointAdopted,
		"the adopted volume is u1's retained data; destroying it is unrecoverable and kills u1's restore")
	for _, r := range renamed {
		if r[0] == chokePointAdopted {
			assert.False(t, strings.HasPrefix(r[1], retainedName(leaseVolumePrefix("u2"))),
				"re-retaining the adopted volume under u2 strands u1's record just as permanently as destroying it")
		}
	}
	rec, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, rec, "u1's record is the adopted volume's finalizer and must survive")
	assert.Equal(t, []string{retainedName(canonicalVolumeName("u1", "app", 0))}, rec.RetainedVolumeNames,
		"and must still name the data reconcileRestoring will re-quarantine")
}

func TestDestroyChokePoint_InFlightRestoreDataIsUnreachableByAnyPath(t *testing.T) {
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
			drive:         func(t *testing.T, b *Backend) { _ = b.doDeprovision(context.Background(), "u2") },
		},
		{
			name:          "uncommitted close, retain",
			retainOnClose: true,
			drive:         func(t *testing.T, b *Backend) { _ = b.doDeprovision(context.Background(), "u2") },
		},
		{
			name:          "uncommitted close, retention cap breached",
			retainOnClose: true,
			setup: func(_ *testing.T, b *Backend, _ *shared.RetentionStore) {
				b.cfg.MaxRetainedDiskMB = 1 // any stateful lease breaches
			},
			drive: func(t *testing.T, b *Backend) { _ = b.doDeprovision(context.Background(), "u2") },
		},
		{
			name: "startup orphan reaper",
			// Errors are not asserted in any drive: the fail-safe sub-case deliberately
			// drives each path with an unreadable store, where refusing IS the contract.
			drive: func(t *testing.T, b *Backend) {
				_ = b.cleanupOrphanedVolumes(context.Background())
			},
			// u2's own volume is NOT an orphan here — u2 is still tracked, and the sweep
			// runs at startup against live leases. The genuine leak is what must go.
			wantReaped: chokePointOrphan,
		},
		{
			name: "retention finalizer executing a pre-existing tombstone",
			setup: func(t *testing.T, b *Backend, rs *shared.RetentionStore) {
				// A give-up deletes the provision before writing its tombstone, so u2's
				// own volume is genuinely abandoned by the time the finalizer runs. Keep
				// that ordering: with the provision still tracked, the finalizer would
				// (correctly) refuse its own name too, and the case would prove nothing.
				b.provisionStore.Delete("u2")
				// The shape a pre-ENG-647 give-up produced: a tombstone that
				// prefix-collected fred-u2-* and so names the adopted volume too.
				require.NoError(t, rs.Put(shared.RetentionEntry{
					OriginalLeaseUUID:   "u2",
					Tenant:              "tenant-a",
					Status:              shared.RetentionStatusReaping,
					Items:               []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "app"}},
					RetainedVolumeNames: []string{chokePointAdopted, chokePointOwn},
				}))
			},
			drive: func(t *testing.T, b *Backend) {
				_ = b.retryReapingRecords(context.Background())
			},
			wantReaped: chokePointOwn,
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
				b.volumeOp("u2", b.logger).destroy(context.Background(), destroySiteProvisionCleanup,
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

// TestCleanupOrphanedVolumes_UnreadableClaims_CountsTheUndecided pins the ticketing
// signal for the startup sweep. The sweep bails before reaching op.destroy, so without
// an explicit count here the documented claims_unreadable series would never appear for
// site="orphan_gc" — an alert that reads 0 whether the store is healthy or unreadable.
func TestCleanupOrphanedVolumes_UnreadableClaims_CountsTheUndecided(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Close())

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			// Two undecidable, plus a retained name the sweep never considers.
			return []string{"fred-a-app-0", "fred-b-app-0", "fred-retained-c-app-0"}, nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("must not destroy %q when ownership could not be established", id)
			return nil
		},
	}

	before := testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteOrphanGC, destroyRefusedUnreadable))
	require.NoError(t, b.cleanupOrphanedVolumes(context.Background()),
		"an unreadable store skips the run; it does not fail startup")

	assert.InDelta(t, before+2,
		testutil.ToFloat64(volumeDestroyRefusedTotal.WithLabelValues(destroySiteOrphanGC, destroyRefusedUnreadable)), 0.0001,
		"counted per volume whose fate the run could not decide — retained names are not among them, "+
			"since the sweep never considers them at all")
}
