package docker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// N-03 / ENG-501: the orphan-record pruner must not treat a give-up-diverged
// record as orphaned. The record lists the fred-retained-* names, but on a
// persistent rename failure the volume is still on disk under its canonical
// fred-{lease}-* name — checking only the retained name would prune the record
// and let a later boot destroy the still-intact data.
func TestOrphanPruner_ProtectsDivergedCanonical(t *testing.T) {
	retained := "fred-retained-u1-app-0"
	canonical := canonicalFromRetained(retained)
	require.NotEqual(t, retained, canonical)

	for _, present := range []string{canonical, retained} {
		t.Run(present, func(t *testing.T) {
			b, store := newOrphanReconcileBackend(t, 1, true, []string{present}, nil)
			putActiveRetention(t, store, "u1", []string{retained})
			pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background())
			require.NoError(t, err)
			assert.Zero(t, pruned)
			assert.NotNil(t, getRetention(t, store, "u1"),
				"either physical spelling must protect the restore handle (ENG-501)")
		})
	}
}

// N-06 / ENG-512: reconcileRestoring must defer for a live provision in the
// Updating state (a running new lease whose restore record merely lingered past
// a failed terminal Delete) rather than tearing it down via the orphaned arm.
func TestReconcileRestoring_DefersForUpdating(t *testing.T) {
	origLease := canonicalRetentionFixtureUUID("u1")
	destinationLease := canonicalRetentionFixtureUUID("u2")
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, map[string]*provision{
		destinationLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: destinationLease,
			Status:    backend.ProvisionStatusUpdating,
		}},
	})
	rs := attachRetentionStore(t, b)

	downCalled := false
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error { downCalled = true; return nil },
	}
	b.volumes = &mockVolumeManager{}

	e := shared.RetentionEntry{
		OriginalLeaseUUID:   origLease,
		NewLeaseUUID:        destinationLease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          3,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(origLease, manifest.DefaultServiceName, 0))},
	}
	e = *putRestoringRetention(t, rs, e)

	b.reconcileRestoring(context.Background(), e)

	assert.False(t, downCalled,
		"a live lease at Updating is not a crashed restore; compose Down must NOT be called (ENG-512)")

	entry, err := rs.Get(origLease)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusRestoring, entry.Status)
	assert.Equal(t, e.Generation, entry.Generation, "orphaned arm / RevertToActiveWithResourceProfiles must NOT fire")

	b.provisionsMu.RLock()
	_, hasU2 := b.provisions[destinationLease]
	b.provisionsMu.RUnlock()
	assert.True(t, hasU2, "the live provision must NOT be removed")
}

func TestLeaseUUIDFromVolumeName(t *testing.T) {
	u := "0192f1a0-1111-7abc-8def-000000000001"
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"fred-" + u + "-app-0", u, true},
		{"fred-" + u + "-web-1-0", u, true}, // hyphenated service name
		{"fred-" + u + "-0", u, true},       // canonical v0.13 migration name
		{"fred-" + u + "-", "", false},      // missing service + idx
		{"fred-" + u + "-foo", "", false},   // missing numeric idx
		{"fred-" + u + "-app-x", "", false}, // non-numeric idx
		{"fred-not-a-uuid-app-0", "", false},
		{"other-" + u + "-app-0", "", false},
		{"fred-retained-" + u + "-app-0", "", false},
	}
	for _, c := range cases {
		got, ok := leaseUUIDFromVolumeName(c.in)
		assert.Equal(t, c.ok, ok, c.in)
		assert.Equal(t, c.want, got, c.in)
	}
}

func eng523RestoringRecord(orig, newLease string) shared.RetentionEntry {
	return shared.RetentionEntry{
		OriginalLeaseUUID:   orig,
		NewLeaseUUID:        newLease,
		Tenant:              "tenant-a",
		ProviderUUID:        "22222222-2222-4222-8222-222222222222",
		Status:              shared.RetentionStatusRestoring,
		Generation:          1,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		StackManifest:       restoreStackManifest(), // realistic, parseable payload (not a nil "null" marshal)
		RetainedVolumeNames: []string{"fred-retained-" + orig + "-app-0"},
	}
}

func projectReadyRestoredLease(b *Backend, entry shared.RetentionEntry) {
	b.provisionsMu.Lock()
	defer b.provisionsMu.Unlock()
	b.provisions[entry.NewLeaseUUID] = &provision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID:            entry.NewLeaseUUID,
			Tenant:               entry.Tenant,
			ProviderUUID:         entry.ProviderUUID,
			SKU:                  entry.DestinationItems[0].SKU,
			Status:               backend.ProvisionStatusReady,
			Quantity:             entry.DestinationItems[0].Quantity,
			CreatedAt:            time.Now(),
			FailCount:            0,
			LastError:            "",
			Reason:               "",
			Message:              "",
			CallbackURL:          entry.DestinationCallbackURL,
			LifecycleCallbackURL: entry.DestinationLifecycleCallbackURL,
			ActiveReleaseVersion: 0,
			ActiveOperationID:    entry.DestinationOperationID,
			Items:                slices.Clone(entry.DestinationItems),
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(entry.DestinationResourceProfiles),
			ContainerIDs:         nil,
			StackManifest:        entry.StackManifest,
			ServiceContainers:    nil,
		},
	}
}

// ENG-523: on a successful restore whose release Append FAILS, finalizeRestoredLease
// must NOT delete the retention record. The restoring record is the adopted volume's
// exact ownership/finalizer authority and reconcileRestoring finalizes it once the lease
// is Ready. Dropping it would leave the lease with neither a release nor a retention
// record and make its data impossible to classify safely.
func TestFinalizeRestoredLease_KeepsFinalizerWhenReleaseAppendFails(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000001"
	newLease := "0192f1a0-2222-7abc-8def-000000000002"

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)

	// A release store whose Append will fail: open then Close so db.Update returns
	// ErrDatabaseNotOpen — a realistic transient bbolt write failure.
	relStore, err := newBoundReleaseStoreForTest(t, shared.ReleaseStoreConfig{DBPath: filepath.Join(t.TempDir(), "rel.db")})
	require.NoError(t, err)
	require.NoError(t, relStore.Close())
	b.releaseStore = relStore

	e := eng523RestoringRecord(orig, newLease)
	e = *putRestoringRetention(t, rs, e)
	projectReadyRestoredLease(b, e)

	before := testutil.ToFloat64(restoreFinalizerPendingTotal)
	b.finalizeRestoredLease(t.Context(), newLease, &e, e.Items, nil, slog.Default())

	got, err := rs.Get(orig)
	require.NoError(t, err)
	require.NotNil(t, got,
		"ENG-523: retention record (the adopted volume's finalizer) must NOT be deleted when the release Append failed")
	assert.Equal(t, shared.RetentionStatusRestoring, got.Status)
	assert.Equal(t, before+1, testutil.ToFloat64(restoreFinalizerPendingTotal),
		"a kept-pending finalizer must be counted for observability (ENG-523)")
}

// ENG-523 (reaper half): a lingering restoring record — the exact state a restore
// leaves when its release Append failed and finalizeRestoredLease kept the record as
// the adopted volume's finalizer — must claim the adopted new-lease canonical
// volume even with NO active release. Together with
// TestFinalizeRestoredLease_KeepsFinalizerWhenReleaseAppendFails this closes the
// ENG-523 window end-to-end: the record is kept and exact destroy paths honor it.
func TestFinalizeRestoredLease_DropsFinalizerWhenReleaseRecorded(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000003"
	newLease := "0192f1a0-2222-7abc-8def-000000000004"

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)

	relStore := attachReleaseStore(t, b)

	e := eng523RestoringRecord(orig, newLease)
	e = *putRestoringRetention(t, rs, e)
	projectReadyRestoredLease(b, e)
	recordRestoreOperationOutcome(t, b, e, backend.CallbackStatusSuccess)

	require.NoError(t, b.finalizeRestoredLeaseStrict(t.Context(), newLease, &e, e.Items))

	got, err := rs.Get(orig)
	require.NoError(t, err)
	assert.Nil(t, got, "retention record must be dropped once the release is durably recorded")

	rel, err := relStore.LatestActive(newLease)
	require.NoError(t, err)
	require.NotNil(t, rel, "the new lease must have an active release recorded")
}

// ENG-523 (Copilot #174): reconcileRestoring is the retry path for the finalizer. When
// the new lease is Ready but no active release is durable yet (finalizeRestoredLease
// kept the record because Append failed) and the release store is STILL failing, it
// must KEEP the record restoring — a bare Delete here would re-open the ENG-523 window
// (no release + no record → reapable on the next boot).
func TestReconcileRestoring_ReadyButReleaseUnrecordable_KeepsFinalizer(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000008"
	newLease := "0192f1a0-2222-7abc-8def-000000000009"
	e := eng523RestoringRecord(orig, newLease)

	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		newLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: newLease, Tenant: "tenant-a",
			ProviderUUID: "22222222-2222-4222-8222-222222222222",
			Status:       backend.ProvisionStatusReady,
			Items:        e.Items, StackManifest: e.StackManifest,
		}},
	})
	rs := attachRetentionStore(t, b)

	relStore, err := newBoundReleaseStoreForTest(t, shared.ReleaseStoreConfig{DBPath: filepath.Join(t.TempDir(), "rel.db")})
	require.NoError(t, err)
	require.NoError(t, relStore.Close()) // Append + LatestActive fail
	b.releaseStore = relStore

	e = *putRestoringRetention(t, rs, e)
	projectReadyRestoredLease(b, e)
	recordRestoreOperationOutcome(t, b, e, backend.CallbackStatusSuccess)

	b.reconcileRestoring(context.Background(), e)

	got, err := rs.Get(orig)
	require.NoError(t, err)
	require.NotNil(t, got,
		"reconcileRestoring must keep the finalizer when the release can't be durably recorded (ENG-523)")
	assert.Equal(t, shared.RetentionStatusRestoring, got.Status)
}

// Companion: when the release store works, reconcileRestoring's Ready path RECORDS the
// missing release (self-heal) and only then drops the finalizer.
func TestReconcileRestoring_ReadyRecordsMissingReleaseThenDropsFinalizer(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-00000000000a"
	newLease := "0192f1a0-2222-7abc-8def-00000000000b"
	e := eng523RestoringRecord(orig, newLease)

	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		newLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: newLease, Status: backend.ProvisionStatusReady,
			Items: e.Items, StackManifest: e.StackManifest,
		}},
	})
	rs := attachRetentionStore(t, b)
	relStore := attachReleaseStore(t, b) // working, empty: no active release yet (Append had failed)

	e = *putRestoringRetention(t, rs, e)
	projectReadyRestoredLease(b, e)
	recordRestoreOperationOutcome(t, b, e, backend.CallbackStatusSuccess)

	b.reconcileRestoring(context.Background(), e)

	got, err := rs.Get(orig)
	require.NoError(t, err)
	assert.Nil(t, got, "finalizer dropped once the release is recorded")

	rel, err := relStore.LatestActive(newLease)
	require.NoError(t, err)
	require.NotNil(t, rel, "reconcileRestoring must record the missing active release before dropping the finalizer")
}

// finalizeRestoredLease is idempotent: when an active release already exists (doRestore
// recorded it but its record Delete failed), the retry drops the record WITHOUT
// appending a duplicate active release.
func TestFinalizeRestoredLease_IdempotentWhenReleaseAlreadyRecorded(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-00000000000c"
	newLease := "0192f1a0-2222-7abc-8def-00000000000d"

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	relStore := attachReleaseStore(t, b)
	e := eng523RestoringRecord(orig, newLease)
	e = *putRestoringRetention(t, rs, e)
	projectReadyRestoredLease(b, e)
	recordRestoreOperationOutcome(t, b, e, backend.CallbackStatusSuccess)

	require.NoError(t, b.finalizeRestoredLeaseStrict(t.Context(), newLease, &e, e.Items))

	got, err := rs.Get(orig)
	require.NoError(t, err)
	assert.Nil(t, got, "record dropped once the release is durable")

	releases, err := relStore.List(newLease)
	require.NoError(t, err)
	assert.Len(t, releases, 1, "must not append a duplicate active release (idempotent)")
}

// A live generation that diverges from the restore's durable destination authority
// must fail closed. Update is now excluded while this finalizer exists, so recovery
// may never bless mutable live state as if it were the committed restore generation.
func TestReconcileRestoring_RejectsLiveManifestOutsideDurableAuthority(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-00000000000e"
	newLease := "0192f1a0-2222-7abc-8def-00000000000f"

	// The lease was Updated after the restore: its live provision carries a NEW manifest
	// (redis:7), distinct from the retention record's frozen restore manifest (nginx:latest).
	updated := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		manifest.DefaultServiceName: {Image: "redis:7"},
	}}
	liveItems := []backend.LeaseItem{{
		SKU: "docker-large", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		newLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: newLease, Tenant: "tenant-a",
			ProviderUUID:  "22222222-2222-4222-8222-222222222222",
			Status:        backend.ProvisionStatusReady,
			StackManifest: updated, Items: liveItems,
			ResourceProfiles: testResourceProfiles(t, liveItems),
		}},
	})
	rs := attachRetentionStore(t, b)
	relStore := attachReleaseStore(t, b)

	e := eng523RestoringRecord(orig, newLease) // e.StackManifest = restoreStackManifest() = nginx:latest (frozen)
	e = *putRestoringRetention(t, rs, e)
	recordRestoreOperationOutcome(t, b, e, backend.CallbackStatusSuccess)

	err := b.reconcileRestoring(context.Background(), e)
	require.ErrorContains(t, err, "do not match durable destination authority")

	rel, err := relStore.LatestActive(newLease)
	require.NoError(t, err)
	require.NotNil(t, rel, "the exact committed restore release remains authoritative")
	frozenManifest, err := json.Marshal(e.StackManifest)
	require.NoError(t, err)
	assert.JSONEq(t, string(frozenManifest), string(rel.Manifest),
		"reconciliation must not replace the exact committed restore release with divergent live state")
	finalizer, err := rs.Get(orig)
	require.NoError(t, err)
	require.NotNil(t, finalizer, "durable restore authority must remain retryable")
}

// A restore-claimed volume cannot collide with a reaping tombstone. Creating the tombstone
// completes the destination lease's close and installs its permanent mutation head; restore
// admission consumes an operation claim for that same destination and is therefore rejected
// before it can turn the source row Restoring. This construction-time fence replaces the old
// downstream tests that manufactured both mutually exclusive states and then checked every
// cleanup branch separately.
func TestRestoreAdmission_RejectsReapingDestinationByConstruction(t *testing.T) {
	sourceLease := "0192f1a0-1111-7abc-8def-000000000010"
	destinationLease := "0192f1a0-2222-7abc-8def-000000000011"
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	putActiveRetentionViaSettlement(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: sourceLease,
		Tenant:            "tenant-a",
		Status:            shared.RetentionStatusActive,
		Items:             items,
	})
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: destinationLease,
		Tenant:            "tenant-a",
		Status:            shared.RetentionStatusReaping,
		Items:             items,
	}))

	operationID, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
	claimed, err := claimRetentionForTest(
		t, rs, sourceLease, destinationLease, 0, items, testResourceProfiles(t, items),
		operationID, callbackURL, lifecycleCallbackURL,
	)
	require.ErrorIs(t, err, shared.ErrOperationIntentConflict)
	assert.ErrorContains(t, err, "lease is permanently closed")
	assert.Nil(t, claimed)

	source, err := rs.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, source)
	assert.Equal(t, shared.RetentionStatusActive, source.Status,
		"rejected admission must not acquire or mutate source retention authority")
	destination, err := rs.Get(destinationLease)
	require.NoError(t, err)
	require.NotNil(t, destination)
	assert.Equal(t, shared.RetentionStatusReaping, destination.Status)
}

// Fail-safe error branch: an unreadable retention store means ownership cannot be proven
// for ANY name, so nothing is destroyed. Over-keeping is recoverable;
// over-destroying is not.
func TestDestroyReapingVolumes_ClaimLookupError_DestroysNothing(t *testing.T) {
	lease := "0192f1a0-2222-7abc-8def-000000000015"
	names := []string{
		canonicalVolumeName(lease, "app", 0),
		canonicalVolumeName(lease, "app", 1),
	}

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: lease,
		Tenant:            "tenant-a",
		Status:            shared.RetentionStatusReaping,
		Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
	}))
	proof := reapingProofForTest(t, rs, lease)
	require.NoError(t, rs.Close()) // every retention read now fails

	b.volumes = &mockVolumeManager{
		// The volumes ARE on disk and enumerable; it is the OWNERSHIP table that cannot be
		// read. Keeping the enumeration healthy is what makes this test still exercise the
		// claim-unreadable arm specifically, rather than the new can't-enumerate arm.
		ListFn: func() ([]string, error) { return names, nil },
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("must not destroy %q when the claim set is unreadable — it may be another lease's data", id)
			return nil
		},
	}
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	skipBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable))

	ok := b.destroyReapingVolumes(context.Background(), b.newManagedVolumeIndex(), proof)

	assert.False(t, ok, "nothing was destroyed, so the record cannot be dropped")
	assert.Equal(t, skipBefore+1, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable)))
	assert.Equal(t, leakBefore, testutil.ToFloat64(retentionLeakedTotal),
		"a fail-safe bailout abandons nothing; the record still counts the footprint")
}

// No-regression control: the ordinary reap. An evicted/expired record carries only the
// fred-retained-* names PutActiveMerged wrote, no restore is in flight, and the record is
// destroyed and dropped without incrementing an uncertainty signal.
func TestDestroyReapingVolumes_NormalReapingRecordStillFullyReaped(t *testing.T) {
	lease := "0192f1a0-3333-7abc-8def-000000000016"
	names := []string{"fred-retained-" + lease + "-app-0", "fred-retained-" + lease + "-app-1"}

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID:   lease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusReaping,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
		RetainedVolumeNames: names,
	}))

	vs := newVolumeSet(names...)
	b.volumes = vs.manager()
	unreadableBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable))

	assert.True(t, b.destroyReapingVolumes(context.Background(), b.newManagedVolumeIndex(), reapingProofForTest(t, b.retentionStore, lease)))
	assert.ElementsMatch(t, names, vs.names())

	rec, err := rs.Get(lease)
	require.NoError(t, err)
	assert.Nil(t, rec, "a fully reaped record is deleted")
	assert.Equal(t, unreadableBefore, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable)))
}

// The claim set is keyed on canonical fred-{lease}-* names (retainedToNewCanonical never
// emits a fred-retained-* name), so an all-retained name list cannot match one and the
// store read is skipped entirely. This keeps the guard off the hot reap path — notably
// evictOldest's up-to-32 records inside a synchronous close. Pinned by closing the store:
// without the pre-filter this would bail at claim_unreadable and destroy nothing.
func TestDestroyReapingVolumes_RetainedOnlyNames_SkipTheClaimLookup(t *testing.T) {
	lease := "0192f1a0-3333-7abc-8def-000000000017"
	names := []string{"fred-retained-" + lease + "-app-0"}

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID:   lease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusReaping,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		RetainedVolumeNames: names,
	}))
	proof := reapingProofForTest(t, rs, lease)
	require.NoError(t, rs.Close())

	vs := newVolumeSet(names...)
	b.volumes = vs.manager()
	unreadableBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable))

	b.destroyReapingVolumes(context.Background(), b.newManagedVolumeIndex(), proof)

	assert.ElementsMatch(t, names, vs.names(),
		"no fred-{lease}-* name in the list ⇒ no claim can match ⇒ no store read, destroy proceeds")
	assert.Equal(t, unreadableBefore, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable)),
		"the pre-filter must not be reached by way of the fail-safe bailout")
}

// TestDestroyReapingVolumes_RefusesAVolumeALiveProvisionHolds covers a second way a
// tombstone can name data it does not own, which the per-record claim check could not see
// (ENG-658). A give-up leaves a tombstone naming fred-{lease}-* and deletes the provision;
// the lease is still ACTIVE on chain, so the reconciler re-provisions it and a fresh
// volume appears under the very name the tombstone carries. The finalizer now asks the
// owner table — which knows about live provisions, not just restores — and refuses.
//
// This is the ENG-505 class reached through the finalizer instead of the orphan reaper.
func TestDestroyReapingVolumes_RefusesAVolumeALiveProvisionHolds(t *testing.T) {
	lease := "0192f1a0-3333-7abc-8def-000000000012"
	live := canonicalVolumeName(lease, "app", 0)
	staleLeak := canonicalVolumeName(lease, "app", 1)

	// The lease is tracked again: the give-up deleted the provision, but the chain still
	// says ACTIVE, so the reconciler re-provisioned it.
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID:   lease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusReaping,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
		RetainedVolumeNames: []string{live, staleLeak},
	}))

	var destroyed []string
	b.volumes = &mockVolumeManager{
		// Both names are on disk under this lease: the re-provisioned volume and the stale
		// leak. Derivation finds the same pair the tombstone used to name — which is the
		// point, since the re-provisioned volume is precisely what must NOT be destroyed.
		ListFn:    func() ([]string, error) { return []string{live, staleLeak}, nil },
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}
	ownerBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipOwnerClaimed))

	ok := b.destroyReapingVolumes(context.Background(), b.newManagedVolumeIndex(), reapingProofForTest(t, b.retentionStore, lease))

	assert.False(t, ok, "a refused name means the record is not fully reaped and must be kept")
	assert.Equal(t, ownerBefore+1, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipOwnerClaimed)))
	assert.NotContains(t, destroyed, live,
		"the re-provisioned lease is running on this volume; a stale tombstone must not reap it (ENG-505 class)")
	assert.Equal(t, []string{staleLeak}, destroyed,
		"the genuinely abandoned name is still reaped — the refusal is per name")

	tomb, err := rs.Get(lease)
	require.NoError(t, err)
	require.NotNil(t, tomb, "the tombstone survives so the remaining leak stays counted and retryable")
	assert.Equal(t, shared.RetentionStatusReaping, tomb.Status)
}

// TestCleanupOrphanedVolumes_LiveProvisionProtectedWithoutAReleaseStore pins that a
// tracked lease's volume is protected by the OWNER TABLE alone, with no release record to
// fall back on. The two gates are independent by design and only one of them is a claim:
// leaseHasActiveRelease is a per-name release-store probe kept out of the table
// deliberately: a release row is lifecycle authority, not a physical-volume claim.
// This is also what keeps the release probe off the hot path — a healthy node's volumes
// are all claimed, so none of them reach it (ENG-658).
func TestOrphanReconcile_UnmountedRootDoesNotPruneLiveRecords(t *testing.T) {
	root := t.TempDir()
	vol := "fred-u1-app-0"
	require.NoError(t, os.MkdirAll(filepath.Join(root, vol), 0o755))

	// btrfs is used only because its List is the plain enumeration with no external tooling;
	// the guard under test is shared by all three managers.
	mgr := &btrfsVolumeManager{dataPath: root, logger: slog.Default()}

	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.VolumeDataPath = root // exists → the G2 gate passes, exactly as it would post-unmount
	b.cfg.RetentionOrphanConfirmations = 1
	b.volumes = mgr
	rs := attachRetentionStore(t, b)
	bindRetentionOrphanPrunerForTest(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID:   "u1",
		Tenant:              "t1",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{retainedName(vol)},
		CreatedAt:           time.Now(),
	}))

	// A healthy pass first: the volume is present, so nothing is orphaned and the manager
	// learns which filesystem this root lives on.
	pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background())
	require.NoError(t, err)
	require.Zero(t, pruned)
	require.NotNil(t, getRetention(t, rs, "u1"))

	// The unmount. The directory survives and is empty, and it is now served by a different
	// device than the one the manager recorded.
	require.NoError(t, os.RemoveAll(filepath.Join(root, vol)))
	mgr.rootWatch.mu.Lock()
	mgr.rootWatch.dev++
	mgr.rootWatch.mu.Unlock()

	skipBefore := testutil.ToFloat64(retentionOrphanSkipsTotal.WithLabelValues(orphanSkipListError))
	prunedBefore := testutil.ToFloat64(retentionOrphansPrunedTotal)

	_, err = b.reconcileOrphanedRetentionsUsing(context.Background())

	require.Error(t, err, "an unvouchable emptiness must abort the pass, not be read as 'all orphaned'")
	got := getRetention(t, rs, "u1")
	require.NotNil(t, got, "ENG-687: the live retention record must survive an unmounted volume root")
	assert.Equal(t, shared.RetentionStatusActive, got.Status)
	assert.Equal(t, prunedBefore, testutil.ToFloat64(retentionOrphansPrunedTotal), "nothing may be pruned")
	assert.Equal(t, skipBefore+1, testutil.ToFloat64(retentionOrphanSkipsTotal.WithLabelValues(orphanSkipListError)))
}

// TestDestroyReapingVolumes_ReclaimsTheWholeNamespace_NotJustTheRecordedNames pins the
// behavioural WIDENING that deriving the destroy set introduces, which is the most
// consequential change in ENG-676 and was previously asserted only in prose.
//
// A tombstone used to destroy exactly the names it carried. Derived, it destroys everything
// in the lease's namespace that nothing claims — so a volume the record never named, such as
// a writable-path-only volume whose reclaim failed before the record was written, is now
// reclaimed rather than surviving indefinitely. The exact tombstone scopes the
// "destroy only what nothing claims" rule to one lease; there is no global inference-based
// destroyer. That is why the record can afford to carry no names at all.
//
// The widening is bounded by the lease's own prefixes and by the ownership table; the
// sibling tests in this file cover a claimed name being refused.
func TestDestroyReapingVolumes_ReclaimsTheWholeNamespace_NotJustTheRecordedNames(t *testing.T) {
	lease := "0192f1a0-5555-7abc-8def-000000000201"
	recorded := retainedName(canonicalVolumeName(lease, "app", 0))
	unrecorded := canonicalVolumeName(lease, "app", 1) // never named by the record
	otherLease := canonicalVolumeName("0192f1a0-6666-7abc-8def-000000000202", "app", 0)

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID:   lease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusReaping,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
		RetainedVolumeNames: []string{recorded}, // deliberately narrower than what is on disk
	}))

	vs := newVolumeSet(recorded, unrecorded, otherLease)
	b.volumes = vs.manager()

	assert.True(t, b.destroyReapingVolumes(context.Background(), b.newManagedVolumeIndex(), reapingProofForTest(t, b.retentionStore, lease)),
		"the whole namespace is gone, so the record has nothing left to account for")
	assert.ElementsMatch(t, []string{recorded, unrecorded}, vs.names(),
		"both namespaces of THIS lease are reclaimed, including the volume the record never named")
	assert.NotContains(t, vs.names(), otherLease,
		"the widening is bounded by the lease's own prefixes — another lease is never in scope")

	got, err := rs.Get(lease)
	require.NoError(t, err)
	assert.Nil(t, got, "record dropped once the footprint is confirmed gone")
}

// TestDestroyReapingVolumes_MountVanishesMidDestroy_KeepsTheRecord closes the last window in
// the ENG-687 family, and it is the one a guard on the ENUMERATION cannot see.
//
// Every destroy is an os.RemoveAll that deliberately treats an already-absent path as done,
// so if the volume root goes away AFTER the footprint is enumerated, each name is "removed"
// from a filesystem that is no longer there and the batch reports complete success. Dropping
// the record on that report loses the only accounting for volumes that come back with the
// mount — the same chain as pruning live records, reached through the destroy path.
//
// The record is therefore dropped only on a confirming re-read, never on the destroys'
// self-report.
func TestDestroyReapingVolumes_MountVanishesMidDestroy_KeepsTheRecord(t *testing.T) {
	lease := "0192f1a0-7777-7abc-8def-000000000301"
	vol := retainedName(canonicalVolumeName(lease, "app", 0))

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: lease, Tenant: "tenant-a", Status: shared.RetentionStatusReaping,
		Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		CreatedAt: time.Now(),
	}))

	// The mount disappears between the enumeration and the destroy: List reports the volume,
	// then every subsequent read of the root fails. Destroy still returns nil, exactly as
	// RemoveAll does against a path that is no longer there.
	var listed bool
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if listed {
				return nil, errors.New("read volume data directory: no such file or directory")
			}
			listed = true
			return []string{vol}, nil
		},
		DestroyFn: func(_ context.Context, _ string) error { return nil },
	}
	skipBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable))

	assert.False(t, b.destroyReapingVolumes(context.Background(), b.newManagedVolumeIndex(), reapingProofForTest(t, b.retentionStore, lease)),
		"a destroy that cannot be confirmed must not drop the record")

	got, err := rs.Get(lease)
	require.NoError(t, err)
	assert.NotNil(t, got, "ENG-687: the record survives so the footprint stays counted and retried")
	assert.Equal(t, skipBefore+1,
		testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable)))
}
