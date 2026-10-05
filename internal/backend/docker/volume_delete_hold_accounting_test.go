package docker

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A hold that becomes residual counts toward admission at once, but the
// readers that infer completion without a destroy of their own (proof
// listings, the lock-free precheck, the close-wait predicates) treat it like
// the removal phase until an admission publication acknowledges it (#250
// review, P1). Destroy's own answer is not gated: it reaches its caller only
// through afterVolumeDestroy, which publishes first.
func TestXFSUncountedResidualHoldStaysVisibleToSettlementReaders(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	mgr.activeIDs[stage.projID] = stage.volumeID.value()
	mgr.volumeToID[stage.volumeID.value()] = stage.projID
	installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) printf '#%d 9 0 20480 0\n' ;;
  *"report -p -i -n -N"*) printf '#%d 1 0 0 0\n' ;;
esac`, stage.projID, stage.projID))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	name := stage.volumeID

	require.NoError(t, mgr.Destroy(ctx, name.value()), "Destroy's own answer reaches its caller through a publication")
	hold := heldForTest(t, mgr, name.value())
	require.Equal(t, holdPhaseResidual, hold.phase)
	require.False(t, hold.callerSettled)
	require.NotZero(t, hold.residualSeq)

	listed, err := mgr.ListForProof(t.Context())
	require.NoError(t, err)
	assert.Contains(t, listed, name.value(), "no reader may infer completion from an uncounted residual name's absence")
	verdict, precheckErr := mgr.PrecheckDestroy(name)
	assert.Equal(t, destroyPrecheckHeld, verdict, "a lock-free answer is not followed by a publication of its own")
	assert.ErrorIs(t, precheckErr, ErrVolumeDeleteHeld)
	require.NoError(t, mgr.AttestManagedVolume(t.Context(), name), "a listed name must attest")
	snapshot := mgr.VolumeDeleteHolds()
	assert.True(t, snapshot.callerHeld(name.value()))
	assert.Equal(t, closeSlotHeld, snapshot.slotState(name.value()))
	account := snapshot.admissionAccount()
	assert.Equal(t, int64(20), account.residualMB, "the footprint counts from the moment the hold is residual")
	require.Equal(t, []residualAccountingToken{{volume: name.value(), seq: hold.residualSeq}}, account.unacknowledged)

	assert.Empty(t, mgr.AcknowledgeResidualAccounting(
		[]residualAccountingToken{{volume: name.value(), seq: hold.residualSeq + 1}}))
	assert.False(t, heldForTest(t, mgr, name.value()).callerSettled, "a token for another entry acknowledges nothing")
	assert.Equal(t, []managedVolumeName{name}, mgr.AcknowledgeResidualAccounting(account.unacknowledged))
	assert.True(t, heldForTest(t, mgr, name.value()).callerSettled)
	assert.Empty(t, mgr.AcknowledgeResidualAccounting(account.unacknowledged),
		"a hold is acknowledged once; only the first acknowledgment releases its caller")

	listed, err = mgr.ListForProof(t.Context())
	require.NoError(t, err)
	assert.NotContains(t, listed, name.value(), "a counted residual name is settled")
	verdict, precheckErr = mgr.PrecheckDestroy(name)
	require.NoError(t, precheckErr)
	assert.NotEqual(t, destroyPrecheckHeld, verdict,
		"no longer held: the precheck answers from the final path, or leaves it to the locked destroy")
	snapshot = mgr.VolumeDeleteHolds()
	assert.Equal(t, closeSlotDone, snapshot.slotState(name.value()))
	assert.Empty(t, snapshot.admissionAccount().unacknowledged)
	assert.Equal(t, int64(20), snapshot.admissionAccount().residualMB, "a settled residual footprint stays counted")
	require.NoError(t, mgr.Destroy(ctx, name.value()))
	assert.True(t, heldForTest(t, mgr, name.value()).callerSettled, "a hold that stays residual stays acknowledged")
}

// setHoldPhaseLocked starts every entry into the residual phase
// unacknowledged under a fresh sequence, keeps both while the hold stays
// residual, and drops both when it leaves; an acknowledgment matches only
// the exact entry it was counted for.
func TestSetHoldPhaseLockedStartsEveryResidualEntryUnacknowledged(t *testing.T) {
	mgr := newXfsManagerForTest(t.TempDir())
	residual, ok := residualHoldPhase(residualFootprintMB{mb: 4, fromParsedRow: true})
	require.True(t, ok)
	hold := &xfsDeleteHold{stage: mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)}
	name := hold.stage.volumeID.value()

	mgr.mu.Lock()
	mgr.setHoldPhaseLocked(hold, residual)
	first := hold.residualSeq
	mgr.mu.Unlock()
	require.NotZero(t, first)
	require.False(t, hold.settlesCaller())

	mgr.deleteHolds = map[string]*xfsDeleteHold{name: hold}
	mgr.AcknowledgeResidualAccounting([]residualAccountingToken{{volume: name, seq: first}})
	require.True(t, hold.settlesCaller())

	mgr.mu.Lock()
	mgr.setHoldPhaseLocked(hold, residual)
	assert.Equal(t, first, hold.residualSeq, "staying residual keeps the entry")
	assert.True(t, hold.settlesCaller(), "and its acknowledgment")
	mgr.setHoldPhaseLocked(hold, removalHoldPhase())
	assert.Zero(t, hold.residualSeq)
	assert.False(t, hold.residualAcknowledged, "leaving the phase drops the acknowledgment")
	mgr.setHoldPhaseLocked(hold, residual)
	second := hold.residualSeq
	mgr.mu.Unlock()
	assert.Greater(t, second, first, "a new entry gets a fresh sequence")
	assert.False(t, hold.settlesCaller(), "a new entry starts unacknowledged")

	assert.Empty(t, mgr.AcknowledgeResidualAccounting([]residualAccountingToken{{volume: name, seq: first}}),
		"a token counted for the earlier entry acknowledges nothing")
	assert.False(t, hold.settlesCaller())
	mgr.AcknowledgeResidualAccounting([]residualAccountingToken{{volume: name, seq: second}})
	assert.True(t, hold.settlesCaller())
}

// The publication acknowledges exactly the holds its own snapshot counted,
// only once the pool accepted that total, and owes their leases a close
// resume; a failed publication acknowledges nothing (#250 review).
func TestPublicationAcknowledgesOnlyWhatItCounted(t *testing.T) {
	const (
		leaseA = "550e8400-e29b-41d4-a716-446655440331"
		leaseB = "550e8400-e29b-41d4-a716-446655440332"
	)
	nameA := canonicalVolumeName(leaseA, "app", 0)
	nameB := canonicalVolumeName(leaseB, "app", 0)
	const footprintMB = 32
	b := newBackendForTest(&mockDockerClient{}, nil)
	attachRetentionStore(t, b)
	var acknowledged [][]residualAccountingToken
	b.volumes = &mockVolumeManager{
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot {
			if b.pool.Stats().RetainedDiskMB >= footprintMB {
				// B became residual after the publication's snapshot: the
				// published total does not count it.
				return uncountedResidualDeletes(footprintMB, nameA, nameB)
			}
			return uncountedResidualDeletes(footprintMB, nameA)
		},
		AcknowledgeResidualAccountingFn: func(tokens []residualAccountingToken) []managedVolumeName {
			acknowledged = append(acknowledged, tokens)
			var names []managedVolumeName
			for _, token := range tokens {
				parsed, err := parseManagedVolumeName(token.volume)
				require.NoError(t, err)
				names = append(names, parsed)
			}
			return names
		},
	}

	b.retentionAccountingMu.Lock()
	b.retentionStoreDiskMB, b.retentionStoreDiskKnown = 0, true
	require.Error(t, b.publishRetainedDiskLocked(math.MaxInt64), "an overflowing total is never published")
	b.retentionAccountingMu.Unlock()
	assert.Empty(t, acknowledged, "a failed publication acknowledges nothing")
	assert.Empty(t, b.holdExecutor.takeResumes())

	// The snapshot includes B as soon as the pool holds the new total.
	require.Zero(t, b.pool.Stats().RetainedDiskMB)
	require.NoError(t, b.refreshHeldResidualAccounting())
	require.Equal(t, int64(footprintMB), b.pool.Stats().RetainedDiskMB)
	require.Len(t, acknowledged, 1)
	assert.Equal(t, []residualAccountingToken{{volume: nameA, seq: 1}}, acknowledged[0],
		"only the hold the published total counted is acknowledged")
	assert.Equal(t, []string{leaseA}, b.holdExecutor.takeResumes(), "its lease is owed a close resume")
}

// A residual hold that a publication other than its own attempt's
// acknowledges (here, a plain refresh) still resumes the close that waited on
// it: the executor's next iteration takes the owed resume although no attempt
// saw the release (#250 review).
func TestAcknowledgmentOutsideAnAttemptResumesTheClose(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseDeprovisionLease(t, b, stores)
	name := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	var counted atomic.Bool
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if counted.Load() {
				return nil, nil
			}
			return []string{name}, nil
		},
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot {
			snapshot := uncountedResidualDeletes(16, name)
			hold := snapshot.holds[name]
			hold.nextAttempt = time.Now().Add(time.Hour) // not due: no executor attempt
			hold.callerSettled = counted.Load()
			snapshot.holds[name] = hold
			return snapshot
		},
		PrecheckDestroyFn: func(managedVolumeName) (destroyPrecheckVerdict, error) {
			if counted.Load() {
				return destroyPrecheckGone, nil
			}
			return destroyPrecheckHeld, heldDeleteErr(name)
		},
		AcknowledgeResidualAccountingFn: func(tokens []residualAccountingToken) []managedVolumeName {
			if len(tokens) == 1 && counted.CompareAndSwap(false, true) {
				parsed, err := parseManagedVolumeName(name)
				require.NoError(t, err)
				return []managedVolumeName{parsed}
			}
			return nil
		},
	}
	installTestStorageMutationAdapters(b)
	b.retentionAccountingMu.Lock()
	b.retentionStoreDiskKnown = false // no publication yet
	b.retentionAccountingMu.Unlock()
	require.Error(t, b.doDeprovisionForTest(t, t.Context(), closeDeprovisionLeaseUUID))
	closePending := func() bool {
		_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
		require.NoError(t, err)
		return found
	}
	require.True(t, closePending(), "the uncounted residual hold keeps the close pending")

	require.NoError(t, b.refreshRetentionAccountingChecked())
	require.True(t, counted.Load(), "the refresh's publication acknowledged the hold")
	require.True(t, closePending())
	b.runVolumeDeleteHoldIteration()
	assert.False(t, closePending(), "the executor took the owed resume and completed the close")
	closeCloseRecoveryBackend(t, b, stores)
}

// A close whose own inline delete ends residual completes in that same pass.
// Destroy answers nil inside the close's Guard Step, as the manager contract
// says, and afterVolumeDestroy publishes and acknowledges the footprint before
// the close's own proof listing is read, so the close is attested destroyed
// rather than left pending (#250 fix review).
func TestCloseWhoseOwnDeleteEndsResidualCompletesInOnePass(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseDeprovisionLease(t, b, stores)
	name := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	const footprintMB = 48
	const (
		live = iota
		residualUncounted
		residualCounted
	)
	var state atomic.Int32
	var retainedAtAcknowledgment atomic.Int64
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if state.Load() == residualCounted {
				return nil, nil
			}
			return []string{name}, nil
		},
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot {
			switch state.Load() {
			case live:
				return volumeDeleteHoldSnapshot{}
			case residualUncounted:
				return uncountedResidualDeletes(footprintMB, name)
			default:
				return residualDeletes(footprintMB, name)
			}
		},
		PrecheckDestroyFn: func(managedVolumeName) (destroyPrecheckVerdict, error) {
			switch state.Load() {
			case live:
				return destroyPrecheckNeedsLock, nil
			case residualUncounted:
				return destroyPrecheckHeld, heldDeleteErr(name)
			default:
				return destroyPrecheckGone, nil
			}
		},
		DestroyFn: func(context.Context, string) error {
			state.CompareAndSwap(live, residualUncounted) // removed; only the project remains
			return nil
		},
		AcknowledgeResidualAccountingFn: func(tokens []residualAccountingToken) []managedVolumeName {
			if len(tokens) == 1 && state.CompareAndSwap(residualUncounted, residualCounted) {
				retainedAtAcknowledgment.Store(b.pool.Stats().RetainedDiskMB)
				parsed, err := parseManagedVolumeName(name)
				require.NoError(t, err)
				return []managedVolumeName{parsed}
			}
			return nil
		},
	}
	installTestStorageMutationAdapters(b)
	require.NoError(t, b.refreshRetentionAccountingChecked())
	base := b.pool.Stats().RetainedDiskMB

	require.NoError(t, b.doDeprovisionForTest(t, t.Context(), closeDeprovisionLeaseUUID),
		"the close is attested destroyed in one pass")
	_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, int32(residualCounted), state.Load())
	assert.Equal(t, base+footprintMB, retainedAtAcknowledgment.Load(),
		"acknowledged only once the published total counted the footprint")
	assert.Equal(t, base+footprintMB, b.pool.Stats().RetainedDiskMB)
	closeCloseRecoveryBackend(t, b, stores)
}

// Unsized holds lead every pass, since they withhold disk admission, but they
// never take more than every other dispatch while another hold is due (#250
// review, P2). Each class keeps its oldest-first order.
func TestInterleaveUnsizedAlternatesWithTheOtherPhases(t *testing.T) {
	now := time.Now()
	view := func(lease string, phase xfsDeleteHoldPhaseKind, age time.Duration) volumeDeleteHoldView {
		hold := dueHold(t, canonicalVolumeName(lease, "app", 0), now.Add(-age))
		hold.phase = phase
		return hold
	}
	u1 := view("550e8400-e29b-41d4-a716-446655440401", holdPhaseUnsized, 9*time.Minute)
	u2 := view("550e8400-e29b-41d4-a716-446655440402", holdPhaseUnsized, 8*time.Minute)
	u3 := view("550e8400-e29b-41d4-a716-446655440403", holdPhaseUnsized, 7*time.Minute)
	r1 := view("550e8400-e29b-41d4-a716-446655440404", holdPhaseRemoval, 3*time.Minute)
	r2 := view("550e8400-e29b-41d4-a716-446655440405", holdPhaseResidual, 2*time.Minute)
	names := func(holds []volumeDeleteHoldView) []string {
		out := make([]string, 0, len(holds))
		for _, hold := range holds {
			out = append(out, hold.volume.value())
		}
		return out
	}
	assert.Equal(t, names([]volumeDeleteHoldView{u1, r1, u2, r2, u3}),
		names(interleaveUnsized([]volumeDeleteHoldView{u1, u2, u3, r1, r2})))
	assert.Equal(t, names([]volumeDeleteHoldView{u1, u2}), names(interleaveUnsized([]volumeDeleteHoldView{u1, u2})),
		"with nothing else due, unsized holds take every dispatch")
	assert.Equal(t, names([]volumeDeleteHoldView{r1, r2}), names(interleaveUnsized([]volumeDeleteHoldView{r1, r2})))
	assert.Empty(t, interleaveUnsized(nil))
}

// Eight unsized holds whose attempts each spend their whole slice can fill a
// pass's budget. They still alternate with an ordinary removal hold, which is
// therefore attempted in the very first pass instead of never (#250 review,
// P2).
func TestVolumeDeleteHoldPassReachesARemovalHoldBehindManyUnsizedOnes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		holds := volumeDeleteHoldSnapshot{holds: map[string]volumeDeleteHoldView{}}
		for i := range 8 {
			name := canonicalVolumeName(fmt.Sprintf("550e8400-e29b-41d4-a716-4466554405%02d", i), "app", 0)
			hold := dueHold(t, name, now.Add(-time.Hour)) // older than the removal hold
			hold.phase = holdPhaseUnsized
			holds.holds[name] = hold
		}
		removal := canonicalVolumeName("550e8400-e29b-41d4-a716-446655440599", "app", 0)
		holds.holds[removal] = dueHold(t, removal, now.Add(-time.Minute))
		b := newBackendForTest(&mockDockerClient{}, nil)
		b.volumes = &mockVolumeManager{VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return holds }}

		var removalAttempted atomic.Bool
		retry := backgroundHeldVolumeDeleteRetry(func(ctx context.Context, name string) error {
			if name == removal {
				removalAttempted.Store(true)
			}
			<-ctx.Done() // every attempt spends its whole slice
			return ctx.Err()
		})
		ctx, cancel := context.WithTimeout(t.Context(), volumeDeleteHoldPassBudget)
		defer cancel()
		report := b.runVolumeDeleteHoldPass(ctx, now, retry)
		assert.True(t, removalAttempted.Load(), "the removal hold must not starve behind unsized holds")
		assert.Less(t, report.attempted, 9, "the unsized holds alone could fill this pass")
	})
}
