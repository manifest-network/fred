package shared

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

const windowTestStorageID = "550e8400-e29b-41d4-a716-446655440000"

// windowReceipt describes one settled receipt seeded into a lease's window.
type windowReceipt struct {
	kind          MaintenanceIntentKind
	status        backend.CallbackStatus
	effectStarted bool
	digest        string
}

// seedMaintenanceWindow writes the given receipts for the fixture lease with
// completion sequences after any already retained, each with its own target
// release version, and reserves one global slot per receipt, as live
// admission would have. Rows are written directly; journal validation checks
// the result.
func seedMaintenanceWindow(
	t *testing.T,
	callbacks *CallbackStore,
	source ReleaseClaim,
	target Release,
	receipts []windowReceipt,
) []maintenanceCompletionRecord {
	t.Helper()
	identity, ok := target.RuntimeIdentity()
	require.True(t, ok)
	records := make([]maintenanceCompletionRecord, 0, len(receipts))
	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		existing, err := listMaintenanceReceiptsTx(tx, source.LeaseUUID())
		if err != nil {
			return err
		}
		first := uint64(1)
		for _, record := range existing {
			first = max(first, record.CompletionSequence+1)
		}
		leaseBucket, err := tx.Bucket(callbackMaintenanceHistoryBucketName).
			CreateBucketIfNotExists([]byte(source.LeaseUUID()))
		if err != nil {
			return err
		}
		for index, receipt := range receipts {
			digest := receipt.digest
			if digest == "" {
				sum := sha256.Sum256([]byte(fmt.Sprintf("window-receipt-%d", index)))
				digest = encodeMaintenanceDigest(sum)
			}
			status := receipt.status
			if status == "" {
				status = backend.CallbackStatusSuccess
			}
			sequence := first + uint64(index)
			record := maintenanceCompletionRecord{
				Version: maintenanceCompletionRecordV1, MaintenanceID: newTestMaintenanceID(t),
				Kind: receipt.kind, LeaseUUID: source.LeaseUUID(), RequestDigest: digest,
				CompletionSequence: sequence, Backend: "docker-a",
				BackendStorageID: windowTestStorageID, Tenant: identity.Tenant(),
				ProviderUUID: identity.ProviderUUID(), Status: status,
				EffectStarted: receipt.effectStarted, TargetReleaseVersion: int(sequence) + 1,
				TargetReleaseDigest: strings.Repeat("a", 64), SettledAt: time.Now().UTC(),
			}
			if status == backend.CallbackStatusFailed {
				record.Error = "maintenance failed"
			}
			data, err := marshalMaintenanceCompletionRecord(record)
			if err != nil {
				return err
			}
			if err := leaseBucket.Put([]byte(record.MaintenanceID.String()), data); err != nil {
				return err
			}
			records = append(records, record)
		}
		heads := tx.Bucket(callbackLeaseMutationHeadBucketName)
		return heads.SetSequence(heads.Sequence() + uint64(len(receipts)))
	}))
	require.NoError(t, callbacks.Healthy())
	return records
}

func providerWindow(count int) []windowReceipt {
	receipts := make([]windowReceipt, count)
	for index := range receipts {
		receipts[index] = windowReceipt{kind: MaintenanceIntentRestart}
	}
	return receipts
}

// stampedMaintenanceSpec is a provider command carrying an admission stamp.
func stampedMaintenanceSpec(
	t *testing.T,
	callbacks *CallbackStore,
	id MaintenanceID,
	kind MaintenanceIntentKind,
	source ReleaseClaim,
	target Release,
	stamp time.Time,
) MaintenanceIntentCandidate {
	t.Helper()
	identity, ok := target.RuntimeIdentity()
	require.True(t, ok)
	var payload []byte
	if kind != MaintenanceIntentRestart {
		payload = target.Manifest
	}
	request, err := newMaintenanceRequestAuthority(
		callbacks, id, kind, source.LeaseUUID(), identity.LifecycleCallbackURL(), payload,
		"docker-a", callbackStorageID(t, windowTestStorageID),
	)
	require.NoError(t, err)
	request.admittedAt = stamp
	candidate, err := callbacks.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)
	return candidate
}

func windowStamp(offset time.Duration) time.Time {
	return time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC).Add(offset)
}

func lineageFor(t *testing.T, callbacks *CallbackStore, leaseUUID string) maintenanceLineage {
	t.Helper()
	var lineage maintenanceLineage
	require.NoError(t, callbacks.db.View(func(tx *bolt.Tx) error {
		var err error
		lineage, err = loadMaintenanceLineageTx(tx, leaseUUID)
		return err
	}))
	return lineage
}

func receiptPresent(t *testing.T, callbacks *CallbackStore, record maintenanceCompletionRecord) bool {
	t.Helper()
	var found bool
	require.NoError(t, callbacks.db.View(func(tx *bolt.Tx) error {
		var err error
		_, found, err = findMaintenanceReceiptTx(tx, record.LeaseUUID, record.MaintenanceID)
		return err
	}))
	return found
}

func reservations(t *testing.T, callbacks *CallbackStore) uint64 {
	t.Helper()
	var count uint64
	require.NoError(t, callbacks.db.View(func(tx *bolt.Tx) error {
		var err error
		count, err = callbackReceiptReservationCountTx(tx)
		return err
	}))
	return count
}

// TestStampedPublishEvictsTheOldestProviderReceipt pins the rolling window: a
// stamped publish into a full window removes exactly the oldest receipt,
// releases its reservation, and raises the admission high-water mark.
func TestStampedPublishEvictsTheOldestProviderReceipt(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "window-evicts-oldest")
	seeded := seedMaintenanceWindow(t, callbacks, source, target, providerWindow(maxProviderMaintenanceWindow))

	id := newTestMaintenanceID(t)
	admission, err := callbacks.BeginMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, id, MaintenanceIntentRestart, source, target, windowStamp(0)),
	)
	require.NoError(t, err)
	require.Equal(t, MaintenanceIntentAdmissionCreated, admission.Disposition())

	assert.False(t, receiptPresent(t, callbacks, seeded[0]), "the oldest receipt left the window")
	assert.True(t, receiptPresent(t, callbacks, seeded[1]))
	assert.Equal(t, uint64(maxProviderMaintenanceWindow), reservations(t, callbacks),
		"one reservation released for the evicted receipt, one taken for the head")
	lineage := lineageFor(t, callbacks, source.LeaseUUID())
	assert.True(t, lineage.HighWaterAdmittedAt.Equal(windowStamp(0)))
	assert.Equal(t, id, lineage.HighWaterID)
	assert.True(t, lineage.ProviderEvicted)
	assert.Equal(t, seeded[0].TargetReleaseVersion, lineage.EvictedThroughReleaseVersion)
	require.NoError(t, callbacks.Healthy())
	require.NoError(t, callbacks.CancelMaintenanceIntent(createdMaintenanceDispatch(t, admission)))
	require.NoError(t, callbacks.Healthy())
}

// TestCommandOlderThanTheHighWaterIsExpired pins the replay rule that makes
// eviction safe: a provider command with no head and no receipt is new work
// only past the lease's high-water mark.
func TestCommandOlderThanTheHighWaterIsExpired(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "window-expired")
	seedMaintenanceWindow(t, callbacks, source, target, providerWindow(maxProviderMaintenanceWindow))
	newest := newTestMaintenanceID(t)
	admission, err := callbacks.BeginMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, newest, MaintenanceIntentRestart, source, target, windowStamp(time.Hour)),
	)
	require.NoError(t, err)
	require.NoError(t, callbacks.CancelMaintenanceIntent(createdMaintenanceDispatch(t, admission)))

	probe := func(id MaintenanceID, stamp time.Time) error {
		_, err := callbacks.ProbeMaintenanceIntent(
			stampedMaintenanceSpec(t, callbacks, id, MaintenanceIntentRestart, source, target, stamp).request,
		)
		return err
	}
	older := probe(newTestMaintenanceID(t), windowStamp(0))
	require.ErrorIs(t, older, ErrMaintenanceExpired, "an older unknown command could roll the lease back")
	var expired *MaintenanceExpiredError
	require.ErrorAs(t, older, &expired)
	assert.True(t, expired.HighWater.Equal(windowStamp(time.Hour)))
	require.ErrorIs(t, probe(newTestMaintenanceID(t), windowStamp(time.Hour)), ErrMaintenanceExpired,
		"an equal stamp is new work only for the same command")
	require.NoError(t, probe(newest, windowStamp(time.Hour)), "the command that set the mark may replay")
	require.NoError(t, probe(newTestMaintenanceID(t), windowStamp(2*time.Hour)))

	unstamped, err := newMaintenanceRequestAuthority(
		callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source.LeaseUUID(),
		mustLifecycleCallbackURL(t, target), nil, "docker-a", callbackStorageID(t, windowTestStorageID),
	)
	require.NoError(t, err)
	_, err = callbacks.ProbeMaintenanceIntent(unstamped)
	require.ErrorIs(t, err, ErrMaintenanceExpired, "an unstamped command cannot prove it is new once a receipt left")

	_, err = callbacks.BeginMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target, windowStamp(0)),
	)
	require.ErrorIs(t, err, ErrMaintenanceExpired)
	intents, err := callbacks.listMaintenanceIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "an expired command publishes nothing")
}

func mustLifecycleCallbackURL(t *testing.T, target Release) string {
	t.Helper()
	identity, ok := target.RuntimeIdentity()
	require.True(t, ok)
	return identity.LifecycleCallbackURL()
}

// TestReplayWithADifferentStampIsAConflict keeps a reused key from being
// deduplicated as the stored command when the provider admitted it anew.
func TestReplayWithADifferentStampIsAConflict(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "window-stamp-conflict")
	id := newTestMaintenanceID(t)
	admission, err := callbacks.BeginMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, id, MaintenanceIntentRestart, source, target, windowStamp(0)),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.CancelMaintenanceIntent(createdMaintenanceDispatch(t, admission)))
	})

	disposition, err := callbacks.ProbeMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, id, MaintenanceIntentRestart, source, target, windowStamp(0)).request,
	)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionExisting, disposition)
	_, err = callbacks.ProbeMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, id, MaintenanceIntentRestart, source, target, windowStamp(time.Second)).request,
	)
	require.ErrorIs(t, err, ErrMaintenanceIntentConflict)
}

// TestAReplayWithoutAStampMatchesTheStampedCommand pins that stamps are
// compared only when both sides carry one. An older provider replays its
// commands without admitted_at, so its replay must match the stamped head or
// receipt by ID and fingerprint, instead of being refused as a conflict for a
// command that ran. Two different stamps still conflict.
func TestAReplayWithoutAStampMatchesTheStampedCommand(t *testing.T) {
	releases, callbacks, source, target := maintenanceFixture(t, "window-unstamped-replay")
	completed := newTestMaintenanceID(t)
	completeMaintenanceForTest(t, releases, callbacks,
		stampedMaintenanceSpec(t, callbacks, completed, MaintenanceIntentRestart, source, target, windowStamp(0)))
	_, latest, err := releases.claimLatestActive(source.LeaseUUID())
	require.NoError(t, err)
	probe := func(id MaintenanceID, stamp time.Time) (MaintenanceIntentAdmissionDisposition, error) {
		return callbacks.ProbeMaintenanceIntent(
			stampedMaintenanceSpec(t, callbacks, id, MaintenanceIntentRestart, latest, target, stamp).request,
		)
	}

	disposition, err := probe(completed, time.Time{})
	require.NoError(t, err, "an unstamped replay of a completed stamped command")
	assert.Equal(t, MaintenanceIntentAdmissionCompleted, disposition)
	_, err = probe(completed, windowStamp(time.Millisecond))
	require.ErrorIs(t, err, ErrMaintenanceIntentConflict, "a different stamp names a different command")

	inFlight := newTestMaintenanceID(t)
	admission, err := callbacks.BeginMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, inFlight, MaintenanceIntentRestart, latest, target, windowStamp(time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.CancelMaintenanceIntent(createdMaintenanceDispatch(t, admission)))
	})
	disposition, err = probe(inFlight, time.Time{})
	require.NoError(t, err, "an unstamped replay of an in-flight stamped command")
	assert.Equal(t, MaintenanceIntentAdmissionExisting, disposition)
}

// TestCustomDomainChurnNeverEvictsProviderReceipts pins the separate window
// for backend-minted commands.
func TestCustomDomainChurnNeverEvictsProviderReceipts(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "window-custom-domain")
	// The provider receipts are the oldest, so a shared window would take them.
	receipts := make([]windowReceipt, 0, maxCustomDomainMaintenanceWindow+10)
	for range 10 {
		receipts = append(receipts, windowReceipt{kind: MaintenanceIntentRestart})
	}
	for range maxCustomDomainMaintenanceWindow {
		receipts = append(receipts, windowReceipt{kind: MaintenanceIntentCustomDomain})
	}
	seeded := seedMaintenanceWindow(t, callbacks, source, target, receipts)

	admission, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentCustomDomain, source, target,
		"docker-a", callbackStorageID(t, windowTestStorageID),
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.CancelMaintenanceIntent(createdMaintenanceDispatch(t, admission)))
	})
	assert.False(t, receiptPresent(t, callbacks, seeded[10]), "the oldest custom-domain receipt left its window")
	for _, record := range seeded[:10] {
		assert.True(t, receiptPresent(t, callbacks, record), "provider receipts are never evicted by custom-domain churn")
	}
	lineage := lineageFor(t, callbacks, source.LeaseUUID())
	assert.False(t, lineage.ProviderEvicted)
	assert.True(t, lineage.HighWaterAdmittedAt.IsZero(), "a backend-minted command never moves the mark")
	require.NoError(t, callbacks.Healthy())
}

// TestFailedEffectStartedReceiptStaysUntilItsCleanupIsConfirmed keeps the
// late-arrival cleanup authority of a failed generation inside the window.
func TestFailedEffectStartedReceiptStaysUntilItsCleanupIsConfirmed(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "window-failed-receipt")
	receipts := providerWindow(maxProviderMaintenanceWindow)
	receipts[0] = windowReceipt{kind: MaintenanceIntentUpdate, status: backend.CallbackStatusFailed, effectStarted: true}
	seeded := seedMaintenanceWindow(t, callbacks, source, target, receipts)

	first, err := callbacks.BeginMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target, windowStamp(0)),
	)
	require.NoError(t, err)
	require.NoError(t, callbacks.CancelMaintenanceIntent(createdMaintenanceDispatch(t, first)))
	assert.True(t, receiptPresent(t, callbacks, seeded[0]), "unconfirmed cleanup authority stays")
	assert.False(t, receiptPresent(t, callbacks, seeded[1]), "the next oldest receipt left instead")

	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		lineage, err := loadMaintenanceLineageTx(tx, source.LeaseUUID())
		if err != nil {
			return err
		}
		lineage.CleanupConfirmedSequence = seeded[0].CompletionSequence
		return putMaintenanceLineageTx(tx, source.LeaseUUID(), lineage)
	}))
	// Refill the slot the cancelled command left, so the next publish must
	// evict again.
	seedMaintenanceWindow(t, callbacks, source, target, providerWindow(1))
	second, err := callbacks.BeginMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target, windowStamp(time.Hour)),
	)
	require.NoError(t, err)
	require.NoError(t, callbacks.CancelMaintenanceIntent(createdMaintenanceDispatch(t, second)))
	assert.False(t, receiptPresent(t, callbacks, seeded[0]), "a confirmed cleanup lets the receipt leave")
	require.NoError(t, callbacks.Healthy())
}

// TestWindowFullOfUnconfirmedFailuresRefusesCapacity bounds the window even
// when nothing in it may leave yet.
func TestWindowFullOfUnconfirmedFailuresRefusesCapacity(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "window-all-failed")
	receipts := make([]windowReceipt, maxProviderMaintenanceWindow)
	for index := range receipts {
		receipts[index] = windowReceipt{kind: MaintenanceIntentUpdate, status: backend.CallbackStatusFailed, effectStarted: true}
	}
	seedMaintenanceWindow(t, callbacks, source, target, receipts)
	_, err := callbacks.BeginMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target, windowStamp(0)),
	)
	require.ErrorIs(t, err, ErrMaintenanceReceiptCapacity)
	require.NoError(t, callbacks.Healthy())
}

// TestSupersessionSurvivesEvictingTheNewerUpdate covers eviction skipping an
// older failed update: the newer update's receipt leaves, yet replaying the
// older one still reads as superseded.
func TestSupersessionSurvivesEvictingTheNewerUpdate(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "window-supersession")
	identity, ok := target.RuntimeIdentity()
	require.True(t, ok)
	olderID := newTestMaintenanceID(t)
	older, err := newMaintenanceRequestAuthority(
		callbacks, olderID, MaintenanceIntentUpdate, source.LeaseUUID(), identity.LifecycleCallbackURL(),
		target.Manifest, "docker-a", callbackStorageID(t, windowTestStorageID),
	)
	require.NoError(t, err)
	receipts := providerWindow(maxProviderMaintenanceWindow)
	receipts[0] = windowReceipt{
		kind: MaintenanceIntentUpdate, status: backend.CallbackStatusFailed, effectStarted: true,
		digest: encodeMaintenanceDigest(older.digest),
	}
	receipts[1] = windowReceipt{kind: MaintenanceIntentUpdate}
	seeded := seedMaintenanceWindow(t, callbacks, source, target, receipts)
	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		// Rebind the first seeded receipt to the request the test replays.
		root := tx.Bucket(callbackMaintenanceHistoryBucketName).Bucket([]byte(source.LeaseUUID()))
		if err := root.Delete([]byte(seeded[0].MaintenanceID.String())); err != nil {
			return err
		}
		record := seeded[0]
		record.MaintenanceID = olderID
		data, err := marshalMaintenanceCompletionRecord(record)
		if err != nil {
			return err
		}
		return root.Put([]byte(olderID.String()), data)
	}))

	admission, err := callbacks.BeginMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target, windowStamp(0)),
	)
	require.NoError(t, err)
	require.NoError(t, callbacks.CancelMaintenanceIntent(createdMaintenanceDispatch(t, admission)))
	require.False(t, receiptPresent(t, callbacks, seeded[1]), "the newer update's receipt left the window")

	disposition, err := callbacks.ProbeMaintenanceIntent(older)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionCompletedSuperseded, disposition,
		"the lineage still remembers the newer update")
}

func TestClosingALeaseDeletesItsLineage(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "window-close")
	seedMaintenanceWindow(t, callbacks, source, target, providerWindow(maxProviderMaintenanceWindow))
	admission, err := callbacks.BeginMaintenanceIntent(
		stampedMaintenanceSpec(t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target, windowStamp(0)),
	)
	require.NoError(t, err)
	require.NoError(t, callbacks.CancelMaintenanceIntent(createdMaintenanceDispatch(t, admission)))
	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		return releaseClosedLeaseMaintenanceReceiptsTx(tx, source.LeaseUUID())
	}))
	require.NoError(t, callbacks.db.View(func(tx *bolt.Tx) error {
		_, stored, err := loadStoredMaintenanceLineageTx(tx, source.LeaseUUID())
		assert.False(t, stored)
		return err
	}))
}

func TestCorruptLineageFailsJournalValidation(t *testing.T) {
	_, callbacks, source, _ := maintenanceFixture(t, "window-corrupt")
	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(maintenanceLineageBucketName)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(source.LeaseUUID()), []byte(`{"version":1,"high_water_admitted_at":"2026-09-30T12:00:00Z"}`))
	}))
	require.Error(t, callbacks.Healthy(), "a high-water stamp without its command is incomplete")
}

func TestDiagnosticOfAnEvictedAttemptIsComplete(t *testing.T) {
	maintenance := diagnosticAttemptIdentity{Kind: "maintenance", ReleaseVersion: 5}
	assert.True(t, diagnosticEvicted(maintenance, 5))
	assert.True(t, diagnosticEvicted(maintenance, 9))
	assert.False(t, diagnosticEvicted(maintenance, 4), "a newer attempt may still be the head's")
	assert.False(t, diagnosticEvicted(diagnosticAttemptIdentity{Kind: "maintenance"}, 9),
		"an attempt without a target release proves nothing")
	assert.False(t, diagnosticEvicted(diagnosticAttemptIdentity{Kind: "operation", ReleaseVersion: 1}, 9))
}

// TestCleanupConfirmationAdvancesOnlyOverAContiguousQualifiedRun pins when a
// failed receipt's cleanup authority may stop holding it in the window.
func TestCleanupConfirmationAdvancesOnlyOverAContiguousQualifiedRun(t *testing.T) {
	releases, callbacks, source, target := maintenanceFixture(t, "window-confirm")
	settlement := maintenancePairForTest(t, callbacks, releases)
	failed := windowReceipt{kind: MaintenanceIntentUpdate, status: backend.CallbackStatusFailed, effectStarted: true}
	seeded := seedMaintenanceWindow(t, callbacks, source, target, []windowReceipt{
		failed, {kind: MaintenanceIntentRestart}, failed, failed,
	})
	proof := func(record maintenanceCompletionRecord, after time.Duration) FailedMaintenanceCleanupProof {
		return FailedMaintenanceCleanupProof{settlement: settlement, record: record, attestedAt: record.SettledAt.Add(after)}
	}
	confirmed := func() uint64 {
		return lineageFor(t, callbacks, source.LeaseUUID()).CleanupConfirmedSequence
	}

	require.NoError(t, settlement.ConfirmFailedMaintenanceCleanup(
		[]FailedMaintenanceCleanupProof{proof(seeded[0], failedReceiptEvictionGrace-time.Second)}, nil,
	))
	assert.Zero(t, confirmed(), "an attestation inside the grace window could precede a late creation")

	require.NoError(t, settlement.ConfirmFailedMaintenanceCleanup([]FailedMaintenanceCleanupProof{
		proof(seeded[0], failedReceiptEvictionGrace), proof(seeded[3], failedReceiptEvictionGrace),
	}, nil))
	assert.Equal(t, seeded[0].CompletionSequence, confirmed(), "the run stops at the unconfirmed failure")

	unverifiable := UnverifiableMaintenanceReceipt{
		LeaseUUID: seeded[2].LeaseUUID, MaintenanceID: seeded[2].MaintenanceID,
		settlement: settlement, record: seeded[2],
	}
	require.NoError(t, settlement.ConfirmFailedMaintenanceCleanup(
		[]FailedMaintenanceCleanupProof{proof(seeded[3], failedReceiptEvictionGrace)},
		[]UnverifiableMaintenanceReceipt{unverifiable},
	))
	assert.Equal(t, seeded[3].CompletionSequence, confirmed(),
		"a receipt that can no longer authorize cleanup needs no attestation")

	require.NoError(t, settlement.ConfirmFailedMaintenanceCleanup(nil, nil))
	assert.Equal(t, seeded[3].CompletionSequence, confirmed(), "confirmation never moves backward")

	foreign := FailedMaintenanceCleanupProof{record: seeded[0], attestedAt: time.Now()}
	require.Error(t, settlement.ConfirmFailedMaintenanceCleanup([]FailedMaintenanceCleanupProof{foreign}, nil))
	require.NoError(t, callbacks.Healthy())
}

// TestOneBadProofDoesNotHoldBackAnotherLease keeps a cleanup that could not
// run, or any unminted proof, from stalling confirmation fleet-wide.
func TestOneBadProofDoesNotHoldBackAnotherLease(t *testing.T) {
	releases, callbacks, source, target := maintenanceFixture(t, "window-confirm-isolation")
	settlement := maintenancePairForTest(t, callbacks, releases)
	failed := windowReceipt{kind: MaintenanceIntentUpdate, status: backend.CallbackStatusFailed, effectStarted: true}
	seeded := seedMaintenanceWindow(t, callbacks, source, target, []windowReceipt{failed})
	valid := FailedMaintenanceCleanupProof{
		settlement: settlement, record: seeded[0], attestedAt: seeded[0].SettledAt.Add(failedReceiptEvictionGrace),
	}

	err := settlement.ConfirmFailedMaintenanceCleanup(
		[]FailedMaintenanceCleanupProof{{}, valid}, nil,
	)
	require.Error(t, err, "the unminted proof is reported")
	assert.Equal(t, seeded[0].CompletionSequence,
		lineageFor(t, callbacks, source.LeaseUUID()).CleanupConfirmedSequence,
		"the valid proof still confirms its lease")
}

func TestProofForAnEarlierReceiptWithTheSameIDQualifiesNothing(t *testing.T) {
	releases, callbacks, source, target := maintenanceFixture(t, "window-confirm-sequence")
	settlement := maintenancePairForTest(t, callbacks, releases)
	failed := windowReceipt{kind: MaintenanceIntentUpdate, status: backend.CallbackStatusFailed, effectStarted: true}
	seeded := seedMaintenanceWindow(t, callbacks, source, target, []windowReceipt{failed})
	stale := seeded[0]
	stale.CompletionSequence--
	proof := FailedMaintenanceCleanupProof{
		settlement: settlement, record: stale, attestedAt: stale.SettledAt.Add(failedReceiptEvictionGrace),
	}
	require.NoError(t, settlement.ConfirmFailedMaintenanceCleanup([]FailedMaintenanceCleanupProof{proof}, nil))
	assert.Zero(t, lineageFor(t, callbacks, source.LeaseUUID()).CleanupConfirmedSequence)
}

func TestProviderRequestAuthorityRefusesAnUnusableStamp(t *testing.T) {
	settlement := &MaintenanceSettlement{}
	lease := testLeaseUUID("window-stamp-validation")
	for name, test := range map[string]struct {
		kind  MaintenanceIntentKind
		stamp time.Time
	}{
		"custom domain is backend-minted": {kind: MaintenanceIntentCustomDomain, stamp: windowStamp(0)},
		"not UTC":                         {kind: MaintenanceIntentRestart, stamp: windowStamp(0).In(time.FixedZone("x", 3600))},
		"before the epoch":                {kind: MaintenanceIntentRestart, stamp: time.Unix(-1, 0).UTC()},
		"monotonic reading":               {kind: MaintenanceIntentRestart, stamp: time.Now()},
	} {
		_, err := settlement.NewProviderMaintenanceRequestAuthority(
			newTestMaintenanceID(t), test.kind, lease, "https://fred.example/callback", nil, test.stamp,
		)
		require.Error(t, err, name)
	}
	require.NoError(t, validateMaintenanceAdmissionStamp(windowStamp(0)))
	require.Error(t, validateMaintenanceAdmissionStamp(time.Time{}))
}

// TestReusingAKeyStillNamedByTheReleaseHistoryIsAConflict covers a key whose
// receipt left the window while its release generation is still retained: it
// is refused as a conflict before admission raises the high-water mark or
// evicts anything, instead of failing later at append and staying pending.
func TestReusingAKeyStillNamedByTheReleaseHistoryIsAConflict(t *testing.T) {
	releases, callbacks, source, target := maintenanceFixture(t, "window-key-reuse")
	reused := newTestMaintenanceID(t)
	completeMaintenanceForTest(t, releases, callbacks, newMaintenanceIntentSpec(
		t, callbacks, reused, MaintenanceIntentRestart, source, target,
		"docker-a", callbackStorageID(t, windowTestStorageID),
	))
	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		// The receipt leaves the window, as eviction would, with its reservation.
		history := tx.Bucket(callbackMaintenanceHistoryBucketName).Bucket([]byte(source.LeaseUUID()))
		if err := history.Delete([]byte(reused.String())); err != nil {
			return err
		}
		return releaseCallbackReceiptReservationsTx(tx, 1)
	}))
	require.NoError(t, callbacks.Healthy())
	before := lineageFor(t, callbacks, source.LeaseUUID())

	_, latest, err := releases.claimLatestActive(source.LeaseUUID())
	require.NoError(t, err)
	reuse := stampedMaintenanceSpec(t, callbacks, reused, MaintenanceIntentRestart, latest, target, windowStamp(time.Hour))
	require.ErrorIs(t, maintenancePairForTest(t, callbacks, releases).refuseMaintenanceIDReuse(
		source.LeaseUUID(), reused), ErrMaintenanceIntentConflict, "the probe refuses it")
	_, err = callbacks.BeginMaintenanceIntent(reuse)
	require.ErrorIs(t, err, ErrMaintenanceIntentConflict)
	assert.Equal(t, before, lineageFor(t, callbacks, source.LeaseUUID()), "a refused reuse moves no mark")
	intents, err := callbacks.listMaintenanceIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
}
