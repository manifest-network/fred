package shared

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

func countAttemptDiagnosticBytes(t *testing.T, store *DiagnosticsStore) (int, int) {
	t.Helper()
	count, total := 0, 0
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(attemptDiagnosticsBucketName).ForEach(func(_, value []byte) error { count++; total += len(value); return nil })
	}))
	return count, total
}

func TestFailureDiagnosticsRetirementRequiresExactTerminalReceiptWhenHeadAbsent(t *testing.T) {
	fixture, diagnostics, execution := newMaintenanceDiagnosticsFixture(t, "retirement-receipt")
	capture, err := diagnostics.CaptureMaintenance(execution.subject, FailureDiagnosticObservation{
		Logs: map[string]string{"web/0": "only captured failure"}, Status: DiagnosticCaptureComplete,
	})
	require.NoError(t, err)
	failed, ok := fixture.settlement.ExecuteMaintenance(testMaintenanceLifetime(t, t.Context()), execution).(MaintenanceExecutionFailure)
	require.True(t, ok)
	proof, err := fixture.settlement.FailMaintenance(failed, backend.ReasonRestartFailed, "startup failed")
	require.NoError(t, err)
	completion, err := resolveMaintenanceFailureForTest(fixture.settlement, proof, "startup failed")
	require.NoError(t, err)
	require.NoError(t, fixture.stores.callbacks.removeEntry(completion))
	leaseKey, receiptKey := []byte(fixture.intent.LeaseUUID()), []byte(fixture.intent.MaintenanceID().String())
	var receipt []byte
	require.NoError(t, fixture.stores.callbacks.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackMaintenanceHistoryBucketName).Bucket(leaseKey)
		receipt = slices.Clone(bucket.Get(receiptKey))
		return bucket.Delete(receiptKey)
	}))
	require.NotEmpty(t, receipt)
	require.NoError(t, diagnostics.RetireHistoricalCapture(capture))
	_, _, err = capture.Snapshot()
	require.NoError(t, err, "missing head and an unrelated source receipt cannot authorize retirement")
	require.NoError(t, fixture.stores.callbacks.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackMaintenanceHistoryBucketName).Bucket(leaseKey).Put(receiptKey, receipt)
	}))
	require.NoError(t, diagnostics.RetireHistoricalCapture(capture))
	_, _, err = capture.Snapshot()
	require.Error(t, err, "the exact completed receipt makes an unpublished capture obsolete")
}

func TestFailureDiagnosticsRetainsConstantCaptureCountAcrossManyFailedAttempts(t *testing.T) {
	fixture, diagnostics, execution := newMaintenanceDiagnosticsFixture(t, "bounded-history")
	firstSubject := execution.subject
	const payloadBytes = 256 << 10
	for attempt := range 16 {
		capture, err := diagnostics.CaptureMaintenance(execution.subject, FailureDiagnosticObservation{
			Error: "startup failed", Logs: map[string]string{"web/0": strings.Repeat(string(rune('a'+attempt)), payloadBytes)}, Status: DiagnosticCaptureComplete,
		})
		require.NoError(t, err)
		require.NoError(t, diagnostics.RetireHistoricalCapture(capture))
		_, _, err = capture.Snapshot()
		require.NoError(t, err, "pending exact attempt must retain its only captured logs")
		failed, ok := fixture.settlement.ExecuteMaintenance(testMaintenanceLifetime(t, t.Context()), execution).(MaintenanceExecutionFailure)
		require.True(t, ok)
		proof, err := fixture.settlement.FailMaintenance(failed, backend.ReasonRestartFailed, "startup failed")
		require.NoError(t, err)
		publication, err := diagnostics.MaintenanceFailureContext(t.Context(), proof, FailureDiagnosticObservation{Status: DiagnosticCaptureUnavailable})
		require.NoError(t, err)
		require.NoError(t, publication.PublishContext(t.Context(), 1))
		completion, err := resolveMaintenanceFailureForTest(fixture.settlement, proof, "startup failed")
		require.NoError(t, err)
		require.NoError(t, fixture.stores.callbacks.removeEntry(completion))
		count, total := countAttemptDiagnosticBytes(t, diagnostics.store)
		require.Equal(t, 1, count, "terminal history must not retain one large log record per retry")
		require.Less(t, total, payloadBytes+16<<10)
		if attempt == 15 {
			break
		}
		source, claim, err := fixture.settlement.ClaimLatestActive(fixture.intent.LeaseUUID())
		require.NoError(t, err)
		source.Version = 0
		source.Status = "deploying"
		candidate := maintenanceCandidateForSettlement(t, fixture.settlement, claim, source, newTestMaintenanceID(t))
		admitted, err := fixture.settlement.BeginMaintenanceIntent(candidate)
		require.NoError(t, err)
		appendClaim, err := fixture.settlement.StartMaintenanceAppend(createdMaintenanceDispatch(t, admitted))
		require.NoError(t, err)
		target, err := fixture.settlement.AppendMaintenance(appendClaim)
		require.NoError(t, err)
		target, err = fixture.settlement.BindMaintenanceIntentTarget(target)
		require.NoError(t, err)
		execution, err = fixture.settlement.StartMaintenanceExecution(target)
		require.NoError(t, err)
	}
	late, err := diagnostics.CaptureMaintenance(firstSubject, FailureDiagnosticObservation{
		Logs: map[string]string{"web/0": "late old container"}, Status: DiagnosticCaptureComplete,
	})
	require.NoError(t, err)
	count, _ := countAttemptDiagnosticBytes(t, diagnostics.store)
	require.Equal(t, 2, count, "late cleanup retains only one temporary historical capture")
	require.NoError(t, diagnostics.RetireHistoricalCapture(late))
	count, _ = countAttemptDiagnosticBytes(t, diagnostics.store)
	require.Equal(t, 1, count)
	visible, err := diagnostics.store.Get(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.Equal(t, 16, visible.FailCount)
	require.Equal(t, strings.Repeat("p", payloadBytes), visible.Logs["web/0"])
	require.NoError(t, diagnostics.store.Delete(fixture.intent.LeaseUUID()))
	count, _ = countAttemptDiagnosticBytes(t, diagnostics.store)
	require.Zero(t, count, "successful runtime cleanup retires obsolete attempt bytes")
}

// A published operation failure is the durable record that the failure was
// decided: recovery settles the operation from it. Diagnostic retention must
// not expire it while the operation is unsettled, or a backend that stopped
// between the decision and its settlement would start with nothing to settle
// and report the lease as provisioning again (ENG-939). It is kept by the
// periodic cleanup and by the cleanup that runs when the store opens, and
// expires as any other diagnostic once the operation is settled.
func TestDiagnosticsExpiryKeepsAPublishedOperationFailureUntilItsOperationSettles(t *testing.T) {
	const reason = "image pull failed"
	// Everything in the store is older than a cutoff an hour from now.
	const everything = -time.Hour
	fixture := newCallbackPublisherFixture(t, "docker-a")
	path := filepath.Join(t.TempDir(), "diagnostics.db")
	store, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: path})
	require.NoError(t, err)
	diagnostics, err := NewFailureDiagnostics(store, fixture.stores.settlement, fixture.maintenance)
	require.NoError(t, err)
	claim := beginHandoffOperation(t, fixture.stores.settlement, testOperationIntentSpec(t, "published-failure"))
	uncommitted := commitHandoffRefusal(t, fixture.stores.settlement, claim)
	publication, err := diagnostics.OperationFailureContext(t.Context(), uncommitted, FailureDiagnosticObservation{
		Error: reason, Message: reason, Reason: backend.ReasonInternal, Status: DiagnosticCaptureUnavailable,
	})
	require.NoError(t, err)
	require.NoError(t, publication.PublishContext(t.Context(), 1))
	requirePublished := func(diagnostics *FailureDiagnostics, when string) {
		t.Helper()
		entry, published, err := diagnostics.PublishedOperationFailure(claim)
		require.NoError(t, err, when)
		require.True(t, published, when)
		require.Equal(t, reason, entry.Message, when)
		visible, err := diagnostics.store.Get(claim.LeaseUUID())
		require.NoError(t, err, when)
		require.NotNil(t, visible, when)
		require.Equal(t, reason, visible.Message, when)
	}

	removed, err := store.RemoveOlderThan(everything)
	require.NoError(t, err)
	require.Zero(t, removed)
	requirePublished(diagnostics, "after a periodic cleanup")

	// The cleanup that runs when the store opens has no journal to ask yet.
	require.NoError(t, store.Close())
	store, err = NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: path, MaxAge: time.Nanosecond, CleanupInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	diagnostics, err = NewFailureDiagnostics(store, fixture.stores.settlement, fixture.maintenance)
	require.NoError(t, err)
	requirePublished(diagnostics, "after the store reopened")

	require.NoError(t, fixture.publisher.PublishOperationFailureContext(t.Context(), uncommitted, reason))
	removed, err = store.RemoveOlderThan(everything)
	require.NoError(t, err)
	require.Equal(t, 1, removed, "a settled operation's failure expires as any other diagnostic")
	_, published, err := diagnostics.PublishedOperationFailure(claim)
	require.NoError(t, err)
	require.False(t, published)
	visible, err := store.Get(claim.LeaseUUID())
	require.NoError(t, err)
	require.Nil(t, visible)
}
