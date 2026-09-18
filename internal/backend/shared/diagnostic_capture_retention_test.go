package shared

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func newRetainedCapture(t *testing.T) (*DiagnosticsStore, FailureDiagnosticCapture) {
	t.Helper()
	_, diagnostics, execution := newMaintenanceDiagnosticsFixture(t, "retained-capture")
	capture, err := diagnostics.CaptureMaintenance(execution.subject, FailureDiagnosticObservation{
		Logs: map[string]string{"web/0": "startup failed"}, Status: DiagnosticCaptureComplete,
	})
	require.NoError(t, err)
	return diagnostics.store, capture
}

func TestFailureDiagnosticRetentionAllowsDatabaseGrowthDuringCleanup(t *testing.T) {
	store, capture := newRetainedCapture(t)
	entered, finish := make(chan struct{}), make(chan struct{})
	actionDone := make(chan error, 1)
	go func() {
		actionDone <- capture.RetainDuring(func() error {
			close(entered)
			<-finish
			return nil
		})
	}()
	<-entered
	assert.Zero(t, store.db.Stats().OpenTxN, "physical cleanup must not retain a read transaction or its mmap lock")
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- store.Store(DiagnosticEntry{LeaseUUID: "unrelated", Logs: map[string]string{"web/0": strings.Repeat("x", 4<<20)}})
	}()
	select {
	case err := <-writeDone:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Error("an unrelated writer cannot grow the database until physical cleanup finishes")
	}
	close(finish)
	require.NoError(t, <-actionDone)
}

func TestFailureDiagnosticRetentionDefersExpiryUntilLastCleanup(t *testing.T) {
	store, capture := newRetainedCapture(t)
	record, err := store.readAttempt(capture.identity)
	require.NoError(t, err)
	record.Entry.CreatedAt = time.Now().Add(-48 * time.Hour)
	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, store.update(func(tx *bolt.Tx) error {
		return tx.Bucket(attemptDiagnosticsBucketName).Put([]byte(capture.identity.key()), encoded)
	}))
	require.NoError(t, capture.RetainDuring(func() error {
		if err := capture.RetainDuring(func() error {
			_, err := store.RemoveOlderThan(24 * time.Hour)
			return err
		}); err != nil {
			return err
		}
		entry, _, err := capture.Snapshot()
		assert.Equal(t, "startup failed", entry.Logs["web/0"], "the outer cleanup still owns its durable capture")
		return err
	}))
	_, _, err = capture.Snapshot()
	require.Error(t, err, "expiry is applied once the last cleanup relinquishes the capture")
}

func TestFailureDiagnosticRetentionDefersOnlyCommittedDeletion(t *testing.T) {
	store, capture := newRetainedCapture(t)
	want := errors.New("rolled back")
	require.NoError(t, capture.RetainDuring(func() error {
		err := store.update(func(tx *bolt.Tx) error {
			if err := store.deleteAttemptTx(tx, []byte(capture.identity.key())); err != nil {
				return err
			}
			return want
		})
		assert.ErrorIs(t, err, want)
		return nil
	}))
	_, _, err := capture.Snapshot()
	require.NoError(t, err, "a failed transaction cannot retire the saved diagnostic later")
}

func TestFailureDiagnosticRetentionCloseWaitsWithoutHoldingDatabaseTransaction(t *testing.T) {
	store, capture := newRetainedCapture(t)
	closeDone := make(chan error, 1)
	require.NoError(t, capture.RetainDuring(func() error {
		go func() { closeDone <- store.Close() }()
		require.Eventually(t, func() bool {
			store.retentionMu.Lock()
			defer store.retentionMu.Unlock()
			return store.closing
		}, 5*time.Second, time.Millisecond)
		select {
		case err := <-closeDone:
			t.Error("store closed while an exact cleanup still depends on its capture")
			closeDone <- err
		default:
		}
		assert.Zero(t, store.db.Stats().OpenTxN)
		_, _, err := capture.Snapshot()
		return err
	}))
	require.NoError(t, <-closeDone)
}

func TestDiagnosticsStoreExpiryPreservesMalformedRowsWithoutBlockingUnrelatedCleanup(t *testing.T) {
	store, capture := newRetainedCapture(t)
	old := time.Now().Add(-48 * time.Hour)
	require.NoError(t, store.Store(DiagnosticEntry{LeaseUUID: "expired", CreatedAt: old}))
	require.NoError(t, store.Store(DiagnosticEntry{LeaseUUID: "fresh", CreatedAt: time.Now()}))
	record, err := store.readAttempt(capture.identity)
	require.NoError(t, err)
	record.Entry.CreatedAt = old
	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	malformed := []byte("{broken")
	require.NoError(t, store.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(diagnosticsBucketName).Put([]byte("bad-visible"), malformed); err != nil {
			return err
		}
		attempts := tx.Bucket(attemptDiagnosticsBucketName)
		if err := attempts.Put([]byte("bad-attempt"), malformed); err != nil {
			return err
		}
		return attempts.Put([]byte(capture.identity.key()), encoded)
	}))
	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	require.Equal(t, 1, removed)
	entry, err := store.Get("expired")
	require.NoError(t, err)
	require.Nil(t, entry)
	entry, err = store.Get("fresh")
	require.NoError(t, err)
	require.NotNil(t, entry)
	_, _, err = capture.Snapshot()
	require.Error(t, err)
	require.NoError(t, store.view(func(tx *bolt.Tx) error {
		assert.Equal(t, malformed, tx.Bucket(diagnosticsBucketName).Get([]byte("bad-visible")))
		assert.Equal(t, malformed, tx.Bucket(attemptDiagnosticsBucketName).Get([]byte("bad-attempt")))
		return nil
	}))
}
