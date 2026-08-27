package placement

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/provisioner/operation"
)

func newTestStore(t *testing.T, opts ...Option) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func requireSetAttempting(t *testing.T, s *Store, leaseUUID, backendName string) uint64 {
	t.Helper()
	revision, err := s.SetAttempting(leaseUUID, backendName)
	require.NoError(t, err)
	return revision
}

func setAttemptingErr(s *Store, leaseUUID, backendName string) error {
	_, err := s.SetAttempting(leaseUUID, backendName)
	return err
}

func requireOperationID(t *testing.T, value string) operation.OperationID {
	t.Helper()
	sequence, err := strconv.ParseUint(value, 10, 64)
	require.NoError(t, err)
	id, err := testOperationID(sequence)
	require.NoError(t, err)
	return id
}

func requireAdmissionBaseline(
	t *testing.T,
	s *Store,
	backendNames ...string,
) AdmissionBaseline {
	t.Helper()
	require.NoError(t, s.ConfigureBackendTopology(backendNames))
	fence := s.BeginInventorySession()
	_, err := s.ProjectInventory(fence, InventoryProjection{Complete: true})
	s.EndInventorySession(fence)
	require.NoError(t, err)
	baseline := s.CurrentAdmissionBaseline()
	require.True(t, baseline.Valid())
	return baseline
}

func testOperationID(sequence uint64) (operation.OperationID, error) {
	return operation.ParseID(fmt.Sprintf("00000000-0000-4000-8000-%012x", sequence))
}

func requireSetBatchIfNotNewer(
	t *testing.T,
	s *Store,
	placements map[string]string,
	maxRevision uint64,
) map[string]uint64 {
	t.Helper()
	applied, _, err := s.SetBatchIfNotNewer(placements, maxRevision)
	require.NoError(t, err)
	return applied
}

func requireSetConflictsIfNotNewer(
	t *testing.T,
	s *Store,
	conflicts map[string][]string,
	maxRevision uint64,
) map[string]struct{} {
	t.Helper()
	_, fenced, err := s.SetConflictsIfNotNewer(conflicts, maxRevision)
	require.NoError(t, err)
	return fenced
}

func writeRawRecords(t *testing.T, dbPath string, records map[string][]byte) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		for key, value := range records {
			if err := b.Put([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
}

func readRawRecords(t *testing.T, dbPath string, leaseUUIDs ...string) map[string][]byte {
	t.Helper()
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)

	records := make(map[string][]byte, len(leaseUUIDs))
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errors.New("placements bucket missing")
		}
		for _, leaseUUID := range leaseUUIDs {
			records[leaseUUID] = append([]byte(nil), b.Get([]byte(leaseUUID))...)
		}
		return nil
	}))
	require.NoError(t, db.Close())
	return records
}

func TestPlacement_StateAndString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		placement Placement
		want      State
		wantText  string
	}{
		{name: "zero is absent", want: StateAbsent, wantText: "absent"},
		{
			name:      "attempt without owner is attempting",
			placement: Placement{Attempt: "backend-a"},
			want:      StateAttempting,
			wantText:  "attempting",
		},
		{
			name:      "backend is confirmed",
			placement: Placement{Backend: "backend-a"},
			want:      StateConfirmed,
			wantText:  "confirmed",
		},
		{
			name:      "confirmed wins when an attempt coexists",
			placement: Placement{Backend: "backend-a", Attempt: "backend-a"},
			want:      StateConfirmed,
			wantText:  "confirmed",
		},
		{
			name:      "timestamp without a fact is unusable",
			placement: Placement{SetAt: time.Unix(1, 0)},
			want:      StateUnusable,
			wantText:  "unusable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.placement.State())
			assert.Equal(t, tt.wantText, tt.placement.State().String())
		})
	}
	assert.Equal(t, "State(99)", State(99).String())
}

func TestStore_AttemptConfirmAndClearLifecycle(t *testing.T) {
	fixed := time.Date(2026, 8, 25, 12, 30, 0, 0, time.FixedZone("EDT", -4*60*60))
	s := newTestStore(t, WithClock(func() time.Time { return fixed }))

	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
	assert.Zero(t, s.SnapshotRevision())

	attemptRevision := requireSetAttempting(t, s, "lease-1", "backend-a")
	attempting := s.Lookup("lease-1")
	assert.Equal(t, StateAttempting, attempting.State())
	assert.Empty(t, attempting.Backend)
	assert.Equal(t, "backend-a", attempting.Attempt)
	assert.Equal(t, fixed.UTC(), attempting.SetAt)
	assert.NotZero(t, attempting.Revision())
	assert.Equal(t, attemptRevision, attempting.Revision(),
		"SetAttempting must return the exact durably committed record revision")
	assert.Equal(t, attempting.Revision(), s.SnapshotRevision())
	assert.Empty(t, s.Lookup("lease-1").Backend, "an attempt is not confirmed affinity")

	before := s.List()
	beforeRevision := s.SnapshotRevision()
	failedRevision, err := s.SetAttempting("lease-1", "backend-a")
	require.ErrorIs(t, err, ErrAttemptConflict)
	assert.Zero(t, failedRevision, "a refused write has no committed revision")
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	_, err = s.SetAttempting("lease-1", "backend-b")
	require.ErrorIs(t, err, ErrAttemptConflict)
	err = s.Confirm("lease-1", "backend-b")
	require.ErrorIs(t, err, ErrAttemptMismatch)
	assert.Equal(t, attempting, s.Lookup("lease-1"))

	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	confirmed := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, confirmed.State())
	assert.Equal(t, "backend-a", confirmed.Backend)
	assert.Empty(t, confirmed.Attempt)
	assert.Equal(t, attempting.SetAt, confirmed.SetAt)
	assert.Greater(t, confirmed.Revision(), attempting.Revision())
	assert.Equal(t, "backend-a", s.Lookup("lease-1").Backend)

	idempotentRevision := s.SnapshotRevision()
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	assert.Equal(t, idempotentRevision, s.SnapshotRevision())

	_, err = s.SetAttempting("lease-1", "backend-b")
	require.ErrorIs(t, err, ErrBackendConflict)
	requireSetAttempting(t, s, "lease-1", "backend-a")
	confirmedAttempt := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, confirmedAttempt.State())
	assert.Equal(t, "backend-a", confirmedAttempt.Backend)
	assert.Equal(t, "backend-a", confirmedAttempt.Attempt)

	err = s.ClearAttempt("lease-1", "backend-b")
	require.ErrorIs(t, err, ErrAttemptMismatch)
	assert.Equal(t, confirmedAttempt, s.Lookup("lease-1"))

	require.NoError(t, s.ClearAttempt("lease-1", "backend-a"))
	cleared := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, cleared.State())
	assert.Equal(t, "backend-a", cleared.Backend)
	assert.Empty(t, cleared.Attempt)
	assert.Greater(t, cleared.Revision(), confirmedAttempt.Revision())

	clearNoopRevision := s.SnapshotRevision()
	require.NoError(t, s.ClearAttempt("lease-1", "backend-a"))
	assert.Equal(t, clearNoopRevision, s.SnapshotRevision())
}

func TestStore_TypedAttemptsRequireInventoryAuthority(t *testing.T) {
	s := newTestStore(t)
	op1 := requireOperationID(t, "101")

	assert.False(t, (RecordRevision{}).Valid())
	assert.False(t, (InventoryFence{}).Valid())
	assert.False(t, (AttemptToken{}).Valid())
	assert.False(t, (AttemptToken{}).OperationID().Valid())

	startupFence := s.SnapshotFence()
	assert.True(t, startupFence.Valid(), "an explicitly minted revision-zero fence is valid")
	token, err := s.BeginAttempt("lease-1", "backend-a", operation.OperationID{})
	require.ErrorIs(t, err, operation.ErrInvalidID)
	assert.False(t, token.Valid())
	token, err = s.BeginAttempt("lease-1", "backend-a", op1)
	require.ErrorIs(t, err, ErrInventoryNotReady)
	assert.False(t, token.Valid())
	token, applied, err := s.BeginAttemptIfNotNewer(
		"lease-1", "backend-a", op1, startupFence,
	)
	require.ErrorIs(t, err, ErrInventoryNotReady)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())

	s.MarkInventoryReady()
	_, applied, err = s.BeginAttemptIfNotNewer(
		"lease-zero-fence", "backend-a", requireOperationID(t, "100"), InventoryFence{},
	)
	require.ErrorIs(t, err, ErrInvalidInventoryFence)
	assert.False(t, applied)
	token, applied, err = s.BeginAttemptIfNotNewer(
		"lease-1", "backend-a", op1, startupFence,
	)
	require.NoError(t, err)
	assert.True(t, applied)
	require.True(t, token.Valid())
	assert.Equal(t, op1, token.OperationID())
	p := s.Lookup("lease-1")
	assert.Equal(t, StateAttempting, p.State())
	assert.True(t, p.RecordRevision().Valid())
	assert.Equal(t, op1, p.AttemptOperationID())

	settled, err := s.RefuseAttempt(token)
	require.NoError(t, err)
	assert.True(t, settled)
	s.InvalidateInventoryAuthority()
	token, err = s.BeginAttempt("lease-2", "backend-a", requireOperationID(t, "102"))
	require.ErrorIs(t, err, ErrInventoryNotReady)
	assert.False(t, token.Valid())

	// Re-establishing authority does not revive evidence minted before the
	// invalidation boundary.
	s.MarkInventoryReady()
	token, applied, err = s.BeginAttemptIfNotNewer(
		"lease-2", "backend-a", requireOperationID(t, "103"), startupFence,
	)
	require.ErrorIs(t, err, ErrInvalidInventoryFence)
	assert.False(t, applied)
	assert.False(t, token.Valid())
}

func TestStore_TypedAttemptSettlementRequiresExactToken(t *testing.T) {
	s := newTestStore(t)
	s.MarkInventoryReady()

	confirmed, err := s.ConfirmAttempt(AttemptToken{})
	require.ErrorIs(t, err, ErrInvalidAttemptToken)
	assert.False(t, confirmed)
	refused, err := s.RefuseAttempt(AttemptToken{})
	require.ErrorIs(t, err, ErrInvalidAttemptToken)
	assert.False(t, refused)

	first, err := s.BeginAttempt(
		"lease-1", "backend-a", requireOperationID(t, "201"),
	)
	require.NoError(t, err)
	require.True(t, first.Valid())
	refused, err = s.RefuseAttempt(first)
	require.NoError(t, err)
	assert.True(t, refused)

	secondID := requireOperationID(t, "202")
	second, err := s.BeginAttempt("lease-1", "backend-a", secondID)
	require.NoError(t, err)
	require.True(t, second.Valid())
	confirmed, err = s.ConfirmAttempt(first)
	require.NoError(t, err)
	assert.False(t, confirmed, "a stale token must not settle a later attempt")
	assert.Equal(t, secondID, s.Lookup("lease-1").AttemptOperationID())

	other := newTestStore(t)
	other.MarkInventoryReady()
	foreign, err := other.BeginAttempt(
		"lease-1", "backend-a", requireOperationID(t, "203"),
	)
	require.NoError(t, err)
	confirmed, err = s.ConfirmAttempt(foreign)
	require.ErrorIs(t, err, ErrInvalidAttemptToken)
	assert.False(t, confirmed, "a token from another store is not a capability here")
	assert.Equal(t, secondID, s.Lookup("lease-1").AttemptOperationID())

	confirmed, err = s.ConfirmAttempt(second)
	require.NoError(t, err)
	assert.True(t, confirmed)
	p := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, p.State())
	assert.Equal(t, "backend-a", p.Backend)
	assert.False(t, p.AttemptOperationID().Valid())
	confirmed, err = s.ConfirmAttempt(second)
	require.NoError(t, err)
	assert.False(t, confirmed, "a consumed token is stale")

	third, err := s.BeginAttempt(
		"lease-1", "backend-a", requireOperationID(t, "204"),
	)
	require.NoError(t, err)
	refused, err = s.RefuseAttempt(third)
	require.NoError(t, err)
	assert.True(t, refused)
	p = s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, p.State(),
		"refusing a new action must retain previously confirmed affinity")
	assert.False(t, p.AttemptOperationID().Valid())
}

func TestStore_TypedAttemptFenceRejectsForeignAndNewerRecords(t *testing.T) {
	s := newTestStore(t)
	s.MarkInventoryReady()
	stale := s.SnapshotFence()
	require.NoError(t, s.Confirm("lease-1", "backend-a"))

	token, applied, err := s.BeginAttemptIfNotNewer(
		"lease-1", "backend-a", requireOperationID(t, "251"), stale,
	)
	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Empty(t, s.Lookup("lease-1").Attempt)

	other := newTestStore(t)
	foreign := other.SnapshotFence()
	token, applied, err = s.BeginAttemptIfNotNewer(
		"lease-2", "backend-a", requireOperationID(t, "252"), foreign,
	)
	require.ErrorIs(t, err, ErrInvalidInventoryFence)
	assert.False(t, applied)
	assert.False(t, token.Valid())
}

func TestStore_CallbackOperationSettlementRequiresPersistedIdentity(t *testing.T) {
	s := newTestStore(t)
	s.MarkInventoryReady()
	firstID := requireOperationID(t, "271")
	secondID := requireOperationID(t, "272")

	_, err := s.BeginAttempt("lease-1", "backend-a", firstID)
	require.NoError(t, err)
	settled, err := s.ConfirmOperation("lease-1", "backend-a", secondID)
	require.NoError(t, err)
	assert.False(t, settled)
	settled, err = s.RefuseOperation("lease-1", "backend-a", secondID)
	require.NoError(t, err)
	assert.False(t, settled)
	assert.Equal(t, firstID, s.Lookup("lease-1").AttemptOperationID())

	settled, err = s.RefuseOperation("lease-1", "backend-a", firstID)
	require.NoError(t, err)
	assert.True(t, settled)
	_, err = s.BeginAttempt("lease-1", "backend-a", secondID)
	require.NoError(t, err)
	settled, err = s.ConfirmOperation("lease-1", "backend-a", firstID)
	require.NoError(t, err)
	assert.False(t, settled,
		"an older same-backend operation cannot settle the current attempt")
	assert.Equal(t, secondID, s.Lookup("lease-1").AttemptOperationID())

	settled, err = s.ConfirmOperation("lease-1", "backend-a", secondID)
	require.NoError(t, err)
	assert.True(t, settled)
	confirmed := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, confirmed.State())
	assert.False(t, confirmed.AttemptOperationID().Valid())
	beforeRevision := confirmed.RecordRevision()
	settled, err = s.ConfirmOperation("lease-1", "backend-a", firstID)
	require.NoError(t, err)
	assert.True(t, settled, "same-backend confirmation is idempotent after settlement")
	assert.Equal(t, beforeRevision, s.Lookup("lease-1").RecordRevision(),
		"idempotent success must not let an old ID mutate the record")

	_, err = s.BeginAttempt(
		"lease-1", "backend-a", requireOperationID(t, "273"),
	)
	require.NoError(t, err)
	settled, err = s.RefuseOperation(
		"lease-1", "backend-a", requireOperationID(t, "273"),
	)
	require.NoError(t, err)
	assert.True(t, settled)
	assert.Equal(t, StateConfirmed, s.Lookup("lease-1").State(),
		"a refusal must preserve previously confirmed ownership")
}

func TestStore_CallbackOperationSettlementRejectsInvalidAndLegacyAttempts(t *testing.T) {
	s := newTestStore(t)
	s.MarkInventoryReady()

	settled, err := s.ConfirmOperation(
		"lease-1", "backend-a", operation.OperationID{},
	)
	require.ErrorIs(t, err, operation.ErrInvalidID)
	assert.False(t, settled)
	settled, err = s.RefuseOperation(
		"lease-1", "backend-a", operation.OperationID{},
	)
	require.ErrorIs(t, err, operation.ErrInvalidID)
	assert.False(t, settled)

	requireSetAttempting(t, s, "legacy-attempt", "backend-a")
	legacy := s.Lookup("legacy-attempt")
	assert.False(t, legacy.AttemptOperationID().Valid())
	validID := requireOperationID(t, "281")
	settled, err = s.ConfirmOperation("legacy-attempt", "backend-a", validID)
	require.NoError(t, err)
	assert.False(t, settled)
	settled, err = s.RefuseOperation("legacy-attempt", "backend-a", validID)
	require.NoError(t, err)
	assert.False(t, settled)
	assert.Equal(t, legacy, s.Lookup("legacy-attempt"))
}

func TestStore_CallbackOperationSettlementSurvivesReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	opID := requireOperationID(t, "291")
	s1, err := NewStore(dbPath)
	require.NoError(t, err)
	s1.MarkInventoryReady()
	_, err = s1.BeginAttempt("lease-1", "backend-a", opID)
	require.NoError(t, err)
	require.NoError(t, s1.Close())

	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()
	settled, err := s2.ConfirmOperation("lease-1", "backend-a", opID)
	require.NoError(t, err)
	assert.True(t, settled)
	assert.Equal(t, StateConfirmed, s2.Lookup("lease-1").State())
	assert.False(t, s2.Lookup("lease-1").AttemptOperationID().Valid())
}

func TestStore_DeleteRecordRequiresExactTypedRevision(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	require.NoError(t, s.Confirm("lease-2", "backend-a"))
	lease1 := s.Lookup("lease-1")
	lease2 := s.Lookup("lease-2")
	require.True(t, lease1.RecordRevision().Valid())
	require.True(t, lease2.RecordRevision().Valid())

	deleted, err := s.DeleteRecord(RecordRevision{})
	require.ErrorIs(t, err, ErrInvalidRecordRevision)
	assert.False(t, deleted)

	other := newTestStore(t)
	require.NoError(t, other.Confirm("lease-1", "backend-a"))
	deleted, err = s.DeleteRecord(other.Lookup("lease-1").RecordRevision())
	require.ErrorIs(t, err, ErrInvalidRecordRevision)
	assert.False(t, deleted)

	deleted, err = s.DeleteRecord(lease2.RecordRevision())
	require.NoError(t, err)
	assert.True(t, deleted, "the capability derives its own lease target")
	assert.Equal(t, lease1, s.Lookup("lease-1"))
	assert.Equal(t, StateAbsent, s.Lookup("lease-2").State())

	deleted, err = s.DeleteRecord(lease1.RecordRevision())
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
	deleted, err = s.DeleteRecord(lease1.RecordRevision())
	require.NoError(t, err)
	assert.False(t, deleted)
}

func TestStore_TypedAttemptPersistsAcrossReopenButAuthorityDoesNot(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	opID := requireOperationID(t, "301")
	s1, err := NewStore(dbPath)
	require.NoError(t, err)
	s1.MarkInventoryReady()
	token, err := s1.BeginAttempt("lease-1", "backend-a", opID)
	require.NoError(t, err)
	require.True(t, token.Valid())
	wantRevision := s1.Lookup("lease-1").RecordRevision()
	require.True(t, wantRevision.Valid())
	require.NoError(t, s1.Close())
	var encoded []byte
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		encoded = append(encoded, tx.Bucket(bucketName).Get([]byte("lease-1"))...)
		return nil
	}))
	require.NoError(t, db.Close())
	assert.Contains(t, string(encoded), `"operation_id":"00000000-0000-4000-8000-00000000012d"`,
		"the durable operation identity is a canonical UUID string")

	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()
	p := s2.Lookup("lease-1")
	assert.Equal(t, StateAttempting, p.State())
	assert.Equal(t, "backend-a", p.Attempt)
	assert.Equal(t, opID, p.AttemptOperationID())
	assert.Equal(t, wantRevision.value, p.RecordRevision().value,
		"the durable record version survives restart")
	assert.NotEqual(t, wantRevision, p.RecordRevision(),
		"a reopened store mints a new process-local capability")
	deleted, err := s2.DeleteRecord(wantRevision)
	require.ErrorIs(t, err, ErrInvalidRecordRevision)
	assert.False(t, deleted, "a revision from the closed issuer is foreign")

	newToken, err := s2.BeginAttempt(
		"lease-2", "backend-a", requireOperationID(t, "302"),
	)
	require.ErrorIs(t, err, ErrInventoryNotReady,
		"a restart must re-establish complete fleet authority")
	assert.False(t, newToken.Valid())
	confirmed, err := s2.ConfirmAttempt(token)
	require.ErrorIs(t, err, ErrInvalidAttemptToken,
		"process-local capabilities are not transferable to a reopened store")
	assert.False(t, confirmed)
}

func TestStore_TypedAttemptUpgradesV013Record(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"lease-legacy":  []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
		"lease-attempt": []byte(`{"backend":"","attempt":"backend-b","set_at":"2026-08-25T15:00:00Z","revision":7}`),
	})

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	legacy := s.Lookup("lease-legacy")
	assert.Equal(t, StateConfirmed, legacy.State())
	assert.True(t, legacy.RecordRevision().Valid(),
		"opening the store must migrate an unambiguous v0.13 owner")
	assert.Greater(t, legacy.Revision(), uint64(7))
	assert.False(t, legacy.AttemptOperationID().Valid())
	oldAttempt := s.Lookup("lease-attempt")
	assert.Equal(t, StateAttempting, oldAttempt.State())
	assert.False(t, oldAttempt.AttemptOperationID().Valid(),
		"pre-token attempts remain readable and fail closed")

	opID := requireOperationID(t, "401")
	_, err = s.BeginAttempt("lease-legacy", "backend-a", opID)
	require.ErrorIs(t, err, ErrInventoryNotReady)
	s.MarkInventoryReady()
	token, err := s.BeginAttempt("lease-legacy", "backend-a", opID)
	require.NoError(t, err)
	require.True(t, token.Valid())
	upgraded := s.Lookup("lease-legacy")
	assert.True(t, upgraded.RecordRevision().Valid())
	assert.Equal(t, opID, upgraded.AttemptOperationID())
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	defer reopened.Close()
	upgraded = reopened.Lookup("lease-legacy")
	assert.Equal(t, StateConfirmed, upgraded.State())
	assert.Equal(t, "backend-a", upgraded.Attempt)
	assert.Equal(t, opID, upgraded.AttemptOperationID())
}

func TestStore_ProjectInventoryAcceptsExplicitRevisionZeroFence(t *testing.T) {
	s := newTestStore(t)
	fence := s.BeginInventorySession()
	defer s.EndInventorySession(fence)
	require.True(t, fence.Valid())

	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	require.NoError(t, err)
	revision, applied := result.Applied["lease-1"]
	assert.True(t, applied)
	assert.True(t, revision.Valid())
	assert.Equal(t, revision, s.Lookup("lease-1").RecordRevision())
	assert.Empty(t, result.Fenced)
}

func TestStore_ProjectInventoryUpgradesIdempotentV013RecordRevision(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"lease-legacy": []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	revision := s.Lookup("lease-legacy").RecordRevision()
	require.True(t, revision.Valid(),
		"opening the store must make legacy ownership immediately usable by typed CAS")
	fence := s.BeginInventorySession()

	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-legacy": "backend-a"},
	})
	require.NoError(t, err)
	assert.NotContains(t, result.Applied, "lease-legacy",
		"an exact projection after open-time migration is idempotent")
	assert.Equal(t, revision, s.Lookup("lease-legacy").RecordRevision())
	s.EndInventorySession(fence)
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	defer reopened.Close()
	reopenedRevision := reopened.Lookup("lease-legacy").RecordRevision()
	assert.Equal(t, revision.value, reopenedRevision.value)
	assert.NotEqual(t, revision, reopenedRevision,
		"record capabilities are rebound to the reopened store")
}

func TestStore_ProjectInventoryAppliesPositiveAndConflictOutcomesAtomically(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)

	requireSetAttempting(t, s, "mismatched", "backend-attempted")
	requireSetAttempting(t, s, "conflict", "backend-possible")
	fence := s.BeginInventorySession()

	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{
			"positive":   "backend-positive",
			"mismatched": "backend-replacement",
		},
		Conflicts: map[string][]string{
			"conflict": {"backend-a", "backend-b"},
		},
	})
	require.NoError(t, err)
	s.EndInventorySession(fence)
	assert.Empty(t, result.Fenced)
	for _, leaseUUID := range []string{"positive", "mismatched", "conflict"} {
		revision, ok := result.Applied[leaseUUID]
		require.True(t, ok, leaseUUID)
		assert.True(t, revision.Valid(), leaseUUID)
		assert.Equal(t, revision, s.Lookup(leaseUUID).RecordRevision(), leaseUUID)
	}

	positive := s.Lookup("positive")
	assert.Equal(t, StateConfirmed, positive.State())
	assert.Equal(t, "backend-positive", positive.Backend)
	mismatched := s.Lookup("mismatched")
	assert.Equal(t, StateConfirmed, mismatched.State())
	assert.Equal(t, "backend-replacement", mismatched.Backend)
	assert.Equal(t, "backend-attempted", mismatched.Attempt,
		"an observation from another backend cannot disprove the pending attempt")
	assert.False(t, mismatched.AttemptOperationID().Valid())
	conflict := s.Lookup("conflict")
	assert.Equal(t, StateUnusable, conflict.State())
	assert.ElementsMatch(t,
		[]string{"backend-a", "backend-b", "backend-possible"},
		conflict.ConflictBackends,
	)
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	defer reopened.Close()
	assert.Equal(t, "backend-positive", reopened.Lookup("positive").Backend)
	assert.Equal(t, "backend-replacement", reopened.Lookup("mismatched").Backend)
	assert.Equal(t, "backend-attempted", reopened.Lookup("mismatched").Attempt)
	assert.True(t, reopened.Lookup("conflict").Conflict)
}

func TestStore_ProjectInventoryPositiveExactObservationConfirmsAttempt(t *testing.T) {
	s := newTestStore(t)
	s.MarkInventoryReady()
	opID := requireOperationID(t, "491")
	_, err := s.BeginAttempt("lease-1", "backend-a", opID)
	require.NoError(t, err)
	fence := s.BeginInventorySession()
	defer s.EndInventorySession(fence)

	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	require.NoError(t, err)
	assert.Contains(t, result.Applied, "lease-1")
	assert.Empty(t, result.Fenced)

	confirmed := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, confirmed.State())
	assert.Equal(t, "backend-a", confirmed.Backend)
	assert.Empty(t, confirmed.Attempt)
	assert.False(t, confirmed.AttemptOperationID().Valid())
}

func TestStore_ProjectInventorySilencePreservesAttemptsAndConflicts(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")

	firstID := requireOperationID(t, "492")
	_, err := s.BeginAttempt("attempt-only", "backend-a", firstID)
	require.NoError(t, err)
	require.NoError(t, s.Confirm("confirmed-attempt", "backend-b"))
	secondID := requireOperationID(t, "493")
	_, err = s.BeginAttempt("confirmed-attempt", "backend-b", secondID)
	require.NoError(t, err)

	conflictFence := s.BeginInventorySession()
	_, err = s.ProjectInventory(conflictFence, InventoryProjection{
		Conflicts: map[string][]string{"conflict": {"backend-a", "backend-b"}},
	})
	s.EndInventorySession(conflictFence)
	require.NoError(t, err)

	before := s.List()
	beforeRevision := s.SnapshotRevision()
	silenceFence := s.BeginInventorySession()
	result, err := s.ProjectInventory(silenceFence, InventoryProjection{Complete: true})
	s.EndInventorySession(silenceFence)
	require.NoError(t, err)
	assert.Empty(t, result.Applied)
	assert.Empty(t, result.Fenced)
	assert.Equal(t, beforeRevision, s.SnapshotRevision())
	assert.Equal(t, before, s.List())
	assert.Equal(t, firstID, s.Lookup("attempt-only").AttemptOperationID())
	assert.Equal(t, secondID, s.Lookup("confirmed-attempt").AttemptOperationID())
	assert.True(t, s.Lookup("conflict").Conflict)
}

func TestStore_ProjectInventoryFencesInvalidForeignAndStaleEvidence(t *testing.T) {
	s := newTestStore(t)
	input := InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	}

	_, err := s.ProjectInventory(InventoryFence{}, input)
	require.ErrorIs(t, err, ErrInvalidInventoryFence)
	other := newTestStore(t)
	_, err = s.ProjectInventory(other.SnapshotFence(), input)
	require.ErrorIs(t, err, ErrInvalidInventoryFence)

	invalidated := s.SnapshotFence()
	s.InvalidateInventoryAuthority()
	_, err = s.ProjectInventory(invalidated, input)
	require.ErrorIs(t, err, ErrInvalidInventoryFence)
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())

	fence := s.BeginInventorySession()
	require.NoError(t, s.Confirm("lease-newer", "backend-current"))
	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-newer": "backend-stale"},
	})
	require.NoError(t, err)
	assert.Contains(t, result.Fenced, "lease-newer")
	assert.Equal(t, "backend-current", s.Lookup("lease-newer").Backend)
	s.EndInventorySession(fence)
}

func TestStore_ProjectInventoryRollsBackEveryKeyWhenBoltRejectsOne(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	fence := s.BeginInventorySession()
	beforeRevision := s.SnapshotRevision()
	oversizedLease := strings.Repeat("z", bolt.MaxKeySize+1)

	_, err = s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{
			"a-valid":      "backend-a",
			oversizedLease: "backend-b",
		},
	})
	require.Error(t, err)
	assert.Equal(t, beforeRevision, s.SnapshotRevision())
	assert.Equal(t, StateAbsent, s.Lookup("a-valid").State(),
		"the earlier bbolt Put must roll back with the later rejected key")
	assert.Equal(t, StateAbsent, s.Lookup(oversizedLease).State())
	s.EndInventorySession(fence)
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	defer reopened.Close()
	assert.Equal(t, StateAbsent, reopened.Lookup("a-valid").State())
	assert.Equal(t, StateAbsent, reopened.Lookup(oversizedLease).State())
}

func TestStore_ProjectInventoryVerifiesEmptyAndIdempotentProjection(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	fence := s.SnapshotFence()
	beforeRevision := s.SnapshotRevision()

	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	require.NoError(t, err)
	assert.Empty(t, result.Applied)
	assert.Empty(t, result.Fenced)
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	require.NoError(t, s.Close())
	_, err = s.ProjectInventory(fence, InventoryProjection{})
	require.Error(t, err,
		"an empty projection must still prove the durable store is readable")
}

func TestStore_BeginAttemptFromProjectionAcceptsRevisionZeroAbsenceProof(t *testing.T) {
	s := newTestStore(t)
	fence := s.BeginInventorySession()
	proof, err := s.ProjectInventory(fence, InventoryProjection{})
	require.NoError(t, err)
	require.True(t, proof.Valid())
	assert.Empty(t, proof.Applied)
	s.MarkInventoryReady()

	token, applied, err := s.BeginAttemptFromProjection(
		"lease-1", "backend-a", requireOperationID(t, "501"), proof,
	)
	require.NoError(t, err)
	assert.True(t, applied)
	assert.True(t, token.Valid())
	assert.Equal(t, StateAttempting, s.Lookup("lease-1").State())
	s.EndInventorySession(fence)
}

func TestStore_BeginAttemptFromProjectionAcceptsAppliedAndNoopRecords(t *testing.T) {
	t.Run("applied", func(t *testing.T) {
		s := newTestStore(t)
		fence := s.BeginInventorySession()
		proof, err := s.ProjectInventory(fence, InventoryProjection{
			Placements: map[string]string{"lease-1": "backend-a"},
		})
		require.NoError(t, err)
		require.Contains(t, proof.Applied, "lease-1")
		s.MarkInventoryReady()

		token, applied, err := s.BeginAttemptFromProjection(
			"lease-1", "backend-a", requireOperationID(t, "511"), proof,
		)
		require.NoError(t, err)
		assert.True(t, applied)
		assert.True(t, token.Valid())
		s.EndInventorySession(fence)
	})

	t.Run("idempotent no-op", func(t *testing.T) {
		s := newTestStore(t)
		require.NoError(t, s.Confirm("lease-1", "backend-a"))
		fence := s.BeginInventorySession()
		proof, err := s.ProjectInventory(fence, InventoryProjection{
			Placements: map[string]string{"lease-1": "backend-a"},
		})
		require.NoError(t, err)
		assert.NotContains(t, proof.Applied, "lease-1")
		s.MarkInventoryReady()

		token, applied, err := s.BeginAttemptFromProjection(
			"lease-1", "backend-a", requireOperationID(t, "512"), proof,
		)
		require.NoError(t, err)
		assert.True(t, applied)
		assert.True(t, token.Valid())
		s.EndInventorySession(fence)
	})
}

func TestStore_BeginAttemptFromProjectionRejectsZeroForeignAndOldEpochProof(t *testing.T) {
	s := newTestStore(t)
	s.MarkInventoryReady()
	token, applied, err := s.BeginAttemptFromProjection(
		"lease-zero", "backend-a", requireOperationID(t, "531"), ProjectionResult{},
	)
	require.ErrorIs(t, err, ErrInvalidProjectionProof)
	assert.False(t, applied)
	assert.False(t, token.Valid())

	other := newTestStore(t)
	otherFence := other.BeginInventorySession()
	foreign, err := other.ProjectInventory(otherFence, InventoryProjection{})
	require.NoError(t, err)
	token, applied, err = s.BeginAttemptFromProjection(
		"lease-foreign", "backend-a", requireOperationID(t, "532"), foreign,
	)
	require.ErrorIs(t, err, ErrInvalidProjectionProof)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	other.EndInventorySession(otherFence)

	fence := s.BeginInventorySession()
	oldEpoch, err := s.ProjectInventory(fence, InventoryProjection{})
	require.NoError(t, err)
	s.EndInventorySession(fence)
	s.InvalidateInventoryAuthority()
	s.MarkInventoryReady()
	token, applied, err = s.BeginAttemptFromProjection(
		"lease-old-epoch", "backend-a", requireOperationID(t, "533"), oldEpoch,
	)
	require.ErrorIs(t, err, ErrInvalidProjectionProof)
	assert.False(t, applied)
	assert.False(t, token.Valid())
}

func TestStore_BeginAttemptFromProjectionRejectsLeaseMutationAfterProjection(t *testing.T) {
	s := newTestStore(t)
	fence := s.BeginInventorySession()
	proof, err := s.ProjectInventory(fence, InventoryProjection{})
	require.NoError(t, err)
	require.NoError(t, s.Confirm("lease-mutated", "backend-a"))
	require.NoError(t, s.Confirm("unrelated", "backend-a"))
	s.MarkInventoryReady()

	token, applied, err := s.BeginAttemptFromProjection(
		"lease-mutated", "backend-a", requireOperationID(t, "541"), proof,
	)
	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Empty(t, s.Lookup("lease-mutated").Attempt)

	token, applied, err = s.BeginAttemptFromProjection(
		"lease-unmodified", "backend-a", requireOperationID(t, "542"), proof,
	)
	require.NoError(t, err)
	assert.True(t, applied,
		"an unrelated lease mutation must not invalidate a lease-local proof")
	assert.True(t, token.Valid())
	s.EndInventorySession(fence)
}

func TestStore_BeginAttemptFromProjectionDoesNotTrustDiagnosticMaps(t *testing.T) {
	s := newTestStore(t)
	fence := s.BeginInventorySession()
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	proof, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	require.NoError(t, err)
	require.Contains(t, proof.Fenced, "lease-1")

	// These maps are diagnostics for planners and logs, not capabilities. Even
	// code that edits them cannot forge away the private per-lease fence.
	delete(proof.Fenced, "lease-1")
	proof.Applied["lease-1"] = s.Lookup("lease-1").RecordRevision()
	s.MarkInventoryReady()
	token, applied, err := s.BeginAttemptFromProjection(
		"lease-1", "backend-a", requireOperationID(t, "551"), proof,
	)
	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Empty(t, s.Lookup("lease-1").Attempt)
	s.EndInventorySession(fence)
}

func TestStore_ConfirmAbsentCreatesPositiveObservation(t *testing.T) {
	fixed := time.Date(2026, 8, 25, 17, 0, 0, 0, time.UTC)
	s := newTestStore(t, WithClock(func() time.Time { return fixed }))

	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	p := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, p.State())
	assert.Equal(t, "backend-a", p.Backend)
	assert.Empty(t, p.Attempt)
	assert.Equal(t, fixed, p.SetAt)
	assert.NotZero(t, p.Revision())
}

func TestStore_SetAttemptingIfNotNewerFencesPostSnapshotMutation(t *testing.T) {
	s := newTestStore(t)
	cutoff := s.SnapshotRevision()
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	changed := s.Lookup("lease-1")

	revision, set, err := s.SetAttemptingIfNotNewer("lease-1", "backend-a", cutoff)
	require.NoError(t, err)
	assert.False(t, set)
	assert.Zero(t, revision)
	assert.Equal(t, changed, s.Lookup("lease-1"),
		"a stale reconciler snapshot must not write an attempt")

	revision, set, err = s.SetAttemptingIfNotNewer(
		"lease-1", "backend-a", changed.Revision(),
	)
	require.NoError(t, err)
	require.True(t, set)
	assert.Equal(t, revision, s.Lookup("lease-1").Revision())
	assert.Equal(t, "backend-a", s.Lookup("lease-1").Attempt)
}

func TestStore_SetAttemptingIfNotNewerFencesPostSnapshotCreateDelete(t *testing.T) {
	s := newTestStore(t)
	cutoff := s.BeginInventorySnapshot()
	defer s.EndInventorySnapshot(cutoff)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	require.NoError(t, s.Delete("lease-1"))
	require.Equal(t, StateAbsent, s.Lookup("lease-1").State())

	revision, set, err := s.SetAttemptingIfNotNewer("lease-1", "backend-a", cutoff)
	require.NoError(t, err)
	assert.False(t, set)
	assert.Zero(t, revision)
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State(),
		"an absent key must retain its deletion fence for the active inventory")
}

func TestStore_ClearAttemptOnlyDeletesRecord(t *testing.T) {
	s := newTestStore(t)

	requireSetAttempting(t, s, "lease-1", "backend-a")
	require.NoError(t, s.ClearAttempt("lease-1", "backend-a"))
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
	assert.Empty(t, s.List())

	// Clearing an already-absent matching attempt is idempotent.
	require.NoError(t, s.ClearAttempt("lease-1", "backend-a"))
}

func TestStore_PersistsAllStatesAndRevisions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	fixed := time.Date(2026, 8, 25, 17, 11, 15, 0, time.UTC)

	s1, err := NewStore(dbPath, WithClock(func() time.Time { return fixed }))
	require.NoError(t, err)
	requireSetAttempting(t, s1, "attempting", "backend-a")
	require.NoError(t, s1.Confirm("confirmed", "backend-b"))
	require.NoError(t, s1.Confirm("confirmed-attempt", "backend-c"))
	requireSetAttempting(t, s1, "confirmed-attempt", "backend-c")
	want := s1.List()
	wantRevision := s1.SnapshotRevision()
	require.NoError(t, s1.Close())

	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	got := s2.List()
	for leaseUUID, p := range want {
		p.recordRevision = RecordRevision{}
		want[leaseUUID] = p
	}
	for leaseUUID, p := range got {
		p.recordRevision = RecordRevision{}
		got[leaseUUID] = p
	}
	assert.Equal(t, want, got,
		"durable placement facts persist while process-local capabilities rebind")
	assert.Equal(t, wantRevision, s2.SnapshotRevision())
	assert.Equal(t, StateAttempting, s2.Lookup("attempting").State())
	assert.Equal(t, StateConfirmed, s2.Lookup("confirmed").State())
	assert.Equal(t, "backend-c", s2.Lookup("confirmed-attempt").Attempt)
}

func TestStore_DeletePersists(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s1, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s1.Confirm("lease-1", "backend-a"))
	require.NoError(t, s1.Confirm("lease-2", "backend-b"))
	require.NoError(t, s1.Delete("lease-1"))
	require.NoError(t, s1.Close())

	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()
	assert.Equal(t, StateAbsent, s2.Lookup("lease-1").State())
	assert.Equal(t, "backend-b", s2.Lookup("lease-2").Backend)
	assert.Len(t, s2.List(), 1)
}

func TestStore_SetBatchMergesInventoryAtomically(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	clock := &fakeClock{now: t0}
	s := newTestStore(t, WithClock(clock.Now))

	require.NoError(t, s.Confirm("confirmed", "backend-a"))
	requireSetAttempting(t, s, "attempting", "backend-a")
	require.NoError(t, s.Confirm("mixed", "backend-a"))
	requireSetAttempting(t, s, "mixed", "backend-a")
	cutoff := s.SnapshotRevision()

	clock.now = t1
	applied := requireSetBatchIfNotNewer(t, s, map[string]string{
		"confirmed":  "backend-b", // positive inventory replaces old affinity
		"attempting": "backend-a", // matching inventory confirms the attempt
		"mixed":      "backend-b", // mismatched attempt remains unresolved
		"new":        "backend-c",
	}, math.MaxUint64)

	confirmed := s.Lookup("confirmed")
	assert.Equal(t, "backend-b", confirmed.Backend)
	assert.Empty(t, confirmed.Attempt)
	assert.Equal(t, t0, confirmed.SetAt)

	attempting := s.Lookup("attempting")
	assert.Equal(t, StateConfirmed, attempting.State())
	assert.Equal(t, "backend-a", attempting.Backend)
	assert.Empty(t, attempting.Attempt)
	assert.Equal(t, t0, attempting.SetAt)

	mixed := s.Lookup("mixed")
	assert.Equal(t, StateConfirmed, mixed.State())
	assert.Equal(t, "backend-b", mixed.Backend)
	assert.Equal(t, "backend-a", mixed.Attempt)
	assert.Equal(t, t0, mixed.SetAt)

	added := s.Lookup("new")
	assert.Equal(t, StateConfirmed, added.State())
	assert.Equal(t, "backend-c", added.Backend)
	assert.Equal(t, t1, added.SetAt)

	for leaseUUID, p := range s.List() {
		assert.Greater(t, p.Revision(), cutoff, leaseUUID)
		assert.Equal(t, p.Revision(), applied[leaseUUID], leaseUUID)
	}
	assert.Equal(t, s.Lookup("new").Revision(), s.SnapshotRevision(),
		"sorted batch keys make the final revision deterministic")

	before := s.List()
	beforeRevision := s.SnapshotRevision()
	_, _, err := s.SetBatchIfNotNewer(map[string]string{"valid": "backend-a", "invalid": ""}, math.MaxUint64)
	require.ErrorIs(t, err, ErrInvalidPlacement)
	assert.Equal(t, before, s.List(), "validation failure must reject the whole batch")
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	assert.Empty(t, requireSetBatchIfNotNewer(t, s, nil, math.MaxUint64))
	assert.Equal(t, beforeRevision, s.SnapshotRevision())
}

func TestStore_SetBatchIfNotNewerPreservesConcurrentAttempt(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("old-record", "backend-a"))
	cutoff := s.SnapshotRevision()

	// This write models a provision/restore that starts after backend inventory
	// fetching began. Even a matching stale positive must not clear its attempt.
	requireSetAttempting(t, s, "new-attempt", "backend-b")
	newAttempt := s.Lookup("new-attempt")
	require.Greater(t, newAttempt.Revision(), cutoff)

	requireSetBatchIfNotNewer(t, s, map[string]string{
		"old-record":  "backend-b",
		"new-attempt": "backend-b",
	}, cutoff)

	old := s.Lookup("old-record")
	assert.Equal(t, "backend-b", old.Backend, "records covered by the inventory still converge")
	assert.Greater(t, old.Revision(), cutoff)
	assert.Equal(t, newAttempt, s.Lookup("new-attempt"),
		"inventory fetched before the attempt must not confirm or overwrite it")
}

func TestStore_SetBatchIfNotNewerDoesNotReviseExactConfirmedObservation(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	before := s.Lookup("lease-1")
	beforeGlobal := s.SnapshotRevision()

	applied := requireSetBatchIfNotNewer(t, s,
		map[string]string{"lease-1": "backend-a"}, beforeGlobal,
	)
	assert.Empty(t, applied)
	assert.Equal(t, before, s.Lookup("lease-1"))
	assert.Equal(t, beforeGlobal, s.SnapshotRevision(),
		"an unchanged inventory must not keep the record newer than every sweep cutoff")

	applied = requireSetBatchIfNotNewer(t, s,
		map[string]string{"lease-1": "backend-b"}, beforeGlobal,
	)
	after := s.Lookup("lease-1")
	assert.Equal(t, after.Revision(), applied["lease-1"])
	assert.Equal(t, "backend-b", after.Backend)
	assert.Greater(t, after.Revision(), before.Revision(),
		"a real ownership transition must still advance the revision")
}

func TestStore_SetBatchIfNotNewerPreservesSetAtOnOverwrite(t *testing.T) {
	s := newTestStore(t)

	requireSetBatchIfNotNewer(t, s,
		map[string]string{"lease-1": "backend-a"}, math.MaxUint64,
	)
	firstSetAt := s.Lookup("lease-1").SetAt
	requireSetBatchIfNotNewer(t, s,
		map[string]string{"lease-1": "backend-b"}, math.MaxUint64,
	)
	current := s.Lookup("lease-1")
	assert.Equal(t, "backend-b", current.Backend)
	assert.Equal(t, firstSetAt, current.SetAt)
}

func TestStore_OpenMigratesLegacyRawAndJSONRecordsMonotonically(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	setAt := "2026-08-25T15:00:00Z"
	writeRawRecords(t, dbPath, map[string][]byte{
		"already-revised": []byte(`{"backend":"backend-current","set_at":"2026-08-24T15:00:00Z","revision":11}`),
		"legacy":          []byte("backend-legacy"),
		"old-json":        []byte(`{"backend":"backend-json","set_at":"` + setAt + `"}`),
	})

	s, err := NewStore(dbPath)
	require.NoError(t, err)

	legacy := s.Lookup("legacy")
	assert.Equal(t, StateConfirmed, legacy.State())
	assert.Equal(t, "backend-legacy", legacy.Backend)
	assert.True(t, legacy.SetAt.IsZero())
	assert.Equal(t, uint64(12), legacy.Revision(),
		"migrated revisions start above the existing durable clock")
	assert.True(t, legacy.RecordRevision().Valid())

	oldJSON := s.Lookup("old-json")
	assert.Equal(t, StateConfirmed, oldJSON.State())
	assert.Equal(t, "backend-json", oldJSON.Backend)
	assert.Equal(t, setAt, oldJSON.SetAt.Format(time.RFC3339))
	assert.Equal(t, uint64(13), oldJSON.Revision())
	assert.True(t, oldJSON.RecordRevision().Valid())
	assert.Equal(t, uint64(11), s.Lookup("already-revised").Revision())
	assert.Equal(t, uint64(13), s.SnapshotRevision())

	require.NoError(t, s.Close())
	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	defer reopened.Close()
	assert.Equal(t, uint64(12), reopened.Lookup("legacy").Revision())
	assert.Equal(t, "backend-legacy", reopened.Lookup("legacy").Backend)
	assert.True(t, reopened.Lookup("legacy").SetAt.IsZero())
	assert.Equal(t, uint64(13), reopened.Lookup("old-json").Revision())
	assert.Equal(t, setAt, reopened.Lookup("old-json").SetAt.Format(time.RFC3339))
	assert.Equal(t, uint64(13), reopened.SnapshotRevision(),
		"reopening an already migrated store is idempotent")
}

func TestStore_OpenMigrationLeavesAmbiguousAndUnusableRecordsUntouched(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	records := map[string][]byte{
		"confirmed": []byte(`{"backend":"backend-confirmed","set_at":"2026-08-25T15:00:00Z"}`),
		"confirmed-with-attempt": []byte(
			`{"backend":"backend-owner","attempt":"backend-target","set_at":"2026-08-25T15:00:00Z"}`,
		),
		"attempt-only": []byte(`{"attempt":"backend-target","set_at":"2026-08-25T15:00:00Z"}`),
		"conflict": []byte(
			`{"backend":"backend-owner","set_at":"2026-08-25T15:00:00Z","conflict":true,"conflict_backends":["backend-owner","backend-other"]}`,
		),
		"operation-without-attempt": []byte(
			`{"backend":"backend-owner","operation_id":"00000000-0000-4000-8000-000000000001","set_at":"2026-08-25T15:00:00Z"}`,
		),
		"malformed-json": []byte(`{"backend":`),
		"invalid-raw":    {0xff, 0xfe},
	}
	writeRawRecords(t, dbPath, records)

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	assert.True(t, s.Lookup("confirmed").RecordRevision().Valid())
	assert.Equal(t, StateConfirmed, s.Lookup("confirmed-with-attempt").State())
	assert.False(t, s.Lookup("confirmed-with-attempt").RecordRevision().Valid())
	assert.Equal(t, StateAttempting, s.Lookup("attempt-only").State())
	assert.False(t, s.Lookup("attempt-only").RecordRevision().Valid())
	for _, leaseUUID := range []string{
		"conflict", "operation-without-attempt", "malformed-json", "invalid-raw",
	} {
		assert.Equal(t, StateUnusable, s.Lookup(leaseUUID).State(), leaseUUID)
		assert.False(t, s.Lookup(leaseUUID).RecordRevision().Valid(), leaseUUID)
	}
	require.NoError(t, s.Close())

	unsafeKeys := []string{
		"confirmed-with-attempt", "attempt-only", "conflict",
		"operation-without-attempt", "malformed-json", "invalid-raw",
	}
	durable := readRawRecords(t, dbPath, unsafeKeys...)
	for _, leaseUUID := range unsafeKeys {
		assert.Equal(t, records[leaseUUID], durable[leaseUUID], leaseUUID)
	}
}

func TestStore_OpenMigrationRollsBackAllRecordsOnRevisionExhaustion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	records := map[string][]byte{
		"a-legacy": []byte("backend-a"),
		"b-legacy": []byte(`{"backend":"backend-b","set_at":"2026-08-25T15:00:00Z"}`),
		"z-revised": []byte(fmt.Sprintf(
			`{"backend":"backend-z","set_at":"2026-08-25T15:00:00Z","revision":%d}`,
			uint64(math.MaxUint64-1),
		)),
	}
	writeRawRecords(t, dbPath, records)

	s, err := NewStore(dbPath)
	require.ErrorContains(t, err, "placement revision exhausted during legacy migration")
	assert.Nil(t, s)

	durable := readRawRecords(t, dbPath, "a-legacy", "b-legacy", "z-revised")
	assert.Equal(t, records, durable,
		"a later migration failure must roll back every earlier record rewrite")
}

func TestStore_LegacyRawBackendNamesRequirePrintableUTF8(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	valid := map[string]string{
		"ascii":   "backend-legacy_01",
		"unicode": "backend-montréal-東京",
	}
	writeRawRecords(t, dbPath, map[string][]byte{
		"ascii":        []byte(valid["ascii"]),
		"unicode":      []byte(valid["unicode"]),
		"invalid-utf8": {0xff, 0xfe},
		"nul":          []byte("backend\x00hidden"),
		"newline":      []byte("backend-a\nbackend-b"),
		"escape":       []byte("backend-\x1b[31m"),
		"zero-width":   []byte("backend-\u200bhidden"),
	})

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	for leaseUUID, backendName := range valid {
		p := s.Lookup(leaseUUID)
		assert.Equal(t, StateConfirmed, p.State(), leaseUUID)
		assert.Equal(t, backendName, p.Backend, leaseUUID)
		assert.NotZero(t, p.Revision(), leaseUUID)
		assert.True(t, p.RecordRevision().Valid(), leaseUUID)
	}
	for _, leaseUUID := range []string{"invalid-utf8", "nul", "newline", "escape", "zero-width"} {
		p := s.Lookup(leaseUUID)
		assert.Equal(t, StateUnusable, p.State(), leaseUUID)
		assert.Empty(t, p.Backend, leaseUUID)
		assert.Contains(t, s.List(), leaseUUID,
			"invalid legacy bytes must remain present and fail closed")
		_, err := s.SetAttempting(leaseUUID, "backend-a")
		require.ErrorIs(t, err, ErrUnusablePlacement, leaseUUID)
	}
}

func TestStore_MalformedRecordsRemainUnusable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"empty":            {},
		"malformed-json":   []byte(`{not-json`),
		"empty-object":     []byte(`{}`),
		"structured-empty": []byte(`{"backend":"","set_at":"2026-08-25T15:00:00Z","revision":7}`),
		"legacy":           []byte("backend-legacy"),
	})

	fixed := time.Date(2026, 8, 25, 18, 0, 0, 0, time.UTC)
	s, err := NewStore(dbPath, WithClock(func() time.Time { return fixed }))
	require.NoError(t, err)
	defer s.Close()

	for _, leaseUUID := range []string{"empty", "malformed-json", "empty-object", "structured-empty"} {
		assert.Equal(t, StateUnusable, s.Lookup(leaseUUID).State(), leaseUUID)
		assert.Contains(t, s.List(), leaseUUID, "unusable records remain visible in snapshots")
	}
	assert.Len(t, s.List(), 5)
	assert.Equal(t, uint64(8), s.SnapshotRevision(),
		"the valid legacy owner is migrated above the existing revision")
	assert.Equal(t, uint64(7), s.Lookup("structured-empty").Revision())

	_, err = s.SetAttempting("empty-object", "backend-a")
	require.ErrorIs(t, err, ErrUnusablePlacement)
	err = s.Confirm("empty-object", "backend-a")
	require.ErrorIs(t, err, ErrUnusablePlacement)
	err = s.ClearAttempt("empty-object", "backend-a")
	require.ErrorIs(t, err, ErrUnusablePlacement)

	// Positive fleet inventory may repair a corrupt derived-index entry.
	requireSetBatchIfNotNewer(t, s,
		map[string]string{"empty-object": "backend-a"}, math.MaxUint64,
	)
	repaired := s.Lookup("empty-object")
	assert.Equal(t, StateConfirmed, repaired.State())
	assert.Equal(t, "backend-a", repaired.Backend)
	assert.Equal(t, fixed, repaired.SetAt)
	assert.Greater(t, repaired.Revision(), uint64(7))

	deleted, err := s.DeleteIfRevision("malformed-json", 0)
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Equal(t, StateAbsent, s.Lookup("malformed-json").State())
	require.NoError(t, s.Delete("structured-empty"))
}

func TestStore_ListReturnsIndependentAtomicSnapshot(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	requireSetAttempting(t, s, "lease-2", "backend-b")

	snapshot := s.List()
	assert.Len(t, snapshot, 2)
	delete(snapshot, "lease-1")
	p := snapshot["lease-2"]
	p.Attempt = "mutated"
	snapshot["lease-2"] = p

	assert.Equal(t, "backend-a", s.Lookup("lease-1").Backend)
	assert.Equal(t, "backend-b", s.Lookup("lease-2").Attempt)
	assert.Len(t, s.List(), 2)
}

func TestStore_IdempotentConflictSyncDoesNotAdvanceRevision(t *testing.T) {
	s := newTestStore(t)
	applied, fenced, err := s.SetConflictsIfNotNewer(map[string][]string{
		"lease-1": {"backend-b", "backend-a", "backend-a"},
	}, s.SnapshotRevision())
	require.NoError(t, err)
	require.Empty(t, fenced)
	require.Contains(t, applied, "lease-1")
	before := s.Lookup("lease-1")
	beforeRevision := s.SnapshotRevision()

	applied, fenced, err = s.SetConflictsIfNotNewer(map[string][]string{
		"lease-1": {"backend-a", "backend-b"},
	}, beforeRevision)
	require.NoError(t, err)
	assert.Empty(t, fenced)
	assert.Empty(t, applied, "the same normalized quarantine must be a durable no-op")
	assert.Equal(t, beforeRevision, s.SnapshotRevision())
	assert.Equal(t, before, s.Lookup("lease-1"))

	applied, fenced, err = s.SetConflictsIfNotNewer(map[string][]string{
		"lease-1": {"backend-a", "backend-b", "backend-c"},
	}, beforeRevision)
	require.NoError(t, err)
	assert.Empty(t, fenced)
	assert.Contains(t, applied, "lease-1")
	assert.Equal(t, beforeRevision+1, s.SnapshotRevision())
	assert.Equal(t, []string{"backend-a", "backend-b", "backend-c"},
		s.Lookup("lease-1").ConflictBackends)
}

func TestStore_DurableWriteFailuresLeaveCacheAndRevisionUnchanged(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("confirmed", "backend-a"))
	require.Empty(t, requireSetConflictsIfNotNewer(t, s, map[string][]string{
		"conflict": {"backend-a", "backend-b"},
	}, s.SnapshotRevision()))
	before := s.List()
	beforeRevision := s.SnapshotRevision()
	beforeDeleteRevisions := len(s.deleteRevisions)
	require.NoError(t, s.Close())

	_, err := s.SetAttempting("confirmed", "backend-a")
	require.Error(t, err)
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	err = s.Confirm("new", "backend-b")
	require.Error(t, err)
	assert.Equal(t, StateAbsent, s.Lookup("new").State())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	_, _, err = s.SetBatchIfNotNewer(map[string]string{"batch": "backend-c"}, math.MaxUint64)
	require.Error(t, err)
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	_, _, err = s.SetBatchIfNotNewer(nil, math.MaxUint64)
	require.Error(t, err, "an empty inventory must not report a durable sync against a closed store")
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	_, fenced, err := s.SetConflictsIfNotNewer(map[string][]string{
		"conflict": {"backend-b", "backend-a"},
	}, math.MaxUint64)
	require.Error(t, err, "an idempotent conflict must still verify the durable store")
	assert.Empty(t, fenced)
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	err = s.Delete("confirmed")
	require.Error(t, err)
	assert.Equal(t, before, s.List(), "failed durable delete must not evict the cache")

	deleted, err := s.DeleteIfRevision("confirmed", before["confirmed"].Revision())
	require.Error(t, err)
	assert.False(t, deleted)
	assert.Equal(t, before, s.List())

	err = s.ClearConflictsIfNotNewer(
		map[string]struct{}{"conflict": {}}, beforeRevision,
	)
	require.Error(t, err)
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())
	assert.Equal(t, beforeDeleteRevisions, len(s.deleteRevisions),
		"failed durable deletes must not advance the stale-snapshot barrier")
}

func TestStore_FailedClearLeavesAttemptCached(t *testing.T) {
	s := newTestStore(t)
	requireSetAttempting(t, s, "lease-1", "backend-a")
	before := s.Lookup("lease-1")
	beforeRevision := s.SnapshotRevision()
	beforeDeleteRevisions := len(s.deleteRevisions)
	require.NoError(t, s.Close())

	err := s.ClearAttempt("lease-1", "backend-a")
	require.Error(t, err)
	assert.Equal(t, before, s.Lookup("lease-1"))

	cleared, err := s.ClearAttemptIfRevision("lease-1", "backend-a", before.Revision())
	require.Error(t, err)
	assert.False(t, cleared)
	assert.Equal(t, before, s.Lookup("lease-1"))
	assert.Equal(t, beforeRevision, s.SnapshotRevision())
	assert.Equal(t, beforeDeleteRevisions, len(s.deleteRevisions))
}

func TestStore_EncodingFailureLeavesCacheAndRevisionUnchanged(t *testing.T) {
	invalidJSONTime := time.Date(10_000, 1, 1, 0, 0, 0, 0, time.UTC)
	s := newTestStore(t, WithClock(func() time.Time { return invalidJSONTime }))

	err := s.Confirm("lease-1", "backend-a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encode placement")
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
	assert.Zero(t, s.SnapshotRevision())

	_, _, err = s.SetBatchIfNotNewer(map[string]string{"lease-2": "backend-b"}, math.MaxUint64)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encode batch placements")
	assert.Equal(t, StateAbsent, s.Lookup("lease-2").State())
	assert.Zero(t, s.SnapshotRevision())
}

func TestStore_SemanticFailuresDoNotMutate(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	requireSetAttempting(t, s, "lease-1", "backend-a")
	before := s.List()
	beforeRevision := s.SnapshotRevision()

	for _, err := range []error{
		setAttemptingErr(s, "lease-1", "backend-a"),
		setAttemptingErr(s, "lease-1", "backend-b"),
		s.Confirm("lease-1", "backend-b"),
		s.ClearAttempt("lease-1", "backend-b"),
		setAttemptingErr(s, "", "backend-a"),
		s.Confirm("lease-2", ""),
	} {
		require.Error(t, err)
	}
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())
}

func TestStore_RevisionConditionalMutationsRejectStaleSnapshots(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	confirmed := s.Lookup("lease-1")

	requireSetAttempting(t, s, "lease-1", "backend-a")
	withAttempt := s.Lookup("lease-1")
	require.Greater(t, withAttempt.Revision(), confirmed.Revision())

	deleted, err := s.DeleteIfRevision("lease-1", confirmed.Revision())
	require.NoError(t, err)
	assert.False(t, deleted)
	assert.Equal(t, withAttempt, s.Lookup("lease-1"))

	cleared, err := s.ClearAttemptIfRevision(
		"lease-1", "backend-a", confirmed.Revision(),
	)
	require.NoError(t, err)
	assert.False(t, cleared)
	assert.Equal(t, withAttempt, s.Lookup("lease-1"))

	cleared, err = s.ClearAttemptIfRevision(
		"lease-1", "backend-b", withAttempt.Revision(),
	)
	require.ErrorIs(t, err, ErrAttemptMismatch)
	assert.False(t, cleared)

	cleared, err = s.ClearAttemptIfRevision(
		"lease-1", "backend-a", withAttempt.Revision(),
	)
	require.NoError(t, err)
	assert.True(t, cleared)
	current := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, current.State())
	assert.Empty(t, current.Attempt)
	assert.Greater(t, current.Revision(), withAttempt.Revision())

	deleted, err = s.DeleteIfRevision("lease-1", withAttempt.Revision())
	require.NoError(t, err)
	assert.False(t, deleted)
	deleted, err = s.DeleteIfRevision("lease-1", current.Revision())
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
}

func TestStore_ConfirmAttemptIfRevisionNeverRecreatesOrOverwrites(t *testing.T) {
	s := newTestStore(t)
	requireSetAttempting(t, s, "lease-1", "backend-a")
	first := s.Lookup("lease-1")

	// A fast failure callback can delete the attempt before the synchronous 202
	// response is delivered. Its stale settlement must not recreate ownership.
	require.NoError(t, s.Delete("lease-1"))
	confirmed, err := s.ConfirmAttemptIfRevision("lease-1", "backend-a", first.Revision())
	require.NoError(t, err)
	assert.False(t, confirmed)
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())

	// Nor may an older response overwrite a newer attempt.
	requireSetAttempting(t, s, "lease-1", "backend-b")
	newer := s.Lookup("lease-1")
	confirmed, err = s.ConfirmAttemptIfRevision("lease-1", "backend-a", first.Revision())
	require.NoError(t, err)
	assert.False(t, confirmed)
	assert.Equal(t, newer, s.Lookup("lease-1"))

	confirmed, err = s.ConfirmAttemptIfRevision("lease-1", "backend-b", newer.Revision())
	require.NoError(t, err)
	assert.True(t, confirmed)
	assert.Equal(t, "backend-b", s.Lookup("lease-1").Backend)
	assert.Empty(t, s.Lookup("lease-1").Attempt)
}

func TestStore_SetBatchIfNotNewerHonorsPostSnapshotDeleteTombstone(t *testing.T) {
	s := newTestStore(t)
	requireSetAttempting(t, s, "lease-1", "backend-a")
	cutoff := s.BeginInventorySnapshot()
	defer s.EndInventorySnapshot(cutoff)
	require.NoError(t, s.Delete("lease-1"))
	require.Greater(t, s.SnapshotRevision(), cutoff)

	applied, fenced, err := s.SetBatchIfNotNewer(
		map[string]string{"lease-1": "backend-a"}, cutoff,
	)
	require.NoError(t, err)
	assert.Empty(t, applied)
	assert.Contains(t, fenced, "lease-1")
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State(),
		"stale positive inventory must not recreate a record deleted after fetch began")
}

func TestStore_InventoryDeleteFencesAreLeaseLocal(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("deleted", "backend-a"))
	cutoff := s.BeginInventorySnapshot()
	require.NoError(t, s.Delete("deleted"))

	applied, fenced, err := s.SetBatchIfNotNewer(map[string]string{
		"deleted":  "backend-a",
		"observed": "backend-b",
	}, cutoff)
	require.NoError(t, err)
	assert.Contains(t, fenced, "deleted")
	assert.NotContains(t, fenced, "observed")
	assert.Contains(t, applied, "observed")
	assert.Equal(t, StateAbsent, s.Lookup("deleted").State())
	assert.Equal(t, "backend-b", s.Lookup("observed").Backend,
		"one lease's deletion must not suppress another lease's positive inventory")

	_, set, err := s.SetAttemptingIfNotNewer("unrelated", "backend-c", cutoff)
	require.NoError(t, err)
	assert.True(t, set, "one lease's deletion must not fence another lease's write-ahead attempt")
	_, set, err = s.SetAttemptingIfNotNewer("deleted", "backend-a", cutoff)
	require.NoError(t, err)
	assert.False(t, set, "the exact deleted lease must remain fenced from its older snapshot")

	s.EndInventorySnapshot(cutoff)
	assert.Empty(t, s.deleteRevisions, "ending the last snapshot releases every tombstone")
}

func TestStore_InventorySnapshotRefcountsAndPrunesDeleteFences(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	first := s.BeginInventorySnapshot()
	second := s.BeginInventorySnapshot()
	require.Equal(t, first, second)
	require.NoError(t, s.Delete("lease-1"))
	assert.Contains(t, s.deleteRevisions, "lease-1")

	s.EndInventorySnapshot(first)
	assert.Contains(t, s.deleteRevisions, "lease-1",
		"one of two callers at the same cutoff still needs the exact-key fence")
	_, fenced, err := s.SetBatchIfNotNewer(
		map[string]string{"lease-1": "backend-a"}, second,
	)
	require.NoError(t, err)
	assert.Contains(t, fenced, "lease-1")

	s.EndInventorySnapshot(second)
	assert.Empty(t, s.deleteRevisions)
	assert.Empty(t, s.activeSnapshots)

	require.NoError(t, s.Confirm("outside-snapshot", "backend-a"))
	require.NoError(t, s.Delete("outside-snapshot"))
	assert.Empty(t, s.deleteRevisions,
		"ordinary mutations after every inventory ends must not retain tombstones")
}

func TestStore_InventorySnapshotPrunesAgainstOldestActiveCutoff(t *testing.T) {
	t.Run("newer snapshot saw deletion", func(t *testing.T) {
		s := newTestStore(t)
		require.NoError(t, s.Confirm("lease-1", "backend-a"))
		older := s.BeginInventorySnapshot()
		require.NoError(t, s.Delete("lease-1"))
		newer := s.BeginInventorySnapshot()
		require.Greater(t, newer, older)

		s.EndInventorySnapshot(older)
		assert.NotContains(t, s.deleteRevisions, "lease-1",
			"the remaining newer snapshot began after the deletion")
		s.EndInventorySnapshot(newer)
		assert.Empty(t, s.activeSnapshots)
	})

	t.Run("older snapshot still needs deletion", func(t *testing.T) {
		s := newTestStore(t)
		require.NoError(t, s.Confirm("lease-1", "backend-a"))
		older := s.BeginInventorySnapshot()
		require.NoError(t, s.Delete("lease-1"))
		newer := s.BeginInventorySnapshot()
		require.Greater(t, newer, older)

		s.EndInventorySnapshot(newer)
		assert.Contains(t, s.deleteRevisions, "lease-1",
			"ending the newer snapshot cannot discard the older snapshot's fence")
		s.EndInventorySnapshot(older)
		assert.Empty(t, s.deleteRevisions)
		assert.Empty(t, s.activeSnapshots)
	})
}

func TestStore_DeleteFencesAreBoundedBySnapshotLifetime(t *testing.T) {
	s := newTestStore(t)
	cutoff := s.BeginInventorySnapshot()
	const deletedKeys = 256
	for i := range deletedKeys {
		leaseUUID := fmt.Sprintf("deleted-%03d", i)
		require.NoError(t, s.Confirm(leaseUUID, "backend-a"))
		require.NoError(t, s.Delete(leaseUUID))
	}
	assert.Len(t, s.deleteRevisions, deletedKeys)

	s.EndInventorySnapshot(cutoff)
	assert.Empty(t, s.deleteRevisions,
		"completed inventory cannot leave process-lifetime per-lease tombstones")
}

func TestStore_ClearConflictDeletionBlocksOlderInventory(t *testing.T) {
	s := newTestStore(t)
	require.Empty(t, requireSetConflictsIfNotNewer(t, s, map[string][]string{
		"lease-1": {"backend-a", "backend-b"},
	}, s.SnapshotRevision()))
	staleCutoff := s.BeginInventorySnapshot()
	defer s.EndInventorySnapshot(staleCutoff)

	require.NoError(t, s.ClearConflictsIfNotNewer(
		map[string]struct{}{"lease-1": {}}, staleCutoff,
	))
	require.Greater(t, s.deleteRevisions["lease-1"], staleCutoff)
	applied, fenced, err := s.SetBatchIfNotNewer(
		map[string]string{"lease-1": "backend-a"}, staleCutoff,
	)
	require.NoError(t, err)
	assert.Empty(t, applied)
	assert.Contains(t, fenced, "lease-1")
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State(),
		"inventory older than conflict deletion must not recreate the placement")

	freshCutoff := s.SnapshotRevision()
	requireSetBatchIfNotNewer(t, s,
		map[string]string{"lease-1": "backend-a"}, freshCutoff,
	)
	assert.Equal(t, StateConfirmed, s.Lookup("lease-1").State())
}

func TestStore_ConflictDeleteFencesAreLeaseLocal(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("deleted", "backend-a"))
	cutoff := s.BeginInventorySnapshot()
	defer s.EndInventorySnapshot(cutoff)
	require.NoError(t, s.Delete("deleted"))

	fenced := requireSetConflictsIfNotNewer(t, s, map[string][]string{
		"deleted":  {"backend-a", "backend-b"},
		"observed": {"backend-a", "backend-b"},
	}, cutoff)
	assert.Contains(t, fenced, "deleted")
	assert.NotContains(t, fenced, "observed")
	assert.Equal(t, StateAbsent, s.Lookup("deleted").State())
	p := s.Lookup("observed")
	assert.Equal(t, StateUnusable, p.State())
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
}

func TestStore_ConditionalInventoryReturnsFencesWithWriteError(t *testing.T) {
	tests := []struct {
		name string
		call func(*Store, uint64) (map[string]struct{}, error)
	}{
		{
			name: "positive batch",
			call: func(s *Store, cutoff uint64) (map[string]struct{}, error) {
				_, fenced, err := s.SetBatchIfNotNewer(map[string]string{
					"deleted":  "backend-a",
					"eligible": "backend-b",
				}, cutoff)
				return fenced, err
			},
		},
		{
			name: "conflict batch",
			call: func(s *Store, cutoff uint64) (map[string]struct{}, error) {
				_, fenced, err := s.SetConflictsIfNotNewer(map[string][]string{
					"deleted":  {"backend-a", "backend-b"},
					"eligible": {"backend-a", "backend-b"},
				}, cutoff)
				return fenced, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t)
			require.NoError(t, s.Confirm("deleted", "backend-a"))
			cutoff := s.BeginInventorySnapshot()
			require.NoError(t, s.Delete("deleted"))
			require.NoError(t, s.Close())

			fenced, err := tt.call(s, cutoff)
			require.Error(t, err)
			assert.Contains(t, fenced, "deleted",
				"a known exact-key fence remains actionable when another write fails")
		})
	}
}

func TestStore_ConflictQuarantineSurvivesRestartAndResolvesOnCompleteEvidence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	cutoff := s.SnapshotRevision()
	require.Empty(t, requireSetConflictsIfNotNewer(t, s, map[string][]string{
		"lease-1": {"backend-b", "backend-a"},
	}, cutoff))
	p := s.Lookup("lease-1")
	assert.True(t, p.Conflict)
	assert.Equal(t, StateUnusable, p.State())
	assert.Equal(t, "backend-a", p.Backend,
		"quarantine must preserve the last exact confirmed-owner fact")
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.False(t, p.ConflictOwnersUnknown)
	require.NoError(t, s.Close())

	s, err = NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	p = s.Lookup("lease-1")
	assert.True(t, p.Conflict, "quarantine must survive a providerd restart")
	assert.Equal(t, StateUnusable, p.State())
	assert.Equal(t, "backend-a", p.Backend)
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.False(t, p.ConflictOwnersUnknown)

	cutoff = s.SnapshotRevision()
	requireSetBatchIfNotNewer(t, s, map[string]string{"lease-1": "backend-b"}, cutoff)
	p = s.Lookup("lease-1")
	assert.False(t, p.Conflict)
	assert.Equal(t, StateConfirmed, p.State())
	assert.Equal(t, "backend-b", p.Backend)

	cutoff = s.SnapshotRevision()
	require.Empty(t, requireSetConflictsIfNotNewer(t, s, map[string][]string{
		"lease-1": {"backend-a", "backend-b"},
	}, cutoff))
	cutoff = s.SnapshotRevision() // a later complete inventory proves absence
	require.NoError(t, s.ClearConflictsIfNotNewer(map[string]struct{}{"lease-1": {}}, cutoff))
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
}

func TestStore_LegacyConflictWithoutCandidatesLoadsFailClosed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"lease-legacy-conflict":  []byte(`{"backend":"","set_at":"2026-08-25T12:00:00Z","revision":7,"conflict":true}`),
		"lease-partial-conflict": []byte(`{"backend":"","set_at":"2026-08-25T12:00:00Z","revision":8,"conflict":true,"conflict_backends":["backend-a"]}`),
	})

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	for _, leaseUUID := range []string{"lease-legacy-conflict", "lease-partial-conflict"} {
		p := s.Lookup(leaseUUID)
		require.Equal(t, StateUnusable, p.State())
		assert.True(t, p.Conflict)
		assert.True(t, p.ConflictOwnersUnknown,
			"incomplete conflict records must not treat the current router as the complete historical owner set")
	}

	cutoff := s.SnapshotRevision()
	require.Empty(t, requireSetConflictsIfNotNewer(t, s, map[string][]string{
		"lease-partial-conflict": {"backend-a", "backend-b"},
	}, cutoff))
	p := s.Lookup("lease-partial-conflict")
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.True(t, p.ConflictOwnersUnknown,
		"later reports cannot prove which candidate was omitted from an incomplete historical record")
}

func TestStore_StaleConflictSnapshotCannotOverwriteNewAttempt(t *testing.T) {
	s := newTestStore(t)
	cutoff := s.SnapshotRevision()
	requireSetAttempting(t, s, "lease-1", "backend-a")

	fenced := requireSetConflictsIfNotNewer(t, s, map[string][]string{
		"lease-1": {"backend-a", "backend-b"},
	}, cutoff)
	assert.Contains(t, fenced, "lease-1")
	p := s.Lookup("lease-1")
	assert.False(t, p.Conflict)
	assert.Equal(t, StateAttempting, p.State())
	assert.Equal(t, "backend-a", p.Attempt)
}

func TestStore_DeleteCASCannotLoseRacingAttempt(t *testing.T) {
	s := newTestStore(t)

	for i := range 32 {
		leaseUUID := fmt.Sprintf("lease-%d", i)
		require.NoError(t, s.Confirm(leaseUUID, "backend-a"))
		stale := s.Lookup(leaseUUID)

		start := make(chan struct{})
		var wg sync.WaitGroup
		var deleted bool
		var deleteErr, attemptErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			deleted, deleteErr = s.DeleteIfRevision(leaseUUID, stale.Revision())
		}()
		go func() {
			defer wg.Done()
			<-start
			_, attemptErr = s.SetAttempting(leaseUUID, "backend-a")
		}()
		close(start)
		wg.Wait()

		require.NoError(t, deleteErr)
		require.NoError(t, attemptErr)
		p := s.Lookup(leaseUUID)
		assert.NotEqual(t, StateAbsent, p.State(),
			"a racing write-ahead attempt must never be lost")
		if deleted {
			assert.Equal(t, StateAttempting, p.State())
			assert.Empty(t, p.Backend)
		} else {
			assert.Equal(t, StateConfirmed, p.State())
			assert.Equal(t, "backend-a", p.Backend)
		}
		assert.Equal(t, "backend-a", p.Attempt)
		require.NoError(t, s.Delete(leaseUUID))
	}
}

func TestStore_ConcurrentIndependentTransitions(t *testing.T) {
	s := newTestStore(t)

	const goroutines = 50
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	wg.Add(goroutines)
	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			leaseUUID := fmt.Sprintf("lease-concurrent-%d", id)
			if _, err := s.SetAttempting(leaseUUID, "backend-a"); err != nil {
				errs <- err
				return
			}
			if err := s.Confirm(leaseUUID, "backend-a"); err != nil {
				errs <- err
				return
			}
			_ = s.Lookup(leaseUUID)
			_ = s.List()
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Len(t, s.List(), goroutines)
}

func TestStore_HealthCloseAndConstruction(t *testing.T) {
	_, err := NewStore("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "placement db path is required")

	s := newTestStore(t, WithClock(nil))
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	assert.False(t, s.Lookup("lease-1").SetAt.IsZero())
	assert.NoError(t, s.Healthy())
	require.NoError(t, s.Close())
	assert.Error(t, s.Healthy())
	assert.NoError(t, s.Close(), "Close must be idempotent")
}

func TestErrorsAreDistinctSemanticSentinels(t *testing.T) {
	sentinels := []error{
		ErrInvalidPlacement,
		ErrAttemptConflict,
		ErrBackendConflict,
		ErrUnusablePlacement,
		ErrAttemptMismatch,
		ErrInvalidAttemptToken,
		ErrInvalidInventoryFence,
		ErrInventoryNotReady,
		ErrInvalidRecordRevision,
		ErrInvalidProjectionProof,
	}
	for i, left := range sentinels {
		for j, right := range sentinels {
			if i == j {
				assert.True(t, errors.Is(left, right))
				continue
			}
			assert.False(t, errors.Is(left, right))
		}
	}
}

// fakeClock is a trivial settable clock for deterministic transition tests.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }
