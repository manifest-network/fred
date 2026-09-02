package placement

import (
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/provisioner/operation"
)

func TestStoreExportsOnlyRevisionBoundRestoreAdmission(t *testing.T) {
	storeType := reflect.TypeOf((*Store)(nil))
	_, unchecked := storeType.MethodByName("BeginRestore")
	assert.False(t, unchecked, "unchecked tenant-unaware restore admission must remain package-private")
	_, authorized := storeType.MethodByName("BeginAuthorizedRestore")
	assert.True(t, authorized, "production restore admission must require an exact source revision")
}

func newRestoreTestStore(t *testing.T) *Store {
	t.Helper()
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "source", "backend-a")
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	return s
}

func TestStore_InventoryBootstrappedIsDurableAndTopologyBound(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	assert.False(t, s.InventoryBootstrapped())

	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	assert.True(t, s.InventoryBootstrapped())
	fence := s.BeginInventorySession()
	_, err = s.ProjectInventory(fence, InventoryProjection{})
	s.EndInventorySession(fence)
	require.NoError(t, err)
	assert.True(t, s.InventoryBootstrapped(),
		"a partial inventory session must not erase the durable baseline")

	require.NoError(t, s.Close())
	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.True(t, reopened.InventoryBootstrapped(),
		"the matching topology baseline must survive restart")
}

func TestStore_BeginRestoreValidatesAuthorityAndSource(t *testing.T) {
	opID := requireOperationID(t, "7001")

	notReady := newTestStore(t)
	requireConfirmedPlacement(t, notReady, "source", "backend-a")
	claim, err := beginTestRestore(
		t, notReady, notReady.CurrentAdmissionBaseline(), "source", "target", opID,
	)
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
	assert.False(t, claim.Valid())
	assert.Equal(t, StateAbsent, notReady.Lookup("target").State())

	tests := []struct {
		name    string
		prepare func(*testing.T, *Store)
		source  string
		target  string
		opID    operation.OperationID
		wantErr error
	}{
		{
			name:    "source is required",
			source:  "",
			target:  "target",
			opID:    opID,
			wantErr: ErrInvalidPlacement,
		},
		{
			name:    "target is required",
			source:  "source",
			target:  "",
			opID:    opID,
			wantErr: ErrInvalidPlacement,
		},
		{
			name:    "source and target differ",
			source:  "source",
			target:  "source",
			opID:    opID,
			wantErr: ErrInvalidPlacement,
		},
		{
			name:    "operation ID is required",
			source:  "source",
			target:  "target",
			wantErr: operation.ErrInvalidID,
		},
		{
			name:    "source is absent",
			source:  "missing",
			target:  "target",
			opID:    opID,
			wantErr: ErrRestoreSourceNotFound,
		},
		{
			name: "source has unresolved attempt",
			prepare: func(t *testing.T, s *Store) {
				requireTypedAttempt(t, s, "source", "backend-a", requireOperationID(t, "7002"))
			},
			source:  "source",
			target:  "target",
			opID:    opID,
			wantErr: ErrRestoreSourceUnavailable,
		},
		{
			name: "source is ambiguous",
			prepare: func(t *testing.T, s *Store) {
				requireConflictPlacement(t, s, "source", "backend-a", "backend-b")
			},
			source:  "source",
			target:  "target",
			opID:    opID,
			wantErr: ErrRestoreSourceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newRestoreTestStore(t)
			if tt.prepare != nil {
				tt.prepare(t, s)
			}
			claim, err := beginTestRestore(
				t, s, s.CurrentAdmissionBaseline(), tt.source, tt.target, tt.opID,
			)
			require.ErrorIs(t, err, tt.wantErr)
			assert.False(t, claim.Valid())
			if tt.target != "" && tt.target != "source" {
				assert.Equal(t, StateAbsent, s.Lookup(tt.target).State())
			}
			assert.Empty(t, s.restoreClaims)
		})
	}
}

func TestStore_BeginRestoreAcceptsMigratedV013Source(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"source": []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	s, err := newStore(dbPath, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	require.True(t, s.Lookup("source").RecordRevision().Valid())

	opID := requireOperationID(t, "7010")
	claim, err := beginTestRestore(t, s, s.CurrentAdmissionBaseline(), "source", "target", opID)
	require.NoError(t, err)
	assert.True(t, claim.Valid())
	assert.Equal(t, "backend-a", claim.Backend())
	assert.Equal(t, StateAttempting, s.Lookup("target").State())
}

func TestStore_BeginRestoreRequiresTrulyAbsentTarget(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *Store)
	}{
		{
			name: "confirmed on source backend",
			prepare: func(t *testing.T, s *Store) {
				requireConfirmedPlacement(t, s, "target", "backend-a")
			},
		},
		{
			name: "confirmed on different backend",
			prepare: func(t *testing.T, s *Store) {
				requireConfirmedPlacement(t, s, "target", "backend-b")
			},
		},
		{
			name: "attempting",
			prepare: func(t *testing.T, s *Store) {
				requireTypedAttempt(t, s, "target", "backend-a", requireOperationID(t, "7051"))
			},
		},
		{
			name: "conflict",
			prepare: func(t *testing.T, s *Store) {
				requireConflictPlacement(t, s, "target", "backend-a", "backend-b")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newRestoreTestStore(t)
			tt.prepare(t, s)
			beforeSource := s.Lookup("source")
			beforeTarget := s.Lookup("target")
			beforeRevision := testRevision(s)

			opID := requireOperationID(t, "7052")
			claim, err := beginTestRestore(
				t, s, s.CurrentAdmissionBaseline(), "source", "target", opID,
			)
			require.ErrorIs(t, err, ErrRestoreTargetUnavailable)
			assert.False(t, claim.Valid())
			assert.Empty(t, s.restoreClaims, "failed admission must not reserve the source")
			assert.Zero(t, s.restoreNonce)
			assert.Equal(t, beforeSource, s.Lookup("source"))
			assert.Equal(t, beforeTarget, s.Lookup("target"))
			assert.Equal(t, beforeRevision, testRevision(s))
		})
	}

	t.Run("unusable", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "placements.db")
		configured, err := newStoreForTest(dbPath)
		require.NoError(t, err)
		requireConfirmedPlacement(t, configured, "source", "backend-a")
		requireAdmissionBaseline(t, configured, "backend-a", "backend-b")
		require.NoError(t, configured.Close())

		writeRawRecords(t, dbPath, map[string][]byte{
			"target": []byte(`{"set_at":"2026-08-25T15:00:00Z","revision":41}`),
		})
		s, err := newStoreForTest(dbPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		beforeSource := s.Lookup("source")
		beforeTarget := s.Lookup("target")
		require.Equal(t, StateUnusable, beforeTarget.State())
		beforeRevision := testRevision(s)

		opID := requireOperationID(t, "7053")
		claim, err := beginTestRestore(
			t, s, s.CurrentAdmissionBaseline(), "source", "target", opID,
		)
		require.ErrorIs(t, err, ErrRestoreTargetUnavailable)
		assert.False(t, claim.Valid())
		assert.Empty(t, s.restoreClaims)
		assert.Zero(t, s.restoreNonce)
		assert.Equal(t, beforeSource, s.Lookup("source"))
		assert.Equal(t, beforeTarget, s.Lookup("target"))
		assert.Equal(t, beforeRevision, testRevision(s))
	})
}

func TestStore_RestoreSettlementLifecycle(t *testing.T) {
	tests := []struct {
		name       string
		settle     func(*Store, RestoreClaim) (bool, error)
		wantState  State
		wantOwner  string
		wantTarget bool
	}{
		{
			name:       "accepted confirmation promotes target",
			settle:     (*Store).ConfirmRestore,
			wantState:  StateConfirmed,
			wantOwner:  "backend-a",
			wantTarget: true,
		},
		{
			name:      "definitive refusal removes attempt-only target",
			settle:    (*Store).RefuseRestore,
			wantState: StateAbsent,
		},
		{
			name:       "ambiguous outcome retains durable target attempt",
			settle:     (*Store).AbandonRestore,
			wantState:  StateAttempting,
			wantTarget: true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newRestoreTestStore(t)
			opID, err := testOperationID(uint64(7100 + i))
			require.NoError(t, err)
			claim, err := beginTestRestore(
				t, s, s.CurrentAdmissionBaseline(), "source", "target", opID,
			)
			require.NoError(t, err)
			require.True(t, claim.Valid())
			assert.Equal(t, "backend-a", claim.Backend())
			assert.Equal(t, "backend-a", s.Lookup("source").Backend)
			target := s.Lookup("target")
			assert.Equal(t, StateAttempting, target.State())
			assert.Equal(t, "backend-a", target.Attempt)
			assert.Equal(t, opID, target.AttemptOperationID())

			consumed, err := tt.settle(s, claim)
			require.NoError(t, err)
			assert.True(t, consumed)
			assert.Empty(t, s.restoreClaims)
			target = s.Lookup("target")
			assert.Equal(t, tt.wantState, target.State())
			assert.Equal(t, tt.wantOwner, target.Backend)
			if tt.wantTarget && tt.wantState == StateAttempting {
				assert.Equal(t, opID, target.AttemptOperationID())
			} else {
				assert.False(t, target.AttemptOperationID().Valid())
			}

			consumed, err = tt.settle(s, claim)
			require.NoError(t, err)
			assert.False(t, consumed, "a consumed restore claim is stale")
		})
	}
}

func TestStore_RestoreSettlementDefersToExactAttemptClaim(t *testing.T) {
	tests := []struct {
		name   string
		settle func(*Store, RestoreClaim) (bool, error)
	}{
		{name: "confirm", settle: (*Store).ConfirmRestore},
		{name: "refuse", settle: (*Store).RefuseRestore},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := newRestoreTestStore(t)
			operationID, err := testOperationID(uint64(7150 + index))
			require.NoError(t, err)
			restoreClaim, err := beginTestRestore(
				t, s, s.CurrentAdmissionBaseline(), "source", "target", operationID,
			)
			require.NoError(t, err)

			recoveryClaim, claimed, err := s.ClaimAttempt("target", operationID)
			require.NoError(t, err)
			require.True(t, claimed)
			before := s.Lookup("target")

			consumed, err := test.settle(s, restoreClaim)
			require.ErrorIs(t, err, ErrAttemptClaimed)
			assert.True(t, consumed, "the synchronous source reservation is still consumed")
			assert.Equal(t, before, s.Lookup("target"),
				"restore settlement cannot cross an exact callback recovery claim")
			assert.Empty(t, s.restoreClaims)

			confirmed, err := s.ConfirmClaimedAttempt(recoveryClaim)
			require.NoError(t, err)
			require.True(t, confirmed)
			assert.Equal(t, StateConfirmed, s.Lookup("target").State())
			assert.Equal(t, "backend-a", s.Lookup("target").Backend)
		})
	}
}

func TestStore_RestoreClaimIsZeroForeignAndNonceSafe(t *testing.T) {
	s := newRestoreTestStore(t)
	other := newRestoreTestStore(t)

	assert.False(t, (RestoreClaim{}).Valid())
	assert.Empty(t, (RestoreClaim{}).Backend())
	for _, settle := range []func(RestoreClaim) (bool, error){
		s.ConfirmRestore,
		s.RefuseRestore,
		s.AbandonRestore,
	} {
		consumed, err := settle(RestoreClaim{})
		require.ErrorIs(t, err, ErrInvalidRestoreClaim)
		assert.False(t, consumed)
	}

	opID := requireOperationID(t, "7201")
	claim, err := beginTestRestore(t, s, s.CurrentAdmissionBaseline(), "source", "target", opID)
	require.NoError(t, err)
	consumed, err := other.AbandonRestore(claim)
	require.ErrorIs(t, err, ErrInvalidRestoreClaim)
	assert.False(t, consumed)
	assert.Contains(t, s.restoreClaims, "source")

	forged := claim
	forged.nonce++
	require.True(t, forged.Valid(), "the structural copy remains nonzero but was never issued")
	consumed, err = s.ConfirmRestore(forged)
	require.NoError(t, err)
	assert.False(t, consumed)
	assert.Contains(t, s.restoreClaims, "source")

	consumed, err = s.AbandonRestore(claim)
	require.NoError(t, err)
	assert.True(t, consumed)
	consumed, err = s.AbandonRestore(claim)
	require.NoError(t, err)
	assert.False(t, consumed)
}

func TestStore_BeginRestoreExclusivelyClaimsSourceConcurrently(t *testing.T) {
	s := newRestoreTestStore(t)
	const contenders = 32
	start := make(chan struct{})
	type outcome struct {
		claim RestoreClaim
		err   error
	}
	outcomes := make(chan outcome, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			opID, err := testOperationID(uint64(7300 + i))
			if err != nil {
				outcomes <- outcome{err: err}
				return
			}
			claim, err := beginTestRestore(
				t, s, s.CurrentAdmissionBaseline(), "source", fmt.Sprintf("target-%d", i), opID,
			)
			outcomes <- outcome{claim: claim, err: err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(outcomes)

	var winner RestoreClaim
	for result := range outcomes {
		if result.err == nil {
			require.False(t, winner.Valid(), "only one source claim may be issued")
			winner = result.claim
			continue
		}
		require.ErrorIs(t, result.err, ErrRestoreSourceClaimed)
		assert.False(t, result.claim.Valid())
	}
	require.True(t, winner.Valid())
	assert.Len(t, s.restoreClaims, 1)
	consumed, err := s.AbandonRestore(winner)
	require.NoError(t, err)
	assert.True(t, consumed)
}

func TestStore_BeginRestoreTargetAttemptIsAtomicWithSourceClaim(t *testing.T) {
	t.Run("revision exhaustion", func(t *testing.T) {
		s := newRestoreTestStore(t)
		s.revision = math.MaxUint64
		opID := requireOperationID(t, "7401")
		claim, err := beginTestRestore(t, s, s.CurrentAdmissionBaseline(), "source", "target", opID)
		require.ErrorContains(t, err, "placement revision exhausted")
		assert.False(t, claim.Valid())
		assert.Empty(t, s.restoreClaims)
		assert.Equal(t, StateAbsent, s.Lookup("target").State())
		assert.Equal(t, StateConfirmed, s.Lookup("source").State())
	})

	t.Run("nonce exhaustion", func(t *testing.T) {
		s := newRestoreTestStore(t)
		s.restoreNonce = math.MaxUint64
		opID := requireOperationID(t, "7402")
		claim, err := beginTestRestore(t, s, s.CurrentAdmissionBaseline(), "source", "target", opID)
		require.ErrorContains(t, err, "restore nonce exhausted")
		assert.False(t, claim.Valid())
		assert.Empty(t, s.restoreClaims)
		assert.Equal(t, StateAbsent, s.Lookup("target").State())
	})

	t.Run("durable write failure", func(t *testing.T) {
		s := newRestoreTestStore(t)
		beforeRevision := testRevision(s)
		// Close only bbolt to inject a definitely pre-commit write failure while
		// leaving the Store lifecycle open. Store.Close itself withdraws cached
		// authority by design and is tested separately.
		require.NoError(t, s.db.Close())
		opID := requireOperationID(t, "7403")
		claim, err := beginTestRestore(t, s, s.CurrentAdmissionBaseline(), "source", "target", opID)
		require.Error(t, err)
		assert.False(t, claim.Valid())
		assert.Empty(t, s.restoreClaims)
		assert.Equal(t, beforeRevision, testRevision(s))
		assert.Equal(t, StateAbsent, s.Lookup("target").State())
		assert.Equal(t, StateConfirmed, s.Lookup("source").State())
	})
}

func TestStore_RestoreClaimFencesTypedSourceMutations(t *testing.T) {
	s := newRestoreTestStore(t)
	sourceRevision := s.Lookup("source").RecordRevision()
	opID := requireOperationID(t, "7501")
	claim, err := beginTestRestore(t, s, s.CurrentAdmissionBaseline(), "source", "target", opID)
	require.NoError(t, err)

	token, applied, err := s.BeginOwnedAttempt(
		s.CurrentAdmissionBaseline(), sourceRevision, "backend-a", requireOperationID(t, "7502"),
		PayloadFingerprint{}, testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "7502")))

	require.ErrorIs(t, err, ErrRestoreSourceClaimed)
	assert.False(t, applied)
	assert.False(t, token.Valid())

	deleted, err := s.DeleteRecord(sourceRevision)
	require.ErrorIs(t, err, ErrRestoreSourceClaimed)
	assert.False(t, deleted)
	assert.Equal(t, StateConfirmed, s.Lookup("source").State())

	consumed, err := s.AbandonRestore(claim)
	require.NoError(t, err)
	assert.True(t, consumed)
	deleted, err = s.DeleteRecord(sourceRevision)
	require.NoError(t, err)
	assert.True(t, deleted, "settlement releases the source mutation fence")
}

func TestStore_ProjectInventoryFencesRestoreSourceOwnerAndConflictMutation(t *testing.T) {
	tests := []struct {
		name        string
		projection  InventoryProjection
		assertAfter func(*testing.T, Placement)
	}{
		{
			name: "owner change",
			projection: InventoryProjection{
				Placements: map[string]string{"source": "backend-b"},
			},
			assertAfter: func(t *testing.T, p Placement) {
				assert.Equal(t, StateConfirmed, p.State())
				assert.Equal(t, "backend-b", p.Backend)
			},
		},
		{
			name: "conflict",
			projection: InventoryProjection{
				Conflicts: map[string][]string{"source": {"backend-a", "backend-b"}},
			},
			assertAfter: func(t *testing.T, p Placement) {
				assert.Equal(t, StateUnusable, p.State())
				assert.True(t, p.Conflict)
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newRestoreTestStore(t)
			opID, err := testOperationID(uint64(7600 + i))
			require.NoError(t, err)
			claim, err := beginTestRestore(
				t, s, s.CurrentAdmissionBaseline(), "source", "target", opID,
			)
			require.NoError(t, err)
			fence := s.BeginInventorySession()
			defer s.EndInventorySession(fence)

			result, err := s.ProjectInventory(fence, tt.projection)
			require.NoError(t, err)
			assert.Contains(t, result.Fenced, "source")
			assert.Equal(t, "backend-a", s.Lookup("source").Backend)
			assert.False(t, s.Lookup("source").Conflict)

			consumed, err := s.AbandonRestore(claim)
			require.NoError(t, err)
			assert.True(t, consumed)
			result, err = s.ProjectInventory(fence, tt.projection)
			require.NoError(t, err)
			assert.NotContains(t, result.Fenced, "source")
			tt.assertAfter(t, s.Lookup("source"))
		})
	}
}

func TestStore_RestoreSettlementToleratesFastExactCallback(t *testing.T) {
	callbacks := []struct {
		name      string
		apply     func(*Store, string, string, operation.OperationID) (bool, error)
		wantState State
	}{
		{name: "success", apply: (*Store).ConfirmOperation, wantState: StateConfirmed},
		{name: "failure", apply: (*Store).RefuseOperation, wantState: StateAbsent},
	}
	settlements := []struct {
		name  string
		apply func(*Store, RestoreClaim) (bool, error)
	}{
		{name: "confirm", apply: (*Store).ConfirmRestore},
		{name: "refuse", apply: (*Store).RefuseRestore},
		{name: "abandon", apply: (*Store).AbandonRestore},
	}

	for i, callback := range callbacks {
		for j, settlement := range settlements {
			t.Run(callback.name+" callback before "+settlement.name, func(t *testing.T) {
				s := newRestoreTestStore(t)
				opID, err := testOperationID(uint64(7700 + i*10 + j))
				require.NoError(t, err)
				claim, err := beginTestRestore(
					t, s, s.CurrentAdmissionBaseline(), "source", "target", opID,
				)
				require.NoError(t, err)
				applied, err := callback.apply(s, "target", "backend-a", opID)
				require.NoError(t, err)
				require.True(t, applied)
				before := s.Lookup("target")
				require.Equal(t, callback.wantState, before.State())

				consumed, err := settlement.apply(s, claim)
				require.NoError(t, err)
				assert.True(t, consumed)
				assert.Equal(t, before, s.Lookup("target"),
					"synchronous settlement must not overwrite a fast exact callback")
				assert.Empty(t, s.restoreClaims)
			})
		}
	}
}

func TestStore_RestoreSettlementReleasesSourceClaimOnTargetWriteFailure(t *testing.T) {
	s := newRestoreTestStore(t)
	opID := requireOperationID(t, "7801")
	claim, err := beginTestRestore(t, s, s.CurrentAdmissionBaseline(), "source", "target", opID)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	consumed, err := s.ConfirmRestore(claim)
	require.Error(t, err)
	assert.True(t, consumed)
	assert.Empty(t, s.restoreClaims,
		"the process-local source claim must not outlive synchronous dispatch")
}

func TestStore_BeginRestoreRefusesLeaseAlreadyClaimedAsAnotherSource(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "source-a", "backend-a")
	requireConfirmedPlacement(t, s, "source-b", "backend-a")
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	firstID := requireOperationID(t, "7901")
	first, err := beginTestRestore(
		t, s, s.CurrentAdmissionBaseline(), "source-a", "target-a", firstID,
	)
	require.NoError(t, err)

	secondID := requireOperationID(t, "7902")
	second, err := beginTestRestore(
		t, s, s.CurrentAdmissionBaseline(), "source-b", "source-a", secondID,
	)
	require.ErrorIs(t, err, ErrRestoreSourceClaimed)
	assert.False(t, second.Valid())
	assert.Equal(t, StateConfirmed, s.Lookup("source-a").State())
	assert.Len(t, s.restoreClaims, 1)

	consumed, err := s.AbandonRestore(first)
	require.NoError(t, err)
	assert.True(t, consumed)
}
