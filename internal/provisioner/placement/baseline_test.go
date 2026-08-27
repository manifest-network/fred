package placement

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func projectForBaselineTest(
	t *testing.T,
	s *Store,
	projection InventoryProjection,
) (ProjectionResult, error) {
	t.Helper()
	fence := s.BeginInventorySession()
	result, err := s.ProjectInventory(fence, projection)
	s.EndInventorySession(fence)
	return result, err
}

func requireAdmissionScope(
	t *testing.T,
	s *Store,
	baseline AdmissionBaseline,
	eligibleNames ...string,
) AdmissionScope {
	t.Helper()
	scope, err := s.ScopeAdmission(baseline, eligibleNames)
	require.NoError(t, err)
	require.True(t, scope.Valid())
	return scope
}

func TestStore_AdmissionBaselineRequiresConfiguredCompleteProjection(t *testing.T) {
	s := newTestStore(t)
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
	assert.False(t, s.InventoryBootstrapped())

	_, err := projectForBaselineTest(t, s, InventoryProjection{Complete: true})
	require.ErrorIs(t, err, ErrBackendTopologyNotConfigured)
	assert.False(t, s.CurrentAdmissionBaseline().Valid())

	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-b", "backend-a"}))
	_, err = projectForBaselineTest(t, s, InventoryProjection{})
	require.NoError(t, err)
	assert.False(t, s.CurrentAdmissionBaseline().Valid(),
		"a partial projection cannot establish absence authority")
	assert.False(t, s.InventoryBootstrapped())

	_, err = projectForBaselineTest(t, s, InventoryProjection{Complete: true})
	require.NoError(t, err)
	baseline := s.CurrentAdmissionBaseline()
	require.True(t, baseline.Valid())
	assert.True(t, s.InventoryBootstrapped())

	partialFence := s.BeginInventorySession()
	assert.True(t, s.CurrentAdmissionBaseline().Valid(),
		"starting another inventory invalidates projection proofs, not the baseline")
	_, err = s.ProjectInventory(partialFence, InventoryProjection{})
	require.NoError(t, err)
	s.EndInventorySession(partialFence)
	assert.Equal(t, baseline, s.CurrentAdmissionBaseline(),
		"a partial projection cannot erase a prior matching baseline")
}

func TestStore_AdmissionBaselineSurvivesReopenWithMatchingTopology(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-b", "backend-a"}))
	_, err = projectForBaselineTest(t, s, InventoryProjection{Complete: true})
	require.NoError(t, err)
	oldProcessBaseline := s.CurrentAdmissionBaseline()
	require.True(t, oldProcessBaseline.Valid())
	oldProcessScope := requireAdmissionScope(t, s, oldProcessBaseline, "backend-a")
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Equal(t, []string{"backend-a", "backend-b"}, reopened.backendTopology)
	assert.True(t, reopened.InventoryBootstrapped())
	reopenedBaseline := reopened.CurrentAdmissionBaseline()
	require.True(t, reopenedBaseline.Valid())
	assert.NotEqual(t, oldProcessBaseline, reopenedBaseline,
		"capabilities are rebound to the reopened Store")

	require.NoError(t, reopened.ConfigureBackendTopology([]string{"backend-b", "backend-a"}))
	assert.Equal(t, reopenedBaseline, reopened.CurrentAdmissionBaseline(),
		"canonical reordering is an idempotent topology configuration")

	token, applied, err := reopened.BeginNewAttempt(
		oldProcessScope, "foreign", "backend-a", requireOperationID(t, "9001"),
	)
	require.ErrorIs(t, err, ErrInvalidAdmissionScope)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	_, err = reopened.ScopeAdmission(oldProcessBaseline, []string{"backend-a"})
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
}

func TestStore_BackendTopologyChangesRequireRebaselineAndRetireNames(t *testing.T) {
	s := newTestStore(t)
	first := requireAdmissionBaseline(t, s, "backend-a")
	firstScope := requireAdmissionScope(t, s, first, "backend-a")

	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-a", "backend-b"}))
	assert.False(t, s.InventoryBootstrapped())
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
	token, applied, err := s.BeginNewAttempt(
		firstScope, "lease-stale-add", "backend-a", requireOperationID(t, "9010"),
	)
	require.ErrorIs(t, err, ErrInvalidAdmissionScope)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	_, err = s.ScopeAdmission(first, []string{"backend-a"})
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)

	_, err = projectForBaselineTest(t, s, InventoryProjection{})
	require.NoError(t, err)
	assert.False(t, s.InventoryBootstrapped(), "partial inventory cannot rebaseline")
	second := requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	require.True(t, second.Valid())

	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-a"}))
	assert.False(t, s.InventoryBootstrapped())
	third := requireAdmissionBaseline(t, s, "backend-a")
	require.True(t, third.Valid())

	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-c"}))
	assert.False(t, s.InventoryBootstrapped(), "a rename is a distinct topology")
	require.ErrorIs(t,
		s.ConfigureBackendTopology([]string{"backend-a", "backend-c"}),
		ErrBackendIdentityReused,
	)
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
}

func TestStore_ConfigureBackendTopologyRejectsRemovedDurableReferences(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *Store)
	}{
		{
			name: "confirmed Backend",
			prepare: func(t *testing.T, s *Store) {
				requireConfirmedPlacement(t, s, "lease", "backend-b")
			},
		},
		{
			name: "pending Attempt",
			prepare: func(t *testing.T, s *Store) {
				requireTestAdmission(t, s)
				requireTypedAttempt(t, s, "lease", "backend-b", requireOperationID(t, "9051"))
			},
		},
		{
			name: "quarantine ConflictBackends",
			prepare: func(t *testing.T, s *Store) {
				requireConflictPlacement(t, s, "lease", "backend-a", "backend-b")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t)
			require.NoError(t, s.ConfigureBackendTopology([]string{"backend-a", "backend-b"}))
			tt.prepare(t, s)
			require.ErrorIs(t,
				s.ConfigureBackendTopology([]string{"backend-a"}),
				ErrBackendTopologyInUse,
			)
			assert.Equal(t, []string{"backend-a", "backend-b"}, s.backendTopology)
		})
	}

	t.Run("v0.13 raw owner", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "placements.db")
		writeRawRecords(t, dbPath, map[string][]byte{"lease": []byte("backend-b")})
		s, err := NewStore(dbPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		require.ErrorIs(t,
			s.ConfigureBackendTopology([]string{"backend-a"}),
			ErrBackendTopologyInUse,
		)
	})

	for name, value := range map[string][]byte{
		"empty record":           {},
		"malformed JSON":         []byte(`{"backend":`),
		"identity-free object":   []byte(`{"set_at":"2026-08-25T15:00:00Z"}`),
		"unknown conflict owner": []byte(`{"conflict":true,"conflict_backends":["backend-a"]}`),
		"invalid legacy owner":   {0xff},
	} {
		t.Run(name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			writeRawRecords(t, dbPath, map[string][]byte{"lease": value})
			s, err := NewStore(dbPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			err = s.ConfigureBackendTopology([]string{"backend-a", "backend-b"})
			require.ErrorIs(t, err, ErrBackendTopologyInUse)
			require.ErrorContains(t, err,
				`lease "lease" has uninterpretable durable placement`,
				"startup diagnostics must name the exact key that requires offline inspection",
			)
		})
	}
}

func TestStore_ConfigureBackendTopologyValidatesNamesAndProjectionMembership(t *testing.T) {
	tests := []struct {
		name  string
		names []string
	}{
		{name: "missing"},
		{name: "empty", names: []string{}},
		{name: "empty name", names: []string{""}},
		{name: "blank name", names: []string{" \t"}},
		{name: "duplicate", names: []string{"backend-a", "backend-a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t)
			require.ErrorIs(t, s.ConfigureBackendTopology(tt.names), ErrInvalidBackendTopology)
			assert.False(t, s.CurrentAdmissionBaseline().Valid())
		})
	}

	s := newTestStore(t)
	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-a"}))
	_, err := projectForBaselineTest(t, s, InventoryProjection{
		Complete:   true,
		Placements: map[string]string{"lease": "backend-b"},
	})
	require.ErrorIs(t, err, ErrBackendNotInTopology)
	assert.False(t, s.CurrentAdmissionBaseline().Valid())

	baseline := requireAdmissionBaseline(t, s, "backend-a")
	scope := requireAdmissionScope(t, s, baseline, "backend-a")
	token, applied, err := s.BeginNewAttempt(
		scope, "lease", "backend-b", requireOperationID(t, "9020"),
	)
	require.ErrorIs(t, err, ErrBackendNotInTopology)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	_, err = s.ScopeAdmission(baseline, []string{"backend-b"})
	require.ErrorIs(t, err, ErrBackendNotInTopology)
}

func TestStore_NewStoreRejectsMalformedTopologyMetadata(t *testing.T) {
	tests := []struct {
		name    string
		encoded []byte
	}{
		{name: "invalid JSON", encoded: []byte("{")},
		{name: "unsupported schema", encoded: []byte(`{"schema":99}`)},
		{
			name: "fingerprint mismatch",
			encoded: []byte(`{"schema":1,"topology":["backend-a"],` +
				`"topology_fingerprint":"bad","known_backends":["backend-a"],"topology_id":1}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			s, err := NewStore(dbPath)
			require.NoError(t, err)
			require.NoError(t, s.Close())

			db, err := bolt.Open(dbPath, 0600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(metadataBucketName).Put(metadataStateKey, tt.encoded)
			}))
			require.NoError(t, db.Close())

			reopened, err := NewStore(dbPath)
			require.Error(t, err)
			assert.Nil(t, reopened)
		})
	}
}

func TestStore_CompleteProjectionDoesNotArmBaselineWhenTransactionFails(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-a"}))
	oversizedLease := strings.Repeat("z", bolt.MaxKeySize+1)

	_, err = projectForBaselineTest(t, s, InventoryProjection{
		Complete:   true,
		Placements: map[string]string{oversizedLease: "backend-a"},
	})
	require.Error(t, err)
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
	assert.False(t, s.InventoryBootstrapped())
	assert.Equal(t, StateAbsent, s.Lookup(oversizedLease).State())
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.False(t, reopened.InventoryBootstrapped(),
		"the placement write and baseline metadata must roll back together")
}

func TestStore_AdmissionBaselineRejectsZeroForeignStaleAndForgedCapabilities(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a")
	other := newTestStore(t)
	foreign := requireAdmissionBaseline(t, other, "backend-a")

	tests := []struct {
		name     string
		baseline AdmissionBaseline
	}{
		{name: "zero"},
		{name: "foreign", baseline: foreign},
		{
			name: "forged fingerprint",
			baseline: AdmissionBaseline{
				issuer: s, topologyID: baseline.topologyID, fingerprint: "forged",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.ScopeAdmission(tt.baseline, []string{"backend-a"})
			require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
		})
	}

	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-a", "backend-b"}))
	_, err := s.ScopeAdmission(baseline, []string{"backend-a"})
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
}

func TestStore_ScopeAdmissionIsZeroSafeSubsetBoundAndDefensive(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b", "backend-c")

	var zero AdmissionScope
	assert.False(t, zero.Valid())
	assert.False(t, zero.Allows("backend-a"))
	assert.False(t, zero.Allows(""))

	eligible := []string{"backend-b", "backend-a"}
	scope, err := s.ScopeAdmission(baseline, eligible)
	require.NoError(t, err)
	require.True(t, scope.Valid())
	assert.True(t, scope.Allows("backend-a"))
	assert.True(t, scope.Allows("backend-b"))
	assert.False(t, scope.Allows("backend-c"))
	assert.False(t, scope.Allows(""))

	eligible[0] = "backend-c"
	assert.True(t, scope.Allows("backend-b"),
		"mutating the caller's slice must not attenuate an issued scope")
	assert.False(t, scope.Allows("backend-c"),
		"mutating the caller's slice must not widen an issued scope")

	empty, err := s.ScopeAdmission(baseline, nil)
	require.NoError(t, err)
	assert.True(t, empty.Valid(), "an explicitly issued deny-all scope is valid")
	assert.False(t, empty.Allows("backend-a"))
	token, applied, err := s.BeginNewAttempt(
		empty, "lease-empty", "backend-a", requireOperationID(t, "9030"),
	)
	require.ErrorIs(t, err, ErrBackendOutsideAdmissionScope)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("lease-empty").State())
}

func TestStore_ScopeAdmissionRejectsMalformedAndForeignBackends(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b")

	for _, tt := range []struct {
		name     string
		eligible []string
		wantErr  error
	}{
		{name: "empty name", eligible: []string{""}, wantErr: ErrInvalidAdmissionScope},
		{name: "blank name", eligible: []string{" \t"}, wantErr: ErrInvalidAdmissionScope},
		{name: "duplicate", eligible: []string{"backend-a", "backend-a"}, wantErr: ErrInvalidAdmissionScope},
		{name: "outside topology", eligible: []string{"backend-c"}, wantErr: ErrBackendNotInTopology},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scope, err := s.ScopeAdmission(baseline, tt.eligible)
			require.ErrorIs(t, err, tt.wantErr)
			assert.False(t, scope.Valid())
		})
	}
}

func TestStore_BeginNewAttemptRequiresCurrentAdmissionScopeAndExactEligibility(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	scopeA := requireAdmissionScope(t, s, baseline, "backend-a")
	other := newTestStore(t)
	otherBaseline := requireAdmissionBaseline(t, other, "backend-a", "backend-b")
	foreign := requireAdmissionScope(t, other, otherBaseline, "backend-a")

	for i, tt := range []struct {
		name  string
		scope AdmissionScope
	}{
		{name: "zero"},
		{name: "foreign", scope: foreign},
		{
			name: "forged fingerprint",
			scope: AdmissionScope{
				issuer: s, topologyID: scopeA.topologyID, fingerprint: "forged",
				eligible: map[string]struct{}{"backend-a": {}},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			token, applied, err := s.BeginNewAttempt(
				tt.scope, "invalid-scope-"+tt.name, "backend-a",
				requireOperationID(t, fmt.Sprint(9040+i)),
			)
			require.ErrorIs(t, err, ErrInvalidAdmissionScope)
			assert.False(t, applied)
			assert.False(t, token.Valid())
		})
	}

	token, applied, err := s.BeginNewAttempt(
		scopeA, "outside-scope", "backend-b", requireOperationID(t, "9050"),
	)
	require.ErrorIs(t, err, ErrBackendOutsideAdmissionScope)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("outside-scope").State())

	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-a", "backend-b", "backend-c"}))
	token, applied, err = s.BeginNewAttempt(
		scopeA, "stale-scope", "backend-a", requireOperationID(t, "9051"),
	)
	require.ErrorIs(t, err, ErrInvalidAdmissionScope)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("stale-scope").State())
}

func TestStore_BeginNewAttemptIsExactInsertIfAbsentCAS(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	scope := requireAdmissionScope(t, s, baseline, "backend-a", "backend-b")

	// An observation inserted after a caller saw absence wins the admission CAS.
	require.Equal(t, StateAbsent, s.Lookup("toctou").State())
	requireConfirmedPlacement(t, s, "toctou", "backend-a")
	token, applied, err := s.BeginNewAttempt(
		scope, "toctou", "backend-a", requireOperationID(t, "9050"),
	)
	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Equal(t, StateConfirmed, s.Lookup("toctou").State())

	const contenders = 32
	type outcome struct {
		token   AttemptToken
		applied bool
		err     error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			opID, parseErr := testOperationID(uint64(9060 + i))
			if parseErr != nil {
				outcomes <- outcome{err: parseErr}
				return
			}
			token, applied, err := s.BeginNewAttempt(
				scope, "concurrent", "backend-a", opID,
			)
			outcomes <- outcome{token: token, applied: applied, err: err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(outcomes)

	winners := 0
	for result := range outcomes {
		require.NoError(t, result.err)
		if result.applied {
			winners++
			assert.True(t, result.token.Valid())
		} else {
			assert.False(t, result.token.Valid())
		}
	}
	assert.Equal(t, 1, winners)
	assert.Equal(t, StateAttempting, s.Lookup("concurrent").State())
}

func TestStore_BeginOwnedAttemptRequiresExactConfirmedOwnerRevision(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "owned", "backend-a")
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	revision := s.Lookup("owned").RecordRevision()
	require.True(t, revision.Valid())

	token, applied, err := s.BeginOwnedAttempt(
		baseline, revision, "backend-a", requireOperationID(t, "9100"),
	)
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, token.Valid())
	assert.Equal(t, "backend-a", s.Lookup("owned").Attempt)

	second, applied, err := s.BeginOwnedAttempt(
		baseline, revision, "backend-a", requireOperationID(t, "9101"),
	)
	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, second.Valid())
	require.True(t, mustRefuseAttempt(t, s, token))

	freshRevision := s.Lookup("owned").RecordRevision()
	wrongOwner, applied, err := s.BeginOwnedAttempt(
		baseline, freshRevision, "backend-b", requireOperationID(t, "9102"),
	)
	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, wrongOwner.Valid())

	require.True(t, mustDeleteRecord(t, s, freshRevision))
	requireConfirmedPlacement(t, s, "owned", "backend-a")
	stale, applied, err := s.BeginOwnedAttempt(
		baseline, freshRevision, "backend-a", requireOperationID(t, "9103"),
	)
	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, stale.Valid())

	other := newTestStore(t)
	requireConfirmedPlacement(t, other, "owned", "backend-a")
	foreignRevision := other.Lookup("owned").RecordRevision()
	foreign, applied, err := s.BeginOwnedAttempt(
		baseline, foreignRevision, "backend-a", requireOperationID(t, "9104"),
	)
	require.ErrorIs(t, err, ErrInvalidRecordRevision)
	assert.False(t, applied)
	assert.False(t, foreign.Valid())
}

func mustRefuseAttempt(t *testing.T, s *Store, token AttemptToken) bool {
	t.Helper()
	applied, err := s.RefuseAttempt(token)
	require.NoError(t, err)
	return applied
}

func mustDeleteRecord(t *testing.T, s *Store, revision RecordRevision) bool {
	t.Helper()
	applied, err := s.DeleteRecord(revision)
	require.NoError(t, err)
	return applied
}

func TestStore_BeginRestoreRequiresCurrentAdmissionBaseline(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "source", "backend-a")
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	opID := requireOperationID(t, "9200")

	claim, err := s.BeginRestore(AdmissionBaseline{}, "source", "target", opID)
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
	assert.False(t, claim.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("target").State())

	require.NoError(t, s.ConfigureBackendTopology(
		[]string{"backend-a", "backend-b", "backend-c"},
	))
	claim, err = s.BeginRestore(baseline, "source", "target", opID)
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
	assert.False(t, claim.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("target").State())

	baseline = requireAdmissionBaseline(t, s, "backend-a", "backend-b", "backend-c")
	claim, err = s.BeginRestore(baseline, "source", "target", opID)
	require.NoError(t, err)
	assert.True(t, claim.Valid())
}

func TestStore_InventoryConflictPreservesPendingAttemptIdentity(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	opID := requireOperationID(t, "9300")
	scope := requireAdmissionScope(t, s, s.CurrentAdmissionBaseline(), "backend-a")
	token, applied, err := s.BeginNewAttempt(
		scope, "lease", "backend-a", opID,
	)
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, token.Valid())

	_, err = projectForBaselineTest(t, s, InventoryProjection{
		Conflicts: map[string][]string{"lease": {"backend-a", "backend-b"}},
	})
	require.NoError(t, err)
	conflict := s.Lookup("lease")
	assert.Equal(t, StateUnusable, conflict.State())
	assert.Equal(t, "backend-a", conflict.Attempt)
	assert.Equal(t, opID, conflict.AttemptOperationID())
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"}, conflict.ConflictBackends)

	_, err = projectForBaselineTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease": "backend-b"},
	})
	require.NoError(t, err)
	observed := s.Lookup("lease")
	assert.Equal(t, "backend-b", observed.Backend)
	assert.Equal(t, "backend-a", observed.Attempt,
		"a different owner's observation cannot clear the pending attempt")
	assert.Equal(t, opID, observed.AttemptOperationID())
}

func TestStore_CompleteProjectionKeepsPriorBaselineOnFailedRefresh(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a")
	oversizedLease := strings.Repeat("x", bolt.MaxKeySize+1)

	_, err := projectForBaselineTest(t, s, InventoryProjection{
		Complete: true,
		Placements: map[string]string{
			"lease-written-before-failure": "backend-a",
			oversizedLease:                 "backend-a",
		},
	})
	require.Error(t, err)
	assert.Equal(t, baseline, s.CurrentAdmissionBaseline())
	assert.True(t, s.InventoryBootstrapped())
	assert.Equal(t, StateAbsent, s.Lookup("lease-written-before-failure").State(),
		"the placement mutation before the rejected key must roll back with metadata")
}

func TestStore_TopologyMetadataErrorClassification(t *testing.T) {
	// Keep the public errors usable with errors.Is after operation context is
	// added. This is a small regression guard for callers' fail-closed handling.
	s := newTestStore(t)
	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-a", "backend-b"}))
	requireTestAdmission(t, s)
	requireTypedAttempt(t, s, "lease", "backend-b", requireOperationID(t, "9251"))
	err := s.ConfigureBackendTopology([]string{"backend-a"})
	assert.True(t, errors.Is(err, ErrBackendTopologyInUse))
}
