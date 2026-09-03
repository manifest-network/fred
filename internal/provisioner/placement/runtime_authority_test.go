package placement

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuntimeAuthorityCommitOutcomeUnknownIsSticky(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a")
	scope, err := s.ScopeAdmission(baseline, []string{"backend-a"})
	require.NoError(t, err)
	require.NoError(t, s.Healthy())

	ordinary := errors.New("definitely uncommitted mutation")
	assert.Same(t, ordinary, s.classifyRuntimeAuthorityUpdateError(ordinary))
	require.NoError(t, s.Healthy(), "pre-commit failures must not poison the Store")

	cause := errors.New("synthetic commit failure")
	commitErr := fmt.Errorf("%w: %w", errBoltCommitOutcomeUnknown, cause)
	classified := s.runtimeAuthorityGate.Run(func() error {
		return s.classifyRuntimeAuthorityUpdateError(commitErr)
	})
	require.ErrorIs(t, classified, ErrRuntimeAuthorityUnavailable)
	require.ErrorIs(t, classified, errBoltCommitOutcomeUnknown)
	require.ErrorIs(t, classified, cause)

	healthErr := s.Healthy()
	require.ErrorIs(t, healthErr, ErrRuntimeAuthorityUnavailable)
	require.ErrorIs(t, healthErr, errBoltCommitOutcomeUnknown)
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
	assert.False(t, s.InventoryBootstrapped())
	assert.Equal(t, StateUnusable, s.Lookup("lease-after-unknown-commit").State())
	assert.False(t, s.BeginInventorySession().Valid())
	assert.Equal(t, LifecycleVerdictUnusable,
		s.CurrentLifecycle("lease-after-unknown-commit").Verdict())

	operationID := requireOperationID(t, "99001")
	token, applied, err := s.BeginNewAttempt(
		scope, "lease-after-unknown-commit", "backend-a", operationID,
		PayloadFingerprint{}, testBackendRequestSnapshot(t), testCallbackPair(operationID),
	)
	require.ErrorIs(t, err, ErrRuntimeAuthorityUnavailable)
	assert.False(t, applied)
	assert.False(t, token.Valid())

	_, err = s.ScopeAdmission(baseline, []string{"backend-a"})
	require.ErrorIs(t, err, ErrRuntimeAuthorityUnavailable)
}

func TestClosedStoreWithdrawsCachedReadAuthority(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "lease-before-close", "backend-a")
	require.Equal(t, StateConfirmed, s.Lookup("lease-before-close").State())

	require.NoError(t, s.Close())

	assert.Equal(t, StateUnusable, s.Lookup("lease-before-close").State())
	listed := s.List()
	require.Contains(t, listed, "lease-before-close")
	assert.Equal(t, StateUnusable, listed["lease-before-close"].State())
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
	assert.False(t, s.InventoryBootstrapped())
	assert.Equal(t, LifecycleVerdictUnusable, s.CurrentLifecycle("lease-before-close").Verdict())
}
