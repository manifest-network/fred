package placement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRestoreSourceReservationOwnsBothAdmissionStages(t *testing.T) {
	s := newRestoreTestStore(t)
	before := s.Lookup("source")
	source, err := s.reserveRestoreSource("source")
	require.NoError(t, err)
	t.Cleanup(func() { s.releaseRestoreSource(source) })
	_, err = s.reserveRestoreSource("source")
	require.ErrorIs(t, err, ErrRestoreSourceClaimed)
	deleted, err := s.deleteRecord(before.RecordRevision())
	require.ErrorIs(t, err, ErrRestoreSourceClaimed)
	require.False(t, deleted)
	opID := requireOperationID(t, "2314")
	_, applied, err := s.beginOwnedAttempt(s.CurrentAdmissionBaseline(), before.RecordRevision(),
		"backend-a", opID, PayloadFingerprint{}, testBackendRequestSnapshot(t), testCallbackPair(opID))
	require.ErrorIs(t, err, ErrRestoreSourceClaimed)
	require.False(t, applied)
	claim, err := s.beginReservedRestore(s.CurrentAdmissionBaseline(), source, "target", opID,
		testBackendRequestSnapshot(t), testCallbackPair(opID))
	require.NoError(t, err)
	require.True(t, claim.Valid())
	require.Equal(t, before.RecordRevision(), claim.sourceRevision)
	s.releaseRestoreSource(source)
	require.Equal(t, claim, s.restoreClaims["source"], "authorization cleanup cannot revoke the transferred dispatch")
	consumed, err := s.abandonRestore(claim)
	require.NoError(t, err)
	require.True(t, consumed)
	require.Empty(t, s.restoreClaims)
	require.Equal(t, StateAttempting, s.Lookup("target").State(), "ambiguous settlement retains the exact durable target")
}

func TestRestoreSourceReservationRejectsForeignCopiedAndReleasedAuthority(t *testing.T) {
	s := newRestoreTestStore(t)
	source, err := s.reserveRestoreSource("source")
	require.NoError(t, err)
	foreign := newRestoreTestStore(t)
	foreignSource, err := foreign.reserveRestoreSource("source")
	require.NoError(t, err)
	t.Cleanup(func() { foreign.releaseRestoreSource(foreignSource) })
	copied := *source
	opID := requireOperationID(t, "2315")
	for _, invalid := range []*restoreSourceReservation{nil, {}, foreignSource, &copied} {
		claim, err := s.beginReservedRestore(s.CurrentAdmissionBaseline(), invalid, "target", opID,
			testBackendRequestSnapshot(t), testCallbackPair(opID))
		require.ErrorIs(t, err, ErrInvalidRestoreClaim)
		require.False(t, claim.Valid())
		require.Equal(t, StateAbsent, s.Lookup("target").State())
	}
	s.releaseRestoreSource(source)
	replacement, err := s.reserveRestoreSource("source")
	require.NoError(t, err)
	t.Cleanup(func() { s.releaseRestoreSource(replacement) })
	s.releaseRestoreSource(source)
	require.Same(t, replacement, s.restoreClaims["source"], "released authority cannot revoke a replacement at the same revision")
	claim, err := s.beginReservedRestore(s.CurrentAdmissionBaseline(), source, "target", opID,
		testBackendRequestSnapshot(t), testCallbackPair(opID))
	require.ErrorIs(t, err, ErrInvalidRestoreClaim)
	require.False(t, claim.Valid())
	claim, err = s.beginReservedRestore(s.CurrentAdmissionBaseline(), replacement, "target", opID,
		testBackendRequestSnapshot(t), testCallbackPair(opID))
	require.NoError(t, err)
	consumed, err := s.refuseRestore(claim)
	require.NoError(t, err)
	require.True(t, consumed)
	require.Empty(t, s.restoreClaims)
}

func TestRestoreSourceReservationReattestsExactSourceAtTransfer(t *testing.T) {
	for _, changed := range []string{"revision", "backend", "storage identity", "principal", "withdrawn namespace"} {
		t.Run(changed, func(t *testing.T) {
			s := newRestoreTestStore(t)
			source, err := s.reserveRestoreSource("source")
			require.NoError(t, err)
			t.Cleanup(func() { s.releaseRestoreSource(source) })
			baseline := s.CurrentAdmissionBaseline()
			switch changed {
			case "revision":
				record := s.cache["source"]
				record.revision++
				s.cache["source"] = record
			case "backend":
				record := s.cache["source"]
				record.Backend = "backend-b"
				s.cache["source"] = record
			case "storage identity":
				s.backendStorageIDs["backend-a"] = testBackendStorageID("replacement")
			case "principal":
				capability := s.lifecycleCache["source"]
				capability.principal = runtimePrincipal{tenant: "foreign", providerUUID: s.providerUUID}
				s.lifecycleCache["source"] = capability
			case "withdrawn namespace":
				require.NoError(t, s.Close())
			}
			opID := requireOperationID(t, "2316")
			claim, err := s.beginReservedRestore(baseline, source, "target", opID,
				testBackendRequestSnapshot(t), testCallbackPair(opID))
			require.Error(t, err)
			require.False(t, claim.Valid())
			require.NotContains(t, s.cache, "target")
			s.releaseRestoreSource(source)
			require.Empty(t, s.restoreClaims, "namespace withdrawal must not leak the process-local reservation")
		})
	}
}
