package shared

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

// maintenancePairForTest preserves older unbound-store fixtures while keeping
// production construction strict. Tests exercising lineage use
// NewMaintenanceSettlement directly.
func maintenancePairForTest(
	t *testing.T,
	callbacks *CallbackStore,
	releases *ReleaseStore,
) *MaintenanceSettlement {
	if t != nil {
		t.Helper()
	}
	return &MaintenanceSettlement{journalPair: journalPair{
		callbacks: callbacks,
		releases:  releases,
	}, mutation: substratemutation.NewProtocol[MaintenancePhysicalSubject]()}
}

// The low-level callback-journal tests intentionally exercise transactions in
// isolation. These test-only adapters preserve that coverage without
// re-exporting phase mutation methods from CallbackStore in production.
func (s *CallbackStore) NewMaintenanceRequestAuthority(
	maintenanceID MaintenanceID,
	kind MaintenanceIntentKind,
	leaseUUID, callbackURL string,
	payload []byte,
) (MaintenanceRequestAuthority, error) {
	backendName, storageID := s.journalBackendIdentity("")
	if backendName == "" || !storageID.Valid() {
		return MaintenanceRequestAuthority{}, errors.New(
			"maintenance request authority requires an identity-bound callback journal",
		)
	}
	return newMaintenanceRequestAuthority(
		s, maintenanceID, kind, leaseUUID, callbackURL, payload, backendName, storageID,
	)
}

func (s *CallbackStore) ProbeMaintenanceIntent(
	request MaintenanceRequestAuthority,
) (MaintenanceIntentAdmissionDisposition, error) {
	if request.issuer != s || s == nil {
		return MaintenanceIntentAdmissionNone, errors.New(
			"maintenance request authority was not minted by this callback journal",
		)
	}
	if !request.Valid() {
		return MaintenanceIntentAdmissionNone, errors.New("maintenance probe requires exact request authority")
	}
	var disposition MaintenanceIntentAdmissionDisposition
	err := s.view(func(tx *bolt.Tx) error {
		var classifyErr error
		disposition, classifyErr = classifyMaintenanceReplayTx(
			tx, request.LeaseUUID(), request.MaintenanceID(),
			encodeMaintenanceDigest(request.digest),
		)
		return classifyErr
	})
	return disposition, err
}

func (s *CallbackStore) NewMaintenanceIntentCandidate(
	request MaintenanceRequestAuthority,
	source ReleaseClaim,
	target Release,
) (MaintenanceIntentCandidate, error) {
	if source.issuer == nil {
		return MaintenanceIntentCandidate{}, errors.New("maintenance source has no issuing release journal")
	}
	settlement := maintenancePairForTest(nil, s, source.issuer)
	return settlement.NewMaintenanceIntentCandidate(
		request,
		MaintenanceSourceClaim{settlement: settlement, releases: source.issuer, claim: source},
		target,
	)
}

func (s *CallbackStore) BeginMaintenanceIntent(
	candidate MaintenanceIntentCandidate,
) (MaintenanceIntentAdmission, error) {
	if candidate.settlement == nil || candidate.issuer != s {
		return MaintenanceIntentAdmission{}, errors.New("maintenance candidate was not minted by this journal pair")
	}
	return candidate.settlement.BeginMaintenanceIntent(candidate)
}

func (s *CallbackStore) StartMaintenanceAppend(
	dispatch MaintenanceIntentDispatch,
) (MaintenanceAppendClaim, error) {
	if dispatch.settlement == nil || dispatch.issuer != s {
		return MaintenanceAppendClaim{}, errors.New("maintenance dispatch was not minted by this journal pair")
	}
	return dispatch.settlement.StartMaintenanceAppend(dispatch)
}

func (s *CallbackStore) BindMaintenanceIntentTarget(
	claim MaintenanceIntentClaim,
	target MaintenanceReleaseClaim,
) (MaintenanceReleaseClaim, error) {
	if !maintenanceIntentClaimsEqual(claim, target.intent) {
		return MaintenanceReleaseClaim{}, errors.New("maintenance target belongs to another intent")
	}
	if target.settlement == nil || target.callbacks != s {
		return MaintenanceReleaseClaim{}, errors.New("maintenance target belongs to another settlement")
	}
	return target.settlement.BindMaintenanceIntentTarget(target)
}

func (s *CallbackStore) TryBindMaintenanceIntentTarget(
	claim MaintenanceIntentClaim,
	target MaintenanceReleaseClaim,
) (MaintenanceReleaseClaim, bool, error) {
	if !maintenanceIntentClaimsEqual(claim, target.intent) {
		return MaintenanceReleaseClaim{}, false, errors.New("maintenance target belongs to another intent")
	}
	if target.settlement == nil || target.callbacks != s {
		return MaintenanceReleaseClaim{}, false, errors.New("maintenance target belongs to another settlement")
	}
	return target.settlement.TryBindMaintenanceIntentTarget(target)
}

func (s *CallbackStore) CancelMaintenanceIntent(dispatch MaintenanceIntentDispatch) error {
	if dispatch.settlement == nil || dispatch.issuer != s {
		return errors.New("maintenance dispatch was not minted by this journal pair")
	}
	return dispatch.settlement.CancelMaintenanceIntent(dispatch)
}

func resolveMaintenanceForTest(
	t *testing.T,
	callbacks *CallbackStore,
	releases *ReleaseStore,
	claim MaintenanceIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) (CallbackEntry, error) {
	t.Helper()
	settlement := claim.settlement
	if settlement == nil || settlement.callbacks != callbacks || settlement.releases != releases {
		return CallbackEntry{}, errors.New("maintenance claim belongs to another settlement")
	}
	// StartMaintenanceExecution advances the callback-side causal phase and
	// invalidates the pre-effect claim retained by older fixtures. Settlement
	// must consume the exact refreshed durable claim, as production recovery
	// does after every process boundary.
	current, found, err := settlement.GetMaintenanceIntent(claim.LeaseUUID())
	if err != nil {
		return CallbackEntry{}, err
	}
	if found && current.MaintenanceID() == claim.MaintenanceID() {
		claim = current
	}
	if status == backend.CallbackStatusSuccess {
		active, err := settlement.ProveMaintenanceActive(claim)
		if err != nil {
			return CallbackEntry{}, err
		}
		return resolveMaintenanceSuccessForTest(settlement, active)
	}
	failed, err := settlement.ProveMaintenanceFailure(claim)
	if err != nil {
		return CallbackEntry{}, err
	}
	return resolveMaintenanceFailureForTest(settlement, failed, errMsg)
}

func activateMaintenanceForTest(
	t *testing.T,
	callbacks *CallbackStore,
	releases *ReleaseStore,
	claim MaintenanceIntentClaim,
	target MaintenanceReleaseClaim,
) error {
	t.Helper()
	settlement := target.settlement
	if settlement == nil || settlement.callbacks != callbacks || settlement.releases != releases {
		return errors.New("maintenance target belongs to another settlement")
	}
	if settlement.execute == nil {
		bindTestMaintenanceMutation(t, settlement, nil)
	}
	execution, err := settlement.StartMaintenanceExecution(target)
	if err != nil {
		return err
	}
	physical := settlement.ExecuteMaintenance(context.Background(), execution)
	success, ok := physical.(MaintenanceExecutionSuccess)
	if !ok {
		return errors.New("test maintenance execution did not succeed")
	}
	_, err = settlement.ActivateMaintenance(success)
	return err
}

func activateMaintenanceOutcomeForTest(
	t *testing.T,
	settlement *MaintenanceSettlement,
	target MaintenanceReleaseClaim,
) MaintenanceReleaseActive {
	t.Helper()
	if settlement.execute == nil {
		bindTestMaintenanceMutation(t, settlement, nil)
	}
	execution, err := settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	physical := settlement.ExecuteMaintenance(context.Background(), execution)
	success, ok := physical.(MaintenanceExecutionSuccess)
	require.True(t, ok)
	active, err := settlement.ActivateMaintenance(success)
	require.NoError(t, err)
	return active
}

func failMaintenanceOutcomeForTest(
	t *testing.T,
	settlement *MaintenanceSettlement,
	target MaintenanceReleaseClaim,
	reason backend.Reason,
	message string,
) MaintenanceReleaseFailure {
	t.Helper()
	failure, err := settlement.RefuseMaintenanceExecution(target)
	require.NoError(t, err)
	failed, err := settlement.FailMaintenance(failure, reason, message)
	require.NoError(t, err)
	return failed
}

func failMaintenanceForTest(
	t *testing.T,
	callbacks *CallbackStore,
	releases *ReleaseStore,
	claim MaintenanceIntentClaim,
	target MaintenanceReleaseClaim,
	reason backend.Reason,
	message string,
) error {
	t.Helper()
	settlement := target.settlement
	if settlement == nil || settlement.callbacks != callbacks || settlement.releases != releases {
		return errors.New("maintenance target belongs to another settlement")
	}
	failure, err := settlement.RefuseMaintenanceExecution(target)
	if err != nil {
		return err
	}
	_, err = settlement.FailMaintenance(failure, reason, message)

	return err
}

func tryResolveMaintenanceForTest(
	t *testing.T,
	callbacks *CallbackStore,
	releases *ReleaseStore,
	claim MaintenanceIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) (CallbackEntry, bool, error) {
	t.Helper()
	settlement := claim.settlement
	if settlement == nil || settlement.callbacks != callbacks || settlement.releases != releases {
		return CallbackEntry{}, false, errors.New("maintenance claim belongs to another settlement")
	}
	if status == backend.CallbackStatusSuccess {
		active, err := settlement.ProveMaintenanceActive(claim)
		if err != nil {
			return CallbackEntry{}, false, err
		}
		return tryResolveMaintenanceSuccessForTest(settlement, active)
	}
	failed, err := settlement.ProveMaintenanceFailure(claim)
	if err != nil {
		return CallbackEntry{}, false, err
	}
	return tryResolveMaintenanceFailureForTest(settlement, failed, errMsg)
}

func resolveMaintenanceSuccessForTest(
	settlement *MaintenanceSettlement,
	active MaintenanceReleaseActive,
) (CallbackEntry, error) {
	claim := active.intent
	if err := settlement.validateIntent(claim); err != nil {
		return CallbackEntry{}, err
	}
	unlock := settlement.lockLease(claim.LeaseUUID())
	defer unlock()
	return settlement.resolveSuccessLocked(claim, active)
}

func resolveMaintenanceFailureForTest(
	settlement *MaintenanceSettlement,
	failed MaintenanceReleaseFailure,
	errMsg string,
) (CallbackEntry, error) {
	claim := failed.intent
	if err := settlement.validateIntent(claim); err != nil {
		return CallbackEntry{}, err
	}
	unlock := settlement.lockLease(claim.LeaseUUID())
	defer unlock()
	return settlement.resolveFailureLocked(claim, failed, errMsg)
}

func tryResolveMaintenanceSuccessForTest(
	settlement *MaintenanceSettlement,
	active MaintenanceReleaseActive,
) (entry CallbackEntry, acquired bool, err error) {
	claim := active.intent
	if err := settlement.validateIntent(claim); err != nil {
		return CallbackEntry{}, false, err
	}
	unlock, acquired := settlement.tryLockLease(claim.LeaseUUID())
	if !acquired {
		return CallbackEntry{}, false, nil
	}
	defer unlock()
	entry, err = settlement.resolveSuccessLocked(claim, active)
	return entry, true, err
}

func tryResolveMaintenanceFailureForTest(
	settlement *MaintenanceSettlement,
	failed MaintenanceReleaseFailure,
	errMsg string,
) (entry CallbackEntry, acquired bool, err error) {
	claim := failed.intent
	if err := settlement.validateIntent(claim); err != nil {
		return CallbackEntry{}, false, err
	}
	unlock, acquired := settlement.tryLockLease(claim.LeaseUUID())
	if !acquired {
		return CallbackEntry{}, false, nil
	}
	defer unlock()
	entry, err = settlement.resolveFailureLocked(claim, failed, errMsg)
	return entry, true, err
}

func tryResolveMaintenanceRuntimeFailureForTest(
	settlement *MaintenanceSettlement,
	active MaintenanceReleaseActive,
	errMsg string,
) (acquired bool, err error) {
	claim := active.intent
	if err := settlement.validateIntent(claim); err != nil {
		return false, err
	}
	unlock, acquired := settlement.tryLockLease(claim.LeaseUUID())
	if !acquired {
		return false, nil
	}
	defer unlock()
	if err := settlement.callbacks.requireCurrentMaintenanceClaim(claim); err != nil {
		return true, err
	}
	if err := settlement.validateActiveProofLocked(claim, active); err != nil {
		return true, err
	}
	maintenance, runtimeFailure, err := prepareDivergedMaintenanceCompletions(claim, errMsg)
	if err != nil {
		return true, err
	}
	_, err = settlement.callbacks.resolveMaintenanceIntentEntriesLocked(
		claim, []CallbackEntry{maintenance, runtimeFailure},
	)
	return true, err
}
