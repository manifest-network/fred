package shared

// These adapters deliberately exist only in the shared package's test build.
// Production callers must cross the exact CallbackStore/ReleaseStore pair via
// OperationSettlement; storage-level tests still need direct access to verify
// the callback journal's encoding and corruption behavior in isolation.
func (s *CallbackStore) NewOperationIntentProbe(
	leaseUUID, callbackURL string,
) (OperationIntentProbe, error) {
	return s.newOperationIntentProbe(leaseUUID, callbackURL)
}

func (s *CallbackStore) ProbeOperationIntent(
	probe OperationIntentProbe,
) (OperationIntentAdmissionDisposition, error) {
	return s.probeOperationIntent(probe)
}

func (s *CallbackStore) NewOperationIntentCandidate(
	spec OperationIntentSpec,
) (OperationIntentCandidate, error) {
	return s.newOperationIntentCandidate(spec)
}

func (s *CallbackStore) BeginOperationIntent(
	candidate OperationIntentCandidate,
) (OperationIntentAdmission, error) {
	return s.beginOperationIntent(candidate)
}

func (s *CallbackStore) ListOperationIntents() ([]OperationIntentClaim, error) {
	return s.listOperationIntents()
}

func (s *CallbackStore) ListOperationRecoveryStates() ([]OperationRecoveryState, error) {
	return s.listOperationRecoveryStates()
}

func (s *CallbackStore) ListFailedOperationReceipts() ([]FailedOperationReceipt, error) {
	return s.listFailedOperationReceipts()
}

func (s *CallbackStore) LookupOperationRecovery(
	probe OperationIntentProbe,
) (OperationRecoveryState, error) {
	return s.lookupOperationRecovery(probe)
}
