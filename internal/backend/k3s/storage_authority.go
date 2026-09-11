package k3s

// terminalStorageAuthorityError returns the first backend-lifetime storage
// failure. It intentionally uses a lock separate from identityVerifyMu so a
// bound-store failure hook can publish its exact cause while cluster identity
// verification is already in progress.
func (b *Backend) terminalStorageAuthorityError() error {
	if b == nil {
		return nil
	}
	return b.storeAuthorityGate.Error()
}

// latchTerminalStorageAuthority publishes the cause before canceling the
// backend lifetime. Every authoritative store consults this sticky latch, so a
// failure in one journal withdraws mutation authority from its siblings.
func (b *Backend) latchTerminalStorageAuthority(cause error) error {
	if b == nil || cause == nil {
		return cause
	}
	return b.storeAuthorityGate.Latch(cause)
}

// latchIdentityVerificationFailureLocked mirrors a terminal cluster-lineage
// failure into both verifier-local and backend-wide state. The caller must hold
// identityVerifyMu.
func (b *Backend) latchIdentityVerificationFailureLocked(cause error) error {
	if b.identityDriftErr == nil {
		b.identityDriftErr = cause
	}
	return b.latchTerminalStorageAuthority(b.identityDriftErr)
}
