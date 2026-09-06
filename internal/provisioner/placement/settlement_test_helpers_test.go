package placement

import "github.com/manifest-network/fred/internal/provisioner/operation"

// confirmAttemptForTest settles an attempt through the same exclusive claim
// path used by production callback and recovery coordinators. Tests may retain
// the admission token to identify the operation, but the token itself grants no
// direct settlement authority.
func confirmAttemptForTest(store *Store, token AttemptToken) (bool, error) {
	if !token.Valid() || token.issuer != store {
		return false, ErrInvalidAttemptToken
	}
	claim, claimed, err := store.claimAttempt(token.leaseUUID, token.operationID)
	if err != nil || !claimed {
		return false, err
	}
	return store.confirmClaimedAttempt(claim)
}

// refuseAttemptForTest is the definitive-refusal counterpart to
// confirmAttemptForTest. It cannot clear a generation without first acquiring
// the exact production AttemptClaim.
func refuseAttemptForTest(store *Store, token AttemptToken) (bool, error) {
	if !token.Valid() || token.issuer != store {
		return false, ErrInvalidAttemptToken
	}
	claim, claimed, err := store.claimAttempt(token.leaseUUID, token.operationID)
	if err != nil || !claimed {
		return false, err
	}
	return store.refuseClaimedAttempt(claim)
}

func confirmOperationForTest(
	store *Store,
	leaseUUID string,
	backendName string,
	operationID operation.OperationID,
) (bool, error) {
	claim, claimed, err := store.claimAttempt(leaseUUID, operationID)
	if err != nil || !claimed {
		return false, err
	}
	if claim.Backend() != backendName {
		store.releaseAttemptClaim(claim)
		return false, nil
	}
	return store.confirmClaimedAttempt(claim)
}

func refuseOperationForTest(
	store *Store,
	leaseUUID string,
	backendName string,
	operationID operation.OperationID,
) (bool, error) {
	claim, claimed, err := store.claimAttempt(leaseUUID, operationID)
	if err != nil || !claimed {
		return false, err
	}
	if claim.Backend() != backendName {
		store.releaseAttemptClaim(claim)
		return false, nil
	}
	return store.refuseClaimedAttempt(claim)
}
