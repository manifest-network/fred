package provisioner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/util"
)

type attemptRedeliveryOutcome uint8

const (
	attemptRedeliveryDeferred attemptRedeliveryOutcome = iota
	attemptRedeliveryAccepted
	attemptRedeliveryRefused
)

type attemptRedeliveryResult struct {
	outcome attemptRedeliveryOutcome
	err     error
}

type attemptRedeliveryClaims struct {
	target operation.LeaseClaim
	source operation.LeaseClaim
}

// redeliverPlacementAttempt supplies the recovery half of the durable
// write-ahead protocol. An unresolved attempt is not merely a reason to wait:
// it is enough exact authority to retry the same operation against the same
// backend. Every uncertainty retains that authority for a later sweep.
func (r *Reconciler) redeliverPlacementAttempt(
	ctx context.Context,
	leaseUUID string,
	record placement.Placement,
	metadata placement.AttemptMetadata,
	snapshot operation.TrackerSnapshot,
) attemptRedeliveryResult {
	if record.Attempt == "" || !metadata.Valid() || !snapshot.Valid() {
		return attemptRedeliveryResult{err: errors.New("invalid durable redelivery authority")}
	}
	if record.State() == placement.StateUnusable || record.Conflict {
		return attemptRedeliveryResult{err: errors.New("unusable placement cannot authorize redelivery")}
	}

	claims, err := r.claimAttemptRedeliveryLeases(
		leaseUUID, metadata.RestoreSourceLeaseUUID(), snapshot,
	)
	if err != nil {
		return attemptRedeliveryResult{err: err}
	}
	defer r.releaseAttemptRedeliveryLeases(leaseUUID, metadata, claims)

	current := r.placementAuthority.Lookup(leaseUUID)
	if current.Revision() != record.Revision() || current.Attempt != record.Attempt ||
		current.AttemptMetadata() != metadata {
		return attemptRedeliveryResult{err: errors.New("durable attempt changed before redelivery claim")}
	}
	attemptClaim, claimed, err := r.placementAuthority.ClaimAttempt(
		leaseUUID, metadata.OperationID(),
	)
	if err != nil {
		return attemptRedeliveryResult{err: fmt.Errorf("claim exact placement attempt: %w", err)}
	}
	if !claimed || !attemptClaim.Valid() || attemptClaim.Backend() != record.Attempt ||
		attemptClaim.Metadata() != metadata {
		return attemptRedeliveryResult{err: errors.New("exact placement attempt is no longer claimable")}
	}
	defer r.placementAuthority.ReleaseAttemptClaim(attemptClaim)

	target, err := r.getLeaseBounded(ctx, leaseUUID)
	if err != nil {
		return attemptRedeliveryResult{err: fmt.Errorf("re-read redelivery target: %w", err)}
	}
	requestSnapshot := metadata.RequestSnapshot()
	if !requestSnapshot.Valid() {
		return attemptRedeliveryResult{err: errors.New("durable backend request snapshot is invalid")}
	}
	if target == nil || target.Uuid != leaseUUID ||
		target.Tenant != requestSnapshot.Tenant() ||
		target.ProviderUuid != requestSnapshot.ProviderUUID() ||
		requestSnapshot.ProviderUUID() != r.providerUUID ||
		(target.State != billingtypes.LEASE_STATE_PENDING &&
			target.State != billingtypes.LEASE_STATE_ACTIVE) {
		return attemptRedeliveryResult{err: errors.New("redelivery target is no longer a live exact lease")}
	}
	if metadata.Kind() == operation.KindRestore {
		if target.State != billingtypes.LEASE_STATE_PENDING {
			return attemptRedeliveryResult{err: errors.New("restore redelivery target is no longer pending")}
		}
		if err := r.validateRestoreRedeliverySource(ctx, target, record.Attempt, metadata); err != nil {
			return attemptRedeliveryResult{err: err}
		}
	}

	backendClient := r.backendRouter.GetBackendByName(record.Attempt)
	if util.IsNilInterface(backendClient) || backendClient.Name() != record.Attempt {
		return attemptRedeliveryResult{
			err: fmt.Errorf("attempt backend %q is unavailable", record.Attempt),
		}
	}
	callbackPair := metadata.CallbackPair()
	callbackURL := callbackPair.OperationURL()
	lifecycleCallbackURL := callbackPair.LifecycleURL()
	if callbackURL == "" || lifecycleCallbackURL == "" {
		return attemptRedeliveryResult{err: errors.New("durable callback destinations are invalid")}
	}
	items := requestSnapshot.Items()
	if len(items) == 0 {
		return attemptRedeliveryResult{err: errors.New("durable backend request items are invalid")}
	}

	var callErr error
	switch metadata.Kind() {
	case operation.KindProvision:
		request, requestErr := r.rebuildProvisionRequest(
			target, requestSnapshot, callbackURL, lifecycleCallbackURL,
			metadata.PayloadFingerprint(),
		)
		if requestErr != nil {
			return attemptRedeliveryResult{err: requestErr}
		}
		callErr = invokeBackendProvision(ctx, backendClient, request)
	case operation.KindRestore:
		callErr = invokeBackendRestore(ctx, backendClient, backend.RestoreRequest{
			LeaseUUID:            target.Uuid,
			FromLeaseUUID:        metadata.RestoreSourceLeaseUUID(),
			Tenant:               requestSnapshot.Tenant(),
			ProviderUUID:         requestSnapshot.ProviderUUID(),
			Items:                items,
			CallbackURL:          callbackURL,
			LifecycleCallbackURL: lifecycleCallbackURL,
		})
	default:
		return attemptRedeliveryResult{err: operation.ErrInvalidKind}
	}

	if callErr != nil {
		if !definitiveRedeliveryRefusal(metadata.Kind(), callErr) {
			return attemptRedeliveryResult{err: fmt.Errorf("exact backend redelivery remains ambiguous: %w", callErr)}
		}
		settled, settleErr := r.placementAuthority.RefuseClaimedAttempt(attemptClaim)
		if settleErr != nil {
			return attemptRedeliveryResult{err: fmt.Errorf("refuse exact redelivered attempt: %w", settleErr)}
		}
		if !settled {
			return attemptRedeliveryResult{err: errors.New("exact refused attempt changed before settlement")}
		}
		slog.Info("reconcile: backend definitively refused exact durable operation redelivery",
			"lease_uuid", leaseUUID,
			"backend", record.Attempt,
			"operation_id", metadata.OperationID(),
			"operation_kind", metadata.Kind(),
			"error", callErr,
		)
		return attemptRedeliveryResult{outcome: attemptRedeliveryRefused}
	}

	recovered := r.operations.RecoverClaimed(
		claims.target,
		metadata.OperationID(),
		operation.TrackSpec{
			LeaseUUID: target.Uuid,
			Tenant:    requestSnapshot.Tenant(),
			Items:     items,
			Backend:   record.Attempt,
			Kind:      metadata.Kind(),
		},
	)
	if !recovered.Recovered() {
		return attemptRedeliveryResult{
			err: fmt.Errorf("recover accepted operation registry gate: %s", recovered),
		}
	}
	confirmed, confirmErr := r.placementAuthority.ConfirmClaimedAttempt(attemptClaim)
	if confirmErr != nil {
		return attemptRedeliveryResult{err: fmt.Errorf("confirm accepted exact attempt: %w", confirmErr)}
	}
	if !confirmed {
		return attemptRedeliveryResult{err: errors.New("accepted exact attempt changed before confirmation")}
	}
	slog.Info("reconcile: recovered exact durable backend operation",
		"lease_uuid", leaseUUID,
		"backend", record.Attempt,
		"operation_id", metadata.OperationID(),
		"operation_kind", metadata.Kind(),
	)
	return attemptRedeliveryResult{outcome: attemptRedeliveryAccepted}
}

func (r *Reconciler) claimAttemptRedeliveryLeases(
	targetLeaseUUID string,
	sourceLeaseUUID string,
	snapshot operation.TrackerSnapshot,
) (attemptRedeliveryClaims, error) {
	leaseUUIDs := []string{targetLeaseUUID}
	if sourceLeaseUUID != "" {
		if sourceLeaseUUID == targetLeaseUUID {
			return attemptRedeliveryClaims{}, errors.New("restore source and target leases are identical")
		}
		leaseUUIDs = append(leaseUUIDs, sourceLeaseUUID)
	}
	slices.Sort(leaseUUIDs)
	claimsByLease := make(map[string]operation.LeaseClaim, len(leaseUUIDs))
	for _, currentLeaseUUID := range leaseUUIDs {
		result := r.operations.TryClaimLease(currentLeaseUUID, snapshot)
		if !result.Acquired() {
			for _, claim := range claimsByLease {
				r.operations.ReleaseLease(claim)
			}
			return attemptRedeliveryClaims{}, fmt.Errorf(
				"claim redelivery lease %q: outcome %d", currentLeaseUUID, result.Outcome(),
			)
		}
		claimsByLease[currentLeaseUUID] = result.Claim()
	}
	return attemptRedeliveryClaims{
		target: claimsByLease[targetLeaseUUID],
		source: claimsByLease[sourceLeaseUUID],
	}, nil
}

func (r *Reconciler) releaseAttemptRedeliveryLeases(
	targetLeaseUUID string,
	metadata placement.AttemptMetadata,
	claims attemptRedeliveryClaims,
) {
	if claims.source.Valid() && !r.operations.ReleaseLease(claims.source) {
		slog.Error("failed to release restore redelivery source claim",
			"lease_uuid", metadata.RestoreSourceLeaseUUID())
	}
	if claims.target.Valid() && !r.operations.ReleaseLease(claims.target) {
		slog.Error("failed to release operation redelivery target claim",
			"lease_uuid", targetLeaseUUID)
	}
}

func (r *Reconciler) validateRestoreRedeliverySource(
	ctx context.Context,
	target *billingtypes.Lease,
	backendName string,
	metadata placement.AttemptMetadata,
) error {
	sourceLeaseUUID := metadata.RestoreSourceLeaseUUID()
	if sourceLeaseUUID == "" || sourceLeaseUUID == target.Uuid {
		return errors.New("restore attempt has invalid durable source identity")
	}
	sourcePlacement := r.placementAuthority.Lookup(sourceLeaseUUID)
	switch sourcePlacement.State() {
	case placement.StateConfirmed:
		if sourcePlacement.Backend != backendName || sourcePlacement.Attempt != "" {
			return errors.New("restore source placement no longer matches the attempted backend")
		}
	case placement.StateAbsent:
		// A previously accepted restore may already have consumed and retired the
		// source. The exact backend intent check precedes its live-source checks, so
		// absence cannot invalidate a safe idempotent retry.
	default:
		return errors.New("restore source placement is ambiguous")
	}

	source, err := r.getLeaseBounded(ctx, sourceLeaseUUID)
	if err != nil {
		return fmt.Errorf("re-read restore redelivery source: %w", err)
	}
	if source == nil {
		// Closed source leases may be pruned before retained data expires. Durable
		// restore admission is the authorization evidence in that case.
		return nil
	}
	if source.Uuid != sourceLeaseUUID || source.Tenant != target.Tenant ||
		source.ProviderUuid != r.providerUUID {
		return errors.New("restore source no longer belongs to the target tenant and provider")
	}
	return nil
}

func (r *Reconciler) rebuildProvisionRequest(
	lease *billingtypes.Lease,
	requestSnapshot placement.BackendRequestSnapshot,
	callbackURL string,
	lifecycleCallbackURL string,
	fingerprint placement.PayloadFingerprint,
) (backend.ProvisionRequest, error) {
	if !requestSnapshot.Valid() {
		return backend.ProvisionRequest{}, errors.New(
			"rebuild exact provision request: invalid durable request snapshot",
		)
	}
	request := backend.ProvisionRequest{
		LeaseUUID:            lease.Uuid,
		Tenant:               requestSnapshot.Tenant(),
		ProviderUUID:         requestSnapshot.ProviderUUID(),
		Items:                requestSnapshot.Items(),
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
	}
	if !fingerprint.Valid() {
		if len(lease.MetaHash) != 0 {
			return backend.ProvisionRequest{}, errors.New(
				"rebuild exact provision request: payload-bearing lease attempt has no durable fingerprint",
			)
		}
		return request, nil
	}
	store := r.payloadStore()
	if store == nil {
		return backend.ProvisionRequest{}, fmt.Errorf(
			"rebuild exact provision request: %w", errPayloadNotAvailable,
		)
	}
	payloadBytes, recordedHash, err := store.GetWithHash(lease.Uuid)
	if err != nil {
		return backend.ProvisionRequest{}, fmt.Errorf("read exact provision payload: %w", err)
	}
	if payloadBytes == nil {
		return backend.ProvisionRequest{}, fmt.Errorf(
			"rebuild exact provision request: %w", errPayloadNotAvailable,
		)
	}
	expectedHash := fingerprint.Bytes()
	if len(recordedHash) != 0 && !bytes.Equal(recordedHash, expectedHash) {
		return backend.ProvisionRequest{}, errors.New(
			"rebuild exact provision request: payload-store hash differs from durable attempt",
		)
	}
	// A legacy payload row may omit its redundant recorded hash. The durable
	// attempt fingerprint still binds the bytes sent before the ambiguous call,
	// including an ENG-619 update whose hash legitimately differs from MetaHash.
	if err := payload.VerifyHash(payloadBytes, expectedHash); err != nil {
		return backend.ProvisionRequest{}, fmt.Errorf("verify exact provision payload: %w", err)
	}
	request.Payload = payloadBytes
	request.PayloadHash = fingerprint.String()
	return request, nil
}

func definitiveRedeliveryRefusal(kind operation.Kind, err error) bool {
	// Local transport gates must retain the exact operation even when they prove
	// this particular retry was unsent. Retrying the same ID is the recovery
	// protocol; allocating a replacement would discard evidence and can duplicate
	// a prior ambiguous delivery.
	if errors.Is(err, backend.ErrCircuitOpen) ||
		errors.Is(err, backend.ErrBackendStorageIdentityUnbound) ||
		errors.Is(err, backend.ErrBackendStorageIdentityMissing) ||
		errors.Is(err, backend.ErrBackendStorageIdentityMismatch) ||
		errors.Is(err, backend.ErrBackendUpgradeRequired) {
		return false
	}
	if kind == operation.KindProvision {
		return errors.Is(err, backend.ErrValidation) ||
			errors.Is(err, backend.ErrCapacityRefused)
	}
	if kind != operation.KindRestore {
		return false
	}
	return errors.Is(err, backend.ErrNotRetained) ||
		errors.Is(err, backend.ErrInvalidState) ||
		errors.Is(err, backend.ErrCapacityRefused) ||
		errors.Is(err, backend.ErrDemoteDataExceedsTier) ||
		errors.Is(err, backend.ErrValidation) ||
		errors.Is(err, backend.ErrRestoreRefused)
}

// convergeTerminalPlacementAttempt closes the other half of exact redelivery:
// once the chain positively says the target is terminal, replaying provision or
// restore would be wrong, but leaving a request-never-received Attempt forever
// would wedge placement and topology retirement. Under the same Registry ->
// placement claim order as callbacks and live redelivery, synchronously tear
// down every exact durable candidate and promote the attempted backend to
// conservative closed-lease affinity. Promotion is intentional: Deprovision may
// have retained data, and the bundled backends durably enqueue the exact failed
// operation followed by the lifecycle teardown observation before returning.
// A true no-op therefore leaves only a ghost owner that normal terminal/absent
// inventory pruning removes on a later sweep.
func (r *Reconciler) convergeTerminalPlacementAttempt(
	ctx context.Context,
	leaseUUID string,
	record placement.Placement,
	metadata placement.AttemptMetadata,
	snapshot operation.TrackerSnapshot,
) (bool, error) {
	if record.Attempt == "" || !metadata.Valid() || !snapshot.Valid() {
		return false, errors.New("invalid terminal durable-attempt authority")
	}
	if record.State() == placement.StateUnusable || record.Conflict {
		return false, errors.New("unusable placement cannot authorize terminal teardown")
	}

	claims, err := r.claimAttemptRedeliveryLeases(
		leaseUUID, metadata.RestoreSourceLeaseUUID(), snapshot,
	)
	if err != nil {
		return false, err
	}
	defer r.releaseAttemptRedeliveryLeases(leaseUUID, metadata, claims)

	current := r.placementAuthority.Lookup(leaseUUID)
	if current.Revision() != record.Revision() || current.Attempt != record.Attempt ||
		current.AttemptMetadata() != metadata {
		return false, errors.New("durable attempt changed before terminal convergence")
	}
	attemptClaim, claimed, err := r.placementAuthority.ClaimAttempt(
		leaseUUID, metadata.OperationID(),
	)
	if err != nil {
		return false, fmt.Errorf("claim terminal placement attempt: %w", err)
	}
	if !claimed || !attemptClaim.Valid() || attemptClaim.Backend() != record.Attempt ||
		attemptClaim.Metadata() != metadata {
		return false, errors.New("exact terminal placement attempt is no longer claimable")
	}
	defer r.placementAuthority.ReleaseAttemptClaim(attemptClaim)

	target, err := r.getLeaseBounded(ctx, leaseUUID)
	if err != nil {
		return false, fmt.Errorf("re-read terminal attempt target: %w", err)
	}
	requestSnapshot := metadata.RequestSnapshot()
	if target == nil || target.Uuid != leaseUUID || !requestSnapshot.Valid() ||
		target.Tenant != requestSnapshot.Tenant() ||
		target.ProviderUuid != requestSnapshot.ProviderUUID() ||
		requestSnapshot.ProviderUUID() != r.providerUUID {
		return false, errors.New("terminal attempt target lacks exact provider chain authority")
	}
	liveness, _ := classifyLease(target, nil)
	if liveness != leaseTerminal {
		return false, fmt.Errorf("attempt target is not terminal (state %s)", target.State.String())
	}

	candidateNames := []string{record.Attempt}
	if record.Backend != "" && record.Backend != record.Attempt {
		candidateNames = append(candidateNames, record.Backend)
	}
	// Resolve the full durable set before the first side effect. A removed name is
	// not permission to sweep or fall back to a different configured backend.
	candidates := make([]backend.Backend, 0, len(candidateNames))
	for _, backendName := range candidateNames {
		backendClient := r.backendRouter.GetBackendByName(backendName)
		if util.IsNilInterface(backendClient) || backendClient.Name() != backendName {
			return false, fmt.Errorf("terminal attempt backend %q is unavailable", backendName)
		}
		candidates = append(candidates, backendClient)
	}

	var deprovisionErrs []error
	for _, backendClient := range candidates {
		if err := invokeBackendDeprovision(ctx, backendClient, leaseUUID); err != nil {
			deprovisionErrs = append(deprovisionErrs, fmt.Errorf(
				"backend %s: %w", backendClient.Name(), err,
			))
		}
	}
	if len(deprovisionErrs) != 0 {
		return false, fmt.Errorf(
			"terminal exact teardown remains ambiguous: %w", errors.Join(deprovisionErrs...),
		)
	}

	confirmed, err := r.placementAuthority.ConfirmClaimedAttempt(attemptClaim)
	if err != nil {
		return false, fmt.Errorf("promote terminal attempted backend affinity: %w", err)
	}
	if !confirmed {
		return false, errors.New("terminal attempt changed before affinity promotion")
	}
	slog.Info("reconcile: converged terminal durable operation by exact teardown",
		"lease_uuid", leaseUUID,
		"backend", record.Attempt,
		"operation_id", metadata.OperationID(),
		"operation_kind", metadata.Kind(),
	)
	return true, nil
}
