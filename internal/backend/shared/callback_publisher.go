package shared

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/backend"
)

// CallbackPublisher is the construction-bound semantic boundary between
// backend outcomes and the durable callback journal. It owns exact operation
// and maintenance settlement authority; CallbackSender owns only replay and
// transport. The zero value is invalid.
//
// Publication and settlement are one bbolt transaction under the callback
// store's per-lease gate. Transport subscribes to the store independently;
// semantic publication cannot invoke or depend on callback HTTP machinery.
type CallbackPublisher struct {
	store        *CallbackStore
	operations   *OperationSettlement
	maintenance  *MaintenanceSettlement
	attestor     *CallbackStorageAttestor
	logger       *slog.Logger
	onStoreError func()
}

// CallbackPublisherConfig binds semantic publication directly to the exact
// journals and their storage attestor. Sender/HTTP configuration is
// deliberately absent: publication commits facts, while a separately composed
// sender transports them.
type CallbackPublisherConfig struct {
	OperationSettlement   *OperationSettlement
	MaintenanceSettlement *MaintenanceSettlement
	StorageAttestor       *CallbackStorageAttestor
	Logger                *slog.Logger
	OnStoreError          func()
}

// NewCallbackPublisher constructs a publisher only when every dependency
// names the same exact open CallbackStore, ReleaseStore, storage identity, and
// backend-lifetime authority gate. Merely matching durable lineage is not
// sufficient after a store has been closed and reopened.
func NewCallbackPublisher(cfg CallbackPublisherConfig) (*CallbackPublisher, error) {
	operations := cfg.OperationSettlement
	maintenance := cfg.MaintenanceSettlement
	attestor := cfg.StorageAttestor
	if operations == nil || !operations.valid() {
		return nil, errors.New("callback publisher: operation settlement is required")
	}
	if maintenance == nil || !maintenance.valid() {
		return nil, errors.New("callback publisher: maintenance settlement is required")
	}
	if operations.callbacks != maintenance.callbacks ||
		operations.releases != maintenance.releases {
		return nil, errors.New("callback publisher: settlement services belong to different journals")
	}
	if attestor == nil || !attestor.validFor(operations.callbacks) {
		return nil, errors.New("callback publisher: exact callback storage attestor is required")
	}
	if cfg.Logger == nil {
		return nil, errors.New("callback publisher: logger is required")
	}
	if operations.callbacks.binding == nil || operations.callbacks.backendAuthorityGate == nil ||
		attestor.storageID != operations.callbacks.binding.storageID {
		return nil, errors.New("callback publisher: attestor and journals do not share storage authority")
	}
	return &CallbackPublisher{
		store:        operations.callbacks,
		operations:   operations,
		maintenance:  maintenance,
		attestor:     attestor,
		logger:       cfg.Logger,
		onStoreError: cfg.OnStoreError,
	}, nil
}

func (p *CallbackPublisher) valid() bool {
	return p != nil && p.store != nil && p.operations != nil && p.maintenance != nil &&
		p.attestor != nil && p.logger != nil &&
		p.operations.valid() && p.maintenance.valid() &&
		p.store == p.operations.callbacks && p.store == p.maintenance.callbacks &&
		p.operations.releases == p.maintenance.releases &&
		p.store.binding != nil && p.store.backendAuthorityGate != nil &&
		p.operations.releases.binding != nil &&
		p.operations.releases.backendAuthorityGate == p.store.backendAuthorityGate &&
		p.attestor.validFor(p.store) &&
		p.attestor.storageID == p.store.binding.storageID
}

// PublishOperationSuccessContext atomically settles an exact provision/restore
// operation and publishes Success. The status, route, backend, and storage
// identity are all derived from the store-issued committed-release proof.
// Cancellation while waiting for the lease gate leaves the exact durable
// operation head for level-triggered recovery; it can never manufacture a
// failure or erase committed evidence.
func (p *CallbackPublisher) PublishOperationSuccessContext(
	ctx context.Context,
	committed OperationReleaseCommitted,
) error {
	if ctx == nil {
		return errors.New("callback publisher: operation success ownership context is required")
	}
	if !p.valid() {
		return errors.New("callback publisher is invalid")
	}
	if !committed.Valid() {
		return errors.New("callback publisher: committed release proof is required")
	}
	if committed.settlement != p.operations ||
		committed.callbacks != p.operations.callbacks ||
		committed.releases != p.operations.releases {
		return errors.New("callback publisher: operation success belongs to another journal pair")
	}
	unlock, publish, err := p.lockOperationPublication(ctx, committed.authority)
	if err != nil || !publish {
		return err
	}
	defer unlock()
	return p.finishOperationPublication(
		committed.LeaseUUID(),
		p.operations.settleOperationSuccessCallbackLocked(committed),
	)
}

// PublishOperationFailureContext atomically settles an exact provision/restore
// operation and publishes Failed. The proof establishes that its release was
// not committed; a success proof cannot be substituted. Cancellation leaves
// the durable operation head for level-triggered recovery.
func (p *CallbackPublisher) PublishOperationFailureContext(
	ctx context.Context,
	uncommitted OperationReleaseUncommitted,
	errMsg string,
) error {
	if ctx == nil {
		return errors.New("callback publisher: operation failure ownership context is required")
	}
	if !p.valid() {
		return errors.New("callback publisher is invalid")
	}
	if !uncommitted.Valid() {
		return errors.New("callback publisher: uncommitted release proof is required")
	}
	if uncommitted.settlement != p.operations ||
		uncommitted.callbacks != p.operations.callbacks ||
		uncommitted.releases != p.operations.releases {
		return errors.New("callback publisher: operation failure belongs to another journal pair")
	}
	unlock, publish, err := p.lockOperationPublication(ctx, uncommitted.authority)
	if err != nil || !publish {
		return err
	}
	defer unlock()
	return p.finishOperationPublication(
		uncommitted.LeaseUUID(),
		p.operations.settleOperationFailureCallbackLocked(uncommitted, errMsg),
	)
}

// lockOperationPublication performs only the common pre-publication checks and
// returns the held lease gate. Phase-specific callers retain their typed proof
// and invoke their matching settlement directly; there is no free-standing
// closure which can be paired with another operation's lock authority.
func (p *CallbackPublisher) lockOperationPublication(
	ctx context.Context,
	authority operationAuthority,
) (unlock func(), publish bool, err error) {
	leaseUUID := authority.LeaseUUID()
	callbackURL := authority.CallbackURL()
	if callbackURL != "" {
		if err := backend.ValidateOperationCallbackURL(callbackURL); err != nil {
			return nil, false, fmt.Errorf("invalid operation callback URL: %w", err)
		}
	}
	if callbackURL == "" {
		p.logger.Warn("no callback URL for lease", "lease_uuid", leaseUUID)
		return nil, false, nil
	}
	if canceled(ctx) {
		p.logCanceled(ctx, leaseUUID, "")
		return nil, false, ctx.Err()
	}
	if err := p.prepublicationCheck(ctx); err != nil {
		if isTerminalStorageAuthorityError(err) {
			return nil, false, fmt.Errorf("callback publication lost storage authority: %w", err)
		}
		p.logger.Warn(
			"persisting callback but deferring delivery until backend identity is re-attested",
			"error", err, "lease_uuid", leaseUUID,
		)
	}

	unlock, err = p.store.lockDeliveryLeaseContext(ctx, leaseUUID)
	if err != nil {
		p.logCanceled(ctx, leaseUUID, "while waiting for FIFO")
		return nil, false, err
	}
	if canceled(ctx) {
		unlock()
		p.logCanceled(ctx, leaseUUID, "")
		return nil, false, ctx.Err()
	}
	if err := p.prepublicationCheck(ctx); err != nil {
		if isTerminalStorageAuthorityError(err) {
			unlock()
			return nil, false, fmt.Errorf("callback publication lost storage authority while waiting for FIFO: %w", err)
		}
	}
	if canceled(ctx) {
		unlock()
		p.logCanceled(ctx, leaseUUID, "after identity verification")
		return nil, false, ctx.Err()
	}
	return unlock, true, nil
}

func (p *CallbackPublisher) finishOperationPublication(
	leaseUUID string,
	settleErr error,
) error {
	if settleErr != nil {
		p.logger.Error(
			"failed to persist callback; suppressing delivery past unknown durable state",
			"error", settleErr, "lease_uuid", leaseUUID,
		)
		p.reportStoreError()
		return settleErr
	}
	return nil
}

// PublishLifecycleFailureContext records the fixed observation-only Failed
// callback for an authorized runtime phase. RuntimeObservationPermit binds
// both the exact active release and the aggregate mutation head seen during
// authorization. Route, backend, storage identity, status, and retained=false
// are all derived after both facts are re-attested under the shared per-lease
// journal gate. Cancellation while waiting publishes nothing; the observation
// carries no requested-operation settlement which would otherwise need to
// complete during shutdown.
func (p *CallbackPublisher) PublishLifecycleFailureContext(
	ctx context.Context,
	permit RuntimeObservationPermit,
	errMsg string,
) error {
	if ctx == nil {
		return errors.New("callback publisher: lifecycle observation ownership context is required")
	}
	if !p.valid() {
		return errors.New("callback publisher is invalid")
	}
	if !permit.Valid() {
		return errors.New("callback publisher: runtime observation permit is required")
	}
	if permit.callbacks != p.store || permit.releases != p.operations.releases {
		return errors.New("callback publisher: runtime observation permit belongs to another journal pair")
	}
	leaseUUID := permit.LeaseUUID()
	if err := p.prepublicationCheck(ctx); err != nil {
		if isTerminalStorageAuthorityError(err) {
			return fmt.Errorf("lifecycle callback lost storage authority: %w", err)
		}
		p.logger.Warn(
			"persisting lifecycle callback while backend identity re-attestation is transiently unavailable",
			"error", err, "lease_uuid", leaseUUID,
		)
	}

	unlock, err := p.store.lockDeliveryLeaseContext(ctx, leaseUUID)
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.prepublicationCheck(ctx); err != nil {
		if isTerminalStorageAuthorityError(err) {
			return fmt.Errorf("lifecycle callback lost storage authority while waiting for FIFO: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	authority, err := p.reattestRuntimeObservationPermitLocked(permit)
	if err != nil {
		return fmt.Errorf("re-attest lifecycle runtime observation: %w", err)
	}
	callbackURL := authority.LifecycleCallbackURL()
	if callbackURL == "" {
		p.logger.Warn("no lifecycle callback URL for active release", "lease_uuid", leaseUUID)
		return nil
	}
	backendName, storageID := p.store.journalBackendIdentity("")
	entry := CallbackEntry{
		LeaseUUID:        leaseUUID,
		CallbackURL:      callbackURL,
		DeliveryKind:     CallbackDeliveryKindLifecycle,
		Status:           backend.CallbackStatusFailed,
		Backend:          backendName,
		BackendStorageID: storageID.String(),
		Error:            errMsg,
		Retained:         false,
		CreatedAt:        time.Now(),
	}
	if _, err := p.store.storeEntryLocked(entry); err != nil {
		if errors.Is(err, errTerminalLifecyclePending) {
			p.logger.Debug(
				"suppressing lifecycle observation behind pending terminal callback",
				"lease_uuid", leaseUUID, "status", backend.CallbackStatusFailed,
			)
			p.store.notifyReplayCommit(leaseUUID)
			return nil
		}
		p.reportStoreError()
		return fmt.Errorf("persist lifecycle callback: %w", err)
	}
	return nil
}

func (p *CallbackPublisher) reattestRuntimeGenerationLocked(
	proof RuntimeGenerationProof,
) (ReleaseRuntimeIdentity, error) {
	release, claim, err := p.operations.releases.claimLatestActive(proof.LeaseUUID())
	if err != nil {
		return ReleaseRuntimeIdentity{}, err
	}
	authority, ok := release.RuntimeIdentity()
	if !ok {
		return ReleaseRuntimeIdentity{}, errors.New("active release has no exact runtime authority")
	}
	if claim.version != proof.claim.version || claim.digest != proof.claim.digest ||
		authority.Class() != proof.authority.Class() ||
		authority.OperationID() != proof.authority.OperationID() {
		return ReleaseRuntimeIdentity{}, errors.New("active runtime changed before lifecycle failure publication")
	}
	if callbackURL := authority.LifecycleCallbackURL(); callbackURL != "" {
		if err := backend.ValidateLifecycleCallbackURL(callbackURL); err != nil {
			return ReleaseRuntimeIdentity{}, fmt.Errorf("active release lifecycle callback URL: %w", err)
		}
	}
	return authority, nil
}

// PublishMaintenanceSuccessContext atomically settles an exact maintenance
// intent using proof that its target release remains active. Success is fixed
// by the proof type and cannot be caller-selected. Cancellation before
// settlement leaves the durable maintenance head for level-triggered recovery.
func (p *CallbackPublisher) PublishMaintenanceSuccessContext(
	ctx context.Context,
	active MaintenanceReleaseActive,
) error {
	if ctx == nil {
		return errors.New("callback publisher: maintenance success ownership context is required")
	}
	if !p.valid() {
		return errors.New("callback publisher is invalid")
	}
	if !active.Valid() || active.settlement != p.maintenance {
		return errors.New("callback publisher: maintenance success belongs to another settlement")
	}
	claim := active.intent
	unlock, err := p.lockMaintenancePublication(ctx, claim)
	if err != nil {
		return err
	}
	defer unlock()
	_, err = p.maintenance.resolveSuccessLocked(claim, active)
	return p.finishMaintenancePublication(err)
}

// PublishMaintenanceFailureContext atomically settles an exact maintenance
// intent using proof that its target failed or was never appended.
// Cancellation leaves the exact failed release and maintenance head available
// to recovery.
func (p *CallbackPublisher) PublishMaintenanceFailureContext(
	ctx context.Context,
	failed MaintenanceReleaseFailure,
	errMsg string,
) error {
	if ctx == nil {
		return errors.New("callback publisher: maintenance failure ownership context is required")
	}
	if !p.valid() {
		return errors.New("callback publisher is invalid")
	}
	if !failed.Valid() || failed.settlement != p.maintenance {
		return errors.New("callback publisher: maintenance failure belongs to another settlement")
	}
	claim := failed.intent
	unlock, err := p.lockMaintenancePublication(ctx, claim)
	if err != nil {
		return err
	}
	defer unlock()
	_, err = p.maintenance.resolveFailureLocked(claim, failed, errMsg)
	return p.finishMaintenancePublication(err)
}

// TryPublishMaintenanceSuccessContext is the cancellation-aware non-blocking
// recovery form. acquired is false only when another journal transition
// currently owns this lease.
func (p *CallbackPublisher) TryPublishMaintenanceSuccessContext(
	ctx context.Context,
	active MaintenanceReleaseActive,
) (acquired bool, err error) {
	if ctx == nil {
		return false, errors.New("callback publisher: maintenance success ownership context is required")
	}
	if !p.valid() {
		return false, errors.New("callback publisher is invalid")
	}
	if !active.Valid() || active.settlement != p.maintenance {
		return false, errors.New("callback publisher: maintenance success belongs to another settlement")
	}
	claim := active.intent
	unlock, acquired, err := p.tryLockMaintenancePublication(ctx, claim)
	if err != nil || !acquired {
		return acquired, err
	}
	defer unlock()
	_, err = p.maintenance.resolveSuccessLocked(claim, active)
	return true, p.finishMaintenancePublication(err)
}

// TryPublishMaintenanceFailureContext is the cancellation-aware non-blocking
// recovery form for an exact failed-or-absent target proof.
func (p *CallbackPublisher) TryPublishMaintenanceFailureContext(
	ctx context.Context,
	failed MaintenanceReleaseFailure,
	errMsg string,
) (acquired bool, err error) {
	if ctx == nil {
		return false, errors.New("callback publisher: maintenance failure ownership context is required")
	}
	if !p.valid() {
		return false, errors.New("callback publisher is invalid")
	}
	if !failed.Valid() || failed.settlement != p.maintenance {
		return false, errors.New("callback publisher: maintenance failure belongs to another settlement")
	}
	claim := failed.intent
	unlock, acquired, err := p.tryLockMaintenancePublication(ctx, claim)
	if err != nil || !acquired {
		return acquired, err
	}
	defer unlock()
	_, err = p.maintenance.resolveFailureLocked(claim, failed, errMsg)
	return true, p.finishMaintenancePublication(err)
}

// TryPublishMaintenanceRuntimeFailureContext atomically publishes the
// successful completion of an already-committed maintenance command followed
// by the fixed Failed lifecycle observation for its now-divergent runtime. It
// is cancellation-aware and non-blocking on an owned lease gate.
func (p *CallbackPublisher) TryPublishMaintenanceRuntimeFailureContext(
	ctx context.Context,
	active MaintenanceReleaseActive,
	errMsg string,
) (acquired bool, err error) {
	if ctx == nil {
		return false, errors.New("callback publisher: maintenance runtime ownership context is required")
	}
	if !p.valid() {
		return false, errors.New("callback publisher is invalid")
	}
	if !active.Valid() || active.settlement != p.maintenance {
		return false, errors.New("callback publisher: maintenance runtime failure belongs to another settlement")
	}
	claim := active.intent
	unlock, acquired, err := p.tryLockMaintenancePublication(ctx, claim)
	if err != nil || !acquired {
		return acquired, err
	}
	defer unlock()
	if err := p.store.requireCurrentMaintenanceClaim(claim); err != nil {
		return true, p.finishMaintenancePublication(err)
	}
	if err := p.maintenance.validateActiveProofLocked(claim, active); err != nil {
		return true, p.finishMaintenancePublication(err)
	}
	maintenance, runtimeFailure, err := prepareDivergedMaintenanceCompletions(claim, errMsg)
	if err != nil {
		return true, p.finishMaintenancePublication(err)
	}
	_, err = p.store.resolveMaintenanceIntentEntriesLocked(
		claim, []CallbackEntry{maintenance, runtimeFailure},
	)
	return true, p.finishMaintenancePublication(err)
}

func (p *CallbackPublisher) lockMaintenancePublication(
	ctx context.Context,
	claim MaintenanceIntentClaim,
) (func(), error) {
	if !claim.Valid() {
		return nil, errors.New("valid maintenance intent claim is required")
	}
	if canceled(ctx) {
		p.logCanceled(ctx, claim.LeaseUUID(), "")
		return nil, ctx.Err()
	}
	if err := p.prepublicationCheck(ctx); err != nil {
		if isTerminalStorageAuthorityError(err) {
			return nil, fmt.Errorf("maintenance callback lost storage authority: %w", err)
		}
		p.logger.Warn(
			"persisting maintenance callback while backend identity re-attestation is transiently unavailable",
			"error", err, "lease_uuid", claim.LeaseUUID(),
		)
	}

	unlock, err := p.store.lockDeliveryLeaseContext(ctx, claim.LeaseUUID())
	if err != nil {
		p.logCanceled(ctx, claim.LeaseUUID(), "while waiting for FIFO")
		return nil, err
	}
	if canceled(ctx) {
		unlock()
		p.logCanceled(ctx, claim.LeaseUUID(), "")
		return nil, ctx.Err()
	}
	if err := p.prepublicationCheck(ctx); err != nil {
		if isTerminalStorageAuthorityError(err) {
			unlock()
			return nil, fmt.Errorf("maintenance callback lost storage authority while waiting for FIFO: %w", err)
		}
		p.logger.Warn(
			"persisting maintenance callback after transient post-FIFO identity re-attestation failure",
			"error", err, "lease_uuid", claim.LeaseUUID(),
		)
	}
	if canceled(ctx) {
		unlock()
		p.logCanceled(ctx, claim.LeaseUUID(), "after identity verification")
		return nil, ctx.Err()
	}
	return unlock, nil
}

func (p *CallbackPublisher) tryLockMaintenancePublication(
	ctx context.Context,
	claim MaintenanceIntentClaim,
) (unlock func(), acquired bool, err error) {
	if !claim.Valid() {
		return nil, false, errors.New("valid maintenance intent claim is required")
	}
	if canceled(ctx) {
		p.logCanceled(ctx, claim.LeaseUUID(), "")
		return nil, false, ctx.Err()
	}
	if err := p.prepublicationCheck(ctx); err != nil {
		if isTerminalStorageAuthorityError(err) {
			return nil, false, fmt.Errorf("maintenance callback lost storage authority: %w", err)
		}
		p.logger.Warn(
			"persisting maintenance callback while backend identity re-attestation is transiently unavailable",
			"error", err, "lease_uuid", claim.LeaseUUID(),
		)
	}
	unlock, acquired = p.store.tryLockDeliveryLease(claim.LeaseUUID())
	if !acquired {
		return nil, false, nil
	}
	if canceled(ctx) {
		unlock()
		p.logCanceled(ctx, claim.LeaseUUID(), "")
		return nil, true, ctx.Err()
	}
	if err := p.prepublicationCheck(ctx); err != nil {
		if isTerminalStorageAuthorityError(err) {
			unlock()
			return nil, true, fmt.Errorf("maintenance callback lost storage authority while entering FIFO: %w", err)
		}
		p.logger.Warn(
			"persisting maintenance callback after transient post-FIFO identity re-attestation failure",
			"error", err, "lease_uuid", claim.LeaseUUID(),
		)
	}
	if canceled(ctx) {
		unlock()
		p.logCanceled(ctx, claim.LeaseUUID(), "after identity verification")
		return nil, true, ctx.Err()
	}
	return unlock, true, nil
}

func (p *CallbackPublisher) finishMaintenancePublication(settleErr error) error {
	if settleErr != nil {
		p.reportStoreError()
		return fmt.Errorf("persist maintenance callback: %w", settleErr)
	}
	return nil
}

func (p *CallbackPublisher) prepublicationCheck(ownerCtx context.Context) error {
	return p.attestor.verify(ownerCtx)
}

func (p *CallbackPublisher) reportStoreError() {
	if p == nil || p.onStoreError == nil {
		return
	}
	// Metrics/observer hooks are foreign application code. A faulty hook cannot
	// unwind semantic settlement or obscure the durable recovery evidence.
	defer func() {
		if recovered := recover(); recovered != nil && p.logger != nil {
			p.logger.Error("panic in callback publisher store-error hook", "panic", recovered)
		}
	}()
	p.onStoreError()
}

func canceled(ctx context.Context) bool { return ctx != nil && ctx.Err() != nil }

func (p *CallbackPublisher) logCanceled(ctx context.Context, leaseUUID, suffix string) {
	if p == nil || p.logger == nil {
		return
	}
	message := "suppressing callback enqueue for canceled lease operation"
	if suffix != "" {
		message += " " + suffix
	}
	p.logger.Debug(message, "lease_uuid", leaseUUID, "error", ctx.Err())
}
