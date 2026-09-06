package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

const interruptedOperationFailure = "backend restarted before operation completion"
const refusedOperationFailure = "backend refused operation before asynchronous acceptance"

var errOperationIntentSubstrateNonterminal = errors.New("container substrate remains non-terminal")

// resolvePreEffectOperationRefusal can settle only an intent whose durable
// execution bit still says NotStarted. RefuseOperationExecution rejects a stale
// post-Started candidate, so release absence alone can never manufacture a
// physical-failure verdict.
func (b *Backend) resolvePreEffectOperationRefusal(
	claim shared.OperationIntentClaim,
	errMsg string,
) error {
	if b.releaseStore == nil || b.callbackStore == nil {
		return errors.New("operation settlement journals are required")
	}
	candidate, err := b.operationSettlement.PrepareOperationRelease(claim)
	if err != nil {
		return err
	}
	failure, err := b.operationSettlement.RefuseOperationExecution(candidate)
	if err != nil {
		return err
	}
	uncommitted, err := b.operationSettlement.CommitOperationFailure(failure)
	if err != nil {
		return err
	}
	if b.callbackPublisher == nil {
		return errors.New("callback publisher is required")
	}
	return b.callbackPublisher.PublishOperationFailureContext(b.stopCtx, uncommitted, errMsg)
}

// recoverFailedOperationSubstrate is the level-triggered half of failed
// operation settlement. A finite series of empty Docker reads cannot prove that
// a ContainerCreate already accepted by the daemon will never become visible.
// The aggregate callback journal therefore writes a permanent, compact Failed
// receipt before settlement and keeps the rich Failed operation as the active
// head until a successor takes over. Every state-recovery sweep reuses those
// immutable callback/principal authorities to remove exact late containers
// before ordinary recovery can publish them as a live provision. Targeted
// removal (rather than project-wide Compose Down) preserves a legitimate newer
// generation after the failed receipt has moved into history.
func (b *Backend) recoverFailedOperationSubstrate(
	ctx context.Context,
) (failedOperationRecoveryFence, error) {
	fence := failedOperationRecoveryFence{
		failedLeases:        make(map[string]struct{}),
		byOperationCallback: make(map[string]shared.FailedOperationReceipt),
		byLifecycleCallback: make(map[string]shared.FailedOperationReceipt),
	}
	if b.operationSettlement == nil {
		return fence, nil
	}
	// Only a current Failed head excludes the complete lease from ordinary
	// projection. Archived failures may share the lease with a legitimate
	// successor; their compact receipts authorize removal of only the exact old
	// callback generation below.
	states, err := b.operationSettlement.ListOperationRecoveryStates()
	if err != nil {
		return failedOperationRecoveryFence{}, fmt.Errorf("list operation recovery states: %w", err)
	}
	for _, state := range states {
		terminal, ok := state.(shared.OperationFailed)
		if !ok {
			continue
		}
		if terminal.Backend() != b.cfg.Name || terminal.BackendStorageID() != b.storageIdentity {
			return failedOperationRecoveryFence{}, fmt.Errorf(
				"failed %s operation for lease %q belongs to backend %q storage %q",
				terminal.Kind(), terminal.LeaseUUID(), terminal.Backend(),
				terminal.BackendStorageID().String(),
			)
		}
		fence.failedLeases[terminal.LeaseUUID()] = struct{}{}
	}

	receipts, err := b.operationSettlement.ListFailedOperationReceipts()
	if err != nil {
		return failedOperationRecoveryFence{}, fmt.Errorf("list failed operation receipts: %w", err)
	}
	for _, receipt := range receipts {
		if receipt.Backend() != b.cfg.Name || receipt.BackendStorageID() != b.storageIdentity {
			return failedOperationRecoveryFence{}, fmt.Errorf(
				"failed %s operation receipt for lease %q belongs to backend %q storage %q",
				receipt.Kind(), receipt.LeaseUUID(), receipt.Backend(),
				receipt.BackendStorageID().String(),
			)
		}
	}
	fence, err = newFailedOperationRecoveryFence(fence.failedLeases, receipts)
	if err != nil {
		return failedOperationRecoveryFence{}, err
	}
	if len(receipts) == 0 {
		return fence, nil
	}

	for observation := range 3 {
		containers, inventoryErr := b.strictIdentityBoundOperationInventory(ctx)
		if inventoryErr != nil {
			return failedOperationRecoveryFence{}, fmt.Errorf("inspect late substrate for failed operations: %w", inventoryErr)
		}
		targets, matchErr := fence.targets(containers)
		if matchErr != nil {
			return failedOperationRecoveryFence{}, matchErr
		}
		if len(targets) == 0 {
			return fence, nil
		}
		if observation == 2 {
			return failedOperationRecoveryFence{}, fmt.Errorf("late failed-operation substrate remained after two cleanup passes")
		}
		byOperation := make(map[shared.OperationID]shared.FailedOperationReceipt)
		for _, target := range targets {
			byOperation[target.receipt.OperationID()] = target.receipt
		}
		for operationID, receipt := range byOperation {
			cleanupCtx, cancelCleanup := context.WithTimeout(ctx, 30*time.Second)
			var cleanupErr error
			acquired, scopeErr := b.recoveryCoordinator.WithLease(
				cleanupCtx, receipt.LeaseUUID(),
				func(scope shared.LeaseRecoveryScope) error {
					cleanupErr = b.operationSettlement.CleanupFailedOperationReceipt(cleanupCtx, scope, receipt)
					return cleanupErr
				},
			)
			cancelCleanup()
			if scopeErr != nil {
				cleanupErr = scopeErr
			}
			if !acquired && cleanupErr == nil {
				// A live command or actor owns this exact lease. Keep the receipt
				// fence and retry on the next level-triggered pass.
				return fence, nil
			}
			if cleanupErr != nil {
				return failedOperationRecoveryFence{}, b.latchAmbiguousOperationOutcome(
					fmt.Sprintf(
						"remove late substrate for failed operation %s",
						operationID.Fingerprint(),
					),
					cleanupErr,
				)
			}
			b.logger.Warn("removed exact late substrate for durably failed operation",
				"lease_uuid", receipt.LeaseUUID(),
				"operation_fingerprint", operationID.Fingerprint(),
				"kind", receipt.Kind(),
			)
		}
	}
	return fence, nil
}

type failedOperationReceiptTarget struct {
	receipt     shared.FailedOperationReceipt
	containerID string
}

type failedOperationRecoveryFence struct {
	failedLeases        map[string]struct{}
	byOperationCallback map[string]shared.FailedOperationReceipt
	byLifecycleCallback map[string]shared.FailedOperationReceipt
}

func newFailedOperationRecoveryFence(
	failedLeases map[string]struct{},
	receipts []shared.FailedOperationReceipt,
) (failedOperationRecoveryFence, error) {
	fence := failedOperationRecoveryFence{
		failedLeases:        failedLeases,
		byOperationCallback: make(map[string]shared.FailedOperationReceipt, len(receipts)),
		byLifecycleCallback: make(map[string]shared.FailedOperationReceipt, len(receipts)),
	}
	for _, receipt := range receipts {
		if existing := fence.byOperationCallback[receipt.CallbackURL()]; existing.Valid() {
			return failedOperationRecoveryFence{}, fmt.Errorf(
				"failed operations %s and %s share an operation callback",
				existing.OperationID().Fingerprint(), receipt.OperationID().Fingerprint(),
			)
		}
		if existing := fence.byLifecycleCallback[receipt.LifecycleCallbackURL()]; existing.Valid() {
			return failedOperationRecoveryFence{}, fmt.Errorf(
				"failed operations %s and %s share a lifecycle callback",
				existing.OperationID().Fingerprint(), receipt.OperationID().Fingerprint(),
			)
		}
		fence.byOperationCallback[receipt.CallbackURL()] = receipt
		fence.byLifecycleCallback[receipt.LifecycleCallbackURL()] = receipt
	}
	return fence, nil
}

func (f failedOperationRecoveryFence) targets(
	containers []ContainerInfo,
) ([]failedOperationReceiptTarget, error) {
	var targets []failedOperationReceiptTarget
	for _, container := range containers {
		operationReceipt := f.byOperationCallback[container.CallbackURL]
		lifecycleReceipt := f.byLifecycleCallback[container.LifecycleCallbackURL]
		switch {
		case !operationReceipt.Valid() && !lifecycleReceipt.Valid():
			continue
		case !operationReceipt.Valid():
			return nil, fmt.Errorf(
				"container %q partially matches failed operation %s lifecycle callback identity",
				container.ContainerID, lifecycleReceipt.OperationID().Fingerprint(),
			)
		case !lifecycleReceipt.Valid():
			return nil, fmt.Errorf(
				"container %q partially matches failed operation %s operation callback identity",
				container.ContainerID, operationReceipt.OperationID().Fingerprint(),
			)
		case operationReceipt.OperationID() != lifecycleReceipt.OperationID() ||
			operationReceipt.CallbackURL() != lifecycleReceipt.CallbackURL():
			return nil, fmt.Errorf(
				"container %q mixes failed operation %s and %s callback identities",
				container.ContainerID,
				operationReceipt.OperationID().Fingerprint(),
				lifecycleReceipt.OperationID().Fingerprint(),
			)
		}
		if container.LeaseUUID != operationReceipt.LeaseUUID() ||
			container.Tenant != operationReceipt.Tenant() ||
			container.ProviderUUID != operationReceipt.ProviderUUID() {
			return nil, fmt.Errorf(
				"container %q with failed operation %s callback identity has divergent lease or principal",
				container.ContainerID, operationReceipt.OperationID().Fingerprint(),
			)
		}
		if operationReceipt.Valid() {
			targets = append(targets, failedOperationReceiptTarget{
				receipt: operationReceipt, containerID: container.ContainerID,
			})
		}
	}
	return targets, nil
}

func (f failedOperationRecoveryFence) targetContainerIDs(
	containers []ContainerInfo,
) (map[string]struct{}, error) {
	targets, err := f.targets(containers)
	if err != nil {
		return nil, err
	}
	ids := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		ids[target.containerID] = struct{}{}
	}
	return ids, nil
}

func (b *Backend) probeOperationIntent(
	leaseUUID, callbackURL string,
) (bool, error) {
	if b.operationSettlement == nil {
		return false, errors.New("durable callback store is required for asynchronous operation")
	}
	probe, err := b.operationSettlement.NewOperationIntentProbe(leaseUUID, callbackURL)
	if err != nil {
		return false, fmt.Errorf("construct exact operation redelivery probe: %w", err)
	}
	disposition, err := b.operationSettlement.ProbeOperationIntent(probe)
	if err != nil {
		return false, fmt.Errorf("probe exact operation redelivery: %w", err)
	}
	return disposition == shared.OperationIntentAdmissionExisting ||
		disposition == shared.OperationIntentAdmissionCompleted, nil
}

func (b *Backend) beginOperationIntent(
	kind shared.OperationIntentKind,
	leaseUUID, callbackURL, lifecycleCallbackURL, tenant, providerUUID string,
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
	effectiveItems []backend.LeaseItem,
	healthCheckServices []string,
	manifestPayload []byte,
	sourceLeaseUUID string,
	sourceGeneration int,
) (shared.OperationIntentClaim, bool, error) {
	if b.operationSettlement == nil {
		return shared.OperationIntentClaim{}, false, errors.New("durable callback store is required for asynchronous operation")
	}
	if err := validateDockerResourceProfiles(items, resourceProfiles); err != nil {
		return shared.OperationIntentClaim{}, false, fmt.Errorf("validate %s operation resource profiles: %w", kind, err)
	}
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind:                 kind,
		LeaseUUID:            leaseUUID,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		Tenant:               tenant,
		ProviderUUID:         providerUUID,
		Items:                items,
		ResourceProfiles:     resourceProfiles,
		EffectiveItems:       effectiveItems,
		HealthCheckServices:  healthCheckServices,
		Manifest:             manifestPayload,
		SourceLeaseUUID:      sourceLeaseUUID,
		SourceGeneration:     sourceGeneration,
	})
	if err != nil {
		return shared.OperationIntentClaim{}, false, fmt.Errorf("construct %s operation intent: %w", kind, err)
	}
	admission, err := b.operationSettlement.BeginOperationIntent(candidate)
	if err != nil {
		return shared.OperationIntentClaim{}, false, fmt.Errorf("persist %s operation intent: %w", kind, err)
	}
	claim, created := admission.CreatedClaim()
	if !created {
		return shared.OperationIntentClaim{}, false, nil
	}
	return claim, true, nil
}

func (b *Backend) refuseOperationIntent(claim shared.OperationIntentClaim, cause error) error {
	if !claim.Valid() {
		return cause
	}
	if authorityErr := b.terminalStorageAuthorityError(); authorityErr != nil ||
		errors.Is(cause, backendidentity.ErrIdentityDrift) ||
		errors.Is(cause, backendidentity.ErrMutationOutcomeAmbiguous) {
		if authorityErr == nil {
			authorityErr = cause
		}
		// Do not turn a lost storage-authority proof into a definitive refusal.
		// Intermediate provisioning helpers may intentionally log-and-default a
		// raw mutation error; the backend-lifetime latch is therefore the source
		// of truth at this final evidence-consuming boundary.
		return errors.Join(cause, fmt.Errorf(
			"%w: preserve %s operation intent for restart recovery: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, claim.Kind(), authorityErr,
		))
	}
	if err := b.resolvePreEffectOperationRefusal(claim, refusedOperationFailure); err != nil {
		b.logger.Error("failed to settle refused operation intent",
			"error", err,
			"lease_uuid", claim.LeaseUUID(),
			"operation", claim.Kind(),
		)
		// Do not preserve a clearable validation/conflict sentinel when durable
		// failure settlement failed: the outcome is now ambiguous and Fred must
		// keep its write-ahead attempt for startup recovery.
		return fmt.Errorf("operation refused but durable intent settlement failed: %s: %w", cause.Error(), err)
	}
	return cause
}

// settleUnacceptedRestoreIntent durably settles a restore that never crossed
// the lease actor's acceptance boundary. It deliberately does not accept nil:
// once beginOperationIntent returns proceed=true, the typed claim is mandatory
// authority for every synchronous failure path.
//
// The caller must invoke this only after teardown, re-quarantine, and source
// quota proof, but before handing the retention row back to Active. That order
// keeps the Restoring row as a level-triggered retry owner if this write fails;
// handing ownership back first would strand an Existing intent until restart.
func (b *Backend) settleUnacceptedRestoreIntent(claim shared.OperationIntentClaim) error {
	if claim.Kind() != shared.OperationIntentRestore {
		return fmt.Errorf("unaccepted restore settlement received %s intent", claim.Kind())
	}
	if err := b.resolvePreEffectOperationRefusal(claim, refusedOperationFailure); err != nil {
		b.logger.Error("failed to settle unaccepted restore intent",
			"error", err,
			"lease_uuid", claim.LeaseUUID(),
		)
		return fmt.Errorf("settle unaccepted restore intent: %w", err)
	}
	return nil
}

type recoveredOperationReadyPromotion struct {
	containerIDs      []string
	serviceContainers map[string][]string
	stackManifest     *manifest.StackManifest
}

type recoveredIntentDecision struct {
	claim              shared.OperationIntentClaim
	status             backend.CallbackStatus
	errMsg             string
	readyProjection    *recoveredOperationReadyPromotion
	needsTeardown      bool
	cleanupIDs         []string
	allocationIDs      []string
	legacyPredecessor  *shared.Release
	legacyAuthority    *shared.LegacyRuntimeAuthority
	preserveProjection bool
	failureOutcome     shared.OperationExecutionFailure
}

// operationIntentWaitEvidence is the closed causal vocabulary for an exact
// non-terminal operation substrate. The classifier alone can mint these value
// variants; recovery consumes their behavior without recomputing status strings
// or accepting caller-selected booleans.
type operationIntentWaitEvidence interface {
	operationIntentWaitEvidence()
	kind() string
	requiresFinalInventory() bool
	usesStartWindow() bool
}

// operationAwaitLateVisibility means no exact object is visible yet, but a
// daemon-side create accepted by the prior process may still publish one.
type operationAwaitLateVisibility struct{}

func (operationAwaitLateVisibility) operationIntentWaitEvidence() {}
func (operationAwaitLateVisibility) kind() string                 { return "late_visibility" }
func (operationAwaitLateVisibility) requiresFinalInventory() bool { return true }
func (operationAwaitLateVisibility) usesStartWindow() bool        { return false }

// operationAwaitProgress means the exact cohort is visible and has a runtime
// mechanism (for example a starting health check) that can still converge.
type operationAwaitProgress struct{}

func (operationAwaitProgress) operationIntentWaitEvidence() {}
func (operationAwaitProgress) kind() string                 { return "progress" }
func (operationAwaitProgress) requiresFinalInventory() bool { return false }
func (operationAwaitProgress) usesStartWindow() bool        { return false }

// operationAwaitInert means the exact cohort is visible but no surviving worker
// can advance it (created, paused, or an unrecognized Docker state). It earns only
// the bounded container-start stabilization window before exact cleanup.
type operationAwaitInert struct{}

func (operationAwaitInert) operationIntentWaitEvidence() {}
func (operationAwaitInert) kind() string                 { return "inert" }
func (operationAwaitInert) requiresFinalInventory() bool { return false }
func (operationAwaitInert) usesStartWindow() bool        { return true }

type operationIntentSubstrate struct {
	status            backend.CallbackStatus
	errMsg            string
	hasCurrent        bool
	needsTeardown     bool
	waitEvidence      operationIntentWaitEvidence
	currentIDs        []string
	serviceContainers map[string][]string
	stackManifest     *manifest.StackManifest
	legacyPredecessor *shared.Release
	legacyAuthority   *shared.LegacyRuntimeAuthority
}

func (s operationIntentSubstrate) requiresFinalInventory() bool {
	return s.waitEvidence != nil && s.waitEvidence.requiresFinalInventory()
}

func (s operationIntentSubstrate) usesStartWindow() bool {
	return s.waitEvidence != nil && s.waitEvidence.usesStartWindow()
}

// classifiedOperationIntent is one durable operation together with the latest
// complete substrate observation made for it. Awaiting operations stay in this
// value slice while the batch coordinator polls; there is deliberately no
// goroutine, timer, or independently fetched fleet inventory per operation.
type classifiedOperationIntent struct {
	claim              shared.OperationIntentClaim
	classification     operationIntentSubstrate
	awaitRecovery      bool
	requiredAwait      bool
	deferred           bool
	preserveProjection bool
	operationDeadline  time.Time
	inertDeadline      time.Time
}

type operationIntentRecoveryMode uint8

const (
	operationIntentRecoveryStartup operationIntentRecoveryMode = iota + 1
	operationIntentRecoveryLive
)

type operationIntentKey struct {
	leaseUUID   string
	operationID shared.OperationID
}

func keyForOperationIntent(claim shared.OperationRecoveryState) operationIntentKey {
	return operationIntentKey{leaseUUID: claim.LeaseUUID(), operationID: claim.OperationID()}
}

// operationInventoryByLease indexes one strict fleet observation exactly once.
// Operation intents are lease-exclusive in the durable journal, so each
// classifier receives only the containers it can legitimately reason about.
func operationInventoryByLease(containers []ContainerInfo) map[string][]ContainerInfo {
	indexed := make(map[string][]ContainerInfo)
	for _, container := range containers {
		indexed[container.LeaseUUID] = append(indexed[container.LeaseUUID], container)
	}
	return indexed
}

// recoverOperationIntents classifies the durable write-ahead window from
// strict Docker substrate evidence. It first classifies every intent and only
// then settles any of them, so one ambiguous lease keeps the complete startup
// evidence set intact and makes readiness fail closed.
func (b *Backend) recoverOperationIntents(ctx context.Context) error {
	if b.operationSettlement == nil || b.recoveryCoordinator == nil {
		return nil
	}
	claims, err := b.operationSettlement.ListOperationIntents()
	if err != nil {
		return fmt.Errorf("list callback operation intents: %w", err)
	}
	// Startup performs one bounded strict observation and defers young empty or
	// transitional generations. Waiting their full provision/start window here
	// would prevent the subscriber and periodic recovery scheduler from starting,
	// turning an otherwise recoverable `created`/`restarting` cohort into a daemon
	// availability failure. The live lane reclaims the exact same durable claims
	// once the actor is quiescent.
	var rebuildAfterSettlement bool
	err = b.withOperationRecoveryScopes(ctx, claims, func(
		current []shared.OperationIntentClaim,
		scopes map[string]shared.LeaseRecoveryScope,
	) error {
		return b.recoverOperationIntentClaims(
			ctx, current, operationIntentRecoveryStartup, nil, scopes,
			&rebuildAfterSettlement,
		)
	})
	if err != nil || !rebuildAfterSettlement {
		return err
	}
	return b.recoverState(ctx)
}

// withOperationRecoveryScopes acquires exact lease scopes in stable order and
// holds every acquired exclusion through one batch callback. Busy leases are
// omitted, allowing independent operations to converge without deadlock or
// fleet-wide head-of-line blocking. Claims are re-read after all scopes are
// held so stale snapshots never become recovery authority.
func (b *Backend) withOperationRecoveryScopes(
	ctx context.Context,
	snapshot []shared.OperationIntentClaim,
	work func([]shared.OperationIntentClaim, map[string]shared.LeaseRecoveryScope) error,
) error {
	if b.recoveryCoordinator == nil {
		return errors.New("operation recovery coordinator is required")
	}
	leaseUUIDs := make([]string, 0, len(snapshot))
	seen := make(map[string]struct{}, len(snapshot))
	for _, claim := range snapshot {
		if _, duplicate := seen[claim.LeaseUUID()]; duplicate {
			return fmt.Errorf("multiple pending operation intents for lease %q", claim.LeaseUUID())
		}
		seen[claim.LeaseUUID()] = struct{}{}
		leaseUUIDs = append(leaseUUIDs, claim.LeaseUUID())
	}
	slices.Sort(leaseUUIDs)
	scopes := make(map[string]shared.LeaseRecoveryScope, len(leaseUUIDs))
	var acquire func(int) error
	acquire = func(index int) error {
		if index == len(leaseUUIDs) {
			current, err := b.operationSettlement.ListOperationIntents()
			if err != nil {
				return fmt.Errorf("re-read callback operation intents under recovery scope: %w", err)
			}
			selected := make([]shared.OperationIntentClaim, 0, len(current))
			for _, claim := range current {
				if _, owned := scopes[claim.LeaseUUID()]; owned {
					selected = append(selected, claim)
				}
			}
			return work(selected, scopes)
		}
		leaseUUID := leaseUUIDs[index]
		acquired, err := b.recoveryCoordinator.WithLease(
			ctx, leaseUUID,
			func(scope shared.LeaseRecoveryScope) error {
				scopes[leaseUUID] = scope
				defer delete(scopes, leaseUUID)
				return acquire(index + 1)
			},
		)
		if err != nil {
			return err
		}
		if !acquired {
			return acquire(index + 1)
		}
		return nil
	}
	return acquire(0)
}

// recoverLiveOperationIntents retries only operations whose lease actor is
// quiescent (or absent) while the per-lease command fence is idle. Those two
// capabilities structurally exclude an admitted prelude, queued actor command,
// and mutation worker. Busy leases are deferred to the next cadence; recovery
// never races live substrate work and never waits behind it.
func (b *Backend) recoverLiveOperationIntents(ctx context.Context) error {
	if b.operationSettlement == nil || b.recoveryCoordinator == nil {
		return nil
	}
	snapshot, err := b.operationSettlement.ListOperationIntents()
	if err != nil {
		return fmt.Errorf("list live callback operation intents: %w", err)
	}
	if len(snapshot) == 0 {
		return nil
	}

	return b.withOperationRecoveryScopes(ctx, snapshot, func(
		claims []shared.OperationIntentClaim,
		scopes map[string]shared.LeaseRecoveryScope,
	) error {
		provenFailures := make(map[operationIntentKey]string)
		for _, claim := range claims {
			b.provisionsMu.RLock()
			provision := b.provisions[claim.LeaseUUID()]
			var status backend.ProvisionStatus
			var callbackURL, lifecycleCallbackURL, errMsg string
			if provision != nil {
				status = provision.Status
				callbackURL = provision.CallbackURL
				lifecycleCallbackURL = provision.LifecycleCallbackURL
				errMsg = provision.Message
			}
			b.provisionsMu.RUnlock()

			// A missing volatile projection is not failure evidence for a Started
			// operation inherited from another process. Docker may still publish an
			// accepted Create after the first empty startup inventory; only the
			// construction-bound classifier and its durable visibility window may
			// resolve that generation. An exact Failed projection, by contrast, was
			// published by the quiescent actor from a typed terminal worker outcome.
			if status == backend.ProvisionStatusFailed &&
				callbackURL == claim.CallbackURL() &&
				lifecycleCallbackURL == claim.LifecycleCallbackURL() {
				// Actor quiescence proves its worker and terminal transition completed.
				// A Failed receipt remains the durable cleanup authority for any Docker
				// Create that becomes visible after the worker's best-effort teardown.
				if errMsg == "" {
					errMsg = interruptedOperationFailure
				}
				provenFailures[keyForOperationIntent(claim)] = errMsg
			}
		}
		return b.recoverOperationIntentClaims(
			ctx, claims, operationIntentRecoveryLive, provenFailures, scopes, nil,
		)
	})
}

// recoverOperationIntentClaims owns classification and settlement for an
// already-authoritative claim snapshot. Both modes take one bounded observation
// and defer young transitional work so neither backend startup nor a periodic
// sweep waits out a tenant operation's full deadline. Startup may rebuild a
// projection after terminal settlement; the live lane already owns a quiescent
// actor generation and conservatively leaves projection rebuilding to the next
// ordinary recovery sweep after releasing that capability.
func (b *Backend) recoverOperationIntentClaims(
	ctx context.Context,
	claims []shared.OperationIntentClaim,
	mode operationIntentRecoveryMode,
	provenFailures map[operationIntentKey]string,
	recoveryScopes map[string]shared.LeaseRecoveryScope,
	rebuildRequested *bool,
) error {
	if len(claims) == 0 {
		return nil
	}
	if mode != operationIntentRecoveryStartup && mode != operationIntentRecoveryLive {
		return errors.New("operation recovery mode is invalid")
	}
	classified := make([]classifiedOperationIntent, 0, len(claims))
	needsInventory := false
	for _, claim := range claims {
		if claim.Backend() != b.cfg.Name || claim.BackendStorageID() != b.storageIdentity {
			return fmt.Errorf("%s operation intent for lease %q belongs to backend %q storage %q",
				claim.Kind(), claim.LeaseUUID(), claim.Backend(), claim.BackendStorageID().String())
		}
		if err := validateDockerResourceProfiles(claim.Items(), claim.ResourceProfiles()); err != nil {
			return fmt.Errorf("%s operation intent for lease %q has invalid resource authority: %w",
				claim.Kind(), claim.LeaseUUID(), err)
		}
		// A durable BeforeEffects claim proves that no substrate mutation
		// capability was ever issued. Settle it directly through the typed
		// refusal path below; consulting Docker would weaken that proof by
		// turning unrelated or uncertain inventory into causal evidence.
		if claim.ExecutionPhase() == shared.OperationExecutionBeforeEffects {
			classified = append(classified, classifiedOperationIntent{
				claim: claim,
				classification: operationIntentSubstrate{
					status: backend.CallbackStatusFailed,
					errMsg: interruptedOperationFailure,
				},
			})
			continue
		}
		committed, commitErr := b.operationIntentHasCommittedRelease(claim)
		if commitErr != nil {
			return fmt.Errorf("classify committed %s operation for lease %q: %w",
				claim.Kind(), claim.LeaseUUID(), commitErr)
		}
		if committed {
			classified = append(classified, classifiedOperationIntent{
				claim: claim,
				classification: operationIntentSubstrate{
					status: backend.CallbackStatusSuccess,
				},
			})
			continue
		}
		if errMsg, proven := provenFailures[keyForOperationIntent(claim)]; proven {
			classified = append(classified, classifiedOperationIntent{
				claim:              claim,
				preserveProjection: true,
				classification: operationIntentSubstrate{
					status: backend.CallbackStatusFailed,
					errMsg: errMsg,
				},
			})
			continue
		}
		needsInventory = true
		classified = append(classified, classifiedOperationIntent{claim: claim})
	}

	var inventory map[string][]ContainerInfo
	if needsInventory {
		containers, err := b.listManagedContainersStrictForRecovery(ctx)
		if err != nil {
			return fmt.Errorf("strict managed-container inventory for operation recovery: %w", err)
		}
		inventory = operationInventoryByLease(containers)
	}
	for index := range classified {
		entry := &classified[index]
		if entry.classification.status != "" {
			continue
		}
		claim := entry.claim
		classification, classifyErr := b.classifyOperationIntent(ctx, claim, inventory[claim.LeaseUUID()])
		awaitRecovery := classification.waitEvidence != nil
		if classifyErr != nil && !awaitRecovery {
			return fmt.Errorf("%s operation intent for lease %q remains unresolved: %w",
				claim.Kind(), claim.LeaseUUID(), classifyErr)
		}
		// An exact Ready cohort is durable success evidence even after a process
		// restart; the deadline only bounds a generation that remains empty or
		// transitional. Once that budget expires we still retain the classified
		// identities so cleanup can preserve a predecessor and reject a
		// contradictory generation. The bound cleanup executor performs its own
		// exhaustive post-Down observation.
		timeout := b.cfg.ProvisionTimeout
		if timeout <= 0 {
			timeout = 10 * time.Minute
		}
		now := time.Now()
		deadline := provisionIntentRecoveryDeadline(claim.CreatedAt(), now, timeout)
		if awaitRecovery && !now.Before(deadline) {
			classification.needsTeardown = true
			classification = b.operationIntentRecoveryTimedOut(
				claim, classification, deadline, operationRecoveryTimeoutProvision,
			)
			awaitRecovery = false
		}
		entry.classification = classification
		entry.awaitRecovery = awaitRecovery
		entry.requiredAwait = awaitRecovery
		entry.operationDeadline = deadline
	}

	now := time.Now()
	timeout := b.cfg.ProvisionTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	for index := range classified {
		entry := &classified[index]
		if mode == operationIntentRecoveryLive {
			entry.preserveProjection = true
		}
		if !entry.awaitRecovery {
			continue
		}
		entry.operationDeadline = provisionIntentRecoveryDeadline(
			entry.claim.CreatedAt(), now, timeout,
		)
		if now.Before(entry.operationDeadline) {
			entry.awaitRecovery = false
			entry.deferred = true
		}
	}
	if err := b.awaitOperationIntentsTerminal(ctx, classified); err != nil {
		return err
	}

	decisions := make([]recoveredIntentDecision, 0, len(classified))
	for _, entry := range classified {
		if entry.deferred {
			continue
		}
		claim := entry.claim
		classification := entry.classification
		decision := recoveredIntentDecision{
			claim:              claim,
			status:             classification.status,
			errMsg:             classification.errMsg,
			preserveProjection: entry.preserveProjection,
		}
		// A Ready exact substrate may have converged between recoverState's
		// projection snapshot and the very first operation classification. A
		// committed operation returns no current IDs, so non-empty exact IDs are
		// also the proof that this is an uncommitted projection we may promote.
		if classification.status == backend.CallbackStatusSuccess &&
			len(classification.currentIDs) != 0 {
			decision.readyProjection = &recoveredOperationReadyPromotion{
				containerIDs:      slices.Clone(classification.currentIDs),
				serviceContainers: cloneOperationServiceContainers(classification.serviceContainers),
				stackManifest:     classification.stackManifest,
			}
		}
		if classification.needsTeardown {
			decision.needsTeardown = true
			decision.cleanupIDs = slices.Clone(classification.currentIDs)
		}
		decision.legacyPredecessor = classification.legacyPredecessor
		decision.legacyAuthority = classification.legacyAuthority
		if !decision.preserveProjection &&
			claim.Kind() == shared.OperationIntentProvision &&
			classification.status == backend.CallbackStatusFailed {
			allocationIDs, _, allocationErr := resolvedProvisionAllocations(
				claim.LeaseUUID(), claim.EffectiveItems(), claim.ResourceProfiles(),
			)
			if allocationErr != nil {
				return fmt.Errorf("resolve failed provision allocation authority for lease %q: %w",
					claim.LeaseUUID(), allocationErr)
			}
			decision.allocationIDs = allocationIDs
		}
		decisions = append(decisions, decision)
	}
	// Preflight every success before mutating any release/finalizer or settling
	// any intent. One missing projection/SKU/corrupt store must preserve the
	// complete multi-lease causal evidence set.
	for _, decision := range decisions {
		if decision.status == backend.CallbackStatusSuccess {
			if err := b.validateRecoveredOperationSuccessForDecision(decision); err != nil {
				return fmt.Errorf("validate recovered %s success for lease %q: %w",
					decision.claim.Kind(), decision.claim.LeaseUUID(), err)
			}
		}
	}
	// recoverState's projection was built from the first Docker observation. An
	// exact created/restarting cohort can become Ready during the bounded wait
	// above, leaving only that in-memory status stale. Every durable and
	// projection precondition has now been validated. Re-check every target and
	// publish fresh pointers as one lock-protected all-or-none batch; never mutate
	// an actor-visible *provision in place.
	if err := b.publishAwaitedProvisionSuccesses(decisions); err != nil {
		return err
	}
	// Freeze legacy predecessor identity before removing its last container.
	// The CAS is durable but non-destructive; a failure leaves every operation
	// intent and substrate object untouched for a later startup retry.
	for _, decision := range decisions {
		if decision.legacyAuthority == nil {
			continue
		}
		if decision.legacyPredecessor == nil {
			return fmt.Errorf("legacy predecessor authority for lease %q has no release fence",
				decision.claim.LeaseUUID())
		}
		if err := b.releaseBackfiller.BackfillLegacyRuntimeAuthorityContext(
			ctx, decision.claim.LeaseUUID(),
			*decision.legacyPredecessor,
			*decision.legacyAuthority,
		); err != nil {
			return fmt.Errorf("persist legacy predecessor runtime authority for lease %q: %w",
				decision.claim.LeaseUUID(), err)
		}
	}
	// An operation that did not cross its exact Release commit boundary may be
	// classified Failed only after every container carrying this operation's
	// unguessable callback identity is gone. Keep all operation intents durable
	// while doing the destructive work: if any teardown is incomplete, the
	// backend-lifetime latch suppresses actor/callback settlement and a fresh
	// process retries from the same immutable authority.
	for index := range decisions {
		decision := &decisions[index]
		if decision.status != backend.CallbackStatusFailed {
			continue
		}
		outcome, cleanupErr := b.operationSettlement.CleanupRecoveredOperation(
			ctx, recoveryScopes[decision.claim.LeaseUUID()], decision.claim,
		)
		if cleanupErr != nil {
			return b.latchAmbiguousOperationOutcome(
				fmt.Sprintf("recover failed %s %q", decision.claim.Kind(), decision.claim.LeaseUUID()),
				cleanupErr,
			)
		}
		failure, ok := outcome.(shared.OperationExecutionFailure)
		if !ok {
			cause := fmt.Errorf("cleanup did not prove exact absence (%T)", outcome)
			if ambiguous, isAmbiguous := outcome.(shared.OperationExecutionAmbiguous); isAmbiguous && ambiguous.Cause() != nil {
				cause = ambiguous.Cause()
			}
			return b.latchAmbiguousOperationOutcome(
				fmt.Sprintf("recover failed %s %q", decision.claim.Kind(), decision.claim.LeaseUUID()),
				cause,
			)
		}
		decision.failureOutcome = failure
	}

	rebuildAfterSettlement := false
	for _, decision := range decisions {
		var committed shared.OperationReleaseCommitted
		if decision.status == backend.CallbackStatusSuccess {
			var err error
			committed, err = b.ensureRecoveredOperationSuccess(
				ctx, recoveryScopes[decision.claim.LeaseUUID()], decision.claim,
			)
			if err != nil {
				return fmt.Errorf("finalize recovered %s success for lease %q: %w",
					decision.claim.Kind(), decision.claim.LeaseUUID(), err)
			}
		}
		var err error
		if decision.status == backend.CallbackStatusSuccess {
			if b.callbackPublisher == nil {
				err = errors.New("callback publisher is required")
			} else {
				err = b.callbackPublisher.PublishOperationSuccessContext(ctx, committed)
			}
		} else {
			var uncommitted shared.OperationReleaseUncommitted
			uncommitted, err = b.operationSettlement.CommitOperationFailure(decision.failureOutcome)
			if err == nil {
				if b.callbackPublisher == nil {
					err = errors.New("callback publisher is required")
				} else {
					err = b.callbackPublisher.PublishOperationFailureContext(ctx, uncommitted, decision.errMsg)
				}
			}
		}
		if err != nil {
			return fmt.Errorf("settle recovered %s operation intent for lease %q: %w",
				decision.claim.Kind(), decision.claim.LeaseUUID(), err)
		}
		if !decision.preserveProjection &&
			decision.claim.Kind() == shared.OperationIntentProvision &&
			decision.status == backend.CallbackStatusFailed {
			rebuildAfterSettlement = true
		}
	}
	if rebuildAfterSettlement {
		// Remove the temporary intent-owned projections and their exact candidate
		// reservations before rebuilding. Leaving
		// one as Provisioning would make recoverState preserve it as a live worker
		// even though its exact failed callback has replaced the intent. Releasing
		// only the immutable intent keys also prevents recovery preservation from
		// guard from carrying candidate-only services into an older Release of the
		// same lease. recoverState immediately reconstructs that committed Release's
		// full accounting/volume claim; a genuinely fresh failed attempt instead
		// loses its claim and becomes eligible for the ordinary startup orphan pass.
		b.provisionsMu.Lock()
		for _, decision := range decisions {
			if !decision.preserveProjection &&
				decision.claim.Kind() == shared.OperationIntentProvision &&
				decision.status == backend.CallbackStatusFailed {
				b.deleteProvisionLocked(decision.claim.LeaseUUID())
				for _, allocationID := range decision.allocationIDs {
					b.pool.Release(allocationID)
				}
			}
		}
		b.provisionsMu.Unlock()
		if rebuildRequested == nil {
			return errors.New("failed provision recovery requires a deferred rebuild target")
		}
		*rebuildRequested = true
	}
	return nil
}

// teardownRecoveredOperation is the destructive half of interrupted-operation
// failure recovery. Compose Down returning nil is not, by itself, absence
// evidence: a daemon-side Create accepted by the previous process can become
// visible after Down took its project snapshot. Require a fresh strict inventory
// bracketed by storage-identity proofs before consuming the durable intent. If
// that confirmation observes a late cohort, reclassify every survivor against
// the immutable operation authority and remove the exact IDs directly. Repeating
// Compose Down would merely trust the same project-sweep success that the fresh
// inventory just disproved. A survivor, contradictory generation, or any read
// uncertainty preserves the intent and withdraws this backend lifetime at the
// caller.
func (b *Backend) teardownRecoveredOperation(
	ctx context.Context,
	recoveryScope shared.LeaseRecoveryScope,
	claim shared.OperationIntentClaim,
	_ []string,
) error {
	outcome, err := b.operationSettlement.CleanupRecoveredOperation(ctx, recoveryScope, claim)
	if err != nil {
		return err
	}
	if _, ok := outcome.(shared.OperationExecutionFailure); !ok {
		if ambiguous, isAmbiguous := outcome.(shared.OperationExecutionAmbiguous); isAmbiguous && ambiguous.Cause() != nil {
			return fmt.Errorf("operation cleanup did not prove exact absence (%T): %w",
				outcome, ambiguous.Cause())
		}
		return fmt.Errorf("operation cleanup did not prove exact absence (%T)", outcome)
	}
	return nil
}

// exactRecoveredOperationCleanupIDs converts a fresh inventory into destructive
// authority. Lease UUID alone is not enough: a successor or contradictory
// principal could otherwise be deleted after the first Compose snapshot. The
// existing operation classifier is the single source of truth for the candidate
// generation and, for a provision retry, its frozen predecessor. Every observed
// container for this lease must be accounted for by that classifier before any
// individual removal is attempted.
func (b *Backend) exactRecoveredOperationCleanupIDs(
	ctx context.Context,
	claim shared.OperationIntentClaim,
	containers []ContainerInfo,
) ([]string, error) {
	observed := make([]string, 0)
	for _, container := range containers {
		if container.LeaseUUID == claim.LeaseUUID() {
			observed = append(observed, container.ContainerID)
		}
	}
	if len(observed) == 0 {
		return nil, nil
	}

	classification, err := b.classifyOperationIntent(ctx, claim, containers)
	if err != nil && !errors.Is(err, errOperationIntentSubstrateNonterminal) {
		return nil, err
	}
	ids := slices.Clone(classification.currentIDs)
	if !sameStringSet(observed, ids) {
		return nil, errors.New("fresh inventory contains containers outside the exact operation or predecessor authority")
	}
	slices.Sort(ids)
	return ids, nil
}

// strictIdentityBoundOperationInventory makes one Docker observation under one
// finite read budget and proves that the same backend/storage identity existed
// immediately before and after it. A list from an unattested replacement daemon
// is never cleanup or settlement authority.
func (b *Backend) strictIdentityBoundOperationInventory(ctx context.Context) ([]ContainerInfo, error) {
	readCtx, cancel := b.recoveryDockerReadContext(ctx)
	defer cancel()
	if err := b.requireStorageIdentity(readCtx); err != nil {
		return nil, fmt.Errorf("verify storage identity before operation inventory: %w", err)
	}
	containers, err := b.docker.ListManagedContainersStrict(readCtx)
	if err != nil {
		return nil, err
	}
	if err := b.requireStorageIdentity(readCtx); err != nil {
		return nil, fmt.Errorf("verify storage identity after operation inventory: %w", err)
	}
	return containers, nil
}

func (b *Backend) publishAwaitedProvisionSuccesses(decisions []recoveredIntentDecision) error {
	b.provisionsMu.Lock()
	defer b.provisionsMu.Unlock()

	replacements := make(map[string]*provision)
	readyDelta := 0
	for _, decision := range decisions {
		if decision.readyProjection == nil {
			continue
		}
		current := b.provisions[decision.claim.LeaseUUID()]
		if current != nil {
			if !projectionMatchesOperationIntent(current, decision.claim) ||
				len(current.ContainerIDs) != 0 &&
					!sameStringSet(current.ContainerIDs, decision.readyProjection.containerIDs) {
				return fmt.Errorf(
					"awaited provision success for lease %q does not match its recovered projection",
					decision.claim.LeaseUUID(),
				)
			}
			if current.Status != backend.ProvisionStatusReady &&
				current.Status != backend.ProvisionStatusProvisioning {
				return fmt.Errorf(
					"awaited provision success for lease %q cannot promote projection status %q",
					decision.claim.LeaseUUID(), current.Status,
				)
			}
		}
		replacement, err := recoveredReadyProjection(decision.claim, decision.readyProjection, current)
		if err != nil {
			return fmt.Errorf(
				"materialize awaited provision success for lease %q: %w",
				decision.claim.LeaseUUID(), err,
			)
		}
		replacements[decision.claim.LeaseUUID()] = replacement
		if current == nil || current.Status != backend.ProvisionStatusReady {
			readyDelta++
		}
	}
	for leaseUUID, replacement := range replacements {
		b.provisions[leaseUUID] = replacement
	}
	if readyDelta != 0 {
		activeProvisions.Add(float64(readyDelta))
	}
	return nil
}

func recoveredReadyProjection(
	claim shared.OperationIntentClaim,
	promotion *recoveredOperationReadyPromotion,
	current *provision,
) (*provision, error) {
	if promotion == nil || promotion.stackManifest == nil {
		return nil, errors.New("awaited Ready substrate has no parsed manifest")
	}
	items := claim.EffectiveItems()
	quantity, err := backend.ValidateOperationQuantities(items)
	if err != nil {
		return nil, fmt.Errorf("validate operation quantities: %w", err)
	}
	if len(promotion.containerIDs) != quantity {
		return nil, fmt.Errorf(
			"awaited Ready substrate has %d containers, expected %d",
			len(promotion.containerIDs), quantity,
		)
	}
	failCount := 0
	if current != nil {
		failCount = current.FailCount
	}
	profiles := claim.ResourceProfiles()
	return (&recoveredProvision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID:            claim.LeaseUUID(),
			Tenant:               claim.Tenant(),
			ProviderUUID:         claim.ProviderUUID(),
			SKU:                  items[0].SKU,
			Status:               backend.ProvisionStatusReady,
			Quantity:             quantity,
			CreatedAt:            claim.CreatedAt(),
			FailCount:            failCount,
			LastError:            "",
			Reason:               "",
			Message:              "",
			CallbackURL:          claim.CallbackURL(),
			LifecycleCallbackURL: claim.LifecycleCallbackURL(),
			ActiveReleaseVersion: 0,
			ActiveOperationID:    claim.OperationID(),
			Items:                slices.Clone(items),
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(profiles),
			ContainerIDs:         slices.Clone(promotion.containerIDs),
			StackManifest:        promotion.stackManifest,
			ServiceContainers:    cloneOperationServiceContainers(promotion.serviceContainers),
		},
	}).materialize(), nil
}

func cloneOperationServiceContainers(source map[string][]string) map[string][]string {
	if source == nil {
		return nil
	}
	cloned := make(map[string][]string, len(source))
	for service, containerIDs := range source {
		cloned[service] = slices.Clone(containerIDs)
	}
	return cloned
}

func projectionMatchesOperationIntent(
	provision *provision,
	claim shared.OperationIntentClaim,
) bool {
	return provision != nil &&
		provision.Tenant == claim.Tenant() &&
		provision.ProviderUUID == claim.ProviderUUID() &&
		provision.CallbackURL == claim.CallbackURL() &&
		provision.LifecycleCallbackURL == claim.LifecycleCallbackURL() &&
		intentItemsMatchProjection(claim.EffectiveItems(), provision.Items) &&
		slices.Equal(provision.ResourceProfiles, claim.ResourceProfiles())
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	remaining := make(map[string]int, len(left))
	for _, value := range left {
		remaining[value]++
	}
	for _, value := range right {
		if remaining[value] == 0 {
			return false
		}
		remaining[value]--
	}
	return true
}

// awaitOperationIntentTerminal gives an exact, self-advancing provision cohort
// only the unspent budget computed from its durable admission timestamp and the
// recovery process's current ProvisionTimeout. A persisted wall-clock timestamp
// carries no monotonic component and may be in the future after clock rollback,
// so the recovery observation is independently capped to one current
// ProvisionTimeout from this process's first look.
//
// A created cohort cannot advance after the worker that would call
// ContainerStart has disappeared (Compose emits RestartPolicyNo), but Docker may
// still be completing a start request accepted just before the crash. Give that
// shape up to one current ContainerStartTimeout stabilization window, still
// capped by the provision deadline. Other exact non-terminal states are
// re-inspected until they become Ready/Failed or that deadline expires. A first
// empty inventory is not terminal evidence: the old daemon-side Create handler
// can publish after the client process has disappeared. Empty substrate therefore
// receives the operation's full remaining ProvisionTimeout and a final fresh
// strict inventory. ContainerStartTimeout is intentionally not its bound: a
// daemon-side Create accepted before the crash can become visible after that
// shorter start window. Every loop re-lists rather than repeatedly inspecting the
// original snapshot, so a late exact cohort becomes ordinary typed recovery
// evidence. Caller cancellation and read uncertainty preserve the intent; only a
// complete exact observation becomes settlement or teardown authority.
func (b *Backend) awaitOperationIntentsTerminal(
	ctx context.Context,
	classified []classifiedOperationIntent,
) error {
	timeout := b.cfg.ProvisionTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	startTimeout := b.cfg.ContainerStartTimeout
	if startTimeout <= 0 {
		startTimeout = 30 * time.Second
	}
	observedAt := time.Now()
	for index := range classified {
		entry := &classified[index]
		if entry.awaitRecovery && entry.operationDeadline.IsZero() {
			entry.operationDeadline = provisionIntentRecoveryDeadline(
				entry.claim.CreatedAt(), observedAt, timeout,
			)
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := time.Now()
		var wakeAt time.Time
		remaining := 0
		for index := range classified {
			entry := &classified[index]
			if !entry.awaitRecovery {
				continue
			}
			deadline, timeoutReason := operationIntentCandidateDeadline(entry, now, startTimeout)
			// Empty substrate needs one final fresh fleet observation at its
			// visibility deadline. A visible non-terminal cohort already has complete
			// identity evidence, so crossing its deadline cannot start a new read that
			// might manufacture a late success.
			if !entry.classification.requiresFinalInventory() && !now.Before(deadline) {
				entry.classification.needsTeardown = true
				entry.classification = b.operationIntentRecoveryTimedOut(
					entry.claim, entry.classification, deadline, timeoutReason,
				)
				entry.awaitRecovery = false
				continue
			}
			remaining++
			pollInterval := healthPollInterval
			if entry.classification.requiresFinalInventory() {
				// ContainerStartTimeout is a cadence, not the absence proof's bound.
				pollInterval = min(pollInterval, startTimeout)
			}
			candidateWake := now.Add(pollInterval)
			if deadline.Before(candidateWake) {
				candidateWake = deadline
			}
			if wakeAt.IsZero() || candidateWake.Before(wakeAt) {
				wakeAt = candidateWake
			}
		}
		if remaining == 0 {
			return nil
		}

		if wait := max(time.Until(wakeAt), 0); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}

		// Before beginning the shared observation, settle every visible candidate
		// whose own deadline elapsed. This preserves independent per-operation
		// deadlines without a goroutine per operation.
		now = time.Now()
		remaining = 0
		for index := range classified {
			entry := &classified[index]
			if !entry.awaitRecovery {
				continue
			}
			deadline, timeoutReason := operationIntentCandidateDeadline(entry, now, startTimeout)
			if !entry.classification.requiresFinalInventory() && !now.Before(deadline) {
				entry.classification.needsTeardown = true
				entry.classification = b.operationIntentRecoveryTimedOut(
					entry.claim, entry.classification, deadline, timeoutReason,
				)
				entry.awaitRecovery = false
				continue
			}
			remaining++
		}
		if remaining == 0 {
			continue
		}

		// One cadence owns exactly one strict fleet list. Index it once, then let
		// each still-live claim inspect only its own cohort under the aggregate
		// caller budget. This changes the old O(claims * fleet) list amplification
		// into O(fleet + exact candidate containers).
		containers, listErr := b.listManagedContainersStrictForRecovery(ctx)
		if listErr != nil {
			return fmt.Errorf("strict managed-container inventory while awaiting operations: %w", listErr)
		}
		inventory := operationInventoryByLease(containers)
		active := make([]int, 0, remaining)
		for index := range classified {
			if classified[index].awaitRecovery {
				active = append(active, index)
			}
		}
		slices.SortFunc(active, func(left, right int) int {
			return currentOperationIntentDeadline(&classified[left]).Compare(
				currentOperationIntentDeadline(&classified[right]),
			)
		})
		for _, index := range active {
			entry := &classified[index]
			deadline, timeoutReason := operationIntentCandidateDeadline(entry, time.Now(), startTimeout)
			claimCtx, cancelClaim := b.recoveryDockerReadContext(ctx)
			next, classifyErr := b.classifyOperationIntent(
				claimCtx, entry.claim, inventory[entry.claim.LeaseUUID()],
			)
			cancelClaim()
			if classifyErr != nil && !errors.Is(classifyErr, errOperationIntentSubstrateNonterminal) {
				return fmt.Errorf("%s operation intent for lease %q remains unresolved: %w",
					entry.claim.Kind(), entry.claim.LeaseUUID(), classifyErr)
			}
			entry.classification = next
			finishedAt := time.Now()
			if !finishedAt.Before(deadline) {
				entry.classification.needsTeardown = true
				entry.classification = b.operationIntentRecoveryTimedOut(
					entry.claim, entry.classification, deadline, timeoutReason,
				)
				entry.awaitRecovery = false
				continue
			}
			if classifyErr == nil && entry.classification.waitEvidence == nil {
				entry.awaitRecovery = false
			}
		}
	}
}

// awaitOperationIntentTerminal is the single-claim façade used by focused
// classifier tests. Production recovery always calls the batch coordinator.
func (b *Backend) awaitOperationIntentTerminal(
	ctx context.Context,
	claim shared.OperationIntentClaim,
	classification operationIntentSubstrate,
) (operationIntentSubstrate, error) {
	classified := []classifiedOperationIntent{{
		claim: claim, classification: classification, awaitRecovery: true,
	}}
	if err := b.awaitOperationIntentsTerminal(ctx, classified); err != nil {
		return classification, err
	}
	return classified[0].classification, nil
}

func operationIntentCandidateDeadline(
	entry *classifiedOperationIntent,
	now time.Time,
	startTimeout time.Duration,
) (time.Time, string) {
	deadline := entry.operationDeadline
	timeoutReason := operationRecoveryTimeoutProvision
	switch {
	case entry.classification.requiresFinalInventory():
		entry.inertDeadline = time.Time{}
	case entry.classification.usesStartWindow():
		if entry.inertDeadline.IsZero() {
			entry.inertDeadline = now.Add(startTimeout)
		}
		if entry.inertDeadline.Before(deadline) {
			deadline = entry.inertDeadline
			timeoutReason = operationRecoveryTimeoutStart
		}
	default:
		entry.inertDeadline = time.Time{}
	}
	return deadline, timeoutReason
}

func currentOperationIntentDeadline(entry *classifiedOperationIntent) time.Time {
	deadline := entry.operationDeadline
	if entry.classification.usesStartWindow() &&
		!entry.inertDeadline.IsZero() && entry.inertDeadline.Before(deadline) {
		return entry.inertDeadline
	}
	return deadline
}

func (b *Backend) operationIntentRecoveryTimedOut(
	claim shared.OperationIntentClaim,
	classification operationIntentSubstrate,
	deadline time.Time,
	timeoutReason string,
) operationIntentSubstrate {
	classification.status = backend.CallbackStatusFailed
	classification.errMsg = interruptedOperationFailure
	waitKind := "settled"
	if classification.waitEvidence != nil {
		waitKind = classification.waitEvidence.kind()
	}
	classification.waitEvidence = nil
	operationIntentRecoveryTimeoutExhaustionsTotal.WithLabelValues(timeoutReason).Inc()
	b.logger.Warn("bounded interrupted operation recovery classified exact substrate as failed",
		"lease_uuid", claim.LeaseUUID(),
		"operation_fingerprint", claim.OperationID().Fingerprint(),
		"wait_evidence", waitKind,
		"admitted_at", claim.CreatedAt(),
		"deadline", deadline,
		"reason", timeoutReason,
	)
	return classification
}

func provisionIntentRecoveryDeadline(admittedAt, observedAt time.Time, timeout time.Duration) time.Time {
	remaining := timeout
	if !admittedAt.After(observedAt) {
		elapsed := observedAt.Sub(admittedAt)
		if elapsed >= timeout {
			remaining = 0
		} else {
			remaining = timeout - elapsed
		}
	}
	// Deriving the absolute deadline from the live observation retains its
	// monotonic clock component. The durable timestamp has wall-clock precision
	// only and is used solely to compute how much of the original budget remains.
	return observedAt.Add(remaining)
}

func (b *Backend) validateRecoveredOperationSuccess(claim shared.OperationIntentClaim) error {
	return b.validateRecoveredOperationSuccessWithPromotion(claim, nil)
}

func (b *Backend) validateRecoveredOperationSuccessForDecision(
	decision recoveredIntentDecision,
) error {
	return b.validateRecoveredOperationSuccessWithPromotion(
		decision.claim, decision.readyProjection,
	)
}

func (b *Backend) validateRecoveredOperationSuccessWithPromotion(
	claim shared.OperationIntentClaim,
	promotion *recoveredOperationReadyPromotion,
) error {
	payload := claim.Manifest()
	if len(payload) == 0 {
		return fmt.Errorf("durable intent has no manifest")
	}
	if _, err := manifest.ParsePayload(payload); err != nil {
		return fmt.Errorf("parse durable operation manifest: %w", err)
	}
	if b.releaseStore == nil {
		return fmt.Errorf("release store is required")
	}
	if err := validateDockerResourceProfiles(claim.Items(), claim.ResourceProfiles()); err != nil {
		return fmt.Errorf("validate recovered operation resource profiles: %w", err)
	}
	committedOperation, err := b.operationIntentHasCommittedRelease(claim)
	if err != nil {
		return fmt.Errorf("validate committed operation release: %w", err)
	}

	b.provisionsMu.RLock()
	provision, exists := b.provisions[claim.LeaseUUID()]
	if exists && !projectionMatchesOperationIntent(provision, claim) {
		b.provisionsMu.RUnlock()
		return fmt.Errorf("strict substrate does not have an exact recovered projection")
	}
	if !exists && promotion == nil {
		b.provisionsMu.RUnlock()
		return fmt.Errorf("strict substrate does not have an exact recovered projection")
	}
	status := backend.ProvisionStatus("")
	containerCount := 0
	if exists {
		status = provision.Status
		containerCount = len(provision.ContainerIDs)
	}
	promotionMatches := promotion != nil &&
		(!exists || len(provision.ContainerIDs) == 0 ||
			sameStringSet(provision.ContainerIDs, promotion.containerIDs))
	if promotion != nil {
		containerCount = len(promotion.containerIDs)
	}
	b.provisionsMu.RUnlock()
	if promotion != nil && !promotionMatches {
		return fmt.Errorf("awaited Ready container set differs from recovered projection")
	}
	if committedOperation {
		if status != backend.ProvisionStatusReady && status != backend.ProvisionStatusFailed {
			return fmt.Errorf("committed operation projection remains non-terminal: %s", status)
		}
		return nil
	}
	readyOrPromotable := status == backend.ProvisionStatusReady || promotionMatches &&
		(!exists || status == backend.ProvisionStatusProvisioning)
	if !readyOrPromotable || containerCount != expectedIntentQuantity(claim.EffectiveItems()) {
		return fmt.Errorf("strict substrate does not have an exact Ready recovered projection")
	}

	if _, err := b.releaseStore.LatestActive(claim.LeaseUUID()); err != nil {
		return fmt.Errorf("read active release: %w", err)
	}
	if claim.Kind() != shared.OperationIntentRestore {
		return nil
	}
	if b.retentionStore == nil {
		return fmt.Errorf("retention store is required for recovered restore")
	}
	record, err := b.retentionStore.Get(claim.SourceLeaseUUID())
	if err != nil {
		return fmt.Errorf("re-read restore source finalizer: %w", err)
	}
	if record != nil && (record.Status != shared.RetentionStatusRestoring ||
		record.NewLeaseUUID != claim.LeaseUUID() ||
		record.Generation != claim.SourceGeneration()) {
		return fmt.Errorf("restore source finalizer changed before recovered success commit")
	}
	if record != nil && len(record.DestinationItems) > 0 &&
		(!slices.Equal(record.DestinationItems, claim.EffectiveItems()) ||
			!slices.Equal(record.DestinationResourceProfiles, claim.ResourceProfiles())) {
		return fmt.Errorf("restore source destination authority differs from recovered operation intent")
	}
	return nil
}

func (b *Backend) ensureRecoveredOperationSuccess(
	ctx context.Context,
	recoveryScope shared.LeaseRecoveryScope,
	claim shared.OperationIntentClaim,
) (shared.OperationReleaseCommitted, error) {
	if err := b.validateRecoveredOperationSuccess(claim); err != nil {
		return shared.OperationReleaseCommitted{}, err
	}
	payload := claim.Manifest()
	stack, err := manifest.ParsePayload(payload)
	if err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("parse durable operation manifest: %w", err)
	}
	b.provisionsMu.Lock()
	b.provisions[claim.LeaseUUID()].StackManifest = stack
	b.provisions[claim.LeaseUUID()].ResourceProfiles = claim.ResourceProfiles()
	b.provisionsMu.Unlock()
	committedOperation, err := b.operationIntentHasCommittedRelease(claim)
	if err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("validate committed operation release: %w", err)
	}
	if committedOperation {
		// The Release is already the durable operation-success marker. A Failed
		// zero-survivor projection keeps its non-expiring identity in the same
		// atomic row, so settling the operation intent cannot erase restart
		// authority.
		committed, proveErr := b.operationSettlement.ProveCommittedOperation(claim)
		if proveErr != nil {
			return shared.OperationReleaseCommitted{}, proveErr
		}
		if err := b.bindRecoveredProjectionRelease(committed); err != nil {
			return shared.OperationReleaseCommitted{}, err
		}
		return committed, nil
	}

	// A byte-identical older generation is still not this operation's commit.
	// Re-run the construction-bound strict classifier and consume only its typed
	// Ready outcome; recovery can no longer append a caller-asserted release.
	outcome, recoverErr := b.operationSettlement.RecoverOperationExecution(ctx, recoveryScope, claim)
	if recoverErr != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("classify recovered active release: %w", recoverErr)
	}
	ready, ok := outcome.(shared.OperationExecutionSuccess)
	if !ok {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("recovered operation is not exactly Ready (%T)", outcome)
	}
	committed, err := b.operationSettlement.CommitOperationSuccess(ready)
	if err != nil {
		return shared.OperationReleaseCommitted{}, fmt.Errorf("record recovered active release: %w", err)
	}
	if err := b.bindRecoveredProjectionRelease(committed); err != nil {
		return shared.OperationReleaseCommitted{}, err
	}

	if claim.Kind() != shared.OperationIntentRestore {
		return committed, nil
	}
	// The caller settles the exact operation intent only after this Release write.
	// The source finalizer deliberately remains for the next level-triggered
	// retention pass, which deletes it only after observing that settlement.
	return committed, nil
}

func (b *Backend) bindRecoveredProjectionRelease(
	committed shared.OperationReleaseCommitted,
) error {
	release, ok := committed.Release()
	if !ok || release.Version <= 0 {
		return errors.New("recovered operation success has no exact committed release")
	}
	b.provisionsMu.Lock()
	defer b.provisionsMu.Unlock()
	projection := b.provisions[committed.LeaseUUID()]
	if projection == nil || projection.ActiveOperationID != committed.OperationID() {
		return errors.New("recovered operation release no longer matches its projection")
	}
	projection.ActiveReleaseVersion = release.Version
	return nil
}

// checkOperationReleaseCapacity proves the exact success record before the
// first tenant-substrate side effect. The durable intent timestamp is reused by
// the live commit and cold recovery so RFC3339Nano's variable-width encoding
// cannot invalidate the byte proof at the final boundary.
func (b *Backend) checkOperationReleaseCapacity(
	claim shared.OperationIntentClaim,
) error {
	planner := b.releaseHistoryCapacityPlanner()
	if planner == nil {
		return errors.New(
			"release store is required for asynchronous operation",
		)
	}
	if b.releaseStore == nil {
		return errors.New(
			"identity-bound release store is required for asynchronous operation",
		)
	}
	candidate, err := b.operationSettlement.PrepareOperationRelease(claim)
	if err != nil {
		return err
	}
	if err := planner.CheckOperationReleaseCapacity(candidate); err != nil {
		return err
	}
	return nil
}

func (b *Backend) releaseHistoryCapacityPlanner() releaseHistoryCapacityPlanner {
	if b == nil {
		return nil
	}
	if b.releaseCapacityPlanner != nil {
		return b.releaseCapacityPlanner
	}
	if b.operationSettlement == nil {
		return nil
	}
	return b.operationSettlement
}

func expectedIntentQuantity(items []backend.LeaseItem) int {
	total := 0
	for _, item := range items {
		total += item.Quantity
	}
	return total
}

func intentItemsMatchProjection(expected, actual []backend.LeaseItem) bool {
	type itemShape struct {
		sku          string
		quantity     int
		customDomain string
	}
	shape := func(items []backend.LeaseItem) (map[string]itemShape, bool) {
		result := make(map[string]itemShape, len(items))
		for _, item := range items {
			current, exists := result[item.ServiceName]
			if exists && (current.sku != item.SKU || current.customDomain != item.CustomDomain) {
				return nil, false
			}
			result[item.ServiceName] = itemShape{
				sku: item.SKU, quantity: current.quantity + item.Quantity, customDomain: item.CustomDomain,
			}
		}
		return result, true
	}
	want, ok := shape(expected)
	if !ok {
		return false
	}
	got, ok := shape(actual)
	if !ok || len(want) != len(got) {
		return false
	}
	for service, expectedShape := range want {
		if got[service] != expectedShape {
			return false
		}
	}
	return true
}

func (b *Backend) classifyOperationIntent(
	ctx context.Context,
	claim shared.OperationIntentClaim,
	all []ContainerInfo,
) (operationIntentSubstrate, error) {
	committed, err := b.operationIntentHasCommittedRelease(claim)
	if err != nil {
		return operationIntentSubstrate{}, err
	}
	if committed {
		return operationIntentSubstrate{status: backend.CallbackStatusSuccess}, nil
	}
	var classification operationIntentSubstrate
	if claim.Kind() == shared.OperationIntentProvision {
		active, readErr := b.releaseStore.LatestActive(claim.LeaseUUID())
		if readErr != nil {
			return operationIntentSubstrate{}, fmt.Errorf("read predecessor active release: %w", readErr)
		}
		classification, err = b.classifyProvisionIntentSubstrate(ctx, claim, active, all)
	} else {
		classification, err = b.classifyOperationIntentSubstrate(ctx, claim, all)
	}
	if err != nil && !errors.Is(err, errOperationIntentSubstrateNonterminal) {
		return classification, err
	}
	if err := b.validateRestoreIntentSource(claim, classification.hasCurrent); err != nil {
		return operationIntentSubstrate{}, err
	}
	return classification, err
}

// classifyProvisionIntentSubstrate recognizes the one older generation a
// provision retry can legitimately interrupt. Provision admission writes the
// candidate intent before tearing down a Failed predecessor, while the older
// active Release remains the durable runtime authority until the candidate
// commits. A crash can therefore leave an exact subset of predecessor
// containers beside zero or partial candidate containers. Both cohorts are
// safe to tear down only after every survivor proves one of those two exact
// authorities; any third/partial identity remains an operator-visible hard
// contradiction.
func (b *Backend) classifyProvisionIntentSubstrate(
	ctx context.Context,
	claim shared.OperationIntentClaim,
	predecessor *shared.Release,
	all []ContainerInfo,
) (operationIntentSubstrate, error) {
	predecessorIdentity, hasPredecessorIdentity := runtimeIdentityForRelease(predecessor)
	if hasPredecessorIdentity &&
		(predecessorIdentity.Tenant() != claim.Tenant() ||
			predecessorIdentity.ProviderUUID() != claim.ProviderUUID()) {
		return operationIntentSubstrate{}, errors.New(
			"candidate and predecessor active release belong to different tenant or provider",
		)
	}
	current := make([]ContainerInfo, 0)
	older := make([]ContainerInfo, 0)
	for _, container := range all {
		if container.LeaseUUID != claim.LeaseUUID() || strings.HasSuffix(container.Name, "-prev") {
			continue
		}
		currentOperation := container.CallbackURL == claim.CallbackURL()
		currentLifecycle := container.LifecycleCallbackURL == claim.LifecycleCallbackURL()
		if currentOperation || currentLifecycle {
			if !currentOperation || !currentLifecycle {
				return operationIntentSubstrate{}, fmt.Errorf(
					"container %q has a partial current callback identity", container.ContainerID,
				)
			}
			current = append(current, container)
			continue
		}
		if hasPredecessorIdentity {
			olderLifecycleURL := container.LifecycleCallbackURL
			if predecessorIdentity.Class() == shared.ReleaseAuthorityLegacy {
				var resolveErr error
				olderLifecycleURL, resolveErr = backend.ResolveLifecycleCallbackURL(
					container.CallbackURL, container.LifecycleCallbackURL,
				)
				if resolveErr != nil {
					return operationIntentSubstrate{}, fmt.Errorf(
						"container %q has an invalid legacy predecessor callback pair: %w",
						container.ContainerID, resolveErr,
					)
				}
			}
			olderOperation := container.CallbackURL == predecessorIdentity.CallbackURL()
			olderLifecycle := olderLifecycleURL == predecessorIdentity.LifecycleCallbackURL()
			if !olderOperation || !olderLifecycle {
				return operationIntentSubstrate{}, fmt.Errorf(
					"container %q does not exactly match the candidate or predecessor callback identity",
					container.ContainerID,
				)
			}
		} else if predecessor == nil || !predecessor.OperationID.IsZero() ||
			predecessor.RuntimeAuthority != nil || predecessor.LegacyRuntimeAuthority != nil ||
			len(predecessor.Items) == 0 || len(predecessor.ResourceProfiles) == 0 {
			return operationIntentSubstrate{}, errors.New(
				"container substrate with another callback generation exists without exact predecessor authority",
			)
		}
		older = append(older, container)
	}

	classification, classifyErr := b.classifyOperationIntentSubstrate(ctx, claim, current)
	if classifyErr != nil && !errors.Is(classifyErr, errOperationIntentSubstrateNonterminal) {
		return classification, classifyErr
	}
	if len(older) == 0 {
		// A complete Ready candidate cohort has enough exact, typed authority to
		// commit a fresh Release and supersede a stale v0.13 row. It does not
		// need recovery authority for that predecessor because no failure cleanup
		// or predecessor reconstruction follows this classification. Every failed
		// or partial candidate still requires the frozen legacy identity below
		// before its teardown can erase the last reconstruction witness.
		if classification.status == backend.CallbackStatusSuccess {
			return classification, nil
		}
		if predecessor != nil && predecessor.OperationID.IsZero() &&
			predecessor.RuntimeAuthority == nil && predecessor.LegacyRuntimeAuthority == nil {
			return operationIntentSubstrate{}, errors.New(
				"legacy predecessor has no durable runtime authority and no surviving cohort to freeze",
			)
		}
		if classifyErr != nil {
			return classification, classifyErr
		}
		return classification, nil
	}
	predecessorIDs, legacyAuthority, err := b.validatePredecessorProvisionSubset(
		ctx, claim, predecessor, older,
	)
	if err != nil {
		return operationIntentSubstrate{}, err
	}
	classification.status = backend.CallbackStatusFailed
	classification.errMsg = interruptedOperationFailure
	classification.hasCurrent = len(current) != 0
	classification.needsTeardown = true
	classification.currentIDs = append(classification.currentIDs, predecessorIDs...)
	if legacyAuthority != nil {
		predecessorCopy := *predecessor
		classification.legacyPredecessor = &predecessorCopy
		classification.legacyAuthority = legacyAuthority
	}
	return classification, nil
}

func (b *Backend) validatePredecessorProvisionSubset(
	ctx context.Context,
	claim shared.OperationIntentClaim,
	release *shared.Release,
	listed []ContainerInfo,
) ([]string, *shared.LegacyRuntimeAuthority, error) {
	if release == nil {
		return nil, nil, errors.New("predecessor active release is absent")
	}
	legacy := release.RuntimeAuthority == nil
	if legacy {
		if !release.OperationID.IsZero() {
			return nil, nil, errors.New("predecessor active release has partial runtime authority")
		}
	} else {
		if !release.OperationID.Valid() || release.OperationID != release.RuntimeAuthority.OperationID() {
			return nil, nil, errors.New("predecessor active release has inconsistent operation identity")
		}
		if release.RuntimeAuthority.Tenant() != claim.Tenant() ||
			release.RuntimeAuthority.ProviderUUID() != claim.ProviderUUID() {
			return nil, nil, errors.New(
				"candidate and predecessor active release belong to different tenant or provider",
			)
		}
	}
	if err := validateDockerResourceProfiles(release.Items, release.ResourceProfiles); err != nil {
		return nil, nil, fmt.Errorf("predecessor active release has invalid resource authority: %w", err)
	}
	stack, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("parse predecessor active manifest: %w", err)
	}
	type instanceKey struct {
		service string
		sku     string
		index   int
	}
	expected := make(map[instanceKey]struct{})
	domains := make(map[string]string, len(release.Items))
	for _, item := range release.Items {
		domains[item.ServiceName] = item.CustomDomain
		for index := range item.Quantity {
			key := instanceKey{service: item.ServiceName, sku: item.SKU, index: index}
			if _, duplicate := expected[key]; duplicate {
				return nil, nil, fmt.Errorf("predecessor release contains duplicate expected instance %+v", key)
			}
			expected[key] = struct{}{}
		}
	}
	seen := make(map[instanceKey]struct{}, len(listed))
	ids := make([]string, 0, len(listed))
	authority := release.RuntimeAuthority
	legacyAuthority := release.LegacyRuntimeAuthority
	var legacyCallbackURL string
	var legacyLifecycleCallbackURL string
	for _, summary := range listed {
		container, inspectErr := b.inspectContainerForRecovery(ctx, summary.ContainerID)
		if inspectErr != nil {
			return nil, nil, fmt.Errorf("inspect predecessor container %q: %w", summary.ContainerID, inspectErr)
		}
		if container.LeaseUUID != claim.LeaseUUID() {
			return nil, nil, fmt.Errorf("predecessor container %q lease identity changed", summary.ContainerID)
		}
		if legacy {
			resolvedLifecycle, resolveErr := backend.ResolveLifecycleCallbackURL(
				container.CallbackURL, container.LifecycleCallbackURL,
			)
			if resolveErr != nil {
				return nil, nil, fmt.Errorf(
					"legacy predecessor container %q has an invalid callback pair: %w",
					summary.ContainerID, resolveErr,
				)
			}
			if legacyAuthority != nil {
				identity, ok := release.RuntimeIdentity()
				if !ok {
					return nil, nil, errors.New("legacy predecessor active release has invalid runtime authority")
				}
				if !containerMatchesReleaseRuntimeIdentity(*container, identity) {
					return nil, nil, fmt.Errorf(
						"legacy predecessor container %q identity differs from its active release",
						summary.ContainerID,
					)
				}
			} else if container.Tenant != claim.Tenant() || container.ProviderUUID != claim.ProviderUUID() {
				return nil, nil, fmt.Errorf(
					"legacy predecessor container %q belongs to a different tenant or provider",
					summary.ContainerID,
				)
			}
			if container.CallbackURL == "" {
				return nil, nil, fmt.Errorf("legacy predecessor container %q has no callback identity", summary.ContainerID)
			}
			if container.CallbackURL == claim.CallbackURL() ||
				resolvedLifecycle == claim.LifecycleCallbackURL() {
				return nil, nil, fmt.Errorf(
					"legacy predecessor container %q partially matches the candidate callback identity",
					summary.ContainerID,
				)
			}
			if legacyCallbackURL == "" {
				legacyCallbackURL = container.CallbackURL
				legacyLifecycleCallbackURL = resolvedLifecycle
			} else if container.CallbackURL != legacyCallbackURL ||
				resolvedLifecycle != legacyLifecycleCallbackURL {
				return nil, nil, errors.New("legacy predecessor cohort has mixed callback identities")
			}
		} else if container.Tenant != authority.Tenant() ||
			container.ProviderUUID != authority.ProviderUUID() ||
			container.CallbackURL != authority.CallbackURL() ||
			container.LifecycleCallbackURL != authority.LifecycleCallbackURL() {
			return nil, nil, fmt.Errorf("predecessor container %q identity changed or differs from its active release", summary.ContainerID)
		}
		serviceManifest, ok := stack.Services[container.ServiceName]
		if !ok || serviceManifest == nil || container.Image != serviceManifest.Image {
			return nil, nil, fmt.Errorf("predecessor container %q image differs from its active release", summary.ContainerID)
		}
		if container.CustomDomain != domains[container.ServiceName] {
			return nil, nil, fmt.Errorf("predecessor container %q custom domain differs from its active release", summary.ContainerID)
		}
		key := instanceKey{service: container.ServiceName, sku: container.SKU, index: container.InstanceIndex}
		if _, ok := expected[key]; !ok {
			return nil, nil, fmt.Errorf("predecessor container %q is not in the exact released instance set", summary.ContainerID)
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, nil, fmt.Errorf("duplicate predecessor container for instance %+v", key)
		}
		seen[key] = struct{}{}
		ids = append(ids, summary.ContainerID)
	}
	if legacy && legacyAuthority == nil {
		frozen, freezeErr := shared.NewLegacyRuntimeAuthority(
			claim.Tenant(),
			claim.ProviderUUID(),
			legacyCallbackURL,
			legacyLifecycleCallbackURL,
		)
		if freezeErr != nil {
			return nil, nil, fmt.Errorf("freeze legacy predecessor runtime authority: %w", freezeErr)
		}
		legacyAuthority = &frozen
	}
	return ids, legacyAuthority, nil
}

// operationIntentHasCommittedRelease recognizes the durable success boundary
// independently of current container health. Provision and restore both write
// their exact active Release before the actor publishes Ready and settles the
// operation callback; containers that fail or disappear after that write are a
// lifecycle failure, not proof that the operation itself failed.
//
// An absent Release, an empty legacy OperationID, or a different OperationID is
// evidence only for another generation and therefore remains uncommitted. Once
// the same OperationID is present, every immutable operation field must match:
// divergence is corruption/ambiguity and fails recovery closed rather than
// silently downgrading a committed generation to Failed.
func (b *Backend) operationIntentHasCommittedRelease(
	claim shared.OperationIntentClaim,
) (bool, error) {
	if b.releaseStore == nil {
		return false, errors.New("release store is required for operation intent recovery")
	}
	active, err := b.releaseStore.LatestActive(claim.LeaseUUID())
	if err != nil {
		return false, fmt.Errorf("read operation active release: %w", err)
	}
	committed, err := operationReleaseMatchesIntent(active, claim)
	if err != nil || !committed {
		return committed, err
	}
	if claim.Kind() != shared.OperationIntentRestore {
		return true, nil
	}
	if b.retentionStore == nil {
		return false, errors.New("retention store is required for restore intent recovery")
	}
	record, err := b.retentionStore.Get(claim.SourceLeaseUUID())
	if err != nil {
		return false, fmt.Errorf("read restore source finalizer: %w", err)
	}
	if record == nil {
		// The commit may have consumed the source finalizer before callback
		// settlement. The exact Release still proves this operation generation.
		return true, nil
	}
	if record.Status != shared.RetentionStatusRestoring ||
		record.NewLeaseUUID != claim.LeaseUUID() ||
		record.Generation != claim.SourceGeneration() {
		return false, errors.New("restore source finalizer differs from committed operation intent")
	}
	if err := b.validateRestoreOperationAuthority(claim, *record); err != nil {
		return false, err
	}
	return true, nil
}

// operationReleaseMatchesIntent is the single exact-Release predicate shared by
// startup recovery and close admission. It deliberately distinguishes an older
// or legacy generation (false, nil) from a same-token divergent generation
// (false, error), because only the latter claims to be this exact operation.
func operationReleaseMatchesIntent(
	active *shared.Release,
	claim shared.OperationIntentClaim,
) (bool, error) {
	if !claim.OperationID().Valid() {
		return false, errors.New("operation intent has an invalid operation ID")
	}
	if active == nil || active.OperationID.IsZero() {
		return false, nil
	}
	if !active.OperationID.Valid() {
		return false, errors.New("active release has an invalid operation ID")
	}
	if active.OperationID != claim.OperationID() {
		return false, nil
	}
	if !bytes.Equal(active.Manifest, claim.Manifest()) ||
		!slices.Equal(active.Items, claim.EffectiveItems()) ||
		!slices.Equal(active.ResourceProfiles, claim.ResourceProfiles()) {
		return false, errors.New("active release with matching operation ID differs from operation intent")
	}
	if active.RuntimeAuthority == nil {
		return false, errors.New("active release with matching operation ID has no runtime authority")
	}
	if !releaseRuntimeAuthorityMatchesIntent(active.RuntimeAuthority, claim) {
		return false, errors.New("active release runtime authority differs from operation intent")
	}
	return true, nil
}

func releaseRuntimeAuthorityForOperation(
	operationID shared.OperationID,
	tenant, providerUUID, callbackURL, lifecycleCallbackURL string,
) (*shared.ReleaseRuntimeAuthority, error) {
	if !operationID.Valid() {
		if operationID.IsZero() {
			return nil, nil
		}
		return nil, errors.New("release runtime authority requires a valid operation ID")
	}
	authority, err := shared.NewReleaseRuntimeAuthority(
		operationID, tenant, providerUUID, callbackURL, lifecycleCallbackURL,
	)
	if err != nil {
		return nil, err
	}
	return &authority, nil
}

func releaseRuntimeAuthorityMatchesIntent(
	authority *shared.ReleaseRuntimeAuthority,
	claim shared.OperationIntentClaim,
) bool {
	return authority != nil &&
		authority.OperationID() == claim.OperationID() &&
		authority.Tenant() == claim.Tenant() &&
		authority.ProviderUUID() == claim.ProviderUUID() &&
		authority.CallbackURL() == claim.CallbackURL() &&
		authority.LifecycleCallbackURL() == claim.LifecycleCallbackURL()
}

func (b *Backend) classifyOperationIntentSubstrate(
	ctx context.Context,
	claim shared.OperationRecoveryState,
	all []ContainerInfo,
) (operationIntentSubstrate, error) {
	var current []ContainerInfo
	oldCount := 0
	for _, container := range all {
		if container.LeaseUUID != claim.LeaseUUID() {
			continue
		}
		callbackMatches := container.CallbackURL == claim.CallbackURL()
		lifecycleMatches := container.LifecycleCallbackURL == claim.LifecycleCallbackURL()
		switch {
		case callbackMatches && lifecycleMatches:
			current = append(current, container)
		case callbackMatches || lifecycleMatches:
			return operationIntentSubstrate{}, fmt.Errorf("container %q has a partial current callback identity", container.ContainerID)
		default:
			oldCount++
		}
	}
	if oldCount != 0 {
		return operationIntentSubstrate{}, fmt.Errorf("container substrate with another callback generation exists")
	}
	if len(current) == 0 {
		// One empty list is not terminal evidence: a Docker Create handler that
		// outlived the old client connection can publish after this observation.
		// The recovery owner must obtain a later strict inventory before it may
		// consume the write-ahead intent.
		return operationIntentSubstrate{
			status:       backend.CallbackStatusFailed,
			errMsg:       interruptedOperationFailure,
			waitEvidence: operationAwaitLateVisibility{},
		}, nil
	}
	stack, err := manifest.ParsePayload(claim.Manifest())
	if err != nil {
		return operationIntentSubstrate{}, fmt.Errorf("parse durable operation manifest: %w", err)
	}

	type instanceKey struct {
		service string
		sku     string
		index   int
	}
	expected := make(map[instanceKey]struct{})
	healthRequired := make(map[string]struct{}, len(claim.HealthCheckServices()))
	for _, service := range claim.HealthCheckServices() {
		healthRequired[service] = struct{}{}
	}
	for _, item := range claim.Items() {
		for index := range item.Quantity {
			key := instanceKey{service: item.ServiceName, sku: item.SKU, index: index}
			if _, duplicate := expected[key]; duplicate {
				return operationIntentSubstrate{}, fmt.Errorf("intent contains duplicate expected instance %+v", key)
			}
			expected[key] = struct{}{}
		}
	}

	seen := make(map[instanceKey]struct{}, len(current))
	currentIDs := make([]string, 0, len(current))
	serviceContainers := make(map[string][]string, len(claim.EffectiveItems()))
	effectiveDomains := make(map[string]string, len(claim.EffectiveItems()))
	for _, item := range claim.EffectiveItems() {
		effectiveDomains[item.ServiceName] = item.CustomDomain
		serviceContainers[item.ServiceName] = make([]string, item.Quantity)
	}
	serviceDomains := make(map[string]string, len(effectiveDomains))
	ready, failed, nonterminal := 0, 0, 0
	var waitEvidence operationIntentWaitEvidence = operationAwaitProgress{}
	for _, listed := range current {
		container, err := b.inspectContainerForRecovery(ctx, listed.ContainerID)
		if err != nil {
			return operationIntentSubstrate{}, fmt.Errorf("inspect current container %q: %w", listed.ContainerID, err)
		}
		if container.LeaseUUID != claim.LeaseUUID() ||
			container.BackendName != b.cfg.Name ||
			container.Tenant != claim.Tenant() ||
			container.ProviderUUID != claim.ProviderUUID() ||
			container.CallbackURL != claim.CallbackURL() ||
			container.LifecycleCallbackURL != claim.LifecycleCallbackURL() {
			return operationIntentSubstrate{}, fmt.Errorf("container %q identity changed or does not match the intent", listed.ContainerID)
		}
		serviceManifest, ok := stack.Services[container.ServiceName]
		if !ok || serviceManifest == nil || container.Image != serviceManifest.Image {
			return operationIntentSubstrate{}, fmt.Errorf("container %q image does not match the durable manifest", listed.ContainerID)
		}
		if domain, exists := serviceDomains[container.ServiceName]; exists && domain != container.CustomDomain {
			return operationIntentSubstrate{}, fmt.Errorf("service %q has inconsistent custom-domain labels", container.ServiceName)
		}
		serviceDomains[container.ServiceName] = container.CustomDomain
		if container.CustomDomain != effectiveDomains[container.ServiceName] {
			return operationIntentSubstrate{}, fmt.Errorf("container %q custom domain does not match durable effective items", listed.ContainerID)
		}
		key := instanceKey{service: container.ServiceName, sku: container.SKU, index: container.InstanceIndex}
		if _, ok := expected[key]; !ok {
			return operationIntentSubstrate{}, fmt.Errorf("container %q is not in the exact expected instance set", listed.ContainerID)
		}
		if _, duplicate := seen[key]; duplicate {
			return operationIntentSubstrate{}, fmt.Errorf("duplicate container for expected instance %+v", key)
		}
		seen[key] = struct{}{}
		currentIDs = append(currentIDs, listed.ContainerID)
		serviceContainers[container.ServiceName][container.InstanceIndex] = listed.ContainerID
		runtimeStatus := strings.ToLower(container.Status)
		// Docker reports a paused container as otherwise "running". It cannot
		// become useful without an explicit unpause, however, and no interrupted
		// operation worker remains to issue one. Treat it like a created-only
		// cohort even when the service has no health check.
		if runtimeStatus == "paused" {
			nonterminal++
			waitEvidence = operationAwaitInert{}
			continue
		}
		switch containerStatusToProvisionStatus(runtimeStatus) {
		case backend.ProvisionStatusReady:
			if _, required := healthRequired[container.ServiceName]; required &&
				container.Health != HealthStatusHealthy {
				if container.Health == HealthStatusUnhealthy {
					failed++
					continue
				}
				nonterminal++
				continue
			}
			ready++
		case backend.ProvisionStatusFailed:
			failed++
		default:
			nonterminal++
			if runtimeStatus != "restarting" {
				// Created and unknown states have no surviving worker that can
				// advance them. A genuine Docker restart remains self-advancing.
				waitEvidence = operationAwaitInert{}
			}
		}
	}
	if len(seen) != len(expected) {
		// Every observed instance was admitted through the expected-set lookup
		// above, and duplicates are rejected, so seen cannot exceed expected.
		return operationIntentSubstrate{
			status:        backend.CallbackStatusFailed,
			errMsg:        interruptedOperationFailure,
			hasCurrent:    true,
			needsTeardown: true,
			currentIDs:    currentIDs,
		}, nil
	}
	switch {
	case ready == len(expected):
		return operationIntentSubstrate{
			status:            backend.CallbackStatusSuccess,
			hasCurrent:        true,
			currentIDs:        currentIDs,
			serviceContainers: serviceContainers,
			stackManifest:     stack,
		}, nil
	case failed == len(expected):
		return operationIntentSubstrate{
			status:        backend.CallbackStatusFailed,
			errMsg:        interruptedOperationFailure,
			hasCurrent:    true,
			needsTeardown: true,
			currentIDs:    currentIDs,
		}, nil
	case failed != 0:
		// One exact failed member makes provision success impossible. Waiting for
		// healthy or restarting siblings cannot change that terminal outcome.
		return operationIntentSubstrate{
			status:        backend.CallbackStatusFailed,
			errMsg:        interruptedOperationFailure,
			hasCurrent:    true,
			needsTeardown: true,
			currentIDs:    currentIDs,
		}, nil
	case nonterminal != 0:
		// A running container whose health check is still starting is evidence
		// that the interrupted worker may still converge. Preserve the exact
		// intent while the bounded in-process recovery loop re-observes it;
		// tearing the cohort down on the first snapshot would turn an observation
		// gap into a terminal failure.
		return operationIntentSubstrate{
			hasCurrent:   true,
			waitEvidence: waitEvidence,
			currentIDs:   currentIDs,
		}, errOperationIntentSubstrateNonterminal
	case claim.Kind() == shared.OperationIntentProvision && ready+failed == len(expected):
		return operationIntentSubstrate{
			status:        backend.CallbackStatusFailed,
			errMsg:        interruptedOperationFailure,
			hasCurrent:    true,
			needsTeardown: true,
			currentIDs:    currentIDs,
		}, nil
	default:
		return operationIntentSubstrate{}, fmt.Errorf("mixed ready and failed substrate state")
	}
}

func (b *Backend) validateRestoreIntentSource(
	claim shared.OperationIntentClaim,
	hasCurrentDestination bool,
) error {
	if claim.Kind() != shared.OperationIntentRestore {
		return nil
	}
	if b.retentionStore == nil {
		return fmt.Errorf("restore intent has no retention store")
	}
	record, err := b.retentionStore.Get(claim.SourceLeaseUUID())
	if err != nil {
		return fmt.Errorf("read restore source finalizer: %w", err)
	}
	if record == nil {
		if hasCurrentDestination {
			// Both callers have already proved that the destination's exact Release
			// is not committed. Success is the only path that may consume this
			// finalizer, and it commits the Release first. With no commit marker,
			// accepting a missing row would leave adopted source bytes under the
			// destination namespace with no durable owner or rollback authority.
			return errors.New("restore source finalizer is absent while uncommitted destination substrate remains")
		}
		return errors.New("restore source finalizer is absent while destination substrate is absent")
	}
	// Operation recovery now runs before retention reconciliation. A post-claim
	// restore therefore remains Restoring whether Docker has published zero, a
	// transitional cohort, or a terminal cohort. The complete finalizer—not the
	// presence of a container—is the authority that permits recovery to wait,
	// commit, or tear down this exact destination generation.
	if record.Status == shared.RetentionStatusRestoring {
		if err := b.validateRestoreOperationAuthority(claim, *record); err != nil {
			return fmt.Errorf("restore source finalizer does not exactly own the destination: %w", err)
		}
		return nil
	}
	if hasCurrentDestination {
		return errors.New("uncommitted destination substrate remains without a restoring source finalizer")
	}
	if record.Status != shared.RetentionStatusActive {
		return fmt.Errorf("restore source has invalid status %q for pending operation recovery", record.Status)
	}
	// The claim either never committed (pre-claim generation) or rollback
	// committed (RollbackRestoring increments the claimed generation once).
	if record.Generation != claim.SourceGeneration()-1 &&
		record.Generation != claim.SourceGeneration()+1 {
		return fmt.Errorf("restore source generation %d is not the pre-claim or rolled-back generation for %d",
			record.Generation, claim.SourceGeneration())
	}
	return nil
}
