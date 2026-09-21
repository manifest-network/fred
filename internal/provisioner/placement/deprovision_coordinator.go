package placement

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// ErrDeprovisionExecution identifies a construction-bound teardown failure.
var ErrDeprovisionExecution = errors.New("deprovision execution failed")

// ErrDeprovisionAuthorityUnresolvable means the exact historical holder set
// cannot be proven from durable authority and the configured topology.
var ErrDeprovisionAuthorityUnresolvable = errors.New("deprovision authority is unresolvable")

// deprovisionCoordinator owns the complete teardown transaction and its
// process-local retry memory. It never exposes a backend name, Registry claim,
// or caller-selected mutation target.
type deprovisionCoordinator struct {
	execution *ExecutionCoordinator

	mu         sync.Mutex
	candidates map[string]map[string]struct{}
}

func newDeprovisionCoordinator(execution *ExecutionCoordinator) *deprovisionCoordinator {
	return &deprovisionCoordinator{
		execution: execution, candidates: make(map[string]map[string]struct{}),
	}
}

func (coordinator *deprovisionCoordinator) remembered(leaseUUID string) []string {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	result := make([]string, 0, len(coordinator.candidates[leaseUUID]))
	for name := range coordinator.candidates[leaseUUID] {
		result = append(result, name)
	}
	return result
}

func (coordinator *deprovisionCoordinator) remember(leaseUUID string, names []string) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	remembered := coordinator.candidates[leaseUUID]
	if remembered == nil {
		remembered = make(map[string]struct{}, len(names))
		coordinator.candidates[leaseUUID] = remembered
	}
	for _, name := range names {
		if name != "" {
			remembered[name] = struct{}{}
		}
	}
}

func (coordinator *deprovisionCoordinator) forget(leaseUUID, backendName string) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if backendName == "" {
		delete(coordinator.candidates, leaseUUID)
		return
	}
	delete(coordinator.candidates[leaseUUID], backendName)
	if len(coordinator.candidates[leaseUUID]) == 0 {
		delete(coordinator.candidates, leaseUUID)
	}
}

func (coordinator *deprovisionCoordinator) executeEvent(
	ctx context.Context,
	leaseUUID string,
) DeprovisionEventResult {
	if coordinator == nil || coordinator.execution == nil ||
		!coordinator.execution.Valid() || leaseUUID == "" || ctx == nil {
		return deprovisionEventFromError(ErrDeprovisionExecution)
	}
	if err := ctx.Err(); err != nil {
		return deprovisionEventFromError(err)
	}
	authority := coordinator.execution.coordinator
	// A known inventory wait grants no mutation authority and therefore needs
	// no Registry claim. Avoid creating a new causal exclusion on every retry:
	// the same sweep must retain its exact terminal cleanup observations.
	// The second observation below still validates ownership after exclusion
	// is acquired, covering a receipt that arrives between these two steps.
	preflight := authority.store.observeDeprovisionPlacement(leaseUUID)
	if preflight.disposition != deprovisionPlacementObserved {
		err := fmt.Errorf("%w: authorize lease %s: %w", ErrDeprovisionExecution, leaseUUID, preflight.err)
		if preflight.disposition == deprovisionPlacementInventoryPending {
			return coordinator.deferredEvent(leaseUUID, DeprovisionDeferredInventory, err)
		}
		return deprovisionEventFromError(err)
	}
	var metadata operation.SettlementMetadata
	var settlement operation.DeprovisionClaim
	var leaseClaim operation.LeaseClaim
	wasInFlight, claimFinished, leaseClaimed := false, false, false
	claimResult := authority.tryClaimDeprovision(leaseUUID)
	if claimResult.Claimed() {
		settlement = claimResult.Claim()
		metadata = settlement.Metadata()
		wasInFlight = true
		defer func() {
			if !claimFinished && !authority.releaseDeprovision(settlement) {
				slog.Error("failed to release deprovision settlement claim",
					"lease_uuid", leaseUUID, "backend", metadata.Backend())
			}
		}()
	} else {
		if claimResult.Outcome() != operation.SettlementNotFound {
			err := fmt.Errorf("%w: lease %s operation is already being settled",
				ErrDeprovisionExecution, leaseUUID)
			if claimResult.Outcome() == operation.SettlementBusy {
				return coordinator.deferredEvent(leaseUUID, DeprovisionDeferredLifecycle, err)
			}
			return deprovisionEventFromError(err)
		}
		claim := authority.operations.TryClaimLeaseNow(leaseUUID)
		if !claim.Acquired() {
			err := fmt.Errorf("%w: lease %s lifecycle action is busy",
				ErrDeprovisionExecution, leaseUUID)
			if claim.Outcome() == operation.LeaseClaimBusy {
				return coordinator.deferredEvent(leaseUUID, DeprovisionDeferredLifecycle, err)
			}
			return deprovisionEventFromError(err)
		}
		leaseClaim, leaseClaimed = claim.Claim(), true
		defer func() {
			if leaseClaimed && !authority.operations.ReleaseLease(leaseClaim) {
				slog.Error("failed to release deprovision lease claim", "lease_uuid", leaseUUID)
			}
		}()
	}
	finish := func() error {
		if !wasInFlight {
			return nil
		}
		if !authority.finishDeprovision(settlement) {
			return fmt.Errorf("%w: lease %s lost exact settlement claim",
				ErrDeprovisionExecution, leaseUUID)
		}
		claimFinished = true
		return nil
	}

	candidates := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)
	add := func(name string) {
		if name == "" {
			return
		}
		if _, exists := seen[name]; exists {
			return
		}
		seen[name] = struct{}{}
		candidates = append(candidates, name)
	}
	for _, name := range coordinator.remembered(leaseUUID) {
		add(name)
	}
	observation := authority.store.observeDeprovisionPlacement(leaseUUID)
	if observation.disposition != deprovisionPlacementObserved {
		err := fmt.Errorf("%w: authorize lease %s: %w", ErrDeprovisionExecution, leaseUUID, observation.err)
		if observation.disposition == deprovisionPlacementInventoryPending {
			// Release the exact acquired claim through the defer above. A local
			// wait has not completed either the operation or its durable attempt.
			return coordinator.deferredEvent(leaseUUID, DeprovisionDeferredInventory, err)
		}
		return deprovisionEventFromError(errors.Join(err, finish()))
	}
	placement := observation.placement
	add(placement.Backend)
	add(placement.Attempt)
	unresolved, unaccountable := false, false
	if placement.State() == StateUnusable {
		for _, name := range placement.ConflictBackends {
			add(name)
		}
		if !placement.Conflict || placement.ConflictOwnersUnknown ||
			len(placement.ConflictBackends) < 2 {
			unresolved, unaccountable = true, true
		}
	}
	if wasInFlight {
		add(metadata.Backend())
	}
	finishWithError := func(cause error, retry []string) DeprovisionEventResult {
		coordinator.remember(leaseUUID, retry)
		return deprovisionEventFromError(errors.Join(cause, finish()))
	}

	configured, err := backendNames(coordinator.execution.backends)
	if err != nil {
		return finishWithError(fmt.Errorf("%w: enumerate backends: %w",
			ErrDeprovisionExecution, err), candidates)
	}
	reachable, missing := make([]string, 0, len(candidates)), make([]string, 0)
	for _, name := range candidates {
		if slices.Contains(configured, name) {
			reachable = append(reachable, name)
		} else {
			unresolved = true
			missing = append(missing, name)
		}
	}
	call := func(name string) deprovisionBackendCall {
		client, resolveErr := exactBackend(coordinator.execution.backends, name)
		if resolveErr != nil {
			return deprovisionBackendCall{disposition: deprovisionCallUnknown, err: resolveErr}
		}
		err := invokeDeprovision(ctx, client, leaseUUID)
		if err == nil {
			return deprovisionBackendCall{disposition: deprovisionCallCompleted}
		}
		if backend.DeprovisionNotDispatched(client, leaseUUID, err) {
			return deprovisionBackendCall{disposition: deprovisionCallNotDispatched, err: err}
		}
		return deprovisionBackendCall{disposition: deprovisionCallUnknown, err: err}
	}
	if !unresolved && len(reachable) > 0 {
		var failures []error
		failed := make([]string, 0)
		allFailedNotDispatched := true
		for _, name := range reachable {
			result := call(name)
			if failure := result.failure(); failure != nil {
				failures = append(failures, fmt.Errorf("backend %s: %w", name, failure))
				failed = append(failed, name)
				allFailedNotDispatched = allFailedNotDispatched && result.disposition == deprovisionCallNotDispatched
			} else {
				coordinator.forget(leaseUUID, name)
			}
		}
		if len(failures) != 0 {
			err := fmt.Errorf("%w: lease %s: %w", ErrDeprovisionExecution, leaseUUID, errors.Join(failures...))
			if allFailedNotDispatched {
				coordinator.remember(leaseUUID, failed)
				return coordinator.deferredEvent(leaseUUID, DeprovisionDeferredBackendUnavailable, err)
			}
			return finishWithError(err, failed)
		}
		coordinator.forget(leaseUUID, "")
		return deprovisionEventFromError(finish())
	}

	var sweepErrs []error
	failed := make([]string, 0)
	allFailedNotDispatched := true
	for _, name := range configured {
		result := call(name)
		if failure := result.failure(); failure != nil {
			failed = append(failed, name)
			sweepErrs = append(sweepErrs, fmt.Errorf("backend %s: %w", name, failure))
			allFailedNotDispatched = allFailedNotDispatched && result.disposition == deprovisionCallNotDispatched
		} else {
			coordinator.forget(leaseUUID, name)
		}
	}
	for _, name := range missing {
		sweepErrs = append(sweepErrs, fmt.Errorf("%w: backend %q is not configured",
			ErrDeprovisionAuthorityUnresolvable, name))
	}
	if unaccountable {
		sweepErrs = append(sweepErrs, fmt.Errorf("%w: durable ownership is incomplete",
			ErrDeprovisionAuthorityUnresolvable))
	}
	if len(sweepErrs) != 0 {
		retry := append(append([]string(nil), failed...), missing...)
		err := fmt.Errorf("%w: lease %s: %w", ErrDeprovisionExecution, leaseUUID, errors.Join(sweepErrs...))
		if len(missing) == 0 && !unaccountable && allFailedNotDispatched {
			coordinator.remember(leaseUUID, retry)
			return coordinator.deferredEvent(leaseUUID, DeprovisionDeferredBackendUnavailable, err)
		}
		return finishWithError(err, retry)
	}
	coordinator.forget(leaseUUID, "")
	return deprovisionEventFromError(finish())
}

type deprovisionCallDisposition uint8

const (
	deprovisionCallUnknown deprovisionCallDisposition = iota
	deprovisionCallCompleted
	deprovisionCallNotDispatched
)

type deprovisionBackendCall struct {
	disposition deprovisionCallDisposition
	err         error
}

// Only an observed successful call can yield nil. The zero value and any
// incomplete failure observation remain conservative execution failures.
func (result deprovisionBackendCall) failure() error {
	if result.disposition == deprovisionCallCompleted || result.err != nil {
		return result.err
	}
	return ErrDeprovisionExecution
}
