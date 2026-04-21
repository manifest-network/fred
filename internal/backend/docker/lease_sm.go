package docker

import (
	"context"
	"fmt"
	"time"

	"github.com/qmuntal/stateless"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// leaseEvent is the typed event enum fired into the lease state machine.
// The underlying stateless library accepts any comparable; this enum is the
// only thing callers ever pass to Fire, so state transitions are checked at
// call sites even though the library stores triggers as any.
type leaseEvent int

const (
	evContainerDied leaseEvent = iota
	evDeprovisionRequested
	evRestartRequested
	evUpdateRequested
	evProvisionRequested
	evProvisionCompleted
	evProvisionErrored
	evDiagGathered
	evContainersRemoved
	// evReplaceCompleted / evReplaceRecovered / evReplaceFailed represent
	// the outcome of a Restart or Update operation. Three events cover the
	// four observable outcomes (success, rollback-restored, rollback-failed,
	// preflight-restored):
	//
	//   err == nil                              → evReplaceCompleted → Ready
	//   err != nil, final Status == Ready       → evReplaceRecovered → Ready
	//   err != nil, final Status == Failed      → evReplaceFailed    → Failed
	//
	// evReplaceRecovered exists to distinguish "lease ended up Ready but
	// the requested change did NOT take effect" from the normal success
	// case — same destination (Ready), different callback (Failed with
	// rollback suffix vs Success).
	evReplaceCompleted
	evReplaceRecovered
	evReplaceFailed
)

func (e leaseEvent) String() string {
	switch e {
	case evContainerDied:
		return "ContainerDied"
	case evDeprovisionRequested:
		return "DeprovisionRequested"
	case evRestartRequested:
		return "RestartRequested"
	case evUpdateRequested:
		return "UpdateRequested"
	case evProvisionRequested:
		return "ProvisionRequested"
	case evProvisionCompleted:
		return "ProvisionCompleted"
	case evProvisionErrored:
		return "ProvisionErrored"
	case evDiagGathered:
		return "DiagGathered"
	case evContainersRemoved:
		return "ContainersRemoved"
	case evReplaceCompleted:
		return "ReplaceCompleted"
	case evReplaceRecovered:
		return "ReplaceRecovered"
	case evReplaceFailed:
		return "ReplaceFailed"
	}
	return fmt.Sprintf("leaseEvent(%d)", int(e))
}

// leaseSM wraps a stateless.StateMachine with a typed façade. Each lease
// actor owns one SM; transitions are serialized by the actor's inbox.
type leaseSM struct {
	actor *leaseActor
	sm    *stateless.StateMachine
}

func newLeaseSM(actor *leaseActor) *leaseSM {
	initial := readProvisionStatus(actor)
	sm := stateless.NewStateMachine(initial)

	// Count every transition for operator visibility. Runs inside Fire
	// in the actor's goroutine, so no additional synchronization needed.
	sm.OnTransitioned(func(_ context.Context, tr stateless.Transition) {
		leaseSMTransitionsTotal.WithLabelValues(
			fmt.Sprintf("%v", tr.Source),
			fmt.Sprintf("%v", tr.Destination),
			fmt.Sprintf("%v", tr.Trigger),
		).Inc()
	})

	lsm := &leaseSM{actor: actor, sm: sm}

	// Configure all existing states so Fire never hits an unconfigured state.
	for _, s := range []backend.ProvisionStatus{
		backend.ProvisionStatusProvisioning,
		backend.ProvisionStatusReady,
		backend.ProvisionStatusFailing,
		backend.ProvisionStatusFailed,
		backend.ProvisionStatusRestarting,
		backend.ProvisionStatusUpdating,
		backend.ProvisionStatusDeprovisioning,
		backend.ProvisionStatusUnknown,
	} {
		sm.Configure(s)
	}

	// Ready: a container died (guard confirms); a deprovision arrived;
	// or the operator initiated a Restart or Update.
	sm.Configure(backend.ProvisionStatusReady).
		Permit(evContainerDied, backend.ProvisionStatusFailing, lsm.guardContainerActuallyDied).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		Permit(evRestartRequested, backend.ProvisionStatusRestarting).
		Permit(evUpdateRequested, backend.ProvisionStatusUpdating)

	// Failing: transitional. The async diag goroutine is running. Either
	// DiagGathered arrives (→ Failed, emit terminal callback) or a
	// DeprovisionRequested preempts (→ Deprovisioning, no callback). On any
	// exit the goroutine's context is cancelled — the structural cc62f3b
	// mechanism. Subsequent ContainerDied/DiagGathered events after we've
	// moved past Failing are Ignore'd so the race between cancellation
	// signal and an in-flight goroutine firing DiagGathered can't resurrect
	// a stale Failed callback.
	sm.Configure(backend.ProvisionStatusFailing).
		OnEntryFrom(evContainerDied, lsm.onEnterFailing).
		OnExit(lsm.onExitFailing).
		Permit(evDiagGathered, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		Ignore(evContainerDied)

	// Failed: terminal callback already emitted on entry from DiagGathered.
	// Deprovision can still be requested; later ContainerDied/DiagGathered
	// are ignored.
	sm.Configure(backend.ProvisionStatusFailed).
		OnEntryFrom(evDiagGathered, lsm.onEnterFailedFromDiag).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		Ignore(evContainerDied).
		Ignore(evDiagGathered)

	// Deprovisioning: work runs in actor.handleDeprovision after Fire returns.
	// Ignore die events and any stale DiagGathered from a cancelled-too-late
	// async goroutine.
	sm.Configure(backend.ProvisionStatusDeprovisioning).
		Ignore(evContainerDied).
		Ignore(evDiagGathered)

	// Provisioning: async goroutine is running. Exits by ProvisionCompleted
	// (→ Ready, emit Success), ProvisionErrored (→ Failed, emit Failed), or
	// DeprovisionRequested (→ Deprovisioning, OnExit cancels goroutine — the
	// structural suppression for Provision+Deprovision races, analogous to
	// Failing's mechanism).
	sm.Configure(backend.ProvisionStatusProvisioning).
		Permit(evProvisionCompleted, backend.ProvisionStatusReady).
		Permit(evProvisionErrored, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		OnExit(lsm.onExitProvisioning).
		Ignore(evContainerDied).
		Ignore(evProvisionRequested)

	// Restarting/Updating: same shape as Provisioning — a goroutine is
	// doing the work; OnExit cancels it on preemption. The goroutine fires
	// evReplaceCompleted (full success), evReplaceRecovered (failure but
	// lease ended up Ready via rollback / preflight-restored), or
	// evReplaceFailed (ended up Failed).
	sm.Configure(backend.ProvisionStatusRestarting).
		Permit(evReplaceCompleted, backend.ProvisionStatusReady).
		Permit(evReplaceRecovered, backend.ProvisionStatusReady).
		Permit(evReplaceFailed, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		OnExit(lsm.onExitProvisioning).
		Ignore(evContainerDied)
	sm.Configure(backend.ProvisionStatusUpdating).
		Permit(evReplaceCompleted, backend.ProvisionStatusReady).
		Permit(evReplaceRecovered, backend.ProvisionStatusReady).
		Permit(evReplaceFailed, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		OnExit(lsm.onExitProvisioning).
		Ignore(evContainerDied)

	// Ready entry actions: emit Success from a Provision or Replace success,
	// or emit Failed-with-rollback-suffix from a Replace recovery. Status
	// and LastError were set by the underlying do* defer — entry actions
	// just send callbacks.
	sm.Configure(backend.ProvisionStatusReady).
		OnEntryFrom(evProvisionCompleted, lsm.onEnterReadyFromProvision).
		OnEntryFrom(evReplaceCompleted, lsm.onEnterReadyFromReplaceCompleted).
		OnEntryFrom(evReplaceRecovered, lsm.onEnterReadyFromReplaceRecovered)

	// Failed entry actions: emit Failed from a Provision error or Replace
	// failure. Permit(ProvisionRequested) for re-provision retries.
	sm.Configure(backend.ProvisionStatusFailed).
		OnEntryFrom(evProvisionErrored, lsm.onEnterFailedFromProvision).
		OnEntryFrom(evReplaceFailed, lsm.onEnterFailedFromReplace).
		Permit(evProvisionRequested, backend.ProvisionStatusProvisioning)

	// Deprovisioning ignores stale provision/replace-completion events that
	// might fire from an async goroutine that already started but hadn't
	// noticed cancellation. Defense-in-depth mirroring Failing's pattern.
	sm.Configure(backend.ProvisionStatusDeprovisioning).
		Ignore(evProvisionCompleted).
		Ignore(evProvisionErrored).
		Ignore(evProvisionRequested).
		Ignore(evReplaceCompleted).
		Ignore(evReplaceRecovered).
		Ignore(evReplaceFailed)

	// Failed: can accept restart/update retry requests in addition to
	// re-provision.
	sm.Configure(backend.ProvisionStatusFailed).
		Permit(evRestartRequested, backend.ProvisionStatusRestarting).
		Permit(evUpdateRequested, backend.ProvisionStatusUpdating)

	return lsm
}

// Fire is the typed entry point. Compile error if callers pass a non-leaseEvent.
func (lsm *leaseSM) Fire(ctx context.Context, ev leaseEvent, args ...any) error {
	return lsm.sm.FireCtx(ctx, ev, args...)
}

func (lsm *leaseSM) State() backend.ProvisionStatus {
	s, err := lsm.sm.State(context.Background())
	if err != nil {
		return backend.ProvisionStatusUnknown
	}
	status, ok := s.(backend.ProvisionStatus)
	if !ok {
		return backend.ProvisionStatusUnknown
	}
	return status
}

// guardContainerActuallyDied verifies via Docker Inspect that the container is
// actually exited. Docker events can be duplicated or arrive out of order.
// Stashes the inspect result on the actor for the entry action to use
// (stateless doesn't forward guard-captured data).
func (lsm *leaseSM) guardContainerActuallyDied(ctx context.Context, args ...any) bool {
	if len(args) < 1 {
		return false
	}
	containerID, ok := args[0].(string)
	if !ok {
		return false
	}
	b := lsm.actor.backend

	b.provisionsMu.RLock()
	p, exists := b.provisions[lsm.actor.leaseUUID]
	if !exists || p.Status != backend.ProvisionStatusReady {
		b.provisionsMu.RUnlock()
		return false
	}
	b.provisionsMu.RUnlock()

	reqCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	info, err := b.docker.InspectContainer(reqCtx, containerID)
	if err != nil {
		b.logger.Warn("failed to inspect container after die event",
			"container_id", shortID(containerID),
			"lease_uuid", lsm.actor.leaseUUID,
			"error", err,
		)
		return false
	}
	if containerStatusToProvisionStatus(info.Status) != backend.ProvisionStatusFailed {
		return false
	}
	lsm.actor.pendingDeathInfo = info
	return true
}

// onEnterFailing runs as the Ready→Failing entry action. Flips provision
// fields under lock and spawns the async diag goroutine whose context
// cancellation is the cc62f3b suppression mechanism.
func (lsm *leaseSM) onEnterFailing(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterFailing: missing containerID")
	}
	containerID, ok := args[0].(string)
	if !ok {
		return fmt.Errorf("onEnterFailing: containerID not string")
	}
	b := lsm.actor.backend
	leaseUUID := lsm.actor.leaseUUID
	info := lsm.actor.pendingDeathInfo

	// No Status recheck: the SM's guard already verified Ready, and between
	// the guard and this entry action no other path can flip Status off
	// Ready — every writer now routes through the SM, and recoverState's
	// Phase 2.5 changes preserve existing.Status for Ready leases. Only
	// the existence check survives (the entry could be gone if Deprovision
	// had completed in a prior ordering).
	b.provisionsMu.Lock()
	currentProv, exists := b.provisions[leaseUUID]
	if !exists {
		b.provisionsMu.Unlock()
		return nil
	}
	currentProv.Status = backend.ProvisionStatusFailing
	currentProv.FailCount++
	currentProv.LastError = errMsgContainerExited
	activeProvisions.Dec()
	b.provisionsMu.Unlock()

	// Spawn the async diag gather. Its context is derived from backend's
	// stopCtx so shutdown cancels; locally scoped so Failing.OnExit can
	// cancel on preemption.
	diagCtx, diagCancel := context.WithCancel(b.stopCtx)
	lsm.actor.diagCancel = diagCancel
	go lsm.actor.gatherDiagAsync(diagCtx, containerID, info)
	return nil
}

// onExitFailing cancels the in-flight diag goroutine whenever we leave
// Failing — whether by DiagGathered (normal) or DeprovisionRequested
// (preemption). Cancellation is the happy-path suppression; the Ignore
// declarations on Failed/Deprovisioning handle the race where the
// goroutine already fired DiagGathered before the cancel signal propagated.
func (lsm *leaseSM) onExitFailing(ctx context.Context, args ...any) error {
	if lsm.actor.diagCancel != nil {
		lsm.actor.diagCancel()
		lsm.actor.diagCancel = nil
	}
	return nil
}

// onExitProvisioning mirrors onExitFailing for the Provision flow: cancels
// the in-flight doProvision goroutine when we leave Provisioning. The same
// two-layer suppression applies — cancellation is best-effort; the Ignore
// declarations on Deprovisioning catch stale ProvisionCompleted/Errored.
func (lsm *leaseSM) onExitProvisioning(ctx context.Context, args ...any) error {
	if lsm.actor.provisionCancel != nil {
		lsm.actor.provisionCancel()
		lsm.actor.provisionCancel = nil
	}
	return nil
}

// onEnterReadyFromProvision fires when doProvision signals success. Owns
// the Status flip, ContainerIDs/Manifest/ServiceContainers update, gauge
// increment, and Success callback emission.
func (lsm *leaseSM) onEnterReadyFromProvision(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterReadyFromProvision: missing result")
	}
	result, ok := args[0].(provisionSuccessResult)
	if !ok {
		return fmt.Errorf("onEnterReadyFromProvision: arg not provisionSuccessResult")
	}
	b := lsm.actor.backend
	leaseUUID := lsm.actor.leaseUUID

	var callbackURL string
	b.provisionsMu.Lock()
	if p, ok := b.provisions[leaseUUID]; ok {
		p.Status = backend.ProvisionStatusReady
		p.ContainerIDs = result.containerIDs
		p.LastError = ""
		if result.manifest != nil {
			p.Manifest = result.manifest
		}
		if result.stackManifest != nil {
			p.StackManifest = result.stackManifest
		}
		if result.serviceContainers != nil {
			p.ServiceContainers = result.serviceContainers
		}
		activeProvisions.Inc()
		callbackURL = p.CallbackURL
	}
	b.provisionsMu.Unlock()

	b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusSuccess, "")
	return nil
}

// onEnterReadyFromReplaceCompleted fires when doReplace* signals success.
// Owns Status flip, ContainerIDs/ServiceContainers update, optional
// OnSuccess hook (update flow sets Manifest/StackManifest), gauge
// increment (if prevStatus was Failed), and Success callback emission.
func (lsm *leaseSM) onEnterReadyFromReplaceCompleted(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterReadyFromReplaceCompleted: missing result")
	}
	result, ok := args[0].(replaceSuccessResult)
	if !ok {
		return fmt.Errorf("onEnterReadyFromReplaceCompleted: arg not replaceSuccessResult")
	}
	b := lsm.actor.backend
	leaseUUID := lsm.actor.leaseUUID

	var callbackURL string
	b.provisionsMu.Lock()
	if p, ok := b.provisions[leaseUUID]; ok {
		p.ContainerIDs = result.containerIDs
		if result.serviceContainers != nil {
			p.ServiceContainers = result.serviceContainers
		}
		p.Status = backend.ProvisionStatusReady
		p.LastError = ""
		if result.prevStatus == backend.ProvisionStatusFailed {
			activeProvisions.Inc()
		}
		if result.onSuccess != nil {
			result.onSuccess(p)
		}
		callbackURL = p.CallbackURL
	}
	b.provisionsMu.Unlock()

	b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusSuccess, "")
	return nil
}

// onEnterReadyFromReplaceRecovered fires when doReplace* failed but the
// rollback restored the lease to Ready (or the preflight check failed
// without touching containers). Status ends up Ready; LastError is set
// to the rich failure diagnostic. For the restart-with-oldStopped case,
// LastError is cleared because we're back to the exact same state as
// before the restart.
func (lsm *leaseSM) onEnterReadyFromReplaceRecovered(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterReadyFromReplaceRecovered: missing info")
	}
	info, ok := args[0].(replaceFailureInfo)
	if !ok {
		return fmt.Errorf("onEnterReadyFromReplaceRecovered: arg not replaceFailureInfo")
	}
	b := lsm.actor.backend
	leaseUUID := lsm.actor.leaseUUID

	var callbackURL string
	var diagSnap shared.DiagnosticEntry
	var snapContainerIDs []string
	b.provisionsMu.Lock()
	if p, ok := b.provisions[leaseUUID]; ok {
		p.LastError = info.lastError
		p.FailCount++
		p.Status = backend.ProvisionStatusReady
		// Restart: if we actually stopped old containers and then restored
		// them, we're back to the exact same state — no persistent error.
		// Update: keep LastError so the UI shows why the update failed.
		if info.oldStopped && info.operation == "restart" {
			p.LastError = ""
		}
		diagSnap = diagnosticSnapshot(p)
		snapContainerIDs = append([]string(nil), p.ContainerIDs...)
		callbackURL = p.CallbackURL
	}
	b.provisionsMu.Unlock()

	if diagSnap.LeaseUUID != "" {
		if info.logKeys != nil {
			b.persistDiagnostics(diagSnap, snapContainerIDs, info.logKeys)
		} else {
			b.persistDiagnostics(diagSnap, snapContainerIDs)
		}
	}

	b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusFailed, info.callbackErr)
	return nil
}

// onEnterFailedFromReplace fires when doReplace* failed AND rollback
// failed (or no rollback was possible). Status ends up Failed; gauge is
// decremented if prevStatus was Ready (the operation took a Ready lease
// and ended it in a Failed state).
func (lsm *leaseSM) onEnterFailedFromReplace(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterFailedFromReplace: missing info")
	}
	info, ok := args[0].(replaceFailureInfo)
	if !ok {
		return fmt.Errorf("onEnterFailedFromReplace: arg not replaceFailureInfo")
	}
	b := lsm.actor.backend
	leaseUUID := lsm.actor.leaseUUID

	var callbackURL string
	var diagSnap shared.DiagnosticEntry
	var snapContainerIDs []string
	b.provisionsMu.Lock()
	if p, ok := b.provisions[leaseUUID]; ok {
		p.LastError = info.lastError
		p.FailCount++
		p.Status = backend.ProvisionStatusFailed
		if info.prevStatus == backend.ProvisionStatusReady {
			activeProvisions.Dec()
		}
		diagSnap = diagnosticSnapshot(p)
		snapContainerIDs = append([]string(nil), p.ContainerIDs...)
		callbackURL = p.CallbackURL
	}
	b.provisionsMu.Unlock()

	if diagSnap.LeaseUUID != "" {
		if info.logKeys != nil {
			b.persistDiagnostics(diagSnap, snapContainerIDs, info.logKeys)
		} else {
			b.persistDiagnostics(diagSnap, snapContainerIDs)
		}
	}

	b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusFailed, info.callbackErr)
	return nil
}

// onEnterFailedFromProvision fires when doProvision signals a failure.
// Owns Status flip, FailCount++, LastError update, persistDiagnostics,
// and the Failed callback. Cleanup (container/volume removal) still
// runs in the goroutine's defer — it's I/O that shouldn't block the
// actor.
func (lsm *leaseSM) onEnterFailedFromProvision(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterFailedFromProvision: missing error info")
	}
	info, ok := args[0].(provisionErrorInfo)
	if !ok {
		return fmt.Errorf("onEnterFailedFromProvision: arg not provisionErrorInfo")
	}
	b := lsm.actor.backend
	leaseUUID := lsm.actor.leaseUUID

	var callbackURL string
	var diagSnap shared.DiagnosticEntry
	var snapContainerIDs []string
	b.provisionsMu.Lock()
	if p, ok := b.provisions[leaseUUID]; ok {
		p.Status = backend.ProvisionStatusFailed
		p.FailCount++
		p.LastError = info.lastError
		diagSnap = diagnosticSnapshot(p)
		snapContainerIDs = append([]string(nil), p.ContainerIDs...)
		callbackURL = p.CallbackURL
	}
	b.provisionsMu.Unlock()

	if diagSnap.LeaseUUID != "" {
		b.persistDiagnostics(diagSnap, snapContainerIDs)
	}

	b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusFailed, info.callbackErr)
	return nil
}

// onEnterFailedFromDiag runs as the Failing→Failed entry action when
// DiagGathered fires. Owns all state mutations for this transition: flips
// Status, applies diag to LastError, persists diagnostics, emits the
// terminal Failed callback. Running in the actor's goroutine means no
// mutex races with the gathering goroutine (which is pure I/O now).
func (lsm *leaseSM) onEnterFailedFromDiag(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterFailedFromDiag: missing diag info")
	}
	result, ok := args[0].(diagResult)
	if !ok {
		return fmt.Errorf("onEnterFailedFromDiag: arg not diagResult")
	}
	b := lsm.actor.backend
	leaseUUID := lsm.actor.leaseUUID

	var callbackURL string
	var failCount int
	var diagSnap shared.DiagnosticEntry
	var diagContainerIDs []string
	var diagKeys map[string]string
	b.provisionsMu.Lock()
	p, exists := b.provisions[leaseUUID]
	if !exists {
		b.provisionsMu.Unlock()
		return nil
	}
	p.Status = backend.ProvisionStatusFailed
	if result.diag != "" {
		p.LastError = errMsgContainerExited + ": " + result.diag
	}
	callbackURL = p.CallbackURL
	failCount = p.FailCount
	diagSnap = diagnosticSnapshot(p)
	diagContainerIDs = append([]string(nil), p.ContainerIDs...)
	diagKeys = containerLogKeys(p)
	b.provisionsMu.Unlock()

	// Persist diagnostics (bbolt write). Runs in the actor goroutine —
	// briefly blocks other messages for this lease, but matches the
	// "actor owns all state" invariant. Bbolt writes are ~ms.
	if diagSnap.LeaseUUID != "" {
		b.persistDiagnostics(diagSnap, diagContainerIDs, diagKeys)
	}

	b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusFailed, errMsgContainerExited)

	logAttrs := []any{
		"lease_uuid", leaseUUID,
		"container_id", shortID(result.containerID),
		"fail_count", failCount,
	}
	if result.info != nil && result.info.ServiceName != "" {
		logAttrs = append(logAttrs, "service_name", result.info.ServiceName)
	}
	b.logger.Warn("container death detected via events API", logAttrs...)
	return nil
}

// diagResult carries gather output from the async goroutine into the Failed
// entry action via Fire args.
type diagResult struct {
	containerID string
	info        *ContainerInfo
	diag        string
}

// provisionSuccessResult carries doProvision / doProvisionStack output
// into Ready.OnEntryFrom(evProvisionCompleted) via Fire args. Manifest
// and StackManifest are mutually exclusive — single-manifest provisions
// populate Manifest; stacks populate StackManifest + ServiceContainers.
type provisionSuccessResult struct {
	containerIDs      []string
	manifest          *DockerManifest
	stackManifest     *StackManifest
	serviceContainers map[string][]string
}

// provisionErrorInfo carries doProvision failure data into
// Failed.OnEntryFrom(evProvisionErrored). callbackErr is the on-chain-safe
// hardcoded message; lastError is the full diagnostic string stashed in
// provision.LastError for authenticated API access.
type provisionErrorInfo struct {
	callbackErr string
	lastError   string
}

// replaceSuccessResult carries doReplaceContainers / doReplaceStackContainers
// success output into onEnterReadyFromReplace. The goroutine returns these;
// the entry action writes them to provision under a single lock acquisition.
// onSuccess is the optional hook supplied by the caller (update flow sets
// Manifest/StackManifest there).
type replaceSuccessResult struct {
	prevStatus        backend.ProvisionStatus
	containerIDs      []string
	serviceContainers map[string][]string // non-nil for stack
	onSuccess         func(*provision)
}

// replaceFailureInfo carries doReplace* failure data. Used by both
// onEnterReadyFromReplaceRecovered (Status ends up Ready) and
// onEnterFailedFromReplace (Status ends up Failed). The entry actions
// set LastError, increment FailCount, persist diagnostics, and adjust
// the activeProvisions gauge based on prevStatus.
type replaceFailureInfo struct {
	prevStatus  backend.ProvisionStatus
	operation   string // "restart" or "update"
	oldStopped  bool   // only meaningful on the recovery path (restart-with-oldStopped clears LastError)
	callbackErr string
	lastError   string
	// logKeys is used for stack persistDiagnostics — empty for single-manifest.
	logKeys map[string]string
}

// replaceResult is doReplace*'s return value bundling everything the
// goroutine wrapper needs to fire the right SM event. The callback path
// depends on (err, restored):
//
//	err == nil            → fire evReplaceCompleted with .success
//	err != nil, restored  → fire evReplaceRecovered with .failure
//	err != nil, !restored → fire evReplaceFailed    with .failure
type replaceResult struct {
	callbackErr string
	err         error
	restored    bool                 // only meaningful if err != nil
	success     replaceSuccessResult // populated when err == nil
	failure     replaceFailureInfo   // populated when err != nil
}

// readProvisionStatus snapshots provision.Status under RLock. Used on SM
// creation to pick the initial state.
func readProvisionStatus(actor *leaseActor) backend.ProvisionStatus {
	actor.backend.provisionsMu.RLock()
	defer actor.backend.provisionsMu.RUnlock()
	p, ok := actor.backend.provisions[actor.leaseUUID]
	if !ok {
		return backend.ProvisionStatusProvisioning
	}
	return p.Status
}

// gatherDiagAsync runs in a goroutine, doing pure I/O: Docker log fetch.
// All state mutations (LastError update, persist, callback) are done by
// onEnterFailedFromDiag, which runs in the actor's goroutine after Fire
// commits the Failing→Failed transition. This makes the goroutine a
// cancellable worker — no provisionsMu acquisition means no ctx-to-lock
// gap, and if the ctx is cancelled (Failing.OnExit on Deprovision
// preemption) the goroutine simply returns. The SM's
// Deprovisioning.Ignore(evDiagGathered) catches the race where the
// goroutine finishes and fires just as preemption happens.
func (a *leaseActor) gatherDiagAsync(ctx context.Context, containerID string, info *ContainerInfo) {
	diag := a.backend.containerFailureDiagnostics(ctx, containerID, info)
	if ctx.Err() != nil {
		return
	}
	a.send(diagGatheredMsg{
		result: diagResult{containerID: containerID, info: info, diag: diag},
	})
}
