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

	// Ready: a container died (guard confirms); a deprovision arrived.
	sm.Configure(backend.ProvisionStatusReady).
		Permit(evContainerDied, backend.ProvisionStatusFailing, lsm.guardContainerActuallyDied).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning)

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

	sm.Configure(backend.ProvisionStatusRestarting).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning)
	sm.Configure(backend.ProvisionStatusUpdating).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning)

	// Failed.Permit(ProvisionRequested) for re-provision retries. No entry
	// actions for the Provision flow transitions — doProvision's defer
	// emits callbacks directly (guarded by Status==Provisioning), since
	// many tests call doProvision directly without going through the actor.
	sm.Configure(backend.ProvisionStatusFailed).
		Permit(evProvisionRequested, backend.ProvisionStatusProvisioning)

	// Deprovisioning ignores stale provision-completion events that might
	// fire from an async goroutine that already started but hadn't noticed
	// cancellation. Defense-in-depth mirroring Failing's pattern.
	sm.Configure(backend.ProvisionStatusDeprovisioning).
		Ignore(evProvisionCompleted).
		Ignore(evProvisionErrored).
		Ignore(evProvisionRequested)

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

	b.provisionsMu.Lock()
	currentProv, exists := b.provisions[leaseUUID]
	if !exists || currentProv.Status != backend.ProvisionStatusReady {
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


// onEnterFailedFromDiag runs as the Failing→Failed entry action when
// DiagGathered fires. Flips provision.Status, applies the diag to
// LastError, and emits the terminal Failed callback.
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
	b.provisionsMu.Unlock()

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

// gatherDiagAsync runs in a goroutine, doing I/O-heavy diag gathering
// outside the actor's serial execution. Fires diagGatheredMsg on success;
// returns silently if its context has been cancelled (the Failing.OnExit
// cancellation path). Defense-in-depth: even if the goroutine races past
// the ctx check and fires DiagGathered, the SM will have transitioned out
// of Failing and Ignore(DiagGathered) drops the event.
func (a *leaseActor) gatherDiagAsync(ctx context.Context, containerID string, info *ContainerInfo) {
	b := a.backend
	leaseUUID := a.leaseUUID

	diag := b.containerFailureDiagnostics(ctx, containerID, info)
	if ctx.Err() != nil {
		return
	}

	if diag != "" {
		b.provisionsMu.Lock()
		if p, ok := b.provisions[leaseUUID]; ok && p.Status == backend.ProvisionStatusFailing {
			p.LastError = errMsgContainerExited + ": " + diag
		}
		b.provisionsMu.Unlock()
	}
	if ctx.Err() != nil {
		return
	}

	var diagSnap shared.DiagnosticEntry
	var diagContainerIDs []string
	var diagKeys map[string]string
	b.provisionsMu.RLock()
	if p, ok := b.provisions[leaseUUID]; ok && p.Status == backend.ProvisionStatusFailing {
		diagSnap = diagnosticSnapshot(p)
		diagContainerIDs = append([]string(nil), p.ContainerIDs...)
		diagKeys = containerLogKeys(p)
	}
	b.provisionsMu.RUnlock()
	if diagSnap.LeaseUUID != "" {
		b.persistDiagnostics(diagSnap, diagContainerIDs, diagKeys)
	}
	if ctx.Err() != nil {
		return
	}

	a.send(diagGatheredMsg{
		result: diagResult{containerID: containerID, info: info, diag: diag},
	})
}
