package leasesm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/workbarrier"
)

// errActorTerminated is returned on the ack/reply channel when a
// caller-facing request (Provision/Restart/Update) arrives at an actor
// that has already terminated via handleDeprovision but whose
// removeFromRegistry defer hasn't yet fired. The caller (Backend.X)
// rolls back its pre-routing state via its error path (e.g.,
// removeProvision); a subsequent retry resolves-or-creates a fresh
// actor through actorForLocked. Without this check, such messages
// would hit SM.Ignore in Deprovisioning state and Fire would return
// nil — the handler would then ack success and spawn a worker under
// a terminated actor, wedging the lease.
var errActorTerminated = errors.New("lease actor terminated; retry will create a fresh actor")

// ErrWorkerDrainTimeout means a state transition could not prove that the
// worker owning the source state had stopped. Callers must treat the outcome as
// still in flight: in particular, deprovision must not tear substrate state
// down or report success while that worker may still publish a later write.
var ErrWorkerDrainTimeout = errors.New("lease mutation worker did not drain before the safety deadline")

type leaseMessage interface {
	isleaseMessage()
	// doneChan returns the channel to close when processing finishes, or nil
	// for fire-and-forget messages. Lifting this out of the handler dispatch
	// lets shutdown drain pending messages without a per-type switch.
	doneChan() chan struct{}
	// onPanic is called by the actor's recover when a message's handler
	// panics. Messages with reply/ack channels must non-blocking-send an
	// error here so their caller (Backend.Deprovision / Provision /
	// Restart / Update blocking on the channel) unblocks instead of
	// hanging until ctx cancellation. Messages without a caller to
	// unblock implement this as a no-op.
	onPanic(err error)
}

// actorCommandMessage is the closed set of commands allowed to resolve or
// create an actor. Observations intentionally do not implement this interface,
// so a stale substrate event cannot materialize a new actor generation.
type actorCommandMessage interface {
	leaseMessage
	isactorCommandMessage()
}

// actorObservationMessage is the closed set of substrate observations. A
// substrate must route these with an exact provision-generation capability.
type actorObservationMessage interface {
	leaseMessage
	isactorObservationMessage()
	onStaleGeneration()
}

// messageEnvelope gives every externally admitted message affine, copy-safe
// semantics. Copying an opaque wrapper copies only this pointer; exactly one
// successful actor admission can consume the envelope. A refused enqueue
// releases the claim so bounded routing retries remain possible.
type messageEnvelope struct {
	message  leaseMessage
	accepted atomic.Bool
}

func newMessageEnvelope(message leaseMessage) *messageEnvelope {
	return &messageEnvelope{message: message}
}

// ActorCommand is the opaque, one-shot capability for caller-authored work.
// Its zero value is invalid and is refused by TryEnqueueCommand.
type ActorCommand struct{ envelope *messageEnvelope }

func newActorCommand(message actorCommandMessage) ActorCommand {
	return ActorCommand{envelope: newMessageEnvelope(message)}
}

// ActorObservation is an opaque, one-shot substrate observation bound to an
// exact durable runtime generation. Its constructors require a store-issued
// RuntimeGenerationProof; admission and handling re-attest that proof through
// the actor's provision store, so callers cannot substitute an always-current
// classifier.
type ActorObservation struct{ envelope *messageEnvelope }

func newActorObservation(message actorObservationMessage) ActorObservation {
	return ActorObservation{envelope: newMessageEnvelope(message)}
}

// RecoveryCommand is deliberately distinct from an ordinary ActorCommand:
// recovery may repair an existing actor but must never resolve or create one.
type RecoveryCommand struct{ envelope *messageEnvelope }

func newRecoveryCommand(message leaseMessage) RecoveryCommand {
	return RecoveryCommand{envelope: newMessageEnvelope(message)}
}

// ActorReply is the receive-only half of a caller-facing actor command. The
// sending capability remains inside the private command value, so callers
// cannot acknowledge their own request or share a writable channel with a
// different actor command.
type ActorReply struct {
	result <-chan error
}

func newActorReply() (chan error, ActorReply) {
	result := make(chan error, 1)
	return result, ActorReply{result: result}
}

// Result exposes the command result as a receive-only channel for callers
// which already own a cancellation policy.
func (r ActorReply) Result() <-chan error { return r.result }

// Wait waits for the actor's linearized decision or for ctx cancellation.
func (r ActorReply) Wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("actor reply context is required")
	}
	if r.result == nil {
		return errors.New("actor reply is invalid")
	}
	select {
	case err := <-r.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ActorCompletion is an optional, receive-only processing barrier for an
// observation. The zero value is invalid; fire-and-forget observations use a
// distinct constructor and therefore carry no latent nullable channel.
type ActorCompletion struct {
	done <-chan struct{}
}

func (c ActorCompletion) Done() <-chan struct{} { return c.done }

// ObservationGenerationState is the exhaustive relationship between a queued
// substrate observation and the current in-memory projection. The distinction
// between Advanced and Superseded matters: an actor may itself advance fields
// on the same projection, while a replacement pointer represents a different
// generation.
type ObservationGenerationState uint8

const (
	ObservationGenerationCurrent ObservationGenerationState = iota + 1
	ObservationGenerationAdvanced
	ObservationGenerationSuperseded
	ObservationGenerationAbsent
)

// LeaseUUID returns the runtime generation to which this observation is bound.
// The zero value returns an empty identity.
func (observation ActorObservation) LeaseUUID() string {
	message := observation.message()
	switch value := message.(type) {
	case containerDiedMsg:
		return value.Runtime.LeaseUUID()
	case cohortDivergedMsg:
		return value.Runtime.LeaseUUID()
	default:
		return ""
	}
}

func (observation ActorObservation) message() actorObservationMessage {
	if observation.envelope == nil {
		return nil
	}
	message, _ := observation.envelope.message.(actorObservationMessage)
	return message
}

// Current reports whether both the durable Release and the volatile Ready
// projection still describe the generation sealed into this observation.
// Routing uses it before actor creation; the actor repeats the classification
// at serial handling time.
func (observation ActorObservation) Current(store LeaseProvisionStore) bool {
	return classifyActorObservation(store, observation.message()) == ObservationGenerationCurrent
}

func classifyActorObservation(
	store LeaseProvisionStore,
	message actorObservationMessage,
) ObservationGenerationState {
	if nilActorCapability(store) || message == nil {
		return ObservationGenerationAbsent
	}
	var proof shared.RuntimeGenerationProof
	switch value := message.(type) {
	case containerDiedMsg:
		proof = value.Runtime
	case cohortDivergedMsg:
		proof = value.Runtime
	default:
		return ObservationGenerationAbsent
	}
	if !proof.Valid() {
		return ObservationGenerationAbsent
	}
	if err := proof.Reattest(); err != nil {
		return ObservationGenerationSuperseded
	}
	switch value := message.(type) {
	case containerDiedMsg:
		return store.ClassifyReadyInstance(proof, value.ContainerID)
	case cohortDivergedMsg:
		return store.ClassifyReadyRuntime(proof)
	default:
		return ObservationGenerationAbsent
	}
}

// workerTerminalMessage is the closed set of messages produced by actor-owned
// workers. Retirement may still apply these messages after external admission
// has closed: they settle work that was accepted before retirement. Keeping the
// distinction in the type system prevents a future caller-facing command from
// being accidentally executed by the post-worker shutdown drain.
type workerTerminalMessage interface {
	leaseMessage
	isWorkerTerminalMessage()
}

// containerDiedMsg signals a container belonging to this lease has died.
// Construction explicitly chooses fire-and-forget or completion-tracked form.
type containerDiedMsg struct {
	ContainerID string
	Runtime     shared.RuntimeGenerationProof
	Done        chan struct{}
}

func (containerDiedMsg) isleaseMessage()            {}
func (containerDiedMsg) isactorObservationMessage() {}
func (m containerDiedMsg) doneChan() chan struct{}  { return m.Done }
func (containerDiedMsg) onStaleGeneration()         {}

// onPanic is a no-op: the caller (if any) unblocks via the done
// channel which is closed by handle()'s defer regardless of panic.
func (containerDiedMsg) onPanic(error) {}

func NewContainerDiedObservation(
	containerID string,
	runtime shared.RuntimeGenerationProof,
) (ActorObservation, error) {
	if containerID == "" {
		return ActorObservation{}, errors.New("container death observation requires a container id")
	}
	if !runtime.Valid() {
		return ActorObservation{}, errors.New("container death observation requires an exact runtime generation")
	}
	return newActorObservation(containerDiedMsg{ContainerID: containerID, Runtime: runtime}), nil
}

func NewTrackedContainerDiedObservation(
	containerID string,
	runtime shared.RuntimeGenerationProof,
) (ActorObservation, ActorCompletion, error) {
	if containerID == "" {
		return ActorObservation{}, ActorCompletion{}, errors.New("container death observation requires a container id")
	}
	if !runtime.Valid() {
		return ActorObservation{}, ActorCompletion{}, errors.New("container death observation requires an exact runtime generation")
	}
	done := make(chan struct{})
	return newActorObservation(containerDiedMsg{ContainerID: containerID, Runtime: runtime, Done: done}), ActorCompletion{done: done}, nil
}

// deprovisionMsg requests that the actor run the deprovision flow. Callers can
// obtain one only through NewDeprovisionCommand, which owns the reply channel.
type deprovisionMsg struct {
	Ctx   context.Context
	Reply chan error
}

func (deprovisionMsg) isleaseMessage()         {}
func (deprovisionMsg) isactorCommandMessage()  {}
func (deprovisionMsg) doneChan() chan struct{} { return nil }
func (m deprovisionMsg) onPanic(err error) {
	// Non-blocking send: Reply is buffered-1, caller receives at most
	// once. On recover, make sure the caller gets something.
	select {
	case m.Reply <- err:
	default:
	}
}

func NewDeprovisionCommand(ctx context.Context) (ActorCommand, ActorReply, error) {
	if ctx == nil {
		return ActorCommand{}, ActorReply{}, errors.New("deprovision context is required")
	}
	reply, receiver := newActorReply()
	return newActorCommand(deprovisionMsg{Ctx: ctx, Reply: reply}), receiver, nil
}

// cohortDivergedMsg reports that recovery observed a running instance cohort
// which does not match the durable desired release. It deliberately carries no
// caller-authored error string: the state machine owns the fixed, tenant-safe
// failure reason and callback message. Reply must be buffered by one, matching
// deprovisionMsg, so recovery can wait until both state mutation and durable
// lifecycle callback enqueue have completed.
type cohortDivergedMsg struct {
	Ctx     context.Context
	Runtime shared.RuntimeGenerationProof
	Reply   chan error
}

func (cohortDivergedMsg) isleaseMessage()            {}
func (cohortDivergedMsg) isactorObservationMessage() {}
func (cohortDivergedMsg) doneChan() chan struct{}    { return nil }
func (m cohortDivergedMsg) onStaleGeneration() {
	select {
	case m.Reply <- nil:
	default:
	}
}
func (m cohortDivergedMsg) onPanic(err error) {
	select {
	case m.Reply <- err:
	default:
	}
}

func NewCohortDivergedObservation(
	ctx context.Context,
	runtime shared.RuntimeGenerationProof,
) (ActorObservation, ActorReply, error) {
	if ctx == nil {
		return ActorObservation{}, ActorReply{}, errors.New("cohort divergence context is required")
	}
	if !runtime.Valid() {
		return ActorObservation{}, ActorReply{}, errors.New("cohort divergence observation requires an exact runtime generation")
	}
	reply, receiver := newActorReply()
	return newActorObservation(cohortDivergedMsg{Ctx: ctx, Runtime: runtime, Reply: reply}), receiver, nil
}

// maintenanceRecoveryOutcome is deliberately private. Docker recovery can
// construct only one of the four valid messages below, so an invalid
// success/failure/projection combination cannot cross the actor boundary.
type maintenanceRecoveryOutcome uint8

const (
	maintenanceRecoveredSuccess maintenanceRecoveryOutcome = iota + 1
	maintenanceRecoveredFailureReady
	maintenanceRecoveredFailureFailed
	maintenanceRecoveredSuccessRuntimeFailed
)

// MaintenanceRecoveryProjection contains only substrate observations. Runtime
// identity, routes, desired items and manifest are deliberately absent: the
// typed constructors derive those authorities from claim.TargetRelease(), so a
// caller cannot redirect or rewrite a durable target through actor recovery.
type MaintenanceRecoveryProjection struct {
	ContainerIDs      []string
	ServiceContainers map[string][]string
}

// maintenanceRecoveredMsg converges a stale Restarting/Updating actor after
// the exact durable maintenance Release has already reached a terminal state.
// Its fields are private so callers cannot forge an outcome or bypass the
// exact intent-generation fence; use one of the typed constructors below.
//
// The actor suppresses callback delivery for this message. Recovery resolves
// the durable MaintenanceIntent only after the projection transition and
// pending-route cleanup succeed, preserving Release -> actor -> outbox order.
type maintenanceRecoveredMsg struct {
	maintenance shared.MaintenanceIntentClaim
	outcome     maintenanceRecoveryOutcome
	success     ReplaceSuccessResult
	failure     ReplaceFailureInfo
	reply       chan error
}

func (maintenanceRecoveredMsg) isleaseMessage()         {}
func (maintenanceRecoveredMsg) doneChan() chan struct{} { return nil }
func (m maintenanceRecoveredMsg) onPanic(err error) {
	select {
	case m.reply <- err:
	default:
	}
}

// NewMaintenanceRecoveredSuccessMsg constructs the only recovery message that
// may promote the pending target route and workload projection.
func NewMaintenanceRecoveredSuccessMsg(
	active shared.MaintenanceReleaseActive,
	projection MaintenanceRecoveryProjection,
) (RecoveryCommand, ActorReply, error) {
	result, err := validateActiveMaintenanceRecoveryProjection(active, projection)
	if err != nil {
		return RecoveryCommand{}, ActorReply{}, err
	}
	claim := active.Intent()
	reply, receiver := newActorReply()
	return newRecoveryCommand(maintenanceRecoveredMsg{
		maintenance: claim,
		outcome:     maintenanceRecoveredSuccess,
		success:     result,
		reply:       reply,
	}), receiver, nil
}

// NewMaintenanceRecoveredFailureReadyMsg constructs a failed-maintenance
// recovery whose exact source cohort was independently proven Ready.
func NewMaintenanceRecoveredFailureReadyMsg(
	claim shared.MaintenanceIntentClaim,
	projection MaintenanceRecoveryProjection,
	info ReplaceFailureInfo,
) (RecoveryCommand, ActorReply, error) {
	return newMaintenanceRecoveredFailureMsg(
		claim, maintenanceRecoveredFailureReady, projection, info,
	)
}

// NewMaintenanceRecoveredFailureFailedMsg constructs a failed-maintenance
// recovery for every case where an exact Ready source cohort was not proven.
func NewMaintenanceRecoveredFailureFailedMsg(
	claim shared.MaintenanceIntentClaim,
	projection MaintenanceRecoveryProjection,
	info ReplaceFailureInfo,
) (RecoveryCommand, ActorReply, error) {
	return newMaintenanceRecoveredFailureMsg(
		claim, maintenanceRecoveredFailureFailed, projection, info,
	)
}

// NewMaintenanceRecoveredRuntimeFailureMsg constructs the compound recovery
// outcome for an exact committed target whose runtime cohort is definitively
// divergent. Applying target authority and failing the projection is one actor
// transition, so a deferred outbox settlement cannot oscillate Ready/Failed or
// replay ordinary transition side effects.
func NewMaintenanceRecoveredRuntimeFailureMsg(
	active shared.MaintenanceReleaseActive,
	projection MaintenanceRecoveryProjection,
) (RecoveryCommand, ActorReply, error) {
	result, err := validateActiveMaintenanceRecoveryProjection(active, projection)
	if err != nil {
		return RecoveryCommand{}, ActorReply{}, err
	}
	claim := active.Intent()
	reply, receiver := newActorReply()
	return newRecoveryCommand(maintenanceRecoveredMsg{
		maintenance: claim,
		outcome:     maintenanceRecoveredSuccessRuntimeFailed,
		success:     result,
		failure: ReplaceFailureInfo{
			operation:     string(claim.Kind()),
			callbackErr:   ErrMsgCohortDiverged,
			reason:        backend.ReasonInternal,
			lastError:     ErrMsgCohortDiverged,
			authorityKind: replaceAuthorityRecovery,
			maintenance:   claim,
		},
		reply: reply,
	}), receiver, nil
}

func newMaintenanceRecoveredFailureMsg(
	claim shared.MaintenanceIntentClaim,
	outcome maintenanceRecoveryOutcome,
	projection MaintenanceRecoveryProjection,
	info ReplaceFailureInfo,
) (RecoveryCommand, ActorReply, error) {
	result, err := validateMaintenanceFailureRecoveryProjection(claim, projection)
	if err != nil {
		return RecoveryCommand{}, ActorReply{}, err
	}
	if info.authorityKind != replaceAuthorityRecovery ||
		!info.maintenance.MatchesIntent(claim) {
		return RecoveryCommand{}, ActorReply{}, errors.New(
			"maintenance recovery failure details belong to another intent",
		)
	}
	reply, receiver := newActorReply()
	info.preserveMaintenance = true
	return newRecoveryCommand(maintenanceRecoveredMsg{
		maintenance: claim,
		outcome:     outcome,
		success:     result,
		failure:     info,
		reply:       reply,
	}), receiver, nil
}

func validateMaintenanceFailureRecoveryProjection(
	claim shared.MaintenanceIntentClaim,
	projection MaintenanceRecoveryProjection,
) (ReplaceSuccessResult, error) {
	if !claim.Valid() {
		return ReplaceSuccessResult{}, errors.New("maintenance recovery requires an exact intent claim")
	}
	serviceContainers := make(map[string][]string, len(projection.ServiceContainers))
	for service, ids := range projection.ServiceContainers {
		serviceContainers[service] = append([]string(nil), ids...)
	}
	result := ReplaceSuccessResult{
		containerIDs:                  append([]string(nil), projection.ContainerIDs...),
		serviceContainers:             serviceContainers,
		authorityKind:                 replaceAuthorityRecovery,
		maintenance:                   claim,
		suppressMaintenanceSettlement: true,
	}
	return result, nil
}

// validateActiveMaintenanceRecoveryProjection binds an observed cohort to the
// exact committed target generation. A bare intent contains a pre-append
// Version=0 target and therefore cannot authorize a live projection update.
func validateActiveMaintenanceRecoveryProjection(
	active shared.MaintenanceReleaseActive,
	projection MaintenanceRecoveryProjection,
) (ReplaceSuccessResult, error) {
	if !active.Valid() {
		return ReplaceSuccessResult{}, errors.New("maintenance recovery requires an exact active release")
	}
	claim := active.Intent()
	target, ok := active.TargetRelease()
	if !ok || target.Version <= 0 {
		return ReplaceSuccessResult{}, errors.New("maintenance recovery active release has no exact target")
	}
	stack, err := validateCompleteReleaseProjection(
		target, projection.ContainerIDs, projection.ServiceContainers,
	)
	if err != nil {
		return ReplaceSuccessResult{}, fmt.Errorf("maintenance recovery target projection: %w", err)
	}
	authority, ok := target.RuntimeIdentity()
	if !ok {
		return ReplaceSuccessResult{}, errors.New("maintenance recovery target has no runtime authority")
	}
	result := newReplaceSuccessProjection(ReplaceSuccessProjection(projection))
	result.authorityKind = replaceAuthorityRecovery
	result.maintenance = claim
	result.maintenanceRelease = active
	result.suppressMaintenanceSettlement = true
	result.release = &target
	result.stackManifest = stack
	result.applyRecoveredRuntimeAuthority = true
	result.recoveredCallbackURL = authority.CallbackURL()
	result.recoveredLifecycleCallbackURL = authority.LifecycleCallbackURL()
	return result, nil
}

// diagGatheredMsg is sent by the async diag goroutine when it finishes.
// Carries the gather output into the Failing→Failed transition.
type diagGatheredMsg struct {
	result diagResult
}

func (diagGatheredMsg) isleaseMessage()          {}
func (diagGatheredMsg) doneChan() chan struct{}  { return nil }
func (diagGatheredMsg) onPanic(error)            {} // no caller to unblock
func (diagGatheredMsg) isWorkerTerminalMessage() {}

// provisionRequestedMsg asks the actor to drive a provision flow. Carries
// the Cancel func (stored on the actor so Provisioning.OnExit can preempt)
// and a Work closure containing everything doProvision needs — the actor
// doesn't need to know the arg shapes. After firing the SM
// transition, the actor sends the Fire result on Ack and spawns a worker
// (tracked by workers) to run Work. Backend.Provision blocks on Ack so it
// knows whether the SM accepted the transition before returning. Only the
// validating NewProvisionCommand constructor can create one.
type provisionRequestedMsg struct {
	Ctx       context.Context
	Ack       chan error
	Operation shared.OperationIntentClaim
}

func (provisionRequestedMsg) isleaseMessage()         {}
func (provisionRequestedMsg) isactorCommandMessage()  {}
func (provisionRequestedMsg) doneChan() chan struct{} { return nil }
func (m provisionRequestedMsg) onPanic(err error) {
	select {
	case m.Ack <- err:
	default:
	}
}

func NewProvisionCommand(
	ctx context.Context,
	operation shared.OperationIntentClaim,
) (ActorCommand, ActorReply, error) {
	if ctx == nil {
		return ActorCommand{}, ActorReply{}, errors.New("provision context is required")
	}
	if !operation.Valid() || operation.Kind() != shared.OperationIntentProvision {
		return ActorCommand{}, ActorReply{}, errors.New("provision requires an exact pending operation")
	}
	ack, receiver := newActorReply()
	return newActorCommand(provisionRequestedMsg{Ctx: ctx, Ack: ack, Operation: operation}), receiver, nil
}

// provisionCompletedMsg is sent by the doProvision goroutine on success.
// Drives the Provisioning→Ready transition. Carries the result data
// (containerIDs, manifest, stackManifest, serviceContainers) that the
// Ready entry action writes into the provision struct.
type provisionCompletedMsg struct {
	result ProvisionSuccessResult
}

func (provisionCompletedMsg) isleaseMessage()          {}
func (provisionCompletedMsg) doneChan() chan struct{}  { return nil }
func (provisionCompletedMsg) onPanic(error)            {} // no caller to unblock
func (provisionCompletedMsg) isWorkerTerminalMessage() {}

// provisionErroredMsg is sent by the doProvision goroutine on failure.
// callbackErr is the hardcoded on-chain-safe message; lastError is the
// full diagnostic string (from err.Error()) that the Failed entry
// action writes into provision.LastError. logs is the pre-captured
// container-log map (fetched BEFORE the cleanup defer removed the
// failed containers) so persistDiagnostics doesn't attempt to re-fetch
// from already-deleted containers — see doProvision's captureContainerLogs
// call.
type provisionErroredMsg struct {
	callbackErr      string
	reason           backend.Reason // ENG-508
	lastError        string
	logs             map[string]string
	operationFailure shared.OperationReleaseUncommitted
}

func (provisionErroredMsg) isleaseMessage()          {}
func (provisionErroredMsg) doneChan() chan struct{}  { return nil }
func (provisionErroredMsg) onPanic(error)            {} // no caller to unblock
func (provisionErroredMsg) isWorkerTerminalMessage() {}

// operationAmbiguousMsg deliberately performs no FSM transition and publishes
// no callback. Its arrival on the actor inbox is nevertheless required: it is
// the ordered worker-to-actor handoff which makes the worker quiescent before
// periodic durable-intent recovery may classify the substrate.
type operationAmbiguousMsg struct {
	kind          string
	operationID   shared.OperationID
	maintenanceID shared.MaintenanceID
	err           error
}

func (operationAmbiguousMsg) isleaseMessage()          {}
func (operationAmbiguousMsg) doneChan() chan struct{}  { return nil }
func (operationAmbiguousMsg) onPanic(error)            {}
func (operationAmbiguousMsg) isWorkerTerminalMessage() {}

// restartRequestedMsg and updateRequestedMsg carry a Cancel func + Work
// closure + Ack chan, analogous to provisionRequestedMsg. The Work closure
// returns a ReplaceResult consumed by the actor to pick the right terminal
// SM event (completed / recovered / failed). Their validating constructors
// derive maintenance identity and callback authority from the exact target.
type restartRequestedMsg struct {
	Ctx    context.Context
	Ack    chan error
	Target shared.MaintenanceReleaseClaim
	// The callback pair remains pending until replacement succeeds. New
	// containers and the completion callback use it immediately, but failed
	// maintenance leaves the committed runtime pair unchanged.
	CallbackURL          string
	LifecycleCallbackURL string
	Maintenance          shared.MaintenanceIntentClaim
}

func (restartRequestedMsg) isleaseMessage()         {}
func (restartRequestedMsg) isactorCommandMessage()  {}
func (restartRequestedMsg) doneChan() chan struct{} { return nil }
func (m restartRequestedMsg) onPanic(err error) {
	select {
	case m.Ack <- err:
	default:
	}
}

type updateRequestedMsg struct {
	Ctx    context.Context
	Ack    chan error
	Target shared.MaintenanceReleaseClaim
	// The callback pair remains pending until replacement succeeds. New
	// containers and the completion callback use it immediately, but failed
	// maintenance leaves the committed runtime pair unchanged.
	CallbackURL          string
	LifecycleCallbackURL string
	Maintenance          shared.MaintenanceIntentClaim
}

func (updateRequestedMsg) isleaseMessage()         {}
func (updateRequestedMsg) isactorCommandMessage()  {}
func (updateRequestedMsg) doneChan() chan struct{} { return nil }
func (m updateRequestedMsg) onPanic(err error) {
	select {
	case m.Ack <- err:
	default:
	}
}

func newMaintenanceCommand(
	kind shared.MaintenanceIntentKind,
	ctx context.Context,
	target shared.MaintenanceReleaseClaim,
) (context.Context, shared.MaintenanceReleaseClaim, shared.MaintenanceIntentClaim, chan error, ActorReply, error) {
	if ctx == nil {
		return nil, shared.MaintenanceReleaseClaim{}, shared.MaintenanceIntentClaim{}, nil, ActorReply{}, errors.New("maintenance context is required")
	}
	if !target.Valid() {
		return nil, shared.MaintenanceReleaseClaim{}, shared.MaintenanceIntentClaim{}, nil, ActorReply{}, errors.New("maintenance command requires an exact bound target release")
	}
	maintenance := target.Intent()
	if !maintenance.Valid() || maintenance.Kind() != kind || maintenance.MaintenanceID() != target.MaintenanceID() {
		return nil, shared.MaintenanceReleaseClaim{}, shared.MaintenanceIntentClaim{}, nil, ActorReply{}, errors.New("maintenance command target differs from intent")
	}
	ack, receiver := newActorReply()
	return ctx, target, maintenance, ack, receiver, nil
}

func newRestartCommand(
	kind shared.MaintenanceIntentKind,
	ctx context.Context,
	target shared.MaintenanceReleaseClaim,
) (ActorCommand, ActorReply, error) {
	ctx, target, maintenance, ack, receiver, err := newMaintenanceCommand(kind, ctx, target)
	if err != nil {
		return ActorCommand{}, ActorReply{}, err
	}
	return newActorCommand(restartRequestedMsg{Ctx: ctx, Target: target, Ack: ack,
		CallbackURL: maintenance.CallbackURL(), LifecycleCallbackURL: maintenance.LifecycleCallbackURL(), Maintenance: maintenance}), receiver, nil
}

func NewRestartCommand(ctx context.Context, target shared.MaintenanceReleaseClaim) (ActorCommand, ActorReply, error) {
	return newRestartCommand(shared.MaintenanceIntentRestart, ctx, target)
}

// NewCustomDomainCommand preserves the restart-shaped actor transition while
// requiring a target minted from custom-domain maintenance authority. Keeping
// this constructor distinct prevents either caller from relabeling a generic
// restart command with the wrong durable intent kind.
func NewCustomDomainCommand(ctx context.Context, target shared.MaintenanceReleaseClaim) (ActorCommand, ActorReply, error) {
	return newRestartCommand(shared.MaintenanceIntentCustomDomain, ctx, target)
}

func NewUpdateCommand(ctx context.Context, target shared.MaintenanceReleaseClaim) (ActorCommand, ActorReply, error) {
	ctx, target, maintenance, ack, receiver, err := newMaintenanceCommand(shared.MaintenanceIntentUpdate, ctx, target)
	if err != nil {
		return ActorCommand{}, ActorReply{}, err
	}
	return newActorCommand(updateRequestedMsg{Ctx: ctx, Target: target, Ack: ack,
		CallbackURL: maintenance.CallbackURL(), LifecycleCallbackURL: maintenance.LifecycleCallbackURL(), Maintenance: maintenance}), receiver, nil
}

// restoreRequestedMsg drives a restore (ENG-325) through the existing
// replace machinery — same Cancel/Work/Ack/callback-pair shape as
// restartRequestedMsg. The difference is purely the SM event it fires:
// evRestoreRequested, which is permitted only from Provisioning (a
// restore's new lease is reserved there), versus evRestartRequested
// from Ready/Failed. The Work closure returns a ReplaceResult consumed
// by spawnReplaceWorker exactly as for restart/update. Only NewRestoreCommand
// can create one, from exact restore operation authority.
type restoreRequestedMsg struct {
	Ctx context.Context
	Ack chan error
	// The callback pair is applied to the provision state by
	// onEnterRestarting (reused for restore) before the actor acks — the
	// actor, not the HTTP prelude, is the sole writer of those fields.
	CallbackURL          string
	LifecycleCallbackURL string
	Operation            shared.OperationIntentClaim
}

func (restoreRequestedMsg) isleaseMessage()         {}
func (restoreRequestedMsg) isactorCommandMessage()  {}
func (restoreRequestedMsg) doneChan() chan struct{} { return nil }
func (m restoreRequestedMsg) onPanic(err error) {
	select {
	case m.Ack <- err:
	default:
	}
}

func NewRestoreCommand(
	ctx context.Context,
	operation shared.OperationIntentClaim,
) (ActorCommand, ActorReply, error) {
	if ctx == nil {
		return ActorCommand{}, ActorReply{}, errors.New("restore context is required")
	}
	if !operation.Valid() || operation.Kind() != shared.OperationIntentRestore {
		return ActorCommand{}, ActorReply{}, errors.New("restore requires an exact pending operation")
	}
	ack, receiver := newActorReply()
	return newActorCommand(restoreRequestedMsg{Ctx: ctx, Ack: ack,
		CallbackURL: operation.CallbackURL(), LifecycleCallbackURL: operation.LifecycleCallbackURL(), Operation: operation}), receiver, nil
}

// replaceCompletedMsg / replaceRecoveredMsg / replaceFailedMsg fire the
// Restarting|Updating exit transition after the goroutine finishes. The
// goroutine picks which to send based on (err == nil, final Status):
//
//	err == nil                → replaceCompletedMsg  → Ready (Success)
//	err != nil, Status=Ready  → replaceRecoveredMsg  → Ready (Failed+suffix)
//	err != nil, Status=Failed → replaceFailedMsg     → Failed (Failed)
//
// Both replaceRecoveredMsg and replaceFailedMsg carry the callbackErr
// string that the SM entry action emits verbatim.
type replaceCompletedMsg struct {
	result        ReplaceSuccessResult
	maintenanceID shared.MaintenanceID
}

func (replaceCompletedMsg) isleaseMessage()          {}
func (replaceCompletedMsg) doneChan() chan struct{}  { return nil }
func (replaceCompletedMsg) onPanic(error)            {} // no caller to unblock
func (replaceCompletedMsg) isWorkerTerminalMessage() {}

type replaceRecoveredMsg struct {
	info          ReplaceFailureInfo
	maintenanceID shared.MaintenanceID
}

func (replaceRecoveredMsg) isleaseMessage()          {}
func (replaceRecoveredMsg) doneChan() chan struct{}  { return nil }
func (replaceRecoveredMsg) onPanic(error)            {} // no caller to unblock
func (replaceRecoveredMsg) isWorkerTerminalMessage() {}

type replaceFailedMsg struct {
	info          ReplaceFailureInfo
	maintenanceID shared.MaintenanceID
}

func (replaceFailedMsg) isleaseMessage()          {}
func (replaceFailedMsg) doneChan() chan struct{}  { return nil }
func (replaceFailedMsg) onPanic(error)            {} // no caller to unblock
func (replaceFailedMsg) isWorkerTerminalMessage() {}

// LeaseActor owns all state transitions and async work for a single lease.
//
// One concept: the actor is the scope of atomicity for its messages and
// its workers. Everything else falls out of that:
//
//   - Registry atomicity — the substrate's actor registry (e.g. docker's
//     b.actors) is guarded by the substrate's mutex; routeToLease resolves-
//     or-creates AND enqueues under that mutex; removeFromRegistry (on
//     actor exit, via cfg.OnTerminated) deletes under the same mutex.
//     External callers never hold a *LeaseActor pointer, so stale-
//     pointer races are unreachable by construction.
//
//   - Worker ownership — every worker goroutine (provision, restart,
//     update, diag) is spawned by the actor (via spawnProvisionWorker /
//     spawnReplaceWorker / onEnterFailing's goroutine) and tracked by
//     workers. Retirement stops external admission, waits for workers, and
//     applies their terminal events before registry deletion. The wait is
//     bounded by WorkerDrainTimeout. A timed-out state transition is refused
//     before substrate teardown; a timed-out backend shutdown is surfaced by
//     Backend.Stop and the process exits uncleanly.
//
//   - Typed retirement drain — worker-terminal messages still drive their SM
//     transitions, while queued caller commands are rejected and unblocked.
//     The workerTerminalMessage marker prevents a future change from accidentally
//     spawning new work after the retirement barrier.
//
//   - Non-blocking routing — routeToLease does a non-blocking send
//     under the registry mutex. A wedged actor cannot stall the
//     event-loop or other routing callers; refusals are counted in
//     die_event_dropped_total and the reconciler re-detects.
//
// Messages are processed serially from inbox, so handlers never race
// with themselves. SM transitions are synchronous inside handle() —
// Fire dispatches OnExit / OnEntry in the actor's own goroutine.
//
// Exported so substrate packages can construct one from validated,
// construction-bound dependencies and hold its pointer in their registries.
type LeaseActor struct {
	leaseUUID string
	// cfg holds the substrate-agnostic interfaces and fixed handlers consumed by
	// the SM/actor. NewLeaseActor validates and freezes them; commands cannot
	// supply executable behavior. The actor holds no substrate pointer of its
	// own.
	cfg   LeaseActorConfig
	inbox chan leaseMessage
	done  chan struct{}
	sm    *leaseSM
	// pendingDeathInfo carries the inspected instance state from the SM's
	// guard into the onEnterFailing action. Single-field handoff works
	// because the actor processes messages serially; no two messages
	// read/write this field concurrently. The service name for the
	// death-event log line rides on pendingDeathInfo.ServiceName.
	pendingDeathInfo *InstanceState
	// diagCancel is set when entering Failing (spawning the async diag
	// goroutine) and called by Failing.OnExit to signal cancellation. Any
	// transition out of Failing cancels the goroutine before a stale Failed
	// callback can be emitted.
	diagCancel context.CancelFunc
	// workCancel is set when a Provision/Restart/Update worker is spawned
	// from handleProvisionRequested / handleRestartRequested /
	// handleUpdateRequested, and called by Provisioning/Restarting/
	// Updating.OnExit on DeprovisionRequested preemption.
	workCancel context.CancelFunc
	// replaceWasActive records whether the lease was counted in
	// activeProvisions (i.e. Status == Ready) at the instant a restart/update
	// transition began — captured by onEnterRestarting/onEnterUpdating reading
	// prov.Status BEFORE overwriting it. The replace-outcome entry actions key
	// the activeProvisions gauge on THIS actor-observed (serial) value instead
	// of a prelude-captured route-time status snapshot, which is stale when an
	// intervening Ready→Failing already Dec'd the gauge (the death-before-queued-restart
	// ordering, ENG-230 / PR#93 finding #2). Single-field handoff is safe: the
	// SM permits at most one in-flight replace at a time and the actor is
	// serial — same discipline as pendingDeathInfo.
	replaceWasActive bool
	// replaceCallbackKind distinguishes provision/restore operation completion
	// from restart/update lifecycle observation. It is set by the serial actor
	// with the replace entry transition and consumed by the terminal entry
	// action, so callback classification never depends on URL inspection.
	replaceCallbackKind replaceCallbackKind
	// pendingReplaceCallbackURL and pendingReplaceLifecycleCallbackURL are the
	// requested maintenance route. Restart/update keep this pair separate from
	// ProvisionState until the substrate and active Release commit, so rollback
	// continues to emit the old committed runtime labels by construction.
	pendingReplaceCallbackURL          string
	pendingReplaceLifecycleCallbackURL string
	pendingMaintenance                 shared.MaintenanceIntentClaim
	maintenanceWorkerMu                sync.RWMutex
	maintenanceWorkerID                shared.MaintenanceID
	// workers tracks every worker goroutine spawned by this actor
	// (provision, restart, update, diag). The actor's retirement path stops
	// caller admission, waits on workers.Zero(), then applies the queued worker
	// terminal event before unregistering. The wait is bounded by
	// WorkerDrainTimeout; a transition timeout aborts before the destination
	// state or substrate teardown can commit.
	// The same barrier is used by onExit* to wait for the
	// active worker when the SM is transitioning out of a work-owning
	// state (Deprovision preempt).
	//
	// The SM enforces at-most-one-worker-at-a-time across the work-owning
	// states, so workers.Zero effectively waits for "the one worker
	// currently running"; the count happens to always be 0 or 1.
	workers *workbarrier.Barrier
	// activity counts accepted/handling messages and workers. The actor overlaps
	// the count across message->worker and worker->terminal-message hand-offs, so
	// zero is a real quiescence proof rather than three racy snapshots. activityMu
	// serializes count transitions with TryAcquireQuiescence; a recovery owner that
	// claims zero holds the mutex and prevents any new actor mutation until release.
	activityMu sync.Mutex
	activity   int64
	// currentMessageStart is the UnixNano timestamp of the message the
	// actor is currently processing in handle(), or 0 when idle. Used by
	// the stuck-actor sampler to detect hung handlers. Written by the
	// actor's goroutine, read atomically by the sampler goroutine.
	currentMessageStart atomic.Int64
	// terminated is set by handleDeprovision once the provision entry has
	// been fully removed. The run loop checks it after every handle() and
	// exits, allowing the actor's slot in the registry to be reused by a
	// new lease with the same UUID (a stale actor stuck in Deprovisioning
	// would otherwise Ignore evProvisionRequested and wedge the lease).
	// Read and written only on the actor's own goroutine — no atomic.
	terminated bool
	// retirementRequested lets an exclusive quiescence capability retire an
	// idle actor after recovery removes its projection. The claim marks retiring
	// while it still owns admissionMu, so no external message can enter between
	// registry-reservation release and the run loop observing this signal.
	retirementRequested     chan struct{}
	retirementRequestedOnce sync.Once
	// terminalAdmissionMu linearizes worker terminal sends with the final
	// retirement drain. Retirement closes admission while holding this gate;
	// therefore every accepted terminal message is already in the inbox before
	// the drain begins, and every later sender is refused. This ordering proof is
	// stronger and smaller than observing a closing channel before and after a
	// racy channel send.
	terminalAdmissionMu     sync.Mutex
	terminalAdmissionClosed bool
	// admissionMu linearizes external TryEnqueue calls with retirement. The
	// substrate holds its registry mutex while calling TryEnqueue, but the actor
	// cannot take that registry mutex while beginning retirement without a lock
	// inversion. This private gate lets retirement stop admission first, release
	// the gate, and only later invoke OnTerminated to remove the actor.
	admissionMu sync.Mutex
	retiring    bool
}

// Bounded inbox: admission refuses a full inbox so substrate event bursts
// cannot grow memory or block while holding the actor-registry lock.
const leaseActorInboxSize = 16

// NewLeaseActor validates all composition capabilities before allocating or
// spawning an actor. OnTerminated receives the completed actor explicitly, so
// construction never leaks a half-built pointer through a config callback.
//
// CRITICAL: install is infallible and runs after validation/initialization but
// before the run goroutine starts. The caller must hold its registry lock for
// this entire call. Consequently an already-canceled StopCtx can retire only
// an actor which is already registered, and a concurrent resolver observes
// either that registered generation or no generation at all.
//
// Construction order:
//  1. Allocate the actor with all per-actor primitives (inbox, done,
//     exiting, workers).
//  2. Store the already-validated LeaseActorConfig on the actor.
//  3. Lift LeaseUUID off the config onto the actor struct (it's hot enough
//     to merit avoiding the cfg dereference on every message).
//  4. Initialize the SM eagerly so any external reader (DebugActors over
//     /debug/actors) sees a non-nil pointer without synchronization on
//     a lazy-init field.
//  5. Invoke the infallible registry installer while the actor is inert.
//  6. Record one ActorCreated metric event — moved here from the
//     substrate's resolve-or-create function so a single SMMetrics
//     adapter sees every actor construction regardless of substrate.
//  7. Spawn the actor's goroutine via cfg.WG.Go(a.run) — moved here for
//     the same "one place creates, one place spawns" reason. Substrate
//     code calls NewLeaseActor while holding its registry mutex, so the
//     resolve-or-create-then-enqueue invariant is preserved (spawn
//     happens before NewLeaseActor returns, before the substrate
//     releases its mutex).
func NewLeaseActor(cfg LeaseActorConfig, install func(*LeaseActor)) (*LeaseActor, error) {
	if err := validateLeaseActorConfig(cfg); err != nil {
		return nil, err
	}
	if install == nil {
		return nil, errors.New("lease actor requires an atomic registry installer")
	}
	a := &LeaseActor{
		inbox:               make(chan leaseMessage, leaseActorInboxSize),
		done:                make(chan struct{}),
		retirementRequested: make(chan struct{}),
		workers:             workbarrier.New(),
	}
	a.cfg = cfg
	a.leaseUUID = a.cfg.LeaseUUID
	a.sm = newLeaseSM(a)
	// Installation precedes the run loop by construction. An already-canceled
	// StopCtx may retire immediately after Go, but OnTerminated will necessarily
	// observe the installed actor and can remove it instead of racing a later
	// caller-side insertion.
	install(a)
	a.cfg.Metrics.ActorCreated()
	a.cfg.WG.Go(a.run)
	return a, nil
}

func validateLeaseActorConfig(cfg LeaseActorConfig) error {
	switch {
	case cfg.LeaseUUID == "":
		return errors.New("lease actor requires a lease identity")
	case cfg.Logger == nil:
		return errors.New("lease actor requires a logger")
	case cfg.StopCtx == nil:
		return errors.New("lease actor requires a stop context")
	case cfg.WG == nil:
		return errors.New("lease actor requires a worker group")
	case nilActorCapability(cfg.Inspector):
		return errors.New("lease actor requires an instance inspector")
	case nilActorCapability(cfg.Diag):
		return errors.New("lease actor requires a diagnostics gatherer")
	case nilActorCapability(cfg.ProvisionStore):
		return errors.New("lease actor requires a provision store")
	case nilActorCapability(cfg.Metrics):
		return errors.New("lease actor requires metrics")
	case cfg.OnTerminated == nil || cfg.PersistDiagnosticsFn == nil ||
		cfg.PersistDiagnosticsWithLogsFn == nil || cfg.SendOperationSuccessFn == nil ||
		cfg.SendOperationFailureFn == nil || cfg.SendLifecycleFailureFn == nil ||
		cfg.SendMaintenanceSuccessFn == nil || cfg.SendMaintenanceFailureFn == nil ||
		cfg.ProvisionWorkFn == nil || cfg.RestoreWorkFn == nil ||
		cfg.MaintenanceWorkFn == nil || cfg.DoDeprovisionFn == nil:
		return errors.New("lease actor requires all handler capabilities")
	case cfg.RecoveryLineage == (shared.RecoveryLineage{}):
		return errors.New("lease actor requires actor-owned close authority")
	case cfg.WorkerDrainTimeout < 0:
		return errors.New("lease actor worker drain timeout cannot be negative")
	default:
		return nil
	}
}

func nilActorCapability(capability any) bool {
	if capability == nil {
		return true
	}
	value := reflect.ValueOf(capability)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (a *LeaseActor) run() {
	defer a.clearAnyMaintenanceWorker()
	defer a.retire()
	for {
		if a.terminated {
			return
		}
		select {
		case msg := <-a.inbox:
			a.handleAcceptedMessage(msg)
		case <-a.cfg.StopCtx.Done():
			// Shutdown: exit the main loop. retire stops caller admission,
			// waits for in-flight workers, and applies their terminal events
			// before the actor is torn down.
			return
		case <-a.retirementRequested:
			return
		}
	}
}

// retire makes actor replacement and accepted-work settlement one ordered
// operation:
//
//  1. close external admission while this actor is still registered;
//  2. reject commands accepted before the gate closed, while applying any
//     already-queued worker terminal events;
//  3. wait for the actor-owned worker, whose terminal send precedes Done;
//  4. close terminal admission and drain that final terminal event;
//  5. unregister the now-quiescent actor and close Done.
//
// The first drain is necessary even though the inbox is bounded: without it,
// accepted caller commands could fill the inbox while run is waiting for a
// worker, preventing that worker from publishing the terminal event on which
// state convergence depends. Neither drain executes caller work, so no worker
// can be spawned after the sole worker barrier.
func (a *LeaseActor) retire() {
	a.stopAdmission()
	a.drainRetiringInbox()
	_ = a.waitForWorkers()
	a.closeTerminalAdmission()
	a.drainRetiringInbox()
	a.removeFromRegistry()
	close(a.done)
}

// stopAdmission linearizes with TryEnqueue. It deliberately does not invoke
// OnTerminated while holding admissionMu: routing calls TryEnqueue under the
// substrate registry mutex, so doing so would invert that lock order.
func (a *LeaseActor) stopAdmission() {
	a.admissionMu.Lock()
	a.retiring = true
	a.admissionMu.Unlock()
}

// waitForWorkers blocks until every worker goroutine this actor has
// spawned has returned. Bounded by workExitWaitTimeout so a wedged
// worker (Docker daemon hang with ctx ignored) can't pin the actor.
// If the timeout fires the error aborts an SM transition before its state
// changes. This is load-bearing for deprovision: teardown is refused while a
// prior worker may still write. The actor run-loop's shutdown defer ignores the
// same error after logging; its backend-level WaitGroup remains non-zero, so
// Backend.Stop's independent deadline forces a process-level failure instead of
// claiming a clean shutdown. workbarrier.Barrier's Zero() is a real channel, so
// the select here spawns no helper goroutine.
func (a *LeaseActor) waitForWorkers() error {
	timeout := a.cfg.WorkerDrainTimeout
	if timeout <= 0 {
		timeout = workExitWaitTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-a.workers.Zero():
		return nil
	case <-timer.C:
		a.cfg.Logger.Warn("actor waitForWorkers: worker did not exit within timeout",
			"lease_uuid", a.leaseUUID,
			"timeout", timeout,
		)
		return fmt.Errorf("%w: lease %s after %s", ErrWorkerDrainTimeout, a.leaseUUID, timeout)
	}
}

// drainRetiringInbox settles worker terminal events but explicitly rejects
// caller-facing messages that were accepted immediately before retirement.
// Running normal handlers here would permit a queued Provision/Restart/Update
// to spawn a worker after waitForWorkers, and unregistering before the drain
// would permit a replacement actor to mutate the same lease concurrently.
func (a *LeaseActor) drainRetiringInbox() {
	for {
		select {
		case msg := <-a.inbox:
			if terminal, ok := msg.(workerTerminalMessage); ok {
				a.handleAcceptedMessage(terminal)
				continue
			}
			a.rejectAcceptedMessage(msg)
		default:
			return
		}
	}
}

func (a *LeaseActor) handleAcceptedMessage(msg leaseMessage) {
	defer a.endActivity()
	a.handle(msg)
}

func (a *LeaseActor) rejectAcceptedMessage(msg leaseMessage) {
	defer a.endActivity()
	a.rejectRetiringMessage(msg)
}

func (a *LeaseActor) endActivity() {
	a.activityMu.Lock()
	a.activity--
	a.activityMu.Unlock()
}

func (a *LeaseActor) beginWorkerActivity() {
	a.activityMu.Lock()
	a.activity++
	a.workers.Add()
	a.activityMu.Unlock()
}

func (a *LeaseActor) endWorkerActivity() {
	// Keep activity non-zero until after the worker barrier is released. The
	// terminal message was counted before sendTerminal returned, so the two
	// activity intervals overlap on the normal hand-off.
	a.workers.Done()
	a.endActivity()
}

func (a *LeaseActor) rejectRetiringMessage(msg leaseMessage) {
	msg.onPanic(errActorTerminated)
	if ch := msg.doneChan(); ch != nil {
		close(ch)
	}
}

func (a *LeaseActor) handle(msg leaseMessage) {
	a.currentMessageStart.Store(time.Now().UnixNano())
	defer a.currentMessageStart.Store(0)
	defer func() {
		if ch := msg.doneChan(); ch != nil {
			close(ch)
		}
	}()
	// Contain the blast radius of a handler panic to a single message:
	// log with stack, bump a counter, and let the actor keep processing.
	// Without this, a panic in an SM entry action or handler kills the
	// actor goroutine, leaving the actor in b.actors with a full inbox
	// and senders blocking forever.
	defer func() {
		if r := recover(); r != nil {
			a.cfg.Logger.Error("lease actor panic",
				"lease_uuid", a.leaseUUID,
				"panic", r,
				"stack", string(debug.Stack()),
			)
			a.cfg.Metrics.ActorPanic()
			// Unblock any caller waiting on this message's reply/ack
			// channel. Without this, a panic in a handler leaves
			// Backend.Deprovision / Provision / Restart / Update
			// stuck on their select{} until ctx/stopCtx cancels —
			// effectively an API hang per bad message.
			msg.onPanic(fmt.Errorf("handler panic: %v", r))
		}
	}()
	if observation, ok := msg.(actorObservationMessage); ok {
		generation := classifyActorObservation(a.cfg.ProvisionStore, observation)
		if generation != ObservationGenerationCurrent {
			// A same-pointer advance may have been authored by this serial actor, so
			// discard only the old observation. Pointer replacement or absence is the
			// generation boundary: close admission before acknowledging it and retire
			// after this handler returns, so a replacement command resolves an FSM
			// initialized from the replacement projection.
			if generation != ObservationGenerationAdvanced {
				a.stopAdmission()
				a.terminated = true
			}
			observation.onStaleGeneration()
			return
		}
	}
	switch m := msg.(type) {
	case containerDiedMsg:
		a.handleContainerDied(m.ContainerID, m.Runtime)
	case deprovisionMsg:
		m.Reply <- a.handleDeprovision(m.Ctx)
	case cohortDivergedMsg:
		m.Reply <- a.handleCohortDiverged(m.Ctx, m.Runtime)
	case maintenanceRecoveredMsg:
		m.reply <- a.handleMaintenanceRecovered(m)
	case diagGatheredMsg:
		a.handleDiagGathered(m.result)
	case provisionRequestedMsg:
		a.handleProvisionRequested(m)
	case restartRequestedMsg:
		a.handleRestartRequested(m)
	case updateRequestedMsg:
		a.handleUpdateRequested(m)
	case restoreRequestedMsg:
		a.handleRestoreRequested(m)
	case provisionCompletedMsg:
		a.handleProvisionCompleted(m.result)
	case provisionErroredMsg:
		a.handleProvisionErrored(
			m.callbackErr, m.reason, m.lastError, m.logs, m.operationFailure,
		)
	case operationAmbiguousMsg:
		a.handleOperationAmbiguous(m)
	case replaceCompletedMsg:
		a.handleReplaceCompleted(m.result, m.maintenanceID)
	case replaceRecoveredMsg:
		a.handleReplaceRecovered(m.info, m.maintenanceID)
	case replaceFailedMsg:
		a.handleReplaceFailed(m.info, m.maintenanceID)
	default:
		a.cfg.Logger.Warn("lease actor: unknown message type",
			"lease_uuid", a.leaseUUID,
		)
	}
}

func (a *LeaseActor) handleOperationAmbiguous(msg operationAmbiguousMsg) {
	// Ambiguity preserves the non-terminal journal and FSM state, but the live
	// worker is finished. Releasing only this exact generation lets periodic
	// recovery become the sole resolver instead of permanently skipping a
	// maintenance intent whose process-local owner no longer exists.
	a.clearMaintenanceWorker(msg.maintenanceID)
	a.cfg.Logger.Error(
		"substrate mutation outcome is ambiguous; preserving non-terminal state for durable recovery",
		"lease_uuid", a.leaseUUID,
		"operation", msg.kind,
		"operation_fingerprint", msg.operationID.Fingerprint(),
		"error", msg.err,
	)
}

func (a *LeaseActor) handleContainerDied(
	containerID string,
	runtime shared.RuntimeGenerationProof,
) {
	_ = a.sm.containerDied(a.cfg.StopCtx, containerID, runtime)
}

func (a *LeaseActor) handleDiagGathered(result diagResult) {
	// If we're no longer in Failing (e.g., Deprovision preempted), the SM's
	// Ignore declarations on Failed/Deprovisioning drop this event; Fire
	// returns an unhandled-trigger error we can safely discard.
	_ = a.sm.diagnosticsGathered(a.cfg.StopCtx, result)
}

func (a *LeaseActor) handleCohortDiverged(
	ctx context.Context,
	runtime shared.RuntimeGenerationProof,
) error {
	return a.sm.cohortDiverged(ctx, runtime)
}

func (a *LeaseActor) handleMaintenanceRecovered(msg maintenanceRecoveredMsg) error {
	if !msg.maintenance.Valid() {
		return errors.New("maintenance recovery message has no exact identity")
	}
	if msg.maintenance.LeaseUUID() != a.leaseUUID {
		return errors.New("maintenance recovery message belongs to another lease")
	}
	if a.OwnsMaintenance(msg.maintenance.MaintenanceID()) {
		return errors.New("maintenance recovery refused while exact worker remains active")
	}
	if a.pendingMaintenance.Valid() &&
		!msg.maintenance.MatchesIntent(a.pendingMaintenance) {
		return errors.New("maintenance recovery identity differs from actor generation")
	}

	state := a.sm.State()
	desired := backend.ProvisionStatusFailed
	if msg.outcome == maintenanceRecoveredSuccess ||
		msg.outcome == maintenanceRecoveredFailureReady {
		desired = backend.ProvisionStatusReady
	}
	if state == desired {
		// The worker terminal event (or an entry-action panic after the FSM state
		// changed) may have won the inbox race. Reapply the exact durable
		// projection without replaying gauge, diagnostic, fail-count, or callback
		// side effects. This makes settlement retries genuinely idempotent.
		var err error
		switch msg.outcome {
		case maintenanceRecoveredSuccess:
			err = a.sm.applyMaintenanceRecoveredSuccess(msg.success)
		case maintenanceRecoveredFailureReady, maintenanceRecoveredFailureFailed:
			err = a.sm.applyMaintenanceRecoveredFailure(msg.success, msg.failure, desired)
		case maintenanceRecoveredSuccessRuntimeFailed:
			err = a.sm.applyMaintenanceRecoveredRuntimeFailure(msg.success, msg.failure)
		default:
			return errors.New("maintenance recovery message has invalid outcome")
		}
		if err != nil {
			return err
		}
		if a.pendingMaintenance.Valid() {
			a.sm.clearPendingReplaceRoute()
		}
		return nil
	}

	switch state {
	case backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
		if !a.pendingMaintenance.Valid() {
			return errors.New("busy actor has no exact maintenance generation")
		}
	case backend.ProvisionStatusReady, backend.ProvisionStatusFailed:
		// A durable terminal Release outranks a contradictory volatile terminal
		// state (for example Activate committed before a later failed terminal
		// message won the inbox). The dedicated recovery triggers below perform
		// the correction without accepting a stale ordinary worker event.
	default:
		return fmt.Errorf("maintenance recovery cannot converge actor state %s", a.sm.State())
	}
	var err error
	switch msg.outcome {
	case maintenanceRecoveredSuccess:
		err = a.sm.maintenanceRecoveredSuccess(a.cfg.StopCtx, msg.success)
	case maintenanceRecoveredFailureReady:
		err = a.sm.maintenanceRecoveredFailureReady(a.cfg.StopCtx,
			maintenanceRecoveryFailureArgs{projection: msg.success, failure: msg.failure})
	case maintenanceRecoveredFailureFailed:
		err = a.sm.maintenanceRecoveredFailureFailed(a.cfg.StopCtx,
			maintenanceRecoveryFailureArgs{projection: msg.success, failure: msg.failure})
	case maintenanceRecoveredSuccessRuntimeFailed:
		err = a.sm.maintenanceRecoveredRuntimeFailed(a.cfg.StopCtx,
			maintenanceRecoveryFailureArgs{projection: msg.success, failure: msg.failure})
	default:
		return errors.New("maintenance recovery message has invalid outcome")
	}
	if err != nil {
		return err
	}
	// Recovery-owned terminal entry actions intentionally suppress delivery.
	// Clear the exact pending authority only after the transition commits; the
	// caller then atomically converts the durable intent into its outbox row.
	a.sm.clearPendingReplaceRoute()
	return nil
}

// classifyReplaceReject maps a failed restart/update fireAndVerify into the
// error the caller (Backend.Restart/Update → HTTP handler) should see.
//
// The safety basis for restart/update is NOT the HTTP prelude's fast-fail
// (that is only a route-time precondition — the lease can change state
// between routing and the actor dequeuing the message). It is that the
// actor inbox is the ONLY path mutating prov.Status, processed serially on
// the actor goroutine. So when fireAndVerify fails here, the SM is
// authoritatively in some non-restartable state, and we classify by that
// state rather than by parsing stateless's untyped unhandled-trigger error
// string (which has no exported sentinel and could change across versions):
//
//   - State ∈ {Restarting, Updating, Deprovisioning, Provisioning} — the
//     lease is busy. This is the concurrent-duplicate / lost-the-race case
//     (e.g. two same-lease restarts: the loser arrives after the winner's
//     onEnterRestarting). Wrap as backend.ErrInvalidState so the HTTP
//     handler returns a clean 409 (not a 500). The duplicate is REJECTED
//     here, not prevented earlier.
//   - State ∈ {Ready, Failed} — restartable; a fireAndVerify failure is
//     unexpected (defensive), so forward the raw error unchanged.
//
// NOTE: callers must NOT route the a.terminated early-return through this —
// that path returns errActorTerminated (caller retries against a fresh
// actor) and must stay distinct from a 409.
func (a *LeaseActor) classifyReplaceReject(err error) error {
	switch a.sm.State() {
	case backend.ProvisionStatusReady, backend.ProvisionStatusFailed:
		return err
	default:
		return fmt.Errorf("%w: lease not in a restartable state (%s)", backend.ErrInvalidState, a.sm.State())
	}
}

// handleProvisionRequested transitions the SM into Provisioning (or from
// Failed on retry), acks the caller, and spawns the work goroutine.
// Spawning inside the actor (rather than in Backend.Provision) means the
// worker is tracked by workers and, under normal operation, the actor
// waits for the worker's terminal sendTerminal to land and be handled
// before exit (bounded by WorkerDrainTimeout). The orphan-worker race class is
// eliminated under that happy-path wait; a timeout refuses the transition
// which would otherwise permit conflicting substrate work.
func (a *LeaseActor) handleProvisionRequested(msg provisionRequestedMsg) {
	if a.terminated {
		// Actor has already completed Deprovision but not yet been
		// removed from the registry (defer ordering). Reject so the
		// caller rolls back and retries — a fresh actor will be
		// created on the next routeToLease.
		msg.Ack <- errActorTerminated
		return
	}
	if !msg.Operation.Valid() || msg.Operation.LeaseUUID() != a.leaseUUID {
		msg.Ack <- errors.New("provision operation belongs to another lease")
		return
	}
	if err := a.sm.requestProvision(a.cfg.StopCtx); err != nil {
		msg.Ack <- err
		return
	}
	// The actor creates and owns the exact context passed to work. Callers cannot
	// pair a closure using one context with an unrelated cancellation capability.
	workerCtx, cancel := context.WithCancel(msg.Ctx)
	a.workCancel = cancel
	msg.Ack <- nil
	a.spawnProvisionWorker(workerCtx, msg.Operation)
}

// spawnProvisionWorker runs doProvision (supplied as the work closure),
// pre-publishes container IDs on success so a preempting
// Deprovision sees them under lock, and sends the terminal SM event via
// sendTerminal. Tracked by workers so the actor waits for this worker
// before exit. On failure, the worker captures container logs BEFORE
// cleanup (see doProvision's defer) so the persisted diagnostic entry
// contains useful debugging output even though the failed containers
// have been removed.
func (a *LeaseActor) spawnProvisionWorker(
	ctx context.Context,
	operation shared.OperationIntentClaim,
) {
	a.beginWorkerActivity()
	a.cfg.WG.Go(func() {
		// Exactly one sendTerminal call site (the middle defer), driven
		// by terminalMsg. Defer ordering (LIFO):
		//   1. recover (innermost, runs FIRST on panic) — may override
		//      terminalMsg to a panic-error variant.
		//   2. sendTerminal (middle, always runs) — fires whatever
		//      terminalMsg was set to.
		//   3. workers.Done (outermost, runs LAST) — releases the
		//      barrier after sendTerminal has landed the event.
		// This structure guarantees at-most-one terminal per worker and
		// eliminates both the double-send race and any possibility of a
		// wedged SM if a panic occurs before the normal path sets the
		// message.
		var terminalMsg workerTerminalMessage
		var event string
		defer a.endWorkerActivity()
		defer func() {
			if terminalMsg == nil {
				// Defensive: should not occur if the normal path runs
				// to completion. If it does, treat as an error so the
				// SM reaches Failed rather than wedging.
				terminalMsg = operationAmbiguousMsg{
					kind: string(operation.Kind()), operationID: operation.OperationID(),
					err: errors.New("provision worker exited without a typed outcome"),
				}
				event = "provision_no_result"
			}
			if !a.sendTerminal(terminalMsg) {
				a.cfg.Metrics.TerminalEventDropped(event)
				a.cfg.Logger.Warn("terminal provision event dropped (actor exited or inbox wedged)",
					"lease_uuid", a.leaseUUID,
					"event", event,
				)
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				a.cfg.Logger.Error("provision worker panic — recovering to keep fred alive",
					"lease_uuid", a.leaseUUID,
					"panic", r,
					"stack", string(debug.Stack()),
				)
				a.cfg.Metrics.WorkerPanic("provision")
				// Override any terminalMsg set by the normal path —
				// the panic means the post-set work did not complete.
				terminalMsg = operationAmbiguousMsg{
					kind: string(operation.Kind()), operationID: operation.OperationID(),
					err: fmt.Errorf("provision worker panic: %v", r),
				}
				event = "provision_panic"
			}
		}()
		outcome := a.cfg.ProvisionWorkFn(ctx, operation)
		if err := validateProvisionWorkOutcome(outcome, operation); err != nil {
			terminalMsg = operationAmbiguousMsg{
				kind: string(operation.Kind()), operationID: operation.OperationID(), err: err,
			}
			event = "provision_invalid_outcome"
			return
		}
		switch typed := outcome.(type) {
		case provisionWorkSuccess:
			// Pre-publish so a concurrent Deprovision-preempt reading
			// prov.ContainerIDs sees the new IDs and can tear them down
			// rather than leaving orphans.
			a.cfg.ProvisionStore.UpdateFn(a.leaseUUID, func(p *ProvisionState) {
				p.ContainerIDs = typed.result.containerIDs
			})
			terminalMsg = provisionCompletedMsg(typed)
			event = "provision_completed"
		case provisionWorkFailure:
			terminalMsg = provisionErroredMsg{
				callbackErr:      typed.callbackErr,
				reason:           typed.reason,
				lastError:        typed.err.Error(),
				logs:             typed.logs,
				operationFailure: typed.proof,
			}
			event = "provision_errored"
		case provisionWorkAmbiguous:
			terminalMsg = operationAmbiguousMsg{
				kind: string(operation.Kind()), operationID: operation.OperationID(), err: typed.err,
			}
			event = "provision_ambiguous"
		}
	})
}

func (a *LeaseActor) handleProvisionCompleted(result ProvisionSuccessResult) {
	_ = a.sm.provisionCompleted(a.cfg.StopCtx, result)
}

func (a *LeaseActor) handleProvisionErrored(
	callbackErr string,
	reason backend.Reason,
	lastError string,
	logs map[string]string,
	operationFailure shared.OperationReleaseUncommitted,
) {
	_ = a.sm.provisionErrored(a.cfg.StopCtx, provisionErrorInfo{
		callbackErr:      callbackErr,
		reason:           reason,
		lastError:        lastError,
		logs:             logs,
		operationFailure: operationFailure,
	})
}

func (a *LeaseActor) handleRestartRequested(msg restartRequestedMsg) {
	if a.terminated {
		msg.Ack <- errActorTerminated
		return
	}
	if err := a.validateMaintenanceRequest(
		msg.Maintenance, msg.CallbackURL, msg.LifecycleCallbackURL,
	); err != nil {
		msg.Ack <- err
		return
	}
	// onEnterRestarting publishes Status=Restarting and captures the validated
	// callback pair as pending inside this Fire before the ack.
	if err := a.sm.requestRestart(a.cfg.StopCtx, replaceEntryArgs{
		CallbackURL: msg.CallbackURL, LifecycleCallbackURL: msg.LifecycleCallbackURL,
		CallbackKind: replaceCallbackLifecycle, Maintenance: msg.Maintenance,
	}); err != nil {
		// A concurrent same-lease restart that lost the race finds the SM
		// already busy → classifyReplaceReject returns ErrInvalidState (409).
		msg.Ack <- a.classifyReplaceReject(err)
		return
	}
	// Set workCancel only AFTER a successful fire (ENG-230 §4): a rejected
	// concurrent restart must not clobber the in-flight worker's cancel
	// func, which onExitProvisioning uses on Deprovision-preempt. workCancel
	// is consumed only by onExitProvisioning, which can run only after the
	// state was entered (i.e. after a successful fire).
	workerCtx, cancel := context.WithCancel(msg.Ctx)
	a.workCancel = cancel
	a.spawnMaintenanceWorker(workerCtx, msg.Target)
	msg.Ack <- nil
}

// handleRestoreRequested drives a restore (ENG-325) onto the existing
// replace machinery. It is a clone of handleRestartRequested with one
// difference: it fires evRestoreRequested (permitted only from
// Provisioning, the state a restore's new lease is reserved in) instead
// of evRestartRequested. The destination state is still Restarting, so
// onEnterRestarting (reused via OnEntryFrom(evRestoreRequested)) writes
// Status=Restarting + the callback pair before the ack, and spawnReplaceWorker
// + the evReplace{Completed,Recovered,Failed} terminal events behave
// identically. Because the prior Status was Provisioning (not Ready),
// applyReplaceEntry sets replaceWasActive=false, so a successful restore
// Inc's activeProvisions — bringing the lease from absent to active.
func (a *LeaseActor) handleRestoreRequested(msg restoreRequestedMsg) {
	if a.terminated {
		msg.Ack <- errActorTerminated
		return
	}
	if !msg.Operation.Valid() || msg.Operation.LeaseUUID() != a.leaseUUID {
		msg.Ack <- errors.New("restore operation belongs to another lease")
		return
	}
	// onEnterRestarting writes Status=Restarting and the callback pair inside
	// this Fire, before the ack — preserving the handler-publish contract.
	if err := a.sm.requestRestore(a.cfg.StopCtx, replaceEntryArgs{
		CallbackURL: msg.CallbackURL, LifecycleCallbackURL: msg.LifecycleCallbackURL,
		CallbackKind: replaceCallbackOperation,
	}); err != nil {
		// Restore is permitted only from Provisioning; from any other state
		// (e.g. a duplicate after the SM already left Provisioning, or a
		// concurrent Deprovision) classifyReplaceReject yields ErrInvalidState
		// (→409).
		msg.Ack <- a.classifyReplaceReject(err)
		return
	}
	// Set workCancel only AFTER a successful fire (ENG-230 §4); see
	// handleRestartRequested for the rationale.
	workerCtx, cancel := context.WithCancel(msg.Ctx)
	a.workCancel = cancel
	a.spawnRestoreWorker(workerCtx, msg.Operation)
	msg.Ack <- nil
}

func (a *LeaseActor) handleUpdateRequested(msg updateRequestedMsg) {
	if a.terminated {
		msg.Ack <- errActorTerminated
		return
	}
	if err := a.validateMaintenanceRequest(
		msg.Maintenance, msg.CallbackURL, msg.LifecycleCallbackURL,
	); err != nil {
		msg.Ack <- err
		return
	}
	// onEnterUpdating publishes Status=Updating and captures the validated
	// callback pair as pending inside this Fire before the ack.
	if err := a.sm.requestUpdate(a.cfg.StopCtx, replaceEntryArgs{
		CallbackURL: msg.CallbackURL, LifecycleCallbackURL: msg.LifecycleCallbackURL,
		CallbackKind: replaceCallbackLifecycle, Maintenance: msg.Maintenance,
	}); err != nil {
		// A concurrent same-lease update that lost the race finds the SM
		// already busy → classifyReplaceReject returns ErrInvalidState (409).
		msg.Ack <- a.classifyReplaceReject(err)
		return
	}
	// Set workCancel only AFTER a successful fire (ENG-230 §4); see
	// handleRestartRequested for the rationale.
	workerCtx, cancel := context.WithCancel(msg.Ctx)
	a.workCancel = cancel
	a.spawnMaintenanceWorker(workerCtx, msg.Target)
	msg.Ack <- nil
}

func (a *LeaseActor) validateMaintenanceRequest(
	claim shared.MaintenanceIntentClaim,
	callbackURL string,
	lifecycleCallbackURL string,
) error {
	if !claim.Valid() {
		return errors.New("restart/update requires a valid maintenance intent claim")
	}
	if claim.LeaseUUID() != a.leaseUUID {
		return errors.New("maintenance intent belongs to another lease")
	}
	if callbackURL != claim.CallbackURL() || lifecycleCallbackURL != claim.LifecycleCallbackURL() {
		return errors.New("maintenance callback route differs from durable intent")
	}
	return nil
}

// spawnReplaceWorker runs a replace operation (restart or update) and
// dispatches the correct terminal SM event based on (err, recovered).
// Pre-publishes new ContainerIDs / ServiceContainers on success so a
// preempting Deprovision reading prov observes the new set under lock.
//
// wasActive (whether the lease was Status==Ready at replace-start) is read
// HERE, on the actor goroutine, before spawning the worker — never inside
// the worker closure. It is used only when the worker's result sets
// RecoveredIfSourceActive (the doRestart preflight branch): there the
// recovered-vs-failed decision keys on wasActive (the actor-observed source)
// instead of result.restored, fixing the stale-route-time-snapshot edge.
func (a *LeaseActor) spawnMaintenanceWorker(
	ctx context.Context,
	target shared.MaintenanceReleaseClaim,
) {
	a.spawnReplaceWorker(ctx, target, shared.OperationIntentClaim{})
}

func (a *LeaseActor) spawnRestoreWorker(
	ctx context.Context,
	operation shared.OperationIntentClaim,
) {
	a.spawnReplaceWorker(ctx, shared.MaintenanceReleaseClaim{}, operation)
}

func (a *LeaseActor) spawnReplaceWorker(
	ctx context.Context,
	target shared.MaintenanceReleaseClaim,
	operation shared.OperationIntentClaim,
) {
	wasActive := a.replaceWasActive
	maintenance := a.pendingMaintenance
	hasMaintenanceAuthority := maintenance.Valid()
	maintenanceID := maintenance.MaintenanceID()
	if hasMaintenanceAuthority {
		a.markMaintenanceWorker(maintenanceID)
	}
	a.beginWorkerActivity()
	a.cfg.WG.Go(func() {
		// Same defer structure as spawnProvisionWorker — see that
		// function for the rationale. One sendTerminal call site in
		// the middle defer; normal path and panic recover both write
		// to terminalMsg.
		var terminalMsg workerTerminalMessage
		var event string
		defer a.endWorkerActivity()
		defer func() {
			if terminalMsg == nil {
				kind := string(shared.OperationIntentRestore)
				operationID := shared.OperationID{}
				if hasMaintenanceAuthority {
					kind = string(maintenance.Kind())
				} else if operation.Valid() {
					operationID = operation.OperationID()
				}
				terminalMsg = operationAmbiguousMsg{
					kind: kind, operationID: operationID, maintenanceID: maintenanceID,
					err: errors.New("replace worker exited without a typed outcome"),
				}
				event = "replace_no_result"
			}
			if !a.sendTerminal(terminalMsg) {
				a.clearMaintenanceWorker(maintenanceID)
				a.cfg.Metrics.TerminalEventDropped(event)
				a.cfg.Logger.Warn("terminal replace event dropped (actor exited or inbox wedged)",
					"lease_uuid", a.leaseUUID,
					"event", event,
				)
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				a.cfg.Logger.Error("replace worker panic — recovering to keep fred alive",
					"lease_uuid", a.leaseUUID,
					"panic", r,
					"stack", string(debug.Stack()),
				)
				a.cfg.Metrics.WorkerPanic("replace")
				kind := string(shared.OperationIntentRestore)
				operationID := shared.OperationID{}
				if hasMaintenanceAuthority {
					kind = string(maintenance.Kind())
				} else if operation.Valid() {
					operationID = operation.OperationID()
				}
				terminalMsg = operationAmbiguousMsg{
					kind: kind, operationID: operationID, maintenanceID: maintenanceID,
					err: fmt.Errorf("replace worker panic: %v", r),
				}
				event = "replace_panic"
			}
		}()
		var outcome ReplaceWorkOutcome
		if hasMaintenanceAuthority {
			outcome = a.cfg.MaintenanceWorkFn(ctx, target)
		} else {
			outcome = a.cfg.RestoreWorkFn(ctx, operation)
		}
		if err := validateReplaceWorkOutcome(outcome, maintenance, operation); err != nil {
			kind := string(shared.OperationIntentRestore)
			operationID := shared.OperationID{}
			if hasMaintenanceAuthority {
				kind = string(maintenance.Kind())
			} else if operation.Valid() {
				operationID = operation.OperationID()
			}
			terminalMsg = operationAmbiguousMsg{
				kind: kind, operationID: operationID, maintenanceID: maintenanceID, err: err,
			}
			event = "replace_invalid_outcome"
			return
		}
		if ambiguous, ok := outcome.(replaceWorkAmbiguous); ok {
			kind := string(shared.OperationIntentRestore)
			operationID := shared.OperationID{}
			if hasMaintenanceAuthority {
				kind = string(maintenance.Kind())
			} else if operation.Valid() {
				operationID = operation.OperationID()
			}
			terminalMsg = operationAmbiguousMsg{
				kind: kind, operationID: operationID, maintenanceID: maintenanceID, err: ambiguous.err,
			}
			event = "replace_ambiguous"
			return
		}
		result := outcome.(replaceWorkTerminal).result
		if result.err == nil {
			a.cfg.ProvisionStore.UpdateFn(a.leaseUUID, func(p *ProvisionState) {
				p.ContainerIDs = result.success.containerIDs
				if result.success.serviceContainers != nil {
					p.ServiceContainers = result.success.serviceContainers
				}
			})
		}
		// recovered-vs-failed: normally result.restored, but a doRestart
		// preflight failure (no container touched) sets RecoveredIfSourceActive
		// so the decision keys on the actor-observed pre-replace activeness
		// (wasActive, captured above on the actor goroutine) — recovered iff
		// the lease was Ready/running at replace-start. Every other path
		// leaves the flag false → recovered == result.restored (unchanged).
		recovered := result.restored
		if result.recoveredIfSourceActive {
			recovered = wasActive
		}
		switch {
		case result.err == nil:
			terminalMsg = replaceCompletedMsg{result: result.success, maintenanceID: maintenanceID}
			event = "replace_completed"
		case recovered:
			terminalMsg = replaceRecoveredMsg{info: result.failure, maintenanceID: maintenanceID}
			event = "replace_recovered"
		default:
			terminalMsg = replaceFailedMsg{info: result.failure, maintenanceID: maintenanceID}
			event = "replace_failed"
		}
	})
}

// OwnsMaintenance is the concurrency-safe, exact worker-ownership proof used
// by periodic recovery. A projected Restarting/Updating status alone is not an
// ownership proof: a dropped terminal event can leave it stale indefinitely.
func (a *LeaseActor) OwnsMaintenance(id shared.MaintenanceID) bool {
	if a == nil || !id.Valid() {
		return false
	}
	a.maintenanceWorkerMu.RLock()
	defer a.maintenanceWorkerMu.RUnlock()
	return a.maintenanceWorkerID == id
}

func (a *LeaseActor) markMaintenanceWorker(id shared.MaintenanceID) {
	if !id.Valid() {
		return
	}
	a.maintenanceWorkerMu.Lock()
	a.maintenanceWorkerID = id
	a.maintenanceWorkerMu.Unlock()
}

func (a *LeaseActor) clearMaintenanceWorker(id shared.MaintenanceID) {
	if !id.Valid() {
		return
	}
	a.maintenanceWorkerMu.Lock()
	if a.maintenanceWorkerID == id {
		a.maintenanceWorkerID = shared.MaintenanceID{}
	}
	a.maintenanceWorkerMu.Unlock()
}

func (a *LeaseActor) clearAnyMaintenanceWorker() {
	a.maintenanceWorkerMu.Lock()
	a.maintenanceWorkerID = shared.MaintenanceID{}
	a.maintenanceWorkerMu.Unlock()
}

func (a *LeaseActor) handleReplaceCompleted(result ReplaceSuccessResult, ids ...shared.MaintenanceID) {
	if len(ids) == 1 {
		defer a.clearMaintenanceWorker(ids[0])
	}
	_ = a.sm.replaceCompleted(a.cfg.StopCtx, result)
}

func (a *LeaseActor) handleReplaceRecovered(info ReplaceFailureInfo, ids ...shared.MaintenanceID) {
	if len(ids) == 1 {
		defer a.clearMaintenanceWorker(ids[0])
	}
	_ = a.sm.replaceRecovered(a.cfg.StopCtx, info)
}

func (a *LeaseActor) handleReplaceFailed(info ReplaceFailureInfo, ids ...shared.MaintenanceID) {
	if len(ids) == 1 {
		defer a.clearMaintenanceWorker(ids[0])
	}
	_ = a.sm.replaceFailed(a.cfg.StopCtx, info)
}

// hasExited reports whether the actor's run loop has returned (a.done
// closed). Used by SendTerminal to make the "actor-already-exited" case
// a definitive refusal rather than a select-randomized 50/50 between
// queueing into an inbox nobody will drain and the closed-done arm.
// External callers observe lifecycle through the exported Done()
// channel getter instead — `<-actor.Done()` is the canonical wait.
func (a *LeaseActor) hasExited() bool {
	select {
	case <-a.done:
		return true
	default:
		return false
	}
}

// sendTerminal enqueues a terminal SM event from an in-flight work
// goroutine. Deliberately does NOT refuse on a canceled stopCtx: the
// goroutine has already done its physical work (containers created,
// swapped, removed) — the SM must record the outcome even during
// shutdown to keep releaseStore / in-memory state / the callback record
// consistent with the host. Returns false only if the actor has fully
// exited (inbox no longer drained), terminal admission is closed
// (actor is in its exit sequence at the final drain), or the bounded inbox is
// wedged; in either case the drop is counted via
// leaseTerminalEventDroppedTotal at the call site.
//
// terminalAdmissionMu is held from the closed check through the send. The
// retirement path takes the same gate before its final drain, making the two
// possible orders explicit: the message is admitted before the drain, or the
// sender observes closed admission and cannot enqueue.
//
// In normal operation (waitForWorkers returns cleanly) workers finish
// before terminal admission closes, so these gates are pure defense
// against the waitForWorkers-timeout edge case.
func (a *LeaseActor) sendTerminal(msg workerTerminalMessage) bool {
	a.terminalAdmissionMu.Lock()
	defer a.terminalAdmissionMu.Unlock()
	if a.terminalAdmissionClosed || a.hasExited() {
		return false
	}
	a.activityMu.Lock()
	a.activity++
	a.activityMu.Unlock()
	select {
	case a.inbox <- msg:
		return true
	case <-a.done:
		a.endActivity()
		return false
	case <-time.After(terminalSendTimeout):
		a.endActivity()
		return false
	}
}

// closeTerminalAdmission is idempotent. Holding the gate until the state is
// closed guarantees the caller's following drain observes every send that won
// the ordering race.
func (a *LeaseActor) closeTerminalAdmission() {
	a.terminalAdmissionMu.Lock()
	a.terminalAdmissionClosed = true
	a.terminalAdmissionMu.Unlock()
}

// terminalSendTimeout bounds how long a terminal send will wait for inbox
// space. Long enough for the actor to drain typical backlogs, short
// enough that a wedged actor doesn't pin the goroutine indefinitely.
const terminalSendTimeout = 10 * time.Second

// removeFromRegistry deletes this actor from the substrate's actor
// registry via the OnTerminated callback wired at construction. The
// docker-side closure preserves CompareAndDelete semantics: only
// deletes if the registered entry is THIS actor, so a fresh actor
// stored for the same UUID after our exit started isn't clobbered.
// Used as a deferred action on actor exit.
func (a *LeaseActor) removeFromRegistry() {
	a.cfg.OnTerminated(a.leaseUUID, a)
}

// State returns the SM's current ProvisionStatus.
func (a *LeaseActor) State() backend.ProvisionStatus {
	return a.sm.State()
}

// InboxDepth returns the number of pending messages in the actor's
// inbox (best-effort read; the value may shift between this call and
// any subsequent use, but len() on a buffered channel is safe to read
// concurrently with sends/receives).
func (a *LeaseActor) InboxDepth() int {
	return len(a.inbox)
}

// InboxCap returns the inbox channel capacity (constant for the
// actor's lifetime).
func (a *LeaseActor) InboxCap() int {
	return cap(a.inbox)
}

// CurrentMessageStart returns the UnixNano timestamp of the message
// the actor is currently processing in handle(), or 0 when idle.
// Atomic read; safe to call from the metrics sample loop concurrently
// with the actor goroutine. Method name uses the noun form (vs the
// underlying field's verb-implying `currentMessageStart atomic.Int64`)
// to avoid the Go field/method name collision while preserving intent.
func (a *LeaseActor) CurrentMessageStart() int64 {
	return a.currentMessageStart.Load()
}

// QuiescenceClaim is an opaque exclusive capability proving that one actor had
// no accepted message or worker and cannot admit another. Only LeaseActor can
// construct one. Release is idempotent so defer plus defensive cleanup is safe.
type QuiescenceClaim struct {
	state *quiescenceClaimState
}

type quiescenceClaimState struct {
	actor    *LeaseActor
	mu       sync.Mutex
	released bool
}

func (c *QuiescenceClaim) Release() {
	if c == nil || c.state == nil || c.state.actor == nil {
		return
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.released {
		return
	}
	c.state.released = true
	c.state.actor.activityMu.Unlock()
	c.state.actor.admissionMu.Unlock()
}

// Retire marks the claimed idle actor as closed to future admission and wakes
// its run loop. It succeeds only while this exact capability still owns both
// actor gates; calling it after Release is a harmless no-op.
func (c *QuiescenceClaim) Retire() bool {
	if c == nil || c.state == nil || c.state.actor == nil {
		return false
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.released {
		return false
	}
	c.state.actor.retiring = true
	c.state.actor.retirementRequestedOnce.Do(func() {
		close(c.state.actor.retirementRequested)
	})
	return true
}

// TryClaimQuiescence returns an exclusive typed capability only when no
// accepted message or worker can still mutate this actor. During the claim,
// non-blocking external enqueue is refused and retirement cannot replace the
// claimed registry actor. A terminal sender cannot be excluded while its worker
// exists because worker activity stays non-zero until after that terminal
// message has itself been counted.
func (a *LeaseActor) TryClaimQuiescence() *QuiescenceClaim {
	if a == nil || !a.admissionMu.TryLock() {
		return nil
	}
	if a.retiring {
		a.admissionMu.Unlock()
		return nil
	}
	if !a.activityMu.TryLock() {
		a.admissionMu.Unlock()
		return nil
	}
	if a.activity != 0 {
		a.activityMu.Unlock()
		a.admissionMu.Unlock()
		return nil
	}
	return &QuiescenceClaim{state: &quiescenceClaimState{actor: a}}
}

// Done returns the channel that closes when the actor's run loop has
// fully torn down (after external admission closes, workers and their
// terminal messages settle, and the actor is removed from its registry).
// Tests and substrate adapters can block on this to wait for an actor's full
// quiescence — e.g., to
// assert that a subsequent re-provision with the same lease UUID
// creates a fresh actor. Returns a `<-chan struct{}` so callers cannot
// accidentally close the underlying channel.
func (a *LeaseActor) Done() <-chan struct{} {
	return a.done
}

// TryEnqueueCommand admits an opaque caller command exactly once.
func (a *LeaseActor) TryEnqueueCommand(command ActorCommand) bool {
	return a.tryEnqueueEnvelope(command.envelope, command.envelopeMessage())
}

func (c ActorCommand) envelopeMessage() leaseMessage {
	if c.envelope == nil {
		return nil
	}
	return c.envelope.message
}

// TryEnqueueObservation admits only an exact runtime-generation observation.
func (a *LeaseActor) TryEnqueueObservation(observation ActorObservation) bool {
	if observation.envelope == nil {
		return false
	}
	message, ok := observation.envelope.message.(actorObservationMessage)
	if !ok || message == nil {
		return false
	}
	if observation.LeaseUUID() != a.leaseUUID {
		return false
	}
	return a.tryEnqueueEnvelope(observation.envelope, message)
}

// TryEnqueueRecovery admits only a recovery capability. Recovery routing must
// resolve an existing actor; ordinary command routing cannot accept this type.
func (a *LeaseActor) TryEnqueueRecovery(command RecoveryCommand) bool {
	if command.envelope == nil {
		return false
	}
	return a.tryEnqueueEnvelope(command.envelope, command.envelope.message)
}

func (a *LeaseActor) tryEnqueueEnvelope(envelope *messageEnvelope, message leaseMessage) bool {
	if envelope == nil || message == nil || !envelope.accepted.CompareAndSwap(false, true) {
		return false
	}
	if a.tryEnqueue(message) {
		return true
	}
	envelope.accepted.Store(false)
	return false
}

// tryEnqueue does a non-blocking enqueue of an internal message into the actor's inbox.
// Returns true on success, false if the actor is retiring or the inbox is
// full. This is the primary entrypoint the substrate's routing layer uses to
// deliver messages atomically with its registry-resolve operation; the caller
// holds the registry mutex across this call so a successful enqueue
// implies the actor is still registered and consuming its inbox.
//
// admissionMu makes a successful send linearizable with retirement: a message
// is either accepted for the live run loop, or refused while the still-
// registered actor completes its retirement. The mutex is held only for the
// non-blocking send and is never held while unregistering the actor.
func (a *LeaseActor) tryEnqueue(msg leaseMessage) bool {
	if msg == nil {
		return false
	}
	// Preserve the non-blocking routing contract even while recovery holds an
	// exclusive quiescence claim or retirement is closing admission.
	if !a.admissionMu.TryLock() {
		return false
	}
	defer a.admissionMu.Unlock()
	if a.retiring {
		return false
	}
	// Routing is documented as non-blocking. A recovery owner may hold the
	// quiescence gate across substrate mutation, so refuse rather than waiting on
	// activityMu; blocking callers retry and lifecycle events have reconciliation
	// as their backstop.
	if !a.activityMu.TryLock() {
		return false
	}
	a.activity++
	select {
	case a.inbox <- msg:
		a.activityMu.Unlock()
		return true
	default:
		a.activity--
		a.activityMu.Unlock()
		return false
	}
}

// handleDeprovision runs inside the lease actor's message handler. It fires
// the SM transition then runs the work synchronously, returning the outcome
// through the constructor-owned deprovision reply capability.
//
// Migrated from internal/backend/docker/deprovision.go at PR5b-2 BC. The
// body now reaches substrate-private state exclusively via the cfg seams:
//   - lease existence check via cfg.ProvisionStore.Exists (was b.provisions[uuid])
//   - deprovision dispatch via cfg.DoDeprovisionFn (was b.doDeprovision)
//   - logger via cfg.Logger
//
// The ctx threaded in is the actor-owned ctx from the inbound
// deprovision command (which carries the caller's ctx from Backend.Deprovision).
func (a *LeaseActor) handleDeprovision(ctx context.Context) error {
	// Attempt the SM transition. If it's not permitted, check whether the
	// provision is already gone or we're in an unexpected state. Absence from
	// this volatile projection is not terminal authority: a substrate finalizer
	// may still own containers, volumes, or a durable close journal, so it must
	// still receive the idempotent request.
	transitioned := true
	if err := a.sm.requestDeprovision(ctx); err != nil {
		transitioned = false
		if a.cfg.ProvisionStore.Exists(a.leaseUUID) {
			a.cfg.Logger.Warn("deprovision transition denied by SM",
				"lease_uuid", a.leaseUUID, "error", err)
			// Every legitimate live state has an explicit deprovision transition or
			// idempotent self-event. Any rejection therefore means the actor could not
			// prove exclusive teardown authority (most importantly, a mutation worker
			// failed to drain). Never compensate by running destructive work anyway.
			return fmt.Errorf("deprovision refused before exclusive teardown: %w", err)
		}
	}
	if transitioned && a.replaceCallbackKind == replaceCallbackLifecycle &&
		a.pendingReplaceCallbackURL != "" && a.pendingReplaceLifecycleCallbackURL != "" {
		// Teardown eliminates the runtime cohort whose labels were protected by
		// the old pair. Promote an accepted maintenance route before close
		// admission so its lifecycle capability owns the terminal callback even
		// when Deprovision preempted the replace terminal event.
		a.cfg.ProvisionStore.UpdateFn(a.leaseUUID, func(p *ProvisionState) {
			p.CallbackURL = a.pendingReplaceCallbackURL
			p.LifecycleCallbackURL = a.pendingReplaceLifecycleCallbackURL
		})
	}
	closeScope := newActorCloseScope(a)
	defer closeScope.revoke()
	err := a.cfg.DoDeprovisionFn(ctx, closeScope)
	// If the provision entry was fully removed (success path), signal the
	// run loop to exit so a subsequent re-provision with the same UUID
	// creates a fresh actor instead of being Ignored by a stale SM.
	if !a.cfg.ProvisionStore.Exists(a.leaseUUID) {
		a.terminated = true
	}
	return err
}
