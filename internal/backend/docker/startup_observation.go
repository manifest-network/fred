package docker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// Startup observation (ENG-1125) decides, after a launch, whether its exact
// cohort started. It watches EVERY container of the cohort on every pass, so a
// service that exits while another service still waits for its health check
// is seen, and it reports Ready only from one whole-cohort pass in which every
// container is ready. A health-gated container that reported healthy once is
// from then on watched only for its exit: a later unhealthy report is a flap,
// which steady state ignores too, never a startup failure. Every other startup
// outcome authority (the classifier that confirms the watch's Ready, operation
// recovery, maintenance readiness) applies the same rule through the backend's
// startup health ledger (gatedHealth). A depends_on service_healthy dependency
// is the exception: Compose judges its health itself during `compose up`, so a
// flap there still rejects the launch.
//
// A launch exchange that settled with an error (Compose reported a dependency
// that exited or turned unhealthy, or the daemon refused a Start) is observed
// once, and fails definitely only on a positive account of one container: an
// exit, an unhealthy report, or a Start the daemon refused on a container that
// never ran. Anything else stays unverified.
//
// Startup failures carry their curated tenant surface, authored here where the
// failure is observed (ENG-508): no caller derives a message or a reason from
// an error's text. Only an observed exit is ContainerExited, the one reason
// the terminal budget can count (ENG-799). A health check that reported
// unhealthy, or that never passed before the startup deadline, is
// HealthCheckFailed, and a refused start is ContainerStartFailed; neither ever
// counts. A failed read, a cancellation, or a container in a state that says
// nothing about the tenant's workload is unverified: Internal, and never a
// definite failure.

// healthPollInterval is the interval between whole-cohort passes while a
// health-gated service is starting.
const healthPollInterval = 2 * time.Second

// startupRollbackReserve is the part of a provision's deadline that startup
// observation leaves unused, so that a definite startup failure (a health
// check that never passed included) can still be rolled back and classified
// before the provision's own deadline. A shorter remaining budget keeps half
// of it in reserve.
const startupRollbackReserve = time.Minute

func startupExitFailure(healthGated bool, cause error) *physicalOperationError {
	callback := backend.MsgContainerExitedDuringStartup
	if healthGated {
		callback = backend.MsgContainerExitedDuringHealthCheck
	}
	return &physicalOperationError{callback: callback, reason: backend.ReasonContainerExited, cause: cause}
}

func startupUnhealthyFailure(cause error) *physicalOperationError {
	return &physicalOperationError{callback: backend.MsgContainerUnhealthy, reason: backend.ReasonHealthCheckFailed, cause: cause}
}

func startupHealthDeadlineFailure(cause error) *physicalOperationError {
	return &physicalOperationError{callback: backend.MsgHealthCheckDeadline, reason: backend.ReasonHealthCheckFailed, cause: cause}
}

func startupStartRefusedFailure(cause error) *physicalOperationError {
	return &physicalOperationError{callback: backend.MsgContainerStartRefused, reason: backend.ReasonContainerStartFailed, cause: cause}
}

// launchRejectedFailure is the surface of a launch exchange that settled with
// an error but showed no positive failure of any container. Compose's error
// is operator detail only.
func launchRejectedFailure(cause error) *physicalOperationError {
	return &physicalOperationError{callback: "container creation failed", reason: backend.ReasonInternal,
		cause: fmt.Errorf("compose up failed: %w", cause)}
}

func startupUnverifiedFailure(cause error) *physicalOperationError {
	return &physicalOperationError{callback: backend.MsgStartupUnverified, reason: backend.ReasonInternal, cause: cause}
}

func startupCanceledFailure(cause error) *physicalOperationError {
	return &physicalOperationError{callback: backend.MsgStartupCanceled, reason: backend.ReasonInternal, cause: cause}
}

// startupInstanceVerdict is the closed classification of one container's
// inspected state during startup observation. The zero value is invalid.
type startupInstanceVerdict uint8

const (
	// startupInstancePending: still starting; observe again.
	startupInstancePending startupInstanceVerdict = iota + 1
	// startupInstanceReady: running, and healthy where a health check gates it.
	startupInstanceReady
	// startupInstanceExited: the workload exited (Docker reported its exit).
	startupInstanceExited
	// startupInstanceUnhealthy: running, and its health check reported unhealthy.
	startupInstanceUnhealthy
	// startupInstanceStartRefused: created, and the daemon refused its Start.
	startupInstanceStartRefused
	// startupInstanceUnverified: a state that says nothing about the workload.
	startupInstanceUnverified
)

// startupMemberFacts is what a watch knows about one member beyond its latest
// inspection.
type startupMemberFacts struct {
	healthGated bool
	// passedHealth: the member reported healthy in an earlier pass of this
	// watch, so from then on only its exit is observed.
	passedHealth bool
	// startRefused: the launch's settled exchange recorded the daemon's final
	// refusal of this member's Start.
	startRefused bool
}

// classifyStartupInstance is the live startup state table, total over every
// Docker status, health value and member fact:
//
//	exited                                       -> exited
//	running, not health-gated                    -> ready
//	running, health-gated, healthy before        -> ready (whatever its health now)
//	running, health-gated, healthy               -> ready
//	running, health-gated, unhealthy             -> unhealthy
//	running, health-gated, starting/none/?       -> pending
//	created, its Start refused by the daemon     -> start refused
//	created/restarting/paused, health-gated      -> pending
//	created/restarting/paused, fixed wait        -> unverified
//	removing/dead/empty/anything else            -> unverified
//
// It differs from the recovery table (containerStatusToProvisionStatus) on
// purpose. Recovery maps removing and dead to Failed: it fails an attempt
// whose worker is gone and attributes nothing, so any cohort that can no
// longer become Ready is enough. This table decides whether a live attempt
// failed DEFINITELY, with an attribution, so it accepts only the workload's
// own account (an exit, a health verdict) or the daemon's final refusal of a
// Start it was sent: removing and dead are the daemon's states, and stay
// unverified. Recovery maps paused to Ready because it re-adopts an existing
// cohort; a paused container cannot be verified as a started workload, so
// here it is pending behind a health check and unverified otherwise. Recovery
// waits on a created container, because it cannot know whether its Start was
// ever sent; this table knows that only from the launch's own exchange. Both
// apply the same sticky health rule: a running member a startup watch saw pass
// its check is judged healthy from then on, through the backend's startup
// health ledger (gatedHealth), as the steady state does after Ready. Recovery
// after a restart has no such memory and reads health afresh.
func classifyStartupInstance(info *ContainerInfo, facts startupMemberFacts) startupInstanceVerdict {
	if info == nil {
		return startupInstanceUnverified
	}
	switch status := strings.ToLower(info.Status); status {
	case "exited":
		return startupInstanceExited
	case "running":
		if !facts.healthGated || facts.passedHealth {
			return startupInstanceReady
		}
		switch info.Health {
		case HealthStatusHealthy:
			return startupInstanceReady
		case HealthStatusUnhealthy:
			return startupInstanceUnhealthy
		default:
			return startupInstancePending
		}
	case "created", "restarting", "paused":
		if status == "created" && facts.startRefused {
			return startupInstanceStartRefused
		}
		if facts.healthGated {
			return startupInstancePending
		}
		return startupInstanceUnverified
	default:
		return startupInstanceUnverified
	}
}

// startupContainer is one member of a launch's Compose PS cohort.
type startupContainer struct {
	id          string
	service     string
	healthGated bool
}

// startupMemory is what one watch knows across its passes: the members whose
// Start the launch's exchange recorded as refused, and, through the backend's
// startup health ledger, the health-gated members that already reported
// healthy. The ledger is the same memory every other startup outcome authority
// reads (gatedHealth), so the watch and they apply one sticky health rule.
type startupMemory struct {
	launch settledLaunch
	health *startupHealthLedger
}

func (b *Backend) newStartupMemory(launch settledLaunch) *startupMemory {
	return &startupMemory{launch: launch, health: &b.startupHealth}
}

func (m *startupMemory) facts(member startupContainer) startupMemberFacts {
	return startupMemberFacts{
		healthGated: member.healthGated, passedHealth: m.health.passedHealth(member.id),
		startRefused: m.launch.startRefused(member.id),
	}
}

// remember records every health-gated member this pass saw running and
// healthy, in the backend's startup health ledger.
func (m *startupMemory) remember(pass startupPass) {
	m.health.recordPassedHealth(pass)
}

// startupCohort is a launch's exact cohort in a deterministic order.
type startupCohort []startupContainer

// newStartupCohort binds the exact PS cohort to its services' startup
// contracts. Every service of the cohort must be declared by the stack.
func newStartupCohort(stack *manifest.StackManifest, serviceContainers map[string][]string) (startupCohort, error) {
	if stack == nil {
		return nil, errors.New("startup observation requires the stack manifest")
	}
	var cohort startupCohort
	for _, service := range slices.Sorted(maps.Keys(serviceContainers)) {
		declared := stack.Services[service]
		if declared == nil {
			return nil, fmt.Errorf("startup cohort service %q is not declared by the stack", service)
		}
		ids := slices.Clone(serviceContainers[service])
		slices.Sort(ids)
		for _, id := range ids {
			cohort = append(cohort, startupContainer{
				id: id, service: service, healthGated: declared.HasActiveHealthCheck(),
			})
		}
	}
	if len(cohort) == 0 {
		return nil, errors.New("startup observation requires a non-empty cohort")
	}
	return cohort, nil
}

func (c startupCohort) contains(member startupContainer) bool {
	return slices.Contains(c, member)
}

func (c startupCohort) healthGated() bool {
	return slices.ContainsFunc(c, func(member startupContainer) bool { return member.healthGated })
}

func (c startupCohort) settles() bool {
	return slices.ContainsFunc(c, func(member startupContainer) bool { return !member.healthGated })
}

// startupVerdict is the closed outcome of watching one cohort. The zero value
// is invalid.
type startupVerdict uint8

const (
	startupVerdictReady startupVerdict = iota + 1
	startupVerdictExited
	startupVerdictUnhealthy
	startupVerdictNeverHealthy
	startupVerdictStartRefused
	startupVerdictUnverified
)

// startupWatch is one cohort watch's result. Every outcome but Ready carries
// its curated surface; a failure also names the container and its last
// inspection.
type startupWatch struct {
	verdict   startupVerdict
	container startupContainer
	info      *ContainerInfo
	surface   *physicalOperationError
}

func (w startupWatch) failed() bool {
	switch w.verdict {
	case startupVerdictExited, startupVerdictUnhealthy, startupVerdictNeverHealthy, startupVerdictStartRefused:
		return true
	default:
		return false
	}
}

func unverifiedStartup(surface *physicalOperationError) startupWatch {
	return startupWatch{verdict: startupVerdictUnverified, surface: surface}
}

// startupPass is one whole-cohort inspection.
type startupPass struct {
	members  []startupContainer
	infos    []*ContainerInfo
	verdicts []startupInstanceVerdict
}

// inspectStartupCohort inspects every container of the cohort once and
// classifies each against what the watch remembers of it. A failed read ends
// the pass unverified: a failed read is never a fact. A completed pass is
// remembered here, before any caller decides on it, so every pass (polling,
// the observation deadline, a rejected launch) records the health checks it
// saw pass and no later flap can erase them.
func (b *Backend) inspectStartupCohort(ctx context.Context, cohort startupCohort, memory *startupMemory) (startupPass, *physicalOperationError) {
	pass := startupPass{
		members:  slices.Clone(cohort),
		infos:    make([]*ContainerInfo, len(cohort)),
		verdicts: make([]startupInstanceVerdict, len(cohort)),
	}
	for i, member := range cohort {
		info, err := b.docker.InspectContainer(ctx, member.id)
		if err != nil {
			return startupPass{}, startupUnverifiedFailure(fmt.Errorf(
				"failed to inspect %s during startup: %w", member, err))
		}
		pass.infos[i] = info
		pass.verdicts[i] = classifyStartupInstance(info, memory.facts(member))
	}
	memory.remember(pass)
	return pass, nil
}

func (m startupContainer) String() string {
	if m.service == "" {
		return "container " + leasesm.ShortID(m.id)
	}
	return "container " + leasesm.ShortID(m.id) + " of service " + m.service
}

// decideStartupFailure returns the pass's most specific positive failure, if
// any. A positively observed exit wins over every other container's state;
// then an unhealthy report; then a Start the daemon refused.
func (b *Backend) decideStartupFailure(ctx context.Context, pass startupPass) (startupWatch, bool) {
	for _, wanted := range []startupInstanceVerdict{
		startupInstanceExited, startupInstanceUnhealthy, startupInstanceStartRefused,
	} {
		for i, verdict := range pass.verdicts {
			if verdict != wanted {
				continue
			}
			member, info := pass.members[i], pass.infos[i]
			switch verdict {
			case startupInstanceExited:
				diag := b.containerFailureDiagnostics(ctx, member.id, containerInfoToInstanceState(info))
				return startupWatch{
					verdict: startupVerdictExited, container: member, info: info,
					surface: startupExitFailure(member.healthGated, fmt.Errorf(
						"%s exited during startup (status: %s): %s", member, info.Status, diag)),
				}, true
			case startupInstanceUnhealthy:
				diag := b.containerFailureDiagnostics(ctx, member.id, containerInfoToInstanceState(info))
				return startupWatch{
					verdict: startupVerdictUnhealthy, container: member, info: info,
					surface: startupUnhealthyFailure(fmt.Errorf("%s reported unhealthy: %s", member, diag)),
				}, true
			default:
				return startupWatch{
					verdict: startupVerdictStartRefused, container: member, info: info,
					surface: startupStartRefusedFailure(fmt.Errorf(
						"the daemon refused to start %s (status: %s)", member, info.Status)),
				}, true
			}
		}
	}
	return startupWatch{}, false
}

// decideStartupPass turns one pass into a verdict: a positive failure first
// (decideStartupFailure), then any unverifiable state. settled reports
// whether the fixed-wait containers' settle period has passed.
func (b *Backend) decideStartupPass(ctx context.Context, pass startupPass, settled bool) (startupWatch, bool) {
	if watch, failed := b.decideStartupFailure(ctx, pass); failed {
		return watch, true
	}
	for i, verdict := range pass.verdicts {
		if verdict == startupInstanceUnverified {
			member, info := pass.members[i], pass.infos[i]
			return unverifiedStartup(startupUnverifiedFailure(fmt.Errorf(
				"%s is in an unverifiable startup state (status: %s)", member, info.Status))), true
		}
	}
	if !settled {
		return startupWatch{}, false
	}
	for _, verdict := range pass.verdicts {
		if verdict != startupInstanceReady {
			return startupWatch{}, false
		}
	}
	return startupWatch{verdict: startupVerdictReady}, true
}

// decideStartupDeadline is the last pass at the observation deadline. A
// container still running behind a health check that never passed is a
// definite failure; any other container that is not yet ready leaves startup
// unverified.
func (b *Backend) decideStartupDeadline(ctx context.Context, pass startupPass, settled bool) startupWatch {
	if watch, decided := b.decideStartupPass(ctx, pass, settled); decided {
		return watch
	}
	for i, verdict := range pass.verdicts {
		member, info := pass.members[i], pass.infos[i]
		if verdict == startupInstanceReady && (member.healthGated || settled) {
			continue
		}
		if verdict == startupInstancePending && member.healthGated &&
			strings.EqualFold(info.Status, "running") {
			continue
		}
		return unverifiedStartup(startupUnverifiedFailure(fmt.Errorf(
			"%s did not finish starting before the startup deadline (status: %s)", member, info.Status)))
	}
	for i, verdict := range pass.verdicts {
		if verdict != startupInstancePending {
			continue
		}
		member, info := pass.members[i], pass.infos[i]
		return startupWatch{
			verdict: startupVerdictNeverHealthy, container: member, info: info,
			surface: startupHealthDeadlineFailure(fmt.Errorf(
				"%s did not become healthy before the startup deadline (health: %q)", member, info.Health)),
		}
	}
	return unverifiedStartup(startupUnverifiedFailure(errors.New("startup deadline reached with no pending container")))
}

// watchStartup observes cohort until every container is ready, one fails, or
// the observation can no longer be trusted. Fixed-wait containers must stay
// running for the configured settle period; health-gated containers must
// report healthy once, and are then watched only for their exit (memory). Every
// pass inspects the whole cohort, so a container that exits after passing its
// own check is still seen, and Ready is reported only from one pass in which
// every container is ready.
//
// observeUntil, when set, is the observation's own deadline: a last pass then
// decides between a health check that never passed (definite) and an
// unverifiable state. Without it, the caller's context bounds the watch and
// its deadline leaves startup unverified.
func (b *Backend) watchStartup(
	ctx context.Context,
	cohort startupCohort,
	memory *startupMemory,
	observeUntil time.Time,
	logger *slog.Logger,
) startupWatch {
	if len(cohort) == 0 {
		return unverifiedStartup(startupUnverifiedFailure(errors.New("startup observation requires a non-empty cohort")))
	}
	start := time.Now()
	settleEnd := start
	if cohort.settles() {
		settleEnd = start.Add(cmp.Or(b.cfg.StartupVerifyDuration, 5*time.Second))
	}
	var deadline <-chan time.Time
	if !observeUntil.IsZero() {
		timer := time.NewTimer(time.Until(observeUntil))
		defer timer.Stop()
		deadline = timer.C
	}
	for {
		// Wake at the settle period's end, and every poll interval while a
		// health check gates the cohort or the settle period has passed.
		wait := time.Until(settleEnd)
		if wait <= 0 || (cohort.healthGated() && wait > healthPollInterval) {
			wait = healthPollInterval
		}
		wake := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			wake.Stop()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) && cohort.healthGated() {
				return unverifiedStartup(startupHealthDeadlineFailure(fmt.Errorf(
					"timed out waiting for containers to become healthy: %w", ctx.Err())))
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return unverifiedStartup(startupUnverifiedFailure(fmt.Errorf(
					"timed out during startup verification: %w", ctx.Err())))
			}
			return unverifiedStartup(startupCanceledFailure(fmt.Errorf(
				"canceled during startup verification: %w", ctx.Err())))
		case <-deadline:
			wake.Stop()
			pass, failure := b.inspectStartupCohort(ctx, cohort, memory)
			if failure != nil {
				return unverifiedStartup(failure)
			}
			return b.decideStartupDeadline(ctx, pass, !time.Now().Before(settleEnd))
		case <-wake.C:
		}
		pass, failure := b.inspectStartupCohort(ctx, cohort, memory)
		if failure != nil {
			return unverifiedStartup(failure)
		}
		if watch, decided := b.decideStartupPass(ctx, pass, !time.Now().Before(settleEnd)); decided {
			if watch.verdict == startupVerdictReady {
				logger.Info("startup cohort verified", "containers", len(cohort))
			}
			return watch
		}
	}
}

// startupObservationDeadline keeps startupRollbackReserve of the provision's
// deadline for a definite failure's rollback and classification.
func startupObservationDeadline(ctx context.Context, now time.Time) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}
	remaining := max(deadline.Sub(now), 0)
	return deadline.Add(-min(startupRollbackReserve, remaining/2))
}

// startupObservationKind is the closed outcome of a provision's startup
// observation.
type startupObservationKind uint8

const (
	startupObservedReady startupObservationKind = iota + 1
	startupObservedFailure
	startupObservedUnverified
)

// startupObservation is the sealed result of observeStartup: Ready, a
// definite failure carrying its finding, or unverified carrying its curated
// surface. The zero value is invalid.
type startupObservation struct {
	kind       startupObservationKind
	failure    startupFailure
	unverified *physicalOperationError
}

// observeStartup observes the startup of a provision's launch that settled with
// every Create and Start successful. A failure becomes a finding only through
// newStartupFailure, which re-checks every fact it needs; when it cannot, the
// failure stays unverified and the attempt Ambiguous.
func (b *Backend) observeStartup(
	ctx context.Context,
	mutations *storageMutations,
	launch settledLaunch,
	cohort startupCohort,
	logger *slog.Logger,
) startupObservation {
	if launch.rejected() {
		return startupObservation{kind: startupObservedUnverified,
			unverified: startupUnverifiedFailure(errors.New("startup observation requires a launched exchange"))}
	}
	watch := b.watchStartup(ctx, cohort, b.newStartupMemory(launch), startupObservationDeadline(ctx, time.Now()), logger)
	return b.startupObservationOf(ctx, mutations, launch, cohort, watch, logger)
}

// observeRejectedLaunch observes, once, the cohort of a provision's launch
// whose exchange settled but was rejected: Compose reported an error, such as
// a depends_on dependency that exited or turned unhealthy, or the daemon
// refused a Start. Only a positive account of one container is a definite
// failure: an exit, an unhealthy report, or a Start the daemon refused on a
// container that never ran. Compose's error itself is never read for a
// verdict. Anything else, Ready included, stays unverified with an Internal
// surface: the launch failed for a reason no container shows.
func (b *Backend) observeRejectedLaunch(
	ctx context.Context,
	mutations *storageMutations,
	launch settledLaunch,
	cohort startupCohort,
	rejection error,
	logger *slog.Logger,
) startupObservation {
	if !launch.rejected() || rejection == nil {
		return startupObservation{kind: startupObservedUnverified,
			unverified: startupUnverifiedFailure(errors.New("rejected-launch observation requires a rejected exchange"))}
	}
	pass, failure := b.inspectStartupCohort(ctx, cohort, b.newStartupMemory(launch))
	if failure != nil {
		return startupObservation{kind: startupObservedUnverified, unverified: failure}
	}
	watch, failed := b.decideStartupFailure(ctx, pass)
	if !failed {
		return startupObservation{kind: startupObservedUnverified, unverified: launchRejectedFailure(rejection)}
	}
	return b.startupObservationOf(ctx, mutations, launch, cohort, watch, logger)
}

// startupObservationOf seals a finished watch into an observation.
func (b *Backend) startupObservationOf(
	ctx context.Context,
	mutations *storageMutations,
	launch settledLaunch,
	cohort startupCohort,
	watch startupWatch,
	logger *slog.Logger,
) startupObservation {
	switch {
	case watch.verdict == startupVerdictReady:
		return startupObservation{kind: startupObservedReady}
	case watch.failed():
		failure, err := b.newStartupFailure(ctx, mutations, launch, cohort, watch)
		if err != nil {
			logger.Warn("startup failure observed but not provable; leaving the attempt to recovery",
				"container_id", leasesm.ShortID(watch.container.id), "error", err)
			return startupObservation{kind: startupObservedUnverified, unverified: watch.surface}
		}
		if launch.uncounted() {
			logger.Warn("startup failed after a launch the platform did not complete; it never counts",
				"container_id", leasesm.ShortID(watch.container.id), "rejected", launch.rejected(),
				"degradations", launch.degradations().String())
		}
		return startupObservation{kind: startupObservedFailure, failure: failure}
	default:
		surface := watch.surface
		if surface == nil {
			surface = startupUnverifiedFailure(errors.New("startup observation ended without a verdict"))
		}
		return startupObservation{kind: startupObservedUnverified, unverified: surface}
	}
}

// replacementStartupError is a startup verification failure on a
// replacement (restart, update, restore) or a compensating relaunch. It keeps
// the observed cause in its chain, but deliberately not the provision path's
// physicalOperationError: a replacement's own operation authors its
// tenant-facing reason (RestartFailed, UpdateFailed, RestoreFailed), and
// errors.As would otherwise take the startup surface over it.
type replacementStartupError struct {
	message string
	cause   error
}

func (e *replacementStartupError) Error() string { return e.message + ": " + e.cause.Error() }
func (e *replacementStartupError) Unwrap() error { return e.cause }

// flattenStartupWatch reduces a watch to a plain error: nil when Ready, and
// otherwise never a definite finding nor a physicalOperationError.
func flattenStartupWatch(watch startupWatch) error {
	if watch.verdict == startupVerdictReady {
		return nil
	}
	surface := watch.surface
	if surface == nil {
		surface = startupUnverifiedFailure(errors.New("startup observation ended without a verdict"))
	}
	return &replacementStartupError{message: surface.callback, cause: surface.cause}
}

// observeReplacementStartup observes a replacement's whole cohort (restart,
// update, restore) and flattens the outcome into a plain error.
func (b *Backend) observeReplacementStartup(
	ctx context.Context,
	stack *manifest.StackManifest,
	serviceContainers map[string][]string,
	logger *slog.Logger,
) error {
	cohort, err := newStartupCohort(stack, serviceContainers)
	if err != nil {
		return err
	}
	return flattenStartupWatch(b.watchStartup(ctx, cohort, b.newStartupMemory(settledLaunch{}), time.Time{}, logger))
}

// verifyStartup is the error-only check of one service's containers that a
// compensating relaunch uses.
func (b *Backend) verifyStartup(ctx context.Context, m *manifest.Manifest, containerIDs []string, logger *slog.Logger) error {
	if m == nil {
		return errors.New("startup verification requires the service manifest")
	}
	cohort := make(startupCohort, 0, len(containerIDs))
	for _, id := range containerIDs {
		cohort = append(cohort, startupContainer{id: id, healthGated: m.HasActiveHealthCheck()})
	}
	return flattenStartupWatch(b.watchStartup(ctx, cohort, b.newStartupMemory(settledLaunch{}), time.Time{}, logger))
}

// waitForHealthy is the error-only wait for a compensating relaunch's health
// dependencies: every container must report healthy.
func (b *Backend) waitForHealthy(ctx context.Context, containerIDs []string, logger *slog.Logger) error {
	cohort := make(startupCohort, 0, len(containerIDs))
	for _, id := range containerIDs {
		cohort = append(cohort, startupContainer{id: id, healthGated: true})
	}
	return flattenStartupWatch(b.watchStartup(ctx, cohort, b.newStartupMemory(settledLaunch{}), time.Time{}, logger))
}
