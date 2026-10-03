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
// container is ready.
//
// Startup failures carry their curated tenant surface, authored here where the
// failure is observed (ENG-508): no caller derives a message or a reason from
// an error's text. Only an observed exit is ContainerExited, the one reason
// the terminal budget can count (ENG-799). A health check that reported
// unhealthy, or that never passed before the startup deadline, is
// HealthCheckFailed, which never counts. A failed read, a cancellation, or a
// container in a state that says nothing about the tenant's workload is
// unverified: Internal, and never a definite failure.

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
	// startupInstanceUnverified: a state that says nothing about the workload.
	startupInstanceUnverified
)

// classifyStartupInstance is the live startup state table, total over every
// Docker status and health value:
//
//	exited                                  -> exited
//	running, not health-gated               -> ready
//	running, health-gated, healthy          -> ready
//	running, health-gated, unhealthy        -> unhealthy
//	running, health-gated, starting/none/?  -> pending
//	created/restarting/paused, health-gated -> pending
//	created/restarting/paused, fixed wait   -> unverified
//	removing/dead/empty/anything else       -> unverified
//
// It differs from the recovery table (containerStatusToProvisionStatus) on
// purpose, and only in two places. Recovery maps removing and dead to Failed:
// it fails an attempt whose worker is gone and attributes nothing, so any
// cohort that can no longer become Ready is enough. This table decides
// whether a live attempt failed DEFINITELY, with an attribution, so it accepts
// only the workload's own account (an exit, a health verdict): removing and
// dead are the daemon's states, and stay unverified. Recovery maps paused to
// Ready because it re-adopts an existing cohort; a paused container cannot be
// verified as a started workload, so here it is pending behind a health check
// and unverified otherwise.
func classifyStartupInstance(info *ContainerInfo, healthGated bool) startupInstanceVerdict {
	if info == nil {
		return startupInstanceUnverified
	}
	switch strings.ToLower(info.Status) {
	case "exited":
		return startupInstanceExited
	case "running":
		if !healthGated {
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
		if healthGated {
			return startupInstancePending
		}
		return startupInstanceUnverified
	default:
		return startupInstanceUnverified
	}
}

// startupContainer is one member of a launch's exact Compose PS cohort.
type startupContainer struct {
	id          string
	service     string
	healthGated bool
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
	case startupVerdictExited, startupVerdictUnhealthy, startupVerdictNeverHealthy:
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

// inspectStartupCohort inspects every container of the cohort once. A failed
// read ends the pass unverified: a failed read is never a fact.
func (b *Backend) inspectStartupCohort(ctx context.Context, cohort startupCohort) (startupPass, *physicalOperationError) {
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
		pass.verdicts[i] = classifyStartupInstance(info, member.healthGated)
	}
	return pass, nil
}

func (m startupContainer) String() string {
	if m.service == "" {
		return "container " + leasesm.ShortID(m.id)
	}
	return "container " + leasesm.ShortID(m.id) + " of service " + m.service
}

// decideStartupPass turns one pass into a verdict. A positively observed exit
// is the most specific fact and wins over every other container's state; then
// an unhealthy report; then any unverifiable state. settled reports whether
// the fixed-wait containers' settle period has passed.
func (b *Backend) decideStartupPass(ctx context.Context, pass startupPass, settled bool) (startupWatch, bool) {
	for _, wanted := range []startupInstanceVerdict{
		startupInstanceExited, startupInstanceUnhealthy, startupInstanceUnverified,
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
				return unverifiedStartup(startupUnverifiedFailure(fmt.Errorf(
					"%s is in an unverifiable startup state (status: %s)", member, info.Status))), true
			}
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
// report healthy. Every pass inspects the whole cohort, so a container that
// exits after passing its own check is still seen, and Ready is reported only
// from one pass in which every container is ready.
//
// observeUntil, when set, is the observation's own deadline: a last pass then
// decides between a health check that never passed (definite) and an
// unverifiable state. Without it, the caller's context bounds the watch and
// its deadline leaves startup unverified.
func (b *Backend) watchStartup(
	ctx context.Context,
	cohort startupCohort,
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
			pass, failure := b.inspectStartupCohort(ctx, cohort)
			if failure != nil {
				return unverifiedStartup(failure)
			}
			return b.decideStartupDeadline(ctx, pass, !time.Now().Before(settleEnd))
		case <-wake.C:
		}
		pass, failure := b.inspectStartupCohort(ctx, cohort)
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

// observeStartup observes a provision's settled launch. A failure becomes a
// finding only through newStartupFailure, which re-checks every fact it needs;
// when it cannot, the failure stays unverified and the attempt Ambiguous.
func (b *Backend) observeStartup(
	ctx context.Context,
	mutations *storageMutations,
	launch settledLaunch,
	cohort startupCohort,
	logger *slog.Logger,
) startupObservation {
	watch := b.watchStartup(ctx, cohort, startupObservationDeadline(ctx, time.Now()), logger)
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
	return flattenStartupWatch(b.watchStartup(ctx, cohort, time.Time{}, logger))
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
	return flattenStartupWatch(b.watchStartup(ctx, cohort, time.Time{}, logger))
}

// waitForHealthy is the error-only wait for a compensating relaunch's health
// dependencies: every container must report healthy.
func (b *Backend) waitForHealthy(ctx context.Context, containerIDs []string, logger *slog.Logger) error {
	cohort := make(startupCohort, 0, len(containerIDs))
	for _, id := range containerIDs {
		cohort = append(cohort, startupContainer{id: id, healthGated: true})
	}
	return flattenStartupWatch(b.watchStartup(ctx, cohort, time.Time{}, logger))
}
