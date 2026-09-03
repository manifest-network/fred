package docker

import (
	"cmp"
	"context"
	"time"
)

// defaultRecoveryDockerReadTimeout prevents a wedged Docker List/Inspect from
// permanently blocking Start, RefreshState, or operation-intent recovery. The
// budget is per Docker request: recover-time legacy migration retains its
// independent, longer readiness budget while every daemon read is still
// finite. Docker's client honors request-context cancellation.
const defaultRecoveryDockerReadTimeout = 30 * time.Second

// defaultBackendConstructionTimeout is the aggregate budget used by the
// convenience New constructor. NewWithContext remains available to callers
// that need a deployment-specific deadline, but New itself must never turn a
// wedged Docker identity read into a process-lifetime startup hang.
const defaultBackendConstructionTimeout = defaultRecoveryDockerReadTimeout

const (
	// A legacy fleet may require many idempotent migrations, so startup gets a
	// generous process-independent budget. Exhaustion is a typed startup failure
	// and the next launch resumes from durable evidence.
	defaultStartupRecoveryTimeout = 30 * time.Minute
	// Interrupted volume mutation recovery runs before Docker/container recovery
	// and never waits for a container stop grace. Give each of its two phases a
	// fixed local cap while still deriving it from the aggregate startup context.
	// Keeping this separate from startupPhaseBudget prevents an unusually large
	// ContainerStopTimeout from inflating filesystem-only work.
	defaultStartupVolumeMutationTimeout = 2 * time.Minute
	// Best-effort fleet sweeps get a smaller aggregate budget. Nested per-call
	// deadlines are capped by this parent instead of multiplying by object count.
	defaultStartupPhaseTimeout = 2 * time.Minute
)

func (b *Backend) recoveryDockerReadContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, cmp.Or(b.recoveryDockerReadTimeout, defaultRecoveryDockerReadTimeout))
}

func (b *Backend) startupRecoveryContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(b.stopCtx, cmp.Or(b.startupRecoveryTimeout, defaultStartupRecoveryTimeout))
}

func startupVolumeMutationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, defaultStartupVolumeMutationTimeout)
}

func (b *Backend) startupPhaseContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, b.startupPhaseBudget())
}

func (b *Backend) startupPhaseBudget() time.Duration {
	phaseTimeout := cmp.Or(b.startupPhaseTimeout, defaultStartupPhaseTimeout)
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, defaultContainerStopTimeout)
	// A startup finalizer may need to give one container its configured Docker
	// stop grace. Never create a phase context that is already shorter than that
	// single operation. The phase deadline remains aggregate, so fleet-sized
	// loops are still bounded rather than multiplying the stop timeout by N.
	return max(phaseTimeout, stopTimeout)
}

func (b *Backend) listManagedContainersForRecovery(ctx context.Context) ([]ContainerInfo, error) {
	readCtx, cancel := b.recoveryDockerReadContext(ctx)
	defer cancel()
	return b.docker.ListManagedContainers(readCtx)
}

func (b *Backend) listManagedContainersStrictForRecovery(ctx context.Context) ([]ContainerInfo, error) {
	readCtx, cancel := b.recoveryDockerReadContext(ctx)
	defer cancel()
	return b.docker.ListManagedContainersStrict(readCtx)
}

func (b *Backend) inspectContainerForRecovery(ctx context.Context, containerID string) (*ContainerInfo, error) {
	readCtx, cancel := b.recoveryDockerReadContext(ctx)
	defer cancel()
	return b.docker.InspectContainer(readCtx, containerID)
}
