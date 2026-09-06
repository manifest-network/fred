package docker

import (
	"cmp"
	"context"
	"time"
)

// defaultRecoveryDockerReadTimeout prevents a wedged Docker List/Inspect from
// permanently blocking Start, RefreshState, or operation-intent recovery. The
// budget is per Docker request, so every daemon read remains finite. Docker's
// client honors request-context cancellation.
const defaultRecoveryDockerReadTimeout = 30 * time.Second

// defaultBackendConstructionTimeout is the aggregate budget used by the
// convenience New constructor. NewWithContext remains available to callers
// that need a deployment-specific deadline, but New itself must never turn a
// wedged Docker identity read into a process-lifetime startup hang.
const defaultBackendConstructionTimeout = defaultRecoveryDockerReadTimeout

const (
	// A fleet may require many idempotent recovery and stopped-adoption steps, so
	// startup gets a generous process-independent budget. Exhaustion is a typed
	// startup failure and the next launch resumes from durable evidence.
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
	return context.WithTimeout(parent, b.recoveryDockerReadBudget())
}

func (b *Backend) recoveryDockerReadBudget() time.Duration {
	return cmp.Or(b.recoveryDockerReadTimeout, defaultRecoveryDockerReadTimeout)
}

func (b *Backend) startupRecoveryContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(b.stopCtx, b.startupRecoveryBudget())
}

func (b *Backend) startupRecoveryBudget() time.Duration {
	// A non-zero value is an explicit whole-startup override used by bounded
	// callers and tests. Production leaves it zero, so size the ordinary
	// aggregate as the saturating sum of every sequential phase's local maximum.
	// This guarantees that an operation admitted just before a clock rollback can
	// still receive one fresh bounded convergence window after earlier phases,
	// without making any individual phase unbounded.
	if b.startupRecoveryTimeout > 0 {
		return b.startupRecoveryTimeout
	}
	phaseBudget := b.startupPhaseBudget()
	return saturatingDurationSum(
		defaultStartupVolumeMutationTimeout, // recover interrupted mutations
		defaultStartupVolumeMutationTimeout, // prove the clean volume inventory
		defaultStartupRecoveryTimeout,       // rebuild Docker and durable state
		b.startupOperationRecoveryBudget(),  // operation-intent convergence
		phaseBudget,                         // retention reconciliation
		phaseBudget,                         // quota reconciliation
		phaseBudget,                         // retention reap
		b.recoveryDockerReadBudget(),        // final storage-identity proof
	)
}

func startupVolumeMutationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, defaultStartupVolumeMutationTimeout)
}

// startupStateRecoveryContext keeps ordinary inventory/migration recovery on
// its historical finite bound even when the aggregate was enlarged for a
// configured provision timeout. Otherwise an unrelated pending-operation knob
// could let a wedged fleet rebuild consume that entire longer window.
func (b *Backend) startupStateRecoveryContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, defaultStartupRecoveryTimeout)
}

func (b *Backend) startupPhaseContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, b.startupPhaseBudget())
}

// startupOperationRecoveryContext allows a self-advancing interrupted
// provision to use the recovery process's configured bound. The phase has its
// own cap within the aggregate startup context: all intents share that single
// child, so several rows cannot multiply it into an unbounded wait.
func (b *Backend) startupOperationRecoveryContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, b.startupOperationRecoveryBudget())
}

func (b *Backend) startupOperationRecoveryBudget() time.Duration {
	provisionTimeout := cmp.Or(b.cfg.ProvisionTimeout, 10*time.Minute)
	return max(
		b.startupPhaseBudget(),
		operationRecoveryPhaseBudget(provisionTimeout, b.recoveryDockerReadBudget()),
	)
}

func operationRecoveryPhaseBudget(provisionTimeout, dockerReadTimeout time.Duration) time.Duration {
	return saturatingDurationSum(provisionTimeout, dockerReadTimeout, volumeCleanupTimeout)
}

func saturatingDurationSum(parts ...time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	var total time.Duration
	for _, part := range parts {
		if part <= 0 {
			continue
		}
		if total > maxDuration-part {
			return maxDuration
		}
		total += part
	}
	return total
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
