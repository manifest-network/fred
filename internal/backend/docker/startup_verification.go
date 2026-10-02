package docker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// Startup verification failures carry their curated tenant surface, authored
// here where the failure is observed (ENG-508, ENG-1125): no caller derives a
// message or a reason from an error's text. Only an observed exit is
// ContainerExited, the one reason the terminal budget can count (ENG-799). A
// health check that never passed is HealthCheckFailed, which never counts. A
// failed read, a cancellation, or a container that never reached a running
// state observes nothing about the tenant's workload and is Internal.

// healthPollInterval is the interval between health check polls during startup verification.
const healthPollInterval = 2 * time.Second

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

// verifyServiceStartup checks that one service's containers started. Services
// with an active health check are polled until healthy; the others must still
// be running after a fixed settle period. A failure carries its curated
// surface; nil means verified.
func (b *Backend) verifyServiceStartup(ctx context.Context, m *manifest.Manifest, containerIDs []string, logger *slog.Logger) *physicalOperationError {
	if m.HasActiveHealthCheck() {
		return b.awaitServiceHealthy(ctx, containerIDs, logger)
	}

	startupVerify := cmp.Or(b.cfg.StartupVerifyDuration, 5*time.Second)
	select {
	case <-ctx.Done():
		return startupCanceledFailure(fmt.Errorf("canceled during startup verification: %w", ctx.Err()))
	case <-time.After(startupVerify):
	}

	for i, containerID := range containerIDs {
		info, err := b.docker.InspectContainer(ctx, containerID)
		if err != nil {
			return startupUnverifiedFailure(fmt.Errorf("failed to verify container %d after startup: %w", i, err))
		}
		if containerStatusToProvisionStatus(info.Status) == backend.ProvisionStatusReady {
			continue
		}
		diag := b.containerFailureDiagnostics(ctx, containerID, containerInfoToInstanceState(info))
		if strings.EqualFold(info.Status, "exited") {
			return startupExitFailure(false, fmt.Errorf("container %d exited during startup (status: %s): %s", i, info.Status, diag))
		}
		return startupUnverifiedFailure(fmt.Errorf("container %d did not stay running during startup (status: %s): %s", i, info.Status, diag))
	}
	return nil
}

// awaitServiceHealthy polls container health status until all containers
// report "healthy". It fails as soon as a container is unhealthy or stops
// running. It is bounded by the caller's context (the ProvisionTimeout for a
// provision worker); reaching that deadline means the health check never
// passed.
func (b *Backend) awaitServiceHealthy(ctx context.Context, containerIDs []string, logger *slog.Logger) *physicalOperationError {
	pending := make(map[int]struct{}, len(containerIDs))
	for i := range containerIDs {
		pending[i] = struct{}{}
	}

	ticker := time.NewTicker(healthPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return startupHealthDeadlineFailure(fmt.Errorf("timed out waiting for containers to become healthy: %w", ctx.Err()))
			}
			return startupCanceledFailure(fmt.Errorf("canceled waiting for containers to become healthy: %w", ctx.Err()))
		case <-ticker.C:
			for i := range pending {
				info, err := b.docker.InspectContainer(ctx, containerIDs[i])
				if err != nil {
					return startupUnverifiedFailure(fmt.Errorf("failed to inspect container %d during health check: %w", i, err))
				}

				switch strings.ToLower(info.Status) {
				case "exited":
					diag := b.containerFailureDiagnostics(ctx, containerIDs[i], containerInfoToInstanceState(info))
					return startupExitFailure(true, fmt.Errorf("container %d exited while waiting for healthy (status: %s): %s", i, info.Status, diag))
				case "removing", "dead":
					diag := b.containerFailureDiagnostics(ctx, containerIDs[i], containerInfoToInstanceState(info))
					return startupUnverifiedFailure(fmt.Errorf("container %d stopped running while waiting for healthy (status: %s): %s", i, info.Status, diag))
				}

				switch info.Health {
				case HealthStatusHealthy:
					logger.Info("container healthy", "instance", i, "container_id", leasesm.ShortID(containerIDs[i]))
					delete(pending, i)
				case HealthStatusUnhealthy:
					diag := b.containerFailureDiagnostics(ctx, containerIDs[i], containerInfoToInstanceState(info))
					return startupUnhealthyFailure(fmt.Errorf("container %d reported unhealthy: %s", i, diag))
				default:
					// "starting" or other — keep polling
				}
			}

			if len(pending) == 0 {
				return nil
			}
		}
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

func flattenStartupFailure(failure *physicalOperationError) error {
	if failure == nil {
		return nil
	}
	return &replacementStartupError{message: failure.callback, cause: failure.cause}
}

// verifyStartup is the error-only form of verifyServiceStartup for
// replacements and compensation (see replacementStartupError).
func (b *Backend) verifyStartup(ctx context.Context, m *manifest.Manifest, containerIDs []string, logger *slog.Logger) error {
	return flattenStartupFailure(b.verifyServiceStartup(ctx, m, containerIDs, logger))
}

// waitForHealthy is the error-only form of awaitServiceHealthy for a
// compensating relaunch's health dependencies.
func (b *Backend) waitForHealthy(ctx context.Context, containerIDs []string, logger *slog.Logger) error {
	return flattenStartupFailure(b.awaitServiceHealthy(ctx, containerIDs, logger))
}
