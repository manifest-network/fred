package docker

import (
	"fmt"
	"log/slog"
)

// requireCapSysAdmin returns a fatal, actionable error when the daemon cannot set
// quotas (not root and no ambient CAP_SYS_ADMIN). It is called from the
// Validate() of backends whose quota operations REQUIRE that capability with no
// delegation alternative — xfs (quotactl Q_XSETQLIM) and btrfs (subvolume/qgroup
// ioctls). It is deliberately NOT called for zfs (which supports `zfs allow`
// delegation, so a cap check would wrongly reject a properly-delegated non-root
// host) or for the noop backend (no privileged ops). Failing fast at startup
// beats failing every provision at runtime with an unenforced disk cap.
//
// The privilege probe (daemonCanSetQuotas) is Linux-only; see capability_linux.go
// and capability_nonlinux.go.
func requireCapSysAdmin(backendKind string, logger *slog.Logger) error {
	canSet, err := daemonCanSetQuotas()
	return capGuardResult(backendKind, canSet, err, logger)
}

// capGuardResult maps a capability-probe result to the guard's decision. Pure
// (no syscall) so the fatal/pass/warn logic is unit-testable without privilege:
//   - probeErr != nil (inconclusive): warn + proceed, so an unexpected prctl
//     failure never bricks startup;
//   - !canSet (authoritative): fatal, actionable error;
//   - canSet: nil.
func capGuardResult(backendKind string, canSet bool, probeErr error, logger *slog.Logger) error {
	if probeErr != nil {
		logger.Warn("could not determine quota-set privilege; proceeding without the startup capability guard",
			"backend", backendKind, "error", probeErr)
		return nil
	}
	if !canSet {
		return fmt.Errorf("docker-backend cannot set %s volume quotas: it is not running as root and lacks "+
			"CAP_SYS_ADMIN in its ambient set; grant it via systemd AmbientCapabilities=CAP_SYS_ADMIN "+
			"(a plain `setcap cap_sys_admin+ep` on the binary does NOT propagate to the exec'd quota tools) "+
			"— refusing to start so per-volume disk_mb limits are enforced, not silently skipped", backendKind)
	}
	return nil
}
