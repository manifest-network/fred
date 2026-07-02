package docker

import (
	"fmt"
	"log/slog"

	"golang.org/x/sys/unix"
)

// daemonCanSetQuotas reports whether an exec'd quota tool (xfs_quota / btrfs /
// zfs) launched by this daemon will run with CAP_SYS_ADMIN. That holds iff the
// daemon runs as root (euid 0) OR carries CAP_SYS_ADMIN in its AMBIENT set.
//
// It deliberately does NOT test the effective set. The privileged work is done
// by exec'd children, and across execve a child with no file capabilities gains
// a capability only via the parent's ambient set (or the root-euid rule) — never
// via the parent's effective set. So an effective-only grant (`setcap
// cap_sys_admin+ep` on the daemon binary, which clears the ambient set) would
// pass an effective-set check yet leave the exec'd tools unprivileged, silently
// re-introducing the exact unenforced-quota bug this guard exists to prevent.
// Pure Go / CGO-free. See ENG-454.
func daemonCanSetQuotas() (bool, error) {
	if unix.Geteuid() == 0 {
		return true, nil
	}
	return ambientHasCap(unix.CAP_SYS_ADMIN)
}

// ambientHasCap reports whether `capability` is in the calling thread's ambient
// capability set, via prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_IS_SET, cap).
func ambientHasCap(capability uintptr) (bool, error) {
	r, _, errno := unix.Syscall6(unix.SYS_PRCTL,
		uintptr(unix.PR_CAP_AMBIENT), uintptr(unix.PR_CAP_AMBIENT_IS_SET), capability, 0, 0, 0)
	if errno != 0 {
		return false, fmt.Errorf("prctl(PR_CAP_AMBIENT_IS_SET, %d): %w", capability, errno)
	}
	return r == 1, nil
}

// requireCapSysAdmin returns a fatal, actionable error when the daemon cannot set
// quotas (not root and no ambient CAP_SYS_ADMIN). It is called from the
// Validate() of backends whose quota operations REQUIRE that capability with no
// delegation alternative — xfs (quotactl Q_XSETQLIM) and btrfs (subvolume/qgroup
// ioctls). It is deliberately NOT called for zfs (which supports `zfs allow`
// delegation, so a cap check would wrongly reject a properly-delegated non-root
// host) or for the noop backend (no privileged ops). Failing fast at startup
// beats failing every provision at runtime with an unenforced disk cap.
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
