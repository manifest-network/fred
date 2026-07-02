//go:build linux

package docker

import (
	"fmt"

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
