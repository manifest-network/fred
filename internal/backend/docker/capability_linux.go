//go:build linux

package docker

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// daemonCanSetQuotas reports whether an exec'd quota tool (xfs_quota / btrfs /
// zfs) launched by this daemon will run with CAP_SYS_ADMIN.
//
// An exec'd child gets the capability only if it lands in the child's permitted
// set. Across execve, for a child with no file capabilities:
//   - a ROOT (euid 0) parent's child gets permitted = (all caps) & BOUNDING set,
//     so the child has CAP_SYS_ADMIN iff it is in the parent's bounding set — a
//     hardened root unit whose CapabilityBoundingSet omits it would still spawn
//     unprivileged quota tools;
//   - a NON-ROOT parent's child gets permitted = the parent's AMBIENT set.
//
// So: root needs the cap in its bounding set; non-root needs it in its ambient
// set. Testing the EFFECTIVE set would be wrong — it does not propagate to such a
// child, so an effective-only grant (`setcap cap_sys_admin+ep` on the binary,
// which clears ambient) would false-pass. Pure Go / CGO-free. See ENG-454.
func daemonCanSetQuotas() (bool, error) {
	if unix.Geteuid() == 0 {
		return boundingHasCap(unix.CAP_SYS_ADMIN)
	}
	return ambientHasCap(unix.CAP_SYS_ADMIN)
}

// boundingHasCap reports whether `capability` is in the calling thread's
// capability bounding set, via prctl(PR_CAPBSET_READ, cap).
func boundingHasCap(capability uintptr) (bool, error) {
	r, _, errno := unix.Syscall6(unix.SYS_PRCTL,
		uintptr(unix.PR_CAPBSET_READ), capability, 0, 0, 0, 0)
	if errno != 0 {
		return false, fmt.Errorf("prctl(PR_CAPBSET_READ, %d): %w", capability, errno)
	}
	return r == 1, nil
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
