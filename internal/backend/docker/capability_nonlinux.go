//go:build !linux

package docker

import "errors"

// daemonCanSetQuotas is a non-Linux stub: the ambient-capability / euid probe
// (prctl(PR_CAP_AMBIENT)) is Linux-only. It returns an inconclusive error so
// capGuardResult warns and proceeds. The docker backend only runs on Linux
// (project quotas, Docker, XFS/btrfs/zfs) — this stub exists solely to keep the
// package buildable for cross-platform dev tooling (gopls, `go build`/`go vet`
// on macOS).
func daemonCanSetQuotas() (bool, error) {
	return false, errors.New("quota-set capability probe is only supported on linux")
}
