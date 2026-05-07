// Package docker implements the Backend interface for Docker container
// provisioning. It is the production backend bundled with Fred.
//
// For operators and tenants, see [the Docker backend README] for the full
// configuration reference, HTTP API, lease state machine, callback protocol,
// and Traefik integration.
//
// # Architecture overview (for developers)
//
// The package is organized around a single concurrency primitive: the
// per-lease actor. Every active lease owns one goroutine that serializes
// all state-mutating operations for that lease through a stateless state
// machine (see lease_sm.go and lease_actor.go). All Docker calls happen
// outside any shared mutex; linearization comes from the actor's inbox.
//
// The actor model is what gives the backend its key properties:
//
//   - No held locks during slow I/O (image pull, container create/start)
//   - Deterministic preemption — a Deprovision arriving mid-provisioning
//     cancels the in-flight worker via OnExit and transitions cleanly
//   - Blast-radius-contained panics — recover() in each handler keeps
//     unrelated leases unaffected
//   - One terminal callback per lease — emission lives only in SM entry
//     actions, never in worker goroutines
//
// # Major components
//
//   - lease_actor.go, lease_sm.go: the per-lease actor and its state machine
//   - work_barrier.go: per-actor worker reference counter (used by OnExit
//     to wait for cancelled goroutines before completing the transition)
//   - provision.go, deprovision.go, restart_update.go: the lifecycle
//     workers that the actor spawns for each long-running operation
//   - recover.go: state recovery from Docker labels on startup and during
//     each reconciliation cycle (via RefreshState)
//   - compose.go, compose_project.go: Compose-based stack provisioning
//   - reconcile_custom_domain.go: Traefik label sync for tenant custom domains
//   - volume.go (+ volume_btrfs.go, volume_xfs.go, volume_zfs.go):
//     filesystem-specific quota enforcement for stateful SKUs
//   - ingress.go: Traefik label generation for routable ports
//   - metrics.go: Prometheus metrics under fred_docker_backend_*
//
// # Container hardening
//
// Every container is created with: dropped capabilities, no-new-privileges,
// read-only rootfs, tmpfs for /tmp and /run, PID limits, no swap, restart
// policy disabled (for crash detection), and per-tenant network isolation.
// See the README for the full list and operator-facing knobs.
//
// [the Docker backend README]: https://github.com/manifest-network/fred/blob/main/internal/backend/docker/README.md
package docker
