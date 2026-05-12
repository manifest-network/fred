// Package docker implements the Backend interface for Docker container
// provisioning. It is the production backend bundled with Fred.
//
// For operators and tenants, see the README.md alongside this package
// (internal/backend/docker/README.md) for the full configuration reference,
// HTTP API, lease state machine, callback protocol, and Traefik integration.
//
// # Architecture overview (for developers)
//
// The package is organized around a single concurrency primitive: the
// per-lease actor. Every active lease owns one goroutine that serializes
// all state-mutating operations for that lease through a stateless state
// machine. The actor and SM implementations are substrate-agnostic and
// live in internal/backend/shared/leasesm; this package supplies the
// Docker-specific seams via the closure-builder factory in
// lease_actor_factory.go. All Docker calls happen outside any shared
// mutex; linearization comes from the actor's inbox.
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
//   - internal/backend/shared/leasesm: per-lease actor + state machine
//     (substrate-agnostic; consumed by every backend, not just Docker)
//   - lease_actor_factory.go, lease_actor_routing.go: factory wiring
//     Docker dependencies into leasesm.NewLeaseActor, plus Backend-side
//     routing/dispatch around the actor inbox (b.actors map, routeToLease,
//     DebugActors)
//   - leasesm_adapters.go, leasesm_metrics.go: Docker implementations of
//     leasesm.InstanceInspector / DiagnosticsGatherer / LeaseProvisionStore
//     / SMMetrics
//   - internal/backend/shared/workbarrier: per-actor worker reference counter
//     (used by OnExit to wait for canceled goroutines before completing the
//     transition)
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
package docker
