// Package docker implements the Backend interface for Docker container
// provisioning. It is the production backend bundled with Fred.
//
// For operators and tenants, see the README.md alongside this package
// (internal/backend/docker/README.md) for the full configuration reference,
// HTTP API, lease state machine, callback protocol, and Traefik integration.
//
// # Architecture overview (for developers)
//
// The package is organized around one main concurrency primitive: the
// per-lease actor. A lease's actor is created on its first message and
// serializes that lease's lifecycle commands through a stateless state
// machine. The actor and SM implementations are substrate-agnostic and
// live in internal/backend/shared/leasesm; this package supplies the
// Docker-specific seams via the closure-builder factory in
// lease_actor_factory.go. Some work is serialized differently: recoverState
// holds the recovery mutexes across its Docker inventory read, tenant
// networks and managed volumes are created under per-tenant and per-volume
// stripe locks, and operation-intent and maintenance recovery replace lease
// projections directly instead of through an actor.
//
// The actor model is what gives the backend its key properties:
//
//   - No held locks during slow I/O (image pull, container create/start)
//   - Deterministic preemption — a Deprovision arriving mid-provisioning
//     cancels the in-flight worker via OnExit and transitions cleanly
//   - Blast-radius-contained panics — recover() in each handler keeps
//     unrelated leases unaffected
//   - One durable terminal result per operation — each result is committed
//     together with its outbox entry in one bbolt transaction under the
//     per-lease journal gate: operation and maintenance results through
//     shared.CallbackPublisher, whether they come from an SM entry action,
//     admission, recovery or maintenance settlement, and close results through
//     shared.CloseSettlement. Delivery then replays until acknowledged
//
// # Major components
//
//   - internal/backend/shared/leasesm: per-lease actor + state machine
//     (substrate-agnostic; Docker is its only consumer today, since the k3s
//     scaffold deliberately does not use it)
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
//   - provision.go, deprovision.go, restart_update.go, restore.go: the lifecycle
//     workers that the actor spawns for each long-running operation
//   - recover.go: state recovery from Docker labels on startup and on each
//     pass of the backend's own periodic reconcile loop
//   - compose.go, compose_project.go: Compose-based stack provisioning
//   - reconcile_custom_domain.go: Traefik label sync for tenant custom domains
//   - volume.go (+ volume_btrfs.go, volume_xfs.go, volume_zfs.go):
//     filesystem-specific quota enforcement for durable SKU volumes and
//     optional diskless writable-path scratch
//   - ingress.go: Traefik label generation for routable ports
//   - metrics.go: Prometheus metrics under fred_docker_backend_*
//
// # Container hardening
//
// Every container is created with: dropped capabilities, no-new-privileges,
// fred's tenant seccomp profile (derived from Docker's default), read-only
// rootfs, tmpfs for /tmp and /run, PID limits, no swap, restart policy
// disabled (for crash detection), and per-tenant network isolation.
// See the README for the full list and operator-facing knobs.
package docker
