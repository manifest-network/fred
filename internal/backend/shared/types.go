// Package shared provides backend-agnostic components that can be reused
// across different backend implementations (Docker, Kubernetes, Nomad, etc.).
//
// # What lives here
//
//   - SKUProfile and the shared SKU types (types.go), and ResourcePool and
//     ResourceStats (resources.go) — the resource pool primitives used by
//     every backend that tracks CPU/memory/disk
//   - Registry helpers (registry.go) — image-registry parsing + allowlist
//     validation (ParseRegistry, IsImageAllowed, ValidateImage) used by
//     substrate adapters to enforce the backend's configured registry allowlist
//   - The identity-bound authoritative journals, each embedding the private
//     boltStore (bolt_store.go): CallbackStore (callbacks.go) holds the
//     operation, maintenance and close intents, the per-lease callback outbox
//     and the compact terminal receipts; ReleaseStore (releases.go) holds
//     release history with typed or legacy runtime authority; RetentionStore
//     (retention.go) holds retained-volume records. DiagnosticsStore
//     (diagnostics.go) persists failure diagnostics for leases no longer in
//     memory.
//   - Settlement capabilities bound to one exact journal pair, the only way to
//     change that authority: OperationSettlement, MaintenanceSettlement,
//     CloseSettlement, RestoreSettlement and ReleaseBackfiller, plus
//     RecoveryCoordinator for lease-scoped recovery
//   - CallbackPublisher (callback_publisher.go) — commits each operation and
//     maintenance result and its outbox entry in one transaction (close
//     results commit through CloseSettlement); CallbackSender
//     (callback_sender.go) only signs, delivers and replays rows that are
//     already durable
//
// All of these are consumed by the docker backend and are usable by any
// future in-process backend (Kubernetes, Nomad, etc.). An HTTP-only backend in
// a separate process implements the same callback contract (BACKEND_GUIDE.md)
// with its own durable outbox; CallbackSender requires an identity-bound
// CallbackStore, so it cannot be reused on its own.
package shared

import (
	"fmt"
	"math"
)

// SKUProfile defines resource limits for a SKU.
type SKUProfile struct {
	CPUCores float64 `yaml:"cpu_cores"`
	MemoryMB int64   `yaml:"memory_mb"`
	DiskMB   int64   `yaml:"disk_mb"`
}

// Validate checks that the profile's resource values are valid.
func (p SKUProfile) Validate() error {
	if math.IsNaN(p.CPUCores) || math.IsInf(p.CPUCores, 0) {
		return fmt.Errorf("cpu_cores must be finite")
	}
	if p.CPUCores <= 0 {
		return fmt.Errorf("cpu_cores must be positive")
	}
	if p.MemoryMB <= 0 {
		return fmt.Errorf("memory_mb must be positive")
	}
	if p.DiskMB < 0 {
		return fmt.Errorf("disk_mb must be non-negative")
	}
	return nil
}

// TenantQuotaConfig configures per-tenant resource limits. When set, each
// tenant's aggregate resource usage is capped; MaxDiskMB is physical admission
// disk and may include substrate-specific ephemeral scratch.
type TenantQuotaConfig struct {
	MaxCPUCores float64 `yaml:"max_cpu_cores"`
	MaxMemoryMB int64   `yaml:"max_memory_mb"`
	MaxDiskMB   int64   `yaml:"max_disk_mb"`
}

// Validate checks that all quota values are positive.
func (q TenantQuotaConfig) Validate() error {
	if math.IsNaN(q.MaxCPUCores) || math.IsInf(q.MaxCPUCores, 0) {
		return fmt.Errorf("max_cpu_cores must be finite")
	}
	if q.MaxCPUCores <= 0 {
		return fmt.Errorf("max_cpu_cores must be positive")
	}
	if q.MaxMemoryMB <= 0 {
		return fmt.Errorf("max_memory_mb must be positive")
	}
	if q.MaxDiskMB <= 0 {
		return fmt.Errorf("max_disk_mb must be positive")
	}
	return nil
}
