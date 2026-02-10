// Package shared provides backend-agnostic components that can be reused
// across different backend implementations (Docker, Kubernetes, Nomad, etc.).
package shared

import "fmt"

// SKUProfile defines resource limits for a SKU.
type SKUProfile struct {
	CPUCores float64 `yaml:"cpu_cores"`
	MemoryMB int64   `yaml:"memory_mb"`
	DiskMB   int64   `yaml:"disk_mb"`
}

// Validate checks that the profile's resource values are valid.
func (p SKUProfile) Validate() error {
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

// TenantQuotaConfig configures per-tenant resource limits.
// When set, each tenant's aggregate resource usage is capped.
type TenantQuotaConfig struct {
	MaxCPUCores float64 `yaml:"max_cpu_cores"`
	MaxMemoryMB int64   `yaml:"max_memory_mb"`
	MaxDiskMB   int64   `yaml:"max_disk_mb"`
}

// Validate checks that all quota values are positive.
func (q TenantQuotaConfig) Validate() error {
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
