// Package shared provides backend-agnostic components that can be reused
// across different backend implementations (Docker, Kubernetes, Nomad, etc.).
package shared

import "fmt"

// SKUProfile defines resource limits for a SKU.
type SKUProfile struct {
	CPUCores      float64 `yaml:"cpu_cores"`
	MemoryMB      int64   `yaml:"memory_mb"`
	DiskMB        int64   `yaml:"disk_mb"`
	BandwidthMbps int64   `yaml:"bandwidth_mbps"` // 0 = unlimited (not pool-tracked)
	BurstKB       int     `yaml:"burst_kb"`       // 0 = auto-calculated from rate
	LatencyMs     int     `yaml:"latency_ms"`     // 0 = default (50ms)
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
	if p.BandwidthMbps < 0 {
		return fmt.Errorf("bandwidth_mbps must be non-negative")
	}
	if p.BandwidthMbps > 100_000 {
		return fmt.Errorf("bandwidth_mbps must be <= 100000 (100 Gbps)")
	}
	if p.BurstKB < 0 {
		return fmt.Errorf("burst_kb must be non-negative")
	}
	if p.BurstKB > 1_048_576 {
		return fmt.Errorf("burst_kb must be <= 1048576 (1 GB)")
	}
	if p.LatencyMs < 0 {
		return fmt.Errorf("latency_ms must be non-negative")
	}
	if p.LatencyMs > 10_000 {
		return fmt.Errorf("latency_ms must be <= 10000 (10 seconds)")
	}
	return nil
}

// TenantQuotaConfig configures per-tenant resource limits.
// When set, each tenant's aggregate resource usage is capped.
// CPU, memory, and disk quotas must be positive. MaxBandwidthMbps is optional:
// 0 means no per-tenant bandwidth quota enforcement.
type TenantQuotaConfig struct {
	MaxCPUCores      float64 `yaml:"max_cpu_cores"`
	MaxMemoryMB      int64   `yaml:"max_memory_mb"`
	MaxDiskMB        int64   `yaml:"max_disk_mb"`
	MaxBandwidthMbps int64   `yaml:"max_bandwidth_mbps"` // 0 = no bandwidth quota
}

// Validate checks that quota values are valid.
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
	if q.MaxBandwidthMbps < 0 {
		return fmt.Errorf("max_bandwidth_mbps must be non-negative")
	}
	return nil
}
