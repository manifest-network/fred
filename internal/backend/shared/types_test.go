package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSKUProfile_Validate(t *testing.T) {
	tests := []struct {
		name    string
		profile SKUProfile
		wantErr string
	}{
		{name: "valid with disk", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}},
		{name: "valid without disk", profile: SKUProfile{CPUCores: 1, MemoryMB: 512}},
		{name: "valid zero disk", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 0}},
		{name: "valid with bandwidth", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, BandwidthMbps: 100}},
		{name: "valid zero bandwidth", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, BandwidthMbps: 0}},
		{name: "valid with burst and latency", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, BandwidthMbps: 100, BurstKB: 64, LatencyMs: 50}},
		{name: "negative disk_mb", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: -1}, wantErr: "disk_mb must be non-negative"},
		{name: "negative bandwidth_mbps", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, BandwidthMbps: -1}, wantErr: "bandwidth_mbps must be non-negative"},
		{name: "negative burst_kb", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, BurstKB: -1}, wantErr: "burst_kb must be non-negative"},
		{name: "excessive burst_kb", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, BurstKB: 1_048_577}, wantErr: "burst_kb must be <= 1048576"},
		{name: "negative latency_ms", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, LatencyMs: -1}, wantErr: "latency_ms must be non-negative"},
		{name: "excessive latency_ms", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, LatencyMs: 10_001}, wantErr: "latency_ms must be <= 10000"},
		{name: "excessive bandwidth_mbps", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, BandwidthMbps: 100_001}, wantErr: "bandwidth_mbps must be <= 100000"},
		{name: "max valid bandwidth", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, BandwidthMbps: 100_000}},
		{name: "max valid burst", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, BurstKB: 1_048_576}},
		{name: "max valid latency", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, LatencyMs: 10_000}},
		{name: "zero cpu", profile: SKUProfile{CPUCores: 0, MemoryMB: 512, DiskMB: 1024}, wantErr: "cpu_cores"},
		{name: "negative cpu", profile: SKUProfile{CPUCores: -1, MemoryMB: 512, DiskMB: 1024}, wantErr: "cpu_cores"},
		{name: "zero memory", profile: SKUProfile{CPUCores: 1, MemoryMB: 0, DiskMB: 1024}, wantErr: "memory_mb"},
		{name: "zero value", profile: SKUProfile{}, wantErr: "cpu_cores"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.profile.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestTenantQuotaConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		quota   TenantQuotaConfig
		wantErr string
	}{
		{name: "valid", quota: TenantQuotaConfig{MaxCPUCores: 4, MaxMemoryMB: 4096, MaxDiskMB: 8192}},
		{name: "valid with bandwidth", quota: TenantQuotaConfig{MaxCPUCores: 4, MaxMemoryMB: 4096, MaxDiskMB: 8192, MaxBandwidthMbps: 500}},
		{name: "valid zero bandwidth", quota: TenantQuotaConfig{MaxCPUCores: 4, MaxMemoryMB: 4096, MaxDiskMB: 8192, MaxBandwidthMbps: 0}},
		{name: "zero cpu", quota: TenantQuotaConfig{MaxCPUCores: 0, MaxMemoryMB: 4096, MaxDiskMB: 8192}, wantErr: "max_cpu_cores"},
		{name: "negative cpu", quota: TenantQuotaConfig{MaxCPUCores: -1, MaxMemoryMB: 4096, MaxDiskMB: 8192}, wantErr: "max_cpu_cores"},
		{name: "zero memory", quota: TenantQuotaConfig{MaxCPUCores: 4, MaxMemoryMB: 0, MaxDiskMB: 8192}, wantErr: "max_memory_mb"},
		{name: "zero disk", quota: TenantQuotaConfig{MaxCPUCores: 4, MaxMemoryMB: 4096, MaxDiskMB: 0}, wantErr: "max_disk_mb"},
		{name: "negative bandwidth", quota: TenantQuotaConfig{MaxCPUCores: 4, MaxMemoryMB: 4096, MaxDiskMB: 8192, MaxBandwidthMbps: -1}, wantErr: "max_bandwidth_mbps must be non-negative"},
		{name: "zero value", quota: TenantQuotaConfig{}, wantErr: "max_cpu_cores"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.quota.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
