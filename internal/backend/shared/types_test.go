package shared

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
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
		{name: "negative disk_mb", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: -1}, wantErr: "disk_mb must be non-negative"},
		{name: "zero cpu", profile: SKUProfile{CPUCores: 0, MemoryMB: 512, DiskMB: 1024}, wantErr: "cpu_cores"},
		{name: "negative cpu", profile: SKUProfile{CPUCores: -1, MemoryMB: 512, DiskMB: 1024}, wantErr: "cpu_cores"},
		{name: "NaN cpu", profile: SKUProfile{CPUCores: math.NaN(), MemoryMB: 512, DiskMB: 1024}, wantErr: "cpu_cores must be finite"},
		{name: "positive infinity cpu", profile: SKUProfile{CPUCores: math.Inf(1), MemoryMB: 512, DiskMB: 1024}, wantErr: "cpu_cores must be finite"},
		{name: "negative infinity cpu", profile: SKUProfile{CPUCores: math.Inf(-1), MemoryMB: 512, DiskMB: 1024}, wantErr: "cpu_cores must be finite"},
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
		{name: "zero cpu", quota: TenantQuotaConfig{MaxCPUCores: 0, MaxMemoryMB: 4096, MaxDiskMB: 8192}, wantErr: "max_cpu_cores"},
		{name: "negative cpu", quota: TenantQuotaConfig{MaxCPUCores: -1, MaxMemoryMB: 4096, MaxDiskMB: 8192}, wantErr: "max_cpu_cores"},
		{name: "NaN cpu", quota: TenantQuotaConfig{MaxCPUCores: math.NaN(), MaxMemoryMB: 4096, MaxDiskMB: 8192}, wantErr: "max_cpu_cores must be finite"},
		{name: "positive infinity cpu", quota: TenantQuotaConfig{MaxCPUCores: math.Inf(1), MaxMemoryMB: 4096, MaxDiskMB: 8192}, wantErr: "max_cpu_cores must be finite"},
		{name: "negative infinity cpu", quota: TenantQuotaConfig{MaxCPUCores: math.Inf(-1), MaxMemoryMB: 4096, MaxDiskMB: 8192}, wantErr: "max_cpu_cores must be finite"},
		{name: "zero memory", quota: TenantQuotaConfig{MaxCPUCores: 4, MaxMemoryMB: 0, MaxDiskMB: 8192}, wantErr: "max_memory_mb"},
		{name: "zero disk", quota: TenantQuotaConfig{MaxCPUCores: 4, MaxMemoryMB: 4096, MaxDiskMB: 0}, wantErr: "max_disk_mb"},
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

func TestSKUProfile_ValidateRejectsYAMLNonFiniteCPU(t *testing.T) {
	for _, scalar := range []string{".nan", ".inf", "-.inf"} {
		t.Run(scalar, func(t *testing.T) {
			var profile SKUProfile
			err := yaml.Unmarshal([]byte("cpu_cores: "+scalar+"\nmemory_mb: 512\ndisk_mb: 1024\n"), &profile)
			require.NoError(t, err, "YAML parser must recognize the scalar for this regression proof")
			require.ErrorContains(t, profile.Validate(), "cpu_cores must be finite")
		})
	}
}
