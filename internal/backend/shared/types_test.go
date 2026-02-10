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
		{name: "negative disk_mb", profile: SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: -1}, wantErr: "disk_mb must be non-negative"},
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
		{name: "zero cpu", quota: TenantQuotaConfig{MaxCPUCores: 0, MaxMemoryMB: 4096, MaxDiskMB: 8192}, wantErr: "max_cpu_cores"},
		{name: "negative cpu", quota: TenantQuotaConfig{MaxCPUCores: -1, MaxMemoryMB: 4096, MaxDiskMB: 8192}, wantErr: "max_cpu_cores"},
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
