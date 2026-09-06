package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

type kindedMockVolumeManager struct {
	*mockVolumeManager
	kind string
}

const (
	quotaRollbackSourceLease      = "0192f1a0-1111-4abc-8def-000000000801"
	quotaRollbackDestinationLease = "0192f1a0-2222-4abc-8def-000000000802"
)

func (m *kindedMockVolumeManager) Kind() string { return m.kind }

func quotaRollbackEntry() shared.RetentionEntry {
	const oldDiskMB = int64(100)
	return shared.RetentionEntry{
		OriginalLeaseUUID: quotaRollbackSourceLease,
		NewLeaseUUID:      quotaRollbackDestinationLease,
		Tenant:            "tenant-a",
		ProviderUUID:      nominalDockerProviderUUID,
		Items: []backend.LeaseItem{{
			SKU: "old-tier", ServiceName: "app", Quantity: 1,
		}},
		ResourceProfiles: []shared.SKUResourceSnapshot{{
			SKU: "old-tier", CPUCores: 1, MemoryMB: 512, DiskMB: oldDiskMB,
		}},
		RetainedVolumeNames: []string{
			retainedName(canonicalVolumeName(quotaRollbackSourceLease, "app", 0)),
		},
		Status:     shared.RetentionStatusRestoring,
		Generation: 7,
		CreatedAt:  time.Now(),
	}
}

func quotaRollbackProvision() map[string]*provision {
	return map[string]*provision{
		quotaRollbackDestinationLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: quotaRollbackDestinationLease,
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusProvisioning,
			Items: []backend.LeaseItem{{
				SKU: "docker-large", ServiceName: "app", Quantity: 1,
			}},
		}},
	}
}

// All supported volume managers implement quota changes through the same
// volumeManager contract. Pin the rollback ordering once per concrete kind so
// a future kind-specific shortcut cannot bypass the immutable source cap.
func TestRollbackRestoreAdoption_ReappliesImmutableSourceQuota_AllVolumeBackends(t *testing.T) {
	for _, kind := range []string{"btrfs", "xfs", "zfs"} {
		t.Run(kind, func(t *testing.T) {
			b := newBackendForProvisionTest(t, &mockDockerClient{}, quotaRollbackProvision())
			rs := attachRetentionStore(t, b)
			b.compose = &mockComposeExecutor{
				DownFn: func(context.Context, string, time.Duration) error { return nil },
			}

			var steps []string
			retained := retainedName(canonicalVolumeName(quotaRollbackSourceLease, "app", 0))
			b.volumes = &kindedMockVolumeManager{
				kind: kind,
				mockVolumeManager: &mockVolumeManager{
					RenameVolumeFn: func(oldName, newName string) error {
						steps = append(steps, fmt.Sprintf("rename:%s:%s", oldName, newName))
						return nil
					},
					UsageFn: func(_ context.Context, name string) (int64, error) {
						require.Equal(t, retained, name)
						steps = append(steps, "usage:"+name)
						return 50 * bytesPerMiB, nil
					},
					EnsureQuotaFn: func(_ context.Context, name string, diskMB int64) error {
						require.Equal(t, retained, name)
						require.Equal(t, int64(100), diskMB,
							"rollback must use the durable source snapshot, not mutable config")
						steps = append(steps, fmt.Sprintf("quota:%s:%d", name, diskMB))
						return nil
					},
				},
			}

			rec := quotaRollbackEntry()
			rec = *putRestoringRetention(t, rs, rec)
			// If production accidentally falls back to mutable config, this value
			// would leak into EnsureQuota instead of the immutable 100 MiB cap.
			b.cfg.SKUProfiles["old-tier"] = SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 999}

			ok := b.rollbackRestoreAdoption(
				context.Background(), quotaRollbackDestinationLease, nil, &rec, true, slog.Default(),
			)
			require.True(t, ok)
			assert.Equal(t, []string{
				"rename:" + canonicalVolumeName(quotaRollbackDestinationLease, "app", 0) + ":" + retained,
				"usage:" + retained,
				"quota:" + retained + ":100",
			}, steps, "re-quarantine, fit proof, then quota restore is the required order")

			stored, err := rs.Get(quotaRollbackSourceLease)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, shared.RetentionStatusActive, stored.Status)
			assert.Equal(t, rec.Generation+1, stored.Generation)
			_, provisionExists := b.provisions[quotaRollbackDestinationLease]
			assert.False(t, provisionExists)
		})
	}
}

func TestRollbackRestoreAdoption_QuotaProofFailureFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name            string
		mutateRecord    func(*shared.RetentionEntry)
		usage           int64
		usageErr        error
		ensureErr       error
		wantEnsureCalls int
	}{
		{
			name:     "usage cannot be measured",
			usageErr: errors.New("filesystem unavailable"),
		},
		{
			name:  "promoted workload grew beyond old cap",
			usage: 100*bytesPerMiB + 1,
		},
		{
			name:            "old quota cannot be applied",
			usage:           50 * bytesPerMiB,
			ensureErr:       errors.New("quota subsystem unavailable"),
			wantEnsureCalls: 1,
		},
		{
			name: "pre-snapshot record has no exact old cap",
			mutateRecord: func(rec *shared.RetentionEntry) {
				rec.ResourceProfiles = nil
			},
		},
		{
			name: "retained name is outside immutable item topology",
			mutateRecord: func(rec *shared.RetentionEntry) {
				rec.RetainedVolumeNames = []string{"fred-retained-source-other-0"}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBackendForProvisionTest(t, &mockDockerClient{}, quotaRollbackProvision())
			rs := attachRetentionStore(t, b)
			b.compose = &mockComposeExecutor{
				DownFn: func(context.Context, string, time.Duration) error { return nil },
			}

			ensureCalls := 0
			b.volumes = &kindedMockVolumeManager{
				kind: "xfs",
				mockVolumeManager: &mockVolumeManager{
					RenameVolumeFn: func(string, string) error { return nil },
					UsageFn: func(context.Context, string) (int64, error) {
						return tc.usage, tc.usageErr
					},
					EnsureQuotaFn: func(context.Context, string, int64) error {
						ensureCalls++
						return tc.ensureErr
					},
				},
			}

			rec := quotaRollbackEntry()
			persisted := putRestoringRetention(t, rs, rec)
			rec = *persisted
			if tc.mutateRecord != nil {
				tc.mutateRecord(&rec)
			}
			const allocationID = "destination-app-0"
			require.NoError(t, b.pool.TryAllocate(allocationID, "docker-large", "tenant-a"))

			ok := b.rollbackRestoreAdoption(
				context.Background(), quotaRollbackDestinationLease, []string{allocationID}, &rec, true, slog.Default(),
			)
			assert.False(t, ok)
			assert.Equal(t, tc.wantEnsureCalls, ensureCalls)

			stored, err := rs.Get(quotaRollbackSourceLease)
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, shared.RetentionStatusRestoring, stored.Status,
				"source ownership must not become active under an unproven old quota")
			assert.Equal(t, rec.Generation, stored.Generation)
			assert.Equal(t, 1, b.pool.Stats().AllocationCount,
				"destination reservation must keep all bytes counted while rollback is parked")
			_, provisionExists := b.provisions[quotaRollbackDestinationLease]
			assert.False(t, provisionExists,
				"a prelude failure has no actor transition; removing only its guard lets reconciliation retry")
		})
	}
}

// This models the crash/restart recovery arm: the destination provision is
// absent, the restoring record and live reservation survived, and a transient
// quota failure must remain retryable without ever publishing the old record at
// the wrong physical cap.
func TestReconcileRestoring_QuotaRestoreRetriesBeforeSourceReactivation(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	b.compose = &mockComposeExecutor{
		DownFn: func(context.Context, string, time.Duration) error { return nil },
	}

	quotaWorks := false
	var quotaAttempts int
	b.volumes = &kindedMockVolumeManager{
		kind: "zfs",
		mockVolumeManager: &mockVolumeManager{
			RenameVolumeFn: func(string, string) error { return nil },
			UsageFn: func(context.Context, string) (int64, error) {
				return 40 * bytesPerMiB, nil
			},
			EnsureQuotaFn: func(_ context.Context, _ string, diskMB int64) error {
				quotaAttempts++
				require.Equal(t, int64(100), diskMB)
				if !quotaWorks {
					return errors.New("transient ZFS control-plane outage")
				}
				return nil
			},
		},
	}

	rec := quotaRollbackEntry()
	rec = *putRestoringRetention(t, rs, rec)
	const allocationID = quotaRollbackDestinationLease + "-app-0"
	require.NoError(t, b.pool.TryAllocate(allocationID, "docker-small", "tenant-a"))
	recordRestoreOperationOutcome(t, b, rec, backend.CallbackStatusFailed)

	err := b.reconcileRestoring(context.Background(), rec)
	require.ErrorContains(t, err, "quotas")
	stored, getErr := rs.Get(quotaRollbackSourceLease)
	require.NoError(t, getErr)
	require.NotNil(t, stored)
	assert.Equal(t, shared.RetentionStatusRestoring, stored.Status)
	assert.Equal(t, 1, b.pool.Stats().AllocationCount)

	quotaWorks = true
	require.NoError(t, b.reconcileRestoring(context.Background(), *stored))
	stored, getErr = rs.Get(quotaRollbackSourceLease)
	require.NoError(t, getErr)
	require.NotNil(t, stored)
	assert.Equal(t, shared.RetentionStatusActive, stored.Status)
	assert.Equal(t, rec.Generation+1, stored.Generation)
	assert.Equal(t, 2, quotaAttempts, "the next sweep retries the idempotent quota restore")
	assert.Zero(t, b.pool.Stats().AllocationCount,
		"live capacity hands off only after source ownership and old quota are durable")
}

func TestRollbackRestoreAdoption_PostCASRefreshFailureKeepsMakeBeforeBreakAccounting(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, quotaRollbackProvision())
	rs := attachRetentionStore(t, b)
	b.compose = &mockComposeExecutor{
		DownFn: func(context.Context, string, time.Duration) error { return nil },
	}
	b.volumes = &kindedMockVolumeManager{
		kind: "xfs",
		mockVolumeManager: &mockVolumeManager{
			RenameVolumeFn: func(string, string) error { return nil },
			UsageFn:        func(context.Context, string) (int64, error) { return 10 * bytesPerMiB, nil },
			EnsureQuotaFn:  func(context.Context, string, int64) error { return nil },
		},
	}

	rec := quotaRollbackEntry()
	rec = *putRestoringRetention(t, rs, rec)
	unresolvedLeaseUUID := "0192f1a0-1111-4abc-8def-000000000802"
	// This independently valid, deliberately enormous retention row makes the
	// aggregate checked projection overflow only after the source CAS. The
	// rollback must still be safe for its own bytes.
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: unresolvedLeaseUUID,
		Tenant:            "other-tenant",
		ProviderUUID:      nominalDockerProviderUUID,
		Items:             []backend.LeaseItem{{SKU: "oversized-tier", ServiceName: "app", Quantity: 1}},
		ResourceProfiles: []shared.SKUResourceSnapshot{{
			SKU: "oversized-tier", CPUCores: 1, MemoryMB: 1, DiskMB: math.MaxInt64 - 99,
		}},
		Status:    shared.RetentionStatusActive,
		CreatedAt: time.Now(),
	}))
	const allocationID = "destination-app-0"
	require.NoError(t, b.pool.TryAllocate(allocationID, "docker-small", "tenant-a"))
	require.Positive(t, b.pool.Stats().AllocatedDiskMB)

	require.True(t, b.rollbackRestoreAdoption(
		context.Background(), quotaRollbackDestinationLease, []string{allocationID}, &rec, true, slog.Default(),
	))
	stored, err := rs.Get(quotaRollbackSourceLease)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, shared.RetentionStatusActive, stored.Status,
		"the source ownership CAS committed before the injected refresh failure")
	stats := b.pool.Stats()
	assert.Zero(t, stats.AllocatedDiskMB)
	assert.Equal(t, int64(100), stats.RetainedDiskMB,
		"the conservative pre-addition must survive a post-CAS full-refresh failure")

	// Once the unrelated bad row is repaired/removed, the ordinary projection
	// path converges to the same exact value without any special pending state.
	reaping, begun, err := rs.BeginReaping(activeRetentionCandidateForTest(t, rs, unresolvedLeaseUUID))
	require.NoError(t, err)
	require.True(t, begun)
	deleted, err := rs.DeleteReaped(reaping)
	require.NoError(t, err)
	require.True(t, deleted)
	require.NoError(t, b.refreshRetentionAccountingChecked())
	assert.Equal(t, int64(100), b.pool.Stats().RetainedDiskMB)
}
