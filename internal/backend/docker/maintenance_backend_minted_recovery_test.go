package docker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// A backend-minted maintenance (a custom-domain redeploy or a runtime refresh)
// interrupted by a crash recovers through the same boundaries as a restart.
// Its recovered failure details used to be refused for any kind but restart
// and update, so the custom-domain head could never settle.
func TestRecoverInterruptedBackendMintedMaintenance(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{
		shared.MaintenanceIntentCustomDomain, shared.MaintenanceIntentRuntimeRefresh,
	} {
		t.Run(string(kind)+"/intent before target append", func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForKind(t, kind)
			h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
			h.reopen()
			require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
			h.assertSettled(backend.CallbackStatusFailed)
			h.b.provisionsMu.RLock()
			provision := h.b.provisions[h.leaseUUID]
			h.b.provisionsMu.RUnlock()
			require.NotNil(t, provision)
			assert.Equal(t, backend.ProvisionStatusReady, provision.Status)
			if kind == shared.MaintenanceIntentRuntimeRefresh {
				assert.Empty(t, provision.LastError, "a rolled-back refresh leaves the lease unchanged")
			} else {
				assert.NotEmpty(t, provision.LastError)
			}
		})

		t.Run(string(kind)+"/append before bind", func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForKind(t, kind)
			h.appendTarget(false)
			h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
			h.reopen()
			require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
			h.assertSettled(backend.CallbackStatusFailed)
		})

		t.Run(string(kind)+"/active target with zero survivors", func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForKind(t, kind)
			h.appendTarget(true)
			_, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
			require.NoError(t, err)
			h.reopen()
			require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
			h.assertCommittedRuntimeFailureSettled()
		})
	}
}
