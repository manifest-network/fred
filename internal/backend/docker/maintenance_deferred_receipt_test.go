package docker

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestFailedMaintenanceReceiptDefersOnlyLeaseLocalObservations(t *testing.T) {
	for _, mode := range []string{"different principal", "transport", "joined local and transport"} {
		t.Run(mode, func(t *testing.T) {
			h := newMaintenanceRecoveryHarness(t)
			h.appendTarget(true)
			failed, err := failMaintenanceForTest(t, h.b.maintenanceSettlement, h.target, backend.ReasonRestartFailed, "restart failed", false)
			require.NoError(t, err)
			require.NoError(t, h.b.resolveMaintenanceFailure(failed, "restart failed"))
			h.inventory.containers = h.containersFor(h.targetRelease, 2, "exited", HealthStatusNone)
			var logs bytes.Buffer
			h.b.logger = slog.New(slog.NewTextHandler(&logs, nil))
			switch mode {
			case "different principal":
				h.inventory.containers[0].Tenant = "another-tenant"
			case "transport":
				h.inventory.inspectErr = errors.New("Docker transport unavailable")
			case "joined local and transport":
				h.inventory.inspectErr = errors.Join(&maintenanceObservationDeferred{cause: errors.New("container changed")}, errors.New("Docker transport unavailable"))
			}
			before := testutil.ToFloat64(maintenanceRecoveryDeferredTotal)
			err = h.b.recoverMaintenanceIntents(t.Context())
			if mode == "different principal" {
				require.NoError(t, err)
				require.Equal(t, before+1, testutil.ToFloat64(maintenanceRecoveryDeferredTotal))
				require.Contains(t, logs.String(), "level=ERROR")
			} else {
				require.Error(t, err, "unknown global failures cannot inherit a lease-local retry classification")
				require.Equal(t, before, testutil.ToFloat64(maintenanceRecoveryDeferredTotal))
			}
			require.Empty(t, h.inventory.removed)
			receipts, err := h.b.maintenanceSettlement.ListFailedMaintenanceReceipts()
			require.NoError(t, err)
			require.Len(t, receipts, 1, "deferral does not erase permanent cleanup authority")
		})
	}
}

func TestMaintenanceGenerationConflictsRemainLeaseLocalWithoutCleanup(t *testing.T) {
	for _, mode := range []string{"missing identity", "runtime principal", "instance image", "instance outside topology"} {
		t.Run(mode, func(t *testing.T) {
			h := newMaintenanceRecoveryHarness(t)
			h.appendTarget(true)
			_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			h.b.cfg.ProvisionTimeout = time.Nanosecond
			h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
			switch mode {
			case "missing identity":
				h.inventory.containers[0].ContainerID = ""
				h.inventory.containers[0].Status = "exited"
			case "runtime principal":
				h.inventory.containers[0].Tenant = "other-tenant"
			case "instance image":
				h.inventory.containers[0].Image = "other-image"
			case "instance outside topology":
				h.inventory.containers[0].InstanceIndex = 7
			}
			before := testutil.ToFloat64(maintenanceRecoveryDeferredTotal)
			require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
			require.Equal(t, before+1, testutil.ToFloat64(maintenanceRecoveryDeferredTotal))
			require.Empty(t, h.inventory.removed)
			_, found, err := h.b.maintenanceSettlement.GetMaintenanceIntent(h.leaseUUID)
			require.NoError(t, err)
			require.True(t, found)
		})
	}
}

func TestReadyMaintenanceFailureReasonSurvivesTransientHealth(t *testing.T) {
	for _, reason := range []backend.Reason{backend.ReasonRestartFailed, backend.ReasonImagePullFailed} {
		t.Run(string(reason), func(t *testing.T) {
			stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"web": {Image: "docker.io/library/nginx:1.27", HealthCheck: &manifest.HealthCheckConfig{Test: []string{"CMD", "true"}}},
			}}
			h := newMaintenanceRecoveryHarnessForAuthorityAtCallbackOptions(t, shared.MaintenanceIntentRestart, false, "", false, stack)
			h.appendTarget(true)
			_, err := failMaintenanceForTest(t, h.b.maintenanceSettlement, h.target, reason, "restart target failed", true)
			require.NoError(t, err)
			h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusHealthy)
			require.NoError(t, h.b.recoverState(t.Context()))
			for _, health := range []HealthStatus{HealthStatusStarting, HealthStatusUnhealthy, HealthStatusHealthy} {
				h.inventory.containers = h.containersFor(h.source, 2, "running", health)
				require.NoError(t, h.b.recoverState(t.Context()))
				current, err := h.b.GetProvision(t.Context(), h.leaseUUID)
				require.NoError(t, err)
				require.Equal(t, backend.ProvisionStatusReady, current.Status)
				require.Equal(t, reason, current.Reason)
				require.Equal(t, "restart target failed", current.Message)
			}
		})
	}
}

func TestMaintenanceObservationConflictEscalatesAfterExactDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newMaintenanceRecoveryHarness(t)
		h.appendTarget(true)
		_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
		require.NoError(t, err)
		h.b.cfg.ProvisionTimeout = time.Minute
		h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
		h.inventory.containers[0].Tenant = "another-tenant"
		var logs bytes.Buffer
		h.b.logger = slog.New(slog.NewTextHandler(&logs, nil))
		require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
		time.Sleep(time.Minute)
		require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
		require.Contains(t, logs.String(), "level=ERROR")
		intent, found, err := h.b.maintenanceSettlement.GetMaintenanceIntent(h.leaseUUID)
		require.NoError(t, err)
		require.True(t, found, "an elapsed observation horizon cannot mint terminal mutation authority")
		require.Equal(t, h.intent.MaintenanceID(), intent.MaintenanceID())
		require.Empty(t, h.inventory.removed)
		pending, err := h.callbacks.ListPending()
		require.NoError(t, err)
		require.Empty(t, pending)
	})
}
