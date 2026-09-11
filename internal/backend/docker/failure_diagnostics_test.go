package docker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func configureDiagnosticStartupFailure(t *testing.T, h *maintenanceRecoveryHarness) {
	t.Helper()
	mock := h.b.docker.(*mockDockerClient)
	mock.PullImageFn = func(context.Context, string, time.Duration) error { return nil }
	mock.ContainerLogsFn = func(_ context.Context, id string, _ int) (string, error) {
		if _, err := h.inventory.inspect(t.Context(), id); err != nil {
			return "", err
		}
		return "fatal: application startup failed", nil
	}
	h.inventory.containers = h.containersFor(h.source, 2, "running", "")
	h.b.compose = &mockComposeExecutor{
		UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
			h.inventory.containers = h.containersFor(h.targetRelease, 2, "exited", "")
			return nil
		},
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			result := make([]composeContainerSummary, 0, 2)
			for i, c := range h.inventory.containers {
				result = append(result, composeContainerSummary{ID: c.ContainerID, Service: fmt.Sprintf("web-%d", i), State: c.Status})
			}
			return result, nil
		},
	}
}

func TestFailedMaintenanceStartupLogsSurviveCleanupAndJournalReopen(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate} {
		t.Run(string(kind), func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForKind(t, kind)
			h.appendTarget(true)
			configureDiagnosticStartupFailure(t, h)
			path := filepath.Join(t.TempDir(), "diagnostics.db")
			store, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: path})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			bindTestDiagnosticsStore(t, h.b, store)
			execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			require.IsType(t, shared.MaintenanceExecutionAmbiguous{}, h.b.maintenanceSettlement.ExecuteMaintenance(t.Context(), execution))
			require.NotEmpty(t, h.inventory.containers, "failure remains recoverable before cleanup")
			// Reopen every journal before the timeout-driven failed-target retirement.
			require.NoError(t, store.Close())
			reopened, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: path})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			h.b.diagnosticsStore = reopened
			h.reopen()
			h.b.cfg.ProvisionTimeout = time.Nanosecond
			require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
			require.Empty(t, h.inventory.containers)
			h.assertSettled(backend.CallbackStatusFailed)
			entry, err := h.b.diagnosticsStore.Get(h.leaseUUID)
			require.NoError(t, err)
			require.NotNil(t, entry)
			require.Contains(t, entry.Logs, "web/0")
			require.Equal(t, "fatal: application startup failed", entry.Logs["web/0"])
			require.NotEmpty(t, entry.Error)
			logs, err := h.b.GetLogs(t.Context(), h.leaseUUID, 100)
			require.NoError(t, err)
			require.Equal(t, entry.Logs, logs)
			h.reopen()
			logs, err = h.b.GetLogs(t.Context(), h.leaseUUID, 100)
			require.NoError(t, err)
			require.Equal(t, entry.Logs, logs, "terminal publication survives a second journal reopen")
		})
	}
}

func TestFailedMaintenanceCleanupWaitsForWritableDiagnosticStore(t *testing.T) {
	h := newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate)
	h.appendTarget(true)
	configureDiagnosticStartupFailure(t, h)
	path := filepath.Join(t.TempDir(), "diagnostics.db")
	store, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: path})
	require.NoError(t, err)
	bindTestDiagnosticsStore(t, h.b, store)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	require.IsType(t, shared.MaintenanceExecutionAmbiguous{}, h.b.maintenanceSettlement.ExecuteMaintenance(t.Context(), execution))
	require.NoError(t, store.Close())
	h.b.cfg.ProvisionTimeout = time.Nanosecond
	require.Error(t, h.b.recoverMaintenanceIntents(t.Context()))
	require.NotEmpty(t, h.inventory.containers, "durable capture failure must not authorize deleting logs")
	pending, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	reopened, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	bindTestDiagnosticsStore(t, h.b, reopened)
	require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
	require.Empty(t, h.inventory.containers)
	h.assertSettled(backend.CallbackStatusFailed)
	logs, err := h.b.GetLogs(t.Context(), h.leaseUUID, 100)
	require.NoError(t, err)
	require.Equal(t, "fatal: application startup failed", logs["web/0"])
}

func TestInterruptedMaintenanceCloseCapturesStartupLogsBeforeTargetRemoval(t *testing.T) {
	for _, test := range []struct {
		name        string
		legacy      bool
		callback    string
		stopError   bool
		stopTimeout time.Duration
	}{
		{name: "current runtime"},
		{name: "legacy source with moved target callback", legacy: true, callback: "https://new-provider.example/callbacks/provision", stopTimeout: 47 * time.Millisecond},
		{name: "failed graceful stop still converges", stopError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForAuthorityAtCallback(t, shared.MaintenanceIntentUpdate, test.legacy, test.callback)
			h.appendTarget(true)
			configureDiagnosticStartupFailure(t, h)
			execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			require.IsType(t, shared.MaintenanceExecutionAmbiguous{}, h.b.maintenanceSettlement.ExecuteMaintenance(t.Context(), execution))
			// Close must retire both the failed target and a surviving source,
			// even when their callback routes name different provider bases.
			h.inventory.containers = append(h.inventory.containers, h.containersFor(h.source, 1, "running", HealthStatusNone)...)
			request, err := h.b.closeSettlement.NewCloseRequest(h.leaseUUID, false)
			require.NoError(t, err)
			admission, err := h.b.closeSettlement.BeginClose(request)
			require.NoError(t, err)
			require.Equal(t, h.intent.MaintenanceID(), admission.Claim().InterruptedMaintenanceID())
			h.reopen()
			bindBackendTestCloseExecutor(t, h.b, h.b.closeSettlement)
			h.b.cfg.ContainerStopTimeout = test.stopTimeout
			wantTimeout := test.stopTimeout
			if wantTimeout == 0 {
				wantTimeout = 30 * time.Second
			}
			mock := h.b.docker.(*mockDockerClient)
			var events []string
			mock.ContainerLogsFn = func(_ context.Context, id string, _ int) (string, error) {
				events = append(events, "log:"+id)
				return "fatal: application startup failed", nil
			}
			mock.StopContainerFn = func(ctx context.Context, id string, timeout time.Duration) error {
				require.Equal(t, wantTimeout, timeout)
				deadline, ok := ctx.Deadline()
				require.True(t, ok, "graceful stop must have a bounded RPC context")
				require.LessOrEqual(t, time.Until(deadline), wantTimeout)
				events = append(events, "stop:"+id)
				if test.stopError {
					return errors.New("daemon stop failed")
				}
				return nil
			}
			mock.RemoveContainerFn = func(ctx context.Context, id string) error {
				require.Equal(t, "stop:"+id, events[len(events)-1], "every captured container gets a graceful stop before removal")
				events = append(events, "remove:"+id)
				return h.inventory.remove(ctx, id)
			}
			claim, present, err := h.b.closeSettlement.GetCloseIntent(h.leaseUUID)
			require.NoError(t, err)
			require.True(t, present)
			closeExecution, err := h.b.closeSettlement.StartCloseExecution(claim)
			require.NoError(t, err)
			outcome := h.b.closeSettlement.ExecuteClose(t.Context(), closeExecution)
			destroyed, ok := outcome.(shared.CloseExecutionDestroyed)
			if !ok {
				t.Fatalf("close failed to retire captured targets: %T %+v", outcome, outcome)
			}
			require.Len(t, events, 8, "two failed-target captures precede stop/removal of the full three-container cohort")
			require.Contains(t, events[0], "log:")
			require.Contains(t, events[1], "log:")
			require.Empty(t, h.inventory.containers)
			require.NoError(t, h.b.completeCloseOutcome(destroyed))
			logs, err := h.b.GetLogs(t.Context(), h.leaseUUID, 100)
			require.NoError(t, err)
			require.Equal(t, "fatal: application startup failed", logs["web/0"])
			h.reopen()
			logs, err = h.b.GetLogs(t.Context(), h.leaseUUID, 100)
			require.NoError(t, err)
			require.Equal(t, "fatal: application startup failed", logs["web/0"])
		})
	}
}

func TestInterruptedMaintenanceCloseReattestsCapturedContainerBeforeStopOrRemoval(t *testing.T) {
	for _, field := range []string{"principal", "callback"} {
		t.Run(field, func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate)
			h.appendTarget(true)
			h.inventory.containers = h.containersFor(h.targetRelease, 2, "exited", HealthStatusNone)
			changedID := h.inventory.containers[0].ContainerID
			request, err := h.b.closeSettlement.NewCloseRequest(h.leaseUUID, false)
			require.NoError(t, err)
			admission, err := h.b.closeSettlement.BeginClose(request)
			require.NoError(t, err)
			bindBackendTestCloseExecutor(t, h.b, h.b.closeSettlement)
			mock := h.b.docker.(*mockDockerClient)
			mock.ContainerLogsFn = func(context.Context, string, int) (string, error) {
				// The inventory capture observed the original authority. A fresh
				// inspect must reject a contradictory identity before any stop.
				if field == "principal" {
					h.inventory.containers[0].Tenant = "another-tenant"
				} else {
					h.inventory.containers[0].CallbackURL = "https://another-provider.example/callbacks/provision"
				}
				return "fatal: startup", nil
			}
			mock.StopContainerFn = func(_ context.Context, id string, _ time.Duration) error {
				require.NotEqual(t, changedID, id, "contradictory captured authority must not authorize a stop")
				return nil
			}
			closeExecution, err := h.b.closeSettlement.StartCloseExecution(admission.Claim())
			require.NoError(t, err)
			outcome := h.b.closeSettlement.ExecuteClose(t.Context(), closeExecution)
			require.IsType(t, shared.CloseExecutionPending{}, outcome)
			require.NotContains(t, h.inventory.removed, changedID)
			_, present, err := h.b.closeSettlement.GetCloseIntent(h.leaseUUID)
			require.NoError(t, err)
			require.True(t, present, "contradictory authority keeps the durable close pending")
		})
	}
}
