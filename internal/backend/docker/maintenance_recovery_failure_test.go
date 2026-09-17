package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/docker/docker/errdefs"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestRecoverStateSettlesMaintenanceWithUnreadySource(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate} {
		for _, phase := range []string{"deploying", "failed"} {
			for _, state := range []string{"exited", "one exited", "unhealthy", "partial target"} {
				t.Run(string(kind)+"/"+phase+"/"+state, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						h := newMaintenanceRecoveryHarnessForKind(t, kind)
						h.appendTarget(true)
						if phase == "failed" {
							_, err := failMaintenanceForTest(t, h.b.maintenanceSettlement, h.target,
								maintenanceFailureReason(kind), "maintenance could not replace the source", false)
							require.NoError(t, err)
						} else {
							_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
							require.NoError(t, err)
						}
						h.b.cfg.ProvisionTimeout = time.Minute
						source := h.containersFor(h.source, 2, "exited", HealthStatusNone)
						if state == "one exited" {
							source[0].Status = "running"
						}
						if state == "unhealthy" {
							source = h.containersFor(h.source, 2, "running", HealthStatusUnhealthy)
						}
						for index := range source {
							source[index].FailCount = 3
						}
						h.inventory.containers = source
						if state == "partial target" {
							h.inventory.containers = append(h.inventory.containers, h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)...)
						}
						h.reopen()
						require.Empty(t, h.b.provisions, "recover a genuinely cold backend")
						if phase == "deploying" {
							require.NoError(t, h.b.recoverState(t.Context()))
							assert.Contains(t, []backend.ProvisionStatus{backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating}, h.b.provisions[h.leaseUUID].Status)
							require.Empty(t, h.inventory.removed, "source failure cannot bypass the target visibility horizon")
							time.Sleep(time.Minute)
						}
						for range 3 {
							require.NoError(t, h.b.recoverState(t.Context()))
							h.assertSettled(backend.CallbackStatusFailed)
							info, err := h.b.GetProvision(t.Context(), h.leaseUUID)
							require.NoError(t, err)
							assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
							assert.Equal(t, maintenanceFailureReason(kind), info.Reason)
							assert.NotEmpty(t, info.Message)
							if phase == "failed" {
								assert.Equal(t, 4, info.FailCount, "cold failure increments existing labels once, including repeated recovery")
							}
						}
						for _, container := range source {
							assert.NotContains(t, h.inventory.removed, container.ContainerID, "terminal source failure never authorizes deleting its containers")
						}
						// Preserve ordinary recovery from a genuinely healthy complete
						// runtime, including clearing the stale maintenance failure reason.
						h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
						require.NoError(t, h.b.recoverState(t.Context()))
						assert.Equal(t, backend.ProvisionStatusReady, h.b.provisions[h.leaseUUID].Status)
						assert.Empty(t, h.b.provisions[h.leaseUUID].Reason)
					})
				})
			}
		}
	}
}

func TestMaintenanceSourceFailureRequiresCleanupAuthority(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	h.inventory.containers = h.containersFor(h.source, 2, "exited", HealthStatusNone)
	intent, found, err := h.b.maintenanceSettlement.GetMaintenanceIntent(h.leaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	_, err = h.b.recoveryCoordinator.WithLease(t.Context(), h.leaseUUID, func(scope shared.LeaseRecoveryScope) error {
		observed, err := h.b.maintenanceSettlement.RecoverMaintenanceExecution(t.Context(), scope, intent)
		require.NoError(t, err)
		require.IsType(t, shared.MaintenanceExecutionAmbiguous{}, observed,
			"ordinary inspection cannot bypass cleanup and mint a failed source")
		cleaned, err := h.b.maintenanceSettlement.CleanupRecoveredMaintenance(t.Context(), scope, intent)
		require.NoError(t, err)
		require.IsType(t, shared.MaintenanceExecutionFailure{}, cleaned)
		failure := cleaned.(shared.MaintenanceExecutionFailure)
		require.True(t, failure.Valid())
		require.False(t, failure.SourceRecovered())
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, h.inventory.removed)
}

func TestRecoverMaintenanceLocalObservationDoesNotBlockSiblingRecovery(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	targets := h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
	h.inventory.inspectErrFor = map[string]error{targets[0].ContainerID: errdefs.NotFound(errors.New("container disappeared"))}
	h.inventory.containers = targets
	siblingLease := uuid.NewString()
	sibling := h.source
	operationID, callbackURL, lifecycleURL := newTestRestoreCallbackAuthority(t)
	sibling.OperationID = operationID
	sibling.RuntimeAuthority = mustTestReleaseRuntimeAuthority(t, sibling.OperationID, "tenant-a",
		"22222222-2222-4222-8222-222222222222", callbackURL, lifecycleURL)
	seedProvisionReleaseForLeaseTest(t, h.callbacks, h.releases, h.operations, siblingLease, sibling)
	for _, container := range h.containersFor(sibling, 2, "exited", HealthStatusNone) {
		container.LeaseUUID = siblingLease
		container.ContainerID = siblingLease + container.ContainerID
		container.Name = "fred-" + siblingLease + "-web-" + fmt.Sprint(container.InstanceIndex)
		h.inventory.containers = append(h.inventory.containers, container)
	}
	before := testutil.ToFloat64(maintenanceRecoveryDeferredTotal)
	for range 2 {
		require.NoError(t, h.b.recoverState(t.Context()))
		assert.Equal(t, backend.ProvisionStatusRestarting, h.b.provisions[h.leaseUUID].Status)
		assert.Equal(t, backend.ProvisionStatusFailed, h.b.provisions[siblingLease].Status,
			"one disappearing maintenance container must not block unrelated runtime recovery")
	}
	assert.Equal(t, before+2, testutil.ToFloat64(maintenanceRecoveryDeferredTotal))
	pending, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Empty(t, h.inventory.removed)
	callbacks, err := h.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, callbacks, "local ambiguity grants no terminal callback authority")
}

func TestMaintenanceRecoveryKeepsGlobalFailuresFatal(t *testing.T) {
	for _, mode := range []string{"journal", "storage identity", "transport", "mixed local and transport", "missing and transport", "missing and storage identity", "missing wraps joined transport"} {
		t.Run(mode, func(t *testing.T) {
			h := newMaintenanceRecoveryHarness(t)
			h.appendTarget(true)
			_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
			switch mode {
			case "journal":
				require.NoError(t, h.callbacks.Close())
			case "storage identity":
				h.b.storageVerifier = testDockerRuntimeStorageVerifier{id: h.b.storageIdentity,
					verify: func(context.Context) error { return backendidentity.ErrIdentityDrift }}
			case "transport":
				h.inventory.inspectErr = errors.New("Docker transport unavailable")
			case "mixed local and transport":
				h.inventory.inspectErr = errors.Join(errMaintenanceReadinessPending, errors.New("Docker transport unavailable"))
			case "missing and transport":
				h.inventory.inspectErr = errors.Join(errdefs.NotFound(errors.New("missing")), errors.New("Docker transport unavailable"))
			case "missing and storage identity":
				h.inventory.inspectErr = errors.Join(errdefs.NotFound(errors.New("missing")), backendidentity.ErrIdentityDrift)
			case "missing wraps joined transport":
				h.inventory.inspectErr = errdefs.NotFound(errors.Join(errors.New("missing"), errors.New("Docker transport unavailable")))
			}
			require.Error(t, h.b.recoverState(t.Context()))
			require.Empty(t, h.inventory.removed)
		})
	}
}

func TestMaintenanceReadinessBranchDeferrals(t *testing.T) {
	for _, branch := range []string{"committed_target", "source_only", "cleanup_source"} {
		t.Run(branch, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newMaintenanceRecoveryHarness(t)
				h.b.cfg.StartupVerifyDuration = 5 * time.Second
				h.b.cfg.ProvisionTimeout = time.Nanosecond
				release := h.source
				if branch != "source_only" {
					h.appendTarget(true)
					if branch == "committed_target" {
						_, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
						require.NoError(t, err)
						release = h.targetRelease
					} else {
						_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
						require.NoError(t, err)
					}
				}
				h.inventory.containers = h.containersFor(release, 2, "running", HealthStatusNone)
				for index := range h.inventory.containers {
					h.inventory.containers[index].CreatedAt = time.Now().Add(time.Hour).Round(0)
				}
				h.reopen()
				if branch == "cleanup_source" {
					time.Sleep(time.Second) // expire admission before opening the source startup-age window
				}
				var logs bytes.Buffer
				h.b.logger = slog.New(slog.NewTextHandler(&logs, nil))
				before := testutil.ToFloat64(maintenanceReadinessPendingTotal.WithLabelValues(branch))
				beforeDeferred := testutil.ToFloat64(maintenanceRecoveryDeferredTotal)
				for range 2 {
					require.NoError(t, h.b.recoverState(t.Context()))
					pending, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
					require.NoError(t, err)
					require.Len(t, pending, 1)
					callbacks, err := h.callbacks.ListPending()
					require.NoError(t, err)
					require.Empty(t, callbacks)
					require.Empty(t, h.inventory.removed)
				}
				assert.Equal(t, before+2, testutil.ToFloat64(maintenanceReadinessPendingTotal.WithLabelValues(branch)))
				assert.Equal(t, beforeDeferred, testutil.ToFloat64(maintenanceRecoveryDeferredTotal),
					"expected readiness waits must not fall through to observation-conflict handling")
				assert.Equal(t, 1, strings.Count(logs.String(), "maintenance recovery is waiting for readiness evidence"))
				assert.Contains(t, logs.String(), "branch="+branch)
				time.Sleep(5 * time.Second)
				require.NoError(t, h.b.recoverState(t.Context()))
				status := backend.CallbackStatusFailed
				if branch == "committed_target" {
					status = backend.CallbackStatusSuccess
				}
				h.assertSettled(status)
				require.NoError(t, h.b.recoverState(t.Context()))
				require.Empty(t, h.b.maintenanceReadinessWarnings.entries, "settled attempts must not accumulate warning state")
			})
		})
	}
}

func TestCommittedMaintenanceStartingHealthRemainsObservableWithoutRollback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
			"web": {Image: "docker.io/library/nginx:1.27", HealthCheck: &manifest.HealthCheckConfig{Test: []string{"CMD", "true"}}},
		}}
		h := newMaintenanceRecoveryHarnessForAuthorityAtCallbackOptions(t, shared.MaintenanceIntentRestart, false, "", false, stack)
		h.appendTarget(true)
		_, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
		require.NoError(t, err)
		h.b.cfg.ProvisionTimeout = time.Second
		h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusStarting)
		var logs bytes.Buffer
		h.b.logger = slog.New(slog.NewTextHandler(&logs, nil))
		before := testutil.ToFloat64(maintenanceReadinessPendingTotal.WithLabelValues("committed_target"))
		for range 3 {
			time.Sleep(time.Minute)
			require.NoError(t, h.b.recoverState(t.Context()))
			pending, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
			require.NoError(t, err)
			require.Len(t, pending, 1, "a committed target cannot be rolled back because its healthcheck remains uncertain")
			require.Empty(t, h.inventory.removed)
			assert.Equal(t, backend.ProvisionStatusRestarting, h.b.provisions[h.leaseUUID].Status)
		}
		assert.Equal(t, before+3, testutil.ToFloat64(maintenanceReadinessPendingTotal.WithLabelValues("committed_target")))
		assert.Equal(t, 1, strings.Count(logs.String(), "maintenance recovery is waiting for readiness evidence"))
		h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusHealthy)
		require.NoError(t, h.b.recoverState(t.Context()))
		h.assertSettled(backend.CallbackStatusSuccess)
	})
}

func TestFailedMaintenanceSourceNeedsHealthyCohortBeforeClearingReason(t *testing.T) {
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"web": {Image: "docker.io/library/nginx:1.27", HealthCheck: &manifest.HealthCheckConfig{Test: []string{"CMD", "true"}}},
	}}
	h := newMaintenanceRecoveryHarnessForAuthorityAtCallbackOptions(t, shared.MaintenanceIntentRestart, false, "", false, stack)
	h.appendTarget(true)
	_, err := failMaintenanceForTest(t, h.b.maintenanceSettlement, h.target, backend.ReasonRestartFailed, "restart failed", false)
	require.NoError(t, err)
	for _, health := range []HealthStatus{HealthStatusUnhealthy, HealthStatusStarting, HealthStatusNone} {
		h.inventory.containers = h.containersFor(h.source, 2, "running", health)
		require.NoError(t, h.b.recoverState(t.Context()))
		h.assertSettled(backend.CallbackStatusFailed)
		assert.Equal(t, backend.ProvisionStatusFailed, h.b.provisions[h.leaseUUID].Status)
		assert.Equal(t, backend.ReasonRestartFailed, h.b.provisions[h.leaseUUID].Reason)
	}
	h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusHealthy)
	require.NoError(t, h.b.recoverState(t.Context()))
	assert.Equal(t, backend.ProvisionStatusReady, h.b.provisions[h.leaseUUID].Status)
	assert.Empty(t, h.b.provisions[h.leaseUUID].Reason)
}

func TestColdReadyMaintenanceProjectionPreservesFailureCountAndSourceReason(t *testing.T) {
	for _, targetCommitted := range []bool{false, true} {
		t.Run(fmt.Sprint(targetCommitted), func(t *testing.T) {
			h := newMaintenanceRecoveryHarness(t)
			h.appendTarget(true)
			release := h.source
			callbackStatus := backend.CallbackStatusFailed
			if targetCommitted {
				_, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
				require.NoError(t, err)
				release = h.targetRelease
				callbackStatus = backend.CallbackStatusSuccess
			} else {
				_, err := failMaintenanceForTest(t, h.b.maintenanceSettlement, h.target, backend.ReasonRestartFailed, "restart failed", true)
				require.NoError(t, err)
			}
			h.inventory.containers = h.containersFor(release, 2, "running", HealthStatusNone)
			for index := range h.inventory.containers {
				h.inventory.containers[index].FailCount = 3
			}
			require.Empty(t, h.b.provisions)
			for range 2 {
				require.NoError(t, h.b.recoverState(t.Context()))
				h.assertSettled(callbackStatus)
				info, err := h.b.GetProvision(t.Context(), h.leaseUUID)
				require.NoError(t, err)
				assert.Equal(t, backend.ProvisionStatusReady, info.Status)
				assert.Equal(t, 3, info.FailCount)
				assert.Equal(t, 2, h.b.pool.Stats().AllocationCount)
				if targetCommitted {
					assert.Empty(t, info.Reason)
				} else {
					assert.Equal(t, backend.ReasonRestartFailed, info.Reason)
					assert.Equal(t, "restart failed", info.Message)
				}
			}
		})
	}
}
