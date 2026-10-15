package leasesm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// A runtime refresh recreates a healthy lease unchanged. When it fails and the
// rollback restores the lease to Ready, the lease is exactly as it was, so the
// attempt records no failure: no FailCount, no error. Every other maintenance
// kind keeps its existing accounting.
func TestRolledBackMaintenanceFailureAccounting(t *testing.T) {
	for _, tt := range []struct {
		name          string
		kind          shared.MaintenanceIntentKind
		wantFailCount int
		wantLastError string
	}{
		{name: "runtime refresh leaves the lease unchanged", kind: shared.MaintenanceIntentRuntimeRefresh,
			wantFailCount: 3, wantLastError: ""},
		{name: "custom-domain redeploy still counts", kind: shared.MaintenanceIntentCustomDomain,
			wantFailCount: 4, wantLastError: "replacement failed"},
		{name: "update still counts", kind: shared.MaintenanceIntentUpdate,
			wantFailCount: 4, wantLastError: "replacement failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			claim := newTestMaintenanceClaim(t, testActorLeaseUUID, tt.kind)
			store := newMockProvisionStore()
			store.put(testActorLeaseUUID, &ProvisionState{
				LeaseUUID:            testActorLeaseUUID,
				Status:               backend.ProvisionStatusReady,
				FailCount:            3,
				LastError:            "earlier error",
				CallbackURL:          "old-operation",
				LifecycleCallbackURL: "old-lifecycle",
				ContainerIDs:         []string{"old"},
				ServiceContainers:    map[string][]string{"app": {"old"}},
			})
			// Before this change a custom-domain or runtime-refresh failure could
			// not even be constructed, and became an ambiguous outcome instead.
			result := testMaintenanceFailure(t, claim, errors.New("replacement failed"), true, false,
				ReplaceFailureDetails{CallbackErr: string(tt.kind) + " failed", LastError: "replacement failed", Reason: backend.ReasonRestartFailed})
			callback := make(chan backend.CallbackStatus, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			actor := newTestActor(t, testActorLeaseUUID, testActorOpts{
				StopCtx:        ctx,
				ProvisionStore: store,
				MaintenanceWorkFn: func(shared.MaintenanceWorkerLifetime, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
					return replaceWorkTerminal{result: result}
				},
				SendMaintenanceCallbackFn: func(_ shared.MaintenanceIntentClaim, status backend.CallbackStatus, _ string) {
					callback <- status
				},
			})
			op := "restart"
			if tt.kind == shared.MaintenanceIntentUpdate {
				op = "update"
			}
			ack := make(chan error, 1)
			requireAccepted(t, func() bool { return routeReplace(t, actor, op, testMaintenanceTarget(t, claim), ack) })
			require.NoError(t, <-ack)
			select {
			case status := <-callback:
				assert.Equal(t, backend.CallbackStatusFailed, status)
			case <-time.After(time.Second):
				t.Fatal("maintenance failure callback was not sent")
			}
			committed, exists := store.Get(testActorLeaseUUID)
			require.True(t, exists)
			assert.Equal(t, backend.ProvisionStatusReady, committed.Status)
			assert.Equal(t, tt.wantFailCount, committed.FailCount)
			assert.Equal(t, tt.wantLastError, committed.LastError)
		})
	}
}

// Recovery of an interrupted maintenance builds its failure details for every
// journaled kind. A custom-domain redeploy used to be refused here, so its
// recovery could never converge.
func TestMaintenanceRecoveryFailureInfoAcceptsEveryJournaledKind(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{
		shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate,
		shared.MaintenanceIntentCustomDomain, shared.MaintenanceIntentRuntimeRefresh,
	} {
		t.Run(string(kind), func(t *testing.T) {
			claim := newTestMaintenanceClaim(t, testActorLeaseUUID, kind)
			info, err := NewMaintenanceRecoveryFailureInfo(claim, ReplaceFailureDetails{
				CallbackErr: "interrupted", Reason: backend.ReasonRestartFailed, LastError: "interrupted",
			})
			require.NoError(t, err)
			assert.Equal(t, kind == shared.MaintenanceIntentRuntimeRefresh, info.LeavesLeaseUnchanged())
			outcome, err := NewAmbiguousMaintenanceWork(errors.New("ambiguous"), claim)
			require.NoError(t, err)
			require.NotNil(t, outcome)
		})
	}
	_, err := NewMaintenanceRecoveryFailureInfo(shared.MaintenanceIntentClaim{}, ReplaceFailureDetails{})
	require.Error(t, err, "the zero claim authorizes nothing")
}

func TestNewRuntimeRefreshCommandRequiresRuntimeRefreshTarget(t *testing.T) {
	refresh := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentRuntimeRefresh)
	_, _, err := NewRuntimeRefreshCommand(testMaintenanceHandoff(t, context.Background()), testMaintenanceTarget(t, refresh))
	require.NoError(t, err)

	restart := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentRestart)
	_, _, err = NewRuntimeRefreshCommand(testMaintenanceHandoff(t, context.Background()), testMaintenanceTarget(t, restart))
	require.Error(t, err, "a restart target cannot be relabeled as a runtime refresh")
	_, _, err = NewRestartCommand(testMaintenanceHandoff(t, context.Background()), testMaintenanceTarget(t, refresh))
	require.Error(t, err, "a runtime-refresh target cannot be relabeled as a restart")
}
