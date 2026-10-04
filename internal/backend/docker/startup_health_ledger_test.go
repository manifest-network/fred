package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func healthyPass(ids ...string) startupPass {
	var pass startupPass
	for _, id := range ids {
		pass.members = append(pass.members, startupContainer{id: id, healthGated: true})
		pass.infos = append(pass.infos, &ContainerInfo{ContainerID: id, Status: "running", Health: HealthStatusHealthy})
	}
	return pass
}

// Only a running, health-gated member that reported healthy is a fact; every
// other observation of a pass is not recorded.
func TestStartupHealthLedgerRecordsOnlyAPassedHealthCheck(t *testing.T) {
	var ledger startupHealthLedger
	ledger.recordPassedHealth(startupPass{
		members: []startupContainer{
			{id: "passed", healthGated: true},
			{id: "unhealthy", healthGated: true},
			{id: "starting", healthGated: true},
			{id: "exited", healthGated: true},
			{id: "ungated"},
			{id: "uninspected", healthGated: true},
		},
		infos: []*ContainerInfo{
			{ContainerID: "passed", Status: "running", Health: HealthStatusHealthy},
			{ContainerID: "unhealthy", Status: "running", Health: HealthStatusUnhealthy},
			{ContainerID: "starting", Status: "running", Health: HealthStatusStarting},
			{ContainerID: "exited", Status: "exited", Health: HealthStatusHealthy},
			{ContainerID: "ungated", Status: "running", Health: HealthStatusHealthy},
			nil,
		},
	})
	assert.True(t, ledger.passedHealth("passed"))
	for _, id := range []string{"unhealthy", "starting", "exited", "ungated", "uninspected", ""} {
		assert.False(t, ledger.passedHealth(id), id)
	}
}

// The ledger is bounded and forgets the oldest fact first; a forgotten fact
// only makes an authority read health fresh again.
func TestStartupHealthLedgerForgetsTheOldestFactFirst(t *testing.T) {
	var ledger startupHealthLedger
	ids := make([]string, 0, startupHealthLedgerCapacity+2)
	for i := range startupHealthLedgerCapacity + 2 {
		ids = append(ids, fmt.Sprintf("c-%d", i))
	}
	ledger.recordPassedHealth(healthyPass(ids...))
	ledger.recordPassedHealth(healthyPass(ids[len(ids)-1])) // a known fact is not re-recorded
	assert.False(t, ledger.passedHealth(ids[0]))
	assert.False(t, ledger.passedHealth(ids[1]))
	assert.True(t, ledger.passedHealth(ids[2]))
	assert.True(t, ledger.passedHealth(ids[len(ids)-1]))
	assert.Len(t, ledger.healthyByID, startupHealthLedgerCapacity)
}

// The sticky health rule outside the watch: maintenance readiness judges a
// running member that a startup watch saw pass its check as healthy, whatever
// its check reports now, and reads health fresh otherwise.
func TestMaintenanceReadinessHonorsStickyHealth(t *testing.T) {
	gated, err := json.Marshal(manifest.Manifest{Image: "nginx:latest", HealthCheck: &manifest.HealthCheckConfig{
		Test: []string{"CMD-SHELL", "true"}, Retries: 1,
	}})
	require.NoError(t, err)
	unhealthy := ContainerInfo{
		ContainerID: "maintained", ServiceName: manifest.DefaultServiceName,
		Status: "running", Health: HealthStatusUnhealthy,
	}
	mock := &mockDockerClient{InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
		info := unhealthy
		return &info, nil
	}}
	b := newBackendForTest(mock, nil)
	target := shared.Release{Manifest: gated}

	readiness, err := b.classifyRecoveredMaintenanceReadiness(context.Background(), shared.MaintenanceIntentClaim{},
		target, []ContainerInfo{unhealthy})
	require.NoError(t, err)
	assert.Equal(t, maintenanceReadinessUnready, readiness, "without a passed check, unhealthy is unready")

	b.startupHealth.recordPassedHealth(healthyPass("maintained"))
	readiness, err = b.classifyRecoveredMaintenanceReadiness(context.Background(), shared.MaintenanceIntentClaim{},
		target, []ContainerInfo{unhealthy})
	require.NoError(t, err)
	assert.Equal(t, maintenanceReadinessReady, readiness, "a passed check stays passed while the member runs")

	exited := unhealthy
	exited.Status = "exited"
	readiness, err = b.classifyRecoveredMaintenanceReadiness(context.Background(), shared.MaintenanceIntentClaim{},
		target, []ContainerInfo{exited})
	require.NoError(t, err)
	assert.Equal(t, maintenanceReadinessUnready, readiness, "an exit is never hidden")
}

// The sticky health rule through the provision's own outcome authority: a
// health-gated member passes its check, so the startup watch reports Ready,
// and then flaps unhealthy before the classifier that confirms that Ready
// re-reads it. The classifier applies the watch's rule, so the provision
// settles Ready at once, with a success callback, instead of an ambiguous
// attempt that recovery would fail.
func TestStartupHealthIsStickyThroughTheOutcomeClassifier(t *testing.T) {
	gated, err := json.Marshal(manifest.Manifest{Image: "nginx:latest", HealthCheck: &manifest.HealthCheckConfig{
		Test: []string{"CMD-SHELL", "true"}, Retries: 1,
	}})
	require.NoError(t, err)
	f := newStartupCrashFixtureWithPayload(t, budgetTestLease, countedBudget(t), gated)
	f.candidate = func(info ContainerInfo) ContainerInfo {
		info.Status, info.Health = "running", HealthStatusHealthy
		if f.b.startupHealth.passedHealth(info.ContainerID) {
			// Healthy once, as the watch saw it; unhealthy on every read after.
			info.Health = HealthStatusUnhealthy
		}
		return info
	}
	f.provision(t)
	awaitProvisionWorkerQuiescence(t, f.b, budgetTestLease)
	require.Eventually(t, func() bool {
		intents, err := f.b.operationSettlement.ListOperationIntents()
		return err == nil && len(intents) == 0
	}, 5*time.Second, 10*time.Millisecond, "the operation settles live")
	ready := f.projection(t)
	assert.Equal(t, backend.ProvisionStatusReady, ready.Status, "a flap after the check passed never fails the provision")
	assert.Empty(t, f.failureCallbacks())
	require.NotEmpty(t, f.leaseContainers(t))
	for _, container := range f.leaseContainers(t) {
		info, err := f.mock.InspectContainer(context.Background(), container.ContainerID)
		require.NoError(t, err)
		assert.Equal(t, HealthStatusUnhealthy, info.Health, "the member really is unhealthy now")
	}
	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return slices.ContainsFunc(f.callbacks, func(callback backend.CallbackPayload) bool {
			return callback.Status == backend.CallbackStatusSuccess
		})
	}, 5*time.Second, 10*time.Millisecond, "the Ready outcome is published")
}
