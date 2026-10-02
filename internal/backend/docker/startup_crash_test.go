package docker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// startupCrashFixture is an ACTIVE lease whose Failed predecessor is being
// re-provisioned, and whose every candidate container exits during startup
// verification. The predecessor ("old-container") is removed by the
// candidate's teardown; candidate containers stay in the inventory, exited,
// until something removes them.
type startupCrashFixture struct {
	b         *Backend
	leaseUUID string
	payload   []byte

	callbacksMu sync.Mutex
	callbacks   []backend.CallbackPayload
	callbackURL func() string
}

func newStartupCrashFixture(t *testing.T, leaseUUID string, budget leasesm.TerminalBudget) *startupCrashFixture {
	t.Helper()
	payload := validManifestJSON("nginx:latest")
	mock := &mockDockerClient{
		RemoveContainerFn: func(context.Context, string) error { return nil },
		PullImageFn:       func(context.Context, string, time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: leaseUUID, Status: backend.ProvisionStatusFailed,
			FailCount: 2, Quantity: 1, ContainerIDs: []string{"old-container"},
			TerminalBudget: budget,
		}},
	})
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	require.NoError(t, b.pool.TryAllocate(leaseUUID+"-app-0", "docker-small", "tenant-a"))
	prepareFailedProvisionReplacement(t, b, mock, leaseUUID, "tenant-a", "docker-small", payload)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	exited := func(info ContainerInfo) ContainerInfo {
		if info.ContainerID != "old-container" {
			info.Status = "exited"
			info.ExitCode = 1
		}
		return info
	}
	inspect := mock.InspectContainerFn
	mock.InspectContainerFn = func(ctx context.Context, containerID string) (*ContainerInfo, error) {
		info, err := inspect(ctx, containerID)
		if err != nil || info == nil {
			return info, err
		}
		observed := exited(*info)
		return &observed, nil
	}
	list := mock.ListManagedContainersFn
	mock.ListManagedContainersFn = func(ctx context.Context) ([]ContainerInfo, error) {
		containers, err := list(ctx)
		for index := range containers {
			containers[index] = exited(containers[index])
		}
		return containers, err
	}

	f := &startupCrashFixture{b: b, leaseUUID: leaseUUID, payload: payload}
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload backend.CallbackPayload
		if json.Unmarshal(body, &payload) == nil {
			f.callbacksMu.Lock()
			f.callbacks = append(f.callbacks, payload)
			f.callbacksMu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(callbackServer.Close)
	rebuildCallbackSender(b, callbackServer.Client())
	b.wg.Go(b.callbackSender.RunReplayLoop)
	f.callbackURL = func() string { return testOperationCallbackURL(callbackServer.URL + "/callbacks/provision") }
	return f
}

func (f *startupCrashFixture) provision(t *testing.T) {
	t.Helper()
	req := newProvisionRequest(f.leaseUUID, "tenant-a", "docker-small", 1, f.payload)
	req.CallbackURL = f.callbackURL()
	require.NoError(t, f.b.Provision(context.Background(), req))
}

func (f *startupCrashFixture) projection(t *testing.T) recoveredProvision {
	t.Helper()
	f.b.provisionsMu.RLock()
	defer f.b.provisionsMu.RUnlock()
	require.Contains(t, f.b.provisions, f.leaseUUID)
	return recoveredFromProvision(f.b.provisions[f.leaseUUID])
}

func (f *startupCrashFixture) failureCallbacks() []backend.CallbackPayload {
	f.callbacksMu.Lock()
	defer f.callbacksMu.Unlock()
	var failures []backend.CallbackPayload
	for _, callback := range f.callbacks {
		if callback.Status == backend.CallbackStatusFailed {
			failures = append(failures, callback)
		}
	}
	return failures
}

// TestActiveReprovisionStartupCrashSettlesFailed pins ENG-1125's un-wedge for
// an ACTIVE lease whose re-provision crashes during startup verification.
// The worker's outcome is ambiguous (its compose launch was a tenant Step), so
// it keeps the exited cohort. A single live recovery pass, with the operation
// deadline still far away, then settles the failure from that positive
// evidence, publishes Failed with the curated startup reason, and leaves the
// lease re-provisionable at once: never Provisioning forever.
func TestActiveReprovisionStartupCrashSettlesFailed(t *testing.T) {
	// One counted death is already on the budget; recovery-published failures
	// neither count nor reset it.
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	b := f.b
	predecessor, err := b.releaseStore.LatestActive(budgetTestLease)
	require.NoError(t, err)
	require.NotNil(t, predecessor)
	identity, ok := predecessor.RuntimeIdentity()
	require.True(t, ok)

	for attempt := 1; attempt <= 2; attempt++ {
		// providerd re-provisions an ACTIVE + Failed lease; admission must accept
		// it, including right after recovery published the previous failure.
		f.provision(t)
		awaitProvisionWorkerQuiescence(t, b, budgetTestLease)
		assert.Equal(t, backend.ProvisionStatusProvisioning, f.projection(t).Status,
			"an ambiguous worker outcome makes no Provisioning -> Failed transition")

		// One pass, with the operation's deadline untouched: the kept exited
		// cohort is positive failure evidence, so nothing waits for the deadline.
		require.Greater(t, b.cfg.ProvisionTimeout, time.Minute)
		require.NoError(t, b.recoverLiveOperationIntents(context.Background()))

		intents, err := b.operationSettlement.ListOperationIntents()
		require.NoError(t, err)
		assert.Empty(t, intents, "one live recovery pass settles the failed operation")
		failed := f.projection(t)
		assert.Equal(t, backend.ProvisionStatusFailed, failed.Status)
		assert.Equal(t, backend.ReasonContainerExited, failed.Reason)
		assert.Equal(t, backend.MsgContainerExitedDuringStartup, failed.Message)
		assert.Equal(t, 2+attempt, failed.FailCount, "the failure is recorded in the lifetime diagnostic")
		assert.Empty(t, failed.ContainerIDs, "recovery proved the attempt's exact absence")
		assert.Equal(t, identity.CallbackURL(), failed.CallbackURL,
			"the Failed projection describes the durable predecessor runtime, so re-provisioning is admitted")
		assert.Equal(t, predecessor.Version, failed.ActiveReleaseVersion)
		assert.Equal(t, &backend.TerminalBudgetObservation{
			Verdict: backend.TerminalVerdictRetry, ConsecutiveFailures: 1,
		}, failed.ObserveTerminalBudget(), "a recovery-published failure neither counts nor resets the streak")
		require.Eventually(t, func() bool { return len(f.failureCallbacks()) == attempt },
			5*time.Second, 10*time.Millisecond, "each failure is published exactly once")
		assert.Equal(t, backend.MsgContainerExitedDuringStartup, f.failureCallbacks()[attempt-1].Error)
	}

	// A later recovery sweep keeps the curated failure and the budget.
	require.NoError(t, b.recoverState(context.Background()))
	rebuilt := f.projection(t)
	assert.Equal(t, backend.ProvisionStatusFailed, rebuilt.Status)
	assert.Equal(t, backend.ReasonContainerExited, rebuilt.Reason)
	assert.Equal(t, 1, rebuilt.ObserveTerminalBudget().ConsecutiveFailures)
}
