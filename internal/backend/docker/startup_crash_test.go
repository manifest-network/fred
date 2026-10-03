package docker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// startupCrashFixture is an ACTIVE lease whose Failed predecessor is being
// re-provisioned, and whose every candidate container fails during startup
// verification (by default it exits with code 1). The predecessor
// ("old-container") is removed by the candidate's teardown.
type startupCrashFixture struct {
	b         *Backend
	mock      *mockDockerClient
	leaseUUID string
	payload   []byte

	mu sync.Mutex
	// candidate is what Docker reports for a candidate container.
	candidate func(ContainerInfo) ContainerInfo
	// death, when set, is the live provenance the event stream records for a
	// candidate container's exit; nil models no live event.
	death     func(id string) failurecause.Provenance
	recorded  map[string]bool
	callbacks []backend.CallbackPayload

	callbackURL func() string
}

func exitedCandidate(info ContainerInfo) ContainerInfo {
	info.Status = "exited"
	info.ExitCode = 1
	return info
}

func observedRunDeath(id string) failurecause.Provenance {
	session := failurecause.NewEventSession()
	session.ObserveStart(id)
	return session.ObserveExit(id)
}

func signaledRunDeath(id string) failurecause.Provenance {
	session := failurecause.NewEventSession()
	session.ObserveStart(id)
	session.ObserveSignal(id)
	return session.ObserveExit(id)
}

func newStartupCrashFixture(t *testing.T, leaseUUID string, budget leasesm.TerminalBudget) *startupCrashFixture {
	t.Helper()
	return newStartupCrashFixtureWithPayload(t, leaseUUID, budget, validManifestJSON("nginx:latest"))
}

func newStartupCrashFixtureWithPayload(t *testing.T, leaseUUID string, budget leasesm.TerminalBudget, payload []byte) *startupCrashFixture {
	t.Helper()
	mock := &mockDockerClient{
		RemoveContainerFn: func(context.Context, string) error { return nil },
		PullImageFn:       func(context.Context, string, time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
		ContainerLogsFn: func(context.Context, string, int) (string, error) { return "startup crash", nil },
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

	f := &startupCrashFixture{
		b: b, mock: mock, leaseUUID: leaseUUID, payload: payload,
		candidate: exitedCandidate, recorded: make(map[string]bool),
	}
	observe := func(info ContainerInfo) ContainerInfo {
		if info.ContainerID == "old-container" || info.LeaseUUID != leaseUUID {
			return info
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		observed := f.candidate(info)
		if observed.Status == "exited" && f.death != nil && !f.recorded[info.ContainerID] {
			// The event reader records the death it observed as it happens.
			f.recorded[info.ContainerID] = true
			b.liveDeaths.recordLiveDeath(f.death(info.ContainerID))
		}
		return observed
	}
	inspect := mock.InspectContainerFn
	mock.InspectContainerFn = func(ctx context.Context, containerID string) (*ContainerInfo, error) {
		info, err := inspect(ctx, containerID)
		if err != nil || info == nil {
			return info, err
		}
		observed := observe(*info)
		return &observed, nil
	}
	list := mock.ListManagedContainersFn
	mock.ListManagedContainersFn = func(ctx context.Context) ([]ContainerInfo, error) {
		containers, err := list(ctx)
		for index := range containers {
			containers[index] = observe(containers[index])
		}
		return containers, err
	}

	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload backend.CallbackPayload
		if json.Unmarshal(body, &payload) == nil {
			f.mu.Lock()
			f.callbacks = append(f.callbacks, payload)
			f.mu.Unlock()
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
	// The fixture's Compose may reuse a container ID across attempts; each
	// attempt's container dies once.
	f.mu.Lock()
	clear(f.recorded)
	f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
	var failures []backend.CallbackPayload
	for _, callback := range f.callbacks {
		if callback.Status == backend.CallbackStatusFailed {
			failures = append(failures, callback)
		}
	}
	return failures
}

// leaseContainers is the lease's part of Docker's authoritative inventory.
func (f *startupCrashFixture) leaseContainers(t *testing.T) []ContainerInfo {
	t.Helper()
	all, err := f.mock.ListManagedContainers(context.Background())
	require.NoError(t, err)
	var mine []ContainerInfo
	for _, container := range all {
		if container.LeaseUUID == f.leaseUUID {
			mine = append(mine, container)
		}
	}
	return mine
}

// settleAttempt provisions once and waits until the worker and the actor's
// terminal publication are done.
func (f *startupCrashFixture) settleAttempt(t *testing.T, attempt int) recoveredProvision {
	t.Helper()
	f.provision(t)
	awaitProvisionWorkerQuiescence(t, f.b, f.leaseUUID)
	require.Eventually(t, func() bool { return len(f.failureCallbacks()) == attempt },
		5*time.Second, 10*time.Millisecond, "each failure is published exactly once")
	require.Eventually(t, func() bool {
		intents, err := f.b.operationSettlement.ListOperationIntents()
		return err == nil && len(intents) == 0
	}, 5*time.Second, 10*time.Millisecond, "the failed operation settles durably")
	return f.projection(t)
}

// TestActiveReprovisionStartupCrashSettlesFailed pins ENG-1125's definite live
// path for an ACTIVE lease whose re-provision exits during startup
// verification. The worker rolls the attempt back exactly and the actor
// publishes Failed at once, with the curated startup reason, no container,
// and the durable predecessor runtime, so the lease re-provisions at once:
// never Provisioning, and no recovery pass is needed. Without a live event
// for the death, the failure is recorded but never counts.
func TestActiveReprovisionStartupCrashSettlesFailed(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	b := f.b
	predecessor, err := b.releaseStore.LatestActive(budgetTestLease)
	require.NoError(t, err)
	require.NotNil(t, predecessor)
	identity, ok := predecessor.RuntimeIdentity()
	require.True(t, ok)
	unknownBefore := failureCount("unknown")

	for attempt := 1; attempt <= 2; attempt++ {
		// providerd re-provisions an ACTIVE + Failed lease; admission must accept
		// it, including right after the previous definite failure.
		failed := f.settleAttempt(t, attempt)
		assert.Equal(t, backend.ProvisionStatusFailed, failed.Status, "the worker's own outcome is definite")
		assert.Equal(t, backend.ReasonContainerExited, failed.Reason)
		assert.Equal(t, backend.MsgContainerExitedDuringStartup, failed.Message)
		assert.Equal(t, 2+attempt, failed.FailCount, "the failure is recorded in the lifetime diagnostic")
		assert.Empty(t, failed.ContainerIDs, "the attempt was rolled back exactly")
		assert.Empty(t, f.leaseContainers(t), "no container of the attempt survives")
		assert.Equal(t, identity.CallbackURL(), failed.CallbackURL,
			"the Failed projection describes the durable predecessor runtime, so re-provisioning is admitted")
		assert.Equal(t, predecessor.Version, failed.ActiveReleaseVersion)
		assert.Equal(t, &backend.TerminalBudgetObservation{
			Verdict: backend.TerminalVerdictRetry, ConsecutiveFailures: 1,
		}, failed.ObserveTerminalBudget(), "a death with no live event neither counts nor resets the streak")
		assert.Equal(t, backend.MsgContainerExitedDuringStartup, f.failureCallbacks()[attempt-1].Error)
	}
	assert.Equal(t, 2.0, failureCount("unknown")-unknownBefore)

	// A later recovery sweep keeps the curated failure and the budget.
	require.NoError(t, b.recoverState(context.Background()))
	rebuilt := f.projection(t)
	assert.Equal(t, backend.ProvisionStatusFailed, rebuilt.Status)
	assert.Equal(t, backend.ReasonContainerExited, rebuilt.Reason)
	assert.Equal(t, 1, rebuilt.ObserveTerminalBudget().ConsecutiveFailures)
}

// A first provision (a PENDING lease, no predecessor) whose container exits
// during startup verification fails definitely too: the failure callback,
// which providerd turns into a rejection, carries the curated reason at once,
// and nothing of the attempt is left.
func TestFirstProvisionStartupCrashFailsDefinitely(t *testing.T) {
	const leaseUUID = "0192f1a0-1125-4abc-8def-00000000f1a0"
	var removed sync.Map
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 3}, nil
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			removed.Store(containerID, true)
			return nil
		},
		ContainerLogsFn: func(context.Context, string, int) (string, error) { return "boom", nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	list := mock.ListManagedContainersFn
	mock.ListManagedContainersFn = func(ctx context.Context) ([]ContainerInfo, error) {
		all, err := list(ctx)
		kept := all[:0]
		for _, container := range all {
			if _, gone := removed.Load(container.ContainerID); !gone {
				kept = append(kept, container)
			}
		}
		return kept, err
	}

	var mu sync.Mutex
	var failures []backend.CallbackPayload
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload backend.CallbackPayload
		if json.Unmarshal(body, &payload) == nil && payload.Status == backend.CallbackStatusFailed {
			mu.Lock()
			failures = append(failures, payload)
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(callbackServer.Close)
	rebuildCallbackSender(b, callbackServer.Client())
	b.wg.Go(b.callbackSender.RunReplayLoop)

	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL + "/callbacks/provision")
	require.NoError(t, b.Provision(context.Background(), req))
	awaitProvisionWorkerQuiescence(t, b, leaseUUID)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(failures) == 1
	}, 5*time.Second, 10*time.Millisecond, "the definite failure is published once")
	mu.Lock()
	assert.Equal(t, backend.MsgContainerExitedDuringStartup, failures[0].Error)
	mu.Unlock()

	b.provisionsMu.RLock()
	failed := recoveredFromProvision(b.provisions[leaseUUID])
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, failed.Status)
	assert.Equal(t, backend.ReasonContainerExited, failed.Reason)
	assert.Empty(t, failed.ContainerIDs)
	inventory, err := mock.ListManagedContainers(context.Background())
	require.NoError(t, err)
	assert.Empty(t, inventory, "the attempt was rolled back exactly")
	require.Eventually(t, func() bool {
		intents, err := b.operationSettlement.ListOperationIntents()
		return err == nil && len(intents) == 0
	}, 5*time.Second, 10*time.Millisecond, "the failed operation settles durably")
}

// The multi-service window (ENG-1125 added scope), end to end through the
// provision worker: a service that passed its own startup check and exits
// while another service still waits for its health check fails the
// provision definitely, instead of reaching Ready with a dead container.
func TestStackSiblingExitDuringHealthWaitFailsDefinitely(t *testing.T) {
	const leaseUUID = "0192f1a0-1125-4abc-8def-00000000f1a1"
	stack := manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"web": {Image: "nginx:latest"},
		"db": {Image: "postgres:16", HealthCheck: &manifest.HealthCheckConfig{
			Test: []string{"CMD-SHELL", "true"}, Retries: 1,
		}},
	}}
	payload, err := json.Marshal(stack)
	require.NoError(t, err)

	var removed sync.Map
	var webInspections sync.Map
	mock := &mockDockerClient{
		PullImageFn:     func(context.Context, string, time.Duration) error { return nil },
		ContainerLogsFn: func(context.Context, string, int) (string, error) { return "", nil },
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			removed.Store(containerID, true)
			return nil
		},
	}
	mock.InspectContainerFn = func(ctx context.Context, containerID string) (*ContainerInfo, error) {
		inventory, err := mock.ListManagedContainersFn(ctx)
		if err != nil {
			return nil, err
		}
		for _, listed := range inventory {
			if listed.ContainerID != containerID {
				continue
			}
			if listed.ServiceName == "db" {
				return &ContainerInfo{ContainerID: containerID, Status: "running", Health: HealthStatusStarting}, nil
			}
			calls, _ := webInspections.LoadOrStore(containerID, new(int))
			count := calls.(*int)
			*count++
			if *count >= 2 {
				return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
			}
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		}
		return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
	}
	b := newBackendForProvisionTest(t, mock, nil)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	list := mock.ListManagedContainersFn
	mock.ListManagedContainersFn = func(ctx context.Context) ([]ContainerInfo, error) {
		all, err := list(ctx)
		kept := all[:0]
		for _, container := range all {
			if _, gone := removed.Load(container.ContainerID); !gone {
				kept = append(kept, container)
			}
		}
		return kept, err
	}
	var mu sync.Mutex
	var failures []backend.CallbackPayload
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload backend.CallbackPayload
		if json.Unmarshal(body, &payload) == nil && payload.Status == backend.CallbackStatusFailed {
			mu.Lock()
			failures = append(failures, payload)
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(callbackServer.Close)
	rebuildCallbackSender(b, callbackServer.Client())
	b.wg.Go(b.callbackSender.RunReplayLoop)

	req := backend.ProvisionRequest{
		LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
		},
		CallbackURL: testOperationCallbackURL(callbackServer.URL + "/callbacks/provision"),
		Payload:     payload,
	}
	require.NoError(t, b.Provision(context.Background(), req))
	awaitProvisionWorkerQuiescence(t, b, leaseUUID)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(failures) == 1
	}, 10*time.Second, 10*time.Millisecond, "the sibling's exit fails the provision definitely")
	mu.Lock()
	assert.Equal(t, backend.MsgContainerExitedDuringStartup, failures[0].Error, "web has no health check of its own")
	mu.Unlock()
	b.provisionsMu.RLock()
	failed := recoveredFromProvision(b.provisions[leaseUUID])
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, failed.Status, "never Ready with a dead container")
	assert.Equal(t, backend.ReasonContainerExited, failed.Reason)
	inventory, err := mock.ListManagedContainers(context.Background())
	require.NoError(t, err)
	assert.Empty(t, inventory, "both services' containers were removed")
}

// A startup crash counts only when the live event stream observed the
// container's whole run with no signal to it, and even then the streak can
// exhaust only behind the 30-minute floor: a fast outage loop of counted
// crashes never closes the lease within the span.
func TestActiveReprovisionStartupCrashAttribution(t *testing.T) {
	t.Run("an observed run counts, behind the floor", func(t *testing.T) {
		f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
		f.death = observedRunDeath
		tenantBefore := failureCount("tenant_workload")
		for attempt := 1; attempt <= 3; attempt++ {
			failed := f.settleAttempt(t, attempt)
			assert.Equal(t, &backend.TerminalBudgetObservation{
				Verdict: backend.TerminalVerdictRetry, ConsecutiveFailures: 1 + attempt,
			}, failed.ObserveTerminalBudget(), "counted, but the streak is minutes old")
		}
		assert.Equal(t, 3.0, failureCount("tenant_workload")-tenantBefore)
	})
	t.Run("a signal during verification is a disruption", func(t *testing.T) {
		f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
		f.death = signaledRunDeath
		disruptionBefore := failureCount("disruption")
		failed := f.settleAttempt(t, 1)
		assert.Equal(t, backend.ReasonContainerExited, failed.Reason)
		assert.Equal(t, 1, failed.ObserveTerminalBudget().ConsecutiveFailures)
		assert.Equal(t, 1.0, failureCount("disruption")-disruptionBefore)
	})
}

// A health check that reports unhealthy, or never passes before the startup
// deadline, fails the provision definitely with HealthCheckFailed, and never
// counts: the workload was running.
func TestActiveReprovisionUnhealthyStartupIsDefiniteAndUncounted(t *testing.T) {
	gated, err := json.Marshal(manifest.Manifest{Image: "nginx:latest", HealthCheck: &manifest.HealthCheckConfig{
		Test: []string{"CMD-SHELL", "true"}, Retries: 1,
	}})
	require.NoError(t, err)
	for _, tt := range []struct {
		name    string
		health  HealthStatus
		message string
	}{
		{"unhealthy", HealthStatusUnhealthy, backend.MsgContainerUnhealthy},
		{"never healthy", HealthStatusStarting, backend.MsgHealthCheckDeadline},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newStartupCrashFixtureWithPayload(t, budgetTestLease, countedBudget(t), gated)
			f.candidate = func(info ContainerInfo) ContainerInfo {
				info.Status, info.Health = "running", tt.health
				return info
			}
			f.b.cfg.ProvisionTimeout = 8 * time.Second
			unhealthyBefore := failureCount("unhealthy")
			failed := f.settleAttempt(t, 1)
			assert.Equal(t, backend.ProvisionStatusFailed, failed.Status)
			assert.Equal(t, backend.ReasonHealthCheckFailed, failed.Reason)
			assert.Equal(t, tt.message, failed.Message)
			assert.Empty(t, f.leaseContainers(t), "the running cohort was removed exactly")
			assert.Equal(t, 1, failed.ObserveTerminalBudget().ConsecutiveFailures, "never counted")
			assert.Equal(t, 1.0, failureCount("unhealthy")-unhealthyBefore)
		})
	}
}

// The classifier mints a definite startup failure only from its own positive
// reads after the rollback. A canonical volume of the lease that is neither
// the predecessor's nor created by this attempt is volume state the rollback
// may not destroy, so the attempt stays Ambiguous.
func TestStartupFailureWithUnattributedVolumeStateStaysAmbiguous(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	leftover := canonicalVolumeName(budgetTestLease, manifest.DefaultServiceName, 7)
	f.b.volumes = &mockVolumeManager{ListForProofFn: func(context.Context) ([]string, error) {
		return []string{leftover}, nil
	}}
	f.provision(t)
	awaitProvisionWorkerQuiescence(t, f.b, budgetTestLease)
	assert.Equal(t, backend.ProvisionStatusProvisioning, f.projection(t).Status,
		"unattributed volume state is not exact absence")
	assert.Empty(t, f.failureCallbacks(), "nothing is published for an ambiguous attempt")
}

// A rollback that cannot complete leaves the attempt Ambiguous: the worker
// publishes nothing, and recovery settles the failure from the remaining
// exited cohort, as for any other ambiguous attempt (PR-A).
func TestStartupRollbackFailureStaysAmbiguousUntilRecovery(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	b := f.b
	var blocked sync.Map
	remove := f.mock.RemoveContainerFn
	f.mock.RemoveContainerFn = func(ctx context.Context, containerID string) error {
		if containerID != "old-container" {
			if _, seen := blocked.LoadOrStore(containerID, true); !seen {
				return errors.New("daemon refused the removal")
			}
		}
		return remove(ctx, containerID)
	}
	f.provision(t)
	awaitProvisionWorkerQuiescence(t, b, budgetTestLease)
	assert.Equal(t, backend.ProvisionStatusProvisioning, f.projection(t).Status,
		"a rollback Step that failed makes the outcome ambiguous")
	assert.NotEmpty(t, f.leaseContainers(t), "the exited cohort is kept for recovery")

	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "one live recovery pass settles the failed operation")
	failed := f.projection(t)
	assert.Equal(t, backend.ProvisionStatusFailed, failed.Status)
	assert.Equal(t, backend.ReasonContainerExited, failed.Reason, "the curated surface captured first wins")
	assert.Equal(t, 1, failed.ObserveTerminalBudget().ConsecutiveFailures, "recovery never counts")
}
