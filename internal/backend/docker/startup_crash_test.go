package docker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
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
			// The event loop records the death it observed as it happens.
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

	// A later recovery sweep keeps the curated failure and the budget. The
	// active Release has no container until providerd re-provisions the
	// lease, which is expected for a settled failure: it is not reported as a
	// cohort divergence at ERROR on every pass.
	logs := &lockedLog{}
	b.logger = slog.New(slog.NewTextHandler(logs, nil))
	require.NoError(t, b.recoverState(context.Background()))
	rebuilt := f.projection(t)
	assert.Equal(t, backend.ProvisionStatusFailed, rebuilt.Status)
	assert.Equal(t, backend.ReasonContainerExited, rebuilt.Reason)
	assert.Equal(t, 1, rebuilt.ObserveTerminalBudget().ConsecutiveFailures)
	assert.NotContains(t, logs.String(), "recovered container cohort differs from durable release")
}

// lockedLog is a log sink safe for concurrent writers.
type lockedLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
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

// A canonical volume of the lease that is neither the predecessor's nor
// created by this attempt is volume state the live rollback may not destroy,
// so the classifier's absence proof could never hold. The rollback is refused
// before anything is removed (P2-1): the attempt stays Ambiguous with its
// exited cohort kept, and one live recovery pass settles the failure, never
// counted, instead of waiting out provision_timeout behind an empty inventory.
func TestStartupFailureWithUnattributedVolumeStateKeepsTheCohortForRecovery(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	f.death = observedRunDeath
	leftover := canonicalVolumeName(budgetTestLease, manifest.DefaultServiceName, 7)
	inventory := newVolumeSet(leftover)
	f.b.volumes = &mockVolumeManager{ListFn: inventory.list, DestroyFn: inventory.destroy}
	f.provision(t)
	awaitProvisionWorkerQuiescence(t, f.b, budgetTestLease)
	assert.Equal(t, backend.ProvisionStatusProvisioning, f.projection(t).Status,
		"unattributed volume state refuses the live rollback")
	assert.Empty(t, f.failureCallbacks(), "nothing is published for an ambiguous attempt")
	assert.NotEmpty(t, f.leaseContainers(t), "nothing was removed before the refusal: the exited cohort is kept")

	f.requireOneRecoveryPassSettles(t)
}

// A startup failure whose execution already holds an issue cannot settle
// live: the Guard would discard its finding, so the session refuses it before
// anything is removed (P2-1). A failed detection is such an issue (the launch
// proceeds degraded, and the crash it may have caused is the platform's).
// The exited cohort is kept, and one live recovery pass settles the failure,
// never counted.
func TestStartupFailureAfterAnEarlierIssueKeepsTheCohortForRecovery(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	f.death = observedRunDeath
	f.mock.DetectWritablePathsFn = func(context.Context, string, int, []string) ([]string, error) {
		return nil, errors.New("writable-path detection helper failed")
	}
	f.provision(t)
	awaitProvisionWorkerQuiescence(t, f.b, budgetTestLease)
	assert.Equal(t, backend.ProvisionStatusProvisioning, f.projection(t).Status,
		"the session refused the finding: the outcome is ambiguous")
	assert.Empty(t, f.failureCallbacks(), "nothing is published for an ambiguous attempt")
	assert.NotEmpty(t, f.leaseContainers(t), "nothing was rolled back: the exited cohort is kept")

	f.requireOneRecoveryPassSettles(t)
}

// Launch debt of the lease that this launch's receipt did not clear would fail
// the classifier's confirmation after any rollback, so the rollback is refused
// before anything is removed, and recovery settles the kept cohort.
func TestStartupFailureWithLaunchDebtKeepsTheCohortForRecovery(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	f.death = observedRunDeath
	check := f.b.volumeLaunches.checkNamespace
	f.b.volumeLaunches.checkNamespace = func(leaseUUID string) error {
		if leaseUUID == budgetTestLease {
			return errors.New("another launch of the lease is unsettled")
		}
		return check(leaseUUID)
	}
	f.provision(t)
	awaitProvisionWorkerQuiescence(t, f.b, budgetTestLease)
	assert.Equal(t, backend.ProvisionStatusProvisioning, f.projection(t).Status)
	assert.Empty(t, f.failureCallbacks(), "nothing is published for an ambiguous attempt")
	assert.NotEmpty(t, f.leaseContainers(t), "nothing was removed before the refusal: the exited cohort is kept")

	f.b.volumeLaunches.checkNamespace = check
	f.requireOneRecoveryPassSettles(t)
}

// requireOneRecoveryPassSettles runs one live operation recovery pass and
// requires it to settle the lease's failed attempt: Failed, with the attempt's
// own curated surface, and never counted.
func (f *startupCrashFixture) requireOneRecoveryPassSettles(t *testing.T) {
	t.Helper()
	pending := f.projection(t)
	before := pending.ObserveTerminalBudget()
	require.NoError(t, f.b.recoverLiveOperationIntents(context.Background()))
	intents, err := f.b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "one live recovery pass settles the failed operation")
	failed := f.projection(t)
	assert.Equal(t, backend.ProvisionStatusFailed, failed.Status)
	assert.Equal(t, backend.ReasonContainerExited, failed.Reason, "the curated surface captured first wins")
	assert.Equal(t, before, failed.ObserveTerminalBudget(), "recovery never counts")
	assert.Empty(t, f.leaseContainers(t), "recovery removed the kept cohort")
}

// rejectLaunch makes the fixture's Compose launch settle its exchange and then
// fail, as `compose up` does when a depends_on dependency exits or turns
// unhealthy, or when the daemon refuses a Start. refused names the containers
// whose Start the daemon answered with a final error.
func (f *startupCrashFixture) rejectLaunch(t *testing.T, refused ...string) {
	t.Helper()
	compose, ok := f.b.compose.(*mockComposeExecutor)
	require.True(t, ok)
	up := compose.UpFn
	compose.LaunchFn = func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) daemonLaunchOutcome {
		if err := up(ctx, project, opts); err != nil {
			return daemonLaunchOutcome{settled: true, err: err}
		}
		return daemonLaunchOutcome{
			settled: true, err: errors.New("compose up: dependency failed to start"), refusedStarts: refused,
		}
	}
}

// A launch exchange that settled with an error fails definitely from what its
// cohort shows (P2-2): a container that exited is a positive fact, and the
// tenant sees ContainerExited. Compose's error text is never read; the attempt
// is rolled back at once. It never counts, even for an observed run: the
// platform did not complete the launch, so the exit may be a consequence of
// what it did not start (only an exit of a launch the platform completed
// counts).
func TestRejectedLaunchWithAnExitedContainerFailsDefinitelyAndNeverCounts(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	f.death = observedRunDeath
	f.rejectLaunch(t)
	tenantBefore, platformBefore := failureCount("tenant_workload"), failureCount("platform")
	failed := f.settleAttempt(t, 1)
	assert.Equal(t, backend.ProvisionStatusFailed, failed.Status)
	assert.Equal(t, backend.ReasonContainerExited, failed.Reason)
	assert.Empty(t, f.leaseContainers(t), "the attempt was rolled back exactly")
	assert.Equal(t, &backend.TerminalBudgetObservation{
		Verdict: backend.TerminalVerdictRetry, ConsecutiveFailures: 1,
	}, failed.ObserveTerminalBudget(), "an exit in a rejected launch never counts")
	assert.Zero(t, failureCount("tenant_workload")-tenantBefore)
	assert.Equal(t, 1.0, failureCount("platform")-platformBefore)
}

// The verifier's case (ENG-1125 P2): Compose starts independent services
// concurrently, so one exchange can hold the daemon's refusal of one member's
// Start next to a sibling that started and then exited (for example, because
// the refused service is missing). The exit is the most specific positive
// fact and keeps its curated surface, but the rejected receipt seals the
// finding as uncounted: the lease's consecutive count is unchanged, and the
// failure is attributed to the platform, even though the live event stream
// observed the sibling's whole run.
func TestRejectedLaunchExitNextToARefusedStartIsPlatform(t *testing.T) {
	const leaseUUID = "0192f1a0-1125-4abc-8def-00000000f1a2"
	stack := manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"web": {Image: "nginx:latest"},
		"db":  {Image: "postgres:16"},
	}}
	payload, err := json.Marshal(stack)
	require.NoError(t, err)

	var removed sync.Map
	var deathRecorded sync.Map
	mock := &mockDockerClient{
		PullImageFn:     func(context.Context, string, time.Duration) error { return nil },
		ContainerLogsFn: func(context.Context, string, int) (string, error) { return "", nil },
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			removed.Store(containerID, true)
			return nil
		},
	}
	var b *Backend
	mock.InspectContainerFn = func(ctx context.Context, containerID string) (*ContainerInfo, error) {
		inventory, err := mock.ListManagedContainersFn(ctx)
		if err != nil {
			return nil, err
		}
		for _, listed := range inventory {
			if listed.ContainerID != containerID {
				continue
			}
			info := listed
			if listed.ServiceName == "db" {
				// The daemon refused its Start: it never ran.
				info.Status = "created"
				return &info, nil
			}
			info.Status, info.ExitCode = "exited", 1
			if _, seen := deathRecorded.LoadOrStore(containerID, true); !seen {
				b.liveDeaths.recordLiveDeath(observedRunDeath(containerID))
			}
			return &info, nil
		}
		return nil, errors.New("no such container")
	}
	b = newBackendForProvisionTest(t, mock, nil)
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
	compose, ok := b.compose.(*mockComposeExecutor)
	require.True(t, ok)
	up := compose.UpFn
	compose.LaunchFn = func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) daemonLaunchOutcome {
		if err := up(ctx, project, opts); err != nil {
			return daemonLaunchOutcome{settled: true, err: err}
		}
		containers, err := compose.PS(ctx, project.Name)
		if err != nil {
			return daemonLaunchOutcome{settled: true, err: err}
		}
		var refused []string
		for _, container := range containers {
			if strings.HasPrefix(container.Service, "db") {
				refused = append(refused, container.ID)
			}
		}
		assert.Len(t, refused, 1, "the db service has exactly one container")
		return daemonLaunchOutcome{
			settled: true, err: errors.New("compose up: Error response from daemon"), refusedStarts: refused,
		}
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

	tenantBefore, platformBefore := failureCount("tenant_workload"), failureCount("platform")
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
	}, 10*time.Second, 10*time.Millisecond, "the rejected launch fails definitely")
	require.Eventually(t, func() bool {
		intents, err := b.operationSettlement.ListOperationIntents()
		return err == nil && len(intents) == 0
	}, 5*time.Second, 10*time.Millisecond, "the failed operation settles durably")
	mu.Lock()
	assert.Equal(t, backend.MsgContainerExitedDuringStartup, failures[0].Error, "the exit keeps its curated surface")
	mu.Unlock()

	b.provisionsMu.RLock()
	failed := recoveredFromProvision(b.provisions[leaseUUID])
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, failed.Status)
	assert.Equal(t, backend.ReasonContainerExited, failed.Reason)
	assert.Equal(t, &backend.TerminalBudgetObservation{Verdict: backend.TerminalVerdictRetry},
		failed.ObserveTerminalBudget(), "the consecutive count is unchanged")
	assert.Zero(t, failureCount("tenant_workload")-tenantBefore, "never attributed to the tenant")
	assert.Equal(t, 1.0, failureCount("platform")-platformBefore, "attributed to the platform")
	inventory, err := mock.ListManagedContainers(context.Background())
	require.NoError(t, err)
	assert.Empty(t, inventory, "the attempt was rolled back exactly")
}

// A Start the daemon refused with a final response, on a container that never
// ran (an OCI runtime error, such as an entrypoint missing from the image),
// fails the provision definitely with ContainerStartFailed (P2-2). No tenant
// process ran, so it never counts.
func TestRejectedLaunchRefusedStartFailsDefinitelyAndNeverCounts(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	f.candidate = func(info ContainerInfo) ContainerInfo {
		info.Status, info.ExitCode = "created", 127
		return info
	}
	f.rejectLaunch(t, "container-1")
	platformBefore := failureCount("platform")
	failed := f.settleAttempt(t, 1)
	assert.Equal(t, backend.ProvisionStatusFailed, failed.Status)
	assert.Equal(t, backend.ReasonContainerStartFailed, failed.Reason)
	assert.Equal(t, backend.MsgContainerStartRefused, failed.Message)
	assert.Equal(t, backend.MsgContainerStartRefused, f.failureCallbacks()[0].Error)
	assert.Empty(t, f.leaseContainers(t), "the attempt was rolled back exactly")
	assert.Equal(t, 1, failed.ObserveTerminalBudget().ConsecutiveFailures, "never counted")
	assert.Equal(t, 1.0, failureCount("platform")-platformBefore)
}

// A rejected exchange whose cohort shows no positive failure is not definite
// (P2-2): a created container whose Start the daemon never refused says
// nothing about why the launch failed. The worker publishes nothing and keeps
// the settled exchange's cohort for recovery.
func TestRejectedLaunchWithoutAPositiveFactStaysAmbiguous(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	f.candidate = func(info ContainerInfo) ContainerInfo {
		info.Status = "created"
		return info
	}
	f.rejectLaunch(t)
	f.provision(t)
	awaitProvisionWorkerQuiescence(t, f.b, budgetTestLease)
	assert.Equal(t, backend.ProvisionStatusProvisioning, f.projection(t).Status)
	assert.Empty(t, f.failureCallbacks(), "nothing is published for an ambiguous attempt")
	assert.NotEmpty(t, f.leaseContainers(t), "a settled exchange keeps its cohort for recovery")
}

// A rejected exchange whose cohort is nevertheless complete and running (a
// Start that got a final error response while its container runs, such as a
// response an authorization plugin denied after the daemon's handler ran)
// shows no positive failure, so the worker leaves the attempt in flight with
// its cohort. The next recovery pass re-inspects every member under the same
// checks it applies to any interrupted provision and adopts the cohort Ready,
// with a success callback; nothing counts.
func TestRejectedLaunchWithACompleteRunningCohortSettlesReadyOnRecovery(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	f.candidate = func(info ContainerInfo) ContainerInfo {
		info.Status = "running"
		return info
	}
	f.rejectLaunch(t)
	f.provision(t)
	awaitProvisionWorkerQuiescence(t, f.b, budgetTestLease)
	pending := f.projection(t)
	assert.Equal(t, backend.ProvisionStatusProvisioning, pending.Status, "no positive failure: the attempt stays in flight")
	assert.Empty(t, f.failureCallbacks(), "nothing is published for an ambiguous attempt")
	require.NotEmpty(t, f.leaseContainers(t), "the running cohort is kept")
	before := pending.ObserveTerminalBudget()

	require.NoError(t, f.b.recoverLiveOperationIntents(context.Background()))
	intents, err := f.b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "one live recovery pass settles the operation")
	ready := f.projection(t)
	assert.Equal(t, backend.ProvisionStatusReady, ready.Status, "recovery adopts the complete running cohort")
	assert.Equal(t, before, ready.ObserveTerminalBudget(), "nothing counts")
	assert.NotEmpty(t, f.leaseContainers(t), "the adopted cohort stays")
	assert.Empty(t, f.failureCallbacks())
	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return slices.ContainsFunc(f.callbacks, func(callback backend.CallbackPayload) bool {
			return callback.Status == backend.CallbackStatusSuccess
		})
	}, 5*time.Second, 10*time.Millisecond, "the Ready outcome is published")
}

// A degraded launch through the docker backend's own preparation path:
// writable-path seeding skipped a path without any failed Step (the
// extraction helper reported that path's failure), so the launch proceeds on
// the tmpfs fallback, degraded. A startup crash after it fails definitely but
// never counts, even with an observed run: the platform skipped part of its
// own preparation.
func TestDegradedLaunchStartupCrashIsDefiniteAndNeverCounts(t *testing.T) {
	f := newStartupCrashFixture(t, budgetTestLease, countedBudget(t))
	f.death = observedRunDeath
	volumeRoot := t.TempDir()
	inventory := newVolumeSet()
	f.b.cfg.VolumeDataPath = volumeRoot
	f.b.volumes = &mockVolumeManager{
		defaultDir: volumeRoot,
		CreateFn: func(_ context.Context, id string, _ int64) (string, bool, error) {
			path := filepath.Join(volumeRoot, id)
			if err := os.MkdirAll(path, 0o755); err != nil {
				return "", false, err
			}
			inventory.mu.Lock()
			inventory.present[id] = true
			inventory.mu.Unlock()
			return path, true, nil
		},
		ListFn:    inventory.list,
		DestroyFn: inventory.destroy,
	}
	f.mock.DetectWritablePathsFn = func(context.Context, string, int, []string) ([]string, error) {
		return []string{"/var/lib/app"}, nil
	}
	f.mock.ExtractImageContentFn = func(_ context.Context, _ string, paths []string, _ string, _, _ int64) map[string]error {
		failures := make(map[string]error, len(paths))
		for _, path := range paths {
			failures[path] = errors.New("image layer read failed")
		}
		return failures
	}
	platformBefore := failureCount("platform")
	tenantBefore := failureCount("tenant_workload")
	failed := f.settleAttempt(t, 1)
	assert.Equal(t, backend.ProvisionStatusFailed, failed.Status, "a degraded launch's startup crash is definite")
	assert.Equal(t, backend.ReasonContainerExited, failed.Reason)
	assert.Equal(t, &backend.TerminalBudgetObservation{
		Verdict: backend.TerminalVerdictRetry, ConsecutiveFailures: 1,
	}, failed.ObserveTerminalBudget(), "an observed run after a degraded launch never counts")
	assert.Equal(t, 1.0, failureCount("platform")-platformBefore)
	assert.Zero(t, failureCount("tenant_workload")-tenantBefore)
	assert.Empty(t, f.leaseContainers(t), "the attempt was rolled back exactly")
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
