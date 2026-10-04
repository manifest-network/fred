package docker

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

const budgetTestLease = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"

// budgetEventHarness drives one Ready docker lease through real container
// deaths (ENG-799): a controllable Docker event stream and inventory feed the
// real event loop, lease actor and recovery sweep.
type budgetEventHarness struct {
	t        *testing.T
	b        *Backend
	loopDone chan struct{}

	mu            sync.Mutex
	events        chan ContainerEvent // the current subscription's stream
	subscriptions int
	container     ContainerInfo
}

func newBudgetEventHarness(t *testing.T) *budgetEventHarness {
	t.Helper()
	h := newUnstartedBudgetEventHarness(t)
	h.start(func(b *Backend) { b.containerEventLoop() })
	return h
}

// start runs the event loop the way run starts it, and stops it at cleanup.
func (h *budgetEventHarness) start(run func(*Backend)) {
	go func() {
		defer close(h.loopDone)
		run(h.b)
	}()
	h.t.Cleanup(func() {
		h.b.stopCancel()
		<-h.loopDone
		h.b.wg.Wait()
	})
}

// newUnstartedBudgetEventHarness builds the harness without starting its
// event loop, so a test can first replace the storage verifier or choose the
// reconnect backoff.
func newUnstartedBudgetEventHarness(t *testing.T) *budgetEventHarness {
	t.Helper()
	h := &budgetEventHarness{t: t, events: make(chan ContainerEvent, 16), loopDone: make(chan struct{})}
	operationID := mustDockerOperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID.String()
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}}
	mock := &mockDockerClient{
		ContainerEventsFn: func(context.Context) (<-chan ContainerEvent, <-chan error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.subscriptions++
			return h.events, make(chan error)
		},
		InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			observed := h.container
			return &observed, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return []ContainerInfo{h.container}, nil
		},
		ContainerLogsFn: func(context.Context, string, int) (string, error) { return "", nil },
	}
	h.b = newBackendForTest(mock, map[string]*provision{
		budgetTestLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: budgetTestLease, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Status: backend.ProvisionStatusReady, ContainerIDs: []string{"c1"},
			ServiceContainers: map[string][]string{manifest.DefaultServiceName: {"c1"}},
			CallbackURL:       callbackURL, LifecycleCallbackURL: lifecycleCallbackURL,
			ActiveOperationID: operationID, Items: items, ResourceProfiles: testResourceProfiles(t, items),
			StackManifest: restoreStackManifest(),
		}},
	})
	installReadyRuntimeProofForTest(t, h.b, budgetTestLease)
	h.b.provisionsMu.RLock()
	projection := h.b.provisions[budgetTestLease]
	h.container = ContainerInfo{
		ContainerID: "c1", LeaseUUID: budgetTestLease, BackendName: h.b.cfg.Name,
		Tenant: projection.Tenant, ProviderUUID: projection.ProviderUUID,
		SKU: items[0].SKU, ServiceName: items[0].ServiceName, InstanceIndex: 0,
		Image:       projection.StackManifest.Services[items[0].ServiceName].Image,
		CallbackURL: projection.CallbackURL, LifecycleCallbackURL: projection.LifecycleCallbackURL,
		Status: "running",
	}
	h.b.provisionsMu.RUnlock()
	return h
}

// setContainer changes what Docker reports for the lease's container.
func (h *budgetEventHarness) setContainer(status string, exitCode int, oomKilled bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.container.Status = status
	h.container.ExitCode = exitCode
	h.container.OOMKilled = oomKilled
}

func (h *budgetEventHarness) send(actions ...string) {
	h.t.Helper()
	h.sendFor("c1", actions...)
}

// sendFor delivers the given events of containerID on the current stream.
func (h *budgetEventHarness) sendFor(containerID string, actions ...string) {
	h.t.Helper()
	h.mu.Lock()
	events := h.events
	h.mu.Unlock()
	for _, action := range actions {
		select {
		case events <- ContainerEvent{ContainerID: containerID, Action: action}:
		case <-time.After(5 * time.Second):
			h.t.Fatalf("the event loop stopped taking events at %s %s", containerID, action)
		}
	}
}

// awaitSubscriptions waits until the event loop has subscribed n times.
func (h *budgetEventHarness) awaitSubscriptions(n int) {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.subscriptions >= n
	}, 5*time.Second, time.Millisecond, "the event loop never subscribed %d times", n)
}

// countStorageVerifications makes b's storage verifier count its calls. The
// event loop verifies once before each subscription, and its dispatch path
// first of all for every death it handles, so the count shows how far the
// dispatcher got.
func countStorageVerifications(b *Backend) *atomic.Int32 {
	calls := new(atomic.Int32)
	b.storageVerifier = testDockerRuntimeStorageVerifier{
		identity: func() backendidentity.ID { return b.storageIdentity },
		verify: func(context.Context) error {
			calls.Add(1)
			return nil
		},
	}
	return calls
}

// reconnectStream ends the current subscription and makes the next one a
// fresh stream, then waits until the event loop has resubscribed.
func (h *budgetEventHarness) reconnectStream() {
	h.t.Helper()
	h.mu.Lock()
	ended := h.events
	h.events = make(chan ContainerEvent, 16)
	subscribed := h.subscriptions
	h.mu.Unlock()
	close(ended)
	require.Eventually(h.t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.subscriptions > subscribed
	}, 5*time.Second, 5*time.Millisecond, "event loop did not reconnect")
}

func (h *budgetEventHarness) status() backend.ProvisionStatus {
	h.b.provisionsMu.RLock()
	defer h.b.provisionsMu.RUnlock()
	return h.b.provisions[budgetTestLease].Status
}

func (h *budgetEventHarness) awaitStatus(want backend.ProvisionStatus) {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.status() == want },
		5*time.Second, 5*time.Millisecond, "lease never reached %s", want)
	// The actor must be idle before a recovery pass may replace its projection.
	awaitProvisionWorkerQuiescence(h.t, h.b, budgetTestLease)
}

// die makes the container exit as Docker reports it, then delivers the
// given live event sequence (which ends with "die").
func (h *budgetEventHarness) die(exitCode int, oomKilled bool, actions ...string) {
	h.t.Helper()
	h.setContainer("exited", exitCode, oomKilled)
	h.send(actions...)
	h.awaitStatus(backend.ProvisionStatusFailed)
}

// restartOutOfBand models the container running again (an operator's docker
// start), observed by the periodic recovery sweep.
func (h *budgetEventHarness) restartOutOfBand() {
	h.t.Helper()
	h.setContainer("running", 0, false)
	require.NoError(h.t, h.b.recoverState(context.Background()))
	require.Equal(h.t, backend.ProvisionStatusReady, h.status())
}

// wireBudget is what the backend serves on GET /provisions for the lease.
func (h *budgetEventHarness) wireBudget() *backend.TerminalBudgetObservation {
	h.t.Helper()
	provisions, err := h.b.ListProvisions(context.Background())
	require.NoError(h.t, err)
	require.Len(h.t, provisions, 1)
	require.NotNil(h.t, provisions[0].TerminalBudget, "live inventory always carries the budget")
	return provisions[0].TerminalBudget
}

func (h *budgetEventHarness) requireWire(consecutive int, verdict backend.TerminalVerdict) {
	h.t.Helper()
	assert.Equal(h.t, &backend.TerminalBudgetObservation{Verdict: verdict, ConsecutiveFailures: consecutive},
		h.wireBudget())
}

func failureCount(attribution string) float64 {
	return testutil.ToFloat64(leaseFailuresTotal.WithLabelValues(attribution))
}

// failedInterval is how long the harness lease sits Failed between real deaths
// in the aged crash loops: two such intervals put the third death past the
// 30-minute minimum streak span. Time spent Failed is not Ready time, so it
// never resets the streak.
const failedInterval = 16 * time.Minute

// A crash loop reaches an exhausted verdict on the wire only through real actor
// deaths delivered by the live event stream, once the streak is both three
// deaths long and 30 minutes old, and the budget survives every recovery
// rebuild in between (Failed -> Failed and Failed -> Ready). Time alone never
// changes what the wire serves.
func TestTerminalBudget_RealDeathsExhaustAcrossRecoveryRebuilds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetEventHarness(t)
		tenantBefore := failureCount("tenant_workload")
		h.requireWire(0, backend.TerminalVerdictRetry)

		for want := 1; want <= 3; want++ {
			h.die(1, false, containerEventStart, containerEventDie)
			verdict := backend.TerminalVerdictRetry
			if want == 3 {
				verdict = backend.TerminalVerdictExhausted
			}
			h.requireWire(want, verdict)

			// A sweep that re-observes the same failed runtime keeps the budget.
			require.NoError(t, h.b.recoverState(context.Background()))
			require.Equal(t, backend.ProvisionStatusFailed, h.status())
			h.requireWire(want, verdict)
			if want < 3 {
				time.Sleep(failedInterval)
				h.requireWire(want, backend.TerminalVerdictRetry)
				h.restartOutOfBand()
				h.requireWire(want, backend.TerminalVerdictRetry)
			}
		}
		assert.Equal(t, 3.0, failureCount("tenant_workload")-tenantBefore)
		h.b.provisionsMu.RLock()
		assert.Equal(t, 3, h.b.provisions[budgetTestLease].FailCount, "fail_count stays the lifetime diagnostic")
		h.b.provisionsMu.RUnlock()
	})
}

// The same real crash loop without the time between deaths: three deaths in
// quick succession, as an outage produces, only ever retry. Later deaths keep
// retrying until one lands 30 minutes after the streak's first.
func TestTerminalBudget_RealDeathsInsideTheMinimumSpanRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetEventHarness(t)
		start := time.Now()
		for want := 1; ; want++ {
			h.die(1, false, containerEventStart, containerEventDie)
			if time.Since(start) >= 30*time.Minute {
				require.GreaterOrEqual(t, want, 3)
				h.requireWire(want, backend.TerminalVerdictExhausted)
				return
			}
			h.requireWire(want, backend.TerminalVerdictRetry)
			time.Sleep(4 * time.Minute)
			h.restartOutOfBand()
		}
	})
}

// readerGate lets the concurrent readers below run while the lease's budget
// changes and parks them, durably blocked, while the synctest clock jumps
// across the minimum streak span.
type readerGate struct {
	mu   sync.Mutex
	open chan struct{}
}

func newReaderGate() *readerGate {
	open := make(chan struct{})
	close(open)
	return &readerGate{open: open}
}

func (g *readerGate) wait() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.open
}

func (g *readerGate) pause() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.open = make(chan struct{})
}

func (g *readerGate) resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	close(g.open)
}

// Inventory reads race every writer of the budget: the lease actor recording
// deaths, and recovery rebuilding and promoting the projection. Run it with
// -race. Every read is one consistent snapshot: an exhausted verdict is only
// ever served together with the Failed status it was minted for.
func TestTerminalBudget_ConcurrentInventoryReadsSeeConsistentSnapshots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetEventHarness(t)
		stop := make(chan struct{})
		gate := newReaderGate()
		var readers sync.WaitGroup
		var reads atomic.Int64
		// Registered after the harness, so it runs before the harness tears
		// down, even when a require below ends the test early.
		stopReaders := sync.OnceFunc(func() {
			close(stop)
			readers.Wait()
		})
		t.Cleanup(stopReaders)
		check := func(info backend.ProvisionInfo) {
			budget := info.TerminalBudget
			if budget == nil {
				t.Errorf("live inventory served no budget for %s", info.LeaseUUID)
				return
			}
			if budget.ConsecutiveFailures < 0 || budget.ConsecutiveFailures > 3 {
				t.Errorf("budget count out of range: %+v", *budget)
			}
			if budget.Verdict == backend.TerminalVerdictExhausted && info.Status != backend.ProvisionStatusFailed {
				t.Errorf("exhausted verdict served with status %s", info.Status)
			}
			reads.Add(1)
		}
		for range 2 {
			readers.Go(func() {
				for {
					select {
					case <-stop:
						return
					case <-gate.wait():
					}
					if provisions, err := h.b.ListProvisions(context.Background()); err == nil {
						for _, info := range provisions {
							check(info)
						}
					}
					if info, err := h.b.GetProvision(context.Background(), budgetTestLease); err == nil {
						check(*info)
					}
					// A durable block, so the bubble's clock can advance.
					time.Sleep(time.Millisecond)
				}
			})
		}
		for want := 1; want <= 3; want++ {
			h.die(1, false, containerEventStart, containerEventDie)
			require.NoError(t, h.b.recoverState(context.Background()))
			if want < 3 {
				gate.pause()
				time.Sleep(failedInterval)
				gate.resume()
				h.restartOutOfBand()
			}
		}
		stopReaders()
		assert.Positive(t, reads.Load())
		h.requireWire(3, backend.TerminalVerdictExhausted)
	})
}

// Provenance, not the exit status, decides whether a live death counts.
func TestTerminalBudget_LiveDeathProvenance(t *testing.T) {
	tests := []struct {
		name        string
		actions     []string
		exitCode    int
		oomKilled   bool
		attribution string
	}{
		{"self exit", []string{containerEventStart, containerEventDie}, 1, false, "tenant_workload"},
		{"self SIGKILL exit 137", []string{containerEventStart, containerEventDie}, 137, false, "tenant_workload"},
		{"self SIGTERM exit 143", []string{containerEventStart, containerEventDie}, 143, false, "tenant_workload"},
		{"OOM killed", []string{containerEventStart, containerEventDie}, 137, true, "tenant_workload"},
		{"api kill then die", []string{containerEventStart, containerEventKill, containerEventDie}, 137, false, "disruption"},
		{"docker stop then clean exit", []string{containerEventStart, containerEventKill, containerEventDie}, 0, false, "disruption"},
		{"run started before the stream", []string{containerEventDie}, 1, false, "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newBudgetEventHarness(t)
			before := failureCount(test.attribution)
			h.die(test.exitCode, test.oomKilled, test.actions...)
			assert.Equal(t, 1.0, failureCount(test.attribution)-before)
			wantCount := 0
			if test.attribution == "tenant_workload" {
				wantCount = 1
			}
			h.requireWire(wantCount, backend.TerminalVerdictRetry)
		})
	}
}

// A stream reconnect starts a new session: a signal observed before it is
// gone, so a later death of the same run attributes as unknown, never as the
// tenant's own exit.
func TestTerminalBudget_StreamReconnectForgetsTheRun(t *testing.T) {
	h := newBudgetEventHarness(t)
	h.send(containerEventStart, containerEventKill)
	// The loop drains the buffered events on the old stream, sees it closed,
	// and reconnects to the fresh one.
	h.reconnectStream()
	unknownBefore := failureCount("unknown")
	h.setContainer("exited", 1, false)
	h.send(containerEventDie)
	h.awaitStatus(backend.ProvisionStatusFailed)
	assert.Equal(t, 1.0, failureCount("unknown")-unknownBefore)
	h.requireWire(0, backend.TerminalVerdictRetry)
}

// A death found only by the recovery sweep has no live provenance and never
// counts (ENG-799): a signal that preceded it cannot be ruled out.
func TestTerminalBudget_SweepDetectedDeathNeverCounts(t *testing.T) {
	h := newBudgetEventHarness(t)
	unknownBefore := failureCount("unknown")
	h.setContainer("exited", 1, false)
	require.NoError(t, h.b.recoverState(context.Background()))
	h.awaitStatus(backend.ProvisionStatusFailed)
	assert.Equal(t, 1.0, failureCount("unknown")-unknownBefore)
	h.requireWire(0, backend.TerminalVerdictRetry)
	h.b.provisionsMu.RLock()
	assert.Equal(t, 1, h.b.provisions[budgetTestLease].FailCount)
	h.b.provisionsMu.RUnlock()
}

// The budget is published only from the live projection. Neither the
// diagnostics fallback nor a retained record can carry one (ENG-799).
func TestTerminalBudget_OnlyLiveProjectionsPublishABudget(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		budgetTestLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: budgetTestLease, Status: backend.ProvisionStatusFailed, FailCount: 5,
			Reason: backend.ReasonContainerExited,
		}},
	})
	defer b.stopCancel()
	live, err := b.GetProvision(context.Background(), budgetTestLease)
	require.NoError(t, err)
	assert.Equal(t, &backend.TerminalBudgetObservation{Verdict: backend.TerminalVerdictRetry}, live.TerminalBudget,
		"a fresh budget on a lease with a high lifetime fail_count reads retry")
	looked, err := b.LookupProvisions(context.Background(), []string{budgetTestLease})
	require.NoError(t, err)
	require.Len(t, looked, 1)
	assert.Equal(t, live.TerminalBudget, looked[0].TerminalBudget)

	diagnostics, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "diag.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = diagnostics.Close() })
	b.diagnosticsStore = diagnostics
	require.NoError(t, diagnostics.Store(shared.DiagnosticEntry{
		LeaseUUID: "7ba7b810-9dad-41d1-80b4-00c04fd430c8", FailCount: 9,
		Reason: backend.ReasonContainerExited, CreatedAt: time.Now(),
	}))
	fallback, err := b.GetProvision(context.Background(), "7ba7b810-9dad-41d1-80b4-00c04fd430c8")
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusFailed, fallback.Status)
	assert.Nil(t, fallback.TerminalBudget, "the diagnostics fallback is never decision evidence")
	encoded, err := json.Marshal(fallback)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "terminal_budget")

	retaining, retentions := newBackendWithRetention(t)
	require.NoError(t, putRetentionForTest(t, retentions,
		retentionEntryFixture(infoRetentionLeaseUUID, "tenant-a", time.Now())))
	retained, err := retaining.GetProvision(context.Background(), infoRetentionLeaseUUID)
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusRetained, retained.Status)
	assert.Nil(t, retained.TerminalBudget, "a retained record carries no budget")
}

// promoteAwaitedReadyForBudgetTest runs the awaited-Ready promotion for the
// current projection's lease with an exact operation claim.
func promoteAwaitedReadyForBudgetTest(t *testing.T, current *provision) *provision {
	t.Helper()
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	spec.LeaseUUID = current.LeaseUUID
	admission, err := beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	stack, err := manifest.ParsePayload(spec.Manifest)
	require.NoError(t, err)
	promoted, err := recoveredReadyProjection(createdDockerOperationClaim(t, admission), &recoveredOperationReadyPromotion{
		containerIDs:      []string{"c1"},
		serviceContainers: map[string][]string{"app": {"c1"}},
		stackManifest:     stack,
	}, current)
	require.NoError(t, err)
	return promoted
}

// The cold-start correction still bumps the lifetime fail_count for a provision
// found already failed with no in-memory predecessor (a host reboot or a backend
// restart), but that failure never counts: the budget is fresh (ENG-799 AC3).
func TestRecoverState_ColdStartFailedProvisionHasFreshBudget(t *testing.T) {
	for label := range 3 {
		got := runRecover(t, nil, []ContainerInfo{{
			ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small",
			ServiceName: "app", Status: "exited", FailCount: label,
		}})
		recovered, ok := got["L1"]
		require.True(t, ok)
		assert.Equal(t, label+1, recovered.FailCount, "the lifetime diagnostic is unchanged")
		assert.Equal(t, backend.ProvisionStatusFailed, recovered.Status)
		assert.Equal(t, backend.ReasonContainerExited, recovered.Reason)
		assert.Equal(t, &backend.TerminalBudgetObservation{Verdict: backend.TerminalVerdictRetry},
			recovered.ObserveTerminalBudget(), "a cold-recovered failure never counts")
	}
}

// countedBudget returns a budget produced by one real counted actor death, for
// carry tests: the type has no exported constructor by design.
func countedBudget(t *testing.T) leasesm.TerminalBudget {
	t.Helper()
	h := newBudgetEventHarness(t)
	h.die(1, false, containerEventStart, containerEventDie)
	h.requireWire(1, backend.TerminalVerdictRetry)
	h.b.provisionsMu.RLock()
	defer h.b.provisionsMu.RUnlock()
	return h.b.provisions[budgetTestLease].TerminalBudget
}

// Every recovery rebuild path that replaces a live projection carries its
// budget: Failing normalized to Failed, a Failed re-observation, a Ready
// rebuild of a Failed lease, and the awaited-Ready promotion.
func TestRecoverState_TerminalBudgetCarriedAcrossRebuild(t *testing.T) {
	budget := countedBudget(t)

	t.Run("failing normalizes to failed", func(t *testing.T) {
		existing := map[string]*provision{budgetTestLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: budgetTestLease, Status: backend.ProvisionStatusFailing,
			Reason: backend.ReasonContainerExited, TerminalBudget: budget,
		}}}
		got := runRecover(t, existing, nil)
		assert.Equal(t, budget, got[budgetTestLease].TerminalBudget)
		assert.Equal(t, 1, got[budgetTestLease].ObserveTerminalBudget().ConsecutiveFailures)
	})
	t.Run("failed re-observed as failed", func(t *testing.T) {
		existing := map[string]*provision{budgetTestLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: budgetTestLease, Tenant: "t", Status: backend.ProvisionStatusFailed,
			Reason: backend.ReasonContainerExited, TerminalBudget: budget,
		}}}
		got := runRecover(t, existing, []ContainerInfo{{
			ContainerID: "c1", LeaseUUID: budgetTestLease, Tenant: "t", SKU: "docker-small",
			ServiceName: "app", Status: "exited",
		}})
		assert.Equal(t, budget, got[budgetTestLease].TerminalBudget)
	})
	t.Run("failed rebuilt ready", func(t *testing.T) {
		existing := map[string]*provision{budgetTestLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: budgetTestLease, Tenant: "t", Status: backend.ProvisionStatusFailed,
			Reason: backend.ReasonContainerExited, TerminalBudget: budget,
		}}}
		got := runRecover(t, existing, []ContainerInfo{{
			ContainerID: "c1", LeaseUUID: budgetTestLease, Tenant: "t", SKU: "docker-small",
			ServiceName: "app", Status: "running",
		}})
		recovered := got[budgetTestLease]
		assert.Equal(t, backend.ProvisionStatusReady, recovered.Status)
		assert.NotEqual(t, budget, recovered.TerminalBudget, "the Ready rebuild anchors a Ready period")
		assert.Equal(t, &backend.TerminalBudgetObservation{
			Verdict: backend.TerminalVerdictRetry, ConsecutiveFailures: 1,
		}, recovered.ObserveTerminalBudget(), "the streak is carried, not reset")
	})
	t.Run("awaited ready promotion", func(t *testing.T) {
		current := &provision{ProvisionState: leasesm.ProvisionState{
			LeaseUUID: budgetTestLease, Status: backend.ProvisionStatusProvisioning, TerminalBudget: budget,
		}}
		promoted := promoteAwaitedReadyForBudgetTest(t, current)
		assert.Equal(t, backend.ProvisionStatusReady, promoted.Status)
		assert.Equal(t, 1, promoted.ObserveTerminalBudget().ConsecutiveFailures,
			"the promotion replaces the pointer but carries the streak")
	})
}
