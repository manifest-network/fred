package docker

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// A death is attributed at most once: whichever reader takes it, the other
// finds nothing.
func TestLiveDeathLedger_TakesEachDeathOnce(t *testing.T) {
	var ledger liveDeathLedger
	ledger.recordLiveDeath(observedRunDeath("a"))
	ledger.recordLiveDeath(failurecause.Provenance{}) // bound to no container: ignored

	deaths := ledger.takeLiveDeaths([]string{"a", "b"})
	require.Len(t, deaths, 1)
	assert.Equal(t, "a", deaths[0].InstanceID())
	assert.Empty(t, ledger.takeLiveDeaths([]string{"a"}), "taken once")
	_, observed := ledger.awaitLiveDeath(t.Context(), "a", 0)
	assert.False(t, observed)
}

// The ledger is bounded and forgets the oldest death first; a forgotten
// death reads as unobserved, which never counts.
func TestLiveDeathLedger_IsBoundedOldestFirst(t *testing.T) {
	var ledger liveDeathLedger
	for i := range liveDeathLedgerCapacity + 1 {
		ledger.recordLiveDeath(observedRunDeath(fmt.Sprintf("c%d", i)))
	}
	assert.Empty(t, ledger.takeLiveDeaths([]string{"c0"}), "the oldest death was evicted")
	assert.Len(t, ledger.takeLiveDeaths([]string{"c1", fmt.Sprintf("c%d", liveDeathLedgerCapacity)}), 2)

	// Taken entries never let the order index grow without bound.
	for i := range 4 * liveDeathLedgerCapacity {
		id := fmt.Sprintf("d%d", i)
		ledger.recordLiveDeath(observedRunDeath(id))
		ledger.takeLiveDeaths([]string{id})
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	assert.LessOrEqual(t, len(ledger.deathOrder), 2*liveDeathLedgerCapacity+1)
	assert.LessOrEqual(t, len(ledger.deathsByID), liveDeathLedgerCapacity)
}

// A startup failure waits for its container's die event only while the event
// stream is connected, and only for the bounded wait.
func TestLiveDeathLedger_AwaitWaitsOnlyWhileConnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var ledger liveDeathLedger
		start := time.Now()
		_, observed := ledger.awaitLiveDeath(t.Context(), "a", liveDeathAwait)
		assert.False(t, observed)
		assert.Zero(t, time.Since(start), "no stream, no wait")

		ledger.markLiveDeathStream(true)
		go func() {
			time.Sleep(liveDeathAwait / 2)
			ledger.recordLiveDeath(observedRunDeath("a"))
		}()
		death, observed := ledger.awaitLiveDeath(t.Context(), "a", liveDeathAwait)
		require.True(t, observed)
		assert.Equal(t, "a", death.InstanceID())

		start = time.Now()
		_, observed = ledger.awaitLiveDeath(t.Context(), "b", liveDeathAwait)
		assert.False(t, observed)
		assert.Equal(t, liveDeathAwait, time.Since(start), "a missing event waits only the bound")

		go func() {
			time.Sleep(liveDeathAwait / 4)
			ledger.markLiveDeathStream(false)
		}()
		start = time.Now()
		_, observed = ledger.awaitLiveDeath(t.Context(), "b", liveDeathAwait)
		assert.False(t, observed)
		assert.Equal(t, liveDeathAwait/4, time.Since(start), "a disconnect ends the wait")
	})
}

// provisioningForReadyRace puts the harness lease back in its provision,
// before the actor's Ready transition. published says whether the provision
// worker has already published the new cohort's container IDs, as it does
// just before it reports success.
func provisioningForReadyRace(h *budgetEventHarness, published bool) {
	h.b.provisionsMu.Lock()
	defer h.b.provisionsMu.Unlock()
	provisioning := h.b.provisions[budgetTestLease]
	provisioning.Status = backend.ProvisionStatusProvisioning
	provisioning.ContainerIDs = nil
	provisioning.ServiceContainers = nil
	if published {
		provisioning.ContainerIDs = []string{"c1"}
	}
}

// enterReadyForRace makes the provision's Ready transition the way the
// actor's entry action makes it, through the provision store.
func enterReadyForRace(h *budgetEventHarness) bool {
	store := &backendProvisionStore{backend: h.b}
	return store.UpdateFn(budgetTestLease, func(p *leasesm.ProvisionState) {
		p.ContainerIDs = []string{"c1"}
		p.ServiceContainers = map[string][]string{manifest.DefaultServiceName: {"c1"}}
		p.SetStatus(backend.ProvisionStatusReady, time.Now())
	})
}

// The Ready race (ENG-1125): a container that dies after the provision's last
// startup inspection is in no Ready projection when its die event arrives, so
// the event loop cannot route it, whether or not the worker has published the
// cohort's IDs yet. The dispatcher is done with that death before the Ready
// entry here (it handles deaths in order, and a later death's verification
// proves it), so only the Ready entry can deliver it: it hands the death
// recorded in the ledger back to the dispatcher, and the actor counts it as
// the death of a Ready workload, with its live provenance. The dispatcher
// does not count the death it left to the Ready entry as dropped.
func TestReadyEntryRedispatchesARecordedStartupDeath(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(fmt.Sprintf("ids published %t", published), func(t *testing.T) {
			h := newUnstartedBudgetEventHarness(t)
			provisioningForReadyRace(h, published)
			verifications := countStorageVerifications(h.b)
			h.start(func(b *Backend) { b.containerEventLoop() })
			h.awaitSubscriptions(1)
			tenantBefore, dropsBefore := failureCount("tenant_workload"), eventLoopDrops()

			verified := verifications.Load()
			h.setContainer("exited", 1, false)
			h.send(containerEventStart, containerEventDie)
			h.sendFor("probe", containerEventDie)
			require.Eventually(t, func() bool { return verifications.Load()-verified >= 2 },
				5*time.Second, time.Millisecond, "the dispatcher never reached the death after c1's")
			assert.Equal(t, backend.ProvisionStatusProvisioning, h.status(), "an unroutable death changes nothing")
			h.b.liveDeaths.mu.Lock()
			_, recorded := h.b.liveDeaths.deathsByID["c1"]
			h.b.liveDeaths.mu.Unlock()
			require.True(t, recorded, "the dispatcher saw a death the ledger does not hold")

			require.True(t, enterReadyForRace(h))
			h.awaitStatus(backend.ProvisionStatusFailed)
			h.requireWire(1, backend.TerminalVerdictRetry)
			assert.Equal(t, 1.0, failureCount("tenant_workload")-tenantBefore)
			assert.Equal(t, dropsBefore, eventLoopDrops(), "a death left to the Ready entry is not a lost death")
			assert.Empty(t, h.b.liveDeaths.takeLiveDeaths([]string{"c1"}), "the ledger handed the death out once")
			require.NoError(t, h.b.recoverState(context.Background()))
		})
	}
}

// A death reaches the dispatcher only after the recorder wrote it into the
// live-death ledger (ENG-1125), so a provision's Ready entry that races it
// cannot miss it: either the dispatcher's lookup comes after the Ready entry
// and sees the Ready projection, or the Ready entry comes after the record and
// takes the death. Here the ledger is held from before the death until the
// Ready entry is under way. While it is held, the dispatcher never touches
// the death; once it is released, the death reaches the actor with its live
// provenance and counts once, whichever of the two delivers it.
func TestLiveDeathIsRecordedBeforeItIsDispatched(t *testing.T) {
	h := newUnstartedBudgetEventHarness(t)
	h.events = make(chan ContainerEvent) // a send returns once the reader took the event
	provisioningForReadyRace(h, false)
	verifications := countStorageVerifications(h.b)
	h.start(func(b *Backend) { b.containerEventLoop() })
	h.awaitSubscriptions(1)
	tenantBefore := failureCount("tenant_workload")

	h.b.liveDeaths.mu.Lock()
	held := true
	release := func() {
		if held {
			held = false
			h.b.liveDeaths.mu.Unlock()
		}
	}
	// Registered after the harness's cleanup, so it runs first: the loop
	// cannot stop while its recorder waits for the ledger.
	t.Cleanup(release)
	verified := verifications.Load()
	h.setContainer("exited", 1, false)
	h.send(containerEventStart, containerEventDie)
	// The reader takes one event at a time, so taking this one proves that it
	// handed the death over, to a recorder held at the ledger.
	h.sendFor("sentinel", containerEventStart)
	assert.Never(t, func() bool { return verifications.Load() != verified }, 200*time.Millisecond, time.Millisecond,
		"the dispatcher handled a death the ledger does not hold yet")

	// The Ready entry publishes the Ready projection, then waits for the
	// ledger while it holds the projection lock.
	entered := make(chan bool, 1)
	go func() { entered <- enterReadyForRace(h) }()
	require.Eventually(t, func() bool {
		if h.b.provisionsMu.TryRLock() {
			h.b.provisionsMu.RUnlock()
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond, "the Ready entry never took the projection lock")
	release()
	select {
	case ok := <-entered:
		require.True(t, ok)
	case <-time.After(5 * time.Second):
		t.Fatal("the Ready entry never finished")
	}
	h.awaitStatus(backend.ProvisionStatusFailed)
	h.requireWire(1, backend.TerminalVerdictRetry)
	assert.Equal(t, 1.0, failureCount("tenant_workload")-tenantBefore,
		"the death reached the actor with its live provenance, and counted once")
}

// The stream reads down only once the recorder has recorded every death the
// reader handed over (ENG-1125). A startup failure that waits for its
// container's death across the end of the stream finds it, even when the
// stream ended while the recorder was still behind: here it is held at the
// ledger, with a backlog in front of that death. The loop ends the
// subscription without waiting on the ledger; only the recorder marks it.
func TestLiveDeathStreamGoesDownAfterItsLastDeath(t *testing.T) {
	h := newUnstartedBudgetEventHarness(t)
	h.events = make(chan ContainerEvent) // a send returns once the reader took the event
	h.start(func(b *Backend) { b.runContainerEventLoop(time.Millisecond, 4*time.Millisecond) })
	h.awaitSubscriptions(1)
	require.Eventually(t, func() bool {
		h.b.liveDeaths.mu.Lock()
		defer h.b.liveDeaths.mu.Unlock()
		return h.b.liveDeaths.streamConnected
	}, 5*time.Second, time.Millisecond, "the recorder never marked the stream up")

	// A startup failure of container s1, which no projection names, waits for
	// its death.
	type awaited struct {
		death    failurecause.Provenance
		observed bool
	}
	result := make(chan awaited, 1)
	go func() {
		death, observed := h.b.liveDeaths.awaitLiveDeath(t.Context(), "s1", time.Minute)
		result <- awaited{death, observed}
	}()
	require.Eventually(t, func() bool {
		h.b.liveDeaths.mu.Lock()
		defer h.b.liveDeaths.mu.Unlock()
		return h.b.liveDeaths.deathRecorded != nil
	}, 5*time.Second, time.Millisecond, "the startup failure never waited")

	h.b.liveDeaths.mu.Lock()
	held := true
	release := func() {
		if held {
			held = false
			h.b.liveDeaths.mu.Unlock()
		}
	}
	t.Cleanup(release)
	// Within both queues' capacity, and long enough that a mark racing the
	// recorder would land before the last death.
	const backlog = containerDeathQueueCapacity / 2
	for index := range backlog {
		h.sendFor(fmt.Sprintf("x%d", index), containerEventDie)
	}
	h.sendFor("s1", containerEventStart, containerEventDie)
	// End the stream while all of those deaths wait for the recorder.
	h.mu.Lock()
	ended := h.events
	h.events = make(chan ContainerEvent)
	h.mu.Unlock()
	reconnects := eventStreamCount(containerEventStreamReconnect)
	close(ended)
	require.Eventually(t, func() bool { return eventStreamCount(containerEventStreamReconnect) > reconnects },
		5*time.Second, time.Millisecond, "the loop waited on the ledger to end the subscription")
	release()

	select {
	case got := <-result:
		require.True(t, got.observed, "the stream read down before a death the reader observed was recorded")
		assert.Equal(t, "s1", got.death.InstanceID())
		assert.Equal(t, "observed_run", got.death.Label())
	case <-time.After(10 * time.Second):
		t.Fatal("the startup failure never stopped waiting")
	}
	// The recorder takes the next subscription only after the last one.
	h.awaitSubscriptions(2)
}
