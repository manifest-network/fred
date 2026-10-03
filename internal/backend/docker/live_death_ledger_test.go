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

// The Ready race (ENG-1125): a container that dies after the provision's last
// startup inspection is in no Ready projection when its die event arrives, so
// the event loop cannot route it. The provision's Ready entry hands the death
// recorded in the ledger back to the dispatcher, and the actor counts it as
// the death of a Ready workload, with its live provenance.
func TestReadyEntryRedispatchesARecordedStartupDeath(t *testing.T) {
	h := newUnstartedBudgetEventHarness(t)
	h.b.provisionsMu.Lock()
	provisioning := h.b.provisions[budgetTestLease]
	provisioning.Status = backend.ProvisionStatusProvisioning
	provisioning.ContainerIDs = nil
	provisioning.ServiceContainers = nil
	h.b.provisionsMu.Unlock()
	h.start(func(b *Backend) { b.containerEventLoop() })
	tenantBefore := failureCount("tenant_workload")

	h.setContainer("exited", 1, false)
	h.send(containerEventStart, containerEventDie)
	require.Eventually(t, func() bool {
		h.b.liveDeaths.mu.Lock()
		defer h.b.liveDeaths.mu.Unlock()
		_, recorded := h.b.liveDeaths.deathsByID["c1"]
		return recorded
	}, 5*time.Second, 5*time.Millisecond, "the event loop records the death")
	assert.Equal(t, backend.ProvisionStatusProvisioning, h.status(), "an unroutable death changes nothing")

	// The provision's Ready transition, as the actor's entry action makes it.
	store := &backendProvisionStore{backend: h.b}
	require.True(t, store.UpdateFn(budgetTestLease, func(p *leasesm.ProvisionState) {
		p.ContainerIDs = []string{"c1"}
		p.ServiceContainers = map[string][]string{manifest.DefaultServiceName: {"c1"}}
		p.SetStatus(backend.ProvisionStatusReady, time.Now())
	}))
	h.awaitStatus(backend.ProvisionStatusFailed)
	h.requireWire(1, backend.TerminalVerdictRetry)
	assert.Equal(t, 1.0, failureCount("tenant_workload")-tenantBefore)
	assert.Empty(t, h.b.liveDeaths.takeLiveDeaths([]string{"c1"}), "the death was attributed once")
	require.NoError(t, h.b.recoverState(context.Background()))
}
