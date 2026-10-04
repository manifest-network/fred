package docker

import (
	"context"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

// liveDeathLedger remembers the live provenance of recent container deaths,
// keyed by the container each provenance was minted for
// (failurecause.Provenance.InstanceID). The event loop's recorder
// (recordLiveContainerDeaths) is its only writer. The reader hands every
// death to the recorder alone, and the recorder records it before it forwards
// it to the dispatcher. A death the dispatcher cannot route arrives while the
// container is in no Ready projection yet: during a provision's startup
// verification, and in the moment between the provision worker's last
// inspection and the lease actor's Ready transition (ENG-1125). Because the
// dispatcher looks a death up only after it is recorded, and the Ready entry
// reads this ledger under the projection lock that lookup takes, either the
// lookup sees the Ready projection and routes the death, or the Ready entry
// comes after the lookup, and so after the record, and takes the death from
// here, unless the bound below evicted it first. The recorder also marks
// whether a subscription is held, in order with its deaths: up before it
// records the first death of a subscription, down only after it recorded the
// last one the reader handed over. The reader never writes the ledger itself,
// because the ledger takes a lock and the reader may wait on nothing but its
// stream (ENG-799).
//
// Two readers consult it, each taking the entry it reads, so the ledger hands
// a death out at most once: a startup failure's minting (newStartupFailure)
// and the re-dispatch at a provision's Ready entry (redispatchStartupDeaths).
// internal/testutil confines the writer, both readers, and the functions that
// feed the recorder or dispatch a death, to their sites.
//
// The ledger is bounded and forgets the oldest entry first. A death the
// reader dropped from a full queue, or one evicted here, reads as unobserved,
// which never counts: every loss errs toward not closing a lease. The zero
// value is ready to use.
type liveDeathLedger struct {
	mu              sync.Mutex
	deathsByID      map[string]failurecause.Provenance
	deathOrder      []string
	deathRecorded   chan struct{}
	streamConnected bool
}

// liveDeathLedgerCapacity bounds the remembered deaths. It matches the death
// dispatch queue: a startup failure or a Ready entry reads its entry within
// seconds of the death, far sooner than this many later deaths can arrive.
const liveDeathLedgerCapacity = containerDeathQueueCapacity

// liveDeathAwait bounds how long a startup failure waits for the die event of
// a container it has already seen exited. The event usually arrives within
// milliseconds; a missing one only leaves the failure uncounted.
const liveDeathAwait = time.Second

// recordLiveDeath remembers one death's provenance. Only the event loop's
// recorder may call it, before it forwards the death to the dispatcher.
func (l *liveDeathLedger) recordLiveDeath(death failurecause.Provenance) {
	id := death.InstanceID()
	if id == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.deathsByID == nil {
		l.deathsByID = make(map[string]failurecause.Provenance)
	}
	if _, tracked := l.deathsByID[id]; !tracked {
		for len(l.deathsByID) >= liveDeathLedgerCapacity && len(l.deathOrder) > 0 {
			oldest := l.deathOrder[0]
			l.deathOrder = l.deathOrder[1:]
			delete(l.deathsByID, oldest)
		}
		l.deathOrder = append(l.deathOrder, id)
	}
	l.deathsByID[id] = death
	// Taken entries leave stale keys in deathOrder; compact once they dominate.
	if len(l.deathOrder) > 2*liveDeathLedgerCapacity {
		live := make([]string, 0, len(l.deathsByID))
		for _, key := range l.deathOrder {
			if _, present := l.deathsByID[key]; present {
				live = append(live, key)
			}
		}
		l.deathOrder = live
	}
	l.notifyLocked()
}

// markLiveDeathStream records whether the reader holds a subscription. Only
// the event loop's recorder may call it, in order with the deaths: up before
// it records the first death of a subscription, and down only after it
// recorded the last death the reader handed over. Once the stream reads down,
// no new death can be observed, so a startup failure does not wait for one.
func (l *liveDeathLedger) markLiveDeathStream(connected bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.streamConnected = connected
	l.notifyLocked()
}

func (l *liveDeathLedger) notifyLocked() {
	if l.deathRecorded != nil {
		close(l.deathRecorded)
		l.deathRecorded = nil
	}
}

func (l *liveDeathLedger) takeLocked(id string) (failurecause.Provenance, bool) {
	death, ok := l.deathsByID[id]
	if ok {
		delete(l.deathsByID, id)
	}
	return death, ok
}

// takeLiveDeaths removes and returns the recorded deaths of ids.
func (l *liveDeathLedger) takeLiveDeaths(ids []string) []failurecause.Provenance {
	l.mu.Lock()
	defer l.mu.Unlock()
	var deaths []failurecause.Provenance
	for _, id := range ids {
		if death, ok := l.takeLocked(id); ok {
			deaths = append(deaths, death)
		}
	}
	return deaths
}

// awaitLiveDeath removes and returns the recorded death of id, waiting at most
// wait for its die event while the stream is connected. ok is false when no
// live death was observed in time.
func (l *liveDeathLedger) awaitLiveDeath(ctx context.Context, id string, wait time.Duration) (failurecause.Provenance, bool) {
	if id == "" {
		return failurecause.Provenance{}, false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		l.mu.Lock()
		if death, ok := l.takeLocked(id); ok {
			l.mu.Unlock()
			return death, true
		}
		if !l.streamConnected {
			l.mu.Unlock()
			return failurecause.Provenance{}, false
		}
		if l.deathRecorded == nil {
			l.deathRecorded = make(chan struct{})
		}
		changed := l.deathRecorded
		l.mu.Unlock()
		select {
		case <-changed:
		case <-timer.C:
			return failurecause.Provenance{}, false
		case <-ctx.Done():
			return failurecause.Provenance{}, false
		}
	}
}
