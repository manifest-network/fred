package docker

import (
	"context"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

// liveDeathLedger remembers the live provenance of recent container deaths,
// keyed by the container each provenance was minted for
// (failurecause.Provenance.InstanceID). The event loop is its only writer:
// the reader hands every die event, without waiting, to the loop's recorder
// (recordLiveContainerDeaths) as well as to the dispatcher, and the loop marks
// whether a subscription is held, so a death the dispatcher cannot route keeps
// its provenance. The reader never writes the ledger itself, because the
// ledger takes a lock and the reader may wait on nothing but its stream
// (ENG-799). A death the dispatcher cannot route arrives while the container
// is in no Ready projection yet: during a provision's startup verification,
// and in the moment between the provision worker's last inspection and the
// lease actor's Ready transition (ENG-1125).
//
// Two readers consult it, each taking the entry it reads so a death is
// attributed at most once: a startup failure's minting (newStartupFailure) and
// the re-dispatch at a provision's Ready entry (redispatchStartupDeaths).
// internal/testutil confines the writer and both readers to those functions.
//
// The ledger is bounded and forgets the oldest entry first. A death that was
// never recorded, was evicted, or was read while the stream was down reads as
// unobserved, which never counts: every loss errs toward not closing a lease.
// The zero value is ready to use.
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
// recorder may call it.
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
// the event loop may call it, around the reader. While the reader holds none,
// no new death can arrive, so a startup failure does not wait for one.
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
