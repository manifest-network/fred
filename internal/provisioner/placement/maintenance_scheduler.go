package placement

import (
	"context"
	"slices"
	"strings"
)

// maintenanceRecoveryScheduler owns scheduling only. Its entries cannot grant
// dispatch or settlement authority: every selected command must still acquire
// its held lane and reload its exact durable receipt.
//
// Each backend rotates independently within two work classes. A pass interleaves
// them, and the next pass starts with the opposite class from this pass's first
// opportunity. Thus a returning call that consumes the entire lane deadline
// cannot always exclude the other class. This is not cancellation authority for
// synchronous storage work: an outstanding write must return before recovery
// can start another pass.
type maintenanceRecoveryScheduler struct {
	lanes map[string]*maintenanceRecoveryLane
}

type maintenanceRecoveryClass uint8

const (
	maintenanceRecoveryConfirmed maintenanceRecoveryClass = iota
	maintenanceRecoveryOrdinary
)

func (class maintenanceRecoveryClass) other() maintenanceRecoveryClass {
	if class == maintenanceRecoveryConfirmed {
		return maintenanceRecoveryOrdinary
	}
	return maintenanceRecoveryConfirmed
}

type maintenanceRecoveryLane struct {
	confirmedAfter string
	ordinaryAfter  string
	first          maintenanceRecoveryClass
}

type maintenanceRecoveryEntry struct {
	candidate maintenanceRecoveryCandidate
	held      *maintenanceHeld
}

func newMaintenanceRecoveryScheduler() maintenanceRecoveryScheduler {
	return maintenanceRecoveryScheduler{lanes: make(map[string]*maintenanceRecoveryLane)}
}

func (scheduler maintenanceRecoveryScheduler) valid() bool { return scheduler.lanes != nil }

// retain and begin run while the application's recovery mutex excludes other
// passes. Once issued, each pass owns its distinct backend lane until joined.
func (scheduler maintenanceRecoveryScheduler) retain(pending map[string][]maintenanceRecoveryEntry) {
	for backendName := range scheduler.lanes {
		if _, exists := pending[backendName]; !exists {
			delete(scheduler.lanes, backendName)
		}
	}
}

func (scheduler maintenanceRecoveryScheduler) begin(backendName string, entries []maintenanceRecoveryEntry) *maintenanceRecoveryPass {
	lane := scheduler.lanes[backendName]
	if lane == nil {
		lane = &maintenanceRecoveryLane{first: maintenanceRecoveryConfirmed}
		scheduler.lanes[backendName] = lane
	}
	var confirmed, ordinary []maintenanceRecoveryEntry
	for _, entry := range entries {
		if entry.candidate.phase == maintenancePayloadConfirmed {
			confirmed = append(confirmed, entry)
		} else {
			ordinary = append(ordinary, entry)
		}
	}
	return &maintenanceRecoveryPass{
		lane: lane, nextClass: lane.first,
		// Every confirmed command is eligible on its callback wake, even when
		// outside the ordinary rotating batch. Only actual opportunities advance
		// either cursor; a deadline cannot skip an unvisited tail.
		confirmed: newMaintenanceRecoveryQueue(confirmed, &lane.confirmedAfter, len(confirmed)),
		ordinary:  newMaintenanceRecoveryQueue(ordinary, &lane.ordinaryAfter, maxMaintenanceRecoveryCommandsPerBackendPass),
	}
}

type maintenanceRecoveryPass struct {
	lane      *maintenanceRecoveryLane
	confirmed maintenanceRecoveryQueue
	ordinary  maintenanceRecoveryQueue
	nextClass maintenanceRecoveryClass
	started   bool
}

func (pass *maintenanceRecoveryPass) next(ctx context.Context) (maintenanceRecoveryEntry, bool) {
	if ctx.Err() != nil {
		return maintenanceRecoveryEntry{}, false
	}
	class := pass.nextClass
	entry, selected := pass.take(class)
	if !selected {
		class = class.other()
		entry, selected = pass.take(class)
	}
	if !selected {
		return maintenanceRecoveryEntry{}, false
	}
	if !pass.started {
		pass.lane.first = class.other()
		pass.started = true
	}
	pass.nextClass = class.other()
	return entry, true
}

func (pass *maintenanceRecoveryPass) take(class maintenanceRecoveryClass) (maintenanceRecoveryEntry, bool) {
	if class == maintenanceRecoveryConfirmed {
		return pass.confirmed.take()
	}
	return pass.ordinary.take()
}

type maintenanceRecoveryQueue struct {
	entries   []maintenanceRecoveryEntry
	after     *string
	index     int
	remaining int
}

func newMaintenanceRecoveryQueue(entries []maintenanceRecoveryEntry, after *string, limit int) maintenanceRecoveryQueue {
	slices.SortFunc(entries, func(left, right maintenanceRecoveryEntry) int {
		return strings.Compare(left.candidate.key(), right.candidate.key())
	})
	start, _ := slices.BinarySearchFunc(entries, *after, func(entry maintenanceRecoveryEntry, key string) int {
		return strings.Compare(entry.candidate.key(), key)
	})
	if start < len(entries) && entries[start].candidate.key() == *after {
		start++
	}
	if start == len(entries) {
		start = 0
	}
	return maintenanceRecoveryQueue{entries: entries, after: after, index: start, remaining: min(len(entries), limit)}
}

func (queue *maintenanceRecoveryQueue) take() (maintenanceRecoveryEntry, bool) {
	if queue.remaining == 0 {
		return maintenanceRecoveryEntry{}, false
	}
	entry := queue.entries[queue.index]
	queue.index = (queue.index + 1) % len(queue.entries)
	queue.remaining--
	// Busy live dispatches also received an opportunity. Advancing past them
	// prevents their ownership from pinning every later recovery candidate.
	*queue.after = entry.candidate.key()
	return entry, true
}
