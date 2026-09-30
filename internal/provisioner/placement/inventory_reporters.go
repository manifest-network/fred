package placement

import (
	"maps"
	"slices"
)

// inventorySweepReporters is the persisted form of a tracked
// sweepReporterJournal. It is present only while its exact pending sweep is
// unresolved, so a database whose sweeps all ended cleanly keeps the metadata
// shape older binaries accept.
type inventorySweepReporters struct {
	SweepID  uint64   `json:"sweep_id"`
	Backends []string `json:"backends"`
}

// sweepReporterJournal is the durable write-ahead record of which configured
// backends returned an identity-validated inventory response naming at least
// one lease while the current pending-sweep chain is unresolved.
//
// It narrows interrupted-sweep recovery. recordUnprojectedPositives commits a
// reporter before it installs that reporter's lease barriers, so a positive a
// restart can lose came from a recorded reporter. A backend that never
// answered with a positive contributed nothing to lose; like any silent
// backend in ordinary degraded operation, it must not hold every lease on the
// provider fenced until it answers.
//
// The zero value is untracked: the pending marker predates reporter tracking,
// or the chain inherited such a marker. Untracked recovery keeps the original
// rule, which requires paired coverage of the whole configured topology.
type sweepReporterJournal struct {
	tracked   bool
	reporters map[string]struct{}
}

// untrackedSweepReporters is the journal of a chain that holds, or may hold,
// unattributed evidence. Only whole-topology coverage can retire it.
func untrackedSweepReporters() sweepReporterJournal {
	return sweepReporterJournal{}
}

// trackedSweepReporters starts the journal of a sweep that inherits no
// unresolved marker, so nothing can yet be lost.
func trackedSweepReporters() sweepReporterJournal {
	return sweepReporterJournal{tracked: true, reporters: make(map[string]struct{})}
}

// sweepReporterJournalFromMetadata restores the journal from loaded metadata.
// loadTopologyMetadata has already validated that a present record names the
// exact pending sweep and only active backends.
func sweepReporterJournalFromMetadata(metadata topologyMetadata) sweepReporterJournal {
	record := metadata.InventorySweepReporters
	if record == nil {
		return sweepReporterJournal{}
	}
	reporters := make(map[string]struct{}, len(record.Backends))
	for _, backendName := range record.Backends {
		reporters[backendName] = struct{}{}
	}
	return sweepReporterJournal{tracked: true, reporters: reporters}
}

// successor carries an unresolved chain into the sweep that supersedes it.
// Recorded reporters stay recorded because their positives may still be
// unrepresented; an untracked chain stays untracked.
func (journal sweepReporterJournal) successor() sweepReporterJournal {
	if !journal.tracked {
		return sweepReporterJournal{}
	}
	return sweepReporterJournal{tracked: true, reporters: maps.Clone(journal.reporters)}
}

func (journal sweepReporterJournal) recorded(backendName string) bool {
	_, ok := journal.reporters[backendName]
	return ok
}

// with returns a copy, so a failed durable write leaves the live journal
// unchanged.
func (journal sweepReporterJournal) with(backendName string) sweepReporterJournal {
	next := journal.successor()
	if next.tracked {
		next.reporters[backendName] = struct{}{}
	}
	return next
}

// names is canonical (sorted) and never nil, so an empty tracked journal
// persists as an explicit empty list rather than as JSON null.
func (journal sweepReporterJournal) names() []string {
	names := slices.Sorted(maps.Keys(journal.reporters))
	if names == nil {
		names = []string{}
	}
	return names
}

func (journal sweepReporterJournal) persisted(sweepID uint64) *inventorySweepReporters {
	if !journal.tracked || sweepID == 0 {
		return nil
	}
	return &inventorySweepReporters{SweepID: sweepID, Backends: journal.names()}
}

// inventoryAttribution says whether an endpoint response's positives are
// provably its reporter's own: a refreshed answer from the backend's pinned
// storage, with well-formed rows that the backend's other endpoint does not
// contradict. Only attributed positives may narrow recovery to their
// reporters. Anything else can name a lease held elsewhere; a sweep that
// projects it writes a quarantine that only whole-topology coverage lifts, so
// the chain that holds it keeps the whole-topology rule until it ends.
type inventoryAttribution uint8

const (
	// The zero value is invalid and treated as unattributed.
	_ inventoryAttribution = iota
	inventoryAttributed
	inventoryUnattributed
)

// sweepReporterRecorded proves that one backend's reporter record is durable
// for one exact sweep, or that the chain is untracked and therefore already
// requires whole-topology recovery. Only recordSweepReporterLocked mints it,
// and installUnprojectedPositivesLocked takes its backend and sweep from it, so
// no lease barrier can exist for a reporter a restart would forget.
type sweepReporterRecorded struct {
	sweepID     uint64
	backendName string
}
