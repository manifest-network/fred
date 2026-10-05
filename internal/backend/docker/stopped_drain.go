package docker

import (
	"fmt"
	"os"
	"strings"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// StoppedDrainReport is the coordinated update's hold-aware reading of a
// stopped docker-backend's callback journal (ENG-1117, ENG-1109).
//
// A held volume deletion keeps the close or operation that waits on it
// pending, and only the hold executor can finish it; a deletion an operator
// must unblock can stay held for days. Counting such a head as pending work
// would refuse every coordinated update, after the fleet was already stopped,
// for as long as one deletion is held. DeleteHeld separates those heads so the
// deploy coordinator can decide not to block on them.
type StoppedDrainReport struct {
	// Pending counts the journal's pending work, excluding the delete-held
	// heads: every row InspectCallbackStoreReadOnly counted as pending, less
	// DeleteHeld.
	Pending int
	// DeleteHeld counts the close and operation heads whose own lease's
	// remaining volume slots all have a delete stage on disk, at least one
	// of them. A restart registers each such stage as a held deletion and
	// finishes it in the background. It is a reading of the volume root
	// only: see ClassifyStoppedDrain for what it does not check.
	DeleteHeld int
}

// ClassifyStoppedDrain splits a stopped backend's pending work into ordinary
// pending work and delete-held heads. It is the native classifier of the
// coordinated-update drain proof, which manifest-deploy builds from this
// source and runs against a STOPPED backend: it reads the journal inspection
// it is given and makes one read-only listing of volumeDataPath's top level.
// An empty volumeDataPath (no managed volumes) holds nothing.
//
// A close or operation head is delete-held when awaitsOnlyHeldDeletes holds
// over its own lease's volume slots, read from on-disk delete stages. That is
// only the volume-slot half of the running backend's close-churn skip
// (closeAwaitsHeldDeletes), and it excuses more than that skip does:
//
//   - operation heads: the running backend applies the predicate to closes
//     only, and retries a pending operation as usual;
//   - every close, without the container half: whether a container of the
//     lease remains is not checked, and a stage's phase is memory-only and
//     not known here;
//   - its own lease's slots only: work an operation still owes in another
//     lease's namespace (a restore returning its source volumes to
//     retention, which can wait on a source container) is not seen.
//
// DeleteHeld therefore does not prove that held deletions are the only work
// left. A restarted hold-aware backend re-checks all of it before it skips a
// close, and retries everything else.
func ClassifyStoppedDrain(inspection shared.CallbackStoreInspection, volumeDataPath string) (StoppedDrainReport, error) {
	report := StoppedDrainReport{Pending: inspection.Pending}
	if volumeDataPath == "" {
		return report, nil
	}
	holds, err := stoppedDeleteHolds(volumeDataPath)
	if err != nil {
		return StoppedDrainReport{}, err
	}
	for _, head := range inspection.PendingHeads {
		switch head.Kind {
		case shared.PendingCloseHead, shared.PendingOperationHead:
		default:
			continue
		}
		if holds.awaitsOnlyHeldDeletes(leaseSlotNames(head.LeaseUUID, head.Items)) {
			report.DeleteHeld++
			report.Pending--
		}
	}
	if report.Pending < 0 {
		return StoppedDrainReport{}, fmt.Errorf("stopped drain: %d delete-held heads exceed %d pending rows",
			report.DeleteHeld, inspection.Pending)
	}
	return report, nil
}

// stoppedDeleteHolds reads a stopped backend's volume root once and builds
// the view a fresh Start would register: every delete stage is a held
// deletion keeping its caller pending, every create stage is staged, and
// every managed volume directory is mapped. A malformed entry in a reserved
// stage namespace fails the read, as it fails Start.
func stoppedDeleteHolds(volumeDataPath string) (volumeDeleteHoldSnapshot, error) {
	entries, err := os.ReadDir(volumeDataPath)
	if err != nil {
		return volumeDeleteHoldSnapshot{}, fmt.Errorf("list stopped volume root: %w", err)
	}
	snapshot := volumeDeleteHoldSnapshot{
		pending: map[string]struct{}{}, holds: map[string]volumeDeleteHoldView{},
		staged: map[string]struct{}{}, mapped: map[string]struct{}{},
	}
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, xfsDeleteStagePrefix):
			stage, parseErr := parseXFSDeleteStageName(name)
			if parseErr != nil {
				return volumeDeleteHoldSnapshot{}, fmt.Errorf("stopped volume root: %w", parseErr)
			}
			volume := stage.volumeID.value()
			snapshot.pending[volume] = struct{}{}
			snapshot.staged[volume] = struct{}{}
			snapshot.holds[volume] = volumeDeleteHoldView{
				volume: stage.volumeID, stage: stage.value(), projectID: stage.projID,
				phase: holdPhaseRemoval, reason: holdReasonRecovered,
			}
		case strings.HasPrefix(name, xfsStagePrefix):
			stage, parseErr := parseXFSStageName(name)
			if parseErr != nil {
				return volumeDeleteHoldSnapshot{}, fmt.Errorf("stopped volume root: %w", parseErr)
			}
			snapshot.staged[stage.volumeID.value()] = struct{}{}
		case strings.HasPrefix(name, volumePrefix) && entry.IsDir():
			if _, parseErr := parseManagedVolumeName(name); parseErr == nil {
				snapshot.mapped[name] = struct{}{}
			}
		}
	}
	return snapshot, nil
}
