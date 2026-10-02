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
	// DeleteHeld counts the close and operation heads that wait only on held
	// volume deletions: every remaining volume slot of the head has a delete
	// stage on disk, and at least one does. A restart registers each such
	// stage as a held deletion and finishes it in the background, so these
	// heads resume by themselves under a hold-aware target.
	DeleteHeld int
}

// ClassifyStoppedDrain splits a stopped backend's pending work into ordinary
// pending work and heads waiting only on held volume deletions. It is the
// native classifier of the coordinated-update drain proof, which manifest-deploy
// builds from this source and runs against a STOPPED backend: it reads the
// journal inspection it is given and makes one read-only listing of
// volumeDataPath's top level, with the same predicate the running backend uses
// for its close-churn skip (awaitsOnlyHeldDeletes). An empty volumeDataPath
// (no managed volumes) holds nothing.
//
// A head is delete-held only from on-disk delete stages; a stage's phase is
// memory-only and is not known here, and whether a container still runs is
// not checked: a restarted backend re-checks both before it skips the close.
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
