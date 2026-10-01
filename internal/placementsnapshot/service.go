package placementsnapshot

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	// copyDeadline bounds how long one snapshot holds its read transactions.
	copyDeadline = 30 * time.Second
	// spaceReserve stays free on the snapshot filesystem beyond twice the
	// bytes a snapshot copies, so snapshots never fill a disk a live database
	// may share: a failed live commit withdraws that store's authority.
	spaceReserve = 256 << 20
	// startupDelay lets the startup reconcile settle before a process's first
	// snapshot.
	startupDelay = time.Minute
)

// Settings are validated snapshot settings. Only NewSettings mints them; the
// zero value is invalid.
type Settings struct {
	interval time.Duration
	retain   int
}

// NewSettings validates the snapshot interval and the number of complete sets
// to retain. Operator-facing bounds are config validation's job.
func NewSettings(interval time.Duration, retain int) (Settings, error) {
	if interval <= 0 {
		return Settings{}, errors.New("snapshot interval must be positive")
	}
	if retain < 1 {
		return Settings{}, errors.New("snapshot retention must keep at least one set")
	}
	return Settings{interval: interval, retain: retain}, nil
}

// outcome is one snapshot attempt's result. The zero value is invalid.
type outcome uint8

const (
	outcomeInvalid outcome = iota
	outcomeSuccess
	outcomeError
	outcomeInsufficientSpace
)

var outcomes = [...]outcome{outcomeSuccess, outcomeError, outcomeInsufficientSpace}

func (result outcome) label() string {
	switch result {
	case outcomeSuccess:
		return "success"
	case outcomeError:
		return "error"
	case outcomeInsufficientSpace:
		return "insufficient_space"
	default:
		return ""
	}
}

// Service snapshots both live stores into one directory on a fixed interval.
// Only NewService mints one.
type Service struct {
	placements *placement.Store
	payloads   *payload.Store
	directory  *Directory
	settings   Settings
}

// NewService binds a snapshot loop to the live stores and an open snapshot
// directory, and exports the snapshot metric series.
func NewService(
	placements *placement.Store,
	payloads *payload.Store,
	directory *Directory,
	settings Settings,
) (*Service, error) {
	if placements == nil || payloads == nil {
		return nil, errors.New("snapshots need both the placement and the payload store")
	}
	if directory == nil || directory.directory == nil {
		return nil, errors.New("snapshot directory is required")
	}
	if settings.interval <= 0 || settings.retain < 1 {
		return nil, errors.New("snapshot settings are invalid")
	}
	for _, result := range outcomes {
		metrics.PlacementSnapshotsTotal.WithLabelValues(result.label())
	}
	for _, failure := range pruneFailures {
		metrics.PlacementSnapshotPruneFailuresTotal.WithLabelValues(failure.label())
	}
	return &Service{
		placements: placements,
		payloads:   payloads,
		directory:  directory,
		settings:   settings,
	}, nil
}

// Run snapshots every interval until ctx ends. The first snapshot waits for
// startupDelay and for one interval since the newest complete set, so a
// restarting process never replaces older sets with near-identical new ones.
// It returns nil when ctx ends.
func (service *Service) Run(ctx context.Context) error {
	delay := startupDelay
	if newest, ok := service.directory.newestComplete(); ok {
		delay = firstSnapshotDelay(time.Now(), newest.created, service.settings.interval)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		result, report := service.snapshotOnce(ctx, time.Now())
		record(result, report)
		timer.Reset(service.settings.interval)
	}
}

// firstSnapshotDelay waits until one interval after the newest complete set,
// at least startupDelay and at most one interval: a clock that moved backwards
// cannot postpone snapshots past their interval.
func firstSnapshotDelay(now, newest time.Time, interval time.Duration) time.Duration {
	return min(max(newest.Add(interval).Sub(now), startupDelay), max(interval, startupDelay))
}

// snapshotOnce makes one attempt and prunes after a successful publication. A
// panic is an error outcome: a captured cut's transactions end at its deadline
// on their own.
func (service *Service) snapshotOnce(ctx context.Context, at time.Time) (result outcome, report pruneReport) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("placement snapshot panicked",
				"panic", recovered, "stack", string(debug.Stack()))
			result, report = outcomeError, pruneReport{}
		}
	}()
	cut, err := service.placements.CaptureConsistentCut(service.payloads, copyDeadline)
	if err != nil {
		slog.Warn("placement snapshot could not capture the databases", "error", err)
		return outcomeError, pruneReport{}
	}
	available, err := service.directory.availableBytes()
	if err != nil {
		slog.Warn("placement snapshot could not measure free space",
			"error", errors.Join(err, cut.Discard()), "snapshot_dir", service.directory.Path())
		return outcomeError, pruneReport{}
	}
	if !hasRoom(available, cut.Size()) {
		slog.Warn("placement snapshot skipped: not enough free space",
			"available_bytes", available, "required_bytes", requiredBytes(cut.Size()),
			"snapshot_dir", service.directory.Path(), "discard_error", cut.Discard())
		return outcomeInsufficientSpace, pruneReport{}
	}
	set, err := service.directory.publish(ctx, cut, at)
	if err != nil {
		slog.Warn("placement snapshot failed", "error", err, "snapshot_dir", service.directory.Path())
		return outcomeError, pruneReport{}
	}
	report = service.directory.prune(set, service.settings.retain)
	slog.Info("placement snapshot published",
		"manifest", service.directory.names.file(set, fileKindManifest),
		"snapshot_dir", service.directory.Path(), "pruned_files", report.removed)
	return outcomeSuccess, report
}

// requiredBytes is the free space a snapshot of cutSize bytes needs.
func requiredBytes(cutSize int64) uint64 {
	return 2*uint64(max(cutSize, 0)) + spaceReserve // #nosec G115 -- clamped non-negative
}

func hasRoom(available uint64, cutSize int64) bool {
	return available >= requiredBytes(cutSize)
}

// record counts one attempt exactly once, and every pruning failure.
func record(result outcome, report pruneReport) {
	if label := result.label(); label != "" {
		metrics.PlacementSnapshotsTotal.WithLabelValues(label).Inc()
	}
	for failure, count := range report.failures {
		if label := failure.label(); label != "" && count > 0 {
			metrics.PlacementSnapshotPruneFailuresTotal.WithLabelValues(label).Add(float64(count))
		}
	}
}
