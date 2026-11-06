package placement

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/metrics/background"
)

// errWorkerPanicked marks work that ended in a recovered panic. It carries no
// outcome: durable state is exactly what the work committed before it
// panicked, so a caller treats it like any other failed attempt and nothing is
// settled or released on the work's behalf.
var errWorkerPanicked = errors.New("worker panicked")

// workerStage is the closed set of places this package runs work behind a
// panic boundary. A stage selects its counter series from workerStages, so a
// caller can neither choose an unbounded label nor skip the count.
type workerStage uint8

const (
	// workerChainInventory is one PENDING or ACTIVE provider inventory read.
	workerChainInventory workerStage = iota
	// workerMaintenanceRecovery is a maintenance recovery pass, each backend
	// lane in it, and each command a lane recovers.
	workerMaintenanceRecovery

	workerStageCount
)

type workerStageEntry struct {
	name   string
	panics prometheus.Counter
}

// workerStages resolves every stage's series at package init, so each exists
// at zero from process start and the first panic after a restart is an
// increase() rather than a newly created series (ENG-1116).
var workerStages = [...]workerStageEntry{
	workerChainInventory: {
		name:   "chain inventory read",
		panics: metrics.ReconcilerPanics.FetchChainLeases,
	},
	workerMaintenanceRecovery: {
		name:   "maintenance recovery",
		panics: background.GoroutinePanics("maintenance_recovery"),
	},
}

// A stage added without its workerStages entry does not compile.
var _ = [1]struct{}{}[len(workerStages)-int(workerStageCount)]

func (stage workerStage) entry() workerStageEntry {
	if stage >= workerStageCount {
		return workerStageEntry{name: fmt.Sprintf("unknown worker stage %d", stage)}
	}
	return workerStages[stage]
}

// runBounded runs work as one unit behind a panic boundary. A panic is counted
// under stage, logged once with its stack, and returned as errWorkerPanicked
// with the zero result. Cleanup that must survive a panic, such as releasing a
// lock, belongs in the work's own defers, which run before this boundary sees
// the panic.
func runBounded[T any](stage workerStage, work func() (T, error)) (result T, err error) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		entry := stage.entry()
		if entry.panics != nil {
			entry.panics.Inc()
		}
		slog.Error("placement worker panicked",
			"stage", entry.name,
			"panic", recovered,
			"stack", string(debug.Stack()),
		)
		var zero T
		result, err = zero, fmt.Errorf("%s: %w (%T)", entry.name, errWorkerPanicked, recovered)
	}()
	return work()
}

// workerResult is the one value a worker delivers.
type workerResult[T any] struct {
	value T
	err   error
}

// startWorker runs work on its own goroutine behind runBounded and returns the
// channel that delivers its result. Apart from copyOwnedRead's reviewed pipe
// pair, it is the only way this package starts a goroutine
// (TestPlacementGoroutinesStartThroughStartWorker). The result is reachable
// only through the channel, so a worker shares no variables with its joiner,
// and however the worker ends (a return, a panic or runtime.Goexit) the
// channel delivers exactly one value. Only a returned result can carry a nil
// error.
func startWorker[T any](stage workerStage, work func() (T, error)) <-chan workerResult[T] {
	done := make(chan workerResult[T], 1)
	go func() {
		result := workerResult[T]{err: fmt.Errorf("%s exited without a result", stage.entry().name)}
		defer func() { done <- result }()
		result.value, result.err = runBounded(stage, work)
	}()
	return done
}
