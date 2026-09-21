// Package healthprobe correlates health diagnostics without carrying admission
// authority or changing a probe's deadline or verdict.
package healthprobe

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/uuidv4"
)

// Header contains one canonical UUIDv4 used only for diagnostic correlation.
const Header = "X-Fred-Health-Probe"

// SlowThreshold selects completed requests for diagnostic logging. It is not
// an execution budget and does not change whether a dependency is healthy.
const SlowThreshold = time.Second

// Side identifies the endpoint of the same HTTP probe.
type Side string

const (
	Client Side = "client"
	Server Side = "server"
)

// Stage is a bounded vocabulary shared by middleware and backend probes.
type Stage string

const (
	IdentityAdmission  Stage = "identity_admission"
	StorageIdentity    Stage = "storage_identity"
	DockerPing         Stage = "docker_ping"
	ResourceAccounting Stage = "resource_accounting"
	CallbackStore      Stage = "callback_store"
	DiagnosticsStore   Stage = "diagnostics_store"
	ReleaseStore       Stage = "release_store"
	RetentionStore     Stage = "retention_store"
	LaunchJournal      Stage = "launch_journal"
)

type contextKey struct{}

// Observation owns one request's diagnostic identity and completed stages.
// Its fields cannot be supplied by a request, and no result authorizes work.
type Observation struct {
	id      uuidv4.Value
	started time.Time
	mu      sync.Mutex
	stages  []slog.Attr
}

// Start accepts exactly one canonical diagnostic ID, otherwise generating a
// fresh one. Invalid input is neither retained nor logged. Outbound clients
// pass no values so every probe has a distinct identity.
func Start(ctx context.Context, values ...string) (context.Context, *Observation) {
	var id uuidv4.Value
	if len(values) == 1 {
		id, _ = uuidv4.Parse(values[0], errors.New("invalid health probe ID"))
	}
	if !id.Valid() {
		id = uuidv4.FromUUID(uuid.New())
	}
	observation := &Observation{id: id, started: time.Now()}
	return context.WithValue(ctx, contextKey{}, observation), observation
}

// ID returns the validated wire identity; it is never a credential.
func (o *Observation) ID() string { return o.id.String() }

// Record retains only stages that actually ran, with their decision-time
// result. Cancellation after a successful stage cannot relabel that result.
func Record(ctx context.Context, stage Stage, duration time.Duration, err error) {
	observation, _ := ctx.Value(contextKey{}).(*Observation)
	if observation == nil {
		return
	}
	observation.mu.Lock()
	defer observation.mu.Unlock()
	observation.stages = append(observation.stages, slog.Group(string(stage),
		slog.Duration("duration", duration), slog.String("outcome", outcome(err))))
}

// Finish logs each server completion, and slow or failed client completions,
// without URLs, payloads, arbitrary error text, or high-cardinality metrics.
// Fast server successes are necessary to distinguish a completed backend check
// from a request that never reached it when the client reports a timeout.
// The caller owns the single terminal observation; no work runs in background.
func (o *Observation) Finish(logger *slog.Logger, side Side, status int, err error) {
	duration := time.Since(o.started)
	level := slog.LevelWarn
	if err == nil && status < 400 && duration < SlowThreshold {
		if side != Server {
			return
		}
		level = slog.LevelInfo
	}
	if logger == nil {
		logger = slog.Default()
	}
	result := outcome(err)
	if err == nil && status >= 400 {
		result = "failed"
	}
	o.mu.Lock()
	stages := slog.GroupAttrs("stages", o.stages...)
	o.mu.Unlock()
	logger.LogAttrs(context.Background(), level, "health probe completed",
		slog.String("probe_id", o.ID()), slog.String("side", string(side)),
		slog.Duration("duration", duration), slog.Int("status", status),
		slog.String("outcome", result), stages)
}

func outcome(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case err != nil:
		return "failed"
	default:
		return "healthy"
	}
}
