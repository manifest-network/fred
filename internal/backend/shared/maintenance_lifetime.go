package shared

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// MaintenanceWorkerHandoff carries one backend-owned lifetime across actor
// admission. It can issue only one worker, even when commands or handoffs are
// copied. Target deadline expiry does not revoke the independent cancellation
// root needed by compensation. The zero handoff is invalid.
type MaintenanceWorkerHandoff struct{ state *maintenanceWorkerLifetime }

// MaintenanceWorkerLifetime is the claimed actor worker's cancellation owner.
// Both target and compensation budgets derive from its backend shutdown root.
// Copies share cancellation; no copy can revive a closed worker.
type MaintenanceWorkerLifetime struct{ owner *maintenanceWorkerLifetime }

type maintenanceWorkerLifetime struct {
	cancellation context.Context
	target       context.Context
	cancel       context.CancelFunc
	claimed      atomic.Bool
}

// NewMaintenanceWorkerHandoff is called with the backend shutdown context.
// The returned cancel discards only an unclaimed handoff. Once accepted, the
// worker retains cancellation ownership through completion.
func NewMaintenanceWorkerHandoff(shutdown context.Context, timeout time.Duration) (MaintenanceWorkerHandoff, context.CancelFunc) {
	cancellation, cancelWorker := context.WithCancel(shutdown)
	target, cancelTarget := context.WithTimeout(cancellation, timeout)
	cancel := func() { cancelWorker(); cancelTarget() }
	handoff := MaintenanceWorkerHandoff{state: &maintenanceWorkerLifetime{
		cancellation: cancellation, target: target, cancel: cancel,
	}}
	return handoff, handoff.Discard
}

func (h MaintenanceWorkerHandoff) Valid() bool { return h.state != nil }

// Discard releases a rejected or undispatched handoff. A lost HTTP reply or a
// copied command cannot revoke a worker that has already claimed ownership.
func (h MaintenanceWorkerHandoff) Discard() {
	if h.Valid() && h.state.claimed.CompareAndSwap(false, true) {
		h.state.cancel()
	}
}

// TargetContext bounds actor admission by the original target deadline without
// granting access to the independent compensation cancellation root.
func (h MaintenanceWorkerHandoff) TargetContext() context.Context {
	if !h.Valid() {
		return nil
	}
	return h.state.target
}

// ClaimWorker transfers cancellation ownership exactly once. A canceled handoff
// remains canceled even when it is claimed after waiting in the actor inbox.
func (h MaintenanceWorkerHandoff) ClaimWorker() (MaintenanceWorkerLifetime, error) {
	if !h.Valid() || !h.state.claimed.CompareAndSwap(false, true) {
		return MaintenanceWorkerLifetime{}, errors.New("maintenance worker lifetime is invalid or already claimed")
	}
	return MaintenanceWorkerLifetime{owner: h.state}, nil
}

func (l MaintenanceWorkerLifetime) Valid() bool { return l.owner != nil }

func (l MaintenanceWorkerLifetime) TargetContext() context.Context {
	if !l.Valid() {
		return nil
	}
	return l.owner.target
}

// Cancel revokes target and compensation work and releases the handoff's
// shutdown registration. The actor invokes it on close and worker completion.
func (l MaintenanceWorkerLifetime) Cancel() {
	if l.Valid() {
		l.owner.cancel()
	}
}

func (l MaintenanceWorkerLifetime) compensationContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(l.owner.cancellation, 2*time.Minute)
}
