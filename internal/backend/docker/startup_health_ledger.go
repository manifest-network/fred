package docker

import (
	"strings"
	"sync"
)

// startupHealthLedger remembers the containers that a startup watch saw
// running and healthy (ENG-1125). It is the single memory behind the sticky
// health rule: once a health-gated container passed its check, every
// authority that decides a startup outcome judges it ready for as long as it
// runs, whatever its health check reports now. That is the live watch itself,
// the provision classifier that confirms the watch's Ready, operation
// recovery, and maintenance readiness. A later unhealthy report is a flap,
// which the steady state ignores too; an exit is still seen, because the rule
// applies only to a running container.
//
// A startup pass is its only writer (recordPassedHealth, which takes the pass
// itself, and which internal/testutil confines to startupMemory.remember), so
// a fact is only ever a health verdict a watch inspected. A container ID is
// never reused, so a fact can never apply to another container.
//
// The ledger lives in memory and is bounded: it forgets the oldest fact first.
// A fact that was never recorded or was forgotten (after a restart, say)
// leaves an authority reading health fresh, which can only find fewer members
// ready, never more. The zero value is ready to use.
type startupHealthLedger struct {
	mu          sync.Mutex
	healthyByID map[string]struct{}
	healthyRing []string
	healthyNext int
}

// startupHealthLedgerCapacity bounds the remembered facts. An authority reads
// a fact within one provision or maintenance deadline of the watch that
// recorded it, far sooner than this many health-gated containers can start.
const startupHealthLedgerCapacity = 4096

// recordPassedHealth records every health-gated member that pass saw running
// and healthy. Only startupMemory.remember may call it.
func (l *startupHealthLedger) recordPassedHealth(pass startupPass) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, member := range pass.members {
		info := pass.infos[i]
		if !member.healthGated || member.id == "" || info == nil ||
			!strings.EqualFold(info.Status, "running") || info.Health != HealthStatusHealthy {
			continue
		}
		if _, known := l.healthyByID[member.id]; known {
			continue
		}
		if l.healthyByID == nil {
			l.healthyByID = make(map[string]struct{})
			l.healthyRing = make([]string, startupHealthLedgerCapacity)
		}
		if evicted := l.healthyRing[l.healthyNext]; evicted != "" {
			delete(l.healthyByID, evicted)
		}
		l.healthyRing[l.healthyNext] = member.id
		l.healthyNext = (l.healthyNext + 1) % startupHealthLedgerCapacity
		l.healthyByID[member.id] = struct{}{}
	}
}

// passedHealth reports whether a startup watch saw containerID running and
// healthy.
func (l *startupHealthLedger) passedHealth(containerID string) bool {
	if containerID == "" {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, passed := l.healthyByID[containerID]
	return passed
}

// gatedHealth is the health every outcome authority judges a running
// health-gated container by: healthy once a startup watch saw it pass, and as
// inspected otherwise. Callers apply it only to a running container; any other
// state is judged by its status, so an exit is never hidden.
func (b *Backend) gatedHealth(containerID string, inspected HealthStatus) HealthStatus {
	if b.startupHealth.passedHealth(containerID) {
		return HealthStatusHealthy
	}
	return inspected
}
