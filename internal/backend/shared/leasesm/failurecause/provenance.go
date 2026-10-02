package failurecause

// Provenance records how one instance death reached the state machine. The
// zero value is a death found without a live event, typically by the periodic
// inventory sweep; such a death never counts, because a signal that preceded it
// cannot be ruled out.
type Provenance struct{ kind provenanceKind }

type provenanceKind uint8

const (
	// provenanceUnobserved: no live event delivered this death.
	provenanceUnobserved provenanceKind = iota
	// provenancePartialRun: a live die event whose run start the stream did not
	// observe. A signal sent before the stream connected, or during a
	// reconnect gap, cannot be ruled out.
	provenancePartialRun
	// provenanceSignaled: a live die event after an observed API kill to the
	// same run (docker kill, docker stop, or the daemon stopping it).
	provenanceSignaled
	// provenanceObservedRun: a live die event, and the same continuous stream
	// observed the run's start and no API kill to it.
	provenanceObservedRun
	// provenanceSentinel bounds the closed set. It is never a valid provenance.
	provenanceSentinel
)

// Label names the provenance for logs.
func (p Provenance) Label() string {
	switch p.kind {
	case provenancePartialRun:
		return "partial_run"
	case provenanceSignaled:
		return "signaled"
	case provenanceObservedRun:
		return "observed_run"
	default:
		return "unobserved"
	}
}

// maxObservedRuns bounds one session's memory. Entries are consumed by the
// run's die event, so a session tracks at most the instances currently
// running. A run started beyond the bound is not tracked, and its death
// attributes as a partial run, which never counts.
const maxObservedRuns = 1 << 16

// EventSession records the instance lifecycle events of ONE continuous event
// subscription. A substrate creates a new session for every (re)connection:
// events missed during a reconnect gap are unknowable, so attribution starts
// over from an empty session. A session is not safe for concurrent use; the
// single goroutine consuming the stream owns it.
type EventSession struct {
	runs map[string]observedRun
}

type observedRun struct {
	signaled bool
}

// NewEventSession starts an empty session for a newly connected stream.
func NewEventSession() *EventSession {
	return &EventSession{runs: make(map[string]observedRun)}
}

// ObserveStart records that the stream saw instanceID start a new run.
func (s *EventSession) ObserveStart(instanceID string) {
	if s == nil || instanceID == "" {
		return
	}
	if _, tracked := s.runs[instanceID]; !tracked && len(s.runs) >= maxObservedRuns {
		return
	}
	s.runs[instanceID] = observedRun{}
}

// ObserveSignal records an API kill delivered to instanceID's current run.
// Docker logs one for docker kill and for every signal docker stop sends,
// including the daemon stopping a container itself. A signal to a run whose
// start the stream did not see needs no record: that death is already a
// partial run.
func (s *EventSession) ObserveSignal(instanceID string) {
	if s == nil {
		return
	}
	if run, tracked := s.runs[instanceID]; tracked {
		run.signaled = true
		s.runs[instanceID] = run
	}
}

// ObserveExit consumes the record of instanceID's run when its die event
// arrives, and returns how the death was observed.
func (s *EventSession) ObserveExit(instanceID string) Provenance {
	if s == nil {
		return Provenance{kind: provenancePartialRun}
	}
	run, tracked := s.runs[instanceID]
	delete(s.runs, instanceID)
	switch {
	case !tracked:
		return Provenance{kind: provenancePartialRun}
	case run.signaled:
		return Provenance{kind: provenanceSignaled}
	default:
		return Provenance{kind: provenanceObservedRun}
	}
}
