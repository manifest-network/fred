package failurecause

// Provenance records how one instance death reached the state machine, and for
// which instance. The zero value is a death found without a live event,
// typically by the periodic inventory sweep; such a death never counts,
// because a signal that preceded it cannot be ruled out.
type Provenance struct {
	kind provenanceKind
	// instanceID is the instance whose die event minted this provenance. It is
	// empty only for the zero value. ClassifyDeath ignores a provenance minted
	// for another instance, so one observed run cannot vouch for another death.
	instanceID string
}

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

// InstanceID is the instance whose live die event minted this provenance, or
// "" for the zero value. A live death observation takes its instance from
// here, so it cannot carry another instance's provenance.
func (p Provenance) InstanceID() string { return p.instanceID }

// maxObservedRuns bounds one session's memory. Entries are consumed by the
// run's die event, so a session tracks at most the instances currently
// running. A run started beyond the bound is not tracked, and its death
// attributes as a partial run, which never counts.
const maxObservedRuns = 1 << 16

// eventSession records the instance lifecycle events of ONE continuous event
// subscription. A substrate creates a new session for every (re)connection:
// events missed during a reconnect gap are unknowable, so attribution starts
// over from an empty session. A session is not safe for concurrent use; the
// single goroutine consuming the stream owns it.
//
// The type is unexported on purpose. Outside this package a session can be
// obtained only from NewEventSession and held only in a local variable: it
// cannot be named as a struct field or a parameter, so it cannot leave the
// frame that consumes the stream. internal/testutil confines the constructor
// and the Observe methods to the Docker event loop's reader. Its zero value
// tracks nothing, so it can mint no observed run.
type eventSession struct {
	runs map[string]observedRun
}

type observedRun struct {
	signaled bool
}

// NewEventSession starts an empty session for a newly connected stream. Only
// a substrate's live event reader may call it. It returns an unexported type
// deliberately (see eventSession).
func NewEventSession() *eventSession {
	return &eventSession{runs: make(map[string]observedRun)}
}

// ObserveStart records that the stream saw instanceID start a new run.
func (s *eventSession) ObserveStart(instanceID string) {
	if s == nil || s.runs == nil || instanceID == "" {
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
func (s *eventSession) ObserveSignal(instanceID string) {
	if s == nil {
		return
	}
	if run, tracked := s.runs[instanceID]; tracked {
		run.signaled = true
		s.runs[instanceID] = run
	}
}

// ObserveExit consumes the record of instanceID's run when its die event
// arrives, and returns how the death was observed, bound to instanceID.
func (s *eventSession) ObserveExit(instanceID string) Provenance {
	if instanceID == "" {
		return Provenance{}
	}
	partial := Provenance{kind: provenancePartialRun, instanceID: instanceID}
	if s == nil {
		return partial
	}
	run, tracked := s.runs[instanceID]
	delete(s.runs, instanceID)
	switch {
	case !tracked:
		return partial
	case run.signaled:
		return Provenance{kind: provenanceSignaled, instanceID: instanceID}
	default:
		return Provenance{kind: provenanceObservedRun, instanceID: instanceID}
	}
}
