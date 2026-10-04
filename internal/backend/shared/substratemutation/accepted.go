package substratemutation

import (
	"errors"
	"fmt"
)

// Accepted is a workflow's Finding that its own live session accepted. It is
// the only way a Finding reaches a classifier, and it is an option: the zero
// value carries no finding, which is what recovery and every unsuccessful
// session hand the classifier. A domain's Finding type therefore needs no
// "the zero value means none" convention, and nothing outside this package can
// make a value present.
//
// Accepting is not settling. The Guard still discards an accepted finding when
// any later Step or Prepare of the session fails, or when the workflow returns
// an error. What acceptance proves is the precondition a workflow needs before
// irreversible work that is worthwhile only for a definite outcome (ENG-1125):
// when Accept refuses, the Guard would have discarded the finding anyway, so
// that work must not run and the substrate stays as recovery needs it.
type Accepted[F any] struct {
	session *session
	finding F
}

// Accept accepts finding for the live session that minted runner. It refuses,
// and returns the zero value, when:
//   - the Runner is inert (zero, or its Execute call already returned);
//   - the session is a recovery execution, which never carries a finding;
//   - any Step or Prepare of the session already failed, even one the
//     workflow swallowed: Execute would discard the finding for that issue.
//
// A workflow calls it before the irreversible work its finding depends on, and
// passes the returned value, never the bare finding, to that work.
func Accept[F any](runner Runner, finding F) (Accepted[F], error) {
	s := runner.session
	if s == nil {
		return Accepted[F]{}, unavailable("accept finding")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return Accepted[F]{}, unavailable("accept finding")
	}
	if !s.live {
		return Accepted[F]{}, errors.New("a recovery execution never carries a finding")
	}
	if s.issues != nil {
		return Accepted[F]{}, fmt.Errorf("finding is unreachable after an earlier issue of this execution: %w", s.issues)
	}
	return Accepted[F]{session: s, finding: finding}, nil
}

// Finding returns the accepted finding, and false for the zero value.
func (a Accepted[F]) Finding() (F, bool) {
	if a.session == nil {
		var none F
		return none, false
	}
	return a.finding, true
}

// acceptedBy reports whether a is absent or was accepted by s. Execute rejects
// a finding another session accepted.
func (a Accepted[F]) acceptedBy(s *session) bool {
	return a.session == nil || a.session == s
}
