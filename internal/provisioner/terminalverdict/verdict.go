package terminalverdict

import "github.com/manifest-network/fred/internal/backend"

// Verdict is providerd's reading of one lease's budget observation. The zero
// value is an absent verdict, which never closes a lease.
type Verdict struct {
	kind                kind
	leaseUUID           string
	consecutiveFailures int
}

type kind uint8

const (
	// kindAbsent: the backend reported no budget.
	kindAbsent kind = iota
	// kindUnknown: an unrecognized verdict, a negative count, or an exhausted
	// verdict that contradicts the lease's status or count.
	kindUnknown
	kindRetry
	kindExhausted
	// kindSentinel bounds the closed set. It is never a valid verdict.
	kindSentinel
)

// FromProvision reads one ProvisionInfo from complete inventory. It is the only
// constructor of a Verdict. A verdict is exhausted only when the lease is
// Failed, the backend reported exactly "exhausted", the recorded count is at
// least one, and the lease is identified.
func FromProvision(info backend.ProvisionInfo) Verdict {
	verdict := Verdict{leaseUUID: info.LeaseUUID}
	observation := info.TerminalBudget
	if observation == nil {
		return verdict
	}
	verdict.consecutiveFailures = observation.ConsecutiveFailures
	switch {
	case observation.ConsecutiveFailures < 0:
		verdict.kind = kindUnknown
	case observation.Verdict == backend.TerminalVerdictRetry:
		verdict.kind = kindRetry
	case observation.Verdict == backend.TerminalVerdictExhausted &&
		info.Status == backend.ProvisionStatusFailed &&
		observation.ConsecutiveFailures >= 1 && info.LeaseUUID != "":
		verdict.kind = kindExhausted
	default:
		verdict.kind = kindUnknown
	}
	return verdict
}

// Label is the bounded metric label of this verdict: exhausted, retry, absent
// or unknown.
func (v Verdict) Label() string {
	switch v.kind {
	case kindRetry:
		return string(backend.TerminalVerdictRetry)
	case kindExhausted:
		return string(backend.TerminalVerdictExhausted)
	case kindUnknown:
		return "unknown"
	default:
		return "absent"
	}
}

// Labels returns every verdict label in a fixed order, so a metric can be
// pre-initialized with exactly the closed set.
func Labels() []string {
	labels := make([]string, 0, int(kindSentinel))
	for k := kindAbsent; k < kindSentinel; k++ {
		labels = append(labels, Verdict{kind: k}.Label())
	}
	return labels
}

// ConsecutiveFailures is the backend's recorded count, for logs only. It never
// decides a close.
func (v Verdict) ConsecutiveFailures() int { return v.consecutiveFailures }

// Exhausted returns the close proof, and true, only for an exhausted verdict.
func (v Verdict) Exhausted() (Exhaustion, bool) {
	if v.kind != kindExhausted {
		return Exhaustion{}, false
	}
	return Exhaustion{leaseUUID: v.leaseUUID, consecutiveFailures: v.consecutiveFailures}, true
}

// Exhaustion proves that complete inventory reported an exhausted budget for
// one Failed lease. The failure-budget close accepts nothing else. The zero
// value is invalid.
type Exhaustion struct {
	leaseUUID           string
	consecutiveFailures int
}

// Valid reports whether this proof came from an exhausted verdict.
func (e Exhaustion) Valid() bool { return e.leaseUUID != "" && e.consecutiveFailures >= 1 }

// LeaseUUID is the lease the verdict is about; the close must match it.
func (e Exhaustion) LeaseUUID() string { return e.leaseUUID }

// ConsecutiveFailures is the backend's recorded count, for logs.
func (e Exhaustion) ConsecutiveFailures() int { return e.consecutiveFailures }

// TenantView returns a copy of the budget observation for a tenant-facing
// response, or nil when the backend reported none or reported one providerd
// does not recognize: a tenant never sees a value providerd would not act on.
func TenantView(info backend.ProvisionInfo) *backend.TerminalBudgetObservation {
	switch FromProvision(info).kind {
	case kindRetry, kindExhausted:
		observation := *info.TerminalBudget
		return &observation
	default:
		return nil
	}
}
