// Package terminalverdict reads a backend's consecutive-failure budget
// observation (ENG-799) into the only value providerd's failure-budget lease
// close accepts.
//
// providerd closes an ACTIVE lease for repeated failure on-chain, which cannot
// be undone and stops a paying tenant. The backend decides at the failure
// source and counts only its tenant's own workload failures; providerd acts
// only on a well-formed exhausted verdict about a Failed lease, read from
// complete inventory. An absent verdict (an older or third-party backend), an
// unrecognized one, or one that contradicts the lease's status never closes.
//
// The package boundary seals that rule. Verdict and Exhaustion have unexported
// fields and FromProvision is the only way to obtain either, so reconciler code
// cannot build an exhausted verdict from an integer, a string literal or
// FailCount, and the close path, which accepts only an Exhaustion, cannot be
// reached without one.
package terminalverdict
