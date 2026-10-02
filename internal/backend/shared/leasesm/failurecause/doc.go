// Package failurecause attributes one lease workload failure to its cause, for
// the consecutive-failure terminal budget the lease state machine keeps
// (ENG-799).
//
// That budget can end in an irreversible on-chain lease close, so attribution
// is an allowlist. A failure counts only when the tenant workload's own process
// is positively observed exiting. Every other failure is recorded with a cause
// that never counts: a death after an observed operator or daemon signal, a
// container that is gone (removing, dead or absent), a platform or maintenance
// failure, and anything unclassified.
//
// This mirrors the Kubernetes Job podFailurePolicy split (KEP-3329), which
// keeps infrastructure disruptions out of backoffLimit. KEP-3329 also found
// that an exit code cannot tell an external kill from an application failure,
// so classification here is by provenance, never by exit code. One inversion is
// deliberate: Kubernetes counts an unmatched failure because a failed Job can be
// retried, whereas fred's close cannot be undone, so an unmatched failure never
// counts here.
//
// The package boundary seals the types. Cause and Provenance have unexported
// fields, so neither the state machine nor a substrate can mint a counting
// Cause from an integer or a literal. ClassifyDeath is the only constructor of
// a counting Cause, and it additionally requires a Provenance that only an
// EventSession mints from one continuous event stream.
package failurecause
