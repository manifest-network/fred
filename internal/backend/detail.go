package backend

import "errors"

// detailedError pairs a fred sentinel with the detail a backend authored inside
// a VALIDATED error envelope.
//
// Error() renders only the detail. The sentinel identity travels through
// Unwrap() — for errors.Is — and never through string concatenation, because
// the backend's detail already opens with its own category text: the docker
// backend serializes err.Error() of an error built with %w around the very same
// sentinel. Re-prefixing it (`fmt.Errorf("%w: %s", sentinel, detail)`) produced
// a doubled tenant-facing message:
//
//	retained data exceeds the requested smaller tier: retained data exceeds the requested smaller tier: service "app": …
//
// Exactly one layer owns a sentinel's text; here that layer is the producer.
// Trimming the duplicated prefix off the string instead would couple this
// package to the producer's exact wording and re-break the moment either side
// rephrases.
//
// Same split as k8s.io/apimachinery's StatusError, whose Error() returns only
// ErrStatus.Message while the classification lives in the separate Reason field.
type detailedError struct {
	sentinel error
	detail   string
}

// withDetail attaches a backend-authored detail to a sentinel. detail must come
// from a declared field of an envelope this package has already parsed — never
// from a raw response body (see errMalformedErrorBody).
func withDetail(sentinel error, detail string) error {
	return &detailedError{sentinel: sentinel, detail: detail}
}

func (e *detailedError) Error() string { return e.detail }

// Unwrap carries the classification so errors.Is(err, ErrValidation),
// errors.Is(err, ErrUnknownSKU) and friends keep matching. Every consumer that
// decides a real outcome — the circuit-breaker allowlist in IsSuccessful, the
// reconciler's reject-vs-close branch, the tenant status mapping in
// internal/api — is errors.Is-keyed, so this is the only property that must
// hold.
func (e *detailedError) Unwrap() error { return e.sentinel }

// Detail returns the backend-authored detail of err, if it carries one. Reports
// false for any other error, including a bare sentinel.
func Detail(err error) (string, bool) {
	var de *detailedError
	if errors.As(err, &de) {
		return de.detail, true
	}
	return "", false
}
