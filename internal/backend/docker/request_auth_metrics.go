package docker

import "github.com/manifest-network/fred/internal/hmacauth"

// requestAuthFailure is the closed cause of a refused providerd request
// signature. The zero value is invalid and never counted.
type requestAuthFailure uint8

const (
	requestAuthFailureInvalid requestAuthFailure = iota
	requestAuthFailureMissing
	requestAuthFailureFormat
	requestAuthFailureExpired
	requestAuthFailureFuture
	requestAuthFailureMismatch
)

var requestAuthFailures = [...]requestAuthFailure{
	requestAuthFailureMissing,
	requestAuthFailureFormat,
	requestAuthFailureExpired,
	requestAuthFailureFuture,
	requestAuthFailureMismatch,
}

func (failure requestAuthFailure) label() string {
	switch failure {
	case requestAuthFailureMissing:
		return "missing"
	case requestAuthFailureFormat:
		return "format"
	case requestAuthFailureExpired:
		return "expired"
	case requestAuthFailureFuture:
		return "future"
	case requestAuthFailureMismatch:
		return "mismatch"
	default:
		return ""
	}
}

func requestAuthFailureOf(err error) requestAuthFailure {
	switch hmacauth.FailureReasonOf(err) {
	case hmacauth.FailureFormat:
		return requestAuthFailureFormat
	case hmacauth.FailureExpired:
		return requestAuthFailureExpired
	case hmacauth.FailureFuture:
		return requestAuthFailureFuture
	case hmacauth.FailureMismatch:
		return requestAuthFailureMismatch
	default:
		return requestAuthFailureInvalid
	}
}

// Request key slot labels.
const (
	requestKeySlotCurrent = "current"
	requestKeySlotNext    = "next"
)

// Missing counts a providerd request with no signature.
func (RequestSignatureMetrics) Missing() {
	requestAuthRefusals.inc(requestAuthFailureMissing)
}

// Checked counts one providerd request signature check: the verifying key
// slot on success, or the closed refusal reason.
func (m RequestSignatureMetrics) Checked(slot hmacauth.KeySlot, err error) {
	if err != nil {
		requestAuthRefusals.inc(requestAuthFailureOf(err))
		return
	}
	switch slot {
	case hmacauth.KeySlotCurrent:
		m.current.Inc()
	case hmacauth.KeySlotRotation:
		if m.next != nil {
			m.next.Inc()
		}
	}
}
