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

// InitRequestKeyMetrics pre-creates the request key series for keys: the
// current slot always, the next slot only while a next key is configured, so
// a zero on "next" means unused rather than not configured.
func InitRequestKeyMetrics(keys hmacauth.VerifyKeys) {
	requestSignatureKeyTotal.WithLabelValues(requestKeySlotCurrent)
	if keys.HasRotation() {
		requestSignatureKeyTotal.WithLabelValues(requestKeySlotNext)
		requestNextKeyConfigured.Set(1)
		return
	}
	requestNextKeyConfigured.Set(0)
}

// RecordRequestSignatureMissing counts a providerd request with no signature.
func RecordRequestSignatureMissing() {
	requestAuthFailuresTotal.WithLabelValues(requestAuthFailureMissing.label()).Inc()
}

// RecordRequestSignatureChecked counts one providerd request signature check:
// the verifying key slot on success, or the closed refusal reason.
func RecordRequestSignatureChecked(slot hmacauth.KeySlot, err error) {
	if err != nil {
		if label := requestAuthFailureOf(err).label(); label != "" {
			requestAuthFailuresTotal.WithLabelValues(label).Inc()
		}
		return
	}
	switch slot {
	case hmacauth.KeySlotCurrent:
		requestSignatureKeyTotal.WithLabelValues(requestKeySlotCurrent).Inc()
	case hmacauth.KeySlotRotation:
		requestSignatureKeyTotal.WithLabelValues(requestKeySlotNext).Inc()
	}
}
