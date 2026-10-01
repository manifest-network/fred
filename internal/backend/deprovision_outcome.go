package backend

// DeprovisionRefusal names why a client withheld a deprovision request. Its
// zero value proves nothing: the request may have been sent.
type DeprovisionRefusal uint8

const (
	DeprovisionRefusalUnproven DeprovisionRefusal = iota
	// DeprovisionRefusedCircuitOpen: the circuit breaker refused admission.
	DeprovisionRefusedCircuitOpen
	// DeprovisionRefusedFenced: the backend is fenced, so its client has no
	// means to send anything until the operator lifts the fence.
	DeprovisionRefusedFenced
)

// deprovisionNotDispatchedError can only be issued by the HTTP client before
// its request exists: by a fenced client, or by the circuit admission boundary
// before the request callback runs. It authorizes scheduling another attempt,
// never settlement or a claim that cleanup ran.
type deprovisionNotDispatchedError struct {
	client    *HTTPClient
	leaseUUID string
	refusal   DeprovisionRefusal
}

func (err *deprovisionNotDispatchedError) Error() string { return err.Unwrap().Error() }
func (err *deprovisionNotDispatchedError) Unwrap() error {
	if err.refusal == DeprovisionRefusedFenced {
		return &fencedError{backend: err.client.name}
	}
	return ErrCircuitOpen
}

// DeprovisionRefusalOf recognizes a local refusal by this exact client for
// this exact lease. A sentinel error, remote response, unknown transport
// effect, or proof from another client or lease cannot establish that no
// request was sent.
func DeprovisionRefusalOf(client Backend, leaseUUID string, err error) DeprovisionRefusal {
	transport, ok := client.(*HTTPClient)
	if !ok || transport == nil || leaseUUID == "" {
		return DeprovisionRefusalUnproven
	}
	// Only the direct call result is evidence; wrappers and joined old proofs
	// must not reclassify a request with unknown effects as undispatched.
	refused, ok := err.(*deprovisionNotDispatchedError) //nolint:errorlint // Authority requires the direct, unwrapped transport result.
	if !ok || refused == nil || refused.client != transport || refused.leaseUUID != leaseUUID {
		return DeprovisionRefusalUnproven
	}
	switch refused.refusal {
	case DeprovisionRefusedCircuitOpen, DeprovisionRefusedFenced:
		return refused.refusal
	default:
		return DeprovisionRefusalUnproven
	}
}

// DeprovisionLifecyclePending recognizes a deferred close observation from
// this exact HTTP client, lease and endpoint. It authorizes scheduling a retry,
// never completing the interrupted operation or claiming cleanup has run.
func DeprovisionLifecyclePending(client Backend, leaseUUID string, err error) bool {
	transport, ok := client.(*HTTPClient)
	if !ok || transport == nil || leaseUUID == "" {
		return false
	}
	pending, ok := err.(*deprovisionLifecyclePendingResponse) //nolint:errorlint // Only the direct transport result carries scheduling provenance.
	return ok && pending != nil && pending.client == transport && pending.leaseUUID == leaseUUID
}

// Only the exact decoded /deprovision response branch creates this variant.
// Other endpoints produce lifecyclePendingResponse, which has no close scope.
type deprovisionLifecyclePendingResponse struct {
	client    *HTTPClient
	leaseUUID string
}

func (*deprovisionLifecyclePendingResponse) Error() string {
	return "admitted lifecycle work remains pending"
}
