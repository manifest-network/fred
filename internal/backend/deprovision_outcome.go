package backend

// deprovisionNotDispatchedError can only be issued by the HTTP client's
// circuit admission boundary, before its request callback runs. It authorizes
// scheduling another attempt, never settlement or a claim that cleanup ran.
type deprovisionNotDispatchedError struct {
	client    *HTTPClient
	leaseUUID string
}

func (*deprovisionNotDispatchedError) Error() string { return ErrCircuitOpen.Error() }
func (*deprovisionNotDispatchedError) Unwrap() error { return ErrCircuitOpen }

// DeprovisionNotDispatched recognizes local circuit refusal for this exact
// client and lease. A sentinel error, remote response, unknown transport effect,
// or proof from another client or lease cannot establish that no request was sent.
func DeprovisionNotDispatched(client Backend, leaseUUID string, err error) bool {
	transport, ok := client.(*HTTPClient)
	if !ok || transport == nil || leaseUUID == "" {
		return false
	}
	// Only the direct call result is evidence; wrappers and joined old proofs
	// must not reclassify a request with unknown effects as undispatched.
	refused, ok := err.(*deprovisionNotDispatchedError) //nolint:errorlint // Authority requires the direct, unwrapped transport result.
	return ok && refused != nil &&
		refused.client == transport && refused.leaseUUID == leaseUUID
}
