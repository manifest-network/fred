package backend

// newUnboundHTTPClientForTest is confined to this package's test binary. Tests
// in other packages exercise the identity-bound production constructor through
// internal/testsupport/backendclient instead.
func newUnboundHTTPClientForTest(cfg HTTPClientConfig) *HTTPClient {
	return newHTTPClient(cfg)
}
