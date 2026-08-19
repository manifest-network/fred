package backend

// export_test.go holds scaffolding that exists ONLY for this package's
// tests. It is compiled into the test binary and never into a fred
// binary -- which matters here because mock.go IS production code: it is
// the implementation behind the cmd/mock-backend binary, which the
// Makefile builds and installs (ENG-354). Releases carry only providerd
// and docker-backend, but a non-test file in a linked package is still
// production code by the rule that matters here.

// GetMockProvision returns a copy of the raw internal provision record
// for leaseUUID, and whether one exists. It reads the map directly so a
// test can assert on stored state as stored -- unlike GetProvision,
// which narrows the record to a ProvisionInfo and turns a miss into an
// ErrNotProvisioned rather than a false.
func (m *MockBackend) GetMockProvision(leaseUUID string) (*mockProvision, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, exists := m.provisions[leaseUUID]
	if !exists {
		return nil, false
	}

	// Return a copy
	copy := *p
	return &copy, true
}
