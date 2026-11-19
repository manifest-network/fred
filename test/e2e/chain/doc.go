// Package chain is the providerd tier of Fred's black-box suite. Each test
// starts a whole provider: a single-node chain run by the real manifestd
// binary, one or more docker-backends, and the providerd that joins them. A
// test acts as a tenant and asserts through the chain, the provider's API and
// metrics, the backends' API and Docker.
//
// chain.go is a wrapper around the manifestd command line that uses only the
// standard library. Provider is the tier's fixture, built on the backend
// fixture of the parent package, and Tenant is what a test acts through. A new
// feature adds a test, not harness code.
//
// Everything but this file carries the integration build tag. The tests need
// what the parent package needs, plus a manifestd binary named by the
// MANIFESTD environment variable; `make test-e2e` passes it on.
package chain
