// Package e2e is Fred's black-box test suite. It starts the released binaries
// as separate processes and asserts only through their external surfaces: the
// backend HTTP API, Docker, and the host filesystem.
//
// The harness is one fixture and one client. Backend owns a docker-backend
// process and everything it needs: an XFS loopback, a storage identity, a
// listen port, a throwaway CA, a sink for its callbacks, and an image
// registry of its own. Client sends signed requests and decodes responses,
// nothing else. A new feature adds a test, not harness code.
//
// This package is the backend tier: its tests drive a backend directly. The
// chain subpackage is the providerd tier, which puts a chain and a providerd
// in front of the same backend fixture.
//
// Everything but this file carries the integration build tag. The tests need
// root, Docker, xfsprogs and capsh, and skip without them; `make test-e2e`
// builds the binaries unprivileged and runs the test binary under sudo. Run
// one e2e test binary at a time per Docker daemon: TestMain removes whatever a
// killed run left behind, which would include a concurrent run's resources.
// A backend needs a daemon to itself, so a test with two backends uses two
// daemons (DockerSocketsEnv).
package e2e
