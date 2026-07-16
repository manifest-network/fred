# Contributing to Fred

Thanks for your interest in contributing. This guide covers getting a working dev environment, running the test suites, code style, and the PR workflow.

For an overview of what Fred does and how it's structured, start with [README.md](README.md) and [ARCHITECTURE.md](ARCHITECTURE.md). For the security model, see [SECURITY.md](SECURITY.md).

---

## Prerequisites

- **Go 1.26.5** (per `go.mod`) — the `go 1.26.5` directive sets the toolchain floor. Fred also uses `sync.WaitGroup.Go()` and `testing.B.Loop()` (added in Go 1.25).
- **Docker 24+** with iptables enabled — required for `make test-integration` and the docker-backend.
- **(Optional) `manifestd`** — only needed if you want to run end-to-end against a local chain via `scripts/dev-init.sh`.
- **(Optional) `golangci-lint`** — `make lint` runs `go vet` unconditionally and adds golangci checks if it's on `PATH`.
- **(Optional) `btrfs-progs` + root** — only for `make test-integration-volume`, which exercises filesystem quotas.

---

## Getting started

```bash
git clone https://github.com/manifest-network/fred.git
cd fred
go mod download
make all              # builds providerd, mock-backend, docker-backend, k3s-backend into ./build/
make test             # all unit tests (runs the stress suite too; see Stress tests)
```

For an end-to-end local environment against a running chain:

```bash
bash scripts/dev-init.sh    # registers a provider + SKUs, generates configs, writes a callback secret
./build/docker-backend --config docker-backend.yaml
./build/providerd --config config.docker.yaml
```

`scripts/dev-init.sh` is parameterized via environment variables (see the script header). All defaults assume a local chain at `http://localhost:26657` with the `acc0` test key.

To exercise just the provisioner without Docker, run `make run-mock` in one shell and `make run` in another — the mock backend ignores SKUs and simulates provisioning.

The experimental K3s backend can be built with `make build-k3s` and run with `make run-k3s` (override its config via `K3S_BACKEND_CONFIG=...`). It is a non-functional scaffold (see *Project layout*) — the provisioner is a stub.

---

## Project layout

```
cmd/
├── providerd/          Main daemon (chain ↔ backends, tenant API)
├── docker-backend/     Production Docker backend
├── k3s-backend/        Experimental K3s backend (scaffold, non-functional)
├── mock-backend/       Test/dev backend
└── loadtest/           Load tester (exercises the API)

internal/
├── adr036/             ADR-036 message signing
├── api/                Tenant HTTP API + auth + rate limiting
├── auth/               Canonical signed-message formats
├── backend/            Backend client + router + circuit breaker
│   ├── docker/         Per-lease actor model, container lifecycle
│   └── shared/         Cross-backend primitives (callbacks, bbolt, registry)
├── chain/              gRPC client + WebSocket subscriber + signer
├── config/             YAML loading + validation
├── hmacauth/           HMAC-SHA256 signing
├── metrics/            Prometheus definitions
├── provisioner/        Watermill event routing + reconciler
│   ├── payload/        bbolt payload store
│   └── placement/      bbolt lease→backend store
├── scheduler/          Periodic withdrawal + credit monitoring
├── testutil/           Shared test fixtures
├── util/               Shared utilities
└── watcher/            Cross-provider credit-depletion watcher

docs/
├── manifest-guide.md   Tenant-facing manifest schema
├── manifest-schema.json   Formal JSON Schema for the manifest
└── tenant-quickstart.md   End-to-end tenant flow
```

Every package has a `doc.go` (or a leading package comment) that describes its purpose and key types. Read those first.

---

## Testing

### Unit tests

```bash
make test                       # all unit tests (no -short, so the stress suite runs too)
go test -short ./...            # unit tests only, skipping the stress suite
go test ./internal/api/         # specific package
go test -run TestAuthToken ./...  # specific test by name
```

### Race detector

**Run the full unit suite under `-race -short` regularly.** Stress tests OOM under `-race` because they allocate large fixtures; they self-skip via `testing.Short()`.

```bash
go test -race -short ./...
```

**CI does not run `-race`** (it is omitted in `.github/workflows/ci.yml` due to memory pressure on GitHub-hosted runners). Locals are the only place this runs, so make a habit of `go test -race -short ./...` before pushing — especially on PRs that touch concurrency.

### Integration tests

Integration tests require a running Docker daemon and use the `integration` build tag.

```bash
make test-integration              # full Docker integration suite (now also sweeps the slower retain/restore tests; ~15-25 min — override the ceiling with `INTEGRATION_TIMEOUT=30m`)
make test-integration-stack        # stack/compose-based provisions
make test-integration-restart-update   # restart, update, release-history flows
make test-integration-k3s          # k3s-backend integration tests (self-builds the binary)
sudo make test-integration-volume  # filesystem quota tests (root + btrfs-progs)
```

Volume tests need root because they create btrfs subvolumes and enable btrfs quotas (the integration suite runs against a btrfs loopback; the xfs project-quota path is a separate, non-integration code path).

**CI runs these suites** via [`.github/workflows/integration.yml`](.github/workflows/integration.yml) on a privileged `ubuntu-latest` runner (root + a btrfs loopback + Docker): the full docker package suite (`make test-integration`, which `-run Integration` sweeps — core lifecycle, stack, restart/update, reconciler, idempotency, volume/quota, and retain/restore) plus `make test-integration-k3s`. It triggers on PRs/pushes touching the docker backend (`internal/backend/docker/**`, `internal/backend/shared/**`, `cmd/k3s-backend/**`, `Makefile`, `go.mod`/`go.sum`) and runs nightly as a safety net. Crucially, the job **fails — it does not pass green — if the privileged environment is missing**: a guard turns any `t.Skip` into a red build, because a silently-skipped run is exactly how a volume-naming change rotted these tests undetected for ~3 months (ENG-330). The regular `ci.yml` still only does `build`, `test` (without `-race`, no `integration` tag), `lint`, and `vulncheck`.

Running the suites locally is still the fastest iteration loop, and required for changes outside the path filter.

> **Convention:** an integration test that needs a missing prerequisite (root, btrfs, a loop device, Docker) must gate via `t.Skip`/`t.Skipf` — never a bare early `return`. The CI guard detects a misprovisioned runner by looking for `--- SKIP:` in the test output; a test that silently returns instead of skipping would slip past it and reopen the ENG-330 gap.

### Stress tests

Stress tests self-skip under `testing.Short()`, so `go test -short ./...` and the CI `test` job skip them — but a plain `make test` (which does **not** pass `-short`) runs them. The largest ones (500K, 1M events) additionally require `STRESS_TEST_LARGE=1`:

```bash
go test -v -run "StressTest|SustainedLoad" ./internal/provisioner/ -timeout 10m
STRESS_TEST_LARGE=1 go test -v -run TestManager_StressTest_1M ./internal/provisioner/ -timeout 15m
```

See [PERFORMANCE.md](PERFORMANCE.md) for the methodology and reference numbers.

### Coverage

```bash
make test-coverage          # generates coverage.html (open it manually); runs with -tags integration, so it needs a Docker daemon
sudo make test-coverage-all # includes volume tests
```

There is no enforced coverage threshold, but PRs that add code should add tests. Ratchet up coverage when touching a poorly-covered area.

### Benchmarks

```bash
go test -bench=. ./internal/provisioner/ -benchtime=1s
go test -bench=BenchmarkManager_EndToEnd ./internal/provisioner/
```

Benchmark files are listed in [PERFORMANCE.md](PERFORMANCE.md#benchmark-files).

---

## Code style

### Formatting

```bash
make fmt    # runs `go fmt ./...`
```

`make fmt` only runs `go fmt`. Import ordering (with `github.com/manifest-network/fred` grouped separately from third-party dependencies) is enforced by `goimports`, which is configured as a formatter in `.golangci.yml` and runs as part of `make lint` and the CI lint job. So the local fast loop is `make fmt && make lint`; CI catches the import grouping there even if you skip it locally.

### Linting

```bash
make lint
```

This runs `go vet` plus `golangci-lint` if installed. The `.golangci.yml` enables: `errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`, `gocritic`, `misspell`, `unconvert`, `unparam`, `nilerr`, `errorlint`, `exhaustruct`, `gosec` (the last scoped by rule-wide excludes such as G115/G104 and per-path suppressions). Test files and `cmd/` are excluded from some strict checks (see `.golangci.yml` for the rules). `exhaustruct` runs in directive-only mode — it checks only struct literals explicitly marked `//exhaustruct:enforce`.

Install golangci-lint. CI pins **v2.9.0** (`.github/workflows/ci.yml`), and the config uses the v2 schema, so match that version locally to avoid local-vs-CI drift:

```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v2.9.0
```

### Conventions used in this codebase

- **Interfaces are defined where they're consumed**, not where they're implemented. So `BackendRouter` lives in `internal/provisioner/interfaces.go`, not in `internal/backend/`. This keeps consumer packages testable without circular imports.
- **`safeGo()` for long-lived goroutines** — wraps in `recover()` so panics surface as errors instead of crashing the daemon. Increment `fred_background_goroutine_panics_total{component=...}` when panicking; the recovery should never be silent.
- **`cmp.Or` for runtime defaults** — used in several places (e.g. `internal/backend/client.go`, `cmd/mock-backend/main.go`) to fold in zero-value defaults at use sites. Not enforced project-wide; explicit zero checks are also fine.
- **Structured logging** — `slog` with consistent field names (`lease_uuid`, `tenant`, `backend`, `error`). Don't `fmt.Sprintf` into log messages; use key-value fields so logs are queryable.
- **Errors at boundaries, panics never** — return errors from public APIs. `recover()` is for actor handlers and long-lived goroutines only, where a single panic must not take out the process.
- **No tests in production code paths** — `internal/backend/chain.MockClient` lives outside the production build (issue context: PR #81). Test mocks go in `chaintest/` or alongside the test files using build tags.

---

## Adding a new package

If you add a new package under `internal/`, include a `doc.go` summarizing:

1. What the package is for (one-paragraph overview)
2. The key types and what they do
3. How it interacts with the rest of the codebase

Look at `internal/scheduler/doc.go` or `internal/backend/docker/doc.go` for the shape.

---

## Adding a new metric

1. Define the metric in `internal/metrics/metrics.go` (or `internal/backend/docker/metrics.go` for docker-backend metrics).
2. Use the `fred_` namespace and an appropriate subsystem.
3. Document the metric in [ARCHITECTURE.md § Metrics](ARCHITECTURE.md#metrics-prometheus). Include the labels and a one-sentence description that says when the value is interesting (e.g. "any non-zero is a bug" or "spikes correlate with X").
4. If the metric is intended to drive an alert, mention it in [OPERATIONS.md § Common alerts](OPERATIONS.md#common-alerts-and-what-they-mean).

---

## Adding an API endpoint

1. Add a handler in `internal/api/handlers.go` (or a focused file if the handler is long).
2. Register the route in `internal/api/server.go` using the existing `withAuthRL` wrapper (most authenticated tenant endpoints) or `withPayloadRL` (the `/data` upload). These wrappers apply per-tenant rate limiting plus the appropriate token validator (`tenantRateLimiter.AuthMiddleware()` or `tenantRateLimiter.PayloadAuthMiddleware()`).
3. Decide whether replay protection applies — it should be **on** for any endpoint that mutates state or returns sensitive details, **off** for idempotent reads. The decision matrix is in [SECURITY.md](SECURITY.md#which-endpoints-check-replay).
4. Document the endpoint in [README.md § API Endpoints](README.md#api-endpoints) — include path, method, auth, replay flag, request shape, response shape, and all status codes.
5. Add a handler test in `internal/api/server_handler_test.go` and an integration test if the path is non-trivial.

---

## Adding a backend operation

Backend operations involve three layers:

1. **`internal/backend/client.go`** — Fred's HTTP client. Define request/response structs, add a method on `HTTPClient`. Wrap with the circuit breaker.
2. **`cmd/docker-backend/main.go`** (and `cmd/mock-backend/main.go`) — the actual HTTP handlers. Apply the HMAC auth middleware.
3. **`internal/backend/docker/`** — the implementation. New mutating operations should typically be a new lease-actor message type with corresponding state-machine triggers (see `internal/backend/shared/leasesm/lease_sm.go`, adapted into the docker backend via `leasesm_adapters.go`).

Update [BACKEND_GUIDE.md](BACKEND_GUIDE.md) to document the new endpoint for third-party backend implementers.

---

## Pull requests

### Before opening a PR

- [ ] `make fmt && make lint && make test` pass cleanly
- [ ] `go test -race -short ./...` passes
- [ ] If touching the docker-backend, `sudo -E env "PATH=$PATH" make test-integration` passes locally (the `-E env "PATH=$PATH"` keeps `sudo`'s `secure_path` from hiding the Go toolchain — a plain `sudo make` can fail with `go: command not found`; requires root + Docker + btrfs-progs). CI runs the same invocation via `integration.yml`, but local is the fast loop and the only signal for changes outside that workflow's path filter
- [ ] New code has tests
- [ ] Public APIs / config / metrics that are user-visible are documented in the relevant `.md`
- [ ] Commit messages are descriptive (we follow [Conventional Commits](https://www.conventionalcommits.org/) loosely: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`)

### What to include in the PR description

- **What changed and why.** The "why" is harder to derive later than the "what".
- **Anything operators should know** — config changes, metric additions, backwards-incompatible changes.
- **Test coverage** — what's tested, what's deliberately not tested, and why.

### Review

PRs go through code review. Expect comments on test coverage, error handling, and consistency with surrounding patterns. The maintainers prioritize:

1. **Correctness under concurrency.** Many bugs in this codebase are actor / goroutine / timeout races. New concurrency must come with a test that demonstrates the invariant under load (`-race -short` is the floor).
2. **Failure modes.** What happens when chain is unreachable? When a backend times out? When a bbolt write fails? Default to fail-closed for security-sensitive paths and fail-open for non-critical paths, but be explicit about which.
3. **Documentation.** If a future operator can't tell from your PR what knob to turn, it isn't done yet.

---

## Releases

Releases are tagged on GitHub. CI builds binaries via `goreleaser` and pushes Docker images.

User-visible changes are tracked in the in-tree [CHANGELOG.md](CHANGELOG.md), which follows [Keep a Changelog](https://keepachangelog.com/). Add your change to the `## [Unreleased]` section under the appropriate heading (Added, Changed, Deprecated, Removed, Fixed, Security) as part of the PR that introduces it. On release, the maintainers stamp `## [Unreleased]` to the new version with the release date and open a fresh empty `## [Unreleased]` above it.

If you're contributing to a release, the maintainers will tag and publish. If you maintain a fork, follow the existing tag convention (`vMAJOR.MINOR.PATCH`) so `goreleaser` recognizes it.

---

## Reporting issues

- **Bug reports**: include Fred version (`providerd --version`), config (redacted secrets), relevant logs (`log_level: debug` ideal), and a minimal reproduction if possible.
- **Security issues**: do not file public issues. Email the maintainers (see `go.mod`'s module path for the org).
- **Feature requests**: describe the use case before the proposed implementation. The maintainers may have context on why something works the way it does.

---

## Useful references

- [README.md](README.md) — overview and config reference
- [ARCHITECTURE.md](ARCHITECTURE.md) — design decisions, event flow, observability
- [SECURITY.md](SECURITY.md) — auth, replay protection, hardening
- [BACKEND_GUIDE.md](BACKEND_GUIDE.md) — third-party backend implementers
- [OPERATIONS.md](OPERATIONS.md) — runbook for operators
- [DEPLOYMENT.md](DEPLOYMENT.md) — production deployment
- [PERFORMANCE.md](PERFORMANCE.md) — benchmarks and tuning
- [docs/manifest-guide.md](docs/manifest-guide.md) — tenant manifest schema
- [docs/tenant-quickstart.md](docs/tenant-quickstart.md) — tenant API walkthrough
