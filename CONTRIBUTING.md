# Contributing to Fred

Thanks for your interest in contributing. This guide covers getting a working dev environment, running the test suites, code style, and the PR workflow.

For an overview of what Fred does and how it's structured, start with [README.md](README.md) and [ARCHITECTURE.md](ARCHITECTURE.md). For the security model, see [SECURITY.md](SECURITY.md).

---

## Prerequisites

- **Go 1.26.11** (per `go.mod`) — the `go 1.26.11` directive sets the toolchain floor. Fred also uses `sync.WaitGroup.Go()` (Go 1.25) and `testing.B.Loop()` (Go 1.24).
- **Docker Engine 28.1+ (API 1.49+)** with iptables enabled — required for `make test-integration` and the docker-backend's immutable image admission.
- **(Optional) `manifestd`** — only needed if you want to run end-to-end against a local chain via `scripts/dev-init.sh`.
- **`golangci-lint`**, at the version pinned in `.golangci-lint-version` — required by `make lint`, which now fails rather than skipping when it's absent or mismatched. See [Linting](#linting) for the install command.
- **(Optional) root, `xfsprogs` and `capsh` (`libcap2-bin`)** — for the privileged integration tests, which exercise filesystem quotas. The full `make test-integration` needs all of them; `make test-integration-volume` and `make test-integration-restore` do not need `capsh`.

---

## Getting started

```bash
git clone https://github.com/manifest-network/fred.git
cd fred
go mod download
make all              # builds the three daemons and both offline placement tools into ./build/
make test             # all unit tests (runs the stress suite too; see Stress tests)
```

For an end-to-end local environment against a running chain, follow [Local Development Setup](README.md#local-development-setup). `scripts/dev-init.sh` registers a provider and SKUs, generates configuration, and prints the complete startup procedure. A fresh deployment must first seal the backend's storage identity and initialize placement authority with `placement-preflight -initialize-fresh` while mutation ingress is fenced and drained. Only then can `providerd` start. Normal startup deliberately does not create placement authority; initialization is a one-time operation, not a restart command.

`scripts/dev-init.sh` is parameterized via environment variables (see the script header). All defaults assume a local chain at `http://localhost:26657` with the `acc0` test key.

To exercise the provisioner without Docker, follow [Local Fred Development with the Mock Backend](README.md#local-fred-development-with-the-mock-backend). That recipe persists `MOCK_BACKEND_STORAGE_ID`, starts the mock, initializes fresh placement authority, and starts `providerd` with the matching configuration. Reuse the stored identity on mock restarts. The mock ignores SKUs and simulates provisioning.

---

## Project layout

```
cmd/
├── providerd/          Main daemon (chain ↔ backends, tenant API)
├── docker-backend/     Production Docker backend
├── mock-backend/       Test/dev backend
├── placement-preflight/ Offline inventory proof and placement initialization/upgrade
├── placement-repair/   Offline placement-authority repair
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

test/
└── e2e/                Black-box suite: the built binaries as separate processes

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

CI also runs the short suite with `-coverprofile=coverage.out`, reports aggregate and per-package statement coverage, and checks the measured 76.0% regression floor in `.coverage-threshold`. Reproduce the report with `python3 scripts/check-coverage.py coverage.out`; do not exclude packages or lower the floor to hide a regression. Schema/runtime boundary tests run with the Go suite and require no separate Python dependencies.

PR CI checks GoReleaser configuration and builds snapshot binaries, archives and the container image without publishing or signing. Tagged releases must point to a commit in `main` history, then pass the same full CI and integration workflows on that exact commit before publishing. Repository tag-creation and release permissions must remain restricted to trusted maintainers.

### Formal models

The [Quint suite](models/quint/README.md) tracks the lifecycle, acknowledgment,
retention, routing, and billing models used to investigate race conditions.
Install its pinned tool with `npm ci --prefix models/quint --ignore-scripts`, then
run `make test-models`. The suite checks both invariants and expected historical
counterexamples. Models of proposed fixes are labeled separately from shipped
behavior; sampled traces supplement Go regression tests, not replace them.

### Race detector

**Run the full unit suite under `-race -short` regularly.** Stress tests OOM under `-race` because they allocate large fixtures; they self-skip via `testing.Short()`.

```bash
go test -race -short ./...
```

CI runs three race shards on every PR and on every push to `main` or a `release/**` branch: provisioner packages, backend packages, and all remaining packages. The partition comes from `go list ./...`, so newly extracted helper packages retain coverage. Each uses `-race -short -p 1`, `GOMAXPROCS=2`, and a bounded timeout to limit runner pressure. See [the race matrix](.github/workflows/ci.yml) for the exact commands. Keep running the full command above locally before pushing concurrency changes; large stress fixtures remain outside these CI shards.

### Multi-backend fleet tests

`internal/provisioner/fleet_harness_test.go` stands up N real `httptest` backends,
each behind a real `backend.HTTPClient` (real HMAC, real circuit breaker) in a real
`backend.Router`, and lets a test fault any single one mid-run — connection reset,
hang past the timeout, 5xx, malformed JSON, oversize body, mid-pagination failure,
or slow-but-successful. Every other reconciler test drives in-process interface
mocks, so this is the only place the fred→backend transport is exercised at the
reconciler tier, and that transport is where "a backend failed to answer this
sweep" is produced.

```bash
go test ./internal/provisioner/ -run TestFleet_
```

**It carries no build tag on purpose.** It needs no Docker, no root and no network
beyond loopback, so it belongs in the ordinary `go test -short ./...` job, where the
whole suite runs in well under a second on every PR. Behind the `integration` tag it
would instead ride the privileged root+XFS+Docker workflow — tens of minutes for a
signal you want on every push, and gated on an environment none of these tests need.

The scenarios in `fleet_characterization_test.go` are characterization tests: they
pin what a sweep must **not** destroy when it cannot see the whole fleet. If you
change reconcile behaviour and one of them goes red, the default assumption is a
regression, not a test that needs updating.

### Integration tests

Integration tests require a running Docker Engine 28.1+ daemon and use the `integration` build tag. CI pins Docker Engine 29.7.2 on Ubuntu 24.04 and runs the suite once per image store: the classic `overlay2` store every deployed host uses, and the containerd image store, which exercises immutable platform selection and preparation of a platform manifest that is not yet independently addressable. Each leg declares its store in `FRED_TEST_IMAGE_STORE` (`overlay2` or `containerd`), and `TestIntegration_Docker_ImageStoreMatchesRunnerDeclaration` fails if the daemon disagrees; it skips when the variable is unset. The containerd leg also sets `FRED_TEST_IMAGE_DATA_PATH=/var/lib/containerd`, because the containerd store requires an explicit `image_data_path`; leave it unset for overlay2. The suite connects to the local system socket (`/var/run/docker.sock`); selecting another Docker CLI context does not redirect those SDK fixtures.

```bash
sudo -E env "PATH=$PATH" make test-integration INTEGRATION_TIMEOUT=30m  # full Docker integration suite (also sweeps the slower retain/restore tests; ~15-25 min, so raise the 15m default ceiling)
make test-integration-stack        # stack/compose-based provisions
make test-integration-restart-update   # restart, update, release-history flows
sudo -E env "PATH=$PATH" make test-integration-volume  # provision-path volume subset on an XFS loopback (root + xfsprogs)
```

Volume tests need root because they create loopback filesystems and set filesystem quotas. The full suite covers XFS project quotas, and needs every tool listed under [Prerequisites](#prerequisites); `make test-integration-volume` selects only the provision-path volume tests, which run on an XFS loopback with project quotas. Run them through `sudo -E env "PATH=$PATH"`: plain `sudo` resets `PATH` (`secure_path`) and can fail with `go: command not found`. A test whose prerequisite is missing skips rather than fails.

**CI runs these suites** via [`.github/workflows/integration.yml`](.github/workflows/integration.yml) on privileged `ubuntu-24.04` runners (root, Docker, loop devices, and the XFS tools), one per image store (`overlay2` and `containerd`): the full docker package suite (`make test-integration`, which `-run Integration` sweeps — core lifecycle, stack, restart/update, reconciler, idempotency, volume/quota, and retain/restore). The workflow triggers on PRs, and on pushes to `main` or a `release/**` branch, touching runtime code (`internal/**`, `cmd/**`), the black-box suite (`test/**`), `Makefile`, `go.mod`/`go.sum`, or the integration workflow itself; markdown-only changes are excluded. It also runs nightly as a safety net. Crucially, the job **fails — it does not pass green — if the privileged environment is missing**: a guard turns any `t.Skip` into a red build, because a silently-skipped run is exactly how a volume-naming change rotted these tests undetected for ~3 months (ENG-330). The regular `ci.yml` runs build, short unit tests, the bounded race shards, lint, action-pin validation, and vulnerability scanning; it does not use the `integration` tag.

Running the suites locally is still the fastest iteration loop, and required for changes outside the path filter.

> **Convention:** an integration test that needs a missing prerequisite (root, the filesystem tools, a loop device, Docker) must gate via `t.Skip`/`t.Skipf` — never a bare early `return`. The CI guard detects a misprovisioned runner by looking for `--- SKIP:` in the test output; a test that silently returns instead of skipping would slip past it and reopen the ENG-330 gap.

### Black-box tests

`test/e2e` runs the built binaries as separate processes and asserts only through their external surfaces: the backend HTTP API, Docker, and the host filesystem. It has two tiers:

- **Backend tier** (`test/e2e`): each test starts its own `docker-backend`, as a non-root account holding the production capabilities, on its own XFS loopback, behind mTLS with a throwaway CA, and drives it directly.
- **providerd tier** (`test/e2e/chain`): each test starts a single-node chain from the real `manifestd` binary, one or more backends, and a `providerd` bootstrapped as an operator does it. The test acts as a tenant: transactions on the chain, and the manifest, restarts, updates and restores through the provider's API with tokens from the `lease-token` binary. A test that must show providerd left something alone waits on providerd's own counters at `/metrics`.

```bash
make test-e2e   # needs sudo, Docker, xfsprogs and capsh; set FRED_TEST_IMAGE_DATA_PATH as for the integration suite
MANIFESTD=/path/to/manifestd make test-e2e   # also runs the providerd tier
```

Run it as yourself. The target builds the binaries and the test binaries unprivileged, then runs only the test binaries under `sudo`, so no root-owned file lands in the Go caches. Process logs go to `build/e2e/logs/`.

A `docker-backend` needs a Docker daemon to itself, as in production: it refuses to work beside another backend's containers. The two-backend tests of the providerd tier therefore need a second daemon, and skip without one:

```bash
sudo scripts/e2e-second-dockerd.sh start   # prints its socket; `stop` removes it and its data
FRED_E2E_DOCKER_SOCKETS=/var/run/docker.sock,/run/fred-e2e-docker-2/docker.sock MANIFESTD=/path/to/manifestd make test-e2e
```

The second daemon takes the system daemon's image store settings, has no default bridge, and writes no firewall rules, so the two share nothing.

The providerd tier skips unless `MANIFESTD` names a `manifestd` binary. `.manifestd-version` pins what CI installs, one artifact per line as `<name> <release tag> <sha256>`: the manifest-ledger release tarball, and the `libwasmvm` shared library its binary links against and does not ship. Use the same release locally, with that library on the loader path.

- The suite uses the `integration` build tag and no `-run` filter, so every test in the package runs. A test whose prerequisite is missing skips, and CI fails on any skip.
- Run one e2e test binary at a time per Docker daemon; `make test-e2e` runs them in turn. `TestMain` first removes what a killed run left behind: the containers and networks of every backend named `e2e-*` on every daemon in `FRED_E2E_DOCKER_SOCKETS`, and every mount under `$TMPDIR/fred-e2e`.
- Workload images are pinned by digest (the constants in `test/e2e/docker.go`). Never use `:latest` or another floating tag; when a pin changes, change the pre-pull list in `integration.yml` with it.
- Every image comes from `ghcr.io/manifest-network/fred-e2e`, the Docker Official Images (busybox, redis) included: `test/e2e/images/` republishes them unchanged. A backend pulls without credentials, once per image for every test, and Docker Hub and ECR Public both refuse a client that asks that often.
- A test that needs an image no public one provides gets one built from a directory under `test/e2e/images/` and published to `ghcr.io/manifest-network/fred-e2e` by `e2e-images.yml`. See `test/e2e/images/README.md`.
- The harness is one fixture (`Backend`) and one client (`Client`: `Apply`, `Get`, `List`, `Close`, `Restore`). The providerd tier adds `Provider`, built on that fixture, and `Tenant`. A new feature adds a test, not harness code, and production code carries no hook for the suite.

#### Proving a regression test

A regression test is worth keeping only if it fails without the fix it guards. The tenant regression tests in `test/e2e/tenant_test.go` were each shown to, and each names its guard. To repeat the proof after changing a guard, build a backend without it and point the one test at that binary:

```bash
cp internal/backend/docker/provision.go /tmp/provision.go    # then remove the guard in the copy
echo "{\"Replace\": {\"$PWD/internal/backend/docker/provision.go\": \"/tmp/provision.go\"}}" > /tmp/overlay.json
mkdir -p /tmp/unguarded && go build -overlay /tmp/overlay.json -o /tmp/unguarded/docker-backend ./cmd/docker-backend
make build-e2e
sudo env "PATH=$PATH" FRED_E2E_BACKEND_BIN_DIR=/tmp/unguarded build/e2e/tests/e2e.test -test.v -test.run '^TestBackend_SymlinkedVolumeDirectoryIsRefused$'
```

The test must fail, and for the reason its comment gives. `go build -overlay` replaces a whole file for one build and leaves the tree untouched. Nothing in CI repeats this, because a copied file goes stale as soon as the original changes: it is a check to make when the guard or its test changes.

CI runs it in `integration.yml` as the `e2e (root, <image store>)` jobs, beside the in-process suite, and the release workflow gates on both. Changes under `test/**` or to `.manifestd-version` trigger that workflow.

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
sudo -E env "PATH=$PATH" make test-coverage-all # includes volume tests
```

CI enforces the measured 76.0% aggregate statement-coverage floor in `.coverage-threshold`. PRs that add code should add tests. Ratchet up coverage when touching a poorly-covered area.

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

`make fmt` only runs `go fmt`. Import ordering (with `github.com/manifest-network/fred` grouped separately from third-party dependencies) is enforced by `goimports`, which is configured as a formatter in `.golangci.yml` and runs as part of `make lint` and the CI lint job. So the local fast loop is `make fmt && make lint` — and since `make lint` no longer skips golangci-lint silently, that loop now catches import grouping before CI does.

### Linting

```bash
make lint
```

This runs `go vet` plus `golangci-lint`. **`make lint` fails if `golangci-lint` is missing from `PATH` or is not the pinned version** — it used to skip silently and exit 0, which made a local pass meaningless. The `.golangci.yml` enables: `errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`, `gocritic`, `misspell`, `unconvert`, `unparam`, `nilerr`, `errorlint`, `exhaustruct`, `gosec` (the last scoped by rule-wide excludes such as G115/G104 and per-path suppressions). Test files and `cmd/` are excluded from some strict checks (see `.golangci.yml` for the rules). `exhaustruct` runs in directive-only mode — it checks only struct literals explicitly marked `//exhaustruct:enforce`.

The pinned version lives in **`.golangci-lint-version`** — the single source of truth, read by both the `lint` make target and the CI/release workflows. Install exactly that version (upstream recommends the binary installer over `go install`, which compiles against your local toolchain):

```bash
curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b "$(go env GOPATH)/bin" "$(cat .golangci-lint-version)"
```

Make sure `$(go env GOPATH)/bin` is on your `PATH`. Matching the pin matters more than it looks: `.golangci.yml` uses the v2 schema, and a **v1** binary does not reject it — it ignores the v2-only keys and quietly runs a different linter set, so you get a green local run and a red CI. To bump the linter, edit `.golangci-lint-version`; nothing else hardcodes a version.

### Conventions used in this codebase

- **Interfaces are defined where they're consumed**, not where they're implemented. So `BackendRouter` lives in `internal/provisioner/interfaces.go`, not in `internal/backend/`. This keeps consumer packages testable without circular imports.
- **Top-level providerd components run under the `components` supervisor** (`cmd/providerd/components.go`). A panic, a non-cancellation error, or a return before shutdown is a recorded failure: it starts shutdown, and providerd exits non-zero. `TestProviderdGoroutinesStartThroughComponents` rejects every `go` statement and every call to a method named `Go` in `cmd/providerd` outside its reviewed functions; it reads syntax, so it cannot see a goroutine started inside a library call. Other long-lived goroutines wrap their loop in `recover()` and increment `fred_background_goroutine_panics_total{component=...}` when panicking; the recovery should never be silent.
- **`cmp.Or` for runtime defaults** — used in several places (e.g. `internal/backend/client.go`, `internal/api/server.go`) to fold in zero-value defaults at use sites. Not enforced project-wide; explicit zero checks are also fine.
- **Structured logging** — `slog` with consistent field names (`lease_uuid`, `tenant`, `backend`, `error`). Don't `fmt.Sprintf` into log messages; use key-value fields so logs are queryable.
- **Errors at boundaries, panics never** — return errors from public APIs. `recover()` is for actor handlers and long-lived goroutines only, where a single panic must not take out the process.
- **No test-only declarations in production files** — no nil-in-prod hook fields, no `testMode` flags, no accessors whose only callers are tests. Enforced by `internal/testutil/no_test_only_code_test.go`, which walks every non-`_test.go` file under `internal/` and `cmd/` and fails CI on a declaration whose name or doc comment advertises it as scaffolding (struct fields included). A build tag is **not** an escape hatch: the guard parses with `go/parser` and never evaluates `//go:build`. The remedies, in order of preference: move it to an in-package `export_test.go`; inject through a dependency interface production already uses; or split the function so a white-box test can drive the pieces in a controlled order. Chain mocks live in `internal/chain/chaintest/`, shared fixtures in `internal/testutil/fixtures.go`.

---

## Adding a new package

If you add a new package under `internal/`, include a `doc.go` summarizing:

1. What the package is for (one-paragraph overview)
2. The key types and what they do
3. How it interacts with the rest of the codebase

Look at `internal/scheduler/doc.go` or `internal/backend/docker/doc.go` for the shape.

---

## Adding a new metric

1. Define the metric in the package owned by the binary that writes it. **Which package is not a filing decision — it decides which binaries export the metric.** Collectors are created with `promauto`, which registers on the default registerer at *package init*, so every binary linking that package exports the metric whether or not it has a call site. An unlabelled one then sits at a permanent 0 on processes that can never write it, and a 0 satisfies ordinary gauge comparisons — that is ENG-712, where `fred_signer_pool_lane_count < 3` matched every docker-backend and missed the only host with a signer pool.

   - only providerd writes it → `internal/metrics/metrics.go`;
   - only one backend writes it → that backend's `metrics.go` (`internal/backend/docker/`), with its series bound in the same package's `series.go`;
   - more than one binary writes it, **or** it belongs to the `fred_background_*` panic family → `internal/metrics/background`, where it **must** carry labels: a `*Vec` registers just as eagerly but exports no series until a package binds a component, which is what makes it free for a binary that never runs one. A component's counter is a package-level variable (`background.CleanupPanics("name")`) of the package that runs it. Label-bearing is the membership rule, not "written by everyone" — the goroutine-panic family has providerd-only components and lives there deliberately, keeping the family whole so the next backend that needs a panic counter does not re-import `internal/metrics`;
   - shared code that must reference no collector at all → take one by injection, as `backend.RouterConfig.RoutingFallback` and `backend.HTTPClientOptions.RequestCounters` do, and wire it in `cmd/providerd`.

   **A labeled counter's series must exist at 0 before its first event**, or `increase()` misses that event after every restart (ENG-1116). Every counter vec is unexported, and every package that declares one has a vocabulary file — `series.go`, or `background.go` for the panic family — that is the only place its series are created:
   - a fixed call site increments a named field of a package-level handle struct (mark multi-line literals `//exhaustruct:enforce`);
   - a call site that labels its own series (a destroy site, a teardown operation) passes a value declared in the vocabulary that carries its handles;
   - a label computed from a domain value is selected by a total function from that value to a bound field, or from a set bound from the type's own complete list (`leasesm.Transitions()`, `failurecause.Labels()`); a new value of such a type then has its series without another edit;
   - a label known only from configuration (a backend, a lane, the volume backend's kind) is bound by a `Bind…`/`bind…` function in that component's constructor, and the handles are kept.

   A caller increments a handle and never names a label value, and a recording path only looks series up (`metrics.ForBackend` cannot create one). `internal/testutil`'s counter series guard fails a counter vec in a package with no vocabulary file, any child-series creation outside a vocabulary file, and a vocabulary's binder called outside a constructor or package initialization. A label set that is open by nature (an HTTP status) gets a `Count…` function in `series.go` and must not drive an `increase()` alert.

   A `depguard` rule blocks `internal/metrics` from `internal/backend/**` and the backend `cmd` packages, and each backend `cmd` package has a test asserting its `/metrics` carries only its own prefix. If either fires, the fix is one of the four options above, never an exclusion.
2. Use the `fred_` namespace and an appropriate subsystem.
3. Document the metric in [ARCHITECTURE.md § Metrics](ARCHITECTURE.md#metrics-prometheus). Include the labels and a one-sentence description that says when the value is interesting (e.g. "any non-zero is a bug" or "spikes correlate with X").
4. If the metric is intended to drive an alert, mention it in [OPERATIONS.md § Common alerts](OPERATIONS.md#common-alerts-and-what-they-mean).

---

## Adding an API endpoint

1. Add a handler in `internal/api/handlers.go` (or a focused file if the handler is long).
2. Register the route in `internal/api/server.go` using the existing `withAuthRL` wrapper (most authenticated tenant endpoints) or `withPayloadRL` (the `/data` upload). These wrappers apply per-tenant rate limiting plus the appropriate token validator (`tenantRateLimiter.AuthMiddleware()` or `tenantRateLimiter.PayloadAuthMiddleware()`).
3. Decide whether replay protection applies — it should be **on** for any endpoint that mutates state or returns sensitive details, **off** for idempotent reads. The decision matrix is in [SECURITY.md](SECURITY.md#replay-protection).
4. Document the endpoint in [README.md § API Endpoints](README.md#api-endpoints) — include path, method, auth, replay flag, request shape, response shape, and all status codes.
5. Add a handler test in `internal/api/server_handler_test.go` and an integration test if the path is non-trivial.

---

## Adding a backend operation

Backend operations involve three layers:

1. **`internal/backend/client.go`** — Fred's HTTP client. Define request/response structs, add a method on `HTTPClient`. Wrap with the circuit breaker.
2. **`cmd/docker-backend/main.go`** (and `cmd/mock-backend/main.go`) — the actual HTTP handlers. Apply the HMAC auth middleware.
3. **`internal/backend/docker/`** — the implementation. New mutating operations should typically be a new lease-actor message type with corresponding state-machine triggers (declare each new edge once in `leaseTransitions`, `internal/backend/shared/leasesm/transitions.go`, which also gives it its metric series; actions go in `lease_sm.go`; adapted into the docker backend via `leasesm_adapters.go`).

Update [BACKEND_GUIDE.md](BACKEND_GUIDE.md) to document the new endpoint for third-party backend implementers.

---

## Pull requests

### Before opening a PR

- [ ] `make fmt && make lint && make test` pass cleanly
- [ ] `go test -race -short ./...` passes
- [ ] If touching the docker-backend, `sudo -E env "PATH=$PATH" make test-integration INTEGRATION_TIMEOUT=30m` passes locally (the `-E env "PATH=$PATH"` keeps `sudo`'s `secure_path` from hiding the Go toolchain — a plain `sudo make` can fail with `go: command not found`; requires root, Docker, the filesystem tools under [Prerequisites](#prerequisites), and `FRED_TEST_IMAGE_STORE`). CI runs the same invocation via `integration.yml`, but local is the fast loop and the only signal for changes outside that workflow's path filter
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

A release is cut from `main` by a pull request that stamps `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md) with the version and date and opens a fresh empty `## [Unreleased]` above it. After it merges, a maintainer tags the merge commit. The tag runs `.github/workflows/release.yml`:

1. `provenance` refuses a tag whose commit is not in `main` history (`scripts/check-release-source.sh`). A branch that merely descends from `main` does not pass. It also refuses a tag that does not release the newest dated CHANGELOG section (`scripts/check-release-changelog.sh`): `vX.Y.Z` and `vX.Y.Z-rc.N` both require `## [X.Y.Z] - <date>` to be the first released section.
2. `verify`, `integration` and `secret-scan` rerun the full CI workflow (race, lint and govulncheck included), the privileged integration suites and gitleaks on the tagged commit. A green run of the same commit on `main` does not count.
3. `release` runs only after all three pass. GoReleaser builds the archives and the `ghcr.io/manifest-network/fred` image, generates SBOMs and signs with cosign.

Tag a release candidate (`vX.Y.Z-rc.N`) first. GoReleaser publishes it as a prerelease and pushes only the `X.Y.Z-rc.N` image tag, never `latest`, `X.Y` or `X`. Once the candidate is validated, tag `vX.Y.Z` on the same commit.

A `release/vX.Y` branch holds a released line once `main` has moved past it. A repository ruleset refuses its deletion and force-pushes to it. A push to it runs the same CI, integration and secret-scan workflows as a push to `main`. `provenance` still requires the tagged commit to be in `main` history, so it refuses a tag on a commit that exists only on a release branch.

User-visible changes are tracked in the in-tree [CHANGELOG.md](CHANGELOG.md), which follows [Keep a Changelog](https://keepachangelog.com/). Add your change to the `## [Unreleased]` section under the appropriate heading (Added, Changed, Deprecated, Removed, Fixed, Security) as part of the PR that introduces it. Never edit a released `## [X.Y.Z]` section: it records what that release shipped, even after later work changes the behavior it describes.

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
