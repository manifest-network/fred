# CLAUDE.md

## What this is

Fred is the Manifest Network **provider daemon**: it watches the chain for lease events and drives provisioning through pluggable backends. `providerd` and a backend are **separate processes** over HTTP, HMAC-SHA256-authenticated in both directions (`X-Fred-Signature`; canonical string = timestamp + method + request URI + sha256(body)). Fred → backend is a request; backend → Fred a callback. Changing one side's wire shape means changing the other **and** `BACKEND_GUIDE.md`.

`make all` builds four binaries; only `providerd` and `docker-backend` are released. `mock-backend` is for dev/E2E, and **`k3s-backend` is a non-functional scaffold (ENG-133)** — it serves the backend contract, but every provision returns `failed / "not implemented"` (`internal/backend/k3s/provision_stub.go`). `cmd/loadtest` and `cmd/lease-token` have no make target — `go run ./cmd/<name>`.

## Commands

```bash
make test                    # NOTE: no -short, so the stress suite runs
go test -short ./...         # what CI runs
go test -race -short ./...   # before pushing concurrency changes; CI does NOT run -race,
                             # and -race without -short OOM-kills the runner on the stress suite
make fmt                     # go fmt only — does NOT cover import grouping
make lint                    # go vet + golangci-lint
```

`make lint` **fails** unless golangci-lint is on `PATH` at the version in `.golangci-lint-version` (the single source of truth; the CI and release workflows read the same file). Match it exactly: a v1 binary does *not* reject the v2 `.golangci.yml` — it ignores the v2-only keys and quietly runs a different linter set. Import grouping (`github.com/manifest-network/fred` in its own block) comes from `goimports` *inside* golangci-lint, so `make fmt` alone never satisfies CI lint.

```bash
sudo -E env "PATH=$PATH" make test-integration INTEGRATION_TIMEOUT=30m  # full Docker suite, slow
sudo -E env "PATH=$PATH" make test-integration-volume   # btrfs-loopback subset, NOT the XFS/ZFS quota tests
make test-integration-k3s                               # self-builds the binary, no cluster needed
```

`sudo -E` alone does not restore `PATH` (`secure_path`) — hence the `env`. Both suites need root — without it the volume subset silently `t.Skip`s almost everything and still exits green. The full suite additionally needs Docker + btrfs-progs + xfsprogs + zfsutils, and the 15m default timeout is now too tight. It selects with `-run Integration`, so a `//go:build integration` test whose *name* lacks "Integration" never runs and never trips the SKIP guard — a filtered test emits neither `--- SKIP:` nor `--- PASS:`, so CI stays green. Name every new one `TestIntegration_…`.

Local stack: `bash scripts/dev-init.sh` (needs a running chain) registers provider + SKUs and writes `docker-backend.yaml` + `config.docker.yaml`. `make run-docker` picks up the former; `make run` is hardwired to the placeholder `config.example.yaml`, which dev-init never updates — start the daemon with `./build/providerd --config config.docker.yaml` instead.

## Architecture invariants

- **Chain is source of truth for leases; the backend is source of truth for provisions.** Never trust cached provision state — query it.
- **Level-triggered reconciliation, not event replay.** Events are an optimization; the reconciler (`internal/provisioner/reconciler.go`) periodically compares chain × backend state and applies `README.md#state-matrix`. A dropped event is a latency bug, not a correctness bug — check any "we might miss X" concern against what reconcile already fixes.
- **Two state machines run side by side.** Chain lease state (`PENDING/ACTIVE/CLOSED/REJECTED/EXPIRED`, plus a zero `UNSPECIFIED`) and Fred's provision status (`provisioning/ready/failing/failed/restarting/updating/deprovisioning/retained/unknown`). Both appear in API responses; only provision status is exported as a metric. Don't conflate them.
- **Routing is by exact SKU UUID** (`internal/backend/router.go`); on multiple matches new provisions go to the least-loaded. An **empty SKU list matches nothing** — designate a fallback with `default: true`. The **placement store** (bbolt, `internal/provisioner/placement`) maps `lease → backend` so reads and retained-lease restores reach the source machine even after close; it is optional, so consumers nil-guard it. Deprovision resolves the backend *positively* (placement, then the in-flight tracker), never by SKU — a guessed deprovision reports success while stranding the volume.
- **Lease actor model (docker-backend).** One goroutine per lease serializes state mutation; the SM lives in the substrate-agnostic `internal/backend/shared/leasesm`. The actor holds no backend pointer and mutates `ProvisionState` only via `LeaseProvisionStore`, with exactly two documented exceptions — read the ENG-229 invariant block on that interface (`leasesm/leasesm.go`) before adding a third. `Backend.provisions` guarded by `provisionsMu` is the deliberate single-writer substrate, **not** unfinished migration: new mutating operations are a new actor message + SM trigger, never a direct mutation.
- **The ResourcePool rebuild keys on ownership, not status.** `recoverState` preserves a reservation when a tracked lease owns the key; three PRs patched the old status allowlist (ENG-546/562/563) before ENG-567 replaced it. The `b.provisions` merge in that same function is a *different* decision and is deliberately status-keyed (ENG-414/ENG-603) — don't "fix" it to match.
- **Startup order is load-bearing** (API server → provision manager, blocking on `Running()` → one-shot `WithdrawOnce` → startup reconcile → everything else). Reconciliation triggers backend callbacks, so the API and Watermill handlers must already be live.
- **Work that outlives its call captures the component's lifecycle context** (`b.stopCtx`), never the caller's startup ctx — that one is canceled the moment `Start` returns. Using it squeezed migration health-waits into what was left of a 30s startup budget (against a 90s timeout) and fired the detached `-prev` grace cleanup at ~0s, silently leaking every migration's `-prev` containers (ENG-592).

## Conventions

- **Every `internal/` package carries a package doc** — a `doc.go` in about half, otherwise a leading comment on the package's main file. Read it before the code; add one for a new package.
- **`recover()` is for goroutine boundaries** — actor handlers, long-lived loops, per-item workers (reconciler per-lease/per-orphan, restore); ordinary call paths return errors. The two exceptions are narrow panic-class guards, not error handling: `chain.trySend` (send on a concurrently-closed subscriber channel) and the `sdkConfigOnce` block in `cmd/providerd/main.go`. That file also wraps its long-lived components in a local `safeGo()` (panic → error on `errChan`); most other recover sites bump a `*_panics_total` counter, and a new one should.
- **No test-only hooks in production code** — no nil-in-prod fields, flags, or injection points; use consumer-side interfaces or exported white-box accessors. Chain mocks live in `internal/chain/chaintest/`, shared fixtures in `internal/testutil/fixtures.go`.
- **`exhaustruct` is directive-only**, and its `//exhaustruct:enforce` literals sit in `internal/backend/docker/` while the struct they build, `leasesm.ProvisionState`, lives in `internal/backend/shared/leasesm` — adding a field there breaks lint in a package you never touched.
- **Integration tests `t.Skip` on a missing prerequisite, never bare-`return`.** CI fails the job on any `--- SKIP:` line (and on zero `--- PASS:`); a silent return reopens ENG-330, where these tests rotted undetected for months.
- **A new backend failure class needs a verdict in three places.** `IsSuccessful` (`internal/backend/client.go`) decides whether it trips the circuit breaker — expected business outcomes are exempt (404 not-provisioned, 400 validation, 409 already-provisioned/invalid-state, 503 insufficient-resources, 422 not-retained / demote-exceeds-tier). `handleProvisionError` (`internal/provisioner/reconciler.go`) decides whether it rejects or **closes the lease on-chain**; filing a transient error as permanent there is tenant-weaponizable (ENG-498). And the tenant-facing `backend.Reason` + curated message is **authored at the failure source** (`internal/backend/reason.go`), never derived from `err.Error()` — an unmapped path silently degrades to `Unknown` (ENG-508).
- **Manifest limits (ports, env vars, labels) are compile-time constants, not config knobs.** Cap the aggregate as well as the unit: list endpoints use keyset pagination, and `/logs` caps both per-container and per-call.
- **Retention and cleanup gates fail open** — refusing to retain and reaping an orphan both destroy data, so collapse uncertainty toward "keep" and count it in a metric. Admission gates fail closed. Either way, test the error branch.
- **User-visible changes go in `CHANGELOG.md`** in the same PR, under the matching `###` subsection of `## [Unreleased]` (Keep a Changelog: Added/Changed/Deprecated/Removed/Fixed/Security). Commits are Conventional Commits with a trailing `(#PR)`; add a scope when the change has one.
- **Process docs under `docs/superpowers/` are no longer committed** (the older ones in git predate the change). They and the ad-hoc `docs/` review notes are untracked but *not* gitignored, so `git add -A` sweeps them into your PR. A PR is code + tests + `CHANGELOG.md`.

## Where to look

| Question | File |
|---|---|
| Design decisions, event flow, metrics, security model | `ARCHITECTURE.md` |
| API + backend endpoint specs, reconciliation matrix | `README.md` |
| Config reference (per-key comments) | `config.example.yaml`, `docker-backend.example.yaml` |
| Code style, how to add a metric / endpoint / backend op | `CONTRIBUTING.md` |
| Implementing a third-party backend | `BACKEND_GUIDE.md` |
| Auth, replay-protection matrix, hardening | `SECURITY.md` |
| Why a path-rewriting proxy breaks callback HMAC | `docs/security-callback-auth.md` |
| Runbook, alerts | `OPERATIONS.md` |
| Install, host/filesystem requirements, systemd, TLS, upgrades | `DEPLOYMENT.md` |
| Tenant manifest schema | `docs/manifest-guide.md`, `docs/manifest-schema.json` |
| Docker backend internals + full SM transition diagram | `internal/backend/docker/README.md` |
