# Chain-driven Restore E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the soft-delete/restore lifecycle to fred's existing in-repo e2e tier (mock chain → real provisioner → real Docker), exercising the full provision→close→restore path in CI — and verifying **both** code paths by which a lease reaches `backend.Deprovision` (and therefore soft-delete).

**Architecture:** Reuse the docker-package integration harnesses (`testBackendWithRealDocker` + btrfs loopback + `chaintest.MockClient`). Two harnesses, one per deprovision route:

| Route | Trigger in production | How to drive it | Covers |
|---|---|---|---|
| **Reconciler** (`processOrphan` → `Deprovision`, reconciler.go:888/985) | lease vanishes from chain state | `testReconcilerSetup` + flip the mock lease invisible + `RunOnce` | **auto-close** (credit exhaustion) + missed-event backstop |
| **Event** (`processLeaseClose` → `Orchestrator.Deprovision` → `Deprovision`, handler_set.go:139-193) | `LeaseClosed` / `LeaseExpired` events | `provisioner.Manager` wired to the real backend + `PublishLeaseEvent` | explicit tenant **close** + **expiry** |

**Verified facts grounding this plan (do not re-derive):**
- The reconciler orphan path and the event path BOTH call `backend.Deprovision`, which honors `RetainOnClose` → soft-delete. So both routes must be exercised, since a regression could break one only.
- **Auto-close is the reconciler's job, not an event handler's.** `Manager.PublishLeaseEvent` routes only `LeaseCreated/Closed/Expired`; `LeaseAutoClosed` hits `default: return nil` (manager.go:331-343). The `watcher` only does credit/withdrawal bookkeeping for auto-close (watcher.go:119-130), it does **not** deprovision. So an auto-closed lease is deprovisioned only when the reconciler detects it as an orphan. The "lease vanishes → reconciler" scenario in Task 2 *is* the auto-close path.
- `processLeaseClose` does NOT require the lease to be Manager-tracked: it derives a SKU hint from `GetLease` and the orchestrator falls back to it (handler_set.go:172-193). So a lease provisioned directly via `b.Provision` can be closed by a published event. It also publishes a `ProvisionStatusRetained` tenant notice (handler_set.go:189) — the reconciler path does not.
- `docker-small` in `defaultTestSKUProfiles()` has `DiskMB:1024` → stateful (managed volume). `testBackendWithRealDocker` wires it + `b.Start(ctx)`; `retentionStore` is live when `RetainOnClose` + `RetentionDBPath` are set.

**Tech Stack:** Go, `//go:build integration`, real Docker + btrfs loopback (`setupBtrfsLoopback`, needs root + `btrfs-progs`), `chaintest.MockClient`, `provisioner.NewManager`/`NewReconciler`, testify.

**Idiom note (why this shape is correct):** real component wiring (`provisioner` + `*docker.Backend`) + an in-process mock chain (`chaintest.MockClient`, no CometBFT node) is exactly the Cosmos SDK ADR-059 *integration tier*, and matches how Akash's provider tests the scheduler/manager against a mocked chain boundary. `interchaintest` is for the protocol/IBC *e2e tier*, not this scope. The `//go:build integration` tag (not `e2e`) and in-package placement reusing the unexported docker helpers are idiomatic. In-package is **import-cycle-safe here**: `provisioner` depends on the `backend.Backend` *interface*, not the `internal/backend/docker` package, so a `package docker` test importing `provisioner` does not cycle — proven by the existing `testReconcilerSetup` and a `go vet -tags integration` of the new harness. (No `package docker_test` split needed.)

**Coverage boundary:** these harnesses deliberately collapse the production providerd↔docker-backend **HTTP seam** — prod wires the Manager to backends via `backend.NewHTTPClient` against the `cmd/docker-backend` process (cmd/providerd/main.go), whereas these tests inject a real in-process `*docker.Backend` into the router. So the `Provision`/`Deprovision`/`Restore` HTTP wire **and** the HMAC request auth (`signRequest`) are NOT exercised here; they stay unit-tested (`internal/api/handlers_test.go`, `internal/backend/client_test.go`, `internal/backend/docker/transport` mTLS tests). A future interchaintest tier would close that transport seam. Restore-from-a-retained-record is path-agnostic (identical `Deprovision`-produced record regardless of trigger), so the **full restore** is verified once (Task 2); Task 3 only adds that the *event* route reaches the same real soft-delete.

---

### Task 1: Make `testReconcilerSetup` accept extra backend config

**Files:** Modify `internal/backend/docker/integration_reconciler_test.go` (`testReconcilerSetup`, ~line 71)

- [ ] **Step 1: Add a variadic `extraCfg ...func(*Config)` applied after the defaults**

```go
func testReconcilerSetup(t *testing.T, chainClient *chaintest.MockClient, extraCfg ...func(*Config)) *reconcilerTestEnv {
	t.Helper()
	callbackServer, callbackCh := startCallbackServer(t)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ReconcileInterval = 2 * time.Second
		for _, fn := range extraCfg {
			fn(cfg)
		}
	})
	// ... rest unchanged ...
```

- [ ] **Step 2:** `go vet -tags integration ./internal/backend/docker/` → clean (the 6 existing call sites are unaffected).
- [ ] **Step 3:** Commit: `test(docker): allow extra backend config in testReconcilerSetup`

---

### Task 2: Reconciler-path lifecycle (covers auto-close + restore)

**Files:** Modify `internal/backend/docker/integration_reconciler_test.go` (append)

Drives the **reconciler route**: chain PENDING (stateful redis) → `RunOnce` provisions → sentinel → lease vanishes from chain (== auto-close / credit exhaustion) → `RunOnce` orphan-cleanup soft-deletes → `backend.Restore` → sentinel survives, record gone. This is the route that handles auto-close, so it doubles as the auto-close verification.

- [ ] **Step 1: Write `TestIntegration_Reconciler_RetainRestoreLifecycle`**

```go
func TestIntegration_Reconciler_RetainRestoreLifecycle(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	leaseUUID := fmt.Sprintf("recon-retain-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	const sku = "docker-small" // stateful: DiskMB=1024

	appManifest := manifest.Manifest{Image: "redis:7", Command: []string{"sleep", "3600"}}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)
	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseVisible := true // flip false to simulate the on-chain close/auto-close
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(_ context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseVisible {
				l := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				l.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{l}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(_ context.Context, _ string) ([]billingtypes.Lease, error) { return nil, nil },
		AcknowledgeLeasesFunc:         func(_ context.Context, _ []string) (uint64, []string, error) { return 1, []string{"tx"}, nil },
	}

	env := testReconcilerSetup(t, mockChain, func(cfg *Config) {
		cfg.VolumeDataPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
		cfg.RetainOnClose = true
		cfg.RetentionDBPath = filepath.Join(t.TempDir(), "retention.db")
		cfg.RetentionMaxAge = 0 // reaper OFF: assert soft-delete persists
		cfg.RetentionReapInterval = 0
	})
	require.True(t, env.tracker.store.Store(leaseUUID, payload))
	ctx := context.Background()

	// 1. Chain-driven provision.
	require.NoError(t, env.reconciler.RunOnce(ctx))
	require.Equal(t, backend.CallbackStatusSuccess, waitForCallback(t, env.callbackCh, leaseUUID, 3*time.Minute).Status)
	env.tracker.UntrackInFlight(leaseUUID)

	// 2. Sentinel into the managed volume.
	cid := getContainerID(t, leaseUUID)
	require.True(t, containerHasBindMount(t, cid, "/data"))
	execInContainer(t, cid, []string{"sh", "-c", "echo recon-restore-sentinel > /data/sentinel.txt"})

	// 3. Lease vanishes from chain (auto-close / credit exhaustion) → orphan cleanup soft-deletes.
	mu.Lock()
	leaseVisible = false
	mu.Unlock()
	require.NoError(t, env.reconciler.RunOnce(ctx))

	retainedPath := filepath.Join(mountPath, retainedName(canonicalVolumeName(leaseUUID, manifest.DefaultServiceName, 0)))
	require.Eventually(t, func() bool {
		rec, _ := env.backend.retentionStore.Get(leaseUUID)
		if rec == nil {
			return false
		}
		_, statErr := os.Stat(retainedPath)
		return statErr == nil
	}, 60*time.Second, 500*time.Millisecond, "reconciler orphan cleanup must soft-delete (retained volume + record)")
	data, err := os.ReadFile(filepath.Join(retainedPath, "data", "sentinel.txt"))
	require.NoError(t, err)
	assert.Contains(t, string(data), "recon-restore-sentinel")

	// 4. Restore into a NEW lease.
	newLease := fmt.Sprintf("recon-restore-new-%d", time.Now().UnixNano())
	require.NoError(t, env.backend.Restore(ctx, backend.RestoreRequest{
		LeaseUUID: newLease, FromLeaseUUID: leaseUUID, Tenant: tenant, ProviderUUID: env.providerUUID,
		Items:       []backend.LeaseItem{{SKU: sku, Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		CallbackURL: env.callbackURL,
	}))
	require.Equal(t, backend.CallbackStatusSuccess, waitForCallback(t, env.callbackCh, newLease, 3*time.Minute).Status)

	// 5. Sentinel survived; record gone.
	out := execInContainer(t, getContainerID(t, newLease), []string{"cat", "/data/sentinel.txt"})
	assert.Contains(t, out, "recon-restore-sentinel")
	rec, err := env.backend.retentionStore.Get(leaseUUID)
	require.NoError(t, err)
	assert.Nil(t, rec)

	env.backend.cfg.RetainOnClose = false
	require.NoError(t, env.backend.Deprovision(ctx, newLease))
}
```

- [ ] **Step 2: Callback-channel interaction.** The orphan `Deprovision` emits a `Deprovisioned` callback on `env.callbackCh`; `waitForCallback` filters by lease UUID so the `newLease` wait skips it. If the test hangs, drain it after the close `RunOnce` (`select { case <-env.callbackCh: case <-time.After(30*time.Second): }`). Add only if needed — do not add speculatively.
- [ ] **Step 3: Run** `sudo go test -tags integration -run TestIntegration_Reconciler_RetainRestoreLifecycle -timeout 10m -v ./internal/backend/docker/` → PASS. If the soft-delete assertion fails, the orphan path isn't honoring `RetainOnClose` — STOP and re-verify reconciler.go:888/985 reach `doDeprovision`'s retain branch.
- [ ] **Step 4:** Commit: `test(docker): reconciler-path retain/restore + auto-close e2e (ENG-325)`

---

### Task 3: Event route reaches a REAL soft-delete (Manager + real backend)

**Files:** Modify `internal/backend/docker/integration_reconciler_test.go` (add a `testManagerSetup` helper + the test). Requires adding the `os` and `github.com/manifest-network/fred/internal/chain` imports (Task 2 also uses `os`).

**What this adds that nothing else covers:** `provisioner/integration_test.go::TestIntegration_LeaseClosed_Deprovisions` already proves a `LeaseClosed` event routes to `Orchestrator.Deprovision` (against a *mock* backend). The *only* non-duplicative thing to prove here is that the event route reaches a **real** `docker.Backend` that performs the actual soft-delete (renamed `fred-retained-` volume on btrfs + retention record honoring `RetainOnClose`) — which a mock backend structurally cannot. So this test is framed around the real-soft-delete assertion, not re-proving routing. It exercises the **SKU-hint / default-route** close path (orchestrator Case 2) — the lease is provisioned directly via `b.Provision`, so it is not in-flight-tracked and no placement is recorded, which mirrors a real post-restart close of an already-active lease. `LeaseExpired` is intentionally NOT a second btrfs row: `HandleLeaseExpired` calls `processLeaseClose` *verbatim* (handler_set.go:126-136, differing only in the metrics topic label), so a second real-Docker run would double the most expensive test in the suite to guard a divergence the code structurally cannot have.

- [ ] **Step 1: Add the Manager harness helper**

```go
// testManagerSetup wires a real provisioner.Manager to the real docker backend +
// mock chain, for exercising the event-driven close/expiry deprovision path.
//
// NOTE: this deliberately collapses the production transport seam — prod wires the
// Manager to backends via backend.NewHTTPClient against the docker-backend process
// (cmd/providerd/main.go), whereas this injects the in-process *docker.Backend
// directly. It exercises the provisioner→backend LIFECYCLE logic, not the HTTP wire
// or HMAC auth (those are unit-tested separately).
func testManagerSetup(t *testing.T, mockChain *chaintest.MockClient, extraCfg ...func(*Config)) (*provisioner.Manager, *Backend, <-chan backend.CallbackPayload, string) {
	t.Helper()
	callbackServer, callbackCh := startCallbackServer(t)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		for _, fn := range extraCfg {
			fn(cfg)
		}
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: b, IsDefault: true}},
	})
	require.NoError(t, err)
	mgr, err := provisioner.NewManager(provisioner.ManagerConfig{
		ProviderUUID:    "test-provider",
		CallbackBaseURL: callbackServer.URL,
	}, router, mockChain)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); _ = mgr.Close() })
	go func() { _ = mgr.Start(ctx) }()
	select {
	case <-mgr.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for manager to start")
	}
	return mgr, b, callbackCh, callbackServer.URL
}
```

- [ ] **Step 2: Write the test**

```go
func TestIntegration_Manager_CloseEvent_RealSoftDelete(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	leaseUUID := fmt.Sprintf("mgr-close-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	const sku = "docker-small"
	const providerUUID = "test-provider"

	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			return chaintest.NewMockLeaseWithSKU(uuid, tenant, providerUUID, billingtypes.LEASE_STATE_ACTIVE, sku), nil
		},
	}
	mgr, b, callbackCh, callbackURL := testManagerSetup(t, mockChain, func(cfg *Config) {
		cfg.VolumeDataPath = mountPath // REQUIRED: empty would zero docker-small's DiskMB → stateless → nothing to retain
		cfg.VolumeFilesystem = "btrfs"
		cfg.RetainOnClose = true
		cfg.RetentionDBPath = filepath.Join(t.TempDir(), "retention.db")
		cfg.RetentionMaxAge = 0
		cfg.RetentionReapInterval = 0
	})
	ctx := context.Background()

	// Provision directly on the backend. This exercises the SKU-hint / default-route
	// close path (orchestrator Case 2): the lease is NOT Manager-tracked and has no
	// placement, mirroring a post-restart close of an already-active lease.
	payload, err := json.Marshal(manifest.Manifest{Image: "redis:7", Command: []string{"sleep", "3600"}})
	require.NoError(t, err)
	require.NoError(t, b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID: leaseUUID, Tenant: tenant, ProviderUUID: providerUUID,
		Items: []backend.LeaseItem{{SKU: sku, Quantity: 1}}, CallbackURL: callbackURL, Payload: payload,
	}))
	require.Equal(t, backend.CallbackStatusSuccess, waitForCallback(t, callbackCh, leaseUUID, 3*time.Minute).Status)

	execInContainer(t, getContainerID(t, leaseUUID), []string{"sh", "-c", "echo mgr-event-sentinel > /data/sentinel.txt"})

	// Drive the on-chain close as an event through the real Manager. LeaseExpired is
	// intentionally not a second btrfs run — HandleLeaseExpired calls processLeaseClose
	// verbatim (handler_set.go:126-136), so this also covers the expiry route.
	require.NoError(t, mgr.PublishLeaseEvent(chain.LeaseEvent{
		Type: chain.LeaseClosed, LeaseUUID: leaseUUID, ProviderUUID: providerUUID, Tenant: tenant,
	}))

	// The KEY assertion: the event route reached a REAL soft-delete (mock backends can't).
	retainedPath := filepath.Join(mountPath, retainedName(canonicalVolumeName(leaseUUID, manifest.DefaultServiceName, 0)))
	require.Eventually(t, func() bool {
		rec, _ := b.retentionStore.Get(leaseUUID)
		if rec == nil {
			return false
		}
		_, statErr := os.Stat(retainedPath)
		return statErr == nil
	}, 60*time.Second, 500*time.Millisecond, "LeaseClosed event must reach a real soft-delete (retained volume + record)")
	data, err := os.ReadFile(filepath.Join(retainedPath, "data", "sentinel.txt"))
	require.NoError(t, err)
	assert.Contains(t, string(data), "mgr-event-sentinel")
	// Retained volume + bbolt DB live under the loopback mount / t.TempDir → torn down automatically.
}
```

- [ ] **Step 3: Run** `sudo go test -tags integration -run TestIntegration_Manager_CloseEvent_RealSoftDelete -timeout 15m -v ./internal/backend/docker/` → PASS.
- [ ] **Step 4:** Commit: `test(docker): event-route reaches real soft-delete e2e (ENG-325)`

---

### Task 4: Makefile target

**Files:** Modify `Makefile` (the `test-integration-restore` target, ~line 106)

- [ ] **Step 1: Broaden the run regex to include the new reconciler + manager tests**

```makefile
test-integration-restore:
	@echo "Running retain/restore integration tests (requires root + Docker + btrfs-progs)..."
	$(GOTEST) -tags integration -v ./internal/backend/docker/ -run "TestIntegration_(Docker_Retain|Reconciler_RetainRestore|Manager_CloseEvent)" -timeout 20m
```

- [ ] **Step 2:** `go test -tags integration -list 'TestIntegration_(Docker_Retain|Reconciler_RetainRestore|Manager_CloseEvent).*' ./internal/backend/docker/` lists all four tests.
- [ ] **Step 3:** Commit: `build: include reconciler + event-path restore tests in test-integration-restore`

---

## Implementer notes (from multi-perspective review)

- **Do NOT shadow the `manifest` package.** The existing tests in this file bind `manifest := manifest.Manifest{...}` in six funcs; the new code must use `appManifest` (Task 2) / inline `manifest.Manifest{...}` without binding (Task 3) so `manifest.DefaultServiceName` resolves. Recent commit `f0dfba8` un-shadowed this file — don't reintroduce it.
- **Imports to add:** `os` (both tasks use `os.Stat`/`os.ReadFile`) and `internal/chain` (Task 3). `goimports` handles it, but the plan calls them out explicitly.
- **`VolumeDataPath` is load-bearing.** `testBackendWithRealDocker` zeros every SKU's `DiskMB` when `VolumeDataPath==""` (applied *after* the cfgFn), so a harness that forgot to set it would make `docker-small` stateless → no managed volume → confusing "no such file" on the retained-volume assertion. Both harnesses set it.
- **In-package is import-cycle-safe:** confirmed by `go vet -tags integration` on the new harness + the existing `testReconcilerSetup` precedent (`provisioner` imports the `backend.Backend` interface, not the docker package). No `package docker_test` split.

## Self-Review

- **Spec coverage — both routes verified:** reconciler/orphan route (Task 2, == auto-close + backstop, full restore) and event route (Task 3, `LeaseClosed`; expiry covered by shared `processLeaseClose`), each asserting **real** soft-delete. The explicit "verify both reconciler path and chain close/auto-close paths" ask is satisfied: auto-close ⊂ reconciler route (documented why), close/expiry = event route.
- **Compiles:** the Task 1/3 helpers + both test bodies were reproduced verbatim and `go vet -tags integration` returned clean (review evidence); every signature/field/struct-literal checked against source.
- **Risks (all resolved):** (a) orphan callback buffering — `waitForCallback` skips-by-UUID, buffer=10, won't overflow (Task 2 Step 2 drain optional); (b) Manager close needing prior tracking — ruled out (handler_set.go:172-193, Case-2 SKU-hint route); (c) timing — `Backend.Deprovision` is synchronous (routeToLeaseBlocking + waitForReply), so the soft-delete is complete before `Deprovision` returns; the only async hop (Watermill publish→dispatch) is absorbed by the 60s `Eventually`; (d) `redis:7` pullable — same dep as the existing restore test.
- **No placeholders:** every step has concrete code/commands.

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-06-16-chain-driven-restore-e2e.md`. Two execution options:
1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review between tasks.
2. **Inline** — execute here with checkpoints.
