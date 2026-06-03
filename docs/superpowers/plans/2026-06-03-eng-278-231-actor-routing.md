# ENG-278/231 — Route ReconcileCustomDomain through the lease actor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the ENG-278 TOCTOU by routing the custom-domain apply + redeploy through the lease actor (ENG-231): the reconciler computes the diff read-only, the existing restart path renders the new domain, and the actor commits `prov.Items` on success — deleting the off-actor mutation and the CAS rollback.

**Architecture:** `ReconcileCustomDomain` computes a `ServiceName`→desiredDomain override map read-only under `provisionsMu`, then calls the shared `routeReplaceRestart`, which (a) applies the overrides to a *copy* of the items handed to the replace worker (so the redeploy renders the new domain regardless of a concurrent `recoverState` swap), and (b) sets an `OnSuccess` hook that commits the overrides into `prov.Items` inside `onEnterReadyFromReplaceCompleted` — a terminal SM entry action on the serial actor goroutine, atomic with `Status→Ready`, only on success. `recover.go` and `internal/backend/shared/leasesm/` are unchanged.

**Tech Stack:** Go; `internal/backend/docker` (Backend, compose path), `internal/backend/shared/leasesm` (lease actor, `ReplaceSuccessResult.OnSuccess`), testify, compose-go types.

**Spec:** `docs/superpowers/specs/2026-06-03-eng-278-231-actor-routing-design.md`

---

## File Structure

| File | Change |
|------|--------|
| `internal/backend/docker/restart_update.go` | Extract `routeReplaceRestart(ctx, leaseUUID, callbackURL, overrides)` from `Restart`; add `applyCustomDomainOverrides` + `customDomainOnSuccess` helpers; thread `onSuccess` through `doRestart`. `Restart` becomes a thin wrapper (`overrides == nil`). |
| `internal/backend/docker/reconcile_custom_domain.go` | Add read-only `computeCustomDomainOverrides`; rewrite `ReconcileCustomDomain` to compute overrides + route via `routeReplaceRestart`; delete the off-actor mutate and the `pendingChange`/CAS rollback. |
| `internal/backend/docker/restart_update_test.go` | Update the one direct `doRestart(...)` call site for the new `onSuccess` parameter. |
| `internal/backend/docker/reconcile_custom_domain_test.go` | Delete the two CAS-rollback tests + the unused `reconcileHarness`; rewrite the sync-error test to the no-off-actor-mutation contract; add the deterministic regression test and the `-race` test. |
| `internal/backend/docker/recover.go` | **No change** (the `Restarting`-preserve branch is what the fix relies on). |
| `internal/backend/shared/leasesm/*` | **No change** (`OnSuccess` already exists and fires on the actor goroutine). |

---

## Task 1: Thread custom-domain overrides through the restart path (behavior-preserving)

Extract the restart routing into `routeReplaceRestart` so it can carry a `ServiceName`→domain override; `Restart` keeps its exact current behavior by passing `nil`. No reconcile changes yet — verified entirely by the existing restart suite.

**Files:**
- Modify: `internal/backend/docker/restart_update.go`
- Modify: `internal/backend/docker/restart_update_test.go:825` (the one direct `doRestart` caller)

- [ ] **Step 1: Add the two override helpers** (top of `restart_update.go`, after the imports)

```go
// applyCustomDomainOverrides applies per-ServiceName custom_domain values to the
// given items slice (which must be a COPY of prov.Items, never prov.Items
// itself). Keyed by ServiceName so it is robust to a recoverState rebuild that
// reorders Items. No-op when overrides is empty. (ENG-231)
func applyCustomDomainOverrides(items []backend.LeaseItem, overrides map[string]string) {
	if len(overrides) == 0 {
		return
	}
	for i := range items {
		if d, ok := overrides[items[i].ServiceName]; ok {
			items[i].CustomDomain = d
		}
	}
}

// customDomainOnSuccess returns an OnSuccess hook that commits the override
// values into prov.Items. It runs inside onEnterReadyFromReplaceCompleted on the
// serial actor goroutine, under the same UpdateFn critical section as the
// Status->Ready flip, and ONLY on a successful redeploy — so a failed redeploy
// leaves prov.Items untouched and the next reconcile tick retries. Returns nil
// when there are no overrides, preserving the plain-restart behavior. (ENG-231)
func customDomainOnSuccess(overrides map[string]string) func(*leasesm.ProvisionState) {
	if len(overrides) == 0 {
		return nil
	}
	return func(p *leasesm.ProvisionState) {
		for i := range p.Items {
			if d, ok := overrides[p.Items[i].ServiceName]; ok {
				p.Items[i].CustomDomain = d
			}
		}
	}
}
```

- [ ] **Step 2: Extract `routeReplaceRestart` and make `Restart` delegate**

Replace the current `func (b *Backend) Restart(...)` body (restart_update.go:45-122) with:

```go
func (b *Backend) Restart(ctx context.Context, req backend.RestartRequest) error {
	return b.routeReplaceRestart(ctx, req.LeaseUUID, req.CallbackURL, nil)
}

// routeReplaceRestart is the shared restart routing used by the public Restart
// (overrides == nil) and by ReconcileCustomDomain (overrides carries the
// per-ServiceName custom_domain changes). The SEAM-CLOSED (ENG-230) prelude is
// unchanged: read-only fast-fail under provisionsMu, field snapshot, no
// prov.Status write. The only addition is that custom-domain overrides are
// applied to the worker's item snapshot (a copy) and committed into prov.Items
// by the actor's success entry action via OnSuccess (ENG-231).
func (b *Backend) routeReplaceRestart(ctx context.Context, leaseUUID, callbackURL string, overrides map[string]string) error {
	logger := b.logger.With("lease_uuid", leaseUUID)

	b.provisionsMu.Lock()
	prov, exists := b.provisions[leaseUUID]
	if !exists {
		b.provisionsMu.Unlock()
		return backend.ErrNotProvisioned
	}
	if prov.Status != backend.ProvisionStatusReady && prov.Status != backend.ProvisionStatusFailed {
		status := prov.Status
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: cannot restart from status %s", backend.ErrInvalidState, status)
	}
	if prov.StackManifest == nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: no stored manifest for restart (pre-migration legacy lease?)", backend.ErrInvalidState)
	}
	stackManifest := prov.StackManifest
	containerIDs := append([]string(nil), prov.ContainerIDs...)
	serviceContainers := make(map[string][]string, len(prov.ServiceContainers))
	for k, v := range prov.ServiceContainers {
		serviceContainers[k] = append([]string(nil), v...)
	}
	items := append([]backend.LeaseItem(nil), prov.Items...)
	// Apply custom-domain overrides to the worker's snapshot COPY (never
	// prov.Items). Keyed by ServiceName, so even if recoverState swapped the
	// struct between the reconciler's diff and here, the desired domain is
	// re-applied onto the current items. (ENG-231/ENG-278)
	applyCustomDomainOverrides(items, overrides)
	b.provisionsMu.Unlock()

	// Record restart release as deploying. Abort if this fails — without a
	// release record, ActivateLatest after success is a no-op, and a cold
	// restart would recover the previous manifest (silently rolling back).
	if b.releaseStore != nil {
		manifestBytes, marshalErr := json.Marshal(stackManifest)
		if marshalErr != nil {
			return fmt.Errorf("failed to marshal manifest for release: %w", marshalErr)
		}
		if relErr := b.releaseStore.Append(leaseUUID, shared.Release{
			Manifest:  manifestBytes,
			Image:     "stack",
			Status:    "deploying",
			CreatedAt: time.Now(),
		}); relErr != nil {
			return fmt.Errorf("failed to record release: %w", relErr)
		}
	}

	// Hand off to the lease actor. The actor's onEnterRestarting writes
	// Status=Restarting (+ CallbackURL) BEFORE acking, then spawns the replace
	// worker. On success, onEnterReadyFromReplaceCompleted runs onSuccess (the
	// prov.Items custom_domain commit) under UpdateFn, atomic with Status->Ready.
	opCtx, opCancel := b.shutdownAwareContext()
	onSuccess := customDomainOnSuccess(overrides)
	work := func() leasesm.ReplaceResult {
		return b.doRestart(opCtx, leaseUUID, stackManifest, containerIDs, serviceContainers, items, onSuccess, logger)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, leaseUUID, leasesm.RestartRequestedMsg{Cancel: opCancel, Work: work, Ack: ack, CallbackURL: callbackURL}); routeErr != nil {
		opCancel()
		return routeErr
	}
	if accepted, err := b.ackOrAbort(ctx, ack); !accepted {
		opCancel()
		return err
	}
	return nil
}
```

- [ ] **Step 3: Thread `onSuccess` through `doRestart`**

In `doRestart` (restart_update.go:134), add the parameter and pass it to the op. Change the signature line and the returned op:

```go
func (b *Backend) doRestart(ctx context.Context, leaseUUID string, stack *manifest.StackManifest, oldContainerIDs []string, serviceContainers map[string][]string, items []backend.LeaseItem, onSuccess func(*leasesm.ProvisionState), logger *slog.Logger) leasesm.ReplaceResult {
```

and in its final `return b.doReplaceContainers(ctx, replaceContainersOp{...})`, add the field:

```go
		Operation:         "restart",
		Logger:            logger,
		OnSuccess:         onSuccess,
	})
```

(The SKU-preflight failure branch is unchanged — it returns before reaching `doReplaceContainers`, so a custom-domain restart that fails preflight simply never commits, which is the intended retry-next-tick behavior.)

- [ ] **Step 4: Update the direct `doRestart` test caller**

In `restart_update_test.go:825`, insert `nil` for the new `onSuccess` parameter (before `b.logger`):

```go
	result := b.doRestart(context.Background(), "lease-1", &manifest.StackManifest{},
		nil, nil, []backend.LeaseItem{{SKU: "nonexistent-sku"}}, nil, b.logger)
```

- [ ] **Step 5: Run the restart suite — verify behavior-preserving**

Run: `go test ./internal/backend/docker/ -run 'TestStackRestart|TestStackUpdate|TestRestart|TestDoRestart|TestUpdate' -count=1`
Expected: PASS (no behavior change; `Restart` still routes with `overrides == nil`).

- [ ] **Step 6: Commit**

```bash
git add internal/backend/docker/restart_update.go internal/backend/docker/restart_update_test.go
git commit -m "refactor(docker-backend): extract routeReplaceRestart with custom-domain override seam (ENG-231)"
```

---

## Task 2: Route ReconcileCustomDomain through the actor (read-only diff, delete CAS)

`ReconcileCustomDomain` computes a `ServiceName`→domain override map read-only, then routes via `routeReplaceRestart`. The off-actor `prov.Items` mutation and the CAS rollback are deleted.

**Files:**
- Modify: `internal/backend/docker/reconcile_custom_domain.go`
- Modify: `internal/backend/docker/reconcile_custom_domain_test.go` (delete 2 rollback tests + unused harness; rewrite 1 sync-error test)

- [ ] **Step 1: Add `computeCustomDomainOverrides`** (read-only) to `reconcile_custom_domain.go`

```go
// computeCustomDomainOverrides returns the per-ServiceName custom_domain changes
// to apply, computed READ-ONLY from the current provision and the incoming chain
// items. The caller must hold provisionsMu. It performs the same
// ServiceName-normalized match (ENG-264), defense-in-depth validation, and
// asymmetric DNS-readiness gate (ENG-266) as the previous in-place mutation, but
// stages nothing in prov.Items — routeReplaceRestart applies the returned
// overrides to the worker snapshot and the actor commits them on success.
func (b *Backend) computeCustomDomainOverrides(prov *provision, items []backend.LeaseItem, dnsReady map[string]bool) map[string]string {
	logger := slog.With("lease_uuid", prov.LeaseUUID)
	chainKeys := normalizedServiceKeys(items)
	provKeys := normalizedServiceKeys(prov.Items)

	overrides := make(map[string]string)
	for ci := range items {
		chainItem := items[ci]
		idx := -1
		for i := range prov.Items {
			if provKeys[i] == chainKeys[ci] {
				idx = i
				break
			}
		}
		if idx == -1 {
			// Item present on chain but not in provision: skip silently.
			continue
		}
		emitted := prov.Items[idx].CustomDomain
		desired := chainItem.CustomDomain
		// Defense-in-depth validation (empty clear bypasses the FQDN check).
		if desired != "" {
			if err := validateCustomDomain(desired, b.cfg.Ingress.WildcardDomain); err != nil {
				logger.Error("skipping custom-domain reconcile (validation failed)",
					"service_name", prov.Items[idx].ServiceName,
					"custom_domain", desired,
					"error", err)
				continue
			}
		}
		// Asymmetric DNS-readiness gate (ENG-266): only gate emitting a domain
		// that is not already the emitted one. Never tear down an already-emitted
		// domain on a transient DNS mismatch; clearing ("") is never gated.
		if desired != "" && desired != emitted && !dnsReady[desired] {
			logger.Debug("custom_domain set but DNS not yet pointing at this host; deferring cert issuance",
				"service_name", prov.Items[idx].ServiceName,
				"custom_domain", desired)
			desired = emitted
		}
		if emitted == desired {
			continue
		}
		overrides[prov.Items[idx].ServiceName] = desired
	}
	return overrides
}
```

- [ ] **Step 2: Rewrite `ReconcileCustomDomain`** — replace the body (reconcile_custom_domain.go:29-214) with:

```go
func (b *Backend) ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error {
	if !b.cfg.Ingress.Enabled {
		return nil
	}

	// Precompute DNS readiness for each non-empty incoming custom domain BEFORE
	// taking provisionsMu — the lookups do network I/O and must not block the
	// lock. Keyed by domain. (ENG-266)
	dnsReady := make(map[string]bool)
	for i := range items {
		d := items[i].CustomDomain
		if d == "" {
			continue
		}
		if _, seen := dnsReady[d]; seen {
			continue
		}
		if err := validateCustomDomain(d, b.cfg.Ingress.WildcardDomain); err != nil {
			dnsReady[d] = false
			continue
		}
		dnsReady[d] = b.dnsGateAllows(ctx, d)
	}

	// Compute the diff READ-ONLY under the lock; stage nothing in prov.Items.
	b.provisionsMu.Lock()
	prov, ok := b.provisions[leaseUUID]
	if !ok {
		b.provisionsMu.Unlock()
		return nil
	}
	if prov.Status != backend.ProvisionStatusReady {
		b.provisionsMu.Unlock()
		return nil
	}
	overrides := b.computeCustomDomainOverrides(prov, items, dnsReady)
	callbackURL := prov.CallbackURL
	b.provisionsMu.Unlock()

	if len(overrides) == 0 {
		return nil
	}

	slog.With("lease_uuid", leaseUUID).Info("custom_domain drift detected; redeploying",
		"changes", len(overrides))

	// Route the redeploy through the lease actor. The worker renders the new
	// domain from the override-applied item snapshot; the actor commits the
	// values into prov.Items on success (ENG-231). No off-actor mutation, no
	// CAS rollback — a failed redeploy leaves prov.Items untouched so the next
	// tick retries (ENG-278).
	return b.routeReplaceRestart(ctx, leaseUUID, callbackURL, overrides)
}
```

Keep `normalizedServiceKeys` (unchanged). Delete the `pendingChange` struct (it no longer exists in the rewritten body).

- [ ] **Step 3: Remove the now-obsolete rollback/harness test code** in `reconcile_custom_domain_test.go`

Delete these three items entirely:
- `TestReconcileCustomDomain_RollbackUsesServiceNameNotIndex` (lines 299-349) — tested the deleted CAS rollback.
- `TestReconcileCustomDomain_RollbackIsCASGated` (lines 351-394) — tested the deleted CAS gate.
- The unused `reconcileHarness` struct + `newReconcileHarness` + `waitForCallback` + `lastCreateParams` (lines 176-261) — never called, and built around `CreateContainerFn` which the compose restart path never invokes.

After deleting the harness, remove now-unused imports if the compiler flags them (likely `sync` stays — used by the new tests in Task 3/4; `net/http`, `net/http/httptest`, `encoding/json`, `time` are reused by Task 3).

- [ ] **Step 4: Rewrite the sync-error test to the new contract**

Replace `TestReconcileCustomDomain_RestartSyncError_RollsBack` (lines 263-297) with:

```go
func TestReconcileCustomDomain_RestartSyncError_LeavesItemsUnchanged(t *testing.T) {
	// A synchronous redeploy error (here: no stored manifest -> ErrInvalidState)
	// must surface to the caller, and prov.Items must be UNCHANGED — the
	// reconciler no longer mutates prov.Items off-actor, so there is nothing to
	// roll back; the staged value only ever lands via the actor on success.
	prov := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
		Tenant:        "tenant-a",
		ProviderUUID:  "prov-1",
		SKU:           "docker-small",
		Status:        backend.ProvisionStatusReady,
		StackManifest: nil, // forces ErrInvalidState in the routeReplaceRestart prelude
		ContainerIDs:  []string{"old-c1"},
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "old.example.com"},
		},
		Quantity: 1},
	}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{"lease-1": prov})
	defer b.stopCancel()
	b.cfg.Ingress = IngressConfig{
		Enabled:        true,
		WildcardDomain: "barney0.manifest0.net",
		Entrypoint:     "websecure",
	}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return true }

	err := b.ReconcileCustomDomain(context.Background(), "lease-1", []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "new.example.com"},
	})
	require.Error(t, err, "ReconcileCustomDomain must surface synchronous redeploy errors")
	assert.ErrorIs(t, err, backend.ErrInvalidState)

	b.provisionsMu.RLock()
	got := b.provisions["lease-1"].Items[0].CustomDomain
	b.provisionsMu.RUnlock()
	assert.Equal(t, "old.example.com", got,
		"prov.Items must be unchanged on a failed redeploy (no off-actor mutation; commit is actor/success-only)")
}
```

Note: the other tests that exploit the nil-`StackManifest` fast-fail (`_DNSReady_Emits`, `_Clear_NotGated`, `_ChangedDomain_NewReady`, `_FreshSingleImage_ChainServiceNameEmpty`, `_DeferredThenReady`, `_NotDNSReady_DoesNotEmit`, `_AlreadyEmitted_NotTornDownOnDNSBlip`, `_MultiService_MixedReadiness`) still pass unchanged: a non-empty override still triggers `routeReplaceRestart`'s `ErrInvalidState`, and their `prov.Items` assertions hold because the value is never mutated (it equals the seeded value).

- [ ] **Step 5: Run the reconcile suite**

Run: `go test ./internal/backend/docker/ -run 'TestReconcileCustomDomain' -count=1`
Expected: PASS (all retained tests + the rewritten sync-error test).

- [ ] **Step 6: Commit**

```bash
git add internal/backend/docker/reconcile_custom_domain.go internal/backend/docker/reconcile_custom_domain_test.go
git commit -m "feat(docker-backend): route ReconcileCustomDomain apply+redeploy through the actor (ENG-231/ENG-278)"
```

---

## Task 3: Deterministic regression test — recoverState swap in the Ready window

The primary ENG-278 guard. White-box test (package `docker`) that calls the factored pieces with a real `recoverState` swap in between, asserting the redeploy renders the **new** domain and the actor commits it — proving the override (captured pre-swap) wins over the swapped-in old domain.

**Files:**
- Modify: `internal/backend/docker/reconcile_custom_domain_test.go` (add the test + the `composetypes` import)

- [ ] **Step 1: Add imports**

Ensure the test file imports (add the missing ones):

```go
	"path/filepath"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
```

- [ ] **Step 2: Write the deterministic regression test**

```go
func TestReconcileCustomDomain_RecoverStateSwap_RedeploysNewDomain(t *testing.T) {
	// ENG-278 regression. A recoverState struct-swap that reverts
	// prov.Items[].CustomDomain to the OLD container-label value, landing in the
	// Ready window, must NOT cause the redeploy to render the old domain. The
	// override is captured read-only before the swap and re-applied by
	// ServiceName, so the redeploy renders NEW and the actor commits NEW.

	// Stack manifest with a routable port so the custom-domain Traefik label
	// (LabelCustomDomain) is actually rendered (ingress gate requires a port).
	stack := &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"app": {Image: "nginx:latest", Ports: map[string]manifest.PortConfig{"80/tcp": {}}},
		},
	}

	// Seed a release store with an ACTIVE stack release so recoverState restores
	// StackManifest after it rebuilds the (Ready) provision from labels.
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	defer releaseStore.Close()
	require.NoError(t, releaseStore.Append("lease-1", shared.Release{
		Manifest: manifestBytes, Image: "stack", Status: "active", CreatedAt: time.Now(),
	}))

	// Callback server signals redeploy completion.
	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	// Fake managed container carrying the OLD domain label — what recoverState
	// rebuilds prov.Items from.
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{{
				ContainerID:  "old-app",
				LeaseUUID:    "lease-1",
				SKU:          "docker-small",
				Tenant:       "tenant-a",
				ProviderUUID: "prov-1",
				CallbackURL:  callbackServer.URL,
				ServiceName:  "app",
				InstanceIndex: 0,
				Status:       "running",
				CustomDomain: "old.example.com",
				Name:         "fred-lease-1-app-0",
			}}, nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	var mu sync.Mutex
	var capturedProject *composetypes.Project
	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			mu.Lock()
			capturedProject = project
			mu.Unlock()
			return nil
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{ID: "new-app-c1", Service: "app", State: "running"}}, nil
		},
	}

	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:        "tenant-a",
			ProviderUUID:  "prov-1",
			SKU:           "docker-small",
			Status:        backend.ProvisionStatusReady,
			StackManifest: stack,
			ContainerIDs:  []string{"old-app"},
			ServiceContainers: map[string][]string{"app": {"old-app"}},
			CallbackURL:   callbackServer.URL,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "old.example.com"},
			},
			Quantity: 1},
		},
	}

	b := newBackendForProvisionTest(t, mock, provisions)
	defer b.stopCancel()
	b.compose = composeMock
	b.releaseStore = releaseStore
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}

	chain := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "new.example.com"}}

	// 1. Compute the override read-only while Ready (mirrors ReconcileCustomDomain
	//    under provisionsMu), with the new domain DNS-ready.
	b.provisionsMu.Lock()
	prov := b.provisions["lease-1"]
	overrides := b.computeCustomDomainOverrides(prov, chain, map[string]bool{"new.example.com": true})
	callbackURL := prov.CallbackURL
	b.provisionsMu.Unlock()
	require.Equal(t, map[string]string{"app": "new.example.com"}, overrides,
		`chain service_name "" must normalize to "app" and stage the new domain`)

	// 2. Concurrent recoverState lands in the Ready window: it rebuilds the
	//    provision from the OLD-labeled container and swaps the map.
	require.NoError(t, b.recoverState(context.Background()))
	b.provisionsMu.RLock()
	swapped := b.provisions["lease-1"].Items[0].CustomDomain
	b.provisionsMu.RUnlock()
	require.Equal(t, "old.example.com", swapped, "sanity: recoverState swapped prov.Items back to the old domain")

	// 3. Route the redeploy with the pre-swap override.
	require.NoError(t, b.routeReplaceRestart(context.Background(), "lease-1", callbackURL, overrides))

	// 4. Wait for the async redeploy to complete.
	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for redeploy callback")
	}
	require.Equal(t, backend.CallbackStatusSuccess, callbackPayload.Status)

	// 5. The redeploy rendered the NEW domain despite the swap.
	mu.Lock()
	proj := capturedProject
	mu.Unlock()
	require.NotNil(t, proj, "compose Up was not called")
	assert.Equal(t, "new.example.com", proj.Services["app"].Labels[LabelCustomDomain],
		"redeploy must render the new domain even though recoverState swapped prov.Items to the old one")

	// 6. The actor committed the new domain to prov.Items on success.
	b.provisionsMu.RLock()
	committed := b.provisions["lease-1"].Items[0].CustomDomain
	status := b.provisions["lease-1"].Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, "new.example.com", committed, "actor must commit the new domain to prov.Items on success")
	assert.Equal(t, backend.ProvisionStatusReady, status)

	b.stopCancel()
	b.wg.Wait()
}
```

- [ ] **Step 3: Run the test — verify it passes (fix in place)**

Run: `go test ./internal/backend/docker/ -run TestReconcileCustomDomain_RecoverStateSwap_RedeploysNewDomain -race -count=1`
Expected: PASS.

- [ ] **Step 4: Prove the test discriminates (the "fails without the fix" check)**

Temporarily make `applyCustomDomainOverrides` a no-op by commenting out its loop body in `restart_update.go` (so the worker snapshot keeps the swapped OLD domain), then re-run:

Run: `go test ./internal/backend/docker/ -run TestReconcileCustomDomain_RecoverStateSwap_RedeploysNewDomain -count=1`
Expected: FAIL at step 5 — `proj.Services["app"].Labels[LabelCustomDomain]` is `"old.example.com"` (the swapped value), and step 6 commit is also old. This confirms the test catches a regression of the fix. **Restore the loop body** and re-run to confirm PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/docker/reconcile_custom_domain_test.go
git commit -m "test(docker-backend): deterministic recoverState-swap regression for custom-domain redeploy (ENG-278)"
```

---

## Task 4: Complementary `-race` test (lock-discipline guard)

A concurrent `ReconcileCustomDomain` + `recoverState` test under `-race`, guarding that the new write path keeps every `prov.Items` access lock-/actor-serialized. It is a race-freedom guard, not the bug discriminator (Task 3 owns correctness), so it asserts completion + convergence, not an exact interleaving.

**Files:**
- Modify: `internal/backend/docker/reconcile_custom_domain_test.go`

- [ ] **Step 1: Write the concurrency test**

```go
func TestReconcileCustomDomain_ConcurrentRecoverState_NoRace(t *testing.T) {
	// Lock-discipline guard (run under -race): ReconcileCustomDomain and
	// recoverState hammering the same lease concurrently must not race and must
	// converge to the new domain. Correctness of the interleaving is pinned by
	// TestReconcileCustomDomain_RecoverStateSwap_RedeploysNewDomain.
	stack := &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"app": {Image: "nginx:latest", Ports: map[string]manifest.PortConfig{"80/tcp": {}}},
		},
	}
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	defer releaseStore.Close()
	require.NoError(t, releaseStore.Append("lease-1", shared.Release{
		Manifest: manifestBytes, Image: "stack", Status: "active", CreatedAt: time.Now(),
	}))

	callbackReceived := make(chan struct{}, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case callbackReceived <- struct{}{}:
		default:
		}
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{{
				ContainerID: "old-app", LeaseUUID: "lease-1", SKU: "docker-small",
				Tenant: "tenant-a", ProviderUUID: "prov-1", CallbackURL: callbackServer.URL,
				ServiceName: "app", InstanceIndex: 0, Status: "running",
				CustomDomain: "old.example.com", Name: "fred-lease-1-app-0",
			}}, nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}
	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error { return nil },
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{ID: "new-app-c1", Service: "app", State: "running"}}, nil
		},
	}
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant: "tenant-a", ProviderUUID: "prov-1", SKU: "docker-small",
			Status: backend.ProvisionStatusReady, StackManifest: stack,
			ContainerIDs: []string{"old-app"}, ServiceContainers: map[string][]string{"app": {"old-app"}},
			CallbackURL: callbackServer.URL,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "old.example.com"}},
			Quantity: 1}},
	}

	b := newBackendForProvisionTest(t, mock, provisions)
	defer b.stopCancel()
	b.compose = composeMock
	b.releaseStore = releaseStore
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure"}
	b.customDomainDNSReady = func(_ context.Context, _ string) bool { return true }

	chain := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "", CustomDomain: "new.example.com"}}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_ = b.recoverState(context.Background())
		}
	}()
	// Reconcile retries against the concurrent swap until the redeploy is
	// accepted (ErrInvalidState/no-op ticks are expected while a swap is mid-flight).
	require.Eventually(t, func() bool {
		_ = b.ReconcileCustomDomain(context.Background(), "lease-1", chain)
		select {
		case <-callbackReceived:
			return true
		default:
			return false
		}
	}, 5*time.Second, 20*time.Millisecond)
	wg.Wait()

	b.stopCancel()
	b.wg.Wait()
}
```

- [ ] **Step 2: Run under the race detector**

Run: `go test ./internal/backend/docker/ -run TestReconcileCustomDomain_ConcurrentRecoverState_NoRace -race -count=5`
Expected: PASS, no race report across all 5 runs.

- [ ] **Step 3: Full package race run**

Run: `go test ./internal/backend/docker/ -race -count=1`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/backend/docker/reconcile_custom_domain_test.go
git commit -m "test(docker-backend): concurrent recoverState lock-discipline guard (ENG-278)"
```

---

## Final verification

- [ ] **Build + vet + full test**

```bash
go build ./... && go vet ./internal/backend/docker/ && go test ./internal/backend/... -race -count=1
```
Expected: clean build, no vet issues, all tests pass under `-race`.

---

## Self-Review (against the spec)

**Spec coverage:**
- "reconciler computes diff read-only" → Task 2 `computeCustomDomainOverrides` (no `prov.Items` write). ✔
- "reuse restart path; worker renders override-applied snapshot" → Task 1 `routeReplaceRestart` + `applyCustomDomainOverrides`. ✔
- "actor commits via OnSuccess in onEnterReadyFromReplaceCompleted" → Task 1 `customDomainOnSuccess` (existing `ReplaceSuccessResult.OnSuccess`). ✔
- "delete off-actor mutate + CAS; failure = next-tick retry" → Task 2 deletes mutate + CAS; Task 2 Step 4 pins the no-mutation-on-failure contract. ✔
- "recover.go / shared/leesm untouched" → no tasks modify them. ✔
- "no test-only production code" → no `Backend` hook/flag added; test uses the production-justified split (`computeCustomDomainOverrides` + `routeReplaceRestart`). ✔
- "deterministic regression test + concurrent -race test" → Task 3 + Task 4; Task 3 Step 4 is the fails-without-fix discriminator. ✔
- ENG-264 normalization + ENG-266 DNS gate preserved → carried verbatim into `computeCustomDomainOverrides`; existing ENG-264/266 tests retained. ✔

**Type consistency:** `overrides map[string]string` (ServiceName→domain) is consistent across `computeCustomDomainOverrides` (returns), `routeReplaceRestart`/`applyCustomDomainOverrides`/`customDomainOnSuccess` (consume). `doRestart` gains `onSuccess func(*leasesm.ProvisionState)`, matching `ReplaceSuccessResult.OnSuccess`'s type and the single direct test caller (updated in Task 1 Step 4).

**Placeholder scan:** none — every step has concrete code or an exact command + expected result.
