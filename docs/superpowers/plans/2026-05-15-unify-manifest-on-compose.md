# Unify Manifest Handling on the Compose Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the legacy single-service Docker backend path. Every lease becomes a 1+-service Compose project. Wire format requires `{"services": {...}}`; lease items require `service_name`.

**Architecture:** Hard cutover with operator-required drain. fred fails to start if it discovers any container with `fred.lease_uuid` but no `fred.service_name`. The Compose path (`buildComposeProject` → `composeService.Up`) becomes the only execution path. `backend.IsStack`, `manifest.Manifest`, `manifest.ParsePayload`, and the dual-format JSON schema are removed.

**Tech Stack:** Go, Docker Engine API, Docker Compose v2 API (`github.com/docker/compose/v2`), `compose-spec/compose-go/v2`.

**Spec:** `docs/superpowers/specs/2026-05-15-unify-manifest-on-compose-design.md`

---

## Working principles

- **Tree green at every commit.** Each task ends with a passing `go build ./...` and the relevant `go test ./...` packages green.
- **Tests as ratchet.** New behavior (reject flat manifests, reject items without `service_name`, fail-fast on legacy containers) is locked in with tests before the code change.
- **One concern per task.** Renames and deletions are split from semantic changes so the diff in any single commit is easy to review.
- **No shims.** No feature flags, no backwards-compat fallbacks. The whole codebase is updated in a sequence of commits on this branch.

## File map (high level)

| File | Action |
|---|---|
| `internal/backend/shared/manifest/manifest.go` | Delete `Manifest`, `ParsePayload`; add `ParseStackPayload`. |
| `internal/backend/shared/manifest/*_test.go` | Drop single-manifest tests; replace `ParsePayload` calls. |
| `internal/backend/client.go` | Delete `IsStack`; validate `ServiceName != ""` on every `LeaseItem`. |
| `internal/backend/docker/provision.go` | Collapse `doProvision`/`doProvisionStack` into one. Delete `setupVolumeBinds`. |
| `internal/backend/docker/restart_update.go` | Collapse `doRestart`/`doRestartStack`, `doUpdate`/`doUpdateStack`. |
| `internal/backend/docker/deprovision.go` | Remove `isStack` branches. |
| `internal/backend/docker/recover.go` | Remove legacy branch; fail-fast on legacy containers. |
| `internal/backend/docker/info.go` | Always populate `LeaseInfo.Services`; derive `Instances`. |
| `internal/backend/docker/lifecycle.go` | Remove the dual container-name branch. |
| `internal/backend/docker/backend.go` | Update `prevContainerName` (single form only). |
| `internal/backend/shared/leasesm/*.go` | Drop `IsStack` callers; always use `StackManifest` + `ServiceContainers`. |
| `internal/api/handlers.go` | Reject items missing `service_name`. |
| `docs/manifest-guide.md` | Rewrite — stack-only. |
| `docs/manifest-schema.json` | Require top-level `services`. |
| `internal/backend/docker/TENANT_MANIFEST.md` | No change (already a pointer). |

---

## Task 1: Lock the new contract with failing tests

**Goal:** Codify the three new invariants as tests before any production code changes. They fail today and serve as the acceptance ratchet for tasks 2, 3, and 7.

**Files:**
- Modify: `internal/backend/shared/manifest/manifest_test.go`
- Modify: `internal/backend/client_test.go`
- Create: `internal/backend/docker/recover_legacy_reject_test.go`

- [ ] **Step 1.1: Add `TestParseStackPayload_RejectsFlatManifest` to `internal/backend/shared/manifest/manifest_test.go`.**

```go
func TestParseStackPayload_RejectsFlatManifest(t *testing.T) {
    flat := []byte(`{"image":"nginx:1.25"}`)
    _, err := manifest.ParseStackPayload(flat)
    if err == nil {
        t.Fatalf("expected error for flat manifest, got nil")
    }
    if !strings.Contains(err.Error(), "services") {
        t.Fatalf("expected error to mention missing services key, got: %v", err)
    }
}

func TestParseStackPayload_AcceptsStack(t *testing.T) {
    stack := []byte(`{"services":{"web":{"image":"nginx:1.25"}}}`)
    sm, err := manifest.ParseStackPayload(stack)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if sm == nil || len(sm.Services) != 1 || sm.Services["web"].Image != "nginx:1.25" {
        t.Fatalf("unexpected parse result: %+v", sm)
    }
}
```

- [ ] **Step 1.2: Add `TestIsStack_*` removal-readiness tests in `internal/backend/client_test.go` (these will be deleted in Task 12 with `IsStack`). Add a new test that exercises the validation introduced in Task 3:**

```go
func TestProvisionRequest_RejectsItemWithoutServiceName(t *testing.T) {
    req := backend.ProvisionRequest{
        LeaseUUID: "u",
        Tenant:    "t",
        Items: []backend.LeaseItem{
            {SKU: "docker-micro", Quantity: 1}, // no ServiceName
        },
    }
    err := backend.ValidateProvisionRequest(req)
    if err == nil {
        t.Fatalf("expected validation error, got nil")
    }
    if !strings.Contains(err.Error(), "service_name") {
        t.Fatalf("expected error to mention service_name, got: %v", err)
    }
}
```

- [ ] **Step 1.3: Create `internal/backend/docker/recover_legacy_reject_test.go`:**

```go
package docker

import (
    "context"
    "strings"
    "testing"
)

// TestRecoverState_RejectsLegacyContainer asserts that a managed container
// missing the fred.service_name label is treated as a fatal startup error.
func TestRecoverState_RejectsLegacyContainer(t *testing.T) {
    b, fakeDocker := newTestBackend(t)
    fakeDocker.containers = []ContainerInfo{{
        ContainerID: "abc123",
        LeaseUUID:   "lease-1",
        Tenant:      "tenant-a",
        SKU:         "docker-micro",
        Image:       "nginx:1.25",
        // ServiceName intentionally empty — legacy container.
    }}

    err := b.recoverState(context.Background())
    if err == nil {
        t.Fatalf("expected recoverState to fail for legacy container, got nil")
    }
    if !strings.Contains(err.Error(), "legacy") {
        t.Fatalf("expected error to mention 'legacy', got: %v", err)
    }
    if !strings.Contains(err.Error(), "abc123") {
        t.Fatalf("expected error to list offending container ID, got: %v", err)
    }
}
```

- [ ] **Step 1.4: Run the new tests; they MUST fail.**

```bash
go test ./internal/backend/shared/manifest/ -run TestParseStackPayload -v
go test ./internal/backend/ -run TestProvisionRequest_RejectsItemWithoutServiceName -v
go test ./internal/backend/docker/ -run TestRecoverState_RejectsLegacyContainer -v
```

Expected: all three fail with "function not defined" or "expected error, got nil".

- [ ] **Step 1.5: Commit.**

```bash
git add internal/backend/shared/manifest/manifest_test.go \
        internal/backend/client_test.go \
        internal/backend/docker/recover_legacy_reject_test.go
git commit -m "test: lock new manifest-handling contract (red)"
```

---

## Task 2: Tighten manifest parsing

**Goal:** Add `ParseStackPayload`; keep `ParsePayload` callable so callers can be migrated one at a time. The single-format struct stays around for now to keep the tree green.

**Files:**
- Modify: `internal/backend/shared/manifest/manifest.go` (around lines 647–680 where `ParsePayload` lives)

- [ ] **Step 2.1: Add `ParseStackPayload` next to `ParsePayload`.**

```go
// ParseStackPayload parses a stack-format manifest payload. The payload MUST
// contain a top-level "services" key. Returns an error for any other shape
// (including the legacy flat single-service form) — that form has been removed.
//
// Unknown top-level fields are rejected.
func ParseStackPayload(data []byte) (*StackManifest, error) {
    if len(data) == 0 {
        return nil, fmt.Errorf("empty payload")
    }
    var probe map[string]json.RawMessage
    if err := json.Unmarshal(data, &probe); err != nil {
        return nil, fmt.Errorf("invalid JSON: %w", err)
    }
    if _, ok := probe["services"]; !ok {
        return nil, fmt.Errorf("payload missing required top-level 'services' key (the legacy single-service flat manifest format is no longer accepted)")
    }
    dec := json.NewDecoder(bytes.NewReader(data))
    dec.DisallowUnknownFields()
    var stack StackManifest
    if err := dec.Decode(&stack); err != nil {
        return nil, fmt.Errorf("parse stack manifest: %w", err)
    }
    if err := stack.Validate(); err != nil {
        return nil, fmt.Errorf("validate stack manifest: %w", err)
    }
    return &stack, nil
}
```

(Adjust imports as needed: `bytes` and `encoding/json` are already in use elsewhere in this file.)

- [ ] **Step 2.2: Run the new manifest tests; they should pass.**

```bash
go test ./internal/backend/shared/manifest/ -run TestParseStackPayload -v
```

Expected: PASS.

- [ ] **Step 2.3: Run the full manifest test suite to confirm no regression.**

```bash
go test ./internal/backend/shared/manifest/ -v
```

Expected: PASS for all existing tests (we haven't deleted anything yet).

- [ ] **Step 2.4: Commit.**

```bash
git add internal/backend/shared/manifest/manifest.go
git commit -m "feat(manifest): add ParseStackPayload (stack-only parser)"
```

---

## Task 3: Add lease-item validation requiring ServiceName

**Goal:** Centralize the "every item must carry `service_name`" check in one helper used by `ProvisionRequest`, `UpdateRequest`, and the API handler. Wire it into the provision flow (the validation is exercised at the gate, before any docker work).

**Files:**
- Modify: `internal/backend/client.go` (after the `LeaseItem` struct)

- [ ] **Step 3.1: Add `ValidateProvisionRequest` in `internal/backend/client.go`.**

```go
// ValidateProvisionRequest enforces the post-cutover contract: every lease item
// must carry a non-empty ServiceName. Callers should invoke this before any
// docker work or pool allocation.
//
// On-chain enforcement is expected to make this redundant; this is fred's
// defense-in-depth check.
func ValidateProvisionRequest(req ProvisionRequest) error {
    if len(req.Items) == 0 {
        return fmt.Errorf("%w: provision request has no items", ErrValidation)
    }
    for i, item := range req.Items {
        if item.ServiceName == "" {
            return fmt.Errorf("%w: items[%d] missing service_name (legacy single-service leases are no longer supported)", ErrInvalidManifest, i)
        }
    }
    return nil
}
```

(If `ErrValidation` / `ErrInvalidManifest` are exported from a different file in this package, no import change is needed.)

- [ ] **Step 3.2: Run the validation test added in Task 1.**

```bash
go test ./internal/backend/ -run TestProvisionRequest_RejectsItemWithoutServiceName -v
```

Expected: PASS.

- [ ] **Step 3.3: Run full `internal/backend/` test suite.**

```bash
go test ./internal/backend/ -v
```

Expected: PASS.

- [ ] **Step 3.4: Commit.**

```bash
git add internal/backend/client.go
git commit -m "feat(backend): add ValidateProvisionRequest (require service_name)"
```

---

## Task 4: Switch provision.go to the unified path

**Goal:** Replace `ParsePayload` + `IsStack` branching with `ValidateProvisionRequest` + `ParseStackPayload`. Always go through `doProvisionStack`. The legacy `doProvision` body is gutted but the function still exists (deletion happens in Task 11) — we redirect to the stack path so any stray caller still works.

**Files:**
- Modify: `internal/backend/docker/provision.go` (lines 120–230, 421-, and the legacy `doProvision` body 608-876)

- [ ] **Step 4.1: At `provision.go:120–230`, replace the entry-point branching:**

Replace this block:

```go
    // Parse payload — auto-detects single manifest vs stack manifest.
    isStack, err := backend.IsStack(req.Items)
    ... (legacy + stack branches) ...
    work := func() (...) {
        if isStack {
            return b.doProvisionStack(provCtx, req, stackManifest, profiles, logger)
        }
        return b.doProvision(provCtx, req, m, profiles, logger)
    }
```

with:

```go
    // Validate items first — every lease item must carry service_name.
    if err := backend.ValidateProvisionRequest(req); err != nil {
        b.removeProvision(req.LeaseUUID)
        return err
    }

    // Parse payload as a stack manifest (the only accepted format).
    stackManifest, err := manifest.ParseStackPayload(req.Payload)
    if err != nil {
        b.removeProvision(req.LeaseUUID)
        return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
    }

    if err := manifest.ValidateStackAgainstItems(stackManifest, req.Items); err != nil {
        b.removeProvision(req.LeaseUUID)
        return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
    }
    for svcName, svc := range stackManifest.Services {
        if err := shared.ValidateImage(svc.Image, b.cfg.AllowedRegistries); err != nil {
            b.removeProvision(req.LeaseUUID)
            return fmt.Errorf("%w: service %s: %w", backend.ErrValidation, svcName, err)
        }
    }

    // Allocation IDs are always service-aware now.
    var allocatedIDs []string
    for _, item := range req.Items {
        for i := range item.Quantity {
            instanceID := fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i)
            if err := b.pool.TryAllocate(instanceID, item.SKU, req.Tenant); err != nil {
                for _, id := range allocatedIDs {
                    b.pool.Release(id)
                }
                b.removeProvision(req.LeaseUUID)
                return fmt.Errorf("%w: %w", backend.ErrInsufficientResources, err)
            }
            allocatedIDs = append(allocatedIDs, instanceID)
        }
    }

    b.provisionsMu.Lock()
    if prov, ok := b.provisions[req.LeaseUUID]; ok {
        prov.SKU = req.RoutingSKU()
        prov.Items = req.Items
        prov.StackManifest = stackManifest
    }
    b.provisionsMu.Unlock()

    provCtx, provCancel := b.shutdownAwareContext()
    work := func() (string, leasesm.ProvisionSuccessResult, map[string]string, error) {
        return b.doProvisionStack(provCtx, req, stackManifest, profiles, logger)
    }
```

- [ ] **Step 4.2: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 4.3: Run docker backend tests.**

```bash
go test ./internal/backend/docker/ -count=1
```

Expected: PASS. (Some legacy-format tests may now fail; that's OK — capture the list and revisit in Task 13. If failures look like core regressions in the stack path, stop and investigate.)

- [ ] **Step 4.4: Commit.**

```bash
git add internal/backend/docker/provision.go
git commit -m "refactor(docker): route all provisions through the stack path"
```

---

## Task 5: Switch restart_update.go to the unified path

**Goal:** Same as Task 4 for Restart and Update flows.

**Files:**
- Modify: `internal/backend/docker/restart_update.go` (lines 90–161 for Restart, 830–972 for Update)

- [ ] **Step 5.1: At `restart_update.go:90-161`, replace the Restart branching:**

Replace the `isStack := prov.IsStack()` … `if isStack { return b.doRestartStack(...) }` … `return b.doRestart(...)` block with an unconditional call to `doRestartStack` plus a guard that the persisted `StackManifest` is non-nil:

```go
    if prov.StackManifest == nil {
        return leasesm.ReplaceResult{Err: fmt.Errorf("internal error: provision %s has no stack manifest (legacy data?)", req.LeaseUUID)}
    }
    return b.doRestartStack(opCtx, req.LeaseUUID, prov.StackManifest, containerIDs, serviceContainers, items, prevStatus, logger)
```

- [ ] **Step 5.2: At `restart_update.go:830-972`, same change for Update.**

Replace the `if isStack { ... } else { ... }` blocks. Use `ParseStackPayload` for the incoming new manifest and always call `doUpdateStack`.

```go
    stackManifest, err := manifest.ParseStackPayload(req.Payload)
    if err != nil {
        return leasesm.ReplaceResult{Err: fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)}
    }
    if err := manifest.ValidateStackAgainstItems(stackManifest, prov.Items); err != nil {
        return leasesm.ReplaceResult{Err: fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)}
    }
    return b.doUpdateStack(opCtx, req.LeaseUUID, stackManifest, profiles, oldContainerIDs, serviceContainers, items, prevStatus, logger)
```

- [ ] **Step 5.3: Build.**

```bash
go build ./...
```

Expected: success. If `m *manifest.Manifest` parameters in `doRestart`/`doUpdate` are now unreferenced, leave them — they're deleted in Task 11.

- [ ] **Step 5.4: Run restart/update tests.**

```bash
go test ./internal/backend/docker/ -run "Restart|Update" -count=1
```

Expected: PASS for stack-format tests; legacy-format tests will fail. Note any unexpected failures.

- [ ] **Step 5.5: Commit.**

```bash
git add internal/backend/docker/restart_update.go
git commit -m "refactor(docker): route all restarts/updates through the stack path"
```

---

## Task 6: Drop the isStack branch from deprovision.go

**Goal:** Deprovision logic was already mostly shared; only the volume-cleanup branch differs.

**Files:**
- Modify: `internal/backend/docker/deprovision.go` (lines 67, 80, 110, 143–155)

- [ ] **Step 6.1: At `deprovision.go:67`, delete `isStack := prov.IsStack()`. Replace all uses below with the stack-form code.**

The remaining stack-aware code is correct; what was the "legacy" branch:

```go
    } else {
        volumeID := fmt.Sprintf("fred-%s-%d", leaseUUID, i)
        ...
    }
```

is deleted entirely. The stack branch above it stays and becomes unconditional.

- [ ] **Step 6.2: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 6.3: Run deprovision tests.**

```bash
go test ./internal/backend/docker/ -run "Deprovision" -count=1
```

Expected: PASS.

- [ ] **Step 6.4: Commit.**

```bash
git add internal/backend/docker/deprovision.go
git commit -m "refactor(docker): drop legacy branch from deprovision"
```

---

## Task 7: Fail-fast on legacy containers in recover.go

**Goal:** Implement the migration-guard behavior: refuse to recover any container without `fred.service_name`.

**Files:**
- Modify: `internal/backend/docker/recover.go` (lines 38–183)

- [ ] **Step 7.1: At the top of the loop body in `recoverState` (after the `LeaseUUID == ""` skip at line 42), add the legacy detection.**

```go
        if c.ServiceName == "" {
            // Legacy single-service container — refuse to start. Operators
            // must drain all legacy leases before upgrading. See
            // docs/superpowers/specs/2026-05-15-unify-manifest-on-compose-design.md
            // for the upgrade runbook.
            return fmt.Errorf(
                "legacy container detected: container %s (lease %s) has no fred.service_name label; "+
                    "the docker backend no longer supports the legacy single-service path. "+
                    "Drain this lease (operator deprovision) and remove the container, then restart fred",
                c.ContainerID, c.LeaseUUID,
            )
        }
```

- [ ] **Step 7.2: Delete the now-unreachable legacy branch (recover.go:135-145) and the legacy allocation-ID branch (recover.go:172-174). The "stack" branches become unconditional.**

After this change, recovery only constructs `prov.ServiceContainers` and the service-aware allocation ID format.

- [ ] **Step 7.3: Run the legacy-reject test.**

```bash
go test ./internal/backend/docker/ -run TestRecoverState_RejectsLegacyContainer -v
```

Expected: PASS.

- [ ] **Step 7.4: Run the recovery test suite.**

```bash
go test ./internal/backend/docker/ -run "Recover" -count=1
```

Expected: stack-format recovery tests PASS; legacy-format tests fail (rebaselined in Task 13).

- [ ] **Step 7.5: Commit.**

```bash
git add internal/backend/docker/recover.go
git commit -m "refactor(docker): fail-fast on legacy containers during recover"
```

---

## Task 8: Unify volume binds

**Goal:** Delete `setupVolumeBinds`. Rename `setupStackVolBinds` → `setupVolBinds`. Update all callers.

**Files:**
- Modify: `internal/backend/docker/provision.go` (lines 421–516 for `setupVolumeBinds`; lines 517–605 for `setupStackVolBinds`)

- [ ] **Step 8.1: Delete `setupVolumeBinds` (provision.go:421–516).**

```bash
# Identify exact line range with: grep -n "^func.*setupVolumeBinds\|^}$" internal/backend/docker/provision.go
```

Delete the function. There are no remaining callers — confirm with:

```bash
grep -rn "setupVolumeBinds\b" internal/backend/docker/ --include="*.go"
```

If any references remain (other than the function itself before deletion), stop and investigate.

- [ ] **Step 8.2: Rename `setupStackVolBinds` → `setupVolBinds`.**

```bash
grep -rln "setupStackVolBinds" internal/backend/docker/ --include="*.go" | xargs sed -i 's/setupStackVolBinds/setupVolBinds/g'
```

- [ ] **Step 8.3: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 8.4: Run volume tests.**

```bash
go test ./internal/backend/docker/ -run "Volume" -count=1
```

Expected: PASS.

- [ ] **Step 8.5: Commit.**

```bash
git add internal/backend/docker/
git commit -m "refactor(docker): collapse volume bind helpers into one"
```

---

## Task 9: Unify container and volume naming

**Goal:** Drop the dual container-name branch in `lifecycle.go` and `backend.go`. Drop the dual volume-ID branch in `provision.go`.

**Files:**
- Modify: `internal/backend/docker/lifecycle.go:1048-1050`
- Modify: `internal/backend/docker/backend.go:616-644` (`prevContainerName` and friends)
- Modify: `internal/backend/docker/provision.go:429` and `restart_update.go:746`

- [ ] **Step 9.1: At `lifecycle.go:1048-1050`, replace:**

```go
if params.ServiceName != "" {
    containerName = fmt.Sprintf("fred-%s-%s-%d", params.LeaseUUID, params.ServiceName, params.InstanceIndex)
} else {
    containerName = fmt.Sprintf("fred-%s-%d", params.LeaseUUID, params.InstanceIndex)
}
```

with:

```go
if params.ServiceName == "" {
    return nil, fmt.Errorf("internal error: container create requested without service_name (lease %s)", params.LeaseUUID)
}
containerName = fmt.Sprintf("fred-%s-%s-%d", params.LeaseUUID, params.ServiceName, params.InstanceIndex)
```

- [ ] **Step 9.2: Update `prevContainerName` in `backend.go:616-619` to take a service name.**

```go
func prevContainerName(leaseUUID, serviceName string, instanceIndex int) string {
    return fmt.Sprintf("fred-%s-%s-%d-prev", leaseUUID, serviceName, instanceIndex)
}
```

Fix all call sites (grep): `restart_update.go:727` etc.

- [ ] **Step 9.3: Update legacy volume-ID call sites at `provision.go:429`, `provision.go:777`, `restart_update.go:746`, `deprovision.go:155` (already handled in Task 6). Each must now use `fred-{uuid}-{svc}-{idx}`. If after Tasks 4-6 those sites are gone, this step is a no-op.**

Verify with:

```bash
grep -rn 'fred-%s-%d' internal/backend/docker/ --include="*.go" | grep -v '_test.go'
```

Expected: no matches (or only matches in deleted-in-Task-11 code).

- [ ] **Step 9.4: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 9.5: Run integration tests.**

```bash
go test ./internal/backend/docker/ -run "Integration|Restart|Update" -count=1
```

Expected: stack-format tests PASS.

- [ ] **Step 9.6: Commit.**

```bash
git add internal/backend/docker/
git commit -m "refactor(docker): use service-aware names everywhere"
```

---

## Task 10: Unify LeaseInfo response shape

**Goal:** Always populate `LeaseInfo.Services`. Derive `LeaseInfo.Instances` from services (flatten by service-name order).

**Files:**
- Modify: `internal/backend/docker/info.go`

- [ ] **Step 10.1: In `info.go`, find the branch where `Instances` and `Services` are populated separately. Replace with: build `Services` always, then flatten into `Instances`.**

```go
// Build Services map first.
info.Services = make(map[string]backend.LeaseService, len(prov.ServiceContainers))
for svcName, cids := range prov.ServiceContainers {
    svcInstances := make([]backend.LeaseInstance, 0, len(cids))
    for i, cid := range cids {
        inst, err := b.buildInstanceForContainer(ctx, cid, i)
        if err != nil {
            return backend.LeaseInfo{}, err
        }
        svcInstances = append(svcInstances, inst)
    }
    info.Services[svcName] = backend.LeaseService{Instances: svcInstances}
}

// Derive flattened Instances view, in deterministic service-name order.
svcNames := slices.Sorted(maps.Keys(info.Services))
for _, name := range svcNames {
    info.Instances = append(info.Instances, info.Services[name].Instances...)
}
```

(`buildInstanceForContainer` is illustrative — use whatever helper already populates a `LeaseInstance` from a container ID.)

- [ ] **Step 10.2: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 10.3: Run info tests.**

```bash
go test ./internal/backend/docker/ -run "Info" -count=1
```

Expected: PASS.

- [ ] **Step 10.4: Commit.**

```bash
git add internal/backend/docker/info.go
git commit -m "refactor(docker): always populate LeaseInfo.Services; derive Instances"
```

---

## Task 11: Delete the legacy function bodies and parameters

**Goal:** Now that nothing routes to them, delete `doProvision` (legacy), `doRestart`, `doUpdate`. Rename `doProvisionStack` → `doProvision`, `doRestartStack` → `doRestart`, `doUpdateStack` → `doUpdate`. Strip the `m *manifest.Manifest` parameter from these signatures (everything now takes `*manifest.StackManifest`).

**Files:**
- Modify: `internal/backend/docker/provision.go` (delete lines 608–876)
- Modify: `internal/backend/docker/restart_update.go` (delete `doRestart`, `doUpdate` and rename)

- [ ] **Step 11.1: Delete legacy `doProvision` (provision.go:608–876).**

```bash
# Locate the function span first:
grep -n "^func (b \*Backend) doProvision\b\|^func (b \*Backend) doProvisionStack" internal/backend/docker/provision.go
```

Delete the legacy `doProvision` function entirely (between its declaration and the next top-level declaration).

- [ ] **Step 11.2: Rename `doProvisionStack` → `doProvision`.**

```bash
grep -rln "doProvisionStack" internal/backend/docker/ --include="*.go" | xargs sed -i 's/doProvisionStack/doProvision/g'
```

- [ ] **Step 11.3: Delete legacy `doRestart` and `doUpdate` in `restart_update.go`.**

```bash
grep -n "^func (b \*Backend) doRestart\b\|^func (b \*Backend) doRestartStack\|^func (b \*Backend) doUpdate\b\|^func (b \*Backend) doUpdateStack" internal/backend/docker/restart_update.go
```

Delete the two legacy functions; rename the two stack functions:

```bash
sed -i 's/doRestartStack/doRestart/g; s/doUpdateStack/doUpdate/g' internal/backend/docker/restart_update.go
```

- [ ] **Step 11.4: Build.**

```bash
go build ./...
```

Expected: success. If any reference to a deleted function lingers, fix it (most likely a test that was importing the legacy symbol — those tests get deleted in Task 13).

- [ ] **Step 11.5: Run the full docker backend test suite.**

```bash
go test ./internal/backend/docker/ -count=1
```

Expected: stack-format tests PASS. Legacy-format test failures expected (Task 13).

- [ ] **Step 11.6: Commit.**

```bash
git add internal/backend/docker/
git commit -m "refactor(docker): delete legacy doProvision/doRestart/doUpdate"
```

---

## Task 12: Delete `Manifest`, `ParsePayload`, and `IsStack`

**Goal:** Remove the now-unreferenced single-service type, the dual-format parser, and the stack-detection helper. Anywhere `prov.Image` was used as the single-service image field, replace with iteration over `prov.StackManifest.Services`.

**Files:**
- Modify: `internal/backend/shared/manifest/manifest.go`
- Modify: `internal/backend/client.go`
- Modify: `internal/backend/shared/leasesm/leasesm.go`, `lease_sm.go`
- Modify: `internal/backend/docker/info.go`, `recover.go` (clean up `prov.Image` references)

- [ ] **Step 12.1: Confirm no production caller of `Manifest` (the struct) or `ParsePayload` remains.**

```bash
grep -rn "manifest\.Manifest\b\|manifest\.ParsePayload\b" --include="*.go" . | grep -v _test.go
```

Expected: no matches (or only matches inside the manifest package itself). If matches exist, stop and update those callers.

- [ ] **Step 12.2: Delete the `Manifest` struct, its validators, and `ParsePayload` from `internal/backend/shared/manifest/manifest.go`.**

The single-manifest validator block (around lines 1–500) and `ParsePayload` (around lines 647–680) are removed. Stack-manifest types and validators stay.

- [ ] **Step 12.3: Delete `backend.IsStack` from `internal/backend/client.go` (the function at lines 121–132).**

- [ ] **Step 12.4: Grep for callers of `IsStack` — all should be gone.**

```bash
grep -rn "\.IsStack\b\|backend\.IsStack\b" --include="*.go" .
```

Expected: no matches. If matches remain, update them (they're all now redundant — replace with the unconditional code path).

- [ ] **Step 12.5: Drop `prov.Image` field from `ProvisionState` (`shared/leasesm/leasesm.go`). Any caller reading the "lease's single image" must now iterate `prov.StackManifest.Services` or `prov.ServiceContainers` keys. For `info.go`'s `ProvisionInfo.Image` (designed as a top-level convenience for single-service leases), replace with reading the first service in deterministic order, or leave empty for now and rely on `ServiceImages`.**

```bash
grep -rn "prov\.Image\b\|ProvisionState.Image\b" --include="*.go" .
```

Expected: small list. Update each.

- [ ] **Step 12.6: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 12.7: Run the full test suite.**

```bash
go test ./... -count=1
```

Expected: stack-format tests PASS. Legacy-format tests fail (Task 13).

- [ ] **Step 12.8: Commit.**

```bash
git add internal/
git commit -m "refactor: delete Manifest, ParsePayload, IsStack (legacy single-service)"
```

---

## Task 13: Rebaseline the test suite

**Goal:** Delete tests that exercise the legacy path. Convert any "single-service smoke test" still considered valuable into a 1-service-stack test. The new contract is: there is no single-service path to exercise.

**Files:**
- Modify: various `*_test.go` under `internal/backend/docker/`
- Modify: `internal/backend/shared/manifest/*_test.go`
- Modify: `internal/backend/client_test.go`

- [ ] **Step 13.1: Inventory failing tests.**

```bash
go test ./... -count=1 2>&1 | tee /tmp/test-failures.txt
grep -E "^--- FAIL|FAIL\s" /tmp/test-failures.txt
```

For each failure, classify:
- (A) Legacy-format test that should be deleted.
- (B) A test that was using `ParsePayload` and just needs to be rewritten to use `ParseStackPayload` with a stack-format input.
- (C) A genuine regression that needs investigation.

- [ ] **Step 13.2: Delete category (A) tests.** Walk the list from Step 13.1 and remove tests whose names mention `Legacy`, `SingleService`, or whose body constructs a flat-manifest payload as the primary fixture.

- [ ] **Step 13.3: Rewrite category (B) tests.** Common pattern:

```go
// before
m, _, err := manifest.ParsePayload([]byte(`{"image":"nginx"}`))
// after
sm, err := manifest.ParseStackPayload([]byte(`{"services":{"app":{"image":"nginx"}}}`))
```

For tests asserting that single-service `LeaseInfo.Instances` matches a fixed shape, update them to construct a 1-service stack and assert against `Services["app"].Instances` (and the flattened `Instances` view).

- [ ] **Step 13.4: Investigate category (C) failures.** These are the real risk: a stack-path bug that the legacy path was masking. Fix or report; do not silence.

- [ ] **Step 13.5: Run the full test suite.**

```bash
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 13.6: Run vet and any project linters.**

```bash
go vet ./...
make lint || true   # if the project defines one
```

Expected: clean.

- [ ] **Step 13.7: Commit.**

```bash
git add internal/
git commit -m "test: rebaseline suites for unified compose path"
```

---

## Task 14: Update docs and JSON schema

**Goal:** Bring tenant-facing documentation in line with the new contract.

**Files:**
- Modify: `docs/manifest-guide.md`
- Modify: `docs/manifest-schema.json`

- [ ] **Step 14.1: Rewrite `docs/manifest-guide.md`.**

Sections to update:
- "Manifest Formats" → keep only the Stack section. Remove "Single-Service Manifest" entirely. Remove "Format auto-detection" paragraph (manifest-guide.md:85).
- "DockerManifest Fields" → rename to "Service Fields" (these are the fields of each entry under `services`).
- "Stack-Specific Topics" → remove the "-Specific" qualifier; this is the only model.
- "Common Mistakes" → remove the "Using `depends_on` in single manifest" row (it can't happen anymore). Add: "Submitting a flat manifest without `services` → `payload missing required top-level 'services' key`".
- "Examples" → rewrite the minimal example as a 1-service stack:

```json
{
  "services": {
    "app": {
      "image": "nginx:latest"
    }
  }
}
```

- [ ] **Step 14.2: Tighten `docs/manifest-schema.json`.**

Replace the union/oneOf at the top level with a single object that requires `services`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "FredManifest",
  "type": "object",
  "required": ["services"],
  "additionalProperties": false,
  "properties": {
    "services": {
      "type": "object",
      "minProperties": 1,
      "additionalProperties": { "$ref": "#/$defs/Service" }
    }
  },
  "$defs": {
    "Service": { ... existing per-service schema ... }
  }
}
```

- [ ] **Step 14.3: Run the schema test if one exists.**

```bash
ls docs/manifest-schema_test.py 2>/dev/null && python3 docs/manifest-schema_test.py || true
```

Expected: PASS, or no test to run.

- [ ] **Step 14.4: Commit.**

```bash
git add docs/manifest-guide.md docs/manifest-schema.json
git commit -m "docs: stack-only manifest format"
```

---

## Task 15: Final verification

**Goal:** End-to-end check that the tree is healthy and nothing legacy remains.

- [ ] **Step 15.1: Full build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 15.2: Full test run with race detector.**

```bash
go test -race -count=1 ./...
```

Expected: PASS.

- [ ] **Step 15.3: Vet and (if defined) lint.**

```bash
go vet ./...
make lint
```

Expected: clean.

- [ ] **Step 15.4: Sweep for legacy strings to confirm full removal.**

```bash
grep -rn "IsStack\|ParsePayload\|setupVolumeBinds\|setupStackVolBinds\|doProvisionStack\|doRestartStack\|doUpdateStack" --include="*.go" .
```

Expected: no matches.

```bash
grep -rn '"fred-%s-%d"' --include="*.go" .
```

Expected: no matches (all naming is now service-aware).

- [ ] **Step 15.5: Run any integration test target the project defines.**

```bash
make test-integration
make test-integration-stack
make test-integration-volume
make test-integration-restart-update
```

Expected: PASS.

- [ ] **Step 15.6: Manual smoke (optional, with a local docker daemon).**

```bash
# Build the docker-backend binary
make build-docker
# Submit a sample 1-service stack manifest via the provisioner test harness
go run ./scripts/create-test-leases.go --services app:nginx:latest
```

Expected: container `fred-<uuid>-app-0` running.

- [ ] **Step 15.7: Final commit (release notes).**

Append a release note to `CHANGELOG.md` (or create one if absent):

```markdown
## Unreleased

### Breaking

- The Docker backend no longer accepts flat single-service manifests. All manifests must use the stack format (`{"services": {...}}`).
- Lease items must carry `service_name`.
- Upgrading from a prior version requires draining all active leases. fred refuses to start if any container with `fred.lease_uuid` but no `fred.service_name` is found.
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): note breaking unification of manifest path"
```

---

## Self-review checklist

- **Spec coverage:** Every section of the spec maps to a task:
  - Manifest model → Tasks 2, 12.
  - Lease item model → Tasks 3, 12.
  - Container provisioning → Tasks 4, 5, 6, 11.
  - Allocation IDs/names → Task 9.
  - LeaseInfo shape → Task 10.
  - Volume binds → Task 8.
  - Recovery → Task 7.
  - Validation rules → covered by Task 2 (parsing) + Task 3 (items).
  - Affected files table → covered across Tasks 4–14.
  - Migration runbook → reflected in Task 7 (the fail-fast guard) and Task 15.7 (release notes).
- **Placeholder scan:** no TBD/TODO; every step has either code or an exact command.
- **Type consistency:** `StackManifest` is the kept name (not renamed to `Manifest`); `ParseStackPayload` is the new entry point; `ValidateProvisionRequest` is the validation helper; `doProvision`/`doRestart`/`doUpdate` are the *only* function names after Task 11 (the `Stack` suffix is removed via rename).
