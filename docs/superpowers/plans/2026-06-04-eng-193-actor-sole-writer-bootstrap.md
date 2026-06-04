# ENG-193 — Type-enforced actor-sole-writer boundary for bootstrap provision writes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route every live `*provision` construction in the two bootstrap paths (`recover.go`, `provision.go`) through one typed value (`recoveredProvision`) + one constructor (`materialize`), so "the actor is the sole writer of a published provision" holds by construction — behavior-preserving, plus an `Items` aliasing fix and an explicit `Deprovisioning` recovery case.

**Architecture:** Add a `recoveredProvision` value type (embeds `leasesm.ProvisionState` + the `VolumeCleanupAttempts` wrapper field) and a `materialize() *provision` constructor — the only path into `b.provisions`. Recovery accumulates `*recoveredProvision` off-map and materializes rebuilt entries during the existing atomic whole-map swap (preserving live entries by pointer). Creation reserves via `materialize` and enriches in place through a controlled `enrichReserved` mutator that deep-copies `Items`. Field-completeness is enforced by the `exhaustruct` linter scoped to the construction literals.

**Tech Stack:** Go; `internal/backend/docker` (the bootstrap paths + tests); `.golangci.yml` (the `exhaustruct` linter).

**Spec:** `docs/superpowers/specs/2026-06-04-eng-193-actor-sole-writer-bootstrap-design.md`

---

## File Structure

| File | Change |
|------|--------|
| `internal/backend/docker/recovered_provision.go` (new) | `recoveredProvision` type, `materialize()`, `recoveredFromProvision()`. |
| `internal/backend/docker/recovered_provision_test.go` (new) | `materialize` completeness + `recoveredFromProvision` clone tests. |
| `internal/backend/docker/provision.go` | Reservation via `materialize`; `enrichReserved` (deep-copies `Items`) replaces the in-place enrichment. |
| `internal/backend/docker/recover.go` | Build loop accumulates `*recoveredProvision`; merge materializes rebuilt + preserves live + explicit `Deprovisioning`; `LastError` suffix via `UpdateFn`. |
| `internal/backend/docker/recover_state_test.go` | Characterization + Deprovisioning + concurrent-reader tests. |
| `internal/backend/docker/provision_test.go` | `Items` aliasing regression + mid-validation concurrent-reader test. |
| `.golangci.yml` | Enable `exhaustruct`, `exclude: ['.*']` (directive-only mode). |

---

## Task 1: `recoveredProvision` value type + `materialize()` + scoped `exhaustruct`

Pure addition — no call site changes yet. Establishes the typed constructor and the linter that guards field-completeness.

**Files:** `recovered_provision.go` (new), `recovered_provision_test.go` (new), `.golangci.yml`

- [ ] **Step 1: Write the failing test** — `internal/backend/docker/recovered_provision_test.go`

```go
package docker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// fullRecoveredProvision builds a recoveredProvision with every field set to a
// distinct non-zero value, so materialize round-tripping can be asserted
// field-by-field.
func fullRecoveredProvision() recoveredProvision {
	return recoveredProvision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         "lease-1",
			Tenant:            "tenant-a",
			ProviderUUID:      "prov-1",
			SKU:               "docker-small",
			Status:            backend.ProvisionStatusReady,
			Quantity:          2,
			CreatedAt:         time.Unix(1700000000, 0),
			FailCount:         3,
			LastError:         "boom",
			CallbackURL:       "http://cb",
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
			ContainerIDs:      []string{"c1", "c2"},
			StackManifest:     nil,
			ServiceContainers: map[string][]string{"app": {"c1", "c2"}},
		},
		volumeCleanupAttempts: 4,
	}
}

func TestRecoveredProvision_Materialize_RoundTripsEveryField(t *testing.T) {
	rec := fullRecoveredProvision()
	p := rec.materialize()
	require.NotNil(t, p)
	assert.Equal(t, rec.ProvisionState, p.ProvisionState, "ProvisionState must round-trip wholesale")
	assert.Equal(t, rec.volumeCleanupAttempts, p.VolumeCleanupAttempts, "wrapper field must round-trip")
}

func TestRecoveredFromProvision_ClonesReferenceFields(t *testing.T) {
	src := &provision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         "lease-1",
			Status:            backend.ProvisionStatusFailing,
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			ContainerIDs:      []string{"c1"},
			ServiceContainers: map[string][]string{"app": {"c1"}},
		},
		VolumeCleanupAttempts: 2,
	}
	rec := recoveredFromProvision(src)
	// Mutating the clone must not touch the source's backing arrays/maps.
	rec.Items[0].ServiceName = "mutated"
	rec.ContainerIDs[0] = "mutated"
	rec.ServiceContainers["app"][0] = "mutated"
	assert.Equal(t, "app", src.Items[0].ServiceName, "Items must be cloned")
	assert.Equal(t, "c1", src.ContainerIDs[0], "ContainerIDs must be cloned")
	assert.Equal(t, "c1", src.ServiceContainers["app"][0], "ServiceContainers must be deep-cloned")
	assert.Equal(t, 2, rec.volumeCleanupAttempts, "wrapper field carried")
}
```

- [ ] **Step 2: Run — verify it fails to compile** (`recoveredProvision` undefined)

Run: `go test ./internal/backend/docker/ -run 'TestRecoveredProvision|TestRecoveredFromProvision' 2>&1 | head`
Expected: FAIL — `undefined: recoveredProvision` / `undefined: recoveredFromProvision`.

- [ ] **Step 3: Create `internal/backend/docker/recovered_provision.go`**

```go
package docker

import (
	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// recoveredProvision is a fully-built, NOT-YET-PUBLISHED provision snapshot.
// It is the typed payload the bootstrap paths (recover.go, provision.go)
// construct off-map; it reaches b.provisions only via materialize() at the two
// publish points (the provision reservation and the recover swap). It has no
// method that mutates a map-resident *provision, so bootstrap code cannot
// publish a half-built pointer or hold a writable handle to a published
// *provision (ENG-193, ENG-229 category B).
//
// It embeds leasesm.ProvisionState exactly as *provision does (no
// method-promotion downside — ProvisionState has no methods), so materialize is
// a trivial struct copy. The construction literals carry a //exhaustruct:enforce
// directive so a newly-added ProvisionState/wrapper field forces every build
// site to set it.
type recoveredProvision struct {
	leasesm.ProvisionState
	volumeCleanupAttempts int
}

// materialize is the ONLY function that turns recovery/creation data into a
// heap *provision. Caller publishes the result into b.provisions under
// provisionsMu.
func (rec recoveredProvision) materialize() *provision {
	return &provision{ //exhaustruct:enforce
		ProvisionState:        rec.ProvisionState,
		VolumeCleanupAttempts: rec.volumeCleanupAttempts,
	}
}

// recoveredFromProvision snapshots a live *provision into an off-map value,
// deep-cloning the reference fields (Items, ContainerIDs, ServiceContainers)
// because worker goroutines re-point those headers off-actor; the materialized
// copy must not alias the live struct. Used by recover to rebuild a kept entry
// as a value instead of mutating the published struct in place.
func recoveredFromProvision(p *provision) recoveredProvision {
	rec := recoveredProvision{ //exhaustruct:enforce
		ProvisionState:        p.ProvisionState,
		volumeCleanupAttempts: p.VolumeCleanupAttempts,
	}
	rec.Items = append([]backend.LeaseItem(nil), p.Items...)
	rec.ContainerIDs = append([]string(nil), p.ContainerIDs...)
	if p.ServiceContainers != nil {
		sc := make(map[string][]string, len(p.ServiceContainers))
		for k, v := range p.ServiceContainers {
			sc[k] = append([]string(nil), v...)
		}
		rec.ServiceContainers = sc
	}
	return rec
}
```

- [ ] **Step 4: Run — green**

Run: `go test ./internal/backend/docker/ -run 'TestRecoveredProvision|TestRecoveredFromProvision' -count=1`
Expected: PASS.

- [ ] **Step 5: Enable the scoped `exhaustruct` linter** — edit `.golangci.yml`. Add `exhaustruct` to `linters.enable` (keep the list alphabetical-ish next to `errorlint`) and add the settings block. The `exclude: ['.*']` makes it directive-only: nothing is checked unless a literal carries `//exhaustruct:enforce`, so no other code or test literal is affected.

In `linters.enable:` add:
```yaml
    - exhaustruct
```
In `linters.settings:` add:
```yaml
    exhaustruct:
      # Directive-only mode: exclude every type globally so the linter checks
      # ONLY literals explicitly marked //exhaustruct:enforce (the recovered-
      # Provision / ProvisionState construction sites). Keeps the guard off the
      # rest of the repo and all test literals. (ENG-193)
      exclude:
        - '.*'
```

- [ ] **Step 6: Run the linter — verify the enforce directives pass**

Run: `golangci-lint run ./internal/backend/docker/ 2>&1 | head`
Expected: no `exhaustruct` issues (the `materialize`/`recoveredFromProvision` literals set all their fields). If `golangci-lint` is not on PATH, install the pinned version: `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.8.0` and run `"$(go env GOPATH)/bin/golangci-lint" run ./internal/backend/docker/`.

- [ ] **Step 7: Commit**

```bash
git add internal/backend/docker/recovered_provision.go internal/backend/docker/recovered_provision_test.go .golangci.yml
git commit -m "feat(docker-backend): add recoveredProvision value type + materialize seam (ENG-193)"
```

---

## Task 2: Route the create path through `materialize` + `enrichReserved` (Items aliasing fix)

Replace `provision.go`'s direct reservation literal with `materialize`, and the in-place enrichment with a controlled `enrichReserved` mutator that deep-copies `Items`. Behavior-preserving except the aliasing fix.

**Files:** `provision.go`, `provision_test.go`

- [ ] **Step 1: Write the failing aliasing-regression test** — append to `internal/backend/docker/provision_test.go`

```go
func TestProvision_EnrichReserved_DeepCopiesItems(t *testing.T) {
	// enrichReserved must not retain the caller's Items slice: NormalizeProvisionRequest
	// mutates req.Items[0] in place (client.go), so a stored alias would change
	// the published provision after the fact.
	p := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusProvisioning}}
	callerItems := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	p.enrichReserved("docker-small", callerItems, nil)
	// Mutate the caller's slice after enrichment.
	callerItems[0].ServiceName = "mutated"
	require.Len(t, p.Items, 1)
	assert.Equal(t, "app", p.Items[0].ServiceName, "stored Items must be a copy, not the caller's slice")
}
```

- [ ] **Step 2: Run — verify it fails to compile** (`enrichReserved` undefined)

Run: `go test ./internal/backend/docker/ -run TestProvision_EnrichReserved_DeepCopiesItems 2>&1 | head`
Expected: FAIL — `p.enrichReserved undefined`.

- [ ] **Step 3: Add `enrichReserved`** to `internal/backend/docker/recovered_provision.go` (it lives with the construction helpers)

```go
// enrichReserved sets the post-validation workload metadata on a reserved
// provision (the slot is a Provisioning marker). It is the ONLY place SKU /
// Items / StackManifest are written outside the actor; the caller holds
// b.provisionsMu. Items is deep-copied so the published provision does not
// alias the caller's request slice (NormalizeProvisionRequest mutates it in
// place).
func (p *provision) enrichReserved(sku string, items []backend.LeaseItem, sm *manifest.StackManifest) {
	p.SKU = sku
	p.Items = append([]backend.LeaseItem(nil), items...)
	p.StackManifest = sm
}
```

Add the `manifest` import to `recovered_provision.go`:
```go
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
```

- [ ] **Step 4: Replace the reservation literal** in `internal/backend/docker/provision.go`. Change the block at `:79-93` (`b.provisions[req.LeaseUUID] = &provision{...}`) to materialize from a `recoveredProvision`. Mark the `ProvisionState` literal `//exhaustruct:enforce` and set **every** field explicitly (deferred `SKU`/`Items`/`StackManifest`/`LastError`/`ServiceContainers` are explicit zero values, documenting the deferral):

```go
	b.provisions[req.LeaseUUID] = recoveredProvision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID:    req.LeaseUUID,
			Tenant:       req.Tenant,
			ProviderUUID: req.ProviderUUID,
			SKU:          "", // set by enrichReserved after validation
			Status:       backend.ProvisionStatusProvisioning,
			Quantity:     totalQuantity,
			CreatedAt:    time.Now(),
			FailCount:    prevFailCount,
			LastError:    "",
			CallbackURL:  req.CallbackURL, // MUST be set at reservation: a failure/Deprovision
			// racing this provision in the validation window resolves CallbackURL from the map.
			Items:             nil, // set by enrichReserved
			ContainerIDs:      make([]string, 0, totalQuantity),
			StackManifest:     nil, // set by enrichReserved
			ServiceContainers: nil,
		},
		// VolumeCleanupAttempts: 0 by struct-zero — structural reset of the
		// per-lease counter is the whole point of the wrapper.
		volumeCleanupAttempts: 0,
	}.materialize()
```

- [ ] **Step 5: Replace the in-place enrichment** in `internal/backend/docker/provision.go` at `:196-202`. Change:

```go
	b.provisionsMu.Lock()
	if prov, ok := b.provisions[req.LeaseUUID]; ok {
		prov.SKU = req.RoutingSKU()
		prov.Items = req.Items
		prov.StackManifest = stackManifest
	}
	b.provisionsMu.Unlock()
```
to:
```go
	b.provisionsMu.Lock()
	if prov, ok := b.provisions[req.LeaseUUID]; ok {
		prov.enrichReserved(req.RoutingSKU(), req.Items, stackManifest)
	}
	b.provisionsMu.Unlock()
```

- [ ] **Step 6: Run — aliasing test passes, provision suite stays green under -race**

Run: `go test ./internal/backend/docker/ -run 'TestProvision' -race -count=1`
Expected: PASS — the new aliasing test passes and all existing `TestProvision_*` stay green (behavior preserved). Also run the linter: `golangci-lint run ./internal/backend/docker/` — no `exhaustruct` issues (the reservation literal sets all 14 `ProvisionState` fields).

- [ ] **Step 7: Commit**

```bash
git add internal/backend/docker/provision.go internal/backend/docker/recovered_provision.go internal/backend/docker/provision_test.go
git commit -m "refactor(docker-backend): reserve+enrich the create path via materialize/enrichReserved, deep-copy Items (ENG-193)"
```

---

## Task 3: Characterization tests for `recoverState` (safety net before the refactor)

`recoverState`'s per-status merge is largely untested at the unit level. Write tests against the **current** code (they pass) so Task 4's refactor is provably behavior-preserving.

**Files:** `recover_state_test.go`

- [ ] **Step 1: Add a recover test helper + the characterization tests** — append to `internal/backend/docker/recover_state_test.go`

```go
// runRecover drives recoverState with a fixed set of managed containers and a
// pre-existing provisions map, returning the resulting b.provisions.
func runRecover(t *testing.T, existing map[string]*provision, containers []ContainerInfo) map[string]*provision {
	t.Helper()
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return containers, nil
		},
	}
	b := newBackendForTest(mock, existing)
	require.NoError(t, b.recoverState(context.Background()))
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	out := make(map[string]*provision, len(b.provisions))
	for k, v := range b.provisions {
		out[k] = v
	}
	return out
}

func TestRecoverState_ReadyFromRunningContainers(t *testing.T) {
	got := runRecover(t, nil, []ContainerInfo{
		{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "running", CallbackURL: "http://cb"},
	})
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusReady, p.Status)
	assert.Equal(t, []string{"c1"}, p.ContainerIDs)
	assert.Equal(t, 1, p.Quantity)
	assert.Equal(t, "http://cb", p.CallbackURL)
}

func TestRecoverState_ColdStartFailed_BumpsFailCountAndLastError(t *testing.T) {
	got := runRecover(t, nil, []ContainerInfo{
		{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "exited", FailCount: 2},
	})
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, p.Status)
	assert.Equal(t, 3, p.FailCount, "cold-start increments the label FailCount")
	assert.Equal(t, leasesm.ErrMsgContainerExited, p.LastError)
}

func TestRecoverState_InFlightProvisioning_PreservedNoContainers(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusProvisioning, FailCount: 4}},
	}
	got := runRecover(t, existing, nil) // no containers
	p, ok := got["L1"]
	require.True(t, ok, "in-flight provisioning entry must survive recovery")
	assert.Equal(t, backend.ProvisionStatusProvisioning, p.Status)
	assert.Equal(t, 4, p.FailCount)
	assert.Same(t, existing["L1"], p, "in-flight entry is preserved by pointer")
}

func TestRecoverState_FailingNormalizedToFailed(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusFailing, FailCount: 1, LastError: "x"}},
	}
	got := runRecover(t, existing, nil)
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, p.Status, "Failing normalizes to Failed")
	assert.Equal(t, 1, p.FailCount)
	assert.Equal(t, "x", p.LastError)
}

func TestRecoverState_FailedPreservedNoContainers(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusFailed, FailCount: 7}},
	}
	got := runRecover(t, existing, nil)
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, p.Status)
	assert.Equal(t, 7, p.FailCount)
}

func TestRecoverState_FailCountAntiRegression(t *testing.T) {
	// Existing in-memory FailCount (5) must win over the stale container label (2).
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusFailed, FailCount: 5}},
	}
	got := runRecover(t, existing, []ContainerInfo{
		{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "running", FailCount: 2},
	})
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, 5, p.FailCount, "higher in-memory FailCount must not regress to the label value")
}
```

- [ ] **Step 2: Run — all characterization tests PASS on current code**

Run: `go test ./internal/backend/docker/ -run 'TestRecoverState_(ReadyFromRunningContainers|ColdStartFailed|InFlightProvisioning|FailingNormalized|FailedPreserved|FailCountAntiRegression)' -race -count=1`
Expected: PASS — these lock the current recovery behavior. If any fails, STOP: the test misencodes current behavior; fix the test, not the code.

- [ ] **Step 3: Commit**

```bash
git add internal/backend/docker/recover_state_test.go
git commit -m "test(docker-backend): characterize recoverState per-status merge behavior (ENG-193)"
```

---

## Task 4: Refactor `recoverState` to construct via `recoveredProvision` (+ explicit Deprovisioning)

The build loop accumulates `*recoveredProvision` off-map; the merge materializes rebuilt entries into the swapped map and overlays preserved-live entries by pointer (no off-actor in-place mutation). Adds an explicit `Deprovisioning` preserve-case (the one intentional behavior change). Task 3 + the existing suite prove the rest is preserved.

**Files:** `recover.go`, `recover_state_test.go`

- [ ] **Step 1: Write the new-behavior Deprovisioning test** — append to `internal/backend/docker/recover_state_test.go`

```go
func TestRecoverState_DeprovisioningPreserved_NoContainers(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusDeprovisioning}},
	}
	got := runRecover(t, existing, nil) // containers already gone
	p, ok := got["L1"]
	require.True(t, ok, "a Deprovisioning lease must be preserved, not dropped")
	assert.Equal(t, backend.ProvisionStatusDeprovisioning, p.Status)
	assert.Same(t, existing["L1"], p, "preserved by pointer — the deprovision goroutine owns it")
}

func TestRecoverState_DeprovisioningPreserved_SurvivingContainers(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusDeprovisioning}},
	}
	got := runRecover(t, existing, []ContainerInfo{
		{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "running"},
	})
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusDeprovisioning, p.Status, "must NOT be resurrected to a container-derived status")
	assert.Same(t, existing["L1"], p)
}
```

- [ ] **Step 2: Run — verify the Deprovisioning tests FAIL on current code**

Run: `go test ./internal/backend/docker/ -run 'TestRecoverState_DeprovisioningPreserved' -count=1 2>&1 | tail`
Expected: FAIL — current code drops the no-container Deprovisioning entry and resurrects the surviving-container one (no Deprovisioning case in the merge switch). This is the intentional behavior change.

- [ ] **Step 3: Convert the build loop to accumulate `*recoveredProvision`** in `internal/backend/docker/recover.go`. At `:81` change:
```go
	recovered := make(map[string]*provision)
```
to:
```go
	building := make(map[string]*recoveredProvision)
```
In the build loop, change the lookup/insert at `:123-162`. Replace:
```go
		prov, exists := recovered[c.LeaseUUID]
		if !exists {
			prov = &provision{
				ProvisionState: leasesm.ProvisionState{
					LeaseUUID:    c.LeaseUUID,
					Tenant:       c.Tenant,
					ProviderUUID: c.ProviderUUID,
					SKU:          c.SKU,
					Status:       containerStatusToProvisionStatus(c.Status),
					CreatedAt:    c.CreatedAt,
					FailCount:    c.FailCount,
					CallbackURL:  c.CallbackURL,
					ContainerIDs: make([]string, 0),
				},
			}
```
with:
```go
		prov, exists := building[c.LeaseUUID]
		if !exists {
			prov = &recoveredProvision{ //exhaustruct:enforce
				ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
					LeaseUUID:    c.LeaseUUID,
					Tenant:       c.Tenant,
					ProviderUUID: c.ProviderUUID,
					SKU:          c.SKU,
					Status:       containerStatusToProvisionStatus(c.Status),
					Quantity:     0,  // set from ContainerIDs below
					CreatedAt:    c.CreatedAt,
					FailCount:    c.FailCount,
					LastError:    "", // populated by cold-start/transition logic below
					CallbackURL:  c.CallbackURL,
					Items:             nil, // rebuilt from labels below
					ContainerIDs:      make([]string, 0),
					StackManifest:     nil, // restored below
					ServiceContainers: nil, // rebuilt from labels below
				},
				volumeCleanupAttempts: 0,
			}
```
Then change the insert at `:161` from `recovered[c.LeaseUUID] = prov` to `building[c.LeaseUUID] = prov`. The rest of the loop (`prov.ContainerIDs = append(...)`, `prov.ServiceContainers`, `prov.Items`, `prov.FailCount`, `prov.Status`, `prov.StackManifest`) is unchanged — every field is promoted through the embedded `ProvisionState`.

- [ ] **Step 4: Replace the merge + swap block** (`:250-341`, from the `// Merge with existing state` comment through `b.provisions = recovered`) with:

```go
	// Merge with existing state and detect status transitions.
	b.provisionsMu.Lock()

	// Detect ready→failed transitions: containers that were running but have
	// since crashed. We hand off to the SM by firing containerDiedMsg on the
	// actor *after* the merge. Status stays Ready in the building value so the
	// actor's guard sees the pre-transition state and permits evContainerDied;
	// FailCount and LastError are populated by the SM's Failing entry action.
	var failedLeases []string
	for uuid, existing := range b.provisions {
		if existing.Status == backend.ProvisionStatusReady {
			if rec, ok := building[uuid]; ok && rec.Status == backend.ProvisionStatusFailed {
				rec.Status = backend.ProvisionStatusReady
				rec.FailCount = existing.FailCount
				rec.LastError = existing.LastError
				failedLeases = append(failedLeases, uuid)
				b.logger.Warn("container crashed after provisioning",
					"lease_uuid", uuid,
					"tenant", existing.Tenant,
				)
			}
		}
	}

	// Cold-start correction: provisions recovered as failed with no prior
	// in-memory state carry a creation-time FailCount label. Increment it to
	// account for the failure evidenced by the dead container. The baseline
	// LastError rides the materialized value.
	var coldStartFailed []string
	for uuid, rec := range building {
		if rec.Status == backend.ProvisionStatusFailed {
			if _, hasExisting := b.provisions[uuid]; !hasExisting {
				rec.FailCount++
				rec.LastError = leasesm.ErrMsgContainerExited
				coldStartFailed = append(coldStartFailed, uuid)
				b.logger.Info("cold-start: adjusted FailCount for already-failed provision",
					"lease_uuid", uuid,
					"fail_count", rec.FailCount,
				)
			}
		}
	}

	// FailCount anti-regression on rebuilt entries: a re-list after an in-memory
	// increment would otherwise regress FailCount to the stale label. Preserve
	// the higher in-memory value. Skipped for in-flight statuses (preserved
	// wholesale below).
	for uuid, rec := range building {
		existing, ok := b.provisions[uuid]
		if !ok {
			continue
		}
		switch existing.Status {
		case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
			// preserved wholesale below
		default:
			if existing.FailCount > rec.FailCount {
				rec.FailCount = existing.FailCount
			}
		}
	}

	// Publish: materialize every rebuilt entry into a fresh *provision (the only
	// path a recoveredProvision reaches b.provisions). A fresh struct clears
	// stale fields (LastError, VolumeCleanupAttempts) exactly as the prior
	// fresh-&provision{}+swap did.
	final := make(map[string]*provision, len(building))
	for uuid, rec := range building {
		final[uuid] = rec.materialize()
	}

	// Overlay existing entries that must be preserved: the actor / deprovision
	// goroutine owns their live state, so reuse the live *provision pointer
	// (no off-actor field mutation).
	for uuid, existing := range b.provisions {
		if _, hasContainers := building[uuid]; hasContainers {
			switch existing.Status {
			case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
				// In-flight re-provision: the rebuilt containers belong to the
				// previous (failed) provision; keep the in-flight entry so the
				// next container creation picks up the right FailCount.
				final[uuid] = existing
			case backend.ProvisionStatusDeprovisioning:
				// The deprovision goroutine owns this lease; do not resurrect it
				// to a container-derived status (ENG-193 explicit case).
				final[uuid] = existing
			}
			continue
		}
		switch existing.Status {
		case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
			// In-flight operation that hasn't produced containers yet.
			final[uuid] = existing
		case backend.ProvisionStatusFailing:
			// Failing is transient (container-death detected, diag goroutine not
			// yet fired DiagGathered). Normalize to Failed so retry paths (which
			// require Status == Failed) can proceed. Build the kept entry as a
			// value — no in-place mutation of the published struct.
			rec := recoveredFromProvision(existing)
			rec.Status = backend.ProvisionStatusFailed
			final[uuid] = rec.materialize()
		case backend.ProvisionStatusFailed:
			// Failed provision whose containers are gone — preserve so the
			// reconciler sees the failure and its FailCount.
			final[uuid] = existing
		case backend.ProvisionStatusDeprovisioning:
			// Owned by the in-flight deprovision goroutine; preserve untouched
			// (ENG-193 explicit case — previously dropped on recovery).
			final[uuid] = existing
		}
	}
	b.provisions = final
```

- [ ] **Step 5: Rename the post-swap `recovered` references to `final`** in `internal/backend/docker/recover.go` (`:343-374`). The allocations loop and the stats snapshot still read the published map. Change `recovered[uuid]` → `final[uuid]` (the `if prov, ok := recovered[uuid]; ok` at ~`:349`), `len(recovered)` → `len(final)` (twice, ~`:364-365`), and `for _, p := range recovered` → `for _, p := range final` (~`:366`).

- [ ] **Step 6: Run — Deprovisioning tests pass, characterization + existing suite green under -race**

Run: `go test ./internal/backend/docker/ -run 'TestRecoverState' -race -count=1`
Expected: PASS — the two new `Deprovisioning` tests now pass, and every Task-3 characterization test + `TestRecoverState_Serialized` stays green. Then the full package: `go test ./internal/backend/docker/ -race -count=1` — green. Then the linter: `golangci-lint run ./internal/backend/docker/` — no `exhaustruct` issues (the build-loop `ProvisionState` literal sets all 14 fields).

If any Task-3 characterization test changes outcome, the refactor is NOT behavior-preserving — STOP and report BLOCKED with the diff and your diagnosis. Do not modify the characterization tests to pass.

- [ ] **Step 7: Commit**

```bash
git add internal/backend/docker/recover.go internal/backend/docker/recover_state_test.go
git commit -m "refactor(docker-backend): construct recovered provisions via materialize; explicit Deprovisioning case (ENG-193)"
```

---

## Task 5: Route the post-publish `LastError` suffix through `UpdateFn`

Remove the last raw `b.provisions[uuid]` field access in `recover.go` — the enriched-diagnostics `LastError` write — by routing it through the actor's `ProvisionStore.UpdateFn` with the `Status==Failed` re-check inside the closure. Behavior-preserving.

**Files:** `recover.go`

- [ ] **Step 1: Replace the enriched-`LastError` write** in `internal/backend/docker/recover.go` at `:421-429`. Change:
```go
	if len(failedDiagnostics) > 0 {
		b.provisionsMu.Lock()
		for uuid, diag := range failedDiagnostics {
			if prov, ok := b.provisions[uuid]; ok && prov.Status == backend.ProvisionStatusFailed {
				prov.LastError = leasesm.ErrMsgContainerExited + ": " + diag
			}
		}
		b.provisionsMu.Unlock()
	}
```
to:
```go
	// Route the enriched LastError through the store seam (UpdateFn) so recover
	// holds no raw b.provisions field access. The Status==Failed re-check stays
	// INSIDE the closure: a concurrent Deprovision/Provision-retry/Restart that
	// took ownership during the diag window must not get its fresh LastError
	// clobbered with this failure's data (ENG-193).
	for uuid, diag := range failedDiagnostics {
		enriched := leasesm.ErrMsgContainerExited + ": " + diag
		b.provisionStore.UpdateFn(uuid, func(p *leasesm.ProvisionState) {
			if p.Status == backend.ProvisionStatusFailed {
				p.LastError = enriched
			}
		})
	}
```

- [ ] **Step 2: Run — green under -race**

Run: `go test ./internal/backend/docker/ -run 'TestRecoverState' -race -count=1 && go vet ./internal/backend/docker/`
Expected: PASS — recovery behavior unchanged (the `UpdateFn` closure applies the same gated write under the same `provisionsMu`).

- [ ] **Step 3: Verify no raw `b.provisions[` field accesses remain in `recover.go`** (deletes/reads in helper loops are fine; the target is field *writes* through a raw map index). 

Run: `grep -n 'b.provisions\[' internal/backend/docker/recover.go`
Expected: only the diag read-snapshots (`prov, ok := b.provisions[uuid]` for `ContainerIDs`/`DiagnosticSnapshot` under RLock) and the `_, hasExisting := b.provisions[uuid]` membership checks remain — no field *write* through a raw index. (The publish is `b.provisions = final`, the swap.)

- [ ] **Step 4: Commit**

```bash
git add internal/backend/docker/recover.go
git commit -m "refactor(docker-backend): route recover's enriched LastError write through UpdateFn (ENG-193)"
```

---

## Task 6: Concurrent-reader tests + final verification

Lock the concurrency claims with tests a synchronous `-race` run cannot make, then verify the whole surface.

**Files:** `recover_state_test.go`, `provision_test.go`

- [ ] **Step 1: Add a concurrent reader-vs-recover test** — append to `internal/backend/docker/recover_state_test.go`

```go
// TestRecoverState_ConcurrentReaderDuringMerge runs recoverState while another
// goroutine continuously reads provision state through the store seam. The race
// detector must stay clean: the merge holds provisionsMu across the swap and the
// reader takes it via Get.
func TestRecoverState_ConcurrentReaderDuringMerge(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusReady, ContainerIDs: []string{"c1"}}},
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "running"},
			}, nil
		},
	}
	b := newBackendForTest(mock, existing)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = b.provisionStore.Get("L1")
			}
		}
	}()

	for range 20 {
		require.NoError(t, b.recoverState(context.Background()))
	}
	close(stop)
	wg.Wait()
}
```

- [ ] **Step 2: Add a reader-vs-enrichment-window test** — append to `internal/backend/docker/provision_test.go`. This guards the reservation→enrichment window: a reader going through the store seam must observe the `Provisioning` marker (with its `CallbackURL` resolved) and never race the in-place `enrichReserved` write. The test drives `enrichReserved` under the lock concurrently with a seam reader (no full `Provision` flow needed).

```go
func TestProvision_ConcurrentReaderDuringValidationWindow(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusProvisioning, CallbackURL: "http://cb"}},
	})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if s, ok := b.provisionStore.Get("L1"); ok {
					// A reader in the validation window sees the Provisioning marker
					// with its CallbackURL resolved — never an empty/torn entry.
					_ = s.CallbackURL
				}
			}
		}
	}()
	// Exercise enrichReserved concurrently with the reader (the create-path write).
	for range 50 {
		b.provisionsMu.Lock()
		if p, ok := b.provisions["L1"]; ok {
			p.enrichReserved("docker-small", []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}, nil)
		}
		b.provisionsMu.Unlock()
	}
	close(stop)
	wg.Wait()
}
```

- [ ] **Step 3: Run the new concurrent tests under -race**

Run: `go test ./internal/backend/docker/ -run 'TestRecoverState_ConcurrentReaderDuringMerge|TestProvision_ConcurrentReaderDuringValidationWindow' -race -count=1`
Expected: PASS, no race detector reports.

- [ ] **Step 4: Full verification** — the whole affected surface under `-race`, plus vet and the linter.

Run:
```bash
go build ./... && \
go vet ./internal/backend/docker/ ./internal/backend/shared/leasesm/ && \
go test ./internal/backend/docker/ ./internal/backend/shared/leasesm/ -race -count=1 && \
golangci-lint run ./...
```
Expected: build clean; vet clean; all tests pass under `-race`; `golangci-lint` reports 0 issues (including the scoped `exhaustruct` enforcement and gofmt). If `golangci-lint` is not on PATH use `"$(go env GOPATH)/bin/golangci-lint"`.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/docker/recover_state_test.go internal/backend/docker/provision_test.go
git commit -m "test(docker-backend): concurrent-reader coverage for recover merge + provision window (ENG-193)"
```

---

## Self-Review (against the spec)

**Spec coverage:**
- `recoveredProvision` value type (embeds `ProvisionState` + `volumeCleanupAttempts`) + `materialize()` → Task 1. ✔
- `materialize` is the only path into `b.provisions`; two publish points (reservation, swap) → Tasks 2 + 4. ✔
- Create path: reservation via `materialize`, in-place `enrichReserved`, `Items` deep-copy, `CallbackURL`-at-reservation invariant → Task 2. ✔
- Recovery: build loop → `*recoveredProvision`; whole-map swap kept; rebuilt entries materialized (byte-equivalent field-clear); preserve-status policy; explicit `Deprovisioning` case; `Failing→Failed` via `materialize` (no in-place mutation) → Task 4. ✔
- `LastError`: baseline folded into the value (Task 4 cold-start logic sets `rec.LastError`); enriched suffix via `UpdateFn` → Task 5. ✔
- Field-completeness via scoped `exhaustruct` (`exclude: ['.*']` + `//exhaustruct:enforce`) → Task 1 config + enforce directives in Tasks 1/2/4. ✔
- Tests: `materialize` completeness, aliasing regression, per-status characterization, `Deprovisioning`, concurrent-reader → Tasks 1/2/3/4/6. ✔
- Out of scope (worker pre-publish, deprovision volume-retry/ENG-285) untouched. ✔

**Placeholder scan:** none — every code step shows verbatim code; every run step has an exact command + expected red/green state (Task 1 Step 2 compile-red; Task 3 Step 2 characterization-green; Task 4 Step 2 new-behavior-red).

**Type consistency:** `recoveredProvision` embeds `leasesm.ProvisionState` + `volumeCleanupAttempts int`; `materialize() *provision` and `recoveredFromProvision(*provision) recoveredProvision` and `(*provision).enrichReserved(string, []backend.LeaseItem, *manifest.StackManifest)` are used identically across Tasks 1/2/4. The recover build accumulator is `building map[string]*recoveredProvision`; the published map is `final map[string]*provision`. `b.provisionStore.UpdateFn(string, func(*leasesm.ProvisionState))` matches `leasesm_adapters.go`.
