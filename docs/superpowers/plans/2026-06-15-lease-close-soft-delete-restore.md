# Lease-Close Soft-Delete + Restore Implementation Plan (ENG-325) — Rev 5

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. **This revision is self-consistent: implement the task bodies verbatim — there is no separate "corrections" layer.**

**Goal:** Stop destroying a tenant's data volumes on lease close. Retain them for a grace period (default 90 days) and let the tenant restore that data onto a fresh lease on the same backend — with no reachable crash/failure/race/panic point that loses, leaks, or corrupts the data.

**Architecture:** On close, the docker backend writes a retention record (record-first) then renames the lease's *actual on-disk* volumes into a `fred-retained-` namespace. **Restore is a first-class backend operation** — a new `RestoreRequestedMsg` that fires a new `evRestoreRequested` SM event into the existing replace machinery — whose `doRestore` worker re-deploys the **retained manifest** and manages the retention record inline in its terminal `defer` (delete on success, roll back on failure/panic). A grace reaper reaps expired records and reconciles crash-orphaned restores; all transitions out of `active` are atomic bbolt-CAS so reaper and restore are mutually exclusive at the store layer.

**Tech Stack:** Go 1.22+ (`github.com/manifest-network/fred`), `go.etcd.io/bbolt`, `encoding/json`, `testify`, Docker Compose v2. White-box unit tests; integration tests `//go:build integration`.

---

## Revision history

- **Rev 1→3:** converged the design (record-first close, restore as a first-class actor op, enumerate-actual-volumes soft-delete, re-deploy-retained-manifest). Four-dimension review (local/architecture/idiom/online) confirmed the design **sound**.
- **Rev 4:** added the two must-fixes the Rev 3 review found (atomic CAS-claim; terminal-`Failed` restore) — but as a prose block over unmodified bodies.
- **Rev 5 (this):** the Rev 4 confirmation pass verified all corrections are design-sound but **inlines them into the task bodies** (Tasks 2/6/7) so the plan is internally consistent, and adds three concrete bugs the pass caught: **(N1)** `compose.Down` the failed restore's project *before* renaming volumes back (else write-after-rename corruption + orphaned containers); **(N2)** a **panic guard** in `doRestore`'s terminal defer (a panic leaves `Err==nil` → the defer would delete the record while the lease fails → orphaned data); **(N3)** set `workCancel` *after* the `evRestoreRequested` fire (`Provisioning` has `OnExit(onExitProvisioning)` which consumes it). It **drops C4** (the post-`Ready` `SendCallbackFn` delete) in favor of the simpler, already-grounded inline delete + reaper backstop.

**Confirmed-closed and carried forward:** record-first close + reconciliation; per-tenant cap; 422 breaker exemption; 3-implementer interface break; boot-eager reaper; 404-collapse oracle; UUID validation; enumerate-at-close; re-deploy-retained-manifest (no S2); CAS-claim closes the prelude-vs-reaper race; terminal-`Failed` restore.

---

## Scope Check — Plan 1 of 2

**Plan 1 (this doc):** retention + single-backend restore; all topologies preserve data. **Plan 2 (deferred):** multi-node restore *routing* affinity. On a multi-backend pool, Plan 1 restore safely returns `ErrNotRetained` (no data loss). Hand-off at the end.

---

## Design Decisions

1. **Restore re-deploys the retained manifest, not a new payload.** The record carries the `StackManifest` running at close; restore re-applies it onto the adopted volumes. The fresh lease must request **items matching the retained set** (same SKUs/services/quantity), validated up front. Image/config changes are a subsequent `update`. Eliminates the divergent-VOLUME-path data-loss (S2).
2. **Soft-delete renames the lease's actual on-disk volumes** — `b.volumes.List()` filtered to the `fred-{leaseUUID}-` prefix (ground truth; excludes `fred-retained-*` and other leases). No `DiskMB` predicate.
3. **The retention record is the durable source of truth,** with a `Status` state machine `active → restoring → (deleted | reverted-active)` and a **`Generation` CAS token**. **Every transition out of `active` is a single atomic bbolt `Update` txn** (`ClaimForRestore`, `ReapIfExpired`) so the grace reaper and an in-flight restore are mutually exclusive at the store layer — no wall-clock race guard.
4. **Restore is a first-class actor operation via a real SM event.** A new `leasesm.RestoreRequestedMsg` whose `Work` returns `ReplaceResult` rides the existing `spawnReplaceWorker` + `replaceCompleted/Recovered/Failed` machinery, entered by a new `evRestoreRequested` event (`Provisioning→Restarting`) with an `onEnterRestoring` entry action cloned from `onEnterRestarting`. The prelude reserves at `Status=Provisioning` and `handleRestoreRequested` `fireAndVerify`s the event (idiomatic — no off-actor `Status` pre-write; ENG-230-clean). `workCancel` is set **only after** a successful fire (N3).
5. **The retention record lifecycle is managed inline on the worker goroutine** in `doRestore`'s terminal `defer` (the `doReplaceContainers` `releaseStore.ActivateLatest`/`UpdateLatestStatus` idiom): **success → `Delete` the record; failure or panic → roll back** (`compose.Down` the new project, rename canonical→retained, release pool, `removeProvision`, revert record to `active`). Exactly **two writers** of record/volume state: this worker defer, and the reaper's reconcile arm (idempotent backstop, generation-gated). The reaper's `Ready`-arm deletes a leftover `restoring` record if the new lease is already `Ready` (covers the theoretical "delete fired pre-`Ready`-commit then the process died" gap).
6. **Crash-safety = record-first + reconcile (generation-CAS); race-safety = atomic store transitions + the actor serializing per-lease.**
7. Config in `internal/backend/docker/config.go`. `b.provisions` writes via `provisionsMu`; `recoveredProvision` publishes via `materialize()`; adding `RestoringFrom` forces every `//exhaustruct:enforce` literal.

### Shared types

```go
// internal/backend/shared/retention.go
const (
	RetentionStatusActive    = "active"
	RetentionStatusRestoring = "restoring"
)
var (
	ErrNoRetention    = errors.New("no retained data for lease") // mapped to backend.ErrNotRetained
	ErrNotRestorable  = errors.New("retained lease not in a restorable state")
)

type RetentionEntry struct {
	OriginalLeaseUUID   string                  `json:"original_lease_uuid"`
	Tenant              string                  `json:"tenant"`
	ProviderUUID        string                  `json:"provider_uuid"`
	Items               []backend.LeaseItem     `json:"items"`
	StackManifest       *manifest.StackManifest `json:"stack_manifest"`
	CallbackURL         string                  `json:"callback_url"`
	RetainedVolumeNames []string                `json:"retained_volume_names"`
	Status              string                  `json:"status"`
	NewLeaseUUID        string                  `json:"new_lease_uuid,omitempty"`
	Generation          int                     `json:"generation"`                // CAS token; bumped on EVERY active<->restoring transition (both directions)
	CreatedAt           time.Time               `json:"created_at"`
	RestoringSince      time.Time               `json:"restoring_since,omitempty"` // diagnostic only
}
```

```go
// internal/backend/client.go — restore takes NO payload (re-deploys the retained manifest).
type RestoreRequest struct {
	LeaseUUID     string      `json:"lease_uuid"`      // NEW lease
	FromLeaseUUID string      `json:"from_lease_uuid"` // original (retained) lease
	Tenant        string      `json:"tenant"`
	ProviderUUID  string      `json:"provider_uuid"`
	Items         []LeaseItem `json:"items"`           // must MATCH the retained set
	CallbackURL   string      `json:"callback_url"`
}
var ErrNotRetained = errors.New("no retained data for lease")
```

---

## File Structure

**Create:** `internal/backend/shared/retention.go` (+`_test.go`); `internal/backend/docker/restore.go` (+`_test.go`).
**Modify:** `internal/backend/shared/leasesm/{lease_actor.go,lease_sm.go}` (`RestoreRequestedMsg`, `evRestoreRequested`, `onEnterRestoring`); `internal/backend/docker/{backend.go,recovered_provision.go,recover.go,deprovision.go,config.go,restart_update.go}`; `internal/backend/client.go`; `internal/backend/mock.go`; `internal/backend/k3s/provision_stub.go`; `cmd/docker-backend/main.go`; `internal/api/{handlers.go,server.go}`; docs.

---

## Task 1: Config fields *(stable)*

Add `RetainOnClose bool`, `RetentionDBPath string` (`"retention.db"`), `RetentionMaxAge time.Duration` (90d), `RetentionReapInterval time.Duration` (1h), `MaxRetainedLeasesPerTenant int` (0) to `Config`/`DefaultConfig`/`Validate` with non-negative guards. TDD: defaults + validation. Commit.

## Task 2: RetentionStore with atomic CAS transitions

**Files:** `internal/backend/shared/retention.go` (+`_test.go`)

- [ ] **Step 1: Failing tests** — CRUD; `ListExpired`/`ListByTenant`/`ListRestoring`; and the **atomic-transition** tests:

```go
func TestRetentionStore_ClaimForRestore(t *testing.T) {
	s := newTestRetentionStore(t)
	require.NoError(t, s.Put(RetentionEntry{OriginalLeaseUUID: "u1", Tenant: "a", Status: RetentionStatusActive, Generation: 0, CreatedAt: time.Now()}))
	got, err := s.ClaimForRestore("u1", "u2", 90*24*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, RetentionStatusRestoring, got.Status)
	assert.Equal(t, "u2", got.NewLeaseUUID)
	assert.Equal(t, 1, got.Generation, "claim bumps generation")
	// second claim fails (not active)
	_, err = s.ClaimForRestore("u1", "u3", 90*24*time.Hour)
	require.ErrorIs(t, err, ErrNotRestorable)
	// absent
	_, err = s.ClaimForRestore("missing", "u9", 90*24*time.Hour)
	require.ErrorIs(t, err, ErrNoRetention)
	// expired
	require.NoError(t, s.Put(RetentionEntry{OriginalLeaseUUID: "old", Tenant: "a", Status: RetentionStatusActive, CreatedAt: time.Now().Add(-100 * 24 * time.Hour)}))
	_, err = s.ClaimForRestore("old", "u9", 90*24*time.Hour)
	require.ErrorIs(t, err, ErrNoRetention)
}

func TestRetentionStore_ReapIfExpired_AtomicVsClaim(t *testing.T) {
	s := newTestRetentionStore(t)
	require.NoError(t, s.Put(RetentionEntry{OriginalLeaseUUID: "u1", Status: RetentionStatusActive, RetainedVolumeNames: []string{"fred-retained-u1-app-0"}, CreatedAt: time.Now().Add(-100 * 24 * time.Hour)}))
	// claim wins -> reap returns nothing (now restoring)
	_, err := s.ClaimForRestore("u1", "u2", 90*24*time.Hour)
	require.NoError(t, err)
	names, err := s.ReapIfExpired("u1", 90*24*time.Hour)
	require.NoError(t, err)
	assert.Nil(t, names, "a claimed (restoring) record is never reaped")
}

func TestRetentionStore_RevertToActive_CAS(t *testing.T) {
	s := newTestRetentionStore(t)
	require.NoError(t, s.Put(RetentionEntry{OriginalLeaseUUID: "u1", Status: RetentionStatusRestoring, NewLeaseUUID: "u2", Generation: 5, CreatedAt: time.Now()}))
	ok, err := s.RevertToActive("u1", 5) // matching generation
	require.NoError(t, err)
	assert.True(t, ok)
	e, _ := s.Get("u1")
	assert.Equal(t, RetentionStatusActive, e.Status)
	assert.Equal(t, 6, e.Generation)
	ok, _ = s.RevertToActive("u1", 5) // stale generation
	assert.False(t, ok, "stale-generation revert is a no-op")
}
```

- [ ] **Step 2: Run → fail.**

- [ ] **Step 3: Implement** `retention.go` — embed `*boltStore`; `Put`/`Get`/`Delete`/`List`/`filter`; `ListExpired` (active+expired), `ListByTenant`, `ListRestoring`; plus the atomic transitions, each a single `db.Update` txn:

```go
func (s *RetentionStore) ClaimForRestore(orig, newLease string, maxAge time.Duration) (*RetentionEntry, error) {
	var out *RetentionEntry
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return ErrNoRetention
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if e.Status != RetentionStatusActive {
			return ErrNotRestorable
		}
		if maxAge > 0 && time.Since(e.CreatedAt) >= maxAge {
			return ErrNoRetention // about to be reaped; do not adopt
		}
		e.Status = RetentionStatusRestoring
		e.NewLeaseUUID = newLease
		e.RestoringSince = time.Now()
		e.Generation++
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		out = &e
		return bkt.Put([]byte(orig), data)
	})
	return out, err
}

// ReapIfExpired deletes the record only if still active AND expired, returning
// its retained volume names for the caller to Destroy AFTER the txn commits.
func (s *RetentionStore) ReapIfExpired(orig string, maxAge time.Duration) ([]string, error) {
	var names []string
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if e.Status != RetentionStatusActive || time.Since(e.CreatedAt) < maxAge {
			return nil
		}
		names = e.RetainedVolumeNames
		return bkt.Delete([]byte(orig))
	})
	return names, err
}

// RevertToActive (restoring->active) succeeds only if the persisted Generation
// still matches expectGen (CAS); bumps Generation. Returns false on mismatch.
func (s *RetentionStore) RevertToActive(orig string, expectGen int) (bool, error) {
	var ok bool
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if e.Status != RetentionStatusRestoring || e.Generation != expectGen {
			return nil
		}
		e.Status = RetentionStatusActive
		e.NewLeaseUUID = ""
		e.RestoringSince = time.Time{}
		e.Generation++
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		ok = true
		return bkt.Put([]byte(orig), data)
	})
	return ok, err
}
```

- [ ] **Step 4: Run → pass** `-race`. **Step 5: Commit** `feat(retention): bbolt RetentionStore with atomic CAS transitions (ENG-325)`.

## Task 3: Wire store + `RestoringFrom` + helpers *(as Rev 3, with exhaustruct fan-out)*

Helpers (`canonicalVolumeName`, `isRetainedVolume`, `retainedName`, `leaseVolumePrefix`); store field + `New`/`Stop` wiring; `RestoringFrom string` on `provision`/`recoveredProvision`/`materialize`/`recoveredFromProvision`; set `restoringFrom: ""` in **every** `//exhaustruct:enforce` literal (the two `recovered_provision.go`, two `recover.go`, and the `provision.go:79` reservation); add `mockVolumeManager.RenameVolumeFn` (unconditional). Build + commit.

## Task 4: Soft-delete at close (enumerate actual volumes, record-first, per-tenant cap) *(as Rev 3)*

Capture `providerUUID`/`stackManifest` in the top `UpdateFn`. Retain branch: `List()` → filter `leaseVolumePrefix(lease)` → record-first `Put`(active) → rename each canonical→`retainedName(c)`. Else branch: unchanged `Destroy` loop. Per-tenant `evictRetentionsToCap`. TDD: renames-exactly-existing, record-before-rename, per-tenant-cap, retain-off-destroys. Commit.

## Task 5: Orphan-reaper exclusion + startup reconciliation (test-asserted ordering) *(as Rev 3)*

`cleanupOrphanedVolumes` skips `isRetainedVolume`. `reconcileRetentions` (active arm: rename still-canonical back to retained; restoring arm: `reconcileRestoring`) runs in `Start` **after `recoverState`, before `cleanupOrphanedVolumes`** — **test-assert** the ordering. Commit.

## Task 6: Grace reaper (boot-eager + interval) + conservative restore reconcile

**Files:** `restore.go`, `backend.go`; Test `restore_test.go`

- [ ] **`reapExpiredRetentions`** uses the atomic store op:

```go
func (b *Backend) reapExpiredRetentions(ctx context.Context) (int, error) {
	if b.retentionStore == nil || b.cfg.RetentionMaxAge <= 0 {
		return 0, nil
	}
	candidates, err := b.retentionStore.ListExpired(b.cfg.RetentionMaxAge) // active+expired snapshot
	if err != nil {
		return 0, err
	}
	var n int
	for _, e := range candidates {
		names, err := b.retentionStore.ReapIfExpired(e.OriginalLeaseUUID, b.cfg.RetentionMaxAge) // atomic re-check + delete
		if err != nil {
			b.logger.Error("reap: store error", "lease_uuid", e.OriginalLeaseUUID, "error", err)
			continue
		}
		if names == nil {
			continue // concurrently claimed/changed — skip
		}
		for _, name := range names { // record already gone; safe to Destroy
			if derr := b.volumes.Destroy(ctx, name); derr != nil {
				b.logger.Error("reap: destroy volume", "volume", name, "error", derr)
			}
		}
		n++
	}
	return n, nil
}
```

- [ ] **`reconcileRestoring`** — generation-CAS, conservative, with `compose.Down` before re-quarantine (N1):

```go
func (b *Backend) reconcileRestoring(ctx context.Context, e shared.RetentionEntry) {
	b.provisionsMu.RLock()
	p, live := b.provisions[e.NewLeaseUUID]
	status := backend.ProvisionStatus("")
	if live {
		status = p.Status
	}
	b.provisionsMu.RUnlock()

	if live && status == backend.ProvisionStatusReady {
		_ = b.retentionStore.Delete(e.OriginalLeaseUUID) // restore finished; drop leftover record (idempotent)
		return
	}
	if live && status == backend.ProvisionStatusRestarting {
		return // in flight — doRestore's terminal defer owns it
	}
	// Orphaned (crash/failed). Tear down any orphaned project, re-quarantine the
	// volumes, then CAS the record back to active (no-op if it was retried).
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	if derr := b.compose.Down(ctx, composeProjectName(e.NewLeaseUUID), stopTimeout); derr != nil {
		b.logger.Warn("reconcile: compose down failed (continuing)", "lease_uuid", e.NewLeaseUUID, "error", derr)
	}
	for _, item := range e.Items {
		for i := range item.Quantity {
			canonical := canonicalVolumeName(e.NewLeaseUUID, item.ServiceName, i)
			_ = b.renameIfPresent(canonical, retainedName(canonicalVolumeName(e.OriginalLeaseUUID, item.ServiceName, i)))
		}
	}
	if ok, err := b.retentionStore.RevertToActive(e.OriginalLeaseUUID, e.Generation); err != nil {
		b.logger.Error("reconcile: revert failed", "lease_uuid", e.OriginalLeaseUUID, "error", err)
	} else if ok {
		b.removeProvision(e.NewLeaseUUID)
	}
}
```

*(Note: `renameIfPresent(old,new)` returns nil on success, on already-done, and on neither-exists; logs on a real error. `retainedName(canonicalVolumeName(orig,…))` yields `fred-retained-{orig}-{svc}-{i}`.)*
- [ ] **`evictRetentionsToCap`** (per-tenant, active-only) and **`runRetentionStartupSweep`** (reap expired + `reconcileRestoring` over `ListRestoring`) and **`startRetentionReaper`** (`util.StartCleanupLoop` every `RetentionReapInterval`) — as Rev 3/4. `Start`: after `cleanupOrphanedVolumes`, call `runRetentionStartupSweep` once, then `startRetentionReaper`.
- [ ] **Tests:** `TestReapExpiredRetentions`, `TestEvictToCap` (per-tenant), `TestStart_RunsEagerReapSweep`, `TestReconcileRestoring_DefersToInFlight` (live `Restarting` → no-op), `TestReconcileRestoring_RollsBackOrphan` (no provision → `compose.Down` called, volumes re-quarantined, record back to `active` via CAS). Commit `feat(retention): boot-eager reaper + generation-CAS restore reconcile (ENG-325)`.

## Task 7: Restore as a first-class actor operation

**Files:** `lease_actor.go`, `lease_sm.go`, `client.go`, `mock.go`, `k3s/provision_stub.go`, `cmd/docker-backend/main.go`, `restart_update.go`, `restore.go`; Test `restore_test.go`, `lease_actor_test.go`, `client_test.go`

- [ ] **Step A — SM event + message (idiomatic; N3-safe).** In `lease_sm.go`: add `evRestoreRequested` to the `leaseEvent` enum + its `String()` case; on the `Provisioning` state's `Configure` block add `.Permit(evRestoreRequested, Restarting)`; add `onEnterRestoring` (clone `onEnterRestarting`/`applyReplaceEntry`) and wire it `OnEntryFrom(evRestoreRequested, …)`. In `lease_actor.go`: add `RestoreRequestedMsg{Cancel, Work func() ReplaceResult, Ack chan error, CallbackURL string}` (clone of `RestartRequestedMsg` incl. the 3 interface methods), the dispatch arm, and:

```go
func (a *LeaseActor) handleRestoreRequested(msg RestoreRequestedMsg) {
	if a.terminated {
		msg.Ack <- errActorTerminated
		return
	}
	if err := a.fireAndVerify(evRestoreRequested, backend.ProvisionStatusRestarting,
		replaceEntryArgs{CallbackURL: msg.CallbackURL}); err != nil {
		msg.Ack <- a.classifyReplaceReject(err)
		return
	}
	a.workCancel = msg.Cancel // N3: set ONLY AFTER the fire (Provisioning.OnExit consumes workCancel)
	msg.Ack <- nil
	a.spawnReplaceWorker(msg.Work)
}
```

- [ ] **Step B — interface, sentinel, breaker, HTTP client, 3 implementers.** `client.go`: add `Restore` to `Backend`; `RestoreRequest` + `ErrNotRetained`; **add `ErrNotRetained` to `IsSuccessful`** (`:524`); `HTTPClient.Restore` (clone `Restart`; `202→nil`, `422→ErrNotRetained`, `409→ErrInvalidState`, `400→ErrValidation`). `mock.go`: `MockBackend.Restore`. `k3s/provision_stub.go`: `Restore(...) error { return backend.ErrNotRetained }`. `cmd/docker-backend/main.go`: add `Restore` to `backendService` + register `POST /restore`.

- [ ] **Step C — `replaceContainersOp.NoComposeRollback` (N2 guard for restore).** In `restart_update.go`: add `NoComposeRollback bool` to `replaceContainersOp`; in `doReplaceContainers`' failure defer (`~:266`) replace `restored := b.rollbackViaCompose(op)` with:

```go
		restored := false
		if !op.NoComposeRollback {
			restored = b.rollbackViaCompose(op)
		}
		// for restore there is no prior state to recover to:
		resultRet = leasesm.ReplaceResult{
			Err:      err,
			Restored: restored,            // false for restore -> terminal Failed
			Failure:  /* unchanged */ ...,
		}
```

(Ensure `RecoveredIfSourceActive` stays `false` for the restore op so the worker dispatches `replaceFailedMsg`, not `replaceRecoveredMsg`.)

- [ ] **Step D — `Backend.Restore` prelude (atomic claim) + `doRestore` (panic-guarded inline lifecycle) + `rollbackRestoreAdoption` (compose.Down first).**

```go
func (b *Backend) Restore(ctx context.Context, req backend.RestoreRequest) error {
	logger := b.logger.With("lease_uuid", req.LeaseUUID, "from_lease", req.FromLeaseUUID, "tenant", req.Tenant)

	// (a) Read for validation only (the authoritative claim is atomic, below).
	rec, err := b.retentionStore.Get(req.FromLeaseUUID)
	if err != nil {
		return fmt.Errorf("read retention store: %w", err)
	}
	if rec == nil || rec.Tenant != req.Tenant { // O1: collapse not-found + cross-tenant into one 404
		if rec != nil {
			logger.Warn("restore tenant mismatch", "entry_tenant", rec.Tenant)
		}
		return backend.ErrNotRetained
	}
	if err := itemsShapeMatch(rec.Items, req.Items); err != nil {
		return fmt.Errorf("%w: %w", backend.ErrValidation, err)
	}
	profiles := map[string]SKUProfile{}
	for _, item := range rec.Items {
		if _, ok := profiles[item.SKU]; ok {
			continue
		}
		p, perr := b.cfg.GetSKUProfile(item.SKU)
		if perr != nil {
			return fmt.Errorf("%w: %w", backend.ErrValidation, perr)
		}
		profiles[item.SKU] = p
	}
	for svc, m := range rec.StackManifest.Services {
		if ierr := shared.ValidateImage(m.Image, b.cfg.AllowedRegistries); ierr != nil {
			return fmt.Errorf("%w: service %s: %w", backend.ErrValidation, svc, ierr)
		}
	}

	// (b) Reserve the new-lease entry at Status=Provisioning, tagged RestoringFrom.
	b.provisionsMu.Lock()
	if _, exists := b.provisions[req.LeaseUUID]; exists {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %s", backend.ErrAlreadyProvisioned, req.LeaseUUID)
	}
	b.provisions[req.LeaseUUID] = recoveredProvision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID: req.LeaseUUID, Tenant: rec.Tenant, ProviderUUID: rec.ProviderUUID,
			SKU: rec.Items[0].SKU, Status: backend.ProvisionStatusProvisioning,
			Quantity: totalQuantity(rec.Items), CreatedAt: time.Now(), FailCount: 0, LastError: "",
			CallbackURL: req.CallbackURL, Items: slices.Clone(rec.Items),
			ContainerIDs: make([]string, 0), StackManifest: rec.StackManifest, ServiceContainers: nil,
		},
		volumeCleanupAttempts: 0,
		restoringFrom:         req.FromLeaseUUID,
	}.materialize()
	b.provisionsMu.Unlock()

	// (c) Allocate pool slots.
	var allocatedIDs []string
	for _, item := range rec.Items {
		for i := range item.Quantity {
			id := fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i)
			if aerr := b.pool.TryAllocate(id, item.SKU, rec.Tenant); aerr != nil {
				releaseAll(b.pool, allocatedIDs)
				b.removeProvision(req.LeaseUUID)
				return fmt.Errorf("%w: %w", backend.ErrInsufficientResources, aerr)
			}
			allocatedIDs = append(allocatedIDs, id)
		}
	}

	// (d) ATOMIC claim active->restoring (closes the prelude-vs-reaper race). On
	// any failure here, nothing was renamed yet — just release + remove.
	claimed, err := b.retentionStore.ClaimForRestore(req.FromLeaseUUID, req.LeaseUUID, b.cfg.RetentionMaxAge)
	if err != nil {
		releaseAll(b.pool, allocatedIDs)
		b.removeProvision(req.LeaseUUID)
		switch {
		case errors.Is(err, shared.ErrNoRetention):
			return backend.ErrNotRetained
		case errors.Is(err, shared.ErrNotRestorable):
			return fmt.Errorf("%w: %w", backend.ErrInvalidState, err)
		default:
			return fmt.Errorf("claim retention: %w", err)
		}
	}

	// (e) Adopt: rename retained->canonical. On failure, full rollback.
	if err := b.adoptRetainedVolumes(ctx, req.LeaseUUID, claimed); err != nil {
		b.rollbackRestoreAdoption(ctx, req.LeaseUUID, allocatedIDs, claimed, logger)
		return fmt.Errorf("adopt retained volumes: %w", err)
	}

	// (f) Hand off. doRestore's terminal defer owns success/failure/panic.
	opCtx, opCancel := b.shutdownAwareContext()
	work := func() leasesm.ReplaceResult {
		return b.doRestore(opCtx, req.LeaseUUID, claimed, profiles, allocatedIDs, logger)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, leasesm.RestoreRequestedMsg{
		Cancel: opCancel, Work: work, Ack: ack, CallbackURL: req.CallbackURL,
	}); routeErr != nil {
		opCancel()
		b.rollbackRestoreAdoption(ctx, req.LeaseUUID, allocatedIDs, claimed, logger) // worker never ran
		return routeErr
	}
	if accepted, err := b.ackOrAbort(ctx, ack); !accepted {
		opCancel()
		b.rollbackRestoreAdoption(ctx, req.LeaseUUID, allocatedIDs, claimed, logger)
		return err
	}
	return nil
}

func (b *Backend) doRestore(ctx context.Context, leaseUUID string, rec *shared.RetentionEntry,
	profiles map[string]SKUProfile, allocatedIDs []string, logger *slog.Logger) (resultRet leasesm.ReplaceResult) {
	defer func() {
		// N2: a panic leaves resultRet.Err==nil; force the failure path so we never
		// delete the record while the lease is not Ready. Convert panic -> Failed.
		if r := recover(); r != nil {
			logger.Error("restore worker panicked", "recover", r)
			b.rollbackRestoreAdoption(ctx, leaseUUID, allocatedIDs, rec, logger)
			resultRet = leasesm.ReplaceResult{Err: fmt.Errorf("restore panic: %v", r), Restored: false}
			return
		}
		if resultRet.Err == nil {
			if delErr := b.retentionStore.Delete(rec.OriginalLeaseUUID); delErr != nil {
				logger.Warn("restore ok but failed to delete retention record", "error", delErr)
			}
			return
		}
		b.rollbackRestoreAdoption(ctx, leaseUUID, allocatedIDs, rec, logger)
	}()
	return b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID: leaseUUID, Stack: rec.StackManifest, Items: rec.Items, Profiles: profiles,
		OldContainerIDs: nil, ServiceContainers: nil, Operation: "restore", NoComposeRollback: true, Logger: logger,
		OnSuccess: func(p *leasesm.ProvisionState) { p.StackManifest = rec.StackManifest },
	})
}

// rollbackRestoreAdoption is idempotent. N1: compose.Down the new project FIRST
// (stop containers on the bind-mounted volumes) BEFORE renaming volumes back.
func (b *Backend) rollbackRestoreAdoption(ctx context.Context, leaseUUID string,
	allocatedIDs []string, rec *shared.RetentionEntry, logger *slog.Logger) {
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	if derr := b.compose.Down(ctx, composeProjectName(leaseUUID), stopTimeout); derr != nil {
		logger.Warn("restore rollback: compose down failed (continuing)", "error", derr)
	}
	for _, item := range rec.Items {
		for i := range item.Quantity {
			canonical := canonicalVolumeName(leaseUUID, item.ServiceName, i)
			_ = b.renameIfPresent(canonical, retainedName(canonicalVolumeName(rec.OriginalLeaseUUID, item.ServiceName, i)))
		}
	}
	releaseAll(b.pool, allocatedIDs)
	updateResourceMetrics(b.pool.Stats())
	if ok, err := b.retentionStore.RevertToActive(rec.OriginalLeaseUUID, rec.Generation); err != nil {
		logger.Error("restore rollback: revert record failed", "error", err)
	} else if !ok {
		logger.Warn("restore rollback: record generation changed; reaper will reconcile", "lease_uuid", rec.OriginalLeaseUUID)
	}
	b.removeProvision(leaseUUID)
}
```

`adoptRetainedVolumes(ctx, newLease, rec)`: for each `(item,i)`, `RenameVolume(retainedName(canonicalVolumeName(rec.OriginalLeaseUUID,svc,i)), canonicalVolumeName(newLease,svc,i))`.

- [ ] **Step E — Tests** (all the verification-mandated regressions): `TestRestore_PreludeRejectsWhenNotRetained`; `TestRestore_PreludeRejectsConcurrentLiveProvision`; **`TestRestore_Success_DeletesRecord`** (drives to Ready); **`TestRestore_Failure_RollsBackInline`** (compose Up fails → terminal `Failed`, `compose.Down` called, volumes back at `fred-retained-`, record `active`, **no running containers**); **`TestRestore_WorkerPanic_RollsBackAndKeepsData`** (panic in adopt → record reverted to `active`, no record deletion, volumes re-quarantined); `TestRestore_RouteFailure_RollsBackSynchronously`; `TestRestore_ItemsMismatch_Validation`; `TestRestoreRequestedMsg_FiresEventAndSpawnsWorker` (leasesm); and a **`-race`** test interleaving `Restore`, a concurrent `Provision(sameLease)`, **and a reaper tick on the same original UUID** (per `[[race-detector-needs-concurrent-tests]]`). Commit `feat(retention): Restore as a first-class actor op (atomic claim, inline lifecycle, panic-safe) (ENG-325)`.

## Task 8: providerd restore endpoint (no payload) *(as Rev 3)*

`RestoreLease`: `authenticateAndResolve(w, r, true, false)` (**`requireActive=false`**); validate `from_lease_uuid` via `config.IsValidUUID`; items via `ExtractLeaseItems(auth.Lease)`; call `Restore`; map `ErrNotRetained→404`, `ErrInvalidState→409`, `ErrAlreadyProvisioned→409`, `ErrValidation→400`, `ErrInsufficientResources→409`; `202 {"status":"provisioning"}`. Register route. Tests: forwards-202, no-retention-404, PENDING-authenticates, malformed-400. Commit.

## Task 9: Notification + docs *(as Rev 3)*

`ProvisionStatusRetained` close event (best-effort, capacity-bounded); docs (config knobs; restore flow = close → fresh lease, same provider, matching items → `POST /restore {from_lease_uuid}` re-deploys the prior manifest onto your data, change image afterward via `update`; single-backend limitation; best-effort/capacity-bounded). Commit.

---

## Final integration gate

- [ ] `go build ./...` clean (implementers + exhaustruct).
- [ ] `go test ./internal/... -race -count=1` green; `golangci-lint run` clean.
- [ ] **Integration** (`//go:build integration`, real Docker + btrfs loopback; `sudo make test-integration-volume`): (1) provision→close-retain→restart (retained survives)→restore→data present; (2) **failed-restore (non-pullable image) → lease `Failed`, compose project gone, data rolled back to `fred-retained-`, record `active`, retry succeeds — NO loss/corruption**; (3) **worker-panic restore → record kept `active`, data re-quarantined**; (4) crash-at-close → reconcile completes; (5) concurrent `Restore` + `Provision(sameLease)` + reaper tick, `-race` clean, data never lost.

---

## Self-Review (Rev 5)

**Invariant:** *a managed volume never exists without either (a) a live provision entry, or (b) a retention record enumerating it.* Close: record-first + reconcile-before-orphan-reap. Restore: the entry is reserved before the claim; the record stays `restoring` until `doRestore`'s terminal defer deletes (success) or rolls back (failure/**panic**); a crash leaves `restoring` + canonical dirs, reconciled by the generation-CAS reaper after `compose.Down`. **Atomic store transitions** make reap and restore mutually exclusive (no time-race). **Exactly two writers** of record/volume state: `doRestore`'s terminal defer (success-delete / failure-rollback), and the reaper's reconcile arm (idempotent, generation-CAS-gated backstop incl. the `Ready`-arm cleanup). No path renames a bind-mounted volume under a running container (every rollback `compose.Down`s first).

**Every Rev 4-confirmation must-fix → inlined here:** C1 atomic claim → Task 2 `ClaimForRestore`/`ReapIfExpired`/`RevertToActive` + Task 7 (d) + Task 6; C2 terminal-`Failed` → Task 7 Step C `NoComposeRollback`; N1 compose.Down-before-rename → `rollbackRestoreAdoption` + `reconcileRestoring`; N2 panic guard → `doRestore` defer; N3 workCancel-after-fire → `handleRestoreRequested`; C3 idiom → real `evRestoreRequested` event (Step A); C4 dropped (inline delete + reaper backstop). Integration coherence: corrections are inlined; no separate block; Design Decisions #4/#5 reflect the event path + the two-writer model.

**Verify-before-claim (confirm in code):** the `Provisioning→Restarting` permit + `onEnterRestoring` entry-action wiring and `classifyReplaceReject` reachability; `replaceEntryArgs`/`fireAndVerify` signatures; `releaseAll`/`totalQuantity`/`itemsShapeMatch`/`adoptRetainedVolumes`/`renameIfPresent` helpers; `shared.ValidateImage`+`AllowedRegistries`; `composeProjectName`/`compose.Down` signature; `ExtractLeaseItems`; the full `//exhaustruct:enforce` literal set; that `ReplaceResult` has the `Restored`/`RecoveredIfSourceActive` fields used in Step C.

---

## Plan 2 hand-off (unchanged)

Multi-node affinity: keep lease→backend `placement` on a `CallbackStatusRetained`; resolve `RestoreLease`'s backend via `placementStore.Get(fromLease)`→`GetBackendByName`; mirror in `orchestrator.StartProvisioning` + `reconciler.doStartProvisioning`; clean up placement on adopt-success/grace-reap. Mind the typed-nil `PlacementStore` + `RouteRoundRobin` non-determinism gotchas.
