# Changelog

All notable changes to Fred are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Docker storage adoption now has a dedicated read-only command,
  `docker-backend -config <path> -preflight-storage-identity-adoption`, whose
  only success output is
  `ready_for_v0_13_storage_identity_adoption`. It repeats the initializer's
  descriptor-bound callback/release/retention, strict Docker-container, daemon,
  and reverse managed-volume proof without publishing a marker or binding a
  store. Wire-recognizable v0.13 restore, close, migration, and rollback-remnant
  crash states either receive a narrow documented recovery class or fail closed;
  missing topology/callback authority is never synthesized. For the exact
  stopped orphan-`-prev` case, the separate
  `placement-preflight -prove-terminal-orphan <lease> -expected-backend <name>`
  mode combines positive terminal membership in the diagnostic's exact provider
  through one complete height-pinned chain snapshot with absence from a pristine
  stopped v0.13 placement database. It prints that provider UUID for an explicit
  cross-check. Both results are necessary inputs to the documented immutable-
  container-ID cleanup, never cleanup authority by themselves. (ENG-632)
- The release now includes `placement-preflight`, a read-only v0.13 upgrade
  inspector plus explicit `-prepare` mode. With `providerd` stopped, it fetches
  identity-bearing complete provision and retention inventories from every
  configured backend plus one signer-free, height-pinned all-state chain
  snapshot over certificate-verified gRPC TLS. An intentionally local development
  chain requires an exact operator attestation before plaintext or skip-verify
  evidence can be used, even when a shared template sets `production_mode: true`;
  there is no automatic downgrade, and verified TLS rejects a stale override.
  The tool verifies the exact first-open lifecycle migration, topology,
  lease-owner, backend-reported provider, and chain-provider membership
  boundaries, including retention-only survivors. Mandatory
  `-prepare -backup <new-path>` first binds the backup parent's physical
  device/inode before remote evidence, then repeats the proof under an exclusive
  lock, runs bbolt's physical page/freelist consistency check, and publishes and verifies a
  descriptor-relative no-overwrite backup with
  `renameat2(RENAME_NOREPLACE)`, and atomically migrates the schema while pinning
  the proved provider and observed backend storage IDs. The exact published
  backup inode and parent remain bound and are re-attested before/after mutation
  and before the success verdict; every check also requires the original
  SHA-256, exact length, `0600` mode, and a single link.
  The explicit `-proof-timeout` bounds remote inventory/chain proof and
  cancellable validation; blocking file open/copy/fsync/commit/close and output
  remain under operator process supervision rather than pretending Go can
  safely cancel storage I/O. Legacy preparation checks proof freshness around
  the exact backup and at commit admission, explicitly reporting a published
  backup without a committed migration so operators preserve it and repeat the
  read-only proof with a new backup path. A bbolt `Commit` error is separately
  reported as `OUTCOME UNKNOWN`, never as a rollback; after a successful commit
  the tool syncs, closes, reopens read-only, and reruns physical and semantic
  postconditions. Preparation also requires an exact operator attestation that
  ingress, delayed backend effects, and callback replay are stopped and drained.
  That statement mints a short-lived opaque capability bound to the preparer
  session and exact proof cancellation scope, provider, canonical database and backup paths, topology/storage and
  inventory evidence, and height-pinned chain snapshot; it is revalidated before
  and inside mutation. The capability is copy-safe single-use and is consumed at
  exact-backup publication admission, so any failure beyond that boundary requires
  a fresh proof, attestation, capability, and backup path. Success output is one
  write ending in a mode-specific verdict: `READY_TO_PREPARE` for the read-only
  inspection, `PREPARED_FOR_CUTOVER` for legacy preparation, or
  `INITIALIZED_FOR_CUTOVER` for fresh initialization. Operators and automation
  must require both exit status zero and the exact applicable token; arbitrary
  error text is JSON-quoted onto one prefixed physical line so a remote body or
  identifier cannot forge a verdict or inject terminal controls.
  Post-mutation failures carry `PREPARED:` or `INITIALIZED:` and require the
  schema-neutral `placement-repair -classify` inspection instead of a blind
  retry.
  Old-binary rollback afterward requires that backup and is forbidden once
  upgraded side effects begin. (ENG-632)
- `placement-preflight -initialize-fresh` is the only path that creates a
  placement authority for a genuinely new provider. It rejects all chain lease
  history, including terminal history, and is never recovery for a lost
  database. It requires an independently supplied exact fleet roster, a
  path/provider/roster-bound operator quiescence acknowledgement, and complete
  identity-bearing empty provision and retention inventories from every
  configured backend. A deadline-bound, single-use capability, hard-capped to a
  two-minute lifetime even for a longer caller deadline, binds the target
  parent's physical device/inode across the print/initialize invocations. A
  rename, unmount, or same-path parent recreation invalidates that
  acknowledgement. The initializer publishes the synced authority through the
  retained parent descriptor with `renameat2(RENAME_NOREPLACE)`, leaving no
  second crash-visible name. The published inode is reopened read-only and
  checked for physical consistency, exact storage pins, topology/baseline
  epochs, and empty placement/lifecycle rows; cancellation before publication
  leaves the destination absent and any post-publication failure is categorically reported as
  `INITIALIZED:`. Normal providerd startup cannot substitute for this proof.
  (ENG-632)
- The release now includes `placement-repair`, an offline, dry-run-by-default
  diagnostic and repair tool. Read-only `-classify` distinguishes a wholly
  pristine v0.13 authority from a wholly prepared current authority after an
  indeterminate preparation commit. It verifies the existing path/inode,
  expected provider/topology, physical bbolt state, storage bindings, placement
  revisions, and lifecycle relationships without creating or replacing the
  database. Its deterministic JSON report has a hard 1 MiB encoded ceiling and
  includes recognized buckets, physical-check state, sorted storage/epoch
  facts, counts, at most 64 entries (plus explicit omitted counts) in each
  topology/storage collection, and at most 128 redacted per-lease
  state/revision/owner-or-attempt/lifecycle summaries. Per-row conflict owners
  are capped at four with an omitted count, and metadata, placement, and
  lifecycle values are bounded before decoding. It
  contains no operation or lifecycle ID, callback URL, token, secret, payload,
  tenant data, or raw row value, and it never prints `PASS`. Only
  `pristine_v0_13` and `prepared_current` exit successfully,
  while `mixed_or_incomplete` and `corrupt` still emit JSON and fail.
  Cancellation observed before reporting suppresses even a safe report, and
  read-only `-classify`, `-list`, and `-inspect` reject an explicitly supplied
  mutation-only `-timeout` rather than silently ignoring it.
  Read-only `-list`/`-inspect` modes expose exact
  placement state, backend, operation ID/kind, restore source or payload hash,
  immutable request tenant/provider/items, revision, and conflict candidates
  without minting mutation authority. Attempt repair requires the canonical
  current lease/backend/operation tuple, and conflict repair binds the selected
  owner to the exact revision, topology identity, and complete durable candidate
  set. Both mutations hold the stopped database exclusively, require fresh
  complete provision and retention inventories, and require an exact operator
  attestation that delayed backend effects and callback replay are drained.
  Attempt evidence is context/deadline-bound through an in-transaction check,
  validates preserved provider/tenant identity, and is re-probed and
  digest-rematched under mutation admission; refusal preserves any prior
  confirmed generation. Conflict resolution uses one opaque expiring plan whose
  confirmation binds the selected storage identity, provider/tenant and
  lifecycle evidence, topology/revision/candidates, and full inventory digest;
  it repeats and exactly rematches that probe under mutation admission.
  Retention-only, identity-incomplete, or unknown-generation evidence preserves
  routing while keeping lifecycle authority quarantined. Every mutation binds a
  new absent backup target's physical parent before remote proof and publishes a
  byte-exact pre-mutation backup through that retained directory immediately
  before its transaction.
  Repair `-timeout` is also the inventory-evidence freshness deadline through
  mutation admission: expiry during blocking backup I/O preserves the published
  artifact but refuses the transaction and requires a fresh proof/new path.
  Publication uses `renameat2(RENAME_NOREPLACE)`. Source consistency plus the
  exact published backup inode and parent identity, bytes, length, `0600` mode,
  and single-link status are checked before/after mutation and again before
  `PASS`; successful mutations are synced, closed,
  reopened read-only, and physically and semantically reverified. A bbolt
  `Commit` error is reported
  as `OUTCOME UNKNOWN` because its on-disk effect cannot be inferred. Exact-backup
  failures after the no-replace rename are reported as `BACKUP PUBLISHED`,
  including backup validation/sync/close/cleanup and later proof or pre-commit
  failures for which no repair mutation committed. Post-commit
  verification, sync, close, reopen, or verdict failures are reported with
  an explicit `COMMITTED:` status requiring read-only inspection, never as if
  the target necessarily remained unchanged. The tool syncs and closes the
  database before reporting success.
  Ambiguous provision and restore logs include the operation UUID needed to
  identify an attempt tuple. (ENG-632)
- Durable provision and restore attempts now bind a private, validated snapshot
  of the exact tenant, provider, and ordered backend items written before
  dispatch. Same-operation recovery uses that snapshot after restart, so a
  later mutable chain update such as `CustomDomain` cannot rewrite an ambiguous
  backend request. Missing or malformed snapshot metadata fails closed, and
  offline inspection exposes the nonsecret request facts. (ENG-632)
- Online provision and retention inventory now rejects noncanonical lease UUIDs,
  duplicate identities (including active/retained overlap), and returns no
  partial evidence. The affected backend remains unanswered for that sweep, so
  malformed inventory cannot establish a placement admission baseline. (ENG-632)
- `fred_placement_write_failures_total` exposes failures to persist a placement
  transition or verify a durable placement-sync point for operator alerting.
  (ENG-632)
- `fred_provisioner_callback_settlement_claim_wait_timeouts_total` exposes
  callbacks that exhaust the bounded wait for another terminal settlement actor,
  making stuck or unusually slow claim holders actionable. (ENG-632)
- `fred_provisioner_callback_placement_semantic_conflicts_total` exposes
  authenticated success-callback settlement attempts that encounter a permanent
  semantic placement verdict and continue toward chain acknowledgement while
  preserving the durable record for operator repair. Retries may increment the
  counter more than once. (ENG-632)
- `fred_provisioner_callback_deprovision_owned_success_total` exposes successful
  provision callbacks that overlap close/deprovision ownership of the same
  operation ID. Fred consumes these without acknowledging the closing
  lease and continues teardown. (ENG-632)
- `fred_provisioner_lifecycle_callback_outcomes_total{outcome,verdict,status}`
  classifies every authenticated lifecycle callback exactly once as applied,
  terminally dropped, or retryable. Its labels use closed vocabularies, so
  stale/retired replay drops are distinguishable from live authority or storage
  failures without tenant-controlled cardinality. The existing
  `fred_api_non_in_flight_callbacks_total` retains its received-at-ingress
  meaning. (ENG-632)
- `fred_provisioner_lifecycle_event_sink_panics_total{event}` exposes recovered
  panics in the best-effort provision-starting, restore-restarting,
  restore-refused, and post-settlement callback event sinks. Recovery preserves
  backend dispatch or durable callback settlement; every increase remains an
  actionable bug. (ENG-632)
- `fred_background_goroutine_panics_total{component="callback_replay"}` now
  exposes a recovered panic in a bundled backend's durable callback replay
  worker. The affected lease remains queued and other lease workers continue;
  any increase is still a bug requiring investigation. (ENG-632)

### Changed

- Deployment and backend guidance now records the read-only ENG-632 fleet
  inventory as of 2026-09-02: the serving production tenant fleet uses
  `docker-backend` on XFS. K3s remains a non-functional scaffold, while Btrfs
  and ZFS have automated coverage but are neither deployed nor
  production-validated. The mandatory cutover gate therefore targets Docker on
  XFS and requires re-inventory immediately before rollout; local development
  is outside that production gate. (ENG-632)
- Durable callback, operation, and close journals tolerate a backward
  wall-clock correction when their existing rows are read after restart. New
  callback writes still reject timestamps beyond the allowed future-skew
  bound, so clock rollback recovery does not weaken future-time admission.
- Docker storage-identity preflight and initialization now share the
  `-storage-identity-operation-timeout` cancellation deadline (default `10m`)
  across context-aware Docker and filesystem-control-plane probes. The deadline
  is cooperative, not a hard wall-clock interrupt for blocking local
  open/bbolt/fsync work. Expiry makes the command unsuccessful but is not proof
  of rollback: initialization may have left its crash-resumable pending state or
  completed the seal before a later check observed cancellation. Keep the
  lineage stopped and rerun the same mode against unchanged input, requiring
  its exact success verdict. (ENG-632)
- **Breaking production configuration:** each `backends[]` entry now requires a
  unique `hmac_secret` of at least 32 bytes matching only that backend's
  `callback_secret`; duplicate keys are rejected. The provider's top-level
  `callback_secret` fallback is non-production-only. This isolates
  cross-backend request and callback authority, but requires a stopped v0.13
  migration: first drain every v0.13 callback outbox, rotate each backend to
  `K_i`, configure the matching provider entry, remove the global key,
  start/verify backends, run classifier/preflight, then start providerd. A
  pending pre-identity v0.13 row blocks storage-identity initialization and
  current callback-store startup/health; if introduced after open, it remains
  quarantined and is never restamped during replay. Identity-bearing current-schema rows created
  after sealing need no rewrite during later key rotation because they retain
  their captured lineage and are signed with the current key at delivery. HMAC timestamps bound
  same-endpoint replay to five minutes; the signed method and URI prevent
  cross-endpoint replay. There is no nonce cache.

- Exact callback settlement now treats a nil chain point-read as unknown and
  retryable rather than terminal. Because `x/billing` retains terminal leases,
  an absent read can be a lagging, reset, or wrong RPC endpoint; Fred preserves
  the operation, durable attempt, and payload until it positively observes a
  supported live or terminal state. (ENG-632)

- Backend identities now reject invalid UTF-8, leading/trailing whitespace,
  control characters, and other non-printable format characters at config,
  router, bundled-backend, and durable-topology boundaries. Offline tool output
  also quotes or JSON-encodes names and backup paths, preventing a malformed
  operator-controlled identity from forging a `PASS:` verdict. Callback base
  URLs now share validated construction for typed and migrated-v0.13 routes,
  preserve unrelated raw query bytes, canonicalize trailing separators, and
  reject dot/empty path segments, backslashes, or encoded separators that a
  proxy or Go's HTTP router could normalize into a redirect. Complete callback
  destinations are immutable validated values and must end in
  `/callbacks/provision`; malformed authorities, ports outside 1–65535,
  fragments, empty query markers, and unstable raw query bytes fail closed at
  configuration, ingress, durable decode, and replay boundaries. Backend
  origins receive the same authority/port and empty-marker checks, negative
  backend timeouts are rejected, and the configured verifier prefix must equal
  the normalized escaped callback-base path byte-for-byte. (ENG-632)

- Backend names are now durably pinned to canonical UUIDv4 storage identities.
  Every inventory page and backend response carries
  `X-Fred-Backend-Storage-ID`; reads carry the HMAC-covered
  `backend_storage_id` query, and new-provider side effects use only
  `/_fred/storage/{id}/...`. The client refuses redirects and an old backend's
  headerless route-level 404 proves the upgraded-only request was never
  dispatched. Callback outbox rows preserve the originating storage ID in the
  HMAC-covered body so copying/replaying completion evidence from replacement
  storage cannot settle the old pin. Docker binds its marker pair to the
  backend name, active data mount/root, and Docker `SystemID`; k3s binds to its
  cluster UID. The seal now also binds every authority-bearing bbolt file to
  that UUID and a distinct store kind: Docker always requires callbacks,
  releases, and retention (including when `retain_on_close` is false), while
  k3s requires callbacks and releases. Diagnostics remain recreateable and do
  not grant lifecycle authority. Explicit initialization accepts only a wholly
  absent fresh set or a complete, valid, stopped-and-drained v0.13 set, records
  that profile in its pending anchor, and binds the stores before committing the
  marker pair so crashes are safely resumable. The pending capability is
  unforgeable, shares revocation across copies, and is withdrawn on every return
  path and before the first committed-marker publication. Before inspecting
  either profile,
  it binds both marker parents and every authoritative journal's physical parent;
  subsequent descriptor-relative I/O rejects a parent rename/recreation instead
  of publishing into replacement storage. Normal startup holds only a typed
  verified-storage capability and never creates, repairs, or rebinds a missing,
  foreign, or cross-kind authority. It also re-attests each opened path/inode
  before reads, writes, and background cleanup. Every authoritative journal is
  an unsymlinked, single-link regular file with exact mode `0600`; startup and
  runtime re-attestation reject permission or link-count drift. Diagnostics
  remains non-authoritative and recreateable, but its opener now applies the
  same no-follow, single-link, regular-file, exact-`0600` checks at open/create
  time. The first terminal authoritative-store or substrate failure is latched
  backend-wide and returned
  by every sibling journal and callback-delivery boundary, preventing a surviving
  store from advancing after lineage authority is withdrawn. Authoritative
  writes hold one shared gate through commit/postcheck, so withdrawal linearizes
  after an already admitted write and before the next admission. Backends must
  be upgraded and explicitly
  sealed first (`adopt` for a verified live v0.13 Docker lineage, `new` for a
  fresh or provably empty lineage), then the placement database must be prepared,
  then providerd may be upgraded. A complete matching snapshot intentionally
  retains the same lineage, so restoring it requires fencing the original and
  rolling markers, stores, and substrate together.
  Response identity still relies on peer-verified HTTPS (private CA/mTLS is
  supported); request HMAC does not sign responses. (ENG-632)

- `production_mode: true` now requires every backend URL and
  `callback_base_url` to use HTTPS, verifies backend peers against a configured
  private CA or system roots, and continues
  to reject `tls_skip_verify`. Bundled-backend production mode separately rejects
  disabled callback certificate verification. Backend requests are
  HMAC-authenticated, but response identity headers, inventories,
  and synchronous refusal bodies are not separately signed; accepting those
  facts over plaintext allowed an on-path responder to forge a definitive
  outcome. Plain HTTP remains available outside production mode for local
  development. (ENG-632)
- Bundled callback senders give the complete delivery retry chain one
  two-minute-fifteen-second budget. A fresh first attempt normally gives Fred
  time to return 503 after its two-minute application timer, without letting
  slow HTTP failures multiply that deadline by the retry count. Quick transport
  and HTTP failures retain their immediate retries and 0/1s/5s backoff within
  the shared budget; a later attempt may hit the sender's remaining deadline
  first. Afterward the durable per-lease FIFO head is retried by the 30-second
  replay loop. This backend-to-Fred budget is independent of providerd's
  `backends[].timeout`, which governs only Fred-to-backend requests.
  (ENG-632)
- Bundled backends now durably record each accepted provision/restore intent in
  `callbacks.db` before the first substrate mutation. Completion atomically
  replaces that exact per-lease intent with its callback outbox entry, so a
  crash after Docker/K3s work succeeds but before callback enqueue cannot lose
  Fred's settlement evidence. Exact request retries are idempotent; a different
  operation for the same lease conflicts while the intent is unresolved.
  Each intent also freezes the exact resolved CPU, memory, durable disk, and
  substrate-specific scratch profiles
  used by admission. On startup Docker first classifies maintenance intents
  lease by lease from each exact Release and a fresh bounded strict inventory,
  before ordinary projection can reinterpret a mixed replacement cohort. It
  then reconstructs ordinary/retention authority and settles interrupted
  provision/restore intents before destructive cleanup, reconstructing success
  only from an exact identity/item/manifest match and failing startup closed on
  partial, mixed, or unreadable evidence. The non-functional k3s scaffold
  deterministically recovers its accepted intents as `not implemented` failures.
  Operation intents do not age out and are covered by the callback-store health,
  backup, and rollback boundary. (ENG-632)
- Docker restart, update, and custom-domain replacement now use a separate typed
  maintenance write-ahead journal. Admission claims the exact active source
  Release, allocates a canonical UUIDv4 shared by the intent, target Release, and
  every target container. A cancel-only admission is durably advanced to a
  distinct append capability before the independent release write, so a stale
  token cannot erase recovery authority in the append-before-bind window. The
  journal then binds the store-assigned target version plus immutable digest
  before Docker mutation. Terminal release state is recorded
  before the intent is atomically converted into a non-coalescible,
  non-expiring lifecycle-route completion. Cold and periodic recovery never
  rerun an uncertain replacement: exact active or complete deploying cohorts
  converge to success and exact absence converges to failure. Partial-target
  cleanup revalidates and removes inspected immutable IDs individually; a later
  error retains the WAL after any earlier safe progress, so the next pass resumes
  idempotently without following reusable names. Divergent evidence is preserved
  fail closed.
  Readiness inspection distinguishes definitive unready evidence from transient
  ambiguity. If a committed active target has already lost its runtime, one
  callback transaction records maintenance Success followed by lifecycle Failed,
  while one idempotent actor transition installs the target authority directly
  as Failed; callback-lock contention preserves the WAL for the next sweep.
  Since the lifecycle callback payload has no maintenance-generation field, a
  lease cannot begin another restart, update, or custom-domain replacement while
  its prior exact maintenance completion remains queued. Delivery and precise
  removal reopen admission; until then the command receives a retryable 409.
  The paired runtime-Failed fact for an already-lost committed target is also an
  exact maintenance-derived barrier, so removing only its Success head cannot
  release admission. The fence is per lease, so callback trouble does not pause unrelated work.
  Operation, maintenance, and close journals are mutually exclusive per lease;
  close settles a committed maintenance target as success or atomically places
  its exact failed completion ahead of deprovision. Public callback-store APIs
  reject raw operation/maintenance deliveries, so exact completion evidence is
  constructible only by consuming its durable intent. (ENG-632)
- Docker's typed active Release is now the atomic provision/restore commit
  boundary. Alongside the exact manifest, effective items, and resource
  profiles, it stores an immutable runtime-authority snapshot that binds the
  operation UUID, tenant, canonical provider UUID, and current operation and
  lifecycle callback pair. A crash after the Release commit but before callback
  enqueue therefore settles the exact operation as success; exited or zero
  survivors recover as Failed with the complete conservative reservation, and a
  second restart retains the same lifecycle/close authority. A complete
  callback-bearing v0.13 cohort is CAS-fenced with a disjoint tokenless
  `LegacyRuntimeAuthority`; an authorityless v0.13 row is accepted only as
  pre-backfill upgrade input. Partial, mixed-class, or divergent authority fails
  closed. Restart/Update callback moves stay
  pending until the replacement succeeds, so preflight failure or rollback keeps
  the prior committed runtime route. (ENG-632)
- Docker release history now has a 32 MiB encoded per-lease contract. Every
  mutator transactionally preserves the index-latest, most recent active, and
  newest legacy-migration authority while pruning expired disposable audit rows
  before the oldest fresh ones. Provision, restore, and legacy migration prove
  exact terminal capacity before their first substrate side effect; legacy
  migration includes its tokenless principal and callback authority in that
  proof and in the atomic post-health Release commit before rollback cleanup is
  scheduled. A protected
  history that cannot fit is refused without mutation. Runtime health streams
  the fleet without the stopped cutover's 256 MiB aggregate ceiling, and the
  release client uses a 48 MiB projected-response budget. (ENG-632)
- Docker deprovision now has its own non-expiring write-ahead close finalizer in
  `callbacks.db`. Before the first destructive side effect it commits an opaque
  UUIDv4 capability containing the immutable backend/storage, tenant/provider,
  ordered items, manifest, callback pair, retention decision, exact selected
  release version/digest fence, and immutable IDs of legacy rollback containers. The
  admission transaction converts any preempted provision/restore or maintenance
  intent into its exact failed callback, while later causal admission is blocked
  until close resolves. Volume-cleanup attempts and cleanup-only failures survive restart.
  Recovery loads close authority before ordinary exact-cohort validation,
  rebuilds a conservative
  deprovisioning projection and capacity reservation even with zero survivors,
  and supports cleanup-only finalization when the volatile projection is already
  absent. Completion must retire the fenced release, atomically enqueue the
  lifecycle result and remove the close row, then delete volatile state. A
  transient failure keeps the row for level-triggered retry; callback-store
  health rejects corrupt rows or simultaneous operation/maintenance/close authority. After
  resolution, a non-blocking coalescing notification wakes the tracked callback
  replay loop; HTTP delivery no longer runs inside the lease actor/startup
  critical path, and the periodic 30-second sweep remains the durable fallback.
  `fred_docker_backend_pending_close_intents` exposes the aggregate outstanding
  finalizer count, and `fred_docker_backend_oldest_close_intent_age_seconds`
  exposes the oldest row's age without introducing lease-cardinality labels.
  (ENG-632)
- Docker and k3s production configs now require a positive
  `callback_max_age`. Zero previously disabled cleanup; an omitted value still
  defaults to `24h`. The age bounds legacy callbacks and typed lifecycle
  observations. Operation/maintenance intents, Docker close intents, and exact
  operation/maintenance completions never expire because they may be the only
  evidence capable of settling a durable effect, classifying a replacement, or
  safely resuming teardown, so an undeliverable
  exact FIFO head requires delivery or operator repair. During the stopped
  fleet cutover, operators with an explicit zero must choose a positive duration
  before starting that upgraded backend or the process refuses to start.
  (ENG-632)

- Provision lifecycle coordination now has one typed `operation.Registry` as
  its process-local source of truth. Opaque initiation, lease, and settlement
  capabilities enforce Preparing → Calling → Active ordering; callback,
  timeout, deprovision, and reconciliation consumers receive narrow ports
  instead of raw wire IDs or the legacy all-purpose tracker. Watermill
  handlers remain transport adapters, while callback and restore policy live
  in application services and backend dispatch remains in the orchestrator.
  The existing `stateless` FSM continues to serialize each backend lease actor;
  fleet reconciliation is deliberately a level-triggered evidence join with a
  pure decision table rather than a second edge-triggered FSM. (ENG-632)
- Reconciliation now has one construction and execution path: the exported
  constructor requires typed operation and placement authorities, and the
  legacy raw tracker/revision fallbacks have been removed. Reconciler tests use
  that same production path, including exact lease re-reads, opaque claims,
  operation-scoped callback URLs, inventory fences, and attempt tokens. (ENG-632)
- Restore admission now atomically reserves the confirmed source placement and
  writes the absent target's operation-scoped attempt under ordered source and
  target lifecycle claims. Exact acceptance/positive evidence confirms it and a
  contract-conforming synchronous refusal, trusted under the configured backend
  transport, may clear it. An ambiguous return or
  panic releases the transient source reservation but retains the durable target
  attempt; inventory absence never settles it. (ENG-632)
- Bundled backends now tag synchronous provision/restore capacity refusals with
  `code="insufficient_resources"`. The HTTP client reconstructs a distinct
  `ErrCapacityRefused` that still matches `ErrInsufficientResources`, allowing
  only that exact write-ahead attempt to be cleared and retried elsewhere.
  The code establishes protocol conformance, not cryptographic authorship:
  backend responses are not HMAC-authenticated and rely on the configured
  transport trust boundary. Legacy, code-less, and unknown-code 503 responses
  remain `ErrInsufficientResources` but ambiguous; malformed/non-envelope 503s
  are `ErrMalformedErrorBody`. Both classes retain their attempt. The
  `fred_backend_insufficient_resources_total` metric now separates
  `verdict="coded_refusal"` from `verdict="ambiguous"`. A definitively refused
  restore now follows its pre-dispatch `restarting` event with a best-effort
  terminal `failed` event whose neutral diagnostic says the restore did not
  start, so subscribers are not left on a stale lifecycle hint. (ENG-632)
- `placement_store_db_path` is now required at startup. Production topology is
  multi-backend, and even single-backend development/test needs durable evidence
  after an ambiguous response or restart, so providerd no longer offers an unsafe
  placement-disabled routing mode. Normal startup opens only an existing,
  fully prepared authority and performs no schema/bootstrap write; an absent,
  empty, or unprepared file fails closed. Existing v0.13 deployments must run
  the mandatory offline preparation above. Fresh environments use the explicit
  empty-fleet initialization workflow rather than allowing startup to create an
  authority from current endpoint names. Prepared and freshly initialized
  stores are bound to the exact provider UUID, so reusing a database with a
  different provider configuration fails closed. Placement and optional payload
  databases now require an unsymlinked, single-link regular file with exact mode
  `0600`; startup rejects insecure existing files and runtime permission,
  hard-link, or pathname drift (plus payload-parent drift) withdraws that store's
  authority for the process lifetime. (ENG-632)
- Backend router construction now rejects nil, empty-name, and duplicate-name
  backends. Backend names are durable placement identities, so silently keeping
  the first duplicate could route a recorded lease to the wrong machine. Names
  are now persisted as immutable storage identities: removing a referenced name
  is rejected. Removal additionally requires the latest complete,
  topology-bound raw provision and retention inventories to prove that backend
  empty, plus no placement or lifecycle reference; silence and projection
  filtering cannot prove a drain. A drained name may leave the active topology
  and later return as the same identity. Every membership change requires an
  identity-bearing probe of the complete proposed fleet and invalidates the old
  inventory baseline until a complete projection commits. An unchanged
  topology retains its baseline across transient node outages. Replacement
  storage must use a new unique name. (ENG-632)
- Provision and restore callback URLs now carry an HMAC-covered `operation_id`
  query parameter containing one lowercase, hyphenated, canonical RFC-4122
  UUIDv4. Backends must preserve and sign the complete request URI, including its
  query, so stale callbacks cannot settle a replacement operation. A present
  empty, nil, non-v4, non-RFC-variant, uppercase, compact, braced, URN, malformed,
  or duplicate value is rejected with 400. Body metadata is overwritten; only
  the authenticated query grants authority. Requests with no parameter remain
  accepted for callbacks emitted by v0.13.0 backends, but operation-ID-scoped
  operations still require the echoed ID to settle. (ENG-632)
- Provision and restore requests now distinguish their exact, operation-scoped
  `callback_url` from a typed `lifecycle_callback_url`. The latter carries a
  canonical UUIDv4 `lifecycle_id` whose current lease/backend binding is durable
  in the placement store and survives provider restarts. After placement
  deletion it narrows to teardown-only authority: runtime success/failure is a
  200 no-op, while the exact terminal deprovision observation can atomically
  retire the capability and publish retained status at most once. It otherwise
  rotates when a newer exact operation succeeds and is retired by terminal
  deprovision. Backends use it for restart/update completion, autonomous failure,
  and teardown observations, so a completed operation cannot make legitimate
  runtime, deprovisioned, or retained events disappear. Lifecycle callbacks can
  publish status but cannot settle operations or mutate placement/chain state;
  stale and consumed IDs are harmless 200 no-ops. Completed capability history
  is pruned when exact teardown consumption and placement deletion make it safe,
  while active teardown-only authority remains durable without a fixed TTL. An
  orphan-pruned placement whose backend reports it absent can therefore leave a
  teardown-only row indefinitely if no terminal callback arrives; that residual
  authority cannot publish runtime status, be reissued for maintenance, or
  mutate placement or chain state.
  Corrupt or revisioned-missing capability rows quarantine only their lease and
  passive inventory cannot recreate tokenless authority. Existing v0.13 owners
  migrate explicitly as legacy only when the lifecycle and metadata buckets are
  both absent at transaction entry, the database has no revisioned row, and the
  individual placement is a confirmed revision-zero owner; they remain tokenless
  until a typed operation replaces their callback route. The Docker backend
  persists both URLs in separate
  container labels and safely derives the paired lifecycle URL when upgrading
  state written by an older Fred. The durable callback outbox assigns
  explicit operation/lifecycle kinds and monotonic per-store sequences: exact
  completions remain FIFO, lifecycle observations are latest-only, and no newer
  lifecycle state can pass an older exact or legacy delivery for that lease. V2
  deliveries are partitioned into nested per-lease bbolt buckets,
  making same-lease operations proportional to that lease's queue and confining
  malformed durable data to its identifiable lease while health still reports
  the preserved corruption. Unavailable or corrupt leases do not block other
  leases, and pending work is retried during runtime as well as at startup.
  Legacy Docker migration preserves the exact completion URL, validates every
  sibling's callback pair before any stop/rename, and derives only a missing
  lifecycle route. Provider ingress now applies
  callbacks and publishes callback-derived events synchronously before returning
  200; startup, shutdown, and transient application failures return retryable
  503 without advancing the backend outbox. Callback application has a dedicated
  two-minute budget, its route extends the connection write deadline beyond the
  generic 15-second server default, and bundled senders wait two minutes fifteen
  seconds with no competing client-wide cap. A fresh first attempt normally
  leaves time for the provider to return 503; retries use only the shared
  deadline's remainder and may be canceled by the sender first. Either result
  leaves the same exact entry as the durable FIFO head. Shutdown cancels admitted
  application before draining it. The stopped v0.13 cutover must drain legacy
  queue entries before storage-identity sealing; current callback-store startup
  and health refuse a nonempty legacy bucket. If such a row is introduced after
  open, the sender preserves it without attributing it to the mounted substrate.
  During the stopped cutover, update and start backends before providerd: new
  backends understand callback state inherited in the old tokenless shape,
  while a v0.13 backend cannot preserve the separate lifecycle route sent by a
  new provider. Docker now freezes every complete callback-bearing v0.13 cohort into a distinct
  zero-value-invalid `LegacyRuntimeAuthority` before any operation may remove
  its last container. That durable principal, tokenless callback pair, manifest,
  item topology, and resource snapshot permit zero-survivor recovery and keep
  the first restart, update, or custom-domain redeploy operational after the
  cutover. The first and every subsequent restart, update, or custom-domain
  replacement stays legacy and tokenless; its UUIDv4 `maintenance_id` is exact
  replacement WAL/cohort identity, not provider callback authority. Only a
  later genuine provision or restore rotates the lifecycle to typed authority.
  Active callbackless pre-label cohorts are rejected by mandatory stopped
  adoption because provider callback authority cannot be minted safely;
  historical callbackless cleanup/close evidence remains readable but does not
  authorize zero-survivor recovery or maintenance.
  (ENG-632)
- The callback JSON `backend` field remains optional metrics-only sender
  metadata for v0.13 compatibility. It need not equal Fred's durable router
  name and cannot authorize or redirect a typed callback; the HMAC-covered URL
  plus the current exact-operation or durable lifecycle capability provide that
  authority. (ENG-632)
- Restore now requires a writable durable placement recorder and a registry-issued
  non-nil `operation.OperationID` bound through an opaque initiation capability
  before contacting the backend. A target with
  an unresolved durable provision/restore attempt returns 409, and an open
  backend circuit returns 503 rather than 500. (ENG-632)
- Direct placement-routed connection, log, release, restart, and update
  operations now return 503 for an unusable/conflicting or attempt-only placement
  instead of substituting a backend by SKU. Read-only provision discovery queries
  every configured confirmed, attempted, or conflicting candidate before SKU
  fan-out and returns 503, not a false 404, when unresolved evidence remains.
  (ENG-632)
- Close/deprovision now fails and retries when a durably named owner, attempt, or
  conflict candidate is unconfigured or cannot be reached, even if other
  backends report a successful no-op. (ENG-632)
- A first complete `/provisions` plus `/retentions` projection now establishes a
  durable admission baseline bound to the configured backend identities. The
  matching baseline survives provider restarts and transient incomplete sweeps.
  On a later partial sweep, the reconciler issues a typed admission scope only
  for nodes that answered both inventories and uses it only for genuinely new
  recordless `PENDING` work. Recordless `ACTIVE` recovery, work pinned to a
  silent owner, attempts, and conflicts remain deferred; inventory silence never
  clears ambiguity, and contradictory positive reports expand durable conflict
  quarantine. The tenant event path has no sweep witness: it uses the durable
  topology baseline, live backend stats, and an exact write-ahead attempt.
  (ENG-632)
- Upgrading from v0.13.0 now requires quiescing provisioning before provider
  cutover and keeping tenant lifecycle ingress closed until the first complete
  inventory establishes that durable baseline. Existing stack-form containers
  are not restarted or moved. Older service-name-less cohorts intentionally go
  through the one-time stop/rename/volume-rename/Compose recreation during the
  first upgraded Docker-backend startup and therefore have a workload restart
  and downtime window. (ENG-632)

### Deprecated

### Removed

### Fixed

- Docker failure diagnostics now persist and re-project the non-secret
  lifecycle generation observed from the provision's callback pair. Removing
  an in-memory failed provision therefore no longer downgrades its diagnostics
  fallback to an unknown observation. Diagnostics remain non-authoritative and
  excluded from full inventory and placement settlement. (ENG-632)
- Docker crash recovery now preserves an exact provision cohort while its
  containers or required health checks are still transitional; a later healthy
  inventory settles the original operation instead of tearing the cohort down
  and reporting a false failure. Failed-predecessor replacement also snapshots
  its projection under lock and compares both generation and value before any
  teardown or publication, preventing torn reads and pointer-ABA overwrites.
  (ENG-632)
- XFS delete-stage normalization now verifies the project-zero inode and
  project-inheritance flag through Linux's typed `FS_IOC_FSGETXATTR` interface
  instead of parsing localized, version-specific `xfs_quota project -c` prose.
  Numeric quota reports keep stdout separate from diagnostics and reject any
  stderr rather than allowing a warning to masquerade as authoritative zero
  usage. ZFS runtime inventory again returns only bind-ready directories; its
  wider dataset/directory union remains confined to storage-identity proof.
  (ENG-632)
- Docker stack admission now proves that quantity expansion is injective before
  Compose can mutate substrate (`web` quantity 2 conflicts with unscaled
  `web-0`). Compose PS attribution uses the same exact generated-key map instead
  of numeric-prefix parsing, so valid services such as `web-01` and `web-999`
  cannot be misclassified as scaled `web` instances. (ENG-632)
- Docker startup quota reconciliation now attempts every expected present live
  or retained volume and then fails startup/readiness on any inventory,
  immutable-resource-authority, or quota-enforcement error. A truly fresh XFS
  root does not need `CAP_FOWNER`, but an existing tree may need it for recursive
  project-ID re-tagging; the backend no longer serves while a known tenant
  volume may remain uncapped. (ENG-632)
- XFS volume creation now prepares a parent-synced, typed hidden stage carrying
  the nonzero project ID and final managed name, writes and syncs its marker,
  applies the project tag and limits, and only then publishes the final name with
  no-replace rename. Startup treats a recovered stage as cleanup-only because
  the original requested quota is not durable: it clears the dquot and removes
  only an empty stage or one whose sole no-follow regular marker is at most ten
  bytes. The marker may be empty, partial, or zero-filled after a crash; the
  parent-synced typed stage name remains cleanup authority, while publication
  still requires the complete marker to parse and match that name. Runtime
  errors after stage durability attempt exact compensation; the external quota
  clear uses a detached cleanup context capped at 30 seconds and by any earlier
  aggregate parent deadline. A failed or ambiguous cleanup preserves the stage,
  withdraws storage authority, gracefully drains a running docker-backend, and
  exits status 1 so its supervisor runs a fresh `Start` before readiness. A
  failure discovered by `Start` exits 1 before listener bind. A persistent
  fault therefore crash-loops closed. An exact unmounted ZFS managed
  child is likewise preserved and remounted/re-attested on normal sealed
  startup rather than destroyed. Read-only preflight and storage-identity
  initialization reject both forms instead of mutating the lineage they are
  proving; Btrfs's already-published subvolume form converges through operation
  recovery, the fatal quota gate, and orphan classification.
  (ENG-632)
- XFS volume destruction now writes a separate empty, project-zero,
  parent-synced `.fred-xfs-delete-<project-id>-<managed-volume>` authority before
  removing any final-volume bytes. A restart can therefore resume the exact
  deletion after the volume marker is gone. It re-normalizes and syncs the
  authority, removes the final tree in place, parent-syncs its absence, proves
  both block and inode usage are zero (including open-but-unlinked files),
  strictly clears every project-quota limit, and removes the authority last.
  Failure preserves the authority and blocks same-name creation. A runtime
  failure gracefully drains and exits status 1; a startup recovery failure
  exits 1 before listener bind. The supervisor's fresh `Start` must prove
  completion. Offline preflight and storage initialization reject
  this upgraded private evidence without mutating it.
  (ENG-632)

- Docker construction and startup can no longer wait for process lifetime on a
  wedged Docker/CLI boundary. The default `New` path bounds substrate/storage
  attestation at
  30 seconds (`NewWithContext` uses an explicit caller deadline without adding
  a fallback); `Start` has a
  30-minute backend-lifecycle recovery budget. Interrupted-volume recovery and
  its clean-inventory proof each receive a fixed two-minute filesystem-only
  child deadline inside that aggregate; later startup phases share one
  `max(2m, container_stop_timeout)` aggregate budget, and recovery Docker reads
  remain 30-second bounded. Cold-start diagnostics and orphan network cleanup
  now share one budget across the whole fleet rather than multiplying a timeout
  per container/network. Exhaustion fails or defers the phase with durable
  operation, maintenance, and close evidence intact for the next launch. Local
  filesystem deadlines are cooperative: one blocking kernel call cannot be
  forcibly interrupted and a very large recursive removal can cross the
  nominal budget, but recovery checks cancellation between top-level entries
  and phases and makes no later mutations.
  (ENG-632)
- Release-history cleanup now fails closed on corrupt or empty durable values
  instead of deleting them as expired. It also preserves the newest legacy
  migration row needed to identify rollback remnants after a later update.
  Deprovision release retirement is required, fenced to the exact generation
  captured by its close intent, and occurs before the close journal may resolve;
  a release-store error therefore leaves a durable retry owner rather than an
  unexplained zero-container active release on the next boot. (ENG-632)
- Update and Restart now serialize their release-history append through the
  lease actor's definitive admission decision. The fence is a zero-value-ready,
  ref-counted exact-key registry, so unrelated leases never collide; restore
  reconciliation holds it across its complete decision and mutation. Caller
  cancellation after enqueue cannot let a retry append a newer row that the
  older worker would settle, and a delayed restore finalizer uses the same
  fence. Restore failures no longer rewrite unrelated prior release history.
  (ENG-632)
- Docker now exports unlabeled pending-close count and oldest-age gauges for its
  non-expiring destructive finalizer journal. A valid but repeatedly deferred
  close can therefore alert without putting tenant or lease UUIDs in Prometheus
  labels; lease identification remains in the recovery log. (ENG-632)
- Docker now carries the exact per-SKU CPU, memory, durable-disk, and scratch profiles unchanged
  from operation intent through live provision, successful release, close, and
  retention authority. Crash recovery, maintenance, retained-volume accounting,
  restore sizing, and reaping cannot reprice an admitted generation after an
  operator edits configuration. Startup migration freezes true v0.13 live rows;
  transitional items-only releases are exact version/items CAS-backfilled.
  Pre-upgrade retention rows retain a current-config fallback until a failed
  restore proves and atomically persists exact source quota authority. Resource
  and quota CPU values also reject non-finite YAML values so `NaN` cannot poison
  capacity comparisons. For a v0.13 active release whose row omitted desired
  items, the backend can freeze only the exact dense cohort it observes locally;
  providerd remains stopped until the placement preflight compares those frozen
  items exactly with the height-pinned chain workload. (ENG-632)
- Docker resource authority now separates retainable SKU disk from ephemeral
  writable-path scratch. Every newly admitted `disk_mb: 0` instance pins the
  then-current positive `container_tmpfs_size_mb` as `scratch_disk_mb` before
  image inspection. Global disk admission, tenant disk quota, `/stats`, recovery,
  maintenance, and close accounting use `disk_mb + scratch_disk_mb`; the two
  fields are mutually exclusive. This deliberately reserves the allowance even
  when that image ultimately needs no managed writable-path volume, avoiding a
  mutable second admission phase. Scratch is never retention entitlement:
  writable-path-only volumes are reclaimed on close. If conservative close
  classification nevertheless preserves an exact scratch-volume name, its
  pinned allowance remains in physical pool accounting and quota backfill until
  destruction; it still does not make the SKU stateful. Image `VOLUME`, `/tmp`,
  `/run`, and tenant-declared tmpfs behavior is unchanged. True v0.13
  diskless generations freeze the cutover value once during startup migration,
  so operators must also keep `container_tmpfs_size_mb` unchanged through the
  first upgraded Docker-backend start. (ENG-632)
- Failed restore rollback now measures every re-quarantined volume, reapplies
  the immutable source quota through the Btrfs/XFS/ZFS abstraction, pre-counts
  the retained footprint, and exact-CAS reactivates/backfills the source before
  releasing destination allocations. Any uncertainty stays `restoring` and
  live-counted, so a transient failure can over-deny but never over-admit. A
  backend-local recovery-snapshot guard now also makes restore admission and
  final rollback handoff indivisible with respect to inventory publication, so
  recovery cannot resurrect a transient destination after the same restore has
  completely rolled back.
  (ENG-632)
- Restore destination authority now fences Provision and Restore for as long as
  any source retention finalizer owns that destination. A plain,
  identity-preserving Restart is admitted only after an exact active Release
  proves commit and the exact restore intent has settled; Update and
  custom-domain redeploys stay fenced until that Restart reaches Ready and the
  periodic finalizer consumes the lingering row.
  The finalizer also persists the destination's typed operation ID and original
  operation/lifecycle callback pair for commit matching. After a successful
  maintenance base move, the typed active Release owns the current runtime
  route while the finalizer remains identity-compatible with that move.
  An accepted worker failure first re-quarantines data and restores source quota,
  but deliberately keeps the source `restoring` and destination capacity live
  until the lease actor durably settles the exact failed callback; the periodic
  finalizer then performs the make-before-break source handback. Pre-actor
  failures settle their exact intent before that handback. An exact active
  destination Release is instead the durable restore commit marker, and rollback
  never deletes it. Cold-start recovery treats an exact Release with zero
  surviving containers as a post-commit runtime
  failure: it settles any matching intent as success, recovers a conservative
  Failed destination plus its exact allocation, and retains the source finalizer
  as tenant/provider identity across repeated restarts instead of handing data
  back. Close remains available only through the complete close-intent handoff.
  (ENG-632)
- Deprovision of a restore destination now records a complete close intent before
  handing off any lingering source ownership. Strict tenant/provider, item,
  manifest, profile, and generation checks then CAS-delete the source finalizer
  before teardown begins, leaving the close journal as the sole durable owner.
  (ENG-632)
- Once the upgraded storage lineage is sealed, Docker's current
  legacy-to-Compose writer is crash-resumable at all four mutation boundaries.
  It rediscovers exact migration-created `-prev` generations, makes container
  and volume renames idempotent, and commits the wrapped manifest plus exact
  ordered `Items`, resource profiles, and migration provenance before scheduling
  rollback cleanup. A release-store failure aborts startup with both generations
  preserved; after commit, recovery validates the complete live cohort against
  `Items` before rescheduling any remaining `-prev` cleanup, so a partially
  cleaned rollback set cannot become desired quantity. Pre-existing v0.13 crash
  generations remain a stopped-adoption problem: a proven rollback-only cohort
  is resumable, an exact pre-`RecordMigration` authorityless duplicate stack
  requires the bounded immutable-ID procedure, and post-`RecordMigration`
  authorityless/partial evidence fails closed pending a known-good snapshot or
  dedicated proof-bearing repair. This replaces only the runtime behavior; the
  v0.4.0 entry below remains the historical behavior of that release. (ENG-632)
- Docker-backend shutdown no longer waits forever for a worker that ignored
  cancellation or closes shared dependencies underneath it. Backend-owned work
  receives a fixed 90-second drain after cancellation; timeout leaves the Docker
  client and durable stores open, returns a typed shutdown error, and makes the
  binary exit non-zero so its supervisor restarts through normal recovery.
  Preempting lease transitions likewise refuse substrate teardown until the
  prior worker drains, preventing a canceled worker from recreating a closed
  workload. This bound is separate from providerd's configurable
  `shutdown_timeout`. (ENG-632)
- Callback outbox age cleanup expires only a contiguous FIFO prefix of v0.13
  and typed lifecycle records. Exact operation completions are non-expiring
  causal barriers, so their durable placement evidence cannot disappear during
  an outage. Lifecycle coalescing now validates a complete per-lease snapshot,
  preserves terminal deprovision observations against later runtime events, and
  avoids mutating a live cursor. Current rows are semantically validated
  fail-closed (including identity, URL authority class, status/retained rules,
  creation time, and duplicate JSON fields) while a separate v0.13 validator
  preserves rolling-upgrade compatibility. Malformed data remains preserved and
  fail-closed for its identifiable lease without blocking unrelated lease
  replay or cleanup. (ENG-632)
- A malformed lifecycle-capability row or a revisioned placement whose
  capability is missing now quarantines only that lease instead of preventing
  providerd startup or silently restoring tokenless authority. Exact terminal
  consumption and later placement deletion prune completed capability state;
  active teardown authority remains durable until consumed. (ENG-632)
- The final authoritative lease re-read before a reconciliation action now uses
  the same bounded per-query timeout as other chain liveness checks. A stalled
  point query defers that lease instead of blocking startup, the worker group,
  and every later sweep behind the reconciliation guard. (ENG-632)
- Placement-enabled provision and all restore transitions are now write-ahead
  and fail closed: Fred durably records an attempted backend before the backend
  call, distinguishes a contract-conforming synchronous refusal trusted under
  the configured backend transport from an ambiguous outcome, reconciles
  attempts only from exact positive evidence (never inventory absence), and
  deprovisions every known owner candidate. Callback and timeout settlement is
  exact-operation scoped so an older operation cannot mutate or reject its
  replacement. (ENG-632)
- Unvalidated HTTP 409 and 503 responses no longer confirm or clear provision
  attempts (and unvalidated 503 responses no longer clear restore attempts);
  authenticated success callbacks continue to chain acknowledgement when
  placement persistence fails; and one lease's durable placement conflict no
  longer prevents unrelated recordless `PENDING` leases from using a typed scope
  of nodes that answered both inventories after durable topology bootstrap.
  (ENG-632)

### Security

- XFS project IDs and dquots are filesystem-global even when
  `volume_data_path` is a subdirectory. Fred allocates across the nonzero 32-bit
  namespace and detects collisions only in its own managed root, so deployments
  must make Fred the sole project-ID allocator on the containing XFS mount.
  Separate directory roots are not isolation. Use a dedicated filesystem, or
  add a coordinated disjoint-range mechanism before introducing another
  allocator; this release has no range-coordination knob. The current
  manifest-managed production layout shares `/data` but has no other observed
  project-quota allocator, which is an operational precondition to keep enforcing
  rather than a safety property of that layout. (ENG-632)
- Managed Docker volume identities are now validated as canonical, single-component
  typed names before any Btrfs, XFS, or ZFS filesystem, quota, rename, destroy, or
  dataset operation. Descriptor-rooted cleanup and XFS marker access reject traversal,
  symlink aliases, cross-volume redirection, and non-directory collisions; invalid input
  cannot escape through the legacy error-less `HostPath` interface. Storage-identity
  adoption applies the same grammar, attests every name as an actual Btrfs subvolume,
  real directory on the configured XFS root with a regular no-follow nonzero
  project-ID marker, or exact ZFS child dataset mounted at its exact managed path,
  and proves each retained or mounted volume against its exact lease, service,
  instance, and namespace before publishing an authority marker. Startup quota
  reconciliation establishes the XFS kernel project tag and limit before
  readiness. ZFS proof inventory also includes depth-one child datasets with no
  expected directory, so an unmounted or externally mounted dataset cannot
  disappear from preflight evidence; preflight cannot seal state the runtime
  would later reject. (ENG-632)
- **docker: a stateful-volume bind source whose leaf is a symlink is now rejected — the
  half of ENG-539 that was never fixed.** ENG-539 confined subdirectory creation to the
  volume root with `os.Root`, and that guard holds: a link that escapes the root, or an
  absolute one, still fails closed. But `os.Root` is *escape*-safe, not *symlink*-free.
  Its `MkdirAll` mirrors `os.MkdirAll` and returns success when the leaf is a symlink that
  resolves — **inside** the root — to a directory, leaving the leaf a symlink on disk. The
  bind `Source` fred emits is the raw joined path, which Docker resolves host-side; runc
  deliberately trusts the mount source and lets the kernel follow it, so nothing downstream
  of fred catches this. `buildStatefulVolumeBinds` now `Lstat`s the leaf and fails the
  operation if it is a symlink, matching the check the migration path (`migrate.go`) and
  the writable-path path (ENG-543) already had. New counter
  `fred_docker_backend_volume_bind_symlink_rejected_total`.

  Reachable in two deploys by a tenant whose image runs as **root** (the volume root is
  `0700 root:root`, and containers are not user-namespace-remapped in production, so
  container UID 0 is host UID 0 — owner bits alone grant write; a non-root image gets
  `EACCES` and cannot exploit this). Deploy an image declaring `VOLUME /data`, run
  `ln -s .. /data/x` inside the container, then `POST /update` to an image declaring
  `VOLUME /data/x`: update preflight validates service names, fixed host ports, the
  registry allowlist and SKU profiles, never the image's `VOLUME` set or on-disk state.

  **The reach is the lease's own volume root — not a sibling tenant's volume and not the
  host filesystem**; `../..` and absolute targets are still refused by `os.Root`, and
  `sanitizeVolumePath` still rejects anything that lexically Cleans above the root. What
  the root holds is the problem. The `.fred-project-id` marker becomes tenant-writable, and
  it is read back unvalidated: `resolveProjectID` prefers the on-disk file over the
  in-memory map, `EnsureQuota` re-tags the directory into whatever project ID the file
  names and re-applies that project's limit, and `Destroy` clears it — so a tenant-authored
  value redirects XFS quota operations onto **another lease's** project. It also falsifies
  the premise `isWritablePathOnly` documents and depends on ("the container can't write the
  volume root"), which decides at close whether a volume is retained or reclaimed.

  Fred **rejects and does not repair**, matching kubelet's handling of the equivalent
  subPath flaw: the planted link is left in place, and the tenant's data under it is
  untouched. The refusal is scoped to the VOLUME paths the deployed image declares, so it
  blocks exactly the deploy that would redirect a mount and leaves the volume otherwise
  usable — a manifest that does not declare the poisoned path deploys normally, a failed
  update still rolls back to the previous image, and the tenant can delete the link from
  any container that mounts its parent. Nothing is stranded and no operator action is
  required; the new counter records attempts. (ENG-795)
- The development `mock-backend` now verifies Fred's method/URI/body-bound HMAC
  on every contract route and caps authenticated request bodies at 2 MiB;
  `GET /health` and `GET /stats` remain public. This closes the prior path where
  any network client could provision or deprovision the in-memory backend and
  supply an outbound callback destination. Its default listener is now
  loopback-only (`127.0.0.1:9000`); an explicit non-loopback
  `MOCK_BACKEND_ADDR` remains available for isolated E2E networks.

## [0.13.0] - 2026-08-20

### Added

- **`GET /readyz`** on `providerd` serves the same body as `/health` with the verdict
  carried into the status code: `503` when a local bbolt store is unreadable, `200`
  otherwise — including when the chain or every backend is unreachable, because
  `degraded` means `providerd` is still ready to serve. Nothing routes on it,
  deliberately: it exists so uptime monitoring and blackbox probes can see the
  `unhealthy` tier, and so an operator can `curl` one path for the deep verdict. **Do
  not point a load balancer at it** — the fault it reports is one a process supervisor
  fixes by restarting and a load balancer cannot fix at all.

- **`fred_health_check_healthy{check}`** exports each non-backend dependency probe
  (`chain`, `token_tracker`, `placement_store`, `payload_store`; 1 = healthy). Three of
  those four had **no health metric at all** — the chain has latency and transaction
  counters, but nothing that answers "can providerd reach it right now", and the two
  bbolt stores had nothing whatsoever. The `/health` status code was their only
  signal, and this release removes it, so the gauge is not a nicety but the replacement
  signal. Alert on it rather than on the status code. Backends are deliberately absent:
  `fred_backend_healthy` already carries a per-backend label this gauge's single `check`
  label could not express.

  Both gauges are written only from inside the health handlers, so each is exactly as
  fresh as whatever polls `/health` or `/readyz`; with no prober they latch at their
  last value instead of going absent. That is why `/health` still probes every backend
  even though backends no longer move its status code — `Router.HealthCheck` is the sole
  writer of `fred_backend_healthy`, and this handler is its only non-test caller.

- **The payload store is now health-checked.** `payload.Store.Healthy` has existed all
  along with nothing calling it, so a `payloads.db` that had been lost, truncated or
  opened against the wrong path was invisible to the health endpoint and to metrics
  alike — while `/update` returned `500` and every reprovision of a lease that needs a
  manifest failed outright. It now reports as `payload_store`, and is omitted rather
  than failed when `payload_store_db_path` is unset.

  Note what the probe does and does not prove: it is a read-only bbolt `View` that
  opens the database and checks its buckets exist. A full or read-only filesystem
  passes it and still fails every write, so it is not a substitute for disk alerting.

- `fred_docker_backend_retention_sweep_total{outcome}` and
  `fred_docker_backend_retention_accounting_refresh_failed_total` make a degraded
  retention store visible for the first time (ENG-680). Until now the only trace of
  an unreadable `retention.db` was a single log line per sweep tick — hourly at the
  default cadence — behind no metric at all, while the reaper reclaimed nothing, the
  orphan pruner reclaimed nothing, every lease close skipped volume teardown, and all
  five retention gauges silently held their last values. The sweep counter increments
  exactly once per pass, so `sum without (outcome) (increase(...))` is a liveness
  heartbeat and `{outcome="error"}` is the degradation signal; the accounting counter
  is the separate "the gauges you are reading are stale" signal, since the refresh runs
  from every retention transition rather than only from the sweep. Both are
  pre-initialised to 0 so "never failed" is distinguishable from "not reporting".
  Deliberately no last-success gauge: one only advances on a fully clean pass, which is
  why the deployed rules already refuse to alert on the equivalent reconciler gauge.

- `mock-backend` now serves `GET /retentions` and `GET /stats`, the two backend
  contract endpoints it was missing. Their absence was not cosmetic: fred's
  client treats any non-200 from `/retentions` as a failure, so a mock-backend
  fleet left the reconciler's retention sweep permanently incomplete, which in
  turn silently disabled the placement pruner. Any test or dev session that
  concluded "placement records were preserved" against this binary was
  observing a short-circuit rather than the behaviour it meant to check. A
  missing `/stats` similarly made every mock backend look like it had no usable
  load signal.

- `fred_docker_backend_volume_destroy_refused_total{site,reason}` counts managed
  volumes the ownership check declined to destroy, per volume, whichever path asked
  (ENG-658). A refusal is the guard succeeding, not a fault: `reason="claimed"` means
  an in-flight restore owns those bytes and the close correctly left them alone, which
  clears when that restore commits or rolls back — expected at a low rate wherever
  tenants restore and close concurrently, and worth investigating only if sustained
  (read it with `restore_finalizer_pending_total`). `reason="claims_unreadable"` is
  the ticketing signal: the retention store could not be read, so ownership was
  unprovable and nothing was destroyed. Distinct from
  `retention_reap_skips_total`, which counts per reap *attempt* — the two are
  deliberately not summable.

- `fred_signer_grant_check_total{outcome}` counts sub-signer authz grant sweeps,
  incremented exactly once per pass of the sub-signer maintenance loop, so
  `sum without (outcome) (increase(...))` is a loop-liveness heartbeat and
  `{outcome="error"}` is the failure signal. It exists because a `providerd` that
  cannot verify its grants no longer sheds its sub-signers (ENG-688), which means
  `fred_signer_pool_lane_count` correctly stays put and nothing else would move —
  this counter is the only signal that the verification is still failing.
  Deliberately not paired with a `*_last_success_timestamp` gauge.

### Changed

- **Test-only scaffolding no longer compiles into fred's production packages**
  (ENG-354). Declarations whose only callers were tests — `leasesm`'s
  `FireProvisionXxxForTest` helpers, `Manager.Tracker` / `Manager.TimeoutChecker`,
  `EventBroker.subscriberCount`, `CallbackAuthenticator.ComputeSignatureWithTime`
  and `hmacauth.SignRequestWithTime` among them — sat in ordinary `.go` files, so
  nothing stopped a production call site from being added to one. They now live in
  in-package `export_test.go` files, or were deleted once their callers moved to the
  production seam underneath, and `internal/testutil` carries a guard test that fails
  on a new one. The linker had already been dropping most of them (`go tool nm` on the
  previous `docker-backend` finds zero `FireProvision` symbols) but not all: three —
  `TrackInFlightWithStartTime`, `Manager.Tracker` and `Manager.TimeoutChecker` — were
  present in the shipped `providerd` and are not any more.

  Moving the scaffolding out surfaced one declaration that should not exist at all:
  `api.MaxCallbackMaxAge` bounded a callback replay window that only a test-only
  constructor ever set, so it documented a limit no production path could reach —
  `NewCallbackAuthenticator` pins `DefaultCallbackMaxAge` and is the only way
  production builds an authenticator. Both the constant and that constructor are
  removed rather than relocated. Replay-window tests now narrow `maxAge` directly,
  which the in-package test file can do without production carrying a second
  constructor; secret validation still runs through the production one.

- **The k3s backend no longer carries test-only hook fields, and the guard can now
  catch that shape** (ENG-765). ENG-354's sweep moved test-only *declarations* out of
  production packages, but its guard only walked top-level declarations; widening it to
  struct fields surfaced two the phrase list still could not name —
  `beforeDiagnosticStore` and `beforeCallbackSend`, both documented "Nil in production;
  set in tests", each a `func()` the stub provisioner called mid-flight so a test could
  freeze a worker goroutine at an exact interleaving. `runStubProvisioner` is now split
  into the three phases it always had — claim under the lock, persist the diagnostic,
  send the callback — and the two ENG-189 teardown-race tests drive those phases in
  order with a real `Deprovision` between them, so the interleaving is expressed by
  where the call sits rather than by a hook in production code. Each cancellation check
  lives inside the phase it guards, so a test driving one phase still exercises it.

  Removing the hooks cost one guarantee that had to be replaced rather than dropped: the
  hooks were the only thing pinning that the failure diagnostic is written *before* the
  callback is sent. That order is a tenant-facing contract — the callback is what makes
  Fred deprovision, and a deprovision arriving first cancels the lease context, after
  which the diagnostic is suppressed and `GetProvision`'s documented diagnostics
  fallback has nothing to surface for a failed lease. A new test asserts the diagnostic
  is already durable at the moment the callback arrives, checked inside the callback
  handler so it cannot pass on timing.

  A third field goes with them, for a different reason: `k3s.Backend.httpClient`,
  which `New` assigned and then never read, since the callback sender receives the
  local. Removing it left the client's construction without a seam, so that moved into
  `newCallbackHTTPClient` — which means `callback_insecure_skip_verify` finally has
  coverage of the transport it wires up, rather than only of the config rule that
  rejects it in production mode.

  `internal/backend/k3s/**` also joins `.github/workflows/integration.yml`'s path
  filters. `cmd/k3s-backend`'s integration test execs the binary built from that
  package, so until now a change confined to it ran that suite only nightly — the same
  rot mode (ENG-330) the filter's reconciler entry already guards against.

- **The docker backend no longer carries a callback HTTP client field that only tests
  read** (ENG-765). `Backend.httpClient` was assigned once in `New` and never read
  again: the callback sender receives the *local* variable, so the field was a slot
  tests wrote and a test helper read back, documented "exposed for test replacement" —
  one word away from the phrase ENG-354's guard already looked for, which is why it
  survived that sweep. It is gone, and the test helper takes the client as a parameter
  instead.

  Extracting `newCallbackHTTPClient` in the field's place gives
  `callback_insecure_skip_verify` its first coverage of the transport it wires up,
  rather than only of the config rule that rejects it in production mode.

  The guard's phrase list now keys on `exposed for test` instead of the longer
  `exposed for testing`, so the variant wordings that hid this field are caught too.

- **`providerd` no longer ships a `Manager` field that only tests read** (ENG-765).
  `Manager.handlers` was assigned in `NewManager` and never read again — the five
  Watermill registrations use the local — so the field existed solely so tests could
  invoke a handler without publishing a message. Unlike the other fields this ticket
  removed, this one was in `providerd`'s dependency graph and shipped. Tests now
  rebuild the handler set through the production constructor `NewHandlerSet`, which
  also means a new `HandlerDeps` field cannot be wired in production and silently
  missed in tests.

  This is the last of five instances of one shape: a production struct field written
  in production and read only from `_test.go`. Three were found by enumerating every
  unexported production struct field rather than by the guard, which cannot see this
  shape at all — there is nothing in a name or doc comment to match. That limit is now
  written down in the guard's header.

- **The retention sweep now runs every stage instead of aborting at the first store
  error** (ENG-680). `List`, `ListExpired`, `ListReaping` and `ListRestoring` are one
  traversal over one bucket, so they fail on identical inputs — which meant the sweep
  always died at the first of them and never reached the orphan reconcile. Its
  `retention_orphan_skips_total{reason="store_error"}` arm was therefore unreachable
  under precisely the condition it was written to report. The stages are independent by
  construction (each re-reads the store for itself, and each already fails safe on its
  own read), so running them all can add information but never a destroy. Stage errors
  are joined, so the sweep's log line now names every failing step rather than only the
  first casualty.

- **Every managed-volume destroy in the docker backend now goes through a single
  ownership-checked primitive** (ENG-658). Six call sites each derived their own set
  of volumes to destroy — three from a name prefix, which does not prove ownership:
  while a restore is in flight, the original lease's data physically wears the new
  lease's canonical name, so a close, a cap-refusal, an orphan sweep or a reaping
  tombstone could all reach data belonging to another lease. Each site carried (or
  forgot) its own guard, and the recurring result was a data-loss ticket per site
  (ENG-505, ENG-501, ENG-523, ENG-647, ENG-659). Ownership is now resolved once, from
  the live provision map plus the retention store, and a volume is destroyed only when
  the lease asking owns the bytes; the orphan sweep is the same rule asked with no
  lease ("destroy only what nothing claims"). The two hand-written derivations of
  "which volumes a restore has claimed" are gone, and `Destroy` has been removed from
  the internal volume-manager interface so reaching it any other way does not compile.
  No configuration or API change; a refusal is visible as
  `fred_docker_backend_volume_destroy_refused_total` (above).

- Build: the Go toolchain floor moves to **1.26.6** (`go.mod` directive and the
  release image's builder stage). This clears six standard-library advisories
  published on 2026-08-13 that the CI vuln gate treats as called —
  GO-2026-5026, GO-2026-5972, GO-2026-6089, GO-2026-6090, GO-2026-6091 and
  GO-2026-6218, spanning `net/http`, `crypto/tls`, `html/template`,
  `encoding/asn1` and `net/url`, all fixed in Go 1.26.6. No allowlist entry is
  needed and none was added: a stdlib fix that exists is not an accepted
  exception.

- Build: `make lint` now **fails** when `golangci-lint` is missing from `PATH` or
  is not the pinned version, instead of printing "not installed, skipping" and
  exiting 0. A local `make lint` previously proved nothing — and a v1 binary is
  worse than none, since it does not reject the v2 `.golangci.yml`, it ignores
  the v2-only keys and silently runs a different linter set. Running `make lint`
  now requires installing the pinned version (see CONTRIBUTING.md § Linting).
  The version moved to a new `.golangci-lint-version` file, read by the make
  target and by both CI workflows, so the three can no longer disagree.

### Deprecated

### Removed

### Fixed

- **A transient chain-RPC error at startup no longer permanently demotes the signer
  pool** (ENG-688). `providerd` verified its authz grants once at boot and, on *any*
  error, discarded every sub-signer for the rest of the process lifetime — a decision
  nothing could undo. On dev that fired on a plain startup race: the daemon came up six
  seconds after the chain node restarted, `grpc.NewClient` dials lazily so the failure
  surfaced at the first RPC, and one `Grants` query timed out. All nine grants were on
  chain the whole time; only the *read* failed. Dev then signed on one lane for three
  weeks. Every environment was exposed on every restart, i.e. every fred pin bump.

  A failed query is not a fact about the grants. They are created with no expiration, so
  once created they stay, and the chain re-checks the authorization inside every
  `MsgExec` regardless — the startup query is observability, not a gate. `EnsureGrants`
  now reports a query failure as `chain.ErrGrantsUnverified`, and `providerd` falls back
  to single-signer **only** on the opposite verdict: the queries succeeded, said the
  grants are missing, and creating them failed. That one is a positive fact worth acting
  on, because signing with an ungranted sub-signer burns a failed on-chain tx per batch.
  Startup additionally retries the whole setup three times (1s, doubling, capped at 10s)
  inside its existing 60-second budget. That is a narrowing, not the fix: in the dev
  incident the chain was still unreachable three seconds later — the initial withdrawal's
  own three-attempt ladder failed across the same window — so the retry would not have
  saved that boot. What saves it is not demoting on the failed read, and the sweep below.

  The degraded state was expensive and quiet: ack throughput drops from ~150 to ~50 per
  block, every ack/reject/close serialises on the provider account's sequence so the
  provider burns the fees the sub-signers exist to absorb, and the
  `fred_signer_balance{role="sub_signer"}` series disappear with the pool — which blinds
  `SubSignerLowBalance` and `SubSignerTopUpStalled`, since a bare `< threshold`
  comparison matches nothing over an absent series.

- **Sub-signer authz grants are now re-checked on every maintenance sweep, not only at
  boot** (ENG-688). The periodic `sub-signer funding` loop is renamed `sub-signer
  maintenance` and runs `EnsureGrants` before `EnsureFunding` on the same
  `sub_signer_fund_check_interval` tick, each half under its own 60-second budget so a
  sluggish grant sweep cannot starve the top-ups. Both halves are level-triggered, so a
  boot that could not verify converges on its own instead of staying degraded until an
  operator restarts the daemon — this is the half that actually retires "permanent for
  the process lifetime" as a property. Note the
  consequence: a grant revoked out-of-band is re-created on the next tick — the daemon
  converges its own desired state, which it already did on every boot.

- **`providerd` no longer logs `chain client connected` before anything has dialed**
  (ENG-688). `grpc.NewClient` is lazy and connects on the first RPC, so the line said
  "connected" about a chain that was still starting up while the first real query timed
  out — it is what made the incident above take three weeks to attribute. It now reads
  `chain client configured`.
- **The lease acknowledgment batcher is now owned by the provision manager's lifecycle
  rather than by its constructor** (ENG-723). `NewManager` started the batcher's lane
  goroutines on a fresh `context.Background()`, so merely constructing a manager
  launched work that no context the manager held could stop — the shape of ENG-592,
  where work that captured the wrong context silently leaked every migration's `-prev`
  containers. Today's blast radius in `providerd` is bounded: its one manager is
  closed on the graceful-shutdown path, which stops the batcher explicitly, and every
  other exit from `run` returns an error `main` turns straight into `os.Exit(1)`.
  The defect is the ownership, and the one path that made it bite was
  `Close()` itself — it returned early when the Watermill router failed to close,
  skipping the batcher shutdown entirely.

  The manager now creates a lifecycle context in `NewManager` and starts the batcher
  from `Start`, before `wmRouter.Run` subscribes the handlers that call `Acknowledge`.
  The lanes' lifetime is unchanged: that context is rooted at `context.Background()`
  and is ended by `Close()`, exactly as the constructor's `context.Background()` was.
  It is deliberately not derived from the ctx passed to `Start`, which `main` cancels
  partway through its shutdown sequence, several steps before it calls `Close()` —
  deriving from it would couple lane teardown to a context that dies mid-shutdown.
  `Close()` now runs every shutdown step even when an earlier one fails and joins their
  errors, `AckBatcher.Start` is once-only, and an `Acknowledge` that somehow arrives
  before `Start` fails with the existing retryable lane-unavailable error — so Watermill
  redelivers it — instead of blocking its handler goroutine indefinitely.

- **`docker-backend` and `k3s-backend` no longer export `providerd`'s metrics at a
  permanent 0** (ENG-712). Collectors are created with `promauto`, which registers on
  Prometheus's default registerer at *package init*, so a binary exports everything its
  dependency closure declares whether or not it has a call site. `internal/backend/docker`
  imported `internal/metrics` for a single panic counter, and each backend consequently
  published 21 `providerd`-only collectors — 27 metric names once the histograms expand
  into `_bucket`/`_sum`/`_count` — describing a signer pool, withdraw loop, chain client,
  payload store, provisioner and backend router that the process does not have. Those 27
  names are ~59 *series* per backend once each histogram's buckets are counted, so roughly
  177 stray series in a three-backend environment and 531 on a nine-backend fleet.

  For counters this was noise, since `rate()` over a flat 0 is 0. For gauges it was a
  trap, because a 0 satisfies ordinary comparisons: `fred_signer_pool_lane_count < 3`
  returned exactly the three docker-backends, which have no signer pool, and *not*
  `providerd`, which does — wrong in both directions at once, and silent.

  **No metric is renamed and no alert rule needs changing.** The affected series simply
  stop appearing under the backend jobs, where they never meant anything; every existing
  rule that queries one without a `job` matcher is in the `>` direction over series that
  were flat 0, so firing behaviour is unchanged. The `fred_background_cleanup_panics_total`
  and `fred_background_goroutine_panics_total` counters — the one family every binary
  emits — keep their names and keep being emitted by all three, from the new
  `internal/metrics/background` package. Expect the removed series to disappear from the
  backend jobs at the deploy that picks up this release.

  Each backend `cmd` package now asserts its `/metrics` surface carries only its own
  prefix, and a `depguard` rule blocks the import that caused it.

- **One impaired dependency no longer takes the whole provider offline** (ENG-522,
  ENG-608). `providerd`'s `GET /health` computed a single boolean over every probe —
  chain gRPC, every backend, and the token-tracker and placement-store bbolt DBs (the
  payload store was never probed at all) — and answered `503` if any one of
  them failed. Deployments health-check that exact path from a load balancer whose
  pool holds `providerd` as its **only** server, so a single failing probe removed the
  provider outright and every `/api/fred/*` request became `no available server`. It
  also severed the backends' completion-callback route, which shares that listener and
  vhost, so a backend that had finished its work could not say so and the lease was
  rejected minutes later with `reason="callback timeout"` while the tenant's workload
  kept running, unbilled and unmanaged.

  This was never a hypothetical. On mainnet-morpheus a ~1-minute upstream chain-RPC
  blip failed one `Ping`, dropped the only server for a single 30s probe interval, and
  cost a provision that had already succeeded. On dev, stopping one docker backend of
  three held the tenant API down for 15 minutes (38 × `503` against 9 × `200`), failed
  four tenant provisions and orphaned a workload — while `providerd` answered
  correctly the entire time when reached directly.

  `/health` is now a liveness contract: **it returns 200 whenever the process can
  answer, and never 503s because a dependency is impaired.** No probe here justifies
  de-registration — there is no peer to shed load onto. Exact callback application
  may itself need the chain or placement store; keeping the route reachable lets
  providerd return a deliberate retryable 503 so the backend preserves its durable
  FIFO head, whereas load-balancer removal severs that recovery protocol entirely.
  The reconciler's behaviour is unchanged; it was already correct throughout both
  incidents.

  Two things had to change for "never 503" to be literally true rather than merely
  true of the verdict. Both endpoints are wrapped in `requestTimeoutMiddleware`, which
  is `http.TimeoutHandler`, and **its timeout response body is a 503** — so a slow
  enough probe made the endpoint emit the very status code it exists to avoid, and the
  server's `http_write_timeout` (15s by default) severed the connection even earlier.
  The whole dependency sweep is therefore bounded by `healthProbeBudget` (10s, under
  both defaults), and `Router.HealthCheck` now probes backends **concurrently** rather
  than serially. Serial probing made the worst case the *sum* of every backend's client
  timeout — 30s each by default — so one backend that accepted a connection and never
  answered was enough to blow the request budget by itself. Concurrency makes it the
  *max*, which is what lets the budget hold without starving the backends probed last.
  A backend that merely *refuses* connections fails in milliseconds, which is why both
  real incidents stayed well inside any budget; a *hung* one was the gap.

  Because those probes now run on their own goroutines, they are also recovered:
  net/http recovers a panic raised on its own request goroutine but not one raised on a
  goroutine the handler spawned, so an unrecovered panic in a backend client would have
  gone from failing one request to taking the daemon down. A panicking probe counts as
  unhealthy and increments `fred_backend_health_probe_panics_total`, which is always a
  bug when non-zero.

- **A degraded dependency is now distinguishable from a broken one.** The health body's
  `status` gains a third verdict alongside `healthy` and `unhealthy`, reusing the
  vocabulary the reconciler already established: `degraded` means a **remote, shared**
  dependency is impaired (the chain, or one or more backends) and `providerd` is still
  serving; `unhealthy` means a **local, process-owned** bbolt store is unreadable and
  the process wants restarting. The distinction is the whole point — the old boolean
  gave a stopped backend and a corrupt database the same answer, and that answer was
  the one that caused an outage. Per-check results are unchanged, and a check absent
  from `checks` still means "not configured", never "passed".

- **An unmounted volume root can no longer be mistaken for a node with no volumes**
  (ENG-687). A plain `umount` leaves the mountpoint directory in place on the parent
  filesystem, so enumerating it succeeds and returns an empty result with no error — and
  every consumer reads that as "nothing here". The consequence was not cosmetic: the orphan
  reconcile treats an absent volume as evidence its retention record is orphaned, so an
  empty enumeration made *every* active record look orphaned, pruned them after the
  confirmation streak, and left their volumes with no record naming them — so the next
  boot's orphan sweep destroyed retained tenant data.

  Two guards, both in the enumeration primitive so every caller inherits them. At startup
  the configured `volume_filesystem` is now verified against the filesystem actually at
  `volume_data_path`; previously the probe ran only when the value was left blank, so a
  provider that pinned it — as the reference deployment does — started happily on a root
  that was not the filesystem it named. At runtime, the first enumeration that finds volumes
  records the device backing the root, and a later *empty* result must come from that same
  device or it is reported as uncertainty rather than emptiness. A root that has never held
  a volume is unaffected, and a genuine reclaim still reports empty, so the reaper converges
  normally.

- **A deprovision give-up no longer loses the abandoned footprint when the retention
  store is degraded** (ENG-676). A close that cannot establish volume ownership —
  because the retention store cannot be enumerated — correctly destroys nothing and
  retries, but after the attempt limit it gives up: it releases the lease's pool
  reservation and deletes its provision, which is right, since nothing can retry after
  that. The reaping record it writes is then the only thing still counting the bytes on
  disk. That record used to double as a destroy plan, and computing the plan needed the
  same ownership table the store could not serve — so the write was skipped entirely
  and the footprint ended up counted by **nothing**: no pool reservation, no active
  record, no reaping record. Fred over-admitted against real disk, permanently, until an
  operator intervened.

  The record now states one fact — how large the abandoned footprint is — and authorises
  no destruction, so there is nothing left for a degraded store to prevent it computing.
  Precisely: the footprint is uncounted **for as long as the store stays broken** (the
  projection is recomputed by scanning the store, and the store is what is broken), but the
  record is durable, so the projection self-corrects on the first readable refresh instead
  of there being nothing to recount from. The change is "uncounted permanently" →
  "uncounted until the store is repaired"; both halves are pinned by tests.
  The finalizer re-derives the volumes to reclaim on every sweep from the lease's
  namespace on disk intersected with the ownership table, which is the same
  "destroy only what nothing claims" rule the startup orphan sweep already applies,
  scoped to one lease. A reaping record with an empty volume list is therefore normal
  and not corruption. This also removes the last way a tombstone written by an older
  build could name another lease's adopted data: those stored names are no longer read
  by anything.

- **The ownership check that authorizes a volume destroy can no longer be stale by the
  time the delete runs** (ENG-681). ENG-658 made every managed-volume destroy ask who owns
  the bytes, but it asked once per operation and cached the answer, and nothing serialized
  that answer against the writes that establish a claim. The retention sweep could
  therefore read a give-up tombstone's canonical name as unclaimed, and only then have the
  lease re-provisioned underneath it and the directory reused — after which the sweep's
  loop deleted a running tenant's data. The claim is now re-established under a per-volume
  lock that the create-or-reuse in the provision path also takes, so the decision and the
  `RemoveAll` it authorizes are one step, and a provision that has adopted the directory is
  seen rather than deleted out from under.

  ENG-658 also left a second, wider gap unaddressed, closed here: a lease's claim is the
  set of canonical volume names derived from its items, and the provision reservation
  published no items at all — they were filled in only after SKU validation, manifest
  parsing and pool allocation. For that whole window a provision claimed nothing, and on a
  re-provision it was worse than a gap: the previous, claim-bearing entry is deleted and
  replaced in the same critical section, so a live claim was *retracted* over volumes that
  a re-provision deliberately keeps. The reservation now carries its items from the start,
  as the restore path's already did.

  No configuration, API or metric change: a refusal from the re-check is counted as the
  same `fred_docker_backend_volume_destroy_refused_total{reason="claimed"}` as any other,
  and reaches the finalizer's existing `retention_reap_skips_total{reason="owner_claimed"}`.
  The WARN a kept reaping record emits now names *which* hold it is — a restore's, which
  clears on that restore's rollback, or a live provision's, which does not and must not be
  reclaimed by hand. It previously told operators to wait for a rollback in both cases.

- **A reaping tombstone can no longer destroy a re-provisioned lease's live data**
  (ENG-658). ENG-659 stopped the retention finalizer from executing a tombstone that
  named a volume an in-flight *restore* had adopted, but it asked a question scoped to
  restores only. A second collision was left open: a deprovision give-up writes a
  tombstone naming `fred-{lease}-*` and deletes the provision, while the lease is still
  ACTIVE on chain — so the reconciler re-provisions it, a fresh volume appears under
  exactly the name the tombstone carries, and the next sweep reaped the running lease's
  data. The finalizer now asks the shared owner table, which knows about live
  provisions as well as retention records, and refuses; the record is kept so the
  genuinely abandoned names stay counted and retryable. This is the ENG-505 class
  reached through the finalizer rather than the orphan reaper.

  **Operators:** this hold is counted as a new
  `fred_docker_backend_retention_reap_skips_total{reason="owner_claimed"}`, kept
  distinct from `restore_claimed` because the two resolve differently — a
  restore-held name clears when that restore rolls back, whereas this one clears only
  when the owning lease is next closed cleanly, so the record can legitimately sit
  `reaping` for as long as that lease lives. On a provider carrying such a tombstone
  from an older build, expect `BackendRetentionVolumeStuckReaping` to start firing
  after the upgrade; check the reason label before reclaiming anything (the volume is
  a running tenant's data). The alert's triage annotation in the deployment repo
  should gain this third case.

- **Tenant deployment updates are no longer silently reverted by the next
  reprovision** (ENG-619). `POST /v1/leases/{uuid}/update` applied the new
  payload to the backend but never wrote it to `payloads.db`, which is the store
  the reconciler replays from. Every reprovision — a maintenance reboot, a host
  failure, a crash-restart — therefore brought the lease back on its *as-created*
  manifest, with no error and no signal anywhere; the backend then recorded the
  reverted manifest as the lease's active release, so the revert stuck. Observed
  on mainnet, where a reboot rolled a production admin UI back two minor
  versions after three weeks of running the updated image.

  The fix is not simply "persist it". The reprovision path re-verified the stored
  payload against the lease's on-chain `meta_hash`, which is set once at lease
  creation and immutable, so an updated payload would have failed that check,
  been deleted as corrupt, and then had its ACTIVE lease **closed on-chain** —
  worse than the revert. The payload store is now self-describing: it records
  each payload's own SHA-256 in a separate `payload_hashes` bucket, written in
  the same transaction as the payload, and the reprovision path verifies against
  that. `meta_hash` remains the reference for payloads written before this
  change, and ENG-643 restores it as the authoritative check once the chain can
  carry a per-update commitment.

  Operator-visible consequences: `/update` now answers `500` (instead of a
  misleading `202`) when the payload cannot be persisted, and counts it in the
  new `fred_payload_persist_failures_total{operation}` counter — each increment
  is a lease whose running deployment fred has no durable record of. Providers
  with `payload_store_db_path` unset now have `/update` rejected outright rather
  than silently applied and lost.

- `/update` requests to backends now carry `payload_hash`, the field both
  `README.md` and `BACKEND_GUIDE.md` have always documented for that endpoint
  but which fred never populated, leaving third-party backends unable to verify
  a payload the contract said was verifiable.

- **A reaping tombstone can no longer destroy an in-flight restore's data**
  (ENG-659). The retention finalizer destroyed every volume name a `reaping`
  record carried, with no ownership check — the only volume-destroy path in the
  docker backend without one. ENG-647 stopped a deprovision give-up from
  *writing* an adopted volume's name into such a record, but tombstones are
  persisted and outlive the binary: a provider upgrading from an older build
  still carries records written before that guard existed, and the next sweep
  executed them, permanently destroying the data a restoring lease had adopted.
  The finalizer now re-checks ownership at destroy time — a name an in-flight
  restore claims is skipped and logged, and an unreadable retention store means
  nothing is destroyed that pass, mirroring the orphan reaper's fail-safe. A
  skipped name leaves the record `reaping`, so its footprint stays counted and
  the next sweep retries once that restore's rollback re-quarantines the volume
  (the only resolution reachable for a tombstoned lease, which has already lost
  its provision); it is deliberately **not** counted as a leak. New
  `fred_docker_backend_retention_reap_skips_total{reason}` counts both cases
  (`restore_claimed`, `claim_unreadable`); see the updated
  "Reclaiming leaked / stuck-reaping orphan volumes" runbook, since a
  deliberately-held tombstone must **not** be reclaimed by hand.

- **A failed container teardown is no longer treated as a completed one**
  (ENG-647). When `compose down` failed, the docker backend logged the error and
  carried on as though the containers were gone. On the restore-rollback path
  that meant reverting the retention record, handing the lease's capacity back to
  the pool, and dropping the provision while its containers were possibly still
  running — after which nothing could reach them: the orphan reconciler only sees
  leases the backend still tracks, and the volume reaper enumerates fred's own
  data directories, never Docker's anonymous-volume store. Their anonymous
  volumes then accumulated silently, the leak class that once taxed every
  `compose up` on a dev backend. Compose v5 made it likelier, since it now runs
  its per-container removals on a context the first failure cancels, so more
  containers survive each failed teardown.

  Teardown now compensates and then confirms. A failed `down` falls back to
  removing the lease's containers individually — reaping their anonymous volumes
  with them — and finds those containers by re-querying the daemon for fred's
  labels rather than trusting the provision's recorded container list, which is
  empty for exactly the leases that leak (a restore that never reached Ready
  records no container IDs at all). Every teardown path shares this, so a close, a
  failed provision's cleanup, and both restore-rollback arms behave the same.

  If containers still survive, fred no longer advances state over them: the
  restore rollback stops before re-quarantining the volumes, leaving the retention
  record, the provision, and the pool reservation in place for the next sweep to
  retry. This matters because the re-quarantine renames a directory a surviving
  container still holds open, which would leave it writing into data fred has just
  marked frozen. Nothing is lost while it waits — a restoring record is
  deliberately exempt from expiry, and its volumes are protected from the orphan
  sweep — with one exception, also fixed here: closing the *new* lease while a
  restore into it was still in flight would destroy (or re-retain under the wrong
  lease) the original lease's retained data, permanently ending its ability to be
  restored. Those volumes are now recognised and left alone.

  The wait is safe but **not** time-bounded. That same exemption from expiry means
  a tenant cannot re-request the restore for as long as the substrate stays broken;
  it becomes claimable again only once teardown succeeds. A sustained
  `fred_docker_backend_teardown_fallback_total{outcome="failed"}` on a blocking
  operation is the signal to go fix the daemon, not something that clears itself.

  New metric `fred_docker_backend_teardown_fallback_total{operation,outcome}`.
  A rising `outcome="failed"` means the fallback could not finish either, but
  what that costs depends on the `operation`: the *blocking* paths
  (`restore_reconcile`, `restore_rollback`, `deprovision`) hold the lease and its
  capacity open and retry, while the *advisory* ones (`restore_prelude`,
  `provision_cleanup`) let state advance regardless, so nothing retries — and
  `restore_prelude` in particular is routinely just a canceled restore request
  with nothing on the host. See OPERATIONS.md for the triage split.

- **The reconciler no longer destroys a lease's state without the chain
  confirming the lease is finished** (ENG-654). Orphan deprovision and orphaned
  payload cleanup both treated "absent from this sweep's lease snapshot" as
  "gone". That snapshot is assembled from two non-atomic queries filtered to
  `PENDING`/`ACTIVE`, so absence actually covers three different situations:
  terminal, created seconds ago, and never known to this chain at all. Both
  passes now re-read the lease (`GetLease`) per candidate and act only on a
  positively reported `CLOSED`/`REJECTED`/`EXPIRED`.

  The third case was the dangerous one. `x/billing` never deletes a lease — a
  close writes `State` in place — so a chain with no record of a lease has never
  heard of it, which happens when providerd is pointed at the wrong or a reset
  chain. Both lease queries then return **empty with no error**, which previously
  made every provision on every backend an orphan candidate. A query error is
  likewise not absence. And terminality is an **allowlist** of the three terminal
  states rather than "anything that is not PENDING or ACTIVE": `LeaseState` is a
  bare `int32` decoded as a raw varint with no validation, so a state added to the
  chain after a providerd build ships arrives as an unrecognized number, and a
  denylist would read the whole fleet as terminal the day that happens.

  Each of these keeps the state and increments
  `fred_reconciler_cleanup_skips_total{pass,reason}`. Two reasons log at WARN
  because they do not self-heal, and their remediations are opposites:
  `chain_unknown` (no record — check the endpoint, or clean up a phantom by hand)
  and `chain_unknown_state` (the chain is fine; upgrade fred).

- **A backend that does not answer no longer pauses cleanup for the backends that
  did** (ENG-654). The three destructive passes were gated on a complete fleet
  view, which preserved the pre-ENG-356 abort behavior exactly but coupled every
  healthy machine to the sickest one: through a multi-hour outage, orphans held
  admission reservations fleet-wide precisely when there was least capacity to
  spare. Each pass is now scoped to what it can attribute. Orphan deprovision
  runs on every sweep — candidates come from backends that answered, so partial
  data can only under-collect — with the chain re-check above covering the hazard
  the gate was standing in for. Payload cleanup runs unconditionally; it compares
  the payload store against the chain and reads no backend state at all.
  Placement pruning keeps its backend evidence requirement but asks it **per
  record**: a record is pruned only if its own backend answered both
  `/provisions` and `/retentions`, so one silent machine costs only its own
  records (counted as `{pass="placement",reason="backend_silent"}`). One
  consequence is deliberate: a decommissioned backend's placement records are
  never auto-pruned, because they are the only surviving pointer to where its
  data was — see OPERATIONS.md § Removing, renaming or pausing a backend.

- **One unreachable backend no longer stops reconciliation for the whole fleet**
  (ENG-356). `fetchAllProvisions` aborted the entire sweep if any single backend
  failed to list its provisions, so self-healing succeeded only when every
  backend answered at once — reliability degraded as `pⁿ`, getting *worse* as the
  fleet grew, and on a 9-backend provider at a 2-minute interval a single quiet
  machine froze reconciliation for the other eight roughly 30 times an hour. The
  abort was a correct safety choice, not a bug: the state matrix classifies from
  the union of all backends, so partial data can make a live lease look
  unprovisioned and get it re-provisioned onto a healthy peer — an empty volume
  laid over live tenant data.

  The sweep now proceeds on partial data and defers exactly the leases it cannot
  positively attribute: one present in the backend data, or one whose placement
  record names a backend that answered, is reconciled; anything else is skipped
  and retried next cycle. Recovery latency becomes independent of fleet size.

  Because a degraded sweep is otherwise silent where the old abort was
  impossible to miss, it ships with its own signals: a distinct
  `fred_reconciler_runs_total{outcome="degraded"}`, a
  `fred_reconciler_sweep_complete` gauge, a per-backend
  `fred_reconciler_backend_fetch_total{backend,outcome}` counter separating
  `error` / `circuit_open` / `panic`, and
  `fred_provisioner_reconciler_deferred_leases_total`. A degraded sweep
  deliberately does **not** advance
  `fred_reconciler_last_success_timestamp_seconds`, so the staleness alert stays
  meaningful during an outage. See OPERATIONS.md § Backend unreachable during
  reconciliation.

  The placement index's retention-derived backfill is suppressed while degraded:
  a retention proves a past deprovision on a backend, not present ownership, and
  that sync is read back by the deferral guard in the same sweep — so writing one
  would manufacture the evidence the guard uses to decide it is safe to act. The
  next complete sweep repaves it. The three passes that delete durable state were
  initially suppressed the same way; see the ENG-654 entry below for how they are
  scoped now.

- **Fred no longer substitutes a backend when a lease's placement record names
  one the router does not know** (ENG-635). Previously both the write and read
  paths logged a warning and fell through to ordinary routing. On the write path
  that meant removing, renaming or pausing a backend holding ACTIVE stateful
  leases caused each of those leases to be re-provisioned on the least-loaded
  peer — creating a brand-new **empty volume** while the tenant's real data sat
  intact on the absent machine. It fired unattended, on a timer, for every
  affected lease at once, and reported success to its caller. Provisioning is now
  refused and retried on the following reconcile cycle (never rejected or closed
  on chain — a paused backend must not terminate paying leases); reads and
  restores return **503, not 404**, because a 404 during what is usually a paused
  or renamed backend tells a tenant their data is gone and invites them to
  destroy and recreate it. A lease with **no** placement record still routes
  freely — that path, which every new lease takes, is unchanged. Recovering a
  removed backend requires restoring its config entry under the original name
  **and restarting providerd**, which reads its backend list only at startup;
  see OPERATIONS.md § Removing, renaming or pausing a backend.

- Tests: the three ENG-372 anonymous-volume integration tests have never
  executed. `make test-integration` selects with `-run Integration`, and these
  were the only `//go:build integration` tests in the package whose names lack
  that substring, so the selector filtered them out from the day they were
  written. CI could not notice: a filtered test emits neither a `--- SKIP:` line
  nor a `--- PASS:` line, so the job's SKIP guard saw nothing and its
  `PASS >= 1` floor was satisfied by the ~85 tests that do run. The one contract
  guarding a silent, cumulative and expensive leak — `compose down` reaping the
  anonymous volumes Docker auto-creates for image `VOLUME` directives — was
  therefore unguarded. Renamed to `TestIntegration_Docker_*`; all three pass
  against the current tree, so this closes a coverage gap rather than a defect.
  The tests were renamed instead of widening the `-run` pattern, which is shared
  by the whole 30-minute suite.

### Security

- **providerd no longer relays an unparseable backend error body to tenants, and no longer
  treats one as the tenant's fault.** ENG-508 closed the stored/async `last_error` leak; this
  closes its synchronous twin on `POST /restore` and `POST /update`. The backend HTTP client
  substituted the raw response body — up to 4 KiB, verbatim — into the error it returned
  whenever a client-error body was not the declared `{"error": ...}` envelope, and
  `internal/api` wrote that straight into its own 4xx body. A body that is valid JSON but
  omits the required `error` field — `{}`, `null`, or a proxy's own `{"message": ...}` — counts
  as off-contract too, on every 4xx fred parses. Only a genuinely **empty** body still means the
  one documented bodiless form (a bare `422` meaning "no retained data"); an unparseable or
  wrong-shaped body is no longer mistaken for it, which previously let an intermediary's error
  page reach the tenant as a confident `404 no retained data found for that lease`. Fred now authors that message
  itself, records the raw body and a new `fred_backend_malformed_error_body_total{backend,
  operation}` counter operator-side, and answers `502` — the fault is upstream. The
  tenant-visible detail a backend *did* author inside a validated envelope still crosses
  (manifest validation messages, the registry allowlist, demote byte counts): those are the
  tenant's own input and the provider's published policy, and suppressing them would only make
  a 4xx unactionable. It is now bounded and stripped of control characters.

  The classification changes with it (ENG-739). An unparseable `400` was wrapped in
  `ErrValidation`, which the reconciler treats as **permanent** — it rejects a PENDING lease
  and **closes an ACTIVE one on-chain** — even though nothing established the backend had
  authored that body. This is an *operator-introduced intermediary being misread as a
  permanent tenant-side failure*, not something a tenant can trigger: `docker-backend` routes
  every 4xx through its error-envelope writer, and the ENG-356 snapshot gate already defers
  every lease of a backend whose `GET /provisions` did not answer, so the reachable variant
  needs a selective failure (a `400` on `POST /provision` while `GET /provisions` still
  answers — a body-inspecting WAF rule). It is now transient, and self-limiting from the
  other side: the client counts it toward the circuit breaker, so a persistently off-contract
  backend degrades into the already-transient circuit-open path instead of retrying forever.

  In practice the shipped `docker-backend` already curates its 4xx bodies — the ENG-438 demote
  gate deliberately withholds the volume-usage error because it can embed host paths — so this
  is hardening rather than a live leak; the in-repo binary that exercised the fallback was
  `mock-backend`, which emitted `text/plain` errors and now emits the envelope.
  `BACKEND_GUIDE.md` gains the matching normative clause: the `error` field is tenant-visible
  and must carry no host paths or raw command output, and `validation_code` is documented for
  the first time. Tenants may now see `502` where an off-contract backend previously produced
  `400`. (ENG-620, ENG-739)
- **A `/restore` rejection carrying an unrecognized `code` no longer becomes a confident wrong
  answer.** `409` and `422` are the two statuses fred overloads on `/restore`, and the client
  compared the body's `code` against a single expected value per status — anything else, whether a
  discriminator a newer backend added or one valid for the *other* status, fell through to the
  no-code branch. On `422` that meant `ErrNotRetained`, which fred **remaps to `404` "no retained
  data found for that lease"**: it changed the status class the backend chose and asserted a
  positive fact about the tenant's data that the backend's own body contradicted, while discarding
  the message `BACKEND_GUIDE.md` obliges the backend to curate. Fred now relays the backend's
  message at the status it sent. Deliberately **not** treated as a malformed body: the envelope was
  well-formed, so it is excluded from `fred_backend_malformed_error_body_total` (whose runbook
  remedy — fix the envelope, suspect an intermediary — would be wrong here) and from the circuit
  breaker, so a backend shipping a new discriminator before `providerd` learns it degrades to a
  plain refusal instead of tripping the breaker for every other tenant on that backend. It is
  logged for operators instead. `already_provisioned` also becomes a shared exported constant like
  `demote_exceeds_tier`, so producer/consumer drift is a compile error. The `409` path keeps
  `ErrInvalidState` deliberately — there fred returns the status the backend chose and invents
  nothing. (ENG-620)
- **A tenant-facing `422` from `/restore` no longer repeats itself.** Fred re-prefixed the
  demote-exceeds-tier message with a sentinel the backend had already baked into it, so the
  body read `retained data exceeds the requested smaller tier: retained data exceeds the
  requested smaller tier: service "app": ...`. The sentinel now travels through the error
  chain for `errors.Is` rather than through string concatenation. (ENG-620)
- providerd no longer returns the verbose backend `last_error` (host filesystem paths + raw
  command stderr) to tenants on `GET /status`, `/provision`, `/releases`, or `/logs`. **Breaking:**
  the `last_error` response field is removed; a K8s-shaped `reason` (machine code) + `message`
  (curated, no host detail) replace it. Verbose detail stays operator-side (diagnostics store +
  structured logs, correlated by `lease_uuid`). The `reason` set is open/add-only — consumers must
  tolerate unknown values and fall back to `message`. (ENG-508)
- **Every third-party GitHub Action is now pinned to a full 40-char commit SHA**
  instead of a floating major tag (`@v3`, `@v4`, ...), across all four workflows.
  The `release` job holds `id-token: write` (Fulcio OIDC), `packages: write`
  (ghcr) and `contents: write`, and GitHub scopes permissions per job, so every
  action it runs executes with the ambient signing and publish identity. A
  hijacked upstream tag — the tj-actions/changed-files 2025 incident is the
  template — could therefore cosign-sign an attacker-built image on the next
  `v*` tag push, and because the certificate identity genuinely reads
  `release.yml@refs/tags/vX`, that image would **pass** `cosign verify`: the
  signature is authentic, only the artifact is not. A new `actions-pinned` CI
  job fails closed on any `uses:` that is not a 40-char SHA, so a later edit
  cannot quietly reintroduce one, and a `github-actions` Dependabot config
  keeps the pins moving under review. `gomod` is deliberately left disabled:
  Go dependency bumps here are driven by the govulncheck gate and its triaged
  allowlist, which needs per-advisory reachability analysis an automated bump
  PR cannot do. (ENG-509)

- deps: migrate `github.com/docker/compose` from `/v2` v2.40.3 to **`/v5`
  v5.4.0**, cutting the govulncheck allowlist from 7 entries to 3. This clears
  GO-2026-4610 (a Windows-only CLI-plugin search-path privilege escalation,
  fixed upstream in `docker/compose` 5.1.0 and `docker/cli` 29.2.0),
  GO-2026-5378 (a containerd `runAsNonRoot` evasion via user-ID handling, fixed
  in containerd v2.2.4 — v5 pulls v2.3.3), and GO-2026-4859 + GO-2026-4858 (two
  BuildKit file-escape flaws, fixed in v0.28.1 — v5 pulls v0.32.1). The called
  set was measured before and after: it goes from 7 IDs to exactly 3, with **no
  new** IDs, despite v5 adding a large number of modules v2 never pulled.

  fred had been pinned to a compose release from 2025-10-30 — roughly 9.5 months
  stale — and no tooling could have reported it. Compose moved to a new **major
  module path**, and a different module path is a different module: `go list -m
  -u`, `go list -m -versions`, MVS and Dependabot are all structurally blind to
  it, so every "is there a newer compose?" check returned "v2.40.3 is newest"
  and was correct but useless. The previous allowlist note for GO-2026-4610
  recorded it as having *no fix anywhere*; that was true of the `compose/v2`
  module and false of the project, for exactly this reason.

  fred's own diff is six lines, because all compose access is already narrowed
  behind the `composeExecutor` interface: the import paths, plus
  `compose.NewComposeService` now returning `(Compose, error)`, plus explicit
  `string(...)` conversions now that `ContainerSummary.State`/`.Health` are the
  distinct named types `container.ContainerState`/`HealthStatus`. All 11
  production call sites and every test are unaffected. `internal/backend/docker`
  now imports `github.com/moby/moby/client` (compose v5's and docker/cli v29's
  client) alongside `github.com/docker/docker/client`, which `lifecycle.go`
  keeps; the two interoperate safely, since both tag errors with the same
  `containerd/errdefs` sentinels and no compose value crosses into the
  docker/docker-typed `dockerClient` interface.

  Forced along by minimal version selection: `compose-go/v2` v2.9.1 → v2.14.0
  (fred touches 26 of its types, all unchanged or additively changed, and every
  literal is field-named), `docker/cli` v28.5.1 → v29.6.2, `k8s.io/client-go`
  v0.32.3 → v0.36.0 (required by containerd v2.3.3 and buildx v0.36.0; the only
  consumer is the non-functional k3s scaffold, and its three call sites —
  `NewForConfig`, `BuildConfigFromFlags`, `Discovery().ServerVersion()` — are
  unchanged across those four minors), `prometheus/client_golang` v1.23.0 →
  v1.23.2, `docker/go-connections` v0.6.0 → v0.7.0, `go.etcd.io/bbolt` v1.4.3 →
  v1.5.0 (purely additive: two new `Options` fields and one new error; no
  on-disk format change, which matters because bbolt backs the placement,
  releases and retention stores), `gorilla/websocket` v1.5.3 → the commit
  pinned by client-go/containerd/buildx (it swaps `math/rand` for `crypto/rand`
  in frame masking and drops the deprecated `Temporary()` wrapper from the read
  path — fred branches only on `*websocket.CloseError`, so its chain-event
  read-deadline handling is unaffected), and `sirupsen/logrus` v1.9.3 → v1.9.4.
  The `spf13/viper` require line moves to v1.21.0 but the existing `replace`
  keeps the build on v1.17.0, so nothing changes there. `docker/docker` stays at
  v28.5.2+incompatible, so `lifecycle.go`'s use of its `client`, `errdefs` and
  `pkg/stdcopy` packages is untouched.

  Compose v5 also collapses its own import graph — where v2's `pkg/compose`
  reached fifteen buildx packages (including a blank import of
  `buildx/driver/kubernetes`) and eight BuildKit packages, v5 reaches one and
  three. The measured effect is a smaller build, not a larger one: `go.sum`
  halves (3052 → 1530 lines) and the **`docker-backend` binary drops 25%**
  (132.4 MB → 99.0 MB), because the buildx Kubernetes driver, BuildKit's
  session/auth/secrets/SSH providers and all 141 `k8s.io/client-go` packages
  leave it. `providerd` is unchanged in size (+0.3%). `client-go` remains a
  direct require, but its only remaining consumer is the non-functional k3s
  scaffold (ENG-133) — deleting that scaffold would now drop the entire
  `k8s.io` tree from `go.mod`, which was not possible while compose v2 pulled it
  in regardless.

  One behavioural delta is worth knowing about: v5 runs its per-container
  removals on the errgroup's *derived* context where v2 discarded it, so a
  single failed removal can now cancel its siblings part-way through — and a
  container that is never removed never has its anonymous volumes reaped, which
  is the ENG-372 leak. fred compensates via the per-container fallback in
  `deprovision.go`, and that fallback's coverage of **every** recorded container
  is now pinned by a test.

  What does **not** clear: GO-2026-4883 and GO-2026-4887 (`docker/docker`, fixed
  only in `moby/moby/v2`, whose 22 published tags are all betas) and
  GO-2026-5932 (`x/crypto/openpgp`, whose OSV entry has no `fixed` event at
  all). Three is the realistic floor for *this* change — though the govulncheck
  traces show every reachable symbol behind 4883/4887 sits in `lifecycle.go`,
  and the only non-fred importer of any `docker/docker` package in the new graph
  is `buildx/store` reaching `pkg/namesgenerator`. Porting `lifecycle.go` to
  `moby/moby/client` therefore looks likely to take the list to one entry; it is
  held to a separate change because that client's API is restructured
  (`filters.Args` is gone, the list-options types moved onto the client
  package), making it a rewrite of fred's Docker layer rather than an import
  swap.

- deps: bump `google.golang.org/grpc` to v1.82.1 (from v1.79.3) to resolve
  GO-2026-6061, a pair of flaws in the xDS RBAC authorization engine and the
  HTTP/2 server transport (fixed upstream in v1.82.1), and
  `go.opentelemetry.io/otel` to v1.44.0 (from v1.43.0) to resolve GO-2026-5158
  (CVE-2026-41178), an uncapped raw header length in `baggage` parsing (fixed
  upstream in v1.44.0). Neither was reachable in practice: fred links no xDS and
  instantiates no gRPC server (`internal/chain` is a gRPC client only), and fred
  imports no OpenTelemetry package directly — both were flagged through vendored
  server code that fred never constructs. Both advisories were published after
  the v0.12.0 cut and flagged versions already in the tree, so the CI vuln gate
  is the only thing that changed; this bump keeps it green. Also pulls
  `go.opentelemetry.io/otel/metric` and `.../trace` to v1.44.0 (they require the
  matching core), the `google.golang.org/genproto/googleapis/{api,rpc}`
  pseudo-versions required by grpc v1.82.1, and `gonum.org/v1/gonum` to v0.17.0
  in `go.sum` only (a test-only transitive of grpc, in none of the binaries).
  `go.opentelemetry.io/otel/sdk`, `.../sdk/metric`, and the otlp exporters are
  deliberately left at v1.43.0/v1.40.0: nothing in the called set flags them,
  and moving them cascades into the docker/compose v2 toolchain that the
  allowlist's Category B entries already defer.
- ci: drop `GO-2026-5746`, `GO-2026-5668`, and `GO-2026-5617` from the
  govulncheck allowlist. This is a **reachability correction, not a fix** —
  `Fixed in: N/A` still holds upstream for all three, and no dependency moved.
  All three scope to `github.com/docker/docker/daemon` symbols
  (`Daemon.containerExtractToDir`, `Daemon.createIfNotExists`,
  `Daemon.openContainerFS`), which run inside `dockerd` and are not in fred's
  import graph at any point: `go list -deps ./...` resolves 31 `docker/docker`
  packages, all client/API-side and none under `daemon/`, and `providerd` links
  no Docker package at all. fred is a Docker *API client* —
  `internal/backend/docker/lifecycle.go` calls `client.CopyFromContainer`, the
  HTTP client of `GET /containers/{id}/archive`, while the flagged symbol is the
  *server* side of `PUT`; there is no `CopyToContainer` call anywhere, and tar
  extraction uses stdlib `archive/tar` under `os.Root`. The original ENG-415
  triage (#143) conflated client and daemon; a later upstream symbol-narrowing
  made it visible by dropping them out of the called set. Gate behavior is
  unchanged — these three were never *called*, so they suppressed nothing, and
  the allowlist now matches the called set exactly (13 entries). Should fred
  ever link the Docker daemon, the gate will now flag it, which is precisely the
  architectural change that warrants review. (ENG-639)
- deps: bump `github.com/containerd/containerd/v2` to v2.1.9 (from v2.1.5) to
  resolve GO-2026-5758, GO-2026-5622, GO-2026-5475, GO-2026-5338 and
  GO-2026-5064, and `github.com/in-toto/in-toto-golang` to v0.11.0 (from v0.9.0)
  to resolve GO-2026-5547 (all fixed upstream in those releases). Unlike the
  docker/docker entries above, these six were genuinely in the called set —
  both modules are linked into `docker-backend` through the docker/compose v2
  toolchain. Both are indirect requires, bumped explicitly so MVS selects them
  over the versions docker/compose v2.40.3 and buildkit v0.25.1 pin. Also pulls
  `containerd/platforms` to v1.0.0-rc.2 (required by containerd v2.1.9), adds
  `in-toto/attestation` v1.1.2, and raises
  `secure-systems-lab/go-securesystemslib` to v0.10.0 and `spf13/cobra` to
  v1.10.2 (a patch bump of a direct require) — all required by in-toto v0.11.0.
  These were previously deferred as "compose-coupled", which was only true of
  the containerd **2.2** line: v2.1.9 pins `k8s.io/*` at exactly the v0.32.3
  fred already selects and `prometheus/client_golang` below fred's v1.23.0, so
  there is no cascade. `containerd/v2` v2.2.4 (GO-2026-5378) and
  `moby/buildkit` v0.28.1 (GO-2026-4859, GO-2026-4858) remain deferred: both
  land on the 2.2 line, which forces `k8s.io/client-go` v0.32.3 → v0.34.1 and
  `prometheus/client_golang` v1.23.0 → v1.23.2, both direct requires. Bumping
  buildkit would not have resolved GO-2026-5547 in any case — v0.28.1 pins
  in-toto at v0.10.0, one release short of the fix. The govulncheck allowlist
  drops from 13 entries to 7 and again matches the called set exactly.
  (ENG-639)

## [0.12.0] - 2026-07-24

### Added

- New metric `fred_provisioner_ack_batcher_lane_restarts_total{lane}` counts
  ack-batcher lanes respawned after a recovered panic (see Fixed). Pair with
  `fred_goroutine_panics_total{component="ack_batcher"}` to detect a
  crash-looping lane.
- New `credit_check_zero_grace_period` config knob (default `5m`) — how long a
  tenant's credit must stay empty before the scheduler auto-closes its leases
  (see Fixed). New metric `fred_withdraw_credit_check_zero_deferred_total` counts
  credit checks that read empty but deferred closure within that window
  (aggregate, no tenant label); a sustained or rising rate points at a
  chronically lagging chain node or too-short a grace period. (ENG-591)

### Changed

### Deprecated

### Removed

### Fixed

- Custom-domain routers now re-attach on their own after a container recreate
  (host reboot / reprovision) instead of leaving the tenant's custom domain
  stuck at 404. The ENG-266 DNS-readiness gate required the tenant domain to
  resolve to the backend's `host_address`, but `host_address` is the private
  br1 service-plane IP (`internal_ip`, e.g. `172.16.x`) while a tenant custom
  domain legitimately resolves to the host's **public** ingress IP — so the
  overlap check could never pass on the production topology and the gate
  deferred every custom domain forever. It only appeared to work because the
  `-custom` label was grandfathered onto long-lived containers; the first
  container recreate (a maintenance reboot re-runs provision, which drops the
  label) exposed it, and nothing re-applied the router — even though DNS was
  correct and the reconciler ran every tick (the re-check deferred silently at
  `DEBUG`). The gate now checks only that the domain **resolves at all** (not
  that it resolves to any particular host IP): the necessary-and-sufficient
  signal for not firing an HTTP-01 order into a negative cache, and the
  idiomatic multi-tenant-TLS split — authorization (the on-chain `custom_domain`
  claim) decides *whether* to issue, and the ACME challenge itself is the
  authoritative "does it point here" test. Post-boot the existing per-tick
  reconcile re-attaches the router within one interval, no fresh provision
  required. (ENG-618)
- Crash recovery (`recoverState`) no longer resurrects a lease that is mid
  volume-cleanup-retry (`Failed` with `VolumeCleanupAttempts > 0`) back to
  `Ready` from a stale container snapshot. Such a lease has already had its
  containers torn down, so any container a concurrent reconcile still lists for
  it was captured before the removal; merging that stale view after the
  deprovision set `Failed` could reset the retry counter and — once the next
  reconcile GC'd the resulting phantom-`Ready` no-container entry — abandon the
  volume-cleanup retry and leak the lease's pool reservation until process
  restart. The entry is now preserved by pointer across the reconcile map swap,
  matching the existing `Deprovisioning` preserve-case. Fixes the intermittent
  `TestDeprovision_VolumeRetry_ConcurrentRecoverState` CI flake. (ENG-603)
- The withdraw scheduler no longer closes a tenant's leases on a single
  zero-balance credit read. Lease closure is destructive (`MsgCloseLease` →
  volume soft-delete + 90-day grace), so a transient stale read — e.g. fred's
  chain node briefly lagging a tenant top-up — could wrongfully soft-delete a
  paying tenant's data (the consecutive-error dampening only guarded the RPC
  *error* path, not a successful-but-empty read). An empty balance must now
  persist for `credit_check_zero_grace_period` (default `5m`, the equivalent of
  Kubernetes' `tolerationSeconds`) before closure fires: the first empty read
  starts the window and schedules an early re-check, and any non-zero read
  clears it (hysteresis). (ENG-591)
- Reconciliation no longer tears down a lease that is still being provisioned as
  if it were an orphan. `ReconcileAll` snapshots chain leases before fetching
  backend provisions, so a lease created on-chain after that snapshot but
  event-provisioned before the provisions fetch appears in provisions yet not in
  the chain snapshot — and `processOrphan` deprovisioned it mid-provision. It now
  skips any lease the in-flight tracker still owns (incrementing
  `fred_reconciler_inflight_skips_total`), matching the guard the ack-skip branch
  and the placement pruner already applied to this same race. The window was
  narrow and self-healed next sweep. (ENG-594)
- The chain event WebSocket now detects a silently-dropped (half-open) peer via a
  read deadline instead of relying on the OS TCP timeout. The 30s ping ticker
  wrote pings but the pong handler never reset a read deadline (there was none),
  so after a NAT idle-drop / peer crash / partition (no FIN/RST) `ReadMessage`
  blocked for minutes — dropping chain lease events (created/closed/expired/
  auto_closed) and delaying reconnect and thus provisioning/deprovisioning. An
  initial read deadline of `2×ping_interval` is now armed and reset on every pong,
  so a run of missing pongs forces a read error and reconnect within
  ~`ping_interval`..`2×ping_interval`. (ENG-593)
- Ack-batcher lanes now respawn after a recovered panic instead of exiting
  permanently. Previously a single cosmos-SDK marshaling panic on a malformed
  chain RPC response killed the lane for good; at the default single-lane
  configuration that disabled **all** acknowledgment, and the timeout checker
  then wrongly rejected healthy, successfully-provisioned leases. Lanes are
  supervised and restarted; restarts are naturally paced by the batch interval.
  (ENG-589)
- The docker-backend now runs crash recovery and legacy migration under the
  backend lifecycle context instead of the caller's 30s startup context. The
  startup context was canceled the instant `Start` returned, which (1) capped
  the migration health-wait below 30s so a legacy workload that legitimately
  took 30–90s to become healthy failed startup, and (2) fired the `-prev`
  grace-cleanup goroutines' cancellation at ~0s, permanently leaking every
  migration's `-prev` containers. Fast connectivity/capability checks still run
  under the short startup context so an unreachable daemon fails fast. (ENG-592)

### Security

- The reserved container-label prefix guard (`traefik.`/`fred.`) now matches
  case-insensitively. Traefik's Docker provider treats label keys
  case-insensitively, so a tenant label keyed `Traefik.http.routers.evil.rule`
  slipped past the case-sensitive `strings.HasPrefix` check and registered a
  working router in the shared routing table — reopening the ENG-497 cross-tenant
  ingress-hijack the guard exists to prevent (deployed Traefik reads tenant
  container labels via the docker-socket-proxy). The reserved prefixes are now
  matched case-insensitively (a fold-compare of the key's prefix head), so
  mixed-case variants like `Traefik.` are rejected too. (ENG-595)
- manifest: reject a tenant-specified fixed host port (`host_port > 0`) at
  provision and update time; the host port is now always assigned dynamically.
  Previously `host_port` was only bounds-checked to `[0, 65535]` — with no
  reservation, privileged-port floor, or allowlist — and honored verbatim,
  binding on `0.0.0.0` in prod. In the permissionless tenant model that let an
  adversarial paying tenant squat any fixed port (including well-known / <1024)
  on the shared host: a second lease requesting the same port fails compose-up
  (`address already in use`), so a tenant could block co-located leases or
  collide with host services — an intra-host, perimeter-independent provisioning
  DoS. Tenants reach services through Traefik ingress (or discover the assigned
  port via `GET /v1/leases/{lease_uuid}/connection`), so forcing dynamic
  assignment removes the vector with no legitimate loss. The gate
  (`ValidateNoFixedHostPorts`) is admission-only — it never runs on
  recover/deprovision, so already-stored manifests stay parseable and their
  retained data restorable. This mirrors the Kubernetes Baseline Pod Security
  Standard, which forbids `hostPort` for tenant workloads. (ENG-605)
- `GET /logs` and diagnostics log capture now bound the **aggregate** bytes
  buffered across all of a lease's containers (32 MiB per call), not just the
  existing 5 MiB per-container cap. A lease may have up to 1024 containers, so
  without an aggregate budget one authenticated `/logs` request could
  materialize gigabytes and OOM the shared docker-backend host — cross-tenant
  denial of service. Output beyond the budget is truncated with a marker.
  (ENG-590)

## [0.11.0] - 2026-07-22

### Added

- Retention records gained an optional `partition` field (a cooperative
  sub-tenant grouping key within one on-chain tenant) plus the pure
  extraction/validation library for it. Nothing writes a non-empty partition
  yet — behavior and stored bytes are identical to the previous release.
- Retention partitioning: an operator-configured `retention_partition_source`
  plus a `retention_tenant_budgets` aggregator allowlist let one on-chain
  tenant's retention be capped and evicted per end-customer
  (`(tenant, partition)` scope), with per-tenant aggregate caps always
  binding (partitions only sub-divide, never raise). New knob
  `max_retained_disk_mb_per_tenant` (default 0 = unlimited). New metrics:
  `retention_partition_{collapsed_total,stamped_total,evicted_total}`,
  `retention_partitions`, `retention_refused_by_scope_total`,
  `retention_cap_check_failed_total`; existing `retention_refused_total` /
  `retention_evicted_total` keep their exact deployed meanings. Everything
  defaults off; with no config the release is behavior- and byte-identical.

### Changed

- docs: mark the `btrfs` and `zfs` volume backends as experimental and untested,
  and document `xfs` as the only backend validated and used in production. The
  docker backend implements all three `volumeManager` filesystems, but only XFS
  is deployed and exercised in prod (all mainnet/Morpheus backends run XFS with
  `pquota`, and per-volume disk and inode `ihard` quotas are enforced only on
  XFS), so the docker-backend README, `docker-backend.example.yaml`, and
  DEPLOYMENT.md now recommend `xfs` for production and flag btrfs/zfs as
  experimental rather than presenting the three as coequal choices. Docs-only; no
  behavior change. (ENG-564)

### Deprecated

### Removed

### Fixed

- Retention close path now runs the per-tenant count-cap eviction BEFORE the
  retained-disk refusal gate, so a full rolling window rolls instead of
  refusing (destroying) every subsequent close; eviction order is now a total
  order (CreatedAt, then lease UUID). In the rare close where both caps would
  trip, the tenant's oldest record is evicted first and the incoming close may
  then be retained (previously the incoming was destroyed and no eviction ran).

### Security

- manifest: bound the number of ports (and, defense-in-depth, expose entries,
  env vars, and labels) a tenant may declare per service. `flatManifest.Ports`
  was validated per entry but never counted, and every entry becomes a published
  host port plus an iptables DNAT rule (`userland-proxy:false` in prod), so a
  single cheap one-container lease could POST tens of thousands of port entries
  via `/update` (bounded only by the ~1 MB request body) and exhaust the shared
  host ephemeral-port range and netfilter — a host-wide, cross-tenant
  provisioning DoS. `Manifest.validate` now rejects manifests exceeding
  `MaxPorts` (64), `MaxExposePorts` (64), `MaxEnvVars` (256), or `MaxLabels`
  (128) — all far above any legitimate single-container workload — before any
  per-entry work, on both the provision and update paths (`ParsePayload`). The
  number of services in a stack remains bounded separately by
  `ValidateStackAgainstItems` (1:1 with paid lease items). (ENG-547)
- docker: preserve an in-flight lease's admission reservation across the periodic
  state rebuild. `recoverState` rebuilds the resource pool from live containers
  and then replaces it wholesale, but it excludes leases mid-operation
  (`Provisioning`/`Restarting`/`Updating`) from that rebuild — a still-pulling
  provision has no containers yet, and a restart/update is mid-cleanup. The
  full-replace `Reset` therefore dropped those leases' authoritative
  `TryAllocate` reservation for the whole window, so `TryAllocate` briefly saw
  phantom free capacity and could admit leases past physical CPU/memory/disk (and
  past a tenant's quota); once the slow lease re-registered, the pool was left
  over-committed (`allocatedDisk > totalDisk`), and because each volume carries a
  hard XFS quota the summed quotas could exceed physical disk and cause
  cross-tenant `ENOSPC`. Recovery now carries those in-flight reservations
  forward from the live pool — read back from the pool itself rather than
  reconstructed from lease state, so it is correct even before the lease's
  `Items` are populated — via a new `ResourcePool.ResetPreserving`, keyed
  identically so the synchronous reservation survives the rebuild. (ENG-546)
- docker: extend that in-flight reservation preservation to leases mid-close.
  `doDeprovision` removes a lease's containers *before* releasing its pool
  reservation — it defers the release until after the volume-destroy / retention
  work so the footprint is never momentarily uncounted while bytes still persist
  on disk — so a `Deprovisioning` lease can have no containers yet still hold its
  reservation. `recoverState` was dropping it on the periodic rebuild (the same
  phantom-capacity over-admission as above, reached via the close path). Recovery
  now preserves `Deprovisioning` reservations too, counted exactly once (dedup
  against any still-present container), matching the deprovision path's
  deliberate release-after-teardown ordering. (ENG-562)
- docker: extend that preservation to a lease whose close is mid-retry. When
  `doDeprovision` removes a lease's containers but a volume destroy/rename then
  fails (under the retry limit), it keeps the lease `Failed` and — because the
  bytes are still on disk — deliberately does not release its pool reservation.
  `recoverState` was dropping that still-held reservation on the periodic rebuild
  (`Failed` was excluded from preservation): the same over-admission, reached via
  the failed-cleanup path. Recovery now preserves a `Failed` lease's reservation
  only in that volume-cleanup-retry sub-state (`VolumeCleanupAttempts > 0`); a
  genuinely-failed provision, whose reservation was already released, is still
  dropped. (ENG-563)
- docker: `recoverState` now preserves the pool reservation of every tracked
  lease by a single structural rule (the pool is authoritative for a tracked
  lease's footprint), replacing the per-status allowlist grown across ENG-546/
  562/563. Besides subsuming those, it closes three more admission under-counts
  the allowlist missed — a `Ready`→crash→GC'd `Failed` lease, a restore-rollback
  re-quarantine failure, and a deprovision partial-removal failure — all of which
  hold a reservation for bytes still on disk that the allowlist dropped, letting
  `TryAllocate` over-admit past physical disk. Container-derived rebuild is
  retained only to seed the pool on cold start and re-establish a lost key from
  running containers. (ENG-567)
- docker: gate the disk **promote delta** when restoring a lease onto a
  larger-disk SKU tier. `Restore` adopts retained volumes via a pool allocation
  that intentionally skips the global disk-capacity check (the adopted bytes are
  already committed on disk and counted in the retained projection). On a
  *promote* (new SKU `disk_mb` greater than the retained tier's), the extra
  headroom was never checked against free capacity, so a tenant could soft-close
  a small-tier lease and restore it onto a much larger tier — repeatedly — to
  drive committed XFS quota past physical disk and cause cross-tenant `ENOSPC`.
  The adopt path now gates only the growth above the retained footprint
  (`max(0, new − old)` disk); same-tier and demote restores still skip the gate
  as before. (ENG-545)
- docker: XFS tenant volumes now carry a per-volume inode hard limit (`ihard`)
  alongside the block limit, bounding host inode exhaustion from zero-byte-file
  floods. The ceiling is `disk_mb × 1 MiB / min_avg_file_bytes` (new
  `min_avg_file_bytes` knob, default 1024 → 1024 inodes/MiB, floored at 262144
  inodes); a workload whose average file is smaller than `min_avg_file_bytes`
  may now hit `EDQUOT` on inodes. A filesystem-agnostic tar entry-count cap
  backstops crafted-image extraction. (ENG-548)
- deps: bump `golang.org/x/text` to v0.40.0 (from v0.37.0) to resolve
  GO-2026-5970, an infinite-loop-on-invalid-input DoS in `x/text` normalization
  (fixed upstream in v0.39.0). The advisory was published after the v0.10.0 cut
  and flagged the `x/text` version already in the tree, so the CI vuln gate is
  the only thing that changed; this bump keeps it green. Also pulls
  `golang.org/x/sync` to v0.22.0, required by `x/text` v0.40.0.

## [0.10.0] - 2026-07-16

### Added

### Changed

### Deprecated

### Removed

### Fixed

- manifest: a `NONE` health check with trailing arguments (e.g.
  `["NONE", "extra"]`) is now rejected instead of silently accepted. The Go
  validator previously ignored elements after `NONE` while the JSON schema
  (`docs/manifest-schema.json`) rejected them; both now agree that `NONE` takes
  no arguments. The canonical `["NONE"]` form is unaffected.

### Security

- docker: harden stateful-volume bind setup against a symlink escape. A tenant
  could plant a symlink inside its read-write stateful volume (e.g. `data -> /`)
  and, on a later deploy declaring a matching `VOLUME`, have
  `buildStatefulVolumeBinds` follow it — bind-mounting or `chown`-ing an arbitrary
  host path (host `/`, another tenant's volume) into the container with
  root-equivalent access. `sanitizeVolumePath` only validated the VOLUME *string*,
  so the raw `os.MkdirAll`/`os.Chown` traversed the on-disk symlink. Subdirectory
  creation is now confined to the volume root via `os.Root` (mirroring the ENG-430
  tar-extraction hardening), so a VOLUME path that traverses a symlink escaping the
  volume root is rejected instead of followed. The same guard is applied to the
  legacy→stack migration bind path (`migrate.go`). (ENG-539)
- docker: harden the writable-path (`_wp`) scaffolding against an image-seeded
  symlink escape (defense-in-depth follow-up to ENG-539). The `_wp` bind Source is
  mounted read-write into the container, and Docker's `CopyFromContainer` does not
  follow a final-component symlink — so a symlink at the Source would redirect the
  mount outside the volume. `DetectWritablePaths` only yields real directories, so
  this was not exploitable, but the extraction and bind paths now defend themselves
  regardless: `writablePathExtractDir` creates the extraction target via `os.Root`
  (an escaping symlink planted by an earlier path can no longer redirect it), and
  `setupWritablePathBinds` skips any bind whose Source is a symlink or escapes the
  `_wp` root. (ENG-543)

## [0.9.0] - 2026-07-14

### Added

- config: `withdraw_limit` (default `100`) sets how many active leases are settled
  per provider-wide withdrawal transaction (`MsgWithdraw.Limit`). Previously fred
  sent `Limit: 0`, so the chain applied its default of 50; the effective page size
  is now 100 (the chain's `MaxBatchLeaseSize` ceiling), halving the number of
  settlement txs per cycle as a provider's active-lease count grows. The withdrawal
  cycle still pages over the cursor, so this only trades tx count against per-tx gas
  — validated to `1..MaxBatchLeaseSize`. Operators near a full page should confirm
  `gas_limit` (the Simulate-down fallback) covers a 100-lease withdrawal. (ENG-529)
- test: Go native fuzz harnesses for the two tenant-facing custom parsers.
  `FuzzSanitizeAndExtractTar` drives the tar extractor with arbitrary archive
  bytes and byte budgets, asserting it never panics, keeps writes within
  `maxBytes` on disk, and never creates a real path outside the destination.
  `FuzzParsePayload` drives the manifest parser and asserts every payload it
  accepts satisfies the validation invariants (DNS-safe service names, no
  reserved `fred.*`/`traefik.*` label prefixes, bounded/absolute tmpfs mounts) —
  a validation-bypass hunter, not just a crash finder. fred previously had no
  fuzz coverage. (ENG-521)
- providerd: new `credit_check_interval` config knob that decouples the
  credit-check cadence from the paid provider-withdrawal cadence; when set
  shorter than `withdraw_interval`, the paid withdrawal is rate-limited to
  `withdraw_interval` while credit checks keep running frequently. Default
  (`0`) preserves the previous coupled behavior. Adds the
  `fred_withdraw_skipped_by_guard_total` counter and the
  `fred_withdraw_guard_active` gauge. (ENG-524)

### Changed

- scheduler: when `credit_check_interval` is set shorter than
  `withdraw_interval`, the provider-revenue withdrawal is rate-limited to its own
  interval instead of running on every credit-check wake. (ENG-524)

### Deprecated

### Removed

### Fixed

- docker-backend: reject a lease whose total (or per-item) container quantity is
  negative or exceeds a hard cap before reserving any state, closing a pre-admission
  memory-exhaustion DoS. `Provision` pre-sized the per-lease container-ID slice from
  the chain-supplied quantity (`make([]string, 0, totalQuantity)`) inside the lock —
  before SKU validation and the resource-pool admission gate — so an honest
  max-quantity lease (billing caps per-item quantity at 1e9) drove a ~16 GB
  allocation, and a negative value from an overflowed `uint64→int` cast panicked the
  `make`. Total and per-item quantity are now validated against `maxLeaseQuantity`
  (1024) at the top of `Provision` and rejected as a validation error before any
  allocation. (ENG-503)
- docker-backend: closed three retention/restore data-integrity gaps that could
  destroy or tear down live tenant data:
  - the startup orphan-volume reaper no longer destroys a still-open lease's
    volume when its containers were removed out-of-band (e.g. an operator
    prune) — it now skips any volume whose lease still has an active release,
    instead of reaping on a single boot observation. (ENG-505)
  - the orphan-record pruner no longer prunes a give-up-diverged retention
    record whose volume is still on disk under its canonical name; it now
    treats the canonical (not-yet-renamed) form as present, not just the
    `fred-retained-*` name, so a later boot can't reap the intact data.
    (ENG-501)
  - `reconcileRestoring` no longer tears down a running new lease in the
    Updating/Deprovisioning state when a completed restore's record lingered
    past a failed terminal delete — it defers for any live non-Failed
    provision, running the orphaned-restore rollback only for an absent or
    Failed provision. (ENG-512)
- docker-backend: a restore whose best-effort release-record write fails no longer
  strands the restored lease. `doRestore` deleted the retention record
  unconditionally after adopting the volumes, so if the release `Append` failed the
  lease was left with neither record — a later boot's orphan reaper (which keys on
  the release record) could then destroy the still-live tenant data. The retention
  record now acts as the adopted volume's finalizer: it is dropped only once the
  release is durably recorded, and otherwise left `restoring` so the boot reaper
  keeps protecting the volume and `reconcileRestoring` finalizes it once the lease
  reaches Ready. Safe now that `reconcileRestoring` never tears down a running
  lease (ENG-512). (ENG-523)
- scheduler: the withdraw scheduler no longer panics or wedges on a very large
  credit balance. `estimateDepletionTime` converted an unbounded balance /
  burn-rate ratio via `LegacyDec.TruncateInt64()` (panics on int64 overflow)
  and a `time.Duration` multiply (overflowed to a bogus past time); a tenant
  with a large balance and a tiny per-interval burn could trip it. Because the
  estimate ran inside a mutex held without `defer`, a recovered panic left the
  lock held and silently wedged revenue withdrawal and credit-exhaustion
  lease-closing. The horizon is now clamped to a finite bound, and the critical
  section releases its lock via `defer`. (ENG-500)
- provisioner: the reconciler no longer terminates a lease on a transient
  backend circuit-breaker trip. `ErrCircuitOpen` was bucketed with permanent
  errors, so a brief backend outage that opened the breaker permanently
  rejected pending leases and closed active ones on-chain — including
  recoverable leases that would have succeeded on the next tick, and letting
  one tenant's load-induced breaker trip terminate other tenants' leases in the
  same cycle. It is now treated as transient: logged and retried next cycle.
  (ENG-498)

### Security

- docker-backend: bound container log reads to 5 MiB per container. The Docker
  `Tail` option limits lines, not bytes, and a container's stdout/stderr is
  tenant-controlled, so `GET /v1/leases/{uuid}/logs` (and the diagnostics
  capture path) previously buffered an unbounded amount of tenant output in
  memory — a tenant emitting very large output could OOM the provider and take
  down co-located tenants. The demuxed output is now capped with a truncation
  marker. (ENG-499)
- docker-backend: reject tenant-supplied container labels under the reserved
  `traefik.*` prefix during manifest validation (previously only `fred.*` was
  blocked). With ingress enabled, Traefik's Docker provider merges router labels
  from every container into one shared routing table, so a tenant could register
  a `traefik.http.routers.*` rule targeting another tenant's deterministic
  subdomain and intercept its HTTPS traffic. (ENG-497)
- docker-backend: reject tar entries whose declared size would overflow the
  extraction byte budget. `sanitizeAndExtractTar` gated its per-extraction size
  cap with `totalBytes + hdr.Size > maxBytes`, where `hdr.Size` is an
  attacker-controlled `int64` from a tenant image's tar stream. Once an earlier
  entry advanced `totalBytes` above zero, a later entry declaring a
  near-`math.MaxInt64` size wrapped the sum negative and slipped past the gate,
  letting a single entry stream unbounded bytes to host disk and defeat the cap
  — a disk-exhaustion DoS affecting co-located tenants on a shared node. The
  gate now compares against the remaining budget and rejects negative sizes.
  (ENG-520)

## [0.8.0] - 2026-07-09

### Added

- docker-backend: end-to-end integration test proving `Restore()` re-applies the
  correct XFS disk quota (`bhard`) on a SKU promote/demote and refuses an
  over-cap demote — closes the restore→quota-reapply routing gap. (ENG-438)

### Changed

- Provider-wide withdrawal now pages through **all** of a provider's active
  leases within a single cycle, threading the chain's opaque `next_key`
  pagination cursor until it is empty (bounded by `max_withdraw_iterations`).
  Previously fred issued one default-limit withdrawal per tick and discarded
  `has_more`, so a provider with more active leases than the per-transaction
  limit had its tail leases left unsettled until lease close — deferred, uneven,
  and silent revenue collection. A new metric
  `fred_withdraw_incomplete_cycles_total` flags a cycle that hit the iteration
  bound with leases still pending. The provider-withdrawable pre-check query is
  likewise paged and summed across all active leases. Requires manifest-ledger
  v2.3.0 or later, which ships the paginated provider-wide withdrawal — this is a
  coordinated, consensus-breaking chain upgrade. (ENG-475)

### Deprecated

### Removed

### Fixed

### Security

- Release binaries are now built with Go 1.26.5, incorporating the standard-
  library fixes for the `crypto/tls` Encrypted Client Hello privacy leak
  (GO-2026-5856) and the `os` symlink-with-trailing-slash root escape
  (GO-2026-4970). (ENG-415)

## [0.7.0] - 2026-07-03

### Added

- Restore can now target a different SKU tier (promote/demote). A demote is
  refused (HTTP 422) when the retained volume's measured data does not fit the
  new tier's disk cap. New metric `restore_demote_refused_total{backend,reason}`.
  (ENG-438)
- docker-backend: added a regression guard for the retained-disk admission
  accounting invariant — the cached pool projection (`SetRetainedDisk`) must
  equal the value recomputed from the retention store (active + reaping) after
  every self-refreshing retention transition (close, reap, evict, sweep,
  reconcile-restoring, restore rollback, recover). A future transition that
  mutates the retention store but skips `refreshRetentionAccounting` — drifting
  the projection stale-low → over-admit → tenant ENOSPC — now fails a test.
  (ENG-456)

### Changed

### Deprecated

### Removed

### Fixed

- docker-backend (xfs): destroying a volume now clears its XFS project-quota
  limit (`limit -p bhard=0 bsoft=0`) instead of leaving it set, fixing a leak
  where every closed lease left a stale project-quota entry behind. Because every
  `xfs_quota` operation scans the whole project-quota table, the accumulating
  entries degraded provisioning latency cumulatively (and persisted across
  restarts with no self-healing). Introduced once ENG-449/454 made per-volume
  quotas actually apply. The clear is best-effort — a failure is logged and
  counted on the new `volume_quota_clear_failed_total` counter but never fails the
  teardown (a stranded lease is worse than a leaked, observable, zero-byte
  entry). Existing already-leaked entries on long-lived backends need a one-time
  operator cleanup (`limit -p bhard=0` per stale projid); the fix stops all
  further accumulation. (ENG-459)

- docker-backend and k3s-backend: the `GET /retentions` list is now keyset-
  paginated end to end, closing the second un-paginated O(N) cliff left by
  ENG-380. Previously the whole retained-lease list was returned in one response
  capped at 1 MiB (~19.4k records/backend); on overflow the reconciler silently
  marked retentions incomplete, which disabled placement-orphan pruning and
  degraded restore backend-affinity (ENG-333) with no operator-visible error.
  The client now walks pages (complete-or-error, each page bounded), and the
  docker retention store serves each page directly from its ordered bbolt index
  via a cursor seek — O(limit) per request, not a full-bucket scan per page. The
  `/provisions` and `/retentions` handlers now paginate symmetrically at the
  backend. Page size is configurable via `RetentionsPageLimit` (default 1000).
  (ENG-451)

- docker-backend: the daemon now **fails fast at startup** when it lacks
  `CAP_SYS_ADMIN` on a backend that requires it to set quotas (xfs, btrfs),
  instead of rejecting every stateful provision at runtime with an opaque
  `xfs_quota: cannot set limits: Operation not permitted`. The privileged
  quota-set (`quotactl`) needs `CAP_SYS_ADMIN`, but the read-only `report` probe
  in `Validate()` did not — so an under-privileged daemon silently failed to
  enforce per-volume `disk_mb` caps (invisible until the ENG-449 mountpoint fix
  made the code actually reach the privileged call). zfs is exempt (it supports
  `zfs allow` delegation); the noop backend (no stateful SKUs) is unaffected.
  Grant it via `AmbientCapabilities=CAP_SYS_ADMIN` on the docker-backend systemd
  unit (see DEPLOYMENT.md). (ENG-454)
- docker-backend: added a startup **quota backfill** — existing volumes (active
  leases and active retained volumes) have their project quota re-applied
  (re-tag + limit) via the new `EnsureQuota` primitive, so leases provisioned
  before the daemon held `CAP_SYS_ADMIN` get their `disk_mb` enforced without a
  re-provision or data move. Best-effort and idempotent; it never creates a
  volume (a concurrent deprovision cannot be resurrected). New metric
  `volume_quota_backfill_total{outcome}`. (ENG-454)
- docker-backend: `/health` now probes the persistence stores (callback,
  diagnostics, release, retention bbolt) in addition to pinging the Docker
  daemon. Previously a locked/corrupt/read-only store left the backend reporting
  healthy while soft-delete/restore silently failed against it. (ENG-448 / F31)

- docker-backend and k3s-backend: the inbound request-body cap is now
  configurable (`max_request_body_size` /
  `{DOCKER,K3S}_BACKEND_MAX_REQUEST_BODY_SIZE`) and defaults to 2 MiB — larger
  than providerd's 1 MiB cap. Previously both backends hardcoded a 1 MiB cap
  equal to providerd's, so a tenant manifest that just cleared providerd could
  be rejected with an opaque 400 at the backend once providerd re-serialized and
  wrapped it for forwarding. (ENG-448 / F42)

- docker-backend (xfs): per-volume XFS project quotas are now actually applied
  and measurable when `volume_data_path` is a subdirectory of the XFS mount (the
  normal deployment layout, e.g. `/data/fred/volumes` under a `/data/fred`
  mount). `xfs_quota` requires the mount point as its trailing filesystem
  argument, but the manager passed the `volume_data_path` subdirectory, so
  `project -s`/`limit -p` silently no-op'd (the `disk_mb` cap was never
  enforced) and `report -p` failed — `Usage()` returned "project id not found",
  which made the ENG-438 restore demote-fit gate refuse every demote as
  `unmeasurable_read_error`. The containing mount point is now resolved once at
  construction (`resolveMountpoint`) and used for all `xfs_quota` invocations;
  the volume subdirectory is named only in `project -s -p`. btrfs and zfs were
  unaffected. Note: volumes provisioned before this fix stay untagged until
  re-provisioned or re-tagged out of band. (ENG-449)
- docker-backend: the `releases.db` age reaper (`RemoveOlderThan`,
  `releases_max_age` default 90d) no longer deletes a live long-lived lease's
  only manifest-rehydration source. It now retains each lease's most-recent
  active release and its newest entry, pruning only older history, so a lease
  running stably for ≥90d survives a backend restart and stays Restartable
  (previously its record was reaped → `recoverState` left `StackManifest` nil →
  `Restart`/custom-domain reconcile hard-failed). `Append` now derives the next
  release version from `max(existing)+1` so within-key pruning cannot reuse a
  version. (ENG-440)
- **Close-time purge of a containerless lease's release history.** When a deprovision
  reaches `doDeprovision` for a lease with no live container / in-flight op (the
  idempotent no-provision short-circuit), it now still deletes the lease's `releases.db`
  history before returning, instead of short-circuiting before the terminal
  `releaseStore.Delete`. This stops a `lease_closed` event delivered after the container
  was already gone from stranding a stale `status=active` record (an `audit-lease-status`
  false positive) until the 90-day `RemoveOlderThan` TTL. Best-effort and chain-driven;
  the three release-history deletes in `doDeprovision` are consolidated behind one helper.
  Purely cosmetic (no pool/admission/routing impact). (ENG-410)

### Security

- fred-api: the request metric `path` label is now the matched route template
  (e.g. `/v1/leases/{lease_uuid}/status`) rather than the raw URL path, with an
  `unmatched` bucket for unrouted requests. Previously an unauthenticated 404
  path scan minted a new Prometheus series per path (cardinality DoS); the label
  is now bounded to the finite registered-route set. (ENG-448 / F28)

## [0.6.0] - 2026-06-30

### Added

- `fred_docker_backend_restore_total{outcome}` metric: restore re-deploy worker
  outcome counter (`success`/`failure`), incremented on both the success and the
  rollback (`rollbackRestoreAdoption`, panics included) paths so a
  docker-backend-side restore success rate is computable —
  `fred_docker_backend_restore_duration_seconds` is success-only. Both series are
  pre-initialized to `0` so the success-rate query reads `0`, not no-data, before
  the first restore. (ENG-408)
- **Per-tenant retention eviction metric.** New counter
  `fred_docker_backend_retention_evicted_total` increments each time a close-time
  per-tenant cap eviction evicts one of a tenant's own oldest retained leases
  (active→reaping) to honor `max_retained_leases_per_tenant`. Distinct from the global
  `fred_docker_backend_retention_refused_total` (`max_retained_disk_mb`); operators
  can now alert on per-tenant grace loss instead of grepping logs. (ENG-407)
- **Signed releases + SBOMs.** Release artifacts are now signed with keyless
  cosign (Sigstore Fulcio/Rekor — no long-lived keys) and ship a syft SPDX SBOM
  per archive. The signature covers `checksums.txt` (which lists every archive and
  SBOM digest) and the published container image is signed by digest. Verify
  published artifacts with `cosign verify-blob` / `cosign verify` (full commands
  in `.goreleaser.yaml`). (ENG-415)

### Changed

- **Per-transaction gas simulation.** Chain transactions now estimate gas by
  simulating each tx (`gas_adjustment` × simulated `GasUsed`) instead of a fixed
  per-operation value. `gas_limit` is now only the fallback ceiling used when
  `Simulate` is unavailable, and a new `max_gas_limit` (`0` = uncapped) rejects any
  tx whose simulated estimate exceeds it. New config knobs: `gas_adjustment` and
  `max_gas_limit`. (ENG-431)
- Build: Go toolchain bumped to **1.26.4**; building from source now requires
  Go ≥ 1.26.4. (#148)

### Deprecated

### Removed

### Fixed

- **Restore now persists a release record for the new lease.** A successful restore
  brought the adopted stack up but never wrote a `releases.db` record (it set the
  manifest in memory only and deleted the retained record). After a docker-backend
  restart, `recoverState` rehydrates a lease's manifest solely from
  `releaseStore.LatestActive`, so a restored lease came back with a nil manifest and
  `Restart` hard-failed with `ErrInvalidState` ("no stored manifest"). The restore
  success path now appends an `active` release (the retained `StackManifest`
  re-serialized via the same `json.Marshal` → `ParsePayload` round-trip used by
  restart/update), so restored leases survive a restart and stay restartable. The
  fix is best-effort like the provision path — a release-write failure cannot undo
  the already-succeeded restore. (ENG-433)

### Security

- **Tar extraction hardened against symlink-follow TOCTOU.** Tenant image-content
  extraction now uses `os.Root` so path resolution is structurally confined to the
  destination directory, eliminating the symlink-then-write traversal class instead
  of relying on lexical path checks. (ENG-430)
- **Supply-chain / CI hardening.** The `govulncheck` gate no longer blanket-
  suppresses findings: fixable CVEs (`x/crypto`, `x/net`, `spdystream`, and Go
  stdlib via the 1.26.4 bump) were cleared, with only documented unfixable IDs
  allowlisted. Added `gosec` static analysis and `gitleaks` secret scanning to CI,
  and signed releases with SBOMs (see Added). (ENG-415)

## [0.5.0] - 2026-06-26

This release ships **lease-close soft-delete + restore**: a closed or
credit-expired lease's stateful volumes can be held for a grace window and
restored into a fresh lease, instead of being destroyed on close.

> **Upgrade notes (v0.4.0 → v0.5.0)**
>
> - **Restore is opt-in.** The retention/restore data path is gated behind the
>   docker-backend `retain_on_close` key, which defaults to `false`. On a stock
>   deployment nothing is retained and every restore returns `404` — set
>   `retain_on_close: true` (and review `retention_max_age` / `max_retained_disk_mb`)
>   to activate the headline feature.
> - **Disk accounting.** When `retain_on_close` is enabled, retained (soft-deleted)
>   volumes now count against the disk admission pool until reaped — re-check
>   `total_disk_mb` headroom. See the OPERATIONS.md "Reclaiming retained volumes
>   under disk pressure" runbook. (ENG-360)
> - **Writable-path-only volumes are reclaimed at close**, not retained (ENG-406);
>   mixed leases keep their stateful volumes restorable.
> - **Provision placement is now least-loaded (CPU)** with round-robin fallback,
>   using a new optional backend `GET /stats` endpoint (ENG-318). Mixed-version
>   fleets degrade gracefully — a backend without `/stats` is simply excluded.
> - **No new deploy-ordering constraint vs v0.4.0** — the ENG-191 HMAC / ENG-103
>   mTLS wire changes already shipped in 0.4.0 and are not repeated here.

### Added

- **Lease-close soft-delete + restore (headline).** When the docker-backend
  `retain_on_close` key is enabled, a closed lease's managed volumes are
  soft-deleted (renamed into the `fred-retained-` namespace and recorded in a
  bbolt retention store) and held for a grace window instead of being destroyed.
  They can then be restored into a fresh `PENDING` lease via
  `POST /v1/leases/{new_lease_uuid}/restore` with `from_lease_uuid` naming the
  source lease. Restore is same-tenant only (the caller must own both leases),
  pinned to the backend that holds the source data, and must shape-match the
  original service names/quantities. New docker-backend config keys:
  `retain_on_close` (default `false`), `retention_db_path` (default
  `retention.db`), `retention_max_age` (grace window, default 90 days; `0`
  disables reaping), `retention_reap_interval` (reaper cadence, default `1h`),
  and `max_retained_leases_per_tenant` (per-tenant cap, default `0` = unlimited).
  (ENG-325, #114)
- **Queryable retention status.** `GET /v1/leases/{uuid}/status` now reports
  `provision_status: retained` for soft-deleted leases, with
  `retained_until` (RFC3339 grace deadline), `items` (the restore shape: service
  name / SKU / quantity), and a `restore_hint`. The status is served from the
  durable retention record even after the chain prunes the closed lease, under
  strict per-tenant authorization (a cross-tenant query returns `404`). The
  deprovision callback now carries a ground-truth `Retained` flag so `providerd`
  reports `retained` only when volumes were actually held. (ENG-329, #118)
- **Restore latency instrumentation.** New
  `fred_docker_backend_restore_duration_seconds` histogram and
  `fred_docker_backend_replace_phase_duration_seconds{operation,phase}` for the
  shared replace machinery (restart/update/restore). The provisioner's
  `fred_provisioner_provisioning_total` and
  `fred_provisioner_provisioning_duration_seconds` gain an `operation` label
  (`provision`|`restore`) so restore latency stays separable from fresh-provision
  latency. (ENG-357 #123, ENG-358 #124)
- `max_retained_disk_mb` docker-backend config key: per-provider cap on the
  retained-volume tier, with refuse-to-retain on breach (ENG-360, #131).
- Retained-volume metrics: `fred_docker_backend_retained_volume_bytes`,
  `fred_docker_backend_retained_leases`, `fred_docker_backend_retention_refused_total`,
  and `fred_docker_backend_disk_pool_bytes` / `..._retained_disk_cap_bytes`
  denominator gauges (ENG-360, #131).
- `fred_docker_backend_retention_index_reindex_total{trigger}` metric: retention
  in-memory index (re)build count, by trigger (`open`|`manual`) (ENG-385, #137).

### Changed

- **Backend selection is now least-loaded (lowest allocated-CPU ratio) instead
  of round-robin.** When several backends match an SKU, a provision goes to the
  one with the lowest allocated-CPU ratio, queried live from each backend's new
  `GET /stats` endpoint; backends without usable stats are excluded and routing
  falls back to round-robin. Restore requests bypass this and are pinned to the
  backend holding the source lease's retained data (restore backend affinity).
  (ENG-318 #117, ENG-333 #120)
- **Restored leases are acknowledged on-chain inline**, like fresh provisions,
  instead of waiting for the next reconciler pass — cutting restore-to-`ACTIVE`
  time. (ENG-358, #124)
- **Retention list paths use an in-memory tenant/status index** (rebuilt on
  open) for O(subset) lookups instead of full-store scans. (ENG-385, #137)
- **`GET /provisions` (backend) is now paginated** via keyset pagination,
  removing the ~8 MiB response cliff that aborted reconcile on providers with
  many leases; reconciler consumers page through the results. (ENG-380, #136)
- **Writable-path-only volumes are reclaimed (destroyed) at close** instead of
  retained. A volume created solely from writable-path auto-detection (no
  declared `VOLUME`) is unrestorable by construction — its contents are wiped and
  reseeded on every restore — so close now destroys it, leaving no retention
  record, per-tenant slot, `fred-retained-` directory, or retained-disk charge.
  Mixed leases (stateful + writable-path-only) keep their stateful volumes
  restorable. Counted by the new
  `fred_docker_backend_retention_writable_path_reclaimed_total` metric.
  (ENG-406, #138)
- **Behavior change:** Retained (soft-deleted) volumes now count against the
  disk admission pool until they are reaped, closing a disk over-commit / ENOSPC
  risk. **Operators:** if `retain_on_close` is enabled, re-check `total_disk_mb`
  headroom — effective live capacity now decreases by the retained footprint. See
  the OPERATIONS.md "Reclaiming retained volumes under disk pressure" runbook.
  (ENG-360, #131)

### Fixed

- **Retained-disk under-count on broken-store abandon.** A reaping-tombstone
  finalizer (the retention record now outlives the volume until its destruction
  is confirmed) plus make-before-break rollback close the disk-accounting
  under-count across the reap / evict / deprovision-give-up / rollback paths;
  give-up leaks now self-heal. New `fred_docker_backend_retention_leaked_total`
  counter and `fred_docker_backend_retention_reaping_bytes` /
  `fred_docker_backend_retention_reaping_leases` gauges for the reaping tier.
  (ENG-376, #135)
- **Orphaned retention records are pruned** when their volumes vanish
  out-of-band, via a periodic fail-closed reconcile that requires N consecutive
  confirming sweeps before pruning. New `retention_orphan_confirmations`
  docker-backend config key (sweep count, default `3`; `0` is a kill-switch) and
  `fred_docker_backend_retention_orphans_pruned_total` /
  `fred_docker_backend_retention_orphan_skips_total{reason}` metrics.
  (ENG-370, #133)
- **Anonymous Docker volumes are reaped on every teardown** (compose-down and
  container / image-introspection cleanup), stopping a leak that taxed
  compose-up under load. Safe by construction — fred data lives in bind mounts /
  managed directories. (ENG-372, #132)
- **Reconciler placement-prune grace window + deprovision fail-safe.** Replaces
  the SKU-route deprovision fallback (which could deprovision a phantom default
  backend and strand retained volumes) with positive single-backend resolution
  or an all-backends idempotent fallback, and adds a grace window to the
  placement pruner so leases that provisioned during a slow sweep are not pruned.
  (ENG-335, #121)
- **`TimeoutChecker` untracks non-pending leases** instead of retrying a reject
  forever. A lost re-provision callback for an already-`ACTIVE` lease used to
  wedge the lease in-flight (the chain rejects `RejectLeases` for non-`PENDING`
  leases), permanently inflating in-flight counts and skewing capacity/routing;
  terminal reject errors now untrack and hand back to the reconciler.
  (ENG-337, #122)

### Security

- **`callback_insecure_skip_verify` is now gated behind `production_mode`** on
  the docker and k3s backends: when `production_mode: true`, the insecure
  TLS-skip toggle is rejected at startup, closing the backend → Fred callback
  MITM exposure (mirrors the existing `providerd`-side gate). New
  `production_mode` config key (default `false`). (ENG-321, #113)

## [0.4.0] - 2026-06-10

### Security

- **TLS/mTLS for the providerd ↔ docker-backend transport.** The control-plane link between `providerd` and the Docker backend can now be secured with TLS, including mutual TLS with client-identity pinning. (ENG-103, #107)
- **HMAC signatures are now bound to the HTTP method and request URI.** The signing envelope changed from `<timestamp>.<body>` to a four-field canonical string `<timestamp>\n<METHOD>\n<canonical-URI>\n<hex(sha256(body))>`, closing a cross-endpoint replay class (e.g. a `POST /provision` body replayed against `POST /deprovision`, or `GET /info/<uuid>` replayed as `GET /logs/<uuid>`). This is a wire-format change with no dual-verify window: old signatures are hard-rejected by the new verifier (fail-closed with `401`). **Deploy order matters** — bring backends down, then Fred, deploy all binaries, bring Fred up first, then the backends. (ENG-191, #88)

### Added

- **TLS configuration knobs for the providerd ↔ docker-backend transport** to enable TLS/mTLS on the control-plane link (see ENG-103 above). (ENG-103, #107)
- **`callback_canonical_path_prefix` configuration field.** Restores the backend → Fred callback leg behind a path-stripping reverse proxy (e.g. Traefik `stripPrefix`) after the ENG-191 URI binding. The verifier prepends this static prefix to `r.URL.RequestURI()` before HMAC verification; the empty default preserves existing direct-call behavior bit-identically. (ENG-198, #91)
- **k3s-backend scaffold (experimental, non-functional).** A `cmd/k3s-backend` binary with full HTTP-contract parity with docker-backend, HMAC-signed inbound + outbound callbacks, a bbolt-persisted callback queue, and a `/health` probe that round-trips against the configured K3s cluster. The provisioner is a stub that returns `202` and posts a `status=failed, error="not implemented"` callback; substantive workload logic is deferred to child issues. Built from source only — **not included in release artifacts** (excluded from goreleaser) until real provisioning lands (ENG-134+). (ENG-133, #86)
- **`scripts/deploy-app.go`** — a focused script that takes `--image` + `--sku` and produces one lease plus an uploaded manifest, printing the lease UUID to stdout. (#92)
- **Apache 2.0 LICENSE** added to the repo root (`Copyright 2026 The Manifest Network`), replacing the README license placeholder. (ENG-176, #96)
- **Recover-time legacy → stack migration.** On Fred startup, the planner scans managed containers and migrates any legacy-named ones to the new service-aware naming convention. Migration is per-lease atomic (stop → rename containers to `-prev` → rename volume directories → `compose up` → verify startup → schedule background `-prev` cleanup → record migration in release store). Required: an active entry in the release store for the lease — see *Troubleshooting* below.
- **`volumeManager.RenameVolume(oldName, newName) error`** on all per-filesystem volume managers (xfs, btrfs, zfs, noop). The xfs implementation preserves project IDs across rename; the btrfs implementation preserves subvolume IDs (metadata-only rename, not copy+delete — verified by `TestIntegration_Docker_BtrfsRenameVolume_PreservesSubvolID`); the zfs implementation uses `zfs rename`.
- **`(*ReleaseStore).RecordMigration(leaseUUID, manifest) error`** for the migration pipeline to durably mark a legacy lease as having been migrated to its wrapped stack form. Idempotent on byte-equal payload.
- **`ProvisionState.StackManifest`** and **`ProvisionState.ServiceContainers`** fields persisted in the lease-state store. These replace the legacy `ProvisionState.Manifest` and the legacy semantic of `ProvisionState.ContainerIDs` indexed by `InstanceIndex` alone.
- **Configuration**: `MigrationReadyTimeout` (per-lease compose-up + verifyStartup bound) and `MigrationGracePeriod` (background `-prev` container retention window for rollback inspection). Reasonable defaults are set.
- **`fred_signer_balance` gauge** — per-signer balance in the configured fee denom, emitted as a `float64` (so large balances cannot be silently dropped — see ENG-252 under *Fixed*), sampled on each `/metrics` scrape via a custom Prometheus collector. Labels: `role` (`provider`|`sub_signer`), bech32 `address`, slice-position `index` (empty for the provider), and `denom` (the bank denom queried, sourced from `cfg.FeeDenom` — defaults to `umfx`). Sampling runs per-address bank queries in parallel under a 5s per-scrape timeout; per-address failures drop only the failing series and increment the new `fred_signer_balance_query_failures_total` counter (labeled by `role`, `address`, `denom` — no `index`). Single-signer mode naturally emits only the provider series, and `DemoteToSingleSigner` is reflected on the next scrape (the collector reads the live pool, no cached state). (ENG-239)
- **`docker-backend --version`** flag prints the build-injected version and exits, matching `providerd --version`. Operators can now query the docker-backend binary's version without a valid config file present (previously the version was only observable in the startup log, which requires a successful boot). (ENG-254)
- **`k3s-backend --version`** flag prints the build-injected version and exits, matching `providerd` and `docker-backend`. Same rationale and behavior as ENG-254 — the version is now queryable without a valid config file present (previously only observable in the startup log, which requires a successful boot). (ENG-255)

### Changed

- **Unified manifest handling on the Compose path.** All provisions, restarts, and updates now flow through Docker Compose end-to-end. The legacy single-service execution path that drove the Docker Engine API directly is removed. Tenants see no API-level behavioural change for new leases. (Plan: `docs/superpowers/plans/2026-05-15-unify-manifest-on-compose.md`)
- **Container naming is now service-aware.** Container names follow the pattern `fred-{lease}-{service}-{idx}` (previously `fred-{lease}-{idx}` for single-service leases). For a 1-service lease with a flat manifest, the service is named `app`, so legacy lease containers under the new code become `fred-{lease}-app-{idx}`. **Breaking change for monitoring tools or scripts that pattern-match container names** — update regexes from `fred-{lease}-\d+` to `fred-{lease}-[a-z][a-z0-9-]*-\d+`.
- **Volume directories follow the same service-aware naming**: `fred-{lease}-{service}-{idx}` under the configured `volume_data_path`. Pre-existing single-service volumes are renamed in-place at startup as part of the auto-migration described below.
- **Compose projects** group every container belonging to a lease under the project name `fred-{lease}`. Operators can inspect a lease's containers with `docker compose -p fred-{lease} ps` and tail logs with `docker compose -p fred-{lease} logs -f`.
- **Restart/Update `provision.Status` is now written exclusively by the lease actor** (ENG-230). The Docker backend's `Restart`/`Update` prelude no longer speculatively flips `Status`/`CallbackURL` outside the actor (and the speculative-write rollback machinery is gone); the actor's state-machine entry actions are the sole writers, firing before the request is acknowledged. **Externally observed behavior is unchanged** — including the HTTP contract: a same-lease concurrent restart/update that loses the race is still rejected with `409 Conflict` (now enforced by the actor rather than a prelude mutex). The internal `fred_docker_backend_lease_failing_race_skipped_total` metric, which only existed to detect the now-closed seam, is removed. (Plan: `docs/superpowers/plans/2026-05-26-eng-230-restart-update-actor-only-write.md`)
- **Custom-domain reconcile resolves only candidate domains** (performance). The reconcile no longer resolves every custom domain on each pass — it resolves only the candidates that could have changed. No behavior change. (ENG-277, #104)
- **Shared backend code lifted from `internal/backend/docker` → `internal/backend/shared`** (internal refactor). The reference-counted work barrier (`workbarrier`) and the tenant-facing manifest parser/validator (`shared/manifest`) moved out of the docker package so the upcoming K3s backend can reuse them. Pure mechanical lift, no behavior change. (#85)
- **Bootstrap provision writes go through a type-enforced actor-sole-writer boundary** (internal hardening). The two bootstrap provision-write paths (recover + provision) now flow through a `recoveredProvision` value type and a `materialize()` typed constructor as the only path into the provision registry. Behavior-preserving structural hardening; field-completeness is guarded by the directive-only `exhaustruct` linter. (ENG-193, #106)
- **`doDeprovision` writes routed through the lease-actor store seam** (internal hardening). Direct `ProvisionState` writes in `doDeprovision` now go through the store seam (`LeaseProvisionStore.Delete` added; initial-mark / partial-failure migrated to `UpdateFn`, success-delete to `store.Delete`). Behavior-preserving. (ENG-232, #105)
- **Deprovision volume-retry writes routed through the store seam** (internal hardening). The volume-retry block in `doDeprovision` — the last live-state writer mutating the provision directly — is split into a short direct span for the docker-private `VolumeCleanupAttempts` counter and `UpdateFn` for the `ProvisionState` writes. Pure structural hardening, no behavior change; covered by a concurrent recoverState/deprovision `-race` test. (ENG-285, #108)

### Deprecated

- **Flat single-service manifest format** (`{"image": "...", "ports": ...}` without a top-level `services` key). The format remains accepted at the wire boundary and is silently auto-wrapped into a 1-service stack manifest under the synthetic service name `app`. Tenants are encouraged to migrate to the explicit stack manifest format (`{"services": {"app": {"image": "...", ...}}}`). The flat format will be removed in a future major release. See [docs/manifest-guide.md](docs/manifest-guide.md) and the deprecation note at the top of [docs/manifest-schema.json](docs/manifest-schema.json).

### Removed

- **`backend.IsStack([]LeaseItem) (bool, error)`** and the associated `ProvisionState.IsStack()` method. Every provision is now stack-shaped by construction; the query had no meaningful counter-case.
- **`ProvisionState.Manifest`** (legacy single-service pointer) and **`ProvisionState.Image`** fields. The canonical workload representation is `Items` + `ServiceImages` (derived from `StackManifest`).
- **`ProvisionSuccessResult.Manifest`** — `StackManifest` is the only surviving manifest field.
- **Legacy single-service `doProvision` / `doRestart` / `doUpdate` / `doReplaceContainers` / `setupVolumeBinds`** function bodies (Task 14). The stack variants, now de-suffixed (`doProvision`, `doRestart`, `doUpdate`, `doReplaceContainers`, `setupVolBinds`), are the only execution paths.

### Fixed

- **Custom-domain HTTP-01 cert issuance is gated on DNS readiness.** Issuance for a custom domain no longer fires before the domain's DNS resolves, avoiding failed/repeated HTTP-01 challenges. (ENG-266, #101)
- **Custom-domain reconcile matches on a normalized `service_name`.** A single-image lease created without `-service-name` carries `service_name=""` on chain, but provision tags the lone item `app`, so an exact-`ServiceName` match (`"app"` vs `""`) silently dropped a `custom_domain` set after deploy (no `-custom` router, no HTTP-01 cert). Both sides are now normalized before matching so a lone unnamed item canonicalizes to `app` on chain and container alike; the rollback is keyed on the item's actual `ServiceName` so legacy `""`-named and multi-service leases are unchanged. (ENG-264, #100)
- **Custom-domain reconcile routed through the lease actor** (TOCTOU + data-race fix). The custom-domain apply + redeploy is serialized through the lease actor against `recoverState`'s struct-swap: the reconciler computes the diff read-only and routes a `ServiceName`-keyed override through the existing restart path; the actor commits `prov.Items` via the success-only `OnSuccess` hook. Deletes the off-actor mutate + CAS rollback. (ENG-278/ENG-231, #103)
- **k3s-backend: per-lease cancellable provisioner context.** Closes the post-unlock race between the stub provisioner's external writes (diagnostic + callback persist) and a concurrent `Deprovision`. Provision creates a per-lease `context.WithCancel`; `Deprovision` cancels it before deleting the map entry; the worker checks `ctx.Err()` at two checkpoints so a `Deprovision` that wins the lock observes cancellation before the next external write. Mirrors docker-backend's `OnExit` pattern. (ENG-189, #89)
- **`fred_signer_balance` gauge no longer drops large balances to an int64 overflow.** The gauge is now emitted via `float64` end-to-end, so balances above the int64 range are reported correctly instead of being silently dropped. (ENG-252, #97)

The change that removes the now-unused route-time `prevStatus` snapshot (replacing it with the lease actor's serially-observed pre-replace state) corrects three latent edges, all in rare races. **Two are `activeProvisions` metric-accuracy corrections (no lease-behavior change); one is a functional lease-behavior delta (the single restart-preflight edge below).** Normal restart/update flows are unaffected. (Plan §9, `docs/superpowers/plans/2026-05-26-eng-230-restart-update-actor-only-write.md`)

- **`activeProvisions` gauge accuracy — correction (a) (metric only):** in the container-death-then-restart-**success** ordering the gauge under-counted. A container death drove the lease `Ready→Failing` (decrementing the gauge); when the queued restart then succeeded, the re-increment was gated on the stale route-time snapshot and skipped, leaving the gauge one short until the next reconcile. It is now keyed on the actor-observed pre-replace state and stays correct.
- **`activeProvisions` gauge accuracy — correction (b) (metric only):** a replace that **recovers from a non-active source** (e.g. a restart-from-`Failed` whose replace fails but whose rollback restores the lease to `Ready`) is now counted. That path previously performed no gauge increment, under-counting the now-`Ready` lease.
- **Restart that fails preflight after a container death no longer reports the lease as `Ready` with dead containers (functional delta — the single lease-behavior change):** a **pre-existing** edge (architect determination): if a restart hit a preflight failure (e.g. an SKU profile removed from config between provision and restart) while the lease had just died and moved to `Failing`, the recovered-vs-failed decision keyed on a stale route-time snapshot and wrongly marked the lease "recovered to `Ready`" (also over-counting the gauge) for up to one reconcile interval. It now keys on the actor-observed source state, so such a lease correctly ends `Failed`. The on-chain callback was `failed` in both the old and new paths, so there was **never a wrong on-chain success** — only a transient wrong API status. This is **not a regression** and is bundled here with the `prevStatus` removal, not buried as an internal refactor.
- **docker-backend**: `DefaultConfig()` no longer pre-populates `SKUProfiles` with the four default tiers. yaml.v3 merges map keys during Unmarshal, so partial operator `sku_profiles:` blocks were silently inheriting defaults — and the bidirectional `sku_mapping`/`sku_profiles` reachability check then rejected the merged config, crash-looping docker-backend on deployments with fewer than four SKUs. `sku_profiles` is now required in YAML (with a clear `Validate` error when missing); see `internal/backend/docker/README.md` for recommended starter profiles. (ENG-238)

### Migration

#### What happens on upgrade

When a Fred instance running the new code starts up against a host that has leases provisioned by the legacy single-service code:

1. Fred's recoverState planner inspects all managed containers and groups legacy-named ones by lease.
2. For each legacy lease, Fred consults the release store. **If the release store has no active entry for the lease, Fred refuses to migrate it and fails startup with the lease UUID in the error message.** This is intentional — the planner refuses to reconstruct a manifest from container inspect alone (it cannot recover tmpfs paths, the resolved User UID, `depends_on` graphs, or `stop_grace_period`).
3. For each migratable lease, Fred runs the atomic pipeline: stop containers → rename to `<name>-prev` → rename volume directories → `compose up` with the wrapped stack manifest → verify startup → schedule background removal of `-prev` containers after `MigrationGracePeriod` → record the wrapped manifest in the release store.
4. Tenants see no API-level change. The lease's leases-state record gains a populated `StackManifest` + `ServiceContainers`.

#### Troubleshooting

##### "lease X has no active release store entry — migration refused"

Fred startup will abort with an error listing the lease UUID. Operator remediation:

- Inspect the release store for the lease (`./fredctl release list <lease-uuid>`).
- If the lease was created **before** the release-store feature was introduced, no automatic migration is possible. Two options:
  1. **Deprovision the lease** (`./fredctl lease deprovision <lease-uuid>`) and have the tenant re-provision. The new provision will be stack-shape from the start.
  2. **Manually add an active release-store entry** for the lease using the current manifest (recover from your provisioning records or the chain's on-chain manifest hash + payload archive).
- Re-start Fred. If the entry exists, migration proceeds.

##### Stuck `-prev` containers and the narrow non-resumable migration window

The migration pipeline has four boundaries; three are crash-resumable, one is not:

- **Boundary 1** (before `Stop + rename-to-prev`): crash-resumable. Next boot re-plans from scratch.
- **Boundary 2** (after `rename-to-prev`, before `compose up`): **NOT resumable.** Containers are stopped and renamed to `<name>-prev`; volumes may be partially renamed. The next boot's planner cannot find the legacy container under its original name (it's `-prev`) and refuses to replan.
  - **Operator remediation:** either (a) manually rename `<name>-prev` containers back to their original names and restart Fred (which will retry the migration cleanly), or (b) deprovision the lease and have the tenant re-provision.
- **Boundary 3** (after `compose up`, before `RecordMigration`): crash-resumable. New stack containers exist alongside `-prev` containers; the release store still has the legacy active entry; the volume rename is already idempotent. Next boot re-plans and reconverges; the operator may see two generations of containers transiently.
- **Boundary 4** (after `RecordMigration`, before background `-prev` removal completes): terminal state. Forward progress is durable. If Fred restarts inside the `MigrationGracePeriod` window, the `-prev` containers linger on disk until manual cleanup (`docker rm <container>`).

##### Orphaned `-prev` containers from pre-upgrade legacy restart attempts

If a pre-upgrade Fred (the legacy single-service code) was interrupted mid-Restart (between `RenameContainer` to `-prev` and the subsequent `RemoveContainer`), the host may carry orphaned `<lease-uuid>-prev` containers from that earlier interrupted Restart attempt. These are unrelated to the recover-time migration `-prev` containers and survive the upgrade. They are operator-cleanup territory:

```
docker ps -a --filter "name=-prev$"
docker rm <container-id>
```

The new code's lease lifecycle does not produce these orphans.

### Configuration

No required configuration changes. New optional knobs:

- `migration_ready_timeout`: per-lease compose-up + verifyStartup bound during the auto-migration. Default: 90 seconds.
- `migration_grace_period`: how long `-prev` legacy containers are retained for operator rollback inspection before background cleanup. Default: 1 minute.

### Acknowledgements

This migration was planned and executed across 16 tasks; the plan and per-task design discussions are in `docs/superpowers/plans/2026-05-15-unify-manifest-on-compose.md`.
