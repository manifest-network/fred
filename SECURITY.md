# Security

This document describes Fred's security architecture, authentication flows, and hardening measures.

## Authentication

Fred uses three distinct authentication mechanisms depending on the caller:

### Tenant Authentication (ADR-036)

Tenants authenticate to Fred's API using signed bearer tokens. Each token is a base64-encoded JSON object containing the tenant address, lease UUID, timestamp, public key, and an ADR-036 signature.

**Token format** (base64-encoded JSON):
```json
{
  "tenant": "manifest1...",
  "lease_uuid": "...",
  "timestamp": 1234567890,
  "pub_key": "<base64-encoded-pubkey>",
  "signature": "<base64-encoded-signature>"
}
```

**Signed message format:** `{tenant}:{lease_uuid}:{unix_timestamp}`

**Payload upload variant:** The `POST /data` endpoint uses a separate token that includes a `meta_hash` field binding the token to a specific payload. Signed message format: `manifest lease data {lease_uuid} {meta_hash_hex} {unix_timestamp}`

**Validation steps:**
1. Decode base64 token, parse JSON fields
2. Validate required fields (lease_uuid, tenant, pub_key, signature)
3. Check timestamp is within window (max 30s old, max 10s in future)
4. Verify ADR-036 secp256k1 signature over the signed message
5. Derive bech32 address from public key, confirm it matches the `tenant` field
6. Normalize signature to low-S canonical form (prevents malleability)
7. Query chain to verify the lease exists, belongs to this tenant, and is served by this provider

**Implementation:** `internal/api/auth.go`, `internal/adr036/adr036.go`

### Callback Authentication (HMAC-SHA256)

Backends authenticate callbacks to Fred using HMAC-SHA256 with a four-field canonical string that binds the timestamp, HTTP method, request URI, and body hash. Binding the method and URI prevents cross-endpoint replay; hashing the body keeps the canonical string binary-safe.

**Header format:** `X-Fred-Signature: t=<unix-timestamp>,sha256=<hex-encoded-hmac>`

**Signed canonical string:** `<timestamp>\n<METHOD>\n<canonical-URI>\n<hex(sha256(body))>` — binding all four fields prevents timestamp substitution AND cross-endpoint replay (a captured `POST /callbacks/provision` signature cannot be replayed against any other endpoint or method).

**Path-stripping reverse proxies.** Because the canonical string includes the request URI, deployments where a reverse proxy rewrites the inbound path (e.g., Traefik `stripPrefix` middleware mapping `/api/fred/*` → fred's bare `/*`) would otherwise see a verifier/signer URI mismatch: backends sign the full external URL while fred receives the stripped URL post-proxy. The static `callback_canonical_path_prefix` config field tells fred's verifier what prefix to prepend to `r.URL.RequestURI()` before computing the canonical string, so signer and verifier agree. The value is **static config**, not derived from request headers (e.g., `X-Forwarded-Prefix`) — this rules out spoofing as a failure mode and leaves "config is correct" as the only invariant to maintain. The callback-base path, canonical prefix, and proxy strip rule must be sourced from the same deployment variable; Fred additionally rejects startup unless the first two normalize to identical escaped bytes. Drift breaks every callback. The TLS-posture context that makes the underlying ENG-191 URI binding load-bearing is documented in the `### Security` (TLS) section of the companion `manifest-deploy` operations repository's `CLAUDE.md` (a separate repo, not part of this one). See [docs/security-callback-auth.md](docs/security-callback-auth.md) for the full threat-model rationale.

**Backend trust boundary.** Production assigns each configured backend a
distinct `backends[].hmac_secret`. The same key is configured as that backend
process's `callback_secret`, authenticating both directions of only that
provider/backend channel. Fred binds the configured name to the backend's
immutable storage UUID during placement startup; callback bodies carry that
UUID inside the HMAC-covered payload, and Fred uses it to select the exact
verification key. The UUID is an untrusted selector until verification
succeeds, then callback application checks it again against durable operation
or lifecycle authority.

A compromised backend therefore cannot sign a callback for another backend's
storage UUID or authenticate a provider command sent to another backend. A
single bidirectional key does let a compromised backend forge the provider side
of its *own* channel, but that adds no storage authority: the process already
controls that substrate. Direction-separated keys would matter only if those
two principals within one backend trust boundary must be isolated. The
top-level fleet-wide `callback_secret` remains a non-production compatibility
mode and is rejected when `production_mode: true`.

| Parameter | Value |
|-----------|-------|
| Algorithm | HMAC-SHA256 |
| Canonical fields | `timestamp`, HTTP method, `r.URL.RequestURI()`, `sha256(body)` |
| Field separator | `\n` (0x0A) |
| Minimum secret length | 32 bytes |
| Maximum callback age | 5 minutes |
| Clock skew tolerance | 1 minute into the future |
| Comparison | `hmac.Equal` (constant-time) |

**Signing example (Go):**
```go
// Build the request first so the signed canonical string includes the
// same method + URI the verifier will see on the wire.
req, _ := http.NewRequest(http.MethodPost, callbackURL, bytes.NewReader(body))

timestamp := time.Now().Unix()
bodyHash := sha256.Sum256(body) // sha256.Sum256(nil) is stable
signed := fmt.Sprintf("%d\n%s\n%s\n%x",
    timestamp, req.Method, req.URL.RequestURI(), bodyHash[:])

mac := hmac.New(sha256.New, []byte(secret))
mac.Write([]byte(signed))
sig := hex.EncodeToString(mac.Sum(nil))

req.Header.Set("X-Fred-Signature", fmt.Sprintf("t=%d,sha256=%s", timestamp, sig))
```

Go backends in this repository can import `internal/hmacauth` and call `hmacauth.SignRequest(secret, req, body)` instead of computing the canonical string by hand.

**Implementation:** `internal/api/callback_auth.go`, `internal/hmacauth/`

### Backend Authentication (HMAC-SHA256)

Fred authenticates requests to backends using the same HMAC-SHA256 scheme. The docker-backend verifies these signatures via auth middleware on all contract endpoints: `POST /provision`, `POST /deprovision`, `POST /restart`, `POST /update`, `POST /restore`, `POST /reconcile_custom_domain`, `GET /info/{lease_uuid}`, `GET /logs/{lease_uuid}`, `GET /provisions`, `GET /provisions/{lease_uuid}`, `GET /retentions`, `GET /releases/{lease_uuid}`. The monitoring endpoints `GET /health`, `GET /stats`, and `GET /metrics` are unauthenticated.

**Implementation:** `cmd/docker-backend/main.go` (auth middleware), `internal/hmacauth/`

## Replay Protection

### Token Replay (Tenant API)

Used tokens are tracked in a persistent bbolt database keyed by the normalized signature. This prevents an attacker from intercepting and replaying a valid token.

| Property | Detail |
|----------|--------|
| Storage | bbolt (persistent across restarts) |
| Key | Base64-encoded normalized signature |
| TTL | 30 seconds (matches token max age) |
| Cleanup | Background goroutine removes expired entries |
| Failure mode | **Fail-closed** — DB errors return 503, not pass-through |
| Concurrency | bbolt transaction serialization prevents race conditions |

**Signature malleability:** ECDSA signatures have two valid forms (high-S and low-S). Before storing and checking, all signatures are normalized to low-S canonical form. This prevents an attacker from flipping the S value to bypass deduplication.

**Which endpoints check replay:**

| Endpoint | Replay | Rationale |
|----------|--------|-----------|
| `GET /connection` | Yes | Returns sensitive connection details |
| `POST /restart` | Yes | Mutating — replaying would restart containers |
| `POST /update` | Yes | Mutating — replaying would redeploy |
| `POST /restore` | Yes | Mutating — replaying would re-trigger a restore |
| `GET /status` | No | Idempotent read |
| `GET /provision` | No | Idempotent read |
| `GET /logs` | No | Idempotent read |
| `GET /releases` | No | Idempotent read |
| `GET /events` | No | Read-only WebSocket stream |
| `POST /data` | No | Has own idempotency guard (409 on duplicate upload) |

**Configuration:** Requires `token_tracker_db_path`. Mandatory when `production_mode: true`. When not configured (non-production), replay protection is disabled entirely — tokens can be replayed within their 30-second validity window. This is acceptable for development but **must not be used in production**.

**Implementation:** `internal/api/token_tracker.go`

### Callback Replay (Backend -> Fred)

Callback timestamps older than 5 minutes are rejected, so an identical signed
request can be replayed only within the configured freshness window. The
protocol intentionally has no callback nonce cache because durable backend
delivery retries the exact request after ambiguous network outcomes. Method and
complete-URI binding prevent moving a captured signature to another endpoint,
resource, or operation; body and timestamp binding prevent payload or timestamp
substitution. Typed operation/lifecycle settlement and idempotent application
remain the defense against duplicate delivery within the freshness window.

## Input Validation

### UUID Validation

All lease UUIDs are validated with `google/uuid.Parse` before use. Invalid UUIDs are rejected with 400.

### URL Validation

All configured URLs (`callback_base_url`, backend URLs) are validated at startup:
- Must be absolute `http://` or `https://` URLs with a non-empty, non-dot ASCII
  hostname (internationalized names use their punycode form); explicit ports
  must be in the range 1–65535
- Backend URLs must be origins: no user info, path, query, or fragment. Raw
  empty `?` and `#` markers are rejected instead of being silently discarded
- `callback_base_url` rejects user info, fragments, opaque URLs, malformed
  query syntax, preexisting `operation_id` or `lifecycle_id` selectors,
  non-canonical dot/empty path segments, backslashes, and percent-encoded path
  separators whose interpretation can differ across proxies
- Trailing path slashes are stripped from `callback_base_url` without
  re-encoding its other query values. Raw query bytes unsafe in an HTTP request
  target (including spaces and non-ASCII bytes) must be percent-encoded
- `callback_canonical_path_prefix` must equal the normalized escaped base path
  byte-for-byte, so a signer/verifier path mismatch fails at startup

Complete callback destinations are revalidated before backend acceptance,
durable persistence, replay, and placement decode. Their path must end in
`/callbacks/provision`, and authority, path, fragment, and raw-query ambiguity
fails closed rather than being deferred to `net/http` or a reverse proxy.

In production mode, additional SSRF checks block:
- Loopback addresses (`127.0.0.0/8`, `::1`, `::ffff:127.0.0.1`)
- Link-local addresses (`169.254.0.0/16`, `fe80::/10`)
- Unspecified addresses (`0.0.0.0`, `::`)
- `localhost` hostname (case-insensitive, including FQDN `localhost.`)

Private IPs (RFC 1918) are intentionally allowed since backends commonly run on private networks. DNS resolution is not performed — hostnames resolving to blocked addresses are not caught. Use network-level controls for defense in depth.

**Durable callback outbox trust boundary.** A bundled backend's callback
database and its configuration are trusted local state owned by the same
backend OS identity. Current outbox rows are validated fail-closed before any
egress, but validation does not make a host-controlled DNS name trustworthy.
Restrict backend callback egress at the network layer to the intended Fred
endpoint(s), and protect both the backend configuration and callback database
with the same filesystem/host controls. Every authoritative backend journal
(callback operation/maintenance/close intents and outbox, release, and retention) must be a single-link
regular file opened without following the final path component and with exact
mode `0600`; initialization and ordinary startup reject group- or world-readable
restores and later permission drift. The diagnostics database grants no
lifecycle authority and is not continuously re-attested, but its open/create
path still rejects symlinks, hard links, non-regular files, and modes other than
exact `0600`.

**Provider-local database trust boundary.** The placement and optional payload
databases contain provider, tenant, lifecycle, routing, and manifest data. Each
must be an unsymlinked, single-link regular file with exact mode `0600`.
`providerd` creates a new payload database with those properties and rejects an
insecure existing payload or placement database before use. While running, both
stores re-attest the retained inode, link count, and mode at every database
transaction boundary. Pathname, permission, or hard-link drift—and physical
parent drift for the descriptor-relative payload store—is process-sticky and
fails later access closed.

The token replay database is a separate short-lived cache. bbolt creates a
missing file with mode `0600`, but Fred does not treat an existing token cache as
provider/lineage authority and does not continuously bind its pathname, inode,
mode, or link count. Configure its parent as trusted local state and replace or
restore it only while `providerd` is stopped.

### Backend storage-lineage boundary

Fred durably pins every configured backend name to one canonical UUIDv4 storage
identity. Identity-bearing inventory responses must agree on that UUID across
all pages and both `/provisions` and `/retentions`. Once pinned, reads include
the HMAC-covered `backend_storage_id` query, and all side effects use only
`/_fred/storage/{uuid}/...`; upgraded backends validate that path before body
decode or mutation. The client never follows redirects. A trusted proxy must
not internally rewrite that upgraded-only namespace onto a legacy mutating
route, because an internal rewrite is not an HTTP redirect Fred can reject.

`X-Fred-Backend-Storage-ID` is not cryptographic proof by itself. Request HMAC
authenticates Fred to a backend; it does not sign the backend's response.
Production deployments rely on peer-verified HTTPS using the configured
private CA or system roots, optionally mTLS, to authenticate response headers,
inventory, and refusal verdicts. Storage identity detects accidental
same-name replacement under that authenticated transport boundary; it is not a
defense against an intermediary that can forge the backend's authenticated
response. A header on a cheap pre-dispatch error identifies only the process's
sealed ID; it is not positive inventory/effect evidence. A backend that confirms
runtime lineage drift deletes the header and returns `503`.

Storage-lineage proof does not accept a canonical-looking directory name as
substrate authority. Every managed name is parsed as one typed path component
and then proved as an actual Btrfs subvolume, a real directory on the configured
XFS root with a regular no-follow nonzero project-ID marker, or the exact
depth-one ZFS child with its exact managed mountpoint. ZFS proof inventory unions
the directory view with the child-dataset view, so an unmounted or externally
mounted child cannot disappear as apparent absence. Container bind evidence
must independently match its label-derived volume and exact target-derived
subtree, with no symlink in any component. The XFS marker is durable project-ID
authority; startup separately re-applies the kernel project tag and limits and
refuses readiness if that enforcement cannot be proved.

XFS project IDs and dquots are scoped to the whole containing filesystem. Fred's
allocator scans only its own managed root and selects from the full nonzero
32-bit namespace, so a different Fred root or foreign project-quota manager on
the same mount could collide and make a later limit or clear affect unrelated
inodes. The deployment trust boundary therefore requires Fred to be the sole
project-ID allocator on that mount. A dedicated XFS filesystem provides that
isolation. Sharing is unsupported until a coordinated disjoint-range mechanism
is added; separate subdirectories do not provide separate project namespaces.

The Docker backend also re-attests lineage after every raw Compose, Docker,
volume-manager, and tenant-path mutation. A postcheck failure makes both a
successful return and a transport error causally ambiguous: the backend latches
that state for its process lifetime, cancels further substrate work, suppresses
callback settlement, and preserves the operation/maintenance intent or retention finalizer
for strict restart recovery. This conservative check detects a replacement
during a daemon/filesystem call; it cannot make an external daemon or CLI
mutation atomic with the marker proof. Local recursive deletion and directory
rename are descriptor-rooted after exact managed-volume-name validation, and
XFS project-marker access uses attested, no-follow descriptor lookups. External
Docker, ZFS, btrfs, and xfs_quota operations still require a substrate-native
generation/CAS primitive to close the residual same-root replacement window.

XFS also withholds the bind-ready final name until a typed hidden stage is
parent-synced, its project marker is durable, and both project tag and limits
have succeeded; publication is a descriptor-rooted no-replace rename and
requires the marker's parsed ID to match the stage name. For cleanup, the synced
typed name remains authoritative when a crash recovers the sole no-follow
regular marker as empty, partial, or zero-filled, up to the ten bytes the writer
could emit. A stage found after restart is cleanup authority only because its
original quota is not durable, so normal sealed startup clears its dquot and
removes only that bounded empty-or-single-marker shape, never publishes it.
Runtime failures after stage durability attempt exact compensation; the
external quota clear uses a detached cleanup context capped at 30 seconds and by
any earlier aggregate parent deadline. Any failed or ambiguous cleanup retains
the stage, latches and stops the current backend instance, and requires a fresh
`Start` to recover the authority before serving.

Destruction has distinct authority:
`.fred-xfs-delete-<project-id>-<managed-volume>` is created empty, normalized to
project ID zero, and parent-synced before the final volume is changed. Recovery
re-normalizes and re-attests that sibling, deletes only the encoded final tree,
syncs its absence, and requires numeric block and inode usage for the encoded
project ID to be zero before clearing its dquot. This detects open-but-unlinked
files that pathname absence cannot. The sibling is removed and the parent
synced only after the strict clear; until then it prevents readiness and
same-name creation. Offline proof refuses the private authority rather than
turning it into mutation permission.

ZFS retains an exact
unmounted child after an ambiguous create and normal sealed startup remounts and
re-attests it rather than destroying it. The stopped preflight and initializer
reject either form without mutation. Failure or ambiguity preserves the evidence
and prevents readiness.

Bundled backends retain old HMAC-authenticated, identity-unbound mutation paths
as a bounded v0.13 compatibility surface, but the supported upgrade never sends
traffic through them. Drain pending work, stop the old provider and all
backends, fence the old host, rotate to unique per-backend credentials, seal
storage identity, and only then start the upgraded fleet. A stale provider that
still has a valid current HMAC and mTLS credential could invoke an old path, so
revoke the old credentials and do not expose those paths through a proxy. The
identity-bound namespace is the only mutation path selected by the upgraded
provider.

The reverse hop carries the same identity in the HMAC-covered callback JSON.
New outbox rows capture it immutably when the completion is enqueued, and replay
must preserve that captured value. Fred compares it with the durable pin before
settling chain or placement authority. Copying an outbox to replacement storage
therefore cannot make old completion evidence belong to the replacement.
Marker pairs, backend control databases, and their substrate are trusted
durable state: protect and back them up together. A full snapshot clone retains
the same identity by design, so operators must fence the old clone before the
restored one starts.

### Request Body Limits

All requests are wrapped with `http.MaxBytesReader` enforcing a configurable maximum via `max_request_body_size`. The tenant-facing API (providerd) defaults to 1 MiB; the docker and k3s backends default to 2 MiB (`DefaultMaxRequestBodySize = 2 << 20`) to leave headroom over the raw tenant body providerd forwards. The backend defaults are overridable via `max_request_body_size` in the backend YAML or the `{DOCKER,K3S}_BACKEND_MAX_REQUEST_BODY_SIZE` env vars (ENG-448).

### Query Parameter Bounds

- `tail` parameter: validated as positive integer, bounded 1–10,000
- Container log reads are additionally capped at 5 MiB total (`maxContainerLogBytes`, docker backend) via a demux-output capping writer — since Docker's `tail` bounds lines, not bytes. Output beyond the cap is truncated with a `[log truncated: exceeded 5 MiB limit]` marker, preventing OOM from an adversarial container emitting oversized stdout/stderr (ENG-499).

### Manifest Validation (Docker Backend)

Tenant-submitted manifests are validated before provisioning:
- `image` field required
- Image validated against configured allowlist using `distribution/reference` (handles Docker Hub normalization)
- Port specs validated (host port range 0–65535)
- Tmpfs paths validated as absolute paths, limited to 4 additional mounts
- Health check command validated (test field required)
- Environment variables passed as key-value map (no shell interpretation)
- Command and args are string arrays (no shell-form injection)

### Payload Hash Verification

Payloads uploaded via `POST /data` are verified against the on-chain `meta_hash`:
- SHA-256 hash computed over raw payload bytes
- Compared using `subtle.ConstantTimeCompare` (prevents timing attacks)
- meta_hash format validated as 64 hex characters

### Archive / Tar Extraction (Docker Backend)

Stateful-volume seed data is extracted from tenant-controlled container images via `sanitizeAndExtractTar` (ENG-430). The extraction is confined and bounded:
- **Structural confinement:** every write goes through an `os.Root` (`os.OpenRoot`) rooted at the destination directory, so the OS refuses any operation that would escape it.
- **Path rejection:** absolute paths and `..` traversal are rejected with an entry-named error; `.`/`./`/empty entries are skipped so a tar cannot `mkdir`/`chown` the extraction root itself.
- **Permission stripping:** setuid/setgid bits are cleared on every entry; ownership uses `Root.Lchown` (never `Chown`) so a symlink is never followed during the ownership change.
- **Symlinks:** links whose targets point outside the root are still created but are harmless — `os.Root` refuses to extract any *later* entry *through* them, and they only resolve inside the bind-mounted container.
- **Byte budget (overflow-safe):** each regular file is checked against the *remaining* budget (`maxBytes - totalBytes`) rather than a running sum, so a tenant-controlled `hdr.Size` near `math.MaxInt64` cannot overflow past the gate; negative sizes are rejected outright. This bounds zip-bomb / oversized-image extraction.
- **Entry-count cap (`maxEntries`, ENG-548):** the number of extracted entries (dirs/files/symlinks) is capped per writable path, so a crafted image can't flood the host with zero-byte files. It is applied per path rather than shared across paths — on a filesystem without an inode quota (btrfs/zfs) the effective bound across all of a container's writable paths is therefore `maxDetectedWritablePaths × maxEntries`, not `maxEntries` alone; on XFS, the volume-wide `ihard` project quota is the true cross-path gate.

## Rate Limiting

### Global (Per-IP)

A single token-bucket limiter applied to **all** HTTP routes, keyed per client IP. It is "global" in the sense of being one limiter shared across every route (not per-route), reported as `fred_api_rate_limit_rejections_total{limiter="global"}` (a single series, no route/path dimension). It is **not** one shared budget for the whole API — each distinct client IP gets its own bucket.

| Parameter | Default |
|-----------|---------|
| Requests per second | 10 |
| Burst size | 20 |
| Max tracked IPs | 10,000 (LRU eviction) |
| TTL per entry | 3 minutes |

Because the bucket is shared across routes, all direct API operations from one client IP contend for the same budget — lease restore, update, restart, and `/data` uploads draw from a single bucket, so a burst of one can `429` the others. The defaults above are protective production values; for load tests or high-throughput direct-API workloads, raise `rate_limit_rps`/`rate_limit_burst` (and/or set `trusted_proxies`); otherwise the limiter, not the backends, becomes the binding throughput ceiling.

Trusted proxies can be configured via CIDR ranges. When the direct connection comes from a trusted proxy, `X-Forwarded-For` is used to extract the real client IP. Untrusted `X-Forwarded-For` headers are ignored. **If fred runs behind a reverse proxy and `trusted_proxies` is not configured, every request appears to come from the proxy's IP, collapsing all clients into one bucket — effectively a single global cap at `rate_limit_rps`.**

### Per-Tenant

Separate token bucket per tenant, applied after token extraction.

| Parameter | Default |
|-----------|---------|
| Requests per second | 5 |
| Burst size | 10 |
| Max tracked tenants | 10,000 (LRU eviction) |
| TTL per entry | 5 minutes |

**Design note:** Tokens are cryptographically validated (signature + timestamp + address) in the rate-limit middleware **before** consuming from the tenant's bucket. This prevents attackers from burning a victim's quota with forged tokens. The validated token is stored in request context so downstream handlers skip redundant ECDSA verification.

### Response Headers

Rate-limited responses include `Retry-After` with the number of seconds until the next request will be accepted.

## Transport Security

### TLS (API Server)

Optional HTTPS for the tenant-facing API. Configured via `tls_cert_file` and `tls_key_file`. HTTP/2 is configured automatically.

### TLS (gRPC to Chain)

Optional TLS for the gRPC connection to the chain. Supports custom CA file. `grpc_tls_skip_verify` is available for testing but blocked in production mode.

### TLS (providerd → backend, ENG-103)

TLS, including optional mutual TLS, protects both backend HTTP hops. Plaintext is supported only outside production mode. Provider `production_mode: true` requires every backend URL and `callback_base_url` to use HTTPS and verifies backend peers against the configured private CA or system roots; bundled-backend production mode independently forbids disabling callback peer verification. A self-signed private CA is a valid trust anchor—the leaf chain and hostname/IP SAN are still verified. Both settings are required for the full production invariant: request HMAC does not authenticate the backend's response identity, inventory, or refusal verdict, while callback HMAC does not provide confidentiality for causal tokens. When TLS is enabled, both sides pin TLS 1.3 as the minimum version (`internal/tlsconfig/tlsconfig.go:37,61`). Certificates are loaded once at startup; rotation requires a restart (tracked in ENG-294).

The reference deployment uses different verified chains in each direction:
providerd verifies backend IP-SAN certificates against an internal private CA
and authenticates with mTLS; backend callback clients verify the public
certificate terminated by Traefik for `callback_base_url` through system roots.
The backend's `callback_insecure_skip_verify: false` is the setting that performs
that client verification. Its separate `production_mode: true` is a
configuration guard—it refuses a future `true` value for the skip flag—but it
does not create verification on another process's behalf. Providerd production
mode likewise cannot substitute for enabling the backend process's guard.

**Server side (docker-backend YAML, `internal/backend/docker/config.go:65-81`):**

| Field | Effect |
|-------|--------|
| `tls_cert_file`, `tls_key_file` | Enable HTTPS on the listener (both required) |
| `tls_client_ca_file` | Require and verify a client certificate signed by this CA (mutual TLS); requires the cert/key pair above |
| `tls_client_allowed_names` | Pin the client's identity (see below); requires `tls_client_ca_file` |

Wired via `tlsconfig.ServerConfig` (`cmd/docker-backend/main.go:108-115`).

**Client side (providerd `backends[]`, `internal/config/config.go:144-147`):**

| Field | Effect |
|-------|--------|
| `tls_ca_file` | Private CA that signed the backend's server cert (otherwise system roots) |
| `tls_client_cert_file`, `tls_client_key_file` | Client cert/key presented for mutual TLS (both or neither) |
| `tls_skip_verify` | Disable server cert verification (dev only; blocked in production mode) |

Wired via `tlsconfig.ClientConfig` (`cmd/providerd/main.go:263`).

**Client-identity pinning.** `tls.Config.RequireAndVerifyClientCert` only proves the client's certificate chains to the configured CA — it does not check *who* the client is. Without `tls_client_allowed_names`, any certificate signed by the configured client CA is accepted. When `tls_client_allowed_names` is set, the verified client leaf's CommonName or one of its DNS SANs must appear in the list. The check is implemented as a `tls.Config.VerifyConnection` callback, **not** `VerifyPeerCertificate` — a `VerifyPeerCertificate` callback is skipped on resumed TLS sessions, so using it would let a previously-authenticated client resume a session and bypass the name pin. `VerifyConnection` runs on every handshake, including resumptions, closing that bypass (`internal/tlsconfig/tlsconfig.go:24-27,47-49,84-113`).

### Security Headers

All responses include:

| Header | Value |
|--------|-------|
| `X-Content-Type-Options` | `nosniff` |
| `X-Frame-Options` | `DENY` |
| `X-XSS-Protection` | `1; mode=block` |
| `Cache-Control` | `no-store` |

### HTTP Timeouts

| Timeout | Default | Purpose |
|---------|---------|---------|
| Read timeout | 15s | Time to read request headers + body |
| Write timeout | 15s | Time to write response |
| Idle timeout | 60s | Time to keep idle connections open |
| Request timeout | 30s | Total handler processing time |

## Container Hardening (Docker Backend)

Every container created by the Docker backend runs with these security measures:

| Feature | Implementation | Notes |
|---------|---------------|-------|
| Drop all capabilities | `CapDrop: ["ALL"]` | No Linux capabilities granted |
| No new privileges | `SecurityOpt: ["no-new-privileges:true"]` | Prevents escalation via setuid/setgid |
| Read-only root filesystem | `ReadonlyRootfs: true` | Configurable via `container_readonly_rootfs` |
| Tmpfs for writable paths | `/tmp` and `/run` mounted as tmpfs | Size from `container_tmpfs_size_mb` (default 64MB) |
| PID limit | `PidsLimit: 256` | Configurable via `container_pids_limit` |
| No swap | `MemorySwap == Memory` | Prevents swap usage entirely |
| Per-volume disk quota | `bhard` block limit (xfs project quota / btrfs qgroup) | Enforces the SKU `disk_mb` cap on stateful volumes; requires the daemon capability noted below |
| Per-volume inode quota | `ihard` limit (xfs project quota) | Bounds host inode exhaustion from zero-byte file floods; same `xfs_quota` call and `CAP_SYS_ADMIN` requirement as the disk quota above (ENG-548) |
| Restart policy disabled | `RestartPolicyDisabled` | Failed containers stay dead for crash detection |
| Network isolation | Per-tenant bridge network | Configurable via `network_isolation` |

Network isolation places each tenant's containers in a dedicated Docker bridge network. Docker's `DOCKER-ISOLATION` iptables chains drop forwarded traffic between different bridge networks, preventing cross-tenant communication.

**Daemon capabilities vs. container privileges.** The controls above constrain the tenant *container*, which continues to run with `CapDrop: ["ALL"]` — that is unchanged. Enforcing the per-volume disk quota, however, requires the docker-backend *daemon itself* to hold `CAP_SYS_ADMIN` to set the xfs/btrfs block limit — granted via `AmbientCapabilities=CAP_SYS_ADMIN` on the systemd unit, or by running as root. On an xfs or btrfs backend the daemon **fails fast at startup** if it lacks `CAP_SYS_ADMIN`, rather than silently skipping the cap and leaving `disk_mb` unenforced (`internal/backend/docker/capability.go`). zfs is exempt (it uses `zfs allow` delegation, so a cap check would wrongly reject a properly-delegated non-root host); the noop backend is unaffected. Re-tagging an existing XFS tree may additionally need `CAP_FOWNER` when it contains tenant-owned inodes. A truly fresh root does not need that capability, so the preliminary capability probe checks only `CAP_SYS_ADMIN`; startup quota reconciliation then attempts every expected present live or retained volume and refuses readiness on any inventory, durable-authority, or enforcement error. See [DEPLOYMENT.md](DEPLOYMENT.md) — filesystem setup (xfs) and the systemd capabilities note — for the full `AmbientCapabilities=CAP_SYS_ADMIN CAP_FOWNER` grant procedure.

## Error Handling

- **Client responses:** Generic error messages (`"internal server error"`) for 500-class errors. Validation errors (400) include specific messages since these describe client input problems.
- **Server-side logging:** Full error details logged via `slog` including stack context, lease UUIDs, and backend names.
- **Error truncation:** Callback error messages (on-chain rejection reasons) are truncated to 256 characters. `LastError` in provision diagnostics retains the full untruncated error for authenticated API access.
- **Auth errors:** Generic `"unauthorized"` message — does not distinguish between missing token, invalid signature, or expired token.

## Secrets Management

| Secret | Minimum Length | Constant-Time | Logged |
|--------|---------------|---------------|--------|
| `backends[].hmac_secret` (providerd) / that backend's `callback_secret` | 32 bytes; unique per backend | Yes (`hmac.Equal`) | Never |
| Payload `meta_hash` | 64 hex chars | Yes (`subtle.ConstantTimeCompare`) | Never |
| ADR-036 signatures | N/A | secp256k1 library verify | Signature logged in debug (public data) |

**Secret rotation:** Each backend key is static for that provider/backend
channel. Rotate one channel with a coordinated stopped restart so queued
callbacks and provider commands never cross a mixed-key interval. See
[DEPLOYMENT.md § Secret rotation](DEPLOYMENT.md#secret-rotation) for the
procedure.

## Production Mode

When `production_mode: true`, Fred enforces security requirements at startup:

| Check | Rationale |
|-------|-----------|
| `token_tracker_db_path` required | Replay protection must be enabled |
| Every backend has a distinct `backends[].hmac_secret`; top-level `callback_secret` rejected | A compromised backend must not authenticate another backend's commands or callbacks |
| `grpc_tls_skip_verify` blocked (when TLS enabled) | Prevent MITM on chain connection |
| Every `backends[].url` uses `https://` | Authenticate storage-identity observations, inventory, and synchronous backend verdicts |
| `backends[].tls_skip_verify` blocked | Prevent MITM on the providerd → backend connection |
| SSRF checks on all URLs | Block loopback, link-local, unspecified addresses |

The daemon refuses to start if any check fails.

**Docker/k3s backend.** Each backend is a separate process with its own config and its own `production_mode` flag. When set, the backend rejects `callback_insecure_skip_verify` (which would disable TLS verification on the backend → Fred callback hop) and refuses to start. This mirrors the `backends[].tls_skip_verify` gate above, closing the same MITM exposure on the reverse (backend → Fred) direction.

## Known Limitations

1. **SSRF checks are IP-literal only.** Hostnames resolving to blocked addresses (e.g., a DNS record pointing `evil.com` to `127.0.0.1`) are not caught. DNS resolution is intentionally skipped to avoid TOCTOU race conditions. Use network-level controls (firewall rules, egress policies) for defense in depth.

2. **Release history contains secrets and causal capabilities.** The `GET /releases` response includes the full manifest payload. If tenants put secrets in environment variables, those persist in the release store and are returned on read. Typed active releases also retain the operation/lifecycle callback destinations needed for non-expiring runtime recovery. Keep `releases.db`, stopped-process copies, and backups private with the matching callback/retention journals; do not expose raw records in tickets or logs. The API remains tenant-visible-to-tenant-only when properly authenticated.

3. **Per-tenant rate limiting adds ECDSA cost to every request.** Tokens are fully validated (secp256k1 signature verification) before bucket consumption, which adds CPU overhead per request. This is the correct trade-off: the previous design (skipping verification) allowed attackers to burn a victim's quota with forged tokens.

## Incident response

For runbook-style guidance on responding to active incidents — replay attempts, suspicious callback traffic, sustained auth failures, wedged actors — see [OPERATIONS.md](OPERATIONS.md). Security-relevant signals are flagged in the alert table there (rate-limit spikes, panic counters, callback timeouts, etc.).
