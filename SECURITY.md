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

Backends authenticate callbacks to Fred using HMAC-SHA256 with timestamp-based replay protection, following the Stripe webhook pattern.

**Header format:** `X-Fred-Signature: t=<unix-timestamp>,sha256=<hex-encoded-hmac>`

**Signed payload:** `<timestamp>.<request-body>` — binding the timestamp to the body prevents timestamp substitution attacks.

| Parameter | Value |
|-----------|-------|
| Algorithm | HMAC-SHA256 |
| Minimum secret length | 32 bytes |
| Maximum callback age | 5 minutes |
| Clock skew tolerance | 1 minute into the future |
| Comparison | `hmac.Equal` (constant-time) |

**Signing example (Go):**
```go
timestamp := time.Now().Unix()
signedPayload := fmt.Sprintf("%d.%s", timestamp, body)

mac := hmac.New(sha256.New, []byte(secret))
mac.Write([]byte(signedPayload))
sig := hex.EncodeToString(mac.Sum(nil))

header := fmt.Sprintf("t=%d,sha256=%s", timestamp, sig)
```

**Implementation:** `internal/api/callback_auth.go`, `internal/hmacauth/`

### Backend Authentication (HMAC-SHA256)

Fred authenticates requests to backends using the same HMAC-SHA256 scheme. The docker-backend verifies these signatures via auth middleware on all operational endpoints: `POST /provision`, `POST /deprovision`, `POST /restart`, `POST /update`, `GET /info/{lease_uuid}`, `GET /logs/{lease_uuid}`, `GET /provisions`, `GET /provisions/{lease_uuid}`, `GET /releases/{lease_uuid}`. Health and metrics endpoints are unauthenticated.

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
| `GET /status` | No | Idempotent read |
| `GET /provision` | No | Idempotent read |
| `GET /logs` | No | Idempotent read |
| `GET /releases` | No | Idempotent read |
| `GET /events` | No | Read-only WebSocket stream |
| `POST /data` | No | Has own idempotency guard (409 on duplicate upload) |

**Configuration:** Requires `token_tracker_db_path`. Mandatory when `production_mode: true`. When not configured (non-production), replay protection is disabled entirely — tokens can be replayed within their 30-second validity window. This is acceptable for development but **must not be used in production**.

**Implementation:** `internal/api/token_tracker.go`

### Callback Replay (Backend -> Fred)

Callback timestamps older than 5 minutes are rejected. Combined with HMAC binding the timestamp to the body, this prevents both replay and timestamp substitution.

## Input Validation

### UUID Validation

All lease UUIDs are validated with `google/uuid.Parse` before use. Invalid UUIDs are rejected with 400.

### URL Validation

All configured URLs (callback_base_url, backend URLs) are validated at startup:
- Must be absolute `http://` or `https://` URLs with a host
- Trailing slashes stripped from callback_base_url

In production mode, additional SSRF checks block:
- Loopback addresses (`127.0.0.0/8`, `::1`, `::ffff:127.0.0.1`)
- Link-local addresses (`169.254.0.0/16`, `fe80::/10`)
- Unspecified addresses (`0.0.0.0`, `::`)
- `localhost` hostname (case-insensitive, including FQDN `localhost.`)

Private IPs (RFC 1918) are intentionally allowed since backends commonly run on private networks. DNS resolution is not performed — hostnames resolving to blocked addresses are not caught. Use network-level controls for defense in depth.

### Request Body Limits

All requests are wrapped with `http.MaxBytesReader` enforcing a configurable maximum (default 1MB).

### Query Parameter Bounds

- `tail` parameter: validated as positive integer, bounded 1–10,000

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

## Rate Limiting

### Global (Per-IP)

Token bucket rate limiting applied to all requests based on client IP.

| Parameter | Default |
|-----------|---------|
| Requests per second | 10 |
| Burst size | 20 |
| Max tracked IPs | 10,000 (LRU eviction) |
| TTL per entry | 3 minutes |

Trusted proxies can be configured via CIDR ranges. When the direct connection comes from a trusted proxy, `X-Forwarded-For` is used to extract the real client IP. Untrusted `X-Forwarded-For` headers are ignored.

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
| Restart policy disabled | `RestartPolicyDisabled` | Failed containers stay dead for crash detection |
| Network isolation | Per-tenant bridge network | Configurable via `network_isolation` |

Network isolation places each tenant's containers in a dedicated Docker bridge network. Docker's `DOCKER-ISOLATION` iptables chains drop forwarded traffic between different bridge networks, preventing cross-tenant communication.

## Error Handling

- **Client responses:** Generic error messages (`"internal server error"`) for 500-class errors. Validation errors (400) include specific messages since these describe client input problems.
- **Server-side logging:** Full error details logged via `slog` including stack context, lease UUIDs, and backend names.
- **Error truncation:** Callback error messages (on-chain rejection reasons) are truncated to 256 characters. `LastError` in provision diagnostics retains the full untruncated error for authenticated API access.
- **Auth errors:** Generic `"unauthorized"` message — does not distinguish between missing token, invalid signature, or expired token.

## Secrets Management

| Secret | Minimum Length | Constant-Time | Logged |
|--------|---------------|---------------|--------|
| `callback_secret` | 32 bytes | Yes (`hmac.Equal`) | Never |
| Payload `meta_hash` | 64 hex chars | Yes (`subtle.ConstantTimeCompare`) | Never |
| ADR-036 signatures | N/A | secp256k1 library verify | Signature logged in debug (public data) |

**Secret rotation:** The callback secret is static per deployment. Rotation requires coordinated restart of Fred and all backends with the new secret.

## Production Mode

When `production_mode: true`, Fred enforces security requirements at startup:

| Check | Rationale |
|-------|-----------|
| `token_tracker_db_path` required | Replay protection must be enabled |
| `grpc_tls_skip_verify` blocked (when TLS enabled) | Prevent MITM on chain connection |
| SSRF checks on all URLs | Block loopback, link-local, unspecified addresses |

The daemon refuses to start if any check fails.

## Known Limitations

1. **SSRF checks are IP-literal only.** Hostnames resolving to blocked addresses (e.g., a DNS record pointing `evil.com` to `127.0.0.1`) are not caught. DNS resolution is intentionally skipped to avoid TOCTOU race conditions. Use network-level controls (firewall rules, egress policies) for defense in depth.

2. **Release history includes raw manifests.** The `GET /releases` response includes the full manifest payload. If tenants put secrets in environment variables, those persist in the release store and are returned on read. This is tenant-visible-to-tenant-only (properly authenticated), but tenants should be aware that manifest contents are stored.

3. **Per-tenant rate limiting adds ECDSA cost to every request.** Tokens are fully validated (secp256k1 signature verification) before bucket consumption, which adds CPU overhead per request. This is the correct trade-off: the previous design (skipping verification) allowed attackers to burn a victim's quota with forged tokens.
