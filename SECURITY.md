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

**Path-stripping reverse proxies.** Because the canonical string includes the request URI, deployments where a reverse proxy rewrites the inbound path (e.g., Traefik `stripPrefix` middleware mapping `/api/fred/*` → fred's bare `/*`) would otherwise see a verifier/signer URI mismatch: backends sign the full external URL while fred receives the stripped URL post-proxy. The static `callback_canonical_path_prefix` config field tells fred's verifier what prefix to prepend to `r.URL.RequestURI()` before computing the canonical string, so signer and verifier agree. The value is **static config**, not derived from request headers (e.g., `X-Forwarded-Prefix`) — this rules out spoofing as a failure mode and leaves "config is correct" as the only invariant to maintain. The prefix must be sourced from the same configuration variable that defines the proxy's strip rule; drift between the two breaks every callback. The TLS-posture context that makes the underlying ENG-191 URI binding load-bearing is documented in `manifest-deploy/CLAUDE.md:181` and `:195`. See [docs/security-callback-auth.md](docs/security-callback-auth.md) for the full threat-model rationale.

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

Fred authenticates requests to backends using the same HMAC-SHA256 scheme. The docker-backend verifies these signatures via auth middleware on all contract endpoints: `POST /provision`, `POST /deprovision`, `POST /restart`, `POST /update`, `POST /reconcile_custom_domain`, `GET /info/{lease_uuid}`, `GET /logs/{lease_uuid}`, `GET /provisions`, `GET /provisions/{lease_uuid}`, `GET /releases/{lease_uuid}`. The monitoring endpoints `GET /health`, `GET /stats`, and `GET /metrics` are unauthenticated.

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

### TLS (providerd → backend, ENG-103)

Optional TLS, including mutual TLS, on the providerd → backend HTTP transport. TLS is opt-in: when no TLS fields are configured the hop serves plaintext HTTP (the default). When enabled, both sides pin TLS 1.3 as the minimum version (`internal/tlsconfig/tlsconfig.go:37,61`). Certificates are loaded once at startup; rotation requires a restart (tracked in ENG-294).

**Server side (docker-backend YAML, `internal/backend/docker/config.go:44-60`):**

| Field | Effect |
|-------|--------|
| `tls_cert_file`, `tls_key_file` | Enable HTTPS on the listener (both required) |
| `tls_client_ca_file` | Require and verify a client certificate signed by this CA (mutual TLS); requires the cert/key pair above |
| `tls_client_allowed_names` | Pin the client's identity (see below); requires `tls_client_ca_file` |

Wired via `tlsconfig.ServerConfig` (`cmd/docker-backend/main.go:108-115`).

**Client side (providerd `backends[]`, `internal/config/config.go:138-141`):**

| Field | Effect |
|-------|--------|
| `tls_ca_file` | Private CA that signed the backend's server cert (otherwise system roots) |
| `tls_client_cert_file`, `tls_client_key_file` | Client cert/key presented for mutual TLS (both or neither) |
| `tls_skip_verify` | Disable server cert verification (dev only; blocked in production mode) |

Wired via `tlsconfig.ClientConfig` (`cmd/providerd/main.go:249-255`).

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

**Secret rotation:** The callback secret is static per deployment. Rotation requires coordinated restart of Fred and all backends with the new secret. See [DEPLOYMENT.md § Secret rotation](DEPLOYMENT.md#secret-rotation) for the procedure.

## Production Mode

When `production_mode: true`, Fred enforces security requirements at startup:

| Check | Rationale |
|-------|-----------|
| `token_tracker_db_path` required | Replay protection must be enabled |
| `grpc_tls_skip_verify` blocked (when TLS enabled) | Prevent MITM on chain connection |
| `backends[].tls_skip_verify` blocked | Prevent MITM on the providerd → backend connection |
| SSRF checks on all URLs | Block loopback, link-local, unspecified addresses |

The daemon refuses to start if any check fails.

**Docker/k3s backend.** Each backend is a separate process with its own config and its own `production_mode` flag. When set, the backend rejects `callback_insecure_skip_verify` (which would disable TLS verification on the backend → Fred callback hop) and refuses to start. This mirrors the `backends[].tls_skip_verify` gate above, closing the same MITM exposure on the reverse (backend → Fred) direction.

## Known Limitations

1. **SSRF checks are IP-literal only.** Hostnames resolving to blocked addresses (e.g., a DNS record pointing `evil.com` to `127.0.0.1`) are not caught. DNS resolution is intentionally skipped to avoid TOCTOU race conditions. Use network-level controls (firewall rules, egress policies) for defense in depth.

2. **Release history includes raw manifests.** The `GET /releases` response includes the full manifest payload. If tenants put secrets in environment variables, those persist in the release store and are returned on read. This is tenant-visible-to-tenant-only (properly authenticated), but tenants should be aware that manifest contents are stored.

3. **Per-tenant rate limiting adds ECDSA cost to every request.** Tokens are fully validated (secp256k1 signature verification) before bucket consumption, which adds CPU overhead per request. This is the correct trade-off: the previous design (skipping verification) allowed attackers to burn a victim's quota with forged tokens.

## Incident response

For runbook-style guidance on responding to active incidents — replay attempts, suspicious callback traffic, sustained auth failures, wedged actors — see [OPERATIONS.md](OPERATIONS.md). Security-relevant signals are flagged in the alert table there (rate-limit spikes, panic counters, callback timeouts, etc.).
