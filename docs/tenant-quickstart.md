# Tenant Quickstart

This guide walks through the tenant-side flow against a Fred provider: creating an authenticated bearer token, querying lease state, uploading a deployment manifest, and observing real-time events.

For the canonical API reference, see [README.md](../README.md#api-endpoints). For manifest format details, see [manifest-guide.md](manifest-guide.md). For the security model, see [SECURITY.md](../SECURITY.md).

> **Note**: This document describes the protocol. Fred does not currently ship a tenant-side SDK. The reference implementation lives at `cmd/loadtest/main.go` (load tester) and `internal/testutil/fixtures.go` (concise token-creation helpers). When integrating, port these patterns to your wallet's signing primitives.

---

## Prerequisites

To exercise the API end-to-end you need:

1. **A wallet with a secp256k1 key** registered on the Manifest chain (the standard Cosmos keyring is fine). The wallet's bech32 address (with the chain's configured prefix, typically `manifest1...`) is the **tenant address**.
2. **An active or pending lease** on the chain that names a provider whose Fred you want to talk to. The lease has a UUID — that's the `lease_uuid`.
3. **The provider's Fred URL**, e.g. `https://fred.example-provider.com:8080`.

If the lease was created with a `meta_hash` set, the chain expects you to upload a deployment manifest before provisioning starts (see [Step 3](#step-3-upload-the-deployment-manifest)).

---

## Step 1: Build a bearer token

Every authenticated tenant request carries a base64-encoded JSON envelope as the `Authorization: Bearer <token>` header. The envelope includes the tenant address, lease UUID, timestamp, public key, and an ADR-036 signature.

### Token shape

```json
{
  "tenant": "manifest1abc...",
  "lease_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": 1735689600,
  "pub_key": "<base64-encoded-secp256k1-pubkey>",
  "signature": "<base64-encoded-ADR-036-signature>"
}
```

The whole object is then base64-encoded and used as the bearer token.

### Signed message

The signature covers the literal string:

```
{tenant}:{lease_uuid}:{unix_timestamp}
```

For example: `manifest1abc...:550e8400-e29b-41d4-a716-446655440000:1735689600`

This message is signed using **ADR-036** (Cosmos's standard for off-chain message signing), which wraps the message in a sign-doc structure before signing with secp256k1. The resulting signature is then **normalized to low-S** canonical form to prevent malleability — most secp256k1 libraries do this by default, but verify before going to production.

### Validity window

| Constraint | Limit |
|---|---|
| Maximum age | 30 seconds |
| Maximum future skew | 10 seconds |
| Replay (mutating endpoints + `/connection`) | One-time use |

`/connection` is included because it returns sensitive endpoint details; the other read endpoints (`/status`, `/provision`, `/logs`, `/releases`, `/events`) are idempotent and skip the replay check. See [SECURITY.md § Which endpoints check replay](../SECURITY.md#which-endpoints-check-replay) for the full table.

Tokens are short-lived. Generate a fresh one for each request, or batch requests within a 30-second window.

### Reference implementation

`internal/testutil/fixtures.go::CreateTestToken` is the most concise reference (~25 lines). The signing uses `internal/auth.FormatSignData` for the message and `internal/adr036.CreateSignBytes` for the ADR-036 wrapper. `cmd/loadtest/main.go` uses ed25519 for its internal mock; **do not use that as a signing reference** for real ADR-036 tokens.

---

## Step 2: Check lease status

The first call you'll typically make is `GET /v1/leases/{uuid}/status` — it's an idempotent read, so no replay protection is enforced and you can retry freely.

```bash
curl -H "Authorization: Bearer $TOKEN" \
  https://fred.example-provider.com:8080/v1/leases/$LEASE_UUID/status
```

```json
{
  "lease_uuid": "550e8400-...",
  "tenant": "manifest1abc...",
  "provider_uuid": "01234567-...",
  "state": "PENDING",
  "requires_payload": true,
  "meta_hash_hex": "a1b2c3...",
  "payload_received": false,
  "provisioning_started": false
}
```

What to look at:

- **`state`** — `PENDING` (waiting for provisioning), `ACTIVE` (running), `CLOSED`, `EXPIRED`.
- **`requires_payload`** — if `true` and `payload_received` is `false`, you need to upload a manifest before provisioning can start.
- **`provision_status`** — present once provisioning has started (`provisioning`, `ready`, `failing`, `failed`, `restarting`, `updating`, `deprovisioning`).
- **`fail_count`** + **`last_error`** — present after failures; only the count and most recent message.

For richer diagnostics during failures, use `GET /v1/leases/{uuid}/provision` (see [Step 5](#step-5-debug-failures)).

---

## Step 3: Upload the deployment manifest

Required only when `meta_hash` is set on the lease. The manifest is a JSON document — see [manifest-guide.md](manifest-guide.md) for the full schema and validation rules. A formal [JSON Schema](manifest-schema.json) is also available for client-side validation.

### Build a payload-specific token

Payload upload uses a **different** signed message format that binds the token to the manifest's hash:

```
manifest lease data {lease_uuid} {meta_hash_hex} {unix_timestamp}
```

The token JSON envelope adds a `meta_hash` field:

```json
{
  "tenant": "manifest1abc...",
  "lease_uuid": "550e8400-...",
  "meta_hash": "a1b2c3...",
  "timestamp": 1735689600,
  "pub_key": "<base64>",
  "signature": "<base64>"
}
```

Reference: `internal/testutil/fixtures.go::CreateTestPayloadToken`.

This binding prevents a stolen access token from being used to upload a different manifest.

### Send the manifest

```bash
# Compute the hash and verify it matches the on-chain meta_hash
sha256sum manifest.json | awk '{print $1}'   # should equal meta_hash_hex from /status

# Build the payload token (with meta_hash bound)
PAYLOAD_TOKEN=...

# Upload — body is the raw JSON (NOT base64-wrapped, unlike /update)
curl -X POST \
  -H "Authorization: Bearer $PAYLOAD_TOKEN" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @manifest.json \
  https://fred.example-provider.com:8080/v1/leases/$LEASE_UUID/data
```

Possible responses:

| Code | Meaning |
|---|---|
| `202 Accepted` | Payload accepted, provisioning will start |
| `400 Bad Request` | Manifest is malformed or hash doesn't match `meta_hash` |
| `401 Unauthorized` | Invalid token / signature / wrong meta_hash field |
| `404 Not Found` | Lease doesn't exist or isn't `PENDING` |
| `409 Conflict` | A payload was already uploaded for this lease (per-lease idempotency) |

The `409 Conflict` is your idempotency guard — if your upload retried successfully but the response was lost, the second attempt safely returns 409.

---

## Step 4: Wait for the resource to come online

You have two choices:

### Polling

```bash
while true; do
  status=$(curl -s -H "Authorization: Bearer $(fresh_token)" \
    https://fred.example-provider.com:8080/v1/leases/$LEASE_UUID/status \
    | jq -r .provision_status)
  echo "$status"
  [[ "$status" == "ready" || "$status" == "failed" ]] && break
  sleep 5
done
```

(Generate a fresh token each iteration since they expire after 30s.)

### WebSocket events stream

```bash
# Connect with the token in the query string (WebSocket can't set custom headers)
wscat -c "wss://fred.example-provider.com:8080/v1/leases/$LEASE_UUID/events?token=$TOKEN"
```

Each frame is JSON:

```json
{"lease_uuid":"550e8400-...","status":"provisioning","timestamp":"2024-01-15T10:30:00Z"}
{"lease_uuid":"550e8400-...","status":"ready","timestamp":"2024-01-15T10:30:42Z"}
```

The server sends WebSocket pings every 30 seconds; clients must respond with pong within 40 seconds. If the connection drops or you fall behind, fetch `/status` to catch up — events for slow clients are dropped.

---

## Step 5: Get connection details

Once the lease reaches `ready`:

```bash
curl -H "Authorization: Bearer $(fresh_token)" \
  https://fred.example-provider.com:8080/v1/leases/$LEASE_UUID/connection
```

The response contains the host (or FQDN if ingress is configured), per-port host bindings, and any backend-specific metadata. Multi-instance leases include an `instances` array; stack leases include a `services` map. See the [API reference](../README.md#get-lease-connection) for full shape.

> **Replay protection**: `/connection` returns sensitive endpoint details, so it does enforce one-time-use replay protection. Each call needs a fresh token.

---

## Step 6: Debug failures

If `provision_status` is `failed`, fetch full diagnostics:

```bash
curl -H "Authorization: Bearer $(fresh_token)" \
  https://fred.example-provider.com:8080/v1/leases/$LEASE_UUID/provision
```

```json
{
  "lease_uuid": "550e8400-...",
  "tenant": "manifest1abc...",
  "provider_uuid": "01234567-...",
  "status": "failed",
  "fail_count": 2,
  "last_error": "container 0 exited during startup (status: exited): exit_code=1; logs:\nError: config file not found"
}
```

For full container logs:

```bash
curl -H "Authorization: Bearer $(fresh_token)" \
  "https://fred.example-provider.com:8080/v1/leases/$LEASE_UUID/logs?tail=500"
```

`last_error` and `logs` are kept for 7 days after the provision is gone (configurable by the operator). On-chain rejection messages are intentionally generic (`container exited unexpectedly`) so secrets in container logs cannot leak there — the rich diagnostics only flow through the authenticated API.

---

## Step 7: Restart or update

Two operations are available on a `ready` (or `failed`) lease:

### Restart — same manifest, fresh containers

```bash
curl -X POST -H "Authorization: Bearer $(fresh_token)" \
  https://fred.example-provider.com:8080/v1/leases/$LEASE_UUID/restart
```

Volumes are preserved. Useful when a container is stuck or you want to re-run startup logic.

### Update — new manifest

```bash
NEW_MANIFEST_B64=$(base64 < new-manifest.json | tr -d '\n')   # portable across GNU and BSD/macOS
curl -X POST -H "Authorization: Bearer $(fresh_token)" \
  -H "Content-Type: application/json" \
  -d "{\"payload\": \"$NEW_MANIFEST_B64\"}" \
  https://fred.example-provider.com:8080/v1/leases/$LEASE_UUID/update
```

The body shape is different from `/data` — the manifest is base64-encoded inside a JSON wrapper. Volumes are preserved across updates. On failure the operation rolls back to the previous containers.

Both endpoints enforce **replay protection** since they're mutating. Each retry needs a fresh token (hence `$(fresh_token)` rather than a stored `$TOKEN` variable).

To see what's been deployed:

```bash
curl -H "Authorization: Bearer $(fresh_token)" \
  https://fred.example-provider.com:8080/v1/leases/$LEASE_UUID/releases
```

---

## Rate limits

| Layer | Default |
|---|---|
| Per-IP global | 10 RPS, burst 20 |
| Per-tenant | 5 RPS, burst 10 |

Rate-limited responses include a `Retry-After` header (seconds). Tokens are validated cryptographically **before** the per-tenant bucket is consumed, so attackers cannot burn your quota with forged tokens.

---

## End-to-end example: pseudocode

```
# Wallet (out of scope for this guide — your existing Cosmos wallet)
keypair = LoadCosmosKey()
tenant = keypair.Address()

# Token helpers (see internal/testutil/fixtures.go for the canonical Go version)
def make_token(lease_uuid):
    timestamp = int(time.time())
    msg = f"{tenant}:{lease_uuid}:{timestamp}".encode()
    sign_doc = adr036_wrap(msg, tenant)
    sig = keypair.sign(sign_doc)            # secp256k1 + low-S
    envelope = {
        "tenant": tenant,
        "lease_uuid": lease_uuid,
        "timestamp": timestamp,
        "pub_key": base64(keypair.PubKey().Bytes()),
        "signature": base64(sig),
    }
    return base64(json.dumps(envelope))

def make_payload_token(lease_uuid, meta_hash_hex):
    timestamp = int(time.time())
    msg = f"manifest lease data {lease_uuid} {meta_hash_hex} {timestamp}".encode()
    sign_doc = adr036_wrap(msg, tenant)
    sig = keypair.sign(sign_doc)
    envelope = {
        "tenant": tenant,
        "lease_uuid": lease_uuid,
        "meta_hash": meta_hash_hex,
        "timestamp": timestamp,
        "pub_key": base64(keypair.PubKey().Bytes()),
        "signature": base64(sig),
    }
    return base64(json.dumps(envelope))

# Lifecycle
lease_uuid = "550e8400-..."
manifest = open("manifest.json", "rb").read()
meta_hash = sha256(manifest).hex()

# Upload manifest
http.post(
    f"{fred_url}/v1/leases/{lease_uuid}/data",
    headers={"Authorization": f"Bearer {make_payload_token(lease_uuid, meta_hash)}"},
    data=manifest,
)

# Wait for ready
while True:
    r = http.get(
        f"{fred_url}/v1/leases/{lease_uuid}/status",
        headers={"Authorization": f"Bearer {make_token(lease_uuid)}"},
    )
    if r.json()["provision_status"] in ("ready", "failed"):
        break
    sleep(5)

# Get connection
r = http.get(
    f"{fred_url}/v1/leases/{lease_uuid}/connection",
    headers={"Authorization": f"Bearer {make_token(lease_uuid)}"},
)
print(r.json())
```
