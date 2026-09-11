# Callback Authentication — Threat Model & Design Rationale

Reference-grade document for engineers about to touch the fred ↔ backend HMAC channel, the `callback_canonical_path_prefix` config, or the upstream proxy strip rule. Read this before "improving" any of them.

Cross-links: [ENG-191 (PR #88)](https://github.com/manifest-network/fred/pull/88) bound method + URI into the HMAC canonical string. [ENG-198](https://linear.app/liftedinit/issue/ENG-198) added the static prefix that compensates for the upstream proxy's path rewrite. See also [SECURITY.md § Callback Authentication](../SECURITY.md#callback-authentication-hmac-sha256).

## 1. Two-leg topology asymmetry

Fred ↔ backends is a **bidirectional HMAC channel**, not a one-way webhook fan-out:

| Leg | Direction | Network path |
| -- | -- | -- |
| Fred → backend | Outbound from fred (provision, deprovision, restart, update, ...) | Direct — no proxy in front of the backend |
| Backend → fred | Inbound callbacks (provision result, lease state) | Traverses Traefik on the way to fred |

The signer's view of the URI and the verifier's view of the URI agree on the outbound leg (no rewriter sits between them) but **diverge on the inbound leg** (Traefik strips the path before fred sees it). Any signature-binding change that includes the URI in its canonical string surfaces this asymmetry. ENG-191 was such a change, and ENG-198 was the compensation.

## 2. TLS and proxy posture

Production deployments use certificate-verified TLS/mTLS on the fred ↔
backend channel, and `providerd` now rejects an `http://` backend URL when
`production_mode: true` (see [SECURITY.md § TLS (providerd → backend,
ENG-103)](../SECURITY.md#tls-providerd--backend-eng-103)). This is required
because request HMAC does not authenticate the backend's response headers or
body. Development mode still permits plaintext, so the HMAC scheme must remain
robust independent of TLS: an observer of such a network can read every byte,
which is why ENG-191 must bind method + URI into the canonical string.

## 3. ENG-191's role on the call leg

ENG-191 binds HTTP method and request URI into the canonical string to prevent **cross-endpoint replay**: a captured `POST /provision` signature must not verify when replayed against `POST /deprovision`, `POST /restart`, `POST /update`, or any other endpoint. All contract endpoints on one backend use that backend's `callback_secret`. Without method/URI binding, anyone who obtains a legitimate signed request could replay it against a different endpoint on that backend. Production TLS limits passive capture, but it does not make weakening the application-layer signature contract safe, and development deployments may still use plaintext.

ENG-191 is doing real work. Do not propose reverting it as a "simpler" fix for the ENG-198 symptom; the cost of reverting is reopening a materially exploitable cross-endpoint replay.

Production gives every backend a distinct bidirectional key. Configure it as
`providerd`'s `backends[].hmac_secret` and as that backend process's existing
`callback_secret`. Provider startup binds each configured backend name to its
prepared immutable storage UUID. Inbound callback JSON carries that UUID in the
HMAC-covered `backend_storage_id`; Fred treats it only as a key selector until
the signature verifies, then checks the same signed identity against durable
operation/lifecycle authority. Missing, malformed, unknown, duplicate, or
case-ambiguous selectors fail closed. A key for backend A cannot authenticate a
callback selecting backend B or a provider command sent to B.

The key is intentionally bidirectional within one backend trust boundary. A
process that has compromised backend A can forge the provider side of A's own
channel, but it already controls A's substrate. Direction-separated keys would
only add protection if the sender and receiver inside that one node must be
separate compromise domains; they do not improve cross-backend isolation.
Provider-level `callback_secret` is retained only as an explicit
non-production compatibility mode. Mixed and partial configurations are always
rejected; production also rejects the global mode.

The timestamp is a freshness bound, not a nonce. An identical signed request
can be replayed against the same method and URI for up to five minutes; method
and URI binding prevent moving it to a different endpoint or operation. Durable
callback retry requires this behavior, while typed settlement and idempotent
application handle duplicate delivery.

### Stopped key migration

There is no safe rolling interval between a fleet-wide key and per-backend
keys. Stop providerd and all backends, configure each upgraded backend with its
unique `callback_secret=K_i`, then start the backends while providerd remains
stopped. Configure the matching providerd entry as
`backends[].hmac_secret=K_i` and remove the top-level key. With the backends
available, run the stopped placement classifier/preflight, then start
providerd. See DEPLOYMENT.md for the complete cutover checklist.

## 4. Deploy uses a path-stripping reverse proxy

The production deploy fronts fred with Traefik `stripPrefix` middleware that maps the public path space `/api/fred/*` to fred's bare path space `/*`. Barney's SPA depends on the same routing, so the strip rule is not optional.

Concrete consequence after ENG-191:

| Side | URI present in HMAC canonical string |
| -- | -- |
| docker-backend (signer) — builds URL from `callback_url` | `/api/fred/callbacks/provision` |
| fred-providerd (verifier) — reads `r.URL.RequestURI()` post-strip | `/callbacks/provision` |

The canonical strings disagree, so HMAC verification fails with `signature mismatch` → 401. Pre-ENG-191 this rewrite was invisible (URI wasn't in the canonical string); post-ENG-191 it 401s every callback. Fred → backend is unaffected because that leg goes direct.

## 5. Resolution = static prefix config

Fred grows a `callback_canonical_path_prefix` config field. The verifier prepends it to `r.URL.RequestURI()` before computing the canonical string. Empty (the default) is a no-op and preserves byte-identical behaviour on direct-call deploys (e.g., load tests, dev environments).

Validation rules:
- Empty is legal (default — no prepend).
- Non-empty: must start with `/`, must not end with `/`. Keeps the join `prefix + r.URL.RequestURI()` simple — exactly one slash at the seam.
- The value must equal the normalized `EscapedPath` of `callback_base_url`
  byte-for-byte. For example, a Unicode base path is compared in its canonical
  percent-encoded wire form, not as decoded text. A root callback URL requires
  an empty prefix.

Callback URL construction and replay preserve an accepted `RawQuery` exactly
because those bytes are HMAC-covered. Raw spaces, non-ASCII bytes, malformed
percent escapes, and other request-target-unstable bytes are rejected; their
percent-encoded forms remain valid. Complete destinations must use a canonical
path ending in `/callbacks/provision`, with no dot segments, encoded separators,
userinfo, fragment, empty query marker, or unusable authority/port.

**Single source of truth.** The deploy is configured so that the path in
fred's `callback_base_url`, `callback_canonical_path_prefix`, and Traefik's
`stripPrefix` middleware definition are rendered from the *same Ansible
variable* (see manifest-deploy). Fred also rejects startup unless the first two
normalize to the same escaped bytes. This is the invariant the next engineer
must preserve.

### Rejected alternatives (do not relitigate)

- **Read `X-Forwarded-Prefix` from a trusted proxy.** Three failure modes (header present + trusted, header present + untrusted, header missing + `production_mode` gating) vs. one (config right or wrong). Header trust requires a list of trusted-proxy CIDRs maintained alongside `trusted_proxies` and a non-trivial verification path. Static config is strictly simpler and Ansible already knows the value.
- **Normalize URIs on both sides** (e.g., always strip a known prefix at the signer, or always work in "bare" path space). Couples backend code to the deployment-specific proxy topology and breaks direct-call deploys (loadtest, dev). The signer should sign what it actually sends; the verifier compensates for what the proxy did.
- **Revert ENG-191 / drop URI binding.** Reopens cross-endpoint replay, which is materially exploitable per (2) and (3) above. Not on the table.
- **Rely on TLS and weaken HMAC URI binding.** Production TLS protects the
  transport, but development deployments may use plaintext and application
  authentication remains a separate defense. The HMAC channel must remain
  robust regardless.

## Invariants the next reader must preserve

1. `callback_base_url` path, `callback_canonical_path_prefix`, and the upstream
   proxy's strip rule are sourced from one variable. Startup enforces equality
   between the first two; do not template the proxy rule independently.
2. Empty prefix is a no-op (default). Do not require non-empty in `production_mode` — direct-call deploys are legitimate.
3. The prefix is config, not header-derived. Do not "improve" it by reading `X-Forwarded-Prefix` without first re-reading section (2) above.
4. ENG-191's method + URI binding stays. Any change that drops either field must first address cross-endpoint replay over plain HTTP.
