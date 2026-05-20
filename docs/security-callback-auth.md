# Callback Authentication — Threat Model & Design Rationale

Reference-grade document for engineers about to touch the fred ↔ backend HMAC channel, the `callback_canonical_path_prefix` config, or the upstream proxy strip rule. Read this before "improving" any of them.

Cross-links: [ENG-191 (PR #88)](https://github.com/manifest-network/fred/pull/88) bound method + URI into the HMAC canonical string. [ENG-198 (this PR)](https://linear.app/liftedinit/issue/ENG-198) adds the static prefix that compensates for the upstream proxy's path rewrite. See also [SECURITY.md § Callback Authentication](../SECURITY.md#callback-authentication-hmac-sha256).

## 1. Two-leg topology asymmetry

Fred ↔ backends is a **bidirectional HMAC channel**, not a one-way webhook fan-out:

| Leg | Direction | Network path |
| -- | -- | -- |
| Fred → backend | Outbound from fred (provision, deprovision, restart, update, ...) | Direct — no proxy in front of the backend |
| Backend → fred | Inbound callbacks (provision result, lease state) | Traverses Traefik on the way to fred |

The signer's view of the URI and the verifier's view of the URI agree on the outbound leg (no rewriter sits between them) but **diverge on the inbound leg** (Traefik strips the path before fred sees it). Any signature-binding change that includes the URI in its canonical string surfaces this asymmetry. ENG-191 was such a change, and ENG-198 is the compensation.

## 2. TLS posture asymmetry per `manifest-deploy/CLAUDE.md:181` / `:195`

In this deployment topology, the fred ↔ backend channel is **plain HTTP (no TLS)** by design — the lines cited are in the `manifest-deploy` repo's `CLAUDE.md`, not this repo's. HMAC is therefore the *only* authenticity mechanism on the wire between fred and backends. An in-zone observer can read every byte. That elevates the impact of any signature-binding weakness from theoretical to materially exploitable, which is the load-bearing reason ENG-191 had to bind method + URI into the canonical string in the first place. Do not reason about this system as if it were TLS-protected.

## 3. ENG-191's role on the call leg

ENG-191 binds HTTP method and request URI into the canonical string to prevent **cross-endpoint replay**: a captured `POST /provision` signature must not verify when replayed against `POST /deprovision`, `POST /restart`, `POST /update`, or any other endpoint. Nine endpoints share one HMAC secret (`callback_secret`). Without method/URI binding, an in-zone observer can flip lease state by replaying a captured legitimate signature against a different endpoint — exploitable, given the plain-HTTP posture in (2).

ENG-191 is doing real work. Do not propose reverting it as a "simpler" fix for the ENG-198 symptom; the cost of reverting is reopening a materially exploitable cross-endpoint replay.

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

**Single source of truth.** The deploy is configured so that fred's `callback_canonical_path_prefix` and Traefik's `stripPrefix` middleware definition are both rendered from the *same Ansible variable* (see manifest-deploy). Drift between the two is mechanically impossible because there is only one input. This is the invariant the next engineer must preserve.

### Rejected alternatives (do not relitigate)

- **Read `X-Forwarded-Prefix` from a trusted proxy.** Three failure modes (header present + trusted, header present + untrusted, header missing + `production_mode` gating) vs. one (config right or wrong). Header trust requires a list of trusted-proxy CIDRs maintained alongside `trusted_proxies` and a non-trivial verification path. Static config is strictly simpler and Ansible already knows the value.
- **Normalize URIs on both sides** (e.g., always strip a known prefix at the signer, or always work in "bare" path space). Couples backend code to the deployment-specific proxy topology and breaks direct-call deploys (loadtest, dev). The signer should sign what it actually sends; the verifier compensates for what the proxy did.
- **Revert ENG-191 / drop URI binding.** Reopens cross-endpoint replay, which is materially exploitable per (2) and (3) above. Not on the table.
- **Switch to TLS between fred and backends to eliminate the in-zone-observer threat.** Out of scope for this fix — would also require changes in `manifest-deploy/CLAUDE.md:181`/`:195`. The HMAC channel must remain robust regardless.

## Invariants the next reader must preserve

1. `callback_canonical_path_prefix` and the upstream proxy's strip rule are sourced from one variable. Do not template them independently.
2. Empty prefix is a no-op (default). Do not require non-empty in `production_mode` — direct-call deploys are legitimate.
3. The prefix is config, not header-derived. Do not "improve" it by reading `X-Forwarded-Prefix` without first re-reading section (2) above.
4. ENG-191's method + URI binding stays. Any change that drops either field must first address cross-endpoint replay over plain HTTP.
