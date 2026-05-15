// Package k3s is the scaffold for Fred's K3s backend.
//
// ENG-133 stands up the wiring: a YAML config schema, an HTTP server
// matching docker-backend's URL/auth/status-code contract, HMAC-signed
// callbacks, and a /health probe that round-trips to the configured
// cluster via k8s.io/client-go's Discovery().ServerVersion(). The
// provisioner is a stub that records a "provisioning" entry then posts
// a status=failed, error="not implemented" callback — substantive
// Pod/Deployment/Service/Namespace/Ingress logic is deferred to
// child issues ENG-134..ENG-147.
//
// # Package layout
//
//   - config.go    — Config schema + Validate
//   - backend.go   — Backend struct + lifecycle (New / Start / Stop / Name)
//   - kubeclient.go, health.go — lazy clientset + /health reachability probe
//   - provision_stub.go, metrics.go — stub provisioner + Prometheus counters
//
// All substrate-agnostic infrastructure (callback delivery, bbolt
// persistence, diagnostics, release history, resource pool) lives
// in internal/backend/shared and is consumed here by import, not
// by copy.
//
// # Scaffold limitations (ENG-134+ scope)
//
// "Matching docker-backend's contract" is true at the wire level
// (routes, auth, status codes, HMAC, callback envelope) but NOT at the
// behavioral level. Until ENG-134+ wires the real provisioner, callers
// should be aware of these gaps:
//
//   - Provision validates only that lease_uuid / callback_url / items
//     are present. It does NOT pre-flight SKU items against the
//     configured SKU profiles, so it cannot return synchronous
//     backend.ErrValidation wrapping backend.ErrUnknownSKU for bad
//     requests — the HTTP layer therefore won't return 400
//     validation_code=unknown_sku for a k3s request that docker-backend
//     would reject up front. Every accepted Provision posts the
//     canonical status=failed, error="not implemented" callback.
//
//   - The in-memory provision record stores lifecycle fields
//     (LeaseUUID, Tenant, ProviderUUID, Status, CallbackURL, LastError,
//     FailCount, CreatedAt) but NOT workload metadata (Items, SKU,
//     Image, total Quantity). GetProvision / ListProvisions /
//     LookupProvisions therefore return backend.ProvisionInfo with
//     Quantity=0 and empty Items / SKU / Image / ServiceImages.
//     Downstream consumers that aggregate workload counts (e.g.
//     Fred's /workloads view) will see zero for k3s leases until
//     ENG-134+ wires the real provisioner and the workload-aware
//     record shape.
//
//   - GetInfo / GetLogs / Restart / Update / GetReleases all return
//     backend.ErrNotProvisioned because the stub never reaches a
//     ready/restartable state and never writes release entries.
//
//   - ReconcileCustomDomain is a no-op (returns nil) — the scaffold
//     rejects ingress.enabled=true at config time, so there are no
//     Ingress / Gateway routes to reconcile.
//
// These are intentional gaps in the ENG-133 deliverable, not
// regressions. ENG-134+ closes them as the real provisioner and
// associated record-shape rework land.
package k3s
