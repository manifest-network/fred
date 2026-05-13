// Package k3s is the scaffold for Fred's K3s backend.
//
// ENG-133 stands up the wiring: a YAML config schema, an HTTP server
// matching docker-backend's contract, HMAC-signed callbacks, and a
// /health probe that round-trips to the configured cluster via
// k8s.io/client-go's Discovery().ServerVersion(). The provisioner is
// a stub that records a "provisioning" entry then posts a
// status=failed, error="not implemented" callback — substantive
// Pod/Deployment/Service/Namespace/Ingress logic is deferred to
// child issues ENG-134..ENG-147.
//
// # State of this package today (ENG-133 in progress)
//
//   - config.go    — Config schema + Validate (T1)
//   - backend.go   — Backend struct + lifecycle (T2)
//   - kubeclient.go, health.go — lazy clientset + /health (T3)
//   - provision_stub.go, metrics.go — stub provisioner (T4)
//
// All substrate-agnostic infrastructure (callback delivery, bbolt
// persistence, diagnostics, release history, resource pool) lives
// in internal/backend/shared and is consumed here by import, not
// by copy.
package k3s
