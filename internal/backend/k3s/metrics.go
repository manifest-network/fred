package k3s

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// k3s-backend Prometheus metrics. The names mirror docker-backend's
// fred_docker_backend_* family but under the fred_k3s_backend_*
// namespace so a Prometheus scrape from a host running both backends
// can distinguish them without external labels.
//
// Scope (ENG-133): only the three counters the stub provisioner and
// the callback sender actually touch. Resource gauges (active
// provisions, CPU/memory utilization, lease actor inbox depth) arrive
// in ENG-134+.
var (
	// provisionsTotal counts provision requests by outcome. Label values:
	//   - "accepted": Provision validated the request, recorded the in-memory
	//     entry, and spawned the failure goroutine.
	//   - "rejected": Provision returned an error (validation or
	//     ErrAlreadyProvisioned) before recording any state.
	provisionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fred_k3s_backend_provisions_total",
			Help: "Total provision requests received by the k3s backend, by outcome (accepted|rejected).",
		},
		[]string{"outcome"},
	)

	// callbackDeliveryTotal counts callback delivery attempts by outcome.
	// Label values "success" / "failure" match the strings emitted by
	// shared.CallbackSender.reportDelivery (see shared/callback_sender.go).
	callbackDeliveryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fred_k3s_backend_callback_delivery_total",
			Help: "Total callback delivery attempts by the k3s backend, by outcome (success|failure).",
		},
		[]string{"outcome"},
	)

	// callbackStoreErrorsTotal counts bbolt errors persisting pending
	// callbacks. Any non-zero value indicates the disk-backed durability
	// guarantee is degraded.
	callbackStoreErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "fred_k3s_backend_callback_store_errors_total",
			Help: "Total bbolt errors persisting pending callbacks in the k3s backend.",
		},
	)
)
