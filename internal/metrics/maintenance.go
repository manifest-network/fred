package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Maintenance pending metrics describe durable unresolved work, including
// commands preserved from an older release. Phase labels are a closed set.
var (
	MaintenancePending                = promauto.NewGaugeVec(prometheus.GaugeOpts{Namespace: namespace, Subsystem: "maintenance", Name: "pending", Help: "Durable unresolved maintenance commands by phase"}, []string{"phase"})
	MaintenancePendingBytes           = promauto.NewGaugeVec(prometheus.GaugeOpts{Namespace: namespace, Subsystem: "maintenance", Name: "pending_bytes", Help: "Encoded durable unresolved maintenance bytes by phase"}, []string{"phase"})
	MaintenancePendingOldestAge       = promauto.NewGaugeVec(prometheus.GaugeOpts{Namespace: namespace, Subsystem: "maintenance", Name: "pending_oldest_age_seconds", Help: "Age of the oldest durable unresolved maintenance command by phase"}, []string{"phase"})
	MaintenanceAdmissionRefusalsTotal = promauto.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Subsystem: "maintenance", Name: "admission_refusals_total", Help: "Fresh maintenance commands refused by aggregate pending budget"}, []string{"reason"})
)
