package k3s

import "github.com/prometheus/client_golang/prometheus"

// This file is the vocabulary of every labeled counter k3s-backend exports,
// and the only file in this package that creates a counter series. Each
// series exists at 0 from package initialization, so increase() sees the
// first event after a restart (ENG-1116). Recording code increments these
// handles; it never names a label.

// provisionOutcomeCounters is fred_k3s_backend_provisions_total by outcome.
type provisionOutcomeCounters struct{ accepted, rejected prometheus.Counter }

var provisionOutcomes = provisionOutcomeCounters{ //exhaustruct:enforce
	accepted: provisionsTotal.WithLabelValues("accepted"),
	rejected: provisionsTotal.WithLabelValues("rejected"),
}

// callbackDeliveryCounters is fred_k3s_backend_callback_delivery_total by
// outcome.
type callbackDeliveryCounters struct{ success, failure prometheus.Counter }

var callbackDeliveries = callbackDeliveryCounters{ //exhaustruct:enforce
	success: callbackDeliveryTotal.WithLabelValues("success"),
	failure: callbackDeliveryTotal.WithLabelValues("failure"),
}

// record counts one delivery outcome.
func (c callbackDeliveryCounters) record(delivered bool) {
	if delivered {
		c.success.Inc()
		return
	}
	c.failure.Inc()
}
