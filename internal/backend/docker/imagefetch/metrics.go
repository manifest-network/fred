package imagefetch

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var imageImportTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "fred", Subsystem: "docker_backend", Name: "image_import_total",
	Help: "Dispatched image imports by completion or loader-owned interruption; interruptions are counted before SDK unwind",
}, []string{"outcome"})

func init() {
	for _, outcome := range []string{"success", "failure", "deadline", "shutdown"} {
		imageImportTotal.WithLabelValues(outcome).Add(0)
	}
}

// observeImport records exactly one result for a dispatched exchange. Only the
// loader-owned context establishes deadline/shutdown; a daemon's error text or
// wrapped context error cannot forge that classification. Recording at expiry
// makes a stalled SDK visible before its transport releases the live owner.
func observeImport(work context.Context) func(bool) {
	var once sync.Once
	record := func(outcome string) { once.Do(func() { imageImportTotal.WithLabelValues(outcome).Inc() }) }
	interruption := func() string {
		switch work.Err() {
		case context.DeadlineExceeded:
			return "deadline"
		case context.Canceled:
			return "shutdown"
		default:
			return ""
		}
	}
	stop := context.AfterFunc(work, func() { record(interruption()) })
	return func(success bool) {
		stop()
		outcome := interruption()
		if outcome == "" {
			outcome = "failure"
			if success {
				outcome = "success"
			}
		}
		record(outcome)
	}
}
