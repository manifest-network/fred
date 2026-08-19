package provisioner

// export_test.go holds scaffolding that exists ONLY for this package's
// tests. It compiles into the test binary and never into providerd,
// which is the point: provisioner's production files must not carry code
// whose only caller is a test (ENG-354).

import (
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
)

// TrackInFlightWithStartTime records an in-flight provision with a
// caller-supplied start time so timeout tests can simulate a provision
// that began in the past. Production always stamps time.Now() via
// TrackInFlight.
func (t *DefaultInFlightTracker) TrackInFlightWithStartTime(leaseUUID, tenant string, items []backend.LeaseItem, backendName string, startTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight[leaseUUID] = InFlightProvision{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		Items:     items,
		Backend:   backendName,
		StartTime: startTime,
	}
	metrics.InFlightProvisions.Set(float64(len(t.inFlight)))
}
