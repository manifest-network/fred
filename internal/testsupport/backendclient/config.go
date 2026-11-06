package backendclient

import (
	"crypto/tls"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/manifest-network/fred/internal/backend"
)

// Config describes legacy HTTP fixture servers wrapped by this test helper.
type Config struct {
	Name                string
	BaseURL             string
	Timeout             time.Duration
	MaxIdleConns        int // Max idle connections across all hosts (default: 100)
	MaxIdleConnsPerHost int // Max idle connections per host (default: 10)
	Secret              string

	// TLSClientConfig is confined to legacy transport unit fixtures.
	TLSClientConfig *tls.Config

	// Circuit breaker settings
	CBMaxRequests   uint32        // Max requests in half-open state (default: 1)
	CBInterval      time.Duration // Interval to clear counts in closed state (default: 0, never clear)
	CBTimeout       time.Duration // Time to wait before transitioning from open to half-open (default: 60s)
	CBFailureThresh uint32        // Number of failures to trip the breaker (default: 5)

	// Response body size limits (0 = use default). Defense-in-depth caps to prevent
	// a buggy or misrouted backend from pressuring memory with unbounded responses.
	MaxInfoBytes             int64 // GetInfo response limit (default: 1 MiB)
	MaxProvisionBytes        int64 // GetProvision response limit (default: 1 MiB)
	MaxProvisionsBytes       int64 // ListProvisions response limit (default: 8 MiB)
	MaxLookupProvisionsBytes int64 // LookupProvisions response limit (default: 8 MiB)
	MaxLogsBytes             int64 // GetLogs encoded response limit (default: backend.MaxProjectedLogsResponseBytes)
	MaxReleasesBytes         int64 // GetReleases response limit (default: 48 MiB projected response)
	MaxStatsBytes            int64 // GetLoadStats response limit (default: 1 MiB)
	MaxRetentionsBytes       int64 // /retentions per-page response limit (default: 1 MiB)
	ProvisionsPageLimit      int   // /provisions page size requested by the client (default: 1000)
	RetentionsPageLimit      int   // /retentions page size requested by the client (default: 1000)

	// Optional Prometheus metrics. When nil, metric recording is skipped.
	// This prevents binaries that don't use these metrics (e.g., docker-backend)
	// from registering phantom fred-level metrics via transitive imports.
	RequestDuration *prometheus.HistogramVec // labels: backend, operation, status
	// RequestCounters binds the request and malformed-error-body counters of
	// one backend operation.
	RequestCounters     backend.RequestCounterBinder
	CircuitBreakerState *prometheus.GaugeVec // labels: backend
}

func (cfg Config) options() backend.HTTPClientOptions {
	return backend.HTTPClientOptions{
		MaxIdleConns:             cfg.MaxIdleConns,
		MaxIdleConnsPerHost:      cfg.MaxIdleConnsPerHost,
		CBMaxRequests:            cfg.CBMaxRequests,
		CBInterval:               cfg.CBInterval,
		CBTimeout:                cfg.CBTimeout,
		CBFailureThresh:          cfg.CBFailureThresh,
		MaxInfoBytes:             cfg.MaxInfoBytes,
		MaxProvisionBytes:        cfg.MaxProvisionBytes,
		MaxProvisionsBytes:       cfg.MaxProvisionsBytes,
		MaxLookupProvisionsBytes: cfg.MaxLookupProvisionsBytes,
		MaxLogsBytes:             cfg.MaxLogsBytes,
		MaxReleasesBytes:         cfg.MaxReleasesBytes,
		MaxStatsBytes:            cfg.MaxStatsBytes,
		MaxRetentionsBytes:       cfg.MaxRetentionsBytes,
		ProvisionsPageLimit:      cfg.ProvisionsPageLimit,
		RetentionsPageLimit:      cfg.RetentionsPageLimit,
		RequestDuration:          cfg.RequestDuration,
		RequestCounters:          cfg.RequestCounters,
		CircuitBreakerState:      cfg.CircuitBreakerState,
	}
}
