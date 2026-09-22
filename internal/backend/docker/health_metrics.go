package docker

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/manifest-network/fred/internal/healthprobe"
)

type backendHealthStage = healthprobe.Stage

const (
	healthStageIdentity    backendHealthStage = healthprobe.StorageIdentity
	healthStageDocker      backendHealthStage = healthprobe.DockerPing
	healthStageAccounting  backendHealthStage = healthprobe.ResourceAccounting
	healthStageCallbacks   backendHealthStage = healthprobe.CallbackStore
	healthStageDiagnostics backendHealthStage = healthprobe.DiagnosticsStore
	healthStageReleases    backendHealthStage = healthprobe.ReleaseStore
	healthStageRetentions  backendHealthStage = healthprobe.RetentionStore
	healthStageLaunches    backendHealthStage = healthprobe.LaunchJournal
)

var backendHealthDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: metricsNamespace,
	Subsystem: metricsSubsystem,
	Name:      "health_check_duration_seconds",
	Help:      "Elapsed time of completed Docker backend health stages, including failures and synchronous store waits. A failed stage prevents later stages from running.",
	Buckets:   prometheus.DefBuckets,
}, []string{"check"})

func measureBackendHealth(ctx context.Context, stage backendHealthStage, probe func() error) error {
	// The stage boundary owns cancellation admission. A canceled request cannot
	// start another synchronous store traversal or publish a skipped timing.
	if err := ctx.Err(); err != nil {
		return err
	}
	started := time.Now()
	err := probe()
	duration := time.Since(started)
	backendHealthDuration.WithLabelValues(string(stage)).Observe(duration.Seconds())
	healthprobe.Record(ctx, stage, duration, err)
	return err
}

type storageIdentityStage string

const (
	identityStageTotal     storageIdentityStage = "total"
	identityStageLock      storageIdentityStage = "lock_wait"
	identityStageSubstrate storageIdentityStage = "substrate"
	identityStageDaemon    storageIdentityStage = "daemon_info"
	identityStageStores    storageIdentityStage = "stores"
)

var storageIdentityDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: metricsNamespace,
	Subsystem: metricsSubsystem,
	Name:      "storage_identity_check_duration_seconds",
	Help:      "Elapsed time of production Docker storage identity verifier invocations, including callbacks and failures. Rejections before invocation are excluded. Total includes the other stages, substrate includes daemon_info, and these overlap the health storage_identity timing.",
	Buckets:   prometheus.DefBuckets,
}, []string{"check"})

func observeStorageIdentityStage(stage storageIdentityStage, started time.Time) {
	storageIdentityDuration.WithLabelValues(string(stage)).Observe(time.Since(started).Seconds())
}
