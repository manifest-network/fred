package docker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type backendHealthStage string

const (
	healthStageIdentity    backendHealthStage = "storage_identity"
	healthStageDocker      backendHealthStage = "docker_ping"
	healthStageAccounting  backendHealthStage = "resource_accounting"
	healthStageCallbacks   backendHealthStage = "callback_store"
	healthStageDiagnostics backendHealthStage = "diagnostics_store"
	healthStageReleases    backendHealthStage = "release_store"
	healthStageRetentions  backendHealthStage = "retention_store"
	healthStageLaunches    backendHealthStage = "launch_journal"
)

var backendHealthDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: metricsNamespace,
	Subsystem: metricsSubsystem,
	Name:      "health_check_duration_seconds",
	Help:      "Elapsed time of completed Docker backend health stages, including failures and synchronous store waits. A failed stage prevents later stages from running.",
	Buckets:   prometheus.DefBuckets,
}, []string{"check"})

func measureBackendHealth(stage backendHealthStage, probe func() error) error {
	started := time.Now()
	err := probe()
	backendHealthDuration.WithLabelValues(string(stage)).Observe(time.Since(started).Seconds())
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
