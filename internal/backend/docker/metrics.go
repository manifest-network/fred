package docker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/manifest-network/fred/internal/backend/shared"
)

const (
	metricsNamespace = "fred"
	metricsSubsystem = "docker_backend"
)

var (
	// provisionsTotal tracks the total number of provision attempts by outcome.
	provisionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "provisions_total",
		Help:      "Total number of provision attempts",
	}, []string{"outcome"})

	// deprovisionsTotal tracks the total number of deprovision operations.
	deprovisionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "deprovisions_total",
		Help:      "Total number of deprovision operations",
	})

	// activeProvisions tracks the current number of active provisions.
	activeProvisions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "active_provisions",
		Help:      "Current number of active provisions",
	})

	// resourceCPUAllocatedRatio tracks the ratio of allocated to total CPU.
	resourceCPUAllocatedRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "resource_cpu_allocated_ratio",
		Help:      "Ratio of allocated CPU to total CPU",
	})

	// resourceMemoryAllocatedRatio tracks the ratio of allocated to total memory.
	resourceMemoryAllocatedRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "resource_memory_allocated_ratio",
		Help:      "Ratio of allocated memory to total memory",
	})

	// resourceDiskAllocatedRatio tracks the ratio of allocated to total disk.
	resourceDiskAllocatedRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "resource_disk_allocated_ratio",
		Help:      "Ratio of allocated disk to total disk",
	})

	// provisionDurationSeconds tracks the end-to-end provision time.
	provisionDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "provision_duration_seconds",
		Help:      "End-to-end provision duration in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.5, 2, 12), // 0.5s to ~17min
	})

	// callbackDeliveryTotal tracks callback delivery outcomes.
	callbackDeliveryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "callback_delivery_total",
		Help:      "Total number of callback delivery attempts by outcome",
	}, []string{"outcome"})

	// callbackStoreErrorsTotal tracks bbolt persistence failures for callbacks.
	callbackStoreErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "callback_store_errors_total",
		Help:      "Total number of callback persistence failures (bbolt store errors)",
	})

	// imagePullDurationSeconds tracks image pull duration.
	imagePullDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "image_pull_duration_seconds",
		Help:      "Duration of image pull operations in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.5, 2, 12), // 0.5s to ~17min
	})

	// containerCreateDurationSeconds tracks container creation duration.
	containerCreateDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "container_create_duration_seconds",
		Help:      "Duration of container creation operations in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 10), // 0.1s to ~51s
	})

	// reconciliationTotal tracks reconciliation runs by outcome.
	reconciliationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "reconciliation_total",
		Help:      "Total number of reconciliation runs by outcome",
	}, []string{"outcome"})

	// reconcilerLastSuccessTimestamp records the unix timestamp of the last
	// successful reconciliation run for this docker backend.
	reconcilerLastSuccessTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "reconciliation_last_success_timestamp_seconds",
		Help:      "Unix timestamp of the last successful reconciliation run",
	})

	// resourceBandwidthAllocatedRatio tracks the ratio of allocated to total bandwidth.
	resourceBandwidthAllocatedRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "resource_bandwidth_allocated_ratio",
		Help:      "Ratio of allocated bandwidth to total link bandwidth",
	})

	// bandwidthApplyTotal tracks bandwidth limit application attempts.
	bandwidthApplyTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "bandwidth_apply_total",
		Help:      "Total number of bandwidth limit application attempts",
	}, []string{"outcome"})
)

// updateResourceMetrics updates the resource allocation ratio gauges.
func updateResourceMetrics(stats shared.ResourceStats) {
	if stats.TotalCPU > 0 {
		resourceCPUAllocatedRatio.Set(stats.AllocatedCPU / stats.TotalCPU)
	}
	if stats.TotalMemoryMB > 0 {
		resourceMemoryAllocatedRatio.Set(float64(stats.AllocatedMemoryMB) / float64(stats.TotalMemoryMB))
	}
	if stats.TotalDiskMB > 0 {
		resourceDiskAllocatedRatio.Set(float64(stats.AllocatedDiskMB) / float64(stats.TotalDiskMB))
	}
	if stats.TotalBandwidthMbps > 0 {
		resourceBandwidthAllocatedRatio.Set(float64(stats.AllocatedBandwidthMbps) / float64(stats.TotalBandwidthMbps))
	}
}
