package imagefetch

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var imageImportTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "fred", Subsystem: "docker_backend", Name: "image_import_total",
	Help: "Dispatched image imports by completion or loader-owned interruption; interruptions are counted before SDK unwind",
}, []string{"outcome"})

var imagePreparationRefusals = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "fred", Subsystem: "docker_backend", Name: "image_preparation_refusals_total",
	Help: "Image preparations refused at the measured import-allocation boundary by bounded reason",
}, []string{"reason"})

var imageAllocationPressure = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "fred", Subsystem: "docker_backend", Name: "image_allocation_pressure_total",
	Help: "Successful new-image preparations using more than 80 percent of their configured import-allocation ceiling",
})

var imageRegistryRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "fred", Subsystem: "docker_backend", Name: "image_registry_requests_total",
	Help: "Registry HTTP exchanges, including retries and redirect hops, by endpoint, method and status class; public registries such as Docker Hub meter manifest GETs as pulls",
}, []string{"endpoint", "method", "status"})

var imageTagResolutions = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "fred", Subsystem: "docker_backend", Name: "image_tag_resolutions_total",
	Help: "Mutable tag resolutions past their HEAD by manifest source: cache (announced digest already verified, no manifest GET), registry (GET of the uncached announced digest), fallback_unsupported or fallback_incomplete (HEAD refused with 405/501 or lacked a usable digest, type or length, so the tag was fetched)",
}, []string{"source"})

// Registry endpoint classes, as counted by the endpoint label.
const (
	endpointManifest = "manifest"
	endpointBlob     = "blob"
	endpointPing     = "ping"
	endpointOther    = "other"
)

// registryEndpoint classifies a registry URL path by its last API marker. A
// repository name may itself contain a manifests or blobs component, but a
// tag or digest cannot contain a slash, so the final marker names the endpoint.
// CDN blob paths such as .../blobs/sha256/... stay blobs.
func registryEndpoint(path string) string {
	manifest, blob := strings.LastIndex(path, "/manifests/"), strings.LastIndex(path, "/blobs/")
	switch {
	case manifest > blob:
		return endpointManifest
	case blob > manifest:
		return endpointBlob
	case path == "/v2/" || path == "/v2":
		return endpointPing
	default:
		return endpointOther
	}
}

// observeRegistryExchange counts one dispatched wire exchange. Every label is
// a bounded class; no registry host, repository, tag or tenant appears.
func observeRegistryExchange(request *http.Request, response *http.Response, err error) {
	statuses := registryRequests.forEndpoint(registryEndpoint(request.URL.Path)).forMethod(request.Method)
	switch {
	case err != nil || response == nil:
		statuses.error.Inc()
	case response.StatusCode == http.StatusTooManyRequests:
		statuses.tooManyRequests.Inc()
	case response.StatusCode < 400:
		statuses.ok.Inc()
	case response.StatusCode < 500:
		statuses.clientError.Inc()
	default:
		statuses.serverError.Inc()
	}
}

// observeImport records exactly one result for a dispatched exchange. Only the
// loader-owned context establishes deadline/shutdown; a daemon's error text or
// wrapped context error cannot forge that classification. Recording at expiry
// makes a stalled SDK visible before its transport releases the live owner.
func observeImport(work context.Context) func(bool) {
	var once sync.Once
	record := func(outcome prometheus.Counter) { once.Do(outcome.Inc) }
	// interruption is the loader-owned interruption of work, if any. A done
	// context's error is always one of the two.
	interruption := func() (prometheus.Counter, bool) {
		switch work.Err() {
		case context.DeadlineExceeded:
			return importOutcomes.deadline, true
		case context.Canceled:
			return importOutcomes.shutdown, true
		default:
			return nil, false
		}
	}
	stop := context.AfterFunc(work, func() {
		if outcome, interrupted := interruption(); interrupted {
			record(outcome)
		}
	})
	return func(success bool) {
		stop()
		outcome, interrupted := interruption()
		switch {
		case interrupted:
		case success:
			outcome = importOutcomes.success
		default:
			outcome = importOutcomes.failure
		}
		record(outcome)
	}
}
