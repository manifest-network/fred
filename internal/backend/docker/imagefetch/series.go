package imagefetch

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

// This file is the vocabulary of every labeled counter this package exports,
// and the only file in it that creates a counter series. Each series exists at
// 0 from package initialization, so increase() sees the first event after a
// restart (ENG-1116). Recording code increments these handles; it never names
// a label.

// importOutcomeCounters is image_import_total by outcome.
type importOutcomeCounters struct{ success, failure, deadline, shutdown prometheus.Counter }

var importOutcomes = importOutcomeCounters{ //exhaustruct:enforce
	success:  imageImportTotal.WithLabelValues("success"),
	failure:  imageImportTotal.WithLabelValues("failure"),
	deadline: imageImportTotal.WithLabelValues("deadline"),
	shutdown: imageImportTotal.WithLabelValues("shutdown"),
}

// importAllocationRefusals is image_preparation_refusals_total for its one
// reason.
var importAllocationRefusals = imagePreparationRefusals.WithLabelValues("import_allocation")

// tagSource is where a mutable tag's manifest came from past its HEAD, with
// its image_tag_resolutions_total series.
type tagSource struct{ resolved prometheus.Counter }

// Tag resolution sources. Each fallback names why the HEAD could not select
// verified bytes; both cost one GET of the tag.
var (
	tagFromCache           = bindTagSource("cache")
	tagFromRegistry        = bindTagSource("registry")
	tagFallbackUnsupported = bindTagSource("fallback_unsupported")
	tagFallbackIncomplete  = bindTagSource("fallback_incomplete")
)

// noTagFallback is the HEAD having selected verified bytes.
var noTagFallback tagSource

func bindTagSource(label string) tagSource {
	return tagSource{resolved: imageTagResolutions.WithLabelValues(label)}
}

// registryStatusCounters is image_registry_requests_total for one endpoint
// and method, by status class.
type registryStatusCounters struct {
	ok, clientError, tooManyRequests, serverError, error prometheus.Counter
}

// registryMethodCounters is image_registry_requests_total for one endpoint,
// by method.
type registryMethodCounters struct{ get, head, other registryStatusCounters }

// registryRequestCounters is image_registry_requests_total by endpoint class.
type registryRequestCounters struct{ manifest, blob, ping, other registryMethodCounters }

var registryRequests = registryRequestCounters{ //exhaustruct:enforce
	manifest: bindRegistryMethods(endpointManifest),
	blob:     bindRegistryMethods(endpointBlob),
	ping:     bindRegistryMethods(endpointPing),
	other:    bindRegistryMethods(endpointOther),
}

func bindRegistryMethods(endpoint string) registryMethodCounters {
	statuses := func(method string) registryStatusCounters {
		status := func(status string) prometheus.Counter {
			return imageRegistryRequests.WithLabelValues(endpoint, method, status)
		}
		return registryStatusCounters{ //exhaustruct:enforce
			ok: status("ok"), clientError: status("4xx"), tooManyRequests: status("429"),
			serverError: status("5xx"), error: status("error"),
		}
	}
	return registryMethodCounters{ //exhaustruct:enforce
		get: statuses(http.MethodGet), head: statuses(http.MethodHead), other: statuses("other"),
	}
}

// forEndpoint is the series of a registryEndpoint class; any other class
// counts as other.
func (c registryRequestCounters) forEndpoint(endpoint string) registryMethodCounters {
	switch endpoint {
	case endpointManifest:
		return c.manifest
	case endpointBlob:
		return c.blob
	case endpointPing:
		return c.ping
	default:
		return c.other
	}
}

// forMethod is the series of a request method; any method but GET and HEAD
// counts as other.
func (c registryMethodCounters) forMethod(method string) registryStatusCounters {
	switch method {
	case http.MethodGet:
		return c.get
	case http.MethodHead:
		return c.head
	default:
		return c.other
	}
}
