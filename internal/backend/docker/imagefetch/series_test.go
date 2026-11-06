package imagefetch

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRegistryRequestSelectionIsTotal pins that every endpoint class and
// method selects its own bound series, and anything else selects other.
func TestRegistryRequestSelectionIsTotal(t *testing.T) {
	for _, endpoint := range []string{endpointManifest, endpointBlob, endpointPing, endpointOther, "unclassified"} {
		label := endpoint
		if endpoint == "unclassified" {
			label = endpointOther
		}
		for method, methodLabel := range map[string]string{
			http.MethodGet: http.MethodGet, http.MethodHead: http.MethodHead, http.MethodPut: "other",
		} {
			assert.Equal(t, imageRegistryRequests.WithLabelValues(label, methodLabel, "5xx"),
				registryRequests.forEndpoint(endpoint).forMethod(method).serverError, "%s %s", endpoint, method)
		}
	}
}
