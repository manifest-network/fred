//go:build integration

package e2e

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// chain is a stack of services in which each waits for the next to have
// started, so its longest dependency chain has links links.
func chain(links int) Spec {
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatelessSKU}
	services := map[string]any{}
	for i := 0; i <= links; i++ {
		name := fmt.Sprintf("s%02d", i)
		service := sleeper(BusyboxImage)
		if i < links {
			service["depends_on"] = map[string]any{fmt.Sprintf("s%02d", i+1): map[string]any{"condition": "service_started"}}
		}
		services[name] = service
		lease.Services = append(lease.Services, name)
	}
	lease.Manifest = map[string]any{"services": services}
	return lease
}

// TestBackend_ManifestOutsideTheLimitsIsRefused sends manifests that break a
// limit the backend could otherwise only trip over later: when Docker refuses
// the container, or when the stack is started again after a restart. Each
// must be refused as an invalid manifest in the answer to the request, the
// same way when it is sent a second time, and start nothing.
//
//   - A health-check timing Docker would refuse: negative, or under a
//     millisecond (ENG-1127).
//   - A chain of dependencies longer than ten (ENG-1515). A chain of exactly
//     ten is accepted and runs.
//   - A service in several instances whose instance names collide with
//     another service's name: two instances of web are web-0 and web-1.
func TestBackend_ManifestOutsideTheLimitsIsRefused(t *testing.T) {
	// Room for the eleven services of the longest chain that is allowed. A
	// container without a volume is still counted 64 MB of the disk pool, for
	// what it writes to its scratch space.
	b := StartBackendWithConfig(t, map[string]any{"total_cpu_cores": 4.0, "total_disk_mb": 1024})
	timed := func(field, value string) Spec {
		lease := stateless("tenant-a")
		lease.Manifest["health_check"] = map[string]any{"test": []string{"CMD", "true"}, field: value}
		return lease
	}
	cases := []struct {
		name string
		spec Spec
	}{
		{"a health-check interval under a millisecond", timed("interval", "500us")},
		{"a negative health-check timeout", timed("timeout", "-1s")},
		{"a health-check start period under a millisecond", timed("start_period", "999us")},
		{"a dependency chain of eleven", chain(11)},
		{"instances that collide with a service", Spec{
			LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatelessSKU,
			Services: []string{"web", "web-1"}, Quantity: map[string]int{"web": 2},
			Manifest: map[string]any{"services": map[string]any{"web": sleeper(BusyboxImage), "web-1": sleeper(BusyboxImage)}},
		}},
	}
	refused := make([]string, 0, len(cases))
	for _, c := range cases {
		err := b.Client.Apply(t.Context(), c.spec)
		require.Error(t, err, c.name)
		first := requireRefused(t, b, c.spec, err, http.StatusBadRequest, CodeInvalidManifest)
		err = b.Client.Apply(t.Context(), c.spec)
		require.Error(t, err, "%s, sent again", c.name)
		again := requireRefused(t, b, c.spec, err, http.StatusBadRequest, CodeInvalidManifest)
		require.Equal(t, first.Message, again.Message, "%s must be refused the same way every time", c.name)
		refused = append(refused, c.spec.LeaseUUID)
	}
	requireNotListed(t, b, refused...)

	longest := chain(10)
	require.NoError(t, b.Client.Apply(t.Context(), longest), "a dependency chain of ten is within the limit")
	requireStatus(t, b, longest.LeaseUUID, statusReady)
	requireRunningContainers(t, b, longest.LeaseUUID, 11)
}
