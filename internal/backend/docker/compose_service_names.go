package docker

import (
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
)

// composeServiceName is the single naming rule used by both admission and
// project construction. A scaled service is expanded into one Compose service
// per instance; an unscaled service keeps its chain-authored name.
func composeServiceName(item backend.LeaseItem, instance int) string {
	if item.Quantity <= 1 {
		return item.ServiceName
	}
	return fmt.Sprintf("%s-%d", item.ServiceName, instance)
}

type composeServiceOrigin struct {
	service  string
	instance int
}

// composeServiceLogicalNames expands every requested instance through the
// canonical Compose naming rule and returns an exact Compose-key-to-logical-
// service lookup. Keeping expansion and collision detection together ensures
// admission, project construction, and PS attribution all use the same proof.
func composeServiceLogicalNames(items []backend.LeaseItem) (map[string]string, error) {
	if _, err := backend.ValidateOperationQuantities(items); err != nil {
		return nil, err
	}

	logicalNames := make(map[string]string)
	origins := make(map[string]composeServiceOrigin)
	for _, item := range items {
		for instance := range item.Quantity {
			name := composeServiceName(item, instance)
			if previous, exists := origins[name]; exists {
				return nil, fmt.Errorf(
					"expanded Compose service name %q collides between %q instance %d and %q instance %d",
					name,
					previous.service,
					previous.instance,
					item.ServiceName,
					instance,
				)
			}
			origins[name] = composeServiceOrigin{service: item.ServiceName, instance: instance}
			logicalNames[name] = item.ServiceName
		}
	}
	return logicalNames, nil
}

// validateComposeServiceNames proves the expansion is injective before a
// durable operation/maintenance intent or any substrate mutation. Without this
// gate, {web x2, web-0 x1} both author the Compose key "web-0" and the project
// map silently replaces one paid instance with another.
func validateComposeServiceNames(items []backend.LeaseItem) error {
	_, err := composeServiceLogicalNames(items)
	return err
}
