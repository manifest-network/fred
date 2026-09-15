package docker

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

type v013ReleaseServiceEvidence struct {
	sku          string
	customDomain string
	indexes      map[int]string
}

// deriveV013ActiveReleaseItems freezes the facts v0.13 persisted for a
// stack-shaped workload. The active manifest supplies the service/image set;
// immutable Docker labels supply SKU, effective domain, and observed indexes.
// The stopped placement preflight independently compares the resulting Items
// with height-pinned chain state before provider authority may start.
func deriveV013ActiveReleaseItems(
	release *shared.Release,
	containers []ContainerInfo,
) ([]backend.LeaseItem, error) {
	if release == nil || release.Status != "active" {
		return nil, errors.New("v0.13 cohort requires an active release")
	}
	if !release.OperationID.IsZero() || len(release.Items) != 0 ||
		len(release.ResourceProfiles) != 0 {
		return nil, errors.New("v0.13 release contains current authority fields")
	}
	if release.Image != "stack" {
		return nil, fmt.Errorf("v0.13 active release has unsupported image class %q", release.Image)
	}
	stack, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return nil, fmt.Errorf("parse v0.13 active release manifest: %w", err)
	}
	if len(containers) == 0 {
		return nil, errors.New("v0.13 active release has no managed container cohort")
	}
	for _, container := range containers {
		if container.ServiceName == "" {
			return nil, fmt.Errorf(
				"%w: managed container %q has no service identity",
				ErrPreStackWorkloadUnsupported,
				container.ContainerID,
			)
		}
	}

	identity := containers[0]
	if !backend.IsCanonicalLeaseUUID(identity.LeaseUUID) ||
		strings.TrimSpace(identity.Tenant) == "" ||
		strings.TrimSpace(identity.ProviderUUID) == "" {
		return nil, fmt.Errorf(
			"managed container %q has incomplete lease, tenant, or provider identity",
			identity.ContainerID,
		)
	}
	services := make(map[string]*v013ReleaseServiceEvidence, len(stack.Services))
	for _, container := range containers {
		if container.LeaseUUID != identity.LeaseUUID ||
			container.Tenant != identity.Tenant ||
			container.ProviderUUID != identity.ProviderUUID {
			return nil, fmt.Errorf(
				"managed container %q has divergent lease, tenant, or provider identity",
				container.ContainerID,
			)
		}
		serviceName := container.ServiceName
		service, exists := stack.Services[serviceName]
		if !exists || service == nil {
			return nil, fmt.Errorf(
				"managed container %q names service %q absent from active manifest",
				container.ContainerID,
				serviceName,
			)
		}
		if container.Image != service.Image {
			return nil, fmt.Errorf(
				"managed container %q image %q differs from active manifest image %q",
				container.ContainerID,
				container.Image,
				service.Image,
			)
		}
		if strings.TrimSpace(container.SKU) == "" {
			return nil, fmt.Errorf("managed container %q has an empty SKU", container.ContainerID)
		}
		if container.InstanceIndex < 0 {
			return nil, fmt.Errorf(
				"managed container %q has negative instance index %d",
				container.ContainerID,
				container.InstanceIndex,
			)
		}
		evidence := services[serviceName]
		if evidence == nil {
			evidence = &v013ReleaseServiceEvidence{
				sku: container.SKU, customDomain: container.CustomDomain,
				indexes: make(map[int]string),
			}
			services[serviceName] = evidence
		}
		if evidence.sku != container.SKU {
			return nil, fmt.Errorf("managed service %q has divergent SKU labels", serviceName)
		}
		if evidence.customDomain != container.CustomDomain {
			return nil, fmt.Errorf("managed service %q has divergent custom-domain labels", serviceName)
		}
		if prior, duplicate := evidence.indexes[container.InstanceIndex]; duplicate {
			return nil, fmt.Errorf(
				"managed service %q has duplicate instance index %d on containers %q and %q",
				serviceName,
				container.InstanceIndex,
				prior,
				container.ContainerID,
			)
		}
		evidence.indexes[container.InstanceIndex] = container.ContainerID
	}

	items := make([]backend.LeaseItem, 0, len(stack.Services))
	for _, serviceName := range slices.Sorted(maps.Keys(stack.Services)) {
		evidence := services[serviceName]
		if evidence == nil {
			return nil, fmt.Errorf("active manifest service %q has no managed containers", serviceName)
		}
		for index := range len(evidence.indexes) {
			if _, exists := evidence.indexes[index]; !exists {
				return nil, fmt.Errorf(
					"managed service %q has a sparse instance cohort; index %d is absent",
					serviceName,
					index,
				)
			}
		}
		items = append(items, backend.LeaseItem{
			SKU: evidence.sku, Quantity: len(evidence.indexes),
			ServiceName: serviceName, CustomDomain: evidence.customDomain,
		})
	}
	if _, err := backend.ValidateOperationQuantities(items); err != nil {
		return nil, fmt.Errorf("validate observed v0.13 cohort quantities: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, items); err != nil {
		return nil, fmt.Errorf("validate observed v0.13 cohort topology: %w", err)
	}
	return items, nil
}
