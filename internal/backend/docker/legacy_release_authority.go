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

type legacyReleaseServiceEvidence struct {
	sku          string
	customDomain string
	indexes      map[int]string
}

type v013MigrationCrashClass uint8

const (
	v013MigrationCrashNone v013MigrationCrashClass = iota
	// Compose Up committed, but v0.13 crashed before RecordMigration. The full
	// old rollback cohort is still the exact migration input, so the upgraded
	// runtime can safely replay the idempotent migration.
	v013MigrationCrashBeforeRelease
	// RecordMigration committed, but v0.13 stopped before its background
	// rollback cleanup completed. The stack cohort is live authority; rollback
	// remnants are cleanup evidence only and must never be replanned as desired.
	v013MigrationCrashAfterRelease
)

// deriveLegacyActiveReleaseItems turns the facts v0.13 did persist into the
// strongest local desired-topology evidence available before sealing: the
// active manifest supplies the exact service/image set, while immutable Docker
// labels supply each service's SKU, effective custom domain, and observed
// instance indexes. It deliberately cannot prove that the highest observed
// index was the originally requested quantity. With providerd still stopped,
// the mandatory placement preflight later compares the recovered backend Items
// with height-pinned chain items before provider authority may start.
func deriveLegacyActiveReleaseItems(
	release *shared.Release,
	containers []ContainerInfo,
) ([]backend.LeaseItem, error) {
	if release == nil || release.Status != "active" {
		return nil, errors.New("legacy cohort requires an active release")
	}
	if release.OperationID != "" || len(release.Items) != 0 ||
		len(release.ResourceProfiles) != 0 || release.LegacyMigration {
		return nil, errors.New("legacy cohort release contains current authority fields")
	}
	stack, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return nil, fmt.Errorf("parse legacy active release manifest: %w", err)
	}
	if len(containers) == 0 {
		return nil, errors.New("legacy active release has no managed container cohort")
	}
	hasLegacyLabels := false
	hasStackLabels := false
	for _, container := range containers {
		if container.ServiceName == "" {
			hasLegacyLabels = true
		} else {
			hasStackLabels = true
		}
	}
	if hasLegacyLabels && hasStackLabels {
		class, items, inspectErr := inspectV013MigrationCrashCohort(release, containers)
		if inspectErr != nil {
			return nil, inspectErr
		}
		if class == v013MigrationCrashNone {
			return nil, errors.New("managed cohort mixes legacy and stack service labels")
		}
		return items, nil
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
	services := make(map[string]*legacyReleaseServiceEvidence, len(stack.Services))
	legacyServiceEncoding := containers[0].ServiceName == ""
	if release.Image != "stack" {
		legacyService := stack.Services[manifest.DefaultServiceName]
		if !legacyServiceEncoding || len(stack.Services) != 1 || legacyService == nil ||
			release.Image != legacyService.Image {
			return nil, fmt.Errorf("legacy active release has unsupported image class %q", release.Image)
		}
	}
	for _, container := range containers {
		if container.LeaseUUID != identity.LeaseUUID ||
			container.Tenant != identity.Tenant ||
			container.ProviderUUID != identity.ProviderUUID {
			return nil, fmt.Errorf(
				"managed container %q has divergent lease, tenant, or provider identity",
				container.ContainerID,
			)
		}
		if (container.ServiceName == "") != legacyServiceEncoding {
			return nil, errors.New("managed cohort mixes legacy and stack service labels")
		}
		serviceName := container.ServiceName
		if serviceName == "" {
			serviceName = manifest.DefaultServiceName
		}
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
			evidence = &legacyReleaseServiceEvidence{
				sku:          container.SKU,
				customDomain: container.CustomDomain,
				indexes:      make(map[int]string),
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
			SKU:          evidence.sku,
			Quantity:     len(evidence.indexes),
			ServiceName:  serviceName,
			CustomDomain: evidence.customDomain,
		})
	}
	if _, err := backend.ValidateOperationQuantities(items); err != nil {
		return nil, fmt.Errorf("validate observed legacy cohort quantities: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, items); err != nil {
		return nil, fmt.Errorf("validate observed legacy cohort topology: %w", err)
	}
	return items, nil
}

// inspectV013MigrationCrashCohort recognizes only the two stopped-Docker
// shapes produced around v0.13's Compose-Up/RecordMigration boundary. It never
// infers desired quantity from rollback remnants after the release commit:
// those may be a partially cleaned subset. Before the commit, by contrast, the
// complete dense rollback cohort is the input that makes migration replay safe.
func inspectV013MigrationCrashCohort(
	release *shared.Release,
	containers []ContainerInfo,
) (v013MigrationCrashClass, []backend.LeaseItem, error) {
	stackContainers := make([]ContainerInfo, 0, len(containers))
	prevContainers := make([]ContainerInfo, 0, len(containers))
	for _, container := range containers {
		if container.ServiceName != "" {
			stackContainers = append(stackContainers, container)
			continue
		}
		prevContainers = append(prevContainers, container)
	}
	if len(stackContainers) == 0 || len(prevContainers) == 0 {
		return v013MigrationCrashNone, nil, nil
	}
	for _, container := range prevContainers {
		if !isLegacyRollbackRemnant(container) {
			return v013MigrationCrashNone, nil, fmt.Errorf(
				"mixed managed cohort contains legacy container %q that is not an exact v0.13 rollback name",
				container.Name,
			)
		}
		if !isDockerNonRunningStatus(container.Status) {
			return v013MigrationCrashNone, nil, fmt.Errorf(
				"v0.13 rollback container %q is not stopped (state %q)",
				container.ContainerID,
				container.Status,
			)
		}
	}

	stackContainers, _, err := hydrateV013MigrationStackAuthority(
		stackContainers,
		prevContainers,
	)
	if err != nil {
		return v013MigrationCrashNone, nil, err
	}
	stackRelease := *release
	stackRelease.Image = "stack"
	stackItems, err := deriveLegacyActiveReleaseItems(&stackRelease, stackContainers)
	if err != nil {
		return v013MigrationCrashNone, nil, fmt.Errorf(
			"validate live stack side of interrupted v0.13 migration: %w",
			err,
		)
	}
	if len(stackItems) != 1 || stackItems[0].ServiceName != manifest.DefaultServiceName {
		return v013MigrationCrashNone, nil, errors.New(
			"interrupted v0.13 migration must have exactly the synthetic app service",
		)
	}

	if release.Image != "stack" {
		prevItems, deriveErr := deriveLegacyActiveReleaseItems(release, prevContainers)
		if deriveErr != nil {
			return v013MigrationCrashNone, nil, fmt.Errorf(
				"validate rollback side of pre-RecordMigration v0.13 cohort: %w",
				deriveErr,
			)
		}
		if len(prevItems) != 1 || !sameInterruptedMigrationItem(prevItems[0], stackItems[0]) {
			return v013MigrationCrashNone, nil, errors.New(
				"pre-RecordMigration v0.13 stack and rollback cohorts differ",
			)
		}
		if stackContainers[0].LeaseUUID != prevContainers[0].LeaseUUID ||
			stackContainers[0].Tenant != prevContainers[0].Tenant ||
			stackContainers[0].ProviderUUID != prevContainers[0].ProviderUUID {
			return v013MigrationCrashNone, nil, errors.New(
				"pre-RecordMigration v0.13 stack and rollback cohorts have divergent identity",
			)
		}
		// v0.13 accidentally omitted CustomDomain while constructing the new
		// migration Item. The untouched rollback labels retain the stronger
		// effective-domain evidence, which current replay now preserves.
		if stackItems[0].CustomDomain != "" &&
			stackItems[0].CustomDomain != prevItems[0].CustomDomain {
			return v013MigrationCrashNone, nil, errors.New(
				"pre-RecordMigration v0.13 stack has divergent custom-domain evidence",
			)
		}
		return v013MigrationCrashBeforeRelease, prevItems, nil
	}

	// A committed v0.13 migration can leave only a subset of rollback
	// containers because cleanup removed them one by one. Prove every survivor
	// belongs to the exact live app cohort, but never use subset cardinality as
	// desired authority.
	live := stackContainers[0]
	item := stackItems[0]
	seenIndexes := make(map[int]string, len(prevContainers))
	service, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return v013MigrationCrashNone, nil, fmt.Errorf("parse migrated active manifest: %w", err)
	}
	app := service.Services[manifest.DefaultServiceName]
	if app == nil || len(service.Services) != 1 {
		return v013MigrationCrashNone, nil, errors.New("migrated active release is not a one-service app stack")
	}
	for _, container := range prevContainers {
		if container.LeaseUUID != live.LeaseUUID || container.Tenant != live.Tenant ||
			container.ProviderUUID != live.ProviderUUID || container.SKU != item.SKU ||
			container.Image != app.Image {
			return v013MigrationCrashNone, nil, fmt.Errorf(
				"rollback remnant %q differs from the committed live migration identity",
				container.ContainerID,
			)
		}
		if container.CustomDomain != "" && item.CustomDomain != "" &&
			container.CustomDomain != item.CustomDomain {
			return v013MigrationCrashNone, nil, fmt.Errorf(
				"rollback remnant %q has divergent custom-domain evidence",
				container.ContainerID,
			)
		}
		if container.InstanceIndex < 0 || container.InstanceIndex >= item.Quantity {
			return v013MigrationCrashNone, nil, fmt.Errorf(
				"rollback remnant %q index %d is outside committed live quantity %d",
				container.ContainerID,
				container.InstanceIndex,
				item.Quantity,
			)
		}
		if prior, duplicate := seenIndexes[container.InstanceIndex]; duplicate {
			return v013MigrationCrashNone, nil, fmt.Errorf(
				"rollback remnants %q and %q duplicate index %d",
				prior,
				container.ContainerID,
				container.InstanceIndex,
			)
		}
		seenIndexes[container.InstanceIndex] = container.ContainerID
	}
	return v013MigrationCrashAfterRelease, stackItems, nil
}

// hydrateV013MigrationStackAuthority restores only the in-memory provider field
// v0.13's migration-generated Compose labels omitted. The exact rollback cohort
// remains the authority: this helper never persists a label, and a partial
// omission or mixed current/v0.13 stack generation is rejected. Callback and
// backend authority deliberately remain absent; preflight accepts that shape
// only before RecordMigration, where upgraded migration replay replaces the
// whole stack generation from the rollback cohort.
func hydrateV013MigrationStackAuthority(
	stackContainers []ContainerInfo,
	prevContainers []ContainerInfo,
) ([]ContainerInfo, bool, error) {
	if len(stackContainers) == 0 || len(prevContainers) == 0 {
		return nil, false, errors.New("interrupted v0.13 migration requires both stack and rollback cohorts")
	}
	authority := prevContainers[0]
	if !backend.IsCanonicalLeaseUUID(authority.LeaseUUID) ||
		strings.TrimSpace(authority.Tenant) == "" ||
		strings.TrimSpace(authority.ProviderUUID) == "" {
		return nil, false, errors.New("v0.13 rollback cohort has incomplete identity authority")
	}
	hydrated := slices.Clone(stackContainers)
	provisionalCount := 0
	for index := range hydrated {
		container := &hydrated[index]
		provisional := isV013MigrationGeneratedStackContainer(*container)
		if provisional {
			expectedName := fmt.Sprintf(
				"fred-%s-%s-%d",
				container.LeaseUUID,
				manifest.DefaultServiceName,
				container.InstanceIndex,
			)
			if container.Name != expectedName {
				return nil, false, fmt.Errorf(
					"v0.13 migration-generated stack container %q has name %q, expected exact writer name %q",
					container.ContainerID,
					container.Name,
					expectedName,
				)
			}
			provisionalCount++
			container.ProviderUUID = authority.ProviderUUID
		}
	}
	if provisionalCount != 0 && provisionalCount != len(hydrated) {
		return nil, false, errors.New(
			"interrupted v0.13 migration mixes migration-generated and current stack authority labels",
		)
	}
	return hydrated, provisionalCount != 0, nil
}

func isV013MigrationGeneratedStackContainer(container ContainerInfo) bool {
	return container.ServiceName == manifest.DefaultServiceName &&
		container.BackendName == "" &&
		container.ProviderUUID == "" &&
		container.CallbackURL == "" &&
		container.LifecycleCallbackURL == ""
}

func containsV013MigrationGeneratedStack(containers []ContainerInfo) bool {
	return slices.ContainsFunc(containers, isV013MigrationGeneratedStackContainer)
}

func sameInterruptedMigrationItem(left, right backend.LeaseItem) bool {
	return left.SKU == right.SKU && left.Quantity == right.Quantity &&
		left.ServiceName == right.ServiceName
}

func isDockerNonRunningStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "created", "exited", "dead":
		return true
	default:
		return false
	}
}
