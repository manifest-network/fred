package docker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// compensationStartup retains the frozen container configuration while
// recovering the source manifest's service dependency ordering. All replicas
// of each dependency are started before any dependent service is considered.
type compensationStartup struct {
	containers []compensationContainer
	healthy    map[string][]string
}

func orderCompensationContainers(plan compensationLaunchPlan) (compensationStartup, error) {
	stack, err := manifest.ParsePayload(plan.Source.Manifest)
	if err != nil {
		return compensationStartup{}, err
	}
	byService := make(map[string][]compensationContainer)
	for _, snapshot := range plan.Containers {
		if snapshot.Config == nil {
			return compensationStartup{}, errors.New("source startup requires frozen runtime configuration")
		}
		service := snapshot.Config.Labels[LabelServiceName]
		if stack.Services[service] == nil {
			return compensationStartup{}, errors.New("source startup contains an unexpected service")
		}
		byService[service] = append(byService[service], snapshot)
	}
	result := compensationStartup{healthy: make(map[string][]string)}
	visited, visiting := make(map[string]bool), make(map[string]bool)
	var visit func(string) error
	visit = func(name string) error {
		if visited[name] {
			return nil
		}
		if visiting[name] {
			return errors.New("source startup dependencies contain a cycle")
		}
		spec := stack.Services[name]
		if spec == nil || len(byService[name]) == 0 {
			return fmt.Errorf("source startup lacks dependency %q", name)
		}
		visiting[name] = true
		for _, dependency := range slices.Sorted(maps.Keys(spec.DependsOn)) {
			condition := spec.DependsOn[dependency].Condition
			if condition != "service_started" && condition != "service_healthy" {
				return errors.New("source startup has unsupported dependency condition")
			}
			if err := visit(dependency); err != nil {
				return err
			}
			if condition == "service_healthy" {
				result.healthy[name] = append(result.healthy[name], dependency)
			}
		}
		visiting[name], visited[name] = false, true
		containers := byService[name]
		slices.SortFunc(containers, func(a, b compensationContainer) int {
			return cmp.Compare(a.Name, b.Name)
		})
		result.containers = append(result.containers, containers...)
		return nil
	}
	for _, name := range slices.Sorted(maps.Keys(byService)) {
		if err := visit(name); err != nil {
			return compensationStartup{}, err
		}
	}
	return result, nil
}

func (b *Backend) waitForCompensationDependencies(ctx context.Context, schedule compensationStartup, service string, created map[string][]string, leaseUUID string) error {
	for _, dependency := range schedule.healthy[service] {
		ids := created[dependency]
		if len(ids) == 0 {
			return errors.New("source health dependency lacks its exact created cohort")
		}
		if err := b.waitForHealthy(ctx, ids, physicalLogger(b, leaseUUID)); err != nil {
			return fmt.Errorf("source dependency %q: %w", dependency, err)
		}
	}
	return nil
}
