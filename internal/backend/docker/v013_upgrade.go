package docker

import (
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
)

// ErrPreStackWorkloadUnsupported identifies the old single-service Docker
// layout which predates fred's Compose stack model. Production inventory was
// verified to contain no such workloads before this migration was removed.
// The current binary deliberately refuses the shape instead of inferring
// missing topology or performing an unjournaled in-place conversion.
var ErrPreStackWorkloadUnsupported = errors.New("pre-stack Docker workload is unsupported")

func isPreStackContainer(container ContainerInfo) bool {
	return container.LeaseUUID != "" && container.ServiceName == ""
}

// resolveV013ContainerCallbackURLs freezes the coherent callback pair carried
// by a complete stack-shaped v0.13 cohort. v0.13 did not persist typed runtime
// authority in the release journal, so stopped-upgrade adoption must derive it
// once from every sibling before normal typed operation generations take over.
func resolveV013ContainerCallbackURLs(containers []ContainerInfo) (string, string, error) {
	if len(containers) == 0 {
		return "", "", errors.New("v0.13 callback cohort has no containers")
	}
	callbackURL := containers[0].CallbackURL
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(
		callbackURL, containers[0].LifecycleCallbackURL,
	)
	if err != nil {
		return "", "", fmt.Errorf("instance %d: %w", containers[0].InstanceIndex, err)
	}
	for _, container := range containers[1:] {
		if container.CallbackURL != callbackURL {
			return "", "", fmt.Errorf(
				"instance %d callback_url differs from instance %d",
				container.InstanceIndex,
				containers[0].InstanceIndex,
			)
		}
		resolved, resolveErr := backend.ResolveLifecycleCallbackURL(
			container.CallbackURL,
			container.LifecycleCallbackURL,
		)
		if resolveErr != nil {
			return "", "", fmt.Errorf("instance %d: %w", container.InstanceIndex, resolveErr)
		}
		if resolved != lifecycleCallbackURL {
			return "", "", fmt.Errorf(
				"instance %d lifecycle callback route differs from instance %d",
				container.InstanceIndex,
				containers[0].InstanceIndex,
			)
		}
	}
	return callbackURL, lifecycleCallbackURL, nil
}
