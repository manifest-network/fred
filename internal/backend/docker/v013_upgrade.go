package docker

import (
	"errors"
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
