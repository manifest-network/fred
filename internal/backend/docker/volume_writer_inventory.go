package docker

import (
	"context"
	"fmt"

	"github.com/docker/docker/api/types/container"
)

// ListVolumeWriters deliberately has no Fred label filter. An unmanaged or
// foreign container with access to a managed volume must remain visible as
// interference. Labels are observations here; exact stop authority requires a
// separate inspected generation match at the launch boundary.
func (d *DockerClient) ListVolumeWriters(ctx context.Context) ([]ContainerInfo, error) {
	all, err := d.client.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("list possible volume writers: %w", err)
	}
	result := make([]ContainerInfo, 0, len(all))
	for _, current := range all {
		info := ContainerInfo{ContainerID: current.ID, Status: current.State}
		for _, mount := range current.Mounts {
			info.Mounts = append(info.Mounts, ContainerMount{
				Source: mount.Source, Target: mount.Destination,
				Type: string(mount.Type), ReadOnly: !mount.RW,
			})
		}
		result = append(result, info)
	}
	return result, nil
}
