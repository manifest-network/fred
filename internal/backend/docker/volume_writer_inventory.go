package docker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/docker/docker/api/types/container"

	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
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
	volumeSources := make(map[[2]string]string)
containers:
	for _, current := range all {
		info := ContainerInfo{ContainerID: current.ID, Status: current.State}
		for _, mount := range current.Mounts {
			if mount.Type == "volume" && mount.RW && mount.Source == "" {
				// A plugin's missing cached path is not proof that it cannot alias
				// our storage. Recover its host mountpoint from the exact volume;
				// retain uncertainty if the daemon still cannot identify it.
				key := [2]string{mount.Name, mount.Driver}
				source, ok := volumeSources[key]
				if !ok {
					if mount.Name == "" {
						return nil, fmt.Errorf("container %q has an unnamed writable volume", current.ID)
					}
					volume, err := d.client.VolumeInspect(ctx, mount.Name)
					if err != nil {
						// The independent list and volume read may straddle another
						// lease's close. Only the configured daemon's exact immutable
						// container observation can retire this stale writer candidate;
						// a volume error alone says nothing about its access to storage.
						observed, inspectErr := d.inspectInstance(ctx, current.ID)
						if inspectErr == nil && observed != nil && observed.Phase == leasesm.PhaseAbsent {
							continue containers
						}
						return nil, fmt.Errorf("container %q writable volume %q remains unresolved: %w",
							current.ID, mount.Name, errors.Join(err, inspectErr, ctx.Err()))
					}
					if volume.Name != mount.Name || volume.Driver != mount.Driver || !filepath.IsAbs(volume.Mountpoint) {
						return nil, fmt.Errorf("writable volume %q has no verified host mountpoint", mount.Name)
					}
					source = volume.Mountpoint
					volumeSources[key] = source
				}
				mount.Source = source
			}
			info.Mounts = append(info.Mounts, ContainerMount{
				Source: mount.Source, Target: mount.Destination,
				Type: string(mount.Type), ReadOnly: !mount.RW,
			})
		}
		result = append(result, info)
	}
	return result, nil
}
