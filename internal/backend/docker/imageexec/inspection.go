package imageexec

import (
	"context"

	"github.com/docker/docker/api/types/container"
)

// InspectionCreator creates only inert image views. It accepts no container or
// mount configuration: every image VOLUME is covered before Create, preventing
// anonymous host-volume population. It exposes no Start operation. The caller
// still owns unpack allocation and the durable helper receipt through its
// storage mutation protocol.
type InspectionCreator struct{ creator *DockerCreator }

// ForInspection projects the general image-bound creator into a constructor
// whose entire configuration is owned by this package.
func (c *DockerCreator) ForInspection() *InspectionCreator {
	return &InspectionCreator{creator: c}
}

// Create constructs a never-started view of the admitted image. Docker's
// archive API reads beneath inert tmpfs declarations, while Create recognizes
// them as volume overrides and performs no volume copy. WorkingDir is neutral
// so image configuration cannot trigger directory creation or copy-up.
func (c *InspectionCreator) Create(ctx context.Context, image Image, labels map[string]string, name string) (container.CreateResponse, error) {
	if c == nil || c.creator == nil {
		return container.CreateResponse{}, ErrUnavailable
	}
	if err := c.creator.ValidateImage(image); err != nil {
		return container.CreateResponse{}, err
	}
	config := &container.Config{
		Labels: labels, WorkingDir: "/", User: "0", NetworkDisabled: true,
		Entrypoint:  []string{"/__fred_image_probe_never_started__"},
		Cmd:         []string{"--never-start"},
		Healthcheck: &container.HealthConfig{Test: []string{"NONE"}},
	}
	tmpfs := make(map[string]string, len(image.Volumes()))
	for _, target := range image.Volumes() {
		tmpfs[target] = "rw,noexec,nosuid,nodev,size=1m"
	}
	host := &container.HostConfig{
		ReadonlyRootfs: true, NetworkMode: "none", Tmpfs: tmpfs,
		CapDrop:       []string{"ALL"},
		SecurityOpt:   []string{"no-new-privileges:true"},
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
	}
	return c.creator.Create(ctx, image, config, host, nil, name)
}
