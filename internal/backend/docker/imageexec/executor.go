package imageexec

import (
	"context"
	"fmt"
	"maps"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/containerd/platforms"
	composeapi "github.com/docker/compose/v5/pkg/api"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/util"
)

const (
	LabelImageReference = "fred.image_reference"
	LabelImageID        = "fred.image_id"
)

// DockerSource binds inspection, pulling and creation to one SDK client at the
// composition boundary. The runtime retains all raw capabilities privately.
type DockerSource interface {
	Source
	ContainerCreate(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error)
}

type dockerCreateFunc func(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error)

// DockerCreator is a creation sink which accepts only images admitted by its
// own Admitter. The raw Docker operation never escapes this type.
type DockerCreator struct {
	issuer *issuer
	create dockerCreateFunc
}

// NewDockerRuntime binds admission and direct creation to the same SDK client.
// Callers retain the typed capabilities instead of the raw creation operation.
func NewDockerRuntime(source DockerSource) (*Admitter, *DockerCreator, error) {
	if util.IsNilInterface(source) {
		return nil, nil, ErrUnavailable
	}
	owner := &issuer{source: source}
	return &Admitter{issuer: owner}, &DockerCreator{issuer: owner, create: source.ContainerCreate}, nil
}

// Create assigns execution identity itself; config.Image and image-binding
// labels supplied by the caller cannot override the admitted content.
func (c *DockerCreator) Create(ctx context.Context, image Image, config *container.Config, host *container.HostConfig, networks *network.NetworkingConfig, name string) (container.CreateResponse, error) {
	if c == nil || c.issuer == nil || c.create == nil {
		return container.CreateResponse{}, ErrUnavailable
	}
	if err := image.requireIssuer(c.issuer); err != nil {
		return container.CreateResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return container.CreateResponse{}, err
	}
	var prepared container.Config
	if config != nil {
		prepared = *config
	}
	prepared.Image = image.ID()
	prepared.Labels = maps.Clone(prepared.Labels)
	if prepared.Labels == nil {
		prepared.Labels = make(map[string]string)
	}
	prepared.Labels[LabelImageReference] = image.Reference()
	prepared.Labels[LabelImageID] = image.ID()
	platform := image.Platform()
	return c.create(ctx, &prepared, host, networks, &platform, name)
}

type preparedProjectRecord struct {
	issuer  *issuer
	project *composetypes.Project
}

// PreparedProject is a complete Compose project bound to admitted images.
// Its contents cannot be changed or recovered as a mutable project by callers.
type PreparedProject struct {
	record *preparedProjectRecord
}

// Name returns the project's name, or an empty string for a zero project.
func (p PreparedProject) Name() string {
	if p.record == nil {
		return ""
	}
	return p.record.project.Name
}

// Compile binds every active service to its admitted image and freezes the
// resulting project. It is pure: it neither inspects nor pulls images.
func (a *Admitter) Compile(project *composetypes.Project, images map[string]Image) (PreparedProject, error) {
	if a == nil || a.issuer == nil {
		return PreparedProject{}, ErrUnavailable
	}
	if project == nil || project.Name == "" || len(project.Services) == 0 || len(project.DisabledServices) != 0 {
		return PreparedProject{}, fmt.Errorf("compose project must contain named active services")
	}
	if len(project.Services) != len(images) {
		return PreparedProject{}, fmt.Errorf("every compose service requires exactly one admitted image")
	}
	prepared, err := project.WithServicesTransform(func(name string, service composetypes.ServiceConfig) (composetypes.ServiceConfig, error) {
		image, ok := images[name]
		if !ok {
			return service, fmt.Errorf("service %s has no admitted image", name)
		}
		if err := image.requireIssuer(a.issuer); err != nil {
			return service, fmt.Errorf("service %s: %w", name, err)
		}
		if service.Build != nil {
			return service, fmt.Errorf("service %s: image builds are not supported", name)
		}
		if service.Image != image.Reference() {
			return service, fmt.Errorf("service %s image differs from its admitted reference", name)
		}
		service.Image = image.ID()
		service.Platform = platforms.FormatAll(image.Platform())
		service.PullPolicy = composetypes.PullPolicyNever
		if service.Labels == nil {
			service.Labels = make(composetypes.Labels)
		}
		service.Labels[LabelImageReference] = image.Reference()
		service.Labels[LabelImageID] = image.ID()
		// Compose merges CustomLabels after Labels. Bind both so neither
		// caller-supplied map can override execution provenance at creation.
		if service.CustomLabels == nil {
			service.CustomLabels = make(composetypes.Labels)
		}
		service.CustomLabels[LabelImageReference] = image.Reference()
		service.CustomLabels[LabelImageID] = image.ID()
		return service, nil
	})
	if err != nil {
		return PreparedProject{}, err
	}
	return PreparedProject{record: &preparedProjectRecord{issuer: a.issuer, project: prepared}}, nil
}

// ComposeUpFunc is captured once when wiring the Compose execution sink.
type ComposeUpFunc func(context.Context, *composetypes.Project, composeapi.UpOptions) error

// ComposeExecutor accepts only complete projects compiled by its own Admitter.
type ComposeExecutor struct {
	issuer *issuer
	up     ComposeUpFunc
}

func (a *Admitter) NewComposeExecutor(up ComposeUpFunc) (*ComposeExecutor, error) {
	if a == nil || a.issuer == nil || up == nil {
		return nil, ErrUnavailable
	}
	return &ComposeExecutor{issuer: a.issuer, up: up}, nil
}

// Up executes a private copy: Compose may mutate its input without changing the
// sealed plan or a concurrent execution. Options cannot inject another project.
func (e *ComposeExecutor) Up(ctx context.Context, prepared PreparedProject, forceRecreate bool) error {
	if e == nil || e.issuer == nil || e.up == nil {
		return ErrUnavailable
	}
	if prepared.record == nil {
		return ErrInvalidProject
	}
	if prepared.record.issuer != e.issuer {
		return ErrForeignProject
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	project, err := prepared.record.project.WithServicesTransform(func(_ string, service composetypes.ServiceConfig) (composetypes.ServiceConfig, error) {
		return service, nil
	})
	if err != nil {
		return err
	}
	recreate := composeapi.RecreateDiverged
	if forceRecreate {
		recreate = composeapi.RecreateForce
	}
	return e.up(ctx, project, composeapi.UpOptions{
		Create: composeapi.CreateOptions{Recreate: recreate, QuietPull: true, RemoveOrphans: true},
		Start:  composeapi.StartOptions{},
	})
}
