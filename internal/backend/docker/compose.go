package docker

import (
	"context"
	"fmt"
	"io"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/cli/cli/command"
	cliflags "github.com/docker/cli/cli/flags"
	composeapi "github.com/docker/compose/v5/pkg/api"
	"github.com/docker/compose/v5/pkg/compose"
	mobyclient "github.com/moby/moby/client"
	"github.com/sirupsen/logrus"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

// composeReader is the only Compose surface retained by Backend.
type composeReader interface {
	// PS lists containers belonging to the project.
	PS(ctx context.Context, projectName string) ([]composeContainerSummary, error)
}

// composeMutationSink is captured only by settlement-bound Guards.
type composeMutationSink interface {
	// PrepareProject compiles the desired project with its complete admitted images.
	PrepareProject(*composetypes.Project, map[string]imageexec.Image) (imageexec.PreparedProject, error)
	// Up creates and starts only services in the prepared project.
	Up(ctx context.Context, project imageexec.PreparedProject, opts composeUpOpts) error

	// Down stops and removes all containers for the project.
	Down(ctx context.Context, projectName string, timeout time.Duration) error
}

// composeUpOpts configures a Compose Up operation.
type composeUpOpts struct {
	ForceRecreate bool // force recreate even if config unchanged (for restart)
}

// composeContainerSummary holds container info from a Compose PS operation.
type composeContainerSummary struct {
	ID      string
	Name    string
	Service string // Compose service name (e.g., "web-0")
	State   string // "running", "exited", etc.
	Health  string // "healthy", "unhealthy", "starting", ""
}

// composeProjectName returns the Compose project name for a lease.
func composeProjectName(leaseUUID string) string {
	return "fred-" + leaseUUID
}

// composeService is the real implementation wrapping the Docker Compose API.
//
// Compose v5 is built on github.com/moby/moby/client, so this file talks to the
// daemon through that module (aliased mobyclient) while lifecycle.go keeps using
// github.com/docker/docker/client. Two Docker client libraries therefore coexist
// in this package, bound to the same `client` identifier in different files —
// hence the alias here, so a reader can tell which module a call belongs to.
// They interoperate safely: both tag their errors with the same
// github.com/containerd/errdefs sentinels, and compose values never cross into
// the docker/docker-typed dockerClient interface (PS results are converted to
// composeContainerSummary below). Porting lifecycle.go across is held out to
// keep this migration reviewable; whether it would clear the two remaining
// docker/docker advisories is unmeasured, since buildx still reaches that
// module via pkg/namesgenerator.
type composeService struct {
	compile  func(*composetypes.Project, map[string]imageexec.Image) (imageexec.PreparedProject, error)
	executor *imageexec.ComposeExecutor
	down     func(context.Context, string, composeapi.DownOptions) error
	ps       func(context.Context, string, composeapi.PsOptions) ([]composeapi.ContainerSummary, error)
}

// newComposeService creates a composeService that uses the Docker daemon at
// the given host for Compose operations.
func newComposeService(dockerHost string, images *imageexec.Admitter) (*composeService, error) {
	// Silence the Compose library's logrus logger. Compose emits noisy
	// warnings (e.g., "No resource found to remove") via its own global
	// logrus instance. Operational information is already logged by the
	// backend's structured logger.
	logrus.SetOutput(io.Discard)

	dockerCli, err := command.NewDockerCli(
		command.WithCombinedStreams(io.Discard),
	)
	if err != nil {
		return nil, fmt.Errorf("create docker cli: %w", err)
	}

	if err := dockerCli.Initialize(
		cliflags.NewClientOptions(),
		command.WithInitializeClient(func(cli *command.DockerCli) (mobyclient.APIClient, error) {
			opts := []mobyclient.Opt{
				mobyclient.WithAPIVersionNegotiation(),
			}
			if dockerHost != "" {
				opts = append(opts, mobyclient.WithHost(dockerHost))
			}
			return mobyclient.New(opts...)
		}),
	); err != nil {
		return nil, fmt.Errorf("initialize docker cli: %w", err)
	}

	backend, err := compose.NewComposeService(dockerCli)
	if err != nil {
		return nil, fmt.Errorf("create compose service: %w", err)
	}

	executor, err := images.NewComposeExecutor(backend.Up)
	if err != nil {
		return nil, err
	}
	return &composeService{compile: images.Compile, executor: executor, down: backend.Down, ps: backend.Ps}, nil
}

func (s *composeService) PrepareProject(project *composetypes.Project, images map[string]imageexec.Image) (imageexec.PreparedProject, error) {
	return s.compile(project, images)
}

func (s *composeService) Up(ctx context.Context, project imageexec.PreparedProject, opts composeUpOpts) error {
	return s.executor.Up(ctx, project, opts.ForceRecreate)
}

func (s *composeService) Down(ctx context.Context, projectName string, timeout time.Duration) error {
	return s.down(ctx, projectName, composeapi.DownOptions{
		Timeout:       &timeout,
		RemoveOrphans: true,
		// Reap anonymous volumes attached to the project's containers (ENG-372).
		// fred's persistent data lives in bind mounts (applyVolumeBinds) and the
		// project declares no top-level `volumes:` section, so Volumes:true only
		// removes the anonymous volumes Docker auto-creates for image VOLUME
		// directives the tmpfs override doesn't cover — never tenant data. Without
		// this, every close leaks one anonymous volume per such container.
		Volumes: true,
	})
}

func (s *composeService) PS(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
	containers, err := s.ps(ctx, projectName, composeapi.PsOptions{
		All: true,
	})
	if err != nil {
		return nil, err
	}

	result := make([]composeContainerSummary, len(containers))
	for i, c := range containers {
		result[i] = composeContainerSummary{
			ID:      c.ID,
			Name:    c.Name,
			Service: c.Service,
			State:   string(c.State),
			Health:  string(c.Health),
		}
	}
	return result, nil
}
