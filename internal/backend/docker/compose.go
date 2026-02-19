package docker

import (
	"context"
	"fmt"
	"io"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/cli/cli/command"
	cliflags "github.com/docker/cli/cli/flags"
	composeapi "github.com/docker/compose/v2/pkg/api"
	"github.com/docker/compose/v2/pkg/compose"
	"github.com/docker/docker/client"
	"github.com/sirupsen/logrus"
)

// composeExecutor wraps Docker Compose operations for stack deployments.
type composeExecutor interface {
	// Up creates and starts services from the project.
	// Compose diffs current vs desired state: creates missing containers,
	// recreates changed ones, starts stopped ones.
	Up(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error

	// Down stops and removes all containers for the project.
	Down(ctx context.Context, projectName string, timeout time.Duration) error

	// PS lists containers belonging to the project.
	PS(ctx context.Context, projectName string) ([]composeContainerSummary, error)
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
type composeService struct {
	backend composeapi.Compose
}

// newComposeService creates a composeService that uses the Docker daemon at
// the given host for Compose operations.
func newComposeService(dockerHost string) (*composeService, error) {
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
		command.WithInitializeClient(func(cli *command.DockerCli) (client.APIClient, error) {
			opts := []client.Opt{
				client.WithAPIVersionNegotiation(),
			}
			if dockerHost != "" {
				opts = append(opts, client.WithHost(dockerHost))
			}
			return client.NewClientWithOpts(opts...)
		}),
	); err != nil {
		return nil, fmt.Errorf("initialize docker cli: %w", err)
	}

	return &composeService{
		backend: compose.NewComposeService(dockerCli),
	}, nil
}

func (s *composeService) Up(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
	recreate := composeapi.RecreateDiverged
	if opts.ForceRecreate {
		recreate = composeapi.RecreateForce
	}
	return s.backend.Up(ctx, project, composeapi.UpOptions{
		Create: composeapi.CreateOptions{
			Recreate:      recreate,
			QuietPull:     true,
			RemoveOrphans: true,
		},
		Start: composeapi.StartOptions{},
	})
}

func (s *composeService) Down(ctx context.Context, projectName string, timeout time.Duration) error {
	return s.backend.Down(ctx, projectName, composeapi.DownOptions{
		Timeout:       &timeout,
		RemoveOrphans: true,
	})
}

func (s *composeService) PS(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
	containers, err := s.backend.Ps(ctx, projectName, composeapi.PsOptions{
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
			State:   c.State,
			Health:  c.Health,
		}
	}
	return result, nil
}
