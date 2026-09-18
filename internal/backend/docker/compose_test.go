package docker

import (
	"context"
	"sync"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	composeapi "github.com/docker/compose/v5/pkg/api"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

// mockComposeExecutor implements the separate compose read and mutation seams
// for testing.
type mockComposeExecutor struct {
	imageMu  sync.Mutex
	images   *imageexec.Admitter
	LaunchFn func(context.Context, *composetypes.Project, composeUpOpts) daemonLaunchOutcome
	UpFn     func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error
	DownFn   func(ctx context.Context, projectName string, timeout time.Duration) error
	PSFn     func(ctx context.Context, projectName string) ([]composeContainerSummary, error)
}

func (m *mockComposeExecutor) bindImages(images *imageexec.Admitter) {
	m.imageMu.Lock()
	defer m.imageMu.Unlock()
	if m.images == images {
		return
	}
	m.images = images

}

func (m *mockComposeExecutor) PrepareProject(project *composetypes.Project, images map[string]imageexec.Image) (imageexec.PreparedProject, error) {
	m.imageMu.Lock()
	defer m.imageMu.Unlock()
	return m.images.Compile(project, images)
}

func (m *mockComposeExecutor) launch(ctx context.Context, project imageexec.PreparedProject, opts composeUpOpts) daemonLaunchOutcome {
	m.imageMu.Lock()
	images := m.images
	m.imageMu.Unlock()
	var outcome daemonLaunchOutcome
	called := false
	executor, err := images.NewComposeExecutor(func(ctx context.Context, project *composetypes.Project, options composeapi.UpOptions) error {
		called = true
		opts := composeUpOpts{ForceRecreate: options.Create.Recreate == composeapi.RecreateForce}
		if m.LaunchFn != nil {
			outcome = m.LaunchFn(ctx, project, opts)
		} else {
			var err error
			if m.UpFn != nil {
				err = m.UpFn(ctx, project, opts)
			}
			outcome = daemonLaunchOutcome{settled: err == nil, err: err}
		}
		return outcome.err
	})
	if err != nil {
		return daemonLaunchOutcome{settled: true, err: err}
	}
	err = executor.Up(ctx, project, opts.ForceRecreate)
	if !called {
		return daemonLaunchOutcome{settled: true, err: err}
	}
	return outcome
}

func (m *mockComposeExecutor) Down(ctx context.Context, projectName string, timeout time.Duration) error {
	if m.DownFn != nil {
		return m.DownFn(ctx, projectName, timeout)
	}
	return nil
}

func (m *mockComposeExecutor) PS(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
	if m.PSFn != nil {
		return m.PSFn(ctx, projectName)
	}
	return nil, nil
}
