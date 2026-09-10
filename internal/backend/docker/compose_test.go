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
	executor *imageexec.ComposeExecutor
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
	executor, err := images.NewComposeExecutor(func(ctx context.Context, project *composetypes.Project, opts composeapi.UpOptions) error {
		if m.UpFn != nil {
			return m.UpFn(ctx, project, composeUpOpts{ForceRecreate: opts.Create.Recreate == composeapi.RecreateForce})
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	m.executor = executor
}

func (m *mockComposeExecutor) PrepareProject(project *composetypes.Project, images map[string]imageexec.Image) (imageexec.PreparedProject, error) {
	m.imageMu.Lock()
	defer m.imageMu.Unlock()
	return m.images.Compile(project, images)
}

func (m *mockComposeExecutor) Up(ctx context.Context, project imageexec.PreparedProject, opts composeUpOpts) error {
	m.imageMu.Lock()
	executor := m.executor
	m.imageMu.Unlock()
	return executor.Up(ctx, project, opts.ForceRecreate)
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
