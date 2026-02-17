package docker

import (
	"context"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// mockComposeExecutor implements composeExecutor for testing.
type mockComposeExecutor struct {
	UpFn   func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error
	DownFn func(ctx context.Context, projectName string, timeout time.Duration) error
	PSFn   func(ctx context.Context, projectName string) ([]composeContainerSummary, error)
}

func (m *mockComposeExecutor) Up(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
	if m.UpFn != nil {
		return m.UpFn(ctx, project, opts)
	}
	return nil
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
