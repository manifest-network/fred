package docker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackendHealthCancellationDoesNotStartOrSampleLaterStages(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	pinged := false
	b := &Backend{docker: &mockDockerClient{PingFn: func(context.Context) error {
		pinged = true
		return nil
	}}}
	installMutationTestVerifier(t, b, func(context.Context) error {
		cancel()
		return nil
	})
	before := backendHealthTimingSamples(t, backendHealthDuration, string(healthStageDocker))
	require.ErrorIs(t, b.Health(ctx), context.Canceled)
	require.False(t, pinged, "the next stage cannot begin after request cancellation")
	require.Equal(t, before, backendHealthTimingSamples(t, backendHealthDuration, string(healthStageDocker)))
}
