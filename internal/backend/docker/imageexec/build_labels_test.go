package imageexec_test

import (
	"context"
	"maps"
	"testing"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	composeapi "github.com/docker/compose/v5/pkg/api"
	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

func TestComposeBuildLabelsCannotCarryGroupingAcrossCreationBoundaries(t *testing.T) {
	response := classicImage()
	response.Config.Labels = map[string]string{
		composeapi.ProjectLabel: "foreign-project", composeapi.ServiceLabel: "foreign-service",
		composeapi.VersionLabel: "foreign-version", "app.owner": "tenant",
	}
	source := &fakeSource{version: "1.51", inspect: func(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
		return response, nil
	}}
	a, creator := newRuntime(t, source)
	admitted, err := a.Admit(t.Context(), "registry.example/app:latest")
	require.NoError(t, err)
	// Existing immutable pins pass the same policy after process restart.
	admitted, err = a.ReAdmit(t.Context(), admitted.ID(), admitted.Platform(), admitted.Reference())
	require.NoError(t, err)
	config := &container.Config{Labels: map[string]string{
		composeapi.ProjectLabel: "caller-project", composeapi.ServiceLabel: "caller-service", composeapi.VersionLabel: "caller-version",
	}}
	source.create = func(_ context.Context, actual *container.Config, _ *container.HostConfig, _ *network.NetworkingConfig, _ *ocispec.Platform, _ string) (container.CreateResponse, error) {
		// Model Docker inheritance: Config.Labels overlays the image map, while
		// deleting a key from Config would preserve its attacker-supplied value.
		inherited := maps.Clone(response.Config.Labels)
		maps.Copy(inherited, actual.Labels)
		for _, key := range []string{composeapi.ProjectLabel, composeapi.ServiceLabel, composeapi.VersionLabel} {
			value, present := actual.Labels[key]
			require.True(t, present, "neutralization must overwrite inherited %s", key)
			require.Empty(t, value)
			require.Empty(t, inherited[key])
		}
		require.Equal(t, "tenant", inherited["app.owner"])
		return container.CreateResponse{ID: "created"}, nil
	}
	_, err = creator.Create(t.Context(), admitted, config, nil, nil, "helper")
	require.NoError(t, err)
	require.Equal(t, "caller-project", config.Labels[composeapi.ProjectLabel], "sink owns its detached projection")

	project := desiredProject(admitted.Reference())
	service := project.Services["app"]
	service.Name = "caller-service"
	maps.Copy(service.Labels, config.Labels)
	maps.Copy(service.CustomLabels, config.Labels)
	project.Services["app"] = service
	prepared, err := a.Compile(project, map[string]imageexec.Image{"app": admitted})
	require.NoError(t, err)
	executor, err := a.NewComposeExecutor(func(_ context.Context, actual *composetypes.Project, _ composeapi.UpOptions) error {
		service := actual.Services["app"]
		require.Equal(t, "app", service.Name)
		want := map[string]string{composeapi.ProjectLabel: actual.Name, composeapi.ServiceLabel: "app", composeapi.VersionLabel: composeapi.ComposeVersion}
		for _, labels := range []map[string]string{service.Labels, service.CustomLabels} {
			for key, value := range want {
				require.Equal(t, value, labels[key])
			}
		}
		inherited := maps.Clone(response.Config.Labels)
		maps.Copy(inherited, service.Labels)
		maps.Copy(inherited, service.CustomLabels)
		for key, value := range want {
			require.Equal(t, value, inherited[key])
		}
		require.Equal(t, "tenant", inherited["app.owner"])
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, executor.Up(t.Context(), prepared, false))
	require.Equal(t, "foreign-project", response.Config.Labels[composeapi.ProjectLabel])
}
