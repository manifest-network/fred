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

func TestProjectContainerBindsImageAndGroupingWithoutRawLabelAuthority(t *testing.T) {
	admitter, creator, source, image := safeRuntime(t)
	_, foreignCreator, _, foreignImage := safeRuntime(t)
	project := &composetypes.Project{Name: "owned-project", Services: composetypes.Services{
		"web-1": {Image: image.Reference()},
	}}
	images := map[string]imageexec.Image{"web-1": image}
	prepared, err := admitter.Compile(project, images)
	require.NoError(t, err)
	// Neither constructor input remains authority after compilation.
	project.Name = "mutated-project"
	delete(project.Services, "web-1")
	images["web-1"] = foreignImage
	binding, err := prepared.Container("web-1")
	require.NoError(t, err)
	_, err = prepared.Container("absent")
	require.Error(t, err)
	_, err = (imageexec.PreparedProject{}).Container("web-1")
	require.ErrorIs(t, err, imageexec.ErrInvalidProject)
	config := &container.Config{Image: "caller-controlled:latest", Labels: map[string]string{
		composeapi.ProjectLabel: "foreign-project", composeapi.ServiceLabel: "foreign-service", composeapi.VersionLabel: "foreign-version",
		composeapi.ConfigHashLabel: "frozen-config-hash", composeapi.OneoffLabel: "False",
		imageexec.LabelImageID: "forged-id",
	}}
	creates := 0
	source.create = func(_ context.Context, actual *container.Config, _ *container.HostConfig, _ *network.NetworkingConfig, _ *ocispec.Platform, _ string) (container.CreateResponse, error) {
		creates++
		require.Equal(t, image.ID(), actual.Image)
		require.Equal(t, image.ID(), actual.Labels[imageexec.LabelImageID])
		require.Equal(t, "owned-project", actual.Labels[composeapi.ProjectLabel])
		require.Equal(t, "web-1", actual.Labels[composeapi.ServiceLabel])
		require.Equal(t, composeapi.ComposeVersion, actual.Labels[composeapi.VersionLabel])
		require.Equal(t, "frozen-config-hash", actual.Labels[composeapi.ConfigHashLabel])
		require.Equal(t, "False", actual.Labels[composeapi.OneoffLabel])
		return container.CreateResponse{ID: "restored"}, nil
	}
	_, err = creator.CreateProjectContainer(t.Context(), binding, config, nil, nil, "source")
	require.NoError(t, err)
	require.Equal(t, "foreign-project", config.Labels[composeapi.ProjectLabel])
	_, err = creator.CreateProjectContainer(t.Context(), imageexec.ProjectContainer{}, config, nil, nil, "source")
	require.ErrorIs(t, err, imageexec.ErrInvalidProject)
	_, err = foreignCreator.CreateProjectContainer(t.Context(), binding, config, nil, nil, "source")
	require.ErrorIs(t, err, imageexec.ErrForeignProject)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = creator.CreateProjectContainer(canceled, binding, config, nil, nil, "source")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, creates, "invalid authority must never enter Docker")
}
