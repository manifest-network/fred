//go:build integration

package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// newImageSecurityFixtureClient owns the raw SDK only in test fixtures. The
// production lifecycle wrapper deliberately cannot create arbitrary containers.
func newImageSecurityFixtureClient(t *testing.T) *client.Client {
	t.Helper()
	sdk, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sdk.Close()) })
	return sdk
}

// importImageSecurityFixture creates a tiny, local-only image. It has no runnable
// program: the tests exercise create/inspection, never execute tenant code, and
// require neither a registry pull nor the Docker build service.
func importImageSecurityFixture(t *testing.T, ctx context.Context, sdk *client.Client, labels map[string]string) (string, string) {
	t.Helper()
	tag := "fred-image-security:" + uuid.NewString()
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	body := []byte(tag)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "fixture", Mode: 0o644, Size: int64(len(body))}))
	_, err := tw.Write(body)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	changes := []string{`CMD ["/fixture-not-executable"]`}
	for key, value := range labels {
		changes = append(changes, fmt.Sprintf("LABEL %s=%q", key, value))
	}
	rc, err := sdk.ImageImport(ctx, image.ImportSource{Source: &archive, SourceName: "-"}, tag, image.ImportOptions{Changes: changes})
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	decoder := json.NewDecoder(rc)
	for {
		var message struct {
			Error string `json:"error"`
		}
		if err := decoder.Decode(&message); err == io.EOF {
			break
		} else {
			require.NoError(t, err)
		}
		require.Empty(t, message.Error)
	}
	inspected, _, err := sdk.ImageInspectWithRaw(ctx, tag)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		_, _ = sdk.ImageRemove(cleanupCtx, tag, image.RemoveOptions{})
		_, _ = sdk.ImageRemove(cleanupCtx, inspected.ID, image.RemoveOptions{})
	})
	return tag, inspected.ID
}

func TestIntegration_Docker_ReservedImageLabelsRejectedBeforeContainerCreation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)
	sdk := newImageSecurityFixtureClient(t)
	compose, err := newComposeService(sdk.DaemonHost(), docker.images)
	require.NoError(t, err)
	for _, key := range []string{"TrAeFiK.enable", "fred.managed", "com.docker.compose.project"} {
		t.Run(key, func(t *testing.T) {
			tag, imageID := importImageSecurityFixture(t, ctx, sdk, map[string]string{key: "true"})
			rejected, err := docker.AdmitImage(ctx, tag)
			require.ErrorContains(t, err, "reserved label")
			require.Empty(t, rejected.ID(), "rejection must not mint an executable image")
			_, err = docker.CreateContainer(ctx, CreateContainerParams{
				Image: rejected, Manifest: &manifest.Manifest{Image: tag}, ServiceName: "app",
			}, time.Second)
			require.Error(t, err, "a zero image cannot reach Docker creation")

			params := baseProjectParams()
			params.LeaseUUID = uuid.NewString()
			params.Stack.Services["web"].Image = tag
			params.NetworkName = ""
			desired := buildComposeProject(params)
			evidence := make(map[string]imageexec.Image, len(desired.Services))
			for name := range desired.Services {
				evidence[name] = rejected
			}
			_, err = compose.PrepareProject(desired, evidence)
			require.Error(t, err, "a rejected image cannot become an executable Compose plan")

			created, err := sdk.ContainerList(ctx, container.ListOptions{All: true, Filters: filters.NewArgs(filters.Arg("ancestor", imageID))})
			require.NoError(t, err)
			assert.Empty(t, created, "a rejected image must never reach a workload or stopped helper")
		})
	}
}

func TestIntegration_Docker_ImmutableImageBindingSurvivesTagMovement(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	docker := newIntegrationDockerClient(t, ctx)
	sdk := newImageSecurityFixtureClient(t)
	tag, imageID := importImageSecurityFixture(t, ctx, sdk, map[string]string{"app.owner": "tenant"})
	admitted, err := docker.AdmitImage(ctx, tag)
	require.NoError(t, err)
	require.Equal(t, imageID, admitted.ID())
	leaseUUID := uuid.NewString()
	id, err := docker.CreateContainer(ctx, CreateContainerParams{
		Image: admitted, Manifest: &manifest.Manifest{Image: tag}, LeaseUUID: leaseUUID,
		ServiceName: "direct", BackendName: "image-security",
	}, 30*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		_ = docker.RemoveContainer(cleanupCtx, id)
	})
	raw, err := sdk.ContainerInspect(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, imageID, raw.Image)
	assert.Equal(t, imageID, raw.Config.Image)
	assert.Equal(t, "tenant", raw.Config.Labels["app.owner"], "ordinary image labels remain supported")
	projected, err := docker.InspectContainer(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, tag, projected.Image)

	// A concurrent pull can move the original reference after setup. Compose
	// must retain the safe image, and its actual Create path must use that ID.
	_, rejectedID := importImageSecurityFixture(t, ctx, sdk, map[string]string{"traefik.enable": "true"})
	require.NoError(t, sdk.ImageTag(ctx, rejectedID, tag))
	_, err = docker.AdmitImage(ctx, tag)
	require.ErrorContains(t, err, "reserved label")
	params := baseProjectParams()
	params.LeaseUUID = leaseUUID
	params.Stack.Services["web"].Image = tag
	params.ImageSetups["web"].Image = admitted
	params.BackendName = "image-security"
	docker.backendName = "image-security"
	params.NetworkName = ""
	compose, err := newComposeService(sdk.DaemonHost(), docker.images)
	require.NoError(t, err)
	desired := buildComposeProject(params)
	evidence := make(map[string]imageexec.Image, len(desired.Services))
	for name := range desired.Services {
		evidence[name] = admitted
	}
	project, err := compose.PrepareProject(desired, evidence)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		_ = compose.Down(cleanupCtx, project.Name(), time.Second)
	})
	// Exercise the sealed production executor. Creation succeeds, then Start
	// fails because this fixture contains no executable. No tenant code runs.
	require.Error(t, compose.Up(ctx, project, composeUpOpts{}))
	containers, err := compose.PS(ctx, project.Name())
	require.NoError(t, err)
	require.Len(t, containers, 1)
	composed, err := sdk.ContainerInspect(ctx, containers[0].ID)
	require.NoError(t, err)
	assert.False(t, composed.State.Running)
	assert.Equal(t, imageID, composed.Image)
	assert.Equal(t, imageID, composed.Config.Image)
	assert.Equal(t, tag, composed.Config.Labels[LabelImageReference])
	assert.Equal(t, imageID, composed.Config.Labels[LabelImageID])
	assert.NotContains(t, composed.Config.Labels, "traefik.enable")
	assert.Equal(t, tag, params.Stack.Services["web"].Image)
	managed, err := docker.ListManagedContainers(ctx)
	require.NoError(t, err)
	found := false
	for _, current := range managed {
		if current.ContainerID == containers[0].ID {
			found = true
			assert.Equal(t, tag, current.Image, "recovery retains the original release reference after the tag moves")
		}
	}
	assert.True(t, found)
}
