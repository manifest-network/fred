package docker

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	dockerspec "github.com/moby/docker-image-spec/specs-go/v1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

// ImageInfo is only a convenient metadata DTO for existing backend test doubles.
// Production executable images can only be minted by imageexec admission.
type ImageInfo struct {
	ID      string
	User    string
	Volumes map[string]struct{}
}

type mockImageSource struct{ mock *mockDockerClient }

func (s mockImageSource) ClientVersion() string { return "1.51" }
func (s mockImageSource) ImagePull(context.Context, string, dockerimage.PullOptions) (io.ReadCloser, error) {
	return nil, fmt.Errorf("unexpected fixture manifest materialization")
}
func (s mockImageSource) ContainerCreate(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error) {
	return container.CreateResponse{}, fmt.Errorf("unexpected raw fixture container creation")
}
func (s mockImageSource) ImageInspect(ctx context.Context, reference string, _ ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
	info, err := s.mock.InspectImage(ctx, reference)
	if err != nil {
		return dockerimage.InspectResponse{}, err
	}
	id := info.ID
	if id == "" {
		id = fixtureImageID(reference)
	}
	return dockerimage.InspectResponse{ID: id, Os: "linux", Architecture: "amd64", Config: &dockerspec.DockerOCIImageConfig{ImageConfig: ocispec.ImageConfig{User: info.User, Volumes: info.Volumes}}}, nil
}
func (m *mockDockerClient) imageAdmitter() *imageexec.Admitter {
	m.imageOnce.Do(func() {
		images, _, err := imageexec.NewDockerRuntime(mockImageSource{mock: m})
		if err != nil {
			panic(err)
		}
		m.images = images
	})
	return m.images
}
func (m *mockDockerClient) AdmitImage(ctx context.Context, reference string) (imageexec.Image, error) {
	return m.imageAdmitter().Admit(ctx, reference)
}
func admittedFixtureImage(t *testing.T, reference string) imageexec.Image {
	t.Helper()
	image, err := (&mockDockerClient{}).AdmitImage(t.Context(), reference)
	require.NoError(t, err)
	return image
}
