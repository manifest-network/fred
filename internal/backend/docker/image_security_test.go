package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

const testImageID = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const otherTestImageID = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

// fixtureImageID gives named mock image versions valid content-addressed IDs.
func fixtureImageID(seed string) string { return digest.FromString(seed).String() }

func newImageSecurityDockerClient(t *testing.T, handler func(*http.Request) (*http.Response, error)) *DockerClient {
	t.Helper()
	cli, err := client.NewClientWithOpts(client.WithHost("http://docker.invalid"), client.WithVersion("1.51"),
		client.WithHTTPClient(&http.Client{Transport: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			if strings.HasSuffix(req.URL.Path, "/version") {
				return imageSecurityResponse(http.StatusOK, `{"ApiVersion":"1.51"}`), nil
			}
			return handler(req)
		})}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return imageSecurityClientFromSDK(t, cli)
}

func imageSecurityClientFromSDK(t *testing.T, cli *client.Client) *DockerClient {
	t.Helper()
	images, creator, err := imageexec.NewDockerRuntime(t.Context(), cli)
	require.NoError(t, err)
	return &DockerClient{client: newDockerSDKView(cli), images: images, creator: creator, backendName: "image-security"}
}

func imageSecurityResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
}

func TestInspectImageRejectsReservedMetadata(t *testing.T) {
	for _, key := range []string{"fred.managed", "FrEd.image_reference", "traefik.enable", "TrAeFiK.http.routers.victim.rule", "com.docker.compose.project", "CoM.DoCkEr.CoMpOsE.oneoff"} {
		t.Run(key, func(t *testing.T) {
			cli := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
				require.Contains(t, req.URL.Path, "/images/")
				data, err := json.Marshal(map[string]any{"Id": testImageID, "Os": "linux", "Architecture": "amd64", "Config": map[string]any{"Labels": map[string]string{key: "tenant-controlled"}}})
				require.NoError(t, err)
				return imageSecurityResponse(http.StatusOK, string(data)), nil
			})
			_, err := cli.AdmitImage(t.Context(), "tenant/app:latest")
			require.ErrorContains(t, err, "reserved label")
			require.ErrorContains(t, err, key)
			assert.NotContains(t, err.Error(), "tenant-controlled")
		})
	}
}

func TestImageCreationBoundariesRequireAdmittedImage(t *testing.T) {
	actions := map[string]func(*DockerClient) error{
		"workload": func(cli *DockerClient) error {
			_, err := cli.CreateContainer(t.Context(), CreateContainerParams{Manifest: &manifest.Manifest{Image: "tenant/app:latest"}, ServiceName: "app"}, time.Second)
			return err
		},
		"passwd helper": func(cli *DockerClient) error {
			_, err := cli.readFileFromImage(t.Context(), imageexec.Image{}, "/etc/passwd", shared.ImageInspectionOrigin{})
			return err
		},
		"user resolution": func(cli *DockerClient) error {
			_, _, err := cli.ResolveImageUser(t.Context(), imageexec.Image{}, "app", shared.ImageInspectionOrigin{})
			return err
		},
		"volume owner helper": func(cli *DockerClient) error {
			_, _, err := cli.DetectVolumeOwner(t.Context(), imageexec.Image{}, []string{"/data"}, shared.ImageInspectionOrigin{})
			return err
		},
		"writable path helper": func(cli *DockerClient) error {
			_, err := cli.DetectWritablePaths(t.Context(), imageexec.Image{}, 1000, []string{"/var/lib"}, shared.ImageInspectionOrigin{})
			return err
		},
		"extraction helper": func(cli *DockerClient) error {
			return cli.ExtractImageContent(t.Context(), imageexec.Image{}, []string{"/data"}, t.TempDir(), 1024, 10, shared.ImageInspectionOrigin{})["/data"]
		},
	}
	for name, action := range actions {
		t.Run(name, func(t *testing.T) {
			creates := 0
			cli := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, "/images/") {
					return imageSecurityResponse(http.StatusOK, fmt.Sprintf(`{"Id":%q,"Os":"linux","Architecture":"amd64","Config":{"Labels":{"traefik.http.routers.victim.rule":"Host(victim.example)"}}}`, testImageID)), nil
				}
				creates++
				return nil, errors.New("unexpected Docker mutation")
			})
			require.ErrorIs(t, action(cli), imageexec.ErrInvalidImage)
			assert.Zero(t, creates, "reject before any container can inherit the image's router")
		})
	}
}

func TestCreateContainerPinsInspectedImageAfterTagMoves(t *testing.T) {
	var created container.Config
	inspections := 0
	cli := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(req.URL.Path, "/images/"):
			inspections++
			// The tag can move immediately after this response; only this ID was checked.
			return imageSecurityResponse(http.StatusOK, fmt.Sprintf(`{"Id":%q,"Os":"linux","Architecture":"amd64","Config":{"Labels":{"app.owner":"tenant"}}}`, testImageID)), nil
		case strings.HasSuffix(req.URL.Path, "/containers/create"):
			require.NoError(t, json.NewDecoder(req.Body).Decode(&created))
			return imageSecurityResponse(http.StatusCreated, `{"Id":"created-container"}`), nil
		default:
			return nil, fmt.Errorf("unexpected Docker request: %s", req.URL.Path)
		}
	})
	m := &manifest.Manifest{Image: "tenant/app:latest"}
	admitted, err := cli.AdmitImage(t.Context(), m.Image)
	require.NoError(t, err)
	id, err := cli.CreateContainer(t.Context(), CreateContainerParams{Image: admitted, Manifest: m, LeaseUUID: "lease", ServiceName: "web", BackendName: "image-security"}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, "created-container", id)
	assert.Equal(t, 1, inspections)
	assert.Equal(t, testImageID, created.Image)
	assert.Equal(t, testImageID, created.Labels[LabelImageID])
	assert.Equal(t, m.Image, created.Labels[LabelImageReference])
	assert.Equal(t, "tenant/app:latest", m.Image)
}

func TestImageInspectionHelperPinsInspectedImage(t *testing.T) {
	h := newInspectionHarness(t)
	h.execute(t, func(ctx context.Context, origin shared.ImageInspectionOrigin) error {
		session, err := h.client.openImageInspection(ctx, h.image, origin)
		require.NoError(t, err)
		actual := h.daemon.containers[session.containerID]
		assert.Equal(t, testImageID, actual.Config.Image)
		assert.Equal(t, testImageID, actual.Config.Labels[LabelImageID])
		assert.Equal(t, h.image.Reference(), actual.Config.Labels[LabelImageReference])
		assert.NotContains(t, actual.Config.Labels, LabelManaged)
		return session.close()
	})
}

func TestPreparedComposeProjectPreservesIntentAndPreventsRepull(t *testing.T) {
	params := baseProjectParams()
	mock := &mockDockerClient{InspectImageFn: func(context.Context, string) (*ImageInfo, error) { return &ImageInfo{ID: testImageID}, nil }}
	admitted, err := mock.AdmitImage(t.Context(), params.Stack.Services["web"].Image)
	require.NoError(t, err)
	params.ImageSetups["web"].Image = admitted
	project := buildComposeProject(params)
	before, err := json.Marshal(project)
	require.NoError(t, err)
	assert.Equal(t, "nginx:latest", project.Services["web"].Image, "builder retains desired reference")
	compose := &mockComposeExecutor{UpFn: func(_ context.Context, pinned *composetypes.Project, _ composeUpOpts) error {
		assert.Equal(t, testImageID, pinned.Services["web"].Image)
		assert.Equal(t, composetypes.PullPolicyNever, pinned.Services["web"].PullPolicy)
		assert.Equal(t, "nginx:latest", pinned.Services["web"].Labels[LabelImageReference])
		return nil
	}}
	compose.bindImages(mock.imageAdmitter())
	prepared, err := compose.PrepareProject(project, composeProjectImages(project, params.ImageSetups))
	require.NoError(t, err)
	require.NoError(t, compose.Up(t.Context(), prepared, composeUpOpts{}))
	after, err := json.Marshal(project)
	require.NoError(t, err)
	assert.JSONEq(t, string(before), string(after))
}

func TestValidateAdoptionRequiresExactImageID(t *testing.T) {
	params := CreateContainerParams{Manifest: &manifest.Manifest{Image: "tenant/app:latest"}, LeaseUUID: "lease", BackendName: "backend"}
	labels := map[string]string{LabelManaged: "true", LabelLeaseUUID: "lease", LabelBackendName: "backend", LabelFailCount: "0"}
	existing := container.InspectResponse{
		ContainerJSONBase: &container.ContainerJSONBase{ID: "existing", Image: otherTestImageID},
		Config:            &container.Config{Image: params.Manifest.Image, Labels: labels},
	}
	_, err := validateAdoption(existing, params, "name", testImageID)
	require.ErrorContains(t, err, "image ID")
	existing.Image = testImageID
	_, err = validateAdoption(existing, params, "name", testImageID)
	require.NoError(t, err, "legacy tag-created containers remain adoptable only when their actual image matches")
	existing.Config.Image = testImageID
	labels[LabelImageReference] = params.Manifest.Image
	labels[LabelImageID] = testImageID
	_, err = validateAdoption(existing, params, "name", testImageID)
	require.NoError(t, err)
	labels[LabelImageReference] = "tenant/different:latest"
	_, err = validateAdoption(existing, params, "name", testImageID)
	require.ErrorContains(t, err, "with image")
}

func TestInspectImageForSetupPinsUserAndFilesystemDiscovery(t *testing.T) {
	for _, explicitUser := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit user %t", explicitUser), func(t *testing.T) {
			ownerCalls, userCalls, writableCalls := 0, 0, 0
			mock := &mockDockerClient{
				InspectImageFn: func(_ context.Context, image string) (*ImageInfo, error) {
					assert.Equal(t, "tenant/app:latest", image)
					return &ImageInfo{ID: testImageID, Volumes: map[string]struct{}{"/data": {}}}, nil
				},
				ResolveImageUserFn: func(_ context.Context, image, user string) (int, int, error) {
					userCalls++
					assert.Equal(t, testImageID, image)
					assert.Equal(t, "app", user)
					return 1000, 1000, nil
				},
				DetectVolumeOwnerFn: func(_ context.Context, image string, _ []string) (int, int, error) {
					ownerCalls++
					assert.Equal(t, testImageID, image)
					return 1000, 1000, nil
				},
				DetectWritablePathsFn: func(_ context.Context, image string, uid int, _ []string) ([]string, error) {
					writableCalls++
					assert.Equal(t, testImageID, image)
					assert.Equal(t, 1000, uid)
					return []string{"/var/cache/app"}, nil
				},
			}
			b := newBackendForTest(mock, nil)
			var user string
			if explicitUser {
				user = "app"
			}
			setup, err := inspectImageForSetupForTest(t, b, t.Context(), "tenant/app:latest", user)
			require.NoError(t, err)
			assert.Equal(t, testImageID, setup.Image.ID())
			assert.Equal(t, 1, writableCalls)
			if explicitUser {
				assert.Equal(t, 1, userCalls)
				assert.Zero(t, ownerCalls)
			} else {
				assert.Equal(t, 1, ownerCalls)
				assert.Zero(t, userCalls)
			}
		})
	}
}
