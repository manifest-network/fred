package imageexec_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

// Platform selection and a completed pull are observations, not authority to
// create. The independently inspected immutable leaf must still agree.
func TestAdmissionRefusesUnresolvedOrReplacedPlatformLeaf(t *testing.T) {
	inspectionFailure := errors.New("image store unavailable")
	for _, scenario := range []struct {
		name     string
		selected dockerimage.InspectResponse
		local    dockerimage.InspectResponse
		err      error
		want     string
	}{
		{name: "selection unavailable", err: inspectionFailure, want: "resolve image platform manifest"},
		{name: "selection still an index", selected: indexImage(), want: "did not resolve"},
		{name: "selection lacks leaf descriptor", selected: classicImage(), want: "did not resolve"},
		{name: "local record changed into index", selected: leafImage(), local: indexImage(), want: "differs from checked manifest"},
		{name: "local record lost descriptor", selected: leafImage(), local: classicImage(), want: "differs from checked manifest"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			source := &fakeSource{version: "1.51", inspect: func(_ context.Context, ref string, _ ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
				switch ref {
				case "registry.example/app:latest":
					return indexImage(), nil
				case indexID:
					return scenario.selected, scenario.err
				case imageID:
					return scenario.local, nil
				default:
					t.Fatalf("unexpected inspection %q", ref)
					return dockerimage.InspectResponse{}, nil
				}
			}, pull: func(context.Context, string, dockerimage.PullOptions) (io.ReadCloser, error) {
				t.Fatal("a failed selection must not cause a registry pull")
				return nil, nil
			}, create: func(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error) {
				t.Fatal("failed platform selection reached container creation")
				return container.CreateResponse{}, nil
			}}
			admitter, creator := newRuntime(t, source)
			admitted, err := admitter.Admit(t.Context(), "registry.example/app:latest")
			require.ErrorContains(t, err, scenario.want)
			_, err = creator.Create(t.Context(), admitted, &container.Config{}, nil, nil, "refused")
			require.Error(t, err, "a refused image must not authorize creation")
		})
	}
}

type observedPullReader struct {
	io.Reader
	closed bool
}

func (reader *observedPullReader) Close() error { reader.closed = true; return nil }

func TestAdmissionRequiresIndependentImageAfterPull(t *testing.T) {
	failure := errors.New("daemon connection lost")
	for _, scenario := range []string{"pull unavailable", "post-pull inspect unavailable", "post-pull leaf replaced"} {
		t.Run(scenario, func(t *testing.T) {
			pulled := false
			reader := &observedPullReader{Reader: strings.NewReader("{\"status\":\"complete\"}\n")}
			source := &fakeSource{version: "1.51", inspect: func(_ context.Context, ref string, _ ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
				switch ref {
				case "registry.example/app:latest":
					return indexImage(), nil
				case indexID:
					return leafImage(), nil
				case imageID:
					if !pulled {
						return dockerimage.InspectResponse{}, errdefs.NotFound(errors.New("leaf is not local"))
					}
					if scenario == "post-pull inspect unavailable" {
						return dockerimage.InspectResponse{}, failure
					}
					return indexImage(), nil
				default:
					t.Fatalf("unexpected inspection %q", ref)
					return dockerimage.InspectResponse{}, nil
				}
			}, pull: func(_ context.Context, ref string, _ dockerimage.PullOptions) (io.ReadCloser, error) {
				require.Equal(t, "registry.example/app@"+imageID, ref)
				pulled = true
				if scenario == "pull unavailable" {
					return nil, failure
				}
				return reader, nil
			}, create: func(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error) {
				t.Fatal("incomplete materialization reached container creation")
				return container.CreateResponse{}, nil
			}}
			admitter, creator := newRuntime(t, source)
			admitted, err := admitter.Admit(t.Context(), "registry.example/app:latest")
			if scenario == "post-pull leaf replaced" {
				require.ErrorContains(t, err, "differs from checked manifest")
			} else {
				require.ErrorIs(t, err, failure)
			}
			require.True(t, pulled)
			require.Equal(t, scenario != "pull unavailable", reader.closed, "every returned pull stream must be closed on refusal")
			_, err = creator.Create(t.Context(), admitted, &container.Config{}, nil, nil, "refused")
			require.Error(t, err)
		})
	}
}

func TestAdmissionMaterializesDigestOnlyReferenceFromKnownRepository(t *testing.T) {
	for _, repositories := range [][]string{{"invalid reference", "registry.example/app@" + indexID}, {"invalid reference"}} {
		t.Run(strings.Join(repositories, ","), func(t *testing.T) {
			pulled := false
			source := &fakeSource{version: "1.51", inspect: func(_ context.Context, ref string, options ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
				switch ref {
				case indexID:
					if len(options) != 0 {
						return leafImage(), nil
					}
					response := indexImage()
					response.RepoDigests = repositories
					return response, nil
				case imageID:
					if !pulled {
						return dockerimage.InspectResponse{}, errdefs.NotFound(errors.New("leaf is not local"))
					}
					return leafImage(), nil
				default:
					t.Fatalf("mutable repository reference resolved during digest admission: %q", ref)
					return dockerimage.InspectResponse{}, nil
				}
			}, pull: func(_ context.Context, ref string, _ dockerimage.PullOptions) (io.ReadCloser, error) {
				require.Equal(t, "registry.example/app@"+imageID, ref)
				pulled = true
				return io.NopCloser(strings.NewReader("{\"status\":\"complete\"}\n")), nil
			}}
			admitter, _ := newRuntime(t, source)
			admitted, err := admitter.Admit(t.Context(), indexID)
			if len(repositories) == 1 {
				require.ErrorContains(t, err, "no repository reference")
				require.False(t, pulled, "an unknown repository cannot authorize a guessed pull")
			} else {
				require.NoError(t, err)
				require.True(t, pulled)
				require.Equal(t, imageID, admitted.ID())
				require.Equal(t, indexID, admitted.Reference())
			}
		})
	}
}
