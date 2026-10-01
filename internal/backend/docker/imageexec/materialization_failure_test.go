package imageexec_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

// Platform selection is an observation, not authority to create. The
// independently inspected immutable leaf must still agree.
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

func TestReadmissionRequiresIndependentImageAfterExplicitMaterialization(t *testing.T) {
	failure := errors.New("daemon connection lost")
	for _, scenario := range []struct {
		name  string
		local dockerimage.InspectResponse
		err   error
		want  string
	}{
		{name: "still unavailable", err: errdefs.NotFound(failure), want: failure.Error()},
		{name: "inspect unavailable", err: failure, want: failure.Error()},
		{name: "different identity", local: otherLeafImage(), want: "persisted execution identity or platform"},
		{name: "different platform", local: armLeafImage(), want: "persisted execution identity or platform"},
		{name: "forbidden metadata", local: reservedLabelImage(), want: "reserved label"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			materialized := false
			source := &fakeSource{version: "1.51", inspect: func(_ context.Context, ref string, _ ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
				if materialized {
					require.Equal(t, imageID, ref, "retry must inspect only the selected immutable identity")
					return scenario.local, scenario.err
				}
				switch ref {
				case "registry.example/app:latest":
					return indexImage(), nil
				case indexID:
					return leafImage(), nil
				case imageID:
					return dockerimage.InspectResponse{}, errdefs.NotFound(errors.New("leaf is not local"))
				default:
					t.Fatalf("unexpected inspection %q", ref)
					return dockerimage.InspectResponse{}, nil
				}
			}, create: func(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error) {
				t.Fatal("incomplete materialization reached container creation")
				return container.CreateResponse{}, nil
			}}
			admitter, creator := newRuntime(t, source)
			admitted, err := admitter.Admit(t.Context(), "registry.example/app:latest")
			var required *imageexec.MaterializationRequired
			require.ErrorAs(t, err, &required)
			require.Empty(t, admitted.ID())
			materialized = true
			admitted, err = admitter.ReAdmit(t.Context(), required.ID(), ocispec.Platform{OS: "linux", Architecture: "amd64"}, "registry.example/app:latest")
			require.ErrorContains(t, err, scenario.want)
			require.Empty(t, admitted.ID())
			_, err = creator.Create(t.Context(), admitted, &container.Config{}, nil, nil, "refused")
			require.ErrorIs(t, err, imageexec.ErrInvalidImage)
		})
	}
}

func otherLeafImage() dockerimage.InspectResponse {
	response := leafImage()
	response.ID = indexID
	response.Descriptor.Digest = indexImage().Descriptor.Digest
	return response
}

func armLeafImage() dockerimage.InspectResponse {
	response := leafImage()
	response.Architecture = "arm64"
	return response
}

func reservedLabelImage() dockerimage.InspectResponse {
	response := leafImage()
	response.Config.Labels["fred.lease_id"] = "attacker"
	return response
}

func TestAdmissionRequestsDigestOnlyMaterializationFromKnownRepository(t *testing.T) {
	for _, repositories := range [][]string{{"invalid reference", "registry.example/app@" + indexID}, {"invalid reference"}} {
		t.Run(strings.Join(repositories, ","), func(t *testing.T) {
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
					return dockerimage.InspectResponse{}, errdefs.NotFound(errors.New("leaf is not local"))
				default:
					t.Fatalf("mutable repository reference resolved during digest admission: %q", ref)
					return dockerimage.InspectResponse{}, nil
				}
			}}
			admitter, _ := newRuntime(t, source)
			admitted, err := admitter.Admit(t.Context(), indexID)
			require.Empty(t, admitted.ID())
			var required *imageexec.MaterializationRequired
			if len(repositories) == 1 {
				require.ErrorContains(t, err, "no repository reference")
				require.False(t, errors.As(err, &required), "an unknown repository cannot authorize a guessed materialization")
			} else {
				require.ErrorAs(t, err, &required)
				require.Equal(t, "registry.example/app@"+imageID, required.Reference())
				require.Equal(t, imageID, required.ID())
			}
		})
	}
}
