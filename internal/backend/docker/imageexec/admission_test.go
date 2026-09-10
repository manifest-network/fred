package imageexec_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	dockerspec "github.com/moby/docker-image-spec/specs-go/v1"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

var (
	imageID = "sha256:" + strings.Repeat("1", 64)
	indexID = "sha256:" + strings.Repeat("2", 64)
)

type fakeSource struct {
	version string
	inspect func(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error)
	pull    func(context.Context, string, dockerimage.PullOptions) (io.ReadCloser, error)
	create  func(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error)
}

func (s *fakeSource) ClientVersion() string { return s.version }

func (s *fakeSource) ImageInspect(ctx context.Context, ref string, opts ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
	return s.inspect(ctx, ref, opts...)
}

func (s *fakeSource) ImagePull(ctx context.Context, ref string, opts dockerimage.PullOptions) (io.ReadCloser, error) {
	if s.pull == nil {
		return nil, errors.New("unexpected image pull")
	}
	return s.pull(ctx, ref, opts)
}

func (s *fakeSource) ContainerCreate(ctx context.Context, cfg *container.Config, host *container.HostConfig, networks *network.NetworkingConfig, platform *ocispec.Platform, name string) (container.CreateResponse, error) {
	if s.create == nil {
		return container.CreateResponse{}, errors.New("unexpected container create")
	}
	return s.create(ctx, cfg, host, networks, platform, name)
}

func classicImage() dockerimage.InspectResponse {
	return dockerimage.InspectResponse{
		ID: imageID, Os: "linux", Architecture: "amd64",
		Config: &dockerspec.DockerOCIImageConfig{ImageConfig: ocispec.ImageConfig{
			User: "123:456", Labels: map[string]string{"app": "safe"},
			Volumes: map[string]struct{}{"/z": {}, "/a": {}},
		}},
	}
}

func leafImage() dockerimage.InspectResponse {
	response := classicImage()
	response.Descriptor = &ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.Digest(imageID)}
	return response
}

func indexImage() dockerimage.InspectResponse {
	response := leafImage()
	response.ID = indexID
	response.Descriptor = &ocispec.Descriptor{MediaType: ocispec.MediaTypeImageIndex, Digest: digest.Digest(indexID)}
	response.RepoDigests = []string{"registry.example/app@" + indexID}
	return response
}

func newRuntime(t *testing.T, source *fakeSource) (*imageexec.Admitter, *imageexec.DockerCreator) {
	t.Helper()
	admitter, creator, err := imageexec.NewDockerRuntime(source)
	if err != nil {
		t.Fatal(err)
	}
	return admitter, creator
}

func safeRuntime(t *testing.T) (*imageexec.Admitter, *imageexec.DockerCreator, *fakeSource, imageexec.Image) {
	t.Helper()
	source := &fakeSource{version: "1.51", inspect: func(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
		return classicImage(), nil
	}}
	a, c := newRuntime(t, source)
	i, err := a.Admit(t.Context(), "registry.example/app:latest")
	if err != nil {
		t.Fatal(err)
	}
	return a, c, source, i
}

func TestAdmissionRejectsUnavailableCapabilitiesAndEmptyReference(t *testing.T) {
	var typedNil *fakeSource
	for _, source := range []imageexec.DockerSource{nil, typedNil} {
		if _, _, err := imageexec.NewDockerRuntime(source); !errors.Is(err, imageexec.ErrUnavailable) {
			t.Fatalf("constructor error = %v", err)
		}
	}
	var zero imageexec.Admitter
	if _, err := zero.Admit(t.Context(), "app"); !errors.Is(err, imageexec.ErrUnavailable) {
		t.Fatalf("zero admitter error = %v", err)
	}
	a, _ := newRuntime(t, &fakeSource{version: "1.51", inspect: func(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
		t.Fatal("invalid reference or canceled admission reached image inspection")
		return dockerimage.InspectResponse{}, nil
	}})
	if _, err := a.Admit(t.Context(), ""); err == nil {
		t.Fatal("empty reference admitted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.Admit(ctx, "app"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission error = %v", err)
	}
}

func TestAdmissionCopiesClassicMetadataWithoutPulling(t *testing.T) {
	response := classicImage()
	source := &fakeSource{version: "1.51", inspect: func(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
		return response, nil
	}}
	a, _ := newRuntime(t, source)
	i, err := a.Admit(t.Context(), "app:latest")
	if err != nil {
		t.Fatal(err)
	}
	response.Config.User = "attacker"
	response.Config.Volumes["/mutated"] = struct{}{}
	volumes := i.Volumes()
	volumes[0] = "/caller-mutation"
	platform := i.Platform()
	platform.OS = "changed"
	if i.ID() != imageID || i.Reference() != "app:latest" || i.User() != "123:456" || i.Platform().OS != "linux" ||
		!reflect.DeepEqual(i.Volumes(), []string{"/a", "/z"}) {
		t.Fatalf("admitted metadata changed: id=%s ref=%s user=%s platform=%+v volumes=%v", i.ID(), i.Reference(), i.User(), i.Platform(), i.Volumes())
	}
}

func TestAdmissionRejectsUntrustedMetadata(t *testing.T) {
	tests := []struct {
		name    string
		version string
		change  func(*dockerimage.InspectResponse)
		want    string
	}{
		{name: "old API", version: "1.48", want: "API 1.49"},
		{name: "malformed ID", change: func(r *dockerimage.InspectResponse) { r.ID = "mutable:tag" }, want: "invalid immutable"},
		{name: "non sha256 ID", change: func(r *dockerimage.InspectResponse) { r.ID = "sha512:" + strings.Repeat("a", 128) }, want: "invalid immutable"},
		{name: "missing config", change: func(r *dockerimage.InspectResponse) { r.Config = nil }, want: "no runnable"},
		{name: "missing OS", change: func(r *dockerimage.InspectResponse) { r.Os = "" }, want: "no runnable"},
		{name: "missing architecture", change: func(r *dockerimage.InspectResponse) { r.Architecture = "" }, want: "no runnable"},
		{name: "mismatched descriptor", change: func(r *dockerimage.InspectResponse) {
			r.Descriptor = &ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.Digest(indexID)}
		}, want: "differs"},
		{name: "unknown media type", change: func(r *dockerimage.InspectResponse) {
			r.Descriptor = &ocispec.Descriptor{MediaType: "application/unknown", Digest: digest.Digest(imageID)}
		}, want: "unsupported"},
	}
	for _, label := range []string{
		"fred.lease_id", "TrAeFiK.enable", "com.docker.compose.project", "COM.DOCKER.COMPOSE.replace",
		"traefiK.enable", "com.docKer.compose.project", "com.docker.compoſe.project",
	} {
		tests = append(tests, struct {
			name    string
			version string
			change  func(*dockerimage.InspectResponse)
			want    string
		}{name: label, change: func(r *dockerimage.InspectResponse) { r.Config.Labels[label] = "attacker" }, want: "reserved label"})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := classicImage()
			if tt.change != nil {
				tt.change(&response)
			}
			version := tt.version
			if version == "" {
				version = "1.51"
			}
			a, _ := newRuntime(t, &fakeSource{version: version, inspect: func(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
				return response, nil
			}})
			i, err := a.Admit(t.Context(), "app:latest")
			if err == nil || !strings.Contains(err.Error(), tt.want) || i.ID() != "" {
				t.Fatalf("Admit = %q, %v; want %q and no capability", i.ID(), err, tt.want)
			}
		})
	}
}

func TestAdmissionMaterializesSelectedLeafWithoutResolvingTagAgain(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(map[bool]string{false: "materialize", true: "already local"}[present], func(t *testing.T) {
			var inspections []string
			var pulls []string
			local := present
			source := &fakeSource{version: "1.51"}
			source.inspect = func(_ context.Context, ref string, opts ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
				inspections = append(inspections, ref)
				switch ref {
				case "registry.example/app:latest":
					if len(inspections) != 1 {
						t.Fatal("mutable reference resolved again after admission began")
					}
					return indexImage(), nil
				case indexID:
					if len(opts) != 1 {
						t.Fatal("platform-specific inspection required")
					}
					return leafImage(), nil
				case imageID:
					if !local {
						return dockerimage.InspectResponse{}, errdefs.NotFound(errors.New("leaf has no standalone record"))
					}
					return leafImage(), nil
				default:
					t.Fatalf("unexpected inspect %q", ref)
					return dockerimage.InspectResponse{}, nil
				}
			}
			source.pull = func(_ context.Context, ref string, _ dockerimage.PullOptions) (io.ReadCloser, error) {
				pulls = append(pulls, ref)
				local = true
				return io.NopCloser(strings.NewReader("{\"status\":\"complete\"}\n")), nil
			}
			a, _ := newRuntime(t, source)
			i, err := a.Admit(t.Context(), "registry.example/app:latest")
			if err != nil || i.ID() != imageID || i.Reference() != "registry.example/app:latest" {
				t.Fatalf("Admit = %q, %v", i.ID(), err)
			}
			wantInspections := []string{"registry.example/app:latest", indexID, imageID}
			if !present {
				wantInspections = append(wantInspections, imageID)
				if !reflect.DeepEqual(pulls, []string{"registry.example/app@" + imageID}) {
					t.Fatalf("pulls = %v", pulls)
				}
			} else if len(pulls) != 0 {
				t.Fatalf("present immutable leaf required registry access: %v", pulls)
			}
			if !reflect.DeepEqual(inspections, wantInspections) {
				t.Fatalf("inspections = %v; want %v", inspections, wantInspections)
			}
		})
	}
}

func TestAdmissionNeverMintsFromFailedMaterialization(t *testing.T) {
	for _, output := range []string{"{\"error\":\"registry denied\"}", "{\"errorDetail\":{\"message\":\"registry denied\"}}", "invalid JSON"} {
		t.Run(output, func(t *testing.T) {
			source := &fakeSource{version: "1.51", inspect: func(_ context.Context, ref string, _ ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
				switch ref {
				case "app:latest":
					return indexImage(), nil
				case indexID:
					return leafImage(), nil
				default:
					return dockerimage.InspectResponse{}, errdefs.NotFound(errors.New("absent"))
				}
			}, pull: func(context.Context, string, dockerimage.PullOptions) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(output)), nil
			}}
			a, _ := newRuntime(t, source)
			i, err := a.Admit(t.Context(), "app:latest")
			if err == nil || i.ID() != "" {
				t.Fatalf("failed pull minted capability: %s, %v", i.ID(), err)
			}
		})
	}
}

func TestAdmissionDoesNotPullAfterOtherInspectionFailure(t *testing.T) {
	for _, failure := range []error{context.Canceled, errdefs.Forbidden(errors.New("denied"))} {
		source := &fakeSource{version: "1.51", inspect: func(_ context.Context, ref string, _ ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
			switch ref {
			case "app:latest":
				return indexImage(), nil
			case indexID:
				return leafImage(), nil
			default:
				return dockerimage.InspectResponse{}, failure
			}
		}, pull: func(context.Context, string, dockerimage.PullOptions) (io.ReadCloser, error) {
			t.Fatal("inspection refusal triggered registry mutation")
			return nil, nil
		}}
		a, _ := newRuntime(t, source)
		if _, err := a.Admit(t.Context(), "app:latest"); !errors.Is(err, failure) {
			t.Fatalf("Admit error = %v; want %v", err, failure)
		}
	}
}
