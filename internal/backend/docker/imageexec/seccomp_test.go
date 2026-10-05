package imageexec_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	composeapi "github.com/docker/compose/v5/pkg/api"
	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/docker/tenantseccomp"
)

// scriptedProfiles answers each request from a script, then from the process
// source. It stands in for a source whose build or sealed file fails.
type scriptedProfiles struct {
	failures int
	zero     bool
	calls    int
}

func (s *scriptedProfiles) TenantSeccompProfile() (tenantseccomp.Profile, error) {
	s.calls++
	if s.zero {
		return tenantseccomp.Profile{}, nil
	}
	if s.calls <= s.failures {
		return tenantseccomp.Profile{}, fmt.Errorf("%w: scripted failure", tenantseccomp.ErrRefused)
	}
	return tenantseccomp.Process().TenantSeccompProfile()
}

func runtimeWithProfiles(t *testing.T, profiles imageexec.TenantSeccompSource) (*imageexec.Admitter, *imageexec.DockerCreator, *fakeSource, imageexec.Image) {
	t.Helper()
	source := &fakeSource{version: "1.51", inspect: func(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
		return classicImage(), nil
	}}
	admitter, creator, err := imageexec.NewDockerRuntime(t.Context(), source, profiles)
	if err != nil {
		t.Fatal(err)
	}
	image, err := admitter.Admit(t.Context(), "registry.example/app:latest")
	if err != nil {
		t.Fatal(err)
	}
	return admitter, creator, source, image
}

func processProfile(t *testing.T) tenantseccomp.Profile {
	t.Helper()
	profile, err := tenantseccomp.Process().TenantSeccompProfile()
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestCompileBindsEveryServiceToTheTenantProfileFile(t *testing.T) {
	admitter, _, _, admitted := safeRuntime(t)
	project := desiredProject(admitted.Reference())
	service := project.Services["app"]
	service.SecurityOpt = []string{"seccomp=unconfined", "no-new-privileges:true", "seccomp:builtin", "label=disable"}
	project.Services["app"] = service
	callerOptions := slices.Clone(service.SecurityOpt)

	prepared, err := admitter.Compile(project, map[string]imageexec.Image{"app": admitted})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(project.Services["app"].SecurityOpt, callerOptions) {
		t.Fatal("compilation mutated the caller's security options")
	}
	path, err := processProfile(t).MemfdPath()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"no-new-privileges:true", "label=disable", "seccomp=" + path}
	executor, err := admitter.NewComposeExecutor(func(_ context.Context, project *composetypes.Project, _ composeapi.UpOptions) error {
		if got := project.Services["app"].SecurityOpt; !slices.Equal(got, want) {
			t.Fatalf("compiled security options = %q, want %q", got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.Up(t.Context(), prepared, false); err != nil {
		t.Fatal(err)
	}
}

func TestCompileRefusesPreStartHooks(t *testing.T) {
	admitter, _, _, admitted := safeRuntime(t)
	project := desiredProject(admitted.Reference())
	service := project.Services["app"]
	service.PreStart = []composetypes.ServiceHook{{Command: composetypes.ShellCommand{"true"}}}
	project.Services["app"] = service
	if _, err := admitter.Compile(project, map[string]imageexec.Image{"app": admitted}); err == nil || !strings.Contains(err.Error(), "pre_start") {
		t.Fatalf("Compile accepted a pre_start hook: %v", err)
	}
}

func TestCreateInjectsTheTenantProfileIntoAPrivateHostCopy(t *testing.T) {
	_, creator, source, admitted := safeRuntime(t)
	profile := processProfile(t)
	host := &container.HostConfig{CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges:true", "seccomp:unconfined"}}
	callerOptions := slices.Clone(host.SecurityOpt)
	var seen []*container.HostConfig
	source.create = func(_ context.Context, _ *container.Config, actual *container.HostConfig, _ *network.NetworkingConfig, _ *ocispec.Platform, _ string) (container.CreateResponse, error) {
		want := []string{"no-new-privileges:true", "seccomp=" + string(profile.CompactJSON())}
		if !slices.Equal(actual.SecurityOpt, want) || !slices.Equal(actual.CapDrop, []string{"ALL"}) {
			t.Fatalf("create received security options %q", actual.SecurityOpt)
		}
		seen = append(seen, actual)
		return container.CreateResponse{ID: "created"}, nil
	}
	for range 2 {
		if _, err := creator.Create(t.Context(), admitted, nil, host, nil, "workload"); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 2 || seen[0] == host || seen[1] == host || seen[0] == seen[1] {
		t.Fatal("each create must send its own private host configuration")
	}
	if !slices.Equal(host.SecurityOpt, callerOptions) {
		t.Fatalf("create mutated the caller's host configuration: %q", host.SecurityOpt)
	}

	// The inspection helper creates through the same sink.
	source.create = func(_ context.Context, _ *container.Config, actual *container.HostConfig, _ *network.NetworkingConfig, _ *ocispec.Platform, _ string) (container.CreateResponse, error) {
		if want := []string{"no-new-privileges:true", "seccomp=" + string(profile.CompactJSON())}; !slices.Equal(actual.SecurityOpt, want) {
			t.Fatalf("inspection helper security options = %q", actual.SecurityOpt)
		}
		return container.CreateResponse{ID: "helper"}, nil
	}
	if _, err := creator.ForInspection().Create(t.Context(), admitted, nil, "helper"); err != nil {
		t.Fatal(err)
	}
}

func TestCreationSinksRefuseWhileTheProfileIsUnavailable(t *testing.T) {
	for name, profiles := range map[string]*scriptedProfiles{
		"failing source": {failures: 1 << 30},
		"zero profile":   {zero: true},
	} {
		t.Run(name, func(t *testing.T) {
			admitter, creator, source, admitted := runtimeWithProfiles(t, profiles)
			source.create = func(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error) {
				t.Fatal("a creation without the tenant profile reached Docker")
				return container.CreateResponse{}, nil
			}
			if _, err := admitter.Compile(desiredProject(admitted.Reference()), map[string]imageexec.Image{"app": admitted}); !errors.Is(err, tenantseccomp.ErrRefused) {
				t.Fatalf("Compile error = %v", err)
			}
			if _, err := creator.Create(t.Context(), admitted, nil, &container.HostConfig{}, nil, "workload"); !errors.Is(err, tenantseccomp.ErrRefused) {
				t.Fatalf("Create error = %v", err)
			}
			if _, err := creator.ForInspection().Create(t.Context(), admitted, nil, "helper"); !errors.Is(err, tenantseccomp.ErrRefused) {
				t.Fatalf("inspection Create error = %v", err)
			}
		})
	}
}

// The error state is not latched: each sink asks the source again, so a
// transient failure ends as soon as the source recovers.
func TestCreationSinksRetryTheProfileSource(t *testing.T) {
	profiles := &scriptedProfiles{failures: 3} // construction, then one refusal each below
	admitter, creator, source, admitted := runtimeWithProfiles(t, profiles)
	created := 0
	source.create = func(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error) {
		created++
		return container.CreateResponse{ID: "created"}, nil
	}
	project := desiredProject(admitted.Reference())
	images := map[string]imageexec.Image{"app": admitted}
	if _, err := admitter.Compile(project, images); !errors.Is(err, tenantseccomp.ErrRefused) {
		t.Fatalf("first Compile error = %v", err)
	}
	if _, err := creator.Create(t.Context(), admitted, nil, &container.HostConfig{}, nil, "workload"); !errors.Is(err, tenantseccomp.ErrRefused) {
		t.Fatalf("first Create error = %v", err)
	}
	if _, err := admitter.Compile(project, images); err != nil {
		t.Fatalf("Compile after recovery: %v", err)
	}
	if _, err := creator.Create(t.Context(), admitted, nil, &container.HostConfig{}, nil, "workload"); err != nil || created != 1 {
		t.Fatalf("Create after recovery: created=%d err=%v", created, err)
	}
}
