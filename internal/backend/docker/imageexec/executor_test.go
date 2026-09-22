package imageexec_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	composeapi "github.com/docker/compose/v5/pkg/api"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

func TestDockerCreatorOwnsExecutionIdentity(t *testing.T) {
	_, creator, source, admitted := safeRuntime(t)
	config := &container.Config{Image: "attacker:latest", Labels: map[string]string{
		imageexec.LabelImageReference: "attacker:latest", imageexec.LabelImageID: "forged", "tenant": "value",
	}}
	host := &container.HostConfig{}
	networks := &network.NetworkingConfig{}
	source.create = func(_ context.Context, actual *container.Config, actualHost *container.HostConfig, actualNetworks *network.NetworkingConfig, platform *ocispec.Platform, name string) (container.CreateResponse, error) {
		if actual.Image != admitted.ID() || actual.Labels[imageexec.LabelImageReference] != admitted.Reference() ||
			actual.Labels[imageexec.LabelImageID] != admitted.ID() || actual.Labels["tenant"] != "value" {
			t.Fatalf("unbound execution config: %+v", actual)
		}
		if actualHost != host || actualNetworks != networks || name != "workload" || platform.OS != "linux" || platform.Architecture != "amd64" {
			t.Fatalf("create options changed: host=%p networks=%p name=%s platform=%+v", actualHost, actualNetworks, name, platform)
		}
		actual.Image = "changed-by-sdk"
		actual.Labels[imageexec.LabelImageReference] = "changed-by-sdk"
		platform.OS = "changed-by-sdk"
		return container.CreateResponse{ID: "created"}, nil
	}
	response, err := creator.Create(t.Context(), admitted, config, host, networks, "workload")
	if err != nil || response.ID != "created" {
		t.Fatalf("Create = %+v, %v", response, err)
	}
	if config.Image != "attacker:latest" || config.Labels[imageexec.LabelImageReference] != "attacker:latest" || config.Labels[imageexec.LabelImageID] != "forged" || admitted.Platform().OS != "linux" {
		t.Fatalf("creation mutated caller config or image: %+v", config)
	}
}

func TestDockerCreatorRejectsZeroForeignAndCanceledCapabilities(t *testing.T) {
	_, creator, source, admitted := safeRuntime(t)
	_, _, _, foreign := safeRuntime(t)
	source.create = func(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *ocispec.Platform, string) (container.CreateResponse, error) {
		t.Fatal("invalid capability reached raw Docker creation")
		return container.CreateResponse{}, nil
	}
	// Exported getters and JSON fields cannot reconstruct an admitted image.
	var forged imageexec.Image
	if err := json.Unmarshal([]byte(`{"ID":"`+admitted.ID()+`","Reference":"`+admitted.Reference()+`"}`), &forged); err != nil { //nolint:staticcheck // SA9005: deliberately verify JSON cannot manufacture an execution capability.
		t.Fatal(err)
	}
	for _, tc := range []struct {
		image imageexec.Image
		want  error
	}{{imageexec.Image{}, imageexec.ErrInvalidImage}, {forged, imageexec.ErrInvalidImage}, {foreign, imageexec.ErrForeignImage}} {
		if _, err := creator.Create(t.Context(), tc.image, nil, nil, nil, "test"); !errors.Is(err, tc.want) {
			t.Fatalf("Create error = %v; want %v", err, tc.want)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := creator.Create(ctx, admitted, nil, nil, nil, "test"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled create error = %v", err)
	}
	var zero imageexec.DockerCreator
	if _, err := zero.Create(t.Context(), admitted, nil, nil, nil, "test"); !errors.Is(err, imageexec.ErrUnavailable) {
		t.Fatalf("zero creator error = %v", err)
	}
}

func desiredProject(reference string) *composetypes.Project {
	return &composetypes.Project{Name: "lease", Services: composetypes.Services{
		"app": {Name: "app", Image: reference, Labels: composetypes.Labels{"tenant": "original"},
			CustomLabels: composetypes.Labels{imageexec.LabelImageReference: "forged"}},
	}}
}

func TestCompileRequiresCompleteUnambiguousImageAssignment(t *testing.T) {
	a, _, _, admitted := safeRuntime(t)
	_, _, _, foreign := safeRuntime(t)
	tests := []struct {
		name   string
		change func(*composetypes.Project, map[string]imageexec.Image)
	}{
		{name: "missing image", change: func(_ *composetypes.Project, images map[string]imageexec.Image) { delete(images, "app") }},
		{name: "wrong service key", change: func(_ *composetypes.Project, images map[string]imageexec.Image) {
			delete(images, "app")
			images["other"] = admitted
		}},
		{name: "extra image", change: func(_ *composetypes.Project, images map[string]imageexec.Image) { images["extra"] = admitted }},
		{name: "foreign image", change: func(_ *composetypes.Project, images map[string]imageexec.Image) { images["app"] = foreign }},
		{name: "zero image", change: func(_ *composetypes.Project, images map[string]imageexec.Image) { images["app"] = imageexec.Image{} }},
		{name: "wrong reference", change: func(p *composetypes.Project, _ map[string]imageexec.Image) {
			s := p.Services["app"]
			s.Image = "other:tag"
			p.Services["app"] = s
		}},
		{name: "build", change: func(p *composetypes.Project, _ map[string]imageexec.Image) {
			s := p.Services["app"]
			s.Build = &composetypes.BuildConfig{Context: "."}
			p.Services["app"] = s
		}},
		{name: "disabled service", change: func(p *composetypes.Project, _ map[string]imageexec.Image) {
			p.DisabledServices = composetypes.Services{"other": {Name: "other", Image: "other:tag"}}
		}},
		{name: "unnamed project", change: func(p *composetypes.Project, _ map[string]imageexec.Image) { p.Name = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			project := desiredProject(admitted.Reference())
			images := map[string]imageexec.Image{"app": admitted}
			tt.change(project, images)
			prepared, err := a.Compile(project, images)
			if err == nil || prepared.Name() != "" {
				t.Fatalf("invalid project compiled: name=%q, error=%v", prepared.Name(), err)
			}
		})
	}
}

func TestPreparedProjectIsSealedAcrossCallerAndSDKMutations(t *testing.T) {
	a, _, _, admitted := safeRuntime(t)
	desired := desiredProject(admitted.Reference())
	images := map[string]imageexec.Image{"app": admitted}
	prepared, err := a.Compile(desired, images)
	if err != nil {
		t.Fatal(err)
	}
	if desired.Services["app"].Image != admitted.Reference() || desired.Services["app"].CustomLabels[imageexec.LabelImageReference] != "forged" {
		t.Fatal("compilation mutated caller project")
	}
	desired.Name = "mutated-name"
	service := desired.Services["app"]
	service.Image = "attacker:latest"
	service.Labels["tenant"] = "mutated-label"
	service.CustomLabels[imageexec.LabelImageID] = "mutated-id"
	desired.Services["app"] = service
	delete(images, "app")
	calls := 0
	executor, err := a.NewComposeExecutor(func(_ context.Context, project *composetypes.Project, options composeapi.UpOptions) error {
		calls++
		service := project.Services["app"]
		if project.Name != "lease" || service.Image != admitted.ID() || service.Platform != "linux/amd64" || service.PullPolicy != composetypes.PullPolicyNever ||
			service.Labels["tenant"] != "original" || service.Labels[imageexec.LabelImageReference] != admitted.Reference() || service.Labels[imageexec.LabelImageID] != admitted.ID() ||
			service.CustomLabels[imageexec.LabelImageReference] != admitted.Reference() || service.CustomLabels[imageexec.LabelImageID] != admitted.ID() {
			t.Fatalf("execution %d received altered plan: project=%s service=%+v", calls, project.Name, service)
		}
		wantRecreate := composeapi.RecreateDiverged
		if calls == 2 {
			wantRecreate = composeapi.RecreateForce
		}
		if options.Create.Recreate != wantRecreate || !options.Create.QuietPull || !options.Create.RemoveOrphans || options.Start.Project != nil {
			t.Fatalf("unexpected Up options: %+v", options)
		}
		project.Name = "sdk-mutation"
		service.Image = "sdk-mutation"
		service.Labels["tenant"] = "sdk-mutation"
		service.CustomLabels[imageexec.LabelImageID] = "sdk-mutation"
		project.Services["app"] = service
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.Up(t.Context(), prepared, false); err != nil {
		t.Fatal(err)
	}
	if err := executor.Up(t.Context(), prepared, true); err != nil {
		t.Fatal(err)
	}
	if prepared.Name() != "lease" || calls != 2 {
		t.Fatalf("sealed plan changed: name=%q calls=%d", prepared.Name(), calls)
	}
}

func TestComposeExecutorRejectsZeroForeignAndCanceledPlans(t *testing.T) {
	a, _, _, admitted := safeRuntime(t)
	other, _, _, foreign := safeRuntime(t)
	prepared, err := a.Compile(desiredProject(admitted.Reference()), map[string]imageexec.Image{"app": admitted})
	if err != nil {
		t.Fatal(err)
	}
	foreignProject, err := other.Compile(desiredProject(foreign.Reference()), map[string]imageexec.Image{"app": foreign})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := a.NewComposeExecutor(func(context.Context, *composetypes.Project, composeapi.UpOptions) error {
		t.Fatal("invalid capability reached raw Compose Up")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		project imageexec.PreparedProject
		want    error
	}{{imageexec.PreparedProject{}, imageexec.ErrInvalidProject}, {foreignProject, imageexec.ErrForeignProject}} {
		if err := executor.Up(t.Context(), tt.project, false); !errors.Is(err, tt.want) {
			t.Fatalf("Up error = %v; want %v", err, tt.want)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := executor.Up(ctx, prepared, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Up error = %v", err)
	}
	var zero imageexec.ComposeExecutor
	if err := zero.Up(t.Context(), prepared, false); !errors.Is(err, imageexec.ErrUnavailable) {
		t.Fatalf("zero executor error = %v", err)
	}
}
