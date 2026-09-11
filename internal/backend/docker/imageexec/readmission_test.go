package imageexec_test

import (
	"context"
	"testing"

	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

func TestReAdmitUsesOnlyExactContentAndPreservesSourceReference(t *testing.T) {
	const reference = "registry.example/app:old-tag"
	source := &fakeSource{version: "1.51", inspect: func(_ context.Context, ref string, _ ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
		if ref != imageID {
			t.Fatalf("resolved mutable source reference %q", ref)
		}
		return classicImage(), nil
	}}
	admitter, creator := newRuntime(t, source)
	image, err := admitter.ReAdmit(t.Context(), imageID, ocispec.Platform{OS: "linux", Architecture: "amd64"}, reference)
	if err != nil {
		t.Fatal(err)
	}
	source.create = func(_ context.Context, config *container.Config, _ *container.HostConfig, _ *network.NetworkingConfig, platform *ocispec.Platform, _ string) (container.CreateResponse, error) {
		if config.Image != imageID || config.Labels[imageexec.LabelImageReference] != reference || platform.Architecture != "amd64" {
			t.Fatalf("source identity changed: %#v %#v", config, platform)
		}
		return container.CreateResponse{ID: "restored"}, nil
	}
	if _, err := creator.Create(t.Context(), image, &container.Config{Image: "attacker/replacement"}, nil, nil, "source"); err != nil {
		t.Fatal(err)
	}
	if _, err := admitter.ReAdmit(t.Context(), imageID, ocispec.Platform{OS: "linux", Architecture: "arm64"}, reference); err == nil {
		t.Fatal("different execution platform accepted")
	}
}
