package imageexec_test

import (
	"context"
	"errors"
	"testing"

	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

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

func TestReAdmitRefusesUnavailableOrMalformedPersistedIdentity(t *testing.T) {
	failure := errors.New("persisted image is unavailable")
	for _, scenario := range []struct {
		name      string
		id        string
		reference string
		inspects  int
		want      string
	}{
		{name: "mutable identity", id: "app:latest", reference: "app:old", want: "invalid persisted execution identity"},
		{name: "invalid display reference", id: imageID, reference: "invalid reference", want: "invalid persisted image reference"},
		{name: "missing exact image", id: imageID, reference: "app:old", inspects: 1, want: failure.Error()},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			inspects := 0
			source := &fakeSource{version: "1.51", inspect: func(_ context.Context, ref string, _ ...client.ImageInspectOption) (dockerimage.InspectResponse, error) {
				inspects++
				require.Equal(t, imageID, ref, "recovery must never resolve the mutable display reference")
				return dockerimage.InspectResponse{}, failure
			}}
			admitter, _ := newRuntime(t, source)
			admitted, err := admitter.ReAdmit(t.Context(), scenario.id, ocispec.Platform{OS: "linux", Architecture: "amd64"}, scenario.reference)
			require.ErrorContains(t, err, scenario.want)
			require.Empty(t, admitted.ID())
			require.Equal(t, scenario.inspects, inspects)
		})
	}
}
