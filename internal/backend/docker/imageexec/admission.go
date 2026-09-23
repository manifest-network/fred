package imageexec

import (
	"context"
	"fmt"
	"slices"

	"github.com/containerd/platforms"
	"github.com/distribution/reference"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Source is the read-only image-store capability captured by an Admitter. It
// cannot fetch images or create or start containers.
type Source interface {
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error)
}

// Admitter owns the source from which images are admitted. Creators bound to
// this admitter accept only the images and projects it minted.
type Admitter struct {
	issuer *issuer
}

// ReAdmit restores a persisted execution identity against this runtime. The
// original reference is display metadata only: it is validated but never
// resolved or pulled. Creation still consumes the newly admitted immutable ID.
func (a *Admitter) ReAdmit(ctx context.Context, id string, platform ocispec.Platform, originalReference string) (Image, error) {
	parsed, err := digest.Parse(id)
	if err != nil || parsed.Algorithm() != digest.SHA256 {
		return Image{}, fmt.Errorf("invalid persisted execution identity")
	}
	if _, err := reference.ParseAnyReference(originalReference); err != nil {
		return Image{}, fmt.Errorf("invalid persisted image reference: %w", err)
	}
	image, err := a.Admit(ctx, id)
	if err != nil {
		return Image{}, err
	}
	if image.ID() != id || !platforms.OnlyStrict(platform).Match(image.Platform()) {
		return Image{}, fmt.Errorf("persisted execution identity or platform is unavailable")
	}
	record := *image.record
	record.reference = originalReference
	return Image{record: &record}, nil
}

type inspectedImage struct {
	id          string
	platform    ocispec.Platform
	user        string
	volumes     []string
	descriptor  *ocispec.Descriptor
	repoDigests []string
}

// MaterializationRequired identifies a selected immutable platform manifest
// without granting execution authority. Its content must pass the caller's
// bounded ingestion before ReAdmit can independently inspect the local image.
type MaterializationRequired struct {
	reference string
	id        string
}

func (m *MaterializationRequired) Error() string {
	return fmt.Sprintf("image platform manifest %s requires local materialization", m.id)
}

// Reference is the immutable repository reference for bounded ingestion.
func (m *MaterializationRequired) Reference() string { return m.reference }

// ID is the exact selected manifest identity required for subsequent admission.
func (m *MaterializationRequired) ID() string { return m.id }

// Admit resolves and validates only local image content. An index whose selected
// leaf lacks an independently addressable local record returns
// MaterializationRequired without an Image. Mutable references are never
// resolved again after the initial inspection.
func (a *Admitter) Admit(ctx context.Context, imageReference string) (Image, error) {
	if a == nil || a.issuer == nil {
		return Image{}, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return Image{}, err
	}
	if imageReference == "" {
		return Image{}, fmt.Errorf("image reference must not be empty")
	}
	initial, err := a.inspect(ctx, imageReference, nil)
	if err != nil {
		return Image{}, fmt.Errorf("inspect image %s: %w", imageReference, err)
	}
	if !isIndex(initial.descriptor) {
		return a.mint(imageReference, initial), nil
	}
	selected, err := a.inspect(ctx, initial.id, &initial.platform)
	if err != nil {
		return Image{}, fmt.Errorf("resolve image platform manifest: %w", err)
	}
	if selected.descriptor == nil || isIndex(selected.descriptor) || selected.id == initial.id {
		return Image{}, fmt.Errorf("image platform inspection did not resolve an immutable leaf manifest")
	}
	local, err := a.inspect(ctx, selected.id, nil)
	if err == nil {
		if err := sameLeaf(selected, local); err != nil {
			return Image{}, err
		}
		return a.mint(imageReference, local), nil
	}
	if !errdefs.IsNotFound(err) {
		return Image{}, fmt.Errorf("inspect selected platform manifest: %w", err)
	}
	materializationReference, err := manifestMaterializationReference(imageReference, initial.repoDigests, selected.id)
	if err != nil {
		return Image{}, err
	}
	return Image{}, &MaterializationRequired{reference: materializationReference, id: selected.id}
}

func (a *Admitter) mint(imageReference string, inspected inspectedImage) Image {
	return Image{record: &imageRecord{
		issuer: a.issuer, reference: imageReference, id: inspected.id,
		platform: clonePlatform(inspected.platform), user: inspected.user,
		volumes: slices.Clone(inspected.volumes),
	}}
}

func (a *Admitter) inspect(ctx context.Context, imageReference string, platform *ocispec.Platform) (inspectedImage, error) {
	var opts []client.ImageInspectOption
	if platform != nil {
		opts = append(opts, client.ImageInspectWithPlatform(platform))
	}
	response, err := a.issuer.source.ImageInspect(ctx, imageReference, opts...)
	if err != nil {
		return inspectedImage{}, err
	}
	if response.Config == nil || response.Os == "" || response.Architecture == "" {
		return inspectedImage{}, fmt.Errorf("image inspection returned no runnable image config")
	}
	parsed, err := digest.Parse(response.ID)
	if err != nil || parsed.Algorithm() != digest.SHA256 {
		return inspectedImage{}, fmt.Errorf("image inspection returned an invalid immutable image ID %q", response.ID)
	}
	metadata, err := admitImageMetadata(response.Config.Labels, response.Config.Volumes)
	if err != nil {
		return inspectedImage{}, err
	}
	// Classic stores return a config ID without a descriptor; multi-platform
	// stores include their target descriptor. Runtime construction rules out
	// older APIs that hide descriptors and erase this distinction.
	if response.Descriptor != nil {
		if response.Descriptor.Digest.String() != response.ID {
			return inspectedImage{}, fmt.Errorf("image descriptor differs from its immutable ID")
		}
		if !isIndex(response.Descriptor) && response.Descriptor.MediaType != ocispec.MediaTypeImageManifest &&
			response.Descriptor.MediaType != "application/vnd.docker.distribution.manifest.v2+json" {
			return inspectedImage{}, fmt.Errorf("unsupported image descriptor media type %q", response.Descriptor.MediaType)
		}
	}
	return inspectedImage{
		id: response.ID, descriptor: response.Descriptor, repoDigests: slices.Clone(response.RepoDigests),
		platform: ocispec.Platform{OS: response.Os, Architecture: response.Architecture,
			Variant: response.Variant, OSVersion: response.OsVersion},
		user: response.Config.User, volumes: metadata.volumes,
	}, nil
}

func isIndex(descriptor *ocispec.Descriptor) bool {
	return descriptor != nil && (descriptor.MediaType == ocispec.MediaTypeImageIndex ||
		descriptor.MediaType == "application/vnd.docker.distribution.manifest.list.v2+json")
}

func sameLeaf(selected, local inspectedImage) error {
	if local.descriptor == nil || isIndex(local.descriptor) || local.id != selected.id {
		return fmt.Errorf("materialized image differs from checked manifest")
	}
	return nil
}

func manifestMaterializationReference(imageReference string, repoDigests []string, imageID string) (string, error) {
	ref, err := reference.ParseAnyReference(imageReference)
	if err != nil {
		return "", fmt.Errorf("parse image reference for platform resolution: %w", err)
	}
	named, ok := ref.(reference.Named)
	if !ok {
		for _, candidate := range repoDigests {
			parsed, parseErr := reference.ParseNormalizedNamed(candidate)
			if parseErr == nil {
				named = parsed
				break
			}
		}
	}
	if named == nil {
		return "", fmt.Errorf("selected image manifest has no repository reference for materialization")
	}
	pinned, err := reference.WithDigest(reference.TrimNamed(named), digest.Digest(imageID))
	if err != nil {
		return "", fmt.Errorf("pin image platform manifest: %w", err)
	}
	return pinned.String(), nil
}
