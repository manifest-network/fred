package imageexec

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/containerd/platforms"

	"github.com/distribution/reference"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// Source is the image-store capability captured by an Admitter. It cannot
// create or start containers. Pulling is restricted here to a selected immutable
// manifest whose contents have already passed admission.
type Source interface {
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (dockerimage.InspectResponse, error)
	ImagePull(context.Context, string, dockerimage.PullOptions) (io.ReadCloser, error)
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

// pendingMaterialization never escapes the admission package and cannot be
// supplied to a creation sink. Its leaf must be resolved independently first.
type pendingMaterialization struct {
	leaf          inspectedImage
	pullReference string
}

// Admit resolves and validates the image that will execute. An index's selected
// leaf is materialized by immutable digest when necessary; mutable references
// are never resolved again after the initial inspection. This method must run
// inside the caller's existing image preparation mutation capability.
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
	pullReference, err := manifestPullReference(imageReference, initial.repoDigests, selected.id)
	if err != nil {
		return Image{}, err
	}
	local, err = a.materialize(ctx, pendingMaterialization{leaf: selected, pullReference: pullReference})
	if err != nil {
		return Image{}, err
	}
	return a.mint(imageReference, local), nil
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
	for _, key := range slices.Sorted(maps.Keys(response.Config.Labels)) {
		if manifest.IsReservedLabelKey(key) {
			return inspectedImage{}, fmt.Errorf("image contains reserved label %q", key)
		}
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
		user: response.Config.User, volumes: slices.Sorted(maps.Keys(response.Config.Volumes)),
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

func (a *Admitter) materialize(ctx context.Context, pending pendingMaterialization) (inspectedImage, error) {
	reader, err := a.issuer.source.ImagePull(ctx, pending.pullReference, dockerimage.PullOptions{})
	if err != nil {
		return inspectedImage{}, fmt.Errorf("materialize checked image manifest: %w", err)
	}
	defer func() { _ = reader.Close() }()
	decoder := json.NewDecoder(reader)
	for {
		var progress struct {
			Error       string `json:"error"`
			ErrorDetail *struct {
				Message string `json:"message"`
			} `json:"errorDetail"`
		}
		if err := decoder.Decode(&progress); err != nil {
			if err == io.EOF {
				break
			}
			return inspectedImage{}, fmt.Errorf("read manifest pull output: %w", err)
		}
		if progress.Error != "" {
			return inspectedImage{}, fmt.Errorf("manifest pull failed: %s", progress.Error)
		}
		if progress.ErrorDetail != nil && progress.ErrorDetail.Message != "" {
			return inspectedImage{}, fmt.Errorf("manifest pull failed: %s", progress.ErrorDetail.Message)
		}
	}
	local, err := a.inspect(ctx, pending.leaf.id, nil)
	if err != nil {
		return inspectedImage{}, fmt.Errorf("inspect materialized manifest: %w", err)
	}
	if err := sameLeaf(pending.leaf, local); err != nil {
		return inspectedImage{}, err
	}
	return local, nil
}

func manifestPullReference(imageReference string, repoDigests []string, imageID string) (string, error) {
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
