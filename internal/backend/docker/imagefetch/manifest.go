package imagefetch

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/containerd/platforms"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

// manifestShape is the bounded descriptor grammar, before config verification.
// It is deliberately insufficient to issue a Resolution.
type manifestShape struct {
	mediaType string
	config    ocispec.Descriptor
	layers    []ocispec.Descriptor
}

func parseManifestShape(raw []byte, metadata int64) (manifestShape, error) {
	var manifest ocispec.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return manifestShape{}, err
	}
	if manifest.SchemaVersion != 2 || manifest.Subject != nil || manifest.ArtifactType != "" || len(manifest.Layers) > maxLayers {
		return manifestShape{}, errors.New("image manifest is not a bounded runnable image")
	}
	if manifest.MediaType == "" {
		manifest.MediaType = ocispec.MediaTypeImageManifest
		if manifest.Config.MediaType == "application/vnd.docker.container.image.v1+json" {
			manifest.MediaType = "application/vnd.docker.distribution.manifest.v2+json"
		}
	}
	if manifest.MediaType != ocispec.MediaTypeImageManifest && manifest.MediaType != "application/vnd.docker.distribution.manifest.v2+json" {
		return manifestShape{}, errors.New("unsupported image manifest media type")
	}
	if err := validDescriptor(manifest.Config); err != nil {
		return manifestShape{}, err
	}
	if manifest.Config.MediaType != ocispec.MediaTypeImageConfig && manifest.Config.MediaType != "application/vnd.docker.container.image.v1+json" {
		return manifestShape{}, errors.New("unsupported image config media type")
	}
	if manifest.Config.Size <= 0 || manifest.Config.Size > maxMetadataBytes-metadata {
		return manifestShape{}, errors.New("image config exceeds metadata budget")
	}
	seen := make(map[digest.Digest]ocispec.Descriptor)
	for _, layer := range manifest.Layers {
		if err := validDescriptor(layer); err != nil {
			return manifestShape{}, err
		}
		switch layer.MediaType {
		case ocispec.MediaTypeImageLayer, ocispec.MediaTypeImageLayerGzip, ocispec.MediaTypeImageLayerZstd,
			"application/vnd.docker.image.rootfs.diff.tar", "application/vnd.docker.image.rootfs.diff.tar.gzip":
		default:
			return manifestShape{}, errors.New("unsupported image layer media type")
		}
		if previous, ok := seen[layer.Digest]; ok && (previous.Size != layer.Size || previous.MediaType != layer.MediaType) {
			return manifestShape{}, errors.New("repeated layer descriptor differs from original blob")
		}
		seen[layer.Digest] = layer
	}
	return manifestShape{mediaType: manifest.MediaType, config: manifest.Config, layers: manifest.Layers}, nil
}

// runnableManifest binds the manifest grammar to digest-verified configuration.
// Resolution and preparation share this evidence; only layer verification can
// subsequently grant import authority. No layer bytes are fetched to mint it.
type runnableManifest struct {
	mediaType        string
	config           ocispec.Descriptor
	layers           []runnableLayer
	configBytes      []byte
	platform         ocispec.Platform
	admittedMetadata imageexec.Metadata
}

// runnableLayer is the descriptor/diffID association established by config
// admission. Preparation never reconstructs that relation from parallel lists.
type runnableLayer struct {
	descriptor ocispec.Descriptor
	diffID     digest.Digest
}

func admitRunnableManifest(shape manifestShape, config []byte, platform ocispec.Platform) (runnableManifest, error) {
	var cfg ocispec.Image
	if err := json.Unmarshal(config, &cfg); err != nil {
		return runnableManifest{}, err
	}
	if cfg.RootFS.Type != "layers" || len(cfg.RootFS.DiffIDs) != len(shape.layers) || !platforms.OnlyStrict(platform).Match(cfg.Platform) {
		return runnableManifest{}, errors.New("image config does not match the selected platform and layers")
	}
	layers := make([]runnableLayer, 0, len(shape.layers))
	seen := make(map[digest.Digest]digest.Digest)
	for index, descriptor := range shape.layers {
		diffID := cfg.RootFS.DiffIDs[index]
		if err := diffID.Validate(); err != nil || diffID.Algorithm() != digest.SHA256 {
			return runnableManifest{}, errors.New("invalid uncompressed layer digest")
		}
		if previous, exists := seen[descriptor.Digest]; exists && previous != diffID {
			return runnableManifest{}, errors.New("repeated layer descriptor has conflicting uncompressed identities")
		}
		if (descriptor.MediaType == ocispec.MediaTypeImageLayer || descriptor.MediaType == "application/vnd.docker.image.rootfs.diff.tar") && descriptor.Digest != diffID {
			return runnableManifest{}, errors.New("uncompressed layer descriptor differs from its uncompressed identity")
		}
		seen[descriptor.Digest] = diffID
		layers = append(layers, runnableLayer{descriptor: descriptor, diffID: diffID})
	}
	metadata, err := imageexec.AdmitMetadata(cfg.Config.Labels, cfg.Config.Volumes)
	if err != nil {
		return runnableManifest{}, fmt.Errorf("admit image configuration: %w", err)
	}
	return runnableManifest{mediaType: shape.mediaType, config: shape.config, layers: layers, configBytes: config, platform: cfg.Platform, admittedMetadata: metadata}, nil
}
