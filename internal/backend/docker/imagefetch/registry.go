package imagefetch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"

	"github.com/containerd/platforms"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	maxMetadataBytes = int64(2 << 20)
	maxLayers        = 128
	maxIndexDepth    = 8
	maxIndexEntries  = 256
)

// Resolution binds one immutable manifest to digest-verified configuration and
// an admitted platform/layer schema. Its zero value carries no authority;
// copies retain the same original evidence. It grants no layer/import budget.
type Resolution struct{ state *resolvedManifest }
type resolvedManifest struct {
	issuer   *Loader
	named    name.Reference
	raw      []byte
	digest   digest.Digest
	manifest runnableManifest
	platform ocispec.Platform
	metadata int64
}

func (r Resolution) SourceReference() string {
	if r.state == nil {
		return ""
	}
	return r.state.named.Context().Digest(r.state.digest.String()).Name()
}
func (r Resolution) ManifestID() string {
	if r.state == nil {
		return ""
	}
	return r.state.digest.String()
}

// ConfigID identifies the exact image config selected through the verified
// manifest. Classic Docker can address already-extracted content by this ID
// even when its repository digests contain only the multi-platform index.
// It is selection evidence only; the local admitter still validates the image.
func (r Resolution) ConfigID() string {
	if r.state == nil {
		return ""
	}
	return r.state.manifest.config.Digest.String()
}
func (r Resolution) Platform() ocispec.Platform {
	if r.state == nil {
		return ocispec.Platform{}
	}
	return clonePlatform(r.state.platform)
}

// Resolve verifies bounded manifest/config metadata without downloading layers.
func (l *Loader) Resolve(ctx context.Context, ref string, platform ocispec.Platform) (Resolution, error) {
	if l == nil {
		return Resolution{}, errors.New("image loader is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return Resolution{}, err
	}
	if platform.OS != "linux" || platform.Architecture == "" {
		return Resolution{}, errors.New("image preparation requires an explicit Linux platform")
	}
	named, err := name.ParseReference(ref)
	if err != nil {
		return Resolution{}, fmt.Errorf("parse registry reference: %w", err)
	}
	metadata := int64(0)
	raw, id, err := l.selectManifest(ctx, named, platform, &metadata)
	if err != nil {
		return Resolution{}, err
	}
	shape, err := parseManifestShape(raw, metadata)
	if err != nil {
		return Resolution{}, err
	}
	config, err := l.resolveConfig(ctx, named, shape.config)
	if err != nil {
		return Resolution{}, err
	}
	manifest, err := admitRunnableManifest(shape, config, platform)
	if err != nil {
		return Resolution{}, err
	}
	l.configs.retain(config)
	metadata += int64(len(config.raw))
	return Resolution{state: &resolvedManifest{issuer: l, named: named, raw: raw, digest: id, manifest: manifest, platform: clonePlatform(platform), metadata: metadata}}, nil
}

// Prepare performs bounded registry I/O and decompression without extracting
// anything into Docker or the host filesystem.
func (l *Loader) Prepare(ctx context.Context, ref string, platform ocispec.Platform) (*Prepared, error) {
	resolved, err := l.Resolve(ctx, ref, platform)
	if err != nil {
		return nil, err
	}
	return l.PrepareResolved(ctx, resolved)
}

// PrepareResolved verifies the exact selection without resolving its tag again.
// Staged blobs are unlinked while open, so a crash releases their disk space.
func (l *Loader) PrepareResolved(ctx context.Context, resolution Resolution) (*Prepared, error) {
	if l == nil || resolution.state == nil || resolution.state.issuer != l {
		return nil, errors.New("invalid or foreign image resolution")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	selected := resolution.state
	named := selected.named
	rawManifest, manifestID, metadata := selected.raw, selected.digest, selected.metadata
	dir, err := os.MkdirTemp(l.stageRoot, ".fred-image-")
	if err != nil {
		return nil, err
	}
	state := &preparedState{issuer: l, dir: dir}
	p := &Prepared{state: state}
	transferred := false
	defer func() {
		if !transferred {
			_ = p.Close()
		}
	}()
	manifest := selected.manifest
	config := manifest.configBytes
	state.metadata = manifest.admittedMetadata
	state.imported = Imported{manifest: manifestID.String(), config: manifest.config.Digest.String(), source: named.Context().Digest(manifestID.String()).Name(), platform: manifest.platform}
	state.blobs = append(state.blobs,
		blob{name: blobPath(manifestID), size: int64(len(rawManifest)), data: rawManifest},
		blob{name: blobPath(manifest.config.Digest), size: int64(len(config)), data: config})
	stageBytes := metadata
	expansion := newLayerBudget(l.budget)
	seen := make(map[digest.Digest]blob)
	layerNames := make([]string, 0, len(manifest.layers))
	for index, layer := range manifest.layers {
		descriptor := layer.descriptor
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		previous, exists := seen[descriptor.Digest]
		var b blob
		if exists {
			b = previous
		} else {
			if descriptor.Size > l.budget.Bytes()-stageBytes {
				return nil, errors.New("image compressed content exceeds staging budget")
			}
			stageBytes += descriptor.Size
			b, err = l.fetchFile(ctx, named, state.dir, descriptor)
			if err != nil {
				return nil, err
			}
			seen[descriptor.Digest] = b
			state.blobs = append(state.blobs, b)
		}
		if err := inspectLayer(ctx, b.file, descriptor.MediaType, layer.diffID, &expansion); err != nil {
			return nil, fmt.Errorf("image layer %d: %w", index, err)
		}
		layerNames = append(layerNames, b.name)
	}
	// Both formats point at identical original blobs. Containerd honors OCI;
	// classic Docker honors manifest.json and retains the config identity.
	var tags []string
	var annotations map[string]string
	if tag, ok := named.(name.Tag); ok {
		tags = []string{tag.Name()}
		annotations = map[string]string{ocispec.AnnotationRefName: tag.Name()}
	}
	index, err := json.Marshal(ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, Manifests: []ocispec.Descriptor{{MediaType: manifest.mediaType, Digest: manifestID, Size: int64(len(rawManifest)), Platform: &manifest.platform, Annotations: annotations}}})
	if err != nil {
		return nil, err
	}
	legacy, err := json.Marshal([]struct {
		Config   string   `json:"Config"`
		RepoTags []string `json:"RepoTags"`
		Layers   []string `json:"Layers"`
	}{{Config: blobPath(manifest.config.Digest), RepoTags: tags, Layers: layerNames}})
	if err != nil {
		return nil, err
	}
	for _, b := range []blob{{name: "oci-layout", data: []byte(`{"imageLayoutVersion":"1.0.0"}`)}, {name: "index.json", data: index}, {name: "manifest.json", data: legacy}} {
		b.size = int64(len(b.data))
		stageBytes += b.size
		if stageBytes > l.budget.Bytes() {
			return nil, errors.New("image archive metadata exceeds staging budget")
		}
		state.blobs = append(state.blobs, b)
	}
	archiveBytes := int64(1024)
	for _, b := range state.blobs {
		archiveBytes += 512 + roundBlock(b.size, 512)
	}
	// Classic stores retain a config copy while the import archive still
	// exists. Both stores also create per-image/per-layer metadata outside the
	// layer tar entries (layerdb, snapshot records and graphdriver links).
	importBytes := archiveBytes + expansion.allocated + 2*metadata + int64(len(manifest.layers)+1)*(128<<10)
	// Recovery covers every verification dimension, including namespace usage
	// above the fixed compatibility floors, without copying the entire issuer's
	// unused allowance into durable pins.
	verificationBytes := max(stageBytes, l.budget.Bytes()-expansion.remaining, (importBytes+1)/2,
		expansion.namespace.recoveryBytes())
	state.budget, err = l.admitPreparedBudget(p.SourceReference(), verificationBytes, importBytes)
	if err != nil {
		return nil, err
	}
	transferred = true
	return p, nil
}

func (l *Loader) selectManifest(ctx context.Context, ref name.Reference, platform ocispec.Platform, used *int64) ([]byte, digest.Digest, error) {
	for range maxIndexDepth {
		descriptor, err := remote.Get(ref, l.options(ctx, maxMetadataBytes)...)
		if err != nil {
			return nil, "", err
		}
		*used += int64(len(descriptor.Manifest))
		if *used > maxMetadataBytes || *used > l.budget.Bytes() {
			return nil, "", errors.New("image index and manifest metadata exceeds budget")
		}
		switch string(descriptor.MediaType) {
		case ocispec.MediaTypeImageManifest, "application/vnd.docker.distribution.manifest.v2+json":
			return descriptor.Manifest, digest.FromBytes(descriptor.Manifest), nil
		case ocispec.MediaTypeImageIndex, "application/vnd.docker.distribution.manifest.list.v2+json":
			var index ocispec.Index
			if err := json.Unmarshal(descriptor.Manifest, &index); err != nil {
				return nil, "", err
			}
			if len(index.Manifests) > maxIndexEntries {
				return nil, "", errors.New("image index exceeds entry limit")
			}
			var selected *ocispec.Descriptor
			for _, candidate := range index.Manifests {
				if candidate.Platform != nil && platforms.OnlyStrict(platform).Match(*candidate.Platform) {
					selected = &candidate
					break
				}
			}
			if selected == nil {
				return nil, "", errors.New("image index has no matching platform")
			}
			if err := validDescriptor(*selected); err != nil {
				return nil, "", err
			}
			ref = ref.Context().Digest(selected.Digest.String())
		default:
			return nil, "", fmt.Errorf("unsupported image manifest type %q", descriptor.MediaType)
		}
	}
	return nil, "", errors.New("image index exceeds nesting limit")
}

func validDescriptor(d ocispec.Descriptor) error {
	if err := d.Digest.Validate(); err != nil || d.Digest.Algorithm() != digest.SHA256 || d.Size < 0 || len(d.URLs) != 0 || len(d.Data) != 0 {
		return errors.New("image descriptor must contain a SHA256 registry blob with an explicit size")
	}
	return nil
}

func blobPath(d digest.Digest) string { return "blobs/sha256/" + d.Encoded() }

func (l *Loader) fetchMemory(ctx context.Context, ref name.Reference, d ocispec.Descriptor) ([]byte, error) {
	var buf bytes.Buffer
	if err := l.fetchBounded(ctx, ref, d, &buf, maxMetadataBytes); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (l *Loader) fetchFile(ctx context.Context, ref name.Reference, dir string, d ocispec.Descriptor) (blob, error) {
	w, err := os.CreateTemp(dir, "blob-")
	if err != nil {
		return blob{}, err
	}
	defer func() { _ = w.Close() }()
	r, err := os.Open(w.Name())
	if err != nil {
		_ = os.Remove(w.Name())
		return blob{}, err
	}
	if err = os.Remove(w.Name()); err != nil {
		_ = r.Close()
		return blob{}, err
	}
	transferred := false
	defer func() {
		if !transferred {
			_ = r.Close()
		}
	}()
	if err := l.fetch(ctx, ref, d, w); err != nil {
		return blob{}, err
	}
	if err := w.Close(); err != nil {
		return blob{}, err
	}
	transferred = true
	return blob{name: blobPath(d.Digest), size: d.Size, file: r}, nil
}

func (l *Loader) fetch(ctx context.Context, ref name.Reference, d ocispec.Descriptor, out io.Writer) error {
	return l.fetchBounded(ctx, ref, d, out, l.budget.Bytes())
}

func (l *Loader) fetchBounded(ctx context.Context, ref name.Reference, d ocispec.Descriptor, out io.Writer, limit int64) error {
	if err := validDescriptor(d); err != nil {
		return err
	}
	transfer, err := newRegistryBlobTransfer(ctx, ref, d, boundedTransport{base: l.transport, limit: limit})
	if err != nil {
		return err
	}
	response, err := transfer.openBlob(0)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		return fmt.Errorf("registry blob returned HTTP %d", response.StatusCode)
	}
	body := &resumingBody{transfer: transfer, body: response.Body}
	defer func() { _ = body.Close() }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), &budgetReader{reader: contextReader{ctx, body}, remaining: d.Size})
	if err != nil {
		return err
	}
	if n != d.Size || hex.EncodeToString(h.Sum(nil)) != d.Digest.Encoded() {
		return errors.New("registry blob differs from its verified descriptor")
	}
	return nil
}

func (l *Loader) options(ctx context.Context, limit int64) []remote.Option {
	return []remote.Option{
		remote.WithContext(ctx), remote.WithAuth(authn.Anonymous),
		remote.WithTransport(registryTransport{base: boundedTransport{base: l.transport, limit: limit}}),
		// Our transport shares one attempt bound across headers and body. Do
		// not multiply it by the registry client's default request retries.
		remote.WithRetryBackoff(remote.Backoff{Steps: 1}),
		remote.WithRetryPredicate(func(error) bool { return false }),
		remote.WithRetryStatusCodes(),
	}
}

var stagingName = regexp.MustCompile(`^\.fred-image-[0-9]{1,10}$`)
var stagingBlobName = regexp.MustCompile(`^blob-[0-9]{1,10}$`)

// CleanupAbandoned removes only recognizable empty staging directories and
// zero-byte creation remnants. Call only after acquiring exclusive backend
// storage ownership; live preparation directories must never be scavenged.
// Actual content is unlinked before downloading and needs no crash scavenger.
func (l *Loader) CleanupAbandoned() error {
	if l == nil {
		return errors.New("image loader is unavailable")
	}
	root, err := os.OpenRoot(l.stageRoot)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	listing, err := root.Open(".")
	if err != nil {
		return err
	}
	entries, err := listing.ReadDir(-1)
	_ = listing.Close()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !stagingName.MatchString(entry.Name()) {
			continue
		}
		dir, err := root.OpenRoot(entry.Name())
		if err != nil {
			return err
		}
		err = cleanEmptyStage(dir)
		_ = dir.Close()
		if err != nil {
			return err
		}
		if err := root.Remove(entry.Name()); err != nil {
			return err
		}
	}
	return nil
}

func cleanEmptyStage(dir *os.Root) error {
	f, err := dir.Open(".")
	if err != nil {
		return err
	}
	entries, err := f.ReadDir(-1)
	_ = f.Close()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !stagingBlobName.MatchString(entry.Name()) || !info.Mode().IsRegular() || info.Size() != 0 {
			return errors.New("image staging directory contains unrecognized content")
		}
	}
	for _, entry := range entries {
		if err := dir.Remove(entry.Name()); err != nil {
			return err
		}
	}
	return nil
}
