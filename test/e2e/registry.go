//go:build integration

package e2e

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

// Registry is an image registry of the test's own, beside one backend. A
// test pushes the images it describes and deploys them by the reference the
// push returns, so it decides what an image carries: its labels, its volumes,
// its platforms, and what a tag points at from one moment to the next.
//
// The backend fetches images itself, over HTTPS and without credentials, and
// only from the registries its configuration allows. The fixture allows this
// one and has the backend trust the authority that signed its certificate.
// Every image runs busybox: its one layer is the filesystem of BusyboxImage,
// with an empty /data for the images that declare it a volume.
type Registry struct {
	// Host is the registry's host and port, the first part of a reference.
	Host string

	docker *client.Client

	mu       sync.Mutex
	objects  map[string]registryObject // by request path
	requests []string                  // "METHOD path", in order
}

type registryObject struct {
	data      []byte
	mediaType string
	digest    digest.Digest
}

// Image describes an image to push.
type Image struct {
	// Labels are the image's labels.
	Labels map[string]string
	// Volumes are the paths the image declares as volumes.
	Volumes []string
	// Architecture is the CPU architecture the image says it is built for.
	// Empty is the host's. The filesystem is the host's either way: an image
	// for another architecture is never meant to run.
	Architecture string
}

// PushedImage is an image in a Registry.
type PushedImage struct {
	// Reference names the image by the tag it was pushed under.
	Reference string
	// Manifest and Config are the digests of the image's manifest and of
	// its configuration. A daemon identifies the image by one of them
	// (Backend.ImageID).
	Manifest string
	Config   string

	descriptor ocispec.Descriptor
}

// startRegistry serves an empty registry on loopback, under a certificate
// the given authority signed.
func startRegistry(t *testing.T, ca *certAuthority, docker *client.Client) *Registry {
	t.Helper()
	r := &Registry{docker: docker, objects: map[string]registryObject{}}
	certDER, keyDER := ca.sign(t, "registry", true)
	key, err := x509.ParseECPrivateKey(keyDER)
	require.NoError(t, err)
	server := httptest.NewUnstartedServer(r)
	server.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: key}},
	}
	server.StartTLS()
	r.Host = strings.TrimPrefix(server.URL, "https://")
	t.Cleanup(func() {
		server.Close()
		r.forget(t)
	})
	return r
}

// forget removes from the daemon the tags of this registry's images, and
// each image that has no other tag. No later registry has this one's address,
// so nothing could use them again.
func (r *Registry) forget(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	listed, err := r.docker.ImageList(ctx, dockerimage.ListOptions{})
	if err != nil {
		t.Logf("list the images of registry %s: %v", r.Host, err)
		return
	}
	for _, one := range listed {
		for _, tag := range one.RepoTags {
			if !strings.HasPrefix(tag, r.Host+"/") {
				continue
			}
			if _, err := r.docker.ImageRemove(ctx, tag, dockerimage.RemoveOptions{}); err != nil {
				t.Logf("remove image %s: %v", tag, err)
			}
		}
	}
}

// ServeHTTP answers the read half of the registry API: the version check,
// and manifests and blobs by GET and HEAD.
func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.requests = append(r.requests, req.Method+" "+req.URL.Path)
	object, found := r.objects[req.URL.Path]
	r.mu.Unlock()
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		http.Error(w, "read-only registry", http.StatusMethodNotAllowed)
		return
	}
	if req.URL.Path == "/v2/" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if !found {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", object.mediaType)
	w.Header().Set("Docker-Content-Digest", object.digest.String())
	w.Header().Set("Content-Length", strconv.Itoa(len(object.data)))
	w.WriteHeader(http.StatusOK)
	if req.Method == http.MethodGet {
		_, _ = w.Write(object.data)
	}
}

// Push stores an image under name, a repository and a tag such as "app:v1",
// and returns it. A name pushed before now points at this image: that is how
// a test moves a tag.
func (r *Registry) Push(t *testing.T, name string, image Image) PushedImage {
	t.Helper()
	repository, tag := splitName(t, name)
	pushed := r.store(t, repository, image)
	r.tag(repository, tag, pushed.descriptor)
	pushed.Reference = r.Host + "/" + name
	return pushed
}

// PushIndex stores one image per description and, under name, an index that
// lists them all by platform, as a multi-platform image is published. It
// returns the images in the order given; each carries the index's reference.
func (r *Registry) PushIndex(t *testing.T, name string, images ...Image) []PushedImage {
	t.Helper()
	repository, tag := splitName(t, name)
	index := ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex}
	pushed := make([]PushedImage, len(images))
	for i, image := range images {
		pushed[i] = r.store(t, repository, image)
		pushed[i].Reference = r.Host + "/" + name
		descriptor := pushed[i].descriptor
		descriptor.Platform = &ocispec.Platform{OS: "linux", Architecture: architectureOf(image)}
		index.Manifests = append(index.Manifests, descriptor)
	}
	encoded, err := json.Marshal(index)
	require.NoError(t, err)
	r.tag(repository, tag, r.add(repository, "manifests", encoded, ocispec.MediaTypeImageIndex))
	return pushed
}

// Requested reports whether the registry was asked for the blob or manifest
// with the given digest. A test that expects the backend to refuse an image
// uses it to show that the backend got as far as reading the image.
func (r *Registry) Requested(digest string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.ContainsFunc(r.requests, func(request string) bool {
		return strings.HasPrefix(request, http.MethodGet+" ") && strings.HasSuffix(request, "/"+digest)
	})
}

// store adds an image's layer, configuration and manifest to a repository,
// under their digests only.
func (r *Registry) store(t *testing.T, repository string, image Image) PushedImage {
	t.Helper()
	layer := busyboxLayer(t, r.docker)
	layerDescriptor := r.add(repository, "blobs", layer.compressed, ocispec.MediaTypeImageLayerGzip)

	platform := ocispec.Platform{OS: "linux", Architecture: architectureOf(image)}
	config := ocispec.Image{
		Platform: platform,
		Config: ocispec.ImageConfig{
			Env:    []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			Cmd:    []string{"sh"},
			Labels: image.Labels,
		},
		RootFS: ocispec.RootFS{Type: "layers", DiffIDs: []digest.Digest{layer.diffID}},
	}
	if len(image.Volumes) > 0 {
		config.Config.Volumes = map[string]struct{}{}
		for _, volume := range image.Volumes {
			config.Config.Volumes[volume] = struct{}{}
		}
	}
	encodedConfig, err := json.Marshal(config)
	require.NoError(t, err)
	configDescriptor := r.add(repository, "blobs", encodedConfig, ocispec.MediaTypeImageConfig)

	encodedManifest, err := json.Marshal(ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDescriptor,
		Layers:    []ocispec.Descriptor{layerDescriptor},
	})
	require.NoError(t, err)
	descriptor := r.add(repository, "manifests", encodedManifest, ocispec.MediaTypeImageManifest)
	return PushedImage{
		Manifest:   descriptor.Digest.String(),
		Config:     configDescriptor.Digest.String(),
		descriptor: descriptor,
	}
}

// add stores one object of a repository under its digest.
func (r *Registry) add(repository, kind string, data []byte, mediaType string) ocispec.Descriptor {
	id := digest.FromBytes(data)
	r.mu.Lock()
	r.objects["/v2/"+repository+"/"+kind+"/"+id.String()] = registryObject{data: data, mediaType: mediaType, digest: id}
	r.mu.Unlock()
	return ocispec.Descriptor{MediaType: mediaType, Digest: id, Size: int64(len(data))}
}

// tag points a repository's tag at a manifest or an index it already holds.
func (r *Registry) tag(repository, tag string, target ocispec.Descriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	manifests := "/v2/" + repository + "/manifests/"
	r.objects[manifests+tag] = r.objects[manifests+target.Digest.String()]
}

func splitName(t *testing.T, name string) (repository, tag string) {
	t.Helper()
	repository, tag, found := strings.Cut(name, ":")
	require.True(t, found && repository != "" && tag != "", "an image is pushed as repository:tag, not %q", name)
	return repository, tag
}

func architectureOf(image Image) string {
	if image.Architecture != "" {
		return image.Architecture
	}
	return runtime.GOARCH
}

// ForeignArchitecture is an architecture this host does not run.
func ForeignArchitecture() string {
	if runtime.GOARCH == "arm64" {
		return "amd64"
	}
	return "arm64"
}

// imageLayer is one layer: the bytes a registry serves, and the digest of
// the archive they decompress to, which an image's configuration lists.
type imageLayer struct {
	compressed []byte
	diffID     digest.Digest
}

// The filesystem of BusyboxImage as one layer, built once per run.
var (
	busyboxOnce  sync.Once
	busyboxBuilt imageLayer
	errBusybox   error
)

func busyboxLayer(t *testing.T, docker *client.Client) imageLayer {
	t.Helper()
	requireImage(t, docker, BusyboxImage)
	busyboxOnce.Do(func() { busyboxBuilt, errBusybox = exportLayer(docker, BusyboxImage) })
	require.NoError(t, errBusybox, "build a layer from %s", BusyboxImage)
	return busyboxBuilt
}

// exportLayer turns an image's filesystem into one layer. It creates a
// container from the image, never starts it, and reads its filesystem back
// as an archive. It keeps the files, directories and links, and drops what
// Docker added for the container: its device nodes and its /.dockerenv.
func exportLayer(docker *client.Client, image string) (imageLayer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	created, err := docker.ContainerCreate(ctx, &container.Config{Image: image}, nil, nil, nil, "")
	if err != nil {
		return imageLayer{}, err
	}
	defer func() {
		_ = docker.ContainerRemove(context.Background(), created.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})
	}()
	exported, err := docker.ContainerExport(ctx, created.ID)
	if err != nil {
		return imageLayer{}, err
	}
	defer func() { _ = exported.Close() }()

	var archive bytes.Buffer
	out := tar.NewWriter(&archive)
	in := tar.NewReader(exported)
	seen := map[string]bool{}
	for {
		header, err := in.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return imageLayer{}, err
		}
		name := path.Clean(strings.TrimPrefix(header.Name, "/"))
		kept := header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeDir ||
			header.Typeflag == tar.TypeSymlink || header.Typeflag == tar.TypeLink
		if !kept || name == "." || name == ".dockerenv" || seen[name] {
			continue
		}
		seen[name] = true
		// A header of its own: nothing of the daemon's archive format, such
		// as extended attributes, reaches the layer.
		entry := &tar.Header{
			Typeflag: header.Typeflag, Name: name, Linkname: header.Linkname,
			Mode: header.Mode, Uid: header.Uid, Gid: header.Gid, ModTime: header.ModTime,
		}
		switch header.Typeflag {
		case tar.TypeDir:
			entry.Name += "/"
		case tar.TypeReg:
			entry.Size = header.Size
		case tar.TypeLink:
			entry.Linkname = path.Clean(strings.TrimPrefix(header.Linkname, "/"))
		}
		if err := out.WriteHeader(entry); err != nil {
			return imageLayer{}, err
		}
		if header.Typeflag == tar.TypeReg {
			if _, err := io.Copy(out, in); err != nil { //nolint:gosec // G110: the filesystem of a pinned image
				return imageLayer{}, err
			}
		}
	}
	if !seen["bin/busybox"] {
		return imageLayer{}, errors.New("the exported filesystem holds no bin/busybox")
	}
	if !seen["data"] {
		if err := out.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: "data/", Mode: 0o755}); err != nil {
			return imageLayer{}, err
		}
	}
	if err := out.Close(); err != nil {
		return imageLayer{}, err
	}

	var compressed bytes.Buffer
	zip := gzip.NewWriter(&compressed)
	if _, err := zip.Write(archive.Bytes()); err != nil {
		return imageLayer{}, err
	}
	if err := zip.Close(); err != nil {
		return imageLayer{}, err
	}
	return imageLayer{compressed: compressed.Bytes(), diffID: digest.FromBytes(archive.Bytes())}, nil
}

// writeTrustBundle writes the certificates a process under test trusts: the
// host's own, and the authority given. The file replaces the host's bundle
// for a process started with SSL_CERT_FILE naming it.
func writeTrustBundle(t *testing.T, file string, ca *certAuthority) {
	t.Helper()
	extra, err := os.ReadFile(ca.certFile)
	require.NoError(t, err)
	var bundle []byte
	// Where Go looks for the host's bundle, in its order.
	for _, system := range []string{
		"/etc/ssl/certs/ca-certificates.crt",
		"/etc/pki/tls/certs/ca-bundle.crt",
		"/etc/ssl/ca-bundle.pem",
		"/etc/pki/tls/cacert.pem",
		"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
		"/etc/ssl/cert.pem",
	} {
		if data, err := os.ReadFile(system); err == nil { //nolint:gosec // G304: the host's certificate bundle
			bundle = slices.Concat(data, []byte("\n"))
			break
		}
	}
	require.NoError(t, os.WriteFile(file, append(bundle, extra...), 0o644)) //nolint:gosec // G306: public certificates
}
