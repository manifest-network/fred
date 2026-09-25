package imagefetch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

type interruptedRegistryBody struct {
	io.ReadCloser
	remaining int
}

func (b *interruptedRegistryBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	n, err := b.ReadCloser.Read(p[:min(len(p), b.remaining)])
	b.remaining -= n
	return n, err
}

func TestRegistryResumesOnlyInterruptedImmutableBlob(t *testing.T) {
	for _, ranges := range []bool{true, false} {
		t.Run(fmt.Sprintf("range=%t", ranges), func(t *testing.T) {
			f := newRegistry(t, layerTar(t, []byte("verified tenant content")))
			var downloads, metadata atomic.Int64
			prefix := len(f.compressed) / 2
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if !strings.HasSuffix(req.URL.Path, f.layerID.String()) {
					metadata.Add(1)
					return f.server.Client().Transport.RoundTrip(req)
				}
				attempt := downloads.Add(1)
				response, err := f.server.Client().Transport.RoundTrip(req)
				if err != nil {
					return nil, err
				}
				if attempt == 1 {
					response.Body = &interruptedRegistryBody{ReadCloser: response.Body, remaining: prefix}
				} else {
					require.Equal(t, fmt.Sprintf("bytes=%d-", prefix), req.Header.Get("Range"))
					if ranges {
						_ = response.Body.Close()
						response.StatusCode = http.StatusPartialContent
						response.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", prefix, len(f.compressed)-1, len(f.compressed)))
						response.ContentLength = int64(len(f.compressed) - prefix)
						response.Body = io.NopCloser(bytes.NewReader(f.compressed[prefix:]))
					}
				}
				return response, nil
			})
			daemon := &recordingImporter{}
			loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(transport))
			require.NoError(t, err)
			prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
			defer prepared.Close()
			require.EqualValues(t, 2, downloads.Load())
			require.EqualValues(t, 5, metadata.Load(), "resuming does not repeat manifest/config selection or authentication")
			require.Zero(t, daemon.loads)
			_, err = loader.Import(t.Context(), prepared)
			require.NoError(t, err)
			require.Equal(t, f.compressed, daemon.archive[blobPath(f.layerID)])
			require.Equal(t, 1, daemon.loads)
		})
	}
}

func TestRegistryRetriesMetadataBeforePublishingAnyBytes(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	var attempts atomic.Int64
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		response, err := f.server.Client().Transport.RoundTrip(req)
		if err == nil && strings.Contains(req.URL.Path, "/manifests/") && attempts.Add(1) == 1 {
			response.Body = &interruptedRegistryBody{ReadCloser: response.Body, remaining: len(f.manifest) / 2}
		}
		return response, err
	})
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.NoError(t, prepared.Close())
	require.EqualValues(t, 2, attempts.Load())
}

func TestRegistryDoesNotRedownloadCompletedLayers(t *testing.T) {
	first := newRegistry(t, layerTar(t, []byte("first layer")))
	second := newRegistry(t, layerTar(t, []byte("second layer")))
	first.updateImage(t, func(cfg *ocispec.Image, manifest *ocispec.Manifest) {
		cfg.RootFS.DiffIDs = append(cfg.RootFS.DiffIDs, digest.FromBytes(second.layer))
		manifest.Layers = append(manifest.Layers, ocispec.Descriptor{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: second.layerID, Size: int64(len(second.compressed))})
	})
	var firstDownloads, secondDownloads atomic.Int64
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, first.layerID.String()) {
			firstDownloads.Add(1)
		}
		if !strings.HasSuffix(req.URL.Path, second.layerID.String()) {
			return first.server.Client().Transport.RoundTrip(req)
		}
		response := &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(second.compressed)), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(second.compressed))}
		if secondDownloads.Add(1) == 1 {
			response.Body = &interruptedRegistryBody{ReadCloser: response.Body, remaining: 1}
		}
		return response, nil
	})
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), first.ref(), testPlatform)
	require.NoError(t, err)
	require.NoError(t, prepared.Close())
	require.EqualValues(t, 1, firstDownloads.Load())
	require.EqualValues(t, 2, secondDownloads.Load())
}

func TestRegistryResumesLargeRedirectedBlobWithinItsDescriptorBound(t *testing.T) {
	f := newRegistry(t, layerTar(t, bytes.Repeat([]byte{'x'}, 3<<20)))
	f.compressed = f.layer
	f.layerID = digest.FromBytes(f.layer)
	f.updateImage(t, func(_ *ocispec.Image, manifest *ocispec.Manifest) {
		manifest.Layers[0] = ocispec.Descriptor{MediaType: ocispec.MediaTypeImageLayer, Digest: f.layerID, Size: int64(len(f.layer))}
	})
	var attempts atomic.Int64
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		response, err := f.server.Client().Transport.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(req.URL.Path, "/blobs/"+f.layerID.String()) {
			_ = response.Body.Close()
			response.StatusCode, response.ContentLength, response.Body = http.StatusTemporaryRedirect, 0, http.NoBody
			response.Header.Set("Location", f.server.URL+"/cdn/content")
		}
		if req.URL.Path == "/cdn/content" {
			_ = response.Body.Close()
			response.StatusCode, response.ContentLength = http.StatusOK, int64(len(f.layer))
			response.Header.Set("Content-Type", "application/octet-stream")
			response.Body = io.NopCloser(bytes.NewReader(f.layer))
			if attempts.Add(1) == 1 {
				response.Body = &interruptedRegistryBody{ReadCloser: response.Body, remaining: 1 << 20}
			} else {
				require.Equal(t, "bytes=1048576-", req.Header.Get("Range"))
				response.StatusCode = http.StatusPartialContent
				response.ContentLength -= 1 << 20
				response.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", 1<<20, len(f.layer)-1, len(f.layer)))
				response.Body = io.NopCloser(bytes.NewReader(f.layer[1<<20:]))
			}
		}
		return response, nil
	})
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 8<<20, WithRegistryTransport(transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.NoError(t, prepared.Close())
	require.EqualValues(t, 2, attempts.Load())
}

func TestRegistryTransientAttemptsShareOneBoundAcrossHeadersAndBody(t *testing.T) {
	for _, fail := range []string{"headers", "body", "mixed"} {
		t.Run(fail, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				data := []byte("immutable-content")
				descriptor := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
				var attempts int
				base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
					attempts++
					if fail == "headers" || (fail == "mixed" && attempts != 2) {
						return &http.Response{StatusCode: http.StatusServiceUnavailable, ContentLength: 0, Header: make(http.Header), Body: http.NoBody}, nil
					}
					return &http.Response{StatusCode: http.StatusOK, ContentLength: descriptor.Size, Header: make(http.Header), Body: &interruptedRegistryBody{ReadCloser: io.NopCloser(bytes.NewReader(data)), remaining: 1}}, nil
				})
				loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(base))
				require.NoError(t, err)
				ref, err := name.ParseReference("registry.example/tenant/image:latest")
				require.NoError(t, err)
				// Exercise the authenticated library path as well: its retries
				// must not multiply our shared bound. The registry ping succeeds.
				loader.transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if req.URL.Path == "/v2/" {
						return &http.Response{StatusCode: http.StatusOK, ContentLength: 0, Header: make(http.Header), Body: http.NoBody}, nil
					}
					return base.RoundTrip(req)
				})
				var output bytes.Buffer
				require.Error(t, loader.fetch(t.Context(), ref, descriptor, &output))
				require.Equal(t, registryAttempts, attempts)
				require.LessOrEqual(t, output.Len(), len(data), "replayed prefixes never grow staged bytes")
			})
		})
	}
}

func TestRegistryPermanentBlobFaultsNeverRetry(t *testing.T) {
	for _, fault := range []string{"digest", "length", "overflow", "denied", "not found", "range", "corrupt resumed suffix"} {
		t.Run(fault, func(t *testing.T) {
			f := newRegistry(t, layerTar(t, []byte("content")))
			var attempts atomic.Int64
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				response, err := f.server.Client().Transport.RoundTrip(req)
				if err != nil || !strings.HasSuffix(req.URL.Path, f.layerID.String()) {
					return response, err
				}
				attempt := attempts.Add(1)
				_ = response.Body.Close()
				data := bytes.Clone(f.compressed)
				switch fault {
				case "digest":
					data[0] ^= 1
				case "length":
					response.ContentLength++
				case "overflow":
					data = append(data, 'x')
					response.ContentLength = -1
				case "denied":
					response.StatusCode = http.StatusForbidden
				case "not found":
					response.StatusCode = http.StatusNotFound
				case "range", "corrupt resumed suffix":
					if attempt == 1 {
						response.Body = &interruptedRegistryBody{ReadCloser: io.NopCloser(bytes.NewReader(data)), remaining: 1}
						return response, nil
					}
					response.StatusCode = http.StatusPartialContent
					response.ContentLength--
					response.Header.Set("Content-Range", fmt.Sprintf("bytes 1-%d/%d", len(data)-1, len(data)))
					if fault == "range" {
						response.Header.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)))
					}
					data = data[1:]
					data[0] ^= 1
				}
				response.Body = io.NopCloser(bytes.NewReader(data))
				return response, nil
			})
			daemon := &recordingImporter{}
			loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(transport))
			require.NoError(t, err)
			_, err = loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.Error(t, err)
			expected := int64(1)
			if fault == "range" || fault == "corrupt resumed suffix" {
				expected++
			}
			require.Equal(t, expected, attempts.Load())
			require.Zero(t, daemon.loads)
			pending, err := loader.PendingBytes()
			require.NoError(t, err)
			require.Zero(t, pending)
		})
	}
}

func TestRegistryRetryBackoffHonorsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		attempts := 0
		base := roundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			return nil, io.ErrUnexpectedEOF
		})
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://registry.example/v2/", nil)
		require.NoError(t, err)
		finished := make(chan error, 1)
		go func() {
			_, err := (registryTransport{base: boundedTransport{base: base, limit: 1 << 20}}).RoundTrip(request)
			finished <- err
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-finished, context.Canceled)
		require.Equal(t, 1, attempts)
		time.Sleep(time.Second)
		require.Equal(t, 1, attempts)
	})
}

type panickingRegistryBody struct{ closed bool }

func (*panickingRegistryBody) Read([]byte) (int, error) { panic("registry body panic") }
func (b *panickingRegistryBody) Close() error           { b.closed = true; return nil }

func TestRegistryMetadataPanicReleasesExchange(t *testing.T) {
	var exchange context.Context
	body := &panickingRegistryBody{}
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		exchange = req.Context()
		return &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Header: make(http.Header), Body: body}, nil
	})
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://registry.example/v2/", nil)
	require.NoError(t, err)
	require.PanicsWithValue(t, "registry body panic", func() {
		_, _ = (registryTransport{base: boundedTransport{base: base, limit: maxMetadataBytes}}).RoundTrip(request)
	})
	require.True(t, body.closed)
	require.ErrorIs(t, exchange.Err(), context.Canceled)
}
