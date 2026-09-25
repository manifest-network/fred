package imagefetch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	registryauth "github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

type stalledRegistryBody struct {
	ctx  context.Context
	data []byte
}

func (b *stalledRegistryBody) Read(p []byte) (int, error) {
	if len(b.data) > 0 {
		n := copy(p, b.data)
		b.data = b.data[n:]
		return n, nil
	}
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (*stalledRegistryBody) Close() error { return nil }

// Use the whole authenticated fetch path: client-library retries must not
// multiply the transfer's one attempt budget, including 30-second idle stalls.
func fetchRegistryTestBlob(t *testing.T, data []byte, transport http.RoundTripper) ([]byte, error) {
	t.Helper()
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/v2/" {
			return &http.Response{StatusCode: http.StatusOK, ContentLength: 0, Header: make(http.Header), Body: http.NoBody}, nil
		}
		return transport.RoundTrip(req)
	})))
	require.NoError(t, err)
	ref, err := name.ParseReference("registry.example/tenant/image:latest")
	require.NoError(t, err)
	var output bytes.Buffer
	err = loader.fetch(t.Context(), ref, ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}, &output)
	return output.Bytes(), err
}

func TestRegistryIdleStallsResumeWithinOneAttemptBudget(t *testing.T) {
	for _, stalls := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("stalls=%d", stalls), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				data := bytes.Repeat([]byte("0123456789abcdef"), 4096)
				attempts := 0
				var ranges []string
				transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
					attempts++
					offset := (attempts - 1) * 10000
					header := make(http.Header)
					status := http.StatusOK
					if attempts > 1 {
						ranges = append(ranges, req.Header.Get("Range"))
						require.Equal(t, fmt.Sprintf("bytes=%d-", offset), req.Header.Get("Range"))
						status = http.StatusPartialContent
						header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(data)-1, len(data)))
					}
					body := io.NopCloser(bytes.NewReader(data[offset:]))
					if attempts <= stalls {
						body = &stalledRegistryBody{ctx: req.Context(), data: data[offset : offset+10000]}
					}
					return &http.Response{StatusCode: status, ContentLength: int64(len(data) - offset), Header: header, Body: body}, nil
				})
				started := time.Now()
				output, err := fetchRegistryTestBlob(t, data, transport)
				if stalls < registryAttempts {
					require.NoError(t, err)
					require.Equal(t, data, output)
				} else {
					require.ErrorIs(t, err, errRegistryIdle)
					require.Len(t, output, 30000)
				}
				require.Equal(t, min(stalls+1, registryAttempts), attempts)
				require.Len(t, ranges, attempts-1)
				require.GreaterOrEqual(t, time.Since(started), time.Duration(stalls)*registryIdleTimeout)
			})
		})
	}
}

type observedRegistryBody struct {
	io.Reader
	reads  int
	closed bool
}

func (b *observedRegistryBody) Read(p []byte) (int, error) { b.reads++; return b.Reader.Read(p) }
func (b *observedRegistryBody) Close() error               { b.closed = true; return nil }

func TestRegistryRejectsWrongResumeRangeBeforeReadingBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := []byte("immutable tenant content")
		attempts := 0
		// The suffix is correct, so a missing header check would pass the
		// final digest. It must instead fail without reading even one byte.
		body := &observedRegistryBody{Reader: bytes.NewReader(data[1:])}
		transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(data)), Header: make(http.Header), Body: &interruptedRegistryBody{ReadCloser: io.NopCloser(bytes.NewReader(data)), remaining: 1}}, nil
			}
			require.Equal(t, "bytes=1-", req.Header.Get("Range"))
			return &http.Response{StatusCode: http.StatusPartialContent, ContentLength: int64(len(data) - 1), Header: http.Header{"Content-Range": {fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data))}}, Body: body}, nil
		})
		output, err := fetchRegistryTestBlob(t, data, transport)
		require.ErrorContains(t, err, "invalid content range")
		require.Equal(t, data[:1], output)
		require.Equal(t, 2, attempts)
		require.Zero(t, body.reads)
		require.True(t, body.closed)
	})
}

func TestRegistryRangeRefusalFallsBackToFullGETWithinAttemptBudget(t *testing.T) {
	for _, failFull := range []bool{false, true} {
		t.Run(fmt.Sprintf("full-get-fails=%t", failFull), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				data := []byte("immutable tenant content")
				attempts := 0
				transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
					attempts++
					response := &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(data)), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data))}
					switch attempts {
					case 1:
						require.Empty(t, req.Header.Get("Range"))
						response.Body = &interruptedRegistryBody{ReadCloser: response.Body, remaining: 7}
					case 2:
						require.Equal(t, "bytes=7-", req.Header.Get("Range"))
						response.StatusCode, response.ContentLength, response.Body = http.StatusRequestedRangeNotSatisfiable, 0, http.NoBody
					case 3:
						require.Empty(t, req.Header.Get("Range"))
						if failFull {
							response.Body = &interruptedRegistryBody{ReadCloser: response.Body, remaining: 10}
						}
					default:
						t.Fatalf("attempt bound exceeded: %d", attempts)
					}
					return response, nil
				})
				output, err := fetchRegistryTestBlob(t, data, transport)
				if failFull {
					require.Error(t, err)
					require.Equal(t, data[:10], output)
				} else {
					require.NoError(t, err)
					require.Equal(t, data, output, "replayed prefix is not published twice")
				}
				require.Equal(t, registryAttempts, attempts)
			})
		})
	}
}

func TestRegistryRedirectExpiryRenewsOnlyThroughOriginalRegistry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := []byte("immutable tenant content")
		descriptor := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
		origin, cdn := 0, 0
		transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/v2/" {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
			}
			if req.URL.Host == "registry.example" {
				origin++
				require.Equal(t, "/v2/tenant/image/blobs/"+descriptor.Digest.String(), req.URL.Path)
				require.Equal(t, "Bearer registry-secret", req.Header.Get("Authorization"))
				return &http.Response{StatusCode: http.StatusTemporaryRedirect, ContentLength: 0, Header: http.Header{"Location": {fmt.Sprintf("https://cdn.example/content?signature=%d", origin)}}, Body: http.NoBody}, nil
			}
			require.Equal(t, "cdn.example", req.URL.Host)
			require.Empty(t, req.Header.Get("Authorization"), "registry credentials must not reach another origin")
			cdn++
			require.Equal(t, fmt.Sprint(cdn), req.URL.Query().Get("signature"))
			if cdn == 1 {
				return &http.Response{StatusCode: http.StatusForbidden, ContentLength: 0, Header: make(http.Header), Body: http.NoBody}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(data)), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data))}, nil
		})
		ref, err := name.ParseReference("registry.example/tenant/image:latest")
		require.NoError(t, err)
		transfer, err := newRegistryBlobTransfer(t.Context(), ref, descriptor, boundedTransport{base: transport, limit: 1 << 20})
		require.NoError(t, err)
		transfer.request.Header.Set("Authorization", "Bearer registry-secret")
		response, err := transfer.openBlob(0)
		require.NoError(t, err)
		defer response.Body.Close()
		actual, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, data, actual)
		require.Equal(t, 2, origin)
		require.Equal(t, 2, cdn)
	})
}

func TestRegistryBlobRedirectsRemainHTTPSAndBounded(t *testing.T) {
	for _, target := range []string{"http://cdn.example/content", "https://cdn.example/content"} {
		t.Run(target, func(t *testing.T) {
			data := []byte("content")
			hops := 0
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				hops++
				require.Equal(t, "https", req.URL.Scheme)
				return &http.Response{StatusCode: http.StatusTemporaryRedirect, ContentLength: 0, Header: http.Header{"Location": {target}}, Body: http.NoBody}, nil
			})
			_, err := fetchRegistryTestBlob(t, data, transport)
			require.Error(t, err)
			if target[:5] == "http:" {
				require.ErrorContains(t, err, "require HTTPS")
				require.Equal(t, 1, hops)
			} else {
				require.ErrorContains(t, err, "redirect limit")
				require.Equal(t, 10, hops)
			}
		})
	}
}

func TestRegistryRetryAfterIsBoundedAndCancelable(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		for _, value := range []string{"2", "999999999999", "date", "invalid", "-1"} {
			t.Run(fmt.Sprintf("status=%d/retry-after=%s", status, value), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					attempts := 0
					started := time.Now()
					expected := 100 * time.Millisecond
					header := value
					switch value {
					case "2":
						expected = 2 * time.Second
					case "999999999999":
						expected = registryRetryAfterMax
					case "date":
						expected = 5 * time.Second
						header = started.Add(expected).UTC().Format(http.TimeFormat)
					}
					transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
						attempts++
						if attempts == 1 {
							return &http.Response{StatusCode: status, ContentLength: 0, Header: http.Header{"Retry-After": {header}}, Body: http.NoBody}, nil
						}
						require.Equal(t, expected, time.Since(started))
						return &http.Response{StatusCode: http.StatusOK, ContentLength: 0, Header: make(http.Header), Body: http.NoBody}, nil
					})
					request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://registry.example/v2/", nil)
					require.NoError(t, err)
					response, err := (registryTransport{base: boundedTransport{base: transport, limit: 1 << 20}}).RoundTrip(request)
					require.NoError(t, err)
					require.NoError(t, response.Body.Close())
					require.Equal(t, 2, attempts)
				})
			})
		}
	}
	t.Run("cancel", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			attempts := 0
			transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
				attempts++
				return &http.Response{StatusCode: http.StatusTooManyRequests, ContentLength: 0, Header: http.Header{"Retry-After": {"30"}}, Body: http.NoBody}, nil
			})
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://registry.example/v2/", nil)
			require.NoError(t, err)
			_, err = (registryTransport{base: boundedTransport{base: transport, limit: 1 << 20}}).RoundTrip(request)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, 1, attempts)
		})
	})
}

func TestRegistryBlobRetryPreservesBearerAuthentication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := []byte("immutable tenant content")
		descriptor := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
		origin, cdn, tokens := 0, 0, 0
		transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}
			switch req.URL.Path {
			case "/v2/":
				response.StatusCode = http.StatusUnauthorized
				response.Header.Set("WWW-Authenticate", `Bearer realm="https://registry.example/token",service="registry.example"`)
			case "/token":
				tokens++
				body := []byte(`{"token":"tenant-token","expires_in":3600}`)
				response.Body, response.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
			case "/v2/tenant/image/blobs/" + descriptor.Digest.String():
				origin++
				require.Equal(t, "Bearer tenant-token", req.Header.Get("Authorization"))
				response.StatusCode = http.StatusTemporaryRedirect
				response.Header.Set("Location", fmt.Sprintf("https://cdn.registry.example/content?signature=%d", origin))
			case "/content":
				cdn++
				require.Equal(t, "cdn.registry.example", req.URL.Host)
				require.Empty(t, req.Header.Get("Authorization"), "a registry subdomain cannot inherit its token")
				require.Equal(t, fmt.Sprint(cdn), req.URL.Query().Get("signature"))
				response.ContentLength = descriptor.Size
				response.Body = io.NopCloser(bytes.NewReader(data))
				if cdn == 1 {
					response.Body = &interruptedRegistryBody{ReadCloser: response.Body, remaining: 1}
				} else {
					require.Equal(t, "bytes=1-", req.Header.Get("Range"))
					response.StatusCode = http.StatusPartialContent
					response.ContentLength--
					response.Header.Set("Content-Range", fmt.Sprintf("bytes 1-%d/%d", len(data)-1, len(data)))
					response.Body = io.NopCloser(bytes.NewReader(data[1:]))
				}
			default:
				t.Fatalf("unexpected registry path: %s", req.URL.Path)
			}
			return response, nil
		})
		loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(transport))
		require.NoError(t, err)
		ref, err := name.ParseReference("registry.example/tenant/image:latest")
		require.NoError(t, err)
		var output bytes.Buffer
		require.NoError(t, loader.fetch(t.Context(), ref, descriptor, &output))
		require.Equal(t, data, output.Bytes())
		require.Equal(t, 1, tokens, "retry reuses registry authorization without repeating the token exchange")
		require.Equal(t, 2, origin)
		require.Equal(t, 2, cdn)
	})
}

func TestRegistryBlobRedirectPreservesPrivateIPRefusal(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "169.254.169.254", "10.0.0.1", "[::1]", "[::]", "[fd00::1]"} {
		t.Run(host, func(t *testing.T) {
			requests := 0
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				requests++
				require.Equal(t, "registry.example", req.URL.Host)
				return &http.Response{StatusCode: http.StatusTemporaryRedirect, ContentLength: 0, Header: http.Header{"Location": {"https://" + host + "/content"}}, Body: http.NoBody}, nil
			})
			_, err := fetchRegistryTestBlob(t, []byte("content"), transport)
			require.ErrorContains(t, err, "private or link-local IP")
			require.Equal(t, 1, requests)
		})
	}
}

func TestRegistryBearerFailuresKeepMetadataBoundsAndOriginRefusal(t *testing.T) {
	for _, fault := range []string{"challenge exceeds metadata", "token exceeds metadata", "origin forbidden"} {
		t.Run(fault, func(t *testing.T) {
			data := []byte("immutable tenant content")
			descriptor := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
			origin, tokens := 0, 0
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}
				var body []byte
				switch req.URL.Path {
				case "/v2/":
					response.StatusCode = http.StatusUnauthorized
					response.Header.Set("WWW-Authenticate", `Bearer realm="https://registry.example/token",service="registry.example"`)
					if fault == "challenge exceeds metadata" {
						body = bytes.Repeat([]byte{' '}, int(maxMetadataBytes)+1)
					}
				case "/token":
					tokens++
					if fault == "token exceeds metadata" {
						body = bytes.Repeat([]byte{' '}, int(maxMetadataBytes)+1)
					} else {
						body = []byte(`{"token":"tenant-token","expires_in":3600}`)
					}
				case "/v2/tenant/image/blobs/" + descriptor.Digest.String():
					origin++
					require.Equal(t, "Bearer tenant-token", req.Header.Get("Authorization"))
					response.StatusCode = http.StatusForbidden
				default:
					t.Fatalf("unexpected registry path: %s", req.URL.Path)
				}
				response.Body, response.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
				return response, nil
			})
			loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 4<<20, WithRegistryTransport(transport))
			require.NoError(t, err)
			ref, err := name.ParseReference("registry.example/tenant/image:latest")
			require.NoError(t, err)
			var output bytes.Buffer
			err = loader.fetch(t.Context(), ref, descriptor, &output)
			require.Error(t, err)
			require.Empty(t, output.Bytes())
			switch fault {
			case "challenge exceeds metadata":
				require.ErrorContains(t, err, "byte limit")
				require.Zero(t, tokens)
				require.Zero(t, origin)
			case "token exceeds metadata":
				require.ErrorContains(t, err, "byte limit")
				require.Equal(t, 1, tokens)
				require.Zero(t, origin)
			case "origin forbidden":
				require.Equal(t, 1, tokens)
				require.Equal(t, 1, origin, "an authoritative registry refusal is terminal")
			}
		})
	}
}

func TestRegistryAuthRenewalKeepsOneDescriptorAttemptBound(t *testing.T) {
	for _, renewAt := range []int{1, 2, 3} {
		for _, failureAfterRenewal := range []bool{false, true} {
			t.Run(fmt.Sprintf("renew-at=%d/fail-after=%t", renewAt, failureAfterRenewal), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					data := []byte("immutable tenant content")
					descriptor := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
					blobs, tokens := 0, 0
					challenge := `Bearer realm="https://registry.example/token",service="registry.example"`
					transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
						response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}
						switch req.URL.Path {
						case "/v2/":
							response.StatusCode = http.StatusUnauthorized
							response.Header.Set("WWW-Authenticate", challenge)
						case "/token":
							tokens++
							body := []byte(fmt.Sprintf(`{"token":"tenant-token-%d","expires_in":3600}`, tokens))
							response.Body, response.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
						case "/v2/tenant/image/blobs/" + descriptor.Digest.String():
							blobs++
							require.Equal(t, fmt.Sprintf("Bearer tenant-token-%d", tokens), req.Header.Get("Authorization"))
							switch {
							case blobs == renewAt:
								response.StatusCode = http.StatusUnauthorized
								response.Header.Set("WWW-Authenticate", challenge)
							case blobs < renewAt || failureAfterRenewal:
								response.StatusCode = http.StatusServiceUnavailable
							default:
								response.Body, response.ContentLength = io.NopCloser(bytes.NewReader(data)), int64(len(data))
							}
						default:
							t.Fatalf("unexpected registry path: %s", req.URL.Path)
						}
						return response, nil
					})
					loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(transport))
					require.NoError(t, err)
					ref, err := name.ParseReference("registry.example/tenant/image:latest")
					require.NoError(t, err)
					var output bytes.Buffer
					err = loader.fetch(t.Context(), ref, descriptor, &output)
					if renewAt == registryAttempts || failureAfterRenewal {
						require.Error(t, err)
						require.Equal(t, registryAttempts, blobs, "token renewal cannot mint a new blob attempt allowance")
						require.Empty(t, output.Bytes())
					} else {
						require.NoError(t, err)
						require.Equal(t, renewAt+1, blobs)
						require.Equal(t, data, output.Bytes())
					}
					if renewAt == registryAttempts {
						require.ErrorIs(t, err, errRegistryAttemptsSpent)
					}
					require.Equal(t, 2, tokens)
				})
			})
		}
	}
}

func TestRegistryTransportCopiesShareDescriptorAttemptAuthority(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := []byte("content")
		descriptor := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
		var exchanges atomic.Int32
		base := roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.URL.Path == "/v2/" {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
			}
			exchanges.Add(1)
			return &http.Response{StatusCode: http.StatusOK, ContentLength: descriptor.Size, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data))}, nil
		})
		ref, err := name.ParseReference("registry.example/tenant/image:latest")
		require.NoError(t, err)
		transfer, err := newRegistryBlobTransfer(t.Context(), ref, descriptor, boundedTransport{base: base, limit: 1 << 20})
		require.NoError(t, err)
		results := make(chan error, 16)
		for range cap(results) {
			copied := *transfer
			go func() {
				response, err := copied.openBlob(0)
				if err == nil {
					err = response.Body.Close()
				}
				results <- err
			}()
		}
		completed := 0
		for range cap(results) {
			err := <-results
			if err == nil {
				completed++
			} else {
				require.ErrorIs(t, err, errRegistryAttemptsSpent)
			}
		}
		require.Equal(t, registryAttempts, completed)
		require.EqualValues(t, registryAttempts, exchanges.Load())
	})
}

func TestRegistryBodyResumeRenewsExpiredTokenWithinAttemptBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := []byte("immutable tenant content")
		descriptor := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
		blobs, tokens := 0, 0
		challenge := `Bearer realm="https://registry.example/token",service="registry.example"`
		transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}
			switch req.URL.Path {
			case "/v2/":
				response.StatusCode = http.StatusUnauthorized
				response.Header.Set("WWW-Authenticate", challenge)
			case "/token":
				tokens++
				body := []byte(fmt.Sprintf(`{"token":"tenant-token-%d","expires_in":3600}`, tokens))
				response.Body, response.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
			case "/v2/tenant/image/blobs/" + descriptor.Digest.String():
				blobs++
				switch blobs {
				case 1:
					response.ContentLength = descriptor.Size
					response.Body = &interruptedRegistryBody{ReadCloser: io.NopCloser(bytes.NewReader(data)), remaining: 1}
				case 2:
					require.Equal(t, "bytes=1-", req.Header.Get("Range"))
					response.StatusCode = http.StatusUnauthorized
					response.Header.Set("WWW-Authenticate", challenge)
				default:
					require.Equal(t, "Bearer tenant-token-2", req.Header.Get("Authorization"))
					response.Body, response.ContentLength = io.NopCloser(bytes.NewReader(data)), descriptor.Size
				}
			default:
				t.Fatalf("unexpected registry path: %s", req.URL.Path)
			}
			return response, nil
		})
		loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(transport))
		require.NoError(t, err)
		ref, err := name.ParseReference("registry.example/tenant/image:latest")
		require.NoError(t, err)
		var output bytes.Buffer
		err = loader.fetch(t.Context(), ref, descriptor, &output)
		t.Logf("tokens=%d blobs=%d prefix=%d error=%v", tokens, blobs, output.Len(), err)
		require.NoError(t, err)
		require.Equal(t, data, output.Bytes())
		require.Equal(t, 2, tokens)
		require.Equal(t, 3, blobs)
	})
}

func TestRegistryTokenURLsCannotAcquireBlobDispatchAuthority(t *testing.T) {
	for _, redirected := range []bool{false, true} {
		t.Run(fmt.Sprintf("redirected=%t", redirected), func(t *testing.T) {
			data := bytes.Repeat([]byte{'x'}, int(maxMetadataBytes)+1)
			descriptor := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
			blobPath := "/v2/tenant/image/blobs/" + descriptor.Digest.String()
			blobURL := "https://registry.example" + blobPath
			body := &observedRegistryBody{Reader: bytes.NewReader(data)}
			tokenResponses := 0
			base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}
				switch req.URL.Path {
				case "/v2/":
					realm := blobURL
					if redirected {
						realm = "https://registry.example/token"
					}
					response.StatusCode = http.StatusUnauthorized
					response.Header.Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s",service="registry.example"`, realm))
				case "/token":
					response.StatusCode = http.StatusTemporaryRedirect
					// Drop the token query: even the exact canonical image URL
					// cannot grant a token exchange the descriptor capability.
					response.Header.Set("Location", blobURL)
				case blobPath:
					tokenResponses++
					response.Header.Set("Content-Type", "application/octet-stream")
					response.Body, response.ContentLength = body, descriptor.Size
				default:
					t.Fatalf("unexpected registry path: %s", req.URL.Path)
				}
				return response, nil
			})
			dispatch := &registryBlobDispatch{base: boundedTransport{base: base, limit: 8 << 20}}
			repository, err := name.NewRepository("registry.example/tenant/image")
			require.NoError(t, err)
			_, err = registryauth.NewWithContext(t.Context(), repository.Registry, authn.Anonymous, dispatch, []string{repository.Scope(registryauth.PullScope)})
			require.ErrorContains(t, err, "byte limit")
			require.Zero(t, dispatch.attempts.spent.Load(), "a token exchange never spends or obtains blob dispatch authority")
			require.Equal(t, 1, tokenResponses)
			require.True(t, body.closed)
		})
	}
}
