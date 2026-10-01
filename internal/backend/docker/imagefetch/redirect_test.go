package imagefetch

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestRegistryMetadataRedirectsShareACompleteChainBound(t *testing.T) {
	for _, route := range []string{"manifest", "ping", "token"} {
		t.Run(route, func(t *testing.T) {
			var hops atomic.Int32
			var server *httptest.Server
			server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v2/" && route != "ping" {
					if route == "token" {
						w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="registry"`, server.URL))
						w.WriteHeader(http.StatusUnauthorized)
					}
					return
				}
				if hops.Add(1) > 10 {
					// Stop a broken implementation promptly without relying on a
					// deadline to establish the request-count assertion.
					w.WriteHeader(http.StatusTeapot)
					return
				}
				http.Redirect(w, r, r.URL.Path+"?hop="+fmt.Sprint(hops.Load()), http.StatusTemporaryRedirect)
			}))
			defer server.Close()
			loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(server.Client().Transport.(*http.Transport)))
			require.NoError(t, err)
			_, err = loader.Resolve(t.Context(), server.Listener.Addr().String()+"/tenant/image:latest", testPlatform)
			require.ErrorContains(t, err, "redirect")
			require.EqualValues(t, 10, hops.Load())
		})
	}
}

func TestRegistryRedirectNormalizesPrivateLiteralSpellings(t *testing.T) {
	for _, host := range []string{
		"[fe80::1%25eth0]", "2852039166", "0xa9fea9fe", "169.254.169.254.",
		"127.1", "2130706433", "0x7f000001", "0177.0.0.1", "127.000.000.001",
		"[::ffff:169.254.169.254]", "[fd00::1%25eth0]", "0", "10.1.",
	} {
		for _, route := range []string{"blob", "metadata"} {
			t.Run(route+"/"+host, func(t *testing.T) {
				requests := 0
				wire := roundTripFunc(func(request *http.Request) (*http.Response, error) {
					requests++
					return &http.Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{"Location": {"https://" + host + "/content"}}, Body: http.NoBody}, nil
				})
				var err error
				if route == "blob" {
					_, err = fetchRegistryTestBlob(t, []byte("content"), wire)
				} else {
					client := http.Client{Transport: registryTransport{base: boundedTransport{base: wire, limit: maxMetadataBytes}}}
					request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://registry.example/v2/", nil)
					require.NoError(t, requestErr)
					_, err = client.Do(request)
				}
				require.ErrorContains(t, err, "private or link-local IP")
				require.Equal(t, 1, requests, "the literal must be rejected before the second network exchange")
			})
		}
	}
}

func TestRegistryMetadataRetriesCannotResetTheRedirectChain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		requests := 0
		wire := roundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			response := &http.Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{"Location": {request.URL.String()}}, Body: http.NoBody}
			if requests > 10 {
				response.StatusCode = http.StatusTeapot
			} else if requests%2 == 1 {
				response.StatusCode = http.StatusServiceUnavailable
			}
			return response, nil
		})
		client := http.Client{
			Transport:     registryTransport{base: boundedTransport{base: wire, limit: maxMetadataBytes}},
			CheckRedirect: func(*http.Request, []*http.Request) error { return nil },
		}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://registry.example/v2/", nil)
		require.NoError(t, err)
		_, err = client.Do(request)
		require.ErrorContains(t, err, "redirect limit")
		require.Equal(t, 10, requests, "each native exchange spends from the same ten-request chain")
	})
}

func TestRegistryRedirectPreservesPublicLiteralAndSameHostAccess(t *testing.T) {
	for _, hosts := range [][2]string{
		{"registry.example", "8.8.8.8"}, {"registry.example", "0x08080808"},
		{"registry.example", "134744072"}, {"registry.example", "[2606:4700:4700::1111]"},
		{"registry.example", "cdn.registry.example"}, {"127.0.0.1:5000", "127.0.0.1:5000"},
	} {
		t.Run(strings.Join(hosts[:], "/"), func(t *testing.T) {
			requests := 0
			wire := roundTripFunc(func(request *http.Request) (*http.Response, error) {
				requests++
				if requests == 1 {
					return &http.Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{"Location": {"https://" + hosts[1] + "/content"}}, Body: http.NoBody}, nil
				}
				if hosts[0] == hosts[1] {
					require.Equal(t, "Bearer origin-only", request.Header.Get("Authorization"))
				} else {
					require.Empty(t, request.Header.Get("Authorization"))
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("metadata"))}, nil
			})
			client := http.Client{Transport: registryTransport{base: boundedTransport{base: wire, limit: maxMetadataBytes}}}
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+hosts[0]+"/v2/", nil)
			require.NoError(t, err)
			request.Header.Set("Authorization", "Bearer origin-only")
			response, err := client.Do(request)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, 2, requests)
		})
	}
}

func TestRegistryRedirectCopiesRetainIssuedAuthority(t *testing.T) {
	var dispatched atomic.Int32
	wire := roundTripFunc(func(*http.Request) (*http.Response, error) {
		dispatched.Add(1)
		return &http.Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{"Location": {"https://registry.example/next"}}, Body: http.NoBody}, nil
	})
	transport := registryTransport{base: boundedTransport{base: wire, limit: maxMetadataBytes}}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://registry.example/start", nil)
	require.NoError(t, err)
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	request.Response = response
	results := make(chan error, 20)
	for range cap(results) {
		copied := request.Clone(request.Context())
		copiedTransport := transport
		go func() {
			next, err := copiedTransport.RoundTrip(copied)
			if next != nil {
				_ = next.Body.Close()
			}
			results <- err
		}()
	}
	accepted := 1
	for range cap(results) {
		if err := <-results; err == nil {
			accepted++
		} else {
			require.ErrorContains(t, err, "redirect limit")
		}
	}
	require.Equal(t, 10, accepted)
	require.EqualValues(t, 10, dispatched.Load())
	copiedResponse := *response
	for _, forged := range []*http.Response{{Request: request}, &copiedResponse} {
		request.Response = forged
		_, err := transport.RoundTrip(request)
		require.ErrorContains(t, err, "no issued chain receipt")
	}
}

func TestRegistryMixedFaultsKeepTheCanonicalAttemptContract(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		data := []byte("immutable tenant content")
		descriptor := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
		blobs, tokens := 0, 0
		challenge := `Bearer realm="https://registry.example/token",service="registry.example"`
		wire := roundTripFunc(func(request *http.Request) (*http.Response, error) {
			response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}
			switch request.URL.Path {
			case "/v2/":
				response.StatusCode = http.StatusUnauthorized
				response.Header.Set("WWW-Authenticate", challenge)
			case "/token":
				tokens++
				body := []byte(fmt.Sprintf(`{"token":"tenant-token-%d","expires_in":3600}`, tokens))
				response.Body, response.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
			default:
				blobs++
				if blobs == 2 {
					response.StatusCode = http.StatusUnauthorized
					response.Header.Set("WWW-Authenticate", challenge)
				} else {
					response.ContentLength = descriptor.Size
					response.Body = &interruptedRegistryBody{ReadCloser: io.NopCloser(bytes.NewReader(data)), remaining: 1}
				}
			}
			return response, nil
		})
		loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, withRegistryTransportForTest(wire))
		require.NoError(t, err)
		ref, err := name.ParseReference("registry.example/tenant/image:latest")
		require.NoError(t, err)
		var output bytes.Buffer
		err = loader.fetch(t.Context(), ref, descriptor, &output)
		require.Error(t, err)
		require.Equal(t, 2, tokens)
		require.Equal(t, 3, blobs, "a token renewal cannot erase either interrupted request")
		require.Less(t, output.Len(), len(data), "partial content must not become verified content")
	})
}
