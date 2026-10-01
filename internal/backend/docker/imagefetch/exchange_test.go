package imagefetch

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestRegistryNativeExchangeOwnsEveryWireAttempt(t *testing.T) {
	for _, failures := range []int32{1, 2, 3} {
		t.Run(fmt.Sprint(failures), func(t *testing.T) {
			data := []byte("immutable tenant content")
			var requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v2/" {
					w.Header().Set("Content-Length", "0")
					return
				}
				if requests.Add(1) <= failures {
					// On a reused connection, net/http would replay this GET
					// internally without another descriptor claim. The owned
					// exchange instead classifies EOF for its one retry owner.
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = connection.Close()
					return
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(data)))
				_, _ = w.Write(data)
			}))
			defer server.Close()
			exchange, err := newSingleRegistryExchange(server.Client().Transport.(*http.Transport))
			require.NoError(t, err)
			ref, err := name.ParseReference(server.Listener.Addr().String() + "/tenant/image:latest")
			require.NoError(t, err)
			transfer, err := newRegistryBlobTransfer(t.Context(), ref, ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}, boundedTransport{base: exchange, limit: 1 << 20})
			require.NoError(t, err)
			response, err := transfer.openBlob(0)
			if failures < registryAttempts {
				require.NoError(t, err)
				defer response.Body.Close()
				actual, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.Equal(t, data, actual)
			} else {
				var interrupted interruptedRegistryHeaders
				require.ErrorAs(t, err, &interrupted)
				require.ErrorIs(t, err, io.EOF)
			}
			require.EqualValues(t, min(failures+1, registryAttempts), requests.Load())
			require.EqualValues(t, requests.Load(), transfer.attempts.spent.Load(), "every canonical wire GET consumes its own claim")
		})
	}
}

func TestRegistryNativeExchangeOwnsProtocolNegotiation(t *testing.T) {
	var requests atomic.Int32
	var alternateCalls atomic.Int32
	type negotiatedProtocol struct {
		major int
		alpn  string
	}
	negotiated := make(chan negotiatedProtocol, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		negotiated <- negotiatedProtocol{major: r.ProtoMajor, alpn: r.TLS.NegotiatedProtocol}
		_, _ = io.WriteString(w, "verified")
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	config := server.Client().Transport.(*http.Transport).Clone()
	config.TLSClientConfig.NextProtos = []string{"h2"}
	config.ForceAttemptHTTP2 = true
	config.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{
		"h2": func(string, *tls.Conn) http.RoundTripper {
			t.Error("caller-owned protocol handler must not become exchange authority")
			return roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("unowned protocol handler")
			})
		},
	}
	config.RegisterProtocol("https", roundTripFunc(func(*http.Request) (*http.Response, error) {
		alternateCalls.Add(1)
		return nil, errors.New("unowned registered protocol")
	}))
	exchange, err := newSingleRegistryExchange(config)
	require.NoError(t, err)
	require.Equal(t, []string{"h2"}, config.TLSClientConfig.NextProtos, "construction must not mutate the caller's TLS settings")
	require.False(t, config.DisableKeepAlives, "construction must not mutate the caller's connection policy")
	// Further mutations of the caller's policy must not reach the owner.
	config.TLSClientConfig.NextProtos[0] = "caller-mutation"
	config.DisableKeepAlives = false
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)
	response, err := exchange.RoundTrip(request)
	require.NoError(t, err)
	defer response.Body.Close()
	actual, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "verified", string(actual))
	require.EqualValues(t, 1, requests.Load())
	require.Zero(t, alternateCalls.Load(), "registered alternate RoundTrippers must not cross the owned native construction boundary")
	protocol := <-negotiated
	require.Equal(t, 1, protocol.major, "HTTP/2 can replay refused streams inside RoundTrip")
	require.NotEqual(t, "h2", protocol.alpn, "HTTP/1 also permits an empty ALPN negotiation")
}

func TestRegistryNativeExchangePreservesOperatorRoutingAndTrust(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "operator trust and routing")
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	var routed, proxied atomic.Int32
	config := &http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) {
			proxied.Add(1)
			return nil, nil
		},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			routed.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		},
		TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: "example.com", MinVersion: tls.VersionTLS12},
	}
	exchange, err := newSingleRegistryExchange(config)
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://operator-routed.invalid/v2/", nil)
	require.NoError(t, err)
	response, err := exchange.RoundTrip(request)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "operator trust and routing", string(body))
	require.EqualValues(t, 1, routed.Load())
	require.EqualValues(t, 1, proxied.Load())
}

func TestRegistryNativeExchangeRejectsUnownedTLSAndZeroAuthority(t *testing.T) {
	for _, config := range []*http.Transport{
		nil,
		{DialTLS: func(string, string) (net.Conn, error) { return nil, errors.New("must not run") }},
		{DialTLSContext: func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("must not run") }},
	} {
		_, err := newSingleRegistryExchange(config)
		require.Error(t, err)
	}
	var unissued singleRegistryExchange
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://registry.example/v2/", nil)
	require.NoError(t, err)
	_, err = unissued.RoundTrip(request)
	require.ErrorContains(t, err, "unavailable")
}

func TestRegistryNativeBodyEOFCannotMintHeaderRetry(t *testing.T) {
	data := []byte("immutable tenant content")
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/" {
			w.Header().Set("Content-Length", "0")
			return
		}
		requests.Add(1)
		// A completed, internally consistent HTTP response may still contain
		// fewer bytes than its immutable descriptor. This is a content failure.
		_, _ = w.Write(data[:3])
	}))
	defer server.Close()
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(server.Client().Transport.(*http.Transport)))
	require.NoError(t, err)
	ref, err := name.ParseReference(server.Listener.Addr().String() + "/tenant/image:latest")
	require.NoError(t, err)
	var output bytes.Buffer
	err = loader.fetch(t.Context(), ref, ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}, &output)
	require.ErrorContains(t, err, "differs from its verified descriptor")
	require.EqualValues(t, 1, requests.Load())
	require.False(t, transientRegistryError(io.EOF), "only the pre-response owner can classify EOF as interrupted headers")
}
