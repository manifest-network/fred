package imagefetch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// memoryRegistry serves a registryFixture's handler in memory, so synctest can
// own every wait, and counts each wire exchange by method and target. It adds
// the distribution HEAD contract the fixture lacks: a manifest HEAD answers
// with the Content-Type, Content-Length and Docker-Content-Digest of the bytes
// a GET of the same reference returns, and no body.
type memoryRegistry struct {
	f      *registryFixture
	mu     sync.Mutex
	counts map[string]int
	// head rewrites the distribution HEAD answer.
	head func(*http.Response) *http.Response
	// get runs before a manifest GET is served; an error aborts the exchange.
	get func(*http.Request) error
}

func newMemoryRegistry(f *registryFixture) *memoryRegistry {
	return &memoryRegistry{f: f, counts: make(map[string]int)}
}

func registryTarget(path string) string {
	switch {
	case path == "/v2/":
		return "ping"
	case strings.Contains(path, "/manifests/sha256:"):
		return "digest"
	case strings.Contains(path, "/manifests/"):
		return "tag"
	case strings.Contains(path, "/blobs/"):
		return "blob"
	default:
		return "other"
	}
}

func (m *memoryRegistry) record(request *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[request.Method+" "+registryTarget(request.URL.Path)]++
}

func (m *memoryRegistry) count(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[key]
}

func (m *memoryRegistry) manifestGETs() int { return m.count("GET tag") + m.count("GET digest") }

func (m *memoryRegistry) total() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, count := range m.counts {
		total += count
	}
	return total
}

func (m *memoryRegistry) RoundTrip(request *http.Request) (*http.Response, error) {
	m.record(request)
	target := registryTarget(request.URL.Path)
	manifest := target == "tag" || target == "digest"
	if manifest && request.Method == http.MethodGet && m.get != nil {
		if err := m.get(request); err != nil {
			return nil, err
		}
	}
	response := m.serve(request)
	if manifest && request.Method == http.MethodHead {
		response = distributionHead(response)
		if m.head != nil {
			response = m.head(response)
		}
	}
	return response, nil
}

func (m *memoryRegistry) serve(request *http.Request) *http.Response {
	get := request.Clone(request.Context())
	get.Method = http.MethodGet
	recorder := httptest.NewRecorder()
	m.f.server.Config.Handler.ServeHTTP(recorder, get)
	response := recorder.Result()
	response.Request = request
	return response
}

func distributionHead(get *http.Response) *http.Response {
	body, _ := io.ReadAll(get.Body)
	_ = get.Body.Close()
	header := make(http.Header)
	header.Set("Content-Type", get.Header.Get("Content-Type"))
	header.Set("Content-Length", strconv.Itoa(len(body)))
	header.Set("Docker-Content-Digest", digest.FromBytes(body).String())
	return &http.Response{StatusCode: get.StatusCode, Status: get.Status, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: header, ContentLength: int64(len(body)), Body: http.NoBody, Request: get.Request}
}

func withStatus(response *http.Response, status int) *http.Response {
	response.StatusCode, response.Status = status, fmt.Sprintf("%d %s", status, http.StatusText(status))
	return response
}

func newMemoryLoader(t *testing.T, transport http.RoundTripper) *Loader {
	t.Helper()
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20, withRegistryTransportForTest(transport))
	require.NoError(t, err)
	return loader
}

// serveIndex serves index as the fixture's tag and under its own digest. The
// fixture answers any other digest with its manifest, so an index served only
// by tag would let a HEAD-announced digest read the leaf instead.
func serveIndex(f *registryFixture, index []byte) digest.Digest {
	id := digest.FromBytes(index)
	f.mu.Lock()
	f.manifests = map[string][]byte{"latest": index, id.String(): index}
	f.mu.Unlock()
	return id
}

// useIndex serves the fixture manifest through a one-platform OCI index.
func useIndex(t *testing.T, f *registryFixture) digest.Digest {
	t.Helper()
	return serveIndex(f, mustJSON(t, ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex,
		Manifests: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageManifest, Digest: f.manifestID, Size: int64(len(f.manifest)), Platform: &testPlatform}}}))
}

func fixtureRepository(t *testing.T, f *registryFixture) name.Repository {
	t.Helper()
	repository, err := name.NewRepository(strings.TrimSuffix(f.ref(), ":latest"))
	require.NoError(t, err)
	return repository
}

func tagResolutionCounts() map[string]float64 {
	counts := make(map[string]float64)
	for _, source := range []string{tagFromCache, tagFromRegistry, tagFallbackUnsupported, tagFallbackIncomplete} {
		counts[source] = testutil.ToFloat64(imageTagResolutions.WithLabelValues(source))
	}
	return counts
}

func tagResolutionDelta(before map[string]float64) map[string]float64 {
	delta := make(map[string]float64)
	for source, count := range tagResolutionCounts() {
		if difference := count - before[source]; difference != 0 {
			delta[source] = difference
		}
	}
	return delta
}

func registryRequestCount(endpoint, method, status string) float64 {
	return testutil.ToFloat64(imageRegistryRequests.WithLabelValues(endpoint, method, status))
}

func TestTagResolutionUsesHeadAndVerifiedManifestCache(t *testing.T) {
	for _, shape := range []string{"single-manifest", "index"} {
		t.Run(shape, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			gets := 1
			if shape == "index" {
				useIndex(t, f)
				gets = 2
			}
			registry := newMemoryRegistry(f)
			loader := newMemoryLoader(t, registry)
			resolutions := tagResolutionCounts()
			heads, manifestGets := registryRequestCount("manifest", http.MethodHead, "ok"), registryRequestCount("manifest", http.MethodGet, "ok")
			first, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
			// One authentication covers the tag HEAD, its GET and any index
			// child; the config transfer authenticates separately.
			require.Equal(t, 2, registry.count("GET ping"), "a cold resolution authenticates its manifest reads once")
			for range 5 {
				again, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
				require.NoError(t, err)
				require.Equal(t, first.SourceReference(), again.SourceReference())
				require.Equal(t, first.ConfigID(), again.ConfigID())
			}
			require.Equal(t, 7, registry.count("GET ping"), "a warm resolution authenticates once, for its HEAD")
			require.Equal(t, 6, registry.count("HEAD tag"), "every preparation still re-resolves the mutable tag")
			require.Zero(t, registry.count("GET tag"), "an announced digest is read by digest, never by its mutable tag")
			require.Equal(t, gets, registry.count("GET digest"), "an unchanged tag costs no further manifest GET")
			require.Equal(t, 1, registry.count("GET blob"), "config remains served from the verified config cache")
			require.Equal(t, map[string]float64{tagFromCache: 5, tagFromRegistry: 1}, tagResolutionDelta(resolutions))
			require.Equal(t, heads+6, registryRequestCount("manifest", http.MethodHead, "ok"))
			require.Equal(t, manifestGets+float64(gets), registryRequestCount("manifest", http.MethodGet, "ok"))
			require.Empty(t, loader.manifests.claims, "no claim outlives its resolution")
		})
	}
}

func TestTagResolutionFallsBackToTagGetOnlyWithoutUsableHead(t *testing.T) {
	for _, test := range []struct {
		name   string
		source string
		head   func(*http.Response) *http.Response
	}{
		{"missing digest", tagFallbackIncomplete, func(r *http.Response) *http.Response { r.Header.Del("Docker-Content-Digest"); return r }},
		{"malformed digest", tagFallbackIncomplete, func(r *http.Response) *http.Response {
			r.Header.Set("Docker-Content-Digest", "sha256:not-a-digest")
			return r
		}},
		{"non-sha256 digest", tagFallbackIncomplete, func(r *http.Response) *http.Response {
			r.Header.Set("Docker-Content-Digest", "sha512:"+strings.Repeat("ab", 64))
			return r
		}},
		{"missing content type", tagFallbackIncomplete, func(r *http.Response) *http.Response { r.Header.Del("Content-Type"); return r }},
		{"missing content length", tagFallbackIncomplete, func(r *http.Response) *http.Response {
			r.Header.Del("Content-Length")
			r.ContentLength = -1
			return r
		}},
		{"non-200 success", tagFallbackIncomplete, func(r *http.Response) *http.Response { return withStatus(r, http.StatusNoContent) }},
		{"method not allowed", tagFallbackUnsupported, func(r *http.Response) *http.Response { return withStatus(r, http.StatusMethodNotAllowed) }},
		{"not implemented", tagFallbackUnsupported, func(r *http.Response) *http.Response { return withStatus(r, http.StatusNotImplemented) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			registry := newMemoryRegistry(f)
			registry.head = test.head
			loader := newMemoryLoader(t, registry)
			before := tagResolutionCounts()
			var resolved Resolution
			for range 3 {
				var err error
				resolved, err = loader.Resolve(t.Context(), f.ref(), testPlatform)
				require.NoError(t, err)
				require.Equal(t, f.manifestID.String(), resolved.ManifestID())
			}
			require.Equal(t, 3, registry.count("HEAD tag"))
			require.Equal(t, 3, registry.count("GET tag"), "a registry without a usable HEAD keeps one GET per resolution")
			require.Zero(t, registry.count("GET digest"))
			require.Equal(t, map[string]float64{test.source: 3}, tagResolutionDelta(before))
			// Recovery by pin and MaterializationRequired resolve the digest.
			exchanges := registry.total()
			_, err := loader.Resolve(t.Context(), resolved.SourceReference(), testPlatform)
			require.NoError(t, err)
			require.Equal(t, exchanges, registry.total(), "bytes verified by a tag GET serve later digest references without a request")
		})
	}
	t.Run("index children stay cached", func(t *testing.T) {
		f := newRegistry(t, encodedTar(t))
		indexID := useIndex(t, f)
		registry := newMemoryRegistry(f)
		registry.head = func(r *http.Response) *http.Response { r.Header.Del("Docker-Content-Digest"); return r }
		loader := newMemoryLoader(t, registry)
		var resolved Resolution
		for range 3 {
			var err error
			resolved, err = loader.Resolve(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
		}
		require.Equal(t, 3, registry.count("GET tag"))
		require.Equal(t, 1, registry.count("GET digest"), "the selected platform manifest is read by digest once")
		exchanges := registry.total()
		for _, reference := range []string{strings.TrimSuffix(f.ref(), ":latest") + "@" + indexID.String(), resolved.SourceReference()} {
			_, err := loader.Resolve(t.Context(), reference, testPlatform)
			require.NoError(t, err)
		}
		require.Equal(t, exchanges, registry.total(), "the index a tag GET verified and its child serve digest references without a request")
	})
}

func TestTagResolutionHeadFailureNeverFallsBackToGet(t *testing.T) {
	status := func(code int, header http.Header) func(*http.Request) (*http.Response, error) {
		return func(r *http.Request) (*http.Response, error) {
			response := &http.Response{Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: make(http.Header), Body: http.NoBody, Request: r}
			for key, values := range header {
				response.Header[key] = append([]string(nil), values...)
			}
			return withStatus(response, code), nil
		}
	}
	for _, test := range []struct {
		name     string
		heads    int
		contains string
		// class is the status label every one of these HEAD exchanges counts
		// under; the refused private hop is never dispatched.
		class string
		fail  func(*http.Request) (*http.Response, error)
	}{
		{"rate limited", 1, "429", "429", status(http.StatusTooManyRequests, nil)},
		{"rate limited past the retry ceiling", 1, "429", "429", status(http.StatusTooManyRequests, http.Header{"Retry-After": {"3600"}})},
		{"rate limited with a short retry-after", registryAttempts, "429", "429", status(http.StatusTooManyRequests, http.Header{"Retry-After": {"1"}})},
		{"not found", 1, "404", "4xx", status(http.StatusNotFound, nil)},
		{"unauthorized", 1, "401", "4xx", status(http.StatusUnauthorized, nil)},
		{"forbidden", 1, "403", "4xx", status(http.StatusForbidden, nil)},
		{"unavailable", registryAttempts, "503", "5xx", status(http.StatusServiceUnavailable, nil)},
		{"connection reset", registryAttempts, "reset", "error", func(*http.Request) (*http.Response, error) { return nil, syscall.ECONNRESET }},
		{"private redirect", 1, "private or link-local", "ok", status(http.StatusTemporaryRedirect, http.Header{"Location": {"https://169.254.169.254/manifest"}})},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			synctest.Test(t, func(t *testing.T) {
				registry := newMemoryRegistry(f)
				var failing atomic.Bool
				wire := roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if failing.Load() && r.Method == http.MethodHead {
						registry.record(r)
						return test.fail(r)
					}
					return registry.RoundTrip(r)
				})
				loader := newMemoryLoader(t, wire)
				_, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
				require.NoError(t, err, "verified bytes for the tag's digest are cached")
				failing.Store(true)
				heads, gets := registry.count("HEAD tag"), registry.manifestGETs()
				before := tagResolutionCounts()
				counted := registryRequestCount(endpointManifest, http.MethodHead, test.class)
				_, err = loader.Resolve(t.Context(), f.ref(), testPlatform)
				require.ErrorContains(t, err, test.contains)
				require.Equal(t, test.heads, registry.count("HEAD tag")-heads, "HEAD spends the metadata attempt budget, no more")
				require.Equal(t, gets, registry.manifestGETs(), "a failed HEAD never spends a manifest GET")
				require.Empty(t, tagResolutionDelta(before))
				require.Equal(t, counted+float64(test.heads), registryRequestCount(endpointManifest, http.MethodHead, test.class),
					"the transport counts every dispatched HEAD, including one without a response")
			})
		})
	}
}

func TestTagResolutionObservesMovedTagWithOneGet(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	registry := newMemoryRegistry(f)
	loader := newMemoryLoader(t, registry)
	first, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	f.updateImage(t, func(cfg *ocispec.Image, _ *ocispec.Manifest) { cfg.Config.Env = []string{"MOVED=1"} })
	moved, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.NotEqual(t, first.SourceReference(), moved.SourceReference())
	require.Equal(t, f.manifestID.String(), moved.ManifestID())
	require.Equal(t, f.configID.String(), moved.ConfigID())
	require.Equal(t, 2, registry.manifestGETs(), "a moved tag is observed and fetched exactly once")
	again, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.Equal(t, moved.SourceReference(), again.SourceReference())
	require.Equal(t, 2, registry.manifestGETs())
	require.Equal(t, 3, registry.count("HEAD tag"))
}

func TestCachedImmutableReferencesNeedNoRegistryExchange(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	indexID := useIndex(t, f)
	registry := newMemoryRegistry(f)
	loader := newMemoryLoader(t, registry)
	resolved, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	recovery, err := loader.WithBudget(loader.VerificationBudget())
	require.NoError(t, err)
	exchanges := registry.total()
	for _, reference := range []string{resolved.SourceReference(), strings.TrimSuffix(f.ref(), ":latest") + "@" + indexID.String()} {
		for _, issuer := range []*Loader{loader, recovery} {
			again, err := issuer.Resolve(t.Context(), reference, testPlatform)
			require.NoError(t, err)
			require.Equal(t, resolved.SourceReference(), again.SourceReference())
		}
	}
	require.Equal(t, exchanges, registry.total(), "verified immutable references need no exchange, not even a ping")
}

func TestManifestCacheIsScopedToRegistryRepository(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	registry := newMemoryRegistry(f)
	loader := newMemoryLoader(t, registry)
	_, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	host := strings.TrimSuffix(f.ref(), "/tenant/image:latest")
	for index, reference := range []string{
		host + "/other/image:latest",
		"mirror.example/tenant/image:latest",
		host + "/third/image@" + f.manifestID.String(),
	} {
		resolved, err := loader.Resolve(t.Context(), reference, testPlatform)
		require.NoError(t, err)
		require.Equal(t, f.manifestID.String(), resolved.ManifestID())
		require.Equal(t, index+2, registry.manifestGETs(), "identical bytes in another repository or registry cannot borrow cached evidence")
	}
}

func TestConcurrentManifestMissesShareOneRegistryRead(t *testing.T) {
	for _, test := range []struct {
		name    string
		index   bool
		pinned  bool
		large   bool
		heads   int
		manifes int
	}{
		{name: "tag", heads: 16, manifes: 1},
		{name: "index tag", index: true, heads: 16, manifes: 2},
		{name: "digest reference", pinned: true, manifes: 1},
		// A large admitted manifest is cached like any other. An uncacheable
		// one would wake each waiter to a miss, and the waiters would then
		// read it again one claim at a time.
		{name: "large manifest", large: true, heads: 16, manifes: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			if test.large {
				f.updateImage(t, func(_ *ocispec.Image, manifest *ocispec.Manifest) {
					manifest.Annotations = map[string]string{"example.invalid/padding": strings.Repeat("a", 768<<10)}
				})
				require.Greater(t, len(f.manifest), 512<<10, "the padding yields a large admitted manifest")
			}
			if test.index {
				useIndex(t, f)
			}
			reference := f.ref()
			if test.pinned {
				reference = strings.TrimSuffix(f.ref(), ":latest") + "@" + f.manifestID.String()
			}
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				registry := newMemoryRegistry(f)
				registry.get = func(r *http.Request) error {
					select {
					case <-release:
						return nil
					case <-r.Context().Done():
						return r.Context().Err()
					}
				}
				loader := newMemoryLoader(t, registry)
				errs := make(chan error, 16)
				var workers sync.WaitGroup
				for range 16 {
					workers.Go(func() {
						_, err := loader.Resolve(t.Context(), reference, testPlatform)
						errs <- err
					})
				}
				synctest.Wait()
				require.Equal(t, test.heads, registry.count("HEAD tag"))
				require.Equal(t, 1, registry.manifestGETs(), "every other resolution waits for the claimed registry read")
				close(release)
				workers.Wait()
				close(errs)
				for err := range errs {
					require.NoError(t, err)
				}
				require.Equal(t, test.manifes, registry.manifestGETs(), "waiters re-read verified bytes instead of repeating the GET")
				require.Equal(t, 1, registry.count("GET blob"), "the collapsed resolution also shares the verified config")
				require.Empty(t, loader.manifests.claims)
			})
		})
	}
}

func TestManifestClaimCancellationStaysWithItsOwner(t *testing.T) {
	t.Run("claimant", func(t *testing.T) {
		f := newRegistry(t, encodedTar(t))
		synctest.Test(t, func(t *testing.T) {
			registry := newMemoryRegistry(f)
			var gets atomic.Int32
			registry.get = func(r *http.Request) error {
				if gets.Add(1) == 1 {
					<-r.Context().Done()
					return r.Context().Err()
				}
				return nil
			}
			loader := newMemoryLoader(t, registry)
			claimant, cancel := context.WithCancel(t.Context())
			defer cancel()
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { _, err := loader.Resolve(claimant, f.ref(), testPlatform); first <- err }()
			synctest.Wait()
			go func() { _, err := loader.Resolve(t.Context(), f.ref(), testPlatform); second <- err }()
			synctest.Wait()
			require.Empty(t, second, "the second resolution waits on the claimed read")
			cancel()
			require.ErrorIs(t, <-first, context.Canceled)
			require.NoError(t, <-second, "a waiter inherits neither the claimant's cancellation nor its error")
			require.Equal(t, 2, registry.manifestGETs(), "the waiter reads the bytes under its own context")
			_, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
			require.Equal(t, 2, registry.manifestGETs())
		})
	})
	t.Run("waiter", func(t *testing.T) {
		f := newRegistry(t, encodedTar(t))
		synctest.Test(t, func(t *testing.T) {
			release := make(chan struct{})
			registry := newMemoryRegistry(f)
			registry.get = func(r *http.Request) error {
				select {
				case <-release:
					return nil
				case <-r.Context().Done():
					return r.Context().Err()
				}
			}
			loader := newMemoryLoader(t, registry)
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { _, err := loader.Resolve(t.Context(), f.ref(), testPlatform); first <- err }()
			synctest.Wait()
			waiter, cancel := context.WithCancel(t.Context())
			go func() { _, err := loader.Resolve(waiter, f.ref(), testPlatform); second <- err }()
			synctest.Wait()
			cancel()
			require.ErrorIs(t, <-second, context.Canceled, "a waiter's own context bounds its wait")
			synctest.Wait()
			require.Empty(t, first, "the claimant keeps its read after a waiter leaves")
			close(release)
			require.NoError(t, <-first)
			_, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
			require.Equal(t, 1, registry.manifestGETs())
		})
	})
}

func TestHeadDigestContradictedByContentIsKeyedByContent(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	registry := newMemoryRegistry(f)
	announced := digest.FromString("bytes this registry never serves")
	// The fixture answers every unknown digest with its manifest.
	registry.head = func(r *http.Response) *http.Response {
		r.Header.Set("Docker-Content-Digest", announced.String())
		return r
	}
	loader := newMemoryLoader(t, registry)
	before := tagResolutionCounts()
	resolved, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.Equal(t, f.manifestID.String(), resolved.ManifestID(), "identity comes from the bytes, never from the header")
	repository := fixtureRepository(t, f)
	require.Contains(t, loader.manifests.entries, newManifestKey(repository, f.manifestID))
	require.NotContains(t, loader.manifests.entries, newManifestKey(repository, announced))
	require.Empty(t, loader.manifests.claims)
	_, err = loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.Equal(t, 2, registry.count("GET digest"), "a contradicted header cannot select cached bytes")
	require.Equal(t, map[string]float64{tagFromRegistry: 2}, tagResolutionDelta(before))
	exchanges := registry.total()
	_, err = loader.Resolve(t.Context(), resolved.SourceReference(), testPlatform)
	require.NoError(t, err)
	require.Equal(t, exchanges, registry.total(), "the content's own digest selects its verified bytes")
}

// A registry can announce, for two tags, digests whose bytes are indexes naming
// each other's announced digest. Held claims that did not name verified bytes
// would then wait on each other forever; both resolutions must instead fail.
func TestContradictedHeadsCannotOrderClaimsIntoACycle(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	first, second := digest.FromString("first announced"), digest.FromString("second announced")
	index := func(child digest.Digest) []byte {
		return mustJSON(t, ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex,
			Manifests: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageManifest, Digest: child, Size: 1, Platform: &testPlatform}}})
	}
	// Each announced digest reads an index whose child is the other one.
	bodies := map[string][]byte{first.String(): index(second), second.String(): index(first)}
	announced := map[string]digest.Digest{"first": first, "second": second}
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var gated atomic.Int32
		wire := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if !strings.Contains(r.URL.Path, "/manifests/") {
				return newMemoryRegistry(f).RoundTrip(r)
			}
			identifier := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			header := http.Header{"Content-Type": {ocispec.MediaTypeImageIndex}}
			response := withStatus(&http.Response{Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: header, Body: http.NoBody, Request: r}, http.StatusOK)
			if r.Method == http.MethodHead {
				header.Set("Docker-Content-Digest", announced[identifier].String())
				response.ContentLength = int64(len(bodies[announced[identifier].String()]))
				return response, nil
			}
			if gated.Add(1) <= 2 {
				select {
				case <-release:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			}
			body := bodies[identifier]
			response.ContentLength, response.Body = int64(len(body)), io.NopCloser(strings.NewReader(string(body)))
			return response, nil
		})
		loader := newMemoryLoader(t, wire)
		repository := strings.TrimSuffix(f.ref(), ":latest")
		results := make(chan error, 2)
		for _, tag := range []string{"first", "second"} {
			go func() {
				_, err := loader.Resolve(t.Context(), repository+":"+tag, testPlatform)
				results <- err
			}()
		}
		synctest.Wait()
		require.EqualValues(t, 2, gated.Load(), "both resolutions claimed their announced digests")
		close(release)
		for range 2 {
			require.ErrorContains(t, <-results, "differs from its requested digest")
		}
		require.Empty(t, loader.manifests.claims)
		require.Empty(t, loader.manifests.entries)
	})
}

func TestImmutableManifestReadsMustHashToTheirRequestedDigest(t *testing.T) {
	for _, test := range []string{"digest reference", "index child"} {
		t.Run(test, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			reference := strings.TrimSuffix(f.ref(), ":latest") + "@" + digest.FromString("absent reference").String()
			if test == "index child" {
				serveIndex(f, mustJSON(t, ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex,
					Manifests: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromString("absent child"), Size: int64(len(f.manifest)), Platform: &testPlatform}}}))
				reference = f.ref()
			}
			registry := newMemoryRegistry(f)
			loader := newMemoryLoader(t, registry)
			_, err := loader.Resolve(t.Context(), reference, testPlatform)
			require.ErrorContains(t, err, "differs from its requested digest")
			require.Empty(t, loader.manifests.entries)
			require.Zero(t, loader.manifests.bytes)
			require.Empty(t, loader.manifests.claims)
		})
	}
}

func TestManifestCacheEvictsWithinBothBoundsAndRefundsBytes(t *testing.T) {
	repository, err := name.NewRepository("registry.example/tenant/image")
	require.NoError(t, err)
	content := func(label string, raw []byte) verifiedManifest {
		id := digest.FromString(label)
		return verifiedManifest{key: newManifestKey(repository, id), digest: id, mediaType: ocispec.MediaTypeImageManifest, raw: raw}
	}
	accounted := func(cache *manifestCache) int {
		total := 0
		for element := cache.lru.Front(); element != nil; element = element.Next() {
			total += len(element.Value.(verifiedManifest).raw)
		}
		return total
	}
	for _, size := range []int{1, 4 << 10, maxCachedManifestEntryBytes} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			cache := &manifestCache{}
			first := content("first", []byte(strings.Repeat("y", size)))
			cache.retain(first)
			retained := cache.entries[first.key].Value.(verifiedManifest)
			// Enough entries to pass whichever bound binds first at this size.
			for i := range min(maxCachedManifests, maxCachedManifestBytes/size) + 1 {
				cache.retain(content(fmt.Sprint(i), make([]byte, size)))
				require.LessOrEqual(t, cache.bytes, maxCachedManifestBytes)
				require.LessOrEqual(t, len(cache.entries), maxCachedManifests)
				require.Equal(t, cache.lru.Len(), len(cache.entries))
				require.Equal(t, accounted(cache), cache.bytes, "eviction refunds exactly the retired bytes")
			}
			require.NotContains(t, cache.entries, first.key)
			require.Equal(t, strings.Repeat("y", size), string(retained.raw), "eviction does not mutate an active resolution's evidence")
		})
	}
	t.Run("oversized entry", func(t *testing.T) {
		require.GreaterOrEqual(t, maxCachedManifestEntryBytes, int(maxMetadataBytes), "every document the reader can admit is cacheable")
		cache := &manifestCache{}
		kept := content("kept", []byte("x"))
		cache.retain(kept)
		cache.retain(content("oversized", make([]byte, maxCachedManifestEntryBytes+1)))
		require.Len(t, cache.entries, 1, "an oversized document displaces nothing")
		require.Contains(t, cache.entries, kept.key)
		require.Equal(t, 1, cache.bytes)
	})
	t.Run("repeat", func(t *testing.T) {
		cache := &manifestCache{}
		older, newer := content("older", []byte("older")), content("newer", []byte("newer"))
		cache.retain(older)
		cache.retain(newer)
		cache.retain(older)
		require.Equal(t, len("older")+len("newer"), cache.bytes, "a repeat is not charged twice")
		require.Equal(t, older.key, cache.lru.Front().Value.(verifiedManifest).key, "a repeat refreshes recency")
	})
	t.Run("hit refreshes recency", func(t *testing.T) {
		cache := &manifestCache{}
		used, idle := content("used", []byte("used")), content("idle", []byte("idle"))
		cache.retain(used)
		cache.retain(idle)
		hit, release, err := cache.claim(t.Context(), used.key)
		require.NoError(t, err)
		require.Nil(t, release, "a hit claims nothing")
		require.Equal(t, used.raw, hit.raw)
		for i := range maxCachedManifests - 1 {
			cache.retain(content(fmt.Sprint(i), []byte("x")))
		}
		require.Contains(t, cache.entries, used.key, "a resolved image stays warm")
		require.NotContains(t, cache.entries, idle.key, "the least recently used entry is evicted first")
	})
}

func TestManifestCacheRetainsOnlyAdmittedResolutions(t *testing.T) {
	for _, test := range []struct {
		name string
		// prepare returns the immutable root to resolve by digest, if not the
		// fixture manifest.
		prepare  func(*testing.T, *registryFixture) digest.Digest
		platform ocispec.Platform
		budget   int64
		contains string
	}{
		{name: "config platform", platform: ocispec.Platform{OS: "linux", Architecture: "arm64"}, contains: "selected platform"},
		{name: "config bytes", prepare: func(_ *testing.T, f *registryFixture) digest.Digest {
			f.config = []byte(strings.Repeat("!", len(f.config)))
			return ""
		}, contains: "differs from its verified descriptor"},
		{name: "runnable manifest", prepare: func(t *testing.T, f *registryFixture) digest.Digest {
			f.updateImage(t, func(_ *ocispec.Image, manifest *ocispec.Manifest) {
				manifest.ArtifactType = "application/vnd.example.artifact"
			})
			return ""
		}, contains: "not a bounded runnable image"},
		{name: "metadata budget", prepare: func(t *testing.T, f *registryFixture) digest.Digest {
			return serveIndex(f, mustJSON(t, ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex,
				Manifests:   []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageManifest, Digest: f.manifestID, Size: int64(len(f.manifest)), Platform: &testPlatform}},
				Annotations: map[string]string{"example.invalid/metadata": strings.Repeat("x", 100<<10)}}))
		}, budget: 64 << 10, contains: "exceeds budget"},
		{name: "index platform", prepare: func(t *testing.T, f *registryFixture) digest.Digest {
			return useIndex(t, f)
		}, platform: ocispec.Platform{OS: "linux", Architecture: "arm64"}, contains: "no matching platform"},
		{name: "index child config", prepare: func(t *testing.T, f *registryFixture) digest.Digest {
			root := useIndex(t, f)
			f.config = []byte(strings.Repeat("!", len(f.config)))
			return root
		}, contains: "differs from its verified descriptor"},
	} {
		for _, route := range []string{"tag", "tag without HEAD digest", "digest"} {
			t.Run(test.name+"/"+route, func(t *testing.T) {
				f := newRegistry(t, encodedTar(t))
				var root digest.Digest
				if test.prepare != nil {
					root = test.prepare(t, f)
				}
				if root == "" {
					root = f.manifestID
				}
				reference := f.ref()
				if route == "digest" {
					reference = strings.TrimSuffix(f.ref(), ":latest") + "@" + root.String()
				}
				platform := test.platform
				if platform.OS == "" {
					platform = testPlatform
				}
				budget := test.budget
				if budget == 0 {
					budget = 1 << 20
				}
				registry := newMemoryRegistry(f)
				if route == "tag without HEAD digest" {
					registry.head = func(r *http.Response) *http.Response { r.Header.Del("Docker-Content-Digest"); return r }
				}
				loader, err := NewLoader(&recordingImporter{}, t.TempDir(), budget, withRegistryTransportForTest(registry))
				require.NoError(t, err)
				_, err = loader.Resolve(t.Context(), reference, platform)
				require.ErrorContains(t, err, test.contains)
				require.Positive(t, registry.manifestGETs(), "the rejected bytes were read from the registry")
				require.Empty(t, loader.manifests.entries, "bytes that fail admission are never retained")
				require.Zero(t, loader.manifests.bytes)
				require.Empty(t, loader.manifests.claims)
			})
		}
	}
}

func TestFailedAdmissionsCannotEvictVerifiedManifests(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	registry := newMemoryRegistry(f)
	var mu sync.Mutex
	junk := make(map[string][]byte)
	var junkGets int
	wire := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.Contains(r.URL.Path, "/attacker/") || !strings.Contains(r.URL.Path, "/manifests/") {
			return registry.RoundTrip(r)
		}
		identifier := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		mu.Lock()
		defer mu.Unlock()
		body, ok := junk[identifier]
		if !ok {
			// A well-formed but non-runnable document that fits the per-entry
			// cap: only admission, not size, keeps it out of the cache.
			body = []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,"artifactType":"application/vnd.example.junk","config":{"mediaType":%q,"digest":%q,"size":1},"layers":[],"annotations":{"pad":%q}}`,
				ocispec.MediaTypeImageManifest, ocispec.MediaTypeImageConfig, f.configID, identifier+strings.Repeat("A", 200<<10)))
			junk[identifier] = body
			junk[digest.FromBytes(body).String()] = body
		}
		header := http.Header{"Content-Type": {ocispec.MediaTypeImageManifest}, "Docker-Content-Digest": {digest.FromBytes(body).String()}, "Content-Length": {strconv.Itoa(len(body))}}
		response := withStatus(&http.Response{Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: header, ContentLength: int64(len(body)), Body: http.NoBody, Request: r}, http.StatusOK)
		if r.Method == http.MethodGet {
			junkGets++
			response.Body = io.NopCloser(strings.NewReader(string(body)))
		}
		return response, nil
	})
	loader := newMemoryLoader(t, wire)
	for range 2 {
		_, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
		require.NoError(t, err)
	}
	entries, bytes := len(loader.manifests.entries), loader.manifests.bytes
	attacker := strings.Replace(f.ref(), "/tenant/image:latest", "/attacker/image:t", 1)
	for i := range 40 {
		_, err := loader.Resolve(t.Context(), attacker+strconv.Itoa(i), testPlatform)
		require.ErrorContains(t, err, "not a bounded runnable image")
	}
	require.Equal(t, 40, junkGets)
	require.Equal(t, entries, len(loader.manifests.entries), "unadmitted bytes occupy no cache entry")
	require.Equal(t, bytes, loader.manifests.bytes)
	_, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	require.Equal(t, 1, registry.manifestGETs(), "the verified tenant image stays warm")
}

// A tag HEAD and the GET it selects are one tag resolution: whether the HEAD
// announces an uncached digest or leads to a tag fallback, a redirected HEAD
// cannot buy its GET a second ten-exchange chain.
func TestTagHeadAndItsGetShareOneRedirectAllowance(t *testing.T) {
	for _, route := range []string{"announced digest", "tag fallback"} {
		for _, test := range []struct {
			name              string
			headHops, getHops int
			heads, gets       int32
			failure           string
		}{
			{name: "spent by the HEAD", headHops: registryRedirectRequests - 1, heads: registryRedirectRequests, failure: "redirect limit"},
			{name: "shared within the allowance", headHops: 4, getHops: 4, heads: 5, gets: 5},
			{name: "exceeded only in combination", headHops: 5, getHops: 5, heads: 6, gets: 4, failure: "redirect limit"},
		} {
			t.Run(route+"/"+test.name, func(t *testing.T) {
				f := newRegistry(t, encodedTar(t))
				registry := newMemoryRegistry(f)
				target := map[string]string{"announced digest": "digest", "tag fallback": "tag"}[route]
				var heads, gets, misdirected atomic.Int32
				wire := roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if !strings.Contains(r.URL.Path, "/manifests/") {
						return registry.RoundTrip(r)
					}
					hops := test.headHops
					if r.Method == http.MethodGet {
						gets.Add(1)
						hops = test.getHops
						if registryTarget(r.URL.Path) != target {
							misdirected.Add(1)
						}
					} else {
						heads.Add(1)
					}
					hop, _ := strconv.Atoi(r.URL.Query().Get("hop"))
					if hop < hops {
						location := "https://" + r.URL.Host + r.URL.Path + "?hop=" + strconv.Itoa(hop+1)
						return withStatus(&http.Response{Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: http.Header{"Location": {location}}, Body: http.NoBody, Request: r}, http.StatusTemporaryRedirect), nil
					}
					response, err := registry.RoundTrip(r)
					if err == nil && r.Method == http.MethodHead && route == "tag fallback" {
						response.Header.Del("Docker-Content-Digest")
					}
					return response, err
				})
				loader := newMemoryLoader(t, wire)
				_, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
				if test.failure == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, test.failure)
				}
				require.Equal(t, test.heads, heads.Load())
				require.Equal(t, test.gets, gets.Load(), "the GET spends only what its HEAD left of one ten-exchange allowance")
				require.Zero(t, misdirected.Load(), "the %s route GETs its %s", route, target)
			})
		}
	}
}

// A tag's HEAD must announce the digest of the representation a GET returns,
// so every manifest request negotiates go-containerregistry's complete media
// type set: the tag HEAD, the GET of its announced digest, a fallback tag GET,
// index children and digest references. A HEAD lacking the index types could
// announce a single-platform or schema1 digest, or draw a 404 from a registry
// that serves the tag only as an index.
func TestManifestRequestsNegotiateOneAcceptSet(t *testing.T) {
	expected := []string{
		string(types.DockerManifestSchema1), string(types.DockerManifestSchema1Signed),
		string(types.DockerManifestSchema2), string(types.OCIManifestSchema1),
		string(types.DockerManifestList), string(types.OCIImageIndex),
	}
	for _, test := range []struct {
		route    string
		requests map[string]int
	}{
		{"announced digest", map[string]int{"HEAD tag": 1, "GET digest": 2}},
		{"tag fallback", map[string]int{"HEAD tag": 1, "GET tag": 1, "GET digest": 1}},
		{"digest reference", map[string]int{"GET digest": 2}},
	} {
		t.Run(test.route, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			indexID := useIndex(t, f)
			registry := newMemoryRegistry(f)
			if test.route == "tag fallback" {
				registry.head = func(r *http.Response) *http.Response { r.Header.Del("Docker-Content-Digest"); return r }
			}
			var mu sync.Mutex
			accepts := make(map[string][]string)
			wire := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if strings.Contains(r.URL.Path, "/manifests/") {
					mu.Lock()
					key := r.Method + " " + registryTarget(r.URL.Path)
					accepts[key] = append(accepts[key], r.Header.Get("Accept"))
					mu.Unlock()
				}
				return registry.RoundTrip(r)
			})
			loader := newMemoryLoader(t, wire)
			reference := f.ref()
			if test.route == "digest reference" {
				reference = strings.TrimSuffix(f.ref(), ":latest") + "@" + indexID.String()
			}
			_, err := loader.Resolve(t.Context(), reference, testPlatform)
			require.NoError(t, err)
			requests, distinct := make(map[string]int), make(map[string]bool)
			for key, values := range accepts {
				requests[key] = len(values)
				for _, accept := range values {
					require.ElementsMatch(t, expected, strings.Split(accept, ","), "%s negotiates the complete manifest set", key)
					distinct[accept] = true
				}
			}
			require.Equal(t, test.requests, requests, "every manifest request path was exercised")
			require.Len(t, distinct, 1, "HEAD and GET send one identical Accept header")
		})
	}
}

func TestRegistryQuotaRefusalIsCountedAndFinal(t *testing.T) {
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			f := newRegistry(t, encodedTar(t))
			registry := newMemoryRegistry(f)
			if method == http.MethodGet {
				registry.head = func(r *http.Response) *http.Response { r.Header.Del("Docker-Content-Digest"); return r }
			}
			var refused atomic.Int32
			wire := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != method || !strings.Contains(r.URL.Path, "/manifests/") {
					return registry.RoundTrip(r)
				}
				refused.Add(1)
				body := `{"errors":[{"code":"TOOMANYREQUESTS","message":"You have reached your unauthenticated pull rate limit."}]}`
				header := http.Header{"Content-Type": {"application/json"}, "Ratelimit-Limit": {"100;w=3600"}, "Ratelimit-Remaining": {"0;w=3600"}}
				response := withStatus(&http.Response{Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: header, Body: http.NoBody, Request: r}, http.StatusTooManyRequests)
				if method == http.MethodGet {
					response.ContentLength, response.Body = int64(len(body)), io.NopCloser(strings.NewReader(body))
				}
				return response, nil
			})
			loader := newMemoryLoader(t, wire)
			before := registryRequestCount("manifest", method, "429")
			_, err := loader.Resolve(t.Context(), f.ref(), testPlatform)
			// A HEAD has no diagnostic body; the GET carries the registry's code.
			require.ErrorContains(t, err, map[string]string{http.MethodHead: "429", http.MethodGet: "TOOMANYREQUESTS"}[method])
			require.EqualValues(t, 1, refused.Load(), "a quota refusal without a short Retry-After is not retried")
			require.Equal(t, before+1, registryRequestCount("manifest", method, "429"))
		})
	}
}

func TestRegistryExchangeMetricLabelsAreBoundedClasses(t *testing.T) {
	for _, test := range []struct {
		method, url string
		status      int
		err         error
		labels      [3]string
	}{
		{http.MethodGet, "https://registry.example/v2/", http.StatusUnauthorized, nil, [3]string{"ping", http.MethodGet, "4xx"}},
		{http.MethodHead, "https://registry.example/v2/tenant/image/manifests/latest", http.StatusOK, nil, [3]string{"manifest", http.MethodHead, "ok"}},
		{http.MethodGet, "https://registry.example/v2/tenant/image/manifests/sha256:" + strings.Repeat("0", 64), http.StatusTooManyRequests, nil, [3]string{"manifest", http.MethodGet, "429"}},
		{http.MethodGet, "https://registry.example/v2/tenant/image/blobs/sha256:" + strings.Repeat("0", 64), http.StatusTemporaryRedirect, nil, [3]string{"blob", http.MethodGet, "ok"}},
		// A repository may itself contain a manifests or blobs component; the
		// final API marker names the endpoint.
		{http.MethodGet, "https://registry.example/v2/acme/manifests/app/blobs/sha256:" + strings.Repeat("0", 64), http.StatusOK, nil, [3]string{"blob", http.MethodGet, "ok"}},
		{http.MethodHead, "https://registry.example/v2/acme/blobs/app/manifests/latest", http.StatusOK, nil, [3]string{"manifest", http.MethodHead, "ok"}},
		{http.MethodGet, "https://cdn.example/registry-v2/docker/registry/v2/blobs/sha256/00/" + strings.Repeat("0", 64) + "/data", http.StatusOK, nil, [3]string{"blob", http.MethodGet, "ok"}},
		{http.MethodGet, "https://cdn.example/content?signature=secret", http.StatusBadGateway, nil, [3]string{"other", http.MethodGet, "5xx"}},
		{http.MethodPost, "https://auth.example/token", http.StatusOK, nil, [3]string{"other", "other", "ok"}},
		{http.MethodGet, "https://registry.example/v2/tenant/image/blobs/sha256:" + strings.Repeat("0", 64), 0, io.ErrUnexpectedEOF, [3]string{"blob", http.MethodGet, "error"}},
	} {
		t.Run(strings.Join(test.labels[:], "/"), func(t *testing.T) {
			request, err := http.NewRequestWithContext(t.Context(), test.method, test.url, nil)
			require.NoError(t, err)
			var response *http.Response
			if test.err == nil {
				response = &http.Response{StatusCode: test.status, Header: make(http.Header), Body: http.NoBody}
			}
			before := registryRequestCount(test.labels[0], test.labels[1], test.labels[2])
			observeRegistryExchange(request, response, test.err)
			require.Equal(t, before+1, registryRequestCount(test.labels[0], test.labels[1], test.labels[2]))
		})
	}
}

// A fresh test process observes package initialization before any exchange
// can create a series; alerts select these series from process start.
func TestRegistryCountersInitializedBeforeFirstExchange(t *testing.T) {
	const child = "FRED_TEST_REGISTRY_METRIC_INITIALIZATION"
	if os.Getenv(child) == "" {
		executable, err := os.Executable()
		require.NoError(t, err)
		command := exec.CommandContext(t.Context(), executable, "-test.run=^TestRegistryCountersInitializedBeforeFirstExchange$")
		command.Env = append(os.Environ(), child+"=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	requests, resolutions := make(map[string]float64), make(map[string]float64)
	for _, family := range families {
		switch family.GetName() {
		case "fred_docker_backend_image_registry_requests_total":
			for _, metric := range family.Metric {
				labels := make(map[string]string)
				for _, label := range metric.Label {
					labels[label.GetName()] = label.GetValue()
				}
				require.Len(t, labels, 3)
				requests[labels["endpoint"]+"/"+labels["method"]+"/"+labels["status"]] = metric.Counter.GetValue()
			}
		case "fred_docker_backend_image_tag_resolutions_total":
			for _, metric := range family.Metric {
				require.Len(t, metric.Label, 1)
				require.Equal(t, "source", metric.Label[0].GetName())
				resolutions[metric.Label[0].GetValue()] = metric.Counter.GetValue()
			}
		}
	}
	expected := make(map[string]float64)
	for _, endpoint := range []string{"manifest", "blob", "ping", "other"} {
		for _, method := range []string{"GET", "HEAD"} {
			for _, status := range []string{"ok", "4xx", "429", "5xx", "error"} {
				expected[endpoint+"/"+method+"/"+status] = 0
			}
		}
	}
	require.Equal(t, expected, requests)
	require.Equal(t, map[string]float64{"cache": 0, "registry": 0, "fallback_unsupported": 0, "fallback_incomplete": 0}, resolutions)
}
