package imagefetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	registryauth "github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/opencontainers/go-digest"
)

// manifestAccept is the negotiation shared by HEAD and GET, so a tag's HEAD
// announces the digest of the representation a GET would return. It is
// go-containerregistry's complete manifest list, which fred's GETs sent before.
var manifestAccept = strings.Join([]string{
	string(types.DockerManifestSchema1), string(types.DockerManifestSchema1Signed),
	string(types.DockerManifestSchema2), string(types.OCIManifestSchema1),
	string(types.DockerManifestList), string(types.OCIImageIndex),
}, ",")

// manifestReader is one resolution's manifest client for one repository. It
// authenticates lazily and at most once, so a tag HEAD, its GET and index
// children share one ping and token, and a failed setup is not repeated. An
// immutable reference served entirely from verified bytes contacts no registry.
//
// A claim whose read returned its key's bytes stays held until Resolve retains
// its admitted bytes or ends, so concurrent resolutions of the same content
// wait for that outcome rather than repeating the registry read. Every claim
// is registered with close as soon as it is granted, before its registry read,
// so a read that panics still releases it while Resolve unwinds.
type manifestReader struct {
	cache         *manifestCache
	exchange      singleRegistryExchange
	repository    name.Repository
	authenticated bool
	client        *http.Client
	setupErr      error
	claims        []func()
	fetched       []verifiedManifest
}

func (l *Loader) newManifestReader(repository name.Repository) *manifestReader {
	return &manifestReader{cache: l.manifests, exchange: l.transport, repository: repository}
}

// retainAdmitted publishes this resolution's registry bytes. Resolve calls it
// only after the metadata budget, the selected manifest and its config passed
// admission, so bytes that fail admission never displace verified evidence.
func (r *manifestReader) retainAdmitted() {
	for _, content := range r.fetched {
		r.cache.retain(content)
	}
}

// hold registers a granted claim with close. Release is idempotent, so a read
// that fails or is contradicted may still release its claim early.
func (r *manifestReader) hold(release func()) {
	r.claims = append(r.claims, release)
}

// close releases every claim after any retain, waking waiters to re-read.
func (r *manifestReader) close() {
	for _, release := range r.claims {
		release()
	}
	r.claims = nil
}

func (r *manifestReader) read(ctx context.Context, reference name.Reference) (verifiedManifest, error) {
	switch reference := reference.(type) {
	case name.Tag:
		return r.resolveTag(ctx, reference)
	case name.Digest:
		return r.readDigest(ctx, reference)
	default:
		return verifiedManifest{}, fmt.Errorf("unsupported registry reference %T", reference)
	}
}

// resolveTag re-resolves a mutable tag on every preparation with one HEAD,
// which Docker Hub does not meter as a pull. The announced digest only selects
// bytes of this repository whose SHA256 this package computed; it never
// supplies content. A miss GETs that digest, as dockerd and containerd do.
// Only a HEAD refused with 405/501, or a 2xx lacking a usable digest, type or
// length, falls back to a GET of the tag. Any other HEAD failure, including a
// 429 or a transport error, is the resolution's result.
func (r *manifestReader) resolveTag(ctx context.Context, tag name.Tag) (verifiedManifest, error) {
	client, err := r.authenticate(ctx)
	if err != nil {
		return verifiedManifest{}, err
	}
	operation := withRegistryOperationAllowance(ctx)
	announced, fallback, err := r.head(operation, client, tag)
	if err != nil {
		return verifiedManifest{}, err
	}
	if fallback != "" {
		imageTagResolutions.WithLabelValues(fallback).Inc()
		content, err := r.get(operation, client, tag)
		if err != nil {
			return verifiedManifest{}, err
		}
		r.fetched = append(r.fetched, content)
		return content, nil
	}
	cached, release, err := r.cache.claim(ctx, newManifestKey(r.repository, announced))
	if err != nil {
		return verifiedManifest{}, err
	}
	if release == nil {
		imageTagResolutions.WithLabelValues(tagFromCache).Inc()
		return cached, nil
	}
	r.hold(release)
	imageTagResolutions.WithLabelValues(tagFromRegistry).Inc()
	content, err := r.get(operation, client, r.repository.Digest(announced.String()))
	if err != nil {
		release()
		return verifiedManifest{}, err
	}
	if content.digest != announced {
		// The content digest wins: the header only chose what to request.
		// Keep no claim naming other bytes, so every held claim remains a
		// verified ancestor of whatever this resolution waits for next.
		release()
	}
	r.fetched = append(r.fetched, content)
	return content, nil
}

// readDigest serves an exact reference or index child from verified bytes
// when possible. A registry response must hash to the requested digest.
func (r *manifestReader) readDigest(ctx context.Context, reference name.Digest) (verifiedManifest, error) {
	want, err := digest.Parse(reference.DigestStr())
	if err != nil {
		return verifiedManifest{}, err
	}
	cached, release, err := r.cache.claim(ctx, newManifestKey(r.repository, want))
	if err != nil {
		return verifiedManifest{}, err
	}
	if release == nil {
		return cached, nil
	}
	r.hold(release)
	content, err := r.fetchExact(ctx, reference, want)
	if err != nil {
		release()
		return verifiedManifest{}, err
	}
	r.fetched = append(r.fetched, content)
	return content, nil
}

func (r *manifestReader) fetchExact(ctx context.Context, reference name.Digest, want digest.Digest) (verifiedManifest, error) {
	client, err := r.authenticate(ctx)
	if err != nil {
		return verifiedManifest{}, err
	}
	content, err := r.get(ctx, client, reference)
	if err != nil {
		return verifiedManifest{}, err
	}
	if content.digest != want {
		return verifiedManifest{}, errors.New("registry manifest differs from its requested digest")
	}
	return content, nil
}

func (r *manifestReader) authenticate(ctx context.Context) (*http.Client, error) {
	if !r.authenticated {
		r.authenticated = true
		transport, err := registryauth.NewWithContext(ctx, r.repository.Registry, authn.Anonymous,
			registryTransport{base: boundedTransport{base: r.exchange, limit: maxMetadataBytes}},
			[]string{r.repository.Scope(registryauth.PullScope)})
		if err != nil {
			r.setupErr = err
		} else {
			r.client = &http.Client{
				Transport: transport,
				// The registry transport owns each complete chain: its HTTPS,
				// private-IP, credential-origin and ten-exchange policies.
				CheckRedirect: func(*http.Request, []*http.Request) error { return nil },
			}
		}
	}
	return r.client, r.setupErr
}

// head returns the tag's announced SHA256 digest, or the fallback source that
// selects a GET of the tag.
func (r *manifestReader) head(ctx context.Context, client *http.Client, tag name.Tag) (digest.Digest, string, error) {
	request, err := r.request(ctx, http.MethodHead, tag.Identifier())
	if err != nil {
		return "", "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = response.Body.Close() }()
	switch {
	case response.StatusCode == http.StatusMethodNotAllowed || response.StatusCode == http.StatusNotImplemented:
		return "", tagFallbackUnsupported, nil
	case response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices:
		return "", "", registryauth.CheckError(response, http.StatusOK)
	}
	announced, err := digest.Parse(response.Header.Get("Docker-Content-Digest"))
	if response.StatusCode != http.StatusOK || err != nil || announced.Algorithm() != digest.SHA256 ||
		response.Header.Get("Content-Type") == "" || response.ContentLength < 0 {
		return "", tagFallbackIncomplete, nil
	}
	return announced, "", nil
}

// get returns registry bytes keyed by their own digest; callers decide how the
// requested reference binds them.
func (r *manifestReader) get(ctx context.Context, client *http.Client, reference name.Reference) (verifiedManifest, error) {
	request, err := r.request(ctx, http.MethodGet, reference.Identifier())
	if err != nil {
		return verifiedManifest{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return verifiedManifest{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if err := registryauth.CheckError(response, http.StatusOK); err != nil {
		return verifiedManifest{}, err
	}
	raw, err := io.ReadAll(&budgetReader{reader: response.Body, remaining: maxMetadataBytes})
	if err != nil {
		return verifiedManifest{}, err
	}
	id := digest.FromBytes(raw)
	return verifiedManifest{key: newManifestKey(r.repository, id), digest: id, mediaType: response.Header.Get("Content-Type"), raw: raw}, nil
}

func (r *manifestReader) request(ctx context.Context, method, identifier string) (*http.Request, error) {
	location := url.URL{Scheme: "https", Host: r.repository.RegistryStr(), Path: fmt.Sprintf("/v2/%s/manifests/%s", r.repository.RepositoryStr(), identifier)}
	request, err := http.NewRequestWithContext(ctx, method, location.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", manifestAccept)
	return request, nil
}
