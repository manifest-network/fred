package imagefetch

import (
	"container/list"
	"context"
	"sync"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/opencontainers/go-digest"
)

const (
	maxCachedManifestBytes = 32 << 20
	// Every document the reader can admit fits one entry, so the bytes a
	// claimant retains always serve the waiters it wakes. A smaller cap would
	// leave larger admitted images uncached, and each waiter would claim and
	// fetch them again in turn. One entry displaces at most a sixteenth of
	// the byte cap.
	maxCachedManifestEntryBytes = int(maxMetadataBytes)
	// An index image uses two entries: its index and the selected manifest.
	maxCachedManifests = 1024
)

// manifestKey binds a digest to one exact registry repository. Registry
// evidence is never shared across repositories, even for identical bytes.
type manifestKey string

func newManifestKey(repository name.Repository, id digest.Digest) manifestKey {
	return manifestKey(repository.RegistryStr() + "/" + repository.RepositoryStr() + "@" + id.String())
}

// verifiedManifest holds registry bytes whose SHA256 this package computed.
// A registry's tag HEAD can select these bytes but never supply content. The
// entry proves identity only: every Resolve re-applies the metadata budget and
// platform/layer admission, including on a cache hit.
type verifiedManifest struct {
	key       manifestKey
	digest    digest.Digest
	mediaType string
	raw       []byte
}

// manifestCache is a bounded LRU of admitted manifest bytes, shared like
// configCache by Loader copies and recovery issuers. Eviction revokes no
// active Resolution; its immutable bytes stay owned by that caller.
//
// claims collapses concurrent registry reads of one key. A claimant whose read
// returns the key's bytes keeps its claim until its Resolve retains them or
// ends; a failed or contradicted read releases it at once. Waiters share no
// result, error or deadline with it: each waits under its own context, then
// re-reads the cache and, on a miss, claims the key itself. Their latency does
// follow the claimant's whole Resolve, including its config read, which lets
// them reuse that verified config too. A stalled claimant therefore delays
// them within its own no-progress timers, attempts and deadline, as the
// members of an image download flight wait for its one download.
type manifestCache struct {
	mu      sync.Mutex
	entries map[manifestKey]*list.Element
	lru     list.List
	bytes   int
	claims  map[manifestKey]chan struct{}
}

// claim returns cached bytes for key, or makes the caller key's only registry
// reader. The lookup and the claim happen under one lock, so a peer cannot
// retain the bytes between them. The claimant calls release after any retain;
// release is idempotent.
func (c *manifestCache) claim(ctx context.Context, key manifestKey) (cached verifiedManifest, release func(), err error) {
	for {
		c.mu.Lock()
		if entry := c.entries[key]; entry != nil {
			c.lru.MoveToFront(entry)
			content := entry.Value.(verifiedManifest)
			c.mu.Unlock()
			return content, nil, nil
		}
		busy, claimed := c.claims[key]
		if !claimed {
			if c.claims == nil {
				c.claims = make(map[manifestKey]chan struct{})
			}
			done := make(chan struct{})
			c.claims[key] = done
			c.mu.Unlock()
			return verifiedManifest{}, sync.OnceFunc(func() {
				c.mu.Lock()
				delete(c.claims, key)
				c.mu.Unlock()
				close(done)
			}), nil
		}
		c.mu.Unlock()
		select {
		case <-busy:
		case <-ctx.Done():
			return verifiedManifest{}, nil, ctx.Err()
		}
	}
}

// retain publishes admitted bytes under their own repository and digest.
func (c *manifestCache) retain(content verifiedManifest) {
	if len(content.raw) > maxCachedManifestEntryBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[manifestKey]*list.Element)
	}
	if previous := c.entries[content.key]; previous != nil {
		c.lru.MoveToFront(previous)
		return
	}
	for c.lru.Len() >= maxCachedManifests || len(content.raw) > maxCachedManifestBytes-c.bytes {
		oldest := c.lru.Back()
		if oldest == nil {
			return
		}
		retired := oldest.Value.(verifiedManifest)
		delete(c.entries, retired.key)
		c.bytes -= len(retired.raw)
		c.lru.Remove(oldest)
	}
	c.entries[content.key] = c.lru.PushFront(content)
	c.bytes += len(content.raw)
}
