package imagefetch

import (
	"container/list"
	"context"
	"sync"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	maxCachedConfigBytes = 32 << 20
	maxCachedConfigs     = 128
)

// verifiedConfig is minted only after the descriptor's exact bytes and SHA256
// have been verified. Its bytes never leave this package or become mutable.
// It proves content identity, not that an arbitrary manifest can run it.
type verifiedConfig struct {
	digest digest.Digest
	raw    []byte
}

// configCache owns a bounded LRU of positive content evidence. Loader copies
// and recovery issuers share it. Cache eviction revokes no active Resolution;
// its immutable evidence remains independently owned by that caller.
type configCache struct {
	mu      sync.Mutex
	entries map[digest.Digest]*list.Element
	lru     list.List
	bytes   int
}

func (c *configCache) get(id digest.Digest) (verifiedConfig, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[id]
	if entry == nil {
		return verifiedConfig{}, false
	}
	content := entry.Value.(verifiedConfig)
	c.lru.MoveToFront(entry)
	return content, true
}

func (c *configCache) retain(content verifiedConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[digest.Digest]*list.Element)
	}
	if previous := c.entries[content.digest]; previous != nil {
		c.lru.MoveToFront(previous)
		return
	}
	for c.lru.Len() >= maxCachedConfigs || len(content.raw) > maxCachedConfigBytes-c.bytes {
		old := c.lru.Back()
		if old == nil {
			return
		}
		retired := old.Value.(verifiedConfig)
		delete(c.entries, retired.digest)
		c.bytes -= len(retired.raw)
		c.lru.Remove(old)
	}
	c.entries[content.digest] = c.lru.PushFront(content)
	c.bytes += len(content.raw)
}

func (l *Loader) resolveConfig(ctx context.Context, ref name.Reference, descriptor ocispec.Descriptor) (verifiedConfig, error) {
	if content, ok := l.configs.get(descriptor.Digest); ok {
		return content, nil
	}
	raw, err := l.fetchMemory(ctx, ref, descriptor)
	if err != nil {
		return verifiedConfig{}, err
	}
	return verifiedConfig{digest: descriptor.Digest, raw: raw}, nil
}
