package docker

import (
	"slices"
	"sync"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

// writablePathDetection keeps the admitted image and resolved runtime UID
// together through detection and caching. UID zero selects every non-root
// owner; it is a distinct query from any specific runtime UID.
type writablePathDetection struct {
	image imageexec.Image
	uid   int
}

func newWritablePathDetection(image imageexec.Image, uid int) writablePathDetection {
	return writablePathDetection{image: image, uid: uid}
}

type writablePathDetectionKey struct {
	imageID string
	uid     int
}

func (d writablePathDetection) key() writablePathDetectionKey {
	return writablePathDetectionKey{imageID: d.image.ID(), uid: d.uid}
}

// writablePathCache accepts complete detection subjects, never independently
// supplied cache keys. The map is typed and zero-value ready. Private copies
// prevent a setup's path filtering or a detector's reused buffer from changing
// another setup's cached result.
type writablePathCache struct {
	mu    sync.RWMutex
	paths map[writablePathDetectionKey][]string
}

func (c *writablePathCache) load(detection writablePathDetection) ([]string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	paths, found := c.paths[detection.key()]
	return slices.Clone(paths), found
}

func (c *writablePathCache) store(detection writablePathDetection, paths []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.paths == nil {
		c.paths = make(map[writablePathDetectionKey][]string)
	}
	c.paths[detection.key()] = slices.Clone(paths)
}
