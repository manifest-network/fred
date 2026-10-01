package docker

import (
	"context"
	"errors"
	"fmt"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/errdefs"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

const imageCacheOwnerVolume = "fred-image-cache-owner-v1"
const imageCacheOwnerStorageLabel = "fred.image_cache_storage_id"
const imageCacheOwnerBackendLabel = "fred.image_cache_backend"
const imageCacheOwnerModeLabel = "fred.image_cache_mode"

type imageCacheOwnershipDaemon interface {
	VolumeInspect(context.Context, string) (volume.Volume, error)
	VolumeCreate(context.Context, volume.CreateOptions) (volume.Volume, error)
}

// imageCacheOwnership binds daemon-wide image deletion to one durable storage
// lineage. Docker's volume name provides the shared namespace and its immutable
// labels survive process shutdown, unlike an advisory lifetime lock. A second
// backend cannot collect images while the first one's retained pins are offline.
type imageCacheOwnership struct {
	daemon  exclusiveImageCacheDaemon
	backend string
	storage backendidentity.ID
}

type exclusiveImageCacheDaemon interface {
	imageCacheOwnershipDaemon
	ImageRemove(context.Context, string, image.RemoveOptions) ([]image.DeleteResponse, error)
}

type imageCacheParticipation interface {
	verify(context.Context) error
}

// sharedImageCache reserves a daemon permanently for non-collecting clients.
// Its persistent marker prevents a later exclusive owner from silently ignoring
// the durable retained pins of a stopped development backend.
type sharedImageCache struct{ daemon imageCacheOwnershipDaemon }

func claimSharedImageCache(ctx context.Context, daemon imageCacheOwnershipDaemon) (*sharedImageCache, error) {
	marker, err := imageCacheMarker(ctx, daemon, map[string]string{imageCacheOwnerModeLabel: "shared"})
	if err != nil {
		return nil, err
	}
	shared := &sharedImageCache{daemon: daemon}
	if err := shared.verifyMarker(marker); err != nil {
		return nil, err
	}
	return shared, nil
}

func (s *sharedImageCache) verifyMarker(marker volume.Volume) error {
	if marker.Name != imageCacheOwnerVolume || marker.Driver != "local" ||
		marker.Labels[imageCacheOwnerModeLabel] != "shared" ||
		marker.Labels[imageCacheOwnerStorageLabel] != "" || marker.Labels[imageCacheOwnerBackendLabel] != "" {
		return errors.New("docker image store has exclusive ownership; development backends require a shared daemon")
	}
	return nil
}

func (s *sharedImageCache) verify(ctx context.Context) error {
	marker, err := s.daemon.VolumeInspect(ctx, imageCacheOwnerVolume)
	if err != nil {
		return fmt.Errorf("shared image cache participation unavailable: %w", err)
	}
	return s.verifyMarker(marker)
}

func claimImageCacheOwnership(ctx context.Context, daemon exclusiveImageCacheDaemon, storage backendidentity.VerifiedStorage) (*imageCacheOwnership, error) {
	if !storage.Valid() {
		return nil, errors.New("image cache ownership requires verified backend storage")
	}
	owner := &imageCacheOwnership{daemon: daemon, backend: storage.BackendName(), storage: storage.ID()}
	marker, err := imageCacheMarker(ctx, daemon, map[string]string{
		imageCacheOwnerModeLabel: "exclusive", imageCacheOwnerStorageLabel: storage.ID().String(), imageCacheOwnerBackendLabel: storage.BackendName(),
	})
	if err != nil {
		return nil, err
	}
	if err := owner.verifyMarker(marker); err != nil {
		return nil, err
	}
	return owner, nil
}

func imageCacheMarker(ctx context.Context, daemon imageCacheOwnershipDaemon, labels map[string]string) (volume.Volume, error) {
	marker, err := daemon.VolumeInspect(ctx, imageCacheOwnerVolume)
	if errdefs.IsNotFound(err) {
		// VolumeCreate returns the existing named volume if a concurrent
		// constructor won. Always verify the returned labels before accepting it.
		marker, err = daemon.VolumeCreate(ctx, volume.CreateOptions{
			Name:   imageCacheOwnerVolume,
			Driver: "local",
			Labels: labels,
		})
	}
	if err != nil {
		return volume.Volume{}, fmt.Errorf("claim durable image cache participation: %w", err)
	}
	return marker, nil
}

func (o *imageCacheOwnership) verifyMarker(marker volume.Volume) error {
	if marker.Name != imageCacheOwnerVolume || marker.Driver != "local" ||
		marker.Labels[imageCacheOwnerModeLabel] != "exclusive" ||
		marker.Labels[imageCacheOwnerStorageLabel] != o.storage.String() ||
		marker.Labels[imageCacheOwnerBackendLabel] != o.backend {
		return errors.New("docker image store is shared or belongs to another backend storage lineage; use a dedicated daemon")
	}
	return nil
}

func (o *imageCacheOwnership) verify(ctx context.Context) error {
	if o == nil {
		return errors.New("daemon-wide image collection requires durable exclusive ownership")
	}
	marker, err := o.daemon.VolumeInspect(ctx, imageCacheOwnerVolume)
	if err != nil {
		return fmt.Errorf("image cache ownership unavailable: %w", err)
	}
	return o.verifyMarker(marker)
}

// remove is the only image deletion sink. Shared participants possess no such
// capability. It consumes a complete journal inventory, targets immutable IDs,
// rechecks durable exclusivity immediately before each request and leaves the
// daemon's own live/stopped-container conflict checks enabled.
func (o *imageCacheOwnership) remove(ctx context.Context, inventory shared.ImagePinInventory, id string) error {
	if !isImmutableImageID(id) || !inventory.CanRemove(id) {
		return errors.New("image removal requires an unpinned immutable image identity")
	}
	if err := o.verify(ctx); err != nil {
		return err
	}
	_, err := o.daemon.ImageRemove(ctx, id, image.RemoveOptions{Force: false, PruneChildren: false})
	return err
}
