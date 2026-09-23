package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// ImagePinBackfillSubject is the exact active generation held by the backfiller's
// lease transition lock. Detached Release values cannot authorize pin writes.
type ImagePinBackfillSubject struct {
	lease   string
	release Release
}

func (s ImagePinBackfillSubject) LeaseUUID() string { return s.lease }
func (s ImagePinBackfillSubject) Release() Release  { return cloneRelease(s.release) }

// ImagePinBackfillObservation reports positive local content evidence. It is
// not write authority: only the constructor-bound observer is invoked while the
// backfiller owns the exact journal and lease generation.
type ImagePinBackfillObservation struct {
	Reference  string
	ImageID    string
	PullDigest string
	Platform   ocispec.Platform
}

type ImagePinBackfillObserver interface {
	ObserveImagePins(context.Context, ImagePinBackfillSubject) ([]ImagePinBackfillObservation, error)
}

// ImagePinBackfillReport distinguishes unavailable historical evidence from a
// journal failure. Incomplete evidence keeps cache collection conservative.
type ImagePinBackfillReport struct {
	PinsAdded        int
	UnresolvedLeases int
}

// unavailableImagePinEvidence is a local observation refusal, distinct from a
// journal failure that must abort the sweep.
type unavailableImagePinEvidence struct{ error }

// ImagePinBackfiller may enrich existing generations from positive physical
// evidence. It cannot accept a caller-selected generation or replacement pin.
type ImagePinBackfiller struct {
	journal  *ImagePinJournal
	observer ImagePinBackfillObserver
}

func NewImagePinBackfiller(journal *ImagePinJournal, observer ImagePinBackfillObserver) (*ImagePinBackfiller, error) {
	if journal == nil || !journal.valid() || !retentionStoreIsOpen(journal.retentions) || observer == nil {
		return nil, errors.New("image pin backfill requires an open bound journal and observer")
	}
	return &ImagePinBackfiller{journal: journal, observer: observer}, nil
}

func (b *ImagePinBackfiller) Sweep(ctx context.Context) (ImagePinBackfillReport, error) {
	var report ImagePinBackfillReport
	if ctx == nil || b == nil || b.journal == nil || !b.journal.valid() || !retentionStoreIsOpen(b.journal.retentions) {
		return report, errors.New("image pin backfill requires live ownership")
	}
	j := b.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	leases, err := j.releases.LeaseUUIDs()
	if err != nil {
		return report, err
	}
	for _, lease := range leases {
		unlock, err := j.lockLeaseContext(ctx, lease)
		if err != nil {
			return report, err
		}
		added, unresolved, err := b.sweepLease(ctx, lease)
		unlock()
		if err != nil {
			var unavailable *unavailableImagePinEvidence
			if errors.As(err, &unavailable) {
				report.UnresolvedLeases++
				continue
			}
			return report, err
		}
		report.PinsAdded += added
		if unresolved {
			report.UnresolvedLeases++
		}
	}
	return report, ctx.Err()
}

// A terminal operation head carries history, not an outstanding substrate
// effect. Other present variants cannot attest a stable active generation.
func imagePinBackfillSettled(tx *bolt.Tx, lease string) (bool, error) {
	head, present, err := getLeaseMutationHeadTx(tx, lease)
	if err != nil || !present {
		return !present, err
	}
	if operation, ok := head.(operationLeaseMutationHead); ok {
		return operation.claim.entry.State != operationIntentPending, nil
	}
	return false, nil
}

func (b *ImagePinBackfiller) sweepLease(ctx context.Context, lease string) (int, bool, error) {
	j := b.journal
	release, err := j.releases.LatestActive(lease)
	if err != nil || release == nil {
		return 0, false, err
	}
	var settled bool
	err = j.callbacks.view(func(tx *bolt.Tx) error {
		var err error
		settled, err = imagePinBackfillSettled(tx, lease)
		return err
	})
	if err != nil || !settled {
		return 0, !settled, err
	}
	stack, err := manifest.ParseStoredPayload(release.Manifest)
	if err != nil {
		return 0, false, err
	}
	hash, err := imagePinManifestHash(release.Manifest)
	if err != nil {
		return 0, false, err
	}
	missing := make(map[string]bool)
	for _, service := range stack.Services {
		pin, err := j.Lookup(lease, release.Manifest, service.Image)
		if err != nil {
			return 0, false, err
		}
		if pin == nil {
			missing[service.Image] = true
		}
	}
	if len(missing) == 0 {
		return 0, false, nil
	}
	observations, err := b.observer.ObserveImagePins(ctx, ImagePinBackfillSubject{lease: lease, release: cloneRelease(*release)})
	if ctx.Err() != nil {
		return 0, false, ctx.Err()
	}
	if err != nil {
		return 0, false, &unavailableImagePinEvidence{err}
	}
	pins := make(map[string]ImagePin)
	for _, observation := range observations {
		if !missing[observation.Reference] {
			continue
		}
		pin := ImagePin{LeaseUUID: lease, ManifestHash: hash, Reference: observation.Reference, ImageID: observation.ImageID, PullDigest: observation.PullDigest, Platform: observation.Platform}
		data, err := json.Marshal(pin)
		if err != nil {
			return 0, false, &unavailableImagePinEvidence{err}
		}
		// Decode the canonical bytes into owned storage. In particular, platform
		// features must not retain mutable slices supplied by the observer.
		pin, err = decodeImagePin(data)
		if err != nil {
			return 0, false, &unavailableImagePinEvidence{err}
		}
		if previous, found := pins[pin.Reference]; found && !reflect.DeepEqual(previous, pin) {
			return 0, true, nil
		}
		pins[pin.Reference] = pin
	}
	if len(pins) == 0 {
		return 0, true, nil
	}
	added := 0
	err = j.callbacks.update(func(tx *bolt.Tx) error {
		settled, err := imagePinBackfillSettled(tx, lease)
		if err != nil {
			return err
		}
		if !settled {
			return errors.New("image pin backfill generation is no longer settled")
		}
		bucket, err := tx.CreateBucketIfNotExists(imagePinsBucketName)
		if err != nil {
			return err
		}
		count := bucket.Stats().KeyN
		for _, pin := range pins {
			key := imagePinKey(lease, hash, pin.Reference)
			if bucket.Get(key) != nil {
				continue
			}
			if count+added >= maxImagePins {
				return fmt.Errorf("image pin journal capacity exceeded")
			}
			data, err := json.Marshal(pin)
			if err != nil {
				return err
			}
			if err := bucket.Put(key, data); err != nil {
				return err
			}
			added++
		}
		return nil
	})
	return added, len(pins) != len(missing), err
}
