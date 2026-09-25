package shared

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared/imagebudget"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

var imagePinsBucketName = []byte("docker_image_pins_v1")

const maxImagePins = 100_000

// ImagePin preserves execution content independently of mutable registry tags.
// Pins share the callback journal's storage identity and commit failure gate.
// A lease/manifest pair always selects the first successfully admitted content.
type ImagePin struct {
	LeaseUUID    string           `json:"lease_uuid"`
	ManifestHash string           `json:"manifest_hash"`
	Reference    string           `json:"reference"`
	ImageID      string           `json:"image_id"`
	PullDigest   string           `json:"pull_digest,omitempty"`
	Platform     ocispec.Platform `json:"platform"`
	// ImportBytes records the verified import allowance for this immutable
	// content. Legacy zero is readable but grants no deferred-unpack budget.
	ImportBytes int64 `json:"import_bytes,omitempty"`
	// VerificationBytes bounds compressed staging and decoded verification.
	// Its absence marks legacy evidence; ImportBytes cannot supply this bound.
	VerificationBytes int64 `json:"verification_bytes,omitempty"`
}

// Budget decodes both durable dimensions together. The pin journal verifies the
// containing immutable identity before returning the record to its owner.
func (pin ImagePin) Budget() (imagebudget.Budget, error) {
	return imagebudget.Decode(imagebudget.Stored{VerificationBytes: pin.VerificationBytes, ImportBytes: pin.ImportBytes})
}

// ImagePinJournal is a bounded cache of immutable execution identities. The
// Started subject, rather than caller-selected lease metadata, authorizes pins.
type ImagePinJournal struct {
	journalPair
	inspections *ImageInspectionJournal
	retentions  *RetentionStore
	mu          *sync.Mutex
	accounting  *imagePinAccounting
}

func NewImagePinJournal(store *CallbackStore, releases *ReleaseStore, retentions *RetentionStore) (*ImagePinJournal, error) {
	pair, err := newJournalPair(store, releases)
	if err != nil {
		return nil, err
	}
	if !retentionStoreIsOpen(retentions) || retentions.backendAuthorityGate != store.backendAuthorityGate ||
		retentions.binding.backendName != store.binding.backendName || retentions.binding.storageID != store.binding.storageID {
		return nil, errors.New("image pins require the exact identity-bound retention journal")
	}
	store.imagePins.mu.Lock()
	defer store.imagePins.mu.Unlock()
	if existing := store.imagePins.journal; existing != nil {
		if existing.releases != releases || existing.retentions != retentions {
			return nil, errors.New("image pins already bind different release or retention journals")
		}
		return existing, nil
	}
	// Pin verification needs inspection-origin authority, but constructing it
	// must not create either optional image extension during read-only startup.
	inspections := &ImageInspectionJournal{store: store}
	if err := store.view(func(tx *bolt.Tx) error {
		if err := requireCompleteCallbackSchema(tx); err != nil {
			return err
		}
		return inspections.validateTx(tx)
	}); err != nil {
		return nil, err
	}
	j := &ImagePinJournal{journalPair: pair, inspections: inspections, retentions: retentions, mu: &store.imagePins.mu}
	// Reading or constructing the extension must not upgrade the journal. The
	// downgrade boundary is its first admitted pin, never a failed startup.
	j.accounting, err = j.loadAccounting()
	if err != nil {
		return nil, err
	}
	store.imagePins.journal = j
	return j, nil
}

func imagePinManifestHash(payload []byte) (string, error) {
	stack, err := manifest.ParseStoredPayload(payload)
	if err != nil {
		return "", err
	}
	canonical, err := json.Marshal(stack)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(canonical)
	return hex.EncodeToString(hash[:]), nil
}

func imagePinKey(lease, hash, ref string) []byte {
	sum := sha256.Sum256([]byte(lease + "\x00" + hash + "\x00" + ref))
	return sum[:]
}

func (j *ImagePinJournal) Lookup(lease string, payload []byte, ref string) (*ImagePin, error) {
	hash, err := imagePinManifestHash(payload)
	if err != nil {
		return nil, err
	}
	var pin *ImagePin
	err = j.inspections.store.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(imagePinsBucketName)
		if bucket == nil {
			return nil
		}
		data := bucket.Get(imagePinKey(lease, hash, ref))
		if data == nil {
			return nil
		}
		value, err := decodeImagePin(data)
		pin = &value
		return err
	})
	return pin, err
}

func imagePinOriginManifest(origin ImageInspectionOrigin) (string, []byte, error) {
	var lease string
	var release Release
	var ok bool
	switch {
	case origin.operation.Valid():
		lease = origin.operation.LeaseUUID()
		release, ok = origin.operation.ExpectedRelease()
	case origin.maintenance.Valid():
		lease = origin.maintenance.LeaseUUID()
		release, ok = origin.maintenance.TargetRelease()
	case origin.compensation.Valid():
		lease = origin.compensation.Intent().LeaseUUID()
		release, ok = origin.compensation.SourceRelease()
	}
	if !ok {
		return "", nil, errors.New("image pin requires a live Started release")
	}
	return lease, release.Manifest, nil
}

// Pin writes only the exact Started manifest's reference; repeated admission
// cannot silently replace a previously pinned image after its tag moves. A
// verified import allowance may increase for the same image and platform, but
// can never shrink or authorize replacing the original execution identity.
// That verification may also fill a missing legacy recovery digest; an existing
// digest is never replaced.
func (j *ImagePinJournal) Pin(origin ImageInspectionOrigin, ref, id, pullDigest string, platform ocispec.Platform, budget imagebudget.Budget) error {
	return j.withLockedPins(func(pins *lockedImagePins) error {
		return pins.pin(origin, ref, id, pullDigest, platform, budget)
	})
}

func (pins *lockedImagePins) pin(origin ImageInspectionOrigin, ref, id, pullDigest string, platform ocispec.Platform, budget imagebudget.Budget) error {
	j := pins.journal
	prepared, err := j.inspections.Prepare(origin, id, ref)
	if err != nil {
		return err
	}
	lease, payload, err := imagePinOriginManifest(origin)
	if err != nil {
		return err
	}
	stack, err := manifest.ParseStoredPayload(payload)
	if err != nil {
		return err
	}
	found := false
	for _, service := range stack.Services {
		found = found || service.Image == ref
	}
	if !found {
		return errors.New("image pin reference is absent from Started manifest")
	}
	hash, err := imagePinManifestHash(payload)
	if err != nil {
		return err
	}
	pin := ImagePin{LeaseUUID: lease, ManifestHash: hash, Reference: ref, ImageID: id, PullDigest: pullDigest, Platform: platform, ImportBytes: budget.Allocation().Bytes(), VerificationBytes: budget.Verification().Bytes()}
	unlock := j.lockLease(lease)
	defer unlock()
	return pins.updatePins(func(writer *imagePinTransaction) error {
		authority, err := writer.forOrigin(prepared)
		if err != nil {
			return err
		}
		return writer.put(authority, pin)
	})
}

func decodeImagePin(data []byte) (ImagePin, error) {
	var pin ImagePin
	if len(data) > 16<<10 {
		return pin, errors.New("image pin exceeds metadata budget")
	}
	if err := decodeStrictAuthoritativeObject(data, 16<<10, &pin); err != nil {
		return pin, err
	}
	if _, err := pin.Budget(); err != nil {
		return pin, err
	}
	id, err := digest.Parse(pin.ImageID)
	if err != nil || id.Algorithm() != digest.SHA256 || !canonicalInspectionUUID(pin.LeaseUUID) || len(pin.ManifestHash) != 64 ||
		pin.Platform.OS == "" || pin.Platform.Architecture == "" {
		return pin, errors.New("invalid immutable image pin")
	}
	if _, err := hex.DecodeString(pin.ManifestHash); err != nil {
		return pin, errors.New("invalid image pin manifest hash")
	}
	if _, err := reference.ParseAnyReference(pin.Reference); err != nil {
		return pin, err
	}
	if pin.PullDigest != "" {
		parsed, err := reference.ParseAnyReference(pin.PullDigest)
		if err != nil {
			return pin, err
		}
		if _, ok := parsed.(reference.Canonical); !ok || !strings.Contains(pin.PullDigest, "@sha256:") {
			return pin, errors.New("image pin recovery requires a repository digest")
		}
	}
	return pin, nil
}

func (j *ImagePinJournal) List() ([]ImagePin, error) {
	var pins []ImagePin
	err := j.inspections.store.view(func(tx *bolt.Tx) error {
		return visitImagePinsContextTx(context.Background(), tx, func(pin ImagePin) error {
			pins = append(pins, pin)
			return nil
		})
	})
	return pins, err
}

func visitImagePinsContextTx(ctx context.Context, tx *bolt.Tx, visit func(ImagePin) error) error {
	bucket := tx.Bucket(imagePinsBucketName)
	if bucket == nil {
		return nil // Optional extension for installations predating image GC.
	}
	count := 0
	return walkCallbackValidationRows(ctx, bucket, func(key, data []byte) error {
		count++
		if count > maxImagePins {
			return errors.New("image pin journal exceeds capacity")
		}
		pin, err := decodeImagePin(data)
		if err != nil {
			return fmt.Errorf("decode image pin: %w", err)
		}
		if string(key) != string(imagePinKey(pin.LeaseUUID, pin.ManifestHash, pin.Reference)) {
			return errors.New("image pin key differs from content")
		}
		if visit != nil {
			return visit(pin)
		}
		return nil
	})
}

// ImagePinInventory is the collector's complete immutable-content inventory.
// Only Collect can mint a value that permits unused image removal.
type ImagePinInventory struct {
	images   map[string]bool
	complete bool
	unpinned imagePinGenerationGaps
}

type imagePinGenerationSource uint8

const (
	imagePinInFlight imagePinGenerationSource = iota
	imagePinActive
	imagePinRetained
)

type imagePinGeneration struct {
	manifestHash string
	source       imagePinGenerationSource
}

type imagePinGenerationGaps struct{ active, retained int }

func (g *imagePinGenerationGaps) add(source imagePinGenerationSource) {
	switch source {
	case imagePinActive:
		g.active++
	case imagePinRetained:
		g.retained++
	}
}

// Complete reports whether every relaunchable generation has immutable pins.
// Incompleteness inhibits deletion without rejecting unrelated admission.
func (i ImagePinInventory) Complete() bool { return i.complete }

// UnpinnedActiveGenerations counts active or required compensation generations
// with at least one missing immutable pin, including unavailable ancestry.
// Pending targets have not completed image admission and are not counted.
func (i ImagePinInventory) UnpinnedActiveGenerations() int { return i.unpinned.active }

// UnpinnedRetainedGenerations counts restorable manifests with at least one
// missing immutable pin. A legacy retention without a manifest counts once.
func (i ImagePinInventory) UnpinnedRetainedGenerations() int { return i.unpinned.retained }

// CanRemove proves that the complete journal snapshot did not name this image.
// A zero value or legacy manifest with missing pins never grants collection.
func (i ImagePinInventory) CanRemove(imageID string) bool {
	return i.complete && !i.images[imageID]
}

// Collect derives pin liveness from its construction-bound journals. Callers
// cannot supply a lease inventory or authorize deletion with a detached DTO.
// The same per-lease lock used by settlement spans all three store reads and
// pruning, preventing a release-to-retention or intent-to-release handoff gap.
// Exact manifest references, rather than mere lease existence, keep pins live.
func (j *ImagePinJournal) Collect(ctx context.Context) (ImagePinInventory, error) {
	var inventory ImagePinInventory
	err := j.withLockedPins(func(pins *lockedImagePins) error {
		var err error
		inventory, err = pins.collect(ctx)
		return err
	})
	return inventory, err
}

func (locked *lockedImagePins) collect(ctx context.Context) (ImagePinInventory, error) {
	j := locked.journal
	pins, err := j.List()
	if err != nil {
		return ImagePinInventory{}, err
	}
	byLease := make(map[string][]ImagePin)
	for _, pin := range pins {
		byLease[pin.LeaseUUID] = append(byLease[pin.LeaseUUID], pin)
	}
	leases, err := j.releases.LeaseUUIDs()
	if err != nil {
		return ImagePinInventory{}, err
	}
	for _, lease := range leases {
		if _, exists := byLease[lease]; !exists {
			byLease[lease] = nil
		}
	}
	retained, err := j.retentions.Keys()
	if err != nil {
		return ImagePinInventory{}, err
	}
	for _, lease := range retained {
		if _, exists := byLease[lease]; !exists {
			byLease[lease] = nil
		}
	}
	protected := make(map[string]bool)
	complete := true
	var unpinned imagePinGenerationGaps
	for _, lease := range slices.Sorted(maps.Keys(byLease)) {
		leaseComplete, err := func() (bool, error) {
			unlock, err := j.lockLeaseContext(ctx, lease)
			if err != nil {
				return false, err
			}
			defer unlock()
			return locked.collectLeasePins(lease, byLease[lease], protected, &unpinned)
		}()
		if err != nil {
			return ImagePinInventory{}, err
		}
		complete = complete && leaseComplete
	}
	return ImagePinInventory{images: protected, complete: complete, unpinned: unpinned}, nil
}

func (locked *lockedImagePins) collectLeasePins(lease string, pins []ImagePin, protected map[string]bool, unpinned *imagePinGenerationGaps) (bool, error) {
	j := locked.journal
	needed := make(map[string]struct{})
	required := make(map[imagePinGeneration][]string)
	complete := true
	var compensationVersion int
	unknownGeneration := false
	add := func(payload []byte, source imagePinGenerationSource) error {
		if len(payload) == 0 {
			if source != imagePinInFlight {
				unknownGeneration = true
				unpinned.add(source)
			}
			return nil
		}
		stack, err := manifest.ParseStoredPayload(payload)
		if err != nil {
			return err
		}
		hash, err := imagePinManifestHash(payload)
		if err != nil {
			return err
		}
		var keys []string
		for _, service := range stack.Services {
			key := string(imagePinKey(lease, hash, service.Image))
			needed[key] = struct{}{}
			keys = append(keys, key)
		}
		if source != imagePinInFlight {
			required[imagePinGeneration{manifestHash: hash, source: source}] = keys
		}
		return nil
	}
	if err := j.callbacks.view(func(tx *bolt.Tx) error {
		head, _, err := getLeaseMutationHeadTx(tx, lease)
		if err != nil {
			return err
		}
		switch head := head.(type) {
		case operationLeaseMutationHead:
			if head.claim.entry.State == operationIntentPending {
				return add(head.claim.Manifest(), imagePinInFlight)
			}
			return nil
		case maintenanceLeaseMutationHead:
			compensationVersion = head.claim.SourceRelease().Version()
			return add(head.claim.TargetRelease().Manifest, imagePinInFlight)
		case closeLeaseMutationHead:
			return add(head.claim.Manifest(), imagePinInFlight)
		}
		return nil
	}); err != nil {
		return false, err
	}
	releases, err := j.releases.List(lease)
	if err != nil {
		return false, err
	}
	compensationFound := compensationVersion == 0
	for _, release := range releases {
		compensationFound = compensationFound || release.Version == compensationVersion
		if release.Status != "active" && release.Version != compensationVersion {
			continue
		}
		if err := add(release.Manifest, imagePinActive); err != nil {
			return false, err
		}
	}
	retained, err := j.retentions.Get(lease)
	if err != nil && !errors.Is(err, ErrNoRetention) {
		return false, err
	}
	if retained != nil && retained.StackManifest == nil {
		// Legacy retention can predate recorded manifests. Preserve its images
		// conservatively; this absence cannot authorize deletion or pin pruning.
		unknownGeneration = true
		unpinned.add(imagePinRetained)
	} else if retained != nil {
		payload, err := json.Marshal(retained.StackManifest)
		if err != nil {
			return false, err
		}
		if err := add(payload, imagePinRetained); err != nil {
			return false, err
		}
	}
	if unknownGeneration || !compensationFound {
		// Missing historical manifests/source generations cannot authorize pin
		// pruning even though collection itself is already inhibited.
		complete = false
		if !compensationFound {
			unpinned.add(imagePinActive)
		}
		for _, pin := range pins {
			protected[pin.ImageID] = true
			needed[string(imagePinKey(pin.LeaseUUID, pin.ManifestHash, pin.Reference))] = struct{}{}
		}
	}
	present := make(map[string]bool, len(pins))
	for _, pin := range pins {
		present[string(imagePinKey(pin.LeaseUUID, pin.ManifestHash, pin.Reference))] = true
	}
	for generation, keys := range required {
		for _, key := range keys {
			if !present[key] {
				complete = false
				unpinned.add(generation.source)
				break
			}
		}
	}
	var obsolete [][]byte
	for _, pin := range pins {
		key := imagePinKey(pin.LeaseUUID, pin.ManifestHash, pin.Reference)
		if _, keep := needed[string(key)]; keep {
			protected[pin.ImageID] = true
		} else {
			obsolete = append(obsolete, key)
		}
	}
	if len(obsolete) == 0 {
		return complete, nil
	}
	err = locked.updatePins(func(writer *imagePinTransaction) error {
		for _, key := range obsolete {
			if err := writer.remove(key); err != nil {
				return err
			}
		}
		return nil
	})
	return complete, err
}
