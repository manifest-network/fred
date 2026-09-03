package backendidentity

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// BoundMarkerPair retains the exact physical parent directories used by a
// marker-pair initialization. It is acquired before substrate and journal
// evidence is read, then retained through publication, so renaming or
// replacing either pathname cannot redirect a marker write into replacement
// storage.
//
// Its zero value is invalid. Call Close after the complete proof-and-
// publication transaction.
type BoundMarkerPair struct {
	primary boundMarkerEntry
	anchor  boundMarkerEntry
}

type boundMarkerEntry struct {
	directory *fsidentity.Directory
	entry     fsidentity.Entry
}

// boundMarkerDurabilityBarrier represents a completed directory-entry state
// transition whose publication must still be flushed before callers may act
// on it. It has no replaceable implementation: sync always reaches the exact
// retained directory descriptor used by the transition.
type boundMarkerDurabilityBarrier struct {
	directory *fsidentity.Directory
}

func (barrier boundMarkerDurabilityBarrier) sync() error {
	if barrier.directory == nil {
		return errors.New("backend storage identity marker durability barrier is invalid")
	}
	return barrier.directory.Sync()
}

func (marker boundMarkerEntry) durabilityBarrier() boundMarkerDurabilityBarrier {
	return boundMarkerDurabilityBarrier{directory: marker.directory}
}

// BindMarkerPair acquires physical-parent capabilities for both marker paths.
// Parent symlinks are resolved before acquisition; every later file operation
// is relative to the retained descriptor and refuses a final-component
// symlink.
func BindMarkerPair(primaryPath, anchorPath string) (*BoundMarkerPair, error) {
	return bindMarkerPair(primaryPath, anchorPath)
}

func bindMarkerPair(primaryPath, anchorPath string) (*BoundMarkerPair, error) {
	if primaryPath == anchorPath || primaryPath == "" || anchorPath == "" ||
		filepath.Clean(primaryPath) != primaryPath || filepath.Clean(anchorPath) != anchorPath {
		return nil, fmt.Errorf("%w: marker pair paths must be clean and distinct", ErrInvalidMarkerBinding)
	}
	primary, err := bindMarkerEntry(primaryPath)
	if err != nil {
		return nil, fmt.Errorf("bind primary backend storage identity marker: %w", err)
	}
	anchor, err := bindMarkerEntry(anchorPath)
	if err != nil {
		_ = primary.directory.Close()
		return nil, fmt.Errorf("bind backend storage identity anchor: %w", err)
	}
	pair := &BoundMarkerPair{primary: primary, anchor: anchor}
	if err := pair.VerifyPaths(); err != nil {
		_ = pair.Close()
		return nil, err
	}
	return pair, nil
}

func bindMarkerEntry(path string) (boundMarkerEntry, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return boundMarkerEntry{}, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return boundMarkerEntry{}, err
	}
	directory, err := fsidentity.OpenDirectory(parent)
	if err != nil {
		return boundMarkerEntry{}, err
	}
	entry, err := directory.Entry(filepath.Base(absolute))
	if err != nil {
		_ = directory.Close()
		return boundMarkerEntry{}, err
	}
	return boundMarkerEntry{directory: directory, entry: entry}, nil
}

// VerifyPaths proves that both acquisition pathnames still name their retained
// physical parent directories.
func (pair *BoundMarkerPair) VerifyPaths() error {
	if !pair.valid() {
		return fmt.Errorf("%w: bound marker pair is invalid", ErrInvalidMarkerBinding)
	}
	if err := pair.primary.directory.VerifyPath(); err != nil {
		return fmt.Errorf("primary marker parent changed: %w", err)
	}
	if err := pair.anchor.directory.VerifyPath(); err != nil {
		return fmt.Errorf("anchor marker parent changed: %w", err)
	}
	return nil
}

// VerifyAbsent proves that neither exact marker entry exists while retaining
// and re-attesting both physical parent directories. It is the read-only gate
// used by a stopped-lineage adoption preflight: unlike initialization or
// committed-marker recovery, it never publishes, removes, or repairs a file.
func (pair *BoundMarkerPair) VerifyAbsent() error {
	if !pair.valid() {
		return fmt.Errorf("%w: bound marker pair is invalid", ErrInvalidMarkerBinding)
	}
	if err := pair.VerifyPaths(); err != nil {
		return err
	}
	markers := []struct {
		name   string
		marker boundMarkerEntry
	}{
		{name: "primary", marker: pair.primary},
		{name: "anchor", marker: pair.anchor},
	}
	for _, marker := range markers {
		if _, err := marker.marker.entry.Lstat(); err == nil {
			return fmt.Errorf(
				"%w: %s marker already exists; adoption preflight requires an unsealed v0.13 lineage",
				ErrMarkerBindingMismatch,
				marker.name,
			)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("inspect %s backend storage identity marker: %w", marker.name, err)
		}
	}
	return pair.VerifyPaths()
}

// Close revokes both retained physical-parent capabilities.
func (pair *BoundMarkerPair) Close() error {
	if pair == nil {
		return nil
	}
	var errs []error
	if pair.primary.directory != nil {
		errs = append(errs, pair.primary.directory.Close())
	}
	if pair.anchor.directory != nil {
		errs = append(errs, pair.anchor.directory.Close())
	}
	pair.primary = boundMarkerEntry{}
	pair.anchor = boundMarkerEntry{}
	return errors.Join(errs...)
}

func (pair *BoundMarkerPair) valid() bool {
	return pair != nil && pair.primary.directory != nil && pair.primary.entry.Valid() &&
		pair.anchor.directory != nil && pair.anchor.entry.Valid()
}

// VerifyCommittedWithStores recognizes and verifies only an already committed
// pair using retained physical-parent capabilities. A false result means the
// pair is fresh or resumable; it never publishes a marker or store.
func (pair *BoundMarkerPair) VerifyCommittedWithStores(
	backendName, substrateID string,
	verify func(VerifiedStorage) error,
) (ID, bool, error) {
	if !pair.valid() || verify == nil {
		return ID{}, false, fmt.Errorf("%w: bound marker pair and store verification are required", ErrInvalidMarkerBinding)
	}
	if err := pair.VerifyPaths(); err != nil {
		return ID{}, false, err
	}
	primaryID, primaryState, primaryErr := inspectBoundMarkerState(pair.primary, backendName, substrateID)
	anchorID, anchorState, anchorErr := inspectBoundMarkerState(pair.anchor, backendName, substrateID)
	if primaryErr != nil && !errors.Is(primaryErr, errMarkerAbsent) {
		return ID{}, false, primaryErr
	}
	if anchorErr != nil && !errors.Is(anchorErr, errMarkerAbsent) {
		return ID{}, false, anchorErr
	}
	primaryAbsent := errors.Is(primaryErr, errMarkerAbsent)
	anchorAbsent := errors.Is(anchorErr, errMarkerAbsent)
	switch {
	case primaryErr == nil && anchorErr == nil && primaryState == "" && anchorState == "":
		if primaryID != anchorID {
			return ID{}, false, fmt.Errorf("%w: marker pair identities differ (%s != %s)",
				ErrMarkerBindingMismatch, primaryID, anchorID)
		}
		if err := pair.recoverAndVerifyCommitted(
			backendName, substrateID, primaryID, verify,
		); err != nil {
			return ID{}, false, err
		}
		return primaryID, true, nil
	case primaryAbsent && anchorAbsent:
		return ID{}, false, nil
	case primaryAbsent && anchorErr == nil && anchorState == markerStatePending:
		return ID{}, false, nil
	case primaryErr == nil && primaryState == "" && anchorErr == nil &&
		anchorState == markerStatePending && primaryID == anchorID:
		return ID{}, false, nil
	case primaryAbsent || anchorAbsent:
		return ID{}, false, fmt.Errorf("%w: one member of the durable marker pair is missing",
			ErrMarkerBindingMismatch)
	default:
		return ID{}, false, fmt.Errorf("%w: invalid marker pair states primary=%q anchor=%q",
			ErrMarkerBindingMismatch, primaryState, anchorState)
	}
}

// PendingInitializationProfile reports a resumable store-aware profile using
// the retained anchor parent. A false result means the anchor is absent or
// committed.
func (pair *BoundMarkerPair) PendingInitializationProfile(
	backendName, substrateID string,
) (InitializationProfile, bool, error) {
	if !pair.valid() {
		return "", false, ErrInvalidMarkerBinding
	}
	_, state, err := inspectBoundMarkerState(pair.anchor, backendName, substrateID)
	if errors.Is(err, errMarkerAbsent) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if state != markerStatePending {
		return "", false, nil
	}
	profile, err := inspectBoundMarkerInitializationProfile(pair.anchor)
	if err != nil {
		return "", false, err
	}
	return profile, true, nil
}

// InitializeWithStores establishes the marker pair and authoritative stores
// using only retained parent-directory capabilities. The caller must acquire
// pair before reading any evidence used to authorize this operation. Success
// returns runtime authority only after both committed markers and the complete
// store set pass final read-only verification.
func (pair *BoundMarkerPair) InitializeWithStores(
	backendName, substrateID string,
	hooks MarkerPairStoreHooks,
) (VerifiedStorage, error) {
	if !pair.valid() || !hooks.Profile.valid() || hooks.Prepare == nil ||
		hooks.Check == nil || hooks.Verify == nil {
		return VerifiedStorage{}, fmt.Errorf("%w: bound marker pair and store hooks are required", ErrInvalidMarkerBinding)
	}
	if err := pair.VerifyPaths(); err != nil {
		return VerifiedStorage{}, err
	}
	primaryID, primaryState, primaryErr := inspectBoundMarkerState(pair.primary, backendName, substrateID)
	anchorID, anchorState, anchorErr := inspectBoundMarkerState(pair.anchor, backendName, substrateID)
	primaryAbsent := errors.Is(primaryErr, errMarkerAbsent)
	anchorAbsent := errors.Is(anchorErr, errMarkerAbsent)
	switch {
	case primaryErr == nil && anchorErr == nil && primaryState == "" && anchorState == "":
		if primaryID != anchorID {
			return VerifiedStorage{}, fmt.Errorf("%w: marker pair identities differ (%s != %s)",
				ErrMarkerBindingMismatch, primaryID, anchorID)
		}
		if err := pair.recoverAndVerifyCommitted(
			backendName, substrateID, primaryID, hooks.Verify,
		); err != nil {
			return VerifiedStorage{}, err
		}
		return VerifiedStorage{id: primaryID}, nil
	case primaryAbsent && anchorAbsent:
		id, err := New()
		if err != nil {
			return VerifiedStorage{}, err
		}
		pending, err := establishBoundPendingMarker(
			pair.anchor, backendName, substrateID, id, hooks.Profile,
		)
		if err != nil {
			return VerifiedStorage{}, err
		}
		return pair.completePendingInitialization(
			backendName, substrateID, pending, hooks,
		)
	case primaryAbsent && anchorErr == nil && anchorState == markerStatePending:
		pending, err := recoverBoundPendingStorage(pair.anchor, anchorID)
		if err != nil {
			return VerifiedStorage{}, err
		}
		profile, err := inspectBoundMarkerInitializationProfile(pair.anchor)
		if err != nil {
			return VerifiedStorage{}, err
		}
		if profile != hooks.Profile {
			return VerifiedStorage{}, fmt.Errorf("%w: pending initialization profile %q differs from requested %q",
				ErrMarkerBindingMismatch, profile, hooks.Profile)
		}
		return pair.completePendingInitialization(
			backendName, substrateID, pending, hooks,
		)
	case primaryErr == nil && primaryState == "" && anchorErr == nil &&
		anchorState == markerStatePending && primaryID == anchorID:
		if err := recoverBoundMarkerTemporaryLink(pair.primary); err != nil {
			return VerifiedStorage{}, err
		}
		if err := recoverBoundMarkerTemporaryLink(pair.anchor); err != nil {
			return VerifiedStorage{}, err
		}
		profile, err := inspectBoundMarkerInitializationProfile(pair.anchor)
		if err != nil {
			return VerifiedStorage{}, err
		}
		if profile != hooks.Profile {
			return VerifiedStorage{}, fmt.Errorf("%w: pending initialization profile %q differs from requested %q",
				ErrMarkerBindingMismatch, profile, hooks.Profile)
		}
		pending := newPendingStorage(primaryID)
		defer pending.revoke()
		if err := hooks.Prepare(pending, profile); err != nil {
			return VerifiedStorage{}, fmt.Errorf("prepare identity-bound stores: %w", err)
		}
		if err := hooks.Check(pending); err != nil {
			return VerifiedStorage{}, fmt.Errorf("check prepared identity-bound stores: %w", err)
		}
		// Store preparation is complete. Withdraw pending authority before any
		// committed-marker publication or committed-store verification begins.
		pending.revoke()
		if err := pair.VerifyPaths(); err != nil {
			return VerifiedStorage{}, err
		}
		if err := finalizeBoundPendingMarker(pair.anchor, backendName, substrateID, primaryID); err != nil {
			return VerifiedStorage{}, err
		}
		if err := pair.recoverAndVerifyCommitted(
			backendName, substrateID, primaryID, hooks.Verify,
		); err != nil {
			return VerifiedStorage{}, err
		}
		return VerifiedStorage{id: primaryID}, nil
	case primaryAbsent || anchorAbsent:
		return VerifiedStorage{}, fmt.Errorf("%w: one member of the durable marker pair is missing",
			ErrMarkerBindingMismatch)
	case primaryErr != nil:
		return VerifiedStorage{}, primaryErr
	case anchorErr != nil:
		return VerifiedStorage{}, anchorErr
	default:
		return VerifiedStorage{}, fmt.Errorf("%w: invalid marker pair states primary=%q anchor=%q",
			ErrMarkerBindingMismatch, primaryState, anchorState)
	}
}

func (pair *BoundMarkerPair) completePendingInitialization(
	backendName, substrateID string,
	pending PendingStorage,
	hooks MarkerPairStoreHooks,
) (VerifiedStorage, error) {
	if !pending.Valid() {
		return VerifiedStorage{}, fmt.Errorf("%w: pending storage authority is required", ErrInvalidMarkerBinding)
	}
	defer pending.revoke()
	id := pending.ID()
	if err := hooks.Prepare(pending, hooks.Profile); err != nil {
		return VerifiedStorage{}, fmt.Errorf("prepare identity-bound stores: %w", err)
	}
	if err := hooks.Check(pending); err != nil {
		return VerifiedStorage{}, fmt.Errorf("check prepared identity-bound stores: %w", err)
	}
	// Store preparation is complete. Withdraw pending authority before any
	// committed-marker publication or committed-store verification begins.
	pending.revoke()
	// This is the final barrier before the first primary-marker publication. A
	// parent replaced during evidence/store preparation remains entirely
	// unsealed at its configured pathname.
	if err := pair.VerifyPaths(); err != nil {
		return VerifiedStorage{}, err
	}
	if err := ensureBoundMarkerRecord(
		pair.primary, backendName, substrateID, id, "", "",
	); err != nil {
		return VerifiedStorage{}, err
	}
	if err := pair.VerifyPaths(); err != nil {
		return VerifiedStorage{}, err
	}
	if err := finalizeBoundPendingMarker(pair.anchor, backendName, substrateID, id); err != nil {
		return VerifiedStorage{}, err
	}
	if err := pair.recoverAndVerifyCommitted(backendName, substrateID, id, hooks.Verify); err != nil {
		return VerifiedStorage{}, err
	}
	return VerifiedStorage{id: id}, nil
}

func (pair *BoundMarkerPair) recoverAndVerifyCommitted(
	backendName, substrateID string,
	expected ID,
	verify func(VerifiedStorage) error,
) error {
	if err := recoverBoundMarkerTemporaryLink(pair.primary); err != nil {
		return err
	}
	if err := recoverBoundMarkerTemporaryLink(pair.anchor); err != nil {
		return err
	}
	if err := pair.verifyMarkerPair(backendName, substrateID, expected); err != nil {
		return fmt.Errorf("re-attest committed backend storage identity: %w", err)
	}
	if err := verify(VerifiedStorage{id: expected}); err != nil {
		return fmt.Errorf("verify identity-bound stores: %w", err)
	}
	if err := pair.verifyMarkerPair(backendName, substrateID, expected); err != nil {
		return fmt.Errorf("re-attest backend storage identity after store verification: %w", err)
	}
	return pair.VerifyPaths()
}

func (pair *BoundMarkerPair) verifyMarkerPair(
	backendName, substrateID string,
	expected ID,
) error {
	if !expected.Valid() {
		return ErrInvalidMarkerBinding
	}
	primaryID, primaryState, err := inspectBoundMarkerState(pair.primary, backendName, substrateID)
	if err != nil {
		return err
	}
	anchorID, anchorState, err := inspectBoundMarkerState(pair.anchor, backendName, substrateID)
	if err != nil {
		return err
	}
	if primaryState != "" || anchorState != "" || primaryID != expected || anchorID != expected {
		return fmt.Errorf("%w: marker pair is (%s,%s), expected %s",
			ErrMarkerBindingMismatch, primaryID, anchorID, expected)
	}
	return verifyDistinctBoundMarkerFiles(pair.primary, pair.anchor)
}

func inspectBoundMarkerState(
	marker boundMarkerEntry,
	backendName, substrateID string,
) (ID, string, error) {
	if !marker.entry.Valid() || !validMarkerBindingValue(backendName) ||
		!validMarkerBindingValue(substrateID) {
		return ID{}, "", ErrInvalidMarkerBinding
	}
	data, err := readBoundRegularFile(marker)
	if err != nil {
		return ID{}, "", err
	}
	record, err := decodeMarker(data)
	if err != nil {
		return ID{}, "", err
	}
	return validateMarkerRecord(record, backendName, substrateID)
}

func validateMarkerRecord(record markerRecord, backendName, substrateID string) (ID, string, error) {
	if record.Schema != markerSchemaVersion {
		return ID{}, "", fmt.Errorf("%w: unsupported schema %d", ErrInvalidMarker, record.Schema)
	}
	if !validMarkerBindingValue(record.BackendName) {
		return ID{}, "", fmt.Errorf("%w: backend_name is invalid", ErrInvalidMarker)
	}
	if !validMarkerBindingValue(record.SubstrateID) {
		return ID{}, "", fmt.Errorf("%w: substrate_id is invalid", ErrInvalidMarker)
	}
	if record.State != "" && record.State != markerStatePending {
		return ID{}, "", fmt.Errorf("%w: unsupported marker state %q", ErrInvalidMarker, record.State)
	}
	profile := InitializationProfile(record.InitializationProfile)
	if record.State == "" && profile != "" {
		return ID{}, "", fmt.Errorf("%w: committed marker retains initialization profile", ErrInvalidMarker)
	}
	if profile != "" && !profile.valid() {
		return ID{}, "", fmt.Errorf("%w: unsupported initialization profile %q", ErrInvalidMarker, profile)
	}
	id, err := Parse(record.StorageID)
	if err != nil {
		return ID{}, "", fmt.Errorf("%w: storage_id: %w", ErrInvalidMarker, err)
	}
	if record.BackendName != backendName {
		return ID{}, "", fmt.Errorf("%w: backend name is %q, expected %q",
			ErrMarkerBindingMismatch, record.BackendName, backendName)
	}
	if record.SubstrateID != substrateID {
		return ID{}, "", fmt.Errorf("%w: substrate ID is %q, expected %q",
			ErrMarkerBindingMismatch, record.SubstrateID, substrateID)
	}
	return id, record.State, nil
}

func inspectBoundMarkerInitializationProfile(marker boundMarkerEntry) (InitializationProfile, error) {
	data, err := readBoundRegularFile(marker)
	if err != nil {
		return "", err
	}
	record, err := decodeMarker(data)
	if err != nil {
		return "", err
	}
	profile := InitializationProfile(record.InitializationProfile)
	if record.State == markerStatePending && (profile == "" || profile.valid()) {
		return profile, nil
	}
	return "", fmt.Errorf("%w: pending marker has invalid initialization profile %q",
		ErrInvalidMarker, profile)
}

func readBoundRegularFile(marker boundMarkerEntry) ([]byte, error) {
	pathInfo, err := marker.entry.Lstat()
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errMarkerAbsent
		}
		return nil, err
	}
	if err := validateMarkerFileMode(marker.entry.Name(), pathInfo.Mode()); err != nil {
		return nil, err
	}
	file, err := marker.entry.OpenFile(os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: marker changed while opening: %w", ErrInvalidMarker, err)
	}
	openedInfo, err := file.Stat()
	if err != nil || validateMarkerFileMode(marker.entry.Name(), openedInfo.Mode()) != nil ||
		!os.SameFile(pathInfo, openedInfo) {
		_ = file.Close()
		if err != nil {
			return nil, fmt.Errorf("stat open backend storage identity marker: %w", err)
		}
		return nil, fmt.Errorf("%w: marker changed while opening", ErrInvalidMarker)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxMarkerBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read backend storage identity marker: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close backend storage identity marker: %w", closeErr)
	}
	if len(data) > maxMarkerBytes {
		return nil, fmt.Errorf("%w: marker exceeds %d bytes", ErrInvalidMarker, maxMarkerBytes)
	}
	currentInfo, err := marker.entry.Lstat()
	if err != nil || validateMarkerFileMode(marker.entry.Name(), currentInfo.Mode()) != nil ||
		!os.SameFile(openedInfo, currentInfo) {
		if err != nil {
			return nil, fmt.Errorf("%w: marker changed while reading: %w", ErrInvalidMarker, err)
		}
		return nil, fmt.Errorf("%w: marker changed while reading", ErrInvalidMarker)
	}
	return data, nil
}

func ensureBoundMarkerRecord(
	marker boundMarkerEntry,
	backendName, substrateID string,
	expected ID,
	state string,
	profile InitializationProfile,
) error {
	barrier, err := ensureBoundMarkerRecordTransition(
		marker, backendName, substrateID, expected, state, profile,
	)
	if err != nil {
		return err
	}
	return barrier.sync()
}

func ensureBoundMarkerRecordTransition(
	marker boundMarkerEntry,
	backendName, substrateID string,
	expected ID,
	state string,
	profile InitializationProfile,
) (boundMarkerDurabilityBarrier, error) {
	if !expected.Valid() {
		return boundMarkerDurabilityBarrier{}, ErrInvalidID
	}
	if existing, existingState, err := inspectBoundMarkerState(marker, backendName, substrateID); err == nil {
		existingProfile, profileErr := inspectBoundMarkerInitializationProfileForState(marker, existingState)
		if profileErr != nil {
			return boundMarkerDurabilityBarrier{}, profileErr
		}
		if existing != expected || existingState != state || existingProfile != profile {
			return boundMarkerDurabilityBarrier{}, fmt.Errorf(
				"%w: marker is %s, expected %s", ErrMarkerBindingMismatch, existing, expected,
			)
		}
		return marker.durabilityBarrier(), nil
	} else if !errors.Is(err, errMarkerAbsent) {
		return boundMarkerDurabilityBarrier{}, err
	}
	data, err := encodeMarkerRecord(markerRecord{
		Schema: markerSchemaVersion, StorageID: expected.String(), BackendName: backendName,
		SubstrateID: substrateID, State: state, InitializationProfile: string(profile),
	})
	if err != nil {
		return boundMarkerDurabilityBarrier{}, err
	}
	created, err := publishBoundMarkerTransition(marker, data)
	if err != nil {
		return boundMarkerDurabilityBarrier{}, err
	}
	if !created {
		existing, existingState, loadErr := inspectBoundMarkerState(marker, backendName, substrateID)
		if loadErr != nil {
			return boundMarkerDurabilityBarrier{}, loadErr
		}
		existingProfile, profileErr := inspectBoundMarkerInitializationProfileForState(marker, existingState)
		if profileErr != nil {
			return boundMarkerDurabilityBarrier{}, profileErr
		}
		if existing != expected || existingState != state || existingProfile != profile {
			return boundMarkerDurabilityBarrier{}, fmt.Errorf("%w: concurrent marker is %s, expected %s",
				ErrMarkerBindingMismatch, existing, expected)
		}
	}
	return marker.durabilityBarrier(), nil
}

func establishBoundPendingMarker(
	marker boundMarkerEntry,
	backendName, substrateID string,
	expected ID,
	profile InitializationProfile,
) (PendingStorage, error) {
	barrier, err := ensureBoundMarkerRecordTransition(
		marker, backendName, substrateID, expected, markerStatePending, profile,
	)
	if err != nil {
		return PendingStorage{}, err
	}
	return pendingStorageAfterDurabilityBarrier(expected, barrier)
}

func recoverBoundPendingStorage(marker boundMarkerEntry, expected ID) (PendingStorage, error) {
	barrier, err := recoverBoundMarkerTemporaryLinkTransition(marker)
	if err != nil {
		return PendingStorage{}, err
	}
	return pendingStorageAfterDurabilityBarrier(expected, barrier)
}

func pendingStorageAfterDurabilityBarrier(
	expected ID,
	barrier boundMarkerDurabilityBarrier,
) (PendingStorage, error) {
	if !expected.Valid() {
		return PendingStorage{}, ErrInvalidID
	}
	if err := barrier.sync(); err != nil {
		return PendingStorage{}, err
	}
	return newPendingStorage(expected), nil
}

func inspectBoundMarkerInitializationProfileForState(
	marker boundMarkerEntry,
	state string,
) (InitializationProfile, error) {
	if state == "" {
		return "", nil
	}
	return inspectBoundMarkerInitializationProfile(marker)
}

func finalizeBoundPendingMarker(
	marker boundMarkerEntry,
	backendName, substrateID string,
	expected ID,
) error {
	barrier, err := finalizeBoundPendingMarkerTransition(marker, backendName, substrateID, expected)
	if err != nil {
		return err
	}
	return barrier.sync()
}

func finalizeBoundPendingMarkerTransition(
	marker boundMarkerEntry,
	backendName, substrateID string,
	expected ID,
) (boundMarkerDurabilityBarrier, error) {
	id, state, err := inspectBoundMarkerState(marker, backendName, substrateID)
	if err != nil {
		return boundMarkerDurabilityBarrier{}, err
	}
	if id != expected {
		return boundMarkerDurabilityBarrier{}, fmt.Errorf(
			"%w: pending anchor is %s, expected %s", ErrMarkerBindingMismatch, id, expected,
		)
	}
	if state == "" {
		return marker.durabilityBarrier(), nil
	}
	if state != markerStatePending {
		return boundMarkerDurabilityBarrier{}, fmt.Errorf("%w: anchor is not pending", ErrInvalidMarker)
	}
	data, err := encodeMarkerRecord(markerRecord{
		Schema: markerSchemaVersion, StorageID: expected.String(),
		BackendName: backendName, SubstrateID: substrateID,
	})
	if err != nil {
		return boundMarkerDurabilityBarrier{}, err
	}
	if err := replaceBoundMarkerTransition(marker, data); err != nil {
		return boundMarkerDurabilityBarrier{}, err
	}
	return marker.durabilityBarrier(), nil
}

func verifyDistinctBoundMarkerFiles(primary, anchor boundMarkerEntry) error {
	primaryInfo, err := primary.entry.Lstat()
	if err != nil {
		return fmt.Errorf("stat primary backend storage identity marker: %w", err)
	}
	anchorInfo, err := anchor.entry.Lstat()
	if err != nil {
		return fmt.Errorf("stat backend storage identity anchor: %w", err)
	}
	if err := validateMarkerFileMode(primary.entry.Name(), primaryInfo.Mode()); err != nil {
		return err
	}
	if err := validateMarkerFileMode(anchor.entry.Name(), anchorInfo.Mode()); err != nil {
		return err
	}
	if os.SameFile(primaryInfo, anchorInfo) {
		return fmt.Errorf("%w: marker pair paths refer to the same file", ErrInvalidMarker)
	}
	for path, info := range map[string]fs.FileInfo{
		primary.entry.DisplayPath(): primaryInfo,
		anchor.entry.DisplayPath():  anchorInfo,
	} {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return fmt.Errorf("%w: marker %q must have exactly one hard link", ErrInvalidMarker, path)
		}
	}
	return nil
}

func publishBoundMarkerTransition(marker boundMarkerEntry, data []byte) (bool, error) {
	temporary, temporaryName, err := createBoundMarkerTemporary(marker)
	if err != nil {
		return false, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = temporary.Close()
		}
		_ = marker.directory.Remove(temporaryName)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return false, fmt.Errorf("set backend storage identity marker permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return false, fmt.Errorf("write backend storage identity marker: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return false, fmt.Errorf("sync backend storage identity marker: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return false, fmt.Errorf("close backend storage identity marker: %w", err)
	}
	closed = true
	if err := marker.directory.LinkNoReplace(temporaryName, marker.entry.Name()); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return false, fmt.Errorf("publish backend storage identity marker without overwrite: %w", err)
		}
		if err := marker.directory.Remove(temporaryName); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return false, err
		}
		return false, nil
	}
	if err := marker.directory.Remove(temporaryName); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	return true, nil
}

func replaceBoundMarkerTransition(marker boundMarkerEntry, data []byte) error {
	temporary, temporaryName, err := createBoundMarkerTemporary(marker)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = temporary.Close()
		}
		_ = marker.directory.Remove(temporaryName)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("set backend storage identity anchor permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return fmt.Errorf("write backend storage identity anchor: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync backend storage identity anchor: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close backend storage identity anchor: %w", err)
	}
	closed = true
	if err := marker.directory.Rename(temporaryName, marker.entry.Name()); err != nil {
		return fmt.Errorf("commit backend storage identity anchor: %w", err)
	}
	return nil
}

func createBoundMarkerTemporary(marker boundMarkerEntry) (*os.File, string, error) {
	for range 100 {
		nonce, err := New()
		if err != nil {
			return nil, "", err
		}
		name := "." + marker.entry.Name() + ".tmp-" + nonce.String()
		temporary, err := marker.directory.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		return temporary, name, nil
	}
	return nil, "", errors.New("allocate unique backend storage identity marker temporary name")
}

func recoverBoundMarkerTemporaryLink(marker boundMarkerEntry) error {
	barrier, err := recoverBoundMarkerTemporaryLinkTransition(marker)
	if err != nil {
		return err
	}
	return barrier.sync()
}

func recoverBoundMarkerTemporaryLinkTransition(
	marker boundMarkerEntry,
) (boundMarkerDurabilityBarrier, error) {
	markerInfo, err := marker.entry.Lstat()
	if err != nil {
		return boundMarkerDurabilityBarrier{}, fmt.Errorf(
			"stat backend storage identity marker for publication recovery: %w", err,
		)
	}
	if err := validateMarkerFileMode(marker.entry.Name(), markerInfo.Mode()); err != nil {
		return boundMarkerDurabilityBarrier{}, err
	}
	markerStat, ok := markerInfo.Sys().(*syscall.Stat_t)
	if !ok || markerStat.Nlink < 1 {
		return boundMarkerDurabilityBarrier{}, fmt.Errorf(
			"%w: cannot inspect marker hard-link count", ErrInvalidMarker,
		)
	}
	if markerStat.Nlink == 1 {
		return marker.durabilityBarrier(), nil
	}
	additionalLinks := markerStat.Nlink - 1
	if additionalLinks > maxMarkerPublicationLinks {
		return boundMarkerDurabilityBarrier{}, fmt.Errorf(
			"%w: marker has %d additional hard links", ErrInvalidMarker, additionalLinks,
		)
	}
	directory, err := marker.directory.OpenSelf()
	if err != nil {
		return boundMarkerDurabilityBarrier{}, err
	}
	defer func() { _ = directory.Close() }()
	prefix := "." + marker.entry.Name() + ".tmp-"
	temporaryNames := make([]string, 0, int(additionalLinks))
	for {
		entries, readErr := directory.ReadDir(128)
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), prefix) {
				continue
			}
			if _, parseErr := Parse(strings.TrimPrefix(entry.Name(), prefix)); parseErr != nil {
				continue
			}
			temporary, statErr := marker.directory.Lstat(entry.Name())
			if statErr == nil && temporary.Mode().IsRegular() && os.SameFile(markerInfo, temporary) {
				temporaryNames = append(temporaryNames, entry.Name())
			}
		}
		if errors.Is(readErr, io.EOF) || len(temporaryNames) == int(additionalLinks) {
			break
		}
		if readErr != nil {
			return boundMarkerDurabilityBarrier{}, readErr
		}
	}
	if len(temporaryNames) != int(additionalLinks) {
		current, statErr := marker.entry.Lstat()
		if statErr == nil && os.SameFile(markerInfo, current) {
			if stat, ok := current.Sys().(*syscall.Stat_t); ok && stat.Nlink == 1 {
				return marker.durabilityBarrier(), nil
			}
		}
		return boundMarkerDurabilityBarrier{}, fmt.Errorf(
			"%w: marker retains an unknown hard link", ErrInvalidMarker,
		)
	}
	for _, name := range temporaryNames {
		info, err := marker.directory.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return boundMarkerDurabilityBarrier{}, err
		}
		if info.Mode().IsRegular() && os.SameFile(markerInfo, info) {
			if err := marker.directory.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return boundMarkerDurabilityBarrier{}, err
			}
		}
	}
	current, err := marker.entry.Lstat()
	if err != nil || !os.SameFile(markerInfo, current) {
		return boundMarkerDurabilityBarrier{}, fmt.Errorf(
			"%w: marker changed during publication recovery", ErrInvalidMarker,
		)
	}
	stat, ok := current.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return boundMarkerDurabilityBarrier{}, fmt.Errorf(
			"%w: marker retains an unknown hard link", ErrInvalidMarker,
		)
	}
	return marker.durabilityBarrier(), nil
}
