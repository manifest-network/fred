// Package backendidentity defines the immutable identity of a backend's
// durable storage substrate.
package backendidentity

import (
	"encoding"
	"errors"
	"fmt"
	"net/url"
	pathpkg "path"
	"strings"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	// QueryParameter carries the expected storage identity on backend requests.
	QueryParameter = "backend_storage_id"

	// ResponseHeader carries the observed storage identity on backend responses.
	ResponseHeader = "X-Fred-Backend-Storage-ID"

	// BoundPathPrefix is an upgraded-only namespace for identity-bound backend
	// operations. A backend that predates storage identity support has no
	// handlers below this prefix and therefore cannot mistake a bound request for
	// a legacy side-effecting operation.
	BoundPathPrefix = "/_fred/storage/"
)

var (
	// ErrInvalidID reports a zero, non-canonical, or non-UUIDv4 storage identity.
	ErrInvalidID = errors.New("backend storage ID must be a canonical UUIDv4")

	// ErrIdentityDrift marks a permanent contradiction between the storage
	// lineage sealed at startup and the substrate currently reachable by the
	// backend. Callers must fail-stop rather than retry the operation against a
	// replacement substrate. Temporary Docker/Kubernetes reachability failures
	// deliberately do not wrap this sentinel.
	ErrIdentityDrift = errors.New("backend storage identity drift")

	// ErrMutationOutcomeAmbiguous marks a raw substrate mutation whose required
	// post-mutation identity proof failed. The side effect may have reached the
	// original substrate, a replacement substrate, or neither; callers must keep
	// durable intent/finalizer evidence and defer classification to restart
	// recovery. Unlike a pre-mutation verification failure, this error latches the
	// backend lifetime because compensating cleanup is another unsafe mutation.
	ErrMutationOutcomeAmbiguous = errors.New("backend mutation outcome is ambiguous")

	// ErrInvalidLegacyPath reports a path that cannot safely be placed below the
	// upgraded-only identity namespace.
	ErrInvalidLegacyPath = errors.New("backend legacy path must be an absolute clean path without query or fragment")
)

// ID identifies one durable backend storage substrate. Its zero value is
// invalid. IDs are comparable and safe to use as map keys.
type ID struct {
	value uuid.UUID
}

// VerifiedStorage is proof that an ID was loaded from a complete, committed
// marker pair. Its fields are intentionally private: runtime components that
// can open or mutate identity-bound control state must receive this capability
// from marker verification, not manufacture authority from an arbitrary UUID.
// The zero value is invalid.
type VerifiedStorage struct {
	id ID
}

// PendingStorage is authority to prepare the authoritative stores for one
// crash-resumable marker-pair initialization. It is minted only after the
// pending anchor that records id is durable. Its fields are intentionally
// private: an arbitrary UUID, including a valid ID parsed from configuration or
// disk, cannot authorize store creation or binding.
//
// The zero value is invalid. Copies share revocation state, and the marker
// coordinator revokes them before InitializeWithStores returns. PendingStorage
// is deliberately distinct from VerifiedStorage because a pending lineage may
// create or bind stores but must not be used by ordinary runtime components
// until both markers are committed and the complete store set has passed
// read-only verification.
type PendingStorage struct {
	state *pendingStorageState
}

type pendingStorageState struct {
	id     ID
	active atomic.Bool
}

var _ encoding.TextMarshaler = ID{}
var _ fmt.Stringer = ID{}

// New allocates a storage identity from the operating system's cryptographic
// random source.
func New() (ID, error) {
	value, err := uuid.NewRandom()
	if err != nil {
		return ID{}, fmt.Errorf("allocate backend storage ID: %w", err)
	}
	return fromUUID(value), nil
}

// Parse accepts only the canonical lowercase, hyphenated RFC 4122 UUIDv4
// representation.
func Parse(text string) (ID, error) {
	value, err := uuid.Parse(text)
	if err != nil || value.String() != text || value.Version() != uuid.Version(4) ||
		value.Variant() != uuid.RFC4122 {
		return ID{}, fmt.Errorf("%w: %q", ErrInvalidID, text)
	}
	return fromUUID(value), nil
}

// Valid reports whether id contains a non-zero RFC 4122 UUIDv4.
func (id ID) Valid() bool {
	return id.value != uuid.Nil && id.value.Version() == uuid.Version(4) &&
		id.value.Variant() == uuid.RFC4122
}

// String renders the canonical identity, or "invalid" for the zero value.
func (id ID) String() string {
	if !id.Valid() {
		return "invalid"
	}
	return id.value.String()
}

// ID returns the immutable storage identity proved by the marker pair.
func (storage VerifiedStorage) ID() ID {
	return storage.id
}

// Valid reports whether storage contains a verified, non-zero identity.
func (storage VerifiedStorage) Valid() bool {
	return storage.id.Valid()
}

// ID returns the immutable storage identity recorded by the durable pending
// anchor while this capability remains active. It returns the invalid ID after
// revocation. The value identifies the lineage but does not itself carry
// pending store-preparation authority.
func (storage PendingStorage) ID() ID {
	if !storage.Valid() {
		return ID{}
	}
	return storage.state.id
}

// Valid reports whether storage contains a pending, non-zero identity minted
// by the marker-pair coordinator.
func (storage PendingStorage) Valid() bool {
	return storage.state != nil && storage.state.id.Valid() && storage.state.active.Load()
}

func newPendingStorage(id ID) PendingStorage {
	if !id.Valid() {
		return PendingStorage{}
	}
	state := &pendingStorageState{id: id}
	state.active.Store(true)
	return PendingStorage{state: state}
}

func (storage PendingStorage) revoke() {
	if storage.state != nil {
		storage.state.active.Store(false)
	}
}

// MarshalText returns the canonical wire representation and rejects the zero
// value.
func (id ID) MarshalText() ([]byte, error) {
	if !id.Valid() {
		return nil, ErrInvalidID
	}
	return []byte(id.value.String()), nil
}

// BoundPath places a legacy backend operation below the upgraded-only storage
// identity namespace. It rejects ambiguous and traversal-bearing paths rather
// than relying on an HTTP client, proxy, or server to normalize them in the
// same way.
func BoundPath(id ID, legacyPath string) (string, error) {
	if !id.Valid() {
		return "", ErrInvalidID
	}
	if err := validateLegacyPath(legacyPath); err != nil {
		return "", err
	}
	return BoundPathPrefix + id.String() + legacyPath, nil
}

func validateLegacyPath(legacyPath string) error {
	if len(legacyPath) <= 1 || !utf8.ValidString(legacyPath) ||
		!strings.HasPrefix(legacyPath, "/") || strings.Contains(legacyPath, "//") ||
		strings.Contains(legacyPath, "..") || strings.ContainsAny(legacyPath, "?#\\") ||
		strings.HasPrefix(legacyPath, BoundPathPrefix) || pathpkg.Clean(legacyPath) != legacyPath {
		return fmt.Errorf("%w: %q", ErrInvalidLegacyPath, legacyPath)
	}

	decoded, err := url.PathUnescape(legacyPath)
	if err != nil || decoded != legacyPath {
		return fmt.Errorf("%w: %q", ErrInvalidLegacyPath, legacyPath)
	}
	for _, character := range legacyPath {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return fmt.Errorf("%w: %q", ErrInvalidLegacyPath, legacyPath)
		}
		allowed := character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			strings.ContainsRune("/_-.", character)
		if character > unicode.MaxASCII || !allowed {
			return fmt.Errorf("%w: %q", ErrInvalidLegacyPath, legacyPath)
		}
	}
	return nil
}

func fromUUID(value uuid.UUID) ID {
	id := ID{value: value}
	if !id.Valid() {
		return ID{}
	}
	return id
}
