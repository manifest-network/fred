package operation

import (
	"encoding"
	"errors"
	"fmt"
	"net/url"

	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/backend"
)

// QueryParameter is the callback query parameter carrying an OperationID.
// Backends treat the complete callback URL as opaque and need not interpret it.
const QueryParameter = backend.CallbackOperationIDQueryParameter

var (
	// ErrInvalidID reports a non-canonical or non-v4 operation identity.
	ErrInvalidID = errors.New("operation ID must be a canonical UUIDv4")

	// ErrAmbiguousQuery reports duplicate operation identity query parameters.
	// Rejecting duplicates avoids depending on a proxy or framework's choice of
	// first-value versus last-value semantics for an authenticated capability.
	ErrAmbiguousQuery = errors.New("operation ID query parameter must occur exactly once")

	// ErrNilQuery reports an attempt to format an ID into a nil url.Values map.
	ErrNilQuery = errors.New("operation ID query destination is nil")
)

// OperationID identifies one lifecycle operation. It is an opaque wrapper
// around a UUID: callers can parse an authenticated canonical wire value or
// receive an ID issued by Registry, but cannot manufacture an arbitrary typed
// capability from a string or integer conversion. The zero value is invalid.
//
// Operation IDs are comparable and safe to use as map keys.
type OperationID struct {
	value uuid.UUID
}

var _ encoding.TextMarshaler = OperationID{}
var _ fmt.Stringer = OperationID{}

// ParseID parses the canonical lowercase, hyphenated UUIDv4 representation
// used by callback URLs and durable placement intent. Alternative UUID forms,
// uppercase text, non-v4 UUIDs, and the nil UUID are rejected.
func ParseID(text string) (OperationID, error) {
	parsed, err := uuid.Parse(text)
	if err != nil || parsed.String() != text || parsed.Version() != uuid.Version(4) ||
		parsed.Variant() != uuid.RFC4122 {
		return OperationID{}, fmt.Errorf("%w: %q", ErrInvalidID, text)
	}
	return newOperationID(parsed), nil
}

// Valid reports whether id contains a canonical UUIDv4 operation identity.
func (id OperationID) Valid() bool {
	return id.value != uuid.Nil && id.value.Version() == uuid.Version(4) &&
		id.value.Variant() == uuid.RFC4122
}

// String returns the canonical operation identity for structured logging. The
// zero value is rendered as an explicit marker rather than the nil UUID.
func (id OperationID) String() string {
	if !id.Valid() {
		return "invalid"
	}
	return id.value.String()
}

// MarshalText returns the canonical lowercase, hyphenated UUIDv4 wire value.
func (id OperationID) MarshalText() ([]byte, error) {
	if !id.Valid() {
		return nil, ErrInvalidID
	}
	return []byte(id.value.String()), nil
}

// ParseQuery parses the optional callback operation ID. The boolean reports
// whether the parameter was present, preserving compatibility with tokenless
// callbacks emitted by v0.13 backends. A present parameter must have exactly
// one canonical UUIDv4 value.
func ParseQuery(values url.Values) (id OperationID, present bool, err error) {
	raw, present := values[QueryParameter]
	if !present {
		return OperationID{}, false, nil
	}
	if len(raw) != 1 {
		return OperationID{}, true, ErrAmbiguousQuery
	}

	id, err = ParseID(raw[0])
	if err != nil {
		return OperationID{}, true, fmt.Errorf("%s: %w", QueryParameter, err)
	}
	return id, true, nil
}

// SetQuery writes id using its canonical UUID representation. Existing values
// for QueryParameter are replaced; unrelated values are retained. The
// destination is not mutated on error.
func SetQuery(values url.Values, id OperationID) error {
	if values == nil {
		return ErrNilQuery
	}
	text, err := id.MarshalText()
	if err != nil {
		return err
	}
	values.Set(QueryParameter, string(text))
	return nil
}

// newOperationID is deliberately package-private. Registry allocation and the
// validated wire parser are the only paths that may mint an OperationID.
func newOperationID(value uuid.UUID) OperationID {
	id := OperationID{value: value}
	if !id.Valid() {
		return OperationID{}
	}
	return id
}
