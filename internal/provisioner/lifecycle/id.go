package lifecycle

import (
	"encoding"
	"errors"
	"fmt"
	"net/url"

	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/callbackid"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// QueryParameter is the callback query parameter carrying an ID. Backends
// treat the complete callback URL as opaque and need not interpret it.
const QueryParameter = backend.CallbackLifecycleIDQueryParameter

var (
	// ErrInvalidID reports a non-canonical or non-v4 lifecycle identity.
	ErrInvalidID = errors.New("lifecycle ID must be a canonical UUIDv4")

	// ErrAmbiguousQuery reports duplicate lifecycle identity query parameters.
	// Rejecting duplicates avoids depending on a proxy or framework's choice of
	// first-value versus last-value semantics for an authenticated capability.
	ErrAmbiguousQuery = errors.New("lifecycle ID query parameter must occur exactly once")

	// ErrNilQuery reports an attempt to format an ID into a nil url.Values map.
	ErrNilQuery = errors.New("lifecycle ID query destination is nil")
)

// ID authorizes observations for one lease lifecycle. It is an opaque wrapper
// around a UUID: callers may parse an authenticated canonical wire value or
// derive the paired identity from a valid operation ID, but cannot manufacture
// a typed identity through a string or integer conversion. The zero value is
// invalid.
//
// IDs are comparable and safe to use as map keys.
type ID struct {
	value callbackid.UUIDv4
}

var _ encoding.TextMarshaler = ID{}
var _ fmt.Stringer = ID{}

// ParseID parses the canonical lowercase, hyphenated UUIDv4 representation
// used by lifecycle callback URLs. Alternative UUID forms, uppercase text,
// non-v4 UUIDs, and the nil UUID are rejected.
func ParseID(text string) (ID, error) {
	parsed, err := callbackid.Parse(text, ErrInvalidID)
	if err != nil {
		return ID{}, err
	}
	return ID{value: parsed}, nil
}

// FromOperationID derives the observational identity paired with a valid
// exact-operation identity. Keeping this conversion explicit prevents callers
// from accidentally passing operation authority where lifecycle authority is
// required. An invalid operation ID is rejected rather than converted to a
// valid-looking value.
func FromOperationID(operationID operation.OperationID) (ID, error) {
	if !operationID.Valid() {
		return ID{}, fmt.Errorf("%w: invalid operation ID", ErrInvalidID)
	}
	return ParseID(operationID.String())
}

// Valid reports whether id contains a canonical UUIDv4 lifecycle identity.
func (id ID) Valid() bool {
	return id.value.Valid()
}

// String returns the canonical lifecycle identity for structured logging. The
// zero value is rendered as an explicit marker rather than the nil UUID.
func (id ID) String() string {
	return id.value.String()
}

// MarshalText returns the canonical lowercase, hyphenated UUIDv4 wire value.
func (id ID) MarshalText() ([]byte, error) {
	return id.value.MarshalText(ErrInvalidID)
}

// ParseQuery parses the optional callback lifecycle ID. The boolean reports
// whether the parameter was present, preserving compatibility with tokenless
// callbacks emitted by legacy backends. A present parameter must have exactly
// one canonical UUIDv4 value.
func ParseQuery(values url.Values) (id ID, present bool, err error) {
	return callbackid.ParseQuery(values, QueryParameter, ErrAmbiguousQuery, ParseID)
}

// SetQuery writes id using its canonical UUID representation. Existing values
// for QueryParameter are replaced; unrelated values are retained. The
// destination is not mutated on error.
func SetQuery(values url.Values, id ID) error {
	return callbackid.SetQuery(values, QueryParameter, ErrNilQuery, id.MarshalText)
}

func newID(value uuid.UUID) ID {
	id := ID{value: callbackid.FromUUID(value)}
	if !id.Valid() {
		return ID{}
	}
	return id
}
