package operation

import (
	"errors"
	"net/url"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/operationid"
	"github.com/manifest-network/fred/internal/uuidv4"
)

// QueryParameter is the callback query parameter carrying an OperationID.
// Backends treat the complete callback URL as opaque and need not interpret it.
const QueryParameter = backend.CallbackOperationIDQueryParameter

var (
	// ErrInvalidID reports a non-canonical or non-v4 operation identity.
	ErrInvalidID = operationid.ErrInvalid

	// ErrAmbiguousQuery reports duplicate operation identity query parameters.
	// Rejecting duplicates avoids depending on a proxy or framework's choice of
	// first-value versus last-value semantics for an authenticated capability.
	ErrAmbiguousQuery = errors.New("operation ID query parameter must occur exactly once")

	// ErrNilQuery reports an attempt to format an ID into a nil url.Values map.
	ErrNilQuery = errors.New("operation ID query destination is nil")
)

// OperationID identifies one lifecycle operation. It aliases the neutral,
// opaque operation identity shared with backends, so the same validated value
// crosses every HTTP and durable boundary without string conversion. The zero
// value is invalid.
//
// Operation IDs are comparable and safe to use as map keys.
type OperationID = operationid.ID

// ParseID parses the canonical lowercase, hyphenated UUIDv4 representation
// used by callback URLs and durable placement intent. Alternative UUID forms,
// uppercase text, non-v4 UUIDs, and the nil UUID are rejected.
func ParseID(text string) (OperationID, error) {
	return operationid.Parse(text)
}

// ParseQuery parses the optional callback operation ID. The boolean reports
// whether the parameter was present, preserving compatibility with tokenless
// callbacks emitted by v0.13 backends. A present parameter must have exactly
// one canonical UUIDv4 value.
func ParseQuery(values url.Values) (id OperationID, present bool, err error) {
	return uuidv4.ParseQuery(values, QueryParameter, ErrAmbiguousQuery, ParseID)
}

// SetQuery writes id using its canonical UUID representation. Existing values
// for QueryParameter are replaced; unrelated values are retained. The
// destination is not mutated on error.
func SetQuery(values url.Values, id OperationID) error {
	return uuidv4.SetQuery(values, QueryParameter, ErrNilQuery, id.MarshalText)
}

func randomOperationID() (OperationID, error) {
	return operationid.New()
}
