// Package uuidv4 contains the shared canonical UUIDv4 wire mechanics used by
// distinct typed identities. Authority remains separated by the public wrapper
// types; this leaf only centralizes validation, formatting, and query helpers.
package uuidv4

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"

	"github.com/google/uuid"
)

// Value is a validated UUIDv4. Its representation is opaque; authority
// packages wrap it in their own non-interchangeable public types.
type Value struct {
	value uuid.UUID
}

// New returns a cryptographically random UUIDv4 value. Entropy failures are
// returned to the caller; domain wrappers decide how to add identity-specific
// context to that error.
func New() (Value, error) {
	value, err := uuid.NewRandom()
	if err != nil {
		return Value{}, err
	}
	return FromUUID(value), nil
}

// FromUUID validates a UUID before it enters a typed identity wrapper.
func FromUUID(value uuid.UUID) Value {
	id := Value{value: value}
	if !id.Valid() {
		return Value{}
	}
	return id
}

// Parse accepts only the canonical lowercase, hyphenated UUIDv4 wire form.
func Parse(text string, invalid error) (Value, error) {
	parsed, err := uuid.Parse(text)
	if err != nil || parsed.String() != text || parsed.Version() != uuid.Version(4) ||
		parsed.Variant() != uuid.RFC4122 {
		// Typed UUIDs commonly carry callback or idempotency authority. Keep the
		// rejected wire value out of the error so routine validation logging cannot
		// disclose a nearly-canonical capability (for example, an upper-case UUID).
		return Value{}, invalid
	}
	return FromUUID(parsed), nil
}

// Valid reports whether the value is a non-zero RFC 4122 UUIDv4.
func (id Value) Valid() bool {
	return id.value != uuid.Nil && id.value.Version() == uuid.Version(4) &&
		id.value.Variant() == uuid.RFC4122
}

// String renders the canonical value or an explicit marker for an invalid ID.
func (id Value) String() string {
	if !id.Valid() {
		return "invalid"
	}
	return id.value.String()
}

// DiagnosticFingerprint returns a stable, non-reversible 96-bit correlation
// value for a validated identity. Domain wrappers supply distinct domains and
// prefixes so fingerprints cannot be confused across authority types. Invalid
// values return the explicit marker "invalid".
func (id Value) DiagnosticFingerprint(domain, prefix string) string {
	if !id.Valid() {
		return "invalid"
	}
	digest := sha256.Sum256([]byte(domain + "\x00" + id.value.String()))
	return prefix + hex.EncodeToString(digest[:12])
}

// EncodeText returns the canonical wire value and rejects the zero value.
// Domain wrappers expose the standard encoding.TextMarshaler signature; this
// helper takes their domain-specific sentinel without pretending to implement
// that interface itself.
func (id Value) EncodeText(invalid error) ([]byte, error) {
	if !id.Valid() {
		return nil, invalid
	}
	return []byte(id.value.String()), nil
}

// EncodeJSON emits one canonical JSON string and rejects an invalid value.
// Domain identity wrappers retain their own json.Marshaler methods so the Go
// types remain non-interchangeable at every caller boundary.
func (id Value) EncodeJSON(invalid error) ([]byte, error) {
	text, err := id.EncodeText(invalid)
	if err != nil {
		return nil, err
	}
	return json.Marshal(string(text))
}

// ParseJSON accepts exactly one JSON string containing a canonical UUIDv4.
// It rejects concatenated values and trailing tokens rather than letting a
// wrapper accidentally depend on decoder first-value semantics.
func ParseJSON(encoded []byte, invalid error) (Value, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	var text string
	if err := decoder.Decode(&text); err != nil {
		return Value{}, invalid
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Value{}, invalid
	}
	return Parse(text, invalid)
}

// ParseQuery parses an optional, exactly-once typed identity.
func ParseQuery[T any](
	values url.Values,
	parameter string,
	ambiguous error,
	parse func(string) (T, error),
) (id T, present bool, err error) {
	raw, present := values[parameter]
	if !present {
		return id, false, nil
	}
	if len(raw) != 1 {
		return id, true, ambiguous
	}
	id, err = parse(raw[0])
	if err != nil {
		return id, true, fmt.Errorf("%s: %w", parameter, err)
	}
	return id, true, nil
}

// SetQuery writes a canonical typed identity without mutating values on error.
func SetQuery(
	values url.Values,
	parameter string,
	nilQuery error,
	marshal func() ([]byte, error),
) error {
	if values == nil {
		return nilQuery
	}
	text, err := marshal()
	if err != nil {
		return err
	}
	values.Set(parameter, string(text))
	return nil
}
