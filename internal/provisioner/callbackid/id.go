// Package callbackid contains the shared UUIDv4 wire mechanics used by the
// distinct operation and lifecycle capability types. Authority stays separated
// by their public wrapper types; this leaf centralizes canonical validation,
// parsing, formatting, and query handling.
package callbackid

import (
	"fmt"
	"net/url"

	"github.com/google/uuid"
)

// UUIDv4 is a validated callback identity value. Its representation is opaque;
// authority packages wrap it in their own non-interchangeable public types.
type UUIDv4 struct {
	value uuid.UUID
}

// FromUUID validates a UUID before it enters a typed authority wrapper.
func FromUUID(value uuid.UUID) UUIDv4 {
	id := UUIDv4{value: value}
	if !id.Valid() {
		return UUIDv4{}
	}
	return id
}

// Parse accepts only the canonical lowercase, hyphenated UUIDv4 wire form.
func Parse(text string, invalid error) (UUIDv4, error) {
	parsed, err := uuid.Parse(text)
	if err != nil || parsed.String() != text || parsed.Version() != uuid.Version(4) ||
		parsed.Variant() != uuid.RFC4122 {
		return UUIDv4{}, fmt.Errorf("%w: %q", invalid, text)
	}
	return FromUUID(parsed), nil
}

// Valid reports whether the value is a non-zero RFC 4122 UUIDv4.
func (id UUIDv4) Valid() bool {
	return id.value != uuid.Nil && id.value.Version() == uuid.Version(4) &&
		id.value.Variant() == uuid.RFC4122
}

// String renders the canonical value or an explicit marker for an invalid ID.
func (id UUIDv4) String() string {
	if !id.Valid() {
		return "invalid"
	}
	return id.value.String()
}

// MarshalText returns the canonical wire value and rejects the zero value.
func (id UUIDv4) MarshalText(invalid error) ([]byte, error) {
	if !id.Valid() {
		return nil, invalid
	}
	return []byte(id.value.String()), nil
}

// ParseQuery parses an optional, exactly-once typed callback identity.
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
