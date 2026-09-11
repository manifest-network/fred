// Package maintenanceid defines the caller-issued identity of one restart or
// update command. Keeping the type below both the API and backend layers lets
// the same opaque value cross every durability boundary without converting it
// back to an unvalidated string.
package maintenanceid

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/uuidv4"
)

var ErrInvalid = errors.New("maintenance request ID must be a canonical UUIDv4")

// ID identifies one logical maintenance command. Its representation is
// private, comparable, and invalid at the zero value. The only construction
// paths either use cryptographic randomness or validate the canonical wire
// representation.
type ID struct {
	value uuidv4.Value
}

var (
	_ encoding.TextMarshaler   = ID{}
	_ encoding.TextUnmarshaler = (*ID)(nil)
	_ json.Marshaler           = ID{}
	_ json.Unmarshaler         = (*ID)(nil)
	_ fmt.Stringer             = ID{}
)

// New returns a cryptographically random UUIDv4 identity. Entropy failure is
// returned to the caller; maintenance identities never have a predictable
// fallback.
func New() (ID, error) {
	value, err := uuidv4.New()
	if err != nil {
		return ID{}, fmt.Errorf("generate maintenance request ID: %w", err)
	}
	return ID{value: value}, nil
}

// Parse accepts only the canonical lowercase, hyphenated RFC 4122 UUIDv4
// representation. Alternative UUID spellings, other versions, and nil are
// rejected so byte equality and logical identity cannot diverge.
func Parse(text string) (ID, error) {
	parsed, err := uuidv4.Parse(text, ErrInvalid)
	if err != nil {
		return ID{}, err
	}
	return ID{value: parsed}, nil
}

// Valid reports whether the value is one canonical UUIDv4 identity.
func (id ID) Valid() bool {
	return id.value.Valid()
}

// IsZero lets encoding/json's omitzero option omit optional typed identities
// without weakening required fields that deliberately marshal the zero value
// as an error.
func (id ID) IsZero() bool { return !id.Valid() }

// String returns the canonical identity. The invalid zero value is rendered
// as an empty string so it cannot be mistaken for a usable nil UUID.
func (id ID) String() string {
	if !id.Valid() {
		return ""
	}
	return id.value.String()
}

// MarshalText implements encoding.TextMarshaler.
func (id ID) MarshalText() ([]byte, error) {
	if !id.Valid() {
		return nil, ErrInvalid
	}
	return id.value.EncodeText(ErrInvalid)
}

// UnmarshalText implements encoding.TextUnmarshaler without mutating the
// receiver on invalid input.
func (id *ID) UnmarshalText(text []byte) error {
	if id == nil {
		return ErrInvalid
	}
	parsed, err := Parse(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// MarshalJSON always emits the canonical string representation.
func (id ID) MarshalJSON() ([]byte, error) {
	return id.value.EncodeJSON(ErrInvalid)
}

// UnmarshalJSON accepts exactly one JSON string containing a canonical UUIDv4.
func (id *ID) UnmarshalJSON(encoded []byte) error {
	if id == nil {
		return ErrInvalid
	}
	parsed, err := uuidv4.ParseJSON(encoded, ErrInvalid)
	if err != nil {
		return err
	}
	*id = ID{value: parsed}
	return nil
}
