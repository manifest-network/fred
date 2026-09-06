// Package operationid defines the identity of one provision or restore
// operation. It is a neutral leaf shared by provider and backend layers so an
// operation keeps one validated type across HTTP and durable boundaries.
package operationid

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/manifest-network/fred/internal/uuidv4"
)

// ErrInvalid reports a missing, non-canonical, or non-v4 operation identity.
var ErrInvalid = errors.New("operation ID must be a canonical UUIDv4")

// ID identifies one provision or restore operation. Its representation is
// private and comparable. The zero value is deliberately invalid; construction
// either uses cryptographic randomness or validates the canonical wire value.
type ID struct {
	value uuidv4.Value
}

var (
	_ encoding.TextMarshaler   = ID{}
	_ encoding.TextUnmarshaler = (*ID)(nil)
	_ json.Marshaler           = ID{}
	_ json.Unmarshaler         = (*ID)(nil)
	_ fmt.Formatter            = ID{}
	_ fmt.Stringer             = ID{}
	_ slog.LogValuer           = ID{}
)

const diagnosticFingerprintDomain = "fred-operation-id-diagnostic-v1"

// New returns a cryptographically random operation identity. Entropy failure
// is returned rather than replaced with a predictable fallback.
func New() (ID, error) {
	value, err := uuidv4.New()
	if err != nil {
		return ID{}, fmt.Errorf("generate operation ID: %w", err)
	}
	return ID{value: value}, nil
}

// Parse accepts only the lowercase, hyphenated RFC 4122 UUIDv4 wire form.
func Parse(text string) (ID, error) {
	parsed, err := uuidv4.Parse(text, ErrInvalid)
	if err != nil {
		return ID{}, err
	}
	return ID{value: parsed}, nil
}

// Valid reports whether id contains one canonical UUIDv4 identity.
func (id ID) Valid() bool { return id.value.Valid() }

// IsZero lets encoding/json's omitzero option omit optional IDs without
// weakening required fields, whose zero value still fails to marshal.
func (id ID) IsZero() bool { return !id.Valid() }

// String returns the canonical identity, or an empty string for the invalid
// zero value. Empty is also the explicit tokenless compatibility marker at
// boundaries that still read v0.13 records.
func (id ID) String() string {
	if !id.Valid() {
		return ""
	}
	return id.value.String()
}

// Fingerprint returns a stable, non-reversible diagnostic correlation value.
// Operation IDs are callback capabilities and must not be copied into logs;
// this domain-separated 96-bit SHA-256 prefix lets operators correlate one
// operation across components without exposing its canonical wire value.
func (id ID) Fingerprint() string {
	if !id.Valid() {
		return "invalid"
	}
	return id.value.DiagnosticFingerprint(diagnosticFingerprintDomain, "op_")
}

// LogValue makes passing a typed operation ID directly to slog safe by
// construction. Wire and persistence code must opt in explicitly through the
// text/JSON marshalers or String; generic structured logging sees only the
// diagnostic fingerprint.
func (id ID) LogValue() slog.Value {
	return slog.StringValue(id.Fingerprint())
}

// Format makes every generic fmt path diagnostic-only. String remains an
// explicit wire escape hatch for callback construction and persistence, while
// fmt.Errorf, fmt.Sprint, and logger implementations that use fmt cannot
// accidentally disclose the callback capability. The diagnostic fingerprint
// is intentionally rendered the same way for every verb and flag: callers
// that need canonical bytes must opt in through String or MarshalText.
func (id ID) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(id.Fingerprint()))
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

// MarshalJSON emits the canonical JSON string and rejects an invalid ID.
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
