// Package callbackurl validates and constructs the static callback URL
// boundary shared by providerd configuration and callback builders.
package callbackurl

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/manifest-network/fred/internal/httpurl"
)

const (
	// ProvisionPath is the only backend callback ingress path.
	ProvisionPath = "/callbacks/provision"

	operationIDQueryParameter = "operation_id"
	lifecycleIDQueryParameter = "lifecycle_id"
)

var errInvalidBase = errors.New("invalid callback URL base")

// Base is a validated, canonical callback URL base. Its zero value is invalid;
// ParseBase is the only constructor. The parsed URL is private so callers
// cannot accidentally mutate validated authority into a different destination.
type Base struct {
	value url.URL
	valid bool
}

// ParseBase validates and canonicalizes a callback base while preserving its
// RawQuery byte-for-byte. Path spelling is canonicalized before HMAC-bound URLs
// are built: trailing separators are removed and RawPath is cleared so
// equivalent escapes and literal Unicode have one RequestURI representation.
func ParseBase(raw string) (Base, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		// *url.Error includes the complete input. Callback-base queries may
		// contain operator-managed opaque values, so returning it would leak
		// those values through ordinary startup error logging.
		return Base{}, errors.New("has invalid URL syntax")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return Base{}, fmt.Errorf("must use http:// or https:// scheme, got %q", parsed.Scheme)
	}
	if err := httpurl.ValidateAuthority(parsed); err != nil {
		return Base{}, err
	}
	if parsed.User != nil {
		return Base{}, errors.New("must not contain user info")
	}
	if parsed.Opaque != "" {
		return Base{}, errors.New("must not be opaque")
	}
	// url.Parse does not preserve whether an empty fragment marker was present.
	// Reject the delimiter in the raw input so no component that HTTP omits is
	// silently accepted and then discarded. An encoded %23 remains path/query
	// data and does not match this check.
	if strings.Contains(raw, "#") || parsed.Fragment != "" {
		return Base{}, errors.New("must not contain a fragment")
	}

	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return Base{}, fmt.Errorf("query is malformed: %w", err)
	}
	if err := ValidateRawQuery(parsed.RawQuery); err != nil {
		return Base{}, err
	}
	for _, parameter := range [...]string{
		operationIDQueryParameter,
		lifecycleIDQueryParameter,
	} {
		if _, exists := query[parameter]; exists {
			return Base{}, fmt.Errorf("must not contain reserved query parameter %q", parameter)
		}
	}

	if !utf8.ValidString(parsed.Path) {
		return Base{}, errors.New("path must be valid UTF-8")
	}
	for _, character := range parsed.Path {
		if !unicode.IsPrint(character) {
			return Base{}, fmt.Errorf("path contains non-printable character %U", character)
		}
	}
	escapedPath := strings.ToLower(parsed.EscapedPath())
	if strings.Contains(escapedPath, "%2f") || strings.Contains(escapedPath, "%5c") {
		return Base{}, errors.New("path must not contain percent-encoded path separators")
	}
	if containsNestedEscape(parsed.Path) {
		return Base{}, errors.New("path must not contain nested percent-encoding")
	}

	parsed.Path = strings.TrimRight(parsed.Path, "/")
	if strings.Contains(parsed.Path, `\`) {
		return Base{}, errors.New("path must not contain backslashes")
	}
	if parsed.Path != "" && path.Clean(parsed.Path) != parsed.Path {
		return Base{}, errors.New("path must not contain empty, dot, or parent segments")
	}

	// Discard alternate spellings only after separator and segment validation.
	// URL.String and RequestURI will now use the single canonical escaping of
	// Path while RawQuery remains untouched.
	parsed.RawPath = ""
	parsed.RawFragment = ""
	if parsed.RawQuery == "" {
		parsed.ForceQuery = false
	}
	return Base{value: *parsed, valid: true}, nil
}

func containsNestedEscape(decodedPath string) bool {
	for index := 0; index+2 < len(decodedPath); index++ {
		if decodedPath[index] == '%' && isHex(decodedPath[index+1]) && isHex(decodedPath[index+2]) {
			return true
		}
	}
	return false
}

func isHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}

// ValidateRawQuery restricts an already-escaped wire representation to
// RFC 3986 query characters. net/url intentionally treats RawQuery as opaque:
// it will place a raw space, backslash, or non-UTF-8 byte directly in an HTTP/1
// request line instead of escaping it. Different servers and proxies can then
// reject or rewrite bytes that the callback HMAC signed. Percent-encoded forms
// remain valid and byte-preserving; url.ParseQuery above proves every percent
// triplet is complete and rejects Go's unsupported raw semicolon separator.
func ValidateRawQuery(rawQuery string) error {
	if _, err := url.ParseQuery(rawQuery); err != nil {
		return fmt.Errorf("query is malformed: %w", err)
	}
	for index := 0; index < len(rawQuery); index++ {
		character := rawQuery[index]
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			strings.ContainsRune("-._~!$&'()*+,=:@/?", rune(character)) ||
			character == '%' {
			continue
		}
		return fmt.Errorf(
			"query contains unescaped byte 0x%02x; percent-encode it for a stable RequestURI",
			character,
		)
	}
	return nil
}

// String returns the normalized base URL, or the empty string for an invalid
// zero value. RawQuery is emitted exactly as supplied to ParseBase.
func (base Base) String() string {
	if !base.valid {
		return ""
	}
	return base.value.String()
}

// EscapedPath returns the canonical path bytes that an HTTP request will carry.
// It is the value a path-stripping verifier prefix must match byte-for-byte.
func (base Base) EscapedPath() string {
	if !base.valid {
		return ""
	}
	return base.value.EscapedPath()
}

// ProvisionURL returns an independent URL value with ProvisionPath appended.
// The returned copy may be given one typed query capability by the provisioner;
// mutating it cannot invalidate Base itself.
func (base Base) ProvisionURL() (*url.URL, error) {
	if !base.valid {
		return nil, errInvalidBase
	}
	result := base.value
	result.Path += ProvisionPath
	return &result, nil
}
