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

var errInvalidEndpoint = errors.New("invalid callback URL endpoint")

// Endpoint is an immutable, wire-stable HTTP callback destination. ParseEndpoint
// is its only constructor; keeping url.URL private prevents callers from
// mutating a validated destination before it is persisted or sent.
type Endpoint struct {
	value url.URL
	valid bool
}

// ParseEndpoint validates a complete callback destination without rewriting
// it. In particular, Path and RawQuery must already be in the exact form that
// net/http will put in RequestURI, because callback authentication signs those
// bytes and durable queues must be able to replay them unchanged.
func ParseEndpoint(raw string) (Endpoint, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		// *url.Error includes the complete input, including the typed bearer ID
		// in RawQuery. Validation errors are routinely logged by callers, so
		// never return the raw parse error across this boundary.
		return Endpoint{}, errors.New("has invalid URL syntax")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return Endpoint{}, fmt.Errorf("scheme must be http or https, got %q", parsed.Scheme)
	}
	if err := httpurl.ValidateAuthority(parsed); err != nil {
		return Endpoint{}, err
	}
	if parsed.User != nil {
		return Endpoint{}, errors.New("must not contain user info")
	}
	if parsed.Opaque != "" {
		return Endpoint{}, errors.New("must not be opaque")
	}
	if strings.Contains(raw, "#") || parsed.Fragment != "" {
		return Endpoint{}, errors.New("must not contain a fragment")
	}
	if parsed.ForceQuery {
		return Endpoint{}, errors.New("must not contain an empty query marker")
	}
	if err := ValidateRawQuery(parsed.RawQuery); err != nil {
		return Endpoint{}, err
	}
	if err := validateEndpointPath(parsed); err != nil {
		return Endpoint{}, err
	}
	if !strings.HasSuffix(parsed.EscapedPath(), ProvisionPath) {
		return Endpoint{}, fmt.Errorf("path must end with %q", ProvisionPath)
	}
	if parsed.String() != raw {
		return Endpoint{}, errors.New("must use the canonical wire representation")
	}
	return Endpoint{value: *parsed, valid: true}, nil
}

func validateEndpointPath(parsed *url.URL) error {
	if !utf8.ValidString(parsed.Path) {
		return errors.New("path must be valid UTF-8")
	}
	for _, character := range parsed.Path {
		if !unicode.IsPrint(character) {
			return fmt.Errorf("path contains non-printable character %U", character)
		}
	}
	if parsed.RawPath != "" {
		return errors.New("path must use canonical percent-encoding")
	}
	escapedPath := strings.ToLower(parsed.EscapedPath())
	if strings.Contains(escapedPath, "%2f") || strings.Contains(escapedPath, "%5c") {
		return errors.New("path must not contain percent-encoded path separators")
	}
	if containsNestedEscape(parsed.Path) {
		return errors.New("path must not contain nested percent-encoding")
	}
	if strings.Contains(parsed.Path, `\`) {
		return errors.New("path must not contain backslashes")
	}
	if parsed.Path != "" && path.Clean(parsed.Path) != parsed.Path {
		return errors.New("path must not contain empty, dot, parent, or trailing segments")
	}
	return nil
}

// String returns the exact validated destination, or the empty string for an
// invalid zero value.
func (endpoint Endpoint) String() string {
	if !endpoint.valid {
		return ""
	}
	return endpoint.value.String()
}

// EscapedPath returns the exact path bytes carried by RequestURI.
func (endpoint Endpoint) EscapedPath() string {
	if !endpoint.valid {
		return ""
	}
	return endpoint.value.EscapedPath()
}

// RawQuery returns the exact query bytes carried by RequestURI.
func (endpoint Endpoint) RawQuery() string {
	if !endpoint.valid {
		return ""
	}
	return endpoint.value.RawQuery
}

// Hostname returns the validated hostname without brackets or port.
func (endpoint Endpoint) Hostname() string {
	if !endpoint.valid {
		return ""
	}
	return endpoint.value.Hostname()
}

// WithRawQuery returns the same validated destination with a replacement
// wire-stable query. It is used only to exchange typed callback capabilities
// while preserving the already-proved authority and path.
func (endpoint Endpoint) WithRawQuery(rawQuery string) (string, error) {
	if !endpoint.valid {
		return "", errInvalidEndpoint
	}
	if err := ValidateRawQuery(rawQuery); err != nil {
		return "", err
	}
	result := endpoint.value
	result.RawQuery = rawQuery
	result.ForceQuery = false
	return result.String(), nil
}
