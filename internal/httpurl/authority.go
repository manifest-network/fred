// Package httpurl contains validation shared by HTTP URL construction
// boundaries. It deliberately does not decide whether a particular caller may
// use plaintext; production-mode policy remains at the application boundary.
package httpurl

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

var (
	// ErrMissingHost reports an HTTP authority with no host component.
	ErrMissingHost = errors.New("must have a host")

	// ErrInvalidHostname reports an authority such as :443 or . that has a
	// syntactic Host value but no usable destination hostname.
	ErrInvalidHostname = errors.New("must have a non-empty, non-dot hostname")

	// ErrInvalidPort reports an explicitly configured TCP port outside the
	// usable 1..65535 range (including an empty explicit port).
	ErrInvalidPort = errors.New("explicit port must be between 1 and 65535")
)

// ValidateAuthority proves that parsed has a usable HTTP destination
// authority. url.Parse rejects malformed and non-numeric ports, but it accepts
// port-only hosts (":443"), empty explicit ports, port zero, and values above
// 65535. Leaving those until transport time can turn a durable side effect into
// an indefinitely undeliverable request; a port-only host can also resolve to a
// local/unspecified destination.
func ValidateAuthority(parsed *url.URL) error {
	if parsed == nil || parsed.Host == "" {
		return ErrMissingHost
	}
	hostname := parsed.Hostname()
	if hostname == "" || strings.Trim(hostname, ".") == "" {
		return ErrInvalidHostname
	}
	for index := 0; index < len(hostname); index++ {
		if hostname[index] < 0x21 || hostname[index] > 0x7e {
			// net/http applies IDNA mapping at dial time. Restricting this
			// construction boundary to ASCII prevents a Unicode separator or
			// compatibility character from becoming a different authority only
			// after validation.
			return ErrInvalidHostname
		}
	}
	address, zone, hasZone := strings.Cut(hostname, "%")
	if strings.HasPrefix(parsed.Host, "[") {
		if ip := net.ParseIP(address); ip == nil || !strings.Contains(address, ":") {
			return ErrInvalidHostname
		}
		if hasZone && !validZone(zone) {
			return ErrInvalidHostname
		}
	} else if hasZone {
		return ErrInvalidHostname
	}

	port := parsed.Port()
	if port == "" {
		if strings.HasSuffix(parsed.Host, ":") {
			return ErrInvalidPort
		}
		return nil
	}
	value, err := strconv.ParseUint(port, 10, 16)
	if err != nil || value == 0 {
		return ErrInvalidPort
	}
	return nil
}

func validZone(zone string) bool {
	if zone == "" {
		return false
	}
	for index := 0; index < len(zone); index++ {
		character := zone[index]
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			strings.ContainsRune("-._~", rune(character)) {
			continue
		}
		return false
	}
	return true
}

// NormalizeOrigin validates raw as an HTTP(S) origin and returns its one
// canonical spelling. A single trailing slash is accepted and removed. Empty
// query and fragment markers are rejected rather than silently discarded, so
// configuration and direct client construction enforce the same boundary.
func NormalizeOrigin(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		// url.Parse errors retain the complete URL; configuration errors are
		// logged at startup and must not disclose query/user-info material.
		return "", errors.New("invalid URL syntax")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("URL must use http:// or https:// scheme, got %q", parsed.Scheme)
	}
	if err := ValidateAuthority(parsed); err != nil {
		return "", fmt.Errorf("URL %w", err)
	}
	if parsed.User != nil || parsed.Opaque != "" {
		return "", errors.New("backend URL must be an origin without user info, path, query, or fragment")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", errors.New("backend URL must be an origin without user info, path, query, or fragment")
	}
	if strings.Contains(raw, "#") || parsed.Fragment != "" {
		return "", errors.New("backend URL must be an origin without user info, path, query, or fragment")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", errors.New("backend URL must be an origin without user info, path, query, or fragment")
	}
	if parsed.RawPath != "" {
		return "", errors.New("backend URL must use a canonical empty path")
	}
	parsed.Path = ""
	return parsed.String(), nil
}
