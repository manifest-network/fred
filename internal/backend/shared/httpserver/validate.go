package httpserver

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ValidateCallbackURL validates that a callback URL is safe to use.
// It rejects non-HTTP(S) schemes and dangerous IP addresses to prevent SSRF.
//
// Note: This validation is defense-in-depth. The callback URL comes from Fred
// (provider), not from untrusted tenants. Fred should validate its own
// callback_base_url configuration. We allow localhost and private IPs here
// since backends commonly run on private networks alongside Fred.
func ValidateCallbackURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("malformed URL")
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}

	if parsed.Host == "" {
		return fmt.Errorf("host is required")
	}

	// Extract hostname (without port) and normalize for SSRF parsing.
	//
	// Trailing-dot trim mirrors internal/config.validateExternalURL — without
	// it, "http://169.254.169.254./..." would make net.ParseIP return nil and
	// skip the link-local block, despite resolving to the same metadata IP.
	//
	// Zone-suffix split handles RFC 6874 zone-scoped IPv6 literals like
	// "http://[fe80::1%25eth0]/...". url.Hostname() returns "fe80::1%eth0"
	// (URL-decodes %25 to %); net.ParseIP rejects zone-suffixed strings;
	// Go's net.Dialer DOES dial them on Linux/BSD. Stripping the zone
	// suffix before ParseIP makes the IP-class check see "fe80::1" and
	// correctly trip IsLinkLocalUnicast.
	hostname := parsed.Hostname()
	hostname = strings.TrimSuffix(hostname, ".")
	if i := strings.IndexByte(hostname, '%'); i >= 0 {
		hostname = hostname[:i]
	}

	// Check if hostname is an IP address
	ip := net.ParseIP(hostname)
	if ip != nil {
		// Block link-local addresses (169.254.0.0/16, fe80::/10) which
		// include cloud metadata endpoints like 169.254.169.254.
		if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return fmt.Errorf("link-local addresses are not allowed")
		}
		// Block unspecified addresses (0.0.0.0, ::). These resolve to
		// "any interface" on the target host and shouldn't be used as
		// callback destinations. Matches internal/config.validateExternalURL.
		if ip.IsUnspecified() {
			return fmt.Errorf("unspecified addresses are not allowed")
		}
	}

	return nil
}
