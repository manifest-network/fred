package docker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// IngressConfig holds configuration for reverse proxy integration.
// When Enabled, containers with routable TCP ports get proxy labels
// pointing Traefik at the per-tenant network for auto-discovery.
// Requires network_isolation to be enabled (validated at config load time).
// Currently generates Traefik-specific labels; the config layer is
// proxy-agnostic so a future backend swap only changes label generation.
//
// The wildcard certificate covering *.WildcardDomain must be provisioned
// at the Traefik level (e.g. via a DNS-01 ACME resolver with domains in
// static config, or a default cert in tls.stores). Fred does not drive
// per-domain ACME challenges — routers are emitted with tls=true but
// name no certresolver, so Traefik serves whichever cert matches SNI.
type IngressConfig struct {
	Enabled        bool   `yaml:"enabled"`
	WildcardDomain string `yaml:"wildcard_domain"`
	Entrypoint     string `yaml:"entrypoint"`

	// CustomDomainCertResolver is the Traefik certresolver name used for
	// per-tenant custom domains (HTTP-01 by default). Defaults to "http01"
	// when empty.
	CustomDomainCertResolver string `yaml:"custom_domain_cert_resolver"`

	// CustomDomainMiddlewares is the list of Traefik middleware references
	// applied to the secondary custom-domain router. Defaults to
	// ["security-headers@file"] when empty.
	CustomDomainMiddlewares []string `yaml:"custom_domain_middlewares"`
}

// Defaults applied at label-emit time when IngressConfig fields are empty.
const (
	defaultCustomDomainCertResolver = "http01"
	defaultCustomDomainMiddleware   = "security-headers@file"
)

// maxDNSLabel is the maximum length of a single DNS label (RFC 1035).
const maxDNSLabel = 63

// ComputeSubdomain derives a unique, human-friendly subdomain from lease and
// service metadata. The result is guaranteed to be at most 63 characters
// (the DNS label limit); long service names are truncated to fit.
//
// The hash suffix is derived from all discriminating fields (leaseUUID,
// serviceName, instanceIndex) to prevent cross-pattern collisions — e.g.,
// service "web" instance 0 vs. service "web-0" instance 0.
//
// Matrix:
//
//	serviceName == "" && quantity <= 1 → {hash7}
//	serviceName == "" && quantity > 1  → {idx}-{hash7}
//	serviceName != "" && quantity <= 1 → {svc}-{hash7}
//	serviceName != "" && quantity > 1  → {svc}-{idx}-{hash7}
func ComputeSubdomain(leaseUUID, serviceName string, instanceIndex, quantity int) string {
	// Hash all discriminating fields so identical prefixes from different
	// naming patterns (e.g., "web" idx=0 vs "web-0" idx=0) get different suffixes.
	h := sha256.Sum256([]byte(leaseUUID + "/" + serviceName + "/" + strconv.Itoa(instanceIndex)))
	hash7 := hex.EncodeToString(h[:])[:7]

	idx := strconv.Itoa(instanceIndex)

	switch {
	case serviceName == "" && quantity <= 1:
		return hash7
	case serviceName == "" && quantity > 1:
		return idx + "-" + hash7
	case serviceName != "" && quantity <= 1:
		svc := truncateLabel(serviceName, maxDNSLabel-len(hash7)-1) // "{svc}-{hash7}"
		return svc + "-" + hash7
	default:
		svc := truncateLabel(serviceName, maxDNSLabel-len(idx)-len(hash7)-2) // "{svc}-{idx}-{hash7}"
		return svc + "-" + idx + "-" + hash7
	}
}

// truncateLabel truncates s to maxLen. When truncation occurs, the last 8
// characters are replaced with a hash of the full string to prevent collisions
// between names that share a common prefix (8 hex chars = 32 bits).
func truncateLabel(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	// 8-char hash suffix + separator = 9 chars reserved for disambiguation.
	h := sha256.Sum256([]byte(s))
	suffix := hex.EncodeToString(h[:])[:8]
	prefix := strings.TrimRight(s[:maxLen-len(suffix)-1], "-")
	return prefix + "-" + suffix
}

// ComputeFQDN returns subdomain + "." + wildcardDomain.
func ComputeFQDN(subdomain, wildcardDomain string) string {
	return subdomain + "." + wildcardDomain
}

// RouterName returns a router name derived from lease metadata.
func RouterName(leaseUUID, serviceName string, instanceIndex, quantity int) string {
	return "fred-" + ComputeSubdomain(leaseUUID, serviceName, instanceIndex, quantity)
}

// SelectIngressPort picks the best TCP port for ingress routing.
// Preference: ingress hint > 80 > 8080 > lowest TCP port number.
// Returns (port, true) if a suitable port is found, (0, false) otherwise.
func SelectIngressPort(ports map[string]PortConfig) (int, bool) {
	if len(ports) == 0 {
		return 0, false
	}

	// First pass: check for an explicit ingress hint (highest priority).
	for spec, cfg := range ports {
		if !cfg.Ingress {
			continue
		}
		if port, ok := parseTCPPort(spec); ok {
			return port, true
		}
	}

	// Second pass: default preference order (80 > 8080 > lowest).
	bestPort := 0
	found := false

	for spec := range ports {
		port, ok := parseTCPPort(spec)
		if !ok {
			continue
		}

		if port == 80 {
			return 80, true
		}

		switch {
		case !found:
			bestPort = port
			found = true
		case port == 8080:
			bestPort = 8080
		case bestPort != 8080 && port < bestPort:
			bestPort = port
		}
	}

	return bestPort, found
}

// parseTCPPort extracts the port number from a "port/tcp" spec string
// (case-insensitive protocol). Returns (port, true) for valid TCP specs,
// (0, false) otherwise.
func parseTCPPort(spec string) (int, bool) {
	parts := strings.SplitN(spec, "/", 2)
	if len(parts) != 2 || strings.ToLower(parts[1]) != "tcp" {
		return 0, false
	}
	port, err := strconv.Atoi(parts[0])
	if err != nil || port < 1 || port > 65535 {
		return 0, false
	}
	return port, true
}

// TraefikLabels generates the Docker labels that Traefik uses for
// auto-discovery and routing configuration. networkName is the
// per-tenant Docker network that Traefik should use to reach the
// container (set via traefik.docker.network). This is the only
// Traefik-specific function; everything else is proxy-agnostic.
//
// Routers are emitted with tls=true and no certresolver; see
// IngressConfig for the wildcard-cert provisioning contract.
func TraefikLabels(cfg IngressConfig, networkName, routerName, fqdn string, containerPort int) map[string]string {
	return map[string]string{
		"traefik.enable":         "true",
		"traefik.docker.network": networkName,
		fmt.Sprintf("traefik.http.routers.%s.rule", routerName):                      fmt.Sprintf("Host(`%s`)", fqdn),
		fmt.Sprintf("traefik.http.routers.%s.entrypoints", routerName):               cfg.Entrypoint,
		fmt.Sprintf("traefik.http.routers.%s.tls", routerName):                       "true",
		fmt.Sprintf("traefik.http.services.%s.loadbalancer.server.port", routerName): strconv.Itoa(containerPort),
	}
}

// Validate checks that all required IngressConfig fields are set when enabled.
func (ic *IngressConfig) Validate() error {
	if !ic.Enabled {
		return nil
	}
	if ic.WildcardDomain == "" {
		return fmt.Errorf("ingress.wildcard_domain is required when ingress is enabled")
	}
	if strings.HasPrefix(ic.WildcardDomain, ".") {
		return fmt.Errorf("ingress.wildcard_domain must not start with '.'")
	}
	if strings.HasSuffix(ic.WildcardDomain, ".") {
		return fmt.Errorf("ingress.wildcard_domain must not end with '.'")
	}
	if ic.Entrypoint == "" {
		return fmt.Errorf("ingress.entrypoint is required when ingress is enabled")
	}
	return nil
}

// CustomDomainRouterName returns a Traefik router name shared across all
// instances of (leaseUUID, serviceName). Custom domain is per-LeaseItem
// (per-service, not per-instance), so this name is the same regardless of
// instance index — every instance container of the service emits the same
// secondary-router labels and Traefik aggregates them into one router with
// N backends. The "-custom" suffix avoids any collision with the
// per-instance primary router name produced by RouterName.
func CustomDomainRouterName(leaseUUID, serviceName string) string {
	return "fred-" + ComputeSubdomain(leaseUUID, serviceName, 0, 1) + "-custom"
}

// TraefikCustomDomainLabels generates the secondary Traefik labels that
// route a tenant-supplied custom domain to the same per-tenant container(s)
// served by the primary router. The returned map is intended to be merged
// into a container's label set alongside the primary TraefikLabels output.
//
// Multi-instance services (quantity > 1) are load-balanced by emitting
// these byte-identical labels on every instance container: Traefik's Docker
// provider deduplicates the router by name and aggregates the service into
// one with N backends. For single-instance services this is the same path
// with a degenerate single backend.
//
// The router uses cfg.CustomDomainCertResolver (default "http01") for
// per-domain ACME and cfg.CustomDomainMiddlewares (default
// "security-headers@file") for transport-level hardening. The entrypoint
// reuses the existing IngressConfig.Entrypoint so the secondary router
// shares the primary's TLS termination posture.
func TraefikCustomDomainLabels(cfg IngressConfig, customRouterName, customDomain string, containerPort int) map[string]string {
	resolver := cfg.CustomDomainCertResolver
	if resolver == "" {
		resolver = defaultCustomDomainCertResolver
	}
	middlewares := cfg.CustomDomainMiddlewares
	if len(middlewares) == 0 {
		middlewares = []string{defaultCustomDomainMiddleware}
	}
	customServiceName := customRouterName + "-svc"
	return map[string]string{
		fmt.Sprintf("traefik.http.routers.%s.rule", customRouterName):              fmt.Sprintf("Host(`%s`)", customDomain),
		fmt.Sprintf("traefik.http.routers.%s.entrypoints", customRouterName):       cfg.Entrypoint,
		fmt.Sprintf("traefik.http.routers.%s.tls", customRouterName):               "true",
		fmt.Sprintf("traefik.http.routers.%s.tls.certresolver", customRouterName):  resolver,
		fmt.Sprintf("traefik.http.routers.%s.middlewares", customRouterName):       strings.Join(middlewares, ","),
		fmt.Sprintf("traefik.http.routers.%s.service", customRouterName):           customServiceName,
		fmt.Sprintf("traefik.http.services.%s.loadbalancer.server.port", customServiceName): strconv.Itoa(containerPort),
	}
}

// ingressLabelParams is the shared input for applyIngressLabels. The same
// inputs flow through both the legacy single-container path and the compose
// stack path, so they share this struct (and the helper) to keep label
// emission in lockstep across both code paths.
type ingressLabelParams struct {
	LeaseUUID    string
	ServiceName  string
	Instance     int
	Quantity     int
	Ingress      IngressConfig
	NetworkName  string
	CustomDomain string
}

// applyIngressLabels merges primary (per-instance generated subdomain) and
// secondary (per-service custom domain) Traefik labels into the given label
// map. Validation failures on the secondary router skip only that router —
// the primary stays. Services with no routable HTTP port emit no labels at
// all (and warn if a CustomDomain was set on such an item).
//
// Used identically by lifecycle.go's CreateContainer (legacy single-item
// lease) and compose_project.go's buildComposeServiceConfig (stack).
// Keeping the glue in one place ensures the two paths can never drift in
// what they emit for the same logical item.
func applyIngressLabels(labels map[string]string, p ingressLabelParams, ports map[string]PortConfig) {
	if !p.Ingress.Enabled {
		return
	}
	port, ok := SelectIngressPort(ports)
	if !ok {
		if p.CustomDomain != "" {
			slog.Warn("custom_domain set on item with no routable HTTP port; skipping",
				"lease_uuid", p.LeaseUUID,
				"service_name", p.ServiceName,
				"custom_domain", p.CustomDomain)
		}
		return
	}

	subdomain := ComputeSubdomain(p.LeaseUUID, p.ServiceName, p.Instance, p.Quantity)
	fqdn := ComputeFQDN(subdomain, p.Ingress.WildcardDomain)
	routerName := RouterName(p.LeaseUUID, p.ServiceName, p.Instance, p.Quantity)
	for k, v := range TraefikLabels(p.Ingress, p.NetworkName, routerName, fqdn, port) {
		labels[k] = v
	}
	labels[LabelFQDN] = fqdn

	if p.CustomDomain == "" {
		return
	}
	if err := validateCustomDomain(p.CustomDomain, p.Ingress.WildcardDomain); err != nil {
		slog.Error("skipping custom-domain router (validation failed)",
			"lease_uuid", p.LeaseUUID,
			"service_name", p.ServiceName,
			"custom_domain", p.CustomDomain,
			"error", err)
		return
	}
	customRouterName := CustomDomainRouterName(p.LeaseUUID, p.ServiceName)
	for k, v := range TraefikCustomDomainLabels(p.Ingress, customRouterName, p.CustomDomain, port) {
		labels[k] = v
	}
	labels[LabelCustomDomain] = p.CustomDomain
}

// validateCustomDomain is Fred's defense-in-depth check on a tenant-supplied
// custom domain before emitting Traefik labels. Chain authoritatively
// validates the FQDN format, reserved-suffix collisions, and global
// uniqueness in MsgSetItemCustomDomain; this function re-runs the
// cheap checks at emit time so a corrupted or out-of-band-set domain
// can never produce labels.
//
// Reuses billingtypes.IsValidFQDN for format. Locally rejects domains
// equal to or under wildcardDomain so the provider's own subdomain space
// (which the wildcard cert covers) cannot be hijacked even if chain's
// reserved-suffix list is misconfigured.
func validateCustomDomain(domain, wildcardDomain string) error {
	if err := billingtypes.IsValidFQDN(domain); err != nil {
		return err
	}
	if wildcardDomain == "" {
		return nil
	}
	d := strings.ToLower(strings.TrimSpace(domain))
	w := strings.ToLower(strings.TrimSpace(wildcardDomain))
	if d == w || strings.HasSuffix(d, "."+w) {
		return fmt.Errorf("custom domain %q is under the provider's wildcard suffix %q", domain, wildcardDomain)
	}
	return nil
}
