package docker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// IngressConfig holds configuration for reverse proxy integration.
// When Enabled, containers with routable TCP ports get proxy labels and
// are connected to the proxy network for auto-discovery.
// Currently generates Traefik-specific labels; the config layer is
// proxy-agnostic so a future backend swap only changes label generation.
type IngressConfig struct {
	Enabled        bool   `yaml:"enabled"`
	WildcardDomain string `yaml:"wildcard_domain"`
	Network        string `yaml:"network"`
	CertResolver   string `yaml:"cert_resolver"`
	Entrypoint     string `yaml:"entrypoint"`
}

// ComputeSubdomain derives a unique, human-friendly subdomain from lease and
// service metadata.
//
// Matrix:
//
//	serviceName == "" && quantity <= 1 → {hash7}
//	serviceName == "" && quantity > 1  → {idx}-{hash7}
//	serviceName != "" && quantity <= 1 → {svc}-{hash7}
//	serviceName != "" && quantity > 1  → {svc}-{idx}-{hash7}
func ComputeSubdomain(leaseUUID, serviceName string, instanceIndex, quantity int) string {
	h := sha256.Sum256([]byte(leaseUUID))
	hash7 := hex.EncodeToString(h[:])[:7]

	idx := strconv.Itoa(instanceIndex)

	switch {
	case serviceName == "" && quantity <= 1:
		return hash7
	case serviceName == "" && quantity > 1:
		return idx + "-" + hash7
	case serviceName != "" && quantity <= 1:
		return serviceName + "-" + hash7
	default:
		return serviceName + "-" + idx + "-" + hash7
	}
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
// Preference: 80 > 8080 > lowest TCP port number.
// Returns (port, true) if a suitable port is found, (0, false) otherwise.
func SelectIngressPort(ports map[string]PortConfig) (int, bool) {
	if len(ports) == 0 {
		return 0, false
	}

	bestPort := 0
	found := false

	for spec := range ports {
		parts := strings.SplitN(spec, "/", 2)
		if len(parts) != 2 {
			continue
		}
		proto := parts[1]
		if proto != "tcp" {
			continue
		}
		port, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}

		if port == 80 {
			return 80, true
		}

		if !found {
			bestPort = port
			found = true
		} else if port == 8080 && bestPort != 80 {
			bestPort = 8080
		} else if bestPort != 80 && bestPort != 8080 && port < bestPort {
			bestPort = port
		}
	}

	return bestPort, found
}

// TraefikLabels generates the Docker labels that Traefik uses for
// auto-discovery and routing configuration. This is the only
// Traefik-specific function; everything else is proxy-agnostic.
func TraefikLabels(cfg IngressConfig, routerName, fqdn string, containerPort int) map[string]string {
	return map[string]string{
		"traefik.enable": "true",
		fmt.Sprintf("traefik.http.routers.%s.rule", routerName):                      fmt.Sprintf("Host(`%s`)", fqdn),
		fmt.Sprintf("traefik.http.routers.%s.entrypoints", routerName):               cfg.Entrypoint,
		fmt.Sprintf("traefik.http.routers.%s.tls.certresolver", routerName):          cfg.CertResolver,
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
	if ic.Network == "" {
		return fmt.Errorf("ingress.network is required when ingress is enabled")
	}
	if ic.CertResolver == "" {
		return fmt.Errorf("ingress.cert_resolver is required when ingress is enabled")
	}
	if ic.Entrypoint == "" {
		return fmt.Errorf("ingress.entrypoint is required when ingress is enabled")
	}
	return nil
}
