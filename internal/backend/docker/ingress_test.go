package docker

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeSubdomain(t *testing.T) {
	const leaseUUID = "019c1ee7-1aaf-7000-802c-ad775c72cc27"

	tests := []struct {
		name          string
		serviceName   string
		instanceIndex int
		quantity      int
		wantPrefix    string // prefix before the hash, empty for hash-only
		wantLen       int    // expected total length
	}{
		{
			name:       "no service, single instance",
			quantity:   1,
			wantPrefix: "",
			wantLen:    7,
		},
		{
			name:          "no service, multi instance",
			instanceIndex: 2,
			quantity:      3,
			wantPrefix:    "2-",
			wantLen:       7 + 2, // "2-" + hash7
		},
		{
			name:        "with service, single instance",
			serviceName: "web",
			quantity:    1,
			wantPrefix:  "web-",
			wantLen:     7 + 4, // "web-" + hash7
		},
		{
			name:          "with service, multi instance",
			serviceName:   "web",
			instanceIndex: 1,
			quantity:      3,
			wantPrefix:    "web-1-",
			wantLen:       7 + 6, // "web-1-" + hash7
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeSubdomain(leaseUUID, tt.serviceName, tt.instanceIndex, tt.quantity)
			assert.Len(t, got, tt.wantLen)
			if tt.wantPrefix != "" {
				assert.True(t, len(got) > len(tt.wantPrefix))
				assert.Equal(t, tt.wantPrefix, got[:len(tt.wantPrefix)])
			}
		})
	}

	t.Run("deterministic", func(t *testing.T) {
		a := ComputeSubdomain(leaseUUID, "web", 0, 1)
		b := ComputeSubdomain(leaseUUID, "web", 0, 1)
		assert.Equal(t, a, b)
	})

	t.Run("hash is 7 hex chars", func(t *testing.T) {
		sub := ComputeSubdomain(leaseUUID, "", 0, 1)
		assert.Len(t, sub, 7)
		// Verify it's valid hex.
		for _, c := range sub {
			assert.Contains(t, "0123456789abcdef", string(c))
		}
	})

	t.Run("long service name truncated to DNS limit", func(t *testing.T) {
		longName := "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0" // 63 chars
		sub := ComputeSubdomain(leaseUUID, longName, 0, 1)
		assert.LessOrEqual(t, len(sub), 63, "subdomain must not exceed DNS label limit")
		assert.Contains(t, sub, "-") // still has hash separator
	})

	t.Run("long service name multi-instance truncated to DNS limit", func(t *testing.T) {
		longName := "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0"
		sub := ComputeSubdomain(leaseUUID, longName, 99, 100)
		assert.LessOrEqual(t, len(sub), 63, "subdomain must not exceed DNS label limit")
		assert.Contains(t, sub, "99-") // instance index present
	})

	t.Run("trailing hyphen trimmed after truncation", func(t *testing.T) {
		// Craft a name where truncation lands exactly on a hyphen.
		name := strings.Repeat("a", 54) + "-" + strings.Repeat("b", 8) // 63 chars total
		sub := ComputeSubdomain(leaseUUID, name, 0, 1)
		assert.LessOrEqual(t, len(sub), 63)
		// No label in the subdomain should end with a hyphen.
		for _, part := range strings.Split(sub, "-") {
			assert.NotEmpty(t, part, "subdomain should not contain empty parts from double hyphens")
		}
	})

	t.Run("different long names with shared prefix do not collide", func(t *testing.T) {
		prefix := strings.Repeat("a", 50)
		nameA := prefix + "-alpha-version"  // 63 chars+
		nameB := prefix + "-bravo-version"  // 63 chars+
		subA := ComputeSubdomain(leaseUUID, nameA, 0, 1)
		subB := ComputeSubdomain(leaseUUID, nameB, 0, 1)
		assert.LessOrEqual(t, len(subA), 63)
		assert.LessOrEqual(t, len(subB), 63)
		assert.NotEqual(t, subA, subB, "truncated names with different suffixes must produce different subdomains")
	})

	t.Run("cross-pattern collision: svc=web idx=0 vs svc=web-0 idx=0", func(t *testing.T) {
		// "web" with quantity=2, instanceIndex=0 → "web-0-{hash}"
		// "web-0" with quantity=1 → "web-0-{hash}"
		// These must differ because the hash incorporates serviceName + instanceIndex.
		subA := ComputeSubdomain(leaseUUID, "web", 0, 2)
		subB := ComputeSubdomain(leaseUUID, "web-0", 0, 1)
		assert.NotEqual(t, subA, subB, "cross-pattern names must not collide")
	})
}

func TestComputeFQDN(t *testing.T) {
	fqdn := ComputeFQDN("web-abc1234", "barney8.manifest0.net")
	assert.Equal(t, "web-abc1234.barney8.manifest0.net", fqdn)
}

func TestRouterName(t *testing.T) {
	name := RouterName("lease-uuid", "web", 0, 1)
	assert.True(t, len(name) > 5)
	assert.Equal(t, "fred-", name[:5])

	// Should be "fred-" + ComputeSubdomain result
	sub := ComputeSubdomain("lease-uuid", "web", 0, 1)
	assert.Equal(t, "fred-"+sub, name)
}

func TestSelectIngressPort(t *testing.T) {
	tests := []struct {
		name     string
		ports    map[string]PortConfig
		wantPort int
		wantOK   bool
	}{
		{
			name:   "empty",
			ports:  map[string]PortConfig{},
			wantOK: false,
		},
		{
			name:     "single TCP",
			ports:    map[string]PortConfig{"3000/tcp": {}},
			wantPort: 3000,
			wantOK:   true,
		},
		{
			name: "prefer 80",
			ports: map[string]PortConfig{
				"3000/tcp": {},
				"80/tcp":   {},
				"8080/tcp": {},
			},
			wantPort: 80,
			wantOK:   true,
		},
		{
			name: "prefer 8080 over others",
			ports: map[string]PortConfig{
				"3000/tcp": {},
				"8080/tcp": {},
				"9090/tcp": {},
			},
			wantPort: 8080,
			wantOK:   true,
		},
		{
			name: "8080 beats lower non-preferred ports",
			ports: map[string]PortConfig{
				"8080/tcp": {},
				"3000/tcp": {},
			},
			wantPort: 8080,
			wantOK:   true,
		},
		{
			name: "lowest fallback",
			ports: map[string]PortConfig{
				"9090/tcp": {},
				"3000/tcp": {},
				"5000/tcp": {},
			},
			wantPort: 3000,
			wantOK:   true,
		},
		{
			name: "UDP only",
			ports: map[string]PortConfig{
				"53/udp": {},
			},
			wantOK: false,
		},
		{
			name: "mixed protocols",
			ports: map[string]PortConfig{
				"53/udp":   {},
				"8080/tcp": {},
			},
			wantPort: 8080,
			wantOK:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			port, ok := SelectIngressPort(tt.ports)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.wantPort, port)
			}
		})
	}
}

func TestTraefikLabels(t *testing.T) {
	cfg := IngressConfig{
		Enabled:        true,
		WildcardDomain: "barney8.manifest0.net",
		Network:        "traefik",
		CertResolver:   "letsencrypt",
		Entrypoint:     "websecure",
	}

	labels := TraefikLabels(cfg, "fred-web-abc1234", "web-abc1234.barney8.manifest0.net", 80)

	require.Len(t, labels, 6)
	assert.Equal(t, "true", labels["traefik.enable"])
	assert.Equal(t, "traefik", labels["traefik.docker.network"])
	assert.Equal(t, "Host(`web-abc1234.barney8.manifest0.net`)", labels["traefik.http.routers.fred-web-abc1234.rule"])
	assert.Equal(t, "websecure", labels["traefik.http.routers.fred-web-abc1234.entrypoints"])
	assert.Equal(t, "letsencrypt", labels["traefik.http.routers.fred-web-abc1234.tls.certresolver"])
	assert.Equal(t, "80", labels["traefik.http.services.fred-web-abc1234.loadbalancer.server.port"])
}

func TestIngressConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     IngressConfig
		wantErr string
	}{
		{
			name: "disabled is valid",
			cfg:  IngressConfig{Enabled: false},
		},
		{
			name: "all fields set",
			cfg: IngressConfig{
				Enabled:        true,
				WildcardDomain: "barney8.manifest0.net",
				Network:        "traefik",
				CertResolver:   "letsencrypt",
				Entrypoint:     "websecure",
			},
		},
		{
			name: "missing wildcard_domain",
			cfg: IngressConfig{
				Enabled:      true,
				Network:      "traefik",
				CertResolver: "letsencrypt",
				Entrypoint:   "websecure",
			},
			wantErr: "wildcard_domain is required",
		},
		{
			name: "wildcard_domain starts with dot",
			cfg: IngressConfig{
				Enabled:        true,
				WildcardDomain: ".barney8.manifest0.net",
				Network:        "traefik",
				CertResolver:   "letsencrypt",
				Entrypoint:     "websecure",
			},
			wantErr: "must not start with '.'",
		},
		{
			name: "wildcard_domain ends with dot",
			cfg: IngressConfig{
				Enabled:        true,
				WildcardDomain: "barney8.manifest0.net.",
				Network:        "traefik",
				CertResolver:   "letsencrypt",
				Entrypoint:     "websecure",
			},
			wantErr: "must not end with '.'",
		},
		{
			name: "missing network",
			cfg: IngressConfig{
				Enabled:        true,
				WildcardDomain: "barney8.manifest0.net",
				CertResolver:   "letsencrypt",
				Entrypoint:     "websecure",
			},
			wantErr: "network is required",
		},
		{
			name: "missing cert_resolver",
			cfg: IngressConfig{
				Enabled:        true,
				WildcardDomain: "barney8.manifest0.net",
				Network:        "traefik",
				Entrypoint:     "websecure",
			},
			wantErr: "cert_resolver is required",
		},
		{
			name: "missing entrypoint",
			cfg: IngressConfig{
				Enabled:        true,
				WildcardDomain: "barney8.manifest0.net",
				Network:        "traefik",
				CertResolver:   "letsencrypt",
			},
			wantErr: "entrypoint is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr, fmt.Sprintf("error: %v", err))
			}
		})
	}
}
