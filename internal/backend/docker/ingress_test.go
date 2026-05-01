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
		nameA := prefix + "-alpha-version" // 63 chars+
		nameB := prefix + "-bravo-version" // 63 chars+
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
		{
			name: "ingress hint overrides default preference",
			ports: map[string]PortConfig{
				"80/tcp":    {},
				"18789/tcp": {Ingress: true},
				"8083/tcp":  {},
			},
			wantPort: 18789,
			wantOK:   true,
		},
		{
			name: "ingress hint on single port",
			ports: map[string]PortConfig{
				"9090/tcp": {Ingress: true},
			},
			wantPort: 9090,
			wantOK:   true,
		},
		{
			name: "ingress hint beats 8080",
			ports: map[string]PortConfig{
				"8080/tcp": {},
				"3000/tcp": {Ingress: true},
			},
			wantPort: 3000,
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
		Entrypoint:     "websecure",
	}

	tenantNetwork := "fred-tenant-abc123"
	labels := TraefikLabels(cfg, tenantNetwork, "fred-web-abc1234", "web-abc1234.barney8.manifest0.net", 80)

	require.Len(t, labels, 7)
	assert.Equal(t, "true", labels["traefik.enable"])
	assert.Equal(t, tenantNetwork, labels["traefik.docker.network"])
	assert.Equal(t, "Host(`web-abc1234.barney8.manifest0.net`)", labels["traefik.http.routers.fred-web-abc1234.rule"])
	assert.Equal(t, "websecure", labels["traefik.http.routers.fred-web-abc1234.entrypoints"])
	assert.Equal(t, "true", labels["traefik.http.routers.fred-web-abc1234.tls"])
	assert.NotContains(t, labels, "traefik.http.routers.fred-web-abc1234.tls.certresolver")
	// Explicit router→service binding is required so Traefik's docker
	// provider does not need to auto-bind by name (which fails as soon as
	// a secondary service is added on the same container — see ENG-93).
	assert.Equal(t, "fred-web-abc1234", labels["traefik.http.routers.fred-web-abc1234.service"])
	assert.Equal(t, "80", labels["traefik.http.services.fred-web-abc1234.loadbalancer.server.port"])
}

func TestCustomDomainRouterName(t *testing.T) {
	const lease = "lease-abc"
	const svc = "web"

	// Per-service: instance index must NOT change the result.
	r0 := CustomDomainRouterName(lease, svc)
	r1 := CustomDomainRouterName(lease, svc)
	assert.Equal(t, r0, r1, "router name must be deterministic per (leaseUUID, serviceName)")

	// Different (lease, service) tuples produce different names.
	assert.NotEqual(t, r0, CustomDomainRouterName(lease, "db"))
	assert.NotEqual(t, r0, CustomDomainRouterName("lease-other", svc))

	// Distinct from the per-instance primary RouterName, regardless of instance.
	assert.NotEqual(t, r0, RouterName(lease, svc, 0, 1))
	assert.NotEqual(t, r0, RouterName(lease, svc, 0, 3))
	assert.NotEqual(t, r0, RouterName(lease, svc, 1, 3))

	// Deterministic suffix.
	assert.Contains(t, r0, "-custom")

	// Legacy single-item lease (empty service name) still produces a name.
	legacy := CustomDomainRouterName(lease, "")
	assert.NotEmpty(t, legacy)
	assert.NotEqual(t, legacy, r0)
}

func TestTraefikCustomDomainLabels(t *testing.T) {
	t.Run("defaults applied", func(t *testing.T) {
		cfg := IngressConfig{Enabled: true, Entrypoint: "websecure"}
		labels := TraefikCustomDomainLabels(cfg, "fred-web-abc-custom", "foo.example.com", 80)

		assert.Equal(t, "Host(`foo.example.com`)", labels["traefik.http.routers.fred-web-abc-custom.rule"])
		assert.Equal(t, "websecure", labels["traefik.http.routers.fred-web-abc-custom.entrypoints"])
		assert.Equal(t, "true", labels["traefik.http.routers.fred-web-abc-custom.tls"])
		assert.Equal(t, "http01", labels["traefik.http.routers.fred-web-abc-custom.tls.certresolver"])
		assert.Equal(t, "security-headers@file", labels["traefik.http.routers.fred-web-abc-custom.middlewares"])
		assert.Equal(t, "fred-web-abc-custom-svc", labels["traefik.http.routers.fred-web-abc-custom.service"])
		assert.Equal(t, "80", labels["traefik.http.services.fred-web-abc-custom-svc.loadbalancer.server.port"])

		// Secondary must NOT re-emit traefik.enable / traefik.docker.network
		// (those are per-container, owned by the primary block).
		assert.NotContains(t, labels, "traefik.enable")
		assert.NotContains(t, labels, "traefik.docker.network")
	})

	t.Run("configurable cert resolver and middlewares", func(t *testing.T) {
		cfg := IngressConfig{
			Enabled:                  true,
			Entrypoint:               "websecure",
			CustomDomainCertResolver: "letsencrypt-staging",
			CustomDomainMiddlewares:  []string{"hsts@file", "compress@file"},
		}
		labels := TraefikCustomDomainLabels(cfg, "fred-web-abc-custom", "foo.example.com", 8080)

		assert.Equal(t, "letsencrypt-staging", labels["traefik.http.routers.fred-web-abc-custom.tls.certresolver"])
		assert.Equal(t, "hsts@file,compress@file", labels["traefik.http.routers.fred-web-abc-custom.middlewares"])
		assert.Equal(t, "8080", labels["traefik.http.services.fred-web-abc-custom-svc.loadbalancer.server.port"])
	})

	t.Run("byte-identical across calls with same inputs", func(t *testing.T) {
		cfg := IngressConfig{Enabled: true, Entrypoint: "websecure"}
		a := TraefikCustomDomainLabels(cfg, "fred-web-custom", "foo.example.com", 80)
		b := TraefikCustomDomainLabels(cfg, "fred-web-custom", "foo.example.com", 80)
		assert.Equal(t, a, b, "load-balancing requires byte-identical labels across all instance containers")
	})
}

func TestValidateCustomDomain(t *testing.T) {
	const wildcard = "barney0.manifest0.net"

	tests := []struct {
		name    string
		domain  string
		wantErr string
	}{
		{name: "well-formed", domain: "foo.example.com"},
		{name: "subdomain", domain: "admin.foo.example.com"},
		{name: "empty", domain: "", wantErr: "empty domain"},
		{name: "uppercase rejected", domain: "Foo.example.com", wantErr: "lowercase"},
		{name: "scheme rejected", domain: "https://foo.example.com", wantErr: "scheme"},
		{name: "leading dot rejected", domain: ".foo.example.com", wantErr: "must not start with"},
		{name: "no separator rejected", domain: "localhost", wantErr: "separator"},
		{name: "equals wildcard", domain: wildcard, wantErr: "wildcard suffix"},
		{name: "subdomain of wildcard", domain: "evil." + wildcard, wantErr: "wildcard suffix"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCustomDomain(tt.domain, wildcard)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr, fmt.Sprintf("error: %v", err))
			}
		})
	}

	t.Run("empty wildcard skips local check", func(t *testing.T) {
		// IngressConfig.Validate already requires non-empty wildcard when
		// enabled; this just covers the defensive nil-cfg branch.
		err := validateCustomDomain("foo.example.com", "")
		require.NoError(t, err)
	})
}

func TestApplyIngressLabels(t *testing.T) {
	cfg := IngressConfig{
		Enabled:        true,
		WildcardDomain: "barney0.manifest0.net",
		Entrypoint:     "websecure",
	}
	httpPorts := map[string]PortConfig{"80/tcp": {}}

	t.Run("disabled ingress emits nothing", func(t *testing.T) {
		labels := map[string]string{}
		applyIngressLabels(labels, ingressLabelParams{
			LeaseUUID:    "lease-1",
			ServiceName:  "web",
			Instance:     0,
			Quantity:     1,
			Ingress:      IngressConfig{Enabled: false},
			NetworkName:  "fred-tenant-abc",
			CustomDomain: "foo.example.com",
		}, httpPorts)
		assert.Empty(t, labels)
	})

	t.Run("no routable port + no custom_domain emits nothing", func(t *testing.T) {
		labels := map[string]string{}
		applyIngressLabels(labels, ingressLabelParams{
			LeaseUUID:   "lease-1",
			ServiceName: "redis",
			Instance:    0,
			Quantity:    1,
			Ingress:     cfg,
			NetworkName: "fred-tenant-abc",
		}, map[string]PortConfig{}) // no ports
		assert.Empty(t, labels)
	})

	t.Run("no routable port + custom_domain skips both with warn", func(t *testing.T) {
		// Regression: a tenant who set custom_domain on a TCP-only / no-HTTP
		// service must not see Fred emit a secondary router that would
		// reference a non-existent port. Primary is also absent (no port).
		labels := map[string]string{}
		applyIngressLabels(labels, ingressLabelParams{
			LeaseUUID:    "lease-1",
			ServiceName:  "redis",
			Instance:     0,
			Quantity:     1,
			Ingress:      cfg,
			NetworkName:  "fred-tenant-abc",
			CustomDomain: "foo.example.com",
		}, map[string]PortConfig{}) // no ports
		assert.Empty(t, labels)
	})

	t.Run("primary only when CustomDomain empty", func(t *testing.T) {
		labels := map[string]string{}
		applyIngressLabels(labels, ingressLabelParams{
			LeaseUUID:   "lease-1",
			ServiceName: "web",
			Instance:    0,
			Quantity:    1,
			Ingress:     cfg,
			NetworkName: "fred-tenant-abc",
		}, httpPorts)

		assert.Equal(t, "true", labels["traefik.enable"])
		assert.NotEmpty(t, labels[LabelFQDN])
		assert.NotContains(t, labels, LabelCustomDomain)

		customRouter := CustomDomainRouterName("lease-1", "web")
		assert.NotContains(t, labels, "traefik.http.routers."+customRouter+".rule")
	})

	t.Run("primary + secondary when CustomDomain valid", func(t *testing.T) {
		labels := map[string]string{}
		applyIngressLabels(labels, ingressLabelParams{
			LeaseUUID:    "lease-1",
			ServiceName:  "web",
			Instance:     0,
			Quantity:     1,
			Ingress:      cfg,
			NetworkName:  "fred-tenant-abc",
			CustomDomain: "foo.example.com",
		}, httpPorts)

		// Primary present.
		primaryRouter := RouterName("lease-1", "web", 0, 1)
		assert.Equal(t, "true", labels["traefik.http.routers."+primaryRouter+".tls"])
		// Secondary present.
		customRouter := CustomDomainRouterName("lease-1", "web")
		assert.Equal(t, "Host(`foo.example.com`)", labels["traefik.http.routers."+customRouter+".rule"])
		assert.Equal(t, "foo.example.com", labels[LabelCustomDomain])

		// ENG-93 regression: when a container declares both the primary and
		// secondary services, Traefik's docker provider can no longer
		// auto-bind the primary router to its same-named service (the
		// container has two services, so the binding is ambiguous). The
		// primary router must therefore carry an explicit `.service` label
		// pointing at its own service. Without it the primary URL returns
		// HTTP 418 once a custom_domain is set.
		assert.Equal(t, primaryRouter, labels["traefik.http.routers."+primaryRouter+".service"])
		assert.Equal(t, customRouter+"-svc", labels["traefik.http.routers."+customRouter+".service"])
	})

	t.Run("invalid CustomDomain skips secondary, primary kept", func(t *testing.T) {
		labels := map[string]string{}
		applyIngressLabels(labels, ingressLabelParams{
			LeaseUUID:    "lease-1",
			ServiceName:  "web",
			Instance:     0,
			Quantity:     1,
			Ingress:      cfg,
			NetworkName:  "fred-tenant-abc",
			CustomDomain: "evil." + cfg.WildcardDomain, // subdomain of provider's wildcard
		}, httpPorts)

		// Primary kept.
		primaryRouter := RouterName("lease-1", "web", 0, 1)
		assert.Equal(t, "true", labels["traefik.http.routers."+primaryRouter+".tls"])
		// Secondary absent.
		customRouter := CustomDomainRouterName("lease-1", "web")
		assert.NotContains(t, labels, "traefik.http.routers."+customRouter+".rule")
		assert.NotContains(t, labels, LabelCustomDomain)
	})

	t.Run("multi-instance shared secondary name across instance indices", func(t *testing.T) {
		// Sanity check the load-balancing contract: the helper produces
		// the same secondary router/service name for every instance index
		// of the same (lease, service). Compose path tests assert this
		// end-to-end; this verifies the helper's invariant directly.
		var l0, l1, l2 map[string]string
		for i, dst := range []*map[string]string{&l0, &l1, &l2} {
			labels := map[string]string{}
			applyIngressLabels(labels, ingressLabelParams{
				LeaseUUID:    "lease-1",
				ServiceName:  "web",
				Instance:     i,
				Quantity:     3,
				Ingress:      cfg,
				NetworkName:  "fred-tenant-abc",
				CustomDomain: "foo.example.com",
			}, httpPorts)
			*dst = labels
		}
		customRouter := CustomDomainRouterName("lease-1", "web")
		ruleKey := "traefik.http.routers." + customRouter + ".rule"
		assert.Equal(t, l0[ruleKey], l1[ruleKey])
		assert.Equal(t, l1[ruleKey], l2[ruleKey])
		// Primary differs per instance.
		assert.NotEqual(t, l0[LabelFQDN], l1[LabelFQDN])
	})
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
				Entrypoint:     "websecure",
			},
		},
		{
			name: "missing wildcard_domain",
			cfg: IngressConfig{
				Enabled:    true,
				Entrypoint: "websecure",
			},
			wantErr: "wildcard_domain is required",
		},
		{
			name: "wildcard_domain starts with dot",
			cfg: IngressConfig{
				Enabled:        true,
				WildcardDomain: ".barney8.manifest0.net",
				Entrypoint:     "websecure",
			},
			wantErr: "must not start with '.'",
		},
		{
			name: "wildcard_domain ends with dot",
			cfg: IngressConfig{
				Enabled:        true,
				WildcardDomain: "barney8.manifest0.net.",
				Entrypoint:     "websecure",
			},
			wantErr: "must not end with '.'",
		},
		{
			name: "missing entrypoint",
			cfg: IngressConfig{
				Enabled:        true,
				WildcardDomain: "barney8.manifest0.net",
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
