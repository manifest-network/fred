package manifest

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestStackManifest_ServiceNameValidation(t *testing.T) {
	valid := []string{
		"web",
		"my-db",
		"a",
		"a1b2",
		"abc-def-123",
		strings.Repeat("a", 63), // max length
	}
	for _, name := range valid {
		t.Run("valid/"+name, func(t *testing.T) {
			sm := StackManifest{
				Services: map[string]*Manifest{
					name: {Image: "nginx"},
				},
			}
			assert.NoError(t, sm.Validate())
		})
	}

	invalid := []struct {
		name   string
		errMsg string
	}{
		{"Web", "must match"},                              // uppercase
		{"my_db", "must match"},                            // underscore
		{"-web", "must match"},                             // leading dash
		{"web-", "must match"},                             // trailing dash
		{"my.svc", "must match"},                           // dot
		{strings.Repeat("a", 64), "exceeds 63 characters"}, // too long
		{"web@1", "must match"},                            // special char
	}
	for _, tc := range invalid {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			sm := StackManifest{
				Services: map[string]*Manifest{
					tc.name: {Image: "nginx"},
				},
			}
			err := sm.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestStackManifest_Validate_EmptyServices(t *testing.T) {
	sm := StackManifest{Services: map[string]*Manifest{}}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one service")
}

func TestStackManifest_Validate_NilServices(t *testing.T) {
	sm := StackManifest{Services: nil}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one service")
}

func TestStackManifest_Validate_NilServiceManifest(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": nil,
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil manifest")
}

func TestStackManifest_Validate_EmptyServiceName(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"": {Image: "nginx"},
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestStackManifest_Validate_InvalidServiceManifest(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {Image: ""}, // image is required
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "web")
}

func TestValidateStackAgainstItems_DuplicateServiceNames(t *testing.T) {
	t.Run("duplicate service names in items", func(t *testing.T) {
		stack := &StackManifest{
			Services: map[string]*Manifest{
				"web": {Image: "nginx"},
			},
		}
		items := []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		}
		err := ValidateStackAgainstItems(stack, items)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
	})

	t.Run("matching items and manifest", func(t *testing.T) {
		stack := &StackManifest{
			Services: map[string]*Manifest{
				"web": {Image: "nginx"},
				"db":  {Image: "postgres"},
			},
		}
		items := []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
		}
		assert.NoError(t, ValidateStackAgainstItems(stack, items))
	})

	t.Run("extra item not in manifest", func(t *testing.T) {
		stack := &StackManifest{
			Services: map[string]*Manifest{
				"web": {Image: "nginx"},
			},
		}
		items := []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "cache"},
		}
		err := ValidateStackAgainstItems(stack, items)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cache")
		assert.Contains(t, err.Error(), "no matching manifest service")
	})

	t.Run("extra service not in items", func(t *testing.T) {
		stack := &StackManifest{
			Services: map[string]*Manifest{
				"web": {Image: "nginx"},
				"db":  {Image: "postgres"},
			},
		}
		items := []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		}
		err := ValidateStackAgainstItems(stack, items)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db")
		assert.Contains(t, err.Error(), "no matching lease item")
	})
}

func TestParsePayload(t *testing.T) {
	t.Run("flat manifest auto-wraps under DefaultServiceName", func(t *testing.T) {
		data := []byte(`{"image":"nginx:latest"}`)
		s, err := ParsePayload(data)
		require.NoError(t, err)
		require.NotNil(t, s)
		require.Len(t, s.Services, 1)
		svc := s.Services[DefaultServiceName]
		require.NotNil(t, svc, "auto-wrapped service should be keyed under DefaultServiceName")
		assert.Equal(t, "nginx:latest", svc.Image)
	})

	t.Run("stack manifest passes through unchanged", func(t *testing.T) {
		data := []byte(`{"services":{"web":{"image":"nginx:latest"},"db":{"image":"postgres:16"}}}`)
		s, err := ParsePayload(data)
		require.NoError(t, err)
		require.NotNil(t, s)
		assert.Len(t, s.Services, 2)
		assert.Equal(t, "nginx:latest", s.Services["web"].Image)
		assert.Equal(t, "postgres:16", s.Services["db"].Image)
	})

	t.Run("empty payload", func(t *testing.T) {
		_, err := ParsePayload(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})

	t.Run("invalid JSON", func(t *testing.T) {
		_, err := ParsePayload([]byte("{invalid"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid JSON")
	})

	t.Run("stack with invalid service name", func(t *testing.T) {
		// Uppercase service name fails the service-name regex.
		data := []byte(`{"services":{"Web":{"image":"nginx:latest"}}}`)
		_, err := ParsePayload(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must match")
	})
}

// --- depends_on validation tests ---

func TestStackManifest_DependsOn_ValidServiceStarted(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db": {Condition: "service_started"},
				},
			},
			"db": {Image: "postgres"},
		},
	}
	assert.NoError(t, sm.Validate())
}

func TestStackManifest_DependsOn_ValidServiceHealthy(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db": {Condition: "service_healthy"},
				},
			},
			"db": {
				Image: "postgres",
				HealthCheck: &HealthCheckConfig{
					Test: []string{"CMD", "pg_isready"},
				},
			},
		},
	}
	assert.NoError(t, sm.Validate())
}

func TestStackManifest_DependsOn_MissingRef(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"cache": {Condition: "service_started"},
				},
			},
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown service")
	assert.Contains(t, err.Error(), "cache")
}

func TestStackManifest_DependsOn_SelfRef(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"web": {Condition: "service_started"},
				},
			},
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot reference itself")
}

func TestStackManifest_DependsOn_InvalidCondition(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db": {Condition: "service_completed_successfully"},
				},
			},
			"db": {Image: "postgres"},
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid condition")
}

func TestStackManifest_DependsOn_EmptyCondition(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db": {Condition: ""},
				},
			},
			"db": {Image: "postgres"},
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty condition")
}

func TestStackManifest_DependsOn_HealthyWithoutHealthCheck(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db": {Condition: "service_healthy"},
				},
			},
			"db": {Image: "postgres"}, // no health check
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "health_check")
}

func TestStackManifest_DependsOn_HealthyWithNoneHealthCheck(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db": {Condition: "service_healthy"},
				},
			},
			"db": {
				Image: "postgres",
				HealthCheck: &HealthCheckConfig{
					Test: []string{"NONE"}, // disabled health check
				},
			},
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "health_check")
}

func TestHealthCheckConfig_Validate_NoneRejectsExtraArgs(t *testing.T) {
	// NONE disables the health check and takes no arguments. Extra elements must
	// be rejected so the Go validator matches the JSON schema contract, which
	// enforces maxItems: 1 for the NONE branch (docs/manifest-schema.json).
	err := (&HealthCheckConfig{Test: []string{"NONE", "extra"}}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NONE")

	// The canonical single-element form stays valid.
	require.NoError(t, (&HealthCheckConfig{Test: []string{"NONE"}}).Validate())
}

func TestStackManifest_DependsOn_MultipleDependencies(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db":    {Condition: "service_healthy"},
					"cache": {Condition: "service_started"},
				},
			},
			"db": {
				Image: "postgres",
				HealthCheck: &HealthCheckConfig{
					Test: []string{"CMD", "pg_isready"},
				},
			},
			"cache": {Image: "redis"},
		},
	}
	assert.NoError(t, sm.Validate())
}

func TestStackManifest_DependsOn_CycleDirectAB(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"a": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"b": {Condition: "service_started"},
				},
			},
			"b": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"a": {Condition: "service_started"},
				},
			},
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle detected")
}

func TestStackManifest_DependsOn_CycleTransitiveABC(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"a": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"b": {Condition: "service_started"},
				},
			},
			"b": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"c": {Condition: "service_started"},
				},
			},
			"c": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"a": {Condition: "service_started"},
				},
			},
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle detected")
}

func TestStackManifest_DependsOn_DiamondNoCycle(t *testing.T) {
	// A→B, A→C, B→D, C→D — diamond, not a cycle.
	sm := StackManifest{
		Services: map[string]*Manifest{
			"a": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"b": {Condition: "service_started"},
					"c": {Condition: "service_started"},
				},
			},
			"b": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"d": {Condition: "service_started"},
				},
			},
			"c": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"d": {Condition: "service_started"},
				},
			},
			"d": {Image: "nginx"},
		},
	}
	assert.NoError(t, sm.Validate())
}

func TestManifest_DependsOn_RejectedOutsideStack(t *testing.T) {
	m := &Manifest{
		Image: "nginx",
		DependsOn: map[string]DependsOnCondition{
			"db": {Condition: "service_started"},
		},
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only allowed in stack manifests")
}

// --- stop_grace_period validation tests ---

func TestManifest_StopGracePeriod_Valid(t *testing.T) {
	d := Duration(10 * time.Second)
	m := &Manifest{
		Image:           "nginx",
		StopGracePeriod: &d,
	}
	assert.NoError(t, m.Validate())
}

func TestManifest_StopGracePeriod_TooLow(t *testing.T) {
	d := Duration(500 * time.Millisecond)
	m := &Manifest{
		Image:           "nginx",
		StopGracePeriod: &d,
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least 1s")
}

func TestManifest_StopGracePeriod_TooHigh(t *testing.T) {
	d := Duration(121 * time.Second)
	m := &Manifest{
		Image:           "nginx",
		StopGracePeriod: &d,
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most 120s")
}

func TestManifest_StopGracePeriod_Boundaries(t *testing.T) {
	t.Run("exactly 1s", func(t *testing.T) {
		d := Duration(time.Second)
		m := &Manifest{Image: "nginx", StopGracePeriod: &d}
		assert.NoError(t, m.Validate())
	})
	t.Run("exactly 120s", func(t *testing.T) {
		d := Duration(120 * time.Second)
		m := &Manifest{Image: "nginx", StopGracePeriod: &d}
		assert.NoError(t, m.Validate())
	})
}

// --- label validation tests ---

func TestManifest_Labels_RejectsTraefikPrefix(t *testing.T) {
	m := &Manifest{
		Image: "nginx",
		Labels: map[string]string{
			"traefik.http.routers.pwn.rule": "Host(`victim.example.com`)",
		},
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved prefix 'traefik.'")
}

func TestManifest_Labels_RejectsFredPrefix(t *testing.T) {
	m := &Manifest{
		Image:  "nginx",
		Labels: map[string]string{"fred.lease": "spoof"},
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved prefix 'fred.'")
}

func TestManifest_Labels_AllowsBenignLabel(t *testing.T) {
	m := &Manifest{
		Image:  "nginx",
		Labels: map[string]string{"com.example.team": "payments"},
	}
	assert.NoError(t, m.Validate())
}

// TestManifest_Labels_RejectsMixedCaseReservedPrefix guards ENG-595: Traefik's
// Docker provider is case-insensitive, so a mixed-case reserved prefix would
// register a working router / bookkeeping key just like the lowercase form. The
// case-sensitive check missed these, bypassing the ENG-497 hijack defense.
func TestManifest_Labels_RejectsMixedCaseReservedPrefix(t *testing.T) {
	for _, tc := range []struct {
		key  string
		want string // reserved prefix named in the error
	}{
		{"Traefik.http.routers.pwn.rule", "traefik."},
		{"TRAEFIK.enable", "traefik."},
		{"TrAeFiK.http.services.x.loadbalancer.server.port", "traefik."},
		{"FRED.lease", "fred."},
		{"Fred.retention", "fred."},
	} {
		t.Run(tc.key, func(t *testing.T) {
			m := &Manifest{Image: "nginx", Labels: map[string]string{tc.key: "x"}}
			err := m.Validate()
			require.Error(t, err, "mixed-case reserved prefix must be rejected")
			assert.Contains(t, err.Error(), "reserved prefix '"+tc.want+"'")
		})
	}
}

// TestManifest_Labels_AllowsReservedLookalike ensures the case-insensitive check
// does not over-reject: a key that merely contains — but does not start with — a
// reserved token (the trailing dot in the prefix is load-bearing) stays allowed.
func TestManifest_Labels_AllowsReservedLookalike(t *testing.T) {
	for _, key := range []string{
		"traefikish.foo",      // no dot boundary
		"com.example.traefik", // reserved token not at the start
		"myfred.bookkeeping",  // not a prefix
		"FREDDIE.mercury",     // lowercases to "freddie." — not "fred."
	} {
		t.Run(key, func(t *testing.T) {
			m := &Manifest{Image: "nginx", Labels: map[string]string{key: "x"}}
			assert.NoError(t, m.Validate(), "benign lookalike must be allowed")
		})
	}
}

func TestStackManifest_Labels_RejectsTraefikPrefix(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*Manifest{
			"web": {
				Image:  "nginx",
				Labels: map[string]string{"traefik.enable": "true"},
			},
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved prefix 'traefik.'")
}

func TestIsReservedLabelKey(t *testing.T) {
	for key, want := range map[string]bool{
		"fred.retention":         true,
		"traefik.http.routers.x": true,
		"Traefik.http.routers.x": true, // ENG-595: case-insensitive
		"TRAEFIK.enable":         true,
		"FRED.lease":             true,
		"com.example.customer":   false,
		"fredx.anything":         false,
		"traefikish.x":           false,
		"":                       false,
	} {
		require.Equal(t, want, IsReservedLabelKey(key), "key=%q", key)
	}
}

func TestIsBlockedEnvKey(t *testing.T) {
	for key, want := range map[string]bool{
		"PATH":            true,
		"path":            true, // checks are case-insensitive (validateEnvVars uppercases)
		"LD_PRELOAD":      true,
		"FRED_ANYTHING":   true,
		"DOCKER_HOST":     true,
		"APP_CUSTOMER_ID": false,
		"A=B":             true, // structurally invalid as an env key
		"":                true,
	} {
		require.Equal(t, want, IsBlockedEnvKey(key), "key=%q", key)
	}
}

// --- expose validation tests ---

func TestManifest_Expose_Valid(t *testing.T) {
	m := &Manifest{
		Image:  "nginx",
		Expose: []string{"3000", "8080"},
	}
	assert.NoError(t, m.Validate())
}

func TestManifest_Expose_InvalidPort(t *testing.T) {
	m := &Manifest{
		Image:  "nginx",
		Expose: []string{"abc"},
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid port number")
}

func TestManifest_Expose_Duplicate(t *testing.T) {
	m := &Manifest{
		Image:  "nginx",
		Expose: []string{"3000", "3000"},
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate port")
}

func TestManifest_Expose_OutOfRange(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		m := &Manifest{Image: "nginx", Expose: []string{"0"}}
		err := m.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "between 1 and 65535")
	})
	t.Run("too high", func(t *testing.T) {
		m := &Manifest{Image: "nginx", Expose: []string{"65536"}}
		err := m.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "between 1 and 65535")
	})
}

// --- collection size caps (ENG-547) ---
//
// Unbounded tenant-controlled collections let a single cheap lease exhaust
// host resources. The port map is the load-bearing case: every entry becomes
// a published host port + iptables DNAT rule (userland-proxy:false in prod),
// so tens of thousands of entries exhaust the shared ephemeral-port range and
// flood netfilter — a cross-tenant provisioning DoS. Expose/Env/Labels are
// capped for defense-in-depth.

func manifestWithNPorts(n int) *Manifest {
	ports := make(map[string]PortConfig, n)
	for i := 1; i <= n; i++ {
		ports[fmt.Sprintf("%d/tcp", i)] = PortConfig{}
	}
	return &Manifest{Image: "nginx", Ports: ports}
}

func TestManifest_Ports_AtCapAllowed(t *testing.T) {
	assert.NoError(t, manifestWithNPorts(MaxPorts).Validate())
}

func TestManifest_Ports_OverCapRejected(t *testing.T) {
	err := manifestWithNPorts(MaxPorts + 1).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many ports")
}

func TestManifest_Expose_OverCapRejected(t *testing.T) {
	expose := make([]string, 0, MaxExposePorts+1)
	for i := 1; i <= MaxExposePorts+1; i++ {
		expose = append(expose, strconv.Itoa(i))
	}
	err := (&Manifest{Image: "nginx", Expose: expose}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many expose ports")
}

func TestManifest_Env_OverCapRejected(t *testing.T) {
	env := make(map[string]string, MaxEnvVars+1)
	for i := 0; i <= MaxEnvVars; i++ {
		env[fmt.Sprintf("VAR_%d", i)] = "x"
	}
	err := (&Manifest{Image: "nginx", Env: env}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many env vars")
}

func TestManifest_Labels_OverCapRejected(t *testing.T) {
	labels := make(map[string]string, MaxLabels+1)
	for i := 0; i <= MaxLabels; i++ {
		labels[fmt.Sprintf("com.example.k%d", i)] = "v"
	}
	err := (&Manifest{Image: "nginx", Labels: labels}).Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many labels")
}

// TestPortSpecValidation exercises the package-internal validatePortSpec
// helper used by Manifest.Validate. (Moved here from the docker test file
// when the manifest parser was lifted out of internal/backend/docker.)
func TestPortSpecValidation(t *testing.T) {
	tests := []struct {
		spec      string
		expectErr bool
	}{
		{"80/tcp", false},
		{"443/tcp", false},
		{"53/udp", false},
		{"8080/tcp", false},
		{"65535/tcp", false},
		{"1/tcp", false},

		// Invalid
		{"80", true},           // Missing protocol
		{"tcp/80", true},       // Wrong order
		{"80/http", true},      // Invalid protocol
		{"0/tcp", true},        // Port too low
		{"65536/tcp", true},    // Port too high
		{"-1/tcp", true},       // Negative port
		{"abc/tcp", true},      // Non-numeric port
		{"80/tcp/extra", true}, // Extra component
	}

	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			err := validatePortSpec(tt.spec)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// --- init validation tests (no validation to fail — just ensure it doesn't error) ---

func TestManifest_Init_Valid(t *testing.T) {
	trueVal := true
	falseVal := false
	t.Run("true", func(t *testing.T) {
		m := &Manifest{Image: "nginx", Init: &trueVal}
		assert.NoError(t, m.Validate())
	})
	t.Run("false", func(t *testing.T) {
		m := &Manifest{Image: "nginx", Init: &falseVal}
		assert.NoError(t, m.Validate())
	})
	t.Run("nil", func(t *testing.T) {
		m := &Manifest{Image: "nginx"}
		assert.NoError(t, m.Validate())
	})
}

// --- Boundary-normalization contract (Task 1, plan §Task 1.1) ---
//
// These tests lock the post-migration ParsePayload contract:
//   - Returns (*StackManifest, error) always — flat input is auto-wrapped as
//     a 1-service stack named DefaultServiceName ("app").
//   - Stack-format input passes through unchanged.
//
// They will compile/pass only after Task 2 rewrites ParsePayload and adds
// DefaultServiceName. Until then they are the RED that proves the contract.

func TestParsePayload_WrapsFlat(t *testing.T) {
	flat := []byte(`{"image":"nginx:1.25","ports":{"80/tcp":{}}}`)
	sm, err := ParsePayload(flat)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sm == nil || len(sm.Services) != 1 {
		t.Fatalf("expected 1 service, got %+v", sm)
	}
	svc, ok := sm.Services[DefaultServiceName]
	if !ok {
		t.Fatalf("expected service %q, got keys %v", DefaultServiceName, mapKeys(sm.Services))
	}
	if svc.Image != "nginx:1.25" {
		t.Fatalf("image not preserved: %q", svc.Image)
	}
}

func TestParsePayload_StackPassThrough(t *testing.T) {
	stack := []byte(`{"services":{"web":{"image":"nginx:1.25"}}}`)
	sm, err := ParsePayload(stack)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := sm.Services["web"]; !ok {
		t.Fatalf("expected service 'web', got keys %v", mapKeys(sm.Services))
	}
}

// TestDefaultServiceName_MirrorMatches locks in the contract that
// backend.NormalizeProvisionRequest tags unnamed lease items with the
// SAME service name that manifest.ParsePayload wraps flat manifests
// under. The two constants (backend.defaultServiceName and
// manifest.DefaultServiceName) are duplicated to avoid an import
// cycle; this test catches drift via behaviour (no direct constant
// comparison needed, which is why the mirror lives here, not in the
// backend package).
//
// If this test fails, either the backend's auto-tag was changed or
// the manifest's auto-wrap was changed, and the two halves of the
// boundary-normalization contract have separated — legacy
// flat-manifest leases would have items whose ServiceName does NOT
// match the wrapped manifest's only service key, and Task-4
// stack-shape provisioning would fail to find the service.
func TestDefaultServiceName_MirrorMatches(t *testing.T) {
	req := backend.ProvisionRequest{
		LeaseUUID:    "lease-mirror",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
	}
	require.NoError(t, backend.NormalizeProvisionRequest(&req))
	require.Equal(t, DefaultServiceName, req.Items[0].ServiceName,
		"backend.NormalizeProvisionRequest must tag an unnamed item with manifest.DefaultServiceName; "+
			"the duplicated constants have drifted and downstream Task-4 stack provisioning will fail "+
			"to locate the service in the wrapped manifest's Services map")
}

// TestNormalizeAndWrap_OneServiceStackNamedApp_AgainstLegacyItem
// locks the QA-flagged wire-shape edge case: a tenant submits a
// 1-service stack manifest whose service is named exactly "app",
// alongside legacy unnamed lease items. The backend normalizes the
// items (auto-tagging with "app") and ParsePayload passes the stack
// through unchanged. The two halves agree by construction in this
// case — the test pins the agreement so a future change to either
// half can't accidentally desynchronise on this specific shape.
func TestNormalizeAndWrap_OneServiceStackNamedApp_AgainstLegacyItem(t *testing.T) {
	// Wire: 1-service stack named "app".
	payload := []byte(`{"services":{"app":{"image":"nginx:1.25"}}}`)
	sm, err := ParsePayload(payload)
	require.NoError(t, err)
	require.Len(t, sm.Services, 1)
	_, ok := sm.Services[DefaultServiceName]
	require.True(t, ok, "stack with service named 'app' must pass through under DefaultServiceName")

	// Wire: legacy unnamed item.
	req := backend.ProvisionRequest{
		LeaseUUID:    "lease-edge",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
	}
	require.NoError(t, backend.NormalizeProvisionRequest(&req))
	require.Equal(t, DefaultServiceName, req.Items[0].ServiceName)

	// The two halves now reference the same service name, so the
	// downstream stack-shape lookup will succeed:
	_, ok = sm.Services[req.Items[0].ServiceName]
	require.True(t, ok, "legacy item's auto-tagged ServiceName must locate the stack's only service")
}

func TestValidateNoFixedHostPorts(t *testing.T) {
	t.Run("dynamic ports (HostPort 0) are allowed", func(t *testing.T) {
		stack := &StackManifest{
			Services: map[string]*Manifest{
				"web": {Image: "nginx", Ports: map[string]PortConfig{"80/tcp": {HostPort: 0}}},
			},
		}
		assert.NoError(t, ValidateNoFixedHostPorts(stack))
	})

	t.Run("no ports at all is allowed", func(t *testing.T) {
		stack := &StackManifest{
			Services: map[string]*Manifest{"web": {Image: "nginx"}},
		}
		assert.NoError(t, ValidateNoFixedHostPorts(stack))
	})

	t.Run("fixed well-known host port is rejected", func(t *testing.T) {
		stack := &StackManifest{
			Services: map[string]*Manifest{
				"web": {Image: "nginx", Ports: map[string]PortConfig{"443/tcp": {HostPort: 443}}},
			},
		}
		err := ValidateNoFixedHostPorts(stack)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "host_port")
		assert.Contains(t, err.Error(), "web")
		assert.Contains(t, err.Error(), "443/tcp")
	})

	t.Run("fixed high host port is rejected", func(t *testing.T) {
		stack := &StackManifest{
			Services: map[string]*Manifest{
				"web": {Image: "nginx", Ports: map[string]PortConfig{"8080/tcp": {HostPort: 8080}}},
			},
		}
		err := ValidateNoFixedHostPorts(stack)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "host_port")
	})

	t.Run("fixed port in one of many services is rejected", func(t *testing.T) {
		stack := &StackManifest{
			Services: map[string]*Manifest{
				"web": {Image: "nginx", Ports: map[string]PortConfig{"80/tcp": {HostPort: 0}}},
				"db":  {Image: "postgres", Ports: map[string]PortConfig{"5432/tcp": {HostPort: 5432}}},
			},
		}
		err := ValidateNoFixedHostPorts(stack)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db")
	})

	t.Run("reported offender is deterministic across services and ports", func(t *testing.T) {
		// Multiple services and multiple ports within a service each pin a
		// fixed port. The implementation sorts service names then port specs
		// and returns the FIRST offender, so the reported one must be service
		// "alpha" (< "beta") port "443/tcp" (< "9000/tcp") — never "beta" or
		// "9000/tcp". This locks the determinism the sorted iteration provides;
		// a regression to raw map iteration would make this flaky.
		stack := &StackManifest{
			Services: map[string]*Manifest{
				"beta": {Image: "nginx", Ports: map[string]PortConfig{"80/tcp": {HostPort: 8080}}},
				"alpha": {Image: "nginx", Ports: map[string]PortConfig{
					"9000/tcp": {HostPort: 9000},
					"443/tcp":  {HostPort: 443},
				}},
			},
		}
		err := ValidateNoFixedHostPorts(stack)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"alpha"`)
		assert.Contains(t, err.Error(), "443/tcp")
		assert.NotContains(t, err.Error(), "beta")
		assert.NotContains(t, err.Error(), "9000/tcp")
	})
}

func mapKeys[K comparable, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
