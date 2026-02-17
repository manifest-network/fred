package docker

import (
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
				Services: map[string]*DockerManifest{
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
		{"Web", "must match"},            // uppercase
		{"my_db", "must match"},          // underscore
		{"-web", "must match"},           // leading dash
		{"web-", "must match"},           // trailing dash
		{"my.svc", "must match"},         // dot
		{strings.Repeat("a", 64), "exceeds 63 characters"}, // too long
		{"web@1", "must match"},          // special char
	}
	for _, tc := range invalid {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			sm := StackManifest{
				Services: map[string]*DockerManifest{
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
	sm := StackManifest{Services: map[string]*DockerManifest{}}
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
		Services: map[string]*DockerManifest{
			"web": nil,
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil manifest")
}

func TestStackManifest_Validate_EmptyServiceName(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*DockerManifest{
			"": {Image: "nginx"},
		},
	}
	err := sm.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestStackManifest_Validate_InvalidServiceManifest(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*DockerManifest{
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
			Services: map[string]*DockerManifest{
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
			Services: map[string]*DockerManifest{
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
			Services: map[string]*DockerManifest{
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
			Services: map[string]*DockerManifest{
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
	t.Run("single manifest", func(t *testing.T) {
		data := validManifestJSON("nginx:latest")
		m, s, err := ParsePayload(data)
		require.NoError(t, err)
		assert.NotNil(t, m)
		assert.Nil(t, s)
		assert.Equal(t, "nginx:latest", m.Image)
	})

	t.Run("stack manifest", func(t *testing.T) {
		data := validStackManifestJSON(map[string]string{
			"web": "nginx:latest",
			"db":  "postgres:16",
		})
		m, s, err := ParsePayload(data)
		require.NoError(t, err)
		assert.Nil(t, m)
		require.NotNil(t, s)
		assert.Len(t, s.Services, 2)
		assert.Equal(t, "nginx:latest", s.Services["web"].Image)
		assert.Equal(t, "postgres:16", s.Services["db"].Image)
	})

	t.Run("empty payload", func(t *testing.T) {
		_, _, err := ParsePayload(nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})

	t.Run("invalid JSON", func(t *testing.T) {
		_, _, err := ParsePayload([]byte("{invalid"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid payload JSON")
	})

	t.Run("stack with invalid service name", func(t *testing.T) {
		data := validStackManifestJSON(map[string]string{
			"Web": "nginx:latest", // uppercase — fails service name regex
		})
		_, _, err := ParsePayload(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must match")
	})
}

// --- depends_on validation tests ---

func TestStackManifest_DependsOn_ValidServiceStarted(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*DockerManifest{
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
		Services: map[string]*DockerManifest{
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
		Services: map[string]*DockerManifest{
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
		Services: map[string]*DockerManifest{
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
		Services: map[string]*DockerManifest{
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
		Services: map[string]*DockerManifest{
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
		Services: map[string]*DockerManifest{
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
		Services: map[string]*DockerManifest{
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

func TestStackManifest_DependsOn_MultipleDependencies(t *testing.T) {
	sm := StackManifest{
		Services: map[string]*DockerManifest{
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
		Services: map[string]*DockerManifest{
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
		Services: map[string]*DockerManifest{
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
		Services: map[string]*DockerManifest{
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

func TestDockerManifest_DependsOn_RejectedOutsideStack(t *testing.T) {
	m := &DockerManifest{
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

func TestDockerManifest_StopGracePeriod_Valid(t *testing.T) {
	d := Duration(10 * time.Second)
	m := &DockerManifest{
		Image:           "nginx",
		StopGracePeriod: &d,
	}
	assert.NoError(t, m.Validate())
}

func TestDockerManifest_StopGracePeriod_TooLow(t *testing.T) {
	d := Duration(500 * time.Millisecond)
	m := &DockerManifest{
		Image:           "nginx",
		StopGracePeriod: &d,
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least 1s")
}

func TestDockerManifest_StopGracePeriod_TooHigh(t *testing.T) {
	d := Duration(121 * time.Second)
	m := &DockerManifest{
		Image:           "nginx",
		StopGracePeriod: &d,
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most 120s")
}

func TestDockerManifest_StopGracePeriod_Boundaries(t *testing.T) {
	t.Run("exactly 1s", func(t *testing.T) {
		d := Duration(time.Second)
		m := &DockerManifest{Image: "nginx", StopGracePeriod: &d}
		assert.NoError(t, m.Validate())
	})
	t.Run("exactly 120s", func(t *testing.T) {
		d := Duration(120 * time.Second)
		m := &DockerManifest{Image: "nginx", StopGracePeriod: &d}
		assert.NoError(t, m.Validate())
	})
}

// --- expose validation tests ---

func TestDockerManifest_Expose_Valid(t *testing.T) {
	m := &DockerManifest{
		Image:  "nginx",
		Expose: []string{"3000", "8080"},
	}
	assert.NoError(t, m.Validate())
}

func TestDockerManifest_Expose_InvalidPort(t *testing.T) {
	m := &DockerManifest{
		Image:  "nginx",
		Expose: []string{"abc"},
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid port number")
}

func TestDockerManifest_Expose_Duplicate(t *testing.T) {
	m := &DockerManifest{
		Image:  "nginx",
		Expose: []string{"3000", "3000"},
	}
	err := m.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate port")
}

func TestDockerManifest_Expose_OutOfRange(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		m := &DockerManifest{Image: "nginx", Expose: []string{"0"}}
		err := m.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "between 1 and 65535")
	})
	t.Run("too high", func(t *testing.T) {
		m := &DockerManifest{Image: "nginx", Expose: []string{"65536"}}
		err := m.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "between 1 and 65535")
	})
}

// --- init validation tests (no validation to fail — just ensure it doesn't error) ---

func TestDockerManifest_Init_Valid(t *testing.T) {
	trueVal := true
	falseVal := false
	t.Run("true", func(t *testing.T) {
		m := &DockerManifest{Image: "nginx", Init: &trueVal}
		assert.NoError(t, m.Validate())
	})
	t.Run("false", func(t *testing.T) {
		m := &DockerManifest{Image: "nginx", Init: &falseVal}
		assert.NoError(t, m.Validate())
	})
	t.Run("nil", func(t *testing.T) {
		m := &DockerManifest{Image: "nginx"}
		assert.NoError(t, m.Validate())
	})
}
