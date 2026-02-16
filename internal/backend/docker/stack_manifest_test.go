package docker

import (
	"strings"
	"testing"

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
