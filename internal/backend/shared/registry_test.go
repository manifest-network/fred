package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRegistry(t *testing.T) {
	tests := []struct {
		image    string
		expected string
	}{
		// Docker Hub official images
		{"nginx", "docker.io"},
		{"nginx:latest", "docker.io"},
		{"nginx:1.25-alpine", "docker.io"},
		{"library/nginx", "docker.io"},
		{"library/nginx:latest", "docker.io"},

		// Docker Hub user images
		{"myorg/myapp", "docker.io"},
		{"myorg/myapp:v1", "docker.io"},
		{"myorg/myapp:v1.2.3", "docker.io"},

		// Other registries
		{"ghcr.io/org/app", "ghcr.io"},
		{"ghcr.io/org/app:latest", "ghcr.io"},
		{"gcr.io/project/image", "gcr.io"},
		{"gcr.io/project/image:tag", "gcr.io"},
		{"registry.example.com/image", "registry.example.com"},
		{"registry.example.com/org/image:tag", "registry.example.com"},
		{"registry.example.com:5000/image", "registry.example.com:5000"},

		// Localhost
		{"localhost/image", "localhost"},
		{"localhost:5000/image", "localhost:5000"},

		// With digests (valid sha256 hex)
		{"nginx@sha256:e4c58958181a5925816faa528ce959e487632f4cfd42f9bc0fb0d8d696503c46", "docker.io"},
		{"ghcr.io/org/app@sha256:e4c58958181a5925816faa528ce959e487632f4cfd42f9bc0fb0d8d696503c46", "ghcr.io"},
	}

	for _, tt := range tests {
		t.Run(tt.image, func(t *testing.T) {
			result, err := ParseRegistry(tt.image)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseRegistry_Invalid(t *testing.T) {
	tests := []string{
		"",
		":latest",
		"/image",
		"nginx@sha256:tooshort",
	}

	for _, image := range tests {
		t.Run(image, func(t *testing.T) {
			_, err := ParseRegistry(image)
			assert.Error(t, err)
		})
	}
}

func TestIsImageAllowed(t *testing.T) {
	allowed := []string{"docker.io", "ghcr.io"}

	tests := []struct {
		image    string
		expected bool
	}{
		{"nginx", true},
		{"nginx:latest", true},
		{"ghcr.io/org/app", true},
		{"gcr.io/project/image", false},
		{"registry.example.com/image", false},
	}

	for _, tt := range tests {
		t.Run(tt.image, func(t *testing.T) {
			result := IsImageAllowed(tt.image, allowed)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateImage(t *testing.T) {
	allowed := []string{"docker.io", "ghcr.io"}

	tests := []struct {
		name      string
		image     string
		expectErr bool
	}{
		{"valid docker hub", "nginx:latest", false},
		{"valid ghcr", "ghcr.io/org/app", false},
		{"invalid registry", "gcr.io/project/image", true},
		{"empty image", "", true},
		{"invalid format colon prefix", ":latest", true},
		{"invalid format slash prefix", "/image", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateImage(tt.image, allowed)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
