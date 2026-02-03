package docker

import (
	"fmt"
	"slices"
	"strings"
)

// ParseRegistry extracts the registry from a container image reference.
// Examples:
//   - "nginx" -> "docker.io"
//   - "nginx:latest" -> "docker.io"
//   - "library/nginx" -> "docker.io"
//   - "myorg/myapp:v1" -> "docker.io"
//   - "ghcr.io/org/app:latest" -> "ghcr.io"
//   - "gcr.io/project/image" -> "gcr.io"
//   - "registry.example.com/image" -> "registry.example.com"
//   - "registry.example.com:5000/image" -> "registry.example.com"
//   - "localhost:5000/image" -> "localhost"
func ParseRegistry(image string) string {
	// Remove digest first
	image = strings.Split(image, "@")[0]

	// Split by slash to get potential registry part
	parts := strings.SplitN(image, "/", 2)

	if len(parts) == 1 {
		// No slash: official image like "nginx" or "nginx:latest"
		return "docker.io"
	}

	// Check if first part looks like a registry
	// Registries have: a dot, a colon (port), OR are "localhost"
	first := parts[0]
	if strings.Contains(first, ".") || strings.Contains(first, ":") || first == "localhost" {
		// Strip port if present
		host := strings.Split(first, ":")[0]
		return host
	}

	// No dot/colon and not localhost: it's a Docker Hub user/org
	// e.g., "myorg/myapp" -> "docker.io"
	return "docker.io"
}

// IsImageAllowed checks if an image is from an allowed registry.
func IsImageAllowed(image string, allowedRegistries []string) bool {
	registry := ParseRegistry(image)
	return slices.Contains(allowedRegistries, registry)
}

// ValidateImage checks if an image reference is valid and allowed.
func ValidateImage(image string, allowedRegistries []string) error {
	if image == "" {
		return fmt.Errorf("image is required")
	}

	// Basic format validation
	if strings.HasPrefix(image, ":") || strings.HasPrefix(image, "/") {
		return fmt.Errorf("invalid image format")
	}

	registry := ParseRegistry(image)
	if !slices.Contains(allowedRegistries, registry) {
		return fmt.Errorf("image from registry %q is not allowed; allowed registries: %v",
			registry, allowedRegistries)
	}

	return nil
}
