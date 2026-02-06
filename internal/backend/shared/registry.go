package shared

import (
	// Ensure sha256 is registered for digest parsing.
	_ "crypto/sha256"
	"fmt"
	"slices"

	"github.com/distribution/reference"

	"github.com/manifest-network/fred/internal/backend"
)

// ParseRegistry extracts the registry domain from a container image reference.
// Uses the distribution/reference library for robust parsing with Docker Hub
// normalization (e.g., "nginx" -> "docker.io").
func ParseRegistry(image string) (string, error) {
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return "", fmt.Errorf("invalid image reference %q: %w", image, err)
	}
	return reference.Domain(named), nil
}

// IsImageAllowed checks if an image is from an allowed registry.
func IsImageAllowed(image string, allowedRegistries []string) bool {
	registry, err := ParseRegistry(image)
	if err != nil {
		return false
	}
	return slices.Contains(allowedRegistries, registry)
}

// ValidateImage checks if an image reference is valid and allowed.
func ValidateImage(image string, allowedRegistries []string) error {
	if image == "" {
		return fmt.Errorf("image is required")
	}

	registry, err := ParseRegistry(image)
	if err != nil {
		return err
	}

	if !slices.Contains(allowedRegistries, registry) {
		return fmt.Errorf("%w: registry %q; allowed registries: %v",
			backend.ErrImageNotAllowed, registry, allowedRegistries)
	}

	return nil
}
