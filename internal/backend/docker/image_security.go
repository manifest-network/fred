package docker

import (
	"fmt"

	"github.com/opencontainers/go-digest"
)

func isImmutableImageID(imageID string) bool {
	parsed, err := digest.Parse(imageID)
	return err == nil && parsed.Algorithm() == digest.SHA256
}

// containerImageReference preserves the original manifest reference for durable
// release comparisons while containers execute the exact inspected image ID.
// Both labels must be bound to Docker's actual immutable image identity. An
// image cannot supply this binding through inherited labels: its config digest
// includes those labels, so embedding its own digest would require a SHA-256
// fixed point. New image admission also rejects every reserved label.
func containerImageReference(configImage, imageID string, labels map[string]string) (string, error) {
	reference, hasReference := labels[LabelImageReference]
	boundID, hasID := labels[LabelImageID]
	if !hasReference && !hasID {
		return configImage, nil // Containers created before immutable-image binding.
	}
	if !hasReference || !hasID || reference == "" || !isImmutableImageID(imageID) ||
		configImage != imageID || boundID != imageID {
		return "", fmt.Errorf("container has an invalid immutable image-reference binding")
	}
	return reference, nil
}
