package docker

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/manifest-network/fred/internal/backend"
)

// managedVolumeName is an exact, single-component identity in fred's managed
// volume namespace. A value can only be obtained from parseManagedVolumeName,
// keeping request- and store-derived strings out of filesystem and dataset
// mutation sinks until their complete on-disk grammar has been proved.
//
// Both live and retained names carry a canonical lease UUID. The trailing
// shape is either the current service-aware form ({service}-{index}) or the
// v0.13 migration form ({index}); the latter can still be present on a stopped
// host during an upgrade even though current code never creates it.
type managedVolumeName storagePathComponent

func parseManagedVolumeName(value string) (managedVolumeName, error) {
	component, err := parseStoragePathComponent(value)
	if err != nil {
		return "", fmt.Errorf("managed volume name: %w", err)
	}

	remainder := value
	switch {
	case strings.HasPrefix(remainder, retainedVolumePrefix):
		remainder = strings.TrimPrefix(remainder, retainedVolumePrefix)
	case strings.HasPrefix(remainder, volumePrefix):
		remainder = strings.TrimPrefix(remainder, volumePrefix)
	default:
		return "", fmt.Errorf("managed volume name %q is outside the managed namespace", value)
	}

	// A canonical UUID is 36 bytes and is followed by the volume-shape dash.
	if len(remainder) <= 37 || remainder[36] != '-' {
		return "", fmt.Errorf("managed volume name %q has no canonical lease identity", value)
	}
	leaseUUID, suffix := remainder[:36], remainder[37:]
	if !backend.IsCanonicalLeaseUUID(leaseUUID) {
		return "", fmt.Errorf("managed volume name %q has invalid lease UUID %q", value, leaseUUID)
	}

	// v0.13 names end directly in the instance index. Current names include a
	// DNS-label-safe service first. Parse from the right because service names
	// may themselves contain dashes.
	serviceName, indexText := "", suffix
	if dash := strings.LastIndexByte(suffix, '-'); dash >= 0 {
		serviceName, indexText = suffix[:dash], suffix[dash+1:]
		if !isManagedVolumeServiceName(serviceName) {
			return "", fmt.Errorf("managed volume name %q has invalid service name %q", value, serviceName)
		}
	}
	if !isManagedVolumeInstanceIndex(indexText) {
		return "", fmt.Errorf("managed volume name %q has invalid instance index %q", value, indexText)
	}
	return managedVolumeName(component), nil
}

func isManagedVolumeServiceName(value string) bool {
	if len(value) == 0 || len(value) > 63 || !isManagedVolumeAlphaNumeric(value[0]) ||
		!isManagedVolumeAlphaNumeric(value[len(value)-1]) {
		return false
	}
	for i := 1; i < len(value)-1; i++ {
		if value[i] != '-' && !isManagedVolumeAlphaNumeric(value[i]) {
			return false
		}
	}
	return true
}

func isManagedVolumeAlphaNumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}

func isManagedVolumeInstanceIndex(value string) bool {
	index, err := strconv.Atoi(value)
	return err == nil && index >= 0 && strconv.Itoa(index) == value
}

func (n managedVolumeName) value() string { return string(n) }

func (n managedVolumeName) hostPath(rootPath string) string {
	return filepath.Join(rootPath, n.value())
}

// rejectedManagedVolumeHostPath is the fail-closed result for HostPath's
// legacy error-less interface. Invalid input maps to a deterministic reserved
// child of the configured root, never to a caller-selected path or the root
// itself. Mutating methods reject the input before reaching this fallback.
func rejectedManagedVolumeHostPath(rootPath, value string) string {
	digest := sha256.Sum256([]byte(value))
	return filepath.Join(rootPath, fmt.Sprintf(".fred-rejected-volume-%x", digest))
}
