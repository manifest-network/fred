package imageexec

import (
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// Image metadata is outside the tenant manifest's HTTP body budget. These
// limits apply before an Image can authorize helpers or workload creation.
const (
	MaxImageVolumes         = 16
	MaxImageVolumePathBytes = 4096
	MaxImageVolumeBytes     = 16 << 10
	MaxImageLabels          = 128
	MaxImageLabelBytes      = 64 << 10
	MaxInspectResponseBytes = 2 << 20
)

type admittedImageMetadata struct {
	volumes     []string
	buildLabels composeBuildLabels
}

// Metadata is image configuration admitted by the same policy used by the
// execution boundary. Its zero value carries no admission evidence.
type Metadata struct{ record *admittedImageMetadata }

// AdmitMetadata validates registry configuration before it can reach Docker,
// and is also used when inspecting a local execution identity.
func AdmitMetadata(labels map[string]string, volumes map[string]struct{}) (Metadata, error) {
	admitted, err := admitImageMetadata(labels, volumes)
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{record: &admitted}, nil
}

// Valid reports whether this metadata passed admission.
func (m Metadata) Valid() bool { return m.record != nil }

// Volumes returns independent storage for the canonical admitted volume paths.
func (m Metadata) Volumes() []string {
	if m.record == nil {
		return nil
	}
	return slices.Clone(m.record.volumes)
}

func admitImageMetadata(labels map[string]string, volumes map[string]struct{}) (admittedImageMetadata, error) {
	buildLabels := composeBuildLabels{}
	if len(labels) > MaxImageLabels {
		return admittedImageMetadata{}, fmt.Errorf("image has more than %d labels", MaxImageLabels)
	}
	labelBytes := 0
	for key, value := range labels {
		if len(key) > MaxImageLabelBytes-labelBytes {
			return admittedImageMetadata{}, fmt.Errorf("image label metadata exceeds %d bytes", MaxImageLabelBytes)
		}
		labelBytes += len(key)
		if len(value) > MaxImageLabelBytes-labelBytes {
			return admittedImageMetadata{}, fmt.Errorf("image label metadata exceeds %d bytes", MaxImageLabelBytes)
		}
		labelBytes += len(value)
	}
	for _, key := range slices.Sorted(maps.Keys(labels)) {
		if manifest.IsReservedLabelKey(key) && !buildLabels.owns(key) {
			return admittedImageMetadata{}, fmt.Errorf("image contains reserved label %q", key)
		}
	}
	if len(volumes) > MaxImageVolumes {
		return admittedImageMetadata{}, fmt.Errorf("image has more than %d VOLUME targets", MaxImageVolumes)
	}
	volumeBytes := 0
	for target := range volumes {
		if len(target) > MaxImageVolumePathBytes || len(target) > MaxImageVolumeBytes-volumeBytes {
			return admittedImageMetadata{}, fmt.Errorf("image VOLUME metadata exceeds its path byte budget")
		}
		volumeBytes += len(target)
		if !utf8.ValidString(target) || strings.ContainsRune(target, 0) || !path.IsAbs(target) || path.Clean(target) != target {
			return admittedImageMetadata{}, fmt.Errorf("image VOLUME target must be a canonical absolute path: %q", target)
		}
		if target == "/" || target == "/tmp" || target == "/run" {
			return admittedImageMetadata{}, fmt.Errorf("image VOLUME target %q masks a backend-managed root", target)
		}
		for _, reserved := range []string{"/proc", "/sys", "/dev", "/_wp"} {
			if target == reserved || strings.HasPrefix(target, reserved+"/") {
				return admittedImageMetadata{}, fmt.Errorf("image VOLUME target %q overlaps reserved path %q", target, reserved)
			}
		}
	}
	ordered := slices.Sorted(maps.Keys(volumes))
	seen := make(map[string]struct{}, len(ordered))
	for _, target := range ordered {
		for parent := path.Dir(target); parent != "/"; parent = path.Dir(parent) {
			if _, exists := seen[parent]; exists {
				return admittedImageMetadata{}, fmt.Errorf("image VOLUME targets %q and %q overlap", parent, target)
			}
		}
		seen[target] = struct{}{}
	}
	return admittedImageMetadata{volumes: ordered, buildLabels: buildLabels}, nil
}
