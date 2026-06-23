package docker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVolumeRootUnverifiable(t *testing.T) {
	// present root → verifiable (do not skip)
	assert.False(t, volumeRootUnverifiable(true, nil))
	// absent root (pathExists → false,nil) → unverifiable (skip)
	assert.True(t, volumeRootUnverifiable(false, nil))
	// unreadable root (non-ENOENT stat error → false,err) → unverifiable (skip).
	// This pins the branch so a future "IsNotExist-only" simplification fails.
	assert.True(t, volumeRootUnverifiable(false, errors.New("permission denied")))
}

func TestAllVolumesAbsent(t *testing.T) {
	present := map[string]bool{"fred-retained-u1-app-0": true}
	// every name present → not absent
	assert.False(t, allVolumesAbsent([]string{"fred-retained-u1-app-0"}, present))
	// a name missing → absent
	assert.True(t, allVolumesAbsent([]string{"fred-retained-u2-app-0"}, present))
	// mixed (one present) → not absent
	assert.False(t, allVolumesAbsent([]string{"fred-retained-u1-app-0", "fred-retained-u2-app-0"}, present))
	// empty name set → vacuously absent
	assert.True(t, allVolumesAbsent(nil, present))
}
