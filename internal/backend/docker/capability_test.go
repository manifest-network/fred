package docker

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCapGuardResult pins the fatal fail-fast DECISION (ENG-454) without needing
// any privilege: a definite "cannot set quotas" must abort startup with an
// actionable error; an inconclusive probe must warn + proceed; "can set" is nil.
// This is the guard's headline behavior — the fire-on-absence path that the
// integration tests (which run as root, and so always "can set") never exercise.
func TestCapGuardResult(t *testing.T) {
	log := slog.Default()

	// Cannot set quotas → fatal, actionable error naming the backend + the fix.
	err := capGuardResult("xfs", false, nil, log)
	require.Error(t, err, "no privilege must be a fatal startup error")
	assert.Contains(t, err.Error(), "xfs")
	assert.Contains(t, err.Error(), "CAP_SYS_ADMIN")
	assert.Contains(t, err.Error(), "AmbientCapabilities")

	// Can set quotas → nil.
	require.NoError(t, capGuardResult("btrfs", true, nil, log))

	// Inconclusive probe (prctl error) → warn + proceed, never fatal.
	require.NoError(t, capGuardResult("btrfs", false, errors.New("prctl boom"), log),
		"an inconclusive probe must not brick startup")
}
