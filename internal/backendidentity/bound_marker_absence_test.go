package backendidentity

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoundMarkerPairVerifyAbsentIsReadOnly(t *testing.T) {
	for _, existing := range []string{"", "primary", "anchor"} {
		t.Run(existing, func(t *testing.T) {
			dir := t.TempDir()
			primary := filepath.Join(dir, "primary.json")
			anchor := filepath.Join(dir, "anchor.json")
			pair, err := BindMarkerPair(primary, anchor)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, pair.Close()) })

			var existingPath string
			switch existing {
			case "primary":
				existingPath = primary
			case "anchor":
				existingPath = anchor
			}
			if existingPath != "" {
				require.NoError(t, os.WriteFile(existingPath, []byte("opaque-existing-marker"), 0o600))
			}

			err = pair.VerifyAbsent()
			if existing == "" {
				require.NoError(t, err)
				assert.NoFileExists(t, primary)
				assert.NoFileExists(t, anchor)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrMarkerBindingMismatch)
			assert.ErrorContains(t, err, existing+" marker already exists")
			contents, readErr := os.ReadFile(existingPath)
			require.NoError(t, readErr)
			assert.Equal(t, []byte("opaque-existing-marker"), contents)
		})
	}
}
