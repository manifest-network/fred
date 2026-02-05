package docker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLabelMeta(t *testing.T) {
	t.Run("all labels present and valid", func(t *testing.T) {
		labels := map[string]string{
			LabelInstanceIndex: "3",
			LabelFailCount:     "2",
			LabelCreatedAt:     "2025-01-15T10:30:00Z",
		}
		meta, err := parseLabelMeta(labels)
		require.NoError(t, err)
		assert.Equal(t, 3, meta.InstanceIndex)
		assert.Equal(t, 2, meta.FailCount)
		assert.Equal(t, time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC), meta.CreatedAt)
	})

	t.Run("empty labels returns zero values", func(t *testing.T) {
		meta, err := parseLabelMeta(map[string]string{})
		require.NoError(t, err)
		assert.Equal(t, 0, meta.InstanceIndex)
		assert.Equal(t, 0, meta.FailCount)
		assert.True(t, meta.CreatedAt.IsZero())
	})

	t.Run("nil labels returns zero values", func(t *testing.T) {
		meta, err := parseLabelMeta(nil)
		require.NoError(t, err)
		assert.Equal(t, 0, meta.InstanceIndex)
		assert.Equal(t, 0, meta.FailCount)
		assert.True(t, meta.CreatedAt.IsZero())
	})

	t.Run("malformed instance_index returns error", func(t *testing.T) {
		labels := map[string]string{
			LabelInstanceIndex: "not-a-number",
		}
		_, err := parseLabelMeta(labels)
		require.Error(t, err)
		assert.Contains(t, err.Error(), LabelInstanceIndex)
		assert.Contains(t, err.Error(), "not-a-number")
	})

	t.Run("malformed fail_count returns error", func(t *testing.T) {
		labels := map[string]string{
			LabelFailCount: "abc",
		}
		_, err := parseLabelMeta(labels)
		require.Error(t, err)
		assert.Contains(t, err.Error(), LabelFailCount)
		assert.Contains(t, err.Error(), "abc")
	})

	t.Run("malformed created_at returns error", func(t *testing.T) {
		labels := map[string]string{
			LabelCreatedAt: "not-a-timestamp",
		}
		_, err := parseLabelMeta(labels)
		require.Error(t, err)
		assert.Contains(t, err.Error(), LabelCreatedAt)
		assert.Contains(t, err.Error(), "not-a-timestamp")
	})

	t.Run("partial labels are valid", func(t *testing.T) {
		labels := map[string]string{
			LabelInstanceIndex: "5",
			// No FailCount or CreatedAt
		}
		meta, err := parseLabelMeta(labels)
		require.NoError(t, err)
		assert.Equal(t, 5, meta.InstanceIndex)
		assert.Equal(t, 0, meta.FailCount)
		assert.True(t, meta.CreatedAt.IsZero())
	})

	t.Run("zero values are valid", func(t *testing.T) {
		labels := map[string]string{
			LabelInstanceIndex: "0",
			LabelFailCount:     "0",
		}
		meta, err := parseLabelMeta(labels)
		require.NoError(t, err)
		assert.Equal(t, 0, meta.InstanceIndex)
		assert.Equal(t, 0, meta.FailCount)
	})
}
