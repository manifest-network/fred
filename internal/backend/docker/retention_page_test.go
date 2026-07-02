package docker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func retIDs(rs []backend.RetainedLease) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.LeaseUUID)
	}
	return out
}

func TestListRetentionsPage(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	for _, id := range []string{"c", "a", "e", "b", "d"} {
		require.NoError(t, rs.Put(retentionEntryFixture(id, "tenant-a", time.Now())))
	}
	ctx := context.Background()

	t.Run("first page yields continue = last", func(t *testing.T) {
		page, next, err := b.ListRetentionsPage(ctx, "", 2)
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, retIDs(page))
		assert.Equal(t, "b", next)
	})
	t.Run("resume strictly after continue", func(t *testing.T) {
		page, next, err := b.ListRetentionsPage(ctx, "b", 2)
		require.NoError(t, err)
		assert.Equal(t, []string{"c", "d"}, retIDs(page))
		assert.Equal(t, "d", next)
	})
	t.Run("passthrough when limit<=0 returns all", func(t *testing.T) {
		page, next, err := b.ListRetentionsPage(ctx, "", 0)
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c", "d", "e"}, retIDs(page))
		assert.Empty(t, next)
	})
	t.Run("nil retention store yields non-nil empty", func(t *testing.T) {
		b.retentionStore = nil
		page, next, err := b.ListRetentionsPage(ctx, "", 10)
		require.NoError(t, err)
		assert.NotNil(t, page)
		assert.Empty(t, page)
		assert.Empty(t, next)
	})
}
