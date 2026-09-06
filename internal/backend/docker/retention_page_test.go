package docker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

const (
	retentionPageA = "0192f1a0-5000-4abc-8def-000000000001"
	retentionPageB = "0192f1a0-5000-4abc-8def-000000000002"
	retentionPageC = "0192f1a0-5000-4abc-8def-000000000003"
	retentionPageD = "0192f1a0-5000-4abc-8def-000000000004"
	retentionPageE = "0192f1a0-5000-4abc-8def-000000000005"
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
	for _, id := range []string{retentionPageC, retentionPageA, retentionPageE, retentionPageB, retentionPageD} {
		require.NoError(t, putRetentionForTest(t, rs, retentionEntryFixture(id, "tenant-a", time.Now())))
	}
	ctx := context.Background()

	t.Run("first page yields continue = last", func(t *testing.T) {
		page, next, err := b.ListRetentionsPage(ctx, "", 2)
		require.NoError(t, err)
		assert.Equal(t, []string{retentionPageA, retentionPageB}, retIDs(page))
		for _, retained := range page {
			assert.Equal(t, nominalDockerProviderUUID, retained.ProviderUUID)
			assert.Equal(t, "tenant-a", retained.Tenant)
		}
		assert.Equal(t, retentionPageB, next)
	})
	t.Run("resume strictly after continue", func(t *testing.T) {
		page, next, err := b.ListRetentionsPage(ctx, retentionPageB, 2)
		require.NoError(t, err)
		assert.Equal(t, []string{retentionPageC, retentionPageD}, retIDs(page))
		assert.Equal(t, retentionPageD, next)
	})
	t.Run("passthrough when limit<=0 returns all", func(t *testing.T) {
		page, next, err := b.ListRetentionsPage(ctx, "", 0)
		require.NoError(t, err)
		assert.Equal(t, []string{retentionPageA, retentionPageB, retentionPageC, retentionPageD, retentionPageE}, retIDs(page))
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
