package docker

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestListRetentionsPageDuringRestoreFinalization(t *testing.T) {
	b, store := newBackendWithRetention(t)
	const sources = 24
	proofs := make([]shared.RestoringRetentionProof, 0, sources)
	for i := range sources {
		source := fmt.Sprintf("0192f1a0-5000-4abc-8def-%012d", i+1)
		entry := retentionEntryFixture(source, "tenant-a", time.Now())
		// Nontrivial valid manifests exercise value decoding while concurrent
		// restore finalizers consume later rows in the ordered inventory.
		entry.StackManifest.Services["web"].Env = map[string]string{"PAYLOAD": strings.Repeat("x", 8192)}
		entry.Status = shared.RetentionStatusRestoring
		entry.NewLeaseUUID = canonicalRetentionFixtureUUID("page-destination-" + source)
		stored := putRestoringRetention(t, store, entry)
		proof, err := store.ProveRestoringSnapshot(*stored)
		require.NoError(t, err)
		proofs = append(proofs, proof)
	}
	start := make(chan struct{})
	done := make(chan struct{})
	readErrors := make(chan error, 2)
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			<-start
			for {
				page, next, err := b.ListRetentionsPage(t.Context(), "", backend.MaxPageLimit)
				if err != nil {
					readErrors <- err
					return
				}
				if next != "" {
					readErrors <- fmt.Errorf("bounded fixture unexpectedly returned continuation %q", next)
					return
				}
				for i, retained := range page {
					if retained.Tenant != "tenant-a" || retained.ProviderUUID != nominalDockerProviderUUID ||
						i > 0 && page[i-1].LeaseUUID >= retained.LeaseUUID {
						readErrors <- fmt.Errorf("page lost exact identity or ordering: %+v", retained)
						return
					}
				}
				select {
				case <-done:
					return
				default:
				}
			}
		})
	}
	close(start)
	for i := len(proofs) - 1; i >= 0; i-- {
		deleted, err := store.DeleteRestoring(proofs[i])
		require.NoError(t, err)
		require.True(t, deleted)
	}
	close(done)
	readers.Wait()
	close(readErrors)
	for err := range readErrors {
		require.NoError(t, err, "ordinary typed finalization must not invalidate another snapshot's page")
	}
	page, next, err := b.ListRetentionsPage(t.Context(), "", backend.MaxPageLimit)
	require.NoError(t, err)
	require.NotNil(t, page)
	require.Empty(t, page)
	require.Empty(t, next)
}
