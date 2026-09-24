package docker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
)

func TestStopIsBoundedAndLeavesDependenciesOpenWhileWorkerMayRun(t *testing.T) {
	var closeCalls atomic.Int64
	mock := &mockDockerClient{CloseFn: func() error {
		closeCalls.Add(1)
		return nil
	}}
	b := newBackendForTest(mock, nil)
	b.shutdownDrainTimeout = 25 * time.Millisecond

	// Model a mutator which ignored stopCtx cancellation. Stop must return a
	// typed failure instead of hanging forever, but it must not close the Docker
	// client (or durable stores) under the still-running goroutine.
	b.wg.Add(1)
	started := time.Now()
	err := b.Stop()
	require.ErrorIs(t, err, ErrShutdownDrainTimeout)
	assert.Less(t, time.Since(started), time.Second)
	assert.Equal(t, int64(0), closeCalls.Load(),
		"dependencies must remain open until every worker has actually drained")
	assert.Error(t, b.stopCtx.Err(), "Stop must signal cancellation before waiting")

	// Stop installs only one waiter. Once the worker returns, a retry observes
	// the same closed drain channel and performs the normal resource close.
	b.wg.Done()
	require.Eventually(t, func() bool {
		select {
		case <-b.shutdownWaitDone:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.NoError(t, b.Stop())
	assert.Equal(t, int64(1), closeCalls.Load())
}

func TestStopOwnsLoaderDrainBeforeClosingDependencies(t *testing.T) {
	server := httptest.NewTLSServer(registry.New())
	defer server.Close()
	ref, err := name.NewTag(strings.TrimPrefix(server.URL, "https://") + "/shutdown:latest")
	require.NoError(t, err)
	require.NoError(t, remote.Write(ref, imageCapacityRegistryImage(t, "shutdown-owned image"),
		remote.WithContext(t.Context()), remote.WithAuth(authn.Anonymous), remote.WithTransport(server.Client().Transport)))
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	for _, scenario := range []string{"completes after 75 seconds", "deadline while SDK still unwinds"} {
		t.Run(scenario, func(t *testing.T) {
			stage := t.TempDir()
			synctest.Test(t, func(t *testing.T) {
				var closeCalls atomic.Int64
				b := newBackendForTest(&mockDockerClient{CloseFn: func() error {
					closeCalls.Add(1)
					return nil
				}}, nil)
				arrivals := make(chan context.Context, 1)
				completed, unwound := make(chan struct{}), make(chan struct{})
				complete := sync.OnceFunc(func() { close(completed) })
				unwind := sync.OnceFunc(func() { close(unwound) })
				loader, err := imagefetch.NewLoader(imageCapacityImporter(func(ctx context.Context, input io.Reader) (image.LoadResponse, error) {
					if _, err := io.Copy(io.Discard, input); err != nil {
						return image.LoadResponse{}, err
					}
					arrivals <- ctx
					select {
					case <-completed:
						return image.LoadResponse{Body: io.NopCloser(strings.NewReader(`{"stream":"Loaded image"}`)), JSON: true}, nil
					case <-ctx.Done():
						// A canceled SDK call can still own client/journal resources
						// while its transport unwinds. Model that separately.
						<-unwound
						return image.LoadResponse{}, ctx.Err()
					}
				}), stage, 1<<20, imagefetch.WithRegistryTransport(transport))
				require.NoError(t, err)
				b.imageCapacity = &imageCapacityManager{loader: loader}
				prepared, err := loader.Prepare(t.Context(), ref.Name(), ocispec.Platform{OS: "linux", Architecture: "amd64"})
				require.NoError(t, err)
				defer prepared.Close()
				defer complete()
				defer unwind()
				// The loader owns this exchange itself, so Stop must drain it
				// even without a separate worker WaitGroup registration.
				imported := make(chan error, 1)
				go func() { _, err := loader.Import(b.stopCtx, prepared); imported <- err }()
				work := <-arrivals
				stopped := make(chan error, 1)
				go func() { stopped <- b.Stop() }()
				synctest.Wait()
				require.ErrorIs(t, b.stopCtx.Err(), context.Canceled)
				require.NoError(t, work.Err(), "Stop must preserve the owned import until its drain deadline")
				require.Zero(t, closeCalls.Load(), "Docker must remain open while an import owns it")
				if scenario == "completes after 75 seconds" {
					time.Sleep(75 * time.Second)
					require.NoError(t, work.Err())
					complete()
					require.NoError(t, <-imported)
					require.NoError(t, <-stopped)
					pending, err := loader.PendingBytes()
					require.NoError(t, err)
					require.Zero(t, pending)
				} else {
					time.Sleep(90 * time.Second)
					synctest.Wait()
					require.ErrorIs(t, <-stopped, ErrShutdownDrainTimeout)
					require.ErrorIs(t, work.Err(), context.Canceled, "the backend deadline must cancel the loader's admitted exchanges")
					require.Zero(t, closeCalls.Load(), "deadline expiry must not close dependencies while the SDK still unwinds")
					unwind()
					require.ErrorIs(t, <-imported, context.Canceled)
					synctest.Wait()
					require.NoError(t, b.Stop(), "a later stop can close only after actual ownership drains")
					pending, err := loader.PendingBytes()
					require.NoError(t, err)
					require.Equal(t, prepared.ImportBytes(), pending, "canceling an incomplete exchange cannot erase its durable debit")
				}
				require.Equal(t, int64(1), closeCalls.Load())
			})
		})
	}
}
