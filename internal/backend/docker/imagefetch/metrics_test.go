package imagefetch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type interruptedMetricImporter struct {
	dispatched chan context.Context
	unwind     <-chan struct{}
}

func (d interruptedMetricImporter) ImageLoad(ctx context.Context, input io.Reader, _ ...client.ImageLoadOption) (image.LoadResponse, error) {
	if _, err := io.Copy(io.Discard, input); err != nil {
		return image.LoadResponse{}, err
	}
	d.dispatched <- ctx
	<-ctx.Done()
	<-d.unwind
	return image.LoadResponse{}, ctx.Err()
}

func TestImportOutcomeCountsOwnedInterruptionBeforeSDKUnwinds(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("metric content")))
	transport := f.server.Client().Transport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	for _, outcome := range []string{"deadline", "shutdown"} {
		t.Run(outcome, func(t *testing.T) {
			stage := t.TempDir()
			synctest.Test(t, func(t *testing.T) {
				before := testutil.ToFloat64(imageImportTotal.WithLabelValues(outcome))
				release := make(chan struct{})
				unwind := sync.OnceFunc(func() { close(release) })
				defer unwind()
				daemon := interruptedMetricImporter{dispatched: make(chan context.Context, 1), unwind: release}
				loader, err := NewLoader(daemon, stage, 1<<20, WithRegistryTransport(transport))
				require.NoError(t, err)
				prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
				require.NoError(t, err)
				defer prepared.Close()
				// Release the SDK before deferred Prepared.Close joins it.
				defer unwind()
				caller, cancel := context.WithCancel(t.Context())
				defer cancel()
				finished := make(chan error, 1)
				go func() { _, err := loader.Import(caller, prepared); finished <- err }()
				work := <-daemon.dispatched
				cancel()
				synctest.Wait()
				require.NoError(t, work.Err(), "tenant cancellation is not a loader interruption")
				require.Equal(t, before, testutil.ToFloat64(imageImportTotal.WithLabelValues(outcome)))
				if outcome == "deadline" {
					time.Sleep(importCompletionTimeout)
				} else {
					drain, stop := context.WithCancel(t.Context())
					stop()
					require.ErrorIs(t, loader.Shutdown(drain), context.Canceled)
				}
				synctest.Wait()
				require.Equal(t, before+1, testutil.ToFloat64(imageImportTotal.WithLabelValues(outcome)))
				require.Empty(t, finished, "the counter must not wait for SDK unwind")
				unwind()
				require.Error(t, <-finished)
				require.Equal(t, before+1, testutil.ToFloat64(imageImportTotal.WithLabelValues(outcome)), "unwind cannot double-count interruption")
			})
		})
	}
}

func TestImportOutcomeDoesNotTrustDaemonContextErrors(t *testing.T) {
	for _, failure := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			before := testutil.ToFloat64(imageImportTotal.WithLabelValues("failure"))
			deadline := testutil.ToFloat64(imageImportTotal.WithLabelValues("deadline"))
			shutdown := testutil.ToFloat64(imageImportTotal.WithLabelValues("shutdown"))
			f := newRegistry(t, layerTar(t, []byte("content")))
			daemon := &coordinatedImporter{arrivals: make(chan context.Context, 1), results: make(chan error, 1)}
			daemon.results <- errors.Join(errors.New("daemon rejection"), failure)
			loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
			require.NoError(t, err)
			prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
			defer prepared.Close()
			_, err = loader.Import(t.Context(), prepared)
			require.ErrorIs(t, err, failure)
			require.Equal(t, before+1, testutil.ToFloat64(imageImportTotal.WithLabelValues("failure")))
			require.Equal(t, deadline, testutil.ToFloat64(imageImportTotal.WithLabelValues("deadline")))
			require.Equal(t, shutdown, testutil.ToFloat64(imageImportTotal.WithLabelValues("shutdown")))
		})
	}
}
