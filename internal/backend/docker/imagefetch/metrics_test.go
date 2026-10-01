package imagefetch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/prometheus/client_golang/prometheus"
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
				loader, err := NewLoader(daemon, stage, 1<<20, withRegistryTransportForTest(transport))
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
			loader, err := NewLoader(daemon, t.TempDir(), 1<<20, withRegistryTransportForTest(f.server.Client().Transport))
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

// A fresh test process observes package initialization before any test can
// create missing series by calling WithLabelValues itself.
func TestImportOutcomeCountersInitializedBeforeFirstImport(t *testing.T) {
	const child = "FRED_TEST_IMPORT_METRIC_INITIALIZATION"
	if os.Getenv(child) == "" {
		executable, err := os.Executable()
		require.NoError(t, err)
		command := exec.CommandContext(t.Context(), executable, "-test.run=^TestImportOutcomeCountersInitializedBeforeFirstImport$")
		command.Env = append(os.Environ(), child+"=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	found := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "fred_docker_backend_image_import_total" {
			continue
		}
		for _, metric := range family.Metric {
			require.Len(t, metric.Label, 1)
			require.Equal(t, "outcome", metric.Label[0].GetName())
			found[metric.Label[0].GetValue()] = metric.Counter.GetValue()
		}
	}
	require.Equal(t, map[string]float64{"success": 0, "failure": 0, "deadline": 0, "shutdown": 0}, found)
}

func TestImportOutcomeSuccessRecordsOnlyCompletedExchange(t *testing.T) {
	before := map[string]float64{}
	for _, outcome := range []string{"success", "failure", "deadline", "shutdown"} {
		before[outcome] = testutil.ToFloat64(imageImportTotal.WithLabelValues(outcome))
	}
	fixture := newRegistry(t, layerTar(t, []byte("successful import")))
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20, withRegistryTransportForTest(fixture.server.Client().Transport))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), fixture.ref(), testPlatform)
	require.NoError(t, err)
	defer prepared.Close()
	_, err = loader.Import(t.Context(), prepared)
	require.NoError(t, err)
	require.Equal(t, 1, daemon.loads)
	for outcome, previous := range before {
		expected := previous
		if outcome == "success" {
			expected++
		}
		require.Equal(t, expected, testutil.ToFloat64(imageImportTotal.WithLabelValues(outcome)), outcome)
	}
}
