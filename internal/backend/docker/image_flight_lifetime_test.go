package docker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
)

func TestImageFlightLastMemberDepartureCancelsAndDrainsBeforeAnotherAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		request := make(chan context.Context, 1)
		released := make(chan struct{})
		f := newImageFlightFixture(t, func(ctx context.Context) error {
			request <- ctx
			<-ctx.Done()
			<-released // Model the registry unwinding after cancellation.
			return ctx.Err()
		}, nil)
		ctx, cancel := context.WithCancel(t.Context())
		preparation := imageTenantPreparationForTest(t, f.m)
		done := make(chan error, 1)
		go func() { _, err := f.m.ingest(ctx, preparation, f.ref, f.ref); preparation.close(); done <- err }()
		work := <-request
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		require.ErrorIs(t, work.Err(), context.Canceled)
		synctest.Wait()
		require.Equal(t, 1, f.m.flights.workers)
		require.Equal(t, 1, f.m.tenantShares.used, "flight retains staging until the registry releases it")
		require.Equal(t, 1, f.m.active, "orphaned worker keeps its independent collection exclusion")
		ctx, cancelDrain := context.WithTimeout(t.Context(), time.Second)
		defer cancelDrain()
		require.ErrorIs(t, f.m.flights.shutdown(ctx), context.DeadlineExceeded)
		close(released)
		require.NoError(t, f.m.flights.shutdown(t.Context()))
		require.Empty(t, f.m.flights.active)
		require.Zero(t, f.m.staging)
		require.Zero(t, f.m.tenantShares.used)
		require.Zero(t, f.m.active)
		require.Zero(t, f.imports.Load())
	})
}

func TestImageFlightShutdownCancelsDownloadsAndRefusesNewWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newImageFlightFixture(t, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }, nil)
		stopCtx, stop := context.WithCancel(t.Context())
		defer stop()
		f.m.lifetime = stopCtx
		preparation := imageTenantPreparationForTest(t, f.m)
		done := make(chan error, 1)
		go func() { _, err := f.m.ingest(t.Context(), preparation, f.ref, f.ref); preparation.close(); done <- err }()
		synctest.Wait()
		stop()
		require.NoError(t, f.m.flights.shutdown(t.Context()))
		require.ErrorContains(t, <-done, "shut down")
		require.Zero(t, f.imports.Load())
		require.Zero(t, f.m.flights.workers)
		require.Empty(t, f.m.flights.active)
		require.Zero(t, f.m.tenantShares.used)
		_, err := f.m.ingest(t.Context(), imageTenantPreparationForTest(t, f.m), f.ref, f.ref)
		require.ErrorContains(t, err, "shut down")
	})
}

func TestImageFlightPanicReleasesFilesAndPublishesFailureToAllMembers(t *testing.T) {
	for _, dispatched := range []bool{false, true} {
		name := "registry"
		if dispatched {
			name = "daemon"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				crash := func(context.Context) error { <-release; panic("foreign worker panic") }
				var f *imageFlightFixture
				if dispatched {
					f = newImageFlightFixture(t, nil, crash)
				} else {
					f = newImageFlightFixture(t, crash, nil)
				}
				results := make(chan error, 2)
				for range 2 {
					preparation := imageTenantPreparationForTest(t, f.m)
					go func() {
						_, err := f.m.ingest(t.Context(), preparation, f.ref, f.ref)
						preparation.close()
						results <- err
					}()
					synctest.Wait()
				}
				close(release)
				for range 2 {
					require.ErrorContains(t, <-results, "foreign worker panic")
				}
				require.NoError(t, f.m.flights.shutdown(t.Context()))
				require.Zero(t, f.m.staging)
				require.Zero(t, f.m.tenantShares.used)
				require.Empty(t, f.m.flights.active)
				pending, err := f.m.loader.PendingBytes()
				require.NoError(t, err)
				if dispatched {
					require.Positive(t, pending)
				} else {
					require.Zero(t, pending)
				}
			})
		})
	}
}

func TestImageFlightRetryEvictsCompletedKeyBeforeOldMembersRetire(t *testing.T) {
	f := newImageFlightFixture(t, nil, nil)
	resolution, err := f.m.loader.Resolve(t.Context(), f.ref, ocispec.Platform{OS: "linux", Architecture: "amd64"})
	require.NoError(t, err)
	preparation := imageTenantPreparationForTest(t, f.m)
	member, worker, err := preparation.joinFlight(f.m, resolution, f.m.loader.VerificationBudget())
	require.NoError(t, err)
	worker.complete(imageFlightRetry{})
	outcome, err := member.wait(t.Context())
	require.NoError(t, err)
	require.IsType(t, imageFlightRetry{}, outcome)
	// The old member may still be processing its result while a new arrival
	// retries. It must not repeatedly join an already completed Retry outcome.
	nextMember, replacement, err := imageTenantPreparationForTest(t, f.m).joinFlight(f.m, resolution, f.m.loader.VerificationBudget())
	require.NoError(t, err)
	require.NotNil(t, replacement)
	replacement.complete(imageFlightFailure{err: errors.New("fixture ended")})
	nextMember.retire()
	member.retire()
	require.Empty(t, f.m.flights.active)
}

func TestImageFlightQueuedWorkUsesBestLiveTenantAndTransfersActiveCharge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		f := newImageFlightFixture(t, func(ctx context.Context) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, nil)
		held := make([]func(), maxImageStages)
		for i := range held {
			var err error
			held[i], err = f.m.tenantShares.acquire(t.Context(), imageShareForTest(&f.m.tenantShares, "heavy"))
			require.NoError(t, err)
			defer held[i]()
		}
		other := make(chan func(), 1)
		go func() {
			release, err := f.m.tenantShares.acquire(t.Context(), imageShareForTest(&f.m.tenantShares, "heavy"))
			require.NoError(t, err)
			other <- release
		}()
		synctest.Wait()
		firstCtx, cancelFirst := context.WithCancel(t.Context())
		first, second := make(chan error, 1), make(chan error, 1)
		heavy := imageTenantPreparationForTenantTest(t, f.m, "heavy")
		light := imageTenantPreparationForTenantTest(t, f.m, "light")
		go func() { _, err := f.m.ingest(firstCtx, heavy, f.ref, f.ref); heavy.close(); first <- err }()
		synctest.Wait()
		lightCtx, cancelLight := context.WithCancel(t.Context())
		go func() { _, err := f.m.ingest(lightCtx, light, f.alias, f.alias); light.close(); second <- err }()
		synctest.Wait()
		held[0]()
		synctest.Wait()
		require.EqualValues(t, 1, f.downloads.Load(), "light follower moves the shared flight ahead of heavy tenant's earlier waiter")
		require.Empty(t, other)
		require.Equal(t, 1, f.m.tenantShares.active["light"])
		cancelLight()
		require.ErrorIs(t, <-second, context.Canceled)
		require.Zero(t, f.m.tenantShares.active["light"])
		require.Equal(t, maxImageStages, f.m.tenantShares.active["heavy"], "active staging charge follows surviving member")
		cancelFirst()
		require.ErrorIs(t, <-first, context.Canceled)
		require.NoError(t, f.m.flights.shutdown(t.Context()))
		(<-other)()
		for _, release := range held {
			release()
		}
		require.Zero(t, f.m.tenantShares.used)
		require.Empty(t, f.m.tenantShares.active)
	})
}

func TestStopRetainsDependenciesUntilCanceledFlightActuallyUnwinds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var closed int
		b := newBackendForTest(&mockDockerClient{CloseFn: func() error { closed++; return nil }}, nil)
		b.shutdownDrainTimeout = time.Second
		unwind := make(chan struct{})
		f := newImageFlightFixture(t, func(ctx context.Context) error { <-ctx.Done(); <-unwind; return ctx.Err() }, nil)
		f.m.lifetime = b.stopCtx
		b.imageCapacity = f.m
		ctx, cancel := context.WithCancel(t.Context())
		preparation := imageTenantPreparationForTest(t, f.m)
		result := make(chan error, 1)
		go func() { _, err := f.m.ingest(ctx, preparation, f.ref, f.ref); preparation.close(); result <- err }()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		require.ErrorIs(t, b.Stop(), ErrShutdownDrainTimeout)
		require.Zero(t, closed, "a canceled registry worker still owns the daemon and journals")
		close(unwind)
		synctest.Wait()
		require.NoError(t, b.Stop())
		require.Equal(t, 1, closed)
		require.Zero(t, f.m.flights.workers)
		require.Zero(t, f.m.active)
	})
}

func TestImageFlightCopiedWorkerCannotPublishCompletionWhileOwnerIsRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newImageFlightFixture(t, nil, nil)
		resolution, err := f.m.loader.Resolve(t.Context(), f.ref, ocispec.Platform{OS: "linux", Architecture: "amd64"})
		require.NoError(t, err)
		member, worker, err := imageTenantPreparationForTest(t, f.m).joinFlight(f.m, resolution, f.m.loader.VerificationBudget())
		require.NoError(t, err)
		copyOfWorker := *worker
		release := make(chan struct{})
		var calls int
		work := func(context.Context) imageFlightOutcome {
			calls++
			<-release
			return imageFlightFailure{err: errors.New("finished")}
		}
		done := make(chan struct{}, 2)
		go func() { worker.run(work); done <- struct{}{} }()
		synctest.Wait()
		go func() { copyOfWorker.run(work); done <- struct{}{} }()
		synctest.Wait()
		require.Equal(t, 1, calls)
		require.Equal(t, 1, f.m.flights.workers)
		select {
		case <-member.state.done:
			t.Fatal("a copied worker published completion while the owner is live")
		default:
		}
		close(release)
		<-done
		<-done
		outcome, err := member.wait(t.Context())
		require.NoError(t, err)
		require.ErrorContains(t, outcome.(imageFlightFailure).err, "finished")
		member.retire()
		require.NoError(t, f.m.flights.shutdown(t.Context()))
		require.Empty(t, f.m.flights.active)
	})
}

// Each chunk progresses through the real registry transport before its
// thirty-second idle deadline. The transfer itself takes eight literal virtual
// minutes, so this exercises both timeout scales without shortening constants.
type progressingFlightLayerBody struct {
	ctx              context.Context
	remaining, ready []byte
	chunks           int
	progress         *atomic.Int64
}

func (b *progressingFlightLayerBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(b.ready) == 0 {
		if len(b.remaining) == 0 {
			return 0, io.EOF
		}
		timer := time.NewTimer(20 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
		size := (len(b.remaining) + b.chunks - 1) / b.chunks
		b.ready, b.remaining = b.remaining[:size], b.remaining[size:]
		b.chunks--
		b.progress.Add(1)
	}
	n := copy(p, b.ready)
	b.ready = b.ready[n:]
	return n, nil
}

func (*progressingFlightLayerBody) Close() error { return nil }

func TestImageFlightEightMinuteProgressingDownloadSurvivesFirstMemberCloseAtSevenMinutes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newImageFlightFixture(t, nil, nil)
		var progress atomic.Int64
		requests := make(chan context.Context, 1)
		transport := dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			response, err := f.transport.RoundTrip(req)
			if err != nil {
				return nil, err
			}
			if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/blobs/") && !strings.HasSuffix(req.URL.Path, "/blobs/"+f.id) {
				raw, err := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				if err != nil || closeErr != nil {
					return nil, errors.Join(err, closeErr)
				}
				require.GreaterOrEqual(t, len(raw), 24, "the fixture needs at least one verified byte per progress tick")
				response.Body = &progressingFlightLayerBody{ctx: req.Context(), remaining: raw, chunks: 24, progress: &progress}
				requests <- req.Context()
			}
			return response, nil
		})
		require.NoError(t, imagefetch.WithRegistryTransport(transport)(f.m.loader))
		started := time.Now()
		firstCtx, cancelFirst := context.WithTimeout(t.Context(), 10*time.Minute)
		defer cancelFirst()
		firstPreparation := imageTenantPreparationForTest(t, f.m)
		first := make(chan error, 1)
		go func() {
			_, err := f.m.ingest(firstCtx, firstPreparation, f.ref, f.ref)
			firstPreparation.close()
			first <- err
		}()
		flightContext := <-requests
		const followers = 16
		results := make(chan error, followers)
		for range followers {
			preparation := imageTenantPreparationForTest(t, f.m)
			go func() {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
				defer cancel()
				_, err := f.m.ingest(ctx, preparation, f.alias, f.alias)
				preparation.close()
				results <- err
			}()
		}
		synctest.Wait()
		require.EqualValues(t, 1, f.downloads.Load())
		require.Equal(t, 1, f.m.tenantShares.used)
		time.Sleep(7 * time.Minute)
		cancelFirst()
		require.ErrorIs(t, <-first, context.Canceled)
		synctest.Wait()
		require.Equal(t, 7*time.Minute, time.Since(started))
		require.NoError(t, flightContext.Err(), "a departed member cannot revoke the progressing registry transfer")
		require.GreaterOrEqual(t, progress.Load(), int64(20))
		require.Empty(t, results)
		for range followers {
			require.NoError(t, <-results)
		}
		require.Equal(t, 8*time.Minute, time.Since(started))
		require.EqualValues(t, 24, progress.Load())
		require.EqualValues(t, 1, f.downloads.Load())
		require.EqualValues(t, 1, f.imports.Load())
		require.Empty(t, f.m.flights.active)
		require.Zero(t, f.m.flights.workers)
		require.Zero(t, f.m.tenantShares.used)
		require.Zero(t, f.m.staging)
	})
}
