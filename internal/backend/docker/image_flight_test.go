package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/system"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/uuid"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
)

type imageFlightFixture struct {
	m                        *imageCapacityManager
	daemon                   *dockerSDKView
	ref, alias, id, manifest string
	downloads, imports       atomic.Int64
	local                    atomic.Bool
	transport                http.RoundTripper
}

func newImageFlightFixture(t *testing.T, download, imported func(context.Context) error) *imageFlightFixture {
	t.Helper()
	m, daemon, _ := imageCapacityFixture(t)
	fixture := imageCapacityRegistryImage(t, "one verified cold rollout")
	id, err := fixture.ConfigName()
	require.NoError(t, err)
	manifest, err := fixture.Digest()
	require.NoError(t, err)
	layers, err := fixture.Layers()
	require.NoError(t, err)
	layer, err := layers[0].Digest()
	require.NoError(t, err)
	f := &imageFlightFixture{m: m, daemon: daemon, ref: "registry.example/rollout:latest", alias: "registry.example/rollout:alias", id: id.String(), manifest: manifest.String()}
	handler := registry.New()
	f.transport = dockerReplayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		if r.Body == nil {
			r.Body = http.NoBody
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/blobs/"+layer.String()) {
			f.downloads.Add(1)
			if download != nil {
				if err := download(r.Context()); err != nil {
					return nil, err
				}
			}
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		response := w.Result()
		response.Request = r
		return response, nil
	})
	for _, ref := range []string{f.ref, f.alias} {
		tag, err := name.NewTag(ref)
		require.NoError(t, err)
		require.NoError(t, remote.Write(tag, fixture, remote.WithContext(t.Context()), remote.WithTransport(f.transport)))
	}
	m.runtime = (&mockDockerClient{InspectImageFn: func(_ context.Context, id string) (*ImageInfo, error) {
		if f.local.Load() {
			return &ImageInfo{ID: id}, nil
		}
		return nil, errdefs.NotFound(errors.New("image absent"))
	}}).imageAdmitter()
	daemon.imageInspect = func(_ context.Context, id string, _ ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: id, Size: imageMiB}, nil
	}
	attachImageCapacityLoader(t, m, func(ctx context.Context, input io.Reader) (image.LoadResponse, error) {
		if _, err := io.Copy(io.Discard, input); err != nil {
			return image.LoadResponse{}, err
		}
		f.imports.Add(1)
		if imported != nil {
			if err := imported(ctx); err != nil {
				return image.LoadResponse{}, err
			}
		}
		f.local.Store(true)
		return image.LoadResponse{Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}, imagefetch.WithRegistryTransport(f.transport))
	return f
}

func TestImageFlightColdRolloutDownloadsOnceAndPublishesEachLeasePin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		f := newImageFlightFixture(t, func(ctx context.Context) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, nil)
		const leases = 16 // More than the four staging slots, all initially cold.
		refs, tenants := make(map[string]string), make(map[string]string)
		for index := range leases {
			lease := uuid.NewString()
			refs[lease] = []string{f.ref, f.alias}[index%2]
			tenants[lease] = "morpheus-aggregator"
		}
		results := make(chan error, leases)
		pins, runs := imagePreparationSubjects(t, refs, tenants, func(ctx context.Context, mutations *storageMutations) error {
			prepared, err := f.m.prepare(ctx, mutations, refs[mutations.leaseUUID], true)
			if err == nil && (prepared.ID() != f.id || prepared.Reference() != refs[mutations.leaseUUID]) {
				err = fmt.Errorf("preparation borrowed another lease's reference or image: %s %s", prepared.Reference(), prepared.ID())
			}
			results <- err
			return err
		})
		f.m.pins = pins
		var workers sync.WaitGroup
		defer workers.Wait()
		defer unblock()
		for _, run := range runs {
			workers.Go(run)
		}
		synctest.Wait()
		require.EqualValues(t, 1, f.downloads.Load())
		require.Equal(t, 1, f.m.tenantShares.used, "followers consume no staging shares")
		require.Len(t, f.m.flights.active, 1, "tags selecting the same immutable manifest share one flight")
		require.Empty(t, results)
		// Every participant already selected the old digest. A mutable tag bump
		// while the download waits must not change their imported/pinned image.
		for _, ref := range []string{f.ref, f.alias} {
			tag, err := name.NewTag(ref)
			require.NoError(t, err)
			require.NoError(t, remote.Write(tag, imageCapacityRegistryImage(t, "later tag content"), remote.WithTransport(f.transport)))
		}
		unblock()
		workers.Wait()
		for range leases {
			require.NoError(t, <-results)
		}
		require.EqualValues(t, 1, f.downloads.Load())
		require.EqualValues(t, 1, f.imports.Load())
		saved, err := pins.List()
		require.NoError(t, err)
		require.Len(t, saved, leases)
		for _, pin := range saved {
			require.Equal(t, f.id, pin.ImageID)
			require.Equal(t, refs[pin.LeaseUUID], pin.Reference)
			require.Positive(t, pin.ImportBytes)
		}
		require.Empty(t, f.m.flights.active)
		require.Zero(t, f.m.tenantShares.used)
		require.Zero(t, f.m.staging)
	})
}

func TestImageFlightFollowerCancellationDoesNotCancelLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		f := newImageFlightFixture(t, func(ctx context.Context) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, nil)
		leader := imageTenantPreparationForTest(t, f.m)
		follower := imageTenantPreparationForTest(t, f.m)
		done := make(chan error, 1)
		go func() { _, err := f.m.ingest(t.Context(), leader, f.ref, f.ref); done <- err }()
		synctest.Wait()
		ctx, cancel := context.WithCancel(t.Context())
		waiting := make(chan error, 1)
		go func() { _, err := f.m.ingest(ctx, follower, f.alias, f.alias); follower.close(); waiting <- err }()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-waiting, context.Canceled)
		require.Empty(t, done)
		require.EqualValues(t, 1, f.downloads.Load())
		require.Equal(t, 1, f.m.tenantShares.used)
		unblock()
		require.NoError(t, <-done)
		leader.close()
		require.EqualValues(t, 1, f.imports.Load())
		require.Empty(t, f.m.flights.active)
	})
}

func TestImageFlightContainerdFollowersReuseVerifiedImportBeforePinPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newImageFlightFixture(t, nil, nil)
		f.m.cfg.ImageDataPath = "/images"
		f.m.docker = newInspectionHarness(t).client
		f.daemon.info = func(context.Context) (system.Info, error) {
			return system.Info{DockerRootDir: "/images", Driver: "overlayfs", DriverStatus: [][2]string{{"driver-type", "io.containerd.snapshotter.v1"}}, OSType: "linux", Architecture: "amd64"}, nil
		}
		leader := imageTenantPreparationForTest(t, f.m)
		first, err := f.m.ingest(t.Context(), leader, f.ref, f.ref)
		require.NoError(t, err)
		require.Equal(t, f.manifest, first.image.ID())
		require.Positive(t, first.budget.Allocation().Bytes())
		pins, err := f.m.pins.List()
		require.NoError(t, err)
		require.Empty(t, pins, "the first preparation has not published its lease pin yet")
		// This arrival comes after import completed, while its owner still holds
		// the interval through pin publication. Containerd cannot use classic
		// config-ID reuse and must consume the verified flight's manifest proof.
		follower := imageTenantPreparationForTest(t, f.m)
		second, err := f.m.ingest(t.Context(), follower, f.alias, f.alias)
		require.NoError(t, err)
		require.Equal(t, first.image.ID(), second.image.ID())
		require.Equal(t, first.budget.Allocation().Bytes(), second.budget.Allocation().Bytes())
		require.Equal(t, f.alias, second.image.Reference())
		require.EqualValues(t, 1, f.downloads.Load())
		require.EqualValues(t, 1, f.imports.Load())
		leader.close()
		require.Len(t, f.m.flights.active, 1, "the follower still owns its publication interval")
		follower.close()
		require.Empty(t, f.m.flights.active)
	})
}

func TestImageFlightCanceledDownloadLeaderTransfersOnlyUndispatchedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var downloads atomic.Int64
		f := newImageFlightFixture(t, func(ctx context.Context) error {
			if downloads.Add(1) == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}, nil)
		ctx, cancel := context.WithCancel(t.Context())
		leader := imageTenantPreparationForTest(t, f.m)
		follower := imageTenantPreparationForTest(t, f.m)
		first, second := make(chan error, 1), make(chan error, 1)
		go func() { _, err := f.m.ingest(ctx, leader, f.ref, f.ref); leader.close(); first <- err }()
		synctest.Wait()
		go func() {
			_, err := f.m.ingest(t.Context(), follower, f.alias, f.alias)
			follower.close()
			second <- err
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-first, context.Canceled)
		require.NoError(t, <-second)
		require.EqualValues(t, 2, f.downloads.Load(), "one canceled attempt and one elected replacement")
		require.EqualValues(t, 1, f.imports.Load())
		require.Empty(t, f.m.flights.active)
		require.Zero(t, f.m.tenantShares.used)
	})
}

func TestImageFlightKeepsDispatchedImportAfterLeaderCancellation(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown_completion=%t", failed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release, dispatched := make(chan struct{}), make(chan context.Context, 1)
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				f := newImageFlightFixture(t, nil, func(ctx context.Context) error {
					dispatched <- ctx
					<-release
					if failed {
						return errors.Join(errors.New("unknown daemon completion"), context.Canceled)
					}
					return nil
				})
				ctx, cancel := context.WithCancel(t.Context())
				leader := imageTenantPreparationForTest(t, f.m)
				follower := imageTenantPreparationForTest(t, f.m)
				first, second := make(chan error, 1), make(chan error, 1)
				go func() { _, err := f.m.ingest(ctx, leader, f.ref, f.ref); leader.close(); first <- err }()
				work := <-dispatched
				go func() {
					_, err := f.m.ingest(t.Context(), follower, f.alias, f.alias)
					follower.close()
					second <- err
				}()
				synctest.Wait()
				cancel()
				synctest.Wait()
				require.NoError(t, work.Err())
				require.Empty(t, first, "leader keeps files/admission until the actual SDK return")
				require.Empty(t, second)
				require.EqualValues(t, 1, f.downloads.Load())
				require.Equal(t, 1, f.m.tenantShares.used)
				unblock()
				require.ErrorIs(t, <-first, context.Canceled)
				if failed {
					require.ErrorContains(t, <-second, "unknown daemon completion")
					pending, err := f.m.loader.PendingBytes()
					require.NoError(t, err)
					require.Positive(t, pending, "a waiting follower cannot forgive or retry an unknown import")
				} else {
					require.NoError(t, <-second)
				}
				require.EqualValues(t, 1, f.downloads.Load())
				require.EqualValues(t, 1, f.imports.Load())
				require.Empty(t, f.m.flights.active)
				require.Zero(t, f.m.tenantShares.used)
			})
		})
	}
}

func TestImageFlightRechecksLocalImageAfterStagingAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newImageFlightFixture(t, nil, nil)
		var held []imageStaging
		for range maxImageStages {
			stage, err := f.m.reserveStaging(t.Context(), imageTenantPreparationForTest(t, f.m), imageMiB)
			require.NoError(t, err)
			held = append(held, stage)
		}
		defer func() {
			for _, stage := range held {
				stage.close()
			}
		}()
		preparation := imageTenantPreparationForTest(t, f.m)
		done := make(chan error, 1)
		go func() {
			_, err := f.m.ingest(t.Context(), preparation, f.ref, f.ref)
			preparation.close()
			done <- err
		}()
		synctest.Wait()
		require.Len(t, f.m.tenantShares.waiters, 1)
		f.local.Store(true)
		held[0].close()
		require.NoError(t, <-done)
		require.Zero(t, f.downloads.Load(), "queued preparation must reuse newly available content")
		require.Zero(t, f.imports.Load())
	})
}

func TestImageFlightDoesNotShareDifferentPlatformSelections(t *testing.T) {
	f := newImageFlightFixture(t, nil, nil)
	firstResolution, err := f.m.loader.Resolve(t.Context(), f.ref, ocispec.Platform{OS: "linux", Architecture: "amd64"})
	require.NoError(t, err)
	secondResolution, err := f.m.loader.Resolve(t.Context(), f.ref, ocispec.Platform{OS: "linux", Architecture: "arm64"})
	require.NoError(t, err)
	require.Equal(t, firstResolution.SourceReference(), secondResolution.SourceReference())
	first := imageTenantPreparationForTest(t, f.m)
	second := imageTenantPreparationForTest(t, f.m)
	_, firstLeader, err := first.joinFlight(f.m, firstResolution)
	require.NoError(t, err)
	require.NotNil(t, firstLeader)
	defer firstLeader.complete(imageFlightFailure{err: errors.New("fixture does not stage")})
	_, secondLeader, err := second.joinFlight(f.m, secondResolution)
	require.NoError(t, err)
	require.NotNil(t, secondLeader, "another requested platform must validate its own config/layers")
	defer secondLeader.complete(imageFlightFailure{err: errors.New("fixture does not stage")})
	require.Len(t, f.m.flights.active, 2)
}

func TestImageFlightCancellationAfterReservationRetriesOnlyClosedUnsentAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newImageFlightFixture(t, nil, nil)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		leaderPreparation := imageTenantPreparationForTest(t, f.m)
		resolution, err := f.m.loader.Resolve(ctx, f.ref, ocispec.Platform{OS: "linux", Architecture: "amd64"})
		require.NoError(t, err)
		_, leader, err := leaderPreparation.joinFlight(f.m, resolution)
		require.NoError(t, err)
		require.NotNil(t, leader)
		defer leader.complete(imageFlightFailure{err: errors.New("fixture aborted")})
		stage, err := f.m.reserveStaging(ctx, leaderPreparation, f.m.cfg.ImageMaxSizeMB*imageMiB)
		require.NoError(t, err)
		defer stage.close()
		prepared, err := f.m.loader.PrepareResolved(ctx, resolution)
		require.NoError(t, err)
		defer prepared.Close()
		admission, err := f.m.reserveImport(ctx, f.m.loader, prepared)
		require.NoError(t, err)
		copied := *admission
		follower := imageTenantPreparationForTest(t, f.m)
		done := make(chan error, 1)
		go func() {
			_, err := f.m.ingest(t.Context(), follower, f.alias, f.alias)
			follower.close()
			done <- err
		}()
		synctest.Wait()
		require.Empty(t, done)
		// Enter the actual import phase at its admission/dispatch boundary, after
		// cancellation. The loader must consume this exact unsent capability.
		cancel()
		info, err := f.daemon.Info(t.Context())
		require.NoError(t, err)
		outcome := f.m.importImageFlight(ctx, f.m.loader, prepared, admission, info)
		require.IsType(t, imageFlightRetry{}, outcome)
		require.Zero(t, f.imports.Load())
		pending, err := f.m.loader.PendingBytes()
		require.NoError(t, err)
		require.Zero(t, pending)
		_, err = f.m.loader.ImportAdmitted(t.Context(), &copied)
		require.Error(t, err, "a copied admission cannot dispatch after no-dispatch evidence was issued")
		require.NoError(t, prepared.Close())
		stage.close()
		leader.complete(outcome)
		leaderPreparation.close()
		require.NoError(t, <-done, "a live follower elects a replacement instead of inheriting caller cancellation")
		require.EqualValues(t, 2, f.downloads.Load())
		require.EqualValues(t, 1, f.imports.Load())
		require.Empty(t, f.m.flights.active)
		require.Zero(t, f.m.tenantShares.used)
	})
}
