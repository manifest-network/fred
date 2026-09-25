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
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
)

func imageTenantPreparationForTest(t *testing.T, m *imageCapacityManager) imageTenantPreparation {
	t.Helper()
	return imageTenantPreparationForTenantTest(t, m, "tenant-a")
}

func imageTenantPreparationForTenantTest(t *testing.T, m *imageCapacityManager, tenant string) imageTenantPreparation {
	t.Helper()
	lease := uuid.NewString()
	var preparation imageTenantPreparation
	_, runs := imagePreparationSubjects(t, map[string]string{lease: "example.invalid/app:1"}, map[string]string{lease: tenant},
		func(ctx context.Context, mutations *storageMutations) error {
			var err error
			preparation, err = m.beginTenantPreparation(ctx, mutations)
			return err
		})
	runs[lease]()
	require.NotNil(t, preparation.state)
	t.Cleanup(preparation.close)
	return preparation
}

func imageShareForTest(shares *imageTenantShares, tenant string) *imageTenantWaiter {
	waiter := &imageTenantWaiter{owner: shares, ready: make(chan struct{}), members: make(map[string]int)}
	shares.updateMember(waiter, tenant, 1)
	return waiter
}

func imageStagingFlightForTest(t *testing.T, m *imageCapacityManager) *imageFlightLeader {
	t.Helper()
	preparation := imageTenantPreparationForTest(t, m)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return &imageFlightLeader{state: &imageFlightState{manager: m, ctx: ctx, cancel: cancel, share: imageShareForTest(&m.tenantShares, preparation.state.tenant)}}
}

func TestImageTenantStagingOwnershipIsSingleUseAndRetainedThroughCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, _, _ := imageCapacityFixture(t)
		preparation := imageStagingFlightForTest(t, m)
		copyOfPreparation := preparation
		_, err := m.reserveStaging(t.Context(), nil, imageMiB)
		require.Error(t, err)
		foreign, _, _ := imageCapacityFixture(t)
		_, err = foreign.reserveStaging(t.Context(), preparation, imageMiB)
		require.Error(t, err)
		stage, err := m.reserveStaging(t.Context(), preparation, imageMiB)
		require.NoError(t, err)
		_, err = m.reserveStaging(t.Context(), copyOfPreparation, imageMiB)
		require.Error(t, err, "copies cannot acquire another provider slot")
		preparation.state.cancel()
		copyOfPreparation.state.cancel()
		require.Equal(t, 1, m.tenantShares.used, "parent close cannot release an outstanding stage")
		copyOfStage := stage
		stage.close()
		copyOfStage.close()
		require.Zero(t, m.tenantShares.used)
		require.Empty(t, m.tenantShares.active)
	})
}

func TestImageTenantStagingPoolBorrowsUnusedCapacityAndPrioritizesNewTenantFIFO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var shares imageTenantShares
		owners := make([]func(), maxImageStages)
		for index := range owners {
			release, err := shares.acquire(t.Context(), imageShareForTest(&shares, "aggregator"))
			require.NoError(t, err)
			owners[index] = release
		}
		require.Equal(t, maxImageStages, shares.active["aggregator"], "a sole aggregator can use all staging slots")
		order := make(chan string, 3)
		acquired := make(chan func(), 3)
		for _, tenant := range []string{"aggregator", "new-tenant", "new-tenant"} {
			go func() {
				release, err := shares.acquire(t.Context(), imageShareForTest(&shares, tenant))
				require.NoError(t, err)
				order <- tenant
				acquired <- release
			}()
			synctest.Wait()
		}
		require.Empty(t, order)
		owners[0]()
		synctest.Wait()
		require.Equal(t, "new-tenant", <-order)
		(<-acquired)()
		synctest.Wait()
		require.Equal(t, "new-tenant", <-order)
		(<-acquired)()
		synctest.Wait()
		require.Equal(t, "aggregator", <-order)
		(<-acquired)()
		for _, owner := range owners {
			owner()
		}
		require.Zero(t, shares.used)
		require.Empty(t, shares.waiters)
		require.Empty(t, shares.active)
	})
}

func TestImageTenantWaitCancellationAndHeadroomRefusalReleaseCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, _, fs := imageCapacityFixture(t)
		var held []imageStaging
		for range maxImageStages {
			stage, err := m.reserveStaging(t.Context(), imageStagingFlightForTest(t, m), imageMiB)
			require.NoError(t, err)
			held = append(held, stage)
		}
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		waiter := imageStagingFlightForTest(t, m)
		go func() { _, err := m.reserveStaging(ctx, waiter, imageMiB); result <- err }()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		require.Equal(t, maxImageStages, m.tenantShares.used, "canceled waiters consume no capacity")
		require.Empty(t, m.tenantShares.waiters)
		for _, stage := range held {
			stage.close()
		}
		fs[m.stageRoot] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(imageMiB)}
		_, err := m.reserveStaging(t.Context(), imageStagingFlightForTest(t, m), imageMiB)
		require.Error(t, err)
		require.Zero(t, m.tenantShares.used, "headroom refusal releases the owned slot")
		require.Zero(t, m.staging)
		require.Empty(t, m.tenantShares.active)
		fs[m.stageRoot] = diskCapacity{total: 100 * uint64(imageMiB), available: 50 * uint64(imageMiB)}
		stage, err := m.reserveStaging(t.Context(), imageStagingFlightForTest(t, m), imageMiB)
		require.NoError(t, err)
		stage.close()
	})
}

func TestImageCapacityAggregatorCachedRolloutBypassesOccupiedStaging(t *testing.T) {
	for _, environment := range []struct {
		name   string
		leases int
	}{{"morpheus", 200}, {"dev", 50}} {
		t.Run(environment.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { testImageCapacityAggregatorCachedRollout(t, environment.leases) })
		})
	}
}

func testImageCapacityAggregatorCachedRollout(t *testing.T, warmLeases int) {
	m, daemon, fs := imageCapacityFixture(t)
	fs[m.stageRoot] = diskCapacity{total: 1000 * uint64(imageMiB), available: 900 * uint64(imageMiB)}
	fs["/images"] = fs[m.stageRoot]
	cold := imageCapacityRegistryImage(t, "cold content requiring import")
	warm := imageCapacityRegistryImage(t, "cached aggregator content")
	layers, err := cold.Layers()
	require.NoError(t, err)
	layer, err := layers[0].Digest()
	require.NoError(t, err)
	coldID, err := cold.ConfigName()
	require.NoError(t, err)
	warmID, err := warm.ConfigName()
	require.NoError(t, err)
	blocked, release := make(chan struct{}, maxImageStages), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	registryHandler := registry.New()
	transport := dockerReplayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		if r.Body == nil {
			r.Body = http.NoBody
		}
		w := httptest.NewRecorder()
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v2/cold") && strings.HasSuffix(r.URL.Path, "/blobs/"+layer.String()) {
			blocked <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		registryHandler.ServeHTTP(w, r)
		response := w.Result()
		response.Request = r
		return response, nil
	})
	t.Cleanup(unblock)
	fixtures := map[string]v1.Image{"registry.example/warm:latest": warm}
	for index := range maxImageStages {
		fixtures[fmt.Sprintf("registry.example/cold%d:latest", index)] = cold
	}
	for ref, fixture := range fixtures {
		tag, err := name.NewTag(ref)
		require.NoError(t, err)
		require.NoError(t, remote.Write(tag, fixture, remote.WithContext(t.Context()), remote.WithTransport(transport)))
	}
	refs, tenants := make(map[string]string), make(map[string]string)
	var coldLeases, cached []string
	for index := range maxImageStages + warmLeases {
		lease := uuid.NewString()
		tenants[lease] = "one-on-chain-aggregator"
		if index < maxImageStages {
			refs[lease] = fmt.Sprintf("registry.example/cold%d:latest", index)
			coldLeases = append(coldLeases, lease)
		} else {
			refs[lease] = "registry.example/warm:latest"
			cached = append(cached, lease)
		}
	}
	var imported atomic.Bool
	m.runtime = (&mockDockerClient{InspectImageFn: func(_ context.Context, id string) (*ImageInfo, error) {
		if id == warmID.String() {
			return &ImageInfo{ID: id}, nil
		}
		if imported.Load() {
			return &ImageInfo{ID: coldID.String()}, nil
		}
		return nil, errdefs.NotFound(errors.New("cold image absent"))
	}}).imageAdmitter()
	daemon.imageInspect = func(_ context.Context, id string, _ ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: id, Size: imageMiB}, nil
	}
	attachImageCapacityLoader(t, m, func(_ context.Context, input io.Reader) (image.LoadResponse, error) {
		if _, err := io.Copy(io.Discard, input); err != nil {
			return image.LoadResponse{}, err
		}
		imported.Store(true)
		return image.LoadResponse{Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}, imagefetch.WithRegistryTransport(transport))
	results := make(chan string, len(refs))
	runs := imagePreparationExecutionsForTenants(t, m, refs, tenants, results)
	var workers sync.WaitGroup
	t.Cleanup(func() { unblock(); workers.Wait() })
	for _, lease := range coldLeases {
		workers.Go(runs[lease])
	}
	synctest.Wait()
	require.Len(t, blocked, maxImageStages, "one aggregator borrows all unused staging capacity")
	for _, lease := range cached {
		workers.Go(runs[lease])
	}
	synctest.Wait()
	require.Len(t, results, warmLeases, "all cached requests must finish while the same tenant's staging pool is occupied")
	for range warmLeases {
		require.Contains(t, cached, <-results)
	}
	require.Equal(t, maxImageStages, m.tenantShares.used)
	unblock()
	workers.Wait()
	for range maxImageStages {
		require.Contains(t, coldLeases, <-results)
	}
	require.Empty(t, m.tenantShares.active)
	require.Zero(t, m.tenantShares.used)
	require.Zero(t, m.active)
}
