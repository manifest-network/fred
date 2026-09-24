package docker

import (
	"context"
	"errors"
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
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
)

func imageTenantPreparationForTest(t *testing.T, m *imageCapacityManager) imageTenantPreparation {
	t.Helper()
	lease := uuid.NewString()
	var preparation imageTenantPreparation
	_, runs := imagePreparationSubjects(t, map[string]string{lease: "example.invalid/app:1"}, nil,
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

func TestImageTenantStagingOwnershipIsSingleUseAndRetainedThroughCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, _, _ := imageCapacityFixture(t)
		const lease = "550e8400-e29b-41d4-a716-446655440001"
		var mutations *storageMutations
		_, runs := imagePreparationSubjects(t, map[string]string{lease: "example.invalid/app:1"}, nil,
			func(_ context.Context, subject *storageMutations) error { mutations = subject; return nil })
		runs[lease]()
		preparation, err := m.beginTenantPreparation(t.Context(), mutations)
		require.NoError(t, err)
		copyOfPreparation := preparation
		_, err = m.reserveStaging(t.Context(), imageTenantPreparation{}, imageMiB)
		require.Error(t, err)
		foreign, _, _ := imageCapacityFixture(t)
		_, err = foreign.reserveStaging(t.Context(), preparation, imageMiB)
		require.Error(t, err)
		stage, err := m.reserveStaging(t.Context(), preparation, imageMiB)
		require.NoError(t, err)
		_, err = m.reserveStaging(t.Context(), copyOfPreparation, imageMiB)
		require.Error(t, err, "copies cannot acquire another provider slot")
		preparation.close()
		copyOfPreparation.close()
		acquired := make(chan imageTenantPreparation, 1)
		go func() {
			next, err := m.beginTenantPreparation(t.Context(), mutations)
			if err == nil {
				acquired <- next
			}
		}()
		synctest.Wait()
		require.Empty(t, acquired, "closing a parent cannot release an outstanding stage's tenant share")
		copyOfStage := stage
		stage.close()
		copyOfStage.close()
		synctest.Wait()
		next := <-acquired
		next.close()
		require.Empty(t, m.stageSlots)
		require.Empty(t, m.tenantShares.active)
	})
}

func TestImageTenantWaitCancellationAndHeadroomRefusalReleaseCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, _, fs := imageCapacityFixture(t)
		const lease = "550e8400-e29b-41d4-a716-446655440001"
		var mutations *storageMutations
		_, runs := imagePreparationSubjects(t, map[string]string{lease: "example.invalid/app:1"}, nil,
			func(_ context.Context, subject *storageMutations) error { mutations = subject; return nil })
		runs[lease]()
		first, err := m.beginTenantPreparation(t.Context(), mutations)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { _, err := m.beginTenantPreparation(ctx, mutations); result <- err }()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		require.Empty(t, m.stageSlots, "tenant waiters never own global staging capacity")
		fs[m.stageRoot] = diskCapacity{total: 100 * uint64(imageMiB), available: uint64(imageMiB)}
		_, err = m.reserveStaging(t.Context(), first, imageMiB)
		require.Error(t, err)
		first.close()
		require.Empty(t, m.stageSlots, "headroom refusal releases the global slot")
		require.Zero(t, m.staging)
		require.Empty(t, m.tenantShares.active, "headroom refusal leaves no tenant owner")
		fs[m.stageRoot] = diskCapacity{total: 100 * uint64(imageMiB), available: 50 * uint64(imageMiB)}
		next, err := m.beginTenantPreparation(t.Context(), mutations)
		require.NoError(t, err)
		defer next.close()
		stage, err := m.reserveStaging(t.Context(), next, imageMiB)
		require.NoError(t, err)
		stage.close()
	})
}

func TestImageCapacityFourSlowLeasesCannotConsumeAnotherTenantsStagingShare(t *testing.T) {
	synctest.Test(t, testImageCapacityFourSlowLeasesCannotConsumeAnotherTenantsStagingShare)
}

func testImageCapacityFourSlowLeasesCannotConsumeAnotherTenantsStagingShare(t *testing.T) {
	m, daemon, _ := imageCapacityFixture(t)
	fixture := imageCapacityRegistryImage(t, "shared content requiring initial import")
	layers, err := fixture.Layers()
	require.NoError(t, err)
	layer, err := layers[0].Digest()
	require.NoError(t, err)
	config, err := fixture.ConfigName()
	require.NoError(t, err)
	blocked, release := make(chan struct{}, 4), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var slowReads atomic.Int32
	registryHandler := registry.New()
	transport := dockerReplayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		if r.Body == nil {
			r.Body = http.NoBody
		}
		w := httptest.NewRecorder()
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v2/slow") && strings.HasSuffix(r.URL.Path, "/blobs/"+layer.String()) {
			slowReads.Add(1)
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
	refs, tenants := make(map[string]string), make(map[string]string)
	leases := []string{
		"550e8400-e29b-41d4-a716-446655440001", "550e8400-e29b-41d4-a716-446655440002",
		"550e8400-e29b-41d4-a716-446655440003", "550e8400-e29b-41d4-a716-446655440004",
		"550e8400-e29b-41d4-a716-446655440005",
	}
	for index, lease := range leases {
		repo := "slow" + lease
		tenants[lease] = "tenant-a"
		if index == 4 {
			repo, tenants[lease] = "other", "tenant-b"
		}
		ref := "registry.example/" + repo + ":latest"
		refs[lease] = ref
		tag, err := name.NewTag(ref)
		require.NoError(t, err)
		require.NoError(t, remote.Write(tag, fixture, remote.WithContext(t.Context()), remote.WithTransport(transport)))
	}
	var present atomic.Bool
	m.runtime = (&mockDockerClient{InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
		if !present.Load() {
			return nil, errdefs.NotFound(errors.New("not imported"))
		}
		return &ImageInfo{ID: config.String()}, nil
	}}).imageAdmitter()
	daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: config.String(), Size: imageMiB}, nil
	}
	attachImageCapacityLoader(t, m, func(_ context.Context, input io.Reader) (image.LoadResponse, error) {
		if _, err := io.Copy(io.Discard, input); err != nil {
			return image.LoadResponse{}, err
		}
		present.Store(true)
		return image.LoadResponse{Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}, imagefetch.WithRegistryTransport(transport))
	results := make(chan string, len(leases))
	runs := imagePreparationExecutionsForTenants(t, m, refs, tenants, results)
	var workers sync.WaitGroup
	t.Cleanup(func() { unblock(); workers.Wait() })
	workers.Go(runs[leases[0]])
	synctest.Wait()
	require.Len(t, blocked, 1, "the first tenant must hold one layer download")
	for _, lease := range leases[1:4] {
		workers.Go(runs[lease])
	}
	synctest.Wait()
	require.Equal(t, int32(1), slowReads.Load(), "all three same-tenant waiters must remain outside provider staging")
	require.Empty(t, results)
	workers.Go(runs[leases[4]])
	synctest.Wait()
	require.Len(t, results, 1, "another tenant must finish while all same-tenant leases remain behind their one download")
	require.Equal(t, leases[4], <-results)
	require.NoError(t, m.lock(t.Context()))
	require.Equal(t, 1, m.active)
	require.Len(t, m.stageSlots, 1)
	m.unlock()
	unblock()
	workers.Wait()
	for range 4 {
		require.Contains(t, leases[:4], <-results)
	}
	require.Empty(t, m.tenantShares.active)
	require.Empty(t, m.stageSlots)
	require.Zero(t, m.active)
}
