package docker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// Concurrent new admission and saved recovery verify the same immutable image
// under different ceilings. Their outcomes must remain independent.
func TestImageFlightRecoveryBudgetDoesNotInheritNewImageRefusal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, daemon, fs := imageCapacityFixture(t)
		for path := range fs {
			fs[path] = diskCapacity{available: uint64(100 * imageMiB), total: uint64(200 * imageMiB)}
		}
		m.cfg.ImageMaxSizeMB = 1
		fixture := imageCapacityRegistryImage(t, strings.Repeat("legacy bytes", 200_000))
		layers, err := fixture.Layers()
		require.NoError(t, err)
		layer, err := layers[0].Digest()
		require.NoError(t, err)
		manifestID, err := fixture.Digest()
		require.NoError(t, err)
		configID, err := fixture.ConfigName()
		require.NoError(t, err)
		release := []chan struct{}{make(chan struct{}), make(chan struct{})}
		var gets atomic.Int32
		handler := registry.New()
		transport := dockerReplayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			r = r.Clone(r.Context())
			if r.Body == nil {
				r.Body = http.NoBody
			}
			if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/blobs/"+layer.String()) {
				attempt := gets.Add(1)
				if attempt > int32(len(release)) {
					panic("unexpected download")
				}
				select {
				case <-release[attempt-1]:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			response := w.Result()
			response.Request = r
			return response, nil
		})
		ref := "registry.example/large:latest"
		tag, err := name.NewTag(ref)
		require.NoError(t, err)
		require.NoError(t, remote.Write(tag, fixture, remote.WithContext(t.Context()), remote.WithTransport(transport)))
		gets.Store(0)
		var present atomic.Bool
		m.runtime = (&mockDockerClient{InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			if !present.Load() {
				return nil, errdefs.NotFound(errors.New("missing pinned content"))
			}
			return &ImageInfo{ID: configID.String()}, nil
		}}).imageAdmitter()
		daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
			return image.InspectResponse{ID: configID.String(), Size: 3 * imageMiB}, nil
		}
		attachImageCapacityLoader(t, m, func(_ context.Context, input io.Reader) (image.LoadResponse, error) {
			_, err := io.Copy(io.Discard, input)
			present.Store(true)
			return image.LoadResponse{Body: io.NopCloser(strings.NewReader("{}"))}, err
		}, imagefetch.WithRegistryTransport(nativeRegistryTransport(t, transport)))
		pin := &shared.ImagePin{ImageID: configID.String(), PullDigest: tag.Context().Digest(manifestID.String()).Name(), ImportBytes: 8 * imageMiB, VerificationBytes: 8 * imageMiB}
		pin.Platform.OS, pin.Platform.Architecture = "linux", "amd64"

		leaderPrep := imageTenantPreparationForTest(t, m)
		followerPrep := imageTenantPreparationForTest(t, m)
		leaderErr, followerErr := make(chan error, 1), make(chan error, 1)
		go func() { _, err := m.ingest(t.Context(), leaderPrep, ref, ref); leaderPrep.close(); leaderErr <- err }()
		synctest.Wait()
		go func() {
			_, err := m.resolveImage(t.Context(), followerPrep, ref, pin, true)
			followerPrep.close()
			followerErr <- err
		}()
		synctest.Wait()
		require.EqualValues(t, 2, gets.Load(), "different verification ceilings require independent flights")
		close(release[0])
		lerr := <-leaderErr
		close(release[1])
		ferr := <-followerErr
		t.Logf("new-image leader err: %v", lerr)
		t.Logf("recovery follower err: %v", ferr)
		// Control: the same recovery alone succeeds under its saved budget.
		alone := imageTenantPreparationForTest(t, m)
		resolved, aloneErr := m.resolveImage(t.Context(), alone, ref, pin, true)
		alone.close()
		t.Logf("recovery alone err: %v id=%v", aloneErr, resolved.image.ID())
		require.Error(t, lerr)
		require.NoError(t, ferr, "saved recovery must not inherit the new-image policy refusal")
		require.NoError(t, aloneErr)
	})
}
