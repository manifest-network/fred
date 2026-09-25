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
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imagefetch"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// A close may observe a canceled worker before it has actually drained. These
// older close-settlement tests continue through that explicit availability
// result so they still exercise the durable namespace/terminal boundary.
func deprovisionAfterWorkerDrain(t *testing.T, ctx context.Context, b *Backend, lease string) error {
	t.Helper()
	var result error
	require.Eventually(t, func() bool {
		result = b.Deprovision(ctx, lease)
		return !leasesm.IsLifecyclePending(result)
	}, 3*time.Second, time.Millisecond, "the canceled worker did not actually drain")
	return result
}

func TestDeprovisionDuringOwnedImageImportReturnsPendingBeforeHTTPDeadline(t *testing.T) {
	server := httptest.NewTLSServer(registry.New())
	defer server.Close()
	ref := strings.TrimPrefix(server.URL, "https://") + "/close:latest"
	tag, err := name.NewTag(ref)
	require.NoError(t, err)
	fixture := imageCapacityRegistryImage(t, "loader-owned close")
	require.NoError(t, remote.Write(tag, fixture, remote.WithTransport(server.Client().Transport)))
	id, err := fixture.ConfigName()
	require.NoError(t, err)
	var imported atomic.Bool
	mock := &mockDockerClient{InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
		if !imported.Load() {
			return nil, errdefs.NotFound(errors.New("image not cached"))
		}
		return &ImageInfo{ID: id.String()}, nil
	}}
	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.AllowedRegistries = []string{strings.TrimPrefix(server.URL, "https://")}
	m, daemon, _ := imageCapacityFixture(t)
	m.pins, err = shared.NewImagePinJournal(b.callbackStore, b.releaseStore, b.retentionStore)
	require.NoError(t, err)
	m.runtime = mock.imageAdmitter()
	b.imageCapacity = m
	daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
		return image.InspectResponse{ID: id.String(), Size: imageMiB}, nil
	}
	arrived := make(chan context.Context, 1)
	complete := make(chan struct{})
	finish := sync.OnceFunc(func() { close(complete) })
	attachImageCapacityLoader(t, m, func(ctx context.Context, input io.Reader) (image.LoadResponse, error) {
		if _, err := io.Copy(io.Discard, input); err != nil {
			return image.LoadResponse{}, err
		}
		arrived <- ctx
		<-complete
		imported.Store(true)
		return image.LoadResponse{Body: io.NopCloser(strings.NewReader(`{"stream":"Loaded image"}`)), JSON: true}, nil
	}, imagefetch.WithRegistryTransport(server.Client().Transport.(*http.Transport)))
	t.Cleanup(func() {
		finish()
		b.stopCancel()
		b.wg.Wait()
	})
	request := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON(ref))
	require.NoError(t, b.Provision(t.Context(), request))
	var work context.Context
	select {
	case work = <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("real provision worker did not dispatch image import")
	}
	// Every retry must receive the same typed availability observation well
	// before providerd's 30-second HTTP deadline, without canceling the import.
	for range 6 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		err := b.Deprovision(ctx, request.LeaseUUID)
		cancel()
		require.True(t, leasesm.IsLifecyclePending(err), "close must observe exact worker ownership: %v", err)
		require.NoError(t, work.Err())
		pending, err := m.loader.PendingBytes()
		require.NoError(t, err)
		require.Positive(t, pending, "close cannot settle an outstanding daemon exchange")
	}
	finish()
	require.Eventually(t, func() bool {
		err := b.Deprovision(t.Context(), request.LeaseUUID)
		if err == nil {
			return true
		}
		require.True(t, leasesm.IsLifecyclePending(err), "after actual import completion, close must drain without a server failure: %v", err)
		return false
	}, 5*time.Second, time.Millisecond)
	pending, err := m.loader.PendingBytes()
	require.NoError(t, err)
	require.Zero(t, pending, "the completed daemon exchange settles its own debit")
	callbacks, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, callbacks, 2, "one interrupted operation and one terminal close")
	require.Equal(t, backend.CallbackStatusFailed, callbacks[0].Status)
	require.Equal(t, "operation preempted by lease close", callbacks[0].Error,
		"the worker's canceled image wait cannot settle an image-pull failure after close requested ownership")
	require.Equal(t, backend.CallbackStatusDeprovisioned, callbacks[1].Status)
	require.NoError(t, b.Deprovision(t.Context(), request.LeaseUUID))
	replayed, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Equal(t, callbacks, replayed, "repeated close cannot publish duplicate settlements")
}
