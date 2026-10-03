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

// imageFlightAccounting is what the image manager still owns for its flights:
// flight workers and registry entries, live members, collection exclusions,
// staging bytes and staging slots.
type imageFlightAccounting struct {
	workers, flights, members, exclusions, slots int
	staging                                      int64
}

// observeImageFlightAccounting reads the flight registry, the tenant shares and
// the manager gate in three separate critical sections, so the fields are not
// one atomic snapshot. Call it only while the flights are quiescent (the flight
// worker parked in the daemon exchange, or joined by shutdown); a mid-transition
// read can mix values from before and after one step.
func observeImageFlightAccounting(t *testing.T, m *imageCapacityManager) imageFlightAccounting {
	t.Helper()
	var observed imageFlightAccounting
	m.flights.mu.Lock()
	observed.workers, observed.flights = m.flights.workers, len(m.flights.active)
	for _, flight := range m.flights.active {
		observed.members += flight.members
	}
	m.flights.mu.Unlock()
	m.tenantShares.mu.Lock()
	observed.slots = m.tenantShares.used
	m.tenantShares.mu.Unlock()
	require.NoError(t, m.lock(t.Context()))
	observed.exclusions, observed.staging = m.active, m.staging
	m.unlock()
	return observed
}

// closeBesideHeldFlightRegistry makes one bounded close attempt while the caller
// holds m.flights.mu, and release gives that lock back. The close path must
// never take the flight registry. A sync.Mutex ignores the attempt's deadline,
// so the attempt runs on its own goroutine: if it has not returned well past its
// deadline, it is blocked behind the held registry. The helper then releases
// the registry so the attempt and the backend can unwind, and fails with that
// cause rather than hanging until the test binary's timeout.
func closeBesideHeldFlightRegistry(t *testing.T, b *Backend, lease string, release func()) error {
	t.Helper()
	const deadline, watchdog = time.Second, 5 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), deadline)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- b.Deprovision(ctx, lease) }()
	select {
	case err := <-result:
		require.NotErrorIs(t, err, context.DeadlineExceeded,
			"close waited out its deadline while the test held the image flight registry: the close path must not wait on m.flights.mu")
		return err
	case <-time.After(watchdog):
	}
	release()
	select {
	case <-result:
		t.Fatal("close blocked on the image flight registry the test held: the close path must not take m.flights.mu")
	case <-time.After(watchdog):
		t.Fatal("close stayed blocked after the test released the image flight registry: the hang has another cause")
	}
	return nil
}

// A dispatched daemon import belongs to the manager-owned image flight and the
// loader's lifetime, never to a lease worker; the lease is only a flight member.
// Close cancels the lease worker and answers each bounded retry with the typed
// availability observation until that worker drains. The worker leaves the
// flight on cancellation, so close then completes without waiting for or
// canceling the import. The flight keeps the import's debit, staging and
// collection exclusion until the daemon exchange itself completes, and the
// canceled image wait never settles an image-pull failure of its own.
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
	mock := &mockDockerClient{
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			if !imported.Load() {
				return nil, errdefs.NotFound(errors.New("image not cached"))
			}
			return &ImageInfo{ID: id.String()}, nil
		},
		CloseFn: func() error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.AllowedRegistries = []string{strings.TrimPrefix(server.URL, "https://")}
	m, daemon, _ := imageCapacityFixture(t)
	// Production composition: flight work shares the backend lifetime.
	m.lifetime = b.stopCtx
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
		// Flight and loader work outlives the lease worker by design, and it
		// still writes the debit ledger and staging under the fixture's
		// directories. Production Stop joins the loader, the flight and the
		// backend workers before closing stores. This cleanup runs before the
		// fixtures close those stores and remove their directories.
		finish()
		require.NoError(t, b.Stop())
	})
	request := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON(ref))
	require.NoError(t, b.Provision(t.Context(), request))
	var work context.Context
	select {
	case work = <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("real provision worker did not dispatch image import")
	}

	// While the test holds the flight registry, the canceled lease worker cannot
	// leave the flight, so it provably has not drained. Every retry must receive
	// the same typed availability observation well before providerd's 30-second
	// HTTP deadline, without canceling the import or settling its debit. The close
	// path must stay free of the flight registry; closeBesideHeldFlightRegistry
	// turns a violation into a named failure instead of a hung test.
	func() {
		m.flights.mu.Lock()
		release := sync.OnceFunc(m.flights.mu.Unlock)
		defer release()
		for range 6 {
			err := closeBesideHeldFlightRegistry(t, b, request.LeaseUUID, release)
			require.True(t, leasesm.IsLifecyclePending(err), "close must observe the exact undrained worker: %v", err)
			require.NoError(t, work.Err())
			pending, err := m.loader.PendingBytes()
			require.NoError(t, err)
			require.Positive(t, pending, "close cannot settle an outstanding daemon exchange")
		}
		require.Len(t, m.flights.active, 1)
		for _, flight := range m.flights.active {
			require.Equal(t, 1, flight.members, "the canceled worker is still the flight's only member")
		}
	}()

	// Released, the worker leaves the flight and drains. Close then completes
	// while the import is still outstanding: only this test can complete it.
	require.NoError(t, deprovisionAfterWorkerDrain(t, t.Context(), b, request.LeaseUUID),
		"close must complete once its own worker drains, without waiting for the flight's import")
	require.NoError(t, work.Err(), "close cannot cancel the flight's dispatched daemon exchange")
	pending, err := m.loader.PendingBytes()
	require.NoError(t, err)
	require.Positive(t, pending, "close cannot settle an outstanding daemon exchange")
	require.Equal(t, imageFlightAccounting{
		workers: 1, flights: 1, members: 0, exclusions: 1, slots: 1,
		staging: m.loader.VerificationBudget().Bytes(),
	}, observeImageFlightAccounting(t, m),
		"the memberless flight keeps its import, staging and collection exclusion; the lease released its own admission")
	callbacks, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, callbacks, 2, "one interrupted operation and one terminal close")
	require.Equal(t, backend.CallbackStatusFailed, callbacks[0].Status)
	require.Equal(t, "operation preempted by lease close", callbacks[0].Error,
		"the worker's canceled image wait cannot settle an image-pull failure after close requested ownership")
	require.Equal(t, backend.CallbackStatusDeprovisioned, callbacks[1].Status)

	// The daemon exchange completes after the close. Join the flight through its
	// owner's shutdown, which returns only after the flight worker has released
	// everything it owned.
	finish()
	drainCtx, cancelDrain := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelDrain()
	require.NoError(t, m.flights.shutdown(drainCtx))
	pending, err = m.loader.PendingBytes()
	require.NoError(t, err)
	require.Zero(t, pending, "the completed daemon exchange settles its own debit")
	require.Equal(t, imageFlightAccounting{}, observeImageFlightAccounting(t, m),
		"the completed flight releases its worker, registry entry, staging and collection exclusion")
	pins, err := m.pins.List()
	require.NoError(t, err)
	require.Empty(t, pins, "a member that left before completion gains no pin from the late import")
	replayed, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Equal(t, callbacks, replayed, "the late import completion cannot publish a settlement")
	require.NoError(t, b.Deprovision(t.Context(), request.LeaseUUID))
	replayed, err = b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Equal(t, callbacks, replayed, "repeated close cannot publish duplicate settlements")
}
