package docker

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

const eventCloseLeaseUUID = "0192f1a0-1111-4abc-8def-000000000977"

func newEventDispatchFixture(t *testing.T, mock *mockDockerClient) (*Backend, shared.RuntimeGenerationProof, *bytes.Buffer) {
	t.Helper()
	b := newBackendForTest(mock, map[string]*provision{
		eventCloseLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: eventCloseLeaseUUID, Status: backend.ProvisionStatusReady,
			ContainerIDs: []string{"close-container"},
		}},
	})
	var logs bytes.Buffer
	b.logger = slog.New(slog.NewTextHandler(&logs, nil))
	proof := installReadyRuntimeProofForTest(t, b, eventCloseLeaseUUID)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	return b, proof, &logs
}

func TestContainerEventLoopCloseDeathIsNotDropped(t *testing.T) {
	events := make(chan ContainerEvent)
	eventErrors := make(chan error)
	removing := make(chan struct{})
	releaseRemove := make(chan struct{})
	var releaseOnce sync.Once
	var removed atomic.Bool
	var container ContainerInfo
	mock := &mockDockerClient{
		ContainerEventsFn: func(context.Context) (<-chan ContainerEvent, <-chan error) {
			return events, eventErrors
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if removed.Load() {
				return nil, nil
			}
			return []ContainerInfo{container}, nil
		},
	}
	b, _, logs := newEventDispatchFixture(t, mock)
	var inspections atomic.Int32
	daemon := newInstanceInspectionTestDocker(t, func(w http.ResponseWriter, _ *http.Request) {
		inspections.Add(1)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"No such container"}`))
	})
	b.inspector = &dockerInstanceInspector{docker: projectDockerRead(daemon)}
	b.compose.(*mockComposeExecutor).DownFn = func(ctx context.Context, _ string, _ time.Duration) error {
		close(removing)
		select {
		case <-releaseRemove:
			removed.Store(true)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseRemove) }) })
	projection := b.provisions[eventCloseLeaseUUID]
	container = ContainerInfo{
		ContainerID: "close-container", LeaseUUID: eventCloseLeaseUUID,
		Tenant: projection.Tenant, ProviderUUID: projection.ProviderUUID, BackendName: b.Name(),
		SKU: projection.Items[0].SKU, ServiceName: projection.Items[0].ServiceName,
		CallbackURL: projection.CallbackURL, LifecycleCallbackURL: projection.LifecycleCallbackURL,
		Image: "busybox", Status: "running", CreatedAt: time.Now().Add(-time.Minute),
	}
	before := testutil.ToFloat64(dieEventDroppedTotal.WithLabelValues("event_loop"))
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		b.containerEventLoop()
	}()
	closeDone := make(chan error, 1)
	go func() { closeDone <- b.Deprovision(t.Context(), eventCloseLeaseUUID) }()
	select {
	case <-removing:
	case err := <-closeDone:
		t.Fatalf("close completed before container removal: %v", err)
	case <-time.After(asyncTestResultTimeout):
		t.Fatal("close never reached container removal")
	}
	// Sending the second unbuffered event proves that the loop finished routing
	// the die event while the real close callback was still removing its runtime.
	for _, event := range []ContainerEvent{
		{ContainerID: "close-container", Action: "die"},
		{Action: "test barrier"},
	} {
		select {
		case events <- event:
		case <-loopDone:
			t.Fatal("event loop stopped before consuming the close event")
		case <-time.After(asyncTestResultTimeout):
			t.Fatal("event loop did not consume the close event")
		}
	}
	assert.Equal(t, before, testutil.ToFloat64(dieEventDroppedTotal.WithLabelValues("event_loop")))
	releaseOnce.Do(func() { close(releaseRemove) })
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(asyncTestResultTimeout):
		t.Fatal("close did not complete after container removal")
	}
	b.stopCancel()
	<-loopDone
	b.wg.Wait()
	assert.Zero(t, inspections.Load(), "the close-owned death must not inspect or fail the runtime it is removing")
	assert.NotContains(t, logs.String(), "die event dropped")
	callbacks, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, callbacks, 1)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, callbacks[0].Status)
}

func TestContainerDeathDispatchKeepsRefusalSignal(t *testing.T) {
	for _, scenario := range []string{"recovery", "shutdown", "stale generation", "unreadable authority", "deprovisioning without close"} {
		t.Run(scenario, func(t *testing.T) {
			b, proof, logs := newEventDispatchFixture(t, &mockDockerClient{})
			observation := mustContainerDiedObservation(t, "close-container", proof)
			switch scenario {
			case "recovery":
				claim := b.tryClaimLeaseActorQuiescence(eventCloseLeaseUUID)
				require.NotNil(t, claim)
				defer claim.Release()
			case "shutdown":
				b.stopCancel()
			case "stale generation":
				b.provisions[eventCloseLeaseUUID].ActiveReleaseVersion++
			case "unreadable authority":
				require.NoError(t, b.releaseStore.Close())
			case "deprovisioning without close":
				b.provisions[eventCloseLeaseUUID].Status = backend.ProvisionStatusDeprovisioning
				b.actorFor(eventCloseLeaseUUID)
			}
			for _, source := range []string{"event_loop", "reconcile"} {
				before := testutil.ToFloat64(dieEventDroppedTotal.WithLabelValues(source))
				b.dispatchContainerDeathObservation(observation, "close-container", source)
				assert.Equal(t, before+1, testutil.ToFloat64(dieEventDroppedTotal.WithLabelValues(source)))
			}
			b.stopCancel()
			b.wg.Wait()
			assert.Contains(t, logs.String(), "level=WARN")
			assert.Contains(t, logs.String(), "die event dropped")
		})
	}
}

func TestContainerDeathDispatchCountsFullInbox(t *testing.T) {
	inspecting := make(chan struct{})
	var inspectOnce sync.Once
	b, proof, logs := newEventDispatchFixture(t, &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, _ string) (*ContainerInfo, error) {
			inspectOnce.Do(func() { close(inspecting) })
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	require.True(t, b.routeActorObservation(mustContainerDiedObservation(t, "close-container", proof)))
	select {
	case <-inspecting:
	case <-time.After(asyncTestResultTimeout):
		t.Fatal("actor did not reach its container inspection")
	}
	claim := actorOperationClaimForTest(t, eventCloseLeaseUUID)
	for range b.actorFor(eventCloseLeaseUUID).InboxCap() {
		require.True(t, b.routeToLease(eventCloseLeaseUUID, actorBackpressureCommand(t, claim)))
	}
	before := testutil.ToFloat64(dieEventDroppedTotal.WithLabelValues("event_loop"))
	b.dispatchContainerDeathObservation(mustContainerDiedObservation(t, "close-container", proof), "close-container", "event_loop")
	assert.Equal(t, before+1, testutil.ToFloat64(dieEventDroppedTotal.WithLabelValues("event_loop")))
	b.stopCancel()
	b.wg.Wait()
	assert.Contains(t, logs.String(), "die event dropped")
}
