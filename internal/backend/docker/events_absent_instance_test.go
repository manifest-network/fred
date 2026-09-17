package docker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/errdefs"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func TestContainerEventLoopDaemonAbsentInstancePublishesFailure(t *testing.T) {
	const leaseUUID = "0192f1a0-1111-4abc-8def-000000000988"
	const instanceID = "removed-instance"
	eventRequested := make(chan struct{})
	sendEvent := make(chan struct{})
	var inspections, logRequests atomic.Int32
	daemon := newInstanceInspectionTestDocker(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/events"):
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			close(eventRequested)
			select {
			case <-sendEvent:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write([]byte(`{"Type":"container","Action":"die","Actor":{"ID":"removed-instance"}}` + "\n"))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case strings.HasSuffix(r.URL.Path, "/containers/"+instanceID+"/json"):
			inspections.Add(1)
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"No such container: removed-instance"}`))
		default:
			t.Errorf("unexpected Docker request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	callbacks := make(chan backend.CallbackPayload, 1)
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode lifecycle callback: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		callbacks <- payload
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()
	mock := &mockDockerClient{
		ContainerEventsFn: daemon.ContainerEvents,
		ContainerLogsFn: func(context.Context, string, int) (string, error) {
			logRequests.Add(1)
			return "", errors.New("removed containers have no logs")
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: leaseUUID, Status: backend.ProvisionStatusReady,
			ContainerIDs: []string{instanceID}, CallbackURL: callbackServer.URL,
		}},
	})
	installReadyRuntimeProofForTest(t, b, leaseUUID)
	b.inspector = &dockerInstanceInspector{docker: projectDockerRead(daemon)}
	rebuildCallbackSender(b, callbackServer.Client())
	startCallbackReplayForTest(b)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		b.containerEventLoop()
	}()
	t.Cleanup(func() {
		b.stopCancel()
		<-loopDone
		b.wg.Wait()
	})
	select {
	case <-eventRequested:
	case <-time.After(asyncTestResultTimeout):
		t.Fatal("event stream did not connect")
	}
	droppedBefore := testutil.ToFloat64(dieEventDroppedTotal.WithLabelValues("event_loop"))
	close(sendEvent)
	select {
	case payload := <-callbacks:
		assert.Equal(t, backend.CallbackStatusFailed, payload.Status)
		assert.Equal(t, leaseUUID, payload.LeaseUUID)
	case <-time.After(asyncTestResultTimeout):
		t.Fatal("daemon die event followed by exact 404 did not publish failure without reconciliation")
	}
	provisions, err := b.LookupProvisions(t.Context(), []string{leaseUUID})
	require.NoError(t, err)
	require.Len(t, provisions, 1)
	assert.Equal(t, backend.ProvisionStatusFailed, provisions[0].Status)
	assert.Equal(t, backend.ReasonContainerExited, provisions[0].Reason)
	assert.Equal(t, 1, provisions[0].FailCount)
	assert.Equal(t, int32(1), inspections.Load())
	assert.Zero(t, logRequests.Load(), "absence must not fetch unavailable container logs")
	assert.Equal(t, droppedBefore, testutil.ToFloat64(dieEventDroppedTotal.WithLabelValues("event_loop")))
}

func TestContainerEventLoopAbsenceStillRequiresCurrentRuntime(t *testing.T) {
	for _, scenario := range []string{"permission", "forged not found", "generation replaced during inspection", "authority unreadable during inspection", "shutdown during inspection"} {
		t.Run(scenario, func(t *testing.T) {
			events := make(chan ContainerEvent, 1)
			inspecting := make(chan struct{})
			var b *Backend
			daemon := newInstanceInspectionTestDocker(t, func(w http.ResponseWriter, r *http.Request) {
				close(inspecting)
				switch scenario {
				case "permission":
					w.WriteHeader(http.StatusForbidden)
				case "generation replaced during inspection":
					b.provisionsMu.Lock()
					b.provisions[eventCloseLeaseUUID].ActiveReleaseVersion++
					b.provisionsMu.Unlock()
					w.WriteHeader(http.StatusNotFound)
				case "authority unreadable during inspection":
					assert.NoError(t, b.releaseStore.Close())
					w.WriteHeader(http.StatusNotFound)
				case "shutdown during inspection":
					b.stopCancel()
					<-r.Context().Done()
					return
				default:
					t.Error("forged inspection must not reach the daemon")
					w.WriteHeader(http.StatusNotFound)
				}
				_, _ = w.Write([]byte(`{"message":"No such container"}`))
			})
			mock := &mockDockerClient{
				ContainerEventsFn: func(context.Context) (<-chan ContainerEvent, <-chan error) {
					return events, make(chan error)
				},
				InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
					close(inspecting)
					return nil, errdefs.NotFound(errors.New("No such container"))
				},
			}
			b, _, _ = newEventDispatchFixture(t, mock)
			if scenario != "forged not found" {
				b.inspector = &dockerInstanceInspector{docker: projectDockerRead(daemon)}
			}
			actor := b.actorFor(eventCloseLeaseUUID)
			loopDone := make(chan struct{})
			go func() {
				defer close(loopDone)
				b.containerEventLoop()
			}()
			t.Cleanup(func() { b.stopCancel(); <-loopDone })
			events <- ContainerEvent{ContainerID: "close-container", Action: "die"}
			select {
			case <-inspecting:
			case <-time.After(asyncTestResultTimeout):
				t.Fatal("event did not reach inspection")
			}
			require.Eventually(t, func() bool { return actor.CurrentMessageStart() == 0 }, asyncTestResultTimeout, time.Millisecond)
			b.provisionsMu.RLock()
			assert.Equal(t, backend.ProvisionStatusReady, b.provisions[eventCloseLeaseUUID].Status)
			assert.Zero(t, b.provisions[eventCloseLeaseUUID].FailCount)
			b.provisionsMu.RUnlock()
			pending, err := b.callbackStore.ListPending()
			require.NoError(t, err)
			assert.Empty(t, pending, "unknown inspection or stale generation must not publish a lifecycle failure")
		})
	}
}
