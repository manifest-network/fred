package docker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/errdefs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// The production constructor binds the real SDK to this HTTP daemon. No test
// can mint absence by returning a NotFound-shaped error from a mock inspector.
func newInstanceInspectionTestDocker(t *testing.T, handler http.HandlerFunc) *DockerClient {
	t.Helper()
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/_ping":
			w.Header().Set("API-Version", "1.51")
		case strings.HasSuffix(r.URL.Path, "/version"):
			_ = json.NewEncoder(w).Encode(map[string]string{"ApiVersion": "1.51"})
		default:
			handler(w, r)
		}
	}))
	t.Cleanup(daemon.Close)
	client, err := NewDockerClient(t.Context(), "tcp://"+strings.TrimPrefix(daemon.URL, "http://"), "inspect-test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func TestDockerInstanceInspectionRequiresExactDaemonAbsence(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusInternalServerError, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := newInstanceInspectionTestDocker(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.True(t, strings.HasSuffix(r.URL.Path, "/containers/exact-instance/json"))
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"message":"daemon inspection refusal"}`))
			})
			// Nested projections must preserve the constructor's read-only seam.
			inspector := &dockerInstanceInspector{docker: projectDockerRead(projectDockerRead(client))}
			state, err := inspector.InspectInstance(t.Context(), "exact-instance")
			if status == http.StatusNotFound {
				require.NoError(t, err)
				require.NotNil(t, state)
				assert.Equal(t, leasesm.PhaseAbsent, state.Phase)
				assert.Nil(t, state.ExitCode, "physical absence does not establish an exit code")
			} else {
				require.Error(t, err)
				assert.Nil(t, state)
			}
			// Other InspectContainer consumers keep the ordinary SDK error.
			info, err := client.InspectContainer(t.Context(), "exact-instance")
			require.Error(t, err)
			assert.Nil(t, info)
		})
	}
}

func TestDockerInstanceInspectionRejectsForgedAbsence(t *testing.T) {
	for _, forged := range []error{errdefs.NotFound(errors.New("No such container")), context.DeadlineExceeded, errors.New("No such container")} {
		t.Run(forged.Error(), func(t *testing.T) {
			mock := &mockDockerClient{InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
				return nil, forged
			}}
			state, err := (&dockerInstanceInspector{docker: projectDockerRead(mock)}).InspectInstance(t.Context(), "exact-instance")
			require.ErrorIs(t, err, forged)
			assert.Nil(t, state)

			// Even replacing the SDK call behind a concrete adapter cannot mint
			// absence without a matching exchange through its bound transport.
			client := newInstanceInspectionTestDocker(t, func(http.ResponseWriter, *http.Request) {
				t.Error("the forged SDK method must not reach the daemon")
			})
			client.client.containerInspect = func(context.Context, string) (container.InspectResponse, error) {
				return container.InspectResponse{}, forged
			}
			state, err = (&dockerInstanceInspector{docker: client}).InspectInstance(t.Context(), "exact-instance")
			require.ErrorIs(t, err, forged)
			assert.Nil(t, state)
		})
	}
}

func TestDockerInstanceInspectionBindsRequestAndClient(t *testing.T) {
	for _, scenario := range []string{"another instance", "another endpoint", "another client"} {
		t.Run(scenario, func(t *testing.T) {
			respondAbsent := func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"message":"No such container"}`))
			}
			client := newInstanceInspectionTestDocker(t, respondAbsent)
			inspect := client.client.containerInspect
			switch scenario {
			case "another instance":
				client.client.containerInspect = func(ctx context.Context, _ string) (container.InspectResponse, error) {
					return inspect(ctx, "another-instance")
				}
			case "another endpoint":
				client.client.containerInspect = func(ctx context.Context, _ string) (container.InspectResponse, error) {
					_, err := client.client.VolumeInspect(ctx, "exact-instance")
					return container.InspectResponse{}, err
				}
			case "another client":
				other := newInstanceInspectionTestDocker(t, respondAbsent)
				client.client.containerInspect = other.client.containerInspect
			}
			state, err := (&dockerInstanceInspector{docker: client}).InspectInstance(t.Context(), "exact-instance")
			require.Error(t, err)
			assert.Nil(t, state)
		})
	}
}

func TestDockerInstanceInspectionCancellationDoesNotProveAbsence(t *testing.T) {
	started := make(chan struct{})
	client := newInstanceInspectionTestDocker(t, func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		state, err := (&dockerInstanceInspector{docker: client}).InspectInstance(ctx, "exact-instance")
		assert.Nil(t, state)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(asyncTestResultTimeout):
		t.Fatal("inspection never reached the daemon")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(asyncTestResultTimeout):
		t.Fatal("inspection did not honor its caller cancellation")
	}
}
