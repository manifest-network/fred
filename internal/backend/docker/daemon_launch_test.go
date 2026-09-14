package docker

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

func TestDaemonLaunchResponsesSeparateBusinessFailureFromCompletion(t *testing.T) {
	for _, status := range []int{http.StatusCreated, http.StatusConflict, http.StatusInternalServerError} {
		scope := new(daemonLaunchScope)
		transport := daemonLaunchTransport{scope: scope, next: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return imageSecurityResponse(status, `{}`), nil
		})}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://docker.invalid/v1.51/containers/create", nil)
		require.NoError(t, err)
		response, err := transport.RoundTrip(request)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		businessErr := errors.New("daemon rejected requested launch")
		outcome := scope.finish(businessErr)
		require.True(t, outcome.settled)
		require.ErrorIs(t, outcome.err, businessErr)
		require.NoError(t, outcome.completionError())
	}
}

func TestDaemonLaunchUnknownTransportNeverSuppliesCompletion(t *testing.T) {
	scope := new(daemonLaunchScope)
	transport := daemonLaunchTransport{scope: scope, next: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://docker.invalid/v1.51/containers/source/start", nil)
	require.NoError(t, err)
	_, err = transport.RoundTrip(request)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	outcome := scope.finish(err)
	require.False(t, outcome.settled)
	require.Error(t, outcome.completionError())
}

func TestDaemonLaunchGatewayResponsesRemainUnknown(t *testing.T) {
	for _, status := range []int{http.StatusMovedPermanently, http.StatusFound, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		scope := new(daemonLaunchScope)
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://docker.invalid/v1.51/containers/create", nil)
		require.NoError(t, err)
		response, err := scope.roundTrip(dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) { return imageSecurityResponse(status, `{}`), nil }), request)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		require.False(t, scope.finish(errors.New("gateway failure")).settled)
	}
}

func TestUnboundDockerClientCannotSupplyLaunchCompletion(t *testing.T) {
	docker := new(DockerClient)
	require.False(t, docker.startCompensationContainer(t.Context(), "source", time.Second).settled)
	_, created := docker.createCompensationContainer(t.Context(), imageexec.Image{}, compensationContainer{})
	require.False(t, created.settled)
}

func TestComposeHTTPTransportExcludesEnvironmentProxy(t *testing.T) {
	transport, err := newComposeHTTPTransport("tcp://docker.example:2375")
	require.NoError(t, err)
	t.Cleanup(transport.CloseIdleConnections)
	require.Nil(t, transport.Proxy)
}

func TestDaemonLaunchScopeFencesDetachedAndLateRequests(t *testing.T) {
	for _, enterBeforeClose := range []bool{false, true} {
		scope := new(daemonLaunchScope)
		var entered atomic.Int64
		started, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
		transport := daemonLaunchTransport{scope: scope, next: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			entered.Add(1)
			close(started)
			<-release
			return imageSecurityResponse(http.StatusCreated, `{"Id":"late-source"}`), nil
		})}
		// A Compose child may intentionally replace its context. The transport
		// instance still binds it to this exact invocation and closes admission.
		request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://docker.invalid/v1.51/containers/create", nil)
		require.NoError(t, err)
		if enterBeforeClose {
			go func() {
				defer close(returned)
				response, err := transport.RoundTrip(request)
				if err == nil {
					_ = response.Body.Close()
				}
			}()
			<-started
		}
		outcome := scope.finish(errors.New("Compose returned"))
		require.Equal(t, !enterBeforeClose, outcome.settled)
		_, err = transport.RoundTrip(request)
		require.ErrorContains(t, err, "invocation has ended")
		if enterBeforeClose {
			close(release)
			<-returned
			require.False(t, outcome.settled, "a late response cannot change the already returned proof")
			require.Equal(t, int64(1), entered.Load())
		} else {
			require.Zero(t, entered.Load(), "closed scope must refuse before network dispatch")
		}
	}
}

func TestCompensationSDKStartPreservesRequestCompletionObservation(t *testing.T) {
	for _, transportFails := range []bool{false, true} {
		observer := new(daemonLaunchObserver)
		transport := daemonContextTransport{observer: observer, next: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			require.True(t, strings.HasSuffix(req.URL.Path, "/containers/source/start"))
			if transportFails {
				return nil, context.DeadlineExceeded
			}
			return imageSecurityResponse(http.StatusConflict, `{"message":"container start rejected"}`), nil
		})}
		sdk, err := client.NewClientWithOpts(client.WithHost("http://docker.invalid"), client.WithVersion("1.51"), client.WithHTTPClient(&http.Client{Transport: transport}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sdk.Close() })
		docker := &DockerClient{client: newDockerSDKView(sdk), launchObserver: observer}
		outcome := docker.startCompensationContainer(t.Context(), "source", time.Second)
		require.Equal(t, !transportFails, outcome.settled)
		require.Error(t, outcome.err)
	}
}

func TestCompensationSDKCreatePreservesRequestCompletionObservation(t *testing.T) {
	for _, transportFails := range []bool{false, true} {
		docker := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.Path, "/images/") {
				return imageSecurityResponse(http.StatusOK, platformSecurityJSON(testImageID, ocispec.MediaTypeImageManifest)), nil
			}
			require.True(t, strings.HasSuffix(req.URL.Path, "/containers/create"))
			if transportFails {
				return nil, context.DeadlineExceeded
			}
			return imageSecurityResponse(http.StatusConflict, `{"message":"container name occupied"}`), nil
		})
		image, err := docker.AdmitImage(t.Context(), testImageID)
		require.NoError(t, err)
		_, outcome := docker.createCompensationContainer(t.Context(), image, compensationContainer{Name: "source", Config: &container.Config{Labels: map[string]string{LabelLeaseUUID: "source-lease"}}})
		require.Equal(t, !transportFails, outcome.settled)
		require.Error(t, outcome.err)
	}
}
