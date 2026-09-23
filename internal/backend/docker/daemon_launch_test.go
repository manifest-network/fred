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
	"github.com/docker/docker/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

func TestDaemonLaunchResponsesSeparateBusinessFailureFromCompletion(t *testing.T) {
	for _, status := range []int{http.StatusCreated, http.StatusConflict, http.StatusForbidden, http.StatusInternalServerError} {
		scope := newDaemonLaunchScope(t.Context(), nil)
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

func TestCompensationSDKAuthorizationDenialRetainsFailureAndCompletesRequest(t *testing.T) {
	observer := new(daemonLaunchObserver)
	requests := 0
	transport := daemonContextTransport{observer: observer, next: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		require.Equal(t, http.MethodPost, req.Method)
		require.True(t, strings.HasSuffix(req.URL.Path, "/containers/source/start"))
		return imageSecurityResponse(http.StatusForbidden, `{"message":"authorization denied by plugin policy: start forbidden"}`), nil
	})}
	sdk, err := client.NewClientWithOpts(client.WithHost("http://docker.invalid"), client.WithVersion("1.51"), client.WithHTTPClient(&http.Client{Transport: transport}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })
	docker := &DockerClient{client: newDockerSDKView(sdk), launchObserver: observer}
	outcome := docker.startCompensationContainer(t.Context(), "source", time.Second)
	require.True(t, outcome.settled, "a trusted daemon's authorization response ends this request")
	require.True(t, errdefs.IsForbidden(outcome.err), "request completion must preserve the denied business outcome")
	require.NoError(t, outcome.completionError())
	require.Equal(t, 1, requests, "completion classification must not replay the denied Start")
}

func TestDaemonLaunchUnknownTransportNeverSuppliesCompletion(t *testing.T) {
	scope := newDaemonLaunchScope(t.Context(), nil)
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
		scope := newDaemonLaunchScope(t.Context(), nil)
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
		scope := newDaemonLaunchScope(t.Context(), nil)
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
		scope.close()
		finished := make(chan daemonLaunchOutcome, 1)
		go func() { finished <- scope.finish(errors.New("Compose returned")) }()
		_, err = transport.RoundTrip(request)
		require.ErrorContains(t, err, "invocation has ended")
		if enterBeforeClose {
			select {
			case <-finished:
				t.Fatal("scope returned before the admitted daemon request drained")
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			<-returned
			require.Equal(t, int64(1), entered.Load())
		} else {
			require.Zero(t, entered.Load(), "closed scope must refuse before network dispatch")
		}
		require.True(t, (<-finished).settled, "admitted exchanges must drain before completion is classified")
	}
}

func TestDaemonLaunchCanceledInvocationRejectsDetachedRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	scope := newDaemonLaunchScope(ctx, nil)
	cancel()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://docker.invalid/v1.51/containers/create", nil)
	require.NoError(t, err)
	_, err = scope.roundTrip(dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("canceled invocation must not admit a detached Compose launch")
		return nil, nil
	}), request)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, scope.finish(err).settled, "refusal before dispatch creates no unknown exchange")
}

func TestDaemonLaunchCancellationDrainsAdmittedExchangeAndClosesBodyContext(t *testing.T) {
	for _, endpoint := range []string{"create", "source/start"} {
		t.Run(endpoint, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			scope := newDaemonLaunchScope(ctx, nil)
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://docker.invalid/v1.51/containers/"+endpoint, nil)
			require.NoError(t, err)
			var exchangeCtx context.Context
			response, err := scope.roundTrip(dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				exchangeCtx = req.Context()
				deadline, bounded := exchangeCtx.Deadline()
				require.True(t, bounded)
				require.InDelta(t, daemonLaunchRequestTimeout.Seconds(), time.Until(deadline).Seconds(), 1)
				cancel()
				require.NoError(t, exchangeCtx.Err(), "preemption must not cancel an admitted exchange")
				status := http.StatusCreated
				if endpoint != "create" {
					status = http.StatusNoContent
				}
				return imageSecurityResponse(status, `{}`), nil
			}), request)
			require.NoError(t, err)
			require.NoError(t, exchangeCtx.Err(), "the SDK still needs to consume the response body")
			require.NoError(t, response.Body.Close())
			require.ErrorIs(t, exchangeCtx.Err(), context.Canceled, "closing the response releases its timeout")
			require.True(t, scope.finish(ctx.Err()).settled)
		})
	}
}

// Model Compose returning cancellation after one admitted Create finishes.
// The actual transport scope supplies completion authority to the durable
// dispatch tests; the mock daemon observes the worker/Stop cancellation itself.
func drainingDaemonLaunchForTest(t *testing.T, ctx context.Context, started chan<- struct{}, effect func()) daemonLaunchOutcome {
	t.Helper()
	scope := newDaemonLaunchScope(ctx, nil)
	defer scope.close()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://docker.invalid/v1.51/containers/create", nil)
	require.NoError(t, err)
	response, err := scope.roundTrip(dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		<-ctx.Done()
		if err := req.Context().Err(); err != nil {
			return nil, err
		}
		if effect != nil {
			effect()
		}
		return imageSecurityResponse(http.StatusCreated, `{"Id":"drained-launch"}`), nil
	}), request)
	if err != nil {
		return scope.finish(err)
	}
	require.NoError(t, response.Body.Close())
	return scope.finish(ctx.Err())
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

func TestCompensationSDKStartDrainsCanceledCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observer := new(daemonLaunchObserver)
	transport := daemonContextTransport{observer: observer, next: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		cancel()
		require.NoError(t, req.Context().Err(), "the SDK's admitted Start must outlive caller cancellation")
		return imageSecurityResponse(http.StatusNoContent, ""), nil
	})}
	sdk, err := client.NewClientWithOpts(client.WithHost("http://docker.invalid"), client.WithVersion("1.51"), client.WithHTTPClient(&http.Client{Transport: transport}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })
	docker := &DockerClient{client: newDockerSDKView(sdk), launchObserver: observer}
	outcome := docker.startCompensationContainer(ctx, "source", time.Second)
	require.True(t, outcome.settled)
	require.NoError(t, outcome.err)
	require.NoError(t, outcome.completionError())
}

func TestDaemonLaunchScopeKeepsReadsCancelable(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	scope := newDaemonLaunchScope(ctx, nil)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker.invalid/v1.51/containers/source/json", nil)
	require.NoError(t, err)
	_, err = scope.roundTrip(dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		cancel()
		require.ErrorIs(t, req.Context().Err(), context.Canceled, "only Create/Start detach admitted requests")
		return nil, req.Context().Err()
	}), request)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, scope.finish(err).settled, "an interrupted read must not create launch debt")
}

func TestDaemonLaunchTransportPanicReleasesRequestContext(t *testing.T) {
	scope := newDaemonLaunchScope(t.Context(), nil)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://docker.invalid/v1.51/containers/create", nil)
	require.NoError(t, err)
	var exchangeCtx context.Context
	require.Panics(t, func() {
		_, _ = scope.roundTrip(dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			exchangeCtx = req.Context()
			panic("transport failure")
		}), request)
	})
	require.NotNil(t, exchangeCtx)
	require.ErrorIs(t, exchangeCtx.Err(), context.Canceled)
	require.False(t, scope.finish(nil).settled, "transport panic leaves physical completion unknown")
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
