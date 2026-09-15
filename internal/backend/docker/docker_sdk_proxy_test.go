package docker

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ProxyFromEnvironment caches process environment. A fresh test subprocess
// makes the proxy assertion independent of other HTTP tests and exercises the
// actual SDK option ordering and its constructor-owned observer transport.
func TestDockerSDKExcludesEnvironmentProxy(t *testing.T) {
	const endpointEnv = "FRED_TEST_DIRECT_DOCKER_ENDPOINT"
	if endpoint := os.Getenv(endpointEnv); endpoint != "" {
		request, err := http.NewRequest(http.MethodPost, endpoint+"/v1.51/containers/source/start", nil)
		require.NoError(t, err)
		proxy, err := http.ProxyFromEnvironment(request)
		require.NoError(t, err)
		require.NotNil(t, proxy, "the fixture endpoint must be routed through the environment proxy by a normal transport")
		require.Equal(t, os.Getenv("HTTP_PROXY"), proxy.String())
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		docker, err := NewDockerClient(ctx, "tcp://"+strings.TrimPrefix(endpoint, "http://"), "proxy-test")
		require.NoError(t, err)
		defer docker.Close()
		outcome := docker.startCompensationContainer(ctx, "source", 10*time.Second)
		require.True(t, outcome.settled)
		require.ErrorContains(t, outcome.err, "direct daemon refusal")
		return
	}

	var daemonStarts, proxyRequests atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyRequests.Add(1)
		http.Error(w, `{"message":"proxy-generated response"}`, http.StatusInternalServerError)
	}))
	defer proxy.Close()
	// Unlike a loopback IP, 0.0.0.0 is not exempt from ProxyFromEnvironment.
	// It still reaches only this local listener, without DNS or external access.
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	require.NoError(t, err)
	daemon := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/_ping":
			w.Header().Set("API-Version", "1.51")
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/version"):
			_ = json.NewEncoder(w).Encode(map[string]string{"ApiVersion": "1.51"})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/containers/source/start"):
			daemonStarts.Add(1)
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"direct daemon refusal"}`))
		default:
			http.Error(w, "unexpected Docker request", http.StatusNotFound)
		}
	}))
	_ = daemon.Listener.Close()
	daemon.Listener = listener
	daemon.Start()
	defer daemon.Close()
	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestDockerSDKExcludesEnvironmentProxy$", "-test.count=1")
	command.Env = append(os.Environ(), endpointEnv+"="+daemon.URL,
		"HTTP_PROXY="+proxy.URL, "HTTPS_PROXY="+proxy.URL, "http_proxy="+proxy.URL, "https_proxy="+proxy.URL,
		"NO_PROXY=", "no_proxy=", "REQUEST_METHOD=")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Zero(t, proxyRequests.Load(), "a gateway cannot replace the configured daemon's completion evidence")
	require.Equal(t, int32(1), daemonStarts.Load(), "the real SDK must reach the configured daemon")
}
