package imageexec_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

type runtimeRequestLog struct {
	mu       sync.Mutex
	requests []string
}

func (l *runtimeRequestLog) record(r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.requests = append(l.requests, r.Method+" "+r.URL.Path)
}

func (l *runtimeRequestLog) require(t *testing.T, want []string) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if !reflect.DeepEqual(want, l.requests) {
		t.Fatalf("Docker requests = %v; want %v", l.requests, want)
	}
}

func newRuntimeSDKFixture(t *testing.T, handler http.Handler, opts ...client.Opt) *client.Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	base := []client.Opt{
		client.WithHost(server.URL),
		client.WithHTTPClient(server.Client()),
		client.WithAPIVersionNegotiation(),
		client.WithTimeout(5 * time.Second),
	}
	sdk, err := client.NewClientWithOpts(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdk.Close(); err != nil {
			t.Error(err)
		}
	})
	return sdk
}

func TestDockerRuntimeNegotiatesVersionBeforeGrantingCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name          string
		serverVersion string
		clientVersion string
		wantVersion   string
		wantError     bool
	}{
		{"old server", "1.48", "", "1.48", true},
		{"minimum supported server", "1.49", "", "1.49", false},
		{"current supported server", "1.51", "", "1.51", false},
		{"manual older client", "1.51", "1.48", "1.48", true},
		{"manual newer client on old server", "1.48", "1.51", "1.51", true},
		{"missing server version with current client", "", "1.51", "1.51", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests runtimeRequestLog
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.record(r)
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/_ping":
					w.Header().Set("API-Version", tc.serverVersion)
					w.WriteHeader(http.StatusOK)
				case strings.HasSuffix(r.URL.Path, "/version"):
					if err := json.NewEncoder(w).Encode(map[string]string{"ApiVersion": tc.serverVersion}); err != nil {
						t.Error(err)
					}
				case strings.HasSuffix(r.URL.Path, "/images/app:latest/json"):
					if err := json.NewEncoder(w).Encode(classicImage()); err != nil {
						t.Error(err)
					}
				default:
					http.Error(w, "unexpected Docker operation", http.StatusInternalServerError)
				}
			})
			var opts []client.Opt
			if tc.clientVersion != "" {
				opts = append(opts, client.WithVersion(tc.clientVersion))
			}
			sdk := newRuntimeSDKFixture(t, handler, opts...)
			if tc.clientVersion == "" && sdk.ClientVersion() != "1.51" {
				t.Fatalf("fixture must begin at SDK default API 1.51, got %s", sdk.ClientVersion())
			}
			admitter, creator, err := imageexec.NewDockerRuntime(t.Context(), sdk)
			if sdk.ClientVersion() != tc.wantVersion {
				t.Fatalf("client API = %s; want %s", sdk.ClientVersion(), tc.wantVersion)
			}
			var wantRequests []string
			if tc.clientVersion == "" {
				wantRequests = append(wantRequests, "HEAD /_ping")
			}
			wantRequests = append(wantRequests, "GET /v"+tc.wantVersion+"/version")
			// Construction must query the daemon without inspecting or pulling
			// an image, or creating a workload or inspection helper.
			requests.require(t, wantRequests)
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "1.49") || admitter != nil || creator != nil {
					t.Fatalf("unsupported runtime = %v, %v, %v; want error and no capabilities", admitter, creator, err)
				}
				return
			}
			if err != nil || admitter == nil || creator == nil {
				t.Fatalf("supported runtime = %v, %v, %v", admitter, creator, err)
			}
			image, err := admitter.Admit(t.Context(), "app:latest")
			if err != nil || image.ID() != imageID {
				t.Fatalf("Admit = %s, %v", image.ID(), err)
			}
			wantRequests = append(wantRequests, "GET /v"+tc.wantVersion+"/images/app:latest/json")
			requests.require(t, wantRequests)
		})
	}
}

func TestDockerRuntimeVersionQueryFailureGrantsNoCapabilities(t *testing.T) {
	var requests runtimeRequestLog
	sdk := newRuntimeSDKFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.record(r)
		if r.URL.Path == "/_ping" {
			w.Header().Set("API-Version", "1.51")
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "version query unavailable", http.StatusInternalServerError)
	}))
	admitter, creator, err := imageexec.NewDockerRuntime(t.Context(), sdk)
	if err == nil || !strings.Contains(err.Error(), "version query unavailable") || admitter != nil || creator != nil {
		t.Fatalf("unavailable runtime = %v, %v, %v; want version-query error and no capabilities", admitter, creator, err)
	}
	requests.require(t, []string{"HEAD /_ping", "GET /v1.51/version"})
}

func TestDockerRuntimeConnectionFailureGrantsNoCapabilities(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()
	sdk, err := client.NewClientWithOpts(client.WithHost(server.URL), client.WithAPIVersionNegotiation(), client.WithTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sdk.Close() })
	admitter, creator, err := imageexec.NewDockerRuntime(t.Context(), sdk)
	if err == nil || !client.IsErrConnectionFailed(err) || admitter != nil || creator != nil {
		t.Fatalf("disconnected runtime = %v, %v, %v; want connection error and no capabilities", admitter, creator, err)
	}
}

func TestDockerRuntimeCanceledVersionQueryGrantsNoCapabilities(t *testing.T) {
	var requests runtimeRequestLog
	versionStarted := make(chan struct{})
	sdk := newRuntimeSDKFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.record(r)
		if r.URL.Path == "/_ping" {
			w.Header().Set("API-Version", "1.51")
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/v1.51/version" {
			http.Error(w, "unexpected Docker operation", http.StatusInternalServerError)
			return
		}
		close(versionStarted)
		<-r.Context().Done()
	}))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type result struct {
		admitter *imageexec.Admitter
		creator  *imageexec.DockerCreator
		err      error
	}
	finished := make(chan result, 1)
	go func() {
		a, c, err := imageexec.NewDockerRuntime(ctx, sdk)
		finished <- result{a, c, err}
	}()
	select {
	case <-versionStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not query the daemon version")
	}
	cancel()
	select {
	case got := <-finished:
		if !errors.Is(got.err, context.Canceled) || got.admitter != nil || got.creator != nil {
			t.Fatalf("canceled runtime = %v, %v, %v; want cancellation and no capabilities", got.admitter, got.creator, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime version query did not honor cancellation")
	}
	requests.require(t, []string{"HEAD /_ping", "GET /v1.51/version"})
}

type cancelAfterVersionSource struct {
	*fakeSource
	cancel context.CancelFunc
}

func (s cancelAfterVersionSource) ServerVersion(ctx context.Context) (types.Version, error) {
	version, err := s.fakeSource.ServerVersion(ctx)
	s.cancel()
	return version, err
}

func TestDockerRuntimeCanceledAfterSuccessfulProbeGrantsNoCapabilities(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	source := cancelAfterVersionSource{fakeSource: &fakeSource{version: "1.51"}, cancel: cancel}
	admitter, creator, err := imageexec.NewDockerRuntime(ctx, source)
	if !errors.Is(err, context.Canceled) || admitter != nil || creator != nil {
		t.Fatalf("runtime canceled after successful probe = %v, %v, %v; want cancellation and no capabilities", admitter, creator, err)
	}
}
