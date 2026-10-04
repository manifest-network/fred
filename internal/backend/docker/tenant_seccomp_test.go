package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/docker/tenantseccomp"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func tenantSeccompTestProfiles() imageexec.TenantSeccompSource {
	return observedTenantSeccomp{source: tenantseccomp.Process()}
}

func currentTenantProfile(t *testing.T) tenantseccomp.Profile {
	t.Helper()
	profile, err := tenantseccomp.Process().TenantSeccompProfile()
	require.NoError(t, err)
	return profile
}

// tenantSeccompSecurityOpt is what every fred creation sends: the caller's
// options plus exactly one inline copy of the current profile.
func tenantSeccompSecurityOpt(t *testing.T, options ...string) []string {
	t.Helper()
	final, err := tenantseccomp.InlineSecurityOpt(options, currentTenantProfile(t))
	require.NoError(t, err)
	return final
}

func tenantSeccompCreateBody(t *testing.T, securityOpt []string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"Image": fixtureImageID("seccomp"), "Labels": map[string]string{"k": "v"},
		"HostConfig": map[string]any{"CapDrop": []string{"ALL"}, "SecurityOpt": securityOpt},
	})
	require.NoError(t, err)
	return body
}

// tenantSeccompCreateRequest builds a create request whose body requests the
// given options, or the current tenant profile when securityOpt is nil.
func tenantSeccompCreateRequest(t *testing.T, ctx context.Context, securityOpt []string) *http.Request {
	t.Helper()
	if securityOpt == nil {
		securityOpt = tenantSeccompSecurityOpt(t, "no-new-privileges:true")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://docker.invalid/v1.51/containers/create?name=fred-test", bytes.NewReader(tenantSeccompCreateBody(t, securityOpt)))
	require.NoError(t, err)
	return request
}

type failingTenantSeccomp struct{ calls atomic.Int64 }

func (f *failingTenantSeccomp) TenantSeccompProfile() (tenantseccomp.Profile, error) {
	f.calls.Add(1)
	return tenantseccomp.Profile{}, fmt.Errorf("%w: test source cannot build the profile", tenantseccomp.ErrRefused)
}

// switchableTenantSeccomp serves the process profile until it is switched off.
type switchableTenantSeccomp struct{ off atomic.Bool }

func (s *switchableTenantSeccomp) TenantSeccompProfile() (tenantseccomp.Profile, error) {
	if s.off.Load() {
		return tenantseccomp.Profile{}, fmt.Errorf("%w: test source switched off", tenantseccomp.ErrRefused)
	}
	return tenantseccomp.Process().TenantSeccompProfile()
}

func TestTenantSeccompSinkLabelsAreClosed(t *testing.T) {
	seen := map[string]bool{}
	for _, sink := range tenantSeccompSinks {
		label := sink.label()
		require.NotEmpty(t, label)
		require.False(t, seen[label], "duplicate sink label %q", label)
		seen[label] = true
		require.GreaterOrEqual(t, testutil.ToFloat64(tenantSeccompProfileRefusalsTotal.WithLabelValues(label)), 0.0)
	}
	require.Empty(t, tenantSeccompSinkInvalid.label())
	require.Empty(t, tenantSeccompSink(255).label())
	require.Len(t, seen, 7)
}

// Every intent that recreates tenant containers has its own counted sink,
// including the provider-initiated redeploy that applies a custom domain.
func TestTenantSeccompSinksCoverEveryRecreatingIntent(t *testing.T) {
	sinks := map[tenantSeccompSink]bool{}
	for kind, want := range map[shared.MaintenanceIntentKind]string{
		shared.MaintenanceIntentRestart:      "restart",
		shared.MaintenanceIntentUpdate:       "update",
		shared.MaintenanceIntentCustomDomain: "custom_domain",
	} {
		sink := tenantSeccompMaintenanceSink(kind)
		require.Equal(t, want, sink.label(), string(kind))
		sinks[sink] = true
	}
	for kind, want := range map[shared.OperationIntentKind]string{
		shared.OperationIntentProvision: "provision",
		shared.OperationIntentRestore:   "restore",
	} {
		sink := tenantSeccompOperationSink(kind)
		require.Equal(t, want, sink.label(), string(kind))
		sinks[sink] = true
	}
	require.Len(t, sinks, 5)
	require.Equal(t, tenantSeccompSinkInvalid, tenantSeccompMaintenanceSink("unknown"))
	require.Equal(t, tenantSeccompSinkInvalid, tenantSeccompOperationSink("unknown"))
	require.Equal(t, tenantSeccompSinkInvalid, (*storageMutations)(nil).tenantSeccompSink())

	counter := tenantSeccompProfileRefusalsTotal.WithLabelValues("custom_domain")
	before := testutil.ToFloat64(counter)
	refused := refuseWithoutTenantSeccomp(tenantSeccompMaintenanceSink(shared.MaintenanceIntentCustomDomain),
		fmt.Errorf("%w: test", tenantseccomp.ErrRefused))
	var authored *physicalOperationError
	require.ErrorAs(t, refused, &authored)
	require.Equal(t, backend.ReasonInternal, authored.reason)
	require.Equal(t, before+1, testutil.ToFloat64(counter), "a refused custom-domain redeploy is counted")
}

// A daemon that reports no seccomp support refuses a custom profile, so the
// readiness gauge reports 0 until it reports seccomp again; creation is still
// left to the daemon.
func TestDaemonWithoutSeccompReportsTheProfileUnready(t *testing.T) {
	var securityOptions atomic.Value
	securityOptions.Store([]string{"name=apparmor", "name=cgroupns"})
	daemon := newTenantSeccompTestDaemon(t, func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, "/info") {
			return false
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ID": "daemon", "SecurityOptions": securityOptions.Load()})
		return true
	})
	docker, err := newDockerClient(t.Context(), daemon, "seccomp-daemon-test", tenantseccomp.Process())
	require.NoError(t, err)
	t.Cleanup(func() { _ = docker.Close() })
	t.Cleanup(func() {
		_, _ = tenantSeccompTestProfiles().TenantSeccompProfile() // restore the gauge for later tests
	})

	require.NoError(t, docker.requireTenantSeccompProfile())
	require.Equal(t, 1.0, testutil.ToFloat64(tenantSeccompProfileReady))
	_, err = docker.DaemonInfo(t.Context())
	require.NoError(t, err)
	require.Equal(t, 0.0, testutil.ToFloat64(tenantSeccompProfileReady), "a report without seccomp drops readiness at once")
	require.NoError(t, docker.requireTenantSeccompProfile(), "the daemon, not fred, refuses the launch")
	require.Equal(t, 0.0, testutil.ToFloat64(tenantSeccompProfileReady), "a profile request keeps it at 0")

	securityOptions.Store([]string{"name=apparmor", "name=seccomp,profile=builtin"})
	_, err = docker.DaemonInfo(t.Context())
	require.NoError(t, err)
	require.NoError(t, docker.requireTenantSeccompProfile())
	require.Equal(t, 1.0, testutil.ToFloat64(tenantSeccompProfileReady))
}

func TestDaemonReportsSeccomp(t *testing.T) {
	require.True(t, daemonReportsSeccomp([]string{"name=apparmor", "name=seccomp,profile=builtin"}))
	require.True(t, daemonReportsSeccomp([]string{"name=seccomp,profile=default"}))
	require.False(t, daemonReportsSeccomp([]string{"name=apparmor", "name=cgroupns"}))
	require.False(t, daemonReportsSeccomp(nil))
}

func TestRefuseWithoutTenantSeccompAuthorsOneProviderFault(t *testing.T) {
	other := errors.New("daemon rejected the create")
	require.Same(t, other, refuseWithoutTenantSeccomp(tenantSeccompSinkProvision, other))
	require.NoError(t, refuseWithoutTenantSeccomp(tenantSeccompSinkProvision, nil))

	counter := tenantSeccompProfileRefusalsTotal.WithLabelValues("compensation")
	before := testutil.ToFloat64(counter)
	cause := fmt.Errorf("create: %w", fmt.Errorf("%w: test", tenantseccomp.ErrRefused))
	refused := refuseWithoutTenantSeccomp(tenantSeccompSinkCompensation, cause)
	var authored *physicalOperationError
	require.ErrorAs(t, refused, &authored)
	require.Equal(t, backend.ReasonInternal, authored.reason)
	require.Equal(t, msgTenantSeccompUnavailable, authored.callback)
	require.ErrorIs(t, refused, tenantseccomp.ErrRefused)
	require.Equal(t, before+1, testutil.ToFloat64(counter))

	// A refusal already authored deeper is neither re-authored nor re-counted.
	wrapped := fmt.Errorf("launch: %w", refused)
	require.Same(t, wrapped, refuseWithoutTenantSeccomp(tenantSeccompSinkProvision, wrapped))
	require.Equal(t, before+1, testutil.ToFloat64(counter))
}

// A launch that fails without a positive container failure reports a generic
// creation failure, except for a creation refused for the tenant seccomp
// profile, which keeps the provider fault authored where it was raised
// (ENG-1118 with ENG-1125). No other authored failure is inherited.
func TestLaunchRejectedFailureKeepsOnlyATenantSeccompRefusal(t *testing.T) {
	refusal := refuseWithoutTenantSeccomp(tenantSeccompSinkInvalid,
		fmt.Errorf("%w: test", tenantseccomp.ErrRefused))
	kept := launchRejectedFailure(fmt.Errorf("compose: %w", refusal))
	require.Equal(t, msgTenantSeccompUnavailable, kept.callback)
	require.Equal(t, backend.ReasonInternal, kept.reason)
	require.ErrorIs(t, kept, tenantseccomp.ErrRefused, "the refusal stays in the operator detail")
	// The attempt's failure capture, which recovery publishes for an
	// ambiguous attempt, reads the same surface.
	captured := failureObservation(errors.Join(kept, errors.New("cleanup detail")))
	require.Equal(t, msgTenantSeccompUnavailable, captured.Message)
	require.Equal(t, backend.ReasonInternal, captured.Reason)

	generic := launchRejectedFailure(errors.New("daemon rejected the create"))
	require.Equal(t, "container creation failed", generic.callback)
	require.Equal(t, backend.ReasonInternal, generic.reason)

	exited := &physicalOperationError{callback: "container exited during startup",
		reason: backend.ReasonContainerExited, cause: errors.New("exit 1")}
	notInherited := launchRejectedFailure(fmt.Errorf("compose: %w", exited))
	require.Equal(t, "container creation failed", notInherited.callback)
	require.Equal(t, backend.ReasonInternal, notInherited.reason, "a launch never inherits another authored reason")

	bare := launchRejectedFailure(fmt.Errorf("%w: not authored", tenantseccomp.ErrRefused))
	require.Equal(t, "container creation failed", bare.callback,
		"only the authored refusal carries the provider-fault surface")
}

func TestObservedTenantSeccompReportsReadiness(t *testing.T) {
	_, err := observedTenantSeccomp{source: &failingTenantSeccomp{}}.TenantSeccompProfile()
	require.ErrorIs(t, err, tenantseccomp.ErrRefused)
	require.Equal(t, 0.0, testutil.ToFloat64(tenantSeccompProfileReady))
	_, err = tenantSeccompTestProfiles().TenantSeccompProfile()
	require.NoError(t, err)
	require.Equal(t, 1.0, testutil.ToFloat64(tenantSeccompProfileReady))
	_, err = observedTenantSeccomp{}.TenantSeccompProfile()
	require.ErrorIs(t, err, tenantseccomp.ErrRefused)
	require.Equal(t, 0.0, testutil.ToFloat64(tenantSeccompProfileReady))
	_, err = tenantSeccompTestProfiles().TenantSeccompProfile()
	require.NoError(t, err)
}

func TestLaunchWireCheckRefusesUnscopedCreateWithoutTheProfile(t *testing.T) {
	observer := new(daemonLaunchObserver)
	dispatched := 0
	transport := daemonContextTransport{observer: observer, profiles: tenantSeccompTestProfiles(),
		next: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			dispatched++
			return imageSecurityResponse(http.StatusCreated, `{"Id":"created"}`), nil
		})}
	valid := tenantSeccompSecurityOpt(t, "no-new-privileges:true")
	for name, securityOpt := range map[string][]string{
		"no entry":              {"no-new-privileges:true"},
		"two entries":           append(append([]string(nil), valid...), valid[len(valid)-1]),
		"valid then unconfined": append(append([]string(nil), valid...), "seccomp:unconfined"),
		"unconfined":            {"seccomp=unconfined"},
		"builtin":               {"seccomp=builtin"},
		"another profile":       {`seccomp={"defaultAction":"SCMP_ACT_ALLOW"}`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := transport.RoundTrip(tenantSeccompCreateRequest(t, t.Context(), securityOpt))
			require.ErrorIs(t, err, tenantseccomp.ErrRefused)
		})
	}
	require.Zero(t, dispatched, "a refused create must never reach the daemon")

	// No body to copy, an oversized body, or no profile source: refused too.
	noCopy, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://docker.invalid/v1.51/containers/create", io.NopCloser(strings.NewReader("{}")))
	require.NoError(t, err)
	_, err = transport.RoundTrip(noCopy)
	require.ErrorIs(t, err, tenantseccomp.ErrRefused)
	oversized, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://docker.invalid/v1.51/containers/create",
		bytes.NewReader(bytes.Repeat([]byte(" "), maxLaunchCreateBody+1)))
	require.NoError(t, err)
	_, err = transport.RoundTrip(oversized)
	require.ErrorIs(t, err, tenantseccomp.ErrRefused)
	_, err = daemonContextTransport{observer: observer, next: transport.next}.RoundTrip(tenantSeccompCreateRequest(t, t.Context(), nil))
	require.ErrorIs(t, err, tenantseccomp.ErrRefused, "a transport without a profile source refuses every create")
	require.Zero(t, dispatched)

	// Other requests are not creates and pass untouched.
	start, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://docker.invalid/v1.51/containers/abc/start", nil)
	require.NoError(t, err)
	response, err := daemonContextTransport{observer: observer, next: transport.next}.RoundTrip(start)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, 1, dispatched)
}

func TestLaunchWireCheckLeavesTheBodyForTheDaemon(t *testing.T) {
	request := tenantSeccompCreateRequest(t, t.Context(), nil)
	want, err := io.ReadAll(must(t, request.GetBody))
	require.NoError(t, err)
	var received []byte
	transport := daemonContextTransport{observer: new(daemonLaunchObserver), profiles: tenantSeccompTestProfiles(),
		next: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			var readErr error
			received, readErr = io.ReadAll(req.Body)
			require.NoError(t, readErr)
			return imageSecurityResponse(http.StatusCreated, `{"Id":"created"}`), nil
		})}
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, want, received, "the daemon must receive the complete, unread body")
}

func must(t *testing.T, open func() (io.ReadCloser, error)) io.ReadCloser {
	t.Helper()
	body, err := open()
	require.NoError(t, err)
	t.Cleanup(func() { _ = body.Close() })
	return body
}

// The retained Compose engine only lists and tears down. Its client refuses
// every container create before dispatch, with or without the profile, and
// passes every other request.
func TestComposeReadEngineRefusesEveryCreate(t *testing.T) {
	var dispatched []string
	client := newComposeReadHTTPClient(dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		dispatched = append(dispatched, req.Method+" "+req.URL.Path)
		return imageSecurityResponse(http.StatusOK, `[]`), nil
	}))
	for name, securityOpt := range map[string][]string{
		"with the profile":    tenantSeccompSecurityOpt(t, "no-new-privileges:true"),
		"without the profile": {"no-new-privileges:true"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := client.Do(tenantSeccompCreateRequest(t, t.Context(), securityOpt))
			require.ErrorIs(t, err, errComposeReadEngineCreate)
		})
	}
	require.Empty(t, dispatched, "no create reaches the daemon")

	for _, request := range []struct{ method, path string }{
		{http.MethodGet, "/v1.51/containers/json"},
		{http.MethodPost, "/v1.51/containers/abc/stop"},
		{http.MethodDelete, "/v1.51/containers/abc"},
	} {
		req, err := http.NewRequestWithContext(t.Context(), request.method, "http://docker.invalid"+request.path, nil)
		require.NoError(t, err)
		response, err := client.Do(req)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
	}
	require.Equal(t, []string{"GET /v1.51/containers/json", "POST /v1.51/containers/abc/stop", "DELETE /v1.51/containers/abc"}, dispatched)
}

// A Compose create carrying the profile passes; one without it settles as a
// refusal before the launch scope admits it, so no unknown exchange remains.
func TestComposeLaunchTransportChecksCreatesBeforeAdmission(t *testing.T) {
	scope := newDaemonLaunchScope(t.Context(), nil)
	dispatched := 0
	transport := daemonLaunchTransport{scope: scope, profiles: tenantSeccompTestProfiles(),
		next: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			dispatched++
			return imageSecurityResponse(http.StatusCreated, `{"Id":"created"}`), nil
		})}
	response, err := transport.RoundTrip(tenantSeccompCreateRequest(t, t.Context(), nil))
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	_, err = transport.RoundTrip(tenantSeccompCreateRequest(t, t.Context(), []string{"no-new-privileges:true"}))
	require.ErrorIs(t, err, tenantseccomp.ErrRefused)
	require.Equal(t, 1, dispatched)
	outcome := scope.finish(err)
	require.True(t, outcome.settled, "a refusal before admission leaves no unknown daemon exchange")
	require.ErrorIs(t, outcome.err, tenantseccomp.ErrRefused)
}

// The real client sends every direct create through the wire check with the
// profile its creator injected; a source that cannot build the profile stops
// the create before any request is made.
func TestDockerClientCreatesCarryTheProfileOnTheWire(t *testing.T) {
	var bodies [][]byte
	docker := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/images/") {
			return imageSecurityResponse(http.StatusOK, platformSecurityJSON(testImageID, "application/vnd.oci.image.manifest.v1+json")), nil
		}
		require.True(t, strings.HasSuffix(req.URL.Path, "/containers/create"))
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		bodies = append(bodies, body)
		return imageSecurityResponse(http.StatusCreated, `{"Id":"created"}`), nil
	})
	image, err := docker.AdmitImage(t.Context(), testImageID)
	require.NoError(t, err)
	_, err = docker.creator.ForInspection().Create(t.Context(), image, nil, "helper")
	require.NoError(t, err)
	require.Len(t, bodies, 1)
	require.NoError(t, tenantseccomp.VerifyCreateRequest(bodies[0], currentTenantProfile(t).Digest()))
	var request struct {
		HostConfig struct{ SecurityOpt []string }
	}
	require.NoError(t, json.Unmarshal(bodies[0], &request))
	require.Equal(t, tenantSeccompSecurityOpt(t, "no-new-privileges:true"), request.HostConfig.SecurityOpt)
}

func TestDockerClientWithoutAProfileRefusesBeforeDispatch(t *testing.T) {
	dispatched := 0
	daemon := newTenantSeccompTestDaemon(t, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/containers/create") {
			dispatched++
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"Id":"created"}`))
			return true
		}
		return false
	})
	source := &failingTenantSeccomp{}
	docker, err := newDockerClient(t.Context(), daemon, "seccomp-test", source)
	require.NoError(t, err, "an unusable profile must not fail construction")
	t.Cleanup(func() { _ = docker.Close() })
	require.ErrorIs(t, docker.requireTenantSeccompProfile(), tenantseccomp.ErrRefused)
	require.Equal(t, 0.0, testutil.ToFloat64(tenantSeccompProfileReady))
	calls := source.calls.Load()
	require.ErrorIs(t, docker.requireTenantSeccompProfile(), tenantseccomp.ErrRefused)
	require.Greater(t, source.calls.Load(), calls, "every request asks the source again")
	require.Zero(t, dispatched)
	_, err = tenantSeccompTestProfiles().TenantSeccompProfile() // restore the gauge for later tests
	require.NoError(t, err)
}

// newTenantSeccompTestDaemon serves the API negotiation every real client
// needs and hands every other request to handle, which reports whether it
// answered. It returns the daemon host for NewDockerClient.
func newTenantSeccompTestDaemon(t *testing.T, handle func(http.ResponseWriter, *http.Request) bool) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/_ping":
			w.Header().Set("API-Version", "1.51")
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/version"):
			_ = json.NewEncoder(w).Encode(map[string]string{"ApiVersion": "1.51"})
		default:
			if !handle(w, r) {
				http.Error(w, "unexpected Docker request "+r.Method+" "+r.URL.Path, http.StatusNotFound)
			}
		}
	}))
	t.Cleanup(server.Close)
	return "tcp://" + strings.TrimPrefix(server.URL, "http://")
}

func TestProvisionRefusedWithoutTenantProfileIsAProviderFault(t *testing.T) {
	source := &switchableTenantSeccomp{}
	source.off.Store(true)
	pulled, launched := atomic.Int64{}, atomic.Int64{}
	mock := &mockDockerClient{
		SeccompProfiles: source,
		PullImageFn: func(context.Context, string, time.Duration) error {
			pulled.Add(1)
			return nil
		},
	}
	b, callbacks := newSeccompRefusalBackend(t, mock, &launched)
	counter := tenantSeccompProfileRefusalsTotal.WithLabelValues("provision")
	before := testutil.ToFloat64(counter)

	const leaseUUID = "0192f1a0-1111-4abc-8def-00000000e118"
	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbacks.url)
	require.NoError(t, b.Provision(context.Background(), req))
	var payload backend.CallbackPayload
	select {
	case payload = <-callbacks.ch:
	case <-time.After(provisionFlowTimeout):
		t.Fatal("timed out waiting for the refused provision callback")
	}
	require.Equal(t, backend.CallbackStatusFailed, payload.Status)
	require.Equal(t, msgTenantSeccompUnavailable, payload.Error)
	require.Eventually(t, func() bool {
		info, err := b.GetProvision(context.Background(), leaseUUID)
		return err == nil && info.Status == backend.ProvisionStatusFailed
	}, 5*time.Second, 10*time.Millisecond)
	info, err := b.GetProvision(context.Background(), leaseUUID)
	require.NoError(t, err)
	require.Equal(t, backend.ReasonInternal, info.Reason, "a missing profile is a provider fault, not a tenant container exit")
	require.Equal(t, msgTenantSeccompUnavailable, info.Message)
	require.Equal(t, before+1, testutil.ToFloat64(counter))
	require.Zero(t, pulled.Load(), "the operation refuses before any substrate effect")
	require.Zero(t, launched.Load(), "no container is launched without the profile")
}

// A profile that breaks after the operation's own check is still refused at
// the compile sink, before Compose runs, and counted against the operation.
// The operation has entered tenant Steps by then, so its outcome is left to
// durable recovery rather than settled here.
func TestProvisionCompileRefusesWhenTheProfileBreaksMidOperation(t *testing.T) {
	source := &switchableTenantSeccomp{}
	launched := atomic.Int64{}
	mock := &mockDockerClient{
		SeccompProfiles: source,
		PullImageFn: func(context.Context, string, time.Duration) error {
			source.off.Store(true)
			return nil
		},
	}
	b, callbacks := newSeccompRefusalBackend(t, mock, &launched)
	counter := tenantSeccompProfileRefusalsTotal.WithLabelValues("provision")
	before := testutil.ToFloat64(counter)

	req := newProvisionRequest("0192f1a0-1111-4abc-8def-00000000e119", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbacks.url)
	require.NoError(t, b.Provision(context.Background(), req))
	require.Eventually(t, func() bool { return testutil.ToFloat64(counter) == before+1 }, provisionFlowTimeout, 10*time.Millisecond)
	require.Zero(t, launched.Load(), "Compose must not run without the profile")
}

type seccompRefusalCallbacks struct {
	url string
	ch  chan backend.CallbackPayload
}

func newSeccompRefusalBackend(t *testing.T, mock *mockDockerClient, launched *atomic.Int64) (*Backend, seccompRefusalCallbacks) {
	t.Helper()
	composeMock := &mockComposeExecutor{UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
		launched.Add(1)
		return nil
	}}
	callbacks := seccompRefusalCallbacks{ch: make(chan backend.CallbackPayload, 1)}
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		select {
		case callbacks.ch <- payload:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(callbackServer.Close)
	callbacks.url = callbackServer.URL
	b := newBackendForProvisionTest(t, mock, nil)
	b.compose = composeMock
	rebuildCallbackSender(b, callbackServer.Client())
	startCallbackReplayForTest(b)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	return b, callbacks
}
