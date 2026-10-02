package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/errdefs"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/docker/tenantseccomp"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/metrics/background"
	"github.com/manifest-network/fred/internal/util"
)

// tenantSeccompSink is the closed set of creation sinks that refuse a launch
// when the tenant seccomp profile cannot be applied. The zero value is never
// counted.
type tenantSeccompSink uint8

const (
	tenantSeccompSinkInvalid tenantSeccompSink = iota
	tenantSeccompSinkProvision
	tenantSeccompSinkRestore
	tenantSeccompSinkRestart
	tenantSeccompSinkUpdate
	tenantSeccompSinkCustomDomain
	tenantSeccompSinkCompensation
	tenantSeccompSinkInspection
)

var tenantSeccompSinks = [...]tenantSeccompSink{
	tenantSeccompSinkProvision, tenantSeccompSinkRestore, tenantSeccompSinkRestart,
	tenantSeccompSinkUpdate, tenantSeccompSinkCustomDomain, tenantSeccompSinkCompensation,
	tenantSeccompSinkInspection,
}

func (sink tenantSeccompSink) label() string {
	switch sink {
	case tenantSeccompSinkProvision:
		return "provision"
	case tenantSeccompSinkRestore:
		return "restore"
	case tenantSeccompSinkRestart:
		return "restart"
	case tenantSeccompSinkUpdate:
		return "update"
	case tenantSeccompSinkCustomDomain:
		return "custom_domain"
	case tenantSeccompSinkCompensation:
		return "compensation"
	case tenantSeccompSinkInspection:
		return "inspection"
	default:
		return ""
	}
}

// tenantSeccompSink names the creation sink of this exact physical subject.
func (m *storageMutations) tenantSeccompSink() tenantSeccompSink {
	switch {
	case m == nil:
		return tenantSeccompSinkInvalid
	case m.compensationSubject.Valid():
		return tenantSeccompSinkCompensation
	case m.maintenanceSubject.Valid():
		return tenantSeccompMaintenanceSink(m.maintenanceSubject.Intent().Kind())
	case m.operationSubject.Valid():
		return tenantSeccompOperationSink(m.operationSubject.Intent().Kind())
	}
	return tenantSeccompSinkInvalid
}

// tenantSeccompMaintenanceSink names the sink of each maintenance intent that
// recreates containers: a restart, an update, and the redeploy that applies a
// custom domain.
func tenantSeccompMaintenanceSink(kind shared.MaintenanceIntentKind) tenantSeccompSink {
	switch kind {
	case shared.MaintenanceIntentRestart:
		return tenantSeccompSinkRestart
	case shared.MaintenanceIntentUpdate:
		return tenantSeccompSinkUpdate
	case shared.MaintenanceIntentCustomDomain:
		return tenantSeccompSinkCustomDomain
	default:
		return tenantSeccompSinkInvalid
	}
}

// tenantSeccompOperationSink names the sink of each operation intent.
func tenantSeccompOperationSink(kind shared.OperationIntentKind) tenantSeccompSink {
	switch kind {
	case shared.OperationIntentProvision:
		return tenantSeccompSinkProvision
	case shared.OperationIntentRestore:
		return tenantSeccompSinkRestore
	default:
		return tenantSeccompSinkInvalid
	}
}

// msgTenantSeccompUnavailable is the tenant-facing message of a launch refused
// because the provider cannot apply its container security profile. It is a
// provider fault, reported with backend.ReasonInternal.
const msgTenantSeccompUnavailable = "container security profile unavailable"

// refuseWithoutTenantSeccomp is the one conversion of a creation refused for
// the tenant seccomp profile into the result tenants see. It authors the
// provider-fault reason and message here and counts the refusal against the
// sink that raised it, once. Every other error is returned unchanged.
func refuseWithoutTenantSeccomp(sink tenantSeccompSink, err error) error {
	if !errors.Is(err, tenantseccomp.ErrRefused) {
		return err
	}
	var authored *physicalOperationError
	if errors.As(err, &authored) && errors.Is(authored.cause, tenantseccomp.ErrRefused) {
		return err
	}
	if label := sink.label(); label != "" {
		tenantSeccompProfileRefusalsTotal.WithLabelValues(label).Inc()
	}
	return &physicalOperationError{callback: msgTenantSeccompUnavailable, reason: backend.ReasonInternal, cause: err}
}

// requireTenantSeccomp refuses a launch before it touches any substrate when
// the tenant profile cannot be applied, so a refusal never stops a workload
// that it then cannot replace. The creation sinks still check for themselves.
func (m *storageMutations) requireTenantSeccomp() error {
	if m == nil || util.IsNilInterface(m.ops.docker) {
		return errors.New("tenant seccomp check requires a bound Docker sink")
	}
	return refuseWithoutTenantSeccomp(m.tenantSeccompSink(), m.ops.docker.requireTenantSeccompProfile())
}

// observedTenantSeccomp reports every profile request, from any sink, on
// fred_docker_backend_tenant_seccomp_profile_ready: 1 when the profile is
// usable and the Docker daemon did not last report itself without seccomp.
type observedTenantSeccomp struct {
	source imageexec.TenantSeccompSource
	daemon *daemonSeccompSupport
}

func (o observedTenantSeccomp) TenantSeccompProfile() (tenantseccomp.Profile, error) {
	if util.IsNilInterface(o.source) {
		tenantSeccompProfileReady.Set(0)
		return tenantseccomp.Profile{}, fmt.Errorf("%w: no profile source", tenantseccomp.ErrRefused)
	}
	profile, err := o.source.TenantSeccompProfile()
	if err != nil {
		tenantSeccompProfileReady.Set(0)
		return tenantseccomp.Profile{}, err
	}
	if o.daemon.reportedMissing() {
		tenantSeccompProfileReady.Set(0)
	} else {
		tenantSeccompProfileReady.Set(1)
	}
	return profile, nil
}

// daemonSeccompSupport is what the Docker daemon last reported about seccomp.
// A daemon without seccomp refuses to run a container under a custom profile,
// and every tenant container runs under fred's, so tenant launches fail there.
// fred still sends them (the daemon is the authority), but the readiness gauge
// reports 0. The zero value has seen no report.
type daemonSeccompSupport struct {
	missing atomic.Bool
}

// observe records one daemon report of its security options. A report
// without seccomp drops the readiness gauge at once; a later report with it
// lets the next profile request raise the gauge again.
func (s *daemonSeccompSupport) observe(securityOptions []string) {
	if s == nil {
		return
	}
	missing := !daemonReportsSeccomp(securityOptions)
	s.missing.Store(missing)
	if missing {
		tenantSeccompProfileReady.Set(0)
	}
}

func (s *daemonSeccompSupport) reportedMissing() bool {
	return s != nil && s.missing.Load()
}

// daemonReportsSeccomp reports whether docker info's security options name
// seccomp ("name=seccomp,profile=...").
func daemonReportsSeccomp(securityOptions []string) bool {
	for _, option := range securityOptions {
		if strings.HasPrefix(option, "name=seccomp") {
			return true
		}
	}
	return false
}

// maxLaunchCreateBody bounds the pre-dispatch read of one container create
// request: one inline profile, about 13 KB, plus the container configuration.
const maxLaunchCreateBody = 8 << 20

func daemonContainerCreateRequest(req *http.Request) bool {
	return daemonContainerLaunchEndpoint(req) == "create"
}

// refuseUnconfinedCreate is the wire check of every container create, scoped
// or not, from either SDK. It reads a copy of the body through GetBody and
// leaves the request untouched, then requires that the body asks for exactly
// the current tenant profile. A refusal happens before the request is admitted
// to any launch scope, so it settles with no daemon effect. The one other
// client to the daemon, the Compose engine that only lists and tears down,
// refuses every create outright (daemonNoCreateTransport).
func refuseUnconfinedCreate(profiles imageexec.TenantSeccompSource, req *http.Request) error {
	if !daemonContainerCreateRequest(req) {
		return nil
	}
	if util.IsNilInterface(profiles) {
		return fmt.Errorf("%w: the transport has no profile source", tenantseccomp.ErrRefused)
	}
	profile, err := profiles.TenantSeccompProfile()
	if err != nil {
		return err
	}
	if req.GetBody == nil {
		return fmt.Errorf("%w: the create request body cannot be read again", tenantseccomp.ErrRefused)
	}
	body, err := req.GetBody()
	if err != nil {
		return fmt.Errorf("%w: copy the create request body: %w", tenantseccomp.ErrRefused, err)
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(io.LimitReader(body, maxLaunchCreateBody+1))
	if err != nil {
		return fmt.Errorf("%w: read the create request body: %w", tenantseccomp.ErrRefused, err)
	}
	if len(data) > maxLaunchCreateBody {
		return fmt.Errorf("%w: the create request body exceeds %d bytes", tenantseccomp.ErrRefused, maxLaunchCreateBody)
	}
	return tenantseccomp.VerifyCreateRequest(data, profile.Digest())
}

// refuseCreate ends a refused exchange as a RoundTripper must: the request
// body is closed and no response is returned.
func refuseCreate(req *http.Request, err error) (*http.Response, error) {
	if req.Body != nil {
		_ = req.Body.Close()
	}
	return nil, err
}

// tenantSeccompCensus is one completed census pass: the live containers of
// this backend whose effective seccomp profile is not the current tenant
// profile. Only a pass that listed and inspected every container yields one;
// a failed pass yields an error and no count.
type tenantSeccompCensus struct{ withoutCurrent int }

// TenantSeccompCensus lists every fred-managed container of this backend,
// whatever its labels say otherwise, inspects each through the SDK, and
// counts those running, restarting or paused without the current profile.
// It judges the unmodified HostConfig with dockerd's own rule: the last
// seccomp option is the one applied, and a privileged container has none. A
// container removed between list and inspect is skipped; any other failure
// fails the pass. It never changes a container.
func (d *DockerClient) TenantSeccompCensus(ctx context.Context) (tenantSeccompCensus, error) {
	if d == nil || util.IsNilInterface(d.tenantSeccomp) {
		return tenantSeccompCensus{}, errors.New("the Docker client has no tenant profile source")
	}
	profile, err := d.tenantSeccomp.TenantSeccompProfile()
	if err != nil {
		return tenantSeccompCensus{}, fmt.Errorf("census needs the current tenant profile: %w", err)
	}
	want := profile.Digest()
	filter := filters.NewArgs(filters.Arg("label", LabelManaged+"=true"))
	if d.backendName != "" {
		filter.Add("label", LabelBackendName+"="+d.backendName)
	}
	listed, err := d.client.ContainerList(ctx, container.ListOptions{All: true, Filters: filter})
	if err != nil {
		return tenantSeccompCensus{}, fmt.Errorf("list managed containers: %w", err)
	}
	var census tenantSeccompCensus
	for _, summary := range listed {
		inspected, err := d.client.ContainerInspect(ctx, summary.ID)
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return tenantSeccompCensus{}, fmt.Errorf("inspect managed container: %w", err)
		}
		if inspected.ContainerJSONBase == nil || inspected.State == nil {
			return tenantSeccompCensus{}, errors.New("container inspection returned no state")
		}
		if !inspected.State.Running && !inspected.State.Restarting && !inspected.State.Paused {
			continue
		}
		host := inspected.HostConfig
		if host == nil || !tenantseccomp.Applied(host.SecurityOpt, host.Privileged, want) {
			census.withoutCurrent++
		}
	}
	return census, nil
}

const (
	tenantSeccompCensusInterval = 10 * time.Minute
	tenantSeccompCensusTimeout  = 2 * time.Minute

	tenantSeccompCensusOK    = "ok"
	tenantSeccompCensusError = "error"
)

var tenantSeccompCensusOutcomes = []string{tenantSeccompCensusOK, tenantSeccompCensusError}

// tenantSeccompCensusLoop runs the census once when Start launches it, after
// recovery, and then every tenantSeccompCensusInterval until shutdown.
func (b *Backend) tenantSeccompCensusLoop() {
	ticker := time.NewTicker(tenantSeccompCensusInterval)
	defer ticker.Stop()
	var reported tenantSeccompCensusReport
	for b.stopCtx.Err() == nil {
		util.RunCleanupIteration(func() error {
			b.runTenantSeccompCensus(&reported)
			return nil
		}, "docker_seccomp_census", func(any) {
			tenantSeccompCensusTotal.WithLabelValues(tenantSeccompCensusError).Inc()
			background.CleanupPanicsTotal.WithLabelValues("docker_seccomp_census").Inc()
		})
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

// runTenantSeccompCensus publishes one completed census. A failed pass is
// counted and leaves the gauge at the last completed value. It only reads.
func (b *Backend) runTenantSeccompCensus(reported *tenantSeccompCensusReport) {
	ctx, cancel := context.WithTimeout(b.stopCtx, tenantSeccompCensusTimeout)
	defer cancel()
	census, err := b.docker.TenantSeccompCensus(ctx)
	if err != nil {
		if b.stopCtx.Err() != nil {
			return
		}
		tenantSeccompCensusTotal.WithLabelValues(tenantSeccompCensusError).Inc()
		b.logger.Warn("tenant seccomp census failed; keeping the last completed count", "error", err)
		return
	}
	tenantContainersWithoutCurrentSeccomp.Set(float64(census.withoutCurrent))
	tenantSeccompCensusTotal.WithLabelValues(tenantSeccompCensusOK).Inc()
	reported.log(b.logger, census.withoutCurrent)
}

// tenantSeccompCensusReport is what the census last logged. Containers
// created before an upgrade keep their profile until their lease is
// restarted or updated, so a nonzero count can hold for a long time: only a
// change warns, and a count that holds is logged at Info. The gauge carries
// the value either way.
type tenantSeccompCensusReport struct {
	logged bool
	count  int
}

// log reports one completed count.
func (r *tenantSeccompCensusReport) log(logger *slog.Logger, withoutCurrent int) {
	changed := !r.logged || r.count != withoutCurrent
	previous := r.count
	r.logged, r.count = true, withoutCurrent
	switch {
	case withoutCurrent > 0 && changed:
		logger.Warn("tenant containers run without the current seccomp profile; restarting or updating their leases recreates them with it",
			"containers", withoutCurrent, "previous", previous)
	case withoutCurrent > 0:
		logger.Info("tenant containers still run without the current seccomp profile",
			"containers", withoutCurrent)
	case changed && previous > 0:
		logger.Info("every tenant container runs under the current seccomp profile", "previous", previous)
	}
}
