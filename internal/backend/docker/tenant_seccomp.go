package docker

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/docker/tenantseccomp"
	"github.com/manifest-network/fred/internal/backend/shared"
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
	tenantSeccompSinkCompensation
	tenantSeccompSinkInspection
)

var tenantSeccompSinks = [...]tenantSeccompSink{
	tenantSeccompSinkProvision, tenantSeccompSinkRestore, tenantSeccompSinkRestart,
	tenantSeccompSinkUpdate, tenantSeccompSinkCompensation, tenantSeccompSinkInspection,
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
		switch m.maintenanceSubject.Intent().Kind() {
		case shared.MaintenanceIntentRestart:
			return tenantSeccompSinkRestart
		case shared.MaintenanceIntentUpdate:
			return tenantSeccompSinkUpdate
		}
	case m.operationSubject.Valid():
		switch m.operationSubject.Intent().Kind() {
		case shared.OperationIntentProvision:
			return tenantSeccompSinkProvision
		case shared.OperationIntentRestore:
			return tenantSeccompSinkRestore
		}
	}
	return tenantSeccompSinkInvalid
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
// fred_docker_backend_tenant_seccomp_profile_ready.
type observedTenantSeccomp struct {
	source imageexec.TenantSeccompSource
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
	tenantSeccompProfileReady.Set(1)
	return profile, nil
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
// to any launch scope, so it settles with no daemon effect.
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
