package placement

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/util"
)

// backendRuntime is the single backend topology bound to provision and
// reconciliation authorities at construction. Routing and exact-name lookup
// therefore cannot come from different router instances.
// It is intentionally private: purpose facets never return a general backend
// client or a mutation-capable router.
type backendRuntime interface {
	Route(string) backend.Backend
	RouteForProvision(context.Context, string, map[string]int) backend.Backend
	RouteForProvisionAmong(context.Context, string, map[string]struct{}, map[string]int) backend.Backend
	GetBackendByName(string) backend.Backend
	Backends() []backend.Backend
}

// ProvisionStartObserver and RestoreStartObserver are read-only observation
// hooks bound beside the backend runtime. They cannot report a transport
// outcome or acquire a settlement capability.
type ProvisionStartObserver func(leaseUUID, backendName string)
type RestoreStartObserver func(leaseUUID, backendName string)

type MaintenancePayloadPersister interface {
	OverwritePayload(string, []byte) error
}

type AttemptPayloadReader interface {
	GetWithHash(string) ([]byte, []byte, error)
}

// ExecutionCoordinator permanently binds the one physical backend runtime to
// the Store+Registry aggregate. Purpose facets derive their resolver from this
// value; accepting a runtime again at each constructor would allow same-name
// facets to execute against different machines.
type ExecutionCoordinator struct {
	coordinator  *OperationCoordinator
	backends     backendRuntime
	callbacks    *CallbackRouteFactory
	controlPlane *boundProviderControlPlane
	deprovision  *deprovisionCoordinator
	issuer       *operationCoordinatorMarker
}

func (coordinator *OperationCoordinator) BindBackendRuntime(
	backends backendRuntime,
	control ProviderControlPlane,
) (*ExecutionCoordinator, error) {
	if !coordinator.Valid() || util.IsNilInterface(backends) ||
		util.IsNilInterface(control) ||
		coordinator.store.callbackRoutes == nil || !coordinator.store.callbackRoutes.Valid() {
		return nil, errors.New("valid operation coordinator, backend runtime, provider control plane, and callback routes are required")
	}
	names, err := backendNames(backends)
	if err != nil {
		return nil, err
	}
	if err := coordinator.store.VerifyBackendTopology(names); err != nil {
		return nil, fmt.Errorf("verify backend runtime topology: %w", err)
	}
	coordinator.bindMu.Lock()
	defer coordinator.bindMu.Unlock()
	if coordinator.execution != nil {
		return nil, errors.New("backend runtime is already bound")
	}
	execution := &ExecutionCoordinator{
		coordinator: coordinator, backends: backends,
		callbacks: coordinator.store.callbackRoutes,
		issuer:    coordinator.marker,
	}
	execution.controlPlane = &boundProviderControlPlane{
		execution: execution, providerUUID: coordinator.store.providerUUID, control: control,
	}
	execution.deprovision = newDeprovisionCoordinator(execution)
	coordinator.execution = execution
	return execution, nil
}

// Valid reports whether the coordinator was atomically constructed with its
// physical runtime and provider control plane. There is no publishable partial
// stage that can mint an executable purpose facet.
func (authority *ExecutionCoordinator) Valid() bool {
	return authority != nil && authority.coordinator != nil &&
		authority.coordinator.Valid() && authority.issuer == authority.coordinator.marker &&
		authority.coordinator.execution == authority && !util.IsNilInterface(authority.backends) &&
		authority.callbacks.Valid() && authority.deprovision != nil &&
		authority.deprovision.execution == authority && authority.controlPlane != nil &&
		authority.controlPlane.validFor(authority)
}

func backendNames(runtime backendRuntime) (names []string, err error) {
	if util.IsNilInterface(runtime) {
		return nil, errors.New("backend runtime is unavailable")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.BackendInvocationPanicsTotal.WithLabelValues(metrics.OperationProvision).Inc()
			names = nil
			err = fmt.Errorf("enumerate backend runtime panicked (%T)", recovered)
		}
	}()
	for _, candidate := range runtime.Backends() {
		if util.IsNilInterface(candidate) {
			return nil, errors.New("backend runtime returned a nil backend")
		}
		name := candidate.Name()
		if name == "" {
			return nil, errors.New("backend runtime returned an unnamed backend")
		}
		names = append(names, name)
	}
	slices.Sort(names)
	if len(names) != len(slices.Compact(slices.Clone(names))) {
		return nil, errors.New("backend runtime returned duplicate backend names")
	}
	return names, nil
}

func exactBackend(runtime backendRuntime, backendName string) (client backend.Backend, err error) {
	if util.IsNilInterface(runtime) || backendName == "" {
		return nil, errors.New("backend runtime or exact backend name is unavailable")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.BackendInvocationPanicsTotal.WithLabelValues(metrics.OperationRestore).Inc()
			client = nil
			err = fmt.Errorf("resolve backend %q panicked (%T)", backendName, recovered)
		}
	}()
	client = runtime.GetBackendByName(backendName)
	if util.IsNilInterface(client) {
		return nil, fmt.Errorf("backend %q is unavailable", backendName)
	}
	if client.Name() != backendName {
		return nil, fmt.Errorf("resolved backend does not match exact name %q", backendName)
	}
	return client, nil
}

func executeProvision(
	ctx context.Context,
	coordinator *OperationCoordinator,
	runtime backendRuntime,
	observer ProvisionStartObserver,
	dispatch ProvisionDispatch,
	payload []byte,
) DispatchResult {
	if coordinator == nil || !coordinator.Valid() || !dispatch.Valid() ||
		dispatch.issuer != coordinator.marker {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid provision dispatch")}
	}
	attempt := dispatch.attempt
	requestSnapshot := attempt.requestSnapshot
	callbackPair := attempt.callbackPair
	abort := func(cause error) DispatchResult {
		result := coordinator.abortProvisionDispatch(dispatch)
		result.err = errors.Join(cause, result.err)
		return result
	}
	if !requestSnapshot.Valid() || !callbackPair.ValidFor(attempt.operationID) {
		return abort(errors.New("durable provision request is invalid"))
	}
	if attempt.payloadFingerprint.Valid() {
		if payload == nil || sha256.Sum256(payload) != attempt.payloadFingerprint.sha256 {
			return abort(errors.New("provision payload differs from durable fingerprint"))
		}
	} else if payload != nil {
		return abort(errors.New("payloadless provision dispatch received payload bytes"))
	}
	backendClient, err := exactBackend(runtime, attempt.backendName)
	if err != nil {
		return abort(err)
	}
	call, calling := coordinator.beginProvisionCall(dispatch)
	if !calling {
		return abort(errors.New("provision dispatch did not enter calling phase"))
	}
	request := backend.ProvisionRequest{
		LeaseUUID: attempt.leaseUUID,
		Tenant:    requestSnapshot.Tenant(), ProviderUUID: requestSnapshot.ProviderUUID(),
		Items:                requestSnapshot.Items(),
		CallbackURL:          callbackPair.OperationURL(),
		LifecycleCallbackURL: callbackPair.LifecycleURL(),
	}
	if attempt.payloadFingerprint.Valid() {
		request.Payload = append([]byte(nil), payload...)
		request.PayloadHash = attempt.payloadFingerprint.String()
	}
	observeProvisionStart(observer, attempt.leaseUUID, attempt.backendName)
	return coordinator.completeProvision(call, invokeProvision(ctx, backendClient, request))
}

func invokeProvision(
	ctx context.Context,
	client backend.Backend,
	request backend.ProvisionRequest,
) (outcome backend.ProvisionCallOutcome) {
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.BackendInvocationPanicsTotal.WithLabelValues(metrics.OperationProvision).Inc()
			slog.Error("backend Provision panicked",
				"lease_uuid", request.LeaseUUID,
				"backend", requestBackendName(client),
				"panic_type", fmt.Sprintf("%T", recovered),
			)
			outcome = backend.ConservativeProvisionCallOutcome(
				fmt.Errorf("backend Provision panicked (%T)", recovered),
			)
		}
	}()
	outcome = backend.InvokeProvision(ctx, client, request)
	if !outcome.Valid() {
		return backend.ConservativeProvisionCallOutcome(
			errors.New("backend returned an invalid provision call outcome"),
		)
	}
	return outcome
}

func invokeRestore(
	ctx context.Context,
	client backend.Backend,
	request backend.RestoreRequest,
) (outcome backend.RestoreCallOutcome) {
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.BackendInvocationPanicsTotal.WithLabelValues(metrics.OperationRestore).Inc()
			slog.Error("backend Restore panicked",
				"lease_uuid", request.LeaseUUID,
				"backend", requestBackendName(client),
				"panic_type", fmt.Sprintf("%T", recovered),
			)
			outcome = backend.ConservativeRestoreCallOutcome(
				fmt.Errorf("backend Restore panicked (%T)", recovered),
			)
		}
	}()
	outcome = backend.InvokeRestore(ctx, client, request)
	if !outcome.Valid() {
		return backend.ConservativeRestoreCallOutcome(
			errors.New("backend returned an invalid restore call outcome"),
		)
	}
	return outcome
}

func invokeDeprovision(
	ctx context.Context,
	client backend.Backend,
	leaseUUID string,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.BackendInvocationPanicsTotal.WithLabelValues(metrics.OperationDeprovision).Inc()
			slog.Error("backend Deprovision panicked",
				"lease_uuid", leaseUUID,
				"backend", requestBackendName(client),
				"panic_type", fmt.Sprintf("%T", recovered),
			)
			err = fmt.Errorf("backend Deprovision panicked (%T)", recovered)
		}
	}()
	return client.Deprovision(ctx, leaseUUID)
}

func invokeMaintenance(
	ctx context.Context,
	client backend.Backend,
	command MaintenanceCommand,
) (outcome backend.MaintenanceCallOutcome) {
	defer func() {
		if recovered := recover(); recovered != nil {
			operation := metrics.OperationRestart
			if command.Kind() == MaintenanceCommandUpdate {
				operation = metrics.OperationUpdate
			}
			metrics.BackendInvocationPanicsTotal.WithLabelValues(operation).Inc()
			slog.Error("backend maintenance call panicked",
				"lease_uuid", command.LeaseUUID(),
				"backend", command.BackendName(),
				"kind", command.Kind().String(),
				"panic_type", fmt.Sprintf("%T", recovered),
			)
			outcome = backend.ConservativeMaintenanceCallOutcome(
				fmt.Errorf("backend %s panicked (%T)", command.Kind(), recovered),
			)
		}
	}()
	switch command.Kind() {
	case MaintenanceCommandRestart:
		request := backend.RestartRequest{
			LeaseUUID: command.LeaseUUID(), MaintenanceID: command.ID(),
			CallbackURL: command.CallbackURL(),
		}
		outcome = backend.InvokeRestart(ctx, client, request)
	case MaintenanceCommandUpdate:
		request := backend.UpdateRequest{
			LeaseUUID: command.LeaseUUID(), MaintenanceID: command.ID(),
			CallbackURL: command.CallbackURL(), Payload: command.Payload(),
		}
		outcome = backend.InvokeUpdate(ctx, client, request)
	default:
		return backend.ConservativeMaintenanceCallOutcome(
			errors.New("invalid durable maintenance kind"),
		)
	}
	if !outcome.Valid() {
		return backend.ConservativeMaintenanceCallOutcome(
			errors.New("backend returned an invalid maintenance call outcome"),
		)
	}
	return outcome
}

func requestBackendName(client backend.Backend) (name string) {
	defer func() { _ = recover() }()
	if !util.IsNilInterface(client) {
		name = client.Name()
	}
	return name
}

func observeProvisionStart(observer ProvisionStartObserver, leaseUUID, backendName string) {
	if observer == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
				metrics.LifecycleEventProvisionStarting,
			).Inc()
			slog.Error("provision start observer panicked; continuing exact backend dispatch",
				"lease_uuid", leaseUUID,
				"backend", backendName,
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	observer(leaseUUID, backendName)
}

func observeRestoreStart(observer RestoreStartObserver, leaseUUID, backendName string) {
	if observer == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
				metrics.LifecycleEventRestoreRestarting,
			).Inc()
			slog.Error("restore start observer panicked; continuing exact backend dispatch",
				"lease_uuid", leaseUUID,
				"backend", backendName,
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	observer(leaseUUID, backendName)
}
