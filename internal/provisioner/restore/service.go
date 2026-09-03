// Package restore coordinates restoration of a fresh lease from retained data.
//
// The package is an application-service boundary: transports authenticate and
// parse requests, while Service owns source routing, operation registration,
// durable write-ahead placement, the backend call, and capability-scoped
// settlement. Neither HTTP status codes nor raw placement revisions cross this
// boundary.
package restore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/leaseitems"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/util"
)

// Outcome is the transport-independent result of a restore command. Its zero
// value is deliberately not success, so an unhandled result fails closed.
type Outcome uint8

const (
	OutcomeInvalid Outcome = iota
	OutcomeAccepted
	OutcomeTargetNotPending
	OutcomeSourceNotFound
	OutcomeSourceUnavailable
	OutcomeAlreadyInProgress
	OutcomeServiceUnavailable
	OutcomeNotRetained
	OutcomeBackendInvalidState
	OutcomeAlreadyProvisioned
	OutcomeInsufficientResources
	OutcomeBackendUnavailable
	OutcomeTierTooSmall
	OutcomeInvalidRequest
	OutcomeBackendRefused
	OutcomeBackendMalformedResponse
	OutcomeInternalFailure
)

// Result is the typed result contract returned by Service. Detail contains
// only detail extracted from a backend error envelope; transports must still
// sanitize and bound it before relaying it to a tenant. Cause is available for
// operator logging and must never be written directly to a tenant response.
type Result struct {
	Outcome     Outcome
	BackendName string
	detail      string
	cause       error
}

// Accepted reports whether the backend accepted asynchronous restore work.
func (result Result) Accepted() bool { return result.Outcome == OutcomeAccepted }

// Detail returns endpoint-provided detail from a validated error envelope.
func (result Result) Detail() string { return result.detail }

// Cause returns the operator-facing error underlying a non-success result.
func (result Result) Cause() error { return result.cause }

// Command contains authenticated restore identities, never a trusted lease
// snapshot. Service authorizes SourceLeaseUUID before acquiring any exclusive
// capability, then re-reads TargetLeaseUUID under exact source and target
// lifecycle claims before deriving backend request data.
type Command struct {
	TargetLeaseUUID string
	Tenant          string
	SourceLeaseUUID string
}

// LeaseReader supplies authoritative chain ownership and target-state reads.
type LeaseReader interface {
	GetLease(context.Context, string) (*billingtypes.Lease, error)
}

// RestoreBackend is the smallest backend surface the service consumes.
type RestoreBackend interface {
	Name() string
	GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error)
	Restore(ctx context.Context, request backend.RestoreRequest) error
}

// BackendResolver resolves an explicitly named owner. It must never fall back
// to SKU routing: retained volumes exist only on their recorded backend.
type BackendResolver interface {
	ResolveRestoreBackend(name string) RestoreBackend
}

// BackendResolverFunc adapts a function to BackendResolver. It keeps router
// adaptation at the composition root instead of widening this service's port
// to the full backend router API.
type BackendResolverFunc func(name string) RestoreBackend

// ResolveRestoreBackend implements BackendResolver.
func (resolve BackendResolverFunc) ResolveRestoreBackend(name string) RestoreBackend {
	return resolve(name)
}

// OperationRegistry is the service-owned capability port for process-local
// restore operations.
type OperationRegistry interface {
	TryClaimLeaseNow(string) operation.LeaseClaimResult
	ReleaseLease(operation.LeaseClaim) bool
	TryInitiateClaimed(operation.LeaseClaim, operation.TrackSpec) operation.InitiationResult
	BindBackend(operation.Initiation, string) bool
	BeginCall(operation.Initiation) bool
	Activate(operation.Initiation) operation.InitiationCompletion
	AbortInitiation(operation.Initiation) operation.InitiationCompletion
}

// RestoreAuthority atomically binds one confirmed source, one absent target,
// and one operation identity under the current durable topology baseline. Its
// opaque claim owns source exclusion only across synchronous dispatch; the
// durable target attempt survives it.
type RestoreAuthority interface {
	Lookup(leaseUUID string) placement.Placement
	CurrentAdmissionBaseline() placement.AdmissionBaseline
	BeginAuthorizedRestore(
		baseline placement.AdmissionBaseline,
		sourceRevision placement.RecordRevision,
		targetLeaseUUID string,
		operationID operation.OperationID,
		requestSnapshot placement.BackendRequestSnapshot,
		callbackPair placement.CallbackPair,
	) (placement.RestoreClaim, error)
	ConfirmRestore(placement.RestoreClaim) (bool, error)
	RefuseRestore(placement.RestoreClaim) (bool, error)
	AbandonRestore(placement.RestoreClaim) (bool, error)
}

// EventSink receives ordered best-effort lifecycle hints around a dispatched
// restore, including terminal compensation for a synchronous refusal.
type EventSink interface {
	Publish(event backend.LeaseStatusEvent)
}

// CallbackURLBuilder binds a validated operation identity to the provision
// callback wire contract owned by the composition root.
type CallbackURLBuilder func(operationID operation.OperationID) (string, error)

var (
	_ BackendResolver   = BackendResolverFunc(nil)
	_ OperationRegistry = (*operation.Registry)(nil)
	_ RestoreAuthority  = (*placement.Store)(nil)
)

// Config contains immutable service dependencies.
type Config struct {
	ProviderUUID string
	CallbackURL  CallbackURLBuilder
	Leases       LeaseReader
	Backends     BackendResolver
	Operations   OperationRegistry
	Authority    RestoreAuthority
	Events       EventSink
	Now          func() time.Time
}

// Service coordinates restore lifecycle work.
type Service struct {
	providerUUID string
	callbackURL  CallbackURLBuilder
	leases       LeaseReader
	backends     BackendResolver
	operations   OperationRegistry
	authority    RestoreAuthority
	events       EventSink
	now          func() time.Time
}

// NewService validates the capability graph up front. A partially wired
// restore path cannot be constructed and therefore cannot contact a backend.
func NewService(config Config) (*Service, error) {
	switch {
	case strings.TrimSpace(config.ProviderUUID) == "":
		return nil, errors.New("restore provider UUID is required")
	case config.CallbackURL == nil:
		return nil, errors.New("restore callback URL builder is required")
	case util.IsNilInterface(config.Leases):
		return nil, errors.New("restore lease reader is required")
	case util.IsNilInterface(config.Backends):
		return nil, errors.New("restore backend resolver is required")
	case util.IsNilInterface(config.Operations):
		return nil, errors.New("restore operation registry is required")
	case util.IsNilInterface(config.Authority):
		return nil, errors.New("restore placement authority is required")
	}

	now := config.Now
	if now == nil {
		now = time.Now
	}
	events := config.Events
	if util.IsNilInterface(events) {
		events = nil
	}
	return &Service{
		providerUUID: config.ProviderUUID,
		callbackURL:  config.CallbackURL,
		leases:       config.Leases,
		backends:     config.Backends,
		operations:   config.Operations,
		authority:    config.Authority,
		events:       events,
		now:          now,
	}, nil
}

// Execute performs one restore command. Both lifecycle claims fence the target
// re-read and synchronous call; one operation identity then binds the Registry,
// durable target attempt, callback URL, and authoritative source backend.
func (service *Service) Execute(ctx context.Context, command Command) Result {
	if result, ok := service.validateCommand(command); !ok {
		return result
	}
	sourceAuthorization, result, ok := service.authorizeSource(ctx, command)
	if !ok {
		return result
	}

	claims, result, ok := service.claimLeases(command)
	if !ok {
		return result
	}
	defer service.releaseLeaseClaims(command, claims)

	targetLease, result, ok := service.readTarget(ctx, command)
	if !ok {
		return result
	}
	items := leaseitems.FromLease(targetLease)
	initiated := service.operations.TryInitiateClaimed(claims.target, operation.TrackSpec{
		LeaseUUID: command.TargetLeaseUUID,
		Tenant:    command.Tenant,
		Items:     items,
		Backend:   "",
		Kind:      operation.KindRestore,
	})
	if !initiated.Started() {
		if initiated.Outcome() == operation.TrackBusy {
			return Result{Outcome: OutcomeAlreadyInProgress}
		}
		return Result{
			Outcome: OutcomeServiceUnavailable,
			cause:   fmt.Errorf("register claimed restore operation: outcome %d", initiated.Outcome()),
		}
	}
	initiation := initiated.Capability()
	abortBeforePlacement := func(cause error) Result {
		completion := service.operations.AbortInitiation(initiation)
		if completion != operation.InitiationAborted &&
			completion != operation.InitiationFinished {
			slog.Error("failed to abort restore initiation before placement admission",
				"lease_uuid", command.TargetLeaseUUID,
				"completion", completion,
			)
		}
		return Result{Outcome: OutcomeServiceUnavailable, cause: cause}
	}
	callbackURL, err := service.callbackURL(initiation.ID())
	if err != nil {
		return abortBeforePlacement(fmt.Errorf("build restore callback URL: %w", err))
	}
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	if err != nil {
		return abortBeforePlacement(fmt.Errorf("build restore lifecycle callback URL: %w", err))
	}
	callbackPair, err := placement.NewCallbackPair(
		initiation.ID(), callbackURL, lifecycleCallbackURL,
	)
	if err != nil {
		return abortBeforePlacement(fmt.Errorf("bind restore callback destinations: %w", err))
	}
	requestSnapshot, err := placement.NewBackendRequestSnapshot(
		targetLease.Tenant, service.providerUUID, items,
	)
	if err != nil {
		return abortBeforePlacement(fmt.Errorf("bind exact restore backend request: %w", err))
	}

	baseline := service.authority.CurrentAdmissionBaseline()
	restoreClaim, err := service.authority.BeginAuthorizedRestore(
		baseline, sourceAuthorization.revision, command.TargetLeaseUUID, initiation.ID(),
		requestSnapshot, callbackPair,
	)
	if err != nil || !restoreClaim.Valid() || restoreClaim.Backend() == "" {
		service.operations.AbortInitiation(initiation)
		if err == nil {
			err = placement.ErrInvalidRestoreClaim
		}
		return service.restoreAdmissionFailure(err)
	}
	backendName := restoreClaim.Backend()
	cleanupPreDispatch := func(cause error) Result {
		service.settleRestore("refuse pre-dispatch restore", command, backendName, func() (bool, error) {
			return service.authority.RefuseRestore(restoreClaim)
		})
		completion := service.operations.AbortInitiation(initiation)
		if completion != operation.InitiationAborted && completion != operation.InitiationFinished {
			slog.Error("failed to abort pre-dispatch restore initiation",
				"lease_uuid", command.TargetLeaseUUID,
				"backend", backendName,
				"completion", completion,
			)
		}
		return Result{Outcome: OutcomeServiceUnavailable, BackendName: backendName, cause: cause}
	}

	if !service.operations.BindBackend(initiation, backendName) {
		return cleanupPreDispatch(errors.New("restore operation did not bind its authoritative backend"))
	}
	backendClient := service.backends.ResolveRestoreBackend(backendName)
	if util.IsNilInterface(backendClient) || backendClient.Name() != backendName {
		return cleanupPreDispatch(fmt.Errorf("restore backend %q is unavailable", backendName))
	}
	if !service.operations.BeginCall(initiation) {
		return cleanupPreDispatch(errors.New("restore operation did not enter calling phase"))
	}

	// The start event belongs immediately before the external call. An inline
	// Ready/Failed callback can therefore never be followed by stale Restarting.
	service.publishEventBestEffort(backendName, metrics.LifecycleEventRestoreRestarting,
		backend.LeaseStatusEvent{
			LeaseUUID: command.TargetLeaseUUID,
			Status:    backend.ProvisionStatusRestarting,
			Timestamp: service.now(),
		})

	callErr := invokeBackendRestore(ctx, backendName, backendClient, backend.RestoreRequest{
		LeaseUUID:            command.TargetLeaseUUID,
		FromLeaseUUID:        command.SourceLeaseUUID,
		Tenant:               command.Tenant,
		ProviderUUID:         service.providerUUID,
		Items:                items,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
	})
	if errors.Is(callErr, backend.ErrInsufficientResources) {
		verdict := metrics.CapacityVerdictAmbiguous
		if errors.Is(callErr, backend.ErrCapacityRefused) {
			verdict = metrics.CapacityVerdictCodedRefusal
		}
		metrics.BackendInsufficientResourcesTotal.WithLabelValues(backendName, verdict).Inc()
	}
	if callErr == nil {
		completion := service.operations.Activate(initiation)
		if callbackCompleted(completion) {
			service.abandonRestore(command, backendName, initiation.ID(), restoreClaim,
				"inline callback settled accepted restore")
			return service.accepted(command, backendName)
		}
		if completion != operation.InitiationActivated {
			slog.Error("accepted restore lost its exact initiation capability",
				"lease_uuid", command.TargetLeaseUUID,
				"backend", backendName,
				"completion", completion,
			)
		}
		service.settleRestore("confirm accepted restore", command, backendName, func() (bool, error) {
			return service.authority.ConfirmRestore(restoreClaim)
		})
		return service.accepted(command, backendName)
	}

	completion := service.operations.AbortInitiation(initiation)
	if callbackCompleted(completion) {
		// An authenticated exact callback is stronger evidence than a later return
		// from the same synchronous call, including a panic recovered as an error.
		service.abandonRestore(command, backendName, initiation.ID(), restoreClaim,
			"inline callback superseded synchronous restore error")
		return service.accepted(command, backendName)
	}
	if completion != operation.InitiationAborted {
		slog.Error("failed to abort synchronous restore initiation",
			"lease_uuid", command.TargetLeaseUUID,
			"backend", backendName,
			"completion", completion,
		)
	}

	result = classifyBackendError(callErr)
	result.BackendName = backendName
	switch {
	case errors.Is(callErr, backend.ErrAlreadyProvisioned):
		// A duplicate response proves only that some generation already exists.
		// Keep the exact new attempt for a matching callback or upgraded backend
		// inventory observation; never mint authority for it from a bare 409.
		service.abandonRestore(command, backendName, initiation.ID(), restoreClaim,
			"already-provisioned restore outcome awaits exact generation evidence")
	case definitelyRefused(callErr):
		service.settleRestore("refuse synchronous restore", command, backendName, func() (bool, error) {
			return service.authority.RefuseRestore(restoreClaim)
		})
		service.publishEventBestEffort(backendName, metrics.LifecycleEventRestoreRefused,
			backend.LeaseStatusEvent{
				LeaseUUID: command.TargetLeaseUUID,
				Status:    backend.ProvisionStatusFailed,
				Error:     "restore did not start",
				Timestamp: service.now(),
			})
	default:
		service.abandonRestore(command, backendName, initiation.ID(), restoreClaim,
			"ambiguous synchronous restore outcome")
	}
	return result
}

type leaseClaims struct {
	source operation.LeaseClaim
	target operation.LeaseClaim
}

func (service *Service) validateCommand(command Command) (Result, bool) {
	if command.TargetLeaseUUID == "" || command.Tenant == "" ||
		command.SourceLeaseUUID == "" || command.TargetLeaseUUID == command.SourceLeaseUUID {
		return Result{Outcome: OutcomeInvalid, cause: errors.New("restore command is incomplete")}, false
	}
	return Result{}, true
}

func (service *Service) claimLeases(command Command) (leaseClaims, Result, bool) {
	firstUUID, secondUUID := command.SourceLeaseUUID, command.TargetLeaseUUID
	firstIsSource := true
	if secondUUID < firstUUID {
		firstUUID, secondUUID = secondUUID, firstUUID
		firstIsSource = false
	}
	first := service.operations.TryClaimLeaseNow(firstUUID)
	if !first.Acquired() {
		return leaseClaims{}, leaseClaimFailure(first.Outcome()), false
	}
	second := service.operations.TryClaimLeaseNow(secondUUID)
	if !second.Acquired() {
		if !service.operations.ReleaseLease(first.Claim()) {
			slog.Error("failed to release partial restore lease claim", "lease_uuid", firstUUID)
		}
		return leaseClaims{}, leaseClaimFailure(second.Outcome()), false
	}

	claims := leaseClaims{}
	if firstIsSource {
		claims.source, claims.target = first.Claim(), second.Claim()
	} else {
		claims.target, claims.source = first.Claim(), second.Claim()
	}
	return claims, Result{}, true
}

func leaseClaimFailure(outcome operation.LeaseClaimOutcome) Result {
	if outcome == operation.LeaseClaimBusy {
		return Result{Outcome: OutcomeAlreadyInProgress}
	}
	return Result{
		Outcome: OutcomeServiceUnavailable,
		cause:   fmt.Errorf("claim restore lifecycle leases: outcome %d", outcome),
	}
}

func (service *Service) releaseLeaseClaims(command Command, claims leaseClaims) {
	if !service.operations.ReleaseLease(claims.target) {
		slog.Error("failed to release restore target lease claim", "lease_uuid", command.TargetLeaseUUID)
	}
	if !service.operations.ReleaseLease(claims.source) {
		slog.Error("failed to release restore source lease claim", "lease_uuid", command.SourceLeaseUUID)
	}
}

// authorizeSource performs only immutable ownership authorization. It runs
// before claimLeases so an authenticated tenant cannot take an exclusive
// process-local capability on a source UUID it does not own. The backend's
// retained record remains authoritative for whether restorable data exists.
type sourceAuthorization struct {
	revision placement.RecordRevision
	backend  string
}

func (service *Service) authorizeSource(
	ctx context.Context,
	command Command,
) (sourceAuthorization, Result, bool) {
	lease, err := service.leases.GetLease(ctx, command.SourceLeaseUUID)
	if err == nil && lease != nil {
		if lease.Uuid != command.SourceLeaseUUID {
			return sourceAuthorization{}, Result{
				Outcome: OutcomeSourceUnavailable,
				cause:   errors.New("restore source chain read returned a different lease"),
			}, false
		}
		if lease.Tenant != command.Tenant || lease.ProviderUuid != service.providerUUID {
			// Collapse ownership mismatch into the same tenant-facing result as an
			// absent source so this authorization check does not become an existence
			// oracle for another tenant's lease.
			return sourceAuthorization{}, Result{
				Outcome: OutcomeSourceNotFound,
				cause:   errors.New("restore source lease is not owned by the authenticated tenant and provider"),
			}, false
		}
		return service.authorizeSourcePlacement(command.SourceLeaseUUID)
	}
	if err != nil && !errors.Is(err, billingtypes.ErrLeaseNotFound) {
		return sourceAuthorization{}, Result{
			Outcome: OutcomeSourceUnavailable,
			cause:   fmt.Errorf("read restore source lease: %w", err),
		}, false
	}

	// The chain may prune a closed source before its retention window expires.
	// Select the backend only from the exact durable placement, then authorize
	// against that backend's read-only retained record before taking any Registry
	// claim. Backend-side Restore still repeats this ownership check under its
	// own mutation lock.
	authorization, result, ok := service.authorizeSourcePlacement(command.SourceLeaseUUID)
	if !ok {
		return sourceAuthorization{}, result, false
	}
	backendName := authorization.backend
	backendClient := service.backends.ResolveRestoreBackend(backendName)
	if util.IsNilInterface(backendClient) || backendClient.Name() != backendName {
		return sourceAuthorization{}, Result{
			Outcome: OutcomeSourceUnavailable,
			cause:   fmt.Errorf("restore source backend %q is unavailable", backendName),
		}, false
	}
	info, infoErr := backendClient.GetProvision(ctx, command.SourceLeaseUUID)
	if infoErr != nil {
		if errors.Is(infoErr, backend.ErrNotProvisioned) {
			return sourceAuthorization{}, Result{Outcome: OutcomeSourceNotFound}, false
		}
		return sourceAuthorization{}, Result{
			Outcome: OutcomeSourceUnavailable,
			cause:   fmt.Errorf("read retained restore source: %w", infoErr),
		}, false
	}
	if info == nil || info.LeaseUUID != command.SourceLeaseUUID || info.Tenant == "" ||
		info.Tenant != command.Tenant ||
		(info.ProviderUUID != "" && info.ProviderUUID != service.providerUUID) {
		return sourceAuthorization{}, Result{Outcome: OutcomeSourceNotFound}, false
	}
	if info.Status != backend.ProvisionStatusRetained {
		return sourceAuthorization{}, Result{Outcome: OutcomeNotRetained}, false
	}
	return authorization, Result{}, true
}

func (service *Service) authorizeSourcePlacement(
	leaseUUID string,
) (sourceAuthorization, Result, bool) {
	source := service.authority.Lookup(leaseUUID)
	if source.State() == placement.StateAbsent {
		return sourceAuthorization{}, Result{Outcome: OutcomeSourceNotFound}, false
	}
	if source.State() != placement.StateConfirmed || source.Backend == "" ||
		source.Attempt != "" || !source.RecordRevision().Valid() {
		return sourceAuthorization{}, Result{
			Outcome: OutcomeSourceUnavailable,
			cause:   placement.ErrRestoreSourceUnavailable,
		}, false
	}
	return sourceAuthorization{
		revision: source.RecordRevision(),
		backend:  source.Backend,
	}, Result{}, true
}

func (service *Service) readTarget(
	ctx context.Context,
	command Command,
) (*billingtypes.Lease, Result, bool) {
	lease, err := service.leases.GetLease(ctx, command.TargetLeaseUUID)
	if err != nil {
		return nil, Result{
			Outcome: OutcomeServiceUnavailable,
			cause:   fmt.Errorf("read restore target lease: %w", err),
		}, false
	}
	if lease == nil || lease.Uuid != command.TargetLeaseUUID {
		return nil, Result{Outcome: OutcomeTargetNotPending}, false
	}
	if lease.Tenant != command.Tenant || lease.ProviderUuid != service.providerUUID {
		return nil, Result{
			Outcome: OutcomeInvalid,
			cause:   errors.New("restore command is not authorized for current target lease"),
		}, false
	}
	if lease.State != billingtypes.LEASE_STATE_PENDING {
		return nil, Result{Outcome: OutcomeTargetNotPending}, false
	}
	return lease, Result{}, true
}

func (service *Service) restoreAdmissionFailure(err error) Result {
	switch {
	case errors.Is(err, placement.ErrRestoreSourceNotFound):
		return Result{Outcome: OutcomeSourceNotFound, cause: err}
	case errors.Is(err, placement.ErrRestoreSourceClaimed),
		errors.Is(err, placement.ErrRestoreTargetUnavailable),
		errors.Is(err, placement.ErrAttemptConflict):
		return Result{Outcome: OutcomeAlreadyInProgress, cause: err}
	case errors.Is(err, placement.ErrRestoreSourceUnavailable):
		return Result{Outcome: OutcomeSourceUnavailable, cause: err}
	case errors.Is(err, placement.ErrInvalidAdmissionBaseline),
		errors.Is(err, placement.ErrBackendTopologyNotConfigured),
		errors.Is(err, placement.ErrBackendNotInTopology):
		return Result{Outcome: OutcomeServiceUnavailable, cause: err}
	default:
		return Result{Outcome: OutcomeServiceUnavailable, cause: err}
	}
}

func callbackCompleted(completion operation.InitiationCompletion) bool {
	return completion == operation.InitiationFinished ||
		completion == operation.InitiationSettling
}

func (service *Service) accepted(command Command, backendName string) Result {
	slog.Info("lease restore initiated",
		"lease_uuid", command.TargetLeaseUUID,
		"from_lease", command.SourceLeaseUUID,
		"tenant", command.Tenant,
		"backend", backendName,
	)
	return Result{Outcome: OutcomeAccepted, BackendName: backendName}
}

func (service *Service) abandonRestore(
	command Command,
	backendName string,
	operationID operation.OperationID,
	claim placement.RestoreClaim,
	reason string,
) {
	slog.Warn(reason,
		"lease_uuid", command.TargetLeaseUUID,
		"from_lease_uuid", command.SourceLeaseUUID,
		"backend", backendName,
		"operation_id", operationID,
	)
	service.settleRestore("abandon restore dispatch", command, backendName, func() (bool, error) {
		return service.authority.AbandonRestore(claim)
	})
}

func (service *Service) settleRestore(
	action string,
	command Command,
	backendName string,
	settle func() (bool, error),
) {
	settled, err := settle()
	if err != nil {
		slog.Error("failed to "+action,
			"lease_uuid", command.TargetLeaseUUID,
			"from_lease_uuid", command.SourceLeaseUUID,
			"backend", backendName,
			"error", err,
		)
	} else if !settled {
		slog.Debug(action+" was already superseded",
			"lease_uuid", command.TargetLeaseUUID,
			"backend", backendName,
		)
	}
}

func (service *Service) publishEventBestEffort(
	backendName string,
	eventName string,
	event backend.LeaseStatusEvent,
) {
	if service.events == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
				eventName,
			).Inc()
			slog.Error("restore lifecycle event sink panicked; continuing lifecycle settlement",
				"lease_uuid", event.LeaseUUID,
				"backend", backendName,
				"event", eventName,
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	service.events.Publish(event)
}

func invokeBackendRestore(
	ctx context.Context,
	backendName string,
	backendClient RestoreBackend,
	request backend.RestoreRequest,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("backend Restore panicked",
				"lease_uuid", request.LeaseUUID,
				"backend", backendName,
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
			err = fmt.Errorf("backend Restore panicked: %v", recovered)
		}
	}()
	return backendClient.Restore(ctx, request)
}

func classifyBackendError(err error) Result {
	result := Result{Outcome: OutcomeInternalFailure, cause: err}
	if detail, ok := backend.Detail(err); ok {
		result.detail = detail
	}
	switch {
	case errors.Is(err, backend.ErrNotRetained):
		result.Outcome = OutcomeNotRetained
	case errors.Is(err, backend.ErrInvalidState):
		result.Outcome = OutcomeBackendInvalidState
	case errors.Is(err, backend.ErrAlreadyProvisioned):
		result.Outcome = OutcomeAlreadyProvisioned
	case errors.Is(err, backend.ErrInsufficientResources):
		result.Outcome = OutcomeInsufficientResources
	case errors.Is(err, backend.ErrCircuitOpen):
		result.Outcome = OutcomeBackendUnavailable
	case errors.Is(err, backend.ErrBackendUpgradeRequired),
		errors.Is(err, backend.ErrBackendStorageIdentityUnbound):
		result.Outcome = OutcomeBackendUnavailable
	case errors.Is(err, backend.ErrDemoteDataExceedsTier):
		result.Outcome = OutcomeTierTooSmall
	case errors.Is(err, backend.ErrValidation):
		result.Outcome = OutcomeInvalidRequest
	case errors.Is(err, backend.ErrRestoreRefused):
		result.Outcome = OutcomeBackendRefused
	case errors.Is(err, backend.ErrMalformedErrorBody):
		result.Outcome = OutcomeBackendMalformedResponse
	}
	return result
}

// definitelyRefused identifies typed results whose configured-backend protocol
// contract says no asynchronous work was accepted. Responses are not HMAC
// authenticated, so this verdict relies on the deployment's transport trust.
// Transport failures, generic 5xx, malformed envelopes, and the HTTP client's
// broad 503/capacity sentinel remain ambiguous.
func definitelyRefused(err error) bool {
	return errors.Is(err, backend.ErrNotRetained) ||
		errors.Is(err, backend.ErrInvalidState) ||
		errors.Is(err, backend.ErrCircuitOpen) ||
		errors.Is(err, backend.ErrCapacityRefused) ||
		errors.Is(err, backend.ErrDemoteDataExceedsTier) ||
		errors.Is(err, backend.ErrValidation) ||
		errors.Is(err, backend.ErrRestoreRefused) ||
		errors.Is(err, backend.ErrBackendStorageIdentityUnbound) ||
		errors.Is(err, backend.ErrBackendUpgradeRequired)
}
