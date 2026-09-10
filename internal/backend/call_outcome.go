package backend

import (
	"context"
	"errors"
)

// InvokeProvision is the package-owned causal boundary for a provision call.
// Only the exact production HTTP transport can mint refusal or no-dispatch
// evidence. A decorator, custom backend, or legacy implementation receives the
// conservative adapter even if it exposes similarly named methods; Go method
// embedding therefore cannot forge causal authority.
func InvokeProvision(
	ctx context.Context,
	client Backend,
	request ProvisionRequest,
) ProvisionCallOutcome {
	if httpClient, ok := client.(*HTTPClient); ok {
		if httpClient == nil {
			return ConservativeProvisionCallOutcome(errors.New("nil HTTP backend"))
		}
		return httpClient.provisionCall(ctx, request)
	}
	if client == nil {
		return ConservativeProvisionCallOutcome(errors.New("nil backend"))
	}
	return ConservativeProvisionCallOutcome(client.Provision(ctx, request))
}

// InvokeRestore is the restore counterpart to InvokeProvision.
func InvokeRestore(
	ctx context.Context,
	client Backend,
	request RestoreRequest,
) RestoreCallOutcome {
	if httpClient, ok := client.(*HTTPClient); ok {
		if httpClient == nil {
			return ConservativeRestoreCallOutcome(errors.New("nil HTTP backend"))
		}
		return httpClient.restoreCall(ctx, request)
	}
	if client == nil {
		return ConservativeRestoreCallOutcome(errors.New("nil backend"))
	}
	return ConservativeRestoreCallOutcome(client.Restore(ctx, request))
}

// InvokeRestart is the restart counterpart to InvokeProvision.
func InvokeRestart(
	ctx context.Context,
	client Backend,
	request RestartRequest,
) MaintenanceCallOutcome {
	if httpClient, ok := client.(*HTTPClient); ok {
		if httpClient == nil {
			return ConservativeMaintenanceCallOutcome(errors.New("nil HTTP backend"))
		}
		return httpClient.restartCall(ctx, request)
	}
	if client == nil {
		return ConservativeMaintenanceCallOutcome(errors.New("nil backend"))
	}
	return ConservativeMaintenanceCallOutcome(client.Restart(ctx, request))
}

// InvokeUpdate is the update counterpart to InvokeProvision.
func InvokeUpdate(
	ctx context.Context,
	client Backend,
	request UpdateRequest,
) MaintenanceCallOutcome {
	if httpClient, ok := client.(*HTTPClient); ok {
		if httpClient == nil {
			return ConservativeMaintenanceCallOutcome(errors.New("nil HTTP backend"))
		}
		return httpClient.updateCall(ctx, request)
	}
	if client == nil {
		return ConservativeMaintenanceCallOutcome(errors.New("nil backend"))
	}
	return ConservativeMaintenanceCallOutcome(client.Update(ctx, request))
}

type callDisposition uint8

const (
	callDispositionAccepted callDisposition = iota + 1
	callDispositionRefused
	callDispositionNotDispatched
	callDispositionAmbiguous
)

type causalCallOutcome struct {
	disposition callDisposition
	err         error
}

func (outcome causalCallOutcome) valid() bool {
	switch outcome.disposition {
	case callDispositionAccepted:
		return outcome.err == nil
	case callDispositionRefused, callDispositionNotDispatched, callDispositionAmbiguous:
		return outcome.err != nil
	default:
		return false
	}
}

func (outcome causalCallOutcome) accepted() bool {
	return outcome.valid() && outcome.disposition == callDispositionAccepted
}
func (outcome causalCallOutcome) refused() bool {
	return outcome.valid() && outcome.disposition == callDispositionRefused
}
func (outcome causalCallOutcome) notDispatched() bool {
	return outcome.valid() && outcome.disposition == callDispositionNotDispatched
}
func (outcome causalCallOutcome) ambiguous() bool {
	return outcome.valid() && outcome.disposition == callDispositionAmbiguous
}

// ProvisionRefusal is the closed protocol verdict returned by a backend that
// positively refused one provision request before mutation.
type ProvisionRefusal uint8

const (
	ProvisionRefusalNone ProvisionRefusal = iota
	ProvisionRefusalValidation
	ProvisionRefusalCapacity
)

// ProvisionCallOutcome is a zero-invalid, closed result for one provision
// transport call. Only the backend package can mint definitive refusal or
// no-dispatch evidence.
type ProvisionCallOutcome struct {
	outcome causalCallOutcome
	refusal ProvisionRefusal
}

func (outcome ProvisionCallOutcome) Valid() bool {
	if !outcome.outcome.valid() {
		return false
	}
	if outcome.outcome.refused() {
		return outcome.refusal >= ProvisionRefusalValidation &&
			outcome.refusal <= ProvisionRefusalCapacity
	}
	return outcome.refusal == ProvisionRefusalNone
}
func (outcome ProvisionCallOutcome) Accepted() bool {
	return outcome.Valid() && outcome.outcome.accepted()
}
func (outcome ProvisionCallOutcome) Refused() bool {
	return outcome.Valid() && outcome.outcome.refused()
}
func (outcome ProvisionCallOutcome) NotDispatched() bool {
	return outcome.Valid() && outcome.outcome.notDispatched()
}
func (outcome ProvisionCallOutcome) Ambiguous() bool {
	return outcome.Valid() && outcome.outcome.ambiguous()
}
func (outcome ProvisionCallOutcome) Err() error { return outcome.outcome.err }
func (outcome ProvisionCallOutcome) Refusal() ProvisionRefusal {
	if !outcome.Refused() {
		return ProvisionRefusalNone
	}
	return outcome.refusal
}

// ConservativeProvisionCallOutcome adapts a Backend implementation that does
// not expose causal transport evidence. Success is accepted; every error is
// ambiguous and can never authorize durable evidence deletion.
func ConservativeProvisionCallOutcome(err error) ProvisionCallOutcome {
	return ProvisionCallOutcome{outcome: conservativeCallOutcome(err)}
}

// RestoreRefusal is the closed protocol verdict returned by a backend that
// positively refused one restore request before mutation.
type RestoreRefusal uint8

const (
	RestoreRefusalNone RestoreRefusal = iota
	RestoreRefusalNotRetained
	RestoreRefusalInvalidState
	RestoreRefusalCapacity
	RestoreRefusalDemoteDataExceedsTier
	RestoreRefusalValidation
)

// RestoreCallOutcome is the restore counterpart to ProvisionCallOutcome.
type RestoreCallOutcome struct {
	outcome causalCallOutcome
	refusal RestoreRefusal
}

func (outcome RestoreCallOutcome) Valid() bool {
	if !outcome.outcome.valid() {
		return false
	}
	if outcome.outcome.refused() {
		return outcome.refusal >= RestoreRefusalNotRetained &&
			outcome.refusal <= RestoreRefusalValidation
	}
	return outcome.refusal == RestoreRefusalNone
}
func (outcome RestoreCallOutcome) Accepted() bool {
	return outcome.Valid() && outcome.outcome.accepted()
}
func (outcome RestoreCallOutcome) Refused() bool {
	return outcome.Valid() && outcome.outcome.refused()
}
func (outcome RestoreCallOutcome) NotDispatched() bool {
	return outcome.Valid() && outcome.outcome.notDispatched()
}
func (outcome RestoreCallOutcome) Ambiguous() bool {
	return outcome.Valid() && outcome.outcome.ambiguous()
}
func (outcome RestoreCallOutcome) Err() error { return outcome.outcome.err }
func (outcome RestoreCallOutcome) Refusal() RestoreRefusal {
	if !outcome.Refused() {
		return RestoreRefusalNone
	}
	return outcome.refusal
}

// ConservativeRestoreCallOutcome adapts a legacy/custom backend without
// manufacturing definitive evidence from its arbitrary error tree.
func ConservativeRestoreCallOutcome(err error) RestoreCallOutcome {
	return RestoreCallOutcome{outcome: conservativeCallOutcome(err)}
}

// MaintenanceRefusal is the closed protocol verdict returned by a backend
// that positively refused a restart or update before mutation.
type MaintenanceRefusal uint8

const (
	MaintenanceRefusalNone MaintenanceRefusal = iota
	MaintenanceRefusalNotProvisioned
	MaintenanceRefusalInvalidState
	MaintenanceRefusalValidation
	MaintenanceRefusalCapacity
)

// MaintenanceCallOutcome is a zero-invalid causal result for restart/update.
// Refusal is meaningful only when Refused reports true.
type MaintenanceCallOutcome struct {
	outcome causalCallOutcome
	refusal MaintenanceRefusal
}

func (outcome MaintenanceCallOutcome) Valid() bool {
	if !outcome.outcome.valid() {
		return false
	}
	if outcome.outcome.refused() {
		return outcome.refusal >= MaintenanceRefusalNotProvisioned &&
			outcome.refusal <= MaintenanceRefusalCapacity
	}
	return outcome.refusal == MaintenanceRefusalNone
}
func (outcome MaintenanceCallOutcome) Accepted() bool {
	return outcome.Valid() && outcome.outcome.accepted()
}
func (outcome MaintenanceCallOutcome) Refused() bool {
	return outcome.Valid() && outcome.outcome.refused()
}
func (outcome MaintenanceCallOutcome) NotDispatched() bool {
	return outcome.Valid() && outcome.outcome.notDispatched()
}
func (outcome MaintenanceCallOutcome) Ambiguous() bool {
	return outcome.Valid() && outcome.outcome.ambiguous()
}
func (outcome MaintenanceCallOutcome) Err() error { return outcome.outcome.err }
func (outcome MaintenanceCallOutcome) Refusal() MaintenanceRefusal {
	if !outcome.Refused() {
		return MaintenanceRefusalNone
	}
	return outcome.refusal
}

// RefusalDetail exposes only the diagnostic carried by a definitive validation
// refusal. It grants no settlement authority; ambiguous/custom-backend errors
// cannot acquire tenant-visible detail by wrapping a public sentinel.
func (outcome MaintenanceCallOutcome) RefusalDetail() string {
	if outcome.Refusal() != MaintenanceRefusalValidation {
		return ""
	}
	detail, _ := Detail(outcome.Err())
	return detail
}

// ConservativeMaintenanceCallOutcome adapts a legacy/custom backend. It can
// represent only accepted or ambiguous; it cannot mint a terminal refusal.
func ConservativeMaintenanceCallOutcome(err error) MaintenanceCallOutcome {
	return MaintenanceCallOutcome{outcome: conservativeCallOutcome(err)}
}

func conservativeCallOutcome(err error) causalCallOutcome {
	if err == nil {
		return causalCallOutcome{disposition: callDispositionAccepted}
	}
	return causalCallOutcome{disposition: callDispositionAmbiguous, err: err}
}

func acceptedProvisionCall() ProvisionCallOutcome {
	return ProvisionCallOutcome{outcome: causalCallOutcome{disposition: callDispositionAccepted}}
}
func refusedProvisionCall(err error, refusal ProvisionRefusal) ProvisionCallOutcome {
	return ProvisionCallOutcome{
		outcome: causalCallOutcome{disposition: callDispositionRefused, err: err},
		refusal: refusal,
	}
}
func notDispatchedProvisionCall(err error) ProvisionCallOutcome {
	return ProvisionCallOutcome{outcome: causalCallOutcome{disposition: callDispositionNotDispatched, err: err}}
}
func ambiguousProvisionCall(err error) ProvisionCallOutcome {
	return ProvisionCallOutcome{outcome: causalCallOutcome{disposition: callDispositionAmbiguous, err: err}}
}

func acceptedRestoreCall() RestoreCallOutcome {
	return RestoreCallOutcome{outcome: causalCallOutcome{disposition: callDispositionAccepted}}
}
func refusedRestoreCall(err error, refusal RestoreRefusal) RestoreCallOutcome {
	return RestoreCallOutcome{
		outcome: causalCallOutcome{disposition: callDispositionRefused, err: err},
		refusal: refusal,
	}
}
func notDispatchedRestoreCall(err error) RestoreCallOutcome {
	return RestoreCallOutcome{outcome: causalCallOutcome{disposition: callDispositionNotDispatched, err: err}}
}
func ambiguousRestoreCall(err error) RestoreCallOutcome {
	return RestoreCallOutcome{outcome: causalCallOutcome{disposition: callDispositionAmbiguous, err: err}}
}

func acceptedMaintenanceCall() MaintenanceCallOutcome {
	return MaintenanceCallOutcome{outcome: causalCallOutcome{disposition: callDispositionAccepted}}
}
func refusedMaintenanceCall(err error, refusal MaintenanceRefusal) MaintenanceCallOutcome {
	return MaintenanceCallOutcome{
		outcome: causalCallOutcome{disposition: callDispositionRefused, err: err},
		refusal: refusal,
	}
}
func notDispatchedMaintenanceCall(err error) MaintenanceCallOutcome {
	return MaintenanceCallOutcome{outcome: causalCallOutcome{disposition: callDispositionNotDispatched, err: err}}
}
func ambiguousMaintenanceCall(err error) MaintenanceCallOutcome {
	return MaintenanceCallOutcome{outcome: causalCallOutcome{disposition: callDispositionAmbiguous, err: err}}
}
