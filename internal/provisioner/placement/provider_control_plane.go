package placement

import (
	"context"
	"errors"
	"fmt"
	"slices"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/util"
)

// ProviderControlPlane is the one broad composition-root port from which all
// purpose-specific placement applications derive their chain authority. It is
// bound once to the Store/Registry/backend aggregate before any application is
// constructed; downstream code receives only narrow typed coordinators.
type ProviderControlPlane interface {
	GetLease(context.Context, string) (*billingtypes.Lease, error)
	GetPendingLeases(context.Context, string) ([]billingtypes.Lease, error)
	GetActiveLeasesByProvider(context.Context, string) ([]billingtypes.Lease, error)
	RejectLeases(context.Context, []string, string) (uint64, []string, error)
	CloseLeases(context.Context, []string, string) (uint64, []string, error)
	Acknowledge(context.Context, string) (bool, string, error)
}

// The following narrow read ports remain useful to adapters and tests, but no
// production coordinator constructor accepts one independently. All executable
// authority is derived from ProviderControlPlane after its one-time binding.
type PruneLeaseReader interface {
	GetLease(context.Context, string) (*billingtypes.Lease, error)
}

type ProvisionLeaseReader = PruneLeaseReader
type RestoreLeaseReader = PruneLeaseReader
type MaintenanceLeaseReader = PruneLeaseReader

type CallbackChain interface {
	PruneLeaseReader
	RejectLeases(context.Context, []string, string) (uint64, []string, error)
}

type CallbackAcknowledger interface {
	Acknowledge(context.Context, string) (bool, string, error)
}

// exactLeaseObservation is a sealed sum. A mutation decision can consume an
// exact lease, a positive authority mismatch, or uncertainty. Ledger history
// is immutable, so NotFound, nil, and wrong-UUID responses are all uncertainty
// rather than authority to retire durable evidence.
type exactLeaseObservation interface {
	exactLeaseObservation()
}

type observedExactLease struct{ lease billingtypes.Lease }
type observedLeaseUnauthorized struct{ lease billingtypes.Lease }
type observedLeaseUnknown struct{ err error }

func (observedExactLease) exactLeaseObservation()        {}
func (observedLeaseUnauthorized) exactLeaseObservation() {}
func (observedLeaseUnknown) exactLeaseObservation()      {}

type boundProviderControlPlane struct {
	execution    *ExecutionCoordinator
	providerUUID string
	control      ProviderControlPlane
}

func cloneProviderLease(lease *billingtypes.Lease) billingtypes.Lease {
	if lease == nil {
		return billingtypes.Lease{}
	}
	clone := *lease
	clone.Items = append([]billingtypes.LeaseItem(nil), lease.Items...)
	clone.MetaHash = append([]byte(nil), lease.MetaHash...)
	if lease.ClosedAt != nil {
		value := *lease.ClosedAt
		clone.ClosedAt = &value
	}
	if lease.AcknowledgedAt != nil {
		value := *lease.AcknowledgedAt
		clone.AcknowledgedAt = &value
	}
	if lease.RejectedAt != nil {
		value := *lease.RejectedAt
		clone.RejectedAt = &value
	}
	if lease.ExpiredAt != nil {
		value := *lease.ExpiredAt
		clone.ExpiredAt = &value
	}
	return clone
}

func (control *boundProviderControlPlane) validFor(execution *ExecutionCoordinator) bool {
	return control != nil && execution != nil && execution.coordinator != nil &&
		execution.coordinator.Valid() && execution.issuer == execution.coordinator.marker &&
		execution.coordinator.execution == execution && !util.IsNilInterface(execution.backends) &&
		execution.callbacks != nil && execution.callbacks.Valid() &&
		execution.deprovision != nil && execution.deprovision.execution == execution &&
		control.execution == execution && execution.controlPlane == control &&
		control.providerUUID != "" &&
		control.providerUUID == execution.coordinator.store.providerUUID &&
		!util.IsNilInterface(control.control)
}

func (execution *ExecutionCoordinator) providerControlPlane() (*boundProviderControlPlane, error) {
	if !execution.Valid() || execution.controlPlane == nil ||
		!execution.controlPlane.validFor(execution) {
		return nil, errors.New("provider control plane is not bound")
	}
	return execution.controlPlane, nil
}

func (control *boundProviderControlPlane) observeLease(
	ctx context.Context,
	leaseUUID string,
	expectedTenant string,
) exactLeaseObservation {
	if !control.validFor(control.execution) || ctx == nil || leaseUUID == "" {
		return observedLeaseUnknown{err: errors.New("invalid exact lease observation")}
	}
	lease, err := control.control.GetLease(ctx, leaseUUID)
	if err != nil {
		return observedLeaseUnknown{err: err}
	}
	if lease == nil {
		return observedLeaseUnknown{err: errors.New("exact lease observation returned no result")}
	}
	if lease.Uuid != leaseUUID {
		return observedLeaseUnknown{
			err: fmt.Errorf("lease %s observation returned UUID %q", leaseUUID, lease.Uuid),
		}
	}
	copy := cloneProviderLease(lease)
	if copy.ProviderUuid != control.providerUUID ||
		(expectedTenant != "" && copy.Tenant != expectedTenant) {
		return observedLeaseUnauthorized{lease: copy}
	}
	return observedExactLease{lease: copy}
}

func (control *boundProviderControlPlane) observeLeaseBounded(
	ctx context.Context,
	leaseUUID string,
	expectedTenant string,
) exactLeaseObservation {
	readCtx, cancel := context.WithTimeout(ctx, pruneConfirmationTimeout)
	defer cancel()
	return control.observeLease(readCtx, leaseUUID, expectedTenant)
}

func exactLeaseObservationError(observation exactLeaseObservation) error {
	switch observation := observation.(type) {
	case observedExactLease:
		return nil
	case observedLeaseUnauthorized:
		return errors.New("lease observation is outside the bound tenant or provider authority")
	case observedLeaseUnknown:
		if observation.err != nil {
			return observation.err
		}
		return errors.New("lease observation is unknown")
	default:
		return errors.New("lease observation is invalid")
	}
}

func exactLeaseFromObservation(
	observation exactLeaseObservation,
) (billingtypes.Lease, bool) {
	exact, ok := observation.(observedExactLease)
	return exact.lease, ok
}

func (control *boundProviderControlPlane) rejectLease(
	ctx context.Context,
	leaseUUID string,
	reason string,
) (uint64, []string, error) {
	if !control.validFor(control.execution) || ctx == nil || leaseUUID == "" {
		return 0, nil, errors.New("invalid provider rejection authority")
	}
	return control.control.RejectLeases(ctx, []string{leaseUUID}, reason)
}

func (control *boundProviderControlPlane) closeLease(
	ctx context.Context,
	leaseUUID string,
	reason string,
) (uint64, []string, error) {
	if !control.validFor(control.execution) || ctx == nil || leaseUUID == "" {
		return 0, nil, errors.New("invalid provider close authority")
	}
	return control.control.CloseLeases(ctx, []string{leaseUUID}, reason)
}

func (control *boundProviderControlPlane) acknowledgeLease(
	ctx context.Context,
	leaseUUID string,
) (bool, string, error) {
	if !control.validFor(control.execution) || ctx == nil || leaseUUID == "" {
		return false, "", errors.New("invalid provider acknowledgement authority")
	}
	return control.control.Acknowledge(ctx, leaseUUID)
}

func (control *boundProviderControlPlane) inventoryLeases(
	ctx context.Context,
	state billingtypes.LeaseState,
) ([]billingtypes.Lease, error) {
	if !control.validFor(control.execution) || ctx == nil {
		return nil, errors.New("invalid provider inventory authority")
	}
	var (
		leases []billingtypes.Lease
		err    error
	)
	switch state {
	case billingtypes.LEASE_STATE_PENDING:
		leases, err = control.control.GetPendingLeases(ctx, control.providerUUID)
	case billingtypes.LEASE_STATE_ACTIVE:
		leases, err = control.control.GetActiveLeasesByProvider(ctx, control.providerUUID)
	default:
		return nil, errors.New("invalid provider inventory state")
	}
	if err != nil {
		return nil, err
	}
	result := make([]billingtypes.Lease, 0, len(leases))
	seen := make(map[string]struct{}, len(leases))
	for index := range leases {
		lease := &leases[index]
		if lease.Uuid == "" || lease.ProviderUuid != control.providerUUID || lease.State != state {
			return nil, fmt.Errorf(
				"provider inventory item %d does not match bound provider/state authority", index,
			)
		}
		if _, duplicate := seen[lease.Uuid]; duplicate {
			return nil, fmt.Errorf("provider inventory repeats lease %s", lease.Uuid)
		}
		seen[lease.Uuid] = struct{}{}
		result = append(result, cloneProviderLease(lease))
	}
	return slices.Clip(result), nil
}
