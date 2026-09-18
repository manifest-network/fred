package placement

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/util"
)

// reconciliationProvisionRoute is an exact name selected by the backend
// runtime permanently bound to one reconciliation authority. It contains no
// backend client and cannot be used by another coordinator.
type reconciliationProvisionRoute struct {
	issuer      *reconciliationCoordinatorMarker
	leaseUUID   string
	backendName string
}

func (route reconciliationProvisionRoute) valid() bool {
	return route.issuer != nil && route.leaseUUID != "" && route.backendName != ""
}
func (route reconciliationProvisionRoute) backend() string {
	if !route.valid() {
		return ""
	}
	return route.backendName
}

func (authority *ReconciliationCoordinator) BackendNames() ([]string, error) {
	if !authority.Valid() {
		return nil, errors.New("reconciliation coordinator is invalid")
	}
	return backendNames(authority.backends)
}

func (authority *ReconciliationCoordinator) routeProvision(
	ctx context.Context,
	leaseUUID, sku string,
	ownedRevision RecordRevision,
	eligible map[string]struct{},
	inFlight map[string]int,
) (reconciliationProvisionRoute, error) {
	if !authority.Valid() || leaseUUID == "" {
		return reconciliationProvisionRoute{}, ErrProvisionRouteUnresolvable
	}
	var candidate backend.Backend
	var err error
	if ownedRevision.Valid() {
		current := authority.coordinator.store.Lookup(leaseUUID)
		if current.RecordRevision() != ownedRevision || current.State() != StateConfirmed {
			return reconciliationProvisionRoute{}, ErrProvisionRouteUnresolvable
		}
		candidate, err = exactBackend(authority.backends, current.Backend)
	} else {
		candidate = authority.backends.RouteForProvisionAmong(ctx, sku, eligible, inFlight)
		if util.IsNilInterface(candidate) {
			return reconciliationProvisionRoute{}, nil
		}
		candidate, err = exactBackend(authority.backends, candidate.Name())
	}
	if err != nil {
		return reconciliationProvisionRoute{}, fmt.Errorf("%w: %w", ErrProvisionRouteUnresolvable, err)
	}
	return reconciliationProvisionRoute{
		issuer: authority.marker, leaseUUID: leaseUUID, backendName: candidate.Name(),
	}, nil
}

func (authority *ReconciliationCoordinator) executeProvision(
	ctx context.Context,
	dispatch ProvisionDispatch,
	payload []byte,
) DispatchResult {
	if !authority.Valid() {
		return DispatchResult{err: ErrOperationSettlementGenerationUnavailable}
	}
	return executeProvision(
		ctx, authority.coordinator, authority.backends, authority.observe, dispatch, payload,
	)
}

type BackendProvisionInventory struct {
	backendName string
	provisions  []backend.ProvisionInfo
	storageID   backendidentity.ID
	refreshErr  error
	sweep       *ReconciliationSweep
	marker      *reconciliationSweepMarker
	sweepID     uint64
	receipt     *inventoryResponseMarker
}

func (inventory BackendProvisionInventory) BackendName() string { return inventory.backendName }
func (inventory BackendProvisionInventory) Provisions() []backend.ProvisionInfo {
	return cloneProvisionInventory(inventory.provisions)
}
func (inventory BackendProvisionInventory) StorageID() backendidentity.ID {
	return inventory.storageID
}
func (inventory BackendProvisionInventory) RefreshErr() error { return inventory.refreshErr }

func cloneProvisionInventory(input []backend.ProvisionInfo) []backend.ProvisionInfo {
	output := make([]backend.ProvisionInfo, len(input))
	for index, provision := range input {
		output[index] = provision
		output[index].Items = slices.Clone(provision.Items)
		output[index].ServiceImages = maps.Clone(provision.ServiceImages)
		if provision.LifecycleGeneration != nil {
			generation := *provision.LifecycleGeneration
			output[index].LifecycleGeneration = &generation
		}
	}
	return output
}

func (authority *ReconciliationCoordinator) collectProvisionInventory(
	ctx context.Context,
	backendName string,
) (result BackendProvisionInventory, err error) {
	if !authority.Valid() {
		return BackendProvisionInventory{}, errors.New("reconciliation coordinator is invalid")
	}
	client, err := exactBackend(authority.backends, backendName)
	if err != nil {
		return BackendProvisionInventory{}, err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.ReconcilerPanicsTotal.WithLabelValues("fetch_provisions").Inc()
			result = BackendProvisionInventory{}
			err = fmt.Errorf("collect provision inventory from %q panicked: %v\n%s",
				backendName, recovered, debug.Stack())
		}
	}()
	refreshErr := client.RefreshState(ctx)
	identityClient, ok := client.(backend.IdentityInventoryBackend)
	if !ok {
		return BackendProvisionInventory{}, errors.New("backend does not expose identity-bearing inventory")
	}
	provisions, storageID, err := identityClient.ListProvisionsWithIdentity(ctx)
	if err != nil {
		return BackendProvisionInventory{}, err
	}
	sealed := cloneProvisionInventory(provisions)
	for index := range sealed {
		sealed[index].BackendName = backendName
	}
	return BackendProvisionInventory{
		backendName: backendName, provisions: sealed,
		storageID: storageID, refreshErr: refreshErr,
	}, nil
}

type BackendRetentionInventory struct {
	backendName string
	retentions  []backend.RetainedLease
	storageID   backendidentity.ID
	sweep       *ReconciliationSweep
	marker      *reconciliationSweepMarker
	sweepID     uint64
	receipt     *inventoryResponseMarker
}

func (inventory BackendRetentionInventory) BackendName() string { return inventory.backendName }
func (inventory BackendRetentionInventory) Retentions() []backend.RetainedLease {
	return append([]backend.RetainedLease(nil), inventory.retentions...)
}
func (inventory BackendRetentionInventory) StorageID() backendidentity.ID {
	return inventory.storageID
}

func (authority *ReconciliationCoordinator) collectRetentionInventory(
	ctx context.Context,
	backendName string,
) (result BackendRetentionInventory, err error) {
	if !authority.Valid() {
		return BackendRetentionInventory{}, errors.New("reconciliation coordinator is invalid")
	}
	client, err := exactBackend(authority.backends, backendName)
	if err != nil {
		return BackendRetentionInventory{}, err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.ReconcilerPanicsTotal.WithLabelValues("fetch_retentions").Inc()
			result = BackendRetentionInventory{}
			err = fmt.Errorf("collect retention inventory from %q panicked: %v\n%s",
				backendName, recovered, debug.Stack())
		}
	}()
	identityClient, ok := client.(backend.IdentityInventoryBackend)
	if !ok {
		return BackendRetentionInventory{}, errors.New("backend does not expose identity-bearing inventory")
	}
	retentions, storageID, err := identityClient.ListRetentionsWithIdentity(ctx)
	if err != nil {
		return BackendRetentionInventory{}, err
	}
	return BackendRetentionInventory{
		backendName: backendName,
		retentions:  append([]backend.RetainedLease(nil), retentions...),
		storageID:   storageID,
	}, nil
}

func (authority *ReconciliationCoordinator) reconcileCustomDomain(
	ctx context.Context,
	backendName, leaseUUID string,
	items []backend.LeaseItem,
) (err error) {
	client, err := exactBackend(authority.backends, backendName)
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.BackendInvocationPanicsTotal.WithLabelValues(
				metrics.OperationReconcileCustomDomain,
			).Inc()
			err = fmt.Errorf("backend ReconcileCustomDomain panicked: %v", recovered)
		}
	}()
	return client.ReconcileCustomDomain(ctx, leaseUUID, append([]backend.LeaseItem(nil), items...))
}

func (authority *ReconciliationCoordinator) deprovisionExact(
	ctx context.Context,
	backendName, leaseUUID string,
) error {
	client, err := exactBackend(authority.backends, backendName)
	if err != nil {
		return err
	}
	return invokeDeprovision(ctx, client, leaseUUID)
}

func (authority *ReconciliationCoordinator) beginNewAttemptForRoute(
	scope AdmissionScope,
	leaseUUID string,
	route reconciliationProvisionRoute,
	operationID operation.OperationID,
	payload PayloadFingerprint,
	request BackendRequestSnapshot,
	callbacks CallbackPair,
) (AttemptToken, bool, error) {
	if !route.valid() || route.issuer != authority.marker || route.leaseUUID != leaseUUID {
		return AttemptToken{}, false, ErrProvisionRouteUnresolvable
	}
	return authority.coordinator.store.beginNewAttempt(
		scope, leaseUUID, route.backendName, operationID, payload, request, callbacks,
	)
}

func (authority *ReconciliationCoordinator) beginOwnedAttemptForRoute(
	baseline AdmissionBaseline,
	revision RecordRevision,
	route reconciliationProvisionRoute,
	operationID operation.OperationID,
	payload PayloadFingerprint,
	request BackendRequestSnapshot,
	callbacks CallbackPair,
) (AttemptToken, bool, error) {
	if !route.valid() || route.issuer != authority.marker ||
		revision.leaseUUID != route.leaseUUID {
		return AttemptToken{}, false, ErrProvisionRouteUnresolvable
	}
	return authority.coordinator.store.beginOwnedAttempt(
		baseline, revision, route.backendName, operationID, payload, request, callbacks,
	)
}
