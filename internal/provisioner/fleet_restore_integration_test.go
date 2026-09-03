package provisioner

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	restoreapp "github.com/manifest-network/fred/internal/provisioner/restore"
)

// TestFleet_RestoreCarriesTypedOperationAcrossHTTPAndSettlesInlineCallback is
// deliberately cross-layer: source ownership comes from a complete multi-
// backend inventory, restore crosses the real signed HTTP client, and an
// exact callback races the synchronous response before returning through the
// application service. Unit tests cover each component; this test proves the
// capabilities remain the same identity at their boundaries.
func TestFleet_RestoreCarriesTypedOperationAcrossHTTPAndSettlesInlineCallback(t *testing.T) {
	t.Parallel()
	f := newFleet(t, fleetOptions{})

	const (
		sourceLease = "lease-retained-source"
		targetLease = "lease-restore-target"
	)
	sourceLeaseUUID := fleetLeaseUUID(sourceLease)
	targetLeaseUUID := fleetLeaseUUID(targetLease)
	owner := f.backendAt(2)
	f.addLease(sourceLease, billingtypes.LEASE_STATE_CLOSED, "sku-restore")
	owner.seedRetention(sourceLease)
	require.NoError(t, f.sweep(), "complete inventory should establish restore affinity")
	f.assertPlacementPinned(sourceLease, owner.name)

	// Add the fresh target only after inventory authority is established. A
	// reconcile sweep here would correctly provision it; the restore command is
	// the competing lifecycle action this test intends to exercise instead.
	f.addLease(targetLease, billingtypes.LEASE_STATE_PENDING, "sku-restore")

	operations := f.tracker.Operations()
	events := &fleetRestoreEventRecorder{}
	callbacks, err := newCallbackServiceForTest(CallbackServiceConfig{
		Operations: operations,
		Chain:      f.chain,
		Acknowledger: fleetRestoreAcknowledgerFunc(func(
			_ context.Context, leaseUUID string,
		) (bool, string, error) {
			f.chainMu.Lock()
			f.acked = append(f.acked, leaseUUID)
			f.chainMu.Unlock()
			return true, "restore-ack", nil
		}),
		Placement: f.placement,
		Events:    events,
		Backends:  f.router,
	})
	require.NoError(t, err)

	owner.setRestoreHook(func(ctx context.Context, request backend.RestoreRequest) error {
		callbackURL, parseErr := url.Parse(request.CallbackURL)
		if parseErr != nil {
			return fmt.Errorf("parse restore callback URL: %w", parseErr)
		}
		operationID, present, parseErr := operation.ParseQuery(callbackURL.Query())
		if parseErr != nil {
			return fmt.Errorf("parse restore operation ID: %w", parseErr)
		}
		if !present {
			return fmt.Errorf("restore callback URL has no operation ID")
		}
		command, commandErr := NewCallbackCommand(backend.CallbackPayload{
			LeaseUUID:        request.LeaseUUID,
			Status:           backend.CallbackStatusSuccess,
			Backend:          owner.name,
			OperationID:      operationID.String(),
			BackendStorageID: defaultCallbackTestStorageIdentity.String(),
		})
		if commandErr != nil {
			return fmt.Errorf("build exact callback command: %w", commandErr)
		}
		if callbackErr := callbacks.HandleCallback(ctx, command); callbackErr != nil {
			return fmt.Errorf("apply inline restore callback: %w", callbackErr)
		}
		return nil
	})

	service, err := restoreapp.NewService(restoreapp.Config{
		ProviderUUID: f.providerUUID,
		CallbackURL: func(id operation.OperationID) (string, error) {
			return BuildCallbackURLForOperation("http://fred.invalid", id)
		},
		Leases:     f.chain,
		Backends:   restoreapp.BackendResolverFunc(f.resolveRestoreBackend),
		Operations: operations,
		Authority:  f.placement,
		Events:     events,
	})
	require.NoError(t, err)

	result := service.Execute(t.Context(), restoreapp.Command{
		TargetLeaseUUID: targetLeaseUUID,
		Tenant:          "tenant-1",
		SourceLeaseUUID: sourceLeaseUUID,
	})
	require.True(t, result.Accepted(), "restore failed: %v", result.Cause())
	require.Equal(t, owner.name, result.BackendName)

	for _, server := range f.servers {
		want := 0
		if server == owner {
			want = 1
		}
		require.Equalf(t, want, server.restoreCount(targetLease),
			"restore affinity must route only to %s", owner.name)
	}
	request, ok := owner.restoreRequest(targetLease)
	require.True(t, ok)
	require.Equal(t, sourceLeaseUUID, request.FromLeaseUUID)
	require.Equal(t, targetLeaseUUID, request.LeaseUUID)
	require.Equal(t, f.providerUUID, request.ProviderUUID)
	require.Equal(t, "tenant-1", request.Tenant)

	require.Equal(t, placement.StateConfirmed, f.placement.Lookup(targetLeaseUUID).State())
	f.assertPlacementPinned(targetLease, owner.name)
	f.assertPlacementPinned(sourceLease, owner.name)
	require.False(t, operations.Contains(targetLeaseUUID),
		"the inline exact callback must finish the process-local operation")
	acked, _, _ := f.chainCalls()
	require.Contains(t, acked, targetLeaseUUID)
	require.Equal(t,
		[]backend.ProvisionStatus{
			backend.ProvisionStatusRestarting,
			backend.ProvisionStatusReady,
		},
		events.statuses(targetLeaseUUID),
		"the pre-call event must precede an inline terminal callback",
	)

	// Both lifecycle claims must be released even though the callback completed
	// during the backend call. Reacquiring them is a black-box leak check.
	for _, leaseUUID := range []string{sourceLeaseUUID, targetLeaseUUID} {
		claim := operations.TryClaimLeaseNow(leaseUUID)
		require.Truef(t, claim.Acquired(), "lifecycle claim leaked for %s", leaseUUID)
		require.True(t, operations.ReleaseLease(claim.Claim()))
	}
}

func (f *fleet) resolveRestoreBackend(name string) restoreapp.RestoreBackend {
	resolved := f.router.GetBackendByName(name)
	if resolved == nil {
		return nil
	}
	return resolved
}

type fleetRestoreAcknowledgerFunc func(context.Context, string) (bool, string, error)

func (ack fleetRestoreAcknowledgerFunc) Acknowledge(
	ctx context.Context,
	leaseUUID string,
) (bool, string, error) {
	return ack(ctx, leaseUUID)
}

type fleetRestoreEventRecorder struct {
	mu     sync.Mutex
	events []backend.LeaseStatusEvent
}

func (recorder *fleetRestoreEventRecorder) Publish(event backend.LeaseStatusEvent) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.events = append(recorder.events, event)
}

func (recorder *fleetRestoreEventRecorder) PublishCallbackLeaseEvent(
	leaseUUID string,
	status backend.ProvisionStatus,
	errMsg string,
) {
	recorder.Publish(backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    status,
		Error:     errMsg,
		Timestamp: time.Now(),
	})
}

func (recorder *fleetRestoreEventRecorder) statuses(leaseUUID string) []backend.ProvisionStatus {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	statuses := make([]backend.ProvisionStatus, 0, len(recorder.events))
	for _, event := range recorder.events {
		if event.LeaseUUID == leaseUUID {
			statuses = append(statuses, event.Status)
		}
	}
	return statuses
}
