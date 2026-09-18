package placement

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/callbackwire"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

type callbackApplyChain struct{}

func (callbackApplyChain) GetLease(context.Context, string) (*billingtypes.Lease, error) {
	return nil, nil
}

func TestRecoveredCallbackDoesNotDoubleCountOperationMetrics(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a")
	id, err := operation.ParseID("2b0fb1e9-b9ad-4f52-a93d-69e8eb72830a")
	require.NoError(t, err)
	requireTypedAttempt(t, store, leaseUUID, "backend-a", id)
	coordinator, err := NewOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	execution := bindExecutionForTest(t, coordinator, executionRuntime("backend-a"))
	setProviderControlPlaneForTest(t, execution, callbackApplyControlPlane{})
	verifier, consumer := hmacauth.NewCallbackProofBoundary()
	callbacks, err := execution.AuthenticatedCallbackCoordinator(consumer)
	require.NoError(t, err)
	storageID, ok := coordinator.ExpectedBackendStorageIdentity("backend-a")
	require.True(t, ok)
	body := []byte(fmt.Sprintf(
		`{"lease_uuid":%q,"status":"success","backend_storage_id":%q}`,
		leaseUUID, storageID.String(),
	))
	uri := "/callbacks/provision?operation_id=" + id.String()
	now := time.Unix(1700000000, 0)
	secret := "callback-apply-test-secret-0123456789"
	proof, err := verifier.VerifyRoutedWithTime(
		secret, http.MethodPost, uri, body,
		hmacauth.SignWithTime(secret, http.MethodPost, uri, body, now),
		storageID.String(), "/callbacks/provision", 5*time.Minute, time.Minute, now,
	)
	require.NoError(t, err)

	result, err := callbacks.Apply(context.Background(), proof)
	require.NoError(t, err)
	require.Equal(t, CallbackOperationNone, result.OperationOutcome())
}

func (callbackApplyChain) RejectLeases(context.Context, []string, string) (uint64, []string, error) {
	return 0, nil, nil
}

type callbackApplyAcknowledger struct{}

func (callbackApplyAcknowledger) Acknowledge(context.Context, string) (bool, string, error) {
	return true, "", nil
}

type callbackApplyControlPlane struct {
	callbackApplyChain
	callbackApplyAcknowledger
}

type callbackMutationControlPlane struct {
	callbackApplyControlPlane

	mu        sync.Mutex
	leases    []billingtypes.Lease
	reads     int
	ackErr    error
	rejectErr error
}

func (control *callbackMutationControlPlane) GetLease(
	context.Context, string,
) (*billingtypes.Lease, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	if len(control.leases) == 0 {
		return nil, errors.New("chain observation unavailable")
	}
	index := min(control.reads, len(control.leases)-1)
	control.reads++
	lease := control.leases[index]
	return &lease, nil
}

func (control *callbackMutationControlPlane) RejectLeases(
	context.Context, []string, string,
) (uint64, []string, error) {
	return 0, nil, control.rejectErr
}

func (control *callbackMutationControlPlane) Acknowledge(
	context.Context, string,
) (bool, string, error) {
	return false, "", control.ackErr
}

func applyOperationCallbackForTest(
	t *testing.T,
	fixture coordinatorFixture,
	leaseUUID string,
	control *callbackMutationControlPlane,
	status backend.CallbackStatus,
) (CallbackResult, error) {
	t.Helper()
	execution := bindExecutionForTest(t, fixture.coordinator, executionRuntime("backend-a"))
	setProviderControlPlaneForTest(t, execution, control)
	verifier, consumer := hmacauth.NewCallbackProofBoundary()
	callbacks, err := execution.AuthenticatedCallbackCoordinator(consumer)
	require.NoError(t, err)
	storageID, ok := fixture.coordinator.ExpectedBackendStorageIdentity("backend-a")
	require.True(t, ok)
	body := []byte(fmt.Sprintf(
		`{"lease_uuid":%q,"status":%q,"error":"backend failed","backend_storage_id":%q}`,
		leaseUUID, status, storageID.String(),
	))
	uri := "/callbacks/provision?operation_id=" + fixture.initiation.ID().String()
	now := time.Unix(1700000000, 0)
	secret := "callback-apply-test-secret-0123456789"
	proof, err := verifier.VerifyRoutedWithTime(
		secret, http.MethodPost, uri, body,
		hmacauth.SignWithTime(secret, http.MethodPost, uri, body, now),
		storageID.String(), "/callbacks/provision", 5*time.Minute, time.Minute, now,
	)
	require.NoError(t, err)
	return callbacks.Apply(t.Context(), proof)
}

func callbackLeaseForTest(leaseUUID string, state billingtypes.LeaseState) billingtypes.Lease {
	return billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
		State: state,
	}
}

func TestCallbackMutationErrorsCannotSettleWithoutPositiveObservation(t *testing.T) {
	tests := []struct {
		name   string
		status backend.CallbackStatus
		build  func(string) *callbackMutationControlPlane
	}{
		{
			name: "success with forged custom Is", status: backend.CallbackStatusSuccess,
			build: func(leaseUUID string) *callbackMutationControlPlane {
				return &callbackMutationControlPlane{
					leases: []billingtypes.Lease{
						callbackLeaseForTest(leaseUUID, billingtypes.LEASE_STATE_PENDING),
					},
					ackErr: forgedNotPendingMutationError{},
				}
			},
		},
		{
			name: "failure with joined error", status: backend.CallbackStatusFailed,
			build: func(leaseUUID string) *callbackMutationControlPlane {
				return &callbackMutationControlPlane{
					leases: []billingtypes.Lease{
						callbackLeaseForTest(leaseUUID, billingtypes.LEASE_STATE_PENDING),
					},
					rejectErr: errors.Join(
						billingtypes.ErrLeaseNotPending, context.DeadlineExceeded,
					),
				}
			},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			leaseUUID := fmt.Sprintf("550e8400-e29b-41d4-a716-4466554401%02d", index)
			fixture := newCoordinatorFixture(t, leaseUUID)
			_, err := applyOperationCallbackForTest(
				t, fixture, leaseUUID, test.build(leaseUUID), test.status,
			)
			require.Error(t, err)
			assert.Equal(t, StateAttempting, fixture.store.Lookup(leaseUUID).State())
			assert.True(t, fixture.coordinator.RuntimeController().Contains(leaseUUID),
				"an error shape cannot manufacture callback settlement authority")
		})
	}
}

func TestCallbackPositiveObservationConvergesRegardlessOfMutationErrorShape(t *testing.T) {
	tests := []struct {
		name        string
		status      backend.CallbackStatus
		control     func(string) *callbackMutationControlPlane
		wantOutcome CallbackOperationOutcome
		wantState   State
	}{
		{
			name:   "plain acknowledgement error with active lease",
			status: backend.CallbackStatusSuccess,
			control: func(leaseUUID string) *callbackMutationControlPlane {
				return &callbackMutationControlPlane{
					leases: []billingtypes.Lease{
						callbackLeaseForTest(leaseUUID, billingtypes.LEASE_STATE_ACTIVE),
					},
					ackErr: errors.New("ambiguous acknowledgement result"),
				}
			},
			wantOutcome: CallbackOperationSucceeded,
			wantState:   StateConfirmed,
		},
		{
			name:   "joined rejection error with rejected lease",
			status: backend.CallbackStatusFailed,
			control: func(leaseUUID string) *callbackMutationControlPlane {
				return &callbackMutationControlPlane{
					leases: []billingtypes.Lease{
						callbackLeaseForTest(leaseUUID, billingtypes.LEASE_STATE_PENDING),
						callbackLeaseForTest(leaseUUID, billingtypes.LEASE_STATE_REJECTED),
					},
					rejectErr: errors.Join(
						billingtypes.ErrLeaseNotPending, context.DeadlineExceeded,
					),
				}
			},
			wantOutcome: CallbackOperationFailed,
			wantState:   StateAbsent,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			leaseUUID := fmt.Sprintf("550e8400-e29b-41d4-a716-4466554402%02d", index)
			fixture := newCoordinatorFixture(t, leaseUUID)
			result, err := applyOperationCallbackForTest(
				t, fixture, leaseUUID, test.control(leaseUUID), test.status,
			)
			require.NoError(t, err)
			assert.Equal(t, test.wantOutcome, result.OperationOutcome())
			assert.Equal(t, test.wantState, fixture.store.Lookup(leaseUUID).State())
			assert.False(t, fixture.coordinator.RuntimeController().Contains(leaseUUID))
		})
	}
}

func TestAuthenticatedCallbackCoordinatorRejectsNonCanonicalLeaseBeforeLookup(t *testing.T) {
	fixture := newCoordinatorFixture(t, "550e8400-e29b-41d4-a716-446655440000")
	execution := bindExecutionForTest(t, fixture.coordinator, executionRuntime("backend-a"))
	setProviderControlPlaneForTest(t, execution, callbackApplyControlPlane{})
	verifier, consumer := hmacauth.NewCallbackProofBoundary()
	coordinator, err := execution.AuthenticatedCallbackCoordinator(consumer)
	require.NoError(t, err)
	body := []byte(`{"lease_uuid":"lease-1","status":"success"}`)
	now := time.Unix(1700000000, 0)
	signature := hmacauth.SignWithTime(
		"callback-apply-test-secret-0123456789", http.MethodPost,
		"/callbacks/provision", body, now,
	)
	proof, err := verifier.VerifyRoutedWithTime(
		"callback-apply-test-secret-0123456789", http.MethodPost,
		"/callbacks/provision", body, signature, "", "/callbacks/provision",
		5*time.Minute, time.Minute, now,
	)
	require.NoError(t, err)

	_, err = coordinator.Apply(context.Background(), proof)
	require.ErrorIs(t, err, callbackwire.ErrInvalidPayload)
}

func TestAuthenticatedCallbackCoordinatorRejectsForeignProofBoundaryBeforeSettlement(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	fixture := newCoordinatorFixture(t, leaseUUID)
	execution := bindExecutionForTest(t, fixture.coordinator, executionRuntime("backend-a"))
	setProviderControlPlaneForTest(t, execution, callbackApplyControlPlane{})
	_, acceptedConsumer := hmacauth.NewCallbackProofBoundary()
	coordinator, err := execution.AuthenticatedCallbackCoordinator(acceptedConsumer)
	require.NoError(t, err)

	foreignVerifier, _ := hmacauth.NewCallbackProofBoundary()
	storageID, ok := fixture.coordinator.ExpectedBackendStorageIdentity("backend-a")
	require.True(t, ok)
	body := []byte(fmt.Sprintf(
		`{"lease_uuid":%q,"status":"success","backend_storage_id":%q}`,
		leaseUUID, storageID.String(),
	))
	uri := "/callbacks/provision?operation_id=" + fixture.initiation.ID().String()
	now := time.Unix(1700000000, 0)
	proof, err := foreignVerifier.VerifyRoutedWithTime(
		"callback-apply-test-secret-0123456789", http.MethodPost, uri, body,
		hmacauth.SignWithTime(
			"callback-apply-test-secret-0123456789", http.MethodPost, uri, body, now,
		),
		storageID.String(), "/callbacks/provision", 5*time.Minute, time.Minute, now,
	)
	require.NoError(t, err)
	before := fixture.store.Lookup(leaseUUID)

	_, err = coordinator.Apply(context.Background(), proof)
	require.ErrorIs(t, err, ErrCallbackProofBoundaryMismatch)
	require.Equal(t, before, fixture.store.Lookup(leaseUUID))
	require.True(t, fixture.coordinator.RuntimeController().Contains(leaseUUID))
}
