package placement

import (
	"context"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/provisioner/operation"
)

func TestRestoreRecoverySourcePlacement(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		prepare func(*testing.T, *Store)
		wantErr string
	}{
		{name: "absent source permits same-ID retry"},
		{
			name: "confirmed on attempted backend",
			prepare: func(t *testing.T, store *Store) {
				requireConfirmedPlacement(t, store, "source", "backend-a")
			},
		},
		{
			name: "confirmed on another backend",
			prepare: func(t *testing.T, store *Store) {
				requireConfirmedPlacement(t, store, "source", "backend-b")
			},
			wantErr: "restore source placement no longer matches attempted backend",
		},
		{
			name: "confirmed source with pending operation",
			prepare: func(t *testing.T, store *Store) {
				requireConfirmedPlacement(t, store, "source", "backend-a")
				requireTypedAttempt(t, store, "source", "backend-a", requireOperationID(t, "7602"))
			},
			wantErr: "restore source placement no longer matches attempted backend",
		},
		{
			name: "ambiguous source placement",
			prepare: func(t *testing.T, store *Store) {
				requireConflictPlacement(t, store, "source", "backend-a", "backend-b")
			},
			wantErr: "restore source placement is ambiguous",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := newRestoreTestStore(t)
			claim, err := beginTestRestore(t, store, store.CurrentAdmissionBaseline(),
				"source", "target", requireOperationID(t, "7601"))
			require.NoError(t, err)
			consumed, err := store.abandonRestore(claim)
			require.NoError(t, err)
			require.True(t, consumed)
			metadata := store.Lookup("target").AttemptMetadata()
			require.True(t, metadata.Valid())

			// Retain the exact target attempt while varying only the source's
			// current placement. Absence can follow consumption by this attempt.
			deleted, err := store.deleteRecord(store.Lookup("source").RecordRevision())
			require.NoError(t, err)
			require.True(t, deleted)
			require.Equal(t, StateAbsent, store.Lookup("source").State())
			if test.prepare != nil {
				test.prepare(t, store)
			}
			coordinator, err := NewOperationCoordinator(store, operation.NewRegistry())
			require.NoError(t, err)
			runtime := executionRuntime("backend-a", "backend-b")
			execution := bindExecutionForTest(t, coordinator, runtime)
			setProviderControlPlaneForTest(t, execution, pruneLeaseReaderFunc(func(
				_ context.Context, leaseUUID string,
			) (*billingtypes.Lease, error) {
				require.Equal(t, "source", leaseUUID)
				return &billingtypes.Lease{
					Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
					State: billingtypes.LEASE_STATE_REJECTED,
				}, nil
			}))
			_, recovery, err := coordinator.bindReconciliation(
				execution.controlPlane, runtime, nil, inventoryProjectorForTest(t, store),
			)
			require.NoError(t, err)
			err = recovery.validateRestoreSource(t.Context(),
				&billingtypes.Lease{Tenant: "tenant-test"}, "backend-a", metadata)
			if test.wantErr != "" {
				require.EqualError(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, metadata.OperationID(), store.Lookup("target").AttemptOperationID())
		})
	}
}
