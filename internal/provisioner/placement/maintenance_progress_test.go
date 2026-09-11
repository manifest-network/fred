package placement

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

type maintenanceProgressPayloads struct {
	lease  string
	bytes  []byte
	writes int
	err    error
}

func (payloads *maintenanceProgressPayloads) OverwritePayload(lease string, data []byte) error {
	payloads.lease = lease
	payloads.bytes = append([]byte(nil), data...)
	payloads.writes++
	return payloads.err
}

func TestAcceptedUpdateTerminalLeaseReleasesDeprovisionFence(t *testing.T) {
	for _, terminal := range []billingtypes.LeaseState{
		billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_REJECTED, billingtypes.LEASE_STATE_EXPIRED,
	} {
		t.Run(terminal.String(), func(t *testing.T) {
			state := billingtypes.LEASE_STATE_ACTIVE
			reader := maintenanceLeaseReaderFunc(func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{Uuid: leaseUUID, Tenant: "tenant-test",
					ProviderUuid: freshTestProviderUUID, State: state}, nil
			})
			var updates, deprovisions int
			client := &executionTestBackend{name: "backend-a",
				update:      func(context.Context, backend.UpdateRequest) error { updates++; return nil },
				deprovision: func(context.Context, string) error { deprovisions++; return nil },
			}
			base, store := newMaintenanceCoordinatorForTest(t, reader, client)
			payloads := &maintenanceProgressPayloads{err: errors.New("payload store is permanently full")}
			coordinator, err := base.coordinator.execution.MaintenanceCoordinator(payloads)
			require.NoError(t, err)
			application, err := coordinator.Application(nil, 0)
			require.NoError(t, err)
			provision, err := base.coordinator.execution.ProvisionCoordinatorWithPayloads(nil, nil)
			require.NoError(t, err)
			command, err := NewMaintenanceApplicationRequest(mustMaintenanceID(t, maintenanceIDA),
				maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("accepted bytes"))
			require.NoError(t, err)
			require.Equal(t, MaintenanceApplicationInternalFailure, application.Execute(t.Context(), command).Outcome())
			require.ErrorContains(t, provision.Deprovision(t.Context(), maintenanceLease), "lifecycle action is busy")
			state = terminal
			require.NoError(t, application.RecoverPending(t.Context()))
			receipt, found, err := store.LookupMaintenanceCommand(maintenanceLease, command.id)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, MaintenanceOutcomeLeaseEnded, receipt.Outcome())
			assert.Empty(t, receipt.Command().Payload(), "terminal lease no longer needs accepted request bytes")
			assert.Equal(t, 1, payloads.writes, "terminal proof bypasses the broken payload store")
			assert.Equal(t, 1, updates, "accepted update cannot acquire backend dispatch authority again")
			assert.Equal(t, MaintenanceApplicationNoLongerActive, application.Execute(t.Context(), command).Outcome())
			require.NoError(t, provision.Deprovision(t.Context(), maintenanceLease))
			assert.Equal(t, 1, deprovisions, "the settled journal head releases actual deprovision execution")
		})
	}
}

func TestAcceptedUpdateWithoutExactTerminalEvidenceKeepsLocalRecovery(t *testing.T) {
	for name, change := range map[string]func(*billingtypes.Lease) (*billingtypes.Lease, error){
		"active": func(lease *billingtypes.Lease) (*billingtypes.Lease, error) { return lease, nil },
		"pending": func(lease *billingtypes.Lease) (*billingtypes.Lease, error) {
			lease.State = billingtypes.LEASE_STATE_PENDING
			return lease, nil
		},
		"unknown state": func(lease *billingtypes.Lease) (*billingtypes.Lease, error) {
			lease.State = billingtypes.LeaseState(127)
			return lease, nil
		},
		"not found": func(*billingtypes.Lease) (*billingtypes.Lease, error) { return nil, nil },
		"unreadable": func(*billingtypes.Lease) (*billingtypes.Lease, error) {
			return nil, errors.New("chain unavailable")
		},
		"wrong lease": func(lease *billingtypes.Lease) (*billingtypes.Lease, error) {
			lease.State, lease.Uuid = billingtypes.LEASE_STATE_CLOSED, "other-lease"
			return lease, nil
		},
		"wrong tenant": func(lease *billingtypes.Lease) (*billingtypes.Lease, error) {
			lease.State, lease.Tenant = billingtypes.LEASE_STATE_CLOSED, "other-tenant"
			return lease, nil
		},
		"wrong provider": func(lease *billingtypes.Lease) (*billingtypes.Lease, error) {
			lease.State, lease.ProviderUuid = billingtypes.LEASE_STATE_CLOSED, "other-provider"
			return lease, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			var updates int
			client := &executionTestBackend{name: "backend-a", update: func(context.Context, backend.UpdateRequest) error {
				updates++
				return nil
			}}
			base, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
			payloads := &maintenanceProgressPayloads{err: errors.New("payload store full")}
			coordinator, err := base.coordinator.execution.MaintenanceCoordinator(payloads)
			require.NoError(t, err)
			application, err := coordinator.Application(nil, 0)
			require.NoError(t, err)
			command, err := NewMaintenanceApplicationRequest(mustMaintenanceID(t, maintenanceIDA),
				maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("exact accepted payload"))
			require.NoError(t, err)
			require.Equal(t, MaintenanceApplicationInternalFailure, application.Execute(t.Context(), command).Outcome())
			setProviderControlPlaneForTest(t, base.coordinator.execution,
				maintenanceLeaseReaderFunc(func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
					lease, err := maintenanceActiveLeaseReader()(ctx, leaseUUID)
					if err != nil {
						return nil, err
					}
					return change(lease)
				}))
			require.ErrorContains(t, application.RecoverPending(t.Context()), "payload store full")
			receipt, found, err := store.LookupMaintenanceCommand(maintenanceLease, command.id)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, MaintenanceOutcomePending, receipt.Outcome())
			assert.Equal(t, command.payload, receipt.Command().Payload())
			assert.Contains(t, base.coordinator.RuntimeController().PendingLeaseUUIDs(), maintenanceLease)
			payloads.err = nil
			require.NoError(t, application.RecoverPending(t.Context()),
				"repairing the payload store must finish already accepted work despite uncertain chain state")
			receipt, found, err = store.LookupMaintenanceCommand(maintenanceLease, command.id)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, MaintenanceOutcomeAccepted, receipt.Outcome())
			assert.Equal(t, command.payload, payloads.bytes)
			assert.Equal(t, 1, updates)
			assert.NotContains(t, base.coordinator.RuntimeController().PendingLeaseUUIDs(), maintenanceLease)
		})
	}
}

func TestAcceptedUpdateTerminalExitRequiresExactOneShotCapability(t *testing.T) {
	base, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(),
		&executionTestBackend{name: "backend-a"})
	coordinator, err := base.coordinator.execution.MaintenanceCoordinator(&maintenanceProgressPayloads{})
	require.NoError(t, err)
	prepared := coordinator.prepareMaintenanceCommand(t.Context(), mustMaintenanceID(t, maintenanceIDA),
		maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("accepted bytes"))
	require.True(t, prepared.Authorized(), prepared.Err())
	admission, err := coordinator.beginMaintenanceCommand(prepared)
	require.NoError(t, err)
	work, err := store.maintenanceWork(admission.Claim())
	require.NoError(t, err)
	delivery, ok := work.(maintenanceDelivery)
	require.True(t, ok)
	accepted, err := store.acceptMaintenanceUpdate(delivery)
	require.NoError(t, err)
	require.ErrorIs(t, store.endAcceptedMaintenanceUpdate(endedMaintenanceUpdate{}), ErrInvalidMaintenanceCommand)
	require.ErrorIs(t, store.settleMaintenanceCommand(admission.Claim(), MaintenanceOutcomeLeaseEnded),
		ErrMaintenanceCommandNotPending, "generic delivery settlement cannot retire the accepted phase")
	assert.False(t, coordinator.observeEndedMaintenanceUpdate(t.Context(), accepted).valid(),
		"Active state cannot mint terminal cancellation")
	setProviderControlPlaneForTest(t, coordinator.coordinator.execution,
		maintenanceLeaseReaderFunc(func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{Uuid: leaseUUID, Tenant: "tenant-test",
				ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_CLOSED}, nil
		}))
	ended := coordinator.observeEndedMaintenanceUpdate(t.Context(), accepted)
	require.True(t, ended.valid())
	foreign := newTestStore(t)
	require.ErrorIs(t, foreign.endAcceptedMaintenanceUpdate(ended), ErrInvalidMaintenanceCommand)
	require.NoError(t, store.endAcceptedMaintenanceUpdate(ended))
	require.ErrorIs(t, store.endAcceptedMaintenanceUpdate(ended), ErrInvalidMaintenanceCommand)
	require.Error(t, coordinator.completeAcceptedUpdate(accepted).Err(),
		"retired accepted authority cannot overwrite payload bytes after lease teardown is unblocked")
}

func TestMaintenanceAcceptedPhaseCannotDispatchOrSettleWithoutPayloadCommit(t *testing.T) {
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(),
		&executionTestBackend{name: "backend-a"})
	payloads := &maintenanceProgressPayloads{}
	coordinator, err := authority.coordinator.execution.MaintenanceCoordinator(payloads)
	require.NoError(t, err)
	prepared := coordinator.prepareMaintenanceCommand(t.Context(), mustMaintenanceID(t, maintenanceIDA),
		maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("exact accepted update"))
	require.True(t, prepared.Authorized(), prepared.Err())
	admission, err := coordinator.beginMaintenanceCommand(prepared)
	require.NoError(t, err)
	claim := admission.Claim()
	require.ErrorIs(t, store.settleMaintenanceCommand(claim, MaintenanceOutcomeAccepted), ErrMaintenanceCommandNotPending,
		"the old generic transport settlement must not consume update bytes")
	work, err := store.maintenanceWork(claim)
	require.NoError(t, err)
	delivery, ok := work.(maintenanceDelivery)
	require.True(t, ok)
	accepted, err := store.acceptMaintenanceUpdate(delivery)
	require.NoError(t, err)
	assert.False(t, accepted.claim.Command().Dispatchable())
	_, err = store.acceptMaintenanceUpdate(delivery)
	require.ErrorIs(t, err, ErrMaintenanceCommandNotPending, "stale delivery authority cannot cross phase commit")
	require.ErrorIs(t, store.settleMaintenanceCommand(claim, MaintenanceOutcomeBackendUnavailable), ErrMaintenanceCommandNotPending)
	require.ErrorIs(t, store.completeMaintenanceUpdate(maintenancePayloadCommit{}), ErrInvalidMaintenanceCommand)
	reauthorized := coordinator.reauthorizeMaintenanceCommand(t.Context(), claim)
	assert.False(t, reauthorized.Authorized(), "rehydration must not mint backend work from known acceptance")
	require.True(t, reauthorized.payload.valid())
	completion := coordinator.completeAcceptedUpdate(reauthorized.payload)
	require.NoError(t, completion.Err())
	require.True(t, completion.Settled())
	assert.Equal(t, MaintenanceOutcomeAccepted, completion.Outcome())
	assert.Equal(t, maintenanceLease, payloads.lease)
	assert.Equal(t, []byte("exact accepted update"), payloads.bytes)
	require.Error(t, coordinator.completeAcceptedUpdate(accepted).Err())
	assert.Equal(t, 1, payloads.writes, "stale accepted capabilities cannot overwrite later payload state")
}

func TestMaintenancePhaseDecoderPreservesLegacyAmbiguityAndRejectsCrossPhaseRows(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	prepared := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("durable bytes"))
	created := time.Now().UTC()
	encoded, err := encodeMaintenanceCommand(prepared.Command(), MaintenanceOutcomePending, created, time.Time{})
	require.NoError(t, err)
	var record map[string]any
	require.NoError(t, json.Unmarshal(encoded, &record))
	delete(record, "phase")
	legacy, err := json.Marshal(record)
	require.NoError(t, err)
	command, outcome, _, _, _, err := decodeMaintenanceCommand(legacy)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceOutcomePending, outcome)
	assert.Equal(t, maintenanceDeliveryOutstanding, command.phase,
		"legacy pending rows cannot prove that an earlier process never dispatched")
	for name, change := range map[string]func(map[string]any){
		"unknown phase": func(row map[string]any) { row["phase"] = "invented" },
		"restart payload finalization": func(row map[string]any) {
			row["phase"] = "payload_outstanding"
			row["kind"] = "restart"
		},
		"terminal pending phase": func(row map[string]any) {
			row["phase"] = "payload_outstanding"
			row["outcome"] = "accepted"
			row["settled_at"] = created.Format(time.RFC3339Nano)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var row map[string]any
			require.NoError(t, json.Unmarshal(legacy, &row))
			change(row)
			invalid, err := json.Marshal(row)
			require.NoError(t, err)
			_, _, _, _, _, err = decodeMaintenanceCommand(invalid)
			require.Error(t, err)
		})
	}
}
