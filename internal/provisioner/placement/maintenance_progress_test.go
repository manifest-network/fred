package placement

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type maintenanceProgressPayloads struct {
	lease  string
	bytes  []byte
	writes int
}

func (payloads *maintenanceProgressPayloads) OverwritePayload(lease string, data []byte) error {
	payloads.lease = lease
	payloads.bytes = append([]byte(nil), data...)
	payloads.writes++
	return nil
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
