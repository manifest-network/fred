package placement

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestMaintenanceRefusalDetailEncodingPreservesBoundedDiagnostics(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	command := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("manifest")).Command()
	created := time.Now().UTC()
	for name, detail := range map[string]string{
		"ASCII at byte limit":          strings.Repeat("a", 4<<10),
		"UTF-8 at byte limit":          strings.Repeat("é", 2<<10),
		"JSON escaping expansion":      strings.Repeat("&", 4<<10),
		"older receipt without detail": "",
	} {
		t.Run(name, func(t *testing.T) {
			encoded, committed, err := encodeMaintenanceSettlement(command, maintenanceSettlement{
				outcome: MaintenanceOutcomeValidationRejected, detail: detail,
			}, created, created)
			require.NoError(t, err)
			assert.Equal(t, detail, committed, "normal bounded diagnostics must not be shortened")
			receipt, outcome, _, _, decoded, err := decodeMaintenanceCommand(encoded)
			require.NoError(t, err)
			assert.Equal(t, MaintenanceOutcomeValidationRejected, outcome)
			assert.Equal(t, detail, decoded)
			assert.False(t, receipt.Dispatchable())
			assert.Empty(t, receipt.Payload())
		})
	}
}

func TestMaintenanceRefusalDetailEncoderRejectsInvalidDiagnostics(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	command := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("manifest")).Command()
	created := time.Now().UTC()
	for name, detail := range map[string]string{
		"one byte beyond limit": strings.Repeat("a", (4<<10)+1),
		"invalid UTF-8":         string([]byte{0xff}),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := encodeMaintenanceSettlement(command, maintenanceSettlement{
				outcome: MaintenanceOutcomeValidationRejected, detail: detail,
			}, created, created)
			require.ErrorContains(t, err, "invalid maintenance refusal detail")
		})
	}
	for outcome := MaintenanceOutcomePending; outcome <= MaintenanceOutcomeBackendUnavailable; outcome++ {
		if outcome == MaintenanceOutcomeValidationRejected {
			continue
		}
		t.Run("detail forbidden on "+outcome.String(), func(t *testing.T) {
			_, _, err := encodeMaintenanceSettlement(command, maintenanceSettlement{
				outcome: outcome, detail: "not a validation refusal",
			}, created, created)
			require.ErrorContains(t, err, "invalid maintenance refusal detail")
		})
	}
}

func TestMaintenanceRefusalDetailDecoderRejectsInvalidDiagnostics(t *testing.T) {
	store := newTestStore(t)
	authority := prepareMaintenanceLease(t, store)
	command := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("manifest")).Command()
	created := time.Now().UTC()
	valid, _, err := encodeMaintenanceSettlement(command, maintenanceSettlement{
		outcome: MaintenanceOutcomeValidationRejected, detail: "seed",
	}, created, created)
	require.NoError(t, err)
	t.Run("one byte beyond limit", func(t *testing.T) {
		var row persistedMaintenanceCommand
		require.NoError(t, json.Unmarshal(valid, &row))
		row.Detail = strings.Repeat("a", (4<<10)+1)
		encoded, err := json.Marshal(row)
		require.NoError(t, err)
		_, _, _, _, _, err = decodeMaintenanceCommand(encoded)
		require.ErrorContains(t, err, "invalid maintenance refusal detail")
	})
	t.Run("invalid UTF-8 wire bytes", func(t *testing.T) {
		// Marshal would repair invalid UTF-8. Corrupt the actual durable bytes
		// so this exercises strict decoding instead of the encoding guard.
		encoded := bytes.Replace(valid, []byte(`"detail":"seed"`), []byte{'"', 'd', 'e', 't', 'a', 'i', 'l', '"', ':', '"', 0xff, '"'}, 1)
		require.NotEqual(t, valid, encoded)
		_, _, _, _, _, err := decodeMaintenanceCommand(encoded)
		require.Error(t, err)
	})
	for outcome := MaintenanceOutcomePending; outcome <= MaintenanceOutcomeBackendUnavailable; outcome++ {
		if outcome == MaintenanceOutcomeValidationRejected {
			continue
		}
		t.Run("detail forbidden on "+outcome.String(), func(t *testing.T) {
			var row persistedMaintenanceCommand
			require.NoError(t, json.Unmarshal(valid, &row))
			row.Outcome = outcome.String()
			encoded, err := json.Marshal(row)
			require.NoError(t, err)
			_, _, _, _, _, err = decodeMaintenanceCommand(encoded)
			require.ErrorContains(t, err, "invalid maintenance refusal detail")
		})
	}
}

func TestLegacyMaintenanceRefusalFitsReceiptAndReturnsCommittedDetail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placements.db")
	store, err := newStoreForTest(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	authority := prepareMaintenanceLease(t, store)
	prepared := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandUpdate, []byte("manifest"))
	admission, err := store.beginMaintenanceCommand(prepared)
	require.NoError(t, err)
	require.True(t, admission.Pending())
	key := maintenanceReceiptKey(maintenanceLease, prepared.Command().ID())
	growLegacyMaintenancePendingRecord(t, store, key)
	// Reopen before settling: the command must acquire authority from its
	// historical durable representation, without borrowing the old claim.
	require.NoError(t, store.Close())
	reopened, err := newStoreForTest(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	claims, err := reopened.pendingMaintenanceCommands()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	want := strings.Repeat("🧪<&", 600)
	committed, err := reopened.settleMaintenanceDelivery(claims[0], maintenanceSettlement{
		outcome: MaintenanceOutcomeValidationRejected, detail: want,
	})
	require.NoError(t, err, "diagnostics must not prevent an admitted command from settling")
	require.True(t, committed.Valid())
	assert.Equal(t, MaintenanceOutcomeValidationRejected, committed.Outcome())
	require.NotEmpty(t, committed.Detail())
	assert.Less(t, len(committed.Detail()), len(want), "only this historical metadata-heavy receipt needs truncation")
	assert.True(t, utf8.ValidString(committed.Detail()))
	assert.False(t, committed.Command().Dispatchable())
	assert.Empty(t, committed.Command().Payload())
	stored, found, err := reopened.LookupMaintenanceCommand(maintenanceLease, prepared.Command().ID())
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, committed.Detail(), stored.Detail(), "first response must use the exact committed diagnostic")
	claims, err = reopened.pendingMaintenanceCommands()
	require.NoError(t, err)
	assert.Empty(t, claims, "a complete refusal must release the pending command")
	require.NoError(t, reopened.Close())
	again, err := newStoreForTest(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = again.Close() })
	stored, found, err = again.LookupMaintenanceCommand(maintenanceLease, prepared.Command().ID())
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, committed.Detail(), stored.Detail(), "reopen replay must preserve the first response's diagnostic")
}

func TestLegacyMaintenanceUpdateCanCommitAcceptedPayloadAboveNewAdmissionLimit(t *testing.T) {
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(),
		&executionTestBackend{name: "backend-a"})
	payloads := &maintenanceProgressPayloads{}
	coordinator, err := authority.coordinator.execution.MaintenanceCoordinator(payloads)
	require.NoError(t, err)
	const payload = "accepted legacy update"
	prepared := coordinator.prepareMaintenanceCommand(t.Context(), mustMaintenanceID(t, maintenanceIDA),
		maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte(payload))
	require.True(t, prepared.Authorized(), prepared.Err())
	admission, err := coordinator.beginMaintenanceCommand(prepared)
	require.NoError(t, err)
	require.True(t, admission.Pending())
	growLegacyMaintenancePendingRecord(t, store, maintenanceReceiptKey(maintenanceLease, admission.Claim().Command().ID()))
	claims, err := store.pendingMaintenanceCommands()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	work, err := store.maintenanceWork(claims[0])
	require.NoError(t, err)
	delivery, ok := work.(maintenanceDelivery)
	require.True(t, ok)
	accepted, err := store.acceptMaintenanceUpdate(delivery)
	require.NoError(t, err, "adding the accepted phase must not apply the stricter new-admission budget to a legacy row")
	assert.False(t, accepted.claim.Command().Dispatchable())
	completion := coordinator.completeAcceptedUpdate(accepted)
	require.NoError(t, completion.Err())
	require.True(t, completion.Settled())
	assert.Equal(t, MaintenanceOutcomeAccepted, completion.Outcome())
	assert.Equal(t, []byte(payload), payloads.bytes)
	assert.Equal(t, 1, payloads.writes)
	stored, found, err := store.LookupMaintenanceCommand(maintenanceLease, admission.Claim().Command().ID())
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, MaintenanceOutcomeAccepted, stored.Outcome())
	assert.Empty(t, stored.Detail())
	assert.Empty(t, stored.Command().Payload())
}

func TestMaintenanceAdmissionReservesDiagnosticSpaceBeforeWritingJournal(t *testing.T) {
	// Construct the large callback through the supported configuration boundary,
	// rather than manufacturing a prepared command with different authority.
	routes, err := NewCallbackRouteFactory("https://provider.test/" + strings.Repeat("a", maxMaintenanceCommandAdmissionBytes))
	require.NoError(t, err)
	store := newTestStore(t, WithCallbackRouteFactory(routes))
	prepareMaintenanceLease(t, store)
	id := mustMaintenanceID(t, maintenanceIDA)
	prepared, err := store.prepareMaintenanceCommand(id, maintenanceLease, MaintenanceCommandRestart, nil)
	require.NoError(t, err)
	encoded, err := encodeMaintenanceCommand(prepared.Command(), MaintenanceOutcomePending, time.Now().UTC(), time.Time{})
	require.NoError(t, err, "the wider serializer must remain available for old commands")
	require.Greater(t, len(encoded), maxMaintenanceCommandAdmissionBytes)
	admission, err := store.beginMaintenanceCommand(prepared)
	require.ErrorContains(t, err, "admission budget")
	assert.False(t, admission.Pending())
	_, found, err := store.LookupMaintenanceCommand(maintenanceLease, id)
	require.NoError(t, err)
	assert.False(t, found, "failed admission must not write a receipt")
	claims, err := store.pendingMaintenanceCommands()
	require.NoError(t, err)
	assert.Empty(t, claims, "failed admission must not hold the lease pending")
}

func growLegacyMaintenancePendingRecord(t *testing.T, store *Store, key []byte) {
	t.Helper()
	// Before diagnostics were persisted, pending commands could fill this
	// larger admission budget. Preserve that compatibility after the new
	// admission limit reserves worst-case JSON-escaped diagnostic space.
	const legacyPendingBudget = maxMaintenanceCommandValueBytes - 512
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		_, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		var row persistedMaintenanceCommand
		if err := json.Unmarshal(records.Get(key), &row); err != nil {
			return err
		}
		row.Phase = "" // Legacy pending rows did not have a phase field.
		encoded, err := json.Marshal(row)
		if err != nil {
			return err
		}
		row.CallbackURL = strings.Replace(row.CallbackURL, "/callbacks/provision",
			strings.Repeat("a", legacyPendingBudget-len(encoded))+"/callbacks/provision", 1)
		encoded, err = json.Marshal(row)
		if err != nil {
			return err
		}
		require.Len(t, encoded, legacyPendingBudget)
		_, _, _, _, _, err = decodeMaintenanceCommand(encoded)
		require.NoError(t, err, "legacy near-limit command must remain a valid journal row")
		return records.Put(key, encoded)
	}))
}
