package placement

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func TestAuthenticatedMaintenanceCompletionReattestsPersistedCommandLineage(t *testing.T) {
	for _, changed := range []string{"backend", "storage", "lifecycle", "legacy route"} {
		t.Run(changed, func(t *testing.T) {
			base, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
			payloads := &maintenanceProgressPayloads{bytes: []byte("committed payload")}
			authority, err := base.coordinator.execution.MaintenanceCoordinator(payloads)
			require.NoError(t, err)
			application, err := authority.Application(nil, 0)
			require.NoError(t, err)
			id := mustMaintenanceID(t, maintenanceIDA)
			request, err := NewMaintenanceApplicationRequest(id, maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("candidate payload"))
			require.NoError(t, err)
			require.Equal(t, MaintenanceApplicationAccepted, application.Execute(t.Context(), request).Outcome())
			record, found, err := store.LookupMaintenanceCommand(maintenanceLease, id)
			require.NoError(t, err)
			require.True(t, found)
			command := record.Command()
			route, err := url.Parse(command.CallbackURL())
			require.NoError(t, err)
			body, err := json.Marshal(backend.CallbackPayload{
				LeaseUUID: maintenanceLease, Status: backend.CallbackStatusSuccess,
				BackendStorageID: command.BackendStorageID().String(), MaintenanceID: id.String(),
			})
			require.NoError(t, err)
			verifier, consumer := hmacauth.NewCallbackProofBoundary()
			callbacks, err := base.coordinator.execution.AuthenticatedCallbackCoordinator(consumer)
			require.NoError(t, err)
			apply := func() error {
				now := time.Now()
				const secret = "maintenance-persisted-lineage-callback-secret"
				proof, err := verifier.VerifyRoutedWithTime(secret, http.MethodPost, route.RequestURI(), body,
					hmacauth.SignWithTime(secret, http.MethodPost, route.RequestURI(), body, now),
					command.BackendStorageID().String(), route.Path, time.Minute, time.Minute, now)
				require.NoError(t, err)
				_, err = callbacks.Apply(t.Context(), proof)
				return err
			}

			// Model a structurally valid journal restored with another runtime's
			// command. The callback still authenticates against current placement;
			// only the exact persisted-command boundary can reject this mismatch.
			var original []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				_, records, err := maintenanceCommandBuckets(tx)
				if err != nil {
					return err
				}
				key := maintenanceReceiptKey(maintenanceLease, id)
				original = append([]byte(nil), records.Get(key)...)
				var row persistedMaintenanceCommand
				require.NoError(t, json.Unmarshal(original, &row))
				switch changed {
				case "backend":
					row.BackendName = "backend-b"
				case "storage":
					row.BackendStorageID = testBackendStorageID("backend-b").String()
				case "lifecycle":
					row.LifecycleID = maintenanceIDB
					foreign := *route
					query := foreign.Query()
					query.Set("lifecycle_id", maintenanceIDB)
					foreign.RawQuery = query.Encode()
					row.CallbackURL = foreign.String()
				case "legacy route":
					row.LifecycleKind, row.LifecycleID = "legacy", ""
					foreign := *route
					foreign.RawQuery = ""
					row.CallbackURL = foreign.String()
				}
				encoded, err := json.Marshal(row)
				if err != nil {
					return err
				}
				_, _, _, _, _, err = decodeMaintenanceCommand(encoded)
				if err != nil {
					return err
				}
				return records.Put(key, encoded)
			}))
			require.ErrorContains(t, apply(), "maintenance completion lineage differs from exact command")
			record, found, err = store.LookupMaintenanceCommand(maintenanceLease, id)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, MaintenanceOutcomePending, record.Outcome())
			require.Equal(t, maintenanceCompletionOutstanding, record.Command().phase)
			require.Empty(t, store.maintenanceChanged, "mismatched command lineage cannot schedule payload promotion")
			require.Zero(t, payloads.writes)

			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				_, records, err := maintenanceCommandBuckets(tx)
				if err != nil {
					return err
				}
				return records.Put(maintenanceReceiptKey(maintenanceLease, id), original)
			}))
			require.NoError(t, apply(), "rejection must preserve the real exact completion")
			require.NoError(t, application.RecoverPending(t.Context()))
			require.Equal(t, []byte("candidate payload"), payloads.bytes)
			require.Equal(t, 1, payloads.writes)
		})
	}
}
