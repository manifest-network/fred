package docker

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// Corruption must be rejected where authority enters the system, not necessarily
// by every downstream comparison. Decoding owns keyed lease and callback-token
// consistency, and the bound probe additionally checks backend/storage lineage;
// restore recovery owns agreement between individually valid journals. Exercise
// both sides without forging a sealed OperationRecoveryState implementation.
func TestRestoreRecoveryRejectsCorruptOperationJournal(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		stateName := "pending"
		if terminal {
			stateName = "terminal probe"
		}
		t.Run(stateName, func(t *testing.T) {
			t.Run("matching reopened authority", func(t *testing.T) {
				b, source, _ := newRestoreAuthorityRecoveryFixture(t, nil)
				if terminal {
					claims, err := b.operationSettlement.ListOperationIntents()
					require.NoError(t, err)
					require.Len(t, claims, 1)
					require.NoError(t, b.resolvePreEffectOperationRefusal(claims[0], "interrupted restore"))
				}
				require.NoError(t, b.callbackStore.Close())
				require.NoError(t, reopenRestoreOperationJournalForTest(t, b))
				state, err := b.currentRestoreOperation(source)
				require.NoError(t, err, "reopening must preserve valid recovery authority")
				if terminal {
					_, failed := state.(shared.OperationFailed)
					require.True(t, failed)
				} else {
					_, pending := state.(shared.OperationIntentClaim)
					require.True(t, pending)
					require.NoError(t, reconcileRestoreAuthorityForTest(t, b))
				}
			})
			for _, test := range []struct {
				name   string
				fields map[string]any
			}{
				{"kind", map[string]any{"kind": "provision", "source_lease_uuid": "", "source_generation": 0}},
				{"lease key", map[string]any{"lease_uuid": restoreAuthorityOtherLease}},
				{"source lease", map[string]any{"source_lease_uuid": restoreAuthorityOtherLease}},
				{"source generation", map[string]any{"source_generation": 999}},
				{"backend", map[string]any{"backend": "another-backend"}},
				{"storage identity", map[string]any{"backend_storage_id": "6ba7b811-9dad-41d1-80b4-00c04fd430c8"}},
				{"operation ID", map[string]any{"operation_id": "6ba7b811-9dad-41d1-80b4-00c04fd430c8"}},
				{"tenant", map[string]any{"tenant": "another-tenant"}},
				{"provider", map[string]any{"provider_uuid": restoreAuthorityOtherLease}},
				{"effective items only", map[string]any{"effective_items": []backend.LeaseItem{{
					SKU: "docker-small", Quantity: 1, ServiceName: "app",
				}}}},
				{"health-check services", map[string]any{"health_check_services": []string{"app"}}},
			} {
				t.Run(test.name, func(t *testing.T) {
					b, source, calls := newRestoreAuthorityRecoveryFixture(t, func(spec *shared.OperationIntentSpec) {
						// A supported DNS deferral can make only EffectiveItems differ
						// from the retained destination. Items must still agree, so this
						// case independently exercises the effective-items comparison.
						spec.Items[0].CustomDomain = "desired.example"
						spec.EffectiveItems[0].CustomDomain = "desired.example"
					})
					if terminal {
						claims, err := b.operationSettlement.ListOperationIntents()
						require.NoError(t, err)
						require.Len(t, claims, 1)
						require.NoError(t, b.resolvePreEffectOperationRefusal(claims[0], "interrupted restore"))
					}
					state, err := b.currentRestoreOperation(source)
					require.NoError(t, err, "the uncorrupted operation must reach the exact recovery probe")
					if terminal {
						_, failed := state.(shared.OperationFailed)
						require.True(t, failed)
					}
					beforeAllocation := b.pool.GetAllocation(restoreAuthorityDestination + "-app-0")
					require.NotNil(t, beforeAllocation)
					beforeProjection := recoveredFromProvision(b.provisions[restoreAuthorityDestination])
					require.NoError(t, b.callbackStore.Close())
					mutateStoppedRestoreOperation(t, b.cfg.CallbackDBPath, test.fields)
					before := restoreAuthorityJournalRecords(t, b)
					recoveryErr := reopenRestoreOperationJournalForTest(t, b)
					if recoveryErr == nil {
						recoveryErr = reconcileRestoreAuthorityForTest(t, b)
					}
					require.Error(t, recoveryErr, "corrupt authority must fail decoding or cross-journal recovery")
					assert.Equal(t, before, restoreAuthorityJournalRecords(t, b))
					assert.Equal(t, *beforeAllocation, *b.pool.GetAllocation(restoreAuthorityDestination + "-app-0"))
					assert.True(t, provisionMatchesRecovered(b.provisions[restoreAuthorityDestination], beforeProjection))
					assert.Equal(t, restoreAuthoritySubstrateCalls{}, *calls)
				})
			}
		})
	}
}

// Opening a callback store may commit an idempotent schema transaction, which
// changes bbolt metapages but not journal records. Compare every bucket, sequence,
// key and value rather than treating storage-engine bookkeeping as a mutation.
func restoreAuthorityJournalRecords(t *testing.T, b *Backend) map[string]map[string][]byte {
	t.Helper()
	result := make(map[string]map[string][]byte)
	for path, data := range restoreAuthorityJournalBytes(t, b) {
		copyPath := filepath.Join(t.TempDir(), "journal.db")
		require.NoError(t, os.WriteFile(copyPath, data, 0o600))
		db, err := bolt.Open(copyPath, 0o600, &bolt.Options{ReadOnly: true})
		require.NoError(t, err)
		records := make(map[string][]byte)
		var visit func([]string, *bolt.Bucket) error
		visit = func(path []string, bucket *bolt.Bucket) error {
			key, err := json.Marshal(path)
			if err != nil {
				return err
			}
			sequence, err := json.Marshal(bucket.Sequence())
			if err != nil {
				return err
			}
			records[string(key)] = sequence
			return bucket.ForEach(func(key, value []byte) error {
				child := append(append([]string(nil), path...), string(key))
				if value == nil {
					return visit(child, bucket.Bucket(key))
				}
				encoded, err := json.Marshal(child)
				if err != nil {
					return err
				}
				records[string(encoded)] = bytes.Clone(value)
				return nil
			})
		}
		require.NoError(t, db.View(func(tx *bolt.Tx) error {
			return tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
				return visit([]string{string(name)}, bucket)
			})
		}))
		require.NoError(t, db.Close())
		result[path] = records
	}
	return result
}

func mutateStoppedRestoreOperation(t *testing.T, path string, fields map[string]any) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("callback_lease_mutation_heads"))
		require.NotNil(t, bucket)
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(bucket.Get([]byte(restoreAuthorityDestination)), &envelope); err != nil {
			return err
		}
		var row map[string]json.RawMessage
		if err := json.Unmarshal(envelope["operation"], &row); err != nil {
			return err
		}
		for field, value := range fields {
			encoded, err := json.Marshal(value)
			if err != nil {
				return err
			}
			row[field] = encoded
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return err
		}
		envelope["operation"] = encoded
		encoded, err = json.Marshal(envelope)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(restoreAuthorityDestination), encoded)
	}))
	require.NoError(t, db.Close())
}

func reopenRestoreOperationJournalForTest(t *testing.T, b *Backend) error {
	t.Helper()
	callbacks, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath}, b.storageAuthority, b.storeAuthorityGate,
	)
	if err != nil {
		return err
	}
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	b.callbackStore = callbacks
	operations, err := shared.NewOperationSettlement(callbacks, b.releaseStore)
	if err != nil {
		return err
	}
	b.operationSettlement = operations
	b.restoreSettlement, err = shared.NewRestoreSettlement(operations, b.retentionStore)
	if err != nil {
		return err
	}
	b.maintenanceSettlement, err = shared.NewMaintenanceSettlement(callbacks, b.releaseStore)
	if err != nil {
		return err
	}
	b.closeSettlement, err = shared.NewCloseSettlement(callbacks, b.releaseStore, b.retentionStore)
	if err == nil {
		bindBackendRecoveryCoordinatorForTest(t, b)
	}
	return err
}
