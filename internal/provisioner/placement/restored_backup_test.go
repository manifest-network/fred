package placement

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// newRestoredBackupFixture returns a stopped provider-bound database carrying
// an admission baseline and complete drain evidence, as any copy of a running
// provider's database does.
func newRestoredBackupFixture(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := newProviderBoundRepairStore(t, dbPath)
	requireTestAdmission(t, store)
	require.NoError(t, store.Close())
	return dbPath
}

func stoppedTopologyMetadata(t *testing.T, dbPath string) topologyMetadata {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	var metadata topologyMetadata
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		metadata, err = loadTopologyMetadata(tx)
		return err
	}))
	return metadata
}

func publishRestoredBackupRollback(t *testing.T, repair *AttemptRepair) *ExactBackupTarget {
	t.Helper()
	target, err := BindExactBackupTarget(filepath.Join(t.TempDir(), "pre-attestation.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })
	require.NoError(t, repair.CreateExactBackup(target))
	return target
}

func TestRestoredBackupAttestationForgetsOnlyAdmissionAndDrainEvidence(t *testing.T) {
	dbPath := newRestoredBackupFixture(t)
	before := stoppedTopologyMetadata(t, dbPath)
	require.NotZero(t, before.BaselineTopologyID)
	require.NotZero(t, before.InventoryTopologyID)
	restoredBytes, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	plan, err := repair.PlanRestoredBackupAttestation()
	require.NoError(t, err)
	facts := plan.Facts()
	assert.True(t, facts.Required)
	assert.Equal(t, freshTestProviderUUID, facts.ProviderUUID)
	assert.Equal(t, []string{"backend-a", "backend-b"}, facts.Topology)
	assert.Equal(t, before.TopologyID, facts.TopologyID)
	assert.Equal(t, before.KnownBackendStorageIDs["backend-a"], facts.StorageIDs["backend-a"])
	assert.Equal(t, before.KnownBackendStorageIDs["backend-b"], facts.StorageIDs["backend-b"])
	assert.True(t, strings.HasPrefix(plan.ConfirmationValue(), "attest-restored-backup:"))

	_, err = repair.AttestRestoredBackup(plan)
	require.Error(t, err, "the exact rollback image must be published before the mutation")
	assert.Equal(t, before, stoppedTopologyMetadataFromRepair(t, repair))

	backup := publishRestoredBackupRollback(t, repair)
	result, err := repair.AttestRestoredBackup(plan)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())

	backupBytes, err := os.ReadFile(backup.Path())
	require.NoError(t, err)
	assert.Equal(t, restoredBytes, backupBytes, "the rollback image is the restored copy, byte for byte")

	expected := before
	expected.BaselineFingerprint = ""
	expected.BaselineTopologyID = 0
	expected.InventoryTopologyID = 0
	expected.EmptyInventoryBackends = nil
	assert.Equal(t, expected, stoppedTopologyMetadata(t, dbPath),
		"attestation must forget only the evidence a stale copy cannot vouch for")

	inspector, err := OpenRepairInspector(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	require.NoError(t, inspector.VerifyRestoredBackupPostcondition(result))
	require.NoError(t, inspector.Close())

	store, err := OpenStore(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	assert.False(t, store.CurrentAdmissionBaseline().Valid(),
		"recordless admission must wait for one complete inventory of the running fleet")
	assert.Equal(t, InventoryAwaitingBaseline, store.InventoryReadiness())
}

// stoppedTopologyMetadataFromRepair reads through the repair session's own
// handle, which holds the exclusive lock.
func stoppedTopologyMetadataFromRepair(t *testing.T, repair *AttemptRepair) topologyMetadata {
	t.Helper()
	_, metadata, err := readRestoredBackupMetadata(repair.store.db)
	require.NoError(t, err)
	return metadata
}

func TestRestoredBackupAttestationRefusesAPlanItDidNotMintOrThatWentStale(t *testing.T) {
	dbPath := newRestoredBackupFixture(t)

	first, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	foreignPlan, err := first.PlanRestoredBackupAttestation()
	require.NoError(t, err)
	require.NoError(t, first.Close())

	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	publishRestoredBackupRollback(t, repair)
	_, err = repair.AttestRestoredBackup(foreignPlan)
	require.ErrorIs(t, err, ErrRestoredBackupTarget)
	_, err = repair.AttestRestoredBackup(RestoredBackupPlan{})
	require.ErrorIs(t, err, ErrRestoredBackupTarget)

	plan, err := repair.PlanRestoredBackupAttestation()
	require.NoError(t, err)
	changed := stoppedTopologyMetadataFromRepair(t, repair)
	changed.EmptyInventoryBackends = changed.EmptyInventoryBackends[:1]
	require.NoError(t, repair.store.db.Update(func(tx *bolt.Tx) error {
		return putTopologyMetadata(tx, changed)
	}))
	_, err = repair.AttestRestoredBackup(plan)
	require.ErrorIs(t, err, ErrRestoredBackupTarget)
	assert.Equal(t, changed, stoppedTopologyMetadataFromRepair(t, repair))
}

func TestRestoredBackupAttestationIsNotRequiredTwice(t *testing.T) {
	dbPath := newRestoredBackupFixture(t)
	repair, err := OpenAttemptRepair(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	plan, err := repair.PlanRestoredBackupAttestation()
	require.NoError(t, err)
	publishRestoredBackupRollback(t, repair)
	_, err = repair.AttestRestoredBackup(plan)
	require.NoError(t, err)

	again, err := repair.PlanRestoredBackupAttestation()
	require.NoError(t, err)
	assert.False(t, again.Facts().Required)
	_, err = repair.AttestRestoredBackup(again)
	require.ErrorIs(t, err, ErrRestoredBackupTarget)
}

func TestRestoredBackupAttestationRefusesAnotherProvidersDatabase(t *testing.T) {
	dbPath := newRestoredBackupFixture(t)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	repair, err := OpenAttemptRepair(dbPath, "00000000-0000-4000-8000-00000000abcd")
	require.ErrorIs(t, err, ErrProviderAuthorityMismatch)
	assert.Nil(t, repair)
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestRestoredBackupConfirmationBindsProviderPathAndMetadata(t *testing.T) {
	metadata := topologyMetadata{ProviderUUID: freshTestProviderUUID, TopologyID: 7}
	encoded := []byte(`{"schema":2}`)
	base := restoredBackupConfirmation(metadata, "/var/lib/fred/placements.db", encoded)
	assert.Equal(t, base, restoredBackupConfirmation(metadata, "/var/lib/fred/placements.db", encoded))

	otherProvider := metadata
	otherProvider.ProviderUUID = "00000000-0000-4000-8000-00000000abcd"
	for name, confirmation := range map[string]string{
		"path":     restoredBackupConfirmation(metadata, "/srv/fred/placements.db", encoded),
		"provider": restoredBackupConfirmation(otherProvider, "/var/lib/fred/placements.db", encoded),
		"metadata": restoredBackupConfirmation(metadata, "/var/lib/fred/placements.db", []byte(`{"schema":3}`)),
		// Length prefixes stop bytes moving between adjacent fields.
		"boundary": restoredBackupConfirmation(metadata, "/var/lib/fred/placements.db{", []byte(`"schema":2}`)),
	} {
		assert.NotEqual(t, base, confirmation, name)
	}
}
