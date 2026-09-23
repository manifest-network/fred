package shared

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func legacyPolicyHistory() []Release {
	return []Release{
		{Version: 1, Manifest: []byte(`{"image":"nginx","labels":{"com.docker.compose.project":"legacy"}}`), Image: "nginx", Status: "superseded", CreatedAt: time.Now().UTC()},
		{Version: 2, Manifest: []byte(`{"image":"nginx","labels":{"traefiK.enable":"true"}}`), Image: "nginx", Status: "failed", CreatedAt: time.Now().UTC()},
		{Version: 3, Manifest: []byte(`{"services":{"web":{"image":"nginx","user":"1:2:3"}}}`), Image: "stack", Status: "active", CreatedAt: time.Now().UTC()},
	}
}

func TestReleaseAdmissionInspectionAndLegacyAdoption(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	path := filepath.Join(t.TempDir(), "releases.db")
	history := legacyPolicyHistory()
	writeRawReleaseHistory(t, path, leaseUUID, history)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	report, err := InspectReleaseAdmissionReadOnly(path)
	require.NoError(t, err)
	require.Equal(t, 1, report.Histories)
	require.Equal(t, 3, report.Releases)
	require.Len(t, report.Findings, 3)
	for i, finding := range report.Findings {
		require.Equal(t, leaseUUID, finding.LeaseUUID)
		require.Equal(t, i+1, finding.Version)
		require.Equal(t, history[i].Status, finding.Status)
		require.NotEmpty(t, finding.PolicyError)
	}
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "offline inspector must preserve exact database bytes")
	bound, err := BindAuthoritativeStorePath(path)
	require.NoError(t, err)
	inspection, err := InspectBoundLegacyReleaseStoreReadOnly(bound)
	require.NoError(t, err)
	require.Equal(t, 3, inspection.ActiveReleases[leaseUUID].Version)
	require.NoError(t, bound.Close())
	require.NoError(t, prepareExistingBoundStoreForTest(t, path, PrepareBoundReleaseStoreStorage))
	inspection, err = InspectReleaseStoreReadOnly(path)
	require.NoError(t, err)
	require.True(t, inspection.IdentityBound)
	report, err = InspectReleaseAdmissionReadOnly(path)
	require.NoError(t, err)
	require.Len(t, report.Findings, 3, "adoption must preserve historical manifest bytes")
}

func TestReleaseStoreOpensAndReadsHistoricalPolicyRows(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	path, storage := initializeBoundReleaseStore(t)
	history := legacyPolicyHistory()
	encoded, err := marshalReleaseHistory(history)
	require.NoError(t, err)
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(releasesBucketName).Put([]byte(leaseUUID), encoded)
	}))
	require.NoError(t, db.Close())
	store, err := OpenIdentityBoundReleaseStore(ReleaseStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(t))
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.Healthy())
	rows, err := store.List(leaseUUID)
	require.NoError(t, err)
	require.Equal(t, history, rows)
	active, err := store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.Equal(t, 3, active.Version)
}

func TestReleaseInspectionIdentifiesLeaseForCorruptHistory(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	path := filepath.Join(t.TempDir(), "releases.db")
	history := legacyPolicyHistory()
	history[0].Manifest = []byte(`{"services":{"web":null}}`)
	writeRawReleaseHistory(t, path, leaseUUID, history)
	_, err := InspectReleaseAdmissionReadOnly(path)
	require.ErrorContains(t, err, leaseUUID)
	require.ErrorContains(t, err, "release 1")
	_, err = InspectReleaseStoreReadOnly(path)
	require.ErrorContains(t, err, leaseUUID)
	require.ErrorContains(t, err, "release 1")
}

func TestReleaseAdmissionInspectionDoesNotCreateMissingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	_, err := InspectReleaseAdmissionReadOnly(path)
	require.Error(t, err)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}
