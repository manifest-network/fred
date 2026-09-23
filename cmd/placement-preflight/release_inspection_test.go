package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestInspectReleasesIsOfflineReadOnlyAndReportsHistoricalRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "releases.db")
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	rows, err := json.Marshal([]shared.Release{{
		Version: 1, Manifest: []byte(`{"image":"nginx","labels":{"com.docker.compose.project":"legacy"}}`),
		Image: "nginx", Status: "superseded", CreatedAt: time.Now().UTC(),
	}})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("releases"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte(preflightCommandProvisionLease), rows)
	}))
	require.NoError(t, db.Close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	var stdout, stderr bytes.Buffer
	// Empty dependencies make any config loading or network use fail the test.
	err = runWithDependencies(context.Background(), []string{"-inspect-releases", path}, &stdout, &stderr, commandDependencies{})
	require.NoError(t, err)
	var report shared.ReleaseAdmissionInspection
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &report))
	require.Len(t, report.Findings, 1)
	require.Equal(t, preflightCommandProvisionLease, report.Findings[0].LeaseUUID)
	require.Equal(t, 1, report.Findings[0].Version)
	require.Equal(t, "superseded", report.Findings[0].Status)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	for _, incompatible := range []string{"-prepare", "-initialize-fresh", "-config=unused", "-backup=unused"} {
		err = runWithDependencies(context.Background(), []string{"-inspect-releases", path, incompatible}, &stdout, &stderr, commandDependencies{})
		require.ErrorContains(t, err, "cannot be combined")
	}
}
