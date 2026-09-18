package placement

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestPlacementAuthorityDecodersRejectCaseAliases(t *testing.T) {
	for _, test := range []struct {
		name   string
		decode func([]byte) error
		alias  string
		both   string
	}{
		{
			name: "placement record",
			decode: func(value []byte) error {
				_, _, err := decodeCurrentPlacementRecord(value)
				return err
			},
			alias: `{"Schema":1}`,
			both:  `{"schema":99,"Schema":1}`,
		},
		{
			name: "lifecycle capability",
			decode: func(value []byte) error {
				_, err := decodeLifecycleCapability(value)
				return err
			},
			alias: `{"Schema":1}`,
			both:  `{"schema":99,"Schema":1}`,
		},
		{
			name: "maintenance command",
			decode: func(value []byte) error {
				_, _, _, _, _, err := decodeMaintenanceCommand(value)
				return err
			},
			alias: `{"Schema":1}`,
			both:  `{"schema":99,"Schema":1}`,
		},
	} {
		t.Run(test.name+" alias only", func(t *testing.T) {
			require.ErrorContains(t, test.decode([]byte(test.alias)), `unknown field "Schema"`)
		})
		t.Run(test.name+" canonical and alias", func(t *testing.T) {
			require.ErrorContains(t, test.decode([]byte(test.both)), `unknown field "Schema"`)
		})
	}
}

func TestPlacementRuntimeRejectsUnknownRootsAndLegacyRows(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*bolt.Tx) error
		want   string
	}{
		{
			name: "unknown root",
			mutate: func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("future_authority"))
				return err
			},
			want: "unexpected top-level placement authority bucket",
		},
		{
			name: "raw pre ENG-335 row",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put(
					[]byte("11111111-1111-4111-8111-111111111111"),
					[]byte("backend-a"),
				)
			},
			want: "is not current schema",
		},
		{
			name: "versionless ENG-335 row",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put(
					[]byte("11111111-1111-4111-8111-111111111111"),
					[]byte(`{"backend":"backend-a","set_at":"2026-01-02T03:04:05Z"}`),
				)
			},
			want: "unsupported placement record schema 0",
		},
		{
			name: "case-aliased current row",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put(
					[]byte("11111111-1111-4111-8111-111111111111"),
					[]byte(`{"schema":1,"Backend":"backend-a"}`),
				)
			},
			want: `unknown field "Backend"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "placements.db")
			store, err := newStoreForTest(path)
			require.NoError(t, err)
			require.NoError(t, store.db.Update(test.mutate))
			require.ErrorContains(t, store.Healthy(), test.want)
			require.NoError(t, store.Close())

			reopened, err := OpenStore(path, freshTestProviderUUID)
			assert.Nil(t, reopened)
			require.ErrorContains(t, err, test.want)
		})
	}
}
