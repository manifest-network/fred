package shared

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// Exercise the runtime entry point after a successful probe: neither a prior
// decode nor the identity binding may hide a malformed current row. The same
// corpus also reaches the stopped-journal discriminator, whose legacy support
// must not change the meaning of a versioned row.
func TestRetentionHealthRevalidatesCurrentRows(t *testing.T) {
	entry := versionlessRetentionSchemaEntry(schemaTestLeaseUUID)
	valid, err := marshalRetentionEntry(entry)
	require.NoError(t, err)
	versionless, err := json.Marshal(entry)
	require.NoError(t, err)
	replace := func(old, next string) []byte {
		t.Helper()
		require.Contains(t, string(valid), old)
		return bytes.Replace(valid, []byte(old), []byte(next), 1)
	}
	cases := map[string][]byte{
		"duplicate envelope":         replace(`"schema_version":1`, `"schema_version":1,"schema_version":1`),
		"escaped duplicate":          replace(`"tenant":"tenant-a"`, `"tenant":"tenant-a","\u0074enant":"tenant-b"`),
		"nested duplicate":           replace(`"image":`, `"image":"different","image":`),
		"unknown envelope":           replace(`"schema_version":1`, `"future":true,"schema_version":1`),
		"unknown manifest":           replace(`"image":`, `"future":true,"image":`),
		"case alias":                 replace(`"tenant":`, `"Tenant":`),
		"nested case alias":          replace(`"image":`, `"Image":`),
		"future schema":              replace(`"schema_version":1`, `"schema_version":2`),
		"missing schema":             replace(`"schema_version":1,`, ``),
		"versionless runtime":        versionless,
		"trailing value":             append(bytes.Clone(valid), []byte(` true`)...),
		"invalid UTF8":               replace(`tenant-a`, "tenant-\xff"),
		"oversized":                  []byte(strings.Repeat(" ", maxAuthoritativeRecordBytes) + string(valid)),
		"wrong row identity":         replace(schemaTestLeaseUUID, "22222222-2222-4222-8222-222222222222"),
		"invalid principal":          replace(`"tenant":"tenant-a"`, `"tenant":""`),
		"invalid source manifest":    replace(`"image":"docker.io/library/alpine:3.22"`, `"image":""`),
		"invalid resource authority": replace(`"quantity":1`, `"quantity":0`),
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			path, storage := initializeBoundRetentionStore(t)
			store, err := OpenIdentityBoundRetentionStore(
				RetentionStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(t))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			write := func(value []byte) {
				t.Helper()
				require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket(retentionBucketName).Put([]byte(schemaTestLeaseUUID), value)
				}))
			}
			write(valid)
			require.NoError(t, store.Healthy())
			write(corrupt)
			require.Error(t, store.Healthy())
			write(valid)
			require.NoError(t, store.Healthy())
			// Identity/source validation follows decoding. These cases need not
			// fail the wire decoder; current-schema framing cases must agree.
			switch name {
			case "versionless runtime", "wrong row identity", "invalid principal", "invalid source manifest", "invalid resource authority":
			default:
				_, err := decodeUnboundRetentionEntry(corrupt)
				require.Error(t, err)
			}
		})
	}
}
