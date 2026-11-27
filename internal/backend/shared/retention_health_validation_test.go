package shared

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
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
				_, err := decodeRetentionEntry(corrupt)
				require.Error(t, err)
			}
		})
	}
}

// A restoring row must carry the destination it is being restored into. The
// stopped-journal inspector refuses a current row that lacks it.
func TestRetentionInspectionRejectsRestoringRowWithoutDestinationAuthority(t *testing.T) {
	const source = "11111111-1111-4111-8111-111111111111"
	dbPath := filepath.Join(t.TempDir(), "retention.db")
	raw, err := marshalRetentionEntry(RetentionEntry{
		OriginalLeaseUUID: source,
		Tenant:            "tenant-a",
		ProviderUUID:      "33333333-3333-4333-8333-333333333333",
		Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
			"app": {Image: "docker.io/library/alpine:3.22"},
		}},
		CallbackURL:         "https://fred.example/callbacks/provision",
		RetainedVolumeNames: []string{"fred-retained-" + source + "-app-0"},
		Status:              RetentionStatusRestoring,
		NewLeaseUUID:        "22222222-2222-4222-8222-222222222222",
		Generation:          7,
		CreatedAt:           time.Date(2026, time.January, 1, 2, 3, 4, 0, time.UTC),
		RestoringSince:      time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC),
	})
	require.NoError(t, err)
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		// The inspector reads the rows of an identity-bound journal only.
		if _, createErr := tx.CreateBucket(storeIdentityBucketName); createErr != nil {
			return createErr
		}
		bucket, createErr := tx.CreateBucket(retentionBucketName)
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte(source), raw)
	}))
	require.NoError(t, db.Close())

	_, err = InspectRetentionStoreReadOnly(dbPath)
	require.ErrorContains(t, err, "requires exact destination items and resource profiles")
}

// Every reader and writer of a retention row validates its source authority
// first. Each defect below is refused, by name.
func TestRetentionSourceAuthorityRejectsIncompleteRows(t *testing.T) {
	const source = "11111111-1111-4111-8111-111111111111"
	valid := func() RetentionEntry {
		return RetentionEntry{
			OriginalLeaseUUID: source,
			Tenant:            "tenant-a",
			ProviderUUID:      "33333333-3333-4333-8333-333333333333",
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"app": {Image: "docker.io/library/alpine:3.22"},
			}},
			CallbackURL:         "https://fred.example/callbacks/provision",
			RetainedVolumeNames: []string{"fred-retained-" + source + "-app-0"},
			Status:              RetentionStatusActive,
			Generation:          1,
			CreatedAt:           time.Date(2026, time.January, 1, 2, 3, 4, 0, time.UTC),
		}
	}
	entry := valid()
	require.NoError(t, validateRetentionSourceAuthorityForBinding(&entry))
	require.ErrorContains(t, validateRetentionSourceAuthorityForBinding(nil), "retention entry is required")

	for _, test := range []struct {
		name   string
		mutate func(*RetentionEntry)
		want   string
	}{
		{"no tenant", func(e *RetentionEntry) { e.Tenant = " " }, "retention tenant is required"},
		{"no creation time", func(e *RetentionEntry) { e.CreatedAt = time.Time{} }, "retention creation timestamp is required"},
		{"negative generation", func(e *RetentionEntry) { e.Generation = -1 }, "retention generation cannot be negative"},
		{"mixed service naming", func(e *RetentionEntry) {
			e.Items = []backend.LeaseItem{{SKU: "a", Quantity: 1}, {SKU: "b", Quantity: 1, ServiceName: "web"}}
		}, "retention source items"},
		{"zero quantity", func(e *RetentionEntry) { e.Items[0].Quantity = 0 }, "retention source quantities"},
		{"empty SKU", func(e *RetentionEntry) { e.Items[0].SKU = " " }, "has an empty SKU"},
		{"duplicate service", func(e *RetentionEntry) {
			e.Items = append(e.Items, backend.LeaseItem{SKU: "docker-large", Quantity: 1, ServiceName: "app"})
		}, "duplicate service name"},
		{"no callback URL", func(e *RetentionEntry) { e.CallbackURL = "" }, "callback URL is required"},
		{"invalid callback URL", func(e *RetentionEntry) { e.CallbackURL = "http://fred.example/x" }, "retention source callback URL:"},
		{"restoring without manifest", func(e *RetentionEntry) {
			e.Status = RetentionStatusRestoring
			e.StackManifest = nil
		}, "restoring retention source manifest is required"},
		{"manifest and items disagree", func(e *RetentionEntry) { e.Items[0].ServiceName = "web" }, "retention source manifest topology"},
	} {
		t.Run(test.name, func(t *testing.T) {
			entry := valid()
			test.mutate(&entry)
			require.ErrorContains(t, validateRetentionSourceAuthorityForBinding(&entry), test.want)
		})
	}

	// A reaping tombstone needs no callback route, and a row without a stored
	// manifest is valid unless it is restoring.
	reaping := valid()
	reaping.Status = RetentionStatusReaping
	reaping.CallbackURL = ""
	reaping.StackManifest = nil
	require.NoError(t, validateRetentionSourceAuthorityForBinding(&reaping))
}
