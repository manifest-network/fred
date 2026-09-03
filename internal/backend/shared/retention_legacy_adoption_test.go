package shared

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// v013RetentionEntryWire is copied from the v0.13.0 RetentionEntry declaration.
// Do not replace it with the current type: omission of every destination-authority
// field is the compatibility boundary these tests exercise.
type v013RetentionEntryWire struct {
	OriginalLeaseUUID   string                  `json:"original_lease_uuid"`
	Tenant              string                  `json:"tenant"`
	Partition           string                  `json:"partition,omitempty"`
	ProviderUUID        string                  `json:"provider_uuid"`
	Items               []backend.LeaseItem     `json:"items"`
	StackManifest       *manifest.StackManifest `json:"stack_manifest"`
	CallbackURL         string                  `json:"callback_url"`
	RetainedVolumeNames []string                `json:"retained_volume_names"`
	Status              string                  `json:"status"`
	NewLeaseUUID        string                  `json:"new_lease_uuid,omitempty"`
	Generation          int                     `json:"generation"`
	CreatedAt           time.Time               `json:"created_at"`
	RestoringSince      time.Time               `json:"restoring_since,omitempty"`
	ReapingSince        time.Time               `json:"reaping_since,omitempty"`
}

func TestV013RestoringRetentionAdoptionFailsReadOnlyWithActionableDiagnosis(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retention.db")
	entry := v013RestoringRetentionFixture()
	wantRaw := writeV013RetentionStore(t, dbPath, entry)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	_, err = InspectRetentionStoreReadOnly(dbPath)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrLegacyRestoringRetention)
	assert.ErrorContains(t, err, "source "+entry.OriginalLeaseUUID)
	assert.ErrorContains(t, err, "destination "+entry.NewLeaseUUID)
	assert.ErrorContains(t, err, "generation 7")
	assert.ErrorContains(t, err, "restart the complete matching v0.13 lineage in isolation")
	assert.ErrorContains(t, err, "drain callbacks")
	assert.ErrorContains(t, err, "do not edit this row or synthesize destination authority")

	afterInspection, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, afterInspection, "read-only preflight must not rewrite the legacy journal")

	err = prepareExistingBoundStoreForTest(t, dbPath, PrepareBoundRetentionStoreStorage)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrLegacyRestoringRetention)

	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(storeIdentityBucketName),
			"a rejected legacy restore must not acquire current storage authority")
		got := tx.Bucket(retentionBucketName).Get([]byte(entry.OriginalLeaseUUID))
		assert.Equal(t, wantRaw, got, "binding refusal must leave the v0.13 row byte-identical")
		return nil
	}))
	require.NoError(t, db.Close())
}

func TestIdentityBoundIncompleteRestoringRetentionIsNeverClassifiedAsV013(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retention.db")
	entry := v013RestoringRetentionFixture()
	writeV013RetentionStore(t, dbPath, entry)

	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(storeIdentityBucketName)
		return err
	}))
	require.NoError(t, db.Close())

	_, err = InspectRetentionStoreReadOnly(dbPath)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrLegacyRestoringRetention)
	assert.ErrorContains(t, err, "requires exact destination items and resource profiles")
}

func TestHybridRestoringAndReapingRetentionIsNeverClassifiedAsV013(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retention.db")
	entry := v013RestoringRetentionFixture()
	entry.ReapingSince = time.Date(2026, time.January, 3, 4, 5, 6, 0, time.UTC)
	writeV013RetentionStore(t, dbPath, entry)

	_, err := InspectRetentionStoreReadOnly(dbPath)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrLegacyRestoringRetention)
	assert.ErrorContains(t, err, "requires exact destination items and resource profiles")
}

func TestV013NonRestoringRetentionsRemainInspectable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retention.db")
	active := v013RestoringRetentionFixture()
	active.Status = RetentionStatusActive
	active.NewLeaseUUID = ""
	active.Generation = 0
	active.RestoringSince = time.Time{}
	// v0.13 deliberately retained active rows with a nil manifest for manual
	// data recovery when release hydration was no longer available.
	active.StackManifest = nil
	reaping := active
	reaping.OriginalLeaseUUID = "44444444-4444-4444-8444-444444444444"
	reaping.Status = RetentionStatusReaping
	reaping.Generation = 3
	reaping.CallbackURL = ""
	reaping.ReapingSince = time.Date(2026, time.January, 3, 4, 5, 6, 0, time.UTC)
	writeV013RetentionStore(t, dbPath, active, reaping)

	inspection, err := InspectRetentionStoreReadOnly(dbPath)
	require.NoError(t, err)
	assert.True(t, inspection.Exists)
	assert.False(t, inspection.IdentityBound)
	assert.Len(t, inspection.Entries, 2)
}

func TestV013RetentionSourceAuthorityMustBeSafeToSeal(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*v013RetentionEntryWire)
		wantError string
	}{
		{
			name:      "tenant required",
			mutate:    func(entry *v013RetentionEntryWire) { entry.Tenant = " \t" },
			wantError: "retention tenant is required",
		},
		{
			name:      "creation timestamp required",
			mutate:    func(entry *v013RetentionEntryWire) { entry.CreatedAt = time.Time{} },
			wantError: "retention creation timestamp is required",
		},
		{
			name:      "negative generation rejected",
			mutate:    func(entry *v013RetentionEntryWire) { entry.Generation = -1 },
			wantError: "retention generation cannot be negative",
		},
		{
			name:      "items required",
			mutate:    func(entry *v013RetentionEntryWire) { entry.Items = nil },
			wantError: "provision request has no items",
		},
		{
			name: "positive bounded quantity required",
			mutate: func(entry *v013RetentionEntryWire) {
				entry.Items[0].Quantity = 0
			},
			wantError: "quantity 0 out of range",
		},
		{
			name: "quantity above operation maximum rejected",
			mutate: func(entry *v013RetentionEntryWire) {
				entry.Items[0].Quantity = backend.MaxOperationQuantity + 1
			},
			wantError: "out of range",
		},
		{
			name: "SKU required",
			mutate: func(entry *v013RetentionEntryWire) {
				entry.Items[0].SKU = " "
			},
			wantError: "empty SKU",
		},
		{
			name: "duplicate service rejected",
			mutate: func(entry *v013RetentionEntryWire) {
				entry.Items = append(entry.Items, entry.Items[0])
			},
			wantError: "duplicate service name",
		},
		{
			name: "active callback required",
			mutate: func(entry *v013RetentionEntryWire) {
				entry.CallbackURL = ""
			},
			wantError: "active/restoring retention source callback URL is required",
		},
		{
			name: "active callback must be a valid legacy operation route",
			mutate: func(entry *v013RetentionEntryWire) {
				entry.CallbackURL = "://not-a-callback"
			},
			wantError: "validate operation callback endpoint",
		},
		{
			name: "manifest topology required when present",
			mutate: func(entry *v013RetentionEntryWire) {
				entry.StackManifest = &manifest.StackManifest{Services: map[string]*manifest.Manifest{
					"different-service": {Image: "docker.io/library/alpine:3.22"},
				}}
			},
			wantError: "manifest service \"different-service\" has no matching lease item",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry := v013RestoringRetentionFixture()
			entry.Status = RetentionStatusActive
			entry.NewLeaseUUID = ""
			entry.Generation = 0
			entry.RestoringSince = time.Time{}
			test.mutate(&entry)
			dbPath := filepath.Join(t.TempDir(), "retention.db")
			writeV013RetentionStore(t, dbPath, entry)

			_, err := InspectRetentionStoreReadOnly(dbPath)
			require.Error(t, err)
			assert.ErrorContains(t, err, test.wantError)
		})
	}
}

func TestV013LegacyUnnamedRetentionServiceIsValidatedWithoutRewrite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retention.db")
	entry := v013RestoringRetentionFixture()
	entry.Status = RetentionStatusActive
	entry.NewLeaseUUID = ""
	entry.Generation = 0
	entry.RestoringSince = time.Time{}
	entry.Items[0].ServiceName = ""
	entry.StackManifest = &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		manifest.DefaultServiceName: {Image: "docker.io/library/alpine:3.22"},
	}}
	wantRaw := writeV013RetentionStore(t, dbPath, entry)
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	inspection, err := InspectRetentionStoreReadOnly(dbPath)
	require.NoError(t, err)
	require.Len(t, inspection.Entries, 1)
	assert.Empty(t, inspection.Entries[0].Items[0].ServiceName,
		"inspection must not rewrite legacy wire authority while validating its normalized clone")
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)

	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		assert.Equal(t, wantRaw, tx.Bucket(retentionBucketName).Get([]byte(entry.OriginalLeaseUUID)))
		return nil
	}))
	require.NoError(t, db.Close())
}

func TestV013RestoringRetentionRequiresSourceManifestBeforeLegacyDiagnosis(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retention.db")
	entry := v013RestoringRetentionFixture()
	entry.StackManifest = nil
	writeV013RetentionStore(t, dbPath, entry)

	_, err := InspectRetentionStoreReadOnly(dbPath)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrLegacyRestoringRetention)
	assert.ErrorContains(t, err, "restoring retention source manifest is required")
}

func v013RestoringRetentionFixture() v013RetentionEntryWire {
	const source = "11111111-1111-4111-8111-111111111111"
	return v013RetentionEntryWire{
		OriginalLeaseUUID: source,
		Tenant:            "tenant-a",
		Partition:         "",
		ProviderUUID:      "33333333-3333-4333-8333-333333333333",
		Items: []backend.LeaseItem{{
			SKU: "docker-small", Quantity: 1, ServiceName: "app",
		}},
		StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
			"app": {Image: "docker.io/library/alpine:3.22"},
		}},
		CallbackURL: "https://fred.example/callbacks/provision",
		RetainedVolumeNames: []string{
			"fred-retained-" + source + "-app-0",
		},
		Status:         RetentionStatusRestoring,
		NewLeaseUUID:   "22222222-2222-4222-8222-222222222222",
		Generation:     7,
		CreatedAt:      time.Date(2026, time.January, 1, 2, 3, 4, 0, time.UTC),
		RestoringSince: time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC),
		ReapingSince:   time.Time{},
	}
}

func writeV013RetentionStore(
	t *testing.T,
	dbPath string,
	entries ...v013RetentionEntryWire,
) []byte {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	encoded := make(map[string][]byte, len(entries))
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket(retentionBucketName)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			value, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			encoded[entry.OriginalLeaseUUID] = value
			if err := bucket.Put([]byte(entry.OriginalLeaseUUID), value); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
	if len(entries) == 0 {
		return nil
	}
	return encoded[entries[0].OriginalLeaseUUID]
}
