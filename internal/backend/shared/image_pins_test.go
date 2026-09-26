package shared

import (
	"encoding/json"
	"strings"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared/imagebudget"
)

func TestImagePinJournalBindsStartedManifestAndSurvivesReopen(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pins")
	origin := startedInspectionOrigin(t, stores)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	const ref = "example.invalid/app:1"
	const importBytes = int64(64 << 20)
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	require.Error(t, journal.Pin(ImageInspectionOrigin{}, ref, inspectionJournalTestImage, "", platform, legacyImagePinBudget(t, importBytes)))
	require.ErrorContains(t, journal.Pin(origin, "other:latest", inspectionJournalTestImage, "", platform, legacyImagePinBudget(t, importBytes)), "absent from Started")
	foreign := openOperationHandoffStores(t, "foreign-image-pins")
	foreignJournal, err := NewImagePinJournal(foreign.callbacks, foreign.releases, foreign.retentions)
	require.NoError(t, err)
	require.ErrorContains(t, foreignJournal.Pin(origin, ref, inspectionJournalTestImage, "", platform, legacyImagePinBudget(t, importBytes)), "another journal")
	const pullDigest = "example.invalid/app@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform, legacyImagePinBudget(t, importBytes)))
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform, legacyImagePinBudget(t, importBytes)))
	require.ErrorContains(t, journal.Pin(origin, ref,
		"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", pullDigest, platform, legacyImagePinBudget(t, importBytes)), "cannot change")
	pin, err := journal.Lookup(origin.operation.LeaseUUID(), origin.operation.Intent().Manifest(), ref)
	require.NoError(t, err)
	require.Equal(t, inspectionJournalTestImage, pin.ImageID)
	require.Equal(t, pullDigest, pin.PullDigest)
	require.Equal(t, importBytes, pin.ImportBytes)
	// A new owner over the same sealed store reads durable pins, independent
	// of the caller's original Go object and mutable registry state.
	reopened, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	pins, err := reopened.List()
	require.NoError(t, err)
	require.Equal(t, []ImagePin{*pin}, pins)
	missing, err := reopened.Lookup(origin.operation.LeaseUUID(), []byte(`{"services":{"app":{"image":"example.invalid/app:2"}}}`), ref)
	require.NoError(t, err)
	require.Nil(t, missing)
	protected, err := reopened.Collect(t.Context())
	require.NoError(t, err)
	require.False(t, protected.CanRemove(pin.ImageID), "a pending Started manifest keeps its image")
}

func TestImagePinJournalUpgradesLegacyImportAllowanceForExactIdentity(t *testing.T) {
	stores := openOperationHandoffStores(t, "legacy-image-pin-import-allowance")
	origin := startedInspectionOrigin(t, stores)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	const ref = "example.invalid/app:1"
	const pullDigest = "example.invalid/app@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform, imagebudget.Budget{}))
	legacy, err := journal.Lookup(origin.operation.LeaseUUID(), origin.operation.Intent().Manifest(), ref)
	require.NoError(t, err)
	require.Zero(t, legacy.ImportBytes)
	require.NoError(t, stores.callbacks.view(func(tx *bolt.Tx) error {
		data := tx.Bucket(imagePinsBucketName).Get(imagePinKey(legacy.LeaseUUID, legacy.ManifestHash, ref))
		require.NotContains(t, string(data), "import_bytes", "zero retains the legacy wire shape")
		return nil
	}))

	reopened, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	const verifiedBytes = int64(64 << 20)
	require.NoError(t, reopened.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform, legacyImagePinBudget(t, verifiedBytes)))
	require.NoError(t, reopened.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform, legacyImagePinBudget(t, verifiedBytes/2)))
	require.NoError(t, reopened.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform, imagebudget.Budget{}))
	pin, err := reopened.Lookup(legacy.LeaseUUID, origin.operation.Intent().Manifest(), ref)
	require.NoError(t, err)
	expected := *legacy
	expected.ImportBytes = verifiedBytes
	require.Equal(t, expected, *pin, "only verified allowance may change, and smaller observations cannot shrink it")

	for _, changedPlatform := range []ocispec.Platform{
		{OS: "linux", Architecture: "arm64"},
		{OS: "windows", Architecture: "amd64"},
		{OS: "linux", Architecture: "amd64", Variant: "v3"},
		{OS: "linux", Architecture: "amd64", OSVersion: "other"},
		{OS: "linux", Architecture: "amd64", OSFeatures: []string{"other"}},
	} {
		require.ErrorContains(t, reopened.Pin(origin, ref, inspectionJournalTestImage, pullDigest, changedPlatform, legacyImagePinBudget(t, 2*verifiedBytes)), "cannot change immutable content")
	}
	require.ErrorContains(t, reopened.Pin(origin, ref,
		"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", pullDigest, platform, legacyImagePinBudget(t, 2*verifiedBytes)), "cannot change immutable content")
	_, invalidErr := imagebudget.Decode(imagebudget.Stored{ImportBytes: -1})
	require.Error(t, invalidErr)
	pin, err = reopened.Lookup(legacy.LeaseUUID, origin.operation.Intent().Manifest(), ref)
	require.NoError(t, err)
	require.Equal(t, expected, *pin, "invalid observations must not raise or replace the stored allowance")

	require.NoError(t, reopened.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform, legacyImagePinBudget(t, 2*verifiedBytes)))
	final, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	pins, err := final.List()
	require.NoError(t, err)
	expected.ImportBytes = 2 * verifiedBytes
	require.Equal(t, []ImagePin{expected}, pins, "the larger verified allowance must survive journal reopen")
}

func TestImagePinRejectsInvalidImportAllowanceEncoding(t *testing.T) {
	base := ImagePin{
		LeaseUUID: "550e8400-e29b-41d4-a716-446655440000", ManifestHash: strings.Repeat("b", 64),
		Reference: "example.invalid/app:1", ImageID: inspectionJournalTestImage,
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
	}
	data, err := json.Marshal(base)
	require.NoError(t, err)
	legacy, err := decodeImagePin(data)
	require.NoError(t, err)
	require.Zero(t, legacy.ImportBytes)
	for _, encoded := range []string{"-1", "9223372036854775808", "18446744073709551616", "1.5", `"1024"`} {
		t.Run(encoded, func(t *testing.T) {
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &fields))
			fields["import_bytes"] = json.RawMessage(encoded)
			invalid, err := json.Marshal(fields)
			require.NoError(t, err)
			_, err = decodeImagePin(invalid)
			require.Error(t, err, "invalid stored allowance must not become deferred-unpack authority")
		})
	}
}

func TestImagePinJournalAddsMissingRecoveryDigestOnlyWithVerifiedAllowance(t *testing.T) {
	stores := openOperationHandoffStores(t, "legacy-pin-recovery-digest")
	origin := startedInspectionOrigin(t, stores)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	const ref = "example.invalid/app:1"
	const pullDigest = "example.invalid/app@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, "", platform, imagebudget.Budget{}))
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform, imagebudget.Budget{}))
	pin, err := journal.Lookup(origin.operation.LeaseUUID(), origin.operation.Intent().Manifest(), ref)
	require.NoError(t, err)
	require.Empty(t, pin.PullDigest, "an unverified observation cannot add recovery authority")

	const verifiedBytes = int64(64 << 20)
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, "", platform, legacyImagePinBudget(t, verifiedBytes)))
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform, legacyImagePinBudget(t, verifiedBytes/2)))
	reopened, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	pin, err = reopened.Lookup(origin.operation.LeaseUUID(), origin.operation.Intent().Manifest(), ref)
	require.NoError(t, err)
	require.Equal(t, pullDigest, pin.PullDigest, "verified content must fill the previously absent durable recovery digest")
	require.Equal(t, verifiedBytes, pin.ImportBytes, "adding a digest cannot shrink the allowance")

	const otherDigest = "other.invalid/app@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	require.NoError(t, reopened.Pin(origin, ref, inspectionJournalTestImage, otherDigest, platform, legacyImagePinBudget(t, 2*verifiedBytes)))
	pin, err = reopened.Lookup(origin.operation.LeaseUUID(), origin.operation.Intent().Manifest(), ref)
	require.NoError(t, err)
	require.Equal(t, pullDigest, pin.PullDigest, "a later observation cannot replace established recovery authority")
	require.Equal(t, 2*verifiedBytes, pin.ImportBytes)
}

func TestImagePinJournalCanonicalManifestHash(t *testing.T) {
	flat, err := imagePinManifestHash([]byte(`{"image":"nginx:1"}`))
	require.NoError(t, err)
	stack, err := imagePinManifestHash([]byte(`{"services":{"app":{"image":"nginx:1"}}}`))
	require.NoError(t, err)
	require.Equal(t, flat, stack)
}

func TestImagePinCollectorPrunesObsoleteManifestWhileLeaseRemainsLive(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pins-exact-manifest")
	origin := startedInspectionOrigin(t, stores)
	const ref = "example.invalid/app:1"
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	oldHash, err := imagePinManifestHash([]byte(`{"services":{"app":{"image":"example.invalid/app:old"}}}`))
	require.NoError(t, err)
	old := ImagePin{
		LeaseUUID: origin.operation.LeaseUUID(), ManifestHash: oldHash,
		Reference: "example.invalid/app:old", ImageID: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		Platform: platform,
	}
	// Model an admitted prior update whose release row has aged out. The
	// current Started manifest still owns the same paying lease UUID.
	data, err := json.Marshal(old)
	require.NoError(t, err)
	require.NoError(t, stores.callbacks.update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(imagePinsBucketName)
		if err != nil {
			return err
		}
		return bucket.Put(imagePinKey(old.LeaseUUID, old.ManifestHash, old.Reference), data)
	}))
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, "", platform, imagebudget.Budget{}))
	inventory, err := journal.Collect(t.Context())
	require.NoError(t, err)
	require.True(t, inventory.CanRemove(old.ImageID))
	require.False(t, inventory.CanRemove(inspectionJournalTestImage))
	pins, err := journal.List()
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.Equal(t, inspectionJournalTestImage, pins[0].ImageID)
}

func TestImagePinCollectorLegacyManifestCannotAuthorizeImageRemoval(t *testing.T) {
	stores := openOperationHandoffStores(t, "legacy-image-pins")
	origin := startedInspectionOrigin(t, stores)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	release, ok := origin.operation.ExpectedRelease()
	require.True(t, ok)
	release.Status = "active"
	require.NoError(t, stores.releases.appendActive(origin.operation.LeaseUUID(), release))
	inventory, err := journal.Collect(t.Context())
	require.NoError(t, err)
	require.False(t, inventory.CanRemove(inspectionJournalTestImage))
	require.False(t, (ImagePinInventory{}).CanRemove(inspectionJournalTestImage))
}

func TestImagePinJournalPersistsDistinctVerificationAndAllocationBounds(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pin-budget-dimensions")
	origin := startedInspectionOrigin(t, stores)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	const ref = "example.invalid/app:1"
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	// An old nonzero allocation supplies no decoded verification evidence.
	// Even a large legacy allowance cannot inflate new decoding authority.
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, "", platform, legacyImagePinBudget(t, 32<<20)))
	legacy, err := journal.Lookup(origin.operation.LeaseUUID(), origin.operation.Intent().Manifest(), ref)
	require.NoError(t, err)
	require.Zero(t, legacy.VerificationBytes)
	// Exact verification enriches its own dimension without changing identity.
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, "", platform, verifiedImagePinBudget(t, 4<<20, 1<<20)))
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, "", platform, verifiedImagePinBudget(t, 1<<20, 1<<20)))
	pin, err := journal.Lookup(origin.operation.LeaseUUID(), origin.operation.Intent().Manifest(), ref)
	require.NoError(t, err)
	require.Equal(t, int64(4<<20), pin.VerificationBytes)
	require.Equal(t, int64(32<<20), pin.ImportBytes)
	data, err := json.Marshal(pin)
	require.NoError(t, err)
	restored, err := decodeImagePin(data)
	require.NoError(t, err)
	require.Equal(t, *pin, restored)

}

func legacyImagePinBudget(t *testing.T, bytes int64) imagebudget.Budget {
	t.Helper()
	budget, err := imagebudget.Decode(imagebudget.Stored{ImportBytes: bytes})
	require.NoError(t, err)
	return budget
}

func verifiedImagePinBudget(t *testing.T, verificationBytes, allocationBytes int64) imagebudget.Budget {
	t.Helper()
	verification, err := imagebudget.NewVerificationBudget(verificationBytes)
	require.NoError(t, err)
	budget, err := imagebudget.Verified(verification, allocationBytes)
	require.NoError(t, err)
	return budget
}
