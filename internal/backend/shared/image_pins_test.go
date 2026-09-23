package shared

import (
	"encoding/json"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestImagePinJournalBindsStartedManifestAndSurvivesReopen(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pins")
	origin := startedInspectionOrigin(t, stores)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	const ref = "example.invalid/app:1"
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	require.Error(t, journal.Pin(ImageInspectionOrigin{}, ref, inspectionJournalTestImage, "", platform))
	require.ErrorContains(t, journal.Pin(origin, "other:latest", inspectionJournalTestImage, "", platform), "absent from Started")
	foreign := openOperationHandoffStores(t, "foreign-image-pins")
	foreignJournal, err := NewImagePinJournal(foreign.callbacks, foreign.releases, foreign.retentions)
	require.NoError(t, err)
	require.ErrorContains(t, foreignJournal.Pin(origin, ref, inspectionJournalTestImage, "", platform), "another journal")
	const pullDigest = "example.invalid/app@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform))
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, pullDigest, platform))
	require.ErrorContains(t, journal.Pin(origin, ref,
		"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", pullDigest, platform), "cannot change")
	pin, err := journal.Lookup(origin.operation.LeaseUUID(), origin.operation.Intent().Manifest(), ref)
	require.NoError(t, err)
	require.Equal(t, inspectionJournalTestImage, pin.ImageID)
	require.Equal(t, pullDigest, pin.PullDigest)
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
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	const ref = "example.invalid/app:1"
	platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	require.NoError(t, journal.Pin(origin, ref, inspectionJournalTestImage, "", platform))
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
		return tx.Bucket(imagePinsBucketName).Put(imagePinKey(old.LeaseUUID, old.ManifestHash, old.Reference), data)
	}))
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
