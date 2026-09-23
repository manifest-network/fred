package shared

import (
	"encoding/json"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

const (
	imagePinOld     = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	imagePinCurrent = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	imagePinTarget  = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
)

// Model an earlier successful image admission. Only the production collector
// decides whether these durable pins still have a relaunchable journal owner.
func seedLivenessPin(t *testing.T, stores operationHandoffStores, lease string, payload []byte, ref, id string) {
	t.Helper()
	hash, err := imagePinManifestHash(payload)
	require.NoError(t, err)
	pin := ImagePin{LeaseUUID: lease, ManifestHash: hash, Reference: ref, ImageID: id, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}}
	data, err := json.Marshal(pin)
	require.NoError(t, err)
	_, err = decodeImagePin(data)
	require.NoError(t, err)
	require.NoError(t, stores.callbacks.update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(imagePinsBucketName)
		if err != nil {
			return err
		}
		return bucket.Put(imagePinKey(lease, hash, ref), data)
	}))
}

func callbackTransactionID(t *testing.T, stores operationHandoffStores) int {
	t.Helper()
	var id int
	require.NoError(t, stores.callbacks.view(func(tx *bolt.Tx) error { id = tx.ID(); return nil }))
	return id
}

func TestImagePinConstructorAndReadsNeverUpgradeOptionalJournalBuckets(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pin-lazy-constructor")
	before := callbackTransactionID(t, stores)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	pins, err := journal.List()
	require.NoError(t, err)
	require.Empty(t, pins)
	inventory, err := journal.Collect(t.Context())
	require.NoError(t, err)
	require.True(t, inventory.Complete())
	require.Equal(t, before, callbackTransactionID(t, stores), "constructing or reading image pins must not open a write transaction")
	require.NoError(t, stores.callbacks.view(func(tx *bolt.Tx) error {
		require.Nil(t, tx.Bucket(imagePinsBucketName))
		require.Nil(t, tx.Bucket(imageInspectionsBucketName))
		return nil
	}))
	origin := startedInspectionOrigin(t, stores)
	require.NoError(t, journal.Pin(origin, "example.invalid/app:1", imagePinCurrent, "", ocispec.Platform{OS: "linux", Architecture: "amd64"}, 0))
	require.NoError(t, stores.callbacks.view(func(tx *bolt.Tx) error { require.NotNil(t, tx.Bucket(imagePinsBucketName)); return nil }))
}

func TestImagePinCollectorReleasesTerminalFailedSuccessorButProtectsActivePredecessor(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pin-failed-operation")
	spec, _, failed := settleActivePredecessorThenFailedSuccessorForClose(t, stores, "image-pin-failed-operation")
	seedLivenessPin(t, stores, spec.LeaseUUID, spec.Manifest, "example.invalid/app:1", imagePinCurrent)
	seedLivenessPin(t, stores, spec.LeaseUUID, failed.Manifest(), "example.invalid/app:2", imagePinTarget)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	inventory, err := journal.Collect(t.Context())
	require.NoError(t, err)
	require.True(t, inventory.Complete())
	require.False(t, inventory.CanRemove(imagePinCurrent))
	require.True(t, inventory.CanRemove(imagePinTarget), "terminal failure history is not future image execution authority")
	pins, err := journal.List()
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.Equal(t, imagePinCurrent, pins[0].ImageID)
}

func TestImagePinCollectorRetainsOnlyCurrentAndExactCompensationSource(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pin-compensation")
	lease := testLeaseUUID("image-pin-compensation")
	old := validRuntimeAuthorityRelease()
	old.Manifest = []byte(`{"services":{"app":{"image":"example.invalid/app:oldest"}}}`)
	require.NoError(t, stores.releases.appendActive(lease, old))
	source := validRuntimeAuthorityRelease()
	source.Manifest = []byte(`{"services":{"app":{"image":"example.invalid/app:source"}}}`)
	require.NoError(t, stores.releases.appendActive(lease, source))
	settlement, err := NewMaintenanceSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)
	current, claim, err := settlement.ClaimLatestActive(lease)
	require.NoError(t, err)
	target := cloneRelease(current)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	target.Manifest = []byte(`{"services":{"app":{"image":"example.invalid/app:target"}}}`)
	identity, ok := target.RuntimeIdentity()
	require.True(t, ok)
	request, err := settlement.NewMaintenanceRequestAuthority(newTestMaintenanceID(t), MaintenanceIntentUpdate, lease, identity.LifecycleCallbackURL(), target.Manifest)
	require.NoError(t, err)
	candidate, err := settlement.NewMaintenanceIntentCandidate(request, claim, target)
	require.NoError(t, err)
	accepted, err := settlement.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	appendClaim, err := settlement.StartMaintenanceAppend(createdMaintenanceDispatch(t, accepted))
	require.NoError(t, err)
	appended, err := settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	bound, err := settlement.BindMaintenanceIntentTarget(appended)
	require.NoError(t, err)
	seedLivenessPin(t, stores, lease, old.Manifest, "example.invalid/app:oldest", imagePinOld)
	seedLivenessPin(t, stores, lease, source.Manifest, "example.invalid/app:source", imagePinCurrent)
	seedLivenessPin(t, stores, lease, target.Manifest, "example.invalid/app:target", imagePinTarget)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	active := activateMaintenanceOutcomeForTest(t, settlement, bound)
	inventory, err := journal.Collect(t.Context())
	require.NoError(t, err)
	require.True(t, inventory.Complete())
	require.True(t, inventory.CanRemove(imagePinOld), "unrelated superseded history must not pin images")
	require.False(t, inventory.CanRemove(imagePinCurrent), "the pending exact compensation source must survive target activation")
	require.False(t, inventory.CanRemove(imagePinTarget))
	_, err = resolveMaintenanceSuccessForTest(settlement, active)
	require.NoError(t, err)
	inventory, err = journal.Collect(t.Context())
	require.NoError(t, err)
	require.True(t, inventory.CanRemove(imagePinCurrent), "settled maintenance no longer grants compensation authority")
	require.False(t, inventory.CanRemove(imagePinTarget))
	pins, err := journal.List()
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.Equal(t, imagePinTarget, pins[0].ImageID)
}

func TestImagePinCollectorLegacyNilRetentionKeepsAllPinsAndInhibitsRemoval(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pin-legacy-retention")
	lease := testLeaseUUID("image-pin-legacy-retention")
	retained := sampleEntry(lease)
	retained.StackManifest = nil
	require.NoError(t, stores.retentions.putForTest(retained))
	payload := []byte(`{"services":{"app":{"image":"example.invalid/app:retained"}}}`)
	seedLivenessPin(t, stores, lease, payload, "example.invalid/app:retained", imagePinOld)
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	inventory, err := journal.Collect(t.Context())
	require.NoError(t, err)
	require.False(t, inventory.Complete())
	require.False(t, inventory.CanRemove(imagePinOld))
	require.False(t, inventory.CanRemove(imagePinTarget), "unknown retention ancestry inhibits the complete destructive inventory")
	pins, err := journal.List()
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.Equal(t, imagePinOld, pins[0].ImageID)
}
