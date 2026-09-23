package shared

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// This represents a previously admitted generation whose labels and user
// syntax no longer pass tenant admission. Durable cleanup must still finish.
const storedPolicyDriftManifest = `{"services":{"app":{"image":"example.invalid/app:1","labels":{"com.docker.compose.project":"legacy"},"user":"1:2:3"}}}`

func storedPolicyDriftStack(t *testing.T) *manifest.StackManifest {
	t.Helper()
	_, err := manifest.ParsePayload([]byte(storedPolicyDriftManifest))
	require.Error(t, err, "the historical fixture must remain forbidden to new tenant admission")
	stack, err := manifest.ParseStoredPayload([]byte(storedPolicyDriftManifest))
	require.NoError(t, err)
	return stack
}

func TestStoredPolicyDriftRetentionSurvivesInspectionAdoptionAndRestart(t *testing.T) {
	entry := v013RestoringRetentionFixture()
	entry.Status = RetentionStatusActive
	entry.NewLeaseUUID = ""
	entry.Generation = 0
	entry.RestoringSince = time.Time{}
	entry.StackManifest = storedPolicyDriftStack(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "retention.db")
	writeV013RetentionStore(t, dbPath, entry)
	inspection, err := InspectRetentionStoreReadOnly(dbPath)
	require.NoError(t, err, "read-only upgrade inspection must accept structurally valid historical policy")
	require.Len(t, inspection.Entries, 1)
	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bound.Close()) })
	pair, err := backendidentity.BindMarkerPair(filepath.Join(dir, "primary.json"), filepath.Join(dir, "anchor.json"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pair.Close()) })
	storage, err := pair.InitializeWithStores("docker-a", "daemon-a", retentionSchemaHooks(bound))
	require.NoError(t, err, "historical policy must not prevent journal adoption")
	for range 2 {
		store, err := OpenIdentityBoundRetentionStore(RetentionStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t))
		require.NoError(t, err, "startup must not reapply current tenant policy to retained rows")
		require.NoError(t, store.Healthy())
		stored, err := store.Get(entry.OriginalLeaseUUID)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, entry.StackManifest, stored.StackManifest)
		entries, err := store.List()
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.NoError(t, store.Close())
	}
}

func TestStoredPolicyDriftCloseIntentRemainsRecoverable(t *testing.T) {
	storedPolicyDriftStack(t)
	store, storage := openBoundCloseIntentTestStore(t)
	spec := testCloseIntentSpec(t, "stored-policy-drift")
	spec.Manifest = []byte(storedPolicyDriftManifest)
	candidate, err := store.NewCloseIntentCandidate(spec)
	require.NoError(t, err, "close authority is derived from an already admitted generation")
	admission, err := store.BeginCloseIntent(candidate)
	require.NoError(t, err)
	assert.Equal(t, spec.Manifest, admission.Claim().Manifest())
	path := store.db.Path()
	require.NoError(t, store.Close())
	reopened, err := OpenIdentityBoundCallbackStore(CallbackStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	claims, err := reopened.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	assert.Equal(t, spec.Manifest, claims[0].Manifest())
}

func TestStoredPolicyDriftCloseSettlementRecordsRetention(t *testing.T) {
	stack := storedPolicyDriftStack(t)
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "stored-policy-retention")
	spec.Manifest = []byte(storedPolicyDriftManifest)
	operationID, err := parseOperationCallbackID(spec.CallbackURL)
	require.NoError(t, err)
	authority, err := NewReleaseRuntimeAuthority(operationID, spec.Tenant, spec.ProviderUUID, spec.CallbackURL, spec.LifecycleCallbackURL)
	require.NoError(t, err)
	require.NoError(t, stores.releases.appendActive(spec.LeaseUUID, Release{
		Manifest: spec.Manifest, Image: "stack", OperationID: operationID,
		Items: spec.Items, ResourceProfiles: spec.ResourceProfiles, RuntimeAuthority: &authority,
	}))
	settlement := newCloseSettlementForTest(t, stores)
	claim := admitSettlementClose(t, settlement, spec.LeaseUUID, true)
	volumes := []string{"fred-retained-" + spec.LeaseUUID + "-app-0"}
	created, err := settlement.RecordRetention(claim, "", volumes)
	require.NoError(t, err, "retention must preserve stored policy instead of parsing it as new tenant input")
	require.True(t, created)
	entry, err := stores.retentions.Get(spec.LeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, stack, entry.StackManifest)
	assert.Equal(t, volumes, entry.RetainedVolumeNames)
	proof, err := settlement.ProveRetention(claim)
	require.NoError(t, err)
	require.True(t, proof.Valid())
}
