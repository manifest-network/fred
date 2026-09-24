package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

func seedHistoricalPinRows(t *testing.T, stores operationHandoffStores, lease string, count int, payload []byte, ref string) {
	t.Helper()
	hash, err := imagePinManifestHash(payload)
	require.NoError(t, err)
	type encodedPin struct{ key, data []byte }
	rows := make([]encodedPin, 0, count)
	for index := range count {
		generation := hash
		if index != 0 {
			generation = fmt.Sprintf("%064x", index)
		}
		pin := ImagePin{LeaseUUID: lease, ManifestHash: generation, Reference: ref,
			ImageID: inspectionJournalTestImage, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}}
		data, err := json.Marshal(pin)
		require.NoError(t, err)
		rows = append(rows, encodedPin{key: imagePinKey(lease, generation, ref), data: data})
	}
	// bbolt splits nodes only at commit. Ordered keys keep this large legacy
	// fixture linear instead of shifting an unsplit node for each SHA256 key.
	slices.SortFunc(rows, func(left, right encodedPin) int { return bytes.Compare(left.key, right.key) })
	require.NoError(t, stores.callbacks.update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(imagePinsBucketName)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := bucket.Put(row.key, row.data); err != nil {
				return err
			}
		}
		return nil
	}))
}

func appendPinAccountingRelease(t *testing.T, stores operationHandoffStores, spec OperationIntentSpec) Release {
	t.Helper()
	operationID, err := parseOperationCallbackID(spec.CallbackURL)
	require.NoError(t, err)
	authority, err := NewReleaseRuntimeAuthority(operationID, spec.Tenant, spec.ProviderUUID, spec.CallbackURL, spec.LifecycleCallbackURL)
	require.NoError(t, err)
	require.NoError(t, stores.releases.appendActive(spec.LeaseUUID, Release{
		Manifest: spec.Manifest, Image: "stack", OperationID: operationID,
		Items: spec.Items, ResourceProfiles: spec.ResourceProfiles, RuntimeAuthority: &authority,
	}))
	release, err := stores.releases.LatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	return *release
}

func startedPinAccountingOrigin(t *testing.T, stores operationHandoffStores, spec OperationIntentSpec) ImageInspectionOrigin {
	t.Helper()
	settlement, err := NewOperationSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)
	var origin ImageInspectionOrigin
	require.NoError(t, BindOperationSubstrateExecutor(settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) { return ctx, func() {}, nil },
		func(context.Context, string, error) error { return nil },
		func(runner substratemutation.Runner, subject OperationPhysicalSubject) func(context.Context) error {
			origin = ImageInspectionForOperation(subject)
			return func(ctx context.Context) error {
				return runner.Step(ctx, "prepare pin fixture", func(context.Context) error { return nil })
			}
		},
		func(ctx context.Context, run func(context.Context) error, _ OperationPhysicalSubject) error {
			return run(ctx)
		},
		func(context.Context, OperationPhysicalSubject) (OperationPhysicalEvidence, error) {
			return OperationPhysicalEvidence{}, errors.New("fixture preserves Started subject")
		},
	))
	claim := beginHandoffOperation(t, settlement, spec)
	candidate, err := settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	execution, err := settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	_ = settlement.ExecuteOperation(t.Context(), execution)
	require.True(t, origin.operation.Valid())
	return origin
}

func pinAccountingImage(t *testing.T, journal *ImagePinJournal, origin ImageInspectionOrigin, budget int64) error {
	t.Helper()
	return journal.Pin(origin, "example.invalid/app:1", inspectionJournalTestImage, "",
		ocispec.Platform{OS: "linux", Architecture: "amd64"}, budget)
}

func reopenPinAccountingStore(t *testing.T, stores operationHandoffStores) operationHandoffStores {
	t.Helper()
	require.NoError(t, stores.callbacks.Close())
	callbacks, err := OpenIdentityBoundCallbackStore(CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	stores.callbacks = callbacks
	return stores
}

func TestImagePinTenantSharePreservesOtherPrincipalsAndPruningCapacity(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pin-tenant-shares")
	saturated := testOperationIntentSpec(t, "saturated")
	origin := startedPinAccountingOrigin(t, stores, saturated)
	fresh := testOperationIntentSpec(t, "fresh-same-tenant")
	freshOrigin := startedPinAccountingOrigin(t, stores, fresh)
	other := testOperationIntentSpec(t, "other-tenant")
	other.Tenant = "tenant-b"
	otherOrigin := startedPinAccountingOrigin(t, stores, other)
	seedHistoricalPinRows(t, stores, saturated.LeaseUUID, maxTenantImagePins, saturated.Manifest, "example.invalid/app:1")
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	require.NoError(t, pinAccountingImage(t, journal, origin, 64<<20), "existing immutable pins may refresh verified recovery allowance at the share limit")
	require.ErrorContains(t, pinAccountingImage(t, journal, freshOrigin, 0), "tenant capacity")
	require.NoError(t, pinAccountingImage(t, journal, otherOrigin, 0), "another principal retains its share")
	require.Equal(t, maxTenantImagePins+1, journal.accounting.total)
	_, err = journal.Collect(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, journal.accounting.total, "only current Started references remain pinned")
	require.Equal(t, 1, journal.accounting.tenants[saturated.Tenant])
	require.NoError(t, pinAccountingImage(t, journal, freshOrigin, 0), "positive pruning returns the tenant's count")
	require.Equal(t, 3, journal.accounting.total)
}

func TestImagePinReopenKeepsLegacyOverShareReuseAndRecovery(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pin-legacy-share")
	spec := testOperationIntentSpec(t, "historical-active")
	appendPinAccountingRelease(t, stores, spec)
	seedHistoricalPinRows(t, stores, spec.LeaseUUID, maxTenantImagePins+1, spec.Manifest, "example.invalid/app:1")
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	require.Equal(t, maxTenantImagePins+1, journal.accounting.tenants[spec.Tenant])
	stores = reopenPinAccountingStore(t, stores)
	reopened, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	require.NotSame(t, journal, reopened)
	require.Equal(t, maxTenantImagePins+1, reopened.accounting.tenants[spec.Tenant])
	replay := testOperationIntentSpec(t, "recovery-same-manifest")
	replay.LeaseUUID = spec.LeaseUUID
	origin := startedPinAccountingOrigin(t, stores, replay)
	require.NoError(t, pinAccountingImage(t, reopened, origin, 128<<20))
	pin, err := reopened.Lookup(spec.LeaseUUID, spec.Manifest, "example.invalid/app:1")
	require.NoError(t, err)
	require.Equal(t, int64(128<<20), pin.ImportBytes)
	fresh := startedPinAccountingOrigin(t, stores, testOperationIntentSpec(t, "new-over-share"))
	require.ErrorContains(t, pinAccountingImage(t, reopened, fresh, 0), "tenant capacity")
	require.Equal(t, maxTenantImagePins+1, reopened.accounting.total)
}

func TestImagePinUnassignedDebtCannotMintAnotherShareAfterReopen(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pin-unknown-ownership")
	orphan := testOperationIntentSpec(t, "orphaned-history")
	seedHistoricalPinRows(t, stores, orphan.LeaseUUID, maxTenantImagePins, orphan.Manifest, "example.invalid/app:1")
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	require.Equal(t, maxTenantImagePins, journal.accounting.unassigned)
	origin := startedPinAccountingOrigin(t, stores, testOperationIntentSpec(t, "first-tenant"))
	require.ErrorContains(t, pinAccountingImage(t, journal, origin, 0), "ownership remains unassigned")
	stores = reopenPinAccountingStore(t, stores)
	journal, err = NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	other := testOperationIntentSpec(t, "second-tenant")
	other.Tenant = "tenant-b"
	otherOrigin := startedPinAccountingOrigin(t, stores, other)
	require.ErrorContains(t, pinAccountingImage(t, journal, otherOrigin, 0), "ownership remains unassigned")
	_, err = journal.Collect(t.Context())
	require.NoError(t, err)
	require.Zero(t, journal.accounting.unassigned)
	require.NoError(t, pinAccountingImage(t, journal, otherOrigin, 0), "only positive orphan pruning frees the unknown debt")
}

func TestImagePinCallbackAliasesShareBackfillAdmissionAndRollbackCounts(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pin-aliases")
	spec := testOperationIntentSpec(t, "near-cap")
	spec.Manifest = []byte(`{"services":{"app":{"image":"example.invalid/app:1"},"worker":{"image":"example.invalid/worker:1"}}}`)
	spec.Items = append(spec.Items, backend.LeaseItem{ServiceName: "worker", SKU: "small", Quantity: 1})
	appendPinAccountingRelease(t, stores, spec)
	seedHistoricalPinRows(t, stores, spec.LeaseUUID, maxTenantImagePins-1, []byte(`{"image":"example.invalid/obsolete:1"}`), "example.invalid/obsolete:1")
	other := testOperationIntentSpec(t, "other-principal")
	other.Tenant = "tenant-b"
	appendPinAccountingRelease(t, stores, other)
	// The value copy predates either constructor: both must still share the
	// owner allocated when the authoritative callback store opened.
	copied := *stores.callbacks //nolint:govet // Deliberately exercise an adversarial value copy before pin construction.
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	alias, err := NewImagePinJournal(&copied, stores.releases, stores.retentions)
	require.NoError(t, err)
	require.Same(t, journal, alias)
	journalCopy := *alias
	owner, err := NewImagePinBackfiller(&journalCopy, imagePinObserverFunc(func(_ context.Context, subject ImagePinBackfillSubject) ([]ImagePinBackfillObservation, error) {
		observations := []ImagePinBackfillObservation{{Reference: "example.invalid/app:1", ImageID: inspectionJournalTestImage, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}}}
		if subject.LeaseUUID() == spec.LeaseUUID {
			observations = append(observations, ImagePinBackfillObservation{Reference: "example.invalid/worker:1", ImageID: inspectionJournalTestImage, Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}})
		}
		return observations, nil
	}))
	require.NoError(t, err)
	report, err := owner.Sweep(t.Context())
	require.NoError(t, err, "one saturated principal cannot abort another tenant's backfill")
	require.Equal(t, ImagePinBackfillReport{PinsAdded: 1, UnresolvedLeases: 1}, report)
	require.Equal(t, maxTenantImagePins-1, journal.accounting.tenants[spec.Tenant], "failed multi-pin backfill must not spend a partial transaction")
	require.Equal(t, maxTenantImagePins, journal.accounting.total)
	for _, ref := range []string{"example.invalid/app:1", "example.invalid/worker:1"} {
		pin, err := alias.Lookup(spec.LeaseUUID, spec.Manifest, ref)
		require.NoError(t, err)
		require.Nil(t, pin, "no partial write may survive the refused backfill")
	}
	_, err = journal.Collect(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, alias.accounting.total)
	report, err = owner.Sweep(t.Context())
	require.NoError(t, err)
	require.Equal(t, ImagePinBackfillReport{PinsAdded: 2}, report)
	require.Equal(t, 3, journal.accounting.total)
	require.Equal(t, 2, journal.accounting.tenants[spec.Tenant])
}

func TestImagePinProviderLimitIncludesLegacyRowsWithoutBlockingExactRecovery(t *testing.T) {
	stores := openOperationHandoffStores(t, "image-pin-provider-limit")
	legacy := testOperationIntentSpec(t, "legacy-provider-history")
	legacyOrigin := startedPinAccountingOrigin(t, stores, legacy)
	last := testOperationIntentSpec(t, "last-provider-slot")
	last.Tenant = "tenant-b"
	lastOrigin := startedPinAccountingOrigin(t, stores, last)
	refused := testOperationIntentSpec(t, "provider-full")
	refused.Tenant = "tenant-c"
	refusedOrigin := startedPinAccountingOrigin(t, stores, refused)
	seedHistoricalPinRows(t, stores, legacy.LeaseUUID, maxImagePins-1, legacy.Manifest, "example.invalid/app:1")
	journal, err := NewImagePinJournal(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	require.NoError(t, pinAccountingImage(t, journal, lastOrigin, 0))
	require.Equal(t, maxImagePins, journal.accounting.total)
	require.ErrorContains(t, pinAccountingImage(t, journal, refusedOrigin, 0), "journal capacity exhausted")
	require.NoError(t, pinAccountingImage(t, journal, legacyOrigin, 256<<20), "a full provider still permits exact immutable recovery")
	require.Equal(t, maxImagePins, journal.accounting.total)
}
