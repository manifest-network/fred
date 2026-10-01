package shared

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

const historicalProvisionPayload = `{
  "labels": {"com.docker.compose.project": "old-build"},
  "image": "example.invalid/app:1"
}`

func historicalProvisionFixture(t *testing.T) (operationHandoffStores, OperationIntentSpec, Release) {
	t.Helper()
	stores := openOperationHandoffStores(t, "historical-provision")
	original := testOperationIntentSpec(t, "historical-provision")
	stack, err := manifest.ParseStoredPayload([]byte(historicalProvisionPayload))
	require.NoError(t, err)
	payload, err := json.Marshal(stack)
	require.NoError(t, err)
	operationID, err := parseOperationCallbackID(original.CallbackURL)
	require.NoError(t, err)
	authority, err := NewReleaseRuntimeAuthority(operationID, original.Tenant, original.ProviderUUID, original.CallbackURL, original.LifecycleCallbackURL)
	require.NoError(t, err)
	require.NoError(t, stores.releases.appendActive(original.LeaseUUID, Release{Manifest: payload, Image: "stack", OperationID: operationID, Items: original.Items, ResourceProfiles: original.ResourceProfiles, RuntimeAuthority: &authority}))
	release, err := stores.releases.LatestActive(original.LeaseUUID)
	require.NoError(t, err)
	replay := testOperationIntentSpec(t, "historical-provision")
	replay.Manifest = []byte(historicalProvisionPayload)
	return stores, replay, *release
}

func TestProvisionManifestAdmissionReplaysCanonicalHistoricalContent(t *testing.T) {
	stores, spec, release := historicalProvisionFixture(t)
	_, err := manifest.ParsePayload(spec.Manifest)
	require.Error(t, err, "new tenant policy must continue rejecting the historical label")
	admitted, err := stores.settlement.AdmitProvisionManifest(t.Context(), spec.LeaseUUID, spec.Tenant, spec.ProviderUUID, spec.Items, spec.Manifest)
	require.NoError(t, err, "flat original JSON must match its canonical durable stack")
	require.Equal(t, release.Manifest, admitted.Payload(), "the durable release owns replay bytes")
	detached, err := admitted.Stack()
	require.NoError(t, err)
	detached.Services["app"].Image = "foreign:latest"
	copy := admitted.Payload()
	copy[0] = '!'
	require.Equal(t, release.Manifest, admitted.Payload())
	spec.Manifest = admitted.Payload()
	candidate, err := stores.settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	bound, err := admitted.Bind(candidate)
	require.NoError(t, err)
	accepted, err := stores.settlement.BeginOperationIntent(bound)
	require.NoError(t, err)
	claim, created := accepted.CreatedClaim()
	require.True(t, created)
	require.Equal(t, release.Manifest, claim.Manifest())
	require.Equal(t, release.Items, claim.Items())
}

func TestProvisionManifestAdmissionReplaysReorderedHistoricalTopology(t *testing.T) {
	stores, spec, release := historicalProvisionFixture(t)
	release.Manifest = []byte(`{"services":{"app":{"image":"example.invalid/app:1","labels":{"com.docker.compose.project":"old-build"}},"worker":{"image":"example.invalid/worker:1"}}}`)
	release.Items = []backend.LeaseItem{
		{SKU: "small", ServiceName: "app", Quantity: 1},
		{SKU: "large", ServiceName: "worker", Quantity: 2, CustomDomain: "worker.example"},
	}
	release.ResourceProfiles = testResourceProfilesForItems(release.Items)
	require.NoError(t, stores.releases.appendActive(spec.LeaseUUID, release))
	spec.Items = []backend.LeaseItem{release.Items[1], release.Items[0]}
	spec.ResourceProfiles = release.ResourceProfiles
	spec.Manifest = release.Manifest
	admission, err := stores.settlement.AdmitProvisionManifest(t.Context(), spec.LeaseUUID, spec.Tenant, spec.ProviderUUID, spec.Items, spec.Manifest)
	require.NoError(t, err, "service-sorted historical authority must admit the chain's original order")
	candidate, err := stores.settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	candidate, err = admission.Bind(candidate)
	require.NoError(t, err, "candidate binding must use the same multiset as manifest admission")
	accepted, err := stores.settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, created := accepted.CreatedClaim()
	require.True(t, created)
	require.Equal(t, spec.Items, claim.Items(), "admission preserves the request order")

	for _, change := range []string{"service", "sku", "quantity", "duplicate", "remove"} {
		t.Run(change, func(t *testing.T) {
			items := slices.Clone(spec.Items)
			switch change {
			case "service":
				items[0].ServiceName, items[1].ServiceName = items[1].ServiceName, items[0].ServiceName
			case "sku":
				items[0].SKU, items[1].SKU = items[1].SKU, items[0].SKU
			case "quantity":
				items[0].Quantity, items[1].Quantity = items[1].Quantity, items[0].Quantity
			case "duplicate":
				items[1] = items[0]
			case "remove":
				items = items[:1]
			}
			_, err := stores.settlement.AdmitProvisionManifest(t.Context(), spec.LeaseUUID, spec.Tenant, spec.ProviderUUID, items, spec.Manifest)
			require.ErrorIs(t, err, backend.ErrInvalidManifest)
			changed := spec
			changed.Items = items
			candidate, err := stores.settlement.NewOperationIntentCandidate(changed)
			if err == nil {
				_, err = admission.Bind(candidate)
			}
			require.Error(t, err, "a replay admission cannot splice a changed topology into acceptance")
		})
	}
}

func TestProvisionManifestReplaySeparatesHistoricalTopologyFromDesiredRouting(t *testing.T) {
	for _, domain := range []string{"", "desired.example"} {
		t.Run("desired="+domain, func(t *testing.T) {
			stores, spec, release := historicalProvisionFixture(t)
			release.Items = slices.Clone(release.Items)
			release.Items[0].CustomDomain = "old-effective.example"
			require.NoError(t, stores.releases.appendActive(spec.LeaseUUID, release))
			spec.Items[0].CustomDomain = domain
			admission, err := stores.settlement.AdmitProvisionManifest(t.Context(), spec.LeaseUUID, spec.Tenant, spec.ProviderUUID, spec.Items, spec.Manifest)
			require.NoError(t, err, "historical effective routing is not the chain's desired routing")
			spec.Manifest = admission.Payload()
			candidate, err := stores.settlement.NewOperationIntentCandidate(spec)
			require.NoError(t, err)
			bound, err := admission.Bind(candidate)
			require.NoError(t, err)
			changed := spec
			changed.Items = slices.Clone(spec.Items)
			changed.Items[0].CustomDomain = "spliced.example"
			foreign, err := stores.settlement.NewOperationIntentCandidate(changed)
			require.NoError(t, err)
			_, err = admission.Bind(foreign)
			require.Error(t, err, "admission must still bind the exact desired routing it accepted")
			accepted, err := stores.settlement.BeginOperationIntent(bound)
			require.NoError(t, err)
			claim, created := accepted.CreatedClaim()
			require.True(t, created)
			require.Equal(t, domain, claim.Items()[0].CustomDomain)
		})
	}
}

func TestProvisionManifestAdmissionKeepsChangedAndNewSubmissionsStrict(t *testing.T) {
	for _, change := range []string{"new-lease", "tenant", "provider", "quantity", "sku", "image", "env", "labels", "port", "malformed-port"} {
		t.Run(change, func(t *testing.T) {
			stores, spec, _ := historicalProvisionFixture(t)
			stack, err := manifest.ParseStoredPayload(spec.Manifest)
			require.NoError(t, err)
			switch change {
			case "new-lease":
				spec.LeaseUUID = "550e8400-e29b-41d4-a716-446655440000"
			case "tenant":
				spec.Tenant = "tenant-b"
			case "provider":
				spec.ProviderUUID = "33333333-3333-4333-8333-333333333333"
			case "quantity":
				spec.Items[0].Quantity++
			case "sku":
				spec.Items[0].SKU = "other"
			case "image":
				stack.Services["app"].Image = "other:latest"
			case "env":
				stack.Services["app"].Env = map[string]string{"CHANGED": "true"}
			case "labels":
				stack.Services["app"].Labels["com.docker.compose.project"] = "new-build"
			case "port":
				stack.Services["app"].Ports = map[string]manifest.PortConfig{"80/tcp": {}}
			case "malformed-port":
				stack.Services["app"].Ports = map[string]manifest.PortConfig{"80": {}}
			}
			spec.Manifest, err = json.Marshal(stack)
			require.NoError(t, err)
			_, err = stores.settlement.AdmitProvisionManifest(t.Context(), spec.LeaseUUID, spec.Tenant, spec.ProviderUUID, spec.Items, spec.Manifest)
			require.Error(t, err)
			pending, err := stores.settlement.ListOperationIntents()
			require.NoError(t, err)
			require.Empty(t, pending)
		})
	}
	stores := openOperationHandoffStores(t, "strict-new-provision")
	spec := testOperationIntentSpec(t, "strict-new-provision")
	admitted, err := stores.settlement.AdmitProvisionManifest(t.Context(), spec.LeaseUUID, spec.Tenant, spec.ProviderUUID, spec.Items, spec.Manifest)
	require.NoError(t, err)
	candidate, err := stores.settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	_, err = admitted.Bind(candidate)
	require.NoError(t, err, "ordinary new submissions retain strict admission without an active predecessor")
}

func TestProvisionManifestAdmissionReattestsReleaseAtAcceptance(t *testing.T) {
	stores, spec, release := historicalProvisionFixture(t)
	admitted, err := stores.settlement.AdmitProvisionManifest(t.Context(), spec.LeaseUUID, spec.Tenant, spec.ProviderUUID, spec.Items, spec.Manifest)
	require.NoError(t, err)
	spec.Manifest = admitted.Payload()
	candidate, err := stores.settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	candidate, err = admitted.Bind(candidate)
	require.NoError(t, err)
	// A replacement generation can commit after validation but before Begin.
	// Identical manifest bytes do not let a stale source fence survive it.
	newID, err := parseOperationCallbackID(spec.CallbackURL)
	require.NoError(t, err)
	authority, err := NewReleaseRuntimeAuthority(newID, spec.Tenant, spec.ProviderUUID, spec.CallbackURL, spec.LifecycleCallbackURL)
	require.NoError(t, err)
	release.OperationID, release.RuntimeAuthority = newID, &authority
	require.NoError(t, stores.releases.appendActive(spec.LeaseUUID, release))
	_, err = stores.settlement.BeginOperationIntent(candidate)
	require.ErrorContains(t, err, "release changed before admission")
	pending, err := stores.settlement.ListOperationIntents()
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestProvisionManifestAdmissionCannotSpliceHistoricalAuthority(t *testing.T) {
	stores, spec, _ := historicalProvisionFixture(t)
	admitted, err := stores.settlement.AdmitProvisionManifest(t.Context(), spec.LeaseUUID, spec.Tenant, spec.ProviderUUID, spec.Items, spec.Manifest)
	require.NoError(t, err)
	spec.Manifest = admitted.Payload()
	foreign := openOperationHandoffStores(t, "foreign-provision-replay")
	candidate, err := foreign.settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	_, err = admitted.Bind(candidate)
	require.Error(t, err)
	spec.Items = slices.Clone(spec.Items)
	spec.Items[0].Quantity++
	candidate, err = stores.settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	_, err = admitted.Bind(candidate)
	require.Error(t, err)
	_, err = (ProvisionManifestAdmission{}).Bind(candidate)
	require.Error(t, err)
}

func TestProvisionManifestAdmissionDoesNotHideCorruptStoredPorts(t *testing.T) {
	stores, spec, release := historicalProvisionFixture(t)
	release.Manifest = []byte(`{"services":{"app":{"image":"example.invalid/app:1","ports":{"80":{}},"labels":{"com.docker.compose.project":"old-build"}}}}`)
	encoded, err := marshalReleaseHistory([]Release{release})
	require.NoError(t, err)
	require.NoError(t, stores.releases.update(func(tx *bolt.Tx) error { return tx.Bucket(releasesBucketName).Put([]byte(spec.LeaseUUID), encoded) }))
	_, err = stores.settlement.AdmitProvisionManifest(t.Context(), spec.LeaseUUID, spec.Tenant, spec.ProviderUUID, spec.Items, release.Manifest)
	require.ErrorContains(t, err, "invalid port")
	require.False(t, errors.Is(err, backend.ErrInvalidManifest), "stored corruption is not a new tenant policy refusal")
	require.NotContains(t, err.Error(), "manifest requires current admission", "journal corruption must retain its original cause")
}
