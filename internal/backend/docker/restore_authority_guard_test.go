package docker

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// Each case starts with a real journal-issued claim and a decoded Restoring
// control. Only plain receiver identity / finalizer metadata is varied: no
// fabricated opaque state or an earlier decoder rejection can hide the guard.
func TestRestoreOperationAuthorityGuardsIndependentIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Backend, *shared.RetentionEntry)
		err    string
	}{
		{
			name: "source lease",
			mutate: func(_ *testing.T, _ *Backend, entry *shared.RetentionEntry) {
				entry.OriginalLeaseUUID = restoreAuthorityOtherLease
			},
			err: "source/destination generation authority",
		},
		{
			name: "source generation",
			mutate: func(_ *testing.T, _ *Backend, entry *shared.RetentionEntry) {
				entry.Generation++
			},
			err: "source/destination generation authority",
		},
		{
			name: "destination lease",
			mutate: func(_ *testing.T, _ *Backend, entry *shared.RetentionEntry) {
				entry.NewLeaseUUID = restoreAuthorityOtherLease
			},
			err: "source/destination generation authority",
		},
		{
			name: "backend name",
			mutate: func(_ *testing.T, receiver *Backend, _ *shared.RetentionEntry) {
				receiver.cfg.Name += "-other"
			},
			err: "restore intent belongs to backend",
		},
		{
			name: "backend storage",
			mutate: func(t *testing.T, receiver *Backend, _ *shared.RetentionEntry) {
				id, err := backendidentity.Parse("8b8d7135-90ac-4a38-9503-a65208d03a9b")
				require.NoError(t, err)
				require.NotEqual(t, receiver.storageIdentity, id)
				receiver.storageIdentity = id
			},
			err: "restore intent belongs to backend",
		},
		{
			name: "complete callback pair with the same operation ID",
			mutate: func(t *testing.T, _ *Backend, entry *shared.RetentionEntry) {
				entry.DestinationCallbackURL = strings.Replace(entry.DestinationCallbackURL, "fred.example", "other.example", 1)
				entry.DestinationLifecycleCallbackURL = strings.Replace(entry.DestinationLifecycleCallbackURL, "fred.example", "other.example", 1)
				resolved, err := backend.ResolveLifecycleCallbackURL(entry.DestinationCallbackURL, entry.DestinationLifecycleCallbackURL)
				require.NoError(t, err, "both rows remain internally consistent; only their callback routes differ")
				require.Equal(t, entry.DestinationLifecycleCallbackURL, resolved)
			},
			err: "callback pair differs",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, _, calls := newRestoreAuthorityRecoveryFixture(t, nil)
			claims, err := b.operationSettlement.ListOperationIntents()
			require.NoError(t, err)
			require.Len(t, claims, 1)
			claim := claims[0]
			entry, err := b.retentionStore.Get(claim.SourceLeaseUUID())
			require.NoError(t, err)
			require.NotNil(t, entry)
			require.Equal(t, shared.RetentionStatusRestoring, entry.Status)
			// The receiver contributes identity values only; do not copy Backend's
			// locks or alter its construction-bound stores to simulate another host.
			receiver := &Backend{cfg: Config{Name: b.Name()}, storageIdentity: b.storageIdentity}
			require.NoError(t, receiver.validateRestoreOperationAuthority(claim, *entry))
			before := restoreAuthorityJournalBytes(t, b)
			test.mutate(t, receiver, entry)
			require.ErrorContains(t, receiver.validateRestoreOperationAuthority(claim, *entry), test.err)
			assert.Equal(t, before, restoreAuthorityJournalBytes(t, b))
			assert.Equal(t, restoreAuthoritySubstrateCalls{}, *calls)
		})
	}
}

func TestRestoreOperationAuthorityDerivesActiveHealthServices(t *testing.T) {
	for _, test := range []struct {
		name     string
		check    *manifest.HealthCheckConfig
		services []string
		wantErr  bool
	}{
		{
			name:     "matching active check",
			check:    &manifest.HealthCheckConfig{Test: []string{"CMD", "true"}},
			services: []string{"app"},
		},
		{
			name:    "active check omitted from operation",
			check:   &manifest.HealthCheckConfig{Test: []string{"CMD", "true"}},
			wantErr: true,
		},
		{
			name:  "matching disabled check",
			check: &manifest.HealthCheckConfig{Test: []string{"NONE"}},
		},
		{
			name:     "disabled check included in operation",
			check:    &manifest.HealthCheckConfig{Test: []string{"NONE"}},
			services: []string{"app"}, wantErr: true,
		},
		{name: "no declared check"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stack := restoreStackManifest()
			stack.Services["app"].HealthCheck = test.check
			b, entry, calls := newRestoreAuthorityRecoveryFixtureWithStack(t, stack, func(spec *shared.OperationIntentSpec) {
				spec.HealthCheckServices = slices.Clone(test.services)
			})
			claims, err := b.operationSettlement.ListOperationIntents()
			require.NoError(t, err)
			require.Len(t, claims, 1)
			before := restoreAuthorityJournalBytes(t, b)
			for _, validate := range []func() error{
				func() error { return b.validateRestoreOperationAuthority(claims[0], entry) },
				func() error { _, err := b.currentRestoreOperation(entry); return err },
			} {
				err := validate()
				if test.wantErr {
					require.ErrorContains(t, err, "health-check authority differs")
				} else {
					require.NoError(t, err, "health-service derivation must accept an exact nonempty set as well as no active checks")
				}
			}
			assert.Equal(t, before, restoreAuthorityJournalBytes(t, b))
			assert.Equal(t, restoreAuthoritySubstrateCalls{}, *calls)
		})
	}
}

func TestRestoreOperationAuthorityRedundantFieldsAreRejectedByRetentionDecoder(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*shared.RetentionEntry)
		err    string
	}{
		{
			name: "missing both callback halves",
			mutate: func(entry *shared.RetentionEntry) {
				entry.DestinationCallbackURL, entry.DestinationLifecycleCallbackURL = "", ""
			},
			err: "exact operation/lifecycle callback pair",
		},
		{
			name:   "missing lifecycle callback",
			mutate: func(entry *shared.RetentionEntry) { entry.DestinationLifecycleCallbackURL = "" },
			err:    "exact operation/lifecycle callback pair",
		},
		{
			name: "lifecycle callback differs from operation route",
			mutate: func(entry *shared.RetentionEntry) {
				entry.DestinationLifecycleCallbackURL = strings.Replace(entry.DestinationLifecycleCallbackURL, "fred.example", "other.example", 1)
			},
			err: "callback pair",
		},
		{
			name:   "legacy zero operation ID is forbidden for restoring rows",
			mutate: func(entry *shared.RetentionEntry) { entry.DestinationOperationID = shared.OperationID{} },
			err:    "canonical UUIDv4",
		},
		{
			name: "operation ID differs from callback token",
			mutate: func(entry *shared.RetentionEntry) {
				entry.DestinationOperationID = mustDockerOperationID("8b8d7135-90ac-4a38-9503-a65208d03a9b")
			},
			err: "differs from callback authority",
		},
		{
			name:   "nil manifest",
			mutate: func(entry *shared.RetentionEntry) { entry.StackManifest = nil },
			err:    "restoring retention source manifest is required",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, _, calls := newRestoreAuthorityRecoveryFixture(t, nil)
			require.NoError(t, b.retentionStore.Close())
			mutateStoppedRestoreFinalizer(t, b.cfg.RetentionDBPath, test.mutate)
			before := restoreAuthorityJournalBytes(t, b)
			store, err := shared.OpenIdentityBoundRetentionStore(
				shared.RetentionStoreConfig{DBPath: b.cfg.RetentionDBPath}, b.storageAuthority, b.storeAuthorityGate,
			)
			if store != nil {
				t.Cleanup(func() { require.NoError(t, store.Close()) })
			}
			require.ErrorContains(t, err, test.err, "the bound decoder itself must reject this shape before any semantic join")
			assert.Equal(t, before, restoreAuthorityJournalBytes(t, b))
			assert.Equal(t, restoreAuthoritySubstrateCalls{}, *calls)
		})
	}
}

func TestRestoreOperationAuthorityDecodedShapePreconditions(t *testing.T) {
	b, entry, _ := newRestoreAuthorityRecoveryFixture(t, nil)
	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	claim := claims[0]
	require.NotEmpty(t, entry.OriginalLeaseUUID)
	require.Positive(t, entry.Generation)
	spec := shared.OperationIntentSpec{
		Kind: claim.Kind(), LeaseUUID: claim.LeaseUUID(),
		CallbackURL: claim.CallbackURL(), LifecycleCallbackURL: claim.LifecycleCallbackURL(),
		Tenant: claim.Tenant(), ProviderUUID: claim.ProviderUUID(),
		Items: claim.Items(), EffectiveItems: claim.EffectiveItems(), ResourceProfiles: claim.ResourceProfiles(),
		Manifest: claim.Manifest(), SourceLeaseUUID: entry.OriginalLeaseUUID, SourceGeneration: entry.Generation,
	}
	for _, test := range []struct {
		name   string
		mutate func(*shared.OperationIntentSpec)
		err    string
	}{
		{
			name:   "provision cannot carry restoring source metadata",
			mutate: func(spec *shared.OperationIntentSpec) { spec.Kind = shared.OperationIntentProvision },
			err:    "provision operation intent cannot carry restore source authority",
		},
		{
			name: "referenced nil service",
			mutate: func(spec *shared.OperationIntentSpec) {
				spec.Manifest = []byte(`{"services":{"app":null}}`)
			},
			err: `service "app" has nil manifest`,
		},
		{
			name: "unreferenced nil service",
			mutate: func(spec *shared.OperationIntentSpec) {
				spec.Manifest = []byte(`{"services":{"app":{"image":"nginx:latest"},"extra":null}}`)
			},
			err: `service "extra" has nil manifest`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := spec
			test.mutate(&changed)
			// Admission and operation-row decoding share this schema validator.
			// No opaque claim can carry a provision source or even an unreferenced
			// nil service into the later cross-journal semantic comparison.
			_, err := b.operationSettlement.NewOperationIntentCandidate(changed)
			require.ErrorContains(t, err, test.err)
		})
	}
}
