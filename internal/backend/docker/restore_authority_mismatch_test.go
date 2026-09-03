package docker

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

const (
	restoreAuthoritySourceLease       = "0192f1a0-1111-7abc-8def-000000000201"
	restoreAuthorityDestinationLease  = "0192f1a0-2222-7abc-8def-000000000202"
	restoreAuthorityProviderUUID      = nominalDockerProviderUUID
	restoreAuthorityOtherProviderUUID = "33333333-3333-4333-8333-333333333333"
	restoreAuthorityOperationID       = shared.OperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	restoreAuthorityOtherOperationID  = shared.OperationID("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
)

type restoreSubstrateMutationCalls struct {
	composeDown     int
	containerList   int
	containerStop   int
	containerRemove int
	volumeRename    int
	volumeUsage     int
	volumeQuota     int
}

func TestReconcileRestoring_ExactAuthorityDivergenceFailsClosed(t *testing.T) {
	otherStorageID, err := backendidentity.Parse("742b99d1-860f-4783-9f24-ef9e526bf08d")
	require.NoError(t, err)

	tests := []struct {
		name   string
		mutate func(*testing.T, *shared.OperationIntentSpec)
	}{
		{
			name: "backend name",
			mutate: func(_ *testing.T, spec *shared.OperationIntentSpec) {
				spec.Backend = "another-docker-backend"
			},
		},
		{
			name: "storage identity",
			mutate: func(_ *testing.T, spec *shared.OperationIntentSpec) {
				spec.BackendStorageID = otherStorageID
			},
		},
		{
			name: "tenant",
			mutate: func(_ *testing.T, spec *shared.OperationIntentSpec) {
				spec.Tenant = "tenant-b"
			},
		},
		{
			name: "provider",
			mutate: func(_ *testing.T, spec *shared.OperationIntentSpec) {
				spec.ProviderUUID = restoreAuthorityOtherProviderUUID
			},
		},
		{
			name: "destination items",
			mutate: func(_ *testing.T, spec *shared.OperationIntentSpec) {
				spec.Items[0].CustomDomain = "other.example"
				spec.EffectiveItems[0].CustomDomain = "other.example"
			},
		},
		{
			name: "resource profiles",
			mutate: func(_ *testing.T, spec *shared.OperationIntentSpec) {
				spec.ResourceProfiles[0].CPUCores += 0.25
			},
		},
		{
			name: "manifest",
			mutate: func(_ *testing.T, spec *shared.OperationIntentSpec) {
				spec.Manifest = []byte(`{"services":{"app":{"image":"redis:7"}}}`)
			},
		},
		{
			name: "operation ID",
			mutate: func(t *testing.T, spec *shared.OperationIntentSpec) {
				spec.CallbackURL, spec.LifecycleCallbackURL = restoreAuthorityCallbackPair(
					t, "https://fred.example/callbacks/provision", restoreAuthorityOtherOperationID,
				)
			},
		},
		{
			name: "operation callback URL",
			mutate: func(t *testing.T, spec *shared.OperationIntentSpec) {
				// The lifecycle half necessarily moves with the operation half: the
				// pair is a bijective, validated derivation of one typed token.
				spec.CallbackURL, spec.LifecycleCallbackURL = restoreAuthorityCallbackPair(
					t, "https://other.example/v2/callbacks/provision?trace=other", restoreAuthorityOperationID,
				)
			},
		},
		{
			name: "lifecycle callback URL",
			mutate: func(t *testing.T, spec *shared.OperationIntentSpec) {
				// A lifecycle-only mismatch is unconstructable. Reverse the owner
				// of the alternate valid pair here; the test below separately pins
				// rejection of changing only this half.
				spec.CallbackURL, spec.LifecycleCallbackURL = restoreAuthorityCallbackPair(
					t, "https://lifecycle-route.example/callbacks/provision?trace=other", restoreAuthorityOperationID,
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, retentions, releases, callbacks, claimed, spec, allocationID, calls :=
				newRestoreAuthorityMismatchFixture(t)
			test.mutate(t, &spec)

			admission, err := callbacks.BeginOperationIntent(spec)
			require.NoError(t, err)
			require.Equal(t, shared.OperationIntentAdmissionCreated, admission.Disposition)

			beforeSource, err := retentions.Get(restoreAuthoritySourceLease)
			require.NoError(t, err)
			require.NotNil(t, beforeSource)
			beforeReleases, err := releases.List(restoreAuthorityDestinationLease)
			require.NoError(t, err)
			beforeAllocation := b.pool.GetAllocation(allocationID)
			require.NotNil(t, beforeAllocation)

			err = b.reconcileRestoringWithAuthority(context.Background(), *claimed)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "restore intent")

			afterSource, getErr := retentions.Get(restoreAuthoritySourceLease)
			require.NoError(t, getErr)
			require.NotNil(t, afterSource)
			assert.Equal(t, *beforeSource, *afterSource,
				"authority failure must neither delete nor hand back the source finalizer")
			assert.Equal(t, shared.RetentionStatusRestoring, afterSource.Status)

			afterReleases, listErr := releases.List(restoreAuthorityDestinationLease)
			require.NoError(t, listErr)
			assert.Equal(t, beforeReleases, afterReleases,
				"authority failure must not append, activate, or supersede a Release")
			afterAllocation := b.pool.GetAllocation(allocationID)
			require.NotNil(t, afterAllocation)
			assert.Equal(t, *beforeAllocation, *afterAllocation,
				"authority failure must keep the live reservation")

			intents, listErr := callbacks.ListOperationIntents()
			require.NoError(t, listErr)
			require.Len(t, intents, 1, "the exact pending intent is the retry vehicle")
			assert.Equal(t, admission.Claim.OperationID(), intents[0].OperationID())
			assert.Equal(t, admission.Claim.CallbackURL(), intents[0].CallbackURL())
			pending, listErr := callbacks.ListPending()
			require.NoError(t, listErr)
			assert.Empty(t, pending, "authority failure must not emit a success or failure callback")

			b.provisionsMu.RLock()
			projected, exists := b.provisions[restoreAuthorityDestinationLease]
			b.provisionsMu.RUnlock()
			require.True(t, exists)
			assert.Equal(t, backend.ProvisionStatusFailed, projected.Status)
			assert.Equal(t, []string{"container-1"}, projected.ContainerIDs)

			assert.Equal(t, restoreSubstrateMutationCalls{}, *calls,
				"authority validation must precede every substrate mutation")
		})
	}
}

func TestRestoreAuthority_UnpairedOperationOrLifecycleMutationIsRejectedByConstruction(t *testing.T) {
	tests := []struct {
		name      string
		operation shared.OperationID
		callback  func(*testing.T) (string, string)
	}{
		{
			name:      "operation ID cannot differ from operation callback",
			operation: restoreAuthorityOtherOperationID,
			callback: func(t *testing.T) (string, string) {
				return restoreAuthorityCallbackPair(
					t, "https://fred.example/callbacks/provision", restoreAuthorityOperationID,
				)
			},
		},
		{
			name:      "lifecycle callback cannot differ from operation callback",
			operation: restoreAuthorityOperationID,
			callback: func(t *testing.T) (string, string) {
				callbackURL, _ := restoreAuthorityCallbackPair(
					t, "https://fred.example/callbacks/provision", restoreAuthorityOperationID,
				)
				_, unrelatedLifecycle := restoreAuthorityCallbackPair(
					t, "https://other.example/callbacks/provision", restoreAuthorityOperationID,
				)
				return callbackURL, unrelatedLifecycle
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := &mockDockerClient{}
			b := newBackendForProvisionTest(t, mock, nil)
			t.Cleanup(b.stopCancel)
			bindTestStorageIdentity(t, b, mock)
			retentions := attachRetentionStore(t, b)
			callbacks := attachRestoreAuthorityCallbackStore(t, b)

			items := []backend.LeaseItem{{
				SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
			}}
			profiles := testResourceProfiles(t, items)
			stack := restoreStackManifest()
			require.NoError(t, retentions.Put(shared.RetentionEntry{
				OriginalLeaseUUID: restoreAuthoritySourceLease,
				Tenant:            "tenant-a",
				ProviderUUID:      restoreAuthorityProviderUUID,
				Items:             items,
				ResourceProfiles:  profiles,
				StackManifest:     stack,
				Status:            shared.RetentionStatusActive,
				CreatedAt:         time.Now(),
			}))

			manifestBytes, err := json.Marshal(stack)
			require.NoError(t, err)
			validCallback, validLifecycle := restoreAuthorityCallbackPair(
				t, "https://fred.example/callbacks/provision", restoreAuthorityOperationID,
			)
			admission, err := callbacks.BeginOperationIntent(shared.OperationIntentSpec{
				Kind:                 shared.OperationIntentRestore,
				LeaseUUID:            restoreAuthorityDestinationLease,
				CallbackURL:          validCallback,
				LifecycleCallbackURL: validLifecycle,
				Backend:              b.Name(),
				BackendStorageID:     b.storageIdentity,
				Tenant:               "tenant-a",
				ProviderUUID:         restoreAuthorityProviderUUID,
				Items:                items,
				ResourceProfiles:     profiles,
				EffectiveItems:       items,
				Manifest:             manifestBytes,
				SourceLeaseUUID:      restoreAuthoritySourceLease,
				SourceGeneration:     1,
			})
			require.NoError(t, err)
			require.Equal(t, shared.OperationIntentAdmissionCreated, admission.Disposition)

			callbackURL, lifecycleURL := test.callback(t)
			claimed, err := retentions.ClaimForRestoreWithAuthority(
				restoreAuthoritySourceLease,
				restoreAuthorityDestinationLease,
				0,
				items,
				profiles,
				test.operation,
				callbackURL,
				lifecycleURL,
			)
			require.Error(t, err)
			assert.Nil(t, claimed)

			source, getErr := retentions.Get(restoreAuthoritySourceLease)
			require.NoError(t, getErr)
			require.NotNil(t, source)
			assert.Equal(t, shared.RetentionStatusActive, source.Status)
			assert.Empty(t, source.NewLeaseUUID)
			assert.Zero(t, source.Generation)

			intents, listErr := callbacks.ListOperationIntents()
			require.NoError(t, listErr)
			require.Len(t, intents, 1)
			assert.Equal(t, admission.Claim.OperationID(), intents[0].OperationID())
			pending, listErr := callbacks.ListPending()
			require.NoError(t, listErr)
			assert.Empty(t, pending)
		})
	}
}

func newRestoreAuthorityMismatchFixture(
	t *testing.T,
) (
	*Backend,
	*shared.RetentionStore,
	*shared.ReleaseStore,
	*shared.CallbackStore,
	*shared.RetentionEntry,
	shared.OperationIntentSpec,
	string,
	*restoreSubstrateMutationCalls,
) {
	t.Helper()
	calls := &restoreSubstrateMutationCalls{}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			calls.containerList++
			return nil, nil
		},
		StopContainerFn: func(context.Context, string, time.Duration) error {
			calls.containerStop++
			return nil
		},
		RemoveContainerFn: func(context.Context, string) error {
			calls.containerRemove++
			return nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	t.Cleanup(b.stopCancel)
	bindTestStorageIdentity(t, b, mock)
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		calls.composeDown++
		return nil
	}}
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(string, string) error {
			calls.volumeRename++
			return nil
		},
		UsageFn: func(context.Context, string) (int64, error) {
			calls.volumeUsage++
			return 0, nil
		},
		EnsureQuotaFn: func(context.Context, string, int64) error {
			calls.volumeQuota++
			return nil
		},
	}

	retentions := attachRetentionStore(t, b)
	releases := attachReleaseStore(t, b)
	callbacks := attachRestoreAuthorityCallbackStore(t, b)

	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	profiles := testResourceProfiles(t, items)
	stack := restoreStackManifest()
	require.NoError(t, retentions.Put(shared.RetentionEntry{
		OriginalLeaseUUID: restoreAuthoritySourceLease,
		Tenant:            "tenant-a",
		ProviderUUID:      restoreAuthorityProviderUUID,
		Items:             items,
		ResourceProfiles:  profiles,
		StackManifest:     stack,
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(
			restoreAuthoritySourceLease, manifest.DefaultServiceName, 0,
		))},
		Status:    shared.RetentionStatusActive,
		CreatedAt: time.Now(),
	}))
	callbackURL, lifecycleURL := restoreAuthorityCallbackPair(
		t, "https://fred.example/callbacks/provision", restoreAuthorityOperationID,
	)
	claimed, err := retentions.ClaimForRestoreWithAuthority(
		restoreAuthoritySourceLease,
		restoreAuthorityDestinationLease,
		0,
		items,
		profiles,
		restoreAuthorityOperationID,
		callbackURL,
		lifecycleURL,
	)
	require.NoError(t, err)

	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	spec := shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentRestore,
		LeaseUUID:            restoreAuthorityDestinationLease,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleURL,
		Backend:              b.Name(),
		BackendStorageID:     b.storageIdentity,
		Tenant:               "tenant-a",
		ProviderUUID:         restoreAuthorityProviderUUID,
		Items:                append([]backend.LeaseItem(nil), items...),
		ResourceProfiles:     shared.CloneSKUResourceSnapshot(profiles),
		EffectiveItems:       append([]backend.LeaseItem(nil), items...),
		Manifest:             manifestBytes,
		SourceLeaseUUID:      restoreAuthoritySourceLease,
		SourceGeneration:     claimed.Generation,
	}

	b.provisionsMu.Lock()
	b.provisions[restoreAuthorityDestinationLease] = &provision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID:            restoreAuthorityDestinationLease,
			Tenant:               "tenant-a",
			ProviderUUID:         restoreAuthorityProviderUUID,
			SKU:                  items[0].SKU,
			Status:               backend.ProvisionStatusFailed,
			Quantity:             1,
			CreatedAt:            time.Now(),
			FailCount:            1,
			LastError:            "restore interrupted",
			Reason:               backend.ReasonRestoreFailed,
			Message:              "restore interrupted",
			CallbackURL:          callbackURL,
			LifecycleCallbackURL: lifecycleURL,
			Items:                items,
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(profiles),
			ContainerIDs:         []string{"container-1"},
			StackManifest:        stack,
			ServiceContainers:    map[string][]string{manifest.DefaultServiceName: {"container-1"}},
		},
		ResourceProfiles:      profiles,
		VolumeCleanupAttempts: 0,
	}
	b.provisionsMu.Unlock()

	allocationID := restoreAuthorityDestinationLease + "-" + manifest.DefaultServiceName + "-0"
	require.NoError(t, b.pool.TryAllocateResolved(allocationID, "tenant-a", profiles[0]))
	return b, retentions, releases, callbacks, claimed, spec, allocationID, calls
}

func attachRestoreAuthorityCallbackStore(t *testing.T, b *Backend) *shared.CallbackStore {
	t.Helper()
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	b.callbackStore = callbacks
	b.operationIntents = callbacks
	return callbacks
}

func restoreAuthorityCallbackPair(
	t *testing.T,
	base string,
	operationID shared.OperationID,
) (string, string) {
	t.Helper()
	separator := "?"
	if len(base) > 0 && base[len(base)-1] != '?' && strings.Contains(base, "?") {
		separator = "&"
	}
	callbackURL := base + separator + backend.CallbackOperationIDQueryParameter + "=" + operationID.String()
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	return callbackURL, lifecycleURL
}
