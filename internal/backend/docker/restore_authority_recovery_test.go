package docker

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

const (
	restoreAuthoritySource      = "0192f1a0-1111-4abc-8def-000000000201"
	restoreAuthorityDestination = "0192f1a0-2222-4abc-8def-000000000202"
	restoreAuthorityOtherLease  = "0192f1a0-3333-4abc-8def-000000000203"
)

type restoreAuthoritySubstrateCalls struct {
	composeDown, containerStop, containerRemove, volumeRename, volumeDestroy, volumeQuota int
}

// Restore authority now comes from one sealed operation claim. Divergent
// callback halves or destination fields cannot be passed to ClaimForRestore.
// Exercise the remaining trust boundary by changing the stopped journal's
// bytes, then using the real bound decoder and complete recovery workflow.
func TestRestoreRecoveryRejectsDivergentDurableAuthorityWithoutMutation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*shared.RetentionEntry)
	}{
		{"source lease", func(e *shared.RetentionEntry) { e.OriginalLeaseUUID = restoreAuthorityOtherLease }},
		{"destination lease", func(e *shared.RetentionEntry) { e.NewLeaseUUID = restoreAuthorityOtherLease }},
		{"source generation", func(e *shared.RetentionEntry) { e.Generation++ }},
		{"tenant", func(e *shared.RetentionEntry) { e.Tenant = "another-tenant" }},
		{"provider", func(e *shared.RetentionEntry) { e.ProviderUUID = restoreAuthorityOtherLease }},
		{"destination items", func(e *shared.RetentionEntry) {
			e.DestinationItems[0].CustomDomain = "unexpected.example"
		}},
		{"destination resource profiles", func(e *shared.RetentionEntry) {
			e.DestinationResourceProfiles[0].CPUCores += 0.25
		}},
		{"operation ID", func(e *shared.RetentionEntry) {
			e.DestinationOperationID = mustDockerOperationID("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
		}},
		{"operation callback half", func(e *shared.RetentionEntry) {
			e.DestinationCallbackURL = strings.Replace(e.DestinationCallbackURL, "fred.example", "other.example", 1)
		}},
		{"lifecycle callback half", func(e *shared.RetentionEntry) {
			e.DestinationLifecycleCallbackURL = strings.Replace(e.DestinationLifecycleCallbackURL, "fred.example", "other.example", 1)
		}},
		{"callback pair", func(e *shared.RetentionEntry) {
			e.DestinationCallbackURL = strings.Replace(e.DestinationCallbackURL, "fred.example", "other.example", 1)
			e.DestinationLifecycleCallbackURL = strings.Replace(e.DestinationLifecycleCallbackURL, "fred.example", "other.example", 1)
		}},
		{"manifest", func(e *shared.RetentionEntry) {
			e.StackManifest.Services["app"].Image = "redis:7"
		}},
		{"missing manifest", func(e *shared.RetentionEntry) { e.StackManifest = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b, source, calls := newRestoreAuthorityRecoveryFixture(t, nil)
			beforeAllocation := b.pool.GetAllocation(restoreAuthorityDestination + "-app-0")
			require.NotNil(t, beforeAllocation)
			beforeProjection := recoveredFromProvision(b.provisions[restoreAuthorityDestination])
			require.NoError(t, b.retentionStore.Close())
			mutateStoppedRestoreFinalizer(t, b.cfg.RetentionDBPath, test.mutate)
			before := restoreAuthorityJournalBytes(t, b)

			retentions, recoveryErr := shared.OpenIdentityBoundRetentionStore(
				shared.RetentionStoreConfig{DBPath: b.cfg.RetentionDBPath},
				b.storageAuthority, b.storeAuthorityGate,
			)
			if recoveryErr == nil {
				b.retentionStore = retentions
				t.Cleanup(func() { require.NoError(t, retentions.Close()) })
				operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
				require.True(t, ok)
				b.restoreSettlement, recoveryErr = shared.NewRestoreSettlement(operations, retentions)
				require.NoError(t, recoveryErr)
				recoveryErr = reconcileRestoreAuthorityForTest(t, b)
				require.NoError(t, retentions.Close())
			}
			require.Error(t, recoveryErr,
				"a mismatched finalizer must fail at decoding or before the recovery plan executes")
			assert.Equal(t, before, restoreAuthorityJournalBytes(t, b),
				"recovery must preserve every byte of all three authoritative journals")
			assert.Equal(t, *beforeAllocation, *b.pool.GetAllocation(restoreAuthorityDestination + "-app-0"))
			assert.True(t, provisionMatchesRecovered(b.provisions[restoreAuthorityDestination], beforeProjection))
			assert.Equal(t, restoreAuthoritySubstrateCalls{}, *calls)
			intents, err := b.operationSettlement.ListOperationIntents()
			require.NoError(t, err)
			require.Len(t, intents, 1)
			assert.Equal(t, source.DestinationOperationID, intents[0].OperationID())
			pending, err := b.callbackStore.ListPending()
			require.NoError(t, err)
			assert.Empty(t, pending)
		})
	}
}

func TestRestoreRecoveryValidatesOperationSemanticAuthorityBeforePlanning(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*shared.OperationIntentSpec)
		wantErr string
	}{
		{name: "matching pending authority"},
		{
			name:    "health-check services differ from source manifest",
			mutate:  func(spec *shared.OperationIntentSpec) { spec.HealthCheckServices = []string{"app"} },
			wantErr: "health-check authority differs",
		},
		{
			name:    "desired items differ from finalizer effective items",
			mutate:  func(spec *shared.OperationIntentSpec) { spec.Items[0].CustomDomain = "desired.example" },
			wantErr: "topology or resource profiles differ",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, _, calls := newRestoreAuthorityRecoveryFixture(t, test.mutate)
			before := restoreAuthorityJournalBytes(t, b)
			err := reconcileRestoreAuthorityForTest(t, b)
			if test.wantErr == "" {
				require.NoError(t, err, "a consistent pending restore must await operation recovery")
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
			assert.Equal(t, before, restoreAuthorityJournalBytes(t, b))
			assert.NotNil(t, b.pool.GetAllocation(restoreAuthorityDestination+"-app-0"))
			assert.Equal(t, restoreAuthoritySubstrateCalls{}, *calls)
		})
	}
}

// A busy recovery fence is a successful deferral, not evidence that validation
// ran. Observe the first journal read inside the fence and retry unentered passes so
// the rejection assertions cannot depend on winning a scheduler race.
func reconcileRestoreAuthorityForTest(t *testing.T, b *Backend) error {
	t.Helper()
	entered := observeRestoreRecoveryEntryForTest(t, b)
	var recoveryErr error
	require.Eventually(t, func() bool {
		recoveryErr = b.reconcileRetentions(t.Context())
		return entered() || recoveryErr != nil
	}, 5*time.Second, time.Millisecond, "restore authority recovery must enter its fenced validation")
	return recoveryErr
}

func observeRestoreRecoveryEntryForTest(t *testing.T, b *Backend) func() bool {
	t.Helper()
	observer := &restoreAuthorityReadObserver{operationSettlementService: b.operationSettlement}
	b.operationSettlement = observer
	return func() bool { return observer.entered }
}

type restoreAuthorityReadObserver struct {
	operationSettlementService
	entered bool
}

func (o *restoreAuthorityReadObserver) ListOperationIntents() ([]shared.OperationIntentClaim, error) {
	o.entered = true
	return o.operationSettlementService.ListOperationIntents()
}

func (o *restoreAuthorityReadObserver) wrappedOperationSettlementForTest() operationSettlementService {
	return o.operationSettlementService
}

func TestRestoreAuthorityValidationDistinguishesBusyDeferralFromExecutedValidation(t *testing.T) {
	b, _, calls := newRestoreAuthorityRecoveryFixture(t, func(spec *shared.OperationIntentSpec) {
		spec.HealthCheckServices = []string{"app"}
	})
	before := restoreAuthorityJournalBytes(t, b)
	entered := observeRestoreRecoveryEntryForTest(t, b)
	unlock := b.commandFence.Lock(restoreAuthorityDestination)
	t.Cleanup(unlock)
	require.NoError(t, b.reconcileRetentions(t.Context()), "busy live work is a normal deferral")
	assert.False(t, entered(), "a deferred sweep has not validated the mismatched authority")
	unlock()
	require.ErrorContains(t, reconcileRestoreAuthorityForTest(t, b), "health-check authority differs")
	assert.Equal(t, before, restoreAuthorityJournalBytes(t, b))
	assert.Equal(t, restoreAuthoritySubstrateCalls{}, *calls)
}

func newRestoreAuthorityRecoveryFixture(
	t *testing.T,
	mutateSpec func(*shared.OperationIntentSpec),
) (*Backend, shared.RetentionEntry, *restoreAuthoritySubstrateCalls) {
	t.Helper()
	calls := &restoreAuthoritySubstrateCalls{}
	mock := &mockDockerClient{
		StopContainerFn:   func(context.Context, string, time.Duration) error { calls.containerStop++; return nil },
		RemoveContainerFn: func(context.Context, string) error { calls.containerRemove++; return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	t.Cleanup(func() { b.stopCancel(); b.wg.Wait() })
	b.compose = &mockComposeExecutor{
		DownFn: func(context.Context, string, time.Duration) error { calls.composeDown++; return nil },
	}
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(string, string) error { calls.volumeRename++; return nil },
		DestroyFn:      func(context.Context, string) error { calls.volumeDestroy++; return nil },
		EnsureQuotaFn:  func(context.Context, string, int64) error { calls.volumeQuota++; return nil },
	}
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	profiles := testResourceProfiles(t, items)
	stack := restoreStackManifest()
	require.NoError(t, putRetentionForTest(t, b.retentionStore, shared.RetentionEntry{
		OriginalLeaseUUID: restoreAuthoritySource, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		Items: items, ResourceProfiles: profiles, StackManifest: stack,
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(restoreAuthoritySource, "app", 0))},
		Status:              shared.RetentionStatusActive, CreatedAt: time.Now(),
	}))
	active, err := b.retentionStore.Get(restoreAuthoritySource)
	require.NoError(t, err)
	require.NotNil(t, active)
	operationID, callbackURL, lifecycleURL := newTestRestoreCallbackAuthority(t)
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	spec := shared.OperationIntentSpec{
		Kind: shared.OperationIntentRestore, LeaseUUID: restoreAuthorityDestination,
		CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleURL,
		Tenant: active.Tenant, ProviderUUID: active.ProviderUUID,
		Items: slices.Clone(items), EffectiveItems: slices.Clone(items), ResourceProfiles: profiles,
		Manifest: manifestBytes, SourceLeaseUUID: restoreAuthoritySource, SourceGeneration: active.Generation + 1,
	}
	if mutateSpec != nil {
		mutateSpec(&spec)
	}
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	admission, err := b.operationSettlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, created := admission.CreatedClaim()
	require.True(t, created)
	restoreCandidate, err := b.restoreSettlement.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	proof, err := b.restoreSettlement.ClaimForRestore(restoreCandidate, 0)
	require.NoError(t, err)
	source := proof.Entry()
	require.Equal(t, operationID, source.DestinationOperationID)
	b.provisions[restoreAuthorityDestination] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: restoreAuthorityDestination, Tenant: active.Tenant, ProviderUUID: active.ProviderUUID,
		SKU: "docker-small", Status: backend.ProvisionStatusFailed, Quantity: 1, FailCount: 1,
		LastError: "interrupted restore", Reason: backend.ReasonRestoreFailed, Message: "interrupted restore",
		CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleURL,
		Items: items, ResourceProfiles: profiles, ContainerIDs: []string{"interrupted-container"},
		StackManifest: stack, ServiceContainers: map[string][]string{"app": {"interrupted-container"}},
	}}
	require.NoError(t, b.pool.TryAllocateResolved(restoreAuthorityDestination+"-app-0", active.Tenant, profiles[0]))
	return b, source, calls
}

func mutateStoppedRestoreFinalizer(t *testing.T, path string, mutate func(*shared.RetentionEntry)) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("retention"))
		var envelope struct {
			SchemaVersion uint8                 `json:"schema_version"`
			Entry         shared.RetentionEntry `json:"entry"`
		}
		if err := json.Unmarshal(bucket.Get([]byte(restoreAuthoritySource)), &envelope); err != nil {
			return err
		}
		mutate(&envelope.Entry)
		encoded, err := json.Marshal(envelope)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(restoreAuthoritySource), encoded)
	}))
	require.NoError(t, db.Close())
}

func restoreAuthorityJournalBytes(t *testing.T, b *Backend) map[string][]byte {
	t.Helper()
	result := make(map[string][]byte, 3)
	for _, path := range []string{b.cfg.CallbackDBPath, b.cfg.ReleasesDBPath, b.cfg.RetentionDBPath} {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		result[path] = data
	}
	return result
}
