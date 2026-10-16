package shared

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceIntentKindValid(t *testing.T) {
	for _, kind := range []MaintenanceIntentKind{
		MaintenanceIntentRestart, MaintenanceIntentUpdate,
		MaintenanceIntentCustomDomain, MaintenanceIntentRuntimeRefresh,
	} {
		assert.True(t, kind.Valid(), string(kind))
	}
	for _, kind := range []MaintenanceIntentKind{"", "bogus", "Restart"} {
		assert.False(t, kind.Valid(), string(kind))
	}
}

// A runtime refresh is backend-minted: it never carries a provider stamp, and
// its receipts count against their own window, never a tenant's or the
// custom-domain reconciler's.
func TestRuntimeRefreshReceiptWindow(t *testing.T) {
	assert.Equal(t, maintenanceWindowRuntimeRefresh, maintenanceWindowFor(MaintenanceIntentRuntimeRefresh))
	assert.Equal(t, maxRuntimeRefreshMaintenanceWindow, maintenanceWindowRuntimeRefresh.limit())
	require.Error(t, validateMaintenanceStampForKind(MaintenanceIntentRuntimeRefresh, windowStamp(0)),
		"only provider kinds carry an admission stamp")

	full := make([]maintenanceCompletionRecord, maxRuntimeRefreshMaintenanceWindow)
	for index := range full {
		full[index] = maintenanceCompletionRecord{Kind: MaintenanceIntentRuntimeRefresh}
	}
	require.NoError(t, validateMaintenanceWindowCounts("550e8400-e29b-41d4-a716-446655440001", full, nil))
	head := MaintenanceIntentRuntimeRefresh
	require.Error(t, validateMaintenanceWindowCounts("550e8400-e29b-41d4-a716-446655440001", full, &head),
		"a live refresh head needs a free slot in its own window")
	overfull := append(full, maintenanceCompletionRecord{Kind: MaintenanceIntentRuntimeRefresh})
	require.Error(t, validateMaintenanceWindowCounts("550e8400-e29b-41d4-a716-446655440001", overfull, nil))
	restart := MaintenanceIntentRestart
	require.NoError(t, validateMaintenanceWindowCounts("550e8400-e29b-41d4-a716-446655440001", full, &restart),
		"a full refresh window does not consume provider capacity")
}

func TestRuntimeRefreshChurnNeverEvictsOtherReceipts(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "window-runtime-refresh")
	receipts := make([]windowReceipt, 0, maxRuntimeRefreshMaintenanceWindow+20)
	for range 10 {
		receipts = append(receipts, windowReceipt{kind: MaintenanceIntentRestart})
	}
	for range 10 {
		receipts = append(receipts, windowReceipt{kind: MaintenanceIntentCustomDomain})
	}
	for range maxRuntimeRefreshMaintenanceWindow {
		receipts = append(receipts, windowReceipt{kind: MaintenanceIntentRuntimeRefresh})
	}
	seeded := seedMaintenanceWindow(t, callbacks, source, target, receipts)

	admission, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRuntimeRefresh, source, target,
		"docker-a", callbackStorageID(t, windowTestStorageID),
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.CancelMaintenanceIntent(createdMaintenanceDispatch(t, admission)))
	})
	assert.False(t, receiptPresent(t, callbacks, seeded[20]), "the oldest refresh receipt left its window")
	for _, record := range seeded[:20] {
		assert.True(t, receiptPresent(t, callbacks, record), "refresh churn never evicts another window's receipt")
	}
	lineage := lineageFor(t, callbacks, source.LeaseUUID())
	assert.False(t, lineage.ProviderEvicted)
	assert.True(t, lineage.HighWaterAdmittedAt.IsZero(), "a backend-minted command never moves the mark")
	require.NoError(t, callbacks.Healthy())
}

// A runtime refresh can only be constructed from its source: the general
// constructor refuses any caller-supplied target for it, and the dedicated one
// derives the unchanged workload from the sealed source release.
func TestRuntimeRefreshCandidateIsDerivedFromItsSource(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement, err := NewMaintenanceSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)
	leaseUUID := testLeaseUUID("runtime-refresh-candidate")
	require.NoError(t, stores.releases.appendActive(leaseUUID, validRuntimeAuthorityRelease()))
	active, source, err := settlement.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	authority, ok := active.RuntimeIdentity()
	require.True(t, ok)
	request, err := settlement.NewMaintenanceRequestAuthority(
		newTestMaintenanceID(t), MaintenanceIntentRuntimeRefresh, leaseUUID, authority.LifecycleCallbackURL(), nil,
	)
	require.NoError(t, err)

	foreign := cloneRelease(active)
	foreign.Version, foreign.Status, foreign.CreatedAt = 0, "deploying", time.Now()
	foreign.Manifest = []byte(`{"services":{"app":{"image":"redis:7"}}}`)
	_, err = settlement.NewMaintenanceIntentCandidate(request, source, foreign)
	require.Error(t, err, "no caller-supplied target can become a runtime refresh")

	candidate, err := settlement.NewRuntimeRefreshIntentCandidate(request, source)
	require.NoError(t, err)
	target := candidate.targetRelease
	assert.Equal(t, active.Manifest, target.Manifest)
	assert.Equal(t, active.Items, target.Items)
	assert.Equal(t, active.ResourceProfiles, target.ResourceProfiles)
	assert.Equal(t, active.OperationID, target.OperationID)
	assert.Equal(t, active.RuntimeAuthority, target.RuntimeAuthority)
	assert.Equal(t, active.LegacyRuntimeAuthority, target.LegacyRuntimeAuthority)
	assert.Zero(t, target.Version)
	assert.Equal(t, "deploying", target.Status)
	assert.True(t, target.MaintenanceID.IsZero())

	restart, err := settlement.NewMaintenanceRequestAuthority(
		newTestMaintenanceID(t), MaintenanceIntentRestart, leaseUUID, authority.LifecycleCallbackURL(), nil,
	)
	require.NoError(t, err)
	_, err = settlement.NewRuntimeRefreshIntentCandidate(restart, source)
	require.Error(t, err, "only a runtime refresh request derives its target")

	unsealed := source
	unsealed.release.Manifest = foreign.Manifest
	_, err = settlement.NewRuntimeRefreshIntentCandidate(request, unsealed)
	require.Error(t, err, "a claim that does not seal its release is refused")
}
