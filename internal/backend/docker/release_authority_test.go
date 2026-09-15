package docker

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

// seedProvisionReleaseForLeaseTest builds the same state as a completed
// provision: exact operation admission, active Release commit, semantic
// Success publication, and successful outbox acknowledgement. It deliberately
// accepts the complete paired services rather than writing ReleaseStore rows
// directly, so Docker tests cannot manufacture a runtime generation that
// production itself cannot reach.
func seedProvisionReleaseForLeaseTest(
	t *testing.T,
	callbacks *shared.CallbackStore,
	releases *shared.ReleaseStore,
	operations *shared.OperationSettlement,
	leaseUUID string,
	release shared.Release,
) shared.Release {
	t.Helper()
	identity, ok := release.RuntimeIdentity()
	require.True(t, ok, "typed release fixture requires complete runtime authority")
	require.Equal(t, shared.ReleaseAuthorityTyped, identity.Class(),
		"legacy release fixtures must use the explicit migration API")
	require.Equal(t, identity.OperationID(), release.OperationID)

	spec := shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            leaseUUID,
		CallbackURL:          identity.CallbackURL(),
		LifecycleCallbackURL: identity.LifecycleCallbackURL(),
		Tenant:               identity.Tenant(),
		ProviderUUID:         identity.ProviderUUID(),
		Items:                append([]backend.LeaseItem(nil), release.Items...),
		ResourceProfiles:     shared.CloneSKUResourceSnapshot(release.ResourceProfiles),
		EffectiveItems:       append([]backend.LeaseItem(nil), release.Items...),
		Manifest:             append([]byte(nil), release.Manifest...),
	}
	candidate, err := operations.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	admission, err := operations.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, created := admission.CreatedClaim()
	require.True(t, created)
	committed := commitOperationSuccessForTest(t, operations, claim)

	maintenance, err := shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	stopCtx := context.Background()
	attestor := callbackStorageAttestorForTest(
		t, callbacks, stopCtx, allowTestCallbackDelivery,
	)
	publisher, err := shared.NewCallbackPublisher(shared.CallbackPublisherConfig{
		OperationSettlement: operations, MaintenanceSettlement: maintenance,
		StorageAttestor: attestor, Logger: slog.Default(),
	})
	require.NoError(t, err)
	require.NoError(t, publisher.PublishOperationSuccessContext(context.Background(), committed))
	acknowledgePendingCallbacksForTest(t, callbacks)

	active, err := releases.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	return *active
}

func seedProvisionReleaseForBackendTest(
	t *testing.T,
	b *Backend,
	leaseUUID string,
	release shared.Release,
) shared.Release {
	t.Helper()
	attachBoundOperationHandoffStores(t, b)
	if b.callbackPublisher == nil {
		rebuildCallbackSender(b, testCallbackClient)
	}
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok, "test backend must use the concrete operation settlement")
	return seedProvisionReleaseForLeaseTest(
		t, b.callbackStore, b.releaseStore, operations, leaseUUID, release,
	)
}

// seedProvisionReleaseFromProjectionForBackendTest upgrades a compact
// in-memory fixture into the exact durable provision state that production
// requires before close. It derives one internally consistent manifest,
// operation identity, principal, and resource snapshot, then commits them only
// through OperationSettlement.
func seedProvisionReleaseFromProjectionForBackendTest(
	t *testing.T,
	b *Backend,
	leaseUUID string,
) shared.Release {
	t.Helper()
	b.provisionsMu.RLock()
	projection := b.provisions[leaseUUID]
	require.NotNil(t, projection, "release fixture requires a live projection")
	items := slices.Clone(projection.Items)
	tenant := projection.Tenant
	providerUUID := projection.ProviderUUID
	profiles := shared.CloneSKUResourceSnapshot(projection.ResourceProfiles)
	stack := projection.StackManifest
	b.provisionsMu.RUnlock()

	require.True(t, backend.IsCanonicalLeaseUUID(leaseUUID))
	require.NotEmpty(t, tenant)
	require.NotEmpty(t, items)
	if providerUUID == "" {
		providerUUID = nominalDockerProviderUUID
	}
	if len(profiles) == 0 {
		profiles = testResourceProfiles(t, items)
	}
	if stack == nil {
		services := make(map[string]*manifest.Manifest, len(items))
		for _, item := range items {
			service := item.ServiceName
			if service == "" {
				service = manifest.DefaultServiceName
			}
			services[service] = &manifest.Manifest{Image: "busybox"}
		}
		stack = &manifest.StackManifest{Services: services}
	}
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	operationID, callbackURL, lifecycleURL := newTestRestoreCallbackAuthority(t)
	authority, err := shared.NewReleaseRuntimeAuthority(
		operationID, tenant, providerUUID, callbackURL, lifecycleURL,
	)
	require.NoError(t, err)

	b.provisionsMu.Lock()
	projection = b.provisions[leaseUUID]
	projection.ProviderUUID = providerUUID
	projection.ResourceProfiles = shared.CloneSKUResourceSnapshot(profiles)
	projection.StackManifest = stack
	projection.CallbackURL = callbackURL
	projection.LifecycleCallbackURL = lifecycleURL
	projection.ActiveOperationID = operationID
	b.provisionsMu.Unlock()

	return seedProvisionReleaseForBackendTest(t, b, leaseUUID, shared.Release{
		Manifest: manifestBytes, Image: "stack", OperationID: operationID,
		Items: items, ResourceProfiles: profiles, RuntimeAuthority: &authority,
		Status: "active", CreatedAt: time.Now(),
	})
}

// commitOperationReleaseWithoutPublishingTest models the exact write-ahead
// crash window after a provision/restore release commit and before terminal
// callback publication. Only a store-issued operation claim can create it.
func commitOperationReleaseWithoutPublishingTest(
	t *testing.T,
	operations operationSettlementService,
	claim shared.OperationIntentClaim,
) shared.OperationReleaseCommitted {
	t.Helper()
	return commitOperationSuccessForTest(t, operations, claim)
}

func beginOperationIntentForSettlementTest(
	t *testing.T,
	operations operationSettlementService,
	spec shared.OperationIntentSpec,
) shared.OperationIntentAdmission {
	t.Helper()
	candidate, err := operations.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	admission, err := operations.BeginOperationIntent(candidate)
	require.NoError(t, err)
	return admission
}

func commitRestoreReleaseWithoutPublishingTest(
	t *testing.T,
	b *Backend,
	entry shared.RetentionEntry,
) shared.OperationIntentClaim {
	t.Helper()
	manifestBytes, err := json.Marshal(entry.StackManifest)
	require.NoError(t, err)
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(
		shared.OperationIntentSpec{
			Kind:                 shared.OperationIntentRestore,
			LeaseUUID:            entry.NewLeaseUUID,
			CallbackURL:          entry.DestinationCallbackURL,
			LifecycleCallbackURL: entry.DestinationLifecycleCallbackURL,
			Tenant:               entry.Tenant,
			ProviderUUID:         entry.ProviderUUID,
			Items:                append([]backend.LeaseItem(nil), entry.DestinationItems...),
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(entry.DestinationResourceProfiles),
			EffectiveItems:       append([]backend.LeaseItem(nil), entry.DestinationItems...),
			Manifest:             manifestBytes,
			SourceLeaseUUID:      entry.OriginalLeaseUUID,
			SourceGeneration:     entry.Generation,
		},
	)
	require.NoError(t, err)
	admission, err := b.operationSettlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, created := admission.CreatedClaim()
	if !created {
		claims, listErr := b.operationSettlement.ListOperationIntents()
		require.NoError(t, listErr)
		found := false
		for _, existing := range claims {
			if existing.LeaseUUID() == entry.NewLeaseUUID &&
				existing.OperationID() == entry.DestinationOperationID {
				claim = existing
				found = true
				break
			}
		}
		require.True(t, found, "exact restore replay has no durable operation claim")
	}
	commitOperationReleaseWithoutPublishingTest(t, b.operationSettlement, claim)
	return claim
}

// activateMaintenanceReleaseForTest drives the complete durable maintenance
// append and activation path but intentionally leaves semantic callback
// publication pending. That models the real actor-drain crash window in which
// the replacement Release is active before its queued terminal message runs.
func activateMaintenanceReleaseForTest(
	t *testing.T,
	maintenance *shared.MaintenanceSettlement,
	leaseUUID string,
	kind shared.MaintenanceIntentKind,
	target shared.Release,
) shared.MaintenanceReleaseActive {
	t.Helper()
	_, source, err := maintenance.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	identity, ok := target.RuntimeIdentity()
	require.True(t, ok, "maintenance target requires complete runtime authority")
	id, err := maintenanceid.New()
	require.NoError(t, err)
	target.Version = 0
	target.Status = "deploying"
	if target.CreatedAt.IsZero() {
		target.CreatedAt = time.Now()
	}
	payload := []byte(nil)
	if kind != shared.MaintenanceIntentRestart {
		payload = target.Manifest
	}
	request, err := maintenance.NewMaintenanceRequestAuthority(
		id, kind, leaseUUID, identity.LifecycleCallbackURL(), payload,
	)
	require.NoError(t, err)
	candidate, err := maintenance.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)
	admission, err := maintenance.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	dispatch, created := admission.CreatedDispatch()
	require.True(t, created)
	appendClaim, err := maintenance.StartMaintenanceAppend(dispatch)
	require.NoError(t, err)
	targetClaim, err := maintenance.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	targetClaim, err = maintenance.BindMaintenanceIntentTarget(targetClaim)
	require.NoError(t, err)
	active, err := activateMaintenanceForTest(t, maintenance, targetClaim)
	require.NoError(t, err)
	return active
}
