package placement

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestDeprovisionEventWaitsForFirstInventoryProjection(t *testing.T) {
	for _, rowKind := range []string{"already represented", "requires durable quarantine"} {
		t.Run(rowKind, func(t *testing.T) {
			const owner = "00000000-0000-4000-8000-000000003101"
			const sibling = "00000000-0000-4000-8000-000000003102"
			store := newTestStore(t)
			baseline := requireAdmissionBaseline(t, store, "backend-a", "backend-b")
			for index, lease := range []string{owner, sibling} {
				id := requireOperationID(t, []string{"3101", "3102"}[index])
				name := []string{"backend-a", "backend-b"}[index]
				token, applied, err := store.beginNewAttempt(requireAdmissionScope(t, store, baseline, name),
					lease, name, id, PayloadFingerprint{}, testBackendRequestSnapshot(t), testCallbackPair(id))
				require.NoError(t, err)
				require.True(t, applied)
				confirmed, err := confirmAttemptForTest(store, token)
				require.NoError(t, err)
				require.True(t, confirmed)
			}
			before := store.Lookup(owner)
			var called []string
			closeLease := func(_ context.Context, leaseUUID string) error { called = append(called, leaseUUID); return nil }
			a := &unrecordedPositiveInventoryBackend{
				executionTestBackend: &executionTestBackend{name: "backend-a", deprovision: closeLease},
				storageID:            testBackendStorageID("backend-a"),
				provisions: []backend.ProvisionInfo{{
					LeaseUUID: owner, BackendName: "backend-a", Tenant: "tenant-test", ProviderUUID: freshTestProviderUUID,
					LifecycleGeneration: &backend.LifecycleGenerationObservation{
						Kind: backend.LifecycleGenerationTyped, ID: store.CurrentLifecycle(owner).ID().String(),
					},
				}},
			}
			if rowKind == "requires durable quarantine" {
				a.provisions[0].LifecycleGeneration.ID = requireOperationID(t, "3109").String()
			}
			b := &unrecordedPositiveInventoryBackend{
				executionTestBackend: &executionTestBackend{name: "backend-b", deprovision: closeLease},
				storageID:            testBackendStorageID("backend-b"),
			}
			base, err := store.BindOperationCoordinator(nil)
			require.NoError(t, err)
			execution := bindExecutionForTest(t, base, newExecutionTestRuntime(a, b))
			provision, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
			require.NoError(t, err)
			reconciliation, err := reconciliationCoordinatorWithReaderForTest(t, execution, &reconciliationSweepReader{
				lease: &billingtypes.Lease{
					Uuid: owner, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
					State: billingtypes.LEASE_STATE_CLOSED,
				},
			})
			require.NoError(t, err)
			sweep, err := reconciliation.BeginSweep()
			require.NoError(t, err)
			defer sweep.End()
			provisions, err := sweep.CollectProvisionInventory(t.Context(), "backend-a")
			require.NoError(t, err)
			// A receipt installs the barrier before the peer endpoint can finish.
			result := provision.DeprovisionEvent(t.Context(), owner)
			require.Equal(t, DeprovisionEventDeferred, result.Disposition())
			require.ErrorIs(t, result.Err(), ErrUnprojectedInventoryPositive)
			deferred := result.Deferred()
			require.True(t, deferred.Valid())
			require.Equal(t, owner, deferred.LeaseUUID())
			require.Equal(t, DeprovisionDeferredInventory, deferred.Reason())
			require.Empty(t, called)
			require.Equal(t, before, store.Lookup(owner))
			for range 3 {
				require.Equal(t, DeprovisionEventDeferred, deferred.Retry(t.Context()).Disposition())
			}
			require.Equal(t, DeprovisionEventCompleted, provision.DeprovisionEvent(t.Context(), sibling).Disposition())
			require.Equal(t, []string{sibling}, called, "another lease retains usable ownership during collection")
			retentions, err := sweep.CollectRetentionInventory(t.Context(), "backend-a")
			require.NoError(t, err)
			_, err = sweep.RecordBackendInventory(provisions, retentions)
			require.NoError(t, err)
			peerProvisions, err := sweep.CollectProvisionInventory(t.Context(), "backend-b")
			require.NoError(t, err)
			peerRetentions, err := sweep.CollectRetentionInventory(t.Context(), "backend-b")
			require.NoError(t, err)
			_, err = sweep.RecordBackendInventory(peerProvisions, peerRetentions)
			require.NoError(t, err)
			require.NoError(t, sweep.SealInventory())
			projected, err := sweep.Project(ReconciliationProjection{Placements: map[string]string{owner: "backend-a"}})
			require.NoError(t, err)
			// No second sweep or cadence is needed. Projection discharges the exact
			// represented or newly quarantined row without a retry crossing its boundary.
			if rowKind == "requires durable quarantine" {
				require.Equal(t, LifecycleVerdictUnusable, store.CurrentLifecycle(owner).Verdict())
			}
			orphan, disposition, err := projected.ObserveTerminalOrphan(t.Context(), owner)
			require.NoError(t, err)
			require.Equal(t, ReconciliationObservationReady, disposition,
				"a scheduling-only wait must not invalidate the same sweep's terminal cleanup observation")
			require.True(t, orphan.Valid())
			require.True(t, reconciliation.ReleaseOrphanAction(orphan))
			result = deferred.Retry(t.Context())
			require.Equal(t, DeprovisionEventCompleted, result.Disposition(), result.Err())
			require.NoError(t, result.Err())
			require.False(t, result.Deferred().Valid())
			require.Equal(t, []string{sibling, owner}, called)
		})
	}
}

func TestDeprovisionInventoryWaitKeepsLiveOperation(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000003103"
	fixture := newProvisionDispatchFixture(t, leaseUUID)
	call, calling := fixture.coordinator.beginProvisionCall(fixture.dispatch)
	require.True(t, calling)
	require.True(t, fixture.coordinator.completeProvision(call, backend.ConservativeProvisionCallOutcome(nil)).Applied())
	before, found := fixture.coordinator.Lookup(leaseUUID)
	require.True(t, found)
	var calls int
	client := &executionTestBackend{name: "backend-a", deprovision: func(context.Context, string) error { calls++; return nil }}
	execution := bindExecutionForTest(t, fixture.coordinator, newExecutionTestRuntime(client))
	provision, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	reconciliation, err := reconciliationCoordinatorWithReaderForTest(t, execution, &reconciliationSweepReader{})
	require.NoError(t, err)
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	require.NoError(t, sweep.RecordProvision("backend-a", testBackendStorageID("backend-a"), []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID, BackendName: "backend-a", Tenant: "tenant-test", ProviderUUID: freshTestProviderUUID,
		LifecycleGeneration: &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped, ID: fixture.initiation.ID().String(),
		},
	}}))
	// Exercise the compatibility API too: a pre-dispatch wait must not finish
	// the live Registry operation while its durable generation remains pending.
	require.ErrorIs(t, provision.Deprovision(t.Context(), leaseUUID), ErrUnprojectedInventoryPositive)
	after, found := fixture.coordinator.Lookup(leaseUUID)
	require.True(t, found, "inventory wait must release, rather than finish, exact settlement ownership")
	require.Equal(t, before, after)
	require.Zero(t, calls)
	require.NoError(t, sweep.RecordRetention("backend-a", testBackendStorageID("backend-a"), nil))
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(ReconciliationProjection{Placements: map[string]string{leaseUUID: "backend-a"}})
	require.NoError(t, err)
	require.NoError(t, provision.Deprovision(t.Context(), leaseUUID))
	require.Equal(t, 1, calls)
	require.False(t, fixture.coordinator.RuntimeController().Contains(leaseUUID))
}

func TestDeprovisionEventDeferralRequiresLocalAuthority(t *testing.T) {
	for _, name := range []string{"lease busy", "settlement busy", "draining", "canceled", "namespace withdrawn", "arbitrary inventory error", "bare circuit error"} {
		t.Run(name, func(t *testing.T) {
			fixture := newProvisionDispatchFixture(t, "close-deferral")
			var calls int
			client := &executionTestBackend{name: "backend-a", deprovision: func(context.Context, string) error { calls++; return nil }}
			execution := bindExecutionForTest(t, fixture.coordinator, newExecutionTestRuntime(client))
			provision, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
			require.NoError(t, err)
			ctx := t.Context()
			want := DeprovisionEventFailed
			switch name {
			case "lease busy":
				fixture.coordinator.operations.AbortInitiation(fixture.initiation)
				want = DeprovisionEventDeferred
			case "settlement busy":
				// The exact operation is still Preparing and cannot be claimed.
				want = DeprovisionEventDeferred
			default:
				call, calling := fixture.coordinator.beginProvisionCall(fixture.dispatch)
				require.True(t, calling)
				require.True(t, fixture.coordinator.completeProvision(call, backend.ConservativeProvisionCallOutcome(nil)).Applied())
				if name == "draining" {
					claim := fixture.coordinator.tryClaimDeprovision("close-deferral")
					require.True(t, claim.Claimed())
					require.True(t, fixture.coordinator.finishDeprovision(claim.Claim()))
					fixture.coordinator.RuntimeController().BeginDrain()
				}
				if name == "canceled" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				if name == "namespace withdrawn" {
					require.NoError(t, fixture.store.Close())
				}
				if name == "arbitrary inventory error" {
					client.deprovision = func(context.Context, string) error {
						calls++
						return errors.Join(ErrUnprojectedInventoryPositive, errors.New("foreign collaborator"))
					}
				}
				if name == "bare circuit error" {
					client.deprovision = func(context.Context, string) error { calls++; return backend.ErrCircuitOpen }
				}
			}
			result := provision.DeprovisionEvent(ctx, "close-deferral")
			require.Equal(t, want, result.Disposition(), result.Err())
			require.Error(t, result.Err())
			if want == DeprovisionEventDeferred {
				require.Equal(t, DeprovisionDeferredLifecycle, result.Deferred().Reason())
				require.Zero(t, calls)
			} else {
				require.False(t, result.Deferred().Valid())
			}
		})
	}
	var zero DeferredDeprovision
	assert.False(t, zero.Valid())
	assert.Empty(t, zero.LeaseUUID())
	assert.Empty(t, zero.Reason())
	assert.Equal(t, DeprovisionEventFailed, zero.Retry(t.Context()).Disposition())
}

func TestDeprovisionLifecyclePendingKeepsLiveOperation(t *testing.T) {
	const lease = "00000000-0000-4000-8000-000000003107"
	fixture := newProvisionDispatchFixture(t, lease)
	call, calling := fixture.coordinator.beginProvisionCall(fixture.dispatch)
	require.True(t, calling)
	require.True(t, fixture.coordinator.completeProvision(call, backend.ConservativeProvisionCallOutcome(nil)).Applied())
	before, found := fixture.coordinator.Lookup(lease)
	require.True(t, found)
	id, bound := fixture.store.ExpectedBackendStorageIdentity("backend-a")
	require.True(t, bound)
	var drained atomic.Bool
	var teardown atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, id.String())
		if !drained.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"pending","code":"lifecycle_pending"}`))
			return
		}
		teardown.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	policy, err := backend.NewConnectionPolicy(backend.ConnectionConfig{
		Name: "backend-a", BaseURL: server.URL, Secret: causalOutcomeTestSecret,
	})
	require.NoError(t, err)
	client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{CBFailureThresh: 1}, causalOutcomeTestIdentity{id: id})
	require.NoError(t, err)
	execution := bindExecutionForTest(t, fixture.coordinator, newExecutionTestRuntime(client))
	provision, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	result := provision.DeprovisionEvent(t.Context(), lease)
	for range 3 {
		require.Equal(t, DeprovisionEventDeferred, result.Disposition(), result.Err())
		require.Equal(t, DeprovisionDeferredLifecycle, result.Deferred().Reason())
		after, found := fixture.coordinator.Lookup(lease)
		require.True(t, found, "pending close releases its claim without finishing the live operation")
		require.Equal(t, before, after)
		require.Zero(t, teardown.Load())
		result = result.Deferred().Retry(t.Context())
	}
	drained.Store(true)
	require.Equal(t, DeprovisionEventCompleted, result.Deferred().Retry(t.Context()).Disposition())
	require.EqualValues(t, 1, teardown.Load())
	require.False(t, fixture.coordinator.RuntimeController().Contains(lease))
}

func TestDeprovisionEventRequiresEveryFailedCandidateToCarryDeferralProvenance(t *testing.T) {
	for _, availability := range []string{"circuit open", "lifecycle pending"} {
		t.Run(availability, func(t *testing.T) {
			for _, peerResult := range []string{"success", "unknown effect", "unknown effect first"} {
				t.Run(peerResult, func(t *testing.T) {
					const leaseUUID = "00000000-0000-4000-8000-000000003104"
					store := newTestStore(t)
					requireAdmissionBaseline(t, store, "backend-a", "backend-b")
					requireConflictPlacement(t, store, leaseUUID, "backend-a", "backend-b")
					pendingName, peerName := "backend-a", "backend-b"
					if peerResult == "unknown effect first" {
						pendingName, peerName = peerName, pendingName
					}
					id, bound := store.ExpectedBackendStorageIdentity(pendingName)
					require.True(t, bound)
					var httpCalls atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						httpCalls.Add(1)
						w.Header().Set(backendidentity.ResponseHeader, id.String())
						if availability == "lifecycle pending" {
							w.WriteHeader(http.StatusServiceUnavailable)
							_, _ = w.Write([]byte(`{"error":"pending","code":"lifecycle_pending"}`))
							return
						}
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = w.Write([]byte(`{"error":"uncertain"}`))
					}))
					t.Cleanup(server.Close)
					policy, err := backend.NewConnectionPolicy(backend.ConnectionConfig{
						Name: pendingName, BaseURL: server.URL, Secret: causalOutcomeTestSecret,
					})
					require.NoError(t, err)
					client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{
						CBFailureThresh: 1, CBTimeout: time.Hour,
					}, causalOutcomeTestIdentity{id: id})
					require.NoError(t, err)
					if availability == "circuit open" {
						_, err = client.GetProvision(t.Context(), leaseUUID)
						require.Error(t, err, "trip the real client circuit with an uncertain response")
					}
					var peerCalls int
					peer := &executionTestBackend{name: peerName, deprovision: func(context.Context, string) error {
						peerCalls++
						if peerResult != "success" {
							return errors.Join(backend.ErrCircuitOpen, context.DeadlineExceeded)
						}
						return nil
					}}
					base, err := store.BindOperationCoordinator(nil)
					require.NoError(t, err)
					execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client, peer))
					provision, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
					require.NoError(t, err)
					result := provision.DeprovisionEvent(t.Context(), leaseUUID)
					require.Equal(t, int32(1), httpCalls.Load())
					require.Equal(t, 1, peerCalls)
					require.Error(t, result.Err(), "a successful sibling does not complete the remaining close")
					if peerResult == "success" {
						require.Equal(t, DeprovisionEventDeferred, result.Disposition())
						wantReason := DeprovisionDeferredBackendUnavailable
						if availability == "lifecycle pending" {
							wantReason = DeprovisionDeferredLifecycle
						}
						require.Equal(t, wantReason, result.Deferred().Reason())
					} else {
						require.Equal(t, DeprovisionEventFailed, result.Disposition())
						require.False(t, result.Deferred().Valid(), "one exact refusal cannot classify another backend's uncertainty")
					}
				})
			}
		})
	}
}

func TestDeferredDeprovisionRetriesExactHTTPSubjectAfterCircuitRecovery(t *testing.T) {
	const owner = "00000000-0000-4000-8000-000000003105"
	const sibling = "00000000-0000-4000-8000-000000003106"
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a", "backend-b")
	requireConfirmedPlacement(t, store, owner, "backend-a")
	requireConfirmedPlacement(t, store, sibling, "backend-b")
	id, bound := store.ExpectedBackendStorageIdentity("backend-a")
	require.True(t, bound)
	deprovisionPath, err := backendidentity.BoundPath(id, "/deprovision")
	require.NoError(t, err)
	var healthy atomic.Bool
	requests := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, id.String())
		if request.URL.Path == deprovisionPath && request.Method == http.MethodPost {
			var body struct {
				LeaseUUID string `json:"lease_uuid"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			requests <- body.LeaseUUID
		}
		if !healthy.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"unknown outcome"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	policy, err := backend.NewConnectionPolicy(backend.ConnectionConfig{
		Name: "backend-a", BaseURL: server.URL, Secret: causalOutcomeTestSecret,
	})
	require.NoError(t, err)
	client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{
		CBFailureThresh: 1, CBTimeout: time.Second,
	}, causalOutcomeTestIdentity{id: id})
	require.NoError(t, err)
	var peerCalls []string
	peer := &executionTestBackend{name: "backend-b", deprovision: func(_ context.Context, leaseUUID string) error {
		peerCalls = append(peerCalls, leaseUUID)
		return nil
	}}
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client, peer))
	provision, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	_, err = client.GetProvision(t.Context(), owner)
	require.Error(t, err)
	result := provision.DeprovisionEvent(t.Context(), owner)
	require.Equal(t, DeprovisionEventDeferred, result.Disposition(), result.Err())
	proof := result.Deferred()
	require.Equal(t, DeprovisionDeferredBackendUnavailable, proof.Reason())
	require.Empty(t, requests)
	require.Equal(t, DeprovisionEventCompleted, provision.DeprovisionEvent(t.Context(), sibling).Disposition())
	require.Equal(t, []string{sibling}, peerCalls)
	healthy.Store(true)
	// The real breaker owns its clock. Poll its public boundary with the same
	// opaque proof; each pre-expiry attempt must remain an explicit deferral.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		result = proof.Retry(t.Context())
		if result.Disposition() != DeprovisionEventCompleted {
			assert.Equal(t, DeprovisionEventDeferred, result.Disposition(), "only local deferral is valid before recovery")
		}
		assert.Equal(collect, DeprovisionEventCompleted, result.Disposition())
	}, 5*time.Second, 10*time.Millisecond)
	require.Len(t, requests, 1, "recovery posts to the exact storage-bound endpoint")
	require.Equal(t, owner, <-requests)
	require.Empty(t, requests)
	require.Equal(t, []string{sibling}, peerCalls, "owner recovery cannot choose the healthy peer")
	healthy.Store(false)
	result = proof.Retry(t.Context())
	require.Equal(t, DeprovisionEventFailed, result.Disposition(), "a later request reached transport and is uncertain")
	require.Error(t, result.Err())
	require.False(t, result.Deferred().Valid())
	require.Len(t, requests, 1)
	require.Equal(t, owner, <-requests)
}

func TestDeprovisionEventUnaccountableOwnerCannotBeHiddenByPendingConfiguredBackend(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000003107"
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a")
	// A new inventory-positive orphan has only one untrusted reporter; it
	// cannot establish that all historical owners have been reached.
	projectInventoryForTest(t, store, InventoryProjection{
		UntrustedPositives: map[string][]string{leaseUUID: {"backend-a"}},
	})
	require.Equal(t, StateUnusable, store.Lookup(leaseUUID).State())

	identity, bound := store.ExpectedBackendStorageIdentity("backend-a")
	require.True(t, bound)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set(backendidentity.ResponseHeader, identity.String())
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"pending","code":"lifecycle_pending"}`))
	}))
	t.Cleanup(server.Close)
	policy, err := backend.NewConnectionPolicy(backend.ConnectionConfig{
		Name: "backend-a", BaseURL: server.URL, Secret: causalOutcomeTestSecret,
	})
	require.NoError(t, err)
	client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{}, causalOutcomeTestIdentity{id: identity})
	require.NoError(t, err)
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
	coordinator, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	result := coordinator.DeprovisionEvent(t.Context(), leaseUUID)
	require.EqualValues(t, 1, calls.Load())
	require.Equal(t, DeprovisionEventFailed, result.Disposition())
	require.ErrorIs(t, result.Err(), ErrDeprovisionAuthorityUnresolvable)
	require.False(t, result.Deferred().Valid(), "the missing historical owner cannot be replaced by a configured backend's wait")
}
