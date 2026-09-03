package docker

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

const (
	maintenanceSerializationLeaseUUID    = "11111111-1111-4111-8111-111111111111"
	maintenanceSerializationProviderUUID = "22222222-2222-4222-8222-222222222222"
)

// TestMaintenanceReleasePreludeSerializedThroughActorAcceptance pins the
// release/actor ordering contract. actorsMu is an intentional test barrier:
// the first command has durably appended its release but cannot yet ask the
// actor for admission. A second command must remain behind commandFence rather
// than append an unowned generation which the first worker's ActivateLatest
// would later activate.
func TestMaintenanceReleasePreludeSerializedThroughActorAcceptance(t *testing.T) {
	tests := []struct {
		name   string
		first  func(*Backend) error
		second func(*Backend) error
	}{
		{
			name: "update then restart",
			first: func(b *Backend) error {
				return b.Update(context.Background(), backend.UpdateRequest{
					LeaseUUID: maintenanceSerializationLeaseUUID,
					Payload:   validManifestJSON("docker.io/library/nginx:1.27"),
				})
			},
			second: func(b *Backend) error {
				return b.Restart(context.Background(), backend.RestartRequest{LeaseUUID: maintenanceSerializationLeaseUUID})
			},
		},
		{
			name: "restart then update",
			first: func(b *Backend) error {
				return b.Restart(context.Background(), backend.RestartRequest{LeaseUUID: maintenanceSerializationLeaseUUID})
			},
			second: func(b *Backend) error {
				return b.Update(context.Background(), backend.UpdateRequest{
					LeaseUUID: maintenanceSerializationLeaseUUID,
					Payload:   validManifestJSON("docker.io/library/nginx:1.27"),
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stack, err := manifest.ParsePayload(validManifestJSON("docker.io/library/nginx:1.26"))
			require.NoError(t, err)
			items := []backend.LeaseItem{{
				SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
			}}
			workerStarted := make(chan struct{})
			var workerStartedOnce sync.Once
			mock := &mockDockerClient{
				PullImageFn: func(ctx context.Context, _ string, _ time.Duration) error {
					workerStartedOnce.Do(func() { close(workerStarted) })
					<-ctx.Done()
					return ctx.Err()
				},
			}
			b := newBackendForProvisionTest(t, mock, map[string]*provision{
				maintenanceSerializationLeaseUUID: {ProvisionState: leasesm.ProvisionState{
					LeaseUUID: maintenanceSerializationLeaseUUID, Tenant: "tenant-a", ProviderUUID: maintenanceSerializationProviderUUID,
					SKU: "docker-small", Status: backend.ProvisionStatusReady,
					StackManifest: stack, Items: items,
				}},
			})
			b.compose = &mockComposeExecutor{
				UpFn: func(ctx context.Context, _ *composetypes.Project, _ composeUpOpts) error {
					workerStartedOnce.Do(func() { close(workerStarted) })
					<-ctx.Done()
					return ctx.Err()
				},
			}
			releases := seedMaintenanceSerializationStores(t, b, stack, items)
			t.Cleanup(func() {
				b.stopCancel()
				b.wg.Wait()
			})

			// Prevent either request from routing to the actor. The accepted
			// command therefore keeps commandFence held after its release Append.
			b.actorsMu.Lock()
			actorsLocked := true
			defer func() {
				if actorsLocked {
					b.actorsMu.Unlock()
				}
			}()

			firstDone := make(chan error, 1)
			go func() { firstDone <- test.first(b) }()
			require.Eventually(t, func() bool {
				got, listErr := releases.List(maintenanceSerializationLeaseUUID)
				return listErr == nil && len(got) == 2
			}, time.Second, time.Millisecond, "first command never appended its release prelude")

			secondEntered := make(chan struct{})
			secondDone := make(chan error, 1)
			go func() {
				close(secondEntered)
				secondDone <- test.second(b)
			}()
			<-secondEntered
			require.Never(t, func() bool {
				got, listErr := releases.List(maintenanceSerializationLeaseUUID)
				return listErr != nil || len(got) != 2
			}, 100*time.Millisecond, time.Millisecond,
				"losing command appended a release before actor admission")

			b.actorsMu.Unlock()
			actorsLocked = false

			require.NoError(t, <-firstDone)
			secondErr := <-secondDone
			require.Error(t, secondErr)
			assert.True(t, errors.Is(secondErr, backend.ErrInvalidState), secondErr)
			select {
			case <-workerStarted:
			case <-time.After(time.Second):
				t.Fatal("accepted maintenance worker did not start")
			}

			got, err := releases.List(maintenanceSerializationLeaseUUID)
			require.NoError(t, err)
			require.Len(t, got, 2,
				"one actor acceptance must add exactly one generation to its source")
			assert.Equal(t, "active", got[0].Status)
			assert.Equal(t, "deploying", got[1].Status)
		})
	}
}

func TestMaintenanceReleaseFenceWaitsForAckAfterCallerCancellation(t *testing.T) {
	stack, err := manifest.ParsePayload(validManifestJSON("docker.io/library/nginx:1.26"))
	require.NoError(t, err)
	workerStarted := make(chan struct{})
	var workerStartedOnce sync.Once
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, _ string, _ time.Duration) error {
			workerStartedOnce.Do(func() { close(workerStarted) })
			<-ctx.Done()
			return ctx.Err()
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		maintenanceSerializationLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: maintenanceSerializationLeaseUUID, Tenant: "tenant-a", ProviderUUID: maintenanceSerializationProviderUUID,
			SKU: "docker-small", Status: backend.ProvisionStatusReady,
			StackManifest: stack,
			Items: []backend.LeaseItem{{
				SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
			}},
		}},
	})
	releases := seedMaintenanceSerializationStores(t, b, stack, b.provisions[maintenanceSerializationLeaseUUID].Items)
	// Construct the actor before the barriers. Actor construction reads the
	// provision status; the test wants provisionsMu to block the transition,
	// not construction itself.
	actor := b.actorFor(maintenanceSerializationLeaseUUID)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})

	b.actorsMu.Lock()
	actorsLocked := true
	defer func() {
		if actorsLocked {
			b.actorsMu.Unlock()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- b.Update(ctx, backend.UpdateRequest{
			LeaseUUID: maintenanceSerializationLeaseUUID,
			Payload:   validManifestJSON("docker.io/library/nginx:1.27"),
		})
	}()
	require.Eventually(t, func() bool {
		got, listErr := releases.List(maintenanceSerializationLeaseUUID)
		return listErr == nil && len(got) == 2
	}, time.Second, time.Millisecond)

	// Once actorsMu opens the request can enqueue, but the actor cannot publish
	// Updating (and therefore cannot ack) until provisionsMu opens. Observe the
	// actor inside handle before introducing cancellation: release history alone
	// proves only that the durable prelude completed, not that the caller passed
	// routeToLeaseBlocking's initial context check.
	b.provisionsMu.Lock()
	provisionsLocked := true
	defer func() {
		if provisionsLocked {
			b.provisionsMu.Unlock()
		}
	}()
	b.actorsMu.Unlock()
	actorsLocked = false
	require.Eventually(t, func() bool {
		return actor.CurrentMessageStart() != 0
	}, time.Second, time.Millisecond,
		"update request did not reach the actor admission transition")
	cancel()
	require.Never(t, func() bool {
		select {
		case <-updateDone:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, time.Millisecond,
		"caller cancellation released the release fence before actor admission")

	b.provisionsMu.Unlock()
	provisionsLocked = false
	require.NoError(t, <-updateDone,
		"an accepted operation remains authoritative when caller cancellation races its ack")
	select {
	case <-workerStarted:
	case <-time.After(time.Second):
		t.Fatal("accepted Update worker did not start")
	}
}

// seedMaintenanceSerializationStores gives the ordering tests the same typed,
// durable source authority that a live provision/restore commits before any
// maintenance command can be admitted.
func seedMaintenanceSerializationStores(
	t *testing.T,
	b *Backend,
	stack *manifest.StackManifest,
	items []backend.LeaseItem,
) *shared.ReleaseStore {
	t.Helper()
	releases := attachReleaseStore(t, b)
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	b.callbackStore = callbacks

	operationID, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
	profiles := testResourceProfiles(t, items)
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	require.NoError(t, releases.AppendActive(maintenanceSerializationLeaseUUID, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      operationID,
		Items:            append([]backend.LeaseItem(nil), items...),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(profiles),
		RuntimeAuthority: mustTestReleaseRuntimeAuthority(
			t, operationID, "tenant-a", maintenanceSerializationProviderUUID,
			callbackURL, lifecycleCallbackURL,
		),
		Status:    "active",
		CreatedAt: time.Now(),
	}))

	b.provisionsMu.Lock()
	provision := b.provisions[maintenanceSerializationLeaseUUID]
	provision.CallbackURL = callbackURL
	provision.LifecycleCallbackURL = lifecycleCallbackURL
	provision.ResourceProfiles = shared.CloneSKUResourceSnapshot(profiles)
	b.provisionsMu.Unlock()
	return releases
}

// TestRestoreFinalizerRejectsUpdate proves that a newer topology cannot be
// admitted while the restore's exact destination authority is still durable.
func TestRestoreFinalizerRejectsUpdate(t *testing.T) {
	const (
		originalLease = "0192f1a0-1111-7abc-8def-000000000101"
		newLease      = "0192f1a0-2222-7abc-8def-000000000102"
	)
	stack, err := manifest.ParsePayload(validManifestJSON("docker.io/library/nginx:1.26"))
	require.NoError(t, err)
	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	profiles := testResourceProfiles(t, items)
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		newLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: newLease, Tenant: "tenant-a", ProviderUUID: "provider-a",
			SKU: "docker-small", Status: backend.ProvisionStatusReady,
			StackManifest: stack, Items: items,
		}, ResourceProfiles: profiles},
	})
	releases := attachReleaseStore(t, b)
	retentions := attachRetentionStore(t, b)
	record := shared.RetentionEntry{
		OriginalLeaseUUID: originalLease, NewLeaseUUID: newLease,
		Tenant: "tenant-a", ProviderUUID: "provider-a",
		Items: items, ResourceProfiles: profiles, StackManifest: stack,
		Status: shared.RetentionStatusRestoring, Generation: 2,
	}
	putRestoringRetention(t, retentions, record)

	err = b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: newLease,
		Payload:   validManifestJSON("docker.io/library/nginx:1.27"),
	})
	require.ErrorIs(t, err, backend.ErrInvalidState)

	got, err := releases.List(newLease)
	require.NoError(t, err)
	assert.Empty(t, got, "rejected Update must not publish a release prelude")
	remaining, err := retentions.Get(originalLease)
	require.NoError(t, err)
	require.NotNil(t, remaining, "busy destination must keep the restore finalizer")
	assert.Equal(t, shared.RetentionStatusRestoring, remaining.Status)
}

func TestFailedRestoreDoesNotSettleResidualReleaseHistory(t *testing.T) {
	const leaseUUID = "0192f1a0-2222-7abc-8def-000000000103"
	stack, err := manifest.ParsePayload(validManifestJSON("docker.io/library/nginx:1.27"))
	require.NoError(t, err)
	mock := &mockDockerClient{
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return nil, errors.New("injected restore image inspection failure")
		},
	}
	b := newBackendForTest(mock, nil)
	releases := attachReleaseStore(t, b)
	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	require.NoError(t, releases.Append(leaseUUID, shared.Release{
		Manifest:         validManifestJSON("docker.io/library/nginx:1.25"),
		Image:            "stack",
		Items:            items,
		ResourceProfiles: testResourceProfiles(t, items),
		Status:           "active",
		CreatedAt:        time.Now(),
	}))

	result := b.doReplaceContainers(context.Background(), replaceContainersOp{
		LeaseUUID:         leaseUUID,
		Stack:             stack,
		Items:             items,
		ResourceProfiles:  testResourceProfiles(t, items),
		Operation:         "restore",
		NoComposeRollback: true,
		Logger:            b.logger,
	})
	require.Error(t, result.Err)

	got, err := releases.List(leaseUUID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "active", got[0].Status,
		"restore has no deploying generation and must not settle unrelated history")
	assert.Empty(t, got[0].Reason)
	assert.Empty(t, got[0].Message)
}
